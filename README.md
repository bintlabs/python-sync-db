dbsync
======

A python library for centralized database synchronization, built over
[SQLAlchemy's ORM](http://docs.sqlalchemy.org/en/latest/orm/tutorial.html).

The library aims to enable applications to function offline when their
internet connection is lost, by using a local database and providing a
few synchronization procedures: `pull`, `push`, `register` and
`repair`.

This library is currently undergoing testing in a real application.

## Restrictions ##

To work properly however, the library requires that several
restrictions be met:

- All primary keys must be integer values of no importance to the
  logic of the application. If using SQLite, these would be INTEGER
  PRIMARY KEY AUTOINCREMENT fields.

- All primary keys must be unique to the row through the history of
  the table. This means the reuse of primary keys is likely to cause
  problems. In SQLite this behaviour can be achieved by changing the
  default PRIMARY KEY algorithm to AUTOINCREMENT (the
  `sqlite_autoincrement` must be set to `True` for the table in
  SQLAlchemy as specified
  [here](http://docs.sqlalchemy.org/en/rel_0_8/dialects/sqlite.html#auto-incrementing-behavior)).

- All synched tables should be wrapped with a mapped class. This
  includes many-to-many tables. This restriction should hopefully be
  lifted in the future, though for now you should follow this
  [suggested pattern](http://docs.sqlalchemy.org/en/rel_0_8/orm/relationships.html#association-object).

- Push and pull client-side synchronization procedures can't be
  invoked parallel to other transactions. This is consequence of bad
  design and should change in the future. For now, don't invoke these
  in a seperate thread if other transactions might run concurrently,
  or the transaction won't be registered correctly.

## Explanation ##

Dbsync works by registering database operations (insert, update,
delete) in seperate tables. These are detected through the SQLAlchemy
event interface, and form a kind of operations log.

The synchronization process starts with the `push` procedure. In it,
the client application builds a message containing only the required
database objects, deciding which to include according to the
operations log, and sends it to the server to execute. If the server
allows the `push`, both the client and the server databases become
equivalent and the process is complete.

The `push` won't be allowed by the server if it's database has
advanced further since the last synchronization. If the `push` is
rejected, the client should execute the `pull` procedure. The `pull`
will fetch all operations executed on the server since the divergence
point, and will merge those with the client's operation log. This
merge operation executes internally and includes the conflict
resolution phase, which ideally will resolve the potential operation
collisions ([further explanation][merge-subroutine] of the merge
subroutine).

[merge-subroutine]: https://github.com/bintlabs/python-sync-db/blob/master/merge.md

If the `pull` procedure completes successfully, the client application
may attempt another `push`, as shown by the cycle in the diagram
below.

![Synchronization sequence](https://raw.github.com/bintlabs/python-sync-db/master/diagram.png)

### Additional procedures ###

#### Registering nodes ####

The `register` procedure exists to provide a mechanism for nodes to be
identified by the server. A node may request it's registration through
the `register` procedure, and if accepted, it will receive a set of
credentials.

These credentials are used (as of this revision) to sign the `push`
message sent by the node, since it's the only procedure that can
potentially destroy data on the server.

Other procedures should also be protected by the programmer (e.g. to
prevent theft), but that is her/his responsibility. Synchronization
procedures usually allow the inclusion of user-set data, which can be
checked on the server for authenticity. Also, the HTTPS protocol may
be used by prepending the 'https://' prefix to the URL for each
procedure.

#### Repairing the client's database ####

The `repair` procedure exists to allow the client application's
database to recover from otherwise stale states. Such a state should
in theory be impossible to reach, but external database intervention,
or poor conflict resolution by this library (which will be monitored
in private testing), might result in achieving it.

The `repair` just fetches the entire server database, serialized as
JSON, and then replaces the current one with it. Since it's meant to
be used to fix infrequent errors, and might take a long time to
complete, it should not be used recurrently.

### Example ###

First, give the library a SQLAlchemy engine to access the database. On
the client application, the current tested database is SQLite.

```python
from sqlalchemy import create_engine
import dbsync

engine = create_engine("sqlite:///storage.db") # sample database URL

dbsync.set_engine(engine)
```

If you don't do this, the library will complain as soon as you attempt
an operation.

Next, start tracking your operations to fill the opertions log. Use
the `dbsync.client.track` or the `dbsync.server.track` depending on
your application. Don't import both `dbsync.client` and
`dbsync.server`.

```python
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from dbsync import client


Base = declarative_base()
Base.__table_args__ = {'sqlite_autoincrement': True,} # important


@client.track
class City(Base):

    __tablename__ = "city"

    id = Column(Integer, primary_key=True) # doesn't have to be called 'id'
    name = Column(String(100))


@client.track
class Person(Base):

    __tablename__ = "person"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    city_id = Column(Integer, ForeignKey("city.id"))

    city = relationship(City, backref="persons")
```

After you've marked all the models you want tracked, you need to
generate the logging infrastructure, explicitly. You can do this once,
or every time the application is started, since it's idempotent.

```python
import dbsync

dbsync.create_all()
```

Next you should register your client application in the server. To do
this, use the `register` procedure:

```python
from dbsync import client

client.register(REGISTER_URL)
```

Where `REGISTER_URL` is the URL pointing to the register handler on
the server. More on this below.

You can register the client application just once, or check whenever
you wish with the `isregistered` predicate.

```python
from dbsync import client

if not client.isregistered():
   client.register(REGISTER_URL)
```

Now you're ready to try synchronization procedures. If the server is
configured correctly (as shown further below), an acceptable
synchronization cycle could be:

```python
from dbsync import client


def synchronize(push_url, pull_url, tries):
    for _ in range(tries):
        try:
            return client.push(push_url)
        except client.PushRejected:
            try:
                client.pull(pull_url)
            except client.UniqueConstraintError as e:
                for model, pk, columns in e.entries:
                    pass # handle exception
    raise Exception("push rejected %d times" % tries)
```

You may catch the different exceptions and react accordingly, since
they can indicate lack of internet connection, integrity conflicts, or
dbsync configuration problems.

#### Server side ####

First of all, instead of importing `dbsync.client`, import
`dbsync.server`. So, to track a model:

```python
from dbsync import server

@server.track
class Person(Base):
    # ...
```

Then, listen to five distinct URLs:

- One for the `repair` procedure, listening GETs.
- One for the `register` procedure, listening POSTs.
- One for the `pull` procedure, listening POSTs.
- One for the `push` procedure, listening POSTs.
- One (optional) for the `query` procedure (for remote queries),
  listening GETs.

These handlers should return JSON and use the dbsync handlers. For
example, using [Flask](http://flask.pocoo.org/):

```python
import json
from flask import Flask, request
from dbsync import server


app = Flask(__name__)


@app.route("/repair", methods=["GET"])
def repair():
    return (json.dumps(server.handle_repair(request.args)),
            200,
            {"Content-Type": "application/json"})


@app.route("/register", methods=["POST"])
def register():
    return (json.dumps(server.handle_register()),
            200,
            {"Content-Type": "application/json"})


@app.route("/pull", methods=["POST"])
def pull():
    return (json.dumps(server.handle_pull_request(request.json)),
            200,
            {"Content-Type": "application/json"})


@app.route("/push", methods=["POST"])
def push():
    try:
        return (json.dumps(server.handle_push(request.json)),
                200,
                {"Content-Type": "application/json"})
    except server.handlers.PushRejected as e:
        return (json.dumps({'error': [repr(arg) for arg in e.args]}),
                400,
                {"Content-Type": "application/json"})


@app.route("/query", methods=["GET"])
def query():
    return (json.dumps(server.handle_query(request.args)),
            200,
            {"Content-Type": "application/json"})
```

Messages to the server usually contain additional user-set data, to
allow for extra checks and custom protection. You can access these
through `request.json.extra_data` when JSON is expected.
