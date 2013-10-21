dbsync
======

A python library for centralized database synchronization, built over
SQLAlchemy's ORM.

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
collisions.

![Synchronization sequence](https://raw.github.com/bintlabs/python-sync-db/master/diagram.png)

If the `pull` procedure completes successfully, the client application
may attempt another `push`, as shown by the cycle in the diagram
above.

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

TODO an example.
