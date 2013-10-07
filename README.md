dbsync
======

A python library for centralized database synchronization, built over
SQLAlchemy's ORM.

The library aims to enable applications to function offline when their
internet connection is lost, by using a local database and providing a
few synchronization procedures: `pull`, `push` and `register`.

Until otherwise stated, assume it hasn't yet been tested in a real
application.

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
  SQLAlchemy
  [docs](http://docs.sqlalchemy.org/en/rel_0_8/dialects/sqlite.html#auto-incrementing-behavior)).

- All synched tables should be wrapped with a mapped class. This
  includes many-to-many tables. This restriction should hopefully be
  lifted in the future.

- Push and pull client-side synchronization procedures can't be
  invoked parallel to other transactions. This is consequence of bad
  design and should change in the future. For now, don't invoke these
  in a seperate thread if other transactions might run concurrently,
  or the transaction won't be registered correctly.

## Explanation ##

Dbsync works by registering database operations (insert, update,
delete) in seperate tables. These are detected through the SQLAlchemy
event interface.

TODO explain further.

### Examples ###

TODO an example.
