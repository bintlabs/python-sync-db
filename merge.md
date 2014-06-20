Merge
=====

The merge operation occurs during the pull synchronization procedure,
and it's purpose is to consolidate the changes fetched from the server
(a journal of operations performed by other nodes) with the changes
performed locally since the last sychronization (last successful push
call). After the merge, the local database should roughly be left in
the state that could have been reached by applying the changes from
the server before modifying the local state, and then changing the
local database in whatever application-specific manner it was
changed. The expected behaviour is somewhat similar to the ['git
rebase'][git-rebase] command.

[git-rebase]: http://www.git-scm.com/book/en/Git-Branching-Rebasing

Operations contained either in the server-sent message or the local
journal are simply references to the object operated upon, and are
split in three groups:

* U: updates
* I: inserts
* D: deletes

To differentiate sent operations from local ones, the following
notation is used:

* U_m: updates in the message (sent from the server)
* U_l: local updates
* I_m: inserts in the message
* I_l: local inserts
* D_m: deletes in the message
* D_l: local deletes

Also, when mentioning 'local operations', the intention is to refer to
the unversioned local operations (i.e. the ones not yet pushed to the
server).

Finally, the following notation for set operators and quantifiers is
used:

* union: set union
* in: element in set (e.g. x in S)
* forall: 'for all' quantifier

Detection of conflicts over object identity
-------------------------------------------

An early step of the merge operation is to detect the conflicts that
could happen if the server operations were performed directly, without
checks. For example, attempting a server-sent update operation over an
object already deleted locally would result in an error. On the other
hand, if the 'rebase' notion were to be respected, the local delete
operation would be performed last and the update sent from the server
would be meaningless (although dbsync handles deletions
differently). Regardless of the actual conflict resolution, the case
needs to be detected properly.

These conflicts are 'over object identity' because they involve object
existence. The other kind of conflicts resolved by dbsync are those
caused by unique constraints on table columns. The detection and
resolution of those is explained further ahead.

The conflicts over object identity are pairs of one one server-sent
operation and local operation, both of them known to be colliding in
some way, and are split into four types based on the nature of the
collision:

- **direct**: two operations collide and both reference the same
  object. Both operations are non-insert operations, since inserted
  objects can't possibly be referenced by other applications before
  synchronization (e.g. node A can't update a record that was inserted
  in node B).

     direct := { (remote, local) forall remote in (U_m union D_m),
                                 forall local in (U_l union D_l) /
                 object(remote) = object(local) }

- **dependency**: two operations collide and one references an object
  that is the "child" of the other referenced object, where being a
  child means having a foreign key relationship with another object
  and having the foreign key column in the table (the "parent" doesn't
  have the foreign key). In these, the local conflicting operation
  references the "child".

  As notation, an object _x_ being the "child" of an object _y_ will
  be written:

  _x_ FK _y_

  With this, the **dependency** conflicts can be defined as::

      dependency := { (remote, local) forall remote in D_m,
                                      forall local in (I_l union U_l) /
                      object(local) FK object(remote) }

- **reversed dependency**: just like the **dependency** conflicts, but
  having the "child" object referenced by the server-sent
  operation. The distinction is made because the resolution methods
  for these conflicts are different.

      reversed dependency := { (remote, local) forall remote in (I_m union U_m),
                                               forall local in D_l /
                               object(remote) FK object(local) }

- **insert**: insert conflicts are the product of automatic primary
  key assignment. When two insert operations reference the same
  object, the object identity assigned by the local application is
  simply accidentally the same as the one sent from the server. The
  objects themselves are known to be different, and thus the conflict
  will be resolved by keeping both (more on this later).

       insert := { (remote, local) forall remote in I_m,
                                   forall local in I_l /
                   object(remote) = object(local) }

  Here, "objects being equal" (object(remote) = object(local)) just
  means the reference is equal. The reference is just the value of the
  primary key and information of the type of object (like the table
  name and SQLAlchemy class name).

Operation compression
---------------------

There's an earlier stage in the merge operation that's required for
the conflict detection to be optimal. Without previous consideration,
the operation journals are filled with every single operation
performed by the application, often redundantly (note: operations
don't store the actual SQL sentence, they're just a reference to the
current state of the object). This often means sequences like the
following:

1. Insert object X
2. Update object X
3. Update object X
4. Update object X
5. Delete object X

In this example, object X is inserted, modified and finally deleted
before ever being synchronized. Without care for these cases, the
merge operation would detect conflicts between operations that could
have been "compressed out" completely. This operation compression is
the earliest phase in the merge operation.

TODO define local operation compression.

TODO define remote operation compression.

TODO define conflict resolution (maybe split in parts). Include
general strategy and possible future "parametric strategies".

TODO define unique constraint conflicts and their resolution.
