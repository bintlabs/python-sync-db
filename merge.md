Merge
=====

The merge subroutine occurs during the pull synchronization procedure,
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

Finally, the following notation for sets, operators and quantifiers is
used:

* DB: set of all objects in the local database
* MSG: set of all objects in the server-sent message
* union: set union
* inter: set intersection
* empty: the empty set
* map: map a function to a set (e.g. map( x -> x*2, { 1, 2, 3 } ) = { 2, 4, 6 })
* in: element in set (e.g. 2 in { 5, 6, 2 })
* forall: 'for all' quantifier

Detection of conflicts over object identity
-------------------------------------------

An early step of the merge subroutine is to detect the conflicts that
could happen if the server operations were performed directly, without
checks. For example, attempting a server-sent update operation over an
object already deleted locally would result in an error. On the other
hand, if the 'rebase' notion were to be respected, the local delete
operation would be performed last and the update sent from the server
would be meaningless (although dbsync doesn't handle deletions that
way). Regardless of the actual conflict resolution, the case needs to
be detected properly.

These conflicts are 'over object identity' because they involve object
existence. The other kind of conflicts resolved by dbsync are those
caused by unique constraints on table columns. The detection and
resolution of those is explained further ahead.

The conflicts over object identity are pairs of one server-sent
operation and one local operation, both of them known to be colliding
in some way, and are split into four types based on the nature of the
collision:

- **direct**: two operations collide and both reference the same
  object. Both operations are non-insert operations, since inserted
  objects can't possibly be referenced by other applications before
  synchronization (e.g. node A can't update a record that was inserted
  in node B).

      direct := { (remote, local) forall remote in (U_m union D_m),
                                  forall local in (U_l union D_l) /
                  object_ref(remote) = object_ref(local) }

  The *object_ref* function takes an operation and returns the
  reference to the object in database. Since the object might not be
  present, only the reference is returned. The reference is just the
  value of the primary key and information of the type of object (like
  the table name and SQLAlchemy class name).

- **dependency**: two operations collide and one references an object
  that is the "child" of the other referenced object, where being a
  child means having a foreign key relationship with another object
  and having the foreign key column in the table (the "parent" doesn't
  have the foreign key). In these, the local conflicting operation
  references the "child".

  As notation, an object _x_ being the "child" of an object referenced
  by _y_ (_y_ is only a reference) will be written:

  _x_ FK _y_

  With this, the **dependency** conflicts can be defined as::

      dependency := { (remote, local) forall remote in D_m,
                                      forall local in (I_l union U_l) /
                      fetch_object(local, DB)) FK object_ref(remote) }

  Since the FK relationship doesn't match references with references
  (the "child" must be a real database object), an aditional 'fetch'
  phase is required. The function *fetch_object* could be defined as a
  container query (container meaning object store, e.g. DB or MSG)
  given the reference, or:

      fetch: References, Containers -> Objects
      fetch(r, c) = o, where reference(o) = r

      fetch_object := op, cont -> fetch(object_ref(op), cont)

  This works because the object being fetched exists (the operation is
  an insert or an update). If it doesn't, the whole merge subroutine
  fails as early as conflict detection.

- **reversed dependency**: just like the **dependency** conflicts, but
  having the "child" object referenced by the server-sent
  operation. The distinction is made because the resolution methods
  for these conflicts are different.

      reversed dependency := { (remote, local) forall remote in (I_m union U_m),
                                               forall local in D_l /
                               fetch_object(remote, MSG) FK object_ref(local) }

  The fetch phase here is different. The remote operation references a
  remote object, one that exists in the server. Thus, the query is
  performed over the server-sent message, which contains all possible
  required objects. The message is constructed in such a way that only
  the objects that could be needed are included. (Currently, the
  server has to pre-detect conflicts while building the message, as an
  optimization done to reduce the size of the message. Previously, the
  server included all "parent" objects for each object added, and the
  message was bloated excessively when adding objects with many
  foreign keys.)

- **insert**: insert conflicts are the product of automatic primary
  key assignment. When two insert operations reference the same
  object, the object identity assigned by the local application is
  simply accidentally the same as the one sent from the server. The
  objects themselves are known to be different, and thus the conflict
  will be resolved by keeping both (more on this later).

       insert := { (remote, local) forall remote in I_m,
                                   forall local in I_l /
                   object_ref(remote) = object_ref(local) }

Operation compression
---------------------

There's an earlier stage in the merge subroutine that's required for
the conflict detection to be optimal and stable. Without previous
consideration, the operation journals are filled with every single
operation performed by the application, often redundantly (note:
operations don't store the actual SQL sentence). This often means
sequences like the following are stored:

1. Insert object _x_
2. Update object _x_
3. Update object _x_
4. Update object _x_
5. Delete object _x_

In this example, object _x_ is inserted, modified and finally deleted
before ever being synchronized. Without care for these cases, the
merge subroutine would detect conflicts between operations that could
have been "compressed out" completely. Also, and as shown in this
example, a 'fetch' call on object _x_ would have failed since it
wouldn't exist any longer. This operation compression is the earliest
phase in the merge subroutine.

(Worth noting from this example is that operations can be sorted in
some way. The sorting criteria is the order in which they were logged
in the journal, the order in which they were executed by the local
application.)

The main requirement for the operation sets before conflict detection
is that at most one operation exists for each database object involved
in the synchronization. This means::

    map(object_ref, U_l) inter map(object_ref, I_l) = empty
    map(object_ref, I_l) inter map(object_ref, D_l) = empty
    map(object_ref, U_l) inter map(object_ref, D_l) = empty

And for the server-sent operations, too::

    map(object_ref, U_m) inter map(object_ref, I_m) = empty
    map(object_ref, I_m) inter map(object_ref, D_m) = empty
    map(object_ref, U_m) inter map(object_ref, D_m) = empty

Operation compression must reach these states for the merge to have a
good chance of success.

The rules applied for compressing sequences of operations are simple,
yet different when compressing the local database or the server-sent
operations. The library assumes that the local database is complying
with the restriction imposed on its use: primary keys cannot be
recycled. When this is true, a delete operation marks the end of an
object reference's lifetime. Thus, a delete operation must be the
final operation of a local operation sequence, each time a delete
exists at all in said sequence.

On the server-sent message, however, the operation set could be
originated from various nodes. Since conflict resolution could (and
currently sometimes does) re-insert objects that were deleted, the
previous rule doesn't apply over server-sent sequences.

The previous example illustrates a compression rule for the local
database: sequences that start with an insert and end in a delete must
be wholly removed from the journal. A less intuitive rule applied to
the server-sent message is: sequences that start with a delete and end
with a non-delete must be reduced to a single update::

    d, i, u, u => u
    d, i => u

More rules are needed to reach the requirements, but they won't be
detailed here.

Conflict resolution
-------------------

TODO define conflict resolution (maybe split in parts). Include
general strategy and possible future "parametric strategies".

TODO define unique constraint conflicts and their resolution.
