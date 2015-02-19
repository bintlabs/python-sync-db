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
changed. The expected behaviour is somewhat similar to the
consolidation performed by the various version control systems when
multiple changes are applied by different authors to a single file.

Operations contained either in the server-sent message or the local
journal are simply references to the object operated upon, and are
split in three groups:

* `U`: updates
* `I`: inserts
* `D`: deletes

To differentiate sent operations from local ones, the following
notation is used:

* `U_m`: updates in the message (sent from the server)
* `U_l`: local updates
* `I_m`: inserts in the message
* `I_l`: local inserts
* `D_m`: deletes in the message
* `D_l`: local deletes

Also, when mentioning 'local operations', the intention is to refer to
the unversioned local operations (i.e. the ones not yet pushed to the
server).

Finally, the following notation for sets, operators and quantifiers is
used:

* `DB`: set of all objects in the local database
* `MSG`: set of all objects in the server-sent message
* `union`: set union
* `inter`: set intersection
* `empty`: the empty set
* `map`: map a function to a set (e.g. `map( x -> x*2, { 1, 2, 3 } ) = { 2, 4, 6 }`)
* `in`: element in set (e.g. `2 in { 5, 6, 2 }`)
* `forall`: 'for all' quantifier

Detection of conflicts over object identity
-------------------------------------------

An early step of the merge subroutine is to detect the conflicts that
could happen if the server operations were performed directly, without
checks. For example, attempting a server-sent update operation over an
object already deleted locally would result in an error. On the other
hand, if the local operation were to be given priority, the (local)
delete would be performed last and the update sent from the server
would be ignored. Regardless of the actual conflict resolution, the
case needs to be detected properly.

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

      x FK y

  With this, the **dependency** conflicts can be defined as:

      dependency := { (remote, local) forall remote in D_m,
                                      forall local in (I_l union U_l) /
                      fetch_object(local, DB)) FK object_ref(remote) }

  Since the `FK` relationship doesn't match references with references
  (the "child" must be a real database object), an aditional 'fetch'
  phase is required. The function *fetch_object* could be defined as a
  container query (container meaning object store, e.g. `DB` or `MSG`)
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
wouldn't exist in the database any longer. This operation compression
is the earliest phase in the merge subroutine.

(Worth noting from this example is that operations can be sorted in
some way. The sorting criteria is the order in which they were logged
in the journal, the order in which they were executed by the local
application.)

The main requirement for the operation sets before conflict detection
is that at most one operation exists for each database object involved
in the synchronization. This means:

    map(object_ref, U_l) inter map(object_ref, I_l) = empty
    map(object_ref, I_l) inter map(object_ref, D_l) = empty
    map(object_ref, U_l) inter map(object_ref, D_l) = empty

And for the server-sent operations, too:

    map(object_ref, U_m) inter map(object_ref, I_m) = empty
    map(object_ref, I_m) inter map(object_ref, D_m) = empty
    map(object_ref, U_m) inter map(object_ref, D_m) = empty

Operation compression must reach these states for the merge to have a
good chance of success. Of course, not any modification of the journal
is a valid one. It's purpose must be preserved: to record a way to
reach the current database state from the previous one.

The rules applied for compressing sequences of operations over a
single object are simple, yet different when compressing the local
database or the server-sent operations. The library assumes that the
local database is complying with the restriction imposed on its use:
primary keys cannot be recycled. When this is true, a delete operation
marks the end of an object reference's lifetime. Thus, a delete
operation must be the final operation of a local operation sequence,
each time a delete exists at all in said sequence.

On the server-sent message, however, the operation set could be
originated from various nodes. Since conflict resolution could (and
currently sometimes does) re-insert objects that were deleted, the
previous rule doesn't apply over server-sent sequences.

The previous example illustrates a compression rule for the local
database: sequences that start with an insert and end in a delete must
be wholly removed from the journal. A less intuitive rule applied to
the server-sent message is: sequences that start with a delete and end
with a non-delete must be reduced to a single update:

    d, i, u, u => u
    d, i => u

With this pattern-based notation, where to the left of the `=>` is a
comma-separated sequence of expressions that match operations, and to
the right is a singleton set or the empty set, the whole set of rules
can be written succinctly:

Let `*` be the [Kleene star][kleene-star], `.` be a matching
expression for any operation, `~x` be a matching expression for
anything but `x` (i.e. not `x`):

[kleene-star]: https://en.wikipedia.org/wiki/Kleene_star

Local compression:

    i, u*    => i
    i, u*, d => empty
    u, u*    => u
    u*, d    => d

Compression on the server-sent message:

    i         => i
    u         => u
    d         => d
    i, .*, d  => empty
    i, .*, ~d => i
    u, .*, d  => d
    u, .*, ~d => u
    d, .*, d  => d
    d, .*, ~d => u

While the rules for compressing the server-sent operations cover all
possible sequences, the rules for the local operations don't. If a
local operation sequence is found not to match any of those rules, a
warning is emitted that notifies the user of probable database
intervention, or failure to comply with the library's restriction on
primary keys.

Conflict resolution
-------------------

A 'merge' is a common operation in version control systems
(VCSs). Usually, a VCS will detect conflicting changes and notify the
users, leaving them with the choices. They don't perform automatic
conflict resolution by default, since in general the criteria applied
in resolving each conflict is specific and even unclear. Dbsync
however is currently implemented with a fixed conflict resolution
strategy. As such, incorrect choices are sometimes made, but the
strategy is forgiving enough that data loss is mostly avoided. A way
to abstract and let the users build their own strategies is an
improvement that will be left for the next major version.

Another point worth comparing with VCSs is history tracking. In
dbsync, all history is linear and irreversible. There are no branches
and there's no 'revert' procedure. These features are consequence of
the core design and can't be changed easily. As such, it's better to
rely on backups on the server if a way to revert changes is
required.

Given this weakness in the library, the strategy chosen is meant to
preserve the operations performed in the node over those in the
server. A merge occurs in the node, and the conflict resolution won't
be reflected back to the server untill the next 'push'. With these
grounds, the strategy can be written plainly as:

1. When delete operations are in any conflict with non-delete
   operations, revert them. Reverting them means to reinsert the
   deleted object (fetching from the complementary container), and
   also to either delete the operation from the journal (a local
   delete operation) or to nullify the operation with a new insert
   operation (a server-sent delete operation). The difference in the
   way to revert the delete operation is what mandates the separation
   of **dependency conflicts** in two categories.

2. When update operations collide, keep the local changes over the
   server-sent ones. This is strictly a case of data loss, but it can
   be handled, though cumbersomely, through centralized back-ups.

3. As mentioned earlier, insert operations colliding will result in
   the allocation of a new primary key for the server-sent
   object. Since primary keys are presumed to be integers, the
   incoming object is currently given a primary key of value equals to
   the successor of the maximum primary key found on the corresponding
   table. Even in the case the library would allow primary keys of
   different types in the future, this part of the current strategy
   would force the application to not give any meaning to them.

4. Delete-delete conflicts translate to a no-operation, and the local
   delete entry is removed from the journal.

The conflict resolution is done as required while attempting to
perform each of the server-sent operations. Each remote operation is
checked for conflicts and is finally either allowed, blocked, or added
extra side-efects as consequence of the strategy defined above. Once
the final remote operation is handled, the local version identifier is
updated and the merge concludes successfully. Any error is reported
before completion in the form of a thrown exception.

Unique constraints
------------------

As consequence of not storing the state transitions of objects, dbsync
currently generates conflicts of another kind: the swapping of values
tied to unique constraints at the database level. Consider the
following example of a state transition sequence of two objects, _x_
and _y_, each of the same type with a unique constraint enforced on
the _col_ attribute:

    1. x[col=1]; y[col=2]
    2. x[col=3]; y[col=2] (update x with temporal value)
    3. x[col=3]; y[col=1] (update y with x's old value)
    4. x[col=2]; y[col=1] (update x with y's old value)

This update sequence is one that could be generated by an application,
and which complies with a unique constraint on the _col_ attribute
(_col_ is also a column of the mapped table). Since the state
transition is lost in the operations journal, simply applying the
compressed operation (a single update for each object) during a merge
subroutine would result in an error not previously accounted for. Also
worth noting is that the given example shows only the case of a
one-step swap, on a single constrained attribute. Multiple-step swaps
(involving more than two objects) and multiple-column constraints are
also variations to consider.

Not every case of unique constraint violation is caused by dbsync,
however. It’s also possible that two insert or update operations,
registered by different nodes, collide on such a constraint without it
being a consequence of poor logging. These cases happen because the
constraint is checked locally by the database management system or
engine, and not against the synchronization server. Dbsync detects
these as well, and interrupts the merge subroutine with a specialized
exception that contains required details for the end user to resolve
it.

To detect unique constraint conflicts, dbsync uses SQLAlchemy’s schema
inspection API to iterate over the unique constraints defined in
tables referenced by operations in the message. For each constraint
found for each operation, a local object that matches the constraining
values in the server-sent object is looked up. If found, the local
object is further inspected to detect whether the case is a solvable
conflict or a user error.

A solvable conflict is determined by the presence of another version
of the conflicting local object in the message (a third object, in the
message, that has the same primary key value as the local conflicting
object). This indicates that an update swap exists in the server-sent
state difference, and the three objects form a single step of it. The
conflicting objects are all collected and this way, multiple-step
swaps are detected in increments. The collection of conflicting
objects is finally deleted and immediately inserted with their final
state, thus resolving the conflict. This method of resolution was
found to be applicable in many different DBMSs, although it required
the temporal desactivation of foreign key cascades, as they could
trigger unexpected changes to the database.

An unsolvable conflict is determined by the absence of the local
conflicting object in the server-sent message. This means that
operations from both devices collide, but do so exclusively in a
pair. Detecting conflicts of this kind causes the interruption of the
merge subroutine.

Findings
--------

<table>
  <thead>
    <tr>
      <th>#</th>
      <th>Source</th>
      <th>Summary</th>
      <th>Comments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td valign="top">1</td>
      <td valign="top">
        <a href="http://grids.ucs.indiana.edu/ptliupages/hhms/pdf/disconnected.pdf">Disconnected Operation in the Coda File System</a>
      </td>
      <td valign="top">
        The Coda File System enables clients to work with shared files
        while disconnected through <i>caching</i>. It employs
        an <i>optimistic strategy</i>, not blocking access to files
        but detecting and resolving conflicts after reconnection. It
        was designed to <i>improve availability</i>.
      </td>
      <td valign="top">
        <ul>
          <li>
            Coda operates on different modes when connected or
            disconnected. An interface exists to make the state change
            transparent to applications [3 Design Rationale].
          </li>
          <li>
            The client holds the majority of Disconnected Operation’s
            complexity [4 Detailed Design And Implementation].
          </li>
          <li>
            Batches of file identifiers are supplied by the server
            while connected. When disconnected, temporary identifiers
            are used once the batch is exhausted [4.5.1 Replay
            Algorithm].
          </li>
          <li>
            It records operations on a <i>replay log</i>, used later
            to reintegrate changes to the server. Also, records of
            previous changes on a single file are discarded, as an
            optimization [4.4.1 Logging].
          </li>
          <li>
            Unsolvable conflicts are forwarded to the user by marking
            the file replicas inconsistent [4.5.2 Conflict
            Handling].
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">2</td>
      <td valign="top">
        <a href="http://people.bu.edu/staro/efficient_pda.pdf">Efficient PDA Synchronization</a>
      </td>
      <td valign="top">
        An algorithm for synchronizing <i>unordered data sets</i>
        which depends on differences between two data sets, and not on
        the number of records. It improves on previous mobile device
        synchronization algorithms in terms of bandwidth usage and
        latency. Tested on PDAs.
      </td>
      <td valign="top">
        <ul>
          <li>
            It’s a peer-to-peer communication scheme, where no
            hierarchy is required between devices, and no operation
            logs need to be maintained.
          </li>
          <li>
            The algorithm finds the symmetric difference of sets of
            <i>integers in a finite field</i>. Thus, the hashing of
            data is required.
          </li>
          <li>
            The algorithm depends on foreknowledge of the <i>number of
            differences</i> between two sets. It proposes, however, a
            probabilistic practical method to find a good estimate of
            a tight upper bound, based on random sampling.
          </li>
          <li>
            It consists of the construction of a characteristic
            polynomial that describes a data set, and the subsequent
            application of said polynomial on fixed evaluation
            points. The computed values are sent from one host to the
            other, which uses them to find the set differences through
            interpolation.
          </li>
          <li>
            An implementation can be made efficient in terms of
            latency (time spent), or communication (data sent).
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">3</td>
      <td valign="top">
        <a href="http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=1226606">Set Reconciliation with Nearly Optimal Communication Complexity</a>
      </td>
      <td valign="top">
        A family of algorithms for set reconciliation is presented, as
        in [2], which depend exclusively on the number of differences
        between sets.
      </td>
      <td valign="top">
        <ul>
          <li>
            It is shown that the algorithm is equivalent to the
            transmission of the redundancy of a Reed-Solomon encoding
            of the coefficients of the characteristic polynomial. It
            is implied that the problem is equivalent when stated as
            an error-correcting problem (differences between sets are
            seen as errors).
          </li>
          <li>
            An information-theoretical explanation is given as to why
            a probabilistic approach to finding the upper bound is
            required, in the interest of keeping the communication
            efficient.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">4</td>
      <td valign="top">
        <a href="http://www.samba.org/~tridge/phd_thesis.pdf">Efficient Algorithms for Sorting and Synchronization</a>
      </td>
      <td valign="top">
        <i>rsync</i> is an algorithm for updating byte strings (files)
        remotely, over a low-bandwidth, high-latency channel. The
        design and special considerations in implementation are given.
      </td>
      <td valign="top">
        <ul>
          <li>
            Only sections 3, 4, 5 and 6 were considered.
          </li>
          <li>
            Grossly, rsync consist of the transmission of block
            signatures, for a fixed number of blocks in the file, and
            the matching of said signatures against the remote file's
            signatures. Only the required blocks are then sent and
            updated.
          </li>
          <li>
            rsync uses two signature functions, a fast signature used
            to filter matching blocks, and a slow, reliable signature
            used to discard false positives [3.2.3 Two signatures].
          </li>
          <li>
            Special consideration is given for file formats that alter
            their structure broadly for generally small editions, such
            as compressed files.
          </li>
          <li>
            rsync can be used as the remote-update tool for
            distributed filesystems on high-latency networks that work
            with <i>file leases</i>, contracts that give the client
            write permissions over a file for a period of time. A
            remote update is then performed to send the local changes
            to the server, which is more efficient than sending the
            whole file [5.5 rsync in a network filesystem].
          </li>
          <li>
            rsync is currently part of the standard unix toolset.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">5</td>
      <td valign="top">
        <a href="http://zoo.cs.yale.edu/classes/cs422/2013/bib/terry95managing.pdf">Managing update conflicts in bayou, a weakly connected replicated storage system</a>
      </td>
      <td valign="top">
        <i>Bayou</i> is a distributed storage system and platform for
        applications, that provides primitives for data
        synchronization and guarantees the <i>eventual consistency</i>
        of the datasets across devices, thanks to its update
        propagation protocol.
      </td>
      <td valign="top">
        <ul>
          <li>
            The system is hierarchical in that a client-server
            distinction is made, but it can support several servers,
            which communicate to homologize their changes.
          </li>
          <li>
            All communication is pairwise: client-server or
            server-server.
          </li>
          <li>
            Applications are required to provide procedures that
            detect write-write and read-write conflicts, which are run
            at the server during synchronization. These procedures
            must meet a specific contract and run with limited
            resources [4.2 Dependency checks].
          </li>
          <li>
            Applications must also provide conflict resolvers, which
            are then used to resolve the detected conflicts
            automatically. These procedures may also fail to correct
            conflicts, and notify users through specific logs [4.3
            Merge procedures].
          </li>
          <li>
            Applications are encouraged to work with the notion of
            <i>pending</i> and <i>committed</i> transactions [6. Write
            Stability and Commitment].
          </li>
          <li>
            Servers communicate with each other in
            an <i>anti-entropy</i> process that reverts, reorders and
            replays operations based on timestamps and a logical clock
            [5. Replica Consistency]. A single server is said to be
            the <i>primary</i> for a specific data set, and is the one
            able to finalize an update in the form of a <i>commit</i>
            [6. Write Stability and Commitment].
          </li>
          <li>
            The custom storage system used is a relational database
            with an extra 2-bit column that indicates the state of
            each tuple [7. Storage System Implementation Issues].
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">6</td>
      <td valign="top">
        <a href="http://static.googleusercontent.com/media/research.google.com/es//pubs/archive/35605.pdf">Differential Synchronization</a>
      </td>
      <td valign="top">
        Differential Synchronization refers to a method for
        homologizing text documents over a network, that is based on
        continuously computing the difference between texts and
        patching the remote copy.
      </td>
      <td valign="top">
        <ul>
          <li>
            The protocol works with backup copies of the document,
            called <i>shadows</i>, that represent the previous state
            of the document. Shadows are used for computing document
            differences locally (either in the client or the server),
            which are then sent to the other party for patching, using
            a best-effort strategy [3. Differential Synchronization
            Overview].
          </li>
          <li>
            Communication interruptions are accounted for through
            backup shadows in the server [5.1 Asymmetry].
          </li>
          <li>
            Different multi-client, multi-server configurations can
            yield different performance measurements, since
            the <i>diff</i> and <i>patch</i> load can be distributed
            along the network [6. Topology].
          </li>
          <li>
            It is entirely based on state, and thus it’s performance
            depends heavily on the <i>diff</i> and <i>patch</i>
            algorithms [7. Diff and Patch].
          </li>
          <li>
            The messages sent are the differences computed, not the
            whole document.
          </li>
          <li>
            This method presents scalability enhancements in
            comparison to other common protocols used in collaborative
            editing and version control, such as the three-way-merge,
            or log-based approaches.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">7</td>
      <td valign="top">
        <a href="http://www.oracle.com/technetwork/topics/olsync-131762.pdf">Oracle Lite Synchronization</a>
      </td>
      <td valign="top">
        Oracle Lite is a database management system (DBMS) that, in
        conjunction with a specialized server and a central database,
        gives applications the ability to synchronize a relational
        database with a centralized database.
      </td>
      <td valign="top">
        <ul>
          <li>
            The client database is a modified version of an Oracle
            DBMS that keeps track of operations internally.
          </li>
          <li>
            The synchronization architecture uses queues and
            asynchronous calls to ensure availability and consistency.
          </li>
          <li>
            Conflict resolution is determined by a customizable,
            single ‘winner’: either the client or the server.
          </li>
          <li>
            Operations in the log are merged together, so that only
            the final state of a record is sent.
          </li>
          <li>
            A specific and customizable execution order is given to
            the operations performed. This order takes into account
            the possible operation interdependencies and
            the <i>weights</i> given to the tables by the application
            owner.
          </li>
          <li>
            Automatic, event-based and manual synchronization can be
            configured.
          </li>
          <li>
            The uniqueness of sequence-generated numbers (e.g. primary
            keys) is guaranteed by a partition set at the server,
            whereby a certain range of numbers is reserved to each
            client.
          </li>
          <li>
            Oracle Lite provides mechanisms to separate the central
            database into subsets that act as a restricted dataset for
            each client.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">8</td>
      <td valign="top">
        <a href="http://essay.utwente.nl/61767/1/Master_thesis_Jan-Henk_Gerritsen.pdf">Detecting synchronization conflicts for horizontally decentralized relational databases</a>
      </td>
      <td valign="top">
        Several theorems that determine presence or absence of
        synchronization conflicts are given. The theorems reference
        <i>atomic</i> and <i>composite</i> operations performed given
        a common initial state.
      </td>
      <td valign="top">
        <ul>
          <li>
            Operations are viewed as functions that, given a database
            state, produce a different database state.
          </li>
          <li>
            Atomic operations are defined to be single-tuple inserts,
            updates or deletes [4.2 Definition of atomic operations].
          </li>
          <li>
            Composite operations are defined as a series of atomic
            operations that should be treated as a single unit [5.1.1
            Definition of a composite operation].
          </li>
          <li>
            Three categories of conflicts are defined, with different
            decision trees for each one: conflicts based on different
            states, intra-relational conflicts, and inter-relational
            conflicts [4.6 Decision graphs for conflicts].
          </li>
          <li>
            The first category groups conflicts that are defined as a
            pair of operations that, when applied sequentially in a
            specific order, produce a different database state that
            when applied in a different order [4.4.1 Unequal database
            states after synchronization].
          </li>
          <li>
            The second category groups conflicts stemmed from
            constraints or keys defined for a table [4.5.1 Violations
            of intra-relational integrity constraints].
          </li>
          <li>
            The third category is composed of conflicts that violate
            foreign keys and inclusion dependencies [4.5.2 Violations
            of inter-relational integrity constraints].
          </li>
          <li>
            Detection of conflicts for composite operations is done by
            inspecting each composite operation in terms of their
            atomic components. Also, related operations in a sequence
            are analysed for dependency, in that one is dependent on
            the state change produced by the other [5.2.1 Related
            atomic operations].
          </li>
          <li>
            The conflict analysis is not based on database state, but
            on the operations themselves. It follows that
            implementations could use operation logs to detect
            conflicts, rather than compute state differences.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">9</td>
      <td valign="top">
        <p><a href="http://msdn.microsoft.com/en-us/library/bb902818%28v=sql.110%29.aspx">Microsoft Sync Framework</a>, last consulted november 22th, 2014.</p>
        <p>Sections:</p>
        <ol>
          <li><a href="http://msdn.microsoft.com/en-us/library/dd918682(v=sql.110).aspx">Infrastructure</a></li>
          <li><a href="http://msdn.microsoft.com/en-us/library/bb725997(v=sql.110).aspx">Conflicts</a></li>
        </ol>
      </td>
      <td valign="top">
        The framework enables databases to synchronize in
        <i>collaborative editing</i> scenarios and <i>occasionally
        connected</i> scenarios.
      </td>
      <td valign="top">
        <ul>
          <li>
            Several database management systems are supported, thanks
            to <i>synchronization providers</i>, application libraries
            that mask the Sync Framework implementation
            details. ADO.NET compatible databases are generally
            supported.
          </li>
          <li>
            Client-server and client-client topologies are fully
            supported.
          </li>
          <li>
            The installation of the framework implies the construction
            of the Sync Framework infrastructure, as dictated by the
            synchronization provider. The infrastructure consists of
            tables, triggers and stored procedures.
          </li>
          <li>
            Conflict detection and automatic conflict resolution are
            performed by the provider library.
          </li>
          <li>
            Complex operation logs (<i>tracking tables</i>) are kept
            up-to-date with each database, and are populated with
            <i>triggers</i> [9.1].
          </li>
          <li>
            Conflicts occur at the row level [9.2].
          </li>
          <li>
            Conflict resolution is fully customizable, through access
            to copies of the datasets involved in synchronization
            [9.2].
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">10</td>
      <td valign="top">
        <p><a href="http://www.sybase.com/files/White_Papers/wp-ias-MobiLink12Performance.pdf">Sybase MobiLink 12 Performance</a></p>
        <p>and</p>
        <p><a href="http://infocenter.sybase.com/help/topic/com.sybase.help.sqlanywhere.11.0.1/PDF/mlserver_en11.pdf">MobiLink Server Administration</a></p>
      </td>
      <td valign="top">
        Sybase MobiLink forms a client-server architecture for
        database synchronization. Many DBMSs are supported for the
        server-side data store, and two Sybase DBMSs are supported for
        the client application: UltraLite and SQL Anywhere.
      </td>
      <td valign="top">
        <ul>
          <li>
            The server architecture is designed to maximize
            performance. It is split in multiple components, each
            handled by at least one separate thread.
          </li>
          <li>
            Simultaneous synchronizations are allowed by the server.
          </li>
          <li>
            Multiple synchronization techniques are supported, such as
            timestamp synchronization (using a ‘last-updated’
            timestamp column to determine whether a row is to be
            included), and snapshot synchronization (which includes
            whole tables).
          </li>
          <li>
            All tables must have primary keys defined, and their
            values should be unique across all databases partaking in
            synchronization. The primary keys should not be modified
            by the applications. Primary key domains for
            autoincrementing keys are partitioned at the server.
          </li>
          <li>
            Primary keys are used to identify synchronization
            conflicts.
          </li>
          <li>
            Conflicts are detected and custom resolution can be
            implemented by listening to specific events. A ‘forced
            mode’ can be used that treats every incoming row as a
            conflict, which then triggers all related events.
          </li>
          <li>
            Deletes can be detected through use of <i>shadow
            tables</i> (a form of simple operations log for deletes)
            or logical deletes (not deleting rows but marking them as
            deleted with a specific column).
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">11</td>
      <td valign="top">
        <p><a href="https://www.firebase.com/blog/2013-03-25-where-does-firebase-fit.html">Where
        does Firebase fit in your app?</a>, last consulted november
        23th, 2014.</p>
        <p><a href="https://www.firebase.com/docs/web/guide/offline-capabilities.html">Offline
        Capabilities - Firebase</a>, last consulted november 23th,
        2014.</p>
      </td>
      <td valign="top">
        Firebase is a data store service that provides event-driven
        libraries with synchronization capabilities. It’s specialized
        in real-time updates, mobile and collaborative
        applications. Three patterns for firebase usage are reviewed,
        each with a different level of involvement in architecture.
      </td>
      <td valign="top">
        <ul>
          <li>
            Firebase can act as a centralized data store for
            applications that don’t require custom server-side
            execution, allowing for fully client-side, multi-user,
            distributed applications.
          </li>
          <li>
            It can act as middleware between servers and client
            devices, either as a message queue or persistence service.
          </li>
          <li>
            It can also be adhered to an existing architecture by
            implementing a subset of real-time features dependent on
            it, leaving existing client-server infrastructure intact.
          </li>
          <li>
            Firebase uses a local database as cache, and transitions
            to a local exclusive mode of operation when a
            disconnection occurs.
          </li>
          <li>
            Synchronization is performed in a ‘best-effort’
            manner. It’s implied that this means an always-automatic
            never-failing conflict resolution strategy.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">12</td>
      <td valign="top">
        <a href="http://tools.ietf.org/html/rfc3501">Internet Message Access Protocol</a>
      </td>
      <td valign="top">
        IMAP is a mail protocol and part of the suit of Internet
        protocols. It includes considerations for mailbox
        synchronization.
      </td>
      <td valign="top">
        <ul>
          <li>
            IMAP requires well-formed, unique identifiers for
            messages. Identifiers must be generated in ascending
            order.
          </li>
          <li>
            The main reference doesn’t include implementation details
            as to how a disconnected client, or the server, may
            compute deltas and issue correct synchronization
            commands. RFC 4549 includes implementation
            recommendations.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">13</td>
      <td valign="top">
        <a href="http://www.ietf.org/rfc/rfc4549.txt">Synchronization Operations for Disconnected IMAP4 Clients</a>, last consulted november 23th, 2014.
      </td>
      <td valign="top">
        A detailed revision and implementation recommendations for
        synchronization mechanisms in IMAP4 mail agents.
      </td>
      <td valign="top">
        <ul>
          <li>
            As per the recommendation, the discovery of new messages
            can be performed through a query that uses the property of
            ascending order of message IDs.
          </li>
          <li>
            Changes to old messages are detected by performing a state
            query (FETCH FLAGS) of all message IDs known and detecting
            differences locally.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">14</td>
      <td valign="top">
        <a href="http://docs.datomic.com/architecture.html">Architecture Overview | Datomic</a>, last consulted november 23th, 2014.
      </td>
      <td valign="top">
        Datomic is a commercial distributed relational database that
        stores data as <i>immutable facts</i>, or
        transactions. Transactions are stored, and not performed until
        the resulting values are requested, giving all stored tuples a
        readable state history.
      </td>
      <td valign="top">
        <ul>
          <li>
            The database state is dependent on time, as a parameter,
            since all transactions up to a given time are
            considered. All historic database states are thus stored.
          </li>
          <li>
            The immutability of data allows for extensive caching in
            client libraries, enhancing their performance
            significantly since reads don’t require network traffic
            after the application’s working set is cached.
          </li>
          <li>
            Immutability of facts allows the database to store values
            in shared data structures.
          </li>
          <li>
            Transactions either succeed or fail atomically. Full ACID
            compliance is ensured.
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td valign="top">15</td>
      <td valign="top">
        <a href="https://www.dropbox.com/developers/datastore/docs/python">Python Datastore API documentation - Dropbox</a>, last consulted november 23th, 2014.
      </td>
      <td valign="top">
        Dropbox Datastore is a library available for mobile, desktop
        and server platforms that uses Dropbox’s servers to
        synchronize simple relational databases.
      </td>
      <td valign="top">
        <ul>
          <li>
            Some libraries provide automatic conflict resolution,
            while other versions promote a mode of operation where
            attempts of transactions are made until the transaction
            succeeds or a maximum number of tries is reached.
          </li>
          <li>
            When conflicts are detected, an encouraged way of
            resolution is to roll back changes, apply deltas, and
            finally re-apply changes manually.
          </li>
          <li>
            The server reports conflicts and computes deltas. A client
            can request deltas at any time.
          </li>
          <li>
            A user and permission hierarchy is used, based on the
            Dropbox service user accounts, and document ownership.
          </li>
          <li>
            Record IDs are assigned automatically. All records have an
            ID. IDs are alphanumeric values.
          </li>
          <li>
            Dropbox Datastore is intended to be used for user-owned
            data sets.
          </li>
          <li>
            It’s not a fully SQL-compliant relational database.
          </li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>
