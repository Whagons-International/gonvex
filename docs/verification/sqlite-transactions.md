# Shared SQLite handle ownership

Replica and outbox instances using the same `ExpoSQLiteDatabase` object share
one admission queue. Each SDK operation owns that handle until its promise
settles, including schema initialization, reads, commit and rollback. Different
handle objects have independent queues.

## Reproduced defect

Expo's documented `withTransactionAsync` callback is
[not exclusive against other async queries](https://docs.expo.dev/versions/latest/sdk/sqlite/).
Its JavaScript implementation runs `BEGIN` inside a try/catch that issues
`ROLLBACK` on failure. A second `BEGIN` on an already active connection can thus
roll back another operation's transaction. Giving each outbox instance its own
queue does not protect a shared replica handle or another outbox instance.

The regressions use Node's real SQLite engine with that Expo transaction
wrapper behavior. On staging `08e45898`, three original scenarios fail and the
independent-handle control passes:

- A replica write pauses after its entity statement succeeds. An overlapping
  outbox append attempts `BEGIN`, fails, and rolls back the replica rows. The
  replica resumes and persists cursor revision 8 outside a transaction, while
  the original row remains and the new row is missing. Both calls reject, but
  a restart would still read that incorrect durable checkpoint.
- Two outbox instances sharing a handle both reject instead of reserving
  distinct committed identities.
- A second replica instance reads changed, uncommitted data alongside the old
  revision 7 checkpoint before the writer commits.

These are controlled defect reproductions, not observed deployed incidents.

## Implementation and argument

`database-operations.ts` keeps a promise tail in a `WeakMap` keyed by the supplied
handle object. An operation captures the previous tail synchronously, chains its
callback after it, and installs a fulfilled-on-settlement tail for its successor.
It returns the original operation promise so its caller still receives errors.

Under JavaScript Promise ordering, an operation cannot enter until its
predecessor settles. Induction over admissions establishes at most one active
SDK operation on a handle, provided callbacks await all their database work.
Failed operations finish rollback before rejection reaches the coordinator.
Rejection releases the successor without changing the caller's error. This is
a code-level argument, not a machine-checked Promise semantics proof.

The public replica facade admits each operation once. Its private core can call
other core methods without reacquiring admission. All outbox instances use the
same coordinator, including their reads and migrations. Guarding only transaction
starts would still allow readers to publish another operation's uncommitted
data, so reads are part of the owned operation too.

`shared-handle.test.ts` exercises the actual SDK entry points against real
SQLite. Gates are placed after successful SQL statements; the Node event-loop
turn drains ready Promise work rather than using elapsed sleeps as evidence of
overlap. Additional cases cover rollback before the next operation, successful
work after rejection and concurrent schema initialization. Existing adapter and
SDK publication tests must continue passing.

The fixed tranche passes all six shared-handle cases, the full 84-test SQLite
adapter suite, and the 31 public SDK persistence-barrier schedules. These checks
run in regular runtime CI. Package generation and runtime import-cycle checks
also cover the new import-free coordinator module.

## Boundary

Use one SDK module instance and the same handle object for SDK operations that
share a connection. Keep that handle dedicated to the SDK: uncoordinated raw
application SQL, another wrapper object around that connection, and another SDK
copy cannot participate in this module's admission queue. Separate real SQLite
connections rely on SQLite's own locking.

A `withReadView` callback must await its reads before returning and use its
provided view. It must not reenter another SDK operation on the same handle
while holding admission. The coordinator cannot supply liveness for a callback
that never settles.

## Native execution and process recovery

The same three baseline defects and all six fixed cases were reproduced with
actual Expo SQLite 16.0.10, Expo 54.0.34, React Native 0.81.5, Hermes and SQLite
3.50.3 on an Android API 36 x86_64 emulator. The fixed adapter files matched the
merged staging source at `1ec0a880`. A successful native entity statement was
held inside a transaction while the app process was killed. Restart recovered
the original rows and checkpoint 7, retained the previously committed intent's
ID/key/arguments, recovered it from inflight to pending and allocated ID 2 next.
After retry committed revision 8, another process restart retained the changed
rows, new row and both intents.

The portable [native fixture](../../verification/native-sqlite/README.md) checks
these postconditions against current repository source and a checksum-pinned
baseline. Required CI runs the actual native engine when its execution inputs
change, records process identity changes and rejects missing or foreign events.
Its host-validator unit vectors are historical observations, not native execution.
The original local evidence archive is
`/mnt/data/work/gonvex-sqlite-guarantees/native/evidence-20261029-android-r2/`.

This establishes selected Android/Hermes schedules and app-process failure
recovery. It does not establish iOS, physical-phone filesystem durability,
OS power-loss behavior, arbitrary raw SQL interleaving or a complete SDK
protocol proof. No OPFS or SQLite WASM work is included.
