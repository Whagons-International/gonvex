# Replica persistence barrier

## Contract

For a watermark-capable tenant runtime, a reducer result whose committed revision
is still awaiting its replica watermark must not report server acceptance, or
retire its queued intent, after the corresponding received data frame fails
persistence. Stop the current stream before later queued frames can run and
reopen from the durable checkpoint. Queueing for retry is a different outcome
from reporting server acceptance.

Only the operation's current scope, connection generation and socket may be
invalidated. A write completing late on an old connection must not close its
replacement. Fence synchronously before notifying observers or calling close:
WebSocket close notifications are asynchronous in real browsers.

A failed metadata-only watermark may retry without dropping already persisted
data. A failed deferred backlog is different: remaining frames have already been
removed from its queue, so reconnect rather than attempt to certify past them.

## Implementation and reproductions

`GonvexClient.handleReplicaFrameFailure` checks the captured owner, marks the
current connection unusable before notifying listeners, and closes that socket
for data failures. `enqueueReplicaFrame` captures ownership when queued.
`drainSnapshotFrames` captures ownership before awaiting deferred storage.
Paged-snapshot restart and backlog overflow use the same synchronous fence.
Recovery freshness notifications cannot prevent transport closure if an observer
throws. General callback exception isolation is a separate contract.

With watermark negotiation, origin-tagged row data does not retire an intent
before its full result/watermark barrier. The decision captures negotiation and
connection ownership before awaiting storage, so replacement capabilities cannot
turn an old capable-runtime operation into a legacy early acknowledgement.
Legacy runtimes without watermark support retain their existing path.

The initial public regressions reproduce failed transaction persistence followed
by a watermark on staging c9eed7ce. Both direct and queued reducers report
acceptance despite missing data. The first fix exposed two further paged cases:
final-page recovery swallowed the failure before the outer catch, and a late
deferred failure closed a replacement socket. Both have public reproductions.
No deployed occurrence is claimed.

`packages/client/src/sdk-persistence-barrier.test.ts` delivers real protocol-shaped
messages through a controlled WebSocket fixture and injects failures at the
storage boundary. It reads the frame-lane promise only for joining, without
calling private handlers or editing private state. Tests check persisted cursors,
missing rows, retained intents, delayed close, late owner failures and replay of
the same command key through a successful data/write watermark barrier.

Memory, both IndexedDB adapters and the Expo SQLite adapter are exercised. The
IndexedDB schedules use `fake-indexeddb`, so real browser-engine persistence,
process restart and quota behavior remain separate verification work. The
SQLite tests use Node's actual SQLite engine, not Expo/Hermes or a phone. Injected
pre-write rejection does not establish every adapter's partial-write/crash
atomicity. Other adapter rollback tests remain separate evidence. The server is
a controlled wire fixture, not a running PostgreSQL runtime in these tests.

## Finite protocol model, not a source proof

`verification/tla/ReplicaDurability.tla` models required data-frame positions,
one in-flight storage operation, durable writes, watermark certificates,
disconnect/replacement, metadata failure and retirement of one queued command
whose server receipt for data position one has already been received. Positions abstract relevant frames;
they are not a claim that every integer server revision changes subscribed data.
Replacement reopens the same partition from its certified checkpoint. Account
partition changes, actual membership/digest logic and full snapshot staging are
not represented by this model.

The checked safety properties are that certified data positions are saved, an
old-owner failure cannot fence a replacement, and queue retirement cannot precede
its certificate. TLC explores the complete finite
state graph at the declared domains: three or four data positions and generations
zero through two. These are model bounds, not production limits. A stuttering
metadata-failure action means no liveness or eventual-success claim is made.

The model assumes ordered frames, atomic persistence, correct checkpoint replay
and the correspondence between its events and production operations. It does
not mechanically derive from TypeScript. TLC, Java, the specifications and that
correspondence remain trusted. The public schedules provide integration evidence
for the modeled counterexamples; they are not a refinement proof.

Three deliberately broken policies must yield the named safety violation and an
actual counterexample: omit the data fence, allow old failures to fence the
current owner, or retire an intent on row persistence before its watermark. Syntax errors, incomplete states, arbitrary tool failure and
partial exploration do not count as successful negative controls. Candidate
runs require the completed/no-error marker and successful exit. CI retains the
model, configurations, complete logs, traces, bounds and source/tool hashes.

Reproduce with Java 21 and the checksum-pinned TLC release from toolchain.json:

```sh
python3 verification/tla/check_replica_durability.py --jar /absolute/tla2tools.jar --output /fresh/output
pnpm --filter @gonvex/client exec vitest run src/sdk-persistence-barrier.test.ts
```

## Remaining guarantees

This closes a concrete failed-data-to-watermark path. It does not prove all
asynchronous SDK behavior, transaction/receipt durability on the server, permanent
idempotency retention, old-runtime behavior without watermark negotiation,
authorization correctness, persistence under power loss or eventual reconnection.
Those remain work in the broader data-guarantee program. The explicit invariant
and correspondence must evolve with protocol changes.
