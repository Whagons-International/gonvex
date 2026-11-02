# IndexedDB browser guarantees

`pnpm --filter @gonvex/client test:browser-storage` runs both production IndexedDB
adapters in real Chromium on Linux. It bundles the SDK source, uses an isolated
persistent browser profile, and verifies these schedules for each adapter:

| Schedule | Required observation |
| --- | --- |
| Abort after a native entity write request succeeds | All rows and the durable checkpoint roll back together |
| Kill the owned browser during an uncommitted transaction | Reopening the same profile reads the old rows and checkpoint |
| Kill the browser after an acknowledged commit | Reopening retains committed values and deletion tombstones; stale transactions cannot restore older values |
| Receive a reducer result and watermark during a paged snapshot | Caller remains pending, intent remains durable, and checkpoint stays behind the unfinished snapshot; successful promotion plus a matching ready frame allows completion |
| Abort native writes during snapshot promotion, then crash/reopen | No success response, no checkpoint advancement, and the same durable intent key, local sequence and arguments and unresolved accepted state survive |

The abort and mid-transaction crash fixtures witness the native IndexedDB
request's `success` event before interrupting the transaction. The crash test
keeps that transaction active with native requests until the browser is killed.
It obtains the browser PID from its own CDP session and verifies the PID's
command line identifies this fixture's temporary profile before sending
`SIGKILL`. It never targets a user's existing browser. Profile data lives in the
configured test directory and is removed at completion.

The SDK scenarios inject protocol-shaped messages through a controlled socket.
They use actual IndexedDB for the replica and outbox. All state changes go
through the normal SDK and adapter interfaces; the frame-lane promise is read
only to join processing. A restarted queue must recover the original durable
intent instead of allocating a replacement identity.

## Negative control and model connection

Separate in-memory bundles remove only snapshot deferral, then both snapshot
and opening-watermark deferral. The single-guard omission remains a passing
safety control because opening-watermark deferral also protects the unfinished
snapshot. The two-guard omission must persist watermark revision 8 while the
covered row is absent. Arbitrary exceptions, missing fixtures and timeouts do
not satisfy the negative control. Production keeps revision 7 until the final
snapshot page and matching ready certificate are persisted.

The fresh run on the prior fixture exposed missing ready-frame delivery and an
outdated expectation that known acceptance became pending after snapshot write
failure. The fixture now emits the ready frame and expects committed acceptance
with an unresolved replica marker after browser restart. The corrected Chromium 149.0.7827.55 run passes all 18 Node-reported tests,
including both adapters, both omission controls and the three upgrade scenarios.

This connects real browser schedules to the saved-data-before-certificate
invariant in `ReplicaDurability.tla`. It is integration evidence rather than a
source refinement proof. The two-guard control shows why an interleaved
watermark is not, by itself, evidence of an unsafe SDK acknowledgement. The
consumer's snapshot and opening deferrals are part of the contract.

The suite contains ten positive adapter scenarios and the broken-bundle check
for both adapters. A separate outbox scenario commits an accepted intent, kills
the browser and verifies late rejection/retry/failure cannot re-arm the same
durable record after restart. The current Node runner reports 18 tests including parent
tests and upgrade schedules. The original 13-test Chromium 149 run used one CPU, 1 GiB memory and no
swap; peak charged memory was 304 MiB. CI repeats the suite and retains a JSON receipt
with completion status, source hashes, bundle hashes and browser version. A
missing declared case or failed negative control fails the run.

## Limits

These checks cover browser-process failure while the OS remains running, not
machine power loss or disk failure. They do not prove Safari, Firefox, quota
exhaustion, storage eviction, authorization admission, arbitrary cross-tab
schedules, server-native encoded snapshot delivery or native app SQLite. The
controlled wire is not a running Rust/PostgreSQL backend. These boundaries
remain in the broader guarantee register. No dedicated OPFS or SQLite WASM work
is included.

CI already installs Chromium. For a local run, install the workspace's pinned
Playwright Chromium and supply a directory on the build filesystem:

```sh
GONVEX_BROWSER_TEST_DIR=/absolute/test-profile-parent \
GONVEX_BROWSER_TEST_OUTPUT=/absolute/receipt.json \
pnpm --filter @gonvex/client test:browser-storage
```


## Historical replica normalization

The current two-file Chromium suite passes thirty-five tests. Schema-1 and
schema-2 replica fixtures cover complete state, owner separation and browser
restart. Each witnesses an actual normalized row request before native abort
and before SIGKILL, then verifies that raw original schema and snapshot rows
survive. Ambiguous ownerless snapshots fail explicitly; a compiled guard-removal
control demonstrates checkpoint overwrite. See
[upgrade-compatibility.md](upgrade-compatibility.md#historical-replica-primary-key-migration)
for the reproduced primary-key failure, fix and remaining matrix.


## Replica sequence exhaustion

A real Chromium regression reproduced the normalized engine committing sequence
`9007199254740992` and revision 8 from a prior counter of `9007199254740991`.
JavaScript cannot represent consecutive integers beyond that boundary, so a
subsequent commit could reuse a position and hide a peer update. The engine now
checks the previous value before incrementing, matching the legacy adapter's
guard. A rejected write preserves its rows, membership, cursor, owner and counter.

Both adapters reject six exercised counter values: the last safe integer, an
unsafe integer, a negative value, a fraction, a nonnumeric string and infinity.
The last valid increment, from `9007199254740990` to `9007199254740991`, succeeds
and persists its data. The baseline fails the engine case with its actual
returned position and durable cursor; the legacy comparison already passes.
The fixed full browser suite passes thirty-five tests and the client suite
passes 963 tests.

[`verify-replica-sequences.mjs`](../../verification/typescript/verify-replica-sequences.mjs)
translates each adapter's actual `nextSequence` guard and increment. It checks
the metadata read, guard, increment, metadata write and return order structurally.
Eight laws per adapter establish invalid-number, negative and exhausted refusal,
valid progress, safe strictly increasing results, one-position increments and
the last permitted increment. Six changed source transitions per adapter produce
solver counterexamples, including ignoring exhaustion, reusing a position and
skipping a position. Seventy extractor tests and the solver run are required
in policy CI, with source hashes and generated queries retained.

This proof assumes ordinary typed fields, standard Number coercion and
`isSafeInteger`, exact addition of admitted safe integers and transaction
serialization. These arithmetic laws start after persisted-integrity validation
and prove the increment expression. The tests separately exercise actual
IndexedDB writes. The additional persisted-counter predicate and transaction
bridges are described below.

A compiled engine source control removes both increment admission and the
persisted-integrity validator, then performs two
real IndexedDB commits. Both return position `9007199254740992`; the second row
and revision 9 are durable, while `readChanges` after the first position omits
that row. The suite requires this observed violation alongside the fixed-adapter
rejection and last-valid-increment cases. This connects numeric exhaustion to
the actual peer change-reading path.


## Validate startup before publication and retire settled activations

Both real IndexedDB adapters previously allowed the shared `LocalReplica` to
restore rows and a cursor with an unsafe startup sequence. The baseline records
actual row `Original` and cursor revision 7 becoming readable from counter
`9007199254740992`. Startup now captures the sequence once and applies the
existing binary64-verified `isReplicaRevision` numeric domain before making the
scope readable, assigning the checkpoint or notifying listeners. Missing
sequence metadata from supported older snapshots retains its zero default.

Six invalid numeric cases per adapter fail without publishing rows or a cursor,
including unsafe, negative, fractional, nonnumeric and infinite positions. Raw
IndexedDB tables remain unchanged. The same replica recovers after the fixture
restores the original counter; ordinary and last-safe valid counters hydrate.
This is a shared publication guard, rather than full validation of every direct
adapter read or persisted encoding.

That recovery check exposed a second bug: `scopeActivation` retained its settled
promise indefinitely, so retrying after repair returned the old failure and
explicitly reloading a healthy scope could skip the new read. Success and failure
now retire only their own activation handle. An old completion cannot clear a
new owner's in-flight handle. Two unit schedules cover forced reload and
overlapping owners.

The numeric startup control removes both adapter integrity and shared numeric
admission. Other controls independently omit the
activation ownership check and retain the settled handle. The first publishes
invalid state through both actual adapters. The second queues Bob twice after
Alice settles, while the intact source shares Bob's one pending promise. The
third retains the original failure after the actual persisted counter has been
restored to `1`. The mandatory browser suite now requires all these observed
violations as well as candidate progress. The current storage and upgrade run
passes 53 Node-reported tests.

The sequence verifier also checks the actual startup call site: captured value,
verified predicate import, prior owner fence, publication order and identity-
guarded terminal cleanup. Its numeric domain is proved by the separately
required `revision-policy.ts` binary64 proof. Structural source checks do not
establish the whole asynchronous hydration protocol; ordinary typed fields,
nonreentrant reads and the application queue remain assumptions.


## Activation identity exhaustion and listener reentrancy

The public activation schedules reproduced three further ownership failures.
At the numeric boundary, Bob and Carol received the same process-local generation
and Bob's rows were published after Carol was requested. With ordinary counters,
an invalidation listener requested Bob but the older outer call restored Alice.
A nested Alice->Bob->Alice request also reused the first Alice promise and
finished with Bob's rows. A disposed or exhausted replica could return a cached
promise instead of reporting its failure.

`activateScope` now rejects closed replicas before consulting the cache. A new
generation immediately retires the old cached handle and makes the old scope
unreadable before notifying listeners. The generation is captured before those
callbacks; numeric admission and a fresh generation comparison precede queuing.
Unsafe generations reject explicitly. The increment still invalidates earlier
safe generations, so an exhausted request cannot leave an old read authoritative.
The last safe generation remains usable. Exhaustion requires a fresh replica;
there is no counter reset that could collide with pending work. Disposal also
invalidates the identity and cached handle before cleanup.

Eight additional binary64 laws use the actual prefix increment and extracted
`isTransactionRevision` policy. They cover positive safe admission, progress,
the first and last valid identities, refusal after exhaustion, distinct admitted
steps, no return to the safe domain after exhaustion and invalidation of a safe
prior identity on rejection. Ignoring the domain and denying all identities each
produce solver counterexamples. The shared model is imported from the existing
revision verifier. The extractor checks both counter-write sites and rejects
resets, decrements, late capture, missing closed/cache fences and premature
publication.

Compiled browser source controls reproduce the numeric alias, the older outer
activation winning over a listener and reuse of a superseded handle. These owner
controls use `MemoryLocalReplicaStorage` to hold exact reads while executing the
production class in Chromium; their receipts state that boundary. The startup
corruption and repair cases use actual IndexedDB. The current suite passes
53 tests, while all 963 client tests pass.

## Persisted counter encoding and durable-position bounds

Both adapters accepted a blank counter as zero. They also accepted a counter
reset to zero while rows already carried position 1. Real Chromium then committed
new revision-8 data at reused position 1. A peer reading changes after position 1
received no row, although the new row was durable. The baseline records the raw
counter, commit receipt, durable row authority and empty peer change set.

[`indexeddb-sequence.ts`](../../packages/client/src/indexeddb-sequence.ts) now
requires the canonical string written by the allocator. Missing metadata is zero
only when the durable positions permit that legacy state. The counter must be
a nonnegative safe integer covering the latest entity, window and reset
positions in the same scope. Six public data readers and five write callers per
adapter validate inside their existing transaction before publishing data or
allocating a position. Watermark transactions include the entity table for this
check, while changing only window metadata and the cursor.

The two position lookups each read at most one compound index key. They do not
load row values or membership payloads. A valid owner continues at position 2
while another owner is already at 1001. Historical normalized records without
position metadata still read at zero and migrate normally.

The mandatory real-browser suite checks twelve noncanonical encodings through
all six data readers and a write. It also checks a counter behind an entity,
behind a window, missing despite positioned data, and behind a reset marker.
Every refusal retains the raw schema, records and metadata. Removing only the
shared integrity validator from a compiled bundle reproduces the hidden peer
update in both adapters. The older numeric exhaustion and startup controls now
explicitly remove their two applicable validation layers.

The source verifier extracts the actual `replicaSequenceCovers` expression. Eight
laws require valid numeric domains and coverage of each durable position, while
admitting valid tuples, the last safe checkpoint and legacy zero. Eleven changed
source policies must produce solver counterexamples. Structural checks bind the
canonical encoding test, bounded key reads, reset capture, validated return and
all 22 adapter transaction entry points. Extractor controls reject unbounded or
row-bearing reads, another owner's bounds, missing validation and publishing a
different position.

This relies on standard Number/String behavior and native compound-index and
transaction semantics. It detects the exercised counter regressions and malformed
metadata. It does not reconstruct retired allocation history, authenticate every
stored field or detect coordinated corruption of both the counter and durable
records. Monotonic allocation through supported writers still relies on their
checked increment and native transaction serialization.

## Future formats and normalized historical upgrades

Real Chromium reproduced five future-format acceptance failures across both
replicas, the direct outbox and both browser-upgrade databases. Four readers
also changed a future schema by adding a missing index. The shared
[`versionedIndexedDB`](../../packages/client/src/indexeddb-version.ts) wrapper
keeps Dexie's native-open retry versioned, so native IndexedDB rejects before
hydrating any state. The five candidate cases require exact raw record and
schema preservation. A compiled retry-bound omission must reproduce all five
unsupported reads and the four native schema changes.

Normalized replica schemas 3 and 4 now have complete historical fixtures with
two owners, distinct epochs, ordered membership and indexed predicates. The
fixture aborts and then kills its owned browser after a new native index answers
a request inside the upgrade transaction. Raw schema and every record must
match the original after each interruption. Normal upgrade and a second browser
restart must retain both owners' complete snapshots and the indexed predicate's
matching row. These checks cover selected real upgrade transaction boundaries,
not every IndexedDB request or machine power loss.
