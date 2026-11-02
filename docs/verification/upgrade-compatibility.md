# Persisted state and protocol compatibility audit

This is the source-derived compatibility register for UPG-1. A declared transition
is distinct from evidence that a particular platform executed it. The tests below
must be bound to the release source before UPG-1 can close.

| Stored format | Reader and transition | Preserved meaning and refusal behavior | Existing evidence and gaps |
| --- | --- | --- | --- |
| PostgreSQL legacy `_gonvex_mutation_idempotency` receipts | `ControlPlane::ensure_reducer_idempotency`, rename in the routed search path | Preserve account/key/path/result and retry barrier; refuse coexisting legacy/current tables before new reducer execution. Serialize cold initialization with a transaction-owned advisory lock and recheck after waiting. | Two real-backend failures reproduced lost legacy replay and ambiguous-history execution. Dedicated native regressions also cover missing-index repair, schema reset and witnessed cold initializer overlap. See reducer-receipts.md for evidence and assumptions. |
| Injected KV outbox, complete scoped entries | `createKvOutboxStore` and `StoreReducerOutbox` | Keep numeric order, owner, intent key and closed delivery state. Malformed JSON, missing identity/state and unknown states require explicit recovery; retain original bytes. | KV recovery controls and real Chromium IndexedDB-backed KV refusal pass; actual native SQLite corrupt-byte/reopen execution remains. |
| IndexedDB outbox schema 1, scoped entries | `DexieReducerOutbox.createDatabase`, schema 1 to 2 | Add scope indexes without changing valid entry identity or acceptance. | Real Chromium now upgrades a scoped schema-1 journal covering pending, inflight, committed, failed and rejected records, retains a second owner, and reopens the same files in a fresh browser process. Raw durable rows preserve every field except the documented inflight recovery transition. A deliberately changed accepted key fails the same identity oracle; this control runs in the required browser suite. |
| IndexedDB outbox schema 1, ownerless entries | Same upgrade transaction | Refuse upgrade with `OutboxRecoveryError`. Aborting retains pending and committed records in schema 1. Never infer an authenticated owner. | Two fake-indexeddb schedules preserve original rows and fail against the deleting baseline. Real Chromium also aborts the upgrade and retains the accepted row in schema 1. |
| IndexedDB outbox schema 2 | `DexieReducerOutbox` | Startup may recover inflight entries to pending. Committed, failed and rejected entries retain their state. | Source-linked state proof and adapter schedules; prove complete read/validation coverage separately. |
| Browser app contract N to N+k | `migrationChain`, `migrateClientData`, `browserUpgradeStorage` | Require each unique adjacent migration, preserve ID, owner, key, receipt path and ordering metadata. Recover only inflight state. Invalidate query membership and cursor after shape transformation. | Five-state/two-version controls and interrupted staged recovery pass locally. Real Chromium process-crash recovery preserves acceptance and receipt identity without rerunning the staged transform. User migration functions remain responsible for business meaning. |
| Browser app contract downgrade, missing or ambiguous step | `migrationChain` | Reject before replacing the original journal and replica. | Unit and browser-storage fixture controls. |
| Backend module and tenant SQL contract 1 to 2 | `apply_dev_sync`, then `browserUpgradeStorage` and reserved receipt reconciliation | Apply the tenant migration, activate the matching V8 module and fence old clients. Preserve original receipt namespace and queue order across local migration and restart. | The composed module-upgrade fixture replaces handlers, renames a real PostgreSQL column and kills Chromium after a committed reply is lost. Recovery requires receipt-only reconciliation, one later edit, complete durable membership and exact business-operation counts. A compiled migration control must fail at the actual backend namespace rejection. |
| Legacy IndexedDB replica schemas 1 and 2 | `IndexedDBLocalReplicaStorage`, through schema 3 normalization | Unnamespaced snapshots move only to the default scope, never an authenticated account. Copy entities, windows and cursor before clearing legacy source rows in the same upgrade transaction. | Real Chromium fixtures for both versions preserve complete rows, memberships and cursors across migration and fresh browser restart. A native abort and SIGKILL after a successful normalized row write each restore the exact original schema and snapshot rows before retry. Version 1 formerly failed with Dexie primary-key UpgradeError; an atomic staged recreation fixes it. Ambiguous ownerless checkpoints refuse without changing original bytes; removing that guard demonstrably overwrites one checkpoint. |
| Legacy IndexedDB replica schemas 3 and 4 | Same adapter through current schema 5 | Add lookup indexes and sequence indexes while retaining scoped row/window/cursor records. | Real Chromium fixtures abort and kill the browser after a new native index answers a request within the version transaction. Raw schema and every stored row must survive. Upgrade and fresh-browser reopen must preserve both owners, ordered membership, cursor and complete indexed-predicate results. |
| Normalized engine IndexedDB schema 1 | `IndexedDBEngineReplicaStorage` | Scoped rows, memberships, window generation and sequences retain their schema. | Reopen/storage contract tests and selected real-browser crash tests. The future-format refusal and compiled control below cover the native open boundary. |
| Future IndexedDB replica, outbox or upgrade-metadata format | `versionedIndexedDB`, used by both replicas, the direct outbox and both browser-upgrade databases | Refuse before opening data, schema repair or journal hydration. Preserve future rows, indexes, version and acceptance history. | Five real Chromium baselines read unsupported future formats; four also changed their schema. All five candidate refusals and five compiled fallback-omission controls run in mandatory CI. |
| SQLite replica unnamespaced schema | `SQLiteReplicaStorageCore.migrateSchema` | Transactionally copy legacy tables into the default scope before dropping source tables. Authenticated scopes cannot read those rows. | Shared historical fixture passes real Node SQLite process kills during replica copy and accepted-intent import, followed by complete recovery and another process reopen. It checks raw schema rollback, tombstones, NUL signatures, indexed predicates and owner isolation. Required Expo/Android execution uses the same schedule; that platform run remains pending. |
| SQLite replica scoped schemas before 3, or historical schemas without a Gonvex checkpoint | Same adapter | Rebuild lookup data transactionally and retain scoped row/window metadata. Preserve application `PRAGMA user_version`. | Real Node SQLite tests cover application versions 0, 2, 3 and 700. The shared historical process fixture also preserves both owners' rows, tombstones, complete membership and indexed predicates across migration/reopen with application version 700. |
| Legacy whole-list journal to SQLite outbox | `ExpoSQLiteOutboxStore.importLegacy`, then `StoreReducerOutbox` | Preserve key, owner, original receipt path, order, arguments and acceptance. Recover only inflight state. Retain source bytes if import is interrupted. | Actual Node SQLite process kill after the accepted row's native SQL statement, rollback, fresh import and another reopen pass. All five states, two owners, idempotent import and monotonic allocation are checked. A compiled source policy that re-arms committed entries must fail the independent accepted-state oracle. Same positive schedule is mandatory in Expo/Android; platform execution remains pending. |
| SQLite outbox with corrupt accepted bytes | `ExpoSQLiteOutboxStore` and `StoreReducerOutbox` | Require explicit recovery on load, mutation, restore and new admission. Preserve corrupt and valid accepted records across restart. | Shared Node SQLite schedule seeds malformed accepted JSON, exits and reopens in a fresh process. All four operations refuse and both raw rows remain byte-identical. The Expo/Android fixture requires the same schedule and cross-process comparison; fresh platform execution remains pending. |
| SQLite Gonvex replica checkpoint | `_gonvex_replica_schema`, current version 3 | Reject unknown, corrupt or future Gonvex versions before modifying the schema or data. | Five refusal controls preserve schema, records and application version. This checkpoint does not make downgrades to older binaries that ignore it supported. |
| Runtime without replica watermark capability | SDK legacy settlement path | Legacy reconciliation behavior is separate from capable-runtime durable watermark barriers. | Existing legacy/capable SDK schedules; exact server-version fixture matrix remains to be assembled. |
| Runtime with replica watermark capability 1 | SDK result/data/watermark lane | Preserve the originating intent until the durable data barrier is reached. Never treat a peer storage sequence as this socket's ACK. | Protocol models, controlled dispatch, real Chromium and Android SQLite evidence cover selected schedules; full upgrade/restart composition remains. |

Unknown journal state is not permission to manufacture a pending intent. An
unsupported ownerless journal is not disposable. Explicit recovery errors retain
its data for inspection or a separately authorized owner-resolution operation.

Query invalidation during a shape migration deliberately withdraws completeness.
It must not claim that old query membership remains valid under the new schema.
The migrated confirmed rows remain durable, and fresh authoritative subscription
state must establish completeness again. Accepted entries must remain accepted
through that recovery.

The numeric database schema versions, application contract versions, server
capability versions and SDK package release versions are different namespaces.
A package version alone cannot establish compatibility. This register still
needs concrete historical package/format fixtures, platform versions, source
hashes, and complete interrupted-upgrade/restart schedules before it is a tested
support matrix.

## Real Chromium upgrade recovery

`packages/client/test/browser-upgrade-recovery.check.mjs` bundles the actual SDK
upgrade, outbox and KV sources. It uses three owned persistent Chromium profiles.
The accepted-upgrade schedule witnesses a completed replica write after staging
the upgrade journal, kills that browser process, and reopens the same profile
and origin. Recovery must preserve committed state, intent key, numeric ID,
original receipt path and transformed confirmed rows. A migration callback that
throws if rerun proves recovery used the staged payload. Completeness stays
withdrawn until a new authoritative subscription establishes it.

Other schedules check native IndexedDB transaction rollback for ownerless
legacy accepted history and retain malformed bytes in an IndexedDB-backed KV
journal while denying both restore and new admission. The required runtime CI
browser step now runs these alongside the existing durability/barrier suite and
uploads separate source-bound receipts.

The retained r2 run passes all three contracts. The earlier r1 attempt used a
localStorage transform counter as a post-crash witness; that counter was lost
on abrupt process death, so the attempt is excluded. The corrected witness
observes the count before the kill and refuses any transform during recovery.
This is a process-crash test, not evidence of machine power-loss durability.

## Coalescing recovered intent identities

A recovered pending intent can have no `sentAt` or counted failure while still
having reached an older server. Absence of those fields cannot establish that
replacing its key and arguments is safe. The old admission rule allowed that
replacement in both Dexie and SQLite recovery schedules.

New pending intents carry `knownUnsent: true`. Inflight admission, recovery,
acceptance, rejection and retry clear that provenance. Legacy SQLite import
clears it even if the source blob contains a true marker. Both the candidate
selection and the atomic browser/SQLite replacement require pending state and
strict equality of the marker to true. Unknown history keeps its original key,
arguments and numeric position; a subsequent edit receives a separate position.
Fresh pending edits can still coalesce.

`verification/typescript/verify-coalescing.mjs` extracts the production helper's
closed grammar. Three laws exclude unknown/sent work and every non-pending
state while admitting fresh pending work. Five typechecked broken helpers must
produce solver counterexamples. Extractor controls reject coercion, truthiness,
calls, type assertions and unmodeled unknown-input comparisons. This proves the
pure admission decision only. It does not prove marker provenance, transactional
call placement or a whole queue execution.

Eight cross-adapter schedules check recovered pending/inflight entries, stale
inflight provenance and fresh-edit controls. Six identity-preservation assertions
fail against the pre-fix source, while both fresh-edit controls pass. Five browser
store schedules also exercise the atomic replacement directly. SQLite import
has a separate stale-marker control.

Durability is part of that provenance contract. A failed durable transition must
not let a sender proceed while the disk still says an intent was never sent.
The direct Dexie queue now reports `OutboxRecoveryError` after a durable operation
fails instead of declaring the operation successful in memory. Memory-only use
remains available when explicitly disabled or IndexedDB is absent. The SQLite
adapter now requires strict persistence. Two injected IndexedDB transaction
failures reproduced the old false success for delivery and acceptance; both
fixed schedules require rejection and verify that retrying a successful durable
send preserves the original command alongside the later edit.

The required policy workflow verifies the helper and retains source/solver
receipts. The required SDK workflow runs the adapter regressions. These checks
still leave historical-format coverage, generic custom-store contracts and the
native-device failure matrix open.

## Older entries without dependency metadata

An omitted `entityKeys` field is compatible legacy input. Cloning it as a
required iterable previously failed, and generic KV hydration could then fall
back to an empty queue. Empty keys with no dependency set also allowed a later
write to pass an older backed-off intent despite having no evidence that the
commands were independent.

Recovered missing keys normalize to an empty list for the queue's public entry
shape. Both missing and empty keys without an explicit dependency set establish
a global ordering barrier. A known empty dependency set remains independent.
The original intent identity and arguments remain intact. Acceptance removes
the delivery barrier without making the accepted entry sendable again.

Nine schedules exercise KV, IndexedDB and SQLite: missing keys, empty unknown
keys and explicit empty dependency controls. All six unknown-metadata schedules
fail against the pre-fix source; the three known-dependency controls pass. These
are adapter execution checks, not a proof of the complete causal scheduler.

## KV persistence and journal positions

The built-in KV outbox now requires persistence. A read error cannot admit an
empty session queue, and a failed append, delivery transition or retirement
cannot report success by switching to memory. Five injected pre-operation KV
failures reproduce those former successes. Enabling strict errors alone exposed
three further failures: the cached queue had already changed before persistence
failed. The queue now restores its own prior cached entry on those paths while
preserving a newer entry installed during the await. The five controls and
related settlement/ownership checks pass. These tests do not model a host that
commits a write and subsequently rejects its promise.

KV journal positions now have a separate version-1 allocation checkpoint in
`reducer_outbox_meta`. Reserving a position persists that checkpoint before
returning its ID. Queue deletion, scope clearing and failed insertion cannot
reuse it. Allocation validates current history and seeds older journals from
their highest ID. Unsupported, malformed, fractional and exhausted checkpoints
require recovery and retain their original bytes.

Two pre-fix schedules demonstrate reused positions: overlapping queue instances
overwrite one another, and a late old-owner retirement deletes a new owner's
row after a cleared journal reopens. The fixed allocator serializes calls sharing
the same `GonvexKv` object. Those schedules pass alongside failed-append, reopen
and checkpoint-refusal controls.

This host interface has no compare-and-swap or transaction operation. Separate
writer processes or distinct host objects addressing the same backing store
must use an adapter with an atomic allocator and delivery updates. The native
SQLite and browser shared-store adapters have separate allocation contracts.
The KV execution checks do not prove distributed exclusion or the entire
retirement protocol. A source-linked refinement of the allocation protocol and
its complete persistence-failure contract remain audit work.

## Acceptance across uncertain completion and stale recovery

A KV host can complete a write and then reject its completion promise. The
previous cache-only transition path could restore an older inflight entry,
then overwrite the durable committed entry during a late failure or recovery.
The KV adapter now serializes its journal operations and implements `update`
by reading the current persisted entry before invoking the SDK transition. A
failed completion cannot make a later transition ignore durable acceptance.
This serialization covers callers sharing the same host object; it does not
supply a cross-process transaction to the generic KV interface.

`StoreReducerOutbox.loadAll` also recovered inflight work from its cached value
with a blind write. That could overwrite a different queue's newly committed
entry, including with the real SQLite adapter. Stores offering `update` now run
the proved recovery policy against the persisted entry inside that operation.
Local execution metadata already used the atomic update path when available;
the new KV operation makes that protection available to its metadata writes too.

The source-restoring negative run reproduces four failures: committed KV write
with rejected completion, delayed KV metadata after acceptance, and recovery
from an old inflight cache in KV and SQLite. Six controls pass on that source.
SQLite's existing atomic metadata update is one of those compatibility controls,
not a reproduced SQLite metadata defect. All ten schedules pass on the fixed
source. Original source and test hashes are retained with the result.

## SQLite version and journal integrity

`SQLiteReplicaStorageCore.migrateSchema` now uses a Gonvex-owned
`_gonvex_replica_schema` checkpoint. Application `PRAGMA user_version` is left
unchanged. Without the owned checkpoint, initialization rebuilds historical
lookup data even when the application's version exceeds 3. Previously that
case could return an empty predicate result with complete coverage despite
matching durable rows. Inspection, rebuilding and checkpoint publication run
inside one transaction. Failed initialization can be retried on the same
adapter. Future or invalid owned versions require recovery before mutation.

The real Node SQLite schedules in
[`schema-migration.test.ts`](../../packages/expo-sqlite/src/schema-migration.test.ts)
exercise four application versions, default-owner legacy migration, cursor
preservation, five invalid/future versions and rollback with retry. The first
source-restoring run produced ten intended failures and two passing historical
controls; all twelve passed with the namespace fix. A further admission schedule
checks that a newer checkpoint committed before transaction entry is not
overwritten by stale schema inspection. That schedule fails with pre-transaction
inspection while the twelve compatibility controls pass. All thirteen pass
when inspection shares the migration transaction.

`ExpoSQLiteOutboxStore` reads and checks its indexed ID, owner, intent key and
state against the JSON record before loading, updating or coalescing it. SQL
JSON quoting preserves embedded NULs in the indexed owner/key across bridges
that truncate ordinary TEXT reads. Update callbacks cannot change identity in
place, and valid in-place state updates are persisted. Sequence allocation
rejects negative, fractional and exhausted counters before returning an intent.

[`outbox.test.ts`](../../packages/expo-sqlite/src/outbox.test.ts) supplies sixteen
column/payload disagreement schedules, six in-place identity mutations, one
permitted state update and three sequence failures. All twenty-six fail against
the prior adapter while its seventeen existing controls pass. All forty-three
pass with the fixes. These are actual Node SQLite checks, not evidence of Expo
driver behavior, native crash recovery or every legacy import ordering case.

Legacy import also had an ordering failure when an early numeric ID collided
with a different existing command above a high-water mark. Remapping that row
could place it after a later legacy row whose old ID remained free. Import now
moves subsequent positions forward with the remapped row. If already-imported
positions contradict the original order, the transaction aborts and the legacy
source remains available. Matching indexed intents are validated against their
JSON owner before a source can be removed. Four additional source-restoring
controls reproduce these failures; the prior forty-three controls still pass,
and all forty-seven outbox schedules pass with the fix.


## Backend-composed browser contract migration

The required `check_sdk.py` coordinator now invokes `sdk_browser_upgrade.mjs`.
It runs the built SDK and production `browserUpgradeStorage` in real Chromium,
using actual IndexedDB and authenticated WebSocket calls to the native backend.
An offline queued `abort` reducer is delivered, inserts a row and throws. The
SDK persists its rejected intent. Before migration can start, the Rust fixture
queries the routed PostgreSQL database in one snapshot and requires no committed
business row and no receipt for the original intent key.

After closing the first browser context, contract 1->2 renames the handler to
`write`. Sequence ID, owner, key, arguments, creation time and attempt count must
survive. Original `receiptPath=abort` is retained, confirmed rows remain, and old
cursor/membership contracts are removed. The rejected record remains parked
until explicit public `retryIntent`. The renamed call must then commit once,
with a durable replica row, collection membership and cursor covering its actual
commit revision. Rust checks the final receipt still uses the original namespace.
A fresh context configured for contract 1 must refuse before sending any frame.

A negative control introduces a business effect into the owned fixture just
before its PostgreSQL rollback witness. The browser must fail the explicit
rollback-effect assertion. This selected Chromium 149.0.7827.55 composition and
its control pass locally. It does not establish native SQLite application-contract
migration, all historical package formats or all supported upgrade combinations.


## Historical replica primary-key migration

The real schema-1 fixture reproduced `UpgradeError: Not yet support for changing
primary key` before any checkpoint could reopen. The old declaration changed
`snapshots` from `&key` to `&scope` in place, which Dexie refuses. The reader now
stages the single ownerless checkpoint, removes and recreates the snapshot store,
and restores that checkpoint only to the default scope. Versions 1.1 through
1.4 describe intermediate steps within one IndexedDB version-change transaction;
they are not separately committed checkpoints. Existing schema-2 and later
databases skip these steps and continue their existing normalization path.

The schema-1 fixture verifies the default scope and empty authenticated scopes.
The schema-2 fixture retains two owners with different epochs, data and cursors.
Both include ordered query membership and completeness. Before each successful
upgrade, the fixture first aborts and then kills its owned browser after an
actual normalized entity request succeeds while the version transaction remains
uncommitted. A separate raw database open must see exactly the original schema
and snapshot records after each failure. Normal upgrade and another fresh
browser process then preserve the complete expected post-image.

Multiple ownerless checkpoints require explicit recovery: selecting the last
record would silently lose another checkpoint. The real browser refusal retains
both source rows. A compiled source control removes this guard, reaches actual
migration and demonstrates the overwritten checkpoint. These cases run in the
existing mandatory browser-storage suite. Process crashes do not establish
machine power-loss durability or behavior in other browser engines. The schema-3
and schema-4 index-upgrade fixtures exercise the separate normalized path.


## Replica counter positions

Both IndexedDB adapters now reject an invalid or exhausted coerced counter before
advancing metadata. The normalized engine previously committed an unsafe next
position; the real browser baseline records that position and advanced cursor.
Actual rejection/progress tests and source-linked arithmetic proofs are described
in [indexeddb-browser.md](indexeddb-browser.md#replica-sequence-exhaustion).
Shared startup validates the captured numeric counter before publication. Both
adapters now also validate canonical counter encoding and coverage of durable
entity, window and reset positions inside each public data-read or allocation
transaction. Real browser baselines reproduce hidden peer updates after a
counter is coerced or regressed; candidate refusals preserve raw state and two
compiled controls reproduce the violation. Eight source-derived integrity laws
and 22 checked transaction entry points cover this boundary. See
[persisted counter validation](indexeddb-browser.md#persisted-counter-encoding-and-durable-position-bounds)
for its native-index assumptions and remaining corruption limits.

## Future format refusal before Dexie fallback

Dexie retries a failed versioned native open without a version. An old Gonvex
reader therefore opened a newer database, and its schema repair could add an
index and advance that future database's native version. Real Chromium reproduced
this in both replica adapters, the direct outbox and the browser-upgrade queue.
The browser-upgrade metadata reader also accepted its future format.

[`versionedIndexedDB`](../../packages/client/src/indexeddb-version.ts) preserves
the declared schema's native version when that retry omits the version. Native
IndexedDB then rejects with `VersionError` before opening the newer database.
The wrapper retains native method receivers and each adapter's injected factory.
Dexie multiplies its declared schema version by ten for native IndexedDB.
Schema and application contract versions remain separate.

The fixture removes an expected index from future replica and queue formats,
and stores an accepted-history sentinel. Candidate refusal must preserve the
exact raw version, table definitions and all records. Removing only the retry
bound from a compiled helper must hydrate each future format again. Four of
those controls must also reproduce the native schema increment. Ordinary opens
and supported historical upgrades remain positive controls in the same suite.

## Renamed intent recovery after a committed lost reply

The composed browser upgrade now withholds the successful renamed reducer reply
before SDK dispatch, confirms a retained previously-sent journal entry, and kills
its owned Chromium process. The backend and PostgreSQL remain alive. A fresh
browser must retain the original numeric ID, owner, key, ordering timestamp,
arguments and historical `receiptPath`, then query that original receipt without
another ordinary reducer call. Retirement requires durable rows, membership and
a cursor covering the lost reply's actual committed revision.

A separate bundle removes `receiptPath` only from production delivery. It keeps
the migrated durable entry and fresh lookup intact. The backend commits under
the wrong path and then rejects the historical-path lookup. The coordinator
requires that exact rejection and its wire/journal witnesses; a timeout cannot
count as this negative control. This establishes the selected local application
contract rename, not a backend module-contract replacement or native app upgrade.

## Backend module and tenant SQL replacement

[`check_module_upgrade.py`](../../verification/backend-receipts/check_module_upgrade.py)
owns PostgreSQL and invokes one explicitly selected native test. That test uses
the production `apply_dev_sync` function, including its project-key check,
tenant SQL migration, module-host load/activation, cache invalidation and
`ModuleReloaded` notification. The sync HTTP parser itself is outside this
fixture. The application connects through real authenticated WebSockets.

Contract 1 exposes `write` and `edit` against `receipt_records.value`. A complete
replica contains an independently confirmed row. Two intents enter the public
offline admission path before either is delivered. The first inserts a row; the
fixture loses its actual committed reply before SDK dispatch. Only that first
call may reach the server. PostgreSQL witnesses one insert and one receipt.

The production sync path applies a tenant migration that renames `value` to
`count`, then activates contract 2 with `writeV2` and `editV2`. The old handlers
are absent. A separate connected contract-1 SDK must receive
`client.updateRequired`, while the retained journal stays unchanged. Only the
owned Chromium process is killed. The runtime, V8 host and PostgreSQL continue.

A fresh browser migrates argument and confirmed-row shapes, retaining numeric
IDs, owner, keys, ordering timestamps, attempt counts and original receipt paths.
It withdraws the old cursor and membership before connecting with contract 2.
The original write must reconcile through receipt path `write` without another
ordinary write call. The edit must send once through `editV2` and retain receipt
path `edit`. Durable rows, complete membership and the new cursor must cover its
commit. A trigger-backed PostgreSQL ledger requires exactly `INSERT`, `UPDATE`
in that order; both original receipt paths remain intact. A later local
contract-1 reopen refuses before connecting.

The negative bundle changes only `migrateClientData`'s receipt-path assignment.
Its migrated journal loses the historical path, and the fresh SDK queries the
wrong namespace. The real backend must reject that lookup with its path-reuse
error. The driver requires that error, the mutated source hash and the durable
journal/wire witnesses. A missing fixture or timeout cannot count. The required
runtime workflow runs both the positive schedule and this control and retains
their source-bound receipts.

The coordinator also runs a separate schedule with a received, durably saved
acceptance. An independent native IndexedDB transaction regresses the replica
counter while actual replica frames are held. The first intent must already be
`committed` with `awaitingReplica` before those frames reach the SDK. Their
release must cause a real data-write rejection and fence the transport. Raw
replica records must remain exactly as they were after fault injection. Neither
a command acknowledgement nor a retirement attempt may occur.

After queued frame work has stopped, the fixture restores only the injected
counter record. It then performs the same backend replacement, Chromium kill and
fresh application migration. Acceptance must remain durable through migration;
receipt reconciliation must recover the first intent without an ordinary
redelivery. The later intent still executes once, and the PostgreSQL ledger
requires the same insert/update order. This exercises known acceptance, in
addition to the lost-result schedule above.

A second compiled source control removes only the processed-watermark guard
inside `registerCall`'s reducer-result branch. It leaves the action-result branch
intact. With the real counter fault committed and the business row still absent,
the SDK acknowledges or attempts retirement too early. The named data-ordering
assertion must reject that execution; setup errors and timeouts receive no credit.

This covers the selected supported contract and SQL transformation. It does not
prove the business meaning of arbitrary application migrations or native app
driver behavior. The existing migration policy and platform matrix remain
separate requirements.
