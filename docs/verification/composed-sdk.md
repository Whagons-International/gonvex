# Authenticated SDK, receipt and replica composition

The fixture in
[`authenticated_sdk_receipt_replica_composition`](../../rust/crates/runtime/src/tests/receipt_guarantee_tests.rs)
is explicitly selected by
[`check_sdk.py`](../../verification/backend-receipts/check_sdk.py).
It is ignored in ordinary Cargo runs because it also requires built JavaScript
packages, a real module host, PostgreSQL and Node's native WebSocket and SQLite.
The required runtime workflow invokes its coordinator separately.

The basic local coordinator passed ordinary commit, lost reply, SQLite failure
and fresh SDK/database-handle recovery. The extended coordinator also kills
actual SDK child processes before and after the local acceptance commit. Its missing-row and missing-coordinator
controls passed too. The extended coordinator also passes a held SQLite commit
and rejects separately compiled SDK barrier, acceptance persistence and recovery
probe mutants at their intended assertions. It ran with Node 22.23.4, SQLite 3.53.4 and PostgreSQL 16 on Linux.
Native binaries are hashed before execution and checked again afterward. ELF
paging is therefore setup; this fixture does not measure cold-start latency.

The Rust side installs a replica collection and visibility plan, provisions an
account, session and active tenant member, and serves the production runtime
router on an owned ephemeral TCP listener. The SDK authenticates over that
WebSocket. It does not receive a constructed tenant session or fabricated server
frames. The fixture also verifies real session admission through the control
plane and checks business and account-scoped receipt counts after the SDK exits.

[`sdk_composition.mjs`](../../verification/backend-receipts/sdk_composition.mjs)
uses the built SDK and Expo SQLite adapter with Node's actual SQLite engine.
Its journal and replica occupy independent files. At retirement, a separate
read connection checks committed rows, collection membership and the commit
cursor synchronously. This oracle cannot add an awaited storage barrier that
would conceal premature retirement.

| Schedule | Required witness |
| --- | --- |
| Ordinary authenticated reducer | Actual result, committed replica post-image and empty durable journal. |
| Lost reply | Drop one real reducer result before SDK dispatch, close the SDK, retain the original intent and reopen the SDK and database handles. Read-only receipt lookup discovers the committed result without resending. Separate backend receipt tests exercise actual retries, including changed transport IDs. |
| SQLite write failure | A real SQLite trigger aborts the target row insertion after the SDK receives acceptance. The old cursor and durable accepted intent survive. Reopening performs read-only receipt lookup and replica catch-up, with exactly one reducer call across the schedule. |
| Missing durable row control | Delete the persisted target row immediately before the same retirement oracle. The coordinator must observe the intended missing-data assertion failure. |
| Missing coordinator control | Explicitly selected native test must fail rather than skip or report zero executed tests. |
| Held SQLite commit | Observe the actual backend result while a SQLite transaction contains the uncommitted replica row. Another connection must not see it, the SDK call must remain pending and the journal must retain its intent. Release the transaction to prove progress. |
| Production-source acceptance control | Omit the received-result persistence callback in a separate SDK bundle. The replica-failure schedule must detect that acceptance was not durably recorded before reopening. |
| SQLite acceptance journal failure | A real SQLite trigger aborts the local committed-marker insertion. The preceding inflight record survives. Fresh SDK/database handles discover the backend receipt before redelivery and reconcile without a second reducer call. |
| Account A->B->A | A real SQLite acceptance write commits for A, but its returned promise is held after A data and watermark are applied. B restores a parked intent with the same key and a distinct sequence ID. Releasing A completion cannot clear B prediction or alter B journal. B cannot read A receipt; explicit B retry commits in B namespace. Returning to A reconciles without another reducer call. |
| Tenant A->B->A | Same account and token, separate physical PostgreSQL databases, independent epochs, private B sentinel and receipt namespaces; delayed A completion cannot alter B, and return to A reconciles without resend. |
| Tenant stale-completion source control | Remove the pending-call identity guard and run tenant rotation first. With a witnessed higher B clock, the replacement prediction assertion must fail while authenticated in B. |
| Stale-completion source control | Remove the current-pending-call identity guard in a separate SDK bundle. The real account-switch schedule must fail because A completion clears B prediction. |
| Concurrent SDK delivery, owner commit | Two authenticated SDKs open independent SQLite journals containing the same immutable intent. PostgreSQL must show one business transaction blocked by the fixture gate and the other receipt claim blocked by that transaction. Both reconcile the single commit into durable replica data and retire their journals. |
| Concurrent SDK delivery, owner rollback | Cancel the witnessed first backend query. PostgreSQL must show the second claimant reaching the business-write gate after the first transaction rolls back. The rejected caller explicitly retries through receipt lookup. Each SDK sends only one ordinary reducer call. |
| Concurrent overlap witness control | Remove the overlap certificate after the actual blocked pair is observed. The SDK schedule must fail the named missing-overlap assertion. |
| Browser rollback and contract rename | A real SDK reducer fails after inserting, and Rust observes zero committed business rows and receipts in one PostgreSQL snapshot before permitting migration. Production browserUpgradeStorage moves contract 1 to 2, preserves the parked intent identity and confirmed rows, invalidates old cursor/memberships, and explicitly retries under its original receipt namespace. Persisted data, membership and cursor must cover the resulting commit. An older contract must refuse before any network frame. |
| Rollback witness control | Insert an effect into the owned tenant fixture before its SQL witness. The browser must fail the named rollback-effect assertion, rather than migrate or succeed. |
| Renamed-handler committed reply loss and restart | Withhold the renamed handler's actual committed result before SDK dispatch, SIGKILL the owned Chromium process, then reopen its profile. Original identity and receipt namespace must survive. A read-only receipt lookup must retire the intent after durable replica catch-up without another ordinary call. |
| Renamed receipt source control | Compile production delivery with both receipt-path fields omitted. Keep the durable migration and fresh lookup intact. Require the backend's actual historical-path mismatch rejection, source hashes and journal/wire witnesses. |
| Backend module, SQL and client-contract replacement | Commit the first of two ordered intents and lose its reply. Use production sync to rename a tenant column, replace handlers and require contract 2. Fence a live old client, kill Chromium and migrate the retained files. Require receipt-only recovery of the first intent, one second delivery, complete durable membership and a PostgreSQL ledger containing exactly insert then update. |
| Migration receipt-identity source control | Remove only the original receipt-path assignment from a compiled `migrateClientData`. Require actual backend namespace rejection after the supported module and browser upgrades. |
| Accepted intent with a corrupt replica counter | Deliver the real committed result and durably retain `committed` plus `awaitingReplica`. Hold actual replica frames, regress the native IndexedDB counter, then release those frames. Require the real data write to reject, no command acknowledgement or retirement, unchanged raw replica records and a retained accepted intent. Restore only the injected counter record, replace the backend module and SQL contract, kill Chromium and recover without resending the first reducer. |
| Reducer-result durability source control | Remove only the processed-watermark guard in the compiled reducer-result branch of `registerCall`. The same native counter fault must expose an acknowledgement or retirement before durable business data, at the named barrier assertion. The action-result branch is unchanged. |
| SDK process kill before/after local acceptance commit | Independent journal and replica connections witness the exact boundary. The parent kills its owned SDK child with SIGKILL. Fresh processes recover the same intent through read-only receipt lookup, without another reducer call, and verify SQLite integrity. |
| Process-recovery missing-row control | Delete the committed row at the recovered process retirement boundary. The actual row assertion must fail immediately; a timeout is insufficient. |
| Production-source recovery control | Bypass the previously-sent-intent receipt probe in a separate SDK bundle. The lost-reply schedule must detect a second reducer call for the already accepted intent. |
| Production-source barrier control | Compile one change to the reducer's watermark wait into a separate artifact. The same held-commit schedule must detect an early retirement attempt. No candidate source or built package is rewritten. |

## Violations found by composition

Receipt replay can finish before a reopened collection's snapshot or resumed
delta reaches the SDK. The first native run received the reducer result and
watermark before its catch-up delta. The SDK accepted that watermark too soon.
`handleReplicaWatermark` now records opening blockers. A deferred watermark is
released only after their verified ready certificates cover it, or a later
ordered watermark arrives after opening has completed. Scope/socket changes
clear the deferred state. Eight regressions fail on the previous source and
pass across memory, both IndexedDB adapters and SQLite. Older ready certificates
remain insufficient; same-socket auth renewal covers both preserved and reopened
subscriptions.

The next native run found a separate hydration error. Node SQLite truncated the
indexed collection signature at its embedded NUL, creating an old alias in RAM
alongside the correctly keyed current window. SQLite reads now JSON-quote the
indexed signature, compare it with the payload and restore the full map key.
Four real SQLite controls reproduce truncated hydration and inconsistent
index/payload acceptance. Android/Hermes CI then exposed a distinct input
boundary: its native TEXT binder stored only the prefix before the signature's
NUL. Window writes and indexed predicates now bind JSON-encoded parameters and
decode them within SQL. This preserves the existing raw-key format; already
truncated index/payload disagreements still refuse recovery. The historical
fixture seeds intact keys through the same SQL transport, independently checks
their raw hex bytes, and exercises all four production window writers, full and
bounded hydration, point lookup, membership deletion and peer-safe removal. A conservative hydrated global floor can also lag an
already current window; a metadata-only watermark now checkpoints that progress
without certifying a revision above the incoming watermark. Its two controls
include failed persistence and retry.

## Durable acceptance before replica completion

The first passing write-failure trace still resent a reducer whose successful
result had already reached the SDK. Four recovery checks reproduced that gap
before the fix. The SDK now invokes an acceptance callback from `registerCall`
when the result arrives. Direct and queued/batched calls capture the original
journal ID. `markAccepted` persists `committed` plus `awaitingReplica: true` in
one journal transition. Failure bookkeeping preserves that verdict. Completion
also awaits that persistence promise, so a watermark cannot release the caller
while the acceptance write remains unresolved.

A retained acceptance marker cannot be delivered by `readyIntents`, but its
causal dependencies remain barriers until reconciliation. Its prediction also
remains available to local execution until data catches up. Two existing local
reducer schedules caught missing dependency and prediction handling in the first
implementation. Both pass after the correction. Owner-rotation tests separately
hold the acceptance write and the later post-data settlement write.

On restart or reconnect, a capable runtime exposes the reserved tenant Query
`__gonvex.reducerReceipt`. It checks current membership and account-scoped receipt
history in one read-only database snapshot and returns only receipt status and
a current synchronization cursor. It neither claims a key nor invokes the
reducer, migrates a table, or exposes the cached business result. Its watermark
follows the same sent-replica ordering rule as a reducer result. The SDK retires
only after the ordered watermark and active replica data agree on the database
epoch. The session directive's epoch identifies module/schema definitions and
is distinct from that database clock. A real native run caught the initial
incorrect comparison between those two epochs.

Missing, malformed or unresolved receipt history keeps the accepted journal
entry. A runtime without the advertised recovery capability retains it too.
Legacy committed records without `awaitingReplica` retain their previous
meaning, written after the data barrier. This is forward recovery, not evidence
that downgrading to an older SDK understands the new pending-replica marker.

The latest complete client run passes 963 tests, including 87 persistence barrier
tests and 30 settlement ownership schedules. The focused SDK/ownership/IndexedDB
run passes 152 tests, the adapter suite passes 138, and the refreshed real
Chromium storage and upgrade suite includes future-format refusal and historical normalized-format interruption. Four journal-rollback
regressions failed before the pre-redelivery probe and pass after it. The latest
composed backend/SQLite coordinator passes all four schedules, the held commit
and eleven negative controls, including the renamed receipt namespace, concurrent-overlap certificate, stale-owner completion, missing data during fresh-process recovery
and a deliberately introduced business effect at the rollback witness. A source-linked five-state delivery
proof covers monotonicity after a committed marker exists. Seven additional
source-linked laws cover the production acceptance/pending-replica record changes
and reject four typed broken helpers; the native
acceptance mutant exercises the received-result bridge. This combination is not
a whole asynchronous protocol refinement proof. The pre-redelivery probe now covers failed acceptance persistence and fresh SDK
reopen on a capable runtime. Actual SDK process kills now exercise both sides of the SQLite acceptance
COMMIT. Independent readers witness committed versus inflight journal state and
missing replica data before SIGKILL. Fresh processes use the same files, recover
receipt status without an ordinary reducer call and pass integrity checks. Older runtimes without receipt lookup retain ordinary unknown-result
retry behavior, so this new recovery claim does not extend to that combination.

This includes Linux backend/Node SQLite schedules and a real Chromium
149.0.7827.55 SDK/IndexedDB contract migration. The browser upgrade uses the
production legacy IndexedDB upgrade store; it does not establish every browser
engine or historical artifact version. Native Expo/Hermes, iOS, physical-device
behavior, automatic same-instance reconnection, power-loss durability and broader
tenant/permission transitions remain outside this experiment, along with
the complete eight-guarantee contract. SDK process kill remains distinct from
operating-system or machine power loss. The database, filesystem, event loop and
fixture deadlines remain trusted assumptions. A whole-path source refinement
proof is still separate work. The [deferred-watermark guard proof](opening-watermarks.md)
now binds owner and blocker admission to production source; blocker capture and
asynchronous persistence remain outside it. The listed regressions provide the
separate execution evidence.


## Account-switch fixture boundary

The account-switch case uses two real accounts, sessions and active tenant
members. They share one tenant whose fixture row-visibility plan is public
within that tenant. Authorized delivery of A's business rows to B is therefore
expected. The owner oracle checks B's separate optimistic row, journal and
account-scoped receipt namespace, rather than calling authorized shared data a
leak. B's parked journal row is seeded through the storage interface before
rotation. The injected store opts into shared fresh reads for that external
write; there is one SDK and the write completes before rotation. This is not a
proof of arbitrary multi-process SQLite coordination.

The positive schedule and its compiled stale-completion mutant pass their
expected checks. Nine ordinary native receipt tests, the receipt-watermark test
and all eighteen snapshot-delivery tests also pass after adding B's fixture.
The separate tenant switch below now passes too. Broader permission and
asynchronous publication races remain open. Concurrent SDK delivery is exercised
separately below.

## Fixed-account tenant rotation and readiness certificates

The same [`sdk_owner_switch.mjs`](../../verification/backend-receipts/sdk_owner_switch.mjs)
now exercises tenant A->B->A while keeping the account and session token fixed.
The native fixture provisions B in a separate PostgreSQL database, authenticates
the existing session through its physical membership, and inserts a B-only row.
B cannot read A's receipt or business row. Returning to A must hide B's business
row and private sentinel, then retire A's retained acceptance without resending.
The native postconditions require twelve business effects and twelve receipts
in A (eleven for the original account, one for the other account), and one
business effect and receipt from the SDK in B.

The first run reached A's real receipt, watermark and ready frames on return,
but could not retire its acceptance. `resetReplicaScopeState` retained the B
subscription's `readyCursor`. A watermark captured B's epoch as its required
readiness epoch, which A's subsequent certificate could never satisfy. Clearing
that cached certificate at scope reset fixes the stall. Four cases in
[`sdk-persistence-barrier.test.ts`](../../packages/client/src/sdk-persistence-barrier.test.ts)
fail on the previous source and pass for memory, legacy IndexedDB, engine
IndexedDB and SQLite. They retain A's acceptance across B's independent epoch,
reject an insufficient A ready cursor and retire only after A's actual data and
sufficient ready cursor arrive. The earlier ownership helper laws do not prove
this readiness lifecycle; these are adapter and composed execution regressions.

The tenant source control runs tenant rotation first with the pending-call
identity guard removed. It must fail the replacement-owner prediction assertion
while authenticated in B. B's clock is advanced by one hundred ordinary committed
updates before rotation, so a larger A revision cannot accidentally postpone the
bad acknowledgement and mask the missing guard. A separate precondition checks
that B has already reached A's numeric revision. Both the low-B adapter schedule
and the high-B real-backend schedule pass on the fixed SDK. The coordinator now
requires this tenth negative control as well as the original account control.

## Concurrent delivery and receipt initialization

[`sdk_concurrent.mjs`](../../verification/backend-receipts/sdk_concurrent.mjs)
copies one persisted intent into two independent journals before constructing
the second SDK. Both use the same authenticated account, tenant, key and
arguments. This represents duplicate delivery of one intent; it does not claim
coordination between SDKs sharing a live journal.

The Rust coordinator holds an advisory gate inside a trigger on the two fixture
business rows. It requires `pg_blocking_pids` to show the first business write
waiting on that gate and a distinct backend's receipt claim waiting on the
first transaction. The rollback schedule cancels only that owned first query.
Before releasing the gate, it observes the second claimant waiting directly on
the gate, establishing takeover after rollback. Both SDKs finish with durable
row, membership and cursor witnesses, empty journals and one ordinary reducer
call each. The cancelled caller reconciles the winner through receipt lookup.

The first runs could not reach that claim graph. Every reducer repeated receipt
table/index initialization, whose DDL waited behind the first active receipt
transaction. `ControlPlane::ensure_reducer_idempotency` now checks the current
tenant catalog for the table and its valid, ready maintenance index before
taking the existing initialization path. The check uses no process cache, so a
schema reset is observed on the next call. Missing or incomplete initialization
still uses the DDL path. These concurrency schedules begin with initialized
receipt storage. [Separate native regressions](reducer-receipts.md#receipt-schema-initialization-and-legacy-history)
cover legacy history, ambiguous namespaces, index repair, schema reset and cold
initializers; those checks are not evidence of live shared-journal coordination.

The complete coordinator passes both outcomes and all eleven negative controls. The refreshed coordinator also checks diagnostics
after storage closure and captures an intentionally failed rollback witness
without another IndexedDB read.
A fresh run with the same native artifact also passes nine ordinary receipt
tests, one receipt-watermark test and eighteen snapshot-delivery tests with
none ignored; its missing-fixture control fails as required.
The overlap control checks evidence integrity; it is not a production-source
mutant. Cancellation here exercises a PostgreSQL statement and transaction,
rather than every runtime cancellation or resource-retirement path.

## Retained browser-upgrade timeout

One earlier overlap-control round timed out during browser contract recovery
before reaching its intended assertion. A read-only IndexedDB inspection of a
copied Chromium profile finds the renamed intent still pending with its original
key and receipt namespace. Confirmed older entities remain, while query windows,
cursor and the target row are absent. The contract metadata records version 2.
The original failed profile's contents were hashed before and after inspection
and remained unchanged.

This narrows the persisted boundary but does not reconstruct the missing network
trace or prove whether the backend committed. Later full coordinators pass.
Failure diagnostics now retain received frames, connection state, cursor,
journal, an explicitly in-memory replica snapshot, Web Lock queues, recovery flags
and browser errors for the next occurrence. Reporting uses the last completed
storage read rather than requesting more data from a potentially blocked storage
lane. The real backend fixture also requires diagnostics to succeed after its
storage has closed; retained browser profiles remain the durable evidence. The
intermittent recovery failure supplied the trace used by the regression below.

## Snapshot preparation and inline receipt lookup

The captured timeout has two replica opens followed by a receipt lookup, with
no server frames after authentication. Recovery has completed locally and the
only held browser lock is the delivery lane. The server's preparation queue
previously stored ordinary futures polled by its socket loop. Its query branch
awaits receipt lookup inline. Two preparations can therefore hold both tenant
pool connections while that branch waits for one of those same connections.
The snapshot futures cannot commit until the socket loop polls them again.

`receipt_lookup_progresses_while_snapshot_preparations_own_pool_connections`
reproduces this on real PostgreSQL. It holds the clock table, polls two actual
snapshot preparations until `pg_blocking_pids` witnesses both pool owners,
releases the table gate and invokes receipt lookup without polling preparation
completion. The previous source fails the named starvation assertion. The
changed source passes, alongside fifteen receipt tests, eight preparation-queue
tests, one receipt-watermark test and eighteen snapshot-delivery tests.

[`ReplicaOpenQueue`](../../rust/crates/runtime/src/replica_open_queue.rs) now
schedules admitted preparation work independently on Tokio. Each active slot
owns its task handle and aborts it on drop. Existing entry tickets remain the
only route to publishing results, so a completed task cannot publish after its
entry is cleared or replaced. Waiting work is still started only within the
existing count, byte and concurrency admission rules. The new cancellation
schedule observes cleanup after clear, remove and queue drop without polling
completion again. This relies on Tokio cancellation/scheduling and PostgreSQL
transaction semantics; it is not a proof of every database-work budget path.

CI selects the starvation and unpolled-cancellation regressions explicitly and
rejects zero-test or ignored execution. The rebuilt server passes the full real
SDK/browser coordinator with all ten expected negative assertions, including
the owner-control phase that previously stalled before its intended assertion.
The deterministic PostgreSQL regression establishes the scheduling defect; one
later successful browser run alone would not establish that correspondence.


## SDK retirement after PostgreSQL process crash

[`check_sdk_postgres_crash.py`](../../verification/backend-receipts/check_sdk_postgres_crash.py)
owns a disposable PostgreSQL container and runs the authenticated SDK composition
against the existing native runtime. In the lost-reply schedule, the SDK drops
the real committed reducer result, closes the old socket and retains its original
intent in SQLite. A marker pauses recovery before opening fresh SDK handles.
An independent PostgreSQL connection must observe exactly one committed business
row and one completed receipt for that key before the coordinator kills the
owned database process with SIGKILL.

The container must exit with status 137. It restarts with the same data and port,
and a second SQL witness must match the first. The runtime process and its pools
remain alive throughout. Fresh SDK and SQLite handles then authenticate through
those pools, discover the retained receipt, persist replica data and retire the
original intent. Exactly one ordinary reducer call is allowed across the whole
schedule; the recovered durable cursor must cover the committed revision.
The remaining composition schedules and Rust SQL postconditions also complete.

The coordinator records the native artifact hashes, all built JavaScript and
TypeScript package inputs, the selected runtime source, durable PostgreSQL
settings, process identity and both SQL witnesses. It refuses changed inputs
during execution. The required CI workflow runs this schedule and a control
that deliberately removes the committed business row after database recovery;
the control must fail the named business/receipt witness assertion. That control
tests evidence integrity and does not simulate PostgreSQL losing committed data.

This establishes a committed lost-reply recovery schedule through SQLx pool
recovery without explicit pool reset or runtime restart. The SDK object itself
is reopened. Automatic reconnection of the same SDK object, an uncommitted
PostgreSQL crash during its business call, native app drivers and machine
power-loss durability remain separate obligations.
