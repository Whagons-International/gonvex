# Reducer receipts and synchronization barriers

> Storage moved from PostgreSQL to SQLite on 2026-11-02 (see
> [SQLite storage](../sqlite-storage.md)). Evidence below that names PostgreSQL
> was established on the earlier backend and must be re-run on SQLite before it
> is relied on for the current runtime.

Authenticated reducer retries use a receipt in the routed tenant database. Its
key is the account subject and logical intent key. The receipt claim, result and
transactional business writes share one PostgreSQL transaction. A successful
replay returns the cached result without invoking the reducer again.

The replay synchronization revision is the current committed database clock.
It can be later than the original mutation's revision. It is a sufficient
barrier for reconciliation, rather than an exact historical commit timestamp.
Changing the transport request ID or pruning old change-feed rows must not
remove this barrier. Failure to read the clock fails the replay response.

## Implementation correspondence

| Protocol step | Production operation |
| --- | --- |
| Acquire an exclusive claim | `TenantTransaction::claim_reducer`, unique `(subject, idempotency_key)`, `INSERT ON CONFLICT DO NOTHING` |
| Prepare a result | `TenantTransaction::store_reducer_result` in the business transaction |
| Commit or roll back | Host transaction disposition and `TenantTransaction::commit` or `rollback` |
| Publish committed changes | Deferred PostgreSQL finalizer increments `_gonvex_sync_clock` and assigns feed revisions in the same transaction |
| Replay | `execute_tenant_reducer_with_access` reads the cached result after losing the claim, then reads the committed clock |
| Retain a sufficient barrier | Optional execution revision tracker observes the replay barrier too |

The committed receipt cannot be read before its transaction commits. The
deferred finalizer updates the singleton clock before that commit, holding its
row lock until completion. A later read of this same database's monotonic clock
therefore cannot predate the receipt's transactional effects. No change-feed
lookup by incoming transport ID is needed.

## Evidence and limits

`verification/tla/ReducerReceipts.tla` explores one key in one namespace, two
transport identities, rollback, reply loss/retry, unrelated commits and pruning.
The checker exhausts declared clock bounds 2 and 3. It checks at most one
business execution, committed-receipt visibility, cached results and reply
barriers. Separate negative controls must produce named counterexamples for a
new transport ID, pruning, premature receipt visibility and nonexclusive claims.
These are finite protocol checks. They do not prove the Rust or SQL source.

The [composed SDK fixture](composed-sdk.md#concurrent-delivery-and-receipt-initialization)
also delivers one immutable intent from two authenticated SDKs with independent
SQLite journals. It witnesses the actual PostgreSQL claim wait graph for commit
and cancellation/rollback takeover, then checks both durable replica retirements.
Receipt initialization uses a catalog check to avoid repeating index DDL on the
initialized hot path. The composed concurrency cases do not establish
coordination through a shared live journal. Cold initialization has separate
native regressions below.

`rust/crates/runtime/src/tests/receipt_guarantee_tests.rs` exercises the real
module host, runtime transaction path and PostgreSQL. It covers changed
transport IDs, actual feed pruning, unreadable clock, rollback, no-op replay,
path mismatch, distinct account subjects, and competing claims with both owner
commit and rollback. The competing tests require `pg_blocking_pids` to witness
the loser waiting on the owner before releasing it. CI sets
`GONVEX_RECEIPT_TEST_REQUIRE_NATIVE=1`, requires an executable host, and rejects a
zero-test run. Local tests without the required fixture can skip these tests.

The [native baseline run](https://github.com/Whagons-International/gonvex/actions/runs/40792259542)
at `ffbb248516026bc59dffafb7e4d2f7540d209115` compiled and executed all four
original tests. Rollback passed. Both changed-ID and pruned-feed replay returned
`committed_revision=None` after original revision 1 and later revision 2. The
unreadable-clock replay also incorrectly succeeded. Fixed-source validation
in the [follow-up native run](https://github.com/Whagons-International/gonvex/actions/runs/40792591655)
at `bdf4697b1143f246017c9eac8f7d80a13ca45860` passed all eight tests with no
ignored tests. This includes the original regressions and both witnessed
competing-claim outcomes. The session fixture is synthetic and trusted; it does
not test authentication admission or tenant routing.

The protocol assumes correct tenant routing, one intact database epoch,
monotonic runtime metadata, PostgreSQL's transactional behavior and a durable
deployment configuration. Reuse a key only for the same logical intent and
arguments. Path mismatch is checked; argument equality is not checked. Effects
outside the tenant transaction, anonymous calls and Actions have no such
deduplication guarantee. The current runtime has no receipt TTL or automatic
receipt-deletion task. Its hourly maintenance deletes old change-feed rows and
transaction metadata, while retaining the receipt namespace. The created-at
index does not expire receipts. This source review does not promise permanent
retention across administrative deletion, database restore or a future policy
change. Those operations need an explicit recovery contract before the same
retry guarantee can span them.

This evidence does not establish liveness, power-loss durability, tenant-routing
authorization, native app filesystem behavior or correctness of snapshot
delivery and watermark ordering. Those require separate checks in the guarantee
register. Record native baseline and fixed-source results before declaring this
change verified.

## Receipt schema initialization and legacy history

`ControlPlane::ensure_reducer_idempotency` inspects the routed connection's
receipt tables and maintenance index. Coexisting current and legacy tables
require explicit recovery before a new reducer can execute, matching the
read-only receipt lookup policy. The initialized path uses catalog reads and
does not repeat table or index DDL.

Cold initialization takes a transaction-owned advisory lock, then rechecks the
catalog. A waiter can therefore observe completed initialization without running
DDL behind an already active business transaction. Legacy rename, table creation
and index creation share that transaction. Legacy detection uses the same search
path as the subsequent rename and receipt claim; it no longer assumes `public`.
Dropping the transaction releases the advisory lock through PostgreSQL rollback.
This assumes cooperating framework initializers and excludes concurrent external
DDL that changes the receipt namespace during a business invocation.

Five native tests exercise legacy replay through a new transport ID, refusal of
ambiguous histories before business execution, missing-index repair plus schema
reset on the same control object, and two cold initializers witnessed waiting on
the schema lock before duplicate delivery, and task cancellation during that
lock wait. The cancellation case checks subsequent lock acquisition, exact query
admission capacity and a successful reducer after recovery. The required runtime workflow selects
these cases explicitly and rejects missing, failed or ignored execution.

The before-fix native run failed both history controls at their intended
assertions. Legacy replay attempted the business insert again and encountered
its original primary key. Ambiguous history allowed a new business row to
commit. The index/reset control passed before the fix, retaining a positive
control for the catalog-based initialized path. This schema evidence concerns
the supported receipt table rename, not every historical application migration.

The previous fixed native artifact passes the four schema initialization cases within thirteen
receipt tests, with no ignored execution. Its real SDK composition also passes
the account-switch, process-kill, browser-upgrade and competing-claim schedules
and all nine negative controls. Both PostgreSQL process-crash boundaries also
pass with that rebuilt artifact and durable database settings enabled. One
earlier final-control attempt timed out in
browser upgrade recovery before reaching its intended assertion. That attempt
is excluded from passing evidence and retained for investigation. Additional
failure-state capture was added without changing a deadline or safety assertion.

Ambiguous history is classified as infrastructure failure by
[`database_error_class`](../../rust/crates/runtime/src/error_class.rs).
The reducer does not run, and the SDK can retain the intent for recovery rather
than report a permanent business rejection. The native ambiguity test checks
both this classification and unchanged business and receipt counts.

## Runtime process-crash experiment

With SQLite storage the tenant database lives inside the runtime process, so the
crash boundary is the runtime process itself. The ignored test
`receipt_recovery_after_process_crash` is driven explicitly by
`verification/backend-receipts/check_restart.py`. Ordinary test runs cannot
establish this experiment. The required runtime workflow builds the exact test
artifact and runs the coordinator, which rejects a missing coordinator,
zero-test output and ignored execution.

For one schedule, the fixture commits a reducer effect and receipt, discards its
reply and signals the boundary with the path of its tenant database file. For
the other, a tenant transaction holds both an uncommitted effect and receipt.
The driver SIGKILLs the fixture's process group, then starts a fresh fixture
process (`GONVEX_RECEIPT_CRASH_PHASE=recover`) on the same owned data directory
(`GONVEX_TEST_DATA_DIR`, `synchronous=full`). SQLite recovers the WAL on open;
the fixture retries the same intent with a new transport ID. Both schedules
require exactly one business effect and receipt for the committed case, none
for the uncommitted case, the expected payload, and a repeated replay with a
sufficient revision barrier.

An evidence-integrity control deletes the committed receipt rows from the
database file after the kill. Recovery must then fail the explicit business
count assertion. This control tests the evidence check; it is not a production
source mutant.

The earlier PostgreSQL version of this experiment (killing an owned database
container while the runtime kept its pools) passed before the storage move and
is retained in Git history. The SQLite schedule must be re-established once the
Rust fixture implements the phase contract documented in `check_restart.py`.
These checks trust SQLite and the filesystem. They do not test machine power
loss, HTTP/WebSocket tenant admission or SDK replica persistence.

## Read-only reconciliation of a known acceptance

The framework-reserved tenant Query `__gonvex.reducerReceipt` accepts the original
`idempotencyKey` and `receiptPath`. `Runtime::reducer_receipt_status` requires an
authenticated account and verifies the physical tenant member is still active.
Membership, receipt status and synchronization cursor come from one repeatable
read, read-only transaction. The configured execution timeout bounds statements.
A completed receipt returns `found: true` and the current database cursor.
Missing or incomplete receipts return `found: false`. Cached business results
are never returned, including after a permission change.

The lookup performs no DDL, claim or application execution. It can read a legacy
receipt table without renaming it. Coexisting current and legacy tables require
explicit recovery. Invalid or unrepresentable cursors fail closed. The native
lookup regression verifies missing history creates no table, account isolation,
path mismatch, legacy reads, ambiguous history refusal and revoked membership,
then checks that effects and receipts were not appended. All nine ordinary
receipt tests and the dedicated receipt-watermark ordering test pass locally.

The runtime advertises `reducerReceiptLookup: 1`. Its query result may synthesize
a terminal watermark only after the connection's subscribed replica work has
been sent through that cursor revision. Ordinary Queries and missing receipts
cannot synthesize this watermark. SDK recovery and the composed real storage
experiment are described in [composed-sdk.md](composed-sdk.md).


## Composed SDK recovery through existing pools (retired)

This PostgreSQL-only schedule (database SIGKILL while runtime pools survive) was
removed with the move to SQLite; see
[composed-sdk.md](composed-sdk.md#sdk-retirement-after-postgresql-process-crash-retired).
