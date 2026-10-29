# Reducer receipts and synchronization barriers

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
deduplication guarantee. Receipt retention is not promised as permanent.

This evidence does not establish liveness, power-loss durability, tenant-routing
authorization, native app filesystem behavior or correctness of snapshot
delivery and watermark ordering. Those require separate checks in the guarantee
register. Record native baseline and fixed-source results before declaring this
change verified.
