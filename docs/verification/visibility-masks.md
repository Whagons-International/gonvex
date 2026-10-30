# VIS-1 visibility feed budgets

Status: intended guarantee, not formally established.

The runtime intends row admission to be pure, ordinary data writes to issue no
visibility SQL or full collection rebuilds, and permission changes to rebuild
only the affected member's shared principal. Grant contributions must be
refcounted from feed rows; inherited parent ACL values must be tenant-shared.

Evidence currently includes the evaluator import boundary check, SDK/CLI shape
and inheritance rejection tests, runtime metrics, a real PostgreSQL collection
fan-out fixture, and legacy-admission log tests. The parent implementation owner
runs the integrated fan-out fixture. Its measured results must be attached before
claiming the statement budgets. These tests are not a Verus proof.

## Production proof boundary still needed

The production decision is embedded in async `Runtime::apply_mask_feed` in
`rust/crates/runtime/src/replica_mask.rs`. It combines principal routing, grant
classification, inheritance, window handling and targeted data fetches. There
is no standalone production scalar decision body that the existing Verus
extractors can verify without abstracting away those behaviors.

Extracting a separately written Skip/Applied/Rebuild boolean model would prove
that model, not this feed implementation. Before proving VIS-1, define a pure
production decision boundary and call it from the real feed path. Pin the
source helper and adapters with hashes, copy the production body into the
harness, and state assumptions about row-to-member routing, transaction order,
complete feed values, token refcounts, and shared-principal synchronization.
The proof must distinguish principal-resolution SQL from targeted entity data
fetches. It must not weaken the ordinary-task zero-statement budget.

Required laws include: a non-permission source change cannot request a principal
rebuild; a permission row excluding the member cannot request that member's
rebuild; grant revocation preserves duplicate contributions without SQL; and
source-table changes cannot force a budgeted collection snapshot. The principal
routing predicate and its LEFT JOIN reachable-key construction need their own
source correspondence, not an assumed boolean named `member_affected`.

Negative controls must verify as well-typed programs but fail their contracts:
remove the member anchor check, let an unrelated grant request a rebuild, let a
source write force a budgeted snapshot, and remove the grant refcount guard.
Compiler errors do not count as failed-proof controls. Receipts must name the
Verus version, source/extractor/harness hashes, proof boundaries and all controls,
following call-budgets.md and verus.md. No Verus result is claimed here.

## Measured legacy and v2 status

Both real-collection tests passed on 1b503997 plus the legacy-baseline fixture
change, with 200 members and 201 connections. The baseline uses the current
runtime's legacy compatibility path with task-ID sets selected FROM tasks and
child sets joining tasks, not the historical incident binary. Both modes use
the same fixture, collections and committed writes. Counters reset per scenario
and before each task operation.

| Scenario | Legacy visibility SQL / full snapshots | V2 visibility SQL / full snapshots / principal rebuilds |
|---|---|---|
| Task insert | 402 / 402 | 0 / 0 / 0 |
| Task update | 201 / 201 | 0 / 0 / 0 |
| Task delete | 402 / 402 | 0 / 0 / 0 |
| Team grant | 402 / 402, every connection | 8 / 0 / 0, m1-m3 only |
| m1 membership | 4 / 4, m1's two connections | 4 / 4 / 4, m1's two connections |
| Parent grant | 402 / 402, unrelated connections included | 2 / 0 / 0, m0 only |

The title-only legacy update rebuilds tasks but column relevance lets notes
skip, explaining the 201 count. Legacy principal_rebuilds is zero because that
counter classifies the v2 path; legacy rebuilds appear as collection_snapshots.
The v2 row-check counts are 603 per task write and 402 for the remaining
scenarios. All live-query rerun counters are zero in this collection fixture.

The two-test body took 12.97 seconds on this run. This is registry cost evidence,
not a test threshold or a latency comparison. The counters are the runtime's
instrumented operations, not an inferred count from timing. The test asserts
that the legacy source shape fans out and that the v2 path stays within its
statement budgets and named-member scope. SQLx tracing checks child-fetch scope.

Cross-connection/subscription principal sharing is an explicitly accepted
follow-up. The four membership-change rebuilds must not be described as one
shared rebuild or as establishing that portion of V3. The guarantee above remains
intended and incomplete. Pure in-memory relevance checks are excluded from the
fixture's definition of expensive per-connection work.
