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
