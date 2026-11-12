# 0073: Explicit owner admission during ordered holder rebuild

Date: 2026-11-12. Status: bounded implementation permitted under 0056/0061/0069; acceptance remains UNMET.
This clarifies the internal recovery path. It does not expand 0071's fresh-client continuation.

## Quiet readers do not require a new client call to rebuild

0061 already requires the actual holder to perform an authorized instance reset/live-query rebuild while
normal host delivery is suppressed. After tenant restart its cached owner authority may be old. A genuine
ordered holder rebuild may obtain explicit confirmed owner admission for its **first** empty registration
and retained live-query rebuild. It need not wait for fresh client traffic merely to perform this existing
recovery operation. A stale-token rejection alone does not authorize recovery or create its event context.

Create a distinct server-only rebuild context inside the actual `Session.bootstrapReplica` or
`Session.recoverReplica` serialized event, after validating the real scope/recovery marker, empty bootstrap
shape where applicable, tenant, physical Session and captured holder epoch. A client frame, a
`queryRecovery` boolean, sender instance hint, digest, echoed status or old token cannot establish this
context. Returned challenge epochs still authorize only empty work, never retained data.

Capture the current server view/owner claim. Use 0069's explicit bounded `ownerAdmission` and real
**non-serial** current-holder confirmation; enforce pending nonce/current-state checks and scope checks
before and after awaits. Then use the same real handlers for empty `replica.openMany` registration and
actual retained `query.subscribe`/resume requests, preserving current Member revalidation. No ordinary
legacy socket dispatch bypass is allowed. Do not replay a client Reducer, Action or `query.call`, restamp
old rows/ref epochs, or invent a new initial generation. Existing 0064/0070 terminal facts remain the sole
authority for their respective correlation cleanup; owner admission is not completion.

This internal path creates no fresh client event, `externalEvent` credit or 0064 recovery retry credit.
Retain old owner locators as UNKNOWN where required; new admission does not prove old calls drained,
retirement, durable job completion or stranded OWNER cleanup.

## One bounded acquisition, honest failure

Coalesce admission by physical owner/scope/barrier, with bounded state and at most one acquisition attempt
for a genuine barrier. Queued duplicates, delayed callbacks or stale outcomes must not start a refresh
loop. If the new token is stale again, confirmation changes scope, admission is ambiguous or transport
fails, stop honestly. No fabricated scope/recovery ACK, progress, repeated automatic refresh or legacy
fallback follows. A later genuine explicit event uses the existing recovery rules; no timer manufactures
one. Do not repeatedly invoke a partially completed rebuild within the same barrier.

Normal engine delivery stays suppressed until the actual authorized rebuild, explicit scope/recovery ACK
and ordered catch-up complete. No ACK is sent merely because admission or an RPC enqueue succeeded.
Normal 0064 full completion validation and 0070 recovery-only exact-subset processing stay separate. No
outlet/X01 placement rewrite, second delivery channel or per-frame confirmation/KV counter is introduced.

## Required proof

Use real native RPC/Host confirmation to prove reentrancy without deadlocking behind the waiting Session.
Test quiet reader tenant restart, live-query/reset/resume, concurrent commits, scope change during
admission, duplicate barriers, delayed confirmation, second restart, missing methods and lost/ambiguous
replies. Show zero mutator/Action replay, zero new client retry credit and no old locator deletion from
admission alone. Preserve unchanged SDK receipt/revision liveness and record bounded UNKNOWN/churn and
aggregate memory costs. Distinguish state-machine fixtures, native RPC and actual SDK execution.

The reported nine uncommitted owner state-machine cases/typecheck are prototype evidence only. N6,
terminal owner/UNKNOWN lifetime, stranded durable OWNER, private CPU/deploy, full local/dev comparison,
enabled files/eight-package domains, X01 capacity/security and actual-duration gates remain open. Review
the eventual immutable implementation independently; no acceptance approval follows from this ruling.
