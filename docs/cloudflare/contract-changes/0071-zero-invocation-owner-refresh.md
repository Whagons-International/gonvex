# 0071: One owner refresh after a proven zero-invocation rejection

Date: 2026-11-12. Status: bounded prototype permitted; implementation, lifecycle and availability remain UNMET.
Extends 0069 without changing client frames, principals, receipts or the original restart scenario.

## Permitted continuation

A tenant restart invalidates old authority. Preserve the unchanged `replica-tenant-restart` first writer
call through at most one continuation that can prove the offered call has **not begun**. The server-only
`dispatchOwned` interface must distinguish a versioned tagged frames outcome from a versioned
`not-invoked` outcome whose only permitted refresh reason is `tenant-instance-changed`. Bind the latter to
the exact offered authority/owner claim, a fresh server-generated external-event nonce and captured server
view. An application result always remains inside the frames branch; its content cannot impersonate this
status. A current-instance hint is not authority.

Emit this status before socket observation, fence creation, admission of deferred work, receipt claiming,
module/tool/Reducer/Action invocation or handoff, and any invocation-linked mutation. A thrown exception,
timeout, transport failure, callee abort or absent legacy method cannot certify zero invocation. Do not infer
this status from an error string, ordinary error frame, idempotency lookup or missing result.

For this same fresh external event, the gateway validates the exact outcome/nonce/offer and checks that the
physical Session, tenant, epoch and auth scope still match. It may then perform explicit `ownerAdmission`,
with the actual non-serial holder confirmation required by 0069, recheck scope, and make the **first actual
invocation** under the new token. Keep the same frame, command/idempotency key, Action generation and
provenance. Do not restamp old rows, pending refs or auth authority.

A second stale status ends the refresh. Any ambiguous invocation/transport outcome also ends it honestly:
no third attempt, ordinary legacy dispatch fallback or automatic replay of an already-started/UNKNOWN
Reducer or Action. Existing client retry/idempotency rules remain unchanged; this is not a general RPC retry
facility. Internal admission/bootstrap creates no 0064 retry credit. The real external event contributes
at most its existing single credit when actually admitted.

## Bounds and evidence

Singleflight refresh per owner/scope and bound its state/work. No per-frame confirmation RPC, per-frame KV
counter or shared fleet allocator is introduced. All 0069 nonce/current-state checks, callback reentrancy,
retire-before-fence-deletion and UNKNOWN rules continue to apply. A zero-invocation outcome describes only
this offered attempt; it proves nothing about historical calls, terminal drain, lost acknowledgments or
stranded durable OWNER cleanup.

Require the unchanged raw tenant-restart first writer to succeed with exactly one invocation. Include real
native-RPC tests for absent methods, unsupported versions, foreign/malformed nonce/authority, auth/scope
change during admission, a second restart and application-result impersonation. Hold an already-invoked
call and lose its reply: it must not be retried by this mechanism. Prove unchanged SDK receipt/revision
liveness and event-credit bounds. Review a new immutable implementation independently.

The tested R1/0070 checkpoint is independent of this follow-up. No CPU guard, raw deploy/close, full
local/private-platform matrix, K22/X01 capacity or actual-duration gate is waived.
