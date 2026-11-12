# 0054: X01 placement API and bounded reconnect/drain

Date: 2026-11-12. Status: decided (lead under Gabriel's explicit runtime-autoscaling authorization).
Continues 0053; every existing gate remains mandatory. This is implementation coordination, not independent review.

## Placement interface

Accept the proposed additive GatewayPlacement.acquire(connectionId): Promise<GatewayAssignment> and
release(assignment): Promise<void>, with assignment {v:1,cell,gateway,epoch,token,expiresAt}. Cell count 32 is an
initial bounded sharding choice, not proof of allocator throughput or a safe fleet/user count. Measure allocator
skew, auth/limiter pressure and per-cell/fleet envelopes; 32 controllers do not remove shared-service bottlenecks.
The release contract must be idempotent and token/connection/owner-bound. Epoch and expiry semantics must be
explicit: an admission lease expires; an already accepted socket does not lose ownership because the lease
expired or a later pool epoch was issued. Persist/recover held ownership through hibernation/controller restart.
Outstanding valid reservations must survive topology publication or fail with bounded cleanup/retry, never
silently admit onto a retired object. Stale/forged reports cannot change current membership/counters.

An optional GatewayBindings.placement(env) runtime hook is compatible with K22's GatewayObjectBase subclass.
Its runtime contract also needs a consume/validate and bounded report path; acquire/release alone is not the
complete controller protocol. Router-generated connectionId and assignment must bind the reservation to the
actual gateway/cell/epoch and at-most-one accepted connection. Gateway validation must check authoritative
controller state, not just untrusted JSON headers. Overwrite/strip client-supplied internal assignment headers;
reject mismatched, forged, replayed or expired admission. Do not log tokens. Preserve publicGuard/telemetry and
K22 actionFinished; stable holders continue to receive the single ordered ReplicaDelivery channel.

Acquire-before-K16 can preserve K16's actual-holder audit semantics, but it adds pre-auth controller work.
Keep method/upgrade/extension checks first, bounded expiring reservations and RPC timeouts. Release BOTH
placement and K16 slots on every refused/failed/abandoned upgrade (including limiter 429/503, consume failure,
non-101 and forwarding exceptions); test cancellation/lost-response/TTL recovery. It cannot lift K16's caps
or turn traffic refused by K16 into unbounded reservation storage. Prove the pre-auth/NAT-storm envelope.
No static fallback on controller outage is accepted: new admission is 503, existing accepted sockets and
ordered commits keep working. Missing optional placement is test/backward-compatibility only, not evidence
that the assembled autoscaling runtime passed while using static placement.

## Approve bounded 1012 rebalancing, with safety conditions

Persistent saturation of already-connected hot holders is not an acceptable substitute for the required
hot-tenant elasticity. Authorize deliberate close 1012, reason `gateway placement rebalance`, as a resumable
placement adaptation. The SDK source reconnects on every close (packages/client/src/index.ts:2170), reauthenticates,
resubscribes and drains its durable outbox on reopening. It also rejects pending tenant calls as potentially
applied, and NEVER blindly replays non-idempotent calls. That behavior is preserved, not waived by this ruling.
The platform must be tested accepting/emitting 1012 locally and on the exact remote deployment.

- Add and health-check replacement capacity before scheduling a bounded reconnect cohort; exclude the draining
  holder from new admissions. Normal scale-in can wait for natural disconnect first, then use the same bounded
  mechanism after a finite grace. Jitter/stagger server cohorts; SDK backoff alone is not sufficient herd control.
- A selected socket reaches a serialized session boundary: finish already accepted queued calls, pending direct
  replies and outbound ordered batch work before closing. Do not close through an unresolved unkeyed call or
  K22 direct Workflow Action reply just to meet a scale-down timer. Defer that socket and expose the reason/count.
  Do not synthesize success or new automatic replays. Any optimization for durable keyed work needs receipt and
  correlation proof across the new holder, coordinated with K22, before it can replace this conservative rule.
- Flush ordered frames before the close frame; keep commit/live-query/watermark order. Resumption uses the
  existing durable cursor/digest/receipt protocol. No in-place socket transfer, new unordered delivery path,
  erased subscriptions, invalidated auth or fabricated receipt/watermark. Writes remain tenant-serialized.
- Bound cohort size, concurrent migrations, reconnect rate, retries, per-socket deferrals and control/queue bytes
  within the explicit measured cost/resource envelope. Do not bounce the same connection repeatedly. If replacement
  or Control/auth/limiter health is poor, pause rebalancing and report blocked drain instead of a reconnect storm.
- Durable routing epoch/drain ownership survive restarts; duplicate commands are idempotent, stale epochs ignored.
  Existing connections stay with their original holder until close. Never delete accepted ownership/counters or
  retire a gateway while held sockets/actions still exist. Once empty, remove it safely and stop idle wakeups.

Tests: active/idle/unkeyed-call/keyed-receipt/direct-Workflow sockets, query+replica ordering during rebalancing,
SDK/thin-client resume, lost reply, owner/tenant change and revocation, drain/controller restart/deploy, failed
admission/consume/release, quiet neighbors, growth/shrink and repeated thresholds. V03 26-tenant/1k-user gate
must pass while scale and bounded drain occur; record pending deferrals, routing/instance timeline, correctness,
latency/errors/backlog and costs. Long-lived pinned Actions/drain delays are explicit limitations, not silent
completion. Fresh independent review still follows the artifact through a new delegated review.

## K22/X01 ownership

K22 retains session/session.ts and delivery/engine.ts changes, plus assembly/gateway.ts/actionFinished and
root serving bindings. X01 starts with new kernel/gateway code and placement/gateway.ts/router.ts; minimal
host/ports/rpc/outlet hooks only after concrete coordination. K22 supplies a narrow serialized-drain eligibility
hook for its pending direct-Action state, or lead integrates that hook; X01 must not overwrite K22 session code.
Agree the hook's types before overlapping changes. Lead owns the merge and contract table update.
