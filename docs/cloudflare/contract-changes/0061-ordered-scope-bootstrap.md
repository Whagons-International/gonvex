# 0061: Ordered scope bootstrap after tenant restart

Date: 2026-11-12. Status: decided (lead). Additive internal RPC negotiation; SDK/wire authority is unchanged.

K22 must preserve the unchanged K10 `replica-tenant-restart` reset/resume test without applying unscoped
frames to a new Member scope. 0056's epoch fence remains mandatory. Missing epoch never means current epoch.

The approved server-only extension is:

- `ReplicaDelivery.scopeFence?: 1` advertises the sender understands the negotiation.
- `ReplicaReceipt.scopeRequired?: Record<connectionId, subscriptionEpoch>` challenges unscoped work,
  using only the holder's authoritative current epochs for actual applicable tenant sockets.
- `ReplicaReceipt.scopeAck?: Record<connectionId, subscriptionEpoch>` acknowledges an applied empty
  bootstrap, separately from the unchanged literal `recoveryAck: 1` whole-holder recovery acknowledgment.

Before any instance, data or watermark application, the holder rejects/challenges unscoped work from an
aware sender. The challenge receipt applies nothing and advances no accepted revision or delivered callback.
The sender may use returned epochs **only** for empty `SocketWork` bootstrap on the same ordered outlet.
It must never stamp retained rows, query replies or opens with those returned epochs.

The holder awaits `Session.bootstrapReplica(tenantId, payload, emptyWork)` on the existing socket serial
queue. Capture tenant/epoch and validate them before and after actual authorized instance reset/live-query
rebuild. Changed scope, failed query or missing hook receives no `scopeAck`. An ordinary old-holder receipt
does not prove bootstrap support. Unknown old senders without `scopeFence` retain the explicit unsupported
1012 path; no digest, client field or absent epoch grants authorization.

Normal per-host delivery stays suppressed through challenge, empty bootstrap, explicit acknowledgment and
serialized current-authorized catch-up. Drop obsolete server replica/open state consistently. Retain deferred
normal live-query semantic requests and execute them after the barrier with current scope/unsubscribe checks;
do not silently drop initial replies. No accepted/delivered progress on undefined, challenge or unacknowledged
receipts. Responses are bounded by actual holder sockets; retry only on fresh explicit events, with no internal loop.

K22 owns protocol/Gateway/Session/engine changes; lead's X01 source fixes own outlet/router/limiter only.
Required regressions include the original unchanged restart scenario, reauth between challenge and response,
stale queued data, new instance, watermark-only work, old sender/holder skew, deferred query open/unsubscribe,
and commits while rebuilding. Source tests alone do not satisfy assembled local/deployed acceptance.
