# 0056: bounded outlet pressure and acknowledged whole-holder recovery

Date: 2026-11-12. Status: coordinated design accepted; implementation/proof pending independent review.

X01 owns outlet/protocol pressure changes, K22 owns engine/Session semantics. A send rejection followed by
logging is not recovery: committed frames cannot silently disappear. Preserve the one ordered channel.

- Internal DELIVERY_CAPACITY_CODE is GONVEX_DELIVERY_CAPACITY; typed DeliveryCapacityError and structural
  error.code predicate are allowed. Engine must not import outlet at runtime and introduce a cycle.
- send rejects before retaining an over-budget normal payload. reset(host,payload) synchronously admits one
  coalesced per-host barrier on the SAME queue, discards queued normal work, preserves the existing in-flight
  RPC, then sends recovery. Its promise settles only on recovery RPC completion, not enqueue. Pending/in-flight
  duplicate reset calls share that barrier promise without replacing its universal payload.
- Discarded normals settle undefined; engine MUST NOT advance accepted revision or delivered callback on
  undefined. Mark host lagged immediately, suppress normal/new-open processing while recovery is required.
- Universal payload: ReplicaDelivery.recovery?: {reason:'change-feed-lagged'}, shared:[], sockets:{}.
  Holder resets installed and pending/failed replica opens and rebuilds retained live queries, preserving
  auth/subscriptionEpoch/direct pendingActions. K22 Session.recoverReplica(tenantId,payload):Promise<void>
  runs on the socket's serial queue, verifies current tenant, awaits real tenant query dispatch/results and
  rejects on failure. Gateway deliverReplica handles recovery BEFORE ordinary membership/payload delivery,
  requires the hook for every applicable session and awaits them all. No ack for queued-but-unapplied work.
- Add ReplicaReceipt.recoveryAck?:1. New reset requires ack===1. An old holder's ordinary receipt is an
  explicit GONVEX_DELIVERY_RECOVERY_UNSUPPORTED failure; never infer support from a success-shaped receipt.
  LocalOutlet gets ack only from its actual local receiver. RPC/unsupported failure keeps recovery-required
  state and blocks normals; a fresh explicit event can retry once. No internal unbounded retry/second channel.
- K22 retains suppression through serialized per-host live-query catch-up snapshot and an acknowledged ordered
  catch-up payload, so commits arriving during async query rebuild are not lost. Optional old-outlet fallback
  is additive-skew compatibility, not proof of the new backpressure behavior.

Initial candidate accounting: module/isolate-scope shared credits across OrderedOutlet instances, 16 MiB
normal retained UTF-16/envelope estimate, 8 MiB recovery reserve, 256 normal items, 2056 host metadata records;
max eight RPCs is a separate scheduling limit. A sole legal oversized singleton is allowed only when all shared
normal/recovery credits are empty. Preserve the protocol size bound rather than silently imposing 8 MiB total.
Account active serialization, retained references and copies; other DO state shares isolate memory. These
numbers are proposed envelopes, NOT proof that a legal near-32-MiB frame plus copies fits 128-MB isolate RAM.
That boundary and many-outlet/DO aggregate pressure remain V04/platform proof requirements.

Metadata ownership must not stay permanently charged after an outlet disappears. A bounded weak-owner sweep
is a candidate implementation (X01 reports WeakRef supported locally); explicit disposeEmptyHost is allowed
only after authoritative empty/no-pending/no-inflight state and epoch fencing. Never clear recovery-required
state of a live holder to regain credits. Do not rely on timely GC/finalizers as an unmeasured liveness guarantee.
Test hibernation/collection/restart, repeat resets/failures, epoch races, interrupted opens and live-query
catch-up under writes. Report credit pressure and unavailable recovery explicitly. K22/X01 do not edit each
other's engine/session/outlet regions; shared GatewayObjectBase recovery method is a narrow coordinated patch.

## K22 ordered query snapshots and scope fencing follow-up

K22 owns normal initial K08 query snapshots through ReplicaDelivery as well as commit updates; returning
initial data on the independent dispatch RPC could reorder it behind a newer commit. It adds synchronous
LiveQueries.subscribeSnapshot, engine.queryReply and pending-live-query holder tracking. Universal recovery
reuses the real handler with server-only RecoverySocketView.queryRecovery=true, then catch-up/ack. That
internal flag must not be accepted from client frame content or bypass auth/visibility checks.

X01 adds only readonly subscriptionEpoch?:number to SocketWork in its protocol patch. K22 owns stamping
EVERY normal/split work part and rejecting stale epoch work in Session BEFORE data, instance or watermark
application. Delayed same-tenant old-Member snapshots are an authorization fence, not merely a UI race.
Test Member/tenant switches, reauth/reset and deploy interleavings; specify missing-epoch older-sender
behavior explicitly and recover rather than apply unverifiable scoped data. K22 retains splitPayload,
Session and engine ownership; no competing placement/router/outlet edits. These source changes remain
subject to fresh independent exact-artifact review and regression/platform evidence.
