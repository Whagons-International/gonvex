# 0074: Sequenced owner rebuild on the existing ordered channel

Date: 2026-11-12. Status: bounded schema/implementation permitted; activation and acceptance unproved.
Continues 0056/0061/0069/0073. No client wire change, extra channel or X01 placement rewrite.

## Producer and three strict phases

Permit server-only `ReplicaDelivery.ownerRebuild?: {v:1; sequence:number}`. Its identity is the tuple
`(payload.instance, sequence)`. The serving Delivery engine uses the inseparable OwnerRegistry instance
from construction; it cannot restore an old instance without the complete corresponding registry/key.
Sequences are positive safe integers, monotonic within that real incarnation. Reject before overflow;
never wrap or reuse. Allocate once per actual barrier, not each queued duplicate or retry.

| Phase | Required shape | Effect |
|---|---|---|
| Whole-holder challenge | `scopeFence:1`, empty shared/sockets, no watermark, membership, recovery or initial work | Read applicable holder-owned epochs only. No application, owner admission, durable attempt context, processing facts or completion ACK. |
| Empty scoped bootstrap | Existing strict0061 empty work with current holder epochs and the same captured tuple | Create the validated bounded context, confirm/admit the actual owner, perform authorized reset/registration/LQ rebuild. Preserve `change-feed-reset`. |
| Whole recovery | Existing strict0056 whole-holder recovery marker and empty shared/sockets | Create the validated context and perform real authorized recovery; retain its actual recovery reason. |

A new tenant/first known host may proactively initiate the explicit challenge, since normal data from a
different/unconfirmed producer must not discover scope by applying data. Use real existing host routing;
do not guess gateway names or add a subscriber SQL loop. The challenge returns `scopeRequired` plus an
exact `ownerRebuildChallenge?: {v:1; instance:string; sequence:number}` **echo only**. It returns no
`ownerRebuildAck`, `scopeAck`, `recoveryAck` or initial-processing facts. Neither echo nor epoch is auth.
Bootstrap uses the same captured tuple. A delayed old producer's challenge cannot establish current
producer authority or overwrite the confirmed producer fence.

Normal stale-producer work advances no instance/data/watermark, challenge, owner admission or delivered
progress. Preserve membership-first security notifications and actual Member/Control revalidation; this
filter cannot suppress existing auth hooks. A stale payload itself supplies no authoritative auth scope.
No normal frame, instance hint, `queryRecovery` flag, digest or typed rejection creates a rebuild context.

## Pre-application support and exact acknowledgment

Use an additive pre-application RPC, for example `deliverReplicaOwner`, on the **same** OrderedOutlet
queue. Missing old-holder methods fail before application, with no ordinary-method fallback. Every
applicable Session must explicitly support the owner barrier before effects begin. The narrow method
selection/accounting addition is authorized; no second RPC channel or queue rewrite is authorized.

Require `ReplicaReceipt.ownerRebuildAck?: {v:1; instance:string; sequence:number}` for completed bootstrap
or recovery, in addition to existing literal scope/recovery ACKs. Return it only after all applicable actual
Session hooks, authorized rebuilds and scope checks complete. Validate exact version/producer/sequence
against the captured payload. Challenge echo or an ordinary old-holder ACK is not completion. Preserve
normal0064 full validation and recovery-only0070 exact-subset facts; this ACK confers no correlation,
receipt, initial/commit watermark or receiver-progress authority.

Coalesced outlet reset preserves its actual payload and promise. Do not validate a receipt from an older
coalesced tuple as a newly allocated sequence. Owner barriers remain intact; do not split them into normal
parts that lose the marker. Charge every request/receipt/ref/envelope and retained context in existing
count/byte admission. No expanded recovery reserve, legal-frame reduction or fit claim follows.

## Durable attempts and producer rejection

Use bounded physical-owner/tenant/scope/producer/sequence state in0058's verified publication lifecycle.
Publish the attempt durably **before** admission, registration or rebuild RPCs; queued persistence is not
proof. Partial publication fails closed. Unconfirmed producers cannot replace the confirmed producer fence.
Pending/failed/ambiguous attempts restored after wake are consumed/UNKNOWN, never fresh acquisitions.
Exact in-memory duplicates may share the actual promise. Do not re-execute after wake or replay stale
initial-completion facts. A duplicate completion ACK needs an exact current applied witness; otherwise
refuse honestly. Do not evict active/UNKNOWN state to make room. Measure barrier/lifecycle writes; there
is no per-normal-frame KV counter or confirmation RPC.

For rebuild admission, pass the expected producer instance as a server-only **rejection precondition**:
the actual tenant rejects a mismatch before creating pending admission or allocating authority. Afterwards,
real non-serial holder confirmation and nonce/current-state checks still apply; the confirmed authority
must match the barrier producer before publish/use. The precondition is not auth. Thus old-A bootstrap
cannot allocate new-B authority or displace B's durable fence. A second restart, scope mismatch or ambiguous
result stops without ACK/progress or another acquisition. Old owner locators remain UNKNOWN.0071's fresh
client path remains separate; no internal retry credit or historical-quiescence inference is introduced.

## Evidence still required

Test native pre-application skew/missing methods, strict phase shapes, exact challenge echo/ACK mismatch,
zero/unsafe/overflow sequence, reset coalescing, duplicate-after-wake, partial publication, instance/key
rotation, delayed old challenge/bootstrap/data and auth changes. Prove non-serial confirmation reentrancy,
one acquisition, no mutator replay/credit and old-locator retention. Include actual quiet-reader restart on
a different holder from the writer, original raw reset/resume, live-query and SDK receipt/revision liveness,
with concurrent commits/deploy/faults. If host discovery cannot reach a quiet holder, retain that gap.

Reported prototype native/state-machine cases remain author evidence, not serving activation or N6/OWNER
acceptance. Fresh immutable review, full local/private-platform matrix, memory/cost, X01/V03/V04, security
and all actual-duration gates remain mandatory. No staging/production action is authorized.
