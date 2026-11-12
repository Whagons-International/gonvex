# 0069: Tenant-instance owner authority and terminal drain

Date: 2026-11-12. Status: bounded prototype permitted; lifecycle/availability acceptance remains UNMET.
This does not delay the tested N1–N5/finite-guard checkpoint or certify its later implementation.

## Interface and trust boundary

Permit these additive server interfaces, with the existing SocketView/ClientFrame types:

```ts
interface OwnerClaim {
  v: 1;
  connectionId: string;
  holder: string;
  physicalTag: string;
  issuer: string; // actual holder incarnation
  subscriptionEpoch: number;
}
interface OwnerAuthority { v: 1; tenantInstance: string; token: string }
interface OwnerTenantRpc {
  ownerAdmission(view: SocketView, claim: OwnerClaim): Promise<OwnerAuthority>;
  dispatchOwned(view: SocketView, authority: OwnerAuthority, frame: ClientFrame): Promise<string[]>;
  drainOwner(authority: OwnerAuthority): Promise<{v:1; status:'retired'}>;
}
```

An actual-holder `confirmOwner` callback verifies the current Session, physical tag, issuer, epoch and
auth scope. The tenant binds opaque random authority to that owner, tenant instance and confirmed server
view. These are private server fields, never decoded client authority, public wire changes, idempotency
keys or a replacement principal. Preserve normal membership revalidation, K17 access and migration gates.
Do not log tokens, private scope or credentials.

Only explicit admission may allocate authority. Unknown/stale/missing tokens on dispatch, reset or recovery
must reject before observation, fence recreation or module invocation. An ordinary legacy socket dispatch
cannot remain a fallback bypass. Missing new methods fail before old ordinary invocation. Trusted service,
scheduler and nested Action adapters retain their separate existing server-principal rules.

## Admission races and reentrancy

Reserve a tenant-instance-bound pending admission nonce in serialized owner state before asynchronous
confirmation. Publish only if that exact state/nonce remains current afterwards. A delayed old confirmation
must not replace a newer admission or recreate an owner after release. Recheck actual holder scope before
installing the returned authority too. Random issuer names alone do not order overlapping callbacks.

Confirmation cannot queue behind the Session serial event currently awaiting ownerAdmission: that would
deadlock the callback. Prove real workerd reentrancy and scope checks; an extracted-method mock is not
sufficient. Bound/coalesce pending admissions and their bytes. No fleet singleton, per-frame confirmation
RPC, SQL subscriber loop or per-frame KV counter is introduced. Wake/issuer rotation costs must be measured;
do not assume a write on every wake is free or different from a sparse per-frame write.

## Drain, restart and ambiguity

Drain serializes behind admitted tenant work and its deferred socket work. Invalidate pending admission
nonces and retire authority before deleting its fence. A late old request never auto-admits; an old or
duplicate release cannot delete newer authority. A retired acknowledgment is processing evidence only,
not a Reducer receipt, durable-write confirmation or proof of receiver application.

Timeout/network/caller-side rejection remains UNKNOWN, including lost admission replies and lost drain
acknowledgments. Do not restore lost in-flight accounting as zero, guess that work never ran, or replay an
Action/Reducer merely to reconcile authority. A new tenant instance rejects old tokens. A waking holder
rotates issuer and reconciles through real confirmed drain/admission, with scope checks before and after;
it cannot restamp an old view or infer quiescence from enumeration absence.

Terminal socket-authority drain must preserve already-started durable Actions/Workflows, idempotency and
their K17 provenance. It does not cancel/rerun durable jobs or confer new module permissions. Later replies
still use N4's exact correlation and honest enqueued/obsolete/UNKNOWN settlement.

Keep 0065's finite active/UNKNOWN authority admission and honest refusal. A missing native socket with a
retained durable owner remains UNKNOWN. Token fencing may address historical epoch-map retention; it does
not by itself solve 0058's stranded durable OWNER cleanup or prove indefinite churn availability.

## Required evidence

Hold and reorder actual dispatch/reset/recovery/deferred work against drain; include delayed confirmation
after a new admission/release, lost admission reply, ambiguous drain, tenant restart, holder hibernate/wake,
late old token/release and missing legacy methods. Verify zero pre-admission invocation, no resurrection,
current owner liveness, bounded state/cleanup, isolation and unchanged SDK receipt/revision behavior.
Retain failure evidence. A fresh independent review pins a new immutable artifact; original K22/X01 and
platform/capacity/lifecycle gates remain unchanged. No CPU timeout workaround, staging or production action
is authorized by this interface ruling.
