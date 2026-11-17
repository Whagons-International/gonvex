# 0076: Terminal drain for a bound admission whose reply was lost

Date: 2026-11-12. Status: narrow interface permitted; implementation and owner lifecycle remain UNMET.
Extends 0069 without turning UNKNOWN into authority or changing 0071's zero-invocation continuation.

## Bind the attempt before issuing authority

A lost admission reply can leave an issued owner without its caller holding the token. Permit a dedicated
versioned admission interface and terminal-only reconciliation:

```ts
interface AdmissionIntent { v: 1; id: string }
interface AdmissionBinding {
  v: 1;
  intentId: string;
  claim: OwnerClaim;
  tenantInstance: string;
  nonce: string;
}
ownerAdmissionIntent(view, claim, intent: AdmissionIntent, expectedProducer?): Promise<OwnerAuthority>;
confirmOwnerAdmission(view, binding: AdmissionBinding): Promise<OwnerAdmissionConfirmation>; // 0078
drainAdmissionIntent(binding: AdmissionBinding): Promise<{
  v: 1; status: 'closed'; binding: AdmissionBinding;
}>;
```

These are private server interfaces. The caller generates and durably publishes a bounded admission intent
BEFORE its one admission RPC. Its id is distinct from a client-event nonce, rebuild sequence and Reducer
idempotency key. The tenant generates the instance/nonce binding and indexes exact pending/issued state by
intent. The holder callback accepts only an ALREADY existing exact pending caller intent and current
physical Session/tag/issuer/epoch/auth scope; an unknown callback cannot create an intent.
Decision0078 defines the versioned exact-echo admission and terminal confirmation replies; a bare boolean
is not the activated interface. All publication, terminal witness and quiescence requirements below remain.

Before confirming true, the holder durably publishes that exact binding through 0058's verified generation
mechanism and rechecks scope after the await. The tenant still requires the same current pending state and
nonce before issuance. Missing methods, partial publication or mismatches deny issuance and retain honest
UNKNOWN. No serving fallback to ordinary legacy admission or per-normal-frame KV write is permitted.

This makes a bound current-instance lost reply addressable. It does not solve unbound pre-callback intents,
legacy state, old tenant instances or missing terminal callbacks. Those cases remain explicitly UNKNOWN.

## Terminal evidence and quiescence

`drainAdmissionIntent` never returns or mints authority and cannot be used for token recovery or re-admission.
It requests a fresh server challenge through an actual-holder `confirmOwnerTerminal(binding, challenge)`
callback. A positive answer echoes the exact binding/challenge and requires a durable authoritative witness
that this physical owner/scope/intent became terminal through real close/error or actual scope finalization.
Publish the denial of future confirmations BEFORE returning positive terminal proof. Absence from native
enumeration, elapsed time, a caller timeout, a digest or a current-scope mismatch alone cannot establish it.

A waking holder may confirm an old issuer only from the verified durable witness for that SAME old physical
token/intent. Its new owner identity cannot speak for the old one. Keep the terminal witness until a matched
closed receipt; a missing/corrupt/collected witness never authorizes positive reconciliation.

The tenant requires its complete current instance/registry, exact binding/state identity and fresh callback
proof. It fences new invocation and cancels the matching pending nonce before awaiting actual pending
confirmation, admitted calls, deferred work and quiescence. Underlying calls and byte/count charges remain
live until they settle; a caller-side timeout is not settlement. Signing failures must not cause replacement
token generation. Only then remove that exact intent/fence/issued record. Late old confirmation/drain cannot
replace or erase a newer admission, including one for the same connection id.

The result is processing evidence only. It certifies no durable write, receiver application, revision,
receipt, job completion or historical non-invocation. Already-started Actions/Workflows and provenance
survive unchanged. Do not map tokenless `closed` into 0069's HMAC-token `retired` proof.

## Lost drain reply and bounds

Without an unbounded tombstone map, a duplicate may return processing-only `closed` only when the complete
current registry proves no exact pending/issued intent remains AND a fresh authoritative holder terminal
proof still denies every late confirmation for that binding. Neither absence alone nor an HMAC alone is
enough. Foreign nonce/intent, old tenant instance, incomplete state or uncertainty remains UNKNOWN.

Bound all caller intents, tenant pending/issued records, durable witnesses, copies and transitions by count
and bytes. Keep uncertainty charged, with honest admission refusals; no active eviction, guessed TTL,
enumeration-based deletion or availability claim. Measure churn/cleanup and shared-isolate fit. The existing
token-drain HMAC invariants and owner/stranded-OWNER gates are unchanged.

Required native tests include lost reply after binding publication, actual holder wake, late confirmation,
terminal transition during confirmation, late old RPC/drain against a new owner, lost drain ACK/duplicate,
wrong intent/nonce/instance, unbound/legacy/corrupt state and hung callee withholding ACK. Prove bounded
churn/bytes and no per-normal-frame writes, then unchanged SDK receipt/revision behavior. Fresh independent
review pins a new immutable implementation. This ruling grants no source approval, serving activation,
N6 acceptance, platform timeout workaround or K22/X01 gate waiver.
