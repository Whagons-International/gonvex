# 0078: Versioned replies for owner-intent callbacks

Date: 2026-11-12. Status: leaf types/validation permitted; durable integration and lifecycle remain UNMET.
Clarifies0076 before serving activation. This changes no client protocol, principal or admission authority.

Permit these plain server-only replies in the proposed `session/owner-intent.ts` leaf:

```ts
interface OwnerAdmissionConfirmation {
  v: 1;
  binding: AdmissionBinding;
  confirmed: boolean;
}
interface OwnerTerminalConfirmation {
  v: 1;
  binding: AdmissionBinding;
  challenge: string;
  terminal: boolean;
}
```

`confirmOwnerAdmission(view, binding)` returns the first shape; `confirmOwnerTerminal(binding, challenge)`
returns the second. Validate version and the exact offered binding, claim, nonce, instance and challenge,
as applicable, against the still-current state before any acceptance or effect. An echoed object or digest
is only correlation, never authority. Malformed/mismatched/unsupported replies and absent old methods fail
closed; false does not imply retirement, historical non-invocation or cleanup permission.

`confirmed:true` still requires an existing exact caller intent, actual physical Session/scope validation,
durable binding publication BEFORE the positive callback and post-await scope checks. `terminal:true`
still requires the durable witness for that actual old physical owner/scope/intent, with future old
confirmations denied before positive proof. All0076 pending/issued state identity, quiescence, duplicate
closed/UNKNOWN and no-token-recovery rules remain. Add no legacy admission fallback or retry credit.

The dedicated tenant interfaces remain `ownerAdmissionIntent(view, claim, intent, expectedProducer?)`
and `drainAdmissionIntent(binding)`. The older `ownerAdmission` prototype stays explicitly unactivated;
its presence cannot create a serving compatibility lane. No outlet, placement or M05 change is implied.

Leaf/model tests are not durable Host0058 publication, native Registry integration, unchanged SDK liveness,
N6 or stranded-OWNER proof. The author reports phase15's64 root/36 gateway cases and root TypeScript
passing, including9 bookkeeping model cases after a reentrant singleflight fix; failed phases13/14 remain
retained. Those results do not activate the model or close any lifecycle gate. Require a coherent immutable
implementation and NEW independent review, preserving every K22/X01/platform/capacity/actual-duration gate.

0080 permits narrow RPC lifetime-metadata normalization only at the real callback consumer, followed by
these unchanged strict leaf checks and disposal of the original. It does not relax untrusted data schemas.
