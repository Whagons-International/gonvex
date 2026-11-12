# 0081: Match and dispose the tokenless closed RPC receipt

Date: 2026-11-12. Status: narrow implementation permitted; terminal/activation acceptance remains UNMET.
Extends0080 ONLY to the real `drainAdmissionIntent` CLOSED RECEIPT consumer under0076/0078.

The native closed object receives RPC disposal metadata too. The [official lifecycle documentation](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/#disposing-rpc-objects-disposes-stubs-that-are-part-of-that-object),
rechecked2026-11-12, describes disposers on RPC object results; disposal does not await server finalization.
The pure exact `validOwnerAdmissionClosed` validator correctly refuses transport metadata and stays unchanged.

Permit identical0080 own-DATA inspection, ALL own string properties reaching exact validation, and only
the standard own data `Symbol.dispose` function as root lifetime metadata. Reject accessors, other symbols,
function/capability fields and nested extras. No JSON roundtrip, recursive stripping or stub retention.
Reconstruct exact `{v:1,status:'closed',binding}` with validated copied primitive/nested data only after
full offered claim/intent/instance/nonce binding validation. Safely captured disposal runs once in finally
with the original receiver on success/refusal/error; a throwing disposer withholds the result. Unknown,
unsupported/malformed/mismatched receipt, missing method or ambiguous RPC retains UNKNOWN and all charges.

After actual RPC AND disposal settle, local Host cleanup must recheck the exact original stored intent/
binding and authoritative terminal witness/current cleanup state. Remove only that matched record through
atomic durable publication. Keep denial of every future old confirmation durable; older retained recovery
generations cannot resurrect it. A delayed old receipt cannot delete a newer/other owner. On failure retain
the witness/uncertainty; absence, timeout, digest or current-scope mismatch is never cleanup authority.

The receipt is processing evidence only under0076, not HMAC `retired`, terminal proof by itself, quiescence,
historical non-invocation, write/receiver success or revision/watermark progress. This grants no adaptation
for `drainOwner`, `dispatchOwned`, normal frames, client/monitor/M05/raw records or other RPC consumers.

Also permit only the additive existing-owner-region Gateway delegate
`confirmOwnerTerminal(binding,challenge) { return this.host.confirmOwnerTerminal(binding,challenge); }`.
Preserve its exact Host promise/result/failure; no fallback, bare boolean ACK, extra channel or placement/
controller/outlet edit. Actual durable terminal witness and quiescence remain prerequisites.

Require native returned-closed/matched cleanup/wake tests; lost ACK, wrong/stale binding/new owner and
disposal/error/malformed/nested-field negatives with zero cleanup. Plain local receipts are a separate
domain, never made native by adding a fake disposer. Foundation48040e6d's97 native/model tests and cycles
1→1 remain scoped author evidence, not source approval. Every N6/OWNER/K22/X01/platform/full-matrix/
capacity/security/actual-duration gate remains; no serving activation, account or live diagnostic release.
