# 0080: Normalize callback RPC lifetime metadata at its consumer boundary

Date: 2026-11-12. Status: narrow transport adaptation permitted; native/activation/lifecycle proof remains.
Continues0078. The pure owner-intent leaf validators remain unchanged and exact.

Author phase27 observes a real callback result with exactly the expected own string fields and one
additional own data `Symbol.dispose` function. The nested binding/claim and current scope validate; the
pure leaf correctly refuses the raw transport root. Earlier native failures are retained, not recast as
authentication success. The [official RPC lifecycle documentation](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/#disposing-rpc-objects-disposes-stubs-that-are-part-of-that-object),
checked2026-11-12, describes disposal metadata on RPC object results and disposal of contained stubs.
It does not promise this exact descriptor shape across future runtime versions. Disposal is not proof
that server finalization ran, or that work/ownership became quiescent.

At the REAL confirmation callback consumer, and future terminal callback consumer only:

1. Inspect own descriptors without invoking property accessors. Copy ALL own string DATA properties into
   a fresh `Object.fromEntries` record, including nonenumerable or unknown strings. Unknown string fields
   must reach the unchanged exact validator and fail; never silently filter them.
2. Permit only the standard own data `Symbol.dispose` FUNCTION as root RPC lifetime metadata. Reject
   accessors, other symbols, capabilities and other function fields. Do not JSON-roundtrip, recursively
   strip metadata, ignore nested fields or extend this rule to client/monitor/M05/raw objects.
3. Apply unchanged exact version/keys/full binding/current-state validation before authority effects.
   Use only primitives or reconstructed validated pure data afterwards; retain no original result,
   nested capability or RPC stub. An echo/digest remains correlation only.
4. Dispose the original result exactly once in `finally`, using the safely captured standard data method
   with its original receiver, including when another field is rejected. Do not invoke an accessor or
   unknown disposer. Data becomes available to the caller only after that disposal path completes.
   Unsupported metadata or disposal failure stays fail-closed/UNKNOWN, never a successful confirmation,
   legacy fallback, retry credit, terminal witness or cleanup grant. Do not duplicate stubs.

This is Cloudflare transport adaptation, not a new wire message, module capability, principal or relaxed
leaf schema.0076/0078 durable pending/binding publication, scope revalidation, witness and quiescence rules
still govern positive outcomes. Future terminal callbacks use the same boundary rule, not a free terminal ACK.

Require native positive and false replies, malformed versions/bindings, unknown symbols/accessors/extra
strings/nested fields, scope changes during await, and disposal on success/refusal/exception. Keep every
prior failure and exact runtime evidence. No serving activation, N6/OWNER, platform CPU, SDK/full-matrix
or original K22/X01/capacity/security/duration acceptance follows.
