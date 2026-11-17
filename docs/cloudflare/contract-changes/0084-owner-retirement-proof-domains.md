# 0084: Keep tokenless closure separate from token retirement

Lead decision 2026-11-12. Clarifies the existing 0069/0076 requirement after independent review of local
`319da32df0517f9c7495c4bf8791aff919d68202`; source changes and serving activation remain held.

The review reproduced a P2 in the actual Registry with real workerd HMAC/crypto and controlled terminal/
quiescence callbacks. Tokenless `drainIntent` removes issuance and increments the token-retirement counter.
The absent-token `drain(authority)` path can then certify `retired` using only the HMAC and complete-registry
counts, even after fresh terminal proof is withdrawn and without calling token-drain quiescence. This is
the certificate conversion that 0076 explicitly forbids. It does not prove that running work was abandoned:
the reproduction awaited controlled intent quiescence. Serving ownership is unactivated.

Correct the bounded issuance/retirement proof model before activation. Keep legitimate token-drain
duplicates and exact tokenless closure duplicates distinct, with complete-registry continuity, actual
underlying settlement, and count/byte charges. Neither absence nor a valid MAC may convert tokenless
closure into token-retirement evidence. Do not add an unbounded retired-token history, evict active or
UNKNOWN records, weaken duplicate assertions, or disable all legitimate token duplicates to hide the bug.
The lead accepts the author's predetermined signed domains for this UNACTIVATED private prototype:

| Issuance domain | Active invocation | Terminal entrypoint | Duplicate proof |
|---|---|---|---|
| T: token-only `admit()` | Existing owned dispatch checks | `drain(authority)` fences and awaits admitted work plus actual token quiescence, then returns `retired` | Current-instance signed T issuance, complete T-registry continuity and genuine retired absence |
| I: intent-only `admitIntent()` | Existing owned dispatch checks | Only exact `drainIntent(binding)` with fresh actual-holder terminal proof and intent quiescence returns `closed` | Fresh exact holder terminal proof plus complete current intent continuity; never HMAC-only token retirement |

Keep the existing `OwnerAuthority` v1/instance/opaque-token shape. Canonical internal nonces are
`t:<positive-safe-sequence>:<uuid>` or `i:<positive-safe-sequence>:<uuid>`, with the entire nonce/domain and
tenant instance signed. Publish the active domain-tagged record before signing/exposure; signing failure
retains charged UNKNOWN. Separate safe scalar domain counters and the bounded active/UNKNOWN maps prove
T-issued minus T-retired and I-issued minus I-closed continuity. Pending/unissued intent accounting remains
separate and complete. No retired-token history is added.

An I authority passed to token `drain` rejects BEFORE mutation or quiescence in EVERY state, including
active and already closed. T has no intent binding and cannot use tokenless closure. This intentionally
restricts the private terminal entrypoint selected at issuance; it is permitted because serving ownership
is unactivated. It grants no deployed legacy compatibility, token reissue/recovery, old-format fallback,
new certificate or handshake. Unknown/old-format/domain/key/instance/continuity stays UNKNOWN. A valid
old duplicate cannot erase or replace a newer owner.

Preserve the independent failing reproduction, then test tokenless closure with terminal proof withdrawn,
legitimate token retirement and duplicates, fresh exact tokenless duplicates, wrong domain/instance/nonce,
incomplete registry, signing/confirmation/drain ambiguity, held underlying work, newer owners, and bounded
churn. Native Registry results with controlled quiescence are not assembled Engine/Workflow/SDK proof.

The review also found a P3 nested-Proxy snapshot edge in the callback/CLOSED result consumer. No native
Proxy serialization exploit or authority bypass was established; actual callers revalidate. The lead
prepares a narrow owned-data snapshot correction, preserving every own string field for exact validation,
rejecting accessors/unknown symbols and keeping pure 0078 validators unchanged. No general recursive
normalizer or additional RPC consumer is authorized. Conditional 0083 remains a separate native boundary.

The lead prepared a two-file source/test patch against local319 without changing the author checkout:
`/mnt/data/work/gonvex-cloudflare-lead/owner-rpc-snapshot/snapshot.patch`, SHA256
`5abc49173f28ebc5b46ca8638fbd91cd198ddd3c73e3acb30e1d15e0da91dde1`.
The local Node24 descriptor/Proxy suite has 15 PASS/6 FAIL before and21 PASS after; original failures are
retained. This is local synthetic boundary evidence, not native Proxy serialization, RPC or authorization
proof. K22 owns ordinary patch integration, canonical workerd/type checks and the next immutable artifact.

Review report and receipt: `/mnt/data/work/gonvex-k22-terminal-review-319da32d/`. Published PR400 remains
`3d92e1339b2d65dbc47ac2f5713f56bd9e57d738`; a review of local319 is not a review of that published head.
Every original K22/X01/N6/OWNER, platform, application, capacity, security and duration gate remains open.
