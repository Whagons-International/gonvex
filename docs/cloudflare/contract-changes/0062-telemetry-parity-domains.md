# 0062: Telemetry comparison uses the actual limiter domain

Date: 2026-11-12. Status: decided (lead). Applies existing 0012 R11; no original gate is waived.

K22's fixed-nine local comparison reports seven passing scenarios and two failures. `error-telemetry`
observed 97 accepted events on the Rust reference versus 116 with the kernel's eight gateway holders.
Keep those raw observations. K15's README, `socket.ts` and `limiter.ts` implement 0012 R11: telemetry
windows belong to the object that owns the socket, rather than a deployment-wide account budget.
The unchanged script's two sockets share a Rust process; kernel sockets on different holders need not
share one telemetry window. This does not justify normalizing counts or changing acknowledgments.

## Canonical parity campaign

The existing `GONVEX_GATEWAY_SHARDS=1` configuration is permitted for the canonical assembled comparison
campaign. Configure and record it up front on ONE actual assembled runtime with the real handlers.
Do not silently reroute individual frames/scenarios or substitute a test telemetry port. Keep all original
count, attribution, scrubbing, sequence and operator-route assertions and normalizers unchanged. Label
the one-holder configuration in exact source/configuration/deployment evidence; do not infer it from
random placement or a requested environment value without verifying the actual holder identities.

Add a separate actual multi-holder regression: same-account sockets on one holder share the 120-event
window; distinct holders have independent windows; anonymous subjects keep their per-connection 20-event
window; error/performance events still share the specified charging domain. Record safe holder IDs,
window timing and offered/accepted/refused counts. Authentication-derived attribution remains unchanged.
Do not generalize this telemetry ruling to K16's distributed per-address limiter or undo 0045.

## Final automatic fleet remains a separate required configuration

The one-holder parity configuration proves neither X01 nor fleet-wide abuse capacity. With automatic
controller bindings, a static shard-count environment value may not control routing. The final X01-enabled
assembled runtime must rerun affected semantic/domain checks and the original V03/V04/V06 campaigns,
including 26 tenants/1,000 users while scaling, hot tenants, quiet neighbors and shared Control/auth/limiter
pressure. Never disable automatic placement in that campaign or claim a global account telemetry budget
from per-object windows. Any future global budget is a separate design decision, not silently added here.

## Holder close cleanup remains a correctness gate

K22's full-suite Host regression showed constructor collection deleting a closing socket's recovery
generation before its close callback, losing the expected presence offline alarm. Preserve
`evidence/full-tests-failed-3.*`; it is not superseded by the seven scenario passes. Absence from
`getWebSockets()` alone does not prove a generation is unused by a pending close/error callback.

Orphan-first-seen grace/alarm cleanup may defer collection, but a guessed grace duration alone does not
prove correctness. Preserve published/active generations and metadata required by queued close/error or
pending actions. Verify real close/error presence alarms and K16 release, closing sockets absent from
enumeration, eviction during grace, concurrent publication/auth changes and bounded eventual orphan
cleanup. Stop orphan alarms when nothing remains. Keep 0058/0059 privacy, generation-integrity, atomic
publication and bounded-storage requirements; no per-frame persistence or silent metadata loss.
