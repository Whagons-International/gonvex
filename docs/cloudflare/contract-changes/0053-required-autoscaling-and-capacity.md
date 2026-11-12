# 0053: required gateway autoscaling and assembled capacity proof

Date: 2026-11-12. Status: decided (Gabriel's explicit direction; lead coordinates implementation).

X01 is required for the initial staging candidate. Its original done_when remains mandatory, augmented by
cross-tenant fleet scale-out, hot-tenant delivery scaling, safe delayed drain/scale-in and failure recovery.
V03 and V04 additions in work-packages.json preserve their original thresholds. X02 stays dependent on X01;
evaluate it against snapshot-storm evidence and pull it forward if needed to meet its original writer-latency gate.

The current gateway placement hashes pre-auth connectionId into GLOBAL names gw-0... in one namespace,
not eight gateways per tenant. Default 8 and the 1..4096 environment option are static stage-1 configuration,
not elasticity. Authorization now includes runtime placement/routing changes with no manual shard-count or
environment/deploy intervention. Do not shard/repartition tenant SQLite: tenant writes stay serialized.

## Shared implementation contract

K22 remains running and owns assembly, root Worker exports/config, control/principals adapter promotion,
query/replica composition and serving Actions/files/telemetry/guard integration. It reports its new
kernel/assembly/gateway.ts extends GatewayObjectBase; actionFinished RPC calls Session.actionFinished.
Its session/session.ts edits retain live-query requests for restart resubscription and direct Workflow
replies. Its delivery/engine.ts additions prepend live-query ops on the single ordered commit channel
and observe live-only sockets. Do not overwrite these.

X01 owns cloudflare/kernel/gateway/ and additive placement/controller APIs, with lead authorization for
kernel/session/placement/gateway.ts, kernel/session/router.ts, minimal host/ports/rpc and delivery/outlet
hooks. K22 confirms those files are untouched. X01 must preserve existing GatewayObjectBase APIs,
telemetry/publicGuard injection, actionFinished and stable ordered holder delivery. K22 can keep a static
placement adapter during independent assembly; lead integrates the automatic adapter before V03 and staging
readiness. Coordinate exact API signatures/shared edits before overlap. Root binding/migration/config
changes are proposed to K22/lead, not competing whole-file replacements. No new unordered commit channel.

X01's design explains durable assignment/version/epoch, restart/controller recovery, stable connection
ownership through membership changes/drain, resume on reconnect/deploy/fault, bounded buffers/telemetry,
sharded or otherwise bounded allocation/auth/limiter work, idle hibernation and a finite cost/resource
ceiling. S3 is initial evidence, not a full-app capacity claim. Autoscale decisions must use outbound
frames/bytes, delivery lag, queue age/RPC delay and sockets; outbound saturation occurred below 20% CPU.

Fresh independent review follows each immutable artifact through a NEW delegate_task. No gate weakening,
production/staging load, forced promotion or live cutover. K22 and X01 each get isolated T3-owned checkouts;
lead resolves conflicts and owns exact-head merges and formal proof gates.
