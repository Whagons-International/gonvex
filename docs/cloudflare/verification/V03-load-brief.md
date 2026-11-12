# V03: assembled capacity and autoscaling implementation brief

Owner: lead schedules a V03 implementer after K22 and X01 artifacts are integrated; fresh verifier is separate.
Current state: planning only. No assembled load result or safe user-count claim exists.

Original done_when is unchanged: 26-tenant mixed run and 1,000 users on one tenant; p95 <500 ms, p99 <1 s,
errors <1%, all connected, including reconnect wave. Both gates must execute WHILE X01 scaling occurs.
Measure durable-confirmation and receiver-delay distributions independently; passing one is not the other.

Run ONE assembled exact gonvex-cf-dev-* deployment, real SDK/app handlers and representative Whagons tenant
copies (small, median, largest; schema/catalog, row/column/text entropy, indexes/triggers/views and membership
skew recorded). Workload includes task create/status/owner/comment, search/live-query subscriptions, scheduled
and background work; derive mixes from sanitized representative evidence, do not call an invented mix typical.
Separate synthetic stress, representative copies and real app scenarios. Never use serving mocks/test adapters.

Sweep per-tenant offered writes/s and query mix; connected vs actively editing clients; schema/data size;
recipient/visibility class fan-out; snapshot/reconnect storms; simultaneous tenants (including 26), hot tenant
plus quiet neighbors, growing/shrinking tenant counts and fleet demand bursts. Compare both fixed aggregate
rate split across tenants and fixed per-tenant rate as aggregate rises. Repeated ascending/descending points
locate the first bottleneck and autoscaler hysteresis. Do not extrapolate S3 socket traffic into app capacity.

The shared gateway pool is global per namespace (base e72d3a43, placement/gateway.ts), not eight gateways per
tenant. Capture actual names/versions/ownership/controller shards, gateways per hot tenant and shared-service
contention. Audit Control/auth/revalidation fan-in, NAT-concentrated public limiters, tenant lookup/routing,
deploy/builder/Workflow/sandbox services and account limits. Keep quiet-tenant baselines through every ramp.

Use capacity/matrix.json and capacity/runner.mjs in the lead continuation artifact directory as a bounded
campaign planner/driver shell; the real SDK driver is a V03 deliverable, not yet supplied. Each campaign
must declare finite cells, wall time, tenant storage, offered operations and cost budget. Admit generators
through current Arch3 gates; no staging/prod load. Start low, stop at a correctness/durability failure,
resource/cost envelope, sustained backlog that cannot drain, gateway/controller failure, or generator
saturation; do not continue an unbounded search. Refine around the first failing point only after diagnosis.

Record scheduled/offered, actually dispatched and durably completed rates, generator lag/dropped scheduling,
p50/p95/p99 (plus sample counts/timeouts/censoring) for durable confirmation and receiver convergence,
errors/retries/disconnects, queue/backlog time series, per-tenant final checksums/expected receipts/no duplicate
writes, autoscaler timeline/instances/epochs/drains, CPU/isolate memory/SQL/rows/bandwidth/cost with V04.
Observe receiver revision application, not merely server send completion. Same-host monotonic writer/reader
clocks preferred; cross-host latency requires clock-offset/error bounds. Retain intended-send timestamps to
avoid coordinated omission. Acknowledgement semantics must prove durable commit, not speculative SDK echo.

Declare run seed, duration/warmup/measurement/cooldown, load-generator regions/colo/host versions, backend
commit/artifact IDs/bindings/compatibility date, source/data hashes, instrumentation and sampling loss.
Repeat gate points and bracket points; report confidence and between-run variance. Instrument generators'
CPU/memory/event-loop lag/network/socket/FD constraints; split generators and replay the same point to
separate client saturation from backend saturation. Publish raw JSONL, summary CSV, capacity curves and
runnable commands under docs/cloudflare evidence references, with private tenant data excluded.

X02: measure 1,000-client cold snapshot storm against active writers. Its original p95 writer confirmation
<150 ms gate remains. If K22+X01 cannot meet it, implement X02 via its written dependency; do not waive it or
claim gateway autoscaling removed the serialized tenant writer/snapshot bottleneck.
