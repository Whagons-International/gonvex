# V04: measured budgets, shared bottlenecks and account constraints

Owner: lead schedules V04 instrumentation alongside V03 after K22+X01 integration. Original done_when remains:
SQL statements/rows read/rows written/CPU per function; peak memory and wake time recorded.

Pair every V03 cell with per-function and per-service telemetry: tenant writer/queries/change capture,
visibility classes, live-query/replica encoding, gateway outbound queues/frames/bytes/lag, Control/auth and
30-second revalidation cohorts, limiter buckets, ingress/cache lookups, builder/deploy, scheduler/Workflow,
files/R2 and sandbox Containers. Measure shared pool and controller allocation contention across tenants.
No per-subscriber SQL/telemetry loop in commit hot paths; aggregate bounded windows and measure its overhead.

Report CPU separately from wall/duration billing and shared-isolate memory separately from JS heap and DO
state. A 128 MB isolate may host several DOs; do not multiply that into independent per-tenant RAM promises.
Record hibernation/wake, alarm/idle activity, steady and burst allocation, queue bytes/age/RPC delay and
maximum sample interval. Keep tenant SQLite bytes (including indexes/log/catalog) separate from R2 attachment
bytes, Workflows, build artifacts and Container disk/memory. No socket-to-user or request-to-write conversion
without workload measurements. One tenant still owns one serialized SQLite writer.

Published platform limits checked 2026-11-12 (not account-specific guarantees): SQLite DO object count is
unlimited; Paid account storage has no stated aggregate cap, while each object has 10 GB. The documented
1,000 requests/s per object is a soft request limit, not writes/s or users. The SQL row/blob limit now says
8 MB versus the brief's 2 MB. Keep the contract unchanged; add a separate local pinned-workerd AND exact
remote-runtime boundary probe with rollback/cleanup and measure row/blob/bind/statement boundaries.
[DO limits](https://developers.cloudflare.com/durable-objects/platform/limits/).
Workers memory is 128 MB per isolate, shared by its requests and potentially multiple DO instances.
[Workers limits](https://developers.cloudflare.com/workers/platform/limits/),
[DO memory measurement](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/).

Read-only Vault account check at 2026-11-12T09:37:45Z observed a Paid workers_paid subscription (Workers Paid)
and workers/account-settings default_usage_model=standard. This verifies plan/model only. Account-specific
quota overrides, remaining headroom (Workers/classes/Containers/Workflows/R2/API limits), negotiated rates
and included-allowance allocation remain UNVERIFIED until separately retrieved for the test account. Never
infer actual quota from a published table or a successful development deployment. Evidence/rerun script:
/mnt/data/work/gonvex-cloudflare-lead/capacity/account-observed.json and account-plan.mjs; use approved Vault
wrapper. No billing/account changes are authorized by this brief.

Cost: retain measured billable request/WS message counts, rows read/written (including index amplification),
GB-seconds/active/hibernated time, storage GB-hours, R2 operations/bytes and any applicable transfer,
Workflow steps/duration, Container CPU/memory/uptime/disk, analytics/logging and generator resource costs.
Attribute tenant, shared gateway/control/controller and deployment/idle baseline separately. Snapshot official
rates and actual account terms at run time, then show marginal measured-usage cost AND gross/allowance-adjusted
estimates with formulas and error bars; invoice reconciliation is separate. Missing attribution is an explicit
gap, not zero. [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/),
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/).

Final report separates MEASURED, PUBLISHED and ESTIMATED/UNMEASURED findings, exact versions/data/regions and
confidence limits. Publish rates-vs-latency/backlog/error/cost curves and scaling timelines plus raw artifacts
and replay commands. State the first observed bottleneck, whether a generator or shared service caused it,
and the tested envelope. Do not publish an arbitrary safe tenant/user count or an unlimited-throughput claim.
