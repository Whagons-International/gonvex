# Executable assembled SDK capacity driver

Source base: `471a01c6ca047c42865e2fd9668a9e3c9d73836e`, normally merged after preserving the initial
864-based driver work. This is source preparation, not V03,
X01, V01 or OWNER acceptance. No target, authentication, provisioning, browser or load call was made.
The stopped historical model qualification/branches are not used. Serving code and SDK contracts are unchanged.

## Commands and shell adapter

```sh
node cloudflare/conformance/capacity/cli.mjs preflight /absolute/private/binding.json
# FUTURE authorized execution only, after fresh V01, resource admission and completed bindings:
node cloudflare/conformance/capacity/cli.mjs runCell /absolute/private/binding.json /absolute/private/output
node cloudflare/conformance/capacity/cli.mjs cleanup /absolute/private/binding.json /absolute/private/output
```

`driver.mjs` exports `kind='real-assembled-sdk'`, `runCell({cell,manifest,signal,emit})` and
`cleanup({cell,manifest})`, matching the lead's existing shell. Add absolute `manifest.driverBinding` and
`manifest.driverOutput`; `cell.id` must match the explicit binding. No shell file is edited. Offered and
attributed measured-cost events are **deltas**. Successful execution returns `EXECUTED_NOT_VERIFIED`, never PASS.
Preflight only hashes/parses local files: it never imports executable modules, reads credentials or contacts a target.

## Closed binding v1

`binding.mjs` is the executable schema. Unknown fields (including `assembled:true`), malformed values,
non-finite bounds and missing required properties refuse. All paths are absolute. Every static file binding
is exactly `{path,sha256}`. JSON inputs are at most 8 MiB; static files at most 32 MiB, source closure at most
128 MiB/4096 unique files. This inventory is checked, not an automatic proof that an arbitrary module has no
undeclared imports: ROOT must review its compiled SDK/app dependency closure and build correspondence.

Top-level required keys:

| Key | Required contents |
|---|---|
| `v`, `runId`, `cell` | `1`, bounded safe filename identity, exact shell cell identity |
| `deployment` | `accountId` (the authorized dev account), exact `sourceSha`, `moduleSha256`, `configSha256`, `endpoint` (`wss`), five `resources` (kernel/runtime/actions/builder/sandbox) with exact `role,name,version`, `catalogVersion`, `catalogReleases:[{projectId,moduleSha256}]` |
| `sources` | hashed compiled `sdk`, generated `refs`, generated `localRuntime` (or null/DORMANT); `sdkSourceSha`, `webSourceSha`, hashed `closure` including those entries |
| `auth` | only `{fileEnv:'WHAGONS_CREDENTIAL_FILE'}`; no token/header/login in a binding or parent argv |
| `tenants` | at most 26 explicit `{alias,tenantId,project,projectId,principals,fixture,dataProvenance}`; `project` is the SDK routing input, `projectId` the authoritative catalog/admission identity; hashed actual copy/schema/data provenance required at execution, never invented memberships |
| `phases` | at most 32 `{name,stage:'warmup'|'measure'|'cooldown',durationMs,tenants}`; each tenant is `{alias,connections,active,reconnect,rates:[{operation,perSecond}]}` |
| `mix` | `{kind:'synthetic-stress'|'source-derived',evidence:hashed-file|null}`; source-derived requires sanitized source evidence, never labelled typical merely because it is declared |
| `telemetry` | `{url,adapter,contractSha256,providerFile,providerWindowId,cells,fresh,maxPages,judgment}`; nulls keep execution DORMANT. The hashed adapter translates K22's actual typed serving contract into the driver's normalized measurement profile. Contract digest hashes `telemetry.mjs`, **not** a shared serving API proposal |
| `budget` | all numeric fields below, with finite enforced ranges |
| `resourceAdmission` | hashed normal resource-helper receipt or null/DORMANT; not a new approval mechanism or proof that compatibility memory flags enforce caps |

Budget fields: `maxWallMs,maxConnections,maxPending,maxOffered,maxSentBytes,maxReceivedBytes,maxBufferedBytes,maxArtifactBytes,
maxResidentRows,maxResidentBytes,maxGeneratorRssBytes,maxGeneratorFds,maxUsd,maxLagMs,operationTimeoutMs,
setupTimeoutMs,cleanupTimeoutMs,opensPerSecond,telemetryIntervalMs,maxTelemetryAgeMs,maxCostAgeMs`.
Whole-run offered operations include cleanup; maximum 100,000. At most 26,000 simultaneous sessions,
1000 per tenant. These are driver storage/admission bounds, **not** safe backend capacities. The last client
per tenant is an independent receiver, excluded from active-writer rotation. 1000 connections is not 1000
unique principals: population records actual admitted account and tenant-Member counts separately.

One-tenant 1000-session and 26-tenant scenarios use the same executable phases. Bind actual fixture/principal
pools; vary rates independently per tenant, grow/shrink tenant populations, retain quiet neighbors with zero
rates and request bounded reconnect cohorts. Opening/reopening is paced independently. The current two genuine
P03 copies and existing credential references do not constitute 26 tenants or 1000 principals. Provisioning is
a separate future authorized fixture step; no IDs/users/permissions were invented or created here.
Readiness requests may run concurrently up to `maxPending`, with paced starts. Planned reconnect cohorts
replace closed clients with fresh SDK clients/subscriptions: public `close()` disposes replica ownership and
must not be followed by reuse of that disposed client. Actual spontaneous SDK reconnect remains observable
through connection-state events. These planned cohorts are labelled fresh-client/cold-store, not a claim of
same-client resume. Measurement starts after readiness; X02's overlapping active-writer/snapshot-storm gate
still needs its separate scenario and is not qualified by this driver preparation.

## Fixtures, app calls and observations

A fixture is exactly `{ownedIds,replica,liveQueries,operations,cleanup,finalChecks}`. `replica` is
`{ref:[generated-api-keys],args,keyPath:[row-key-fields]}`; each live query is `{ref,args}`. An operation is
`{alias,kind:'query'|'reducer'|'action',ref,identityPaths,cases}`. A case is `{args,expect}`; expectation is null
for queries or `{key,valuePath,value}`. Every non-query identity selector must resolve to an explicitly owned
fixture ID. Write expectations must be distinct and not already satisfied before sending. Non-query cases
are consumed once, without automatic replay/retry; enough cases for all offered slots are required.
Ownership allowlists bound effects; they do not prove that copied customer rows are synthetic. ROOT must bind
the actual fixture provisioning/ownership evidence before execution. Cleanup operations use the same genuine
refs, ownership checks, command evidence and independent receiver checks. `finalChecks` verifies a declared
field projection and emits its hash, not a fabricated whole-tenant checksum/exactly-once assertion.
Write fixtures without an explicit cleanup operation refuse before client construction.

Refs are resolved from actual generated `api`, preserving `localExecution`, authorization and other metadata.
Current genuine task refs include `tasks.create`, `tasks.assign`, `tasks.requestTransition`, `tasks.rename`
and `tasks.archive`. Queries/search/live subscriptions use the supplied real generated refs and arguments.
The audited catalog does not expose a task-comment reducer under an assumed `tasks.comment` name; board,
people-feed and workbench comments are different app operations. No substitute is silently selected. Scheduled
or background work may be triggered by genuine app actions/reducers, but an action response is explicitly
NOT durable completion of its downstream jobs. A real job-completion port/scenario is still required.
Source-derived workloads containing actions refuse with `UNSUPPORTED: command-bound-action/background-completion-port`.
Synthetic stress can exercise the supported SDK action method, but its response-only metric cannot qualify
the original durable-write gate.

Reducers call **only** `reducerWithTransactionEvidence(...,{offline:'reject',optimistic:[],timeoutMs,
execution:'server'})`. Genuine local refs require the genuine generated `localRuntime` artifact and an
already authenticated, complete, server-backed observation. No ordinary reducer/later receipt-fence fallback.
Missing/no-op/filtered/unsupported evidence or post-send failure remains UNKNOWN; a precondition refusal does
not authorize another attempt. The SDK's matching original transaction confirmation is measured separately
from receiver field convergence. It includes SDK observation/receipt processing and is not a bare SQLite
fsync timer. Measure the independent serialized tenant-writer ceiling in low-fanout phases plus actual
writer telemetry; gateway movement does not partition SQLite.

Open-loop lane clocks preserve each intended timestamp without materializing an unbounded queue. A busy
client/global inflight ceiling produces an explicit censored offer; excessive scheduling lag stops. Receipts
separate offered, SDK attempts, possibly dispatched, command-bound durable, receiver-applied, failures and
censored timeouts. They record no args/results/tokens/customer rows. Latencies share one worker monotonic
clock and include scheduling delay; missing samples are null, never zero. Raw private JSONL plus terminal
sample counts/p50/p95/p99 remain evidence to verify, not a capacity result. Warmup/measurement/cooldown and
cleanup distributions are separate; raw receipts retain phase, tenant alias and operation for later grouping.

## Effect lifetime and cleanup

The parent reads no credential contents. The worker reads a mode-0600 Vault child file with exactly
`{sessions,monitorToken}`. Each session is `{principal,tenantId,accountId,memberId,token,expiresAtUnixMs}`;
it must match the explicit pool and remain valid for the entire bounded run. Actual `tenantAdmission()` is
checked against those identities. A future approved Vault child must supply these scoped sessions; this
preparation neither authenticates nor exports them.

Before SDK/app import, the owned realm installs deadline/AbortSignal guards on ambient WebSocket constructor,
every socket `send` and fetch, plus the auth callback. SDK direct receipt sends and `sendAuth(useFetcher:false)`
cross that same boundary. Token callback failure cannot bypass it through a retained token. Stop is permanent,
not dependent on a timer callback firing; cleanup only shortens the lifetime. No SDK API was added. This is
an environment boundary for reviewed SDK/app modules, **not a sandbox for arbitrary Node code**: raw `node:net`
in an unreviewed dependency is outside it. Reviewed provenance is mandatory.

All owned clients/subscriptions are closed in finally; sockets/fetches are stopped and local close events are
awaited up to the declared cleanup bound. The parent awaits worker exit/termination even on stalls, faults or
emit failures. IPC is capped at 256 unacknowledged, payload-free events; artifacts have a byte ceiling before
further effects. Separate-process `cleanup` reports UNKNOWN instead of blindly recreating/replaying old writes.
Worker exit proves local producer termination only. Previously admitted server work, missing cleanup receipts,
remote quiescence and FIN/OWNER remain UNKNOWN unless independently observed.

## Telemetry and remaining runtime bindings

Existing authenticated `/__assembly/holders`, `/__assembly/version` and project-auth `/dev/metrics` are partial.
Holders scans 32 controllers and exposes per-socket labels; metrics has placeholder zeros/empty objects/logs.
They are not promoted to the new content-free capacity contract. K22 owns the serving port. Its typed source handoff is now implemented by the driver adapter, while
independent review/installation/execution binding remain separate ROOT closures.

The actual 471 `/__assembly/version` shape is checked through its existing authenticated fixture gate before
SDK construction: stable Worker ID, Runtime Worker ID, catalog version, ready and project release mapping.
This does not substitute for five-component provider/deployment evidence. Old four-component/monolith
bindings refuse; the current live 864 release is not represented as a Runtime-aware target.

The serving wire remains K22-owned. `capacity-v1.mjs` translates it into the driver-owned normalized
profile without requiring the server to implement that profile. Cached reads report one catalog and one
controller RPC attempt: two known attempts, zero holder/snapshot calls, a possible controller wake and
UNKNOWN actual wakeups. Fresh reads preserve capacity/drain attempt counts and possible snapshot effects;
failed HTTP responses leave additional RPC effects UNKNOWN. No attempt count is execution attribution.

Sampler requests/responses/errors/request-loss are separate from the source's last64 event history, whose
loss is always UNKNOWN. Page COMPLETE is never fleet COMPLETE. Per-cell snapshot digests are compared;
changed snapshots reset pagination instead of being joined. Original source at/epoch/window values and
service-clock-skew labels are retained. Latest-bucket duplicates/gaps/restarts are not integrated into exact
traffic totals. `framesPerSecond` and `bytesPerSecond` retain their rate units; cumulative frames/bytes,
TLS/billed bytes, service CPU/memory/SQL and inner DO versions remain null/UNAVAILABLE. Drain results keep
UNKNOWN age and never establish emptiness or OWNER.

Five-component versions, source labels and full config digests are **not** echoed from expected bindings
as observed facts. Kernel/Runtime/catalog facts come from the actual ports; other physical identities and
full config/source facts must come from a separate actual provider observation. Missing provider config/
source labels stay null. `judgment:'bounded-execution'` can collect bounded diagnostic workload evidence
when actual monitor reads and cost budgeting are usable, while `full-capacity` refuses the still-missing
history/service/adoption facts. The v1 adapter always reports full capacity NOT_ELIGIBLE; no callbacks,
partial page coverage or successful bounded execution turn that into PASS.

Cost is a separate private `providerFile`, refreshed by an actual reviewed collector. Its closed schema is
`{v:1,resourceVersions,identityAsOfUnixMs,sourceSha,configurationSha256,window,facts,attribution}`; `facts`
and `attribution` are hashed evidence files. The observed window is
`{id,startUnixMs,asOfUnixMs,baselineCostUsd,totalCostUsd,coverage}`. Its id must match the explicit provider
window binding; caller runId is correlation only, never ownership authority. Baseline id/start/value are
immutable through updates. For DATA arithmetic, a lifetime cost of900USD with baseline900USD has run delta0 (not a measured bill): no
completed future-run cost is required. The budget uses only `totalCostUsd-baselineCostUsd`, emits update
deltas once, rejects regressions/stale data and requires COMPLETE_TO_ASOF coverage. Incomplete/missing
collector facts refuse eligibility. Full identity/source or invoice facts unavailable from actual evidence
remain UNKNOWN. Delayed unmetered tail remains UNKNOWN: this is an observed run-delta stop, not a final
invoice ceiling or a cost estimate from sockets.

Generator CPU, RSS/heap, event-loop lag, FDs and application-frame bytes are sampled from actual local ports;
application bytes exclude TLS/billed network overhead. They are never serving-isolate memory measurements.
Before sends, aggregate native `bufferedAmount` is checked against the declared queue-byte ceiling; inspecting
owned sockets is generator work and can itself saturate the producer. Queued bytes do not prove a heap cap.
Repeat/split generators and repeat gate/bracket points later to distinguish generator saturation from service
limits; no workload/cost curve or safe maximum user count is claimed by these source tests.

## Source verification

Run under the normal resource helper/headroom gate, with existing dependencies and acquisition disabled:

```sh
cd cloudflare
pnpm exec vitest run --config conformance/capacity/vitest.native.config.mjs
COREPACK_ENABLE_NETWORK=0 pnpm exec vitest run --config conformance/capacity/vitest.config.mjs
pnpm exec tsc -p conformance/capacity/tsconfig.json
node --test conformance/test/*.test.mjs
```

For local qualification, `GONVEX_CAPACITY_APP_REFS` points to the immutable cached real `api.ts`; without it,
tests use a frozen selected-descriptor fixture. `source-audit.json` records actual source/file hashes and the
cached compiled SDK's missing receipt method despite its identical version string. Current SDK public calls
are typechecked directly against source. The Node tests use inert transports/DATA only, not an assembled
target. Earlier author alias/auth-fixture corrections remain in local logs, not rewritten as runtime evidence.

The native capture/Node-adapter sequencing and final generators/types/cycle/bundle commands are in
`/mnt/data/work/gonvex-x01/assembled-load-driver-20261117/check-v1.sh` and `qualify471.sh`. They use the
normal resource/headroom gates. One author launch incorrectly continued after a PSI headroom refusal;
the unit had exited before the stop request. That attempt is retained and not credited as qualification;
the helper now exits on gate failure and subsequent qualification is separately gated. No other jobs/caps
were changed. Parity and the clean30-minute transport gate take priority over later capacity execution.
