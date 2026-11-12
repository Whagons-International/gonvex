# Cloudflare backend: uptime, tracing and operational proof

Owner request: 2026-11-12. Status: accepted planning requirement; monitors, alerts and status page are not yet deployed.

This extends the current Gonvex `docs/cloudflare/` plan on `cloudflare/integration`. That live branch is authoritative; older local planning snapshots do not authorize staging merges. Integrate this scope into M05, K15, R01, V02, V05, X01 and the staging-readiness ledger, preserving their existing requirements.

## Outcome and scope

Measure whether a user can authenticate, create or update a task, obtain a durable confirmation, and see the committed change in a second authorized client. An HTTP 200 alone does not establish availability. Track regional availability, component availability, task-operation success and latency separately.

Prepare the checks, monitoring configuration, dashboards and runbooks now. Prove them against the isolated assembled Cloudflare development deployment. Live staging activation belongs to the separately authorized migration; this plan does not authorize production synthetic writes, a live backend switch, a new paid subscription, DNS publication or unsolicited incident messages.

## External monitoring

Use an external execution provider outside the application's Cloudflare account and serving path. Checkly is the preferred candidate for the existing TypeScript/Playwright checks; inspect existing access, actual plan features, supported Playwright options and cost before selecting its production configuration. Keep the scenarios portable. Better Stack remains an alternative, not a second mandatory purchase.

Version monitor definitions and environment mappings in the repository. Important checks run from at least three geographic locations; include the Americas locations required by R01. Record actual runner/provider/location rather than assuming independent failure domains from a region label. State how monitor scheduling, retries and alert confirmation affect detection latency.

| Check | Assertion | Initial planned cadence |
|---|---|---|
| Web | DNS/TLS and real application bootstrap succeed | 60 seconds |
| Authenticated API | Valid test identity can read expected scoped data; response semantics checked | 60 seconds |
| Tenant database | Authenticated application operation reaches that tenant's real SQLite path | 60 seconds |
| Realtime | SDK authenticates over WebSocket and receives the expected protocol response | 60 seconds |
| Durable task + sync | Client A commits a unique synthetic task/edit; client B observes matching fields and revision; a fresh read/reload confirms persistence | 5 minutes |
| Browser workflow | Real login, task create/update, read and second-session synchronization, then cleanup | 5 minutes |
| Files | Private upload/download matches bytes; unauthorized access is denied | Cadence selected from measured cost and risk |
| Background processing | Expected scheduled job reaches durable terminal state within its documented deadline | Per job |
| Monitoring system | Missing external-runner heartbeat or missing check samples is detected independently | Cadence tied to configured checks |

These are proposed operating frequencies from the owner's request, not measured detection guarantees. The database is SQLite in Durable Objects, not PostgreSQL; do not expose a public SQL endpoint or add PostgreSQL merely to satisfy the pasted monitor list.

Use least-privilege dedicated synthetic tenants/accounts, unique operation IDs, bounded data and cleanup/retention. Synthetic activity must not notify customers or trigger external integrations. Test credential renewal and distinguish expired monitor credentials from a general authentication outage. Mark test traffic so it can be separated from customer metrics without silently excluding real failures.

Keep warm-path probes separate from cold-wake experiments: scheduled checks must not accidentally keep the only cold-wake fixture warm and invalidate R01 or hibernation/cost evidence. Production R01 retains its original read-only single-user scope.

## Internal observability and trace continuity

Use Cloudflare Workers/Durable Object metrics, logs and available distributed tracing, plus explicit Gonvex instrumentation for asynchronous WebSocket and commit/delivery work. Verify the deployed runtime's actual propagation; do not assume automatic HTTP spans prove the complete realtime path.

Correlate a task operation across client intent, edge routing, gateway admission, tenant queue, authorization, Reducer execution, SQLite transaction, durable commit/receipt, ordered delivery, and second-client application. Include background Action/Workflow and scheduler hand-offs where relevant. Distinguish queued time, execution time, storage/commit time and receiver delay. Use local monotonic durations and document clock uncertainty for cross-region measurements.

Record release SHA, Worker deployment/version, environment, region, tenant pseudonym, operation ID, gateway/controller identity and routing epoch where useful. Trace IDs are distinct from idempotency keys and never credentials. Preserve one ordered delivery channel and existing protocol compatibility; any additional wire instrumentation requires the existing contract/change process.

Metrics include operation success, durable confirmation and sync p50/p95/p99, classified failures/timeouts, receipt replay/ambiguity, query/storage errors, reconnects, subscription lag, last successful synchronization, scheduler lateness, dead letters, gateway load/queue age, scale-out/scale-in/deferred drains, control/auth pressure, memory/CPU, SQL work and cost.

Set an explicit trace sampling, retention and cardinality budget. Preserve useful failure and slow-operation diagnostics without assuming full trace retention. Scrub credentials, cookies, auth headers, signed URLs, task content and personal data from logs, traces, screenshots and incident packets. Missing telemetry means unknown coverage, never success.

## Availability reporting and alerting

Use 99.9% monthly availability as the initial proposed objective from the owner's request. It is not an achieved result, customer promise or substitute for measured baseline and the existing validation gates.

For each component and region, define the eligible time window, expected samples, maintenance policy, assertion/latency deadline, regional-failure classification, confirmation/recovery policy and incident start/end estimator. Report coverage gaps and the uncertainty introduced by check intervals. Separate first-attempt failures, retries and confirmed incidents; a successful retry must not erase failed task operations or slow-service evidence.

Define task-operation success as eligible attempted create/update operations that receive durable confirmation and reach the second authorized client within the declared deadline. Keep failures, latency and time-based uptime separate; never average component percentages to manufacture an overall availability claim. Include denied-visibility behavior as a correctness assertion, not an availability success for an operation that was supposed to succeed.

Alerts must cover unavailable/degraded HTTP or auth, failed durable writes, stalled sync, overdue/dead-letter jobs, storage/resource exhaustion and monitoring-system failure. Keep M05's original five paging-condition drills intact and add these coverage requirements explicitly. Use cross-location confirmation, deduplication and recovery notices, while retaining regional-only incidents instead of hiding them behind a global quorum. Specify channels, escalation owner and runbooks before activation; drill delivery only through an authorized test destination.

## Status page

Prepare an independently hosted public status page, with `status.whagons.com` as the proposed name. Track Web, API, Synchronization, File Uploads and Background Processing separately. Verify its real DNS/CDN/hosting dependencies; independence is not established by a different hostname. Preserve an operator update path during a Cloudflare/application outage. Keep incident content free of tenant/private details. Domain publication and public incident announcements are separate deployment actions, not completed by this plan.

## Required evidence before staging readiness

1. Run the real assembled dev deployment through the external API and browser scenarios with exact source/deployment/fixture provenance and bounded fixture cleanup.
2. Inject scoped failures: site/route failure, invalid or unavailable auth, SQLite write rejection/rollback, HTTP-healthy but blocked replica delivery, overdue job, file-storage unavailability, and missing monitor heartbeat. Verify the correct component/region signal and recovery; no lost or duplicate user writes.
3. Demonstrate a task operation's trace/correlation from client A through durable commit to client B, including an autoscale drain/reconnect or deploy boundary. Name any uninstrumented segment.
4. Measure detection time, confirmation time, notification delivery to a test destination, recovery time, sampling/coverage gaps and run cost. Do not infer these from configured cadence alone.
5. Demonstrate a replayable availability report and incident record from raw monitor results, with component/regional accounting and task-operation success separate.
6. Prepare monitoring-as-code, credential ownership/rotation, deployment mapping, alert runbooks, dashboards, independent status-page configuration and an activation/rollback checklist for the staging candidate.

Implementation fixtures and short failure drills do not establish 24/7 coverage or a month of uptime. Maintain actual-duration observation for soak/shadow gates. Report each part as planned, implemented, drilled, continuously observed or still blocked.

## Current source references

- [Canonical Cloudflare brief](https://github.com/Whagons-International/gonvex/blob/cloudflare/integration/docs/cloudflare/AGENT_BRIEF.md)
- [Canonical packages and gates](https://github.com/Whagons-International/gonvex/blob/cloudflare/integration/docs/cloudflare/work-packages.json)
- [Checkly Playwright support and configuration limits](https://www.checklyhq.com/docs/detect/synthetic-monitoring/browser-checks/playwright-support/)
- [Cloudflare Workers tracing](https://developers.cloudflare.com/workers/observability/traces/)

Provider capabilities were checked on 2026-11-12. Validate the selected account and current documentation again during implementation.

## Lead implementation sequence and gate ownership

Planning revision: 2026-11-12. Canonical source is this file on the integration planning branch;
coordinator draft was read from the supplied Mac scratch path. Everything here is **planned** unless
an evidence record explicitly says otherwise. No monitor, vendor account, alert destination, status page,
DNS record or live tracing configuration was created/activated by this revision. K22 and X01 stay focused
on their existing implementations; no new child is assigned merely to write/acknowledge this plan.

| Owner/package | Existing gate preserved | Added evidence responsibility / sequence |
|---|---|---|
| Lead | Exact-artifact review, contract and staging-readiness evidence | Own this plan, scope boundaries, activation dependencies, ledger and fresh verification tasks |
| K15 | Error triage routes return the same groups as Rust for the same input | Keep grouping/scrubbing/rate-limit/retention behavior; separate new correlation diagnostics from those contracts |
| R01 | Two weeks per environment, at least two Americas locations, unchanged probe on kernel | Keep production read-only single-user scope; separate warm monitor tenants from cold/hibernation fixtures |
| V02 | App Playwright, module and compatibility suites pass against preview | Portable real browser/SDK task and second-client paths against exact assembled dev artifact |
| V05 | Deploy under load, snapshot eviction, failed alarm, unavailable R2 and unavailable Control end with no lost/duplicate writes | Add scoped route/auth/write/sync/job/file/monitor failures and record detection/recovery; retain all original drills |
| M05 | Each of the original five paging conditions fires in a drill | Implement monitoring-as-code, structured correlation/dashboard/runbooks and authorized test-alert evidence |
| X01 + V03/V04 | Original autoscale/load/budget gates remain | Prove correlation across placement epoch/drain/reconnect/deploy, quantify telemetry overhead and shared Control pressure |

Sequence: prepare portable code/config and failure-run manifests independently; integrate K22+X01; instrument
and run exact development artifact checks/drills with bounded fixtures; fresh independent verification;
prepare reviewable monitoring/status activation and rollback. Live staging/production activation, external
subscription spending/new accounts/DNS publication and real paging destinations remain separate dependencies.
Do not represent a passing development drill as continuously observed service or a completed monthly/soak gate.

M05 definition gap: current repository work-packages.json says "five paging conditions" without enumerating
their identities/thresholds. Preserve five source-linked slots P1–P5 and recover their canonical design
source before claiming M05 passes. The additional coverage below does not replace those five or silently
invent their original meanings. This source audit is independent preparation, not a reason to stop code work. See [the exact original-source audit](verification/M05-original-paging-source-audit.md) for the import commit, coordinator copies and inaccessible design-body reference.

## Vendor/access, portability and cost audit (2026-11-12)

**Checkly remains preferred, not selected/activated.** Its hosted Browser checks accept JS/TS and only a
subset of Playwright configuration. In particular projects/global setup/teardown/storageState and test-level
retry/sharding cannot simply be copied from the app suite. Build portable scenario functions with explicit
setup/cleanup and provider adapters; validate runner/node/browser versions, dependencies, SDK WebSocket
support, trace headers, credentials and actual per-location schedules on the existing account before use.
Check interval is not per-region cadence when the scheduler rotates locations. Require three geographically
distinct runners including two Americas locations; use explicit regional definitions if necessary.
[Checkly supported Playwright configuration](https://www.checklyhq.com/docs/detect/synthetic-monitoring/browser-checks/playwright-support/).

Public Checkly pricing currently lists Hobby with 1,000 browser/10,000 API runs; Starter $24/month equivalent
billed annually with 3,000/25,000; Team $64 equivalent billed annually with 12,000/100,000. Browser add-ons
are listed at $4 per 1,000 and API add-ons $1.80 per 10,000; automatic overage rates differ. Product category,
regional billing, retries, status pages, taxes, contract and actual account pricing must be verified.
For a 30-day proposal, one check at 60 seconds in each of three regions means 129,600 location-executions;
one at five minutes means 25,920. A bootstrap browser plus five-minute browser workflow would therefore
schedule 155,520 browser executions before retries; included runs alone do not cover that proposal. Cost
formula: base + billable regional runs by check type/add-on or overage + backend/telemetry/retention/notification
cost. Publish a configuration-specific quote and hard budget before activation; no purchase is authorized.
[Checkly pricing](https://www.checklyhq.com/pricing/).

**Better Stack is an alternative.** Its public uptime page advertises a free tier with ten monitors,
ten heartbeats, a status page and three-minute checks, plus Playwright transaction monitoring. This does
not establish that the existing account supports the proposed 60-second/three-region/browser schedule.
[Better Stack uptime](https://betterstack.com/uptime). The attempted separate uptime-pricing URL was
unavailable during this audit; paid quote and actual account entitlement remain unverified.

No Checkly/Better Stack credential name was exposed by the current read-only Vault discovery. An existing
Whagons monitor-debug credential name exists, but was not read or assumed to be vendor access. Existing
browser-account access/plan entitlements, external runner/heartbeat host and alert permissions remain
**unverified dependencies**. Do not create an account, activate a trial/paid plan or reuse an unrelated
credential. Portable code, schemas, replay reports, dashboard definitions and runbooks can proceed offline.

## Monitor contract and bounded synthetic data

Definitions carry check/component/environment/region, release/deploy IDs, expected schedule, assertion and
latency deadlines, maximum concurrent runs/retries, credential owner and explicit side-effect budget.
Proposed implementation layout: cloudflare/observability/{checks,providers,correlation,reports,runbooks};
shared portable scenarios reference the real SDK/application paths from V02, not mocked deployment handlers.
A successful application read must reach the named tenant's actual SQLite-backed handler; never expose SQL,
add PostgreSQL, infer DB health from a landing-page 200 or treat a cached read as durable-storage proof.

Every region/run uses synthetic-owned operation IDs and isolated fixtures, with a bounded cleanup ledger.
Keep the monitor tenant separate from copies used for cold wakes/load/soak. Audit task hooks, notifications,
webhooks and integrations so synthetic writes cause no customer/external effects. Use least-privilege identities
for writer, second authorized reader and unauthorized file reader; no operator/admin key in public checks.
Store secrets through the approved provider/Vault path with rotation/expiry drills; never commit them.

Proposed initial dev-only execution envelope, to be calibrated by measured cost (not a proven safe capacity):
no overlapping run per region/check fixture, at most one initial retry, file probes use <=1 KiB synthetic
content at most once/15 minutes/region, <=100 retained synthetic task/file fixtures per monitor tenant and
cleanup expiry <=24 hours. If cleanup or budget fails, suspend new synthetic writes, report monitor coverage
unknown/degraded and alert the authorized test sink. Never keep generating unbounded data or silently mark
paused checks successful. Job checks use each job's documented deadline plus recorded scheduler/clock tolerance;
do not replace job deadlines with a universal five-minute timeout. Record explicit assertion deadlines before
running each check; preserve the stricter V03 latency criteria as separate performance gates.

## Correlation and telemetry budget

Cloudflare currently documents automatic fetch/binding/RPC/handler spans and optional custom spans. Tracing
requires its explicit tracing setting; logs enabled alone are not proof of traces. Inspect actual deployed
configuration/compatibility and sampled spans, rather than enabling or assuming it in this planning commit.
[Workers tracing](https://developers.cloudflare.com/workers/observability/traces/).

Use an internal, versioned correlation envelope/link scheme at async boundaries. It must join browser intent,
edge request, pre-auth placement/admission, gateway serial queue, tenant queue, auth decision, Reducer, SQL
transaction, durable commit/receipt, ordered query+replica delivery and second-client applied revision. Carry
links through Action claim/Workflow step/scheduler/outbox transitions, not only HTTP parent spans. Retries share
an operation identifier but have separate attempts/spans; trace ID is never an idempotency/receipt key.
If a public wire extension is needed, use the contract process and preserve old SDK/bridge behavior. Tenant
database/wire identifiers are distinct from sanitized correlation aliases. Correlation cannot change write,
visibility, acknowledgement, ordering or replay semantics.

Measure these separately: client intent-to-dispatch, edge/admission and RPC wait, gateway/tenant queue,
auth/Reducer execution, SQL transaction+durable commit, writer durable confirmation and receiver application.
Use monotonic clocks within a process; for cross-process/region endpoints record offset and uncertainty,
not a subtraction of unsynchronized wall clocks. Require trace/receipt evidence of a durable commit and
receiver-applied revision, not optimistic UI echo or merely a gateway send completion.

Proposed dev observation budget: sample 1% of healthy operation traces; retain all outcome counters and
attempt bounded failure/slow diagnostics, without promising full failure traces after head sampling. Cap
export at 20,000 spans/20 MiB per deployment/day, <=64 spans and 1 KiB safe attributes per trace segment;
record dropped/sampled/export-failed counts explicitly. Begin with seven-day diagnostic retention, 35-day
redacted raw monitor results (for monthly replay) and 90-day aggregate availability/incident reports. Check
actual vendor/Workers retention and cost before adopting those values. These are initial experiment caps,
not measured production needs, and do not change K15's existing retention/rate-limit contract.

Metrics labels are bounded component/environment/region/release class/failure class; operation/trace IDs,
tenant/gateway/controller aliases and epochs belong in bounded logs/trace records, not unbounded time-series
labels. Do not export task/comment contents, emails, credentials/cookies/auth headers, signed URLs, raw
customer IDs or query arguments. Browser traces/screenshots require redaction/synthetic-only data. Track
sampling/retention/export cardinality and cost alongside V04; a missing signal/heartbeat is UNKNOWN, never green.

## Availability and task-operation accounting rules

99.9% monthly is **owner-proposed**, not achieved uptime, an SLA or a customer commitment. For a full eligible
30-day month it would permit 43.2 minutes of downtime; real eligible windows and unknown coverage must be shown.
Compute each component AND each region independently, with explicit deployment/location/version provenance.
Record all expected scheduled samples, actual first attempts, retries, timeouts, missing/late samples and
monitor-provider outages. Preserve failures even when retries succeed. Maintain both sample success and
interval-based eligible-time estimates; they are not interchangeable metrics.

Predeclare any authorized maintenance interval; publish inclusive availability alongside any maintenance-adjusted
view and never remove an incident retrospectively. Keep coverage unknown separately. For eligible time E,
proven healthy H and unknown U, publish lower/upper availability bounds H/E and (H+U)/E; only apply this to
intervals classified under the declared estimator. Adjacent samples bracket the event rather than proving
continuous health. Record onset/recovery bounds from last success/first failure/last failure/first success;
check interval, execution deadline, scheduling lag, retries and clock error bound detection uncertainty.

Initial confirmation proposal: one failing first attempt records a regional symptom immediately; one retry
can classify transient versus persistent, and two successive healthy regional checks confirm recovery.
Cross-region checks can corroborate broad incidents but may not veto a region-only outage. Raw first-failure
and first-recovery times remain visible, and these confirmation rules are proposals to drill, not detection
guarantees. No averaging Web/API/Sync/Files/Background percentages into an overall success number.

Task-operation success has its own denominator: all eligible attempted creates/updates, including timeout
and receipt-ambiguous attempts. An attempt succeeds only if durable confirmation AND the second authorized
client's matching applied result arrive within the declared operation deadline. Later receipt reconciliation
is a separate recovery outcome, not a rewrite of the original failed/late attempt. Keep operation success,
regional time availability, correctness and latency distributions separate; account for expected operation
scheduling gaps and latency censoring. Unauthorized access denied is a security assertion, not success for
an authorized operation that should have completed.

## Alert/drill matrix, runbooks and test delivery

Retain source-linked original M05 conditions P1–P5 (canonical identities still to be recovered). Add:

| Injected scoped dev failure | Expected evidence/component | Recovery/runbook |
|---|---|---|
| Route/DNS/TLS/bootstrap or auth refusal/outage | Web/API, regional first attempt and confirmation | Check external reachability vs serving/auth vs monitor credentials |
| SQLite write rejection/rollback or receipt ambiguity | Failed durable task, error class/receipt state; no false success | Inspect real transaction/receipt, retry only under existing idempotency contract |
| HTTP 200 but blocked ordered delivery | Sync/receiver deadline, writer durable success separately | Inspect gateway/controller epoch/queue, resubscribe/reset/drain and final digest |
| Failed/late alarm, overdue/dead-letter Action/Workflow | Background deadline and terminal-state record | Scheduler lease/outbox/runbook; never double-execute to silence a page |
| File dependency unavailable or storage/resource exhausted | Files or resource component; ownership/byte checksum preserved | R2/SQLite/Container-specific recovery; distinguish DB and file bytes |
| Missing check/runner/export/monitor heartbeat | Monitoring outage/UNKNOWN, not application green | Independent external heartbeat witness and operator fallback |

Dedupe by environment/component/region/failure class and stable incident ID, preserving individual attempts
and related-operation links. Define page owner, acknowledgement deadline, escalation order and recovery rule
in configuration before activation. Use an explicitly authorized test destination/recipient for drills;
prepare a sink adapter and dry-run payloads now but do not send unsolicited email/Slack/SMS/webhooks. Actual
notification delivery/receipt and recovery must be measured when authorized; a queued API call or local mock
is not proof. Keep first detection, confirmation, test-notification receipt and recovery timestamps separate.

## Independent status and activation/rollback dependencies

Proposed components: Web, API, Sync, Files and Background. Prepare the status provider definition, operator
update runbook and external fallback hostname; do not publish status.whagons.com or incident announcements.
Read-only DNS observation on 2026-11-12 found Cloudflare authoritative nameservers for whagons.com and no
CNAME answer for status.whagons.com; absence of a CNAME is not proof no other record/service exists. A status
hostname under this zone inherits a DNS dependency even if its hosting is elsewhere. Evaluate separately
hosted status service, DNS delegation/TTL behavior and provider-owned fallback reachable without that zone;
verify actual DNS/CDN/host/account dependencies and the operator's update/login path during Cloudflare failure.
Do not assume delegation removes every parent-zone dependency or store operator secrets in the runbook.

Staging activation package must include reviewable monitoring-as-code/env mappings, account entitlement and
cost evidence, synthetic identities/rotation/cleanup ownership, dashboards, per-component regional reports,
source-linked M05 conditions, tested alert runbooks, independent status configuration and a rollout/rollback
checklist. Rollback pauses write monitors and retains raw history, disables only newly enabled tracing/exports,
restores prior schedules/config and removes only owned dev fixtures; it never erases incidents to restore green.
A separate authorization is needed before live monitor/DNS/status activation, account spending/new accounts
or production writes. Current production R01 remains read-only. Keep real 14-day/30-day gates unchanged.

## Evidence-state checklist

| Item | State at this planning commit | Completion artifact |
|---|---|---|
| Mac draft + canonical requirements + public documentation | planned / source audit done | This file and decision 0055 |
| Vendor account/entitlements, regional scheduling and independent heartbeat host | blocked/unverified | Read-only account observation + reviewed budget/ownership |
| Portable scenarios/config/dashboard/runbooks | planned | Source commit and provider portability test results |
| Actual assembled external check + cross-autoscale trace continuity | planned | Exact deploy/config/fixture/region IDs, raw checks/spans and path-gap matrix |
| Fault/test-notification/recovery drills | planned; no live notification permission | Raw outcomes and measured detection/confirmation/delivery/recovery/cost |
| Regional/component/task-operation replay report | planned | Deterministic replay commands + raw evidence + coverage/uncertainty report |
| Independent status/activation rollback | planned; publication unapproved | Dependency audit and reviewable package |
| 24/7 or monthly observed uptime / soak/shadow | not established | Actual-duration evidence; no synthetic replacement |
