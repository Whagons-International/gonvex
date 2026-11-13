# M05 monitoring and tracing foundation

This is an independent, disabled foundation. It imports no serving code and activates nothing.
M05 remains stage 2 / wave 4, depends **only** on P01, with the original done_when:
**“Each of the five paging conditions fires in a drill”.** P1–P5 remain unresolved, source-linked
in `providers/operations.mjs` and [the source audit](../../docs/cloudflare/verification/M05-original-paging-source-audit.md).
Added conditions never replace them. Uptime/tracing is still required for the initial staging candidate.
99.9% monthly, cadences and budgets are proposals, not measurements or an SLA.

## Run offline

Node >=22.13 (CI: Node 24), no install or credentials. Arch3 tests must use the registered
`/mnt/data/work/gonvex-m05-foundation` root and normal headroom/resource admission. Example, after admission:

```sh
M05_TEST_ROOT=/mnt/data/work/gonvex-m05-foundation/tmp node --test cloudflare/observability/test/*.test.mjs
node cloudflare/observability/cli.mjs audit
node cloudflare/observability/cli.mjs estimate
node cloudflare/observability/cli.mjs replay cloudflare/observability/fixtures/replay.json
node scripts/sync-sqlite-schema-options.mjs --check
```

On a hosted GitHub runner, tests use its `RUNNER_TEMP/m05-foundation`, deleting their own SQLite fixtures
in `finally`. CI retains the existing conformance tests and adds this suite; either failing command fails
the step. No shared dependencies, symlinks, package metadata or lockfiles are written. The test SQLite
fixture is a **real Node SQLite application-handler fixture**, not Whagons, SDK, workerd, remote tenant,
three regions or continuous monitoring. It creates no tenant/account and sends no messages.

## Bind the real application later

At framework `5c5fa3795d821d5995179e19b712c89bfa0eb8d8`, the general proposed
`reducerWithCommitEvidence` port is **unsupported** from ordinary result plus receipt lookup.
The audit now reports `command-bound-commit-cursor-unavailable` and `executionAllowed:false`.
Receipt lookup authenticates the subject/key/path and atomically observes completion, then
returns the current clock fence, which can advance after unrelated writes. That recovery
contract remains intact. It does not supply the original command's full commit cursor.
The historical proposal below is preserved, not authorized for implementation. See the
[scoped decision and actual native witness](../../docs/cloudflare/verification/M05-commit-evidence-contract/report.md).
No SDK stub, private-field access, later-epoch/older-revision pairing or capability assertion
fills this gap. Fresh tenant-SQLite read and a separate authorized same-epoch applied client
remain two independent missing prerequisites.

The [Whagons task preparation](bindings/whagons-task.json) pins cached app source
`73276b5589d363c1190a6d028413a21559226631` separately from framework source
`8851a125dbd7316e6de30717bbcaf807ee2f5eb7`. It contains function/argument metadata,
source blob hashes and missing prerequisites, with no executable binding or credentials.

```sh
node cloudflare/observability/cli.mjs binding-audit
# Optional: audit declaration-only JSON with the same shape as proposedTaskBinding().
node cloudflare/observability/cli.mjs binding-audit declarations.json
```

The command always exits nonzero: **2** for a BLOCKED report, **1** for unreadable,
oversized or malformed input. A consistent source plan is not an app success. Arbitrary input
values are never echoed; only fixed issue codes and expected source pins leave the audit.
Claiming configured prerequisites cannot enable it. This command performs no SDK/network/app call.
See [the source/binding report](../../docs/cloudflare/verification/M05-binding-preparation/report.md)
and [the proposed public evidence port](../../docs/cloudflare/verification/M05-binding-preparation/port-proposal.md).
`tasks.get` is query-kind **and replica-delivery**. `sdkApplication` refuses that fresh-read
mapping before connecting; never strip generated delivery metadata to make it fit `query.call`.
The public SDK evidence proposal remains separate, unimplemented work requiring lead coordination.

`providers/dispatch.mjs` runs portable functions with explicit `context`, bindings and a serialized guard.
There is no execute-live CLI, default origin, login, identity or selector. The shipped configuration has
null app bindings and is **BLOCKED** for real Whagons. Never replace these gaps with the test fixture.

- Context names actual source SHA, config SHA, environment, deployment, executor and actual region.
  Local results say `local`, never pretend to run in the three proposed hosted locations.
- `sdk.mjs` calls the existing public SDK `query`, `reducer`, `watchReplica`, `localReplicaState` and
  `localReplicaResult`; `connect` must use real application auth, fresh isolated clients, no operator
  permission, no optimistic/local execution and no external effects. Supply generated function refs,
  bounded argument builders and exact synthetic-field matcher. The adapter does not export app values.
- Supply **audited real SQLite handler** read evidence and actual durable-receipt revision evidence.
  SDK `reducer()` returns an app result; it does not expose `committedRevision` as a public result.
  `durableEvidence` is an explicit binding gap, not permission to invent a revision from a response.
  Review any needed shared hook with lead first. Do not inspect SDK private fields or change protocol.
- Reader starts before writer, is a different authorized identity/client, and must apply matching fields
  with a complete, up-to-date collection at the commit revision. A sink send or HTTP span proves neither
  durable commit nor second-client application. Fresh server Query/reload must verify persistence.
- `browser.mjs` performs actual form login in two contexts. `bindPages` must use real UI create/edit/save
  controls plus real second-page applied state and fresh reload/SQLite proof. Missing paths/selectors or
  the observation bridge block execution. No hidden auth bypass or setting a readiness flag to pass.
  The caller owns browser lifetime: launch headless with `args:['--disable-gpu']`, close in `finally`.
- HTTP/DNS/TLS/HTML bootstrap have distinct functions in `network.mjs`; HTML marker alone does not prove JS
  bootstrap. Browser-bootstrap needs actual app readiness. Redirects are refused, no arbitrary headers,
  TLS validation is enabled, HTML bytes capped. DNS resolution may outlive caller cancellation; this
  has no synthetic write effects, and result remains failed/unknown when deadline expires.

Checkly adapter injects constructors from a later **owned pinned** install. Every construct is disabled,
alertChannels empty/muted, explicit same-region single retry after 5s, one location per construct (36 definitions, including two Americas locations).
Only the `http` check uses native API GET. Every semantic/DNS/TLS/bootstrap/SDK/browser check requires an
explicit scenario entrypoint; absence blocks instead of substituting HTTP 200. The adapter is locally
tested against a constructor boundary, **not installed/hosted vendor validation**. Use `dispatch` inside
an actual provider spec and fail the test unless `raw.status === 'pass'`; never let BLOCKED return green.
Provider wrappers must export raw results even on assertion failure, via a separately authorized sink.
Checkly supported JS/TS/fixtures and unsupported projects/global setup/teardown/storageState/test retries/
sharding were verified against [official docs](https://www.checklyhq.com/docs/detect/synthetic-monitoring/browser-checks/playwright-support/)
on 2026-11-12. [Construct reference](https://www.checklyhq.com/docs/constructs/browser-check/) confirms
`activated`, `locations`, `frequency`, entrypoints and alert channels. Actual account, pinned CLI/runtime,
Node/browser versions, packaging/SDK WebSocket, scheduling and plan/access remain unverified. No login,
trial, subscription, provider account discovery or provisioning was needed for this offline work.

## Synthetic identity, cleanup and clocks

Dedicated synthetic tenant; separate least-privilege writer, authorized reader and unauthorized file
reader, no operator credential. Only `local-synthetic` / `dev-synthetic` environments; explicit origin
allowlist and audited hooks/notification/webhook/integration suppression are prerequisites. Those are
configuration assertions requiring real external verification, not a substitute for application auth.
Warm fixture cannot be the cold/hibernation fixture. Do not provision any identities from this package.

Operation UUID, trace ID and idempotency UUID are independent. SDK allocates its own command key; this
adapter does not claim to override it or inject new public wire fields. Correlation hook/receipt binding
needs coordinated lead review. Every create/update begins a failed operation record **before** invocation;
it only becomes successful after durable confirmation, second applied result and fresh persistence.
Lost replies/timeouts stay failed even after reconciliation. IDs belong in bounded logs, never labels.

`WriteGuard` reserves both writes and fixture before effects, awaits ledger persistence, refuses overlap,
old fixtures (24h), >=100 retained fixtures, failed cleanup or exhausted write budget. Set a measured
finite `remainingWrites`; it never replenishes automatically. Its state and save callback must be owned,
serialized and shared across **all** regional writers (singleflight lease required), durable across
runner restarts. Tests persist a real ledger locally; distributed hosted persistence is **not configured**.
A timeout pauses writes and leaves unresolved work charged; a thrown caller timeout cannot prove settlement.
Adapters must honor AbortSignal and settle/close owned clients. Cleanup is separately bounded; cleanup
failure remains UNKNOWN with retained ledger, never silently successful paused coverage. A permanently
hung adapter needs external executor termination/independent heartbeat; no resources are declared settled.

File probes require <=1KiB actual bytes, >=15min declared interval, byte equality and unauthorized reader
zero-byte denial, plus ledger/cleanup. The scheduler must enforce the interval, not merely set the field.
Job observations require documented **per-job** deadline/source and durable completed state. They do not
create a job or silently replace its deadline with five minutes. Job/file actual application bindings
remain absent. No customer data/credentials in fixtures or error output.

## Correlation, exports and reporting

`correlation/envelope.mjs` is an internal v1 allowlisted envelope. Fixed stages cover browser -> edge ->
admission -> gateway/tenant queues -> auth/Reducer -> SQLite transaction/commit/receipt -> ordered delivery
-> **second-client applied**, plus Action/Workflow/scheduler/outbox links. `attachments.json` marks every
serving attachment **planned/uninstrumented**, preserving K15/K22 and decisions 0064/0069–0077.
No built-in HTTP span is treated as complete realtime correlation. HMAC aliases require a separate secret
with rotation ownership; raw tenant/gateway/customer IDs, owner tokens, args, contents, PII, cookies,
headers, signed URLs and error strings are excluded. Numeric/enum attributes only; aliases never labels.

Local durations are monotonic; cross-clock endpoints retain offset uncertainty including negative lower
bounds rather than subtracting unsynchronized clocks as latency. 1% deterministic healthy head sampling,
bounded slow/failure diagnostics, <=64 spans/segment and 1KiB safe attributes, <=20k spans/20MiB per deploy/day,
7d diagnostics/35d raw/90d aggregates are **safeguards to calibrate**, not measured needs or altered K15 limits.
TraceBudget requires one persisted serialized deployment/day ledger. It reserves bytes/spans before sends,
counts outcome/sampled/dropped/export-failed/exported records, never refunds failed export, and stops on
ledger failure. Day rotation, actual retention deletion, exports and independent heartbeat are **not
activated**; an external owner must supply/verify them. Failure sampling cannot recover absent head spans.

`reports/replay.mjs` validates the v1 raw schema (including rejecting unknown fields) and replays each
check/component/region/deploy separately with exact source/config provenance. Input has `schedule`,
`records`, independent `heartbeat`, predeclared `maintenance` and `asOf`. See the runnable replay fixture,
which is explicitly adverse **fixture data**, not uptime observations. Eligible windows cannot extend past
asOf. Missing, late, paused, provider-heartbeat loss and export-failed coverage remain UNKNOWN. First
failures survive retries; no quorum or component average manufactures global uptime. The slot-hold time
estimator reports H/E .. (H+U)/E; adjacent samples never prove continuous health. Inclusive and adjusted
maintenance views stay visible, including partial slot overlaps. Expected samples and operation attempted/
success/failed denominators remain separate, with scheduling gaps visible. Only durable AND second-applied
within the declared deadline succeeds. Reconciliation is separate recovery data, never an overwrite.

Incident first failures, retry confirmations, first recovery and two-healthy-observation recovery are
separate from schedule/deadline/clock uncertainty bounds. Notification receipt stays unobserved. A report
with missing samples is not green. Raw-result schema is the executable strict `validateRaw` function;
only fixed fields, bounded synthetic UUIDs, two operation records and safe provenance/code strings enter.
Caller must validate before retaining any third-party output. No arbitrary error/body/header export.

`cli estimate` prints per-check/per-region scheduled/max-retry runs for an explicit 30-day proposal, with
cost **null** until a verified quote supplies plan, dated primary source, currency, base, included runs,
per-run rates, and Cloudflare/notification upper bounds. No guessed vendor/billing price. Categories are
conservative browser executions for code-driven semantic checks; verify actual billing before activation.
Per-tenant serving costs and rows/CPU/memory attribution require real V04 measurements, not this estimator.

## Evidence and next release

[Local evidence report](../../docs/cloudflare/verification/M05-foundation/report.md) identifies hashes,
runtime, checks, real boundary and non-executed scope. `operations.mjs` contains disabled Analytics Engine,
health sweep, dashboard/status/alert-test payload and activation/rollback definitions. There is no sender,
public status page or resources/bindings. The runbook names the remaining owner/test-destination dependencies.
Lead owns integration, fresh review, activation and every original V02/V05/X01/V03/V04/R01/14d/30d gate.

Provider retry configuration follows the [official RetryStrategyBuilder documentation](https://www.checklyhq.com/docs/constructs/retry-strategy/); the default can retry a different region, so this adapter explicitly uses sameRegion. Five-second retry backoff is included in the proposed confirmation bound. Checkly ignores launchOptions/most Chromium args: the arch3 GPU flag requirement applies to local launches; no hosted launch compatibility is assumed. Hosted automatic traces/video can capture login/fixture data: verify vendor redaction/retention before uploading any secret-bearing browser flow.

## Revision after the 445695ee review

The original report/raw records remain immutable under `docs/cloudflare/verification/M05-foundation/`.
New revision evidence is appended separately. The three foundation fixes enforce monotonic completion
elapsed time even when synchronous work starves the timer, account actual operation attempts independently
of schedule-grid membership, and require finite nonnegative numeric job elapsed evidence.

`runCheck` records `deadlineExceeded`, actual `settlement` (fulfilled/rejected/unsettled), optional
`settledElapsedMs`, and `cleanupState`. A late result is FAIL even if the timer did not run. `observedStatus`
and `observedCode` retain the pre-enforcement outcome; they do not claim it arrived before the deadline.
`finishedAt` ends the observation, not necessarily underlying work. An abort request never proves cancellation
or cleanup settlement; unresolved work remains charged and new writes pause. Task/file mutator admission
also checks monotonic elapsed time before starting another write. No automatic ambiguous-write retry follows.

Replay keeps expected schedule-slot accounting separate from `recordAccounting` and scoped `observations`.
Every eligible actual create/update contributes to the operation denominator, including off-grid/retry/late
attempts. New operation records have actual `attemptedAt`; historical v1 records use the enclosing check's
actual interval with explicitly counted timing fallback/boundary uncertainty. An unexpected observation
removes affected slots' healthy certificate (UNKNOWN); it does not invent an entire interval of outage.
Exact on-time failed first samples stay failed. Raw symptoms, retries and first failures remain visible.
Attempts unconfirmed at `asOf` are pending until their declared deadline expires, or UNKNOWN at an ambiguous
window boundary; they stay in the denominator without being fabricated successes or premature service
failures. `operationsPending`/`operationsUnknown` explain any difference between attempted and success+failed.
An entire duplicate/malformed/contradictory input report throws an integrity error and emits no availability
object. It must be presented as UNKNOWN/integrity failure, never an empty healthy report. Per-operation
elapsed evidence cannot exceed the enclosing observation or widen the predeclared check deadline.

Independent prior-slot heartbeat matching is unchanged by these fixes; its existing interval-wide witness
window is an unrated input-contract diagnostic in the 445 review, not new heartbeat/hosted proof. Actual
independent scheduling, grace policy and outage drills remain activation dependencies. No reviewed-source
clearance is inferred from author regression tests; lead must arrange a NEW independent review of the pin.

## Revision after the 49399492 recheck

The original445 and corrected493 evidence and both older test files remain immutable. New evidence is
appended under `verification/M05-foundation/revision-r2/`; it is author validation, not independent clearance.

Operation eligibility is computed from **all scoped operations before check-record selection**, using the
actual attempted timestamp and declared clock uncertainty. A potentially in-window attempt at a coarse/
uncertain boundary stays in the denominator even when its enclosing check starts outside the window.
Affected coverage is UNKNOWN rather than an invented full-slot outage; known failed attempts remain failed.
An uncertain boundary cannot certify successful operation membership in the window. Exact out-of-window
attempts remain explicitly outside. Original legacy check-start deadline fallback remains labeled as such.

Raw validation now lives in the dependency-free `checks/raw-result.mjs`, re-exported by replay and used by
`runCheck` **before returning anything eligible for retention/export**. Every present timing field, for
both failed and successful operations, must be a finite nonnegative number bounded by the representable
millisecond domain and its enclosing observation. Missing failure timing remains absent; malformed values
are not silently stripped into a successful/empty report. A malformed producer outcome aborts/pauses new
writes and rejects the entire raw output with a fixed UNKNOWN integrity error. It does not claim cleanup
settlement or manufacture a zero-attempt healthy result. Provider wrappers must preserve that rejection as
UNKNOWN/failed coverage, just as for invalid replay input.

Validation snapshots own data descriptors and returns independent plain records. Accessors, custom
serializers/prototypes, unknown fields and nonnumeric nested timing data cannot survive the boundary.
No raw operation spread is retained by producer or reporting. CLI errors emit only a fixed integrity code,
without parser context, input values, arbitrary provider errors, headers, contents or paths. Consumers
must never serialize rejected input or convert rejection to an empty healthy report.

Every present elapsed value fits within the enclosing monotonic duration. With `attemptedAt`, its relative
proof must also fit the remaining observation, allowing **only the declared clock tolerance**. Contradictory
future application raises integrity UNKNOWN and returns no availability object. Clock-consistent evidence
whose latest possible applied time is beyond `asOf` remains UNKNOWN, not premature success. Legacy v1
records without attempt timestamps still use the explicitly counted enclosing-check fallback. New task
records additionally carry monotonic `attemptedAfterMs`; offset+elapsed must fit the observation exactly.
The producer declares Date.now's one-millisecond quantization plus measured alignment-read time, in addition
to configured clock uncertainty, rather than claiming unsynchronized sub-millisecond wall-clock precision.

All full M05/real application/hosted/trace/drill/status/security/capacity/cost/actual-duration holds remain.
No provider, sender, account, resource or serving instrumentation was activated. A NEW independent recheck
of the resulting pin belongs to lead; completed reviewers are never reused by this implementation.

The generic SDK adapter now refuses generated `localExecution` or `optimistic` refs before authentication and rechecks them before a mutation or cleanup. It passes supported refs unchanged. An explicit `execution:'server'` option remains a proposal; it is not implemented here. The genuine historical Whagons refs and native preparation results are recorded in [M05 real-app evidence](../../docs/cloudflare/verification/M05-real-app-evidence/report.md). Full app compilation and native activation remain blocked, and monitoring remains disabled.
