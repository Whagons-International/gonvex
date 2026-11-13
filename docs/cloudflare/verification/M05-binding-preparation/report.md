# M05 offline Whagons task-binding preparation

This finite implementation adds a disabled source manifest, declaration-only CLI audit and
pre-connect SDK read guard. It performs no application call and creates no binding factory,
tenant, identity, resource, monitor, status page or alert. Local test results below describe
offline validation and existing isolated fixtures only.

| Item | State |
| --- | --- |
| Manifest, audit, privacy/scope checks, read-delivery guard | implemented, disabled, locally tested |
| Public `reducerWithCommitEvidence` | exact proposal only; separate lead ownership/review required |
| Real Whagons/SDK/read/browser path, durable commit and second applied result | BLOCKED / unexecuted |
| Hosted three-region operation, alert drills, serving tracing, cost/security and actual duration | UNMET |
| Continuously observed service | none |

## Immutable inputs

The existing registered checkout was clean on `cf/M05-monitoring-foundation` at
`1b3455bb46132fd4b6cd658d38fb1796cd0d01a9`, expected Gonvex origin, before switching in place
to `cf/M05-whagons-binding-preparation` at locally present integration
`8851a125dbd7316e6de30717bbcaf807ee2f5eb7`. No checkout/worktree was created or moved.
The old branch, evidence and failure history are preserved.

Application input is **cached source**, not verified current staging or any deployment:
[whagons5-client at 73276b](https://github.com/Whagons-International/whagons5-client/tree/73276b5589d363c1190a6d028413a21559226631).
Only immutable Git blobs were copied to the owned build root; the app working tree and dependencies
were never read or changed. The manifest lists every consulted app blob OID, SHA-256 and bytes.
Framework/public SDK input is independently pinned to `8851a125dbd7316e6de30717bbcaf807ee2f5eb7`.
The app package declares Gonvex `0.5.2-sqlite.1`; source compatibility with this integration
framework and any deployed artifact remains unexecuted. Catalog artifact hashes are source
metadata, not deployment or receipt attestations.

## Actual task paths and limits

All app paths below refer to that immutable application commit. The generated API contains
delivery metadata that the agent catalog's `kind: query` alone does not convey.

| Role | Actual reference / declaration | Prepared use and limitation |
| --- | --- | --- |
| Create | `tasks.create`, reducer; `tasks/createCommands.ts` → `createTaskIntent` | Schema requires `name`, `workspaceId`; optional `entityId` is required fresh by the proposed synthetic contract. Result is app data (`any`), not commit evidence. |
| Update | `tasks.rename`, reducer; same module → `updateTask` | Required `taskId`, `name`. Chosen narrow update. `tasks.update` is a real legacy reducer in `bulkCommands.ts` with optional identifiers/arbitrary updates; excluded. |
| Remove | `tasks.remove`, reducer; `bulkCommands.ts` → `archiveTask` | Schema identifiers optional; runtime requires a resolvable reference. Proposed `_id` avoids `id`/`taskId` numeric-string dispatch. Soft archive sets `deletedAt`/`deletedBy`; no physical cleanup proof. |
| Read / second-client collection | `tasks.get`, query kind, **replica delivery**; `replicaQueries.ts` | `{id: string}` targets `_id`, returns an array, `maxRows:1`, `maxBytes:4194304`. `sdkApplication.query` mapping is refused before connection. |
| Other reads | `getByNumericId`, `list`, `grid`, `listWindow` | Numeric ID differs from `_id`; list is broad; grid/window are **live** delivery. They cannot substitute for the bounded replica observation or imply one-shot/SQLite proof. |

Creation requires a nonblank name/workspace and real auth. `taskCreateAuthorization.ts` requires
normal `create-tasks` permission plus workspace visibility, with distinct personal/reporting/template
rules. Category/template defaults and `validateTaskCreationMode` can add real requirements.
`updatePolicy.ts` and `archiveTask` enforce visibility/action permissions (`edit-tasks`, `archive-tasks`).
The preparation proposes a dedicated normal synthetic workspace and forbids extra relationship,
recipient, attachment, provenance and arbitrary update arguments. It does not assert such a workspace
exists or that omitting optional arguments suppresses defaults/effects. A nonblank synthetic prefix
is a matching constraint, never authentication or tenant isolation.

`tasksVisibility` in `replicaQueries.ts` defines real workspace/member/team/share/assignment grants.
Same tenant or path existence does not prove reader authorization. `useTaskPermissions.ts` uses
authenticated membership/grants and `useEntity('tasks', taskId)`; `TaskHistoryDialog.tsx` also selects
by exact task ID. The app runtime-contract test uses exact `_id` array matching and a cross-tenant
negative rename/read, but uses admin fixture setup: it is source provenance only, not a least-privilege
reader drill and was not run here. Proposed monitor matching must check exact `_id`, workspace,
expected synthetic name and expected archive state locally, without exporting row values.

## Side effects, cleanup and SDK boundaries

`createPolicy.ts` invokes task logs, defaults/lifecycle/approval, audience, SLA, gamification,
workflow, guest-report and auto-tagging paths. `createTaskIntent` schedules
`tasks.notifyIfUnassigned` after 3000 ms when no assignees. `updatePolicy.ts` invokes task-change
notifications (including name edits), logs and conditional status/custom-field workflow/guest paths.
`archiveTask` releases supplies and synchronizes finding/cleaning-task state while retaining rows.
No public synthetic suppression argument or audited isolated configuration has been proven.
Do not forge `createdVia` or workflow IDs to bypass these effects. Notification/workflow/webhook,
job and integration suppression plus bounded derived-data retention require explicit future audit.
Archive alone cannot clear cleanup health or resume paused writes.

The existing public SDK surface and [exact port proposal](port-proposal.md) are source-audited only.
The generic adapter still requires real auth, argument builders, matching, durable evidence and
read evidence. It now rejects replica/live/unknown-delivery fresh-read refs and wrong read kinds
before any connection. The Whagons manifest retains the real replica metadata and reports this
incompatibility; no private SDK fields, fabricated revisions, receipt keys or local cache are used.
`watchReplica` plus complete/up-to-date matching and computed revision is the proposed reader
path, not implemented Whagons proof; actual durable cursor association and epoch comparison are
still absent. The app source's `query()` examples do not waive this M05 binding restriction.

## Offline audit and validation

`node cloudflare/observability/cli.mjs binding-audit [declarations.json]` checks source pins,
function kind/delivery, selected operation/method, exact allowed argument names, server-only
SDK policy and prerequisite declaration shape. It accepts no credentials, IDs, row values or
executable callbacks. Results export only fixed codes and expected pins. Input is capped at 64 KiB.
BLOCKED reports exit 2; input errors exit 1 without raw parser/error/input echo. Even asserting
every prerequisite cannot enable execution or emit application success/availability.

All original 115 foundation tests remain byte-identical, including adverse timing, accounting,
privacy and old-review regressions. New tests exercise wrong function kind/delivery, broad/numeric
substitution, missing scope, forged provenance suppression, queued/local/optimistic execution,
asserted auth/receipt/apply/cleanup claims, nested private input, CLI size/error redaction and the
actual `sdkApplication` pre-connect refusal. Conformance tests remain unchanged.
Exact check exits, resource receipts, file/tool hashes, full-diff classification and cycle comparison
are recorded alongside this report. Existing 445/493/1b/a59 evidence and failures are not rewritten.

| Local check | Result |
| --- | --- |
| Monitoring suite | 132 passed (115 unchanged originals, 17 new); no skips/failures |
| Conformance unit suite | 29 passed; unchanged |
| SQLite schema generator + freshness check | passed, no tracked output changes |
| Docs metadata / Next typegen / TypeScript | passed via explicit existing owned executables |
| Runtime import cycles against exact `8851` base blobs | existing 1 → 1; 430 base / 432 head graph nodes; unresolved-import warnings retained |
| Built-in disabled config / replay | passed within their declared offline domains |
| Binding audit | BLOCKED, expected exit 2; no operations or availability emitted |
| Source verification | 24 immutable app blobs, 9 catalog/generated refs, 4 framework inputs checked; 34 original files preserved |

The first source verifier truncated a generated reference before its replica bounds and failed;
its corrected parser includes the full object. The first cycle verifier omitted package nodes
reachable through workspace aliases from its source-only baseline and failed 0 → 1. The corrected
baseline supplies every existing graph node from exact Git blobs, without resolving mutable
workspace dependencies. Both initial failures, logs and resource receipts are retained under
`/mnt/data/work/gonvex-m05-foundation/binding-preparation/artifacts-initial`, separately from passes.
No implementation assertion was removed or weakened to obtain these results.

Serving bundle, tenant rows/bytes, production memory, hosted CPU and app latency/overhead were
**not executed or measured**: no serving code imports this foundation. The audit itself does not
read/write application rows. Local test resource usage is tool-process evidence, not serving gains.
No install occurred; the earlier 610-package/approximately 1.3 GiB tooling deviation and its
unproven global-store history remain limitations, with no further pnpm/Corepack/autoinstall.

## Next actual proof and unchanged gates

Lead must separately own/review the proposed SDK evidence port and a real fresh SQLite read
binding, then authorize dedicated least-privilege writer/reader/unauthorized identities, synthetic
tenant/workspace/argument scope, side-effect isolation, cleanup retention/budgets and deployment
verification. An assembled dev test must prove actual create and rename durable confirmation,
same-epoch second-client apply within deadline, fresh read/reload, unauthorized visibility rejection,
soft archive plus actual bounded cleanup, and ambiguous-write/cleanup-failure stop behavior.
Browser selectors/login, files and documented per-job terminal deadlines remain missing.
External regional execution, faults, authorized test-destination alert drills, serving trace links,
cost/security and real duration evidence follow separate activation authorization.

M05 remains stage 2 / wave 4, depends only on P01, and retains exactly
**“Each of the five paging conditions fires in a drill”.** P1–P5 definitions remain unresolved in
the original source audit. New preparation blockers never replace those conditions. Full M05,
V02/V05/X01/V03/V04/R01 and real 14d/30d dependencies are UNMET/unchanged; no achieved uptime
or SLA is claimed. Nothing here authorizes serving, K22 or provider activation.
