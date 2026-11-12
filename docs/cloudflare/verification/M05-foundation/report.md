# M05 foundation: immutable local evidence

Date: 2026-11-12. Implementation source **f58d75f9850acce5ac854fa34f1052d8a005c0fa**.
Base **6273dddf6af1890f039fc14f210753593add5be6**, verified unchanged on origin before publication.
Checkout `/home/gabriel/.t3/worktrees/gonvex/t3-9ef46623-057033777841386e`, branch
`cf/M05-monitoring-foundation`, origin `git@github.com:Whagons-International/gonvex.git`.
Initial HEAD exactly matched base and porcelain was clean. P01 `554b60d9` is an ancestor.
This evidence commit adds reports only; implementation bytes remain pinned by source receipt hashes.

## Delivered / evidence domains

| Item | State / evidence |
| --- | --- |
| Portable read/realtime/task/browser/file/job scenarios, synthetic ledger, SDK adapter | **Implemented**; generic task/read boundary **locally tested** with actual isolated Node SQLite handler. SDK adapter uses existing public APIs but actual application/receipt bindings remain **blocked**. |
| Local adverse tests | **31 PASS, 0 failures/skips**, Node **24.13.1** (also passed on Node22). Write rejection, lost reply, stalled second-client state, cleanup failure, ignored abort, owner authorization, privacy, budgets, missing/late/heartbeat accounting. |
| Existing conformance unit command | **29 PASS**, unchanged command retained. Both exact CI commands pass with cwd `cloudflare/conformance`. |
| SQLite schema generator | Generator and `--check` pass; no generated tracked changes. |
| Docs metadata / typecheck | `pnpm --filter @gonvex/docs typecheck` passed on Node22; after relocation, explicit owned fumadocs/next/tsc entrypoints all passed, without another pnpm call. |
| Runtime import cycles | **1 before / 1 after**, madge8.0.0; 417 base /429 head graph nodes. Every pre-existing graph node byte-compared to exact base. Entire Cloudflare roots, type imports/tests/fixtures/conformance excluded. No shared import/instrumentation change. See `cycles.json`, including unresolved-tool warnings. |
| Raw results and replay | **Implemented/local-tested**; actual local SQLite raw records name local executor, source/config hashes and handler-only domain. Separate adverse replay fixture is constructed data, never hosted evidence. |
| Checkly definitions/cost | **Implemented/disabled**; 36 separately scheduled definitions at us-east-1/us-west-1/eu-west-1, 12 per location. Account/runtime/plan/access/scheduled execution **unverified**. Constructor-boundary tests are not vendor integration. Cost remains null; no guessed plan/rates/Cloudflare bill. |
| Correlation/export/Analytics/dashboard/alert/status | Library and disabled definitions **implemented/local-tested**. All serving attachment points **planned/uninstrumented**. No actual spans/export/provider/heartbeat/status/DNS/messages activated. |
| Original M05 acceptance | **UNMET**. Original stage2/wave4/P01-only dependency and “Each of the five paging conditions fires in a drill” unchanged. All source-linked P1–P5 unresolved. Added conditions do not replace them. |

`source-receipt.json` carries file/tool/runtime/config/artifact hashes. `checks.json` records commands,
resource domain and exits. `workflow.diff` is the **complete** approved workflow change: retain existing
conformance command, append observability command; no jobs/triggers/secrets/deployment/timeouts changed.
`classification.json` uses the complete base-to-head filenames including rename originals (none here).
No child reviewer, watch or merge was started; lead owns integration and fresh independent review.

## Actual measurements and precise non-execution scope

Pinned Node24 verification phase `gonvex-m05-foundation-20261112-phase-a988bc3d6d1a.service`:
9.667s wall, 9,710,667 CPU microseconds, **665,346,048 bytes** peak memory, zero contained OOM kills.
This combines tests, real local handler captures and cycle analysis; it is **not serving overhead**.
Source receipt counts **100,538 bytes** across the 19 implementation/workflow files, including tests,
docs and fixtures. This is source size, not a bundled/gzip Worker measurement.

No serving function, entry/config, K15/K22 module, Rust/module source, protocol, package or lockfile changed.
No runtime import of observability was added. **Serving bundle/gzip, isolate overhead, SQL rowsRead/
rowsWritten, throughput/capacity/cost were not executed/measured locally**; no serving gain is asserted.
The local fixture actually commits SQLite create/update and cleanup and opens fresh read connections;
Node SQLite does not expose workerd SqlStorageCursor row counters here. Its synthetic row/guard outcomes
and monotonic durations are in `local-handler-raw.json`; these are not Whagons application metrics.
Full Cloudflare typecheck/workerd/bundle checks are left to existing PR CI, not claimed locally.

Every heavy install/test/typecheck/movement phase used build-headroom and coach-resources kernel-v1 in
heavy.slice/heavy-workloads.slice. No native/Rust compile/browser/Container/platform campaign was run.
Resource registration is task-scoped path compatibility, not a cap reservation/bypass. Initial corrected
absolute-path test failure remains in the deviation receipt; it was not a passing test run.

## Tooling deviation / remediation

The initial docs command unexpectedly auto-installed **610 workspace packages** through Corepack/pnpm11.
Root/cloudflare node_modules were absent before the command; the new local dependency paths were proven
fresh by timestamps before movement. The admitted original phase was
`gonvex-m05-foundation-20261112-phase-f97917521cea.service`; this was a tooling deviation from explicit
owned install/cache paths, not permission for future automatic installs.

Only 16 proven newly task-owned ignored dependency/docs-cache paths were moved under
`/mnt/data/work/gonvex-m05-foundation/relocated` and original paths became links. **1,065,808,396 bytes**,
71,730 files and 2,262 links are recorded; zero resulting broken links. No global prune/cleanup, other
checkout or process was changed. **Global store effects are unverified; unchanged cannot be asserted.**
The initial relocation's file-hash/link assertions completed, then its final working-diff guard failed
because the implementing agent staged authorized source while it ran. That failed phase is retained.
A second receipt checks exact permitted source scope and package/lock hashes, and reconstructs the exact
canonical before/after link mapping from retained absolute links. **Literal original relative-link spelling
was not saved before that failed receipt assertion.** This evidence limitation is preserved, not concealed.

`tooling-deviation.json` has exact top-level paths, command/phase, bytes and locks. The complete canonical
map/file hashes are at the hashed local artifact path named there. `source-receipt.json` pins the actual
Node/docs/madge/helper executable contents after movement. Explicit owned docs tools passed again, then
all affected final tests/cycles/generator/replay/raw-capture commands passed under Node24. No subsequent
pnpm/install auto-execution was used. Ignored links remain only in this registered owned checkout.

## Remaining acceptance and next dependencies

1. Recover the canonical original five names/thresholds/windows from the exact original design body;
   retain the source audit and conduct all original P1–P5 drills.
2. Lead integrates/reviews the exact assembled artifact. Configure real Whagons generated paths,
   actual auth/identities/visibility and browser controls; coordinate the missing durable-receipt /
   second-applied revision observation hooks before any shared edit. No fixture may stand in for that.
3. Verify existing Checkly account/access/plan, pinned package/runtime/Node/browser and supported subset,
   actual three separately scheduled geographic executors (two Americas), independent heartbeat/export,
   serialized shared cleanup ledger, side-effect audit and verified bounded budget. No account provisioned.
4. Run real SDK and actual two-session browser login/create/update/read/reload against exact development
   source/config/deployment; file bytes/unauthorized owner and job-specific durable deadline checks.
5. V05 original faults plus scoped route/auth/SQLite-write/HTTP200-stalled-sync/job/file/resource/monitor
   outages. Observe detection/confirmation/recovery, and separately authorized **actual test-alert receipt**.
6. Coordinate actual serving spans/links through Action/Workflow/scheduler/outbox, K22/X01 autoscale/drain/
   reconnect/deploy, retaining every uninstrumented gap. Measure V04 SQL/CPU/memory/bytes/billing/retention.
7. Verify independent status hosting/DNS/CDN/account/operator update and provider-owned fallback under
   Cloudflare failure. Review activation/rollback manifest, then obtain separate live authorization.

K22 PR4003d92/runtime18afb and X01 PR3976b2 remain acceptance UNMET despite prior CI green. Retain
full38 local34PASS/4FAIL, CPU UNEXECUTED, enabled-file/eight-package/owner lifecycle and all V02/V05/
X01/V03/V04/R01 requirements. No mutable K22 dependency tree was read. Real14d/30d gates remain.
99.9% is owner-proposed; cadence/detection/recovery bounds are conditional proposals, not detection
promises or observed uptime. Nothing is drilled or continuously observed by this foundation PR.
