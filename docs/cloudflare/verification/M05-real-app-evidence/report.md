# Historical Whagons binding/native preparation at b9

The finite implementation closes an unsafe preparation path: the generic M05 SDK adapter previously cleared `localExecution` and `optimistic` on mutation and cleanup refs. It now refuses those refs and wrong write kinds before authentication, rechecks after authentication/argument mapping, and forwards supported refs by identity. The read-delivery guard and bounded CLI are unchanged. No public SDK option, server/protocol/module change, serving instrumentation or activation was implemented.

The actual historical Whagons app was collected from immutable Git objects at `73276b5589d363c1190a6d028413a21559226631`, with current framework/public SDK source at `b9c4b2483a5629421caf8a29c2ee867f0d8f21d5`. The app working tree and dependencies were never read. This is a historical source domain, not a verified current deployment. The prior 8851/5c5/ceb binding, unsupported assessment, reviews and receipts retain their original pins.

## Executed results and honest boundaries

| Work | Result | Domain / limit |
| --- | --- | --- |
| Six framework package TypeScript builds | PASS | protocol/module-sdk/local-runtime/client/react/gonvex, own archive/output |
| Full genuine app buildModuleArtifact | BLOCKED / actual failure | unresolved `ai`, `@ai-sdk/openai`, `zod`, `parse5`, `@json-render/core`, `@json-render/react/schema`, `linkedom/worker` |
| Genuine tasks.ts relative source closure | BLOCKED / actual failure | declared `taskAutoTagging.classify` tool targets `aiUsage.record`, outside this closure; no substitute handler was produced |
| Genuine tasks/replicaQueries.ts artifact | locally compiled | actual 6 functions, tasks/taskNumberCounters visibility, all original migrations; scope is read module only, not full app |
| Native activation of that real read artifact | BLOCKED / actual failure | `SQLITE_TOOBIG` storing 19,641,222-byte manifest in original KernelControlStub.deploy; failed before tenant creation/read |
| Fresh Whagons watch + two independent applied readers | NOT established | blocked by activation; zero real task writes; no positive row, command cursor, SQLite read or second-client applied-command proof |
| Existing M05 adapter + genuine generated tasks.get | PASS refusal | actual offline adapter rejects replica-delivery mismatch before any connect/callback; query-kind is not query.call eligibility |
| Genuine create/rename/remove refs + merged public SDK | PASS refusal | ready controlled native SDK window; each preserves localExecution:1/offline allowed and refuses before send |
| Added native artifact omission test | PASS | original K12 + host + SQLite + socket in controlled board fixture, explicitly NOT Whagons execution |
| Original native receipt tests | 8 PASS | assertions unchanged, actual native recovery semantics preserved |
| Original SDK transaction regressions | 29 PASS | includes both corrected stale-skip R4/R4 and R4/R5 schedules; no full 1178-test SDK rerun claimed |
| Original conformance unit tests | 29 PASS | local unit boundary, not app conformance |
| Current monitoring tests | 156 PASS | original 140 retained + 16 additive adapter regressions |
| Same added tests against exact b9 adapter | 15 intended FAIL, 1 PASS | exercises metadata erasure, absent refusal, late metadata changes, wrong kinds, unchanged-ref identity; original read guard passes |

The pinned app lock resolves ai 7.0.73, parse5 7.3.0, linkedom 0.18.12 and json-render 0.21.0. The inspected approved tool cache lacks those exact required packages (its ai is 7.0.71 and parse5 is 8.0.1). Matching openai/zod packages alone cannot close the graph. No package acquisition, pnpm, Corepack or install was attempted. No old package-cache deviation is reclassified or waived.

The read-artifact manifest includes all original migrations, including the 6,579,085-byte 0005 file. The native failure is specifically the existing small-fixture control stub's single-row manifest persistence. It is not evidence that a migration or tasks.get itself executed or failed. The app migrations, metadata and file set were not trimmed to bypass the platform row bound. A test-only fixture adaptation or production storage boundary needs an explicit lead ruling before such a workaround; no shared edit was made.

## Actual source and proposed port

The genuine generated refs select `tasks.create`, `tasks.rename` and `tasks.remove`, each with `localExecution:1` and offline allowed. Their server handlers remain createTaskIntent/createTaskWithPolicy, updateTask and archiveTask(false). Authorization, task visibility, defaults, workflow/notification/SLA/inventory effects and soft-archive obligations remain actual app behavior. No synthetic suppression flag, SQL writer, success-mocked capability, path-only ref or board task-handler substitution was used for Whagons.

Generated localRuntime has artifact hash `f8336ddff29861c552dc03aeb9e5b0a8c7056589c709fd29d0d4e4dbd91171bc`, contract 3 and actual generated reducer chunk selection. runLocalReducer chooses that binding, crypto entropy, principal/scope and coordinated outbox; the server-only evidence path must not enter it. Generated task refs do not contain artifactHash or intentEntropy. A historical local artifact or catalog path is not authenticated deployment evidence.

[Proposal v1](server-execution-proposal.md) gives the additive execution:'server' cutpoint, strict option validation, accepted latest tenant auth-result artifact state, local binding mismatch refusal, one fresh keyed/entropy direct send, and unchanged transaction/application/watermark/single exact receipt checks. It preserves option-absent local-ref rejection and ordinary reducer behavior. It has no applied SDK implementation.

The actual native dispatch test captured v1 artifact from a real controlled auth result, deployed v2 with the same auth contract 3, then showed:

1. Stale hash plus optional per-command contract 3 executes under the existing compatibility rule.
2. The same stale hash with only that optional per-command field absent refuses; its task row and receipt are absent.
3. Current exact hash without that field executes under the still contract-3-authenticated session.

This source/native evidence supports the existing-wire exact-hash route. It does not grant SDK implementation or prove a real Whagons negotiated artifact. The proposal requires a separate accepted-auth artifact record; current activeArtifactHash advertisement/fallback is insufficient. Session auth contract enforcement stays intact. Rust request/module/generation semantics were source-audited, not rebuilt or run. No epoch transition or restore history was simulated as an observed runtime fact.

Fresh read source feasibility is separate: ReplicaTenantObject supplies snapshotRunner(kernel.storage), whose query callback invokes actual TenantStorage. openReplica first reads clock/principal/scan plan in one invocation; scanWindow reads separate chunks. ReplicaDeliveryEngine.processOpen is held under ReducerPath.serial across that open, as is activation/write execution. This is a source association argument only: actual interleaving schedules, SQL observations and current/complete fresh-reader states were not reached after the activation failure. A new client, matching row, later fence or caller readEvidence cannot fill this gap.

## Reproduction and raw evidence

All task outputs live under `/mnt/data/work/gonvex-m05-foundation/real-app-evidence-b9c4`. The existing exact T3 checkout remains `/home/gabriel/.t3/worktrees/gonvex/t3-9ef46623-057033777841386e`, branch `cf/M05-real-app-evidence-b9c4`. Registration is retained in `artifacts/registration.json`. Source/copy/tool/config hashes and measured sizes are indexed in [evidence.json](evidence.json); the full immutable-source inventories, final private native fixtures, emitted read artifact and sanitized raw logs remain in the task root for independent review.

From an already admitted phase, with a fresh checked inner headroom gate and the unchanged coach-compile wrapper for every heavy command:

```sh
# Node 24, owned temporary fixture directory; no dependencies/install:
M05_TEST_ROOT=/mnt/data/work/gonvex-m05-foundation/real-app-evidence-b9c4/tmp/monitoring \
  /home/gabriel/.config/nvm/versions/node/v24.13.1/bin/node \
  --test cloudflare/observability/test/*.test.mjs
```

`build.sh`/`compile.mjs` retain the actual full app compiler failure. `scoped.mjs read` records the narrowed compiler success; `tasks` retains its failure. `native-05.sh` with its final private sources/config records the actual read activation failure. `native-artifact-03.sh` records the separate 9-pass controlled native run; those source files were frozen for collection. `binding-audit.mjs` executes the actual adapter ingress against genuine transpiled refs, with zero callbacks. The TypeScript transpile removes types only and retains generated runtime metadata.

All failures remain: initial private relative import/config omissions; runner WebSocket message-size failures; actual full compilation/closure/activation failures; initial monitoring fixture-root omission (129 pass/11 setup failures, then 140/140 with the documented owned root); generator CLI format typo (corrected to actual `typescript`); initial incomplete type archive and built SDK package context; initial asymmetric cycle-resolution context. None was relabeled an app PASS.

Final generators passed: SQLite capability sync/check, dashboard/template Gonvex codegen, template NDJSON/types/catalog check, Cloudflare/control wrangler types, docs fumadocs/Next typegen and docs tsc. Required Cloudflare root/control plus host/router/actions/files/principals/ingress/telemetry/sandbox tsc configurations passed. The Cloudflare fixture deliberately expects an unbuilt module-sdk package via ts-expect-error; the final type-only source archive preserves that CI resolution context, original source/config/assertions, and its exact link map. The built-SDK context errors and missing relative-input errors remain in raw evidence. All tracked generator outputs are compared with the exact source pin; no shared output edits are authorized.

Runtime cloudflare cycles compare exact b9 and the candidate under matching workspace resolution; no new cycle is allowed. The complete diff classification includes every changed file and rename original. No UI or serving source is in scope. Full serving bundle, deployed rows/latency/memory/cost/overhead and full SDK/CPU/browser campaigns were not executed. Native phase memory/CPU measurements describe the entire local test runner, not serving gains. No traces or 3-region/continuous/drill proof was produced.

## Holds and next dependencies

Monitoring is disabled. The original M05 stage2/wave4 depends only on P01. Its exact done_when remains “Each of the five paging conditions fires in a drill”; original P1–P5 source/thresholds remain unresolved, and none of these additional checks substitutes for them.

Next actual prerequisites are an authorized exact app dependency/tool closure and original full module build, a permitted native fixture storage/deployment fit, source-backed accepted-artifact negotiation plus separately authorized SDK option, original application auth/visibility/default/effect isolation, authoritative fresh SQLite observation with scheduling association, and an independent same-epoch authorized client actually applying the exact command. Soft archive leaves retained tasks/related effects; it is not fixture erasure or external notification suppression. No actual application write/effect/cleanup was performed here. Controlled fixtures are private, synthetic and locally reaped, with no provider/AI/notification egress enabled.

New independent review and integration belong to the lead. No reviewer, watch, merge, account, credential, provider, resource, status, DNS, staging/production write or activation was initiated. All real app/browser/hosted regions/tracing/drill/privacy/security/cost/capacity/uptime and actual 14d/30d observations, plus the original K22/X01/X02/V01–V06/26-tenant/R01-G3b/shadow/backup/rollback/soak gates remain OPEN. Full M05 acceptance is UNMET.
