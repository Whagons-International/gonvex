# M05 explicit server transaction evidence candidate

> Historical implementation and verification at source35f7b53e follow below. The new independent review found three P2 ownership defects on that source. Current correction and verification are in [review-corrections.md](review-corrections.md); the original1248/native9 evidence is retained and is not source clearance for the corrected head.

The public SDK now accepts `execution:'server'` on `reducerWithTransactionEvidence`. A generated `localExecution:1` ref requires this explicit opt-in, a genuine installed binding hash matching the latest accepted tenant auth artifact, and the unchanged transaction/application/watermark/receipt proof. The method preserves the ref and offline metadata, never enters LocalRuntime/outbox/generic send/reconnect helpers, and never retries an ambiguous write. This is an isolated candidate, not monitoring activation or Whagons application proof.

Source began at b9c4b2483a5629421caf8a29c2ee867f0d8f21d5. At a settled collection boundary it ordinarily fast-forwarded to integration 1b934f15a2bd11db8753fd39dedf895a31952bb3, which adds only reviewed PR425 observability files. SDK/protocol/kernel/Rust source is byte-identical between those bases. Frozen b9 results retain their labels; the final complete SDK invocation and final proof collections use current candidate SDK bytes with the adopted integration context.

## Contract implementation

- A private accepted-auth artifact record is populated only after the latest actual tenant auth.result validates identity/admission and durable local activation/recovery completes. It does not inherit a previous hash, read the public activeArtifactHash fallback, restore/cache/catalog or system.reload. It binds the exact admitted object, auth frame, physical socket/generation, principal, scope/session/outbox generations, activation/definition ownership and observed full DB epoch. The first real cursor supplies the DB epoch when an empty client has none; the directive's module/schema epoch is never treated as that DB epoch.
- Auth replacement/start/error, transport loss/error, reload/updateRequired, public reset and epoch changes supersede the auth attempt. Internal activation clears the record while retaining only the still-current auth attempt. Scope/activation/reset hooks clear the record before observers, retaining the existing reset-symbol ordering. A pending explicit call fails UNKNOWN; these new invalidations do not invalidate an option-absent call beyond its existing rules.
- Unknown execution values, unknown own keys (including symbols/non-enumerable fields), unknown localExecution values, optimistic refs, missing/mismatched binding hashes, caller ref entropy (including a present undefined field), or a supplied nonmatching artifact hash refuse before sending. Options accept no identity/hash/entropy/key/proof/callback authority.
- One fresh internal ID is also the idempotency key, with the exact path as receiptPath. Explicit mode generates one cryptographic 32-byte/64-hex entropy and sends the actual accepted auth hash directly on the captured ready OPEN socket. It omits only the optional per-command clientContract field. Auth's configured contract and all server admission/update checks remain intact. Option-absent payload/ref refusal and ordinary reducer behavior are retained.
- Every existing TransactionEvidenceCall predicate remains unchanged: exact result/path/origin/root/actor, full same-epoch matching transaction cursor, actual non-skipped serialized application, normal persisted watermark, and one exact authenticated current-subject/key/path receipt found=true at same epoch/fence>=R. The result uses transaction R, never receipt fence R+1. No-op/filtered/missing/stale/duplicate/late/unsubscribed or invalidated evidence stays UNKNOWN with no automatic retry. The corrected R4-after-R5 skip cannot send a receipt query.

The source ruling is `/mnt/data/work/gonvex-cloudflare-lead/staging-critical-path-ceb10028/m05-explicit-server-execution-implementation-ruling.md`. This implements that narrow contract. The unsupported GENERAL ordinary reducer.result + later receipt fence assessment is unchanged. There is no new wire field, capability, server/Rust/schema implementation or public receipt/epoch exposure.

## Actual verification

| Check | Actual result / domain |
| --- | --- |
| Full SDK, one final invocation | **1248/1248 PASS = original1178 +70 new**, including original29 transaction regressions and real npm pack --dry-run |
| OPFS | 19 PASS, actual existing Node check suite |
| Closed policy checks | 199 PASS, original source/assertions unchanged |
| Watermark runtime | 12 PASS plus three compiled controls with 4 intended assertion failures each; all compiled source SHA markers validated |
| Control-watch runtime | 38 PASS and 20 intended failures across 12 compiled controls; includes four actual loopback Node WebSocket close/reconnect schedules, not Chromium |
| Current monitoring/conformance | 156 /29 PASS, local foundation/unit domains only |
| Generated consumer typing | PASS: actual generated client/ref/binding compile with execution:'server'; invalid execution/caller-hash options remain type errors |
| Native composed SDK | 1 new composition +8 unchanged original receipt tests PASS, actual workerd/DO/SQLite/K12/capture/delivery/public SDK |
| Required generators/types | SQLite capability sync/check, dashboard/template codegen/catalog/types/check, wrangler declarations, all Cloudflare type configurations, docs metadata/Next types/tsc PASS; no tracked generated drift |
| Runtime cycles | client **0→0**, Cloudflare **1→1**, against exact adopted integration under equivalent resolution |
| Read-only algorithms | TransactionEvidenceCall file unchanged; ordinary reducer/LocalRuntime/query/receipt/watermark/storage bodies checked by source hashes and existing proof suites |

The final source also rechecks ownership after serializing the private receipt request. A new behavioral control changes auth during that serialization: the prior candidate sends the receipt and fails the new assertion; the corrected explicit branch sends none and stays UNKNOWN after its single write. Frozen 1247/9 collections and the pre-guard source patch are retained; final 1248/9 and compiled watermark/Control-watch collections have separate receipt-guard paths. The option-absent receipt behavior is unchanged.

The native controlled module is new test source under `packages/client/test/explicit-server-fixture`. Its module, generated API and portable local binding are built by the actual production compiler/codegen; their artifacts are not handwritten hashes or seeded SDK private fields. Native assembly loads those exact emitted server exports. The positive commands are actual public SDK calls using genuine generated local refs and the actual accepted authenticated hash. Only private fixture setup and a separately labeled unrelated write use the original raw helper.

The native test actually seeds two rows, then creates/renames/soft-archives a bounded synthetic row through the new SDK option. It observes three distinct one-shot keyed commands, fresh entropy, no own per-command contract, the matching transaction cursors, actual published SDK data and three authenticated receipts. The first command is R4; an unrelated actual write advances its later receipt fence to R5, while the SDK returns the original transaction R4. A no-op remains UNKNOWN without a receipt read. An actual same-contract deployment rebuild while application is held invalidates the proof; the actual server write remains in SQLite, with no retry/cancellation claim. Fresh authentication with the old generated binding still refuses. All clients close in finally and all owned services settle.

The same-contract rebuild changes source artifact identity while preserving compiled handler bytes; it is an actual controlled deployment, not a restore or invented epoch transition. The reusable offline producer reproduces collected native JS/manifests byte-for-byte. The module has a maximum 10-row/16-KiB replica window and no external effects/providers. This is NOT Whagons execution, real app authorization/security, fresh Whagons SQLite read, an independent real app second client, deployed/hosted regions, browser or continuous/drill evidence.

## Reproduction

All builds/caches/tmp/native data/receipts are under `/mnt/data/work/gonvex-m05-foundation/explicit-server-evidence-b9c4`; prior PR425/real-app and K22 roots are not mutated. The exact T3 checkout is `/home/gabriel/.t3/worktrees/gonvex/t3-9ef46623-057033777841386e`, branch `cf/M05-explicit-server-evidence-b9c4`. Registration, source/tool/link/config hashes, exact argv/cwd/exit receipts and raw failures are indexed in [receipt.json](receipt.json). Final head/publication/sealed archive metadata is retained in that root's `handoff/`.

Use pinned Node24.13.1/read-only lead tools inside the normal owner grant. Every heavy phase/command has fresh outer+checked inner headroom and the unchanged FOUR-slot coach-compile helper in kernel-v1 heavy.slice/heavy-workloads.slice. Tests use <=2 workers. The compiler-slot descriptor was verified inside the read-only host sandbox. A narrow RW bind for the helper's existing compile-lock state permits the unchanged npm artifact test's child compiler to use the real helper; tool/dependency stores remain read-only. User/global npmrc paths are distinct empty owned files; npm pack is the real pinned npm, not a stub.

In an owned copied framework, after building the existing compiler/SDK with direct pinned tsc, the controlled producer is:

```sh
node packages/gonvex/dist/index.js codegen --project packages/client/test/explicit-server-fixture
node packages/client/test/explicit-server.build.mjs /absolute/owned/evidence/root
```

The producer refuses a framework outside the explicit evidence root and uses only existing owned/pinned dependencies. The actual native config is retained in the root's copied `cloudflare/kernel/reducer/vitest.m05-explicit.config.mjs`, with its private wrangler config. Run those commands only under the resource gates. No pnpm/install is required. SDK tests use direct Vitest; OPFS uses pinned tsc then Node's existing engine checks. Full exact commands and private configs are retained rather than assuming another checkout's dependency layout.

## Retained failures and tooling limits

The initial focused run was 78/79: its new unsubscribe schedule delivered evidence before deferred unsubscribe completed. Waiting for actual teardown preserves the same no-receipt refusal assertion. The first full run was 1239/1241: the new observer test had not triggered its intended callback, and the unchanged npm artifact test's child compiler exited nonzero in the read-only lock-state context. The corrected observer now rotates auth after the actual base storage apply and asserts R4 was applied, UNKNOWN and no receipt. The child used original stdio=ignore, so its exact original stderr is unavailable; the corrected gate/context logs and successful actual compiler/pack are retained. Original SDK assertions were not edited. A v2 fixture build initially lacked its owned dependency-resolution link; it failed without substituting another module and then compiled with the explicit owned link.

One prohibited-manager attempt occurred: the unchanged watermark runtime checker hard-coded pnpm. Node24 Corepack attempted to download pnpm11.1.2 and failed DNS (EAI_AGAIN) inside the network namespace; there was no test report. This is a retained failure/deviation, not a PASS or waiver. No successful manager acquisition/materialization was observed, and no global-other-writers/store-unchanged claim is made.

The lead approved an OWN-copy launcher-only adaptation. Each private `.direct.mjs` differs only by replacing pnpm/filter/exec with process.execPath, the exact pinned Vitest entry and maxWorkers1. The generated config's explicit packages/client root preserves effective package context; original cwd, include/testPath/patterns/counts/timeouts, extractor imports, source replacements/guard text, compiled SHA checks and intended failure regex/assertions are unchanged. The original/shared checker files stay byte-identical. Exact before/after replacement and hashes are in `artifacts/direct-launcher-adaptation.json`; the original Corepack stderr and missing-report failure remain separate.

A type-only archive initially inherited a built module-sdk through a root dependency fallback, defeating an original fixture's intentional unbuilt-import ts-expect-error. That failed type report is retained. Only the new task-owned ancestor link was removed; CF-local pinned dependency links and all original source/config/assertions remain unchanged, then the full configurations passed. No type/test/extractor weakening was used.

## Measurement, rollout and holds

[receipt.json](receipt.json) records exact before/after compiled SDK bytes/gzip, source/copy sizes, native SQLite call counters and whole-runner memory/CPU. Those local measurements are not serving latency/cost/heap improvements or tenant capacity. No full serving bundle, hosted duration, CPU fleet, K22/native-binary campaign or app dependency acquisition was executed.

Rollback removes/denies only the additive execution option and private accepted-artifact state/hooks. Ordinary APIs, protocol/server/schema and existing atomic receipt recovery remain unchanged. Missing/unknown auth/binding/evidence refuses; there is no weaker fallback, queue or replay. New independent exact-head source review and CI are lead-owned; no author watch/merge/activation follows.

Monitoring remains disabled. The original M05 stage2/wave4 depends only on P01; “Each of the five paging conditions fires in a drill” and unresolved P1–P5/source thresholds remain unchanged and UNMET. The cached real Whagons app's dependency and manifest failures remain blocked separately. Fresh actual Whagons SQLite and independently authorized same-epoch applied-client evidence remain separate missing prerequisites. All original M05/K22/X01/X02/V01–V06/26-tenant/R01-G3b/shadow/backup/rollback/security/privacy/cost/capacity/soak/14d/30d gates remain OPEN. No provider/status/account/credential/CF resource/staging/production/0077 activation or actual Whagons write was performed.
