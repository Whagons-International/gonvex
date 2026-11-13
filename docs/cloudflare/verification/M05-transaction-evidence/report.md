# Conditional SDK transaction-evidence candidate

This is a candidate implementation of the [pinned lead contract](decision.md), not API/source-clear,
activation, application readiness or full M05 acceptance. Framework source is
`ceb10028c090fc38ad1d361376885adb8117f79e`; the [source manifest](source-manifest.json) keeps that
profile separate from cached app `73276b` and all older evidence. App files were read only as
immutable Git blobs for source verification; no app working tree, dependency or deployment was used.

The new public `GonvexClient.reducerWithTransactionEvidence(ref,args,options)` returns:

```ts
{ result, confirmation: {
  source: 'server-durable', basis: 'matching-replica-transaction',
  cursor: { epoch, revision }
} }
```

It requires `offline:'reject'`, an empty `optimistic` list, a finite total timeout in `(0,60000]` ms,
and **exactly one active complete server-ready observation window** on an already accepted tenant
session. The one-window restriction bounds this finite candidate's ownership snapshot; it is not
a general multi-subscription readiness claim. Generated local-execution/optimistic/public/control
refs are refused, not rewritten. The cursor is the matching transaction's cursor, never a later
receipt read fence. The caller's ordinary app result remains separate.

## Contract-to-code mapping

| Required fact | Implementation / refusal |
| --- | --- |
| Accepted owner and ready DB epoch | Index preflight captures physical socket/generation, accepted/latest auth ownership, admission project/tenant/account/member, session/outbox/replica activation/reconciliation generations, definition epoch and the ready DB epoch. Restored/cache/opening states do not qualify. |
| One internally keyed direct call | One fresh command/receipt key and one separate fresh query ID. Collector is installed first; serialization and current-owner/deadline checks precede the captured socket's direct send. Neither generic send queue, connect, reauth, outbox nor LocalRuntime is used. |
| Actual result + transaction | Exact result ID/path/origin/revision; exact transaction origin/root/direct channel/actor and same full DB cursor epoch/revision. Either order is accepted. Matching duplicates are conservatively refused, including identical duplicates; no historical frame map or payload fingerprint is kept. |
| Original processing barrier | The actual `applyReplicaTransaction` completion and persisted ordered watermark must both finish. This is the original client's barrier, not evidence of a second client's application. |
| Exactly one receipt read | Only after result/transaction/barrier match, one direct authenticated same-capture key/path lookup must return found=true and same epoch at fence >=R. It confirms completion; the returned confirmation still uses transaction R. |
| Every-boundary ownership | Captured owner/window tokens are rechecked at processing completion, watermark, query issue and final resolution. Auth start/result/error, close/error, resets/reconciliation, epoch/scope changes and processing failures invalidate before ordinary observers can settle the candidate. No late callback rebinds it. |

The support helper holds one call's bounded primitive proof fields and its app return value until
settlement, then clears them. It stores no full frames, application-field proof, token or historical
cursor map. Account/member values are private matching inputs, not confirmation/log attributes.
Caller trace/operation IDs and idempotency overrides are not options. Server authorization and
ordinary module/notification/receipt behavior are preserved. Returned evidence is not a signed
certificate or a permission. `TransactionEvidenceError` exposes fixed reason/outcome fields; after
send its meaning is UNKNOWN/possibly applied. Timeout is not cancellation and permits no automatic
retry, replay, lookup reacquisition or scope transfer. Argument serialization is included in the
monotonic deadline and cannot send after it expires.

## Executed proof and its domain

The [native observation](native-witness.json) comes from the **actual public SDK** talking over a
transport-only SELF WebSocket adapter to the existing production K10 `ReplicaHarnessTenant`:
K12 reducer, native Durable Object SQLite, K03 capture, K10 feed/delivery and gateway/session paths.
The test worker merely supplies the unchanged fixture bundle and its actual declared board
visibility. No transaction, SQL clock, receipt or delivery algorithm was copied or mocked.
The eight original receipt tests run unchanged alongside the new native proof.

The extra fixture archive operation is explicitly synthetic: an additive test module uses the
normal module `db.update` API to retain a board row with a fixed archive marker. It is not
Whagons `tasks.remove`, its `deletedAt` semantics, effects or cleanup. No Whagons route was replaced.
The real application's create/rename/archive bindings and authorization/visibility/effects remain
unproved, and refs with generated local execution remain refused by this candidate.

The native proof establishes create R=4, an unrelated actual write R=5 before receipt completion,
and returned confirmation **R=4**; rename R=6 and synthetic retained-row archive R=7 each use one
fresh candidate write and one receipt read. It checks matching transaction cursors, ordinary SDK
physical deletion's unchanged null result, and explicit existing receipt replay without reapplying
the original create. Actual no-op and unsubscribed-entity writes refuse evidence without a second
write or receipt acquisition. No epoch transition, restore or manual clock mutation is claimed.

This is controlled test authentication and native local execution with the SDK's memory replica,
not real Whagons, browser/OPFS, deployed tenant auth, a disk-crash/restore campaign, hosted regions,
external monitors, tracing/drills or continuous observation. The transport adapter records only
bounded test facts, never substitutes transactions, and closes both clients in `finally`.

The additive SDK race tests use fake sockets **only for SDK races**, with real LocalReplica/storage
processing. They cover both frame orders, held apply, total deadlines/late frames, auth/transport/
epoch/reset/unsubscribe invalidation, actor/root/result/revision conflicts and duplicates, unsafe
metadata, malformed/missing/late fences, serialization privacy, and bounded observation capture.
Fake sockets are not native proof. Original SDK tests/assertions and all monitoring/conformance/
receipt sources remain unchanged.

## Checks and reproducibility

Run the ordinary SDK build/tests using pinned executables and owned output paths; run the new
`transaction-evidence.native-proof.ts` only with the private native worker/config, not the Node
SDK test runner. There is no new repo workflow/config/package script or serving assembly edit.
The immutable receipt records actual commands, source/tool hashes, raw paths, resource units and
results. `node cloudflare/observability/cli.mjs binding-audit` remains disabled/BLOCKED and the
unsupported GENERAL `reducerWithCommitEvidence` assessment/history is byte-identical.

All artifacts, caches, TMP, configs and built framework dependencies are under
`/mnt/data/work/gonvex-m05-foundation/transaction-evidence-candidate-ceb10028`. The same registered
checkout was clean at `e0a16a3e` before switching to `cf/M05-transaction-evidence-candidate` at `ceb`.
No Git checkout/worktree was created or moved. Own source archives and workspace links point to
own emitted dist; pinned third-party tools are resolved read-only. No install/Corepack/pnpm or
shared dist is used. Original package-artifact tests use a private pinned tsc shim, fresh headroom
and the unchanged four-slot helper, then offline `npm pack --dry-run` with an owned npm cache.
The isolated SDK phase permits only the normal global compile-lock control directory in addition
to own outputs; no cap or other job changes occur. Other phases mount the host read-only and isolate
network access. Every heavy command has outer/checked inner headroom and normal kernel-v1 admission.

All failed attempts are retained: K12-only fixture lacked delivery; the assembled fixture initially
omitted its declared visibility; native observation initially missed batched transactions; and the
first SDK archives lacked a Rust JSON fixture/private tsc shim, then denied the compiler's normal
gate lock. These were input/assembly/instrumentation issues, not waived assertions. An initial test
timeout hid the more precise fixture error; its later total budget allowed the unchanged readiness
assertion to report it. No original fixture/assertion was changed. Passing intermediate artifacts
are retained separately from failures and final outputs.

ESM output byte/gzip counts are measured against an owned exact-base SDK build. They are not a
tree-shaken browser bundle or serving gain. Per-call proof state is bounded, but serving CPU,
latency, retained memory and cost were not benchmarked. Process resource peaks are tool evidence
only. Earlier 610-package/global-store-history limitations are retained, not re-certified.

## Handoff and holds

### Lead CI integration correction

Head `2811337c` failed the existing Control-watch and watermark publication proof
extractors. Their assertions and semantics remain unchanged. Scope reset keeps
its two original Symbol allocations first, then invalidates transaction evidence
before any observer. `persistReplicaWatermark` keeps its original closed publication
path; the captured evidence call is pumped only after the frame operation,
deferred frames and opening-watermark drain finish, and only if it is still the
same installed call. The original CI failures remain recorded separately from
the correction's checks. Earlier author source-equivalence receipts describe
their original validated head; this correction requires its own exact-head
validation and fresh independent review.

Lead owns ONE NEW independent review and any integration/API acceptance. No author reviewer,
watch, merge or automatic next task follows. A fresh authoritative tenant-SQLite application read
and an independent authorized same-epoch applied client are still two missing prerequisites.
App/browser fit, notification/effect isolation, retention cleanup, hosted regions, security/cost,
tracing and all time/duration evidence remain open. M05 stays stage2/wave4, depends only on P01,
and retains exactly **“Each of the five paging conditions fires in a drill”.** Original P1–P5
definitions are unresolved and none has been drilled here. Full M05 and all original K22/X01/
V01–V06/26-tenant-1k/security/14d/30d gates remain UNMET. Nothing activates a monitor, app, account,
provider, status/alert/DNS/Cloudflare resource, K22 default or 0077/Container/CPU campaign.
