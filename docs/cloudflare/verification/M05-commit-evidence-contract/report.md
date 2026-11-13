# M05 ordinary commit-evidence contract at 5c5

The general proposed `reducerWithCommitEvidence` success object is **unsupported** from the
ordinary reducer-result plus existing receipt-lookup contract. The disabled binding audit now
emits `command-bound-commit-cursor-unavailable`, `executionAllowed:false` and a separate
source profile pinned to `5c5fa3795d821d5995179e19b712c89bfa0eb8d8`. This is executable planner
behavior, not a public SDK implementation or stub. No caller claim can enable execution.

The [lead decision](decision.md) is preserved verbatim. The historical
[port proposal](../M05-binding-preparation/port-proposal.md) remains unchanged (blob
`38f64adf52713b0bde1a5c1d02a0b6b4cffd2707`, 3783 bytes,
SHA-256 `39a1bba1b774b474283939107c01f339387d7b41eb366f500945449f183ae85d`).
It is not authorized for implementation. The older app manifest/framework `8851` profile and
its evidence remain historical inputs, not relabelled `5c5` observations. Cached app `73276b`
is still not a deployment pin; no app working tree or dependency was read in this continuation.

## What the actual contract supplies

[Source identities](source-manifest.json) bind these observations to immutable Git blobs:

- `packages/protocol/src/index.ts`: ordinary `reducer.result` carries routing/origin identity and
  optional numeric `committedRevision`, not a command-bound `{epoch,revision}` cursor.
- `packages/client/src/index.ts`: public ordinary `reducer()` returns app data. An SDK-generated
  random direct call ID does not automatically supply a persistent tenant receipt key. No monitor
  reads private SDK fields or treats app fields as evidence. Ordinary SDK/protocol source is unchanged.
- K12 `cloudflare/kernel/reducer/path.ts:387` authenticates the active member and subject/key/path,
  checks a completed stored receipt, then reads the **current** `_gonvex_sync_clock` epoch/revision.
  `receipts.ts` DDL/store rows contain subject, key, path, result and creation time, no per-command cursor.
- Rust `rust/crates/runtime/src/execution.rs:143–245` observes membership, receipt and current clock
  in one read-only snapshot. That atomic completion/read-fence recovery fact is real and preserved.
  Rust was inspected as immutable source, not executed by this task.

Lookup/watermark capability flags promise no stronger fact. Pairing a later lookup epoch with an
older result revision, or using a module/schema directive epoch as a database-clock epoch, does not
establish the proposed association. No receipt/retry/idempotency/capability/wire/schema behavior
was changed or weakened. This is scoped to the general ordinary-result-plus-lookup proposal;
it is not a theorem excluding every restricted matching-transaction/full-cursor design. Any such
design needs separate explicit guarantees, ownership and review.

## Native behavioral witness

The [allowlisted observation](native-witness.json) was emitted by the actual immutable K12
`ReducerPath.receiptStatus` through the existing workerd / native Durable Object SQLite / socket
fixture. [The extra private test](native-witness.spec.ts.txt) runs alongside all eight unchanged
`cloudflare/kernel/reducer/test/receipts.spec.ts` tests in an owned source-only archive of `5c5`.
The test worker, client, fixture, production reducer path, SQL and assertions were not patched.
The only added files in that archive are the private test/config and copied author planner modules.

| Actual observation | Result |
| --- | --- |
| Completed uniquely keyed `board.bump` | original `{bumps:1}`, numeric command revision **4**, no epoch/full cursor |
| Same subject/key/path lookup before another write | `found:true`, fence revision **4** |
| Unrelated actual `board.add` through the same socket production path | revision **5** |
| Same original receipt lookup afterward | `found:true`, current fence revision **5**, same observed epoch |
| Stored receipt / recovery | original result retained, only five declared receipt columns, replay leaves counter at one |
| Different authenticated subject / wrong path | not found / rejected, respectively |
| Feeding actual misleading observations to the planner | BLOCKED; no operation, confirmation or commit cursor returned |

Thus receipt existence, the original app result and a later read fence are distinct facts. The
extra witness never alters SQL clock state, simulates a restore or claims an observed epoch transition.
There is no epoch-history countermodel in this artifact. A controlled test auth stand-in and tiny
conformance board are the declared domain, **not Whagons, public SDK execution, real tenant auth,
application read/apply proof, a deployed service, alert drill or continuous observation**.
No token, account/subject, receipt key, raw error or unfiltered frame is exported in the witness.

## Checks, lifetime and resource boundaries

| Check | Result |
| --- | --- |
| Native receipt suite | 9 tests passed: 8 originals + 1 private witness; no unhandled errors in final run |
| Monitoring suite | 140 passed: all 135 originals unchanged + 5 new refusal/privacy/native-witness regressions |
| Conformance unit suite | 29 passed, unchanged |
| Schema generator / freshness | passed; no generated tracked source change |
| Docs metadata / Next typegen / TypeScript | passed using pinned existing tools, with outputs in the new owned archive |
| Import graph versus exact `5c5` archive | existing cycles 1 → 1; scanner warnings retained, not runtime execution proof |
| Binding audit / replay / disabled config | expected BLOCKED exit 2 / passed / passed |

New tests feed the actual misleading witness and forged subject/session/epoch/cursor/capability
assertions into the planner, verify that all declared prerequisites still cannot enable it, refuse
executable callbacks without invoking them, and check that private input never enters the output.
The support leaf inspects no observation/private field and imports no serving code. The SDK
read-delivery guard and bounded CLI from `e82` are byte-identical, as are prior tests and evidence.
Fresh tenant-SQLite read and a separate authorized same-epoch complete/applied client remain
**two independent missing prerequisites**, even if a future commit-evidence design is accepted.

All outputs, caches, temporary files and configurations are under
`/mnt/data/work/gonvex-m05-foundation/commit-evidence-contract`. The same registered checkout
was clean at `24968803` before switching in place to `cf/M05-commit-evidence-contract` at `5c5`.
No checkout/worktree was created or moved. The private archive has no `.git` or copied dependency
tree; entry symlinks resolve existing tools read-only. Heavy commands ran under fresh outer and
checked inner headroom, kernel-v1 heavy slices and the unchanged four-slot `coach-compile`.
Bubblewrap mounts the host read-only, allows writes only under the new output root, and isolates
network access. Node verification also binds writable `/dev/null` for normal ignored child stdio.
No install/Corepack/pnpm, dependency upgrade, shared dist write, caps change or other-job action occurred.

All failures are retained separately: the first native archive lacked `tsconfig.base.json` and ran
no tests; two native attempts passed nine tests but failed on pending RPC resolution at teardown.
The private witness now awaits both actual socket close events before finishing, and the final
native run exits zero without suppressing errors. The first read-only Node verification denied
the original privacy test's ignored child stdio; binding `/dev/null` fixed the environment, with
the original assertion unchanged. No failure is relabelled PASS. Logs/config/test hashes and
resource receipts are recorded under `artifacts/native-initial`, `native-runner`, `native-bundle`,
`verify-initial` and the successful artifacts in that same new root.

Serving bundle/gzip, tenant SQL rows/bytes, serving memory/CPU/latency, cost and monitoring overhead
are **not measured**: serving source is unchanged and imports no foundation module. Native fixture
observations and tool-process resource peaks are not serving or production estimates. Earlier
610-package tooling/global-store-history limitations remain; this task makes no global-store claim.

## Remaining work and unchanged holds

Lead owns one NEW independent review of this immutable artifact. No author watch, merge or
reviewer reuse follows. There is no shared-file grant or public SDK implementation proposal
activation. A restricted command-bound full-cursor design, if needed, must first establish its
exact guarantees through separate coordination. This artifact supplies no fallback or permission
for a wire/server/Rust/schema change.

M05 is still stage 2 / wave 4, depends only on P01, and retains exactly **“Each of the five paging
conditions fires in a drill”.** P1–P5 source definitions remain unresolved; no drill or continuous
observation occurred. Real app/browser/regions/tracing/privacy/cost/security/uptime and real
14d/30d evidence remain UNMET. All original K22/X01/X02/V01–V06/26-tenant-1k/R01-G3b/shadow/
backup/rollback/soak gates stay OPEN. No live monitor/account/alert/status/DNS/app/tenant/CF
resource/default activation or Container/CPU/fleet/0077 campaign is included.
