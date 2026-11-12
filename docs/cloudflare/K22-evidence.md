# K22 assembly evidence (in progress)

K22 is incomplete. No V01–V06, R01–R06, device/bridge, load, soak, shadow,
backup/rollback or staging-readiness gate is claimed or waived here.

Owned checkout: `/home/gabriel/.t3/worktrees/gonvex/t3-a83efd3f-eae2dffcd48614ba`.
Initial HEAD was verified clean at `e72d3a437b65bea5a8524064e3e04904fc4a1f7b`.
Branch: `cf/K22-assembly`. Incremental results below predate the final commit;
they are not final-head evidence. All fixture data is synthetic. Private
runtime logs/configs stay under `/mnt/data/work/gonvex-k22`; no private tenant
data or credential values are included in this document.

The initial scenario inventory is [scenarios.json](../../cloudflare/kernel/assembly/evidence/scenarios.json).
Package-evidence attribution and the complete local/remote matrix remain open.

## Serving composition

The root entry now uses K06/K17 Control, K07 tenant reducers, K08 query plans,
K09 visibility and K10 ordered replica delivery. K12 Actions/Workflow,
outbox/scheduler, K13 files, K14 presence, K15 telemetry/error ports, K16 guards,
K17 principals and K18/K19 routing/operator/release paths are composed in
`cloudflare/kernel/assembly`. The promoted Control adapter has runtime ownership;
deployed code does not import the K17 harness. Original K05 fixture suites retain
their explicit fixture Worker/config and assertions.

Actions have a separate script/CPU limit. K20 uses its unchanged Rust/DuckDB
Container implementation with per-tenant ownership; L09 is not imported.
The builder has a real Container entry. Deployment/resource/bootstrap wiring,
immutable image provenance and actual deployment evidence remain open.
Reviewed X01 head `6b2ec526f42b47c62c8172d618fc1a507f1d9653` was ordinary-merged
at `26d7261bea13785499f95307ce6a5cf4b729b0b1`. The serving root exports/binds the
automatic controller with a new v5 migration after K22 v4; its static shard
variable was removed. Package source review is not assembled acceptance.

Live-query initial replies and commit updates share `ReplicaDelivery` with
replicas; query frames precede replica frames. Known socket work and every split
part carry a subscription epoch. Missing-epoch scoped work fails closed and
unscoped watermarks do not certify a current auth scope. Universal recovery
requires an explicit holder acknowledgment after real query rebuilding; The combined outlet/protocol/Gateway changes require final assembled recovery
proof. Separate `scopeRequired`/`scopeAck` negotiation uses returned epochs only
for empty bootstrap work, then real authorized rebuild/catch-up on the same queue.

0058/0059 recovery metadata lives in versioned private KV generations, chunked
to at most 64 KiB UTF-8 per text chunk. It includes semantic query requests,
replica tracking and direct Action correlation, with conditional oversized
identity storage. Correlation metadata does not execute/replay Actions. Latest
per-commit resume hashes stay in memory; restore can require a fresh full result.
Generation ownership, digest, scope/epoch and token changes are checked before
restoring the Session. Incomplete publication fails closed; old generations are
kept until publication succeeds and actual close/error callbacks abandon only the matching physical-socket owner.
Durable current+pending references protect both sides of uncertain attachment
publication. Obsolete generations use a bounded orphan index, avoiding a full
active-manifest scan. **Stranded socket owners when a terminal callback never
arrives remain an acceptance blocker.** No TTL or enumeration absence purges
referenced owners. Native attachment/eviction tests prove ordinary hibernation,
not abrupt process reset or reliable platform callback delivery.

Current official documentation describes [disconnected sockets being omitted](https://developers.cloudflare.com/durable-objects/api/state/#getwebsockets)
and [attachment lifetime/size](https://developers.cloudflare.com/durable-objects/best-practices/websockets/#websocketserializeattachment).
It does not provide an owner-release watermark for the missing-callback case.
Private platform fault cases remain required.

## Observed comparisons and source checks

These are incremental observations, not a final-head acceptance claim. All paths
below are relative to `/mnt/data/work/gonvex-k22` unless shown otherwise.

| Check | Observed result | Evidence |
|---|---|---|
| Frozen install and package builds | Passed; caches/output on `/mnt` | `install.log`, `build-packages-2.log` |
| First combined suite/typecheck | Failed: X01 fixture imported removed scaffold exports from serving entry | `evidence/merged-first-failure/` |
| Combined package rerun after fixture import fix | Passed: 820 workerd cases across package phases, 15 existing skips; Node deploy checks also passed. Sandbox CI protocol-stub mode is not real sandbox proof | `evidence/merged-package-pass/` |
| Ordered query/replica, scope/recovery, private-KV regressions | 38 passed | `targeted.log` |
| Native Host suite | 22 passed, including eviction before/after attachment publication and large metadata | `host-tests.log` |
| K10 commit reads at 1/10/100 subscribers | 4 statements / 108 rows read for each | `targeted.log` |
| Synthetic 2M-character permissions | JSON attachment 4,001,540 → 768 bytes, 245 private chunks; JSON/KV measurement, distinct from native attachment tests | `targeted.log` |
| Fresh unchanged Rust reference | Built with `test-firebase-auth-emulator`; binary SHA256 `ec11c67352775fe44a0b0c354d2c335e8b68d7066d7bddc7677bc0c3e2e42036` | `rust-reference/{build.log,source-files.sha256,binary.sha256}` |
| Frozen 38-scenario comparison | **24 pass / 12 fail / 2 skip**, 481.5 s. Overall incomplete/failed; excludes separately blocked reducer-errors | `assembly-compare-local-1.{log,json,result}` |
| Fresh fixed-nine subset before X01 merge | **7 pass / 2 fail**, 80.6 s. Receipt expiry lacked adapter method; telemetry multi-holder domain mismatch | `assembly-fixed-local-1.{log,json,result}` |
| Actual development deployment | Not run; no artifact/config IDs | Open |

The original Host/presence failure is retained in `evidence/full-tests-failed-3.*`.
Later successful source tests do not erase it or establish remote lifecycle proof.

The fixed-nine passing rows are live-query-window, control-support,
change-capture, live-query-task-grid, actions, replica-tenant-restart (unchanged
raw reset/resume assertions) and public-limits. The receipt-expiry adapter now
calls the real existing prune handler; fresh proof is pending. Scheduler private
durable-hop provenance and the original K19 1500 ms defer default were restored;
fresh comparison passed as recorded below. Original raw receipt-upgrade/session-handshake
socket continuity failures remain failed. An additional unchanged SDK recovery
probe across the actual deployment boundary is required by 0060.

0062 canonical telemetry parity is an explicit startup configuration:
`GONVEX_ASSEMBLY_PLACEMENT=static-one-holder` removes the controller binding and
sets `GONVEX_GATEWAY_SHARDS=1` on the actual serving Worker. Its generated private
configuration and actual holder identities must be recorded. The default
`automatic` campaign retains `GATEWAY_CONTROLLER`; a shard-count variable alone
cannot disable it. Static-one-holder parity is not X01 or capacity proof. The
original 97-reference/116-kernel multi-holder result remains domain-mismatch
evidence; a separate actual multi-holder budget/attribution regression is open.

## Local CPU-enforcement blocker (0057)

An earlier fresh assembled local run reached unchanged `reducer-errors` /
`errors.spin` and timed out after 15 seconds. The local workerd invocation did
not enforce the configured `cpu_ms: 5000`; subsequent calls to that tenant
stopped responding. The owned process was stopped. That row remains FAILED /
BLOCKED. The same unsupported local configuration has not been rerun. There is
no timer-generated transient result, fixture change, pre-quarantine or assertion
skip. Named subsets explicitly exclude this blocked row and cannot establish a
complete local pass.

The real gateway caller maps only the platform CPU-reset error, uses a fresh
tenant stub and a command/path/artifact-bound active-call marker to quarantine,
and preserves committed keyed receipts. Deterministic error-shape tests can
prove wiring only. The bounded isolated development-platform test of the
unchanged scenario, retries, rollback, same-tenant follow-up, quiet neighbor and
receipt/digest behavior remains required. A remote pass cannot relabel local
CPU enforcement as passing.

## Remaining work and known failures

- Verify the combined X01 recovery ACK/capacity/placement work; verify
  suppression, stale scope, bounded staggered reconnect and recovery interleavings.
- Finish full scenario attribution; implement missing file/sandbox/application
  call-path coverage without substituting package harnesses for deployed handlers.
- Finish current-head generators, cycles against integration, full applicable
  checks, bundle/CPU/memory/wake/SQL measurements and one real dev deployment.
- Establish immutable Container input provenance. Existing K20 image ID is
  `sha256:c9acf7bb395b58c6bda81684269332e4213239d730b27f3bdf870f96100c4414`,
  registry digest `sha256:64b293d9a5f74cd7ea9bb498e40bf12c137026feb8824b4d076903e933ea8359`.
  Manager hash matches `6f6297c6f62ac564aa347f09f5203157f7db028e24e559594133bd3f39bc3f7f`;
  relevant source inputs are unchanged between K20 review SHA and the base.
  A complete build-input receipt linking those inputs to that image is still
  needed; image tag/time alone is insufficient.
- Direct Workflow reply fault/large-result delivery, aggregate isolate memory
  with legal large frames and full generation lifecycle bounds still need proof.

## Frozen combined candidate ad2c6078

Exact source: `ad2c6078aa27d9261fc3a76ac4ffb45b216de263` (parent `26d7261b`).
The full Cloudflare suite completed successfully on this frozen serving source:
820 workerd passes, 15 existing skips, plus four Node deploy checks. The 15 skips
are five external-emulator/public cases and ten Rust-only sandbox cases; they
remain coverage gaps, not parity passes. Typecheck and all generators passed.
Runtime import cycles compared with integration c97: one existing cycle on each
side, zero added (type-only imports excluded, matching package source inputs).
An initial comparison with missing base package inputs was invalid and retained.
Dry-run serving bundle: 1,607,447 bytes; gzip 385,156 bytes. These are bundle
measurements, not deployed CPU/memory/wake measurements.

Fresh assembled comparisons on separate owned fixtures:

- Explicit controller-removed/static-one-holder: receipt-expiry, scheduler and
  unchanged error-telemetry **3 pass / 0 fail / 0 skip**, 27.5 s. Actual holder
  inspection was not collected in this run, so the canonical-domain evidence
  still needs its authoritative holder/window receipt.
- Default controller-enabled automatic placement: live-query-window, Actions,
  replica-tenant-restart and public-calls **4 pass / 0 fail / 0 skip**, 29.8 s.
  This is semantic subset proof, not V03 capacity or full X01 acceptance.

Logs, reports, source hashes and commands: `evidence/ad2c6078/`,
`named-merged.sh`, `automatic-merged.sh`, `full-tests.sh`, `bundle-cycles.sh`.
The later optional diagnostic collector records actual holder/config identities
outside compared transcripts. Its 32 controller-status reads and holder RPCs
wake objects: it must be disabled for true cold/hibernation/cost fixtures, or its
overhead attributed explicitly. It neither drives a different serving handler
nor changes an assertion/normalizer. Diagnostic failures fail the scenario.

## Published candidate and retained follow-up failures

PR400 was opened non-draft against cloudflare/integration at
`a957d17b17b5d851e7ef6dc29108d1aa81f95912`. Independent review is pinned to that
immutable artifact; later changes need fresh review. No implementation-child
watcher or merge was started.

The fresh 38-selected canonical run did not complete: its first thin-SDK
scenario timed out on initial views, then waited indefinitely for an SDK
revision barrier. The finite job ended **exit124 / 900.026 s**, with 37 selected
cases unexecuted and no final runner JSON. The wrapper receipt measured whole-job
CPU 51.637397 s and peak 1,054,380,032 bytes; these are not Worker-isolate or
platform-memory measurements. `evidence/a957-followups/canonical-timeout.json`
and `assembly-combined-canonical-1.log` retain the failure.

0063's targeted-recipient fix resolved initial mounts in a fresh four-selected
run, but writes still stalled: catch-up used a global zero watermark while
ordinary writer sockets were not registered in the delivery engine. That run
ended at its finite 240 s bound without a completed report. Logs remain in
`assembly-0063-local-1.log`, with source reconstruction/patch snapshots in
`evidence/canonical-0063-failed-source`. It is not a passing named subset.

Source fixes now observe the actual server SocketView on ordinary tenant
dispatch, and register authorized quiet sessions during existing bootstrap/
recovery through the real empty-openMany handler. Catch-up uses only scoped
own work and invents no receipt/progress when there are no recipients.
Current verification remains incremental until a new frozen complete run.

Independent named observations on fresh owned fixtures:

- `external-auth-local-1`: real Firebase Auth Emulator 15.35.0 with synthetic
  users, unchanged control-external-auth **1 pass / 0 fail / 0 skip**, 10.8 s.
  Both real serving handlers were used; the owned emulator stopped afterward.
- `telemetry-domains-local-1`: actual automatic placement, measured holder IDs
  and window starts/counts. Same-account/same-holder accepted 20 performance +
  100 error units then refused two; a distinct holder accepted 120/refused one;
  two anonymous sockets on the SAME holder each accepted20/refused1. Operator
  attribution/privacy assertions passed. Inspection overhead is explicit; this
  is not a cold/hibernation/cost or automatic-capacity result.
- `sdk-deploy-local-1`: original supplemental probe FAIL preserved (wrong new
  contract-field expectation and digest-checker resume model gap).
  `sdk-deploy-local-2`: corrected numeric contract expectation and verified
  same-socket resume model, **zero failures on Rust and kernel,16/18 digests,
  equal final DB checksums**. It exercised actual K19 entry swaps, unchanged
  SDK reconnects, generation/schema/defaults, stale-artifact refusal, durable
  receipt/replay, visibility/reauth and contract gating. This was incremental
  working-tree proof with gateway/driver/checker hashes in its directory, not
  final-head or private-platform rollout acceptance. Original raw continuity
  scenarios and all failed artifacts remain unchanged.

The verified-resume checker preserves all ready/delta digest assertions and
adds negative socket/id/descriptor/scope/cursor/hash/staging/reset/close cases.
Current Node conformance units:49 passed. The native Host ABC test uses a
controlled stall around the actual hook; it is source-boundary evidence, not a
platform fault. Early failed test/cleanup-context receipts are retained.

Session-owned, generation- and scope-bound terminal errors for failed quiet
initial requests, including real unchanged-SDK post-error reducer/revision
liveness, remain OPEN. A provisional engine-only helper was removed before
publication because it removed correlations too early and could apply instance
state while reporting errors. No passing assertion was removed to hide it.

Expanded attribution inventory: `cloudflare/kernel/assembly/evidence/inventory.mjs`
indexes all39 formal scenarios with source hashes/document references and six
package golden/definition sources. `package-comparisons.json` records eight
explicit additional package coverage gaps; documentation mentions are not
passing receipts. These missing cases must be implemented, not waived.


## Independent a957 review follow-up (working tree, not acceptance)

The independent review of published `a957d17b17b5d851e7ef6dc29108d1aa81f95912`
returned **CHANGES REQUIRED** ([formal COMMENT](https://github.com/Whagons-International/gonvex/pull/400#pullrequestreview-5874287424)).
Its six findings and the original acceptance failures remain binding until a new immutable artifact,
regressions and fresh independent review establish their resolution. The original report and seven
synthetic source-method reproductions remain at `/mnt/data/work/gonvex-k22-review-a957d17b/`.

Current changes under verification:

- N1: nested Reducer calls propagate server-derived anonymity. Synthetic app modules run through the
  actual AssemblyTenant/Action Worker in workerd. They prove omitted scheduler/outbox surfaces and
  rollback of an uncaught nested attempt; an outer Action may catch that failure. Catching an omitted
  surface's local JavaScript TypeError retains ordinary JS semantics and can commit permitted DB writes,
  while no jobs/outbox entries appear. The existing dispatched-host denial latch test is unchanged.
- N2: normal, nested and scheduled Reducer write entries share the same migration admission rule.
  The new integration holds an actual multi-chunk TenantMigrator between chunks and refuses nested
  and scheduled writes without acknowledging them. This is local module integration, not platform rollback proof.
- N3: trusted service access, original command id and configured API/system provenance reach inline
  and Workflow Actions. Ingress/share await actual agent results; pending cannot become successful null.
  New real-handler tests cover internal standard/agent calls, failure/retry and an actual agent Workflow.
- N4: execution completion and terminal reply settlement are separate. Full replies use private 64-KiB
  binary chunks, generation/integrity metadata and an additive generation-bound gateway receiver.
  Failed delivery retains bytes/correlation without rerunning the Action. Missing native enumeration
  with durable socket ownership is UNKNOWN, not obsolete. Legal large principal/argument inputs also
  use an atomic private chunk manifest; integrity failure never invokes a partial/default context.
  Valid legacy inline inputs remain readable, without minting missing legacy reply generations.
- N5/0064/0067: actual initial-operation completion has an original server generation, operation index,
  accepted/refused outcome and explicit enqueued/obsolete receipt. Arbitrary old change/progress frames
  cannot consume a replacement. A completed no-change initial progress can accept its own request.
  `initialScope` fences every initial page/split before instance/data/watermark changes; the additive
  initial-only receiver fails before an older holder can apply an unknown field. Initial-only trailing
  watermark authority is suppressed when the contributing completion cannot accompany its part.
- N6/0065: provisional authority admission bounds historical/UNKNOWN retention, without eviction or TTL.
  This does **not** prove terminal release, safety after losing in-flight accounting, eventual cleanup,
  availability under sustained churn or 128-MB fit. An authoritative token/drain lifecycle is still open.

Working-tree verification is separate from the published head. Receipts and preserved earlier failures
are under `/mnt/data/work/gonvex-k22/review-fixes/`. `source-4.json` pins the earlier incremental source;
its base SHA is not a claim that those edits were present in that commit. The fourth focused run passed
67 delivery/Session/native Host/recovery/admission cases, 26 gateway/outlet cases, 11 ActionCalls cases
and 6 actual-handler synthetic-module integrations. The fifth ActionCalls run passed14, including
2M permissions plus agent args, corrupt/missing input refusal and the complete legal8-MiB result through
failed delivery/duplicate settlement. Further atomic-transfer and actual-host overlimit regressions were
added afterward and require their own result. These are author reports, not a source approval or deployed proof.
A healthy small completion measured **11 statements,8 rows read,9 row/index writes**, versus2 prior writes;
this excludes once-per-object schema DDL. Large/chunk/failure costs and isolate fit still need measurement.

The frozen `abc8e1a6eed2bb77be8dd875105b404567e8c90c` package run passed821 workerd tests with15 existing
skips plus4 Node deploy tests. Its named static-one-holder comparison passed thin-SDK, live-query,
tenant-restart and telemetry4/4 in46.3s. Both predate these review fixes and remain partial evidence.
All original38-scenario failures, timeouts and unexecuted rows remain unchanged.

Real unchanged-SDK quiet-initial fault followed by immediate write/receipt/revision liveness remains OPEN.
So do full canonical/default automatic campaigns, exact local pure-loop enforcement (0057), actual
private-platform CPU/quarantine/rollback/neighbor proof, abrupt owner lifecycle, legacy Action rollout/drain
preconditions, all additional package matrix gaps, Container/file/application/bridge/device and V/R gates.
No staging switch, production operation or monitoring activation occurred.

The former hosted preview at a957 was skipped for missing Cloudflare secrets; its green conclusion is
not image/deploy evidence. The lead now owns standalone credential-free build publication and artifact
verification (PR404), followed separately by approved Vault-only daemonless publication. K22 removed its
unpublished duplicate image job. The prepared helper's export path is not an executed build receipt.
No Cloudflare credential was retrieved or copied to GitHub, and no local Docker build/workaround ran.

The sixth focused run retained one new failed assertion (`calls-6.log`): local SqlStorage accepted
an aggregate legacy principal/argument row instead of enforcing the expected platform row limit.
That supplemental expectation was replaced by an exact UTF-8 byte observation; it does not establish
platform limit enforcement. The same run passed67 root delivery/Session cases,26 gateway cases,
14 other ActionCalls cases,7 real-handler module integrations and typecheck. Follow-on tests cover
partial input-publication rollback, atomic terminal transfer, corrupt reply retention and the automatic
reply retry ceiling; their results belong to the next frozen source.

Reply settlement admits at most8 known failed automatic deliveries at30-second renewal intervals.
After that it retains the complete private manifest/chunks in an explicit operator-recovery state,
excludes the row from automatic alarms and receipt expiry, and refuses duplicate completion settlement.
No Action execution is repeated to reconstruct an answer. This bounds automatic retries, not retained
unknown-owner availability or memory fit; a recovery/operator lifecycle is still required.

0069 authorizes a prototype tenant-instance owner token/admission/drain protocol, with actual-holder
confirmation and UNKNOWN transport outcomes. This checkpoint deliberately contains only the finite
0065 admission guard. No token lifecycle or eventual terminal-owner cleanup is claimed implemented.

### Frozen e07 combined collection: FAILED

Implementation checkpoint728ce38a ordinarily merged integration86c80c8c without conflicts, producing
`e07a616be0ff10b065864b4a3f33cdfcf5b11e83`. Exact source hashes, commands, configs and raw logs are under
`/mnt/data/work/gonvex-k22/evidence/e07a616be0ff10b065864b4a3f33cdfcf5b11e83/`.
Typecheck, all CI generators,49 conformance unit cases and the bundle/cycle comparison passed.
Runtime cycles stayed1→1 with no additions; bundle1,662,248 bytes /397,491 gzip bytes.
The full package command FAILED in an original S4 Action scenario: the new completed-run branch returned
rejected for an older attempt, whereas the original receipt is duplicate. Later suites were not executed.
The follow-up restores that original receipt; only the actual completed attempt can retry its retained reply.
No original S4 assertion changed.

The four-scenario actual assembled named campaign timed out at240 seconds (exit124), during thin-SDK.
There is no final runner JSON, and later scenarios are UNEXECUTED. New initial completion receipt maps
used a null prototype, which native Gateway RPC could not serialize (DataCloneError). They are converted
to plain own-key records only at RPC boundaries, without a row/result JSON roundtrip. A native RPC positive
ENQUEUED receipt, obsolete path, ordinary missing-metadata path and special own-key regression follow.
These fixes do not relabel the frozen e07 failures or establish unchanged-SDK fault liveness.

The real local SQL observation measured2,300,241 UTF-8 column bytes in the legacy principal/argument shape.
Its chunked input manifest covers2,300,357 bytes in36 chunks, each<=65,536 bytes. These exclude SQLite
row headers/physical page overhead and are representation/integrity evidence. Per-column measurements
are emitted by the follow-up fixture. Local acceptance does not prove the conservative2-MB platform ceiling
or current documented8-MB remote enforcement. [Cloudflare's current limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
was read2026-11-12 (page updated2026-11-07); no brief/contract limit was changed.

Follow-up source checks found the barrier floor must precede the no-subscription return: recovery
clears old LQ state, whose absence cannot certify a quiet writer's progress. Malformed/missing completion
receipt tests cover this zero floor. An older K22 supplemental0061 unit assumption of an undefined floor
before installing its deferred query was strengthened to exact0 while the holder fence remains unacknowledged.
No original package/Rust-vs-kernel scenario assertion changed. The retained Engine initial-reference lifecycle
after an acknowledged Session rebuild remains a source follow-up: recoveryAck alone is not a0064 terminal
completion receipt and is never used to bulk-clear refs.

### Frozen77e source checkpoint: local suites and four named scenarios PASS; acceptance UNMET

`77e63e5c1dfeec0640404ea5b1a22af1a7cdec07` passed the full combined package command:
857 workerd cases /15 existing skips plus4 Node deploy cases;49 conformance unit cases; typecheck;
all CI generators with no tracked drift; and runtime-cycle comparison1→1/0 added. The built serving
bundle measured1,662,345 bytes /397,514 gzip bytes. The machine-readable checkpoint in
`cloudflare/kernel/assembly/evidence/checkpoint-77e63e5c.json` pins its bundle/report hashes and all39
formal scenario statuses. Source/config/commands/raw reports are under
`/mnt/data/work/gonvex-k22/evidence/77e63e5c1dfeec0640404ea5b1a22af1a7cdec07/`.

The unchanged thin-SDK, live-query-window, tenant-restart and telemetry scenarios passed4/4 in39.6s
against ONE fresh local assembled runtime and the independently validated Rust reference. This campaign
selected the approved controller-removed/static-one-holder baseline up front; it is not X01/default fleet
proof. Every other scenario is explicitly unexecuted on this checkpoint, and all original failed campaigns
remain retained. Native Gateway RPC positive ENQUEUED/obsolete/own-key receipts and all original Action S4
assertions passed. Real unchanged-SDK fault/error/revision liveness remains unproven.

Actual local SQL fixture column bytes were args300,012 +session2,000,229 =2,300,241 UTF-8 bytes.
The encoded INPUT (path/args/session/provenance) manifest declares2,300,357 bytes /36 chunks; this is not
metadata-row size. None of those sums include SQLite row headers/physical pages. Actual remote limits,
isolate RAM/copies, Action legacy deployment preconditions, reference cleanup and terminal-owner lifetime
remain open. No Cloudflare deploy/artifact/config id exists for this checkpoint; no credential or deployment
was used. The resource receipt measures the WHOLE admitted unit, not a Worker isolate.
