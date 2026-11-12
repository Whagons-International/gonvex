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
fresh comparison is pending. Original raw receipt-upgrade/session-handshake
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
