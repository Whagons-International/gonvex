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
Current gateway placement is static pending the lead's X01 integration.

Live-query initial replies and commit updates share `ReplicaDelivery` with
replicas; query frames precede replica frames. Known socket work and every split
part carry a subscription epoch. Missing-epoch scoped work fails closed and
unscoped watermarks do not certify a current auth scope. Universal recovery
requires an explicit holder acknowledgment after real query rebuilding; X01's
outlet/protocol/Gateway changes must be integrated before final recovery proof.

0058/0059 recovery metadata lives in versioned private KV generations, chunked
to at most 64 KiB UTF-8 per text chunk. It includes semantic query requests,
replica tracking and direct Action correlation, with conditional oversized
identity storage. Correlation metadata does not execute/replay Actions. Latest
per-commit resume hashes stay in memory; restore can require a fresh full result.
Generation ownership, digest, scope/epoch and token changes are checked before
restoring the Session. Incomplete publication fails closed; old generations are
kept until publication succeeds and abandoned/closed generations are reclaimed.

## Observations so far

| Check | Observed result | Evidence |
|---|---|---|
| Frozen dependency install; package builds | Passed with caches/output on `/mnt` | `install.log`, `build-packages-2.log` |
| Incremental Cloudflare typecheck 15 | Passed | `typecheck.log`, `typecheck.result` |
| Incremental complete Cloudflare package suites | Passed before later recovery/Control changes; sandbox uses existing CI stub mode | `full-tests.log`, `full-tests.result` |
| Focused ordering/query/Session/private-KV tests | 31 passed | `targeted.log` |
| Real holder attachment/eviction/cleanup tests | 19 socket tests passed, including two new metadata cases; synthetic fixture setup uses actual Host code | `host-tests.log` |
| K10 commit reads, 1/10/100 subscribers | 4 statements / 108 rows read in each case | `targeted.log` |
| Large synthetic identity | 2,000,000-character permissions: JSON attachment 4,001,540 → 715 bytes, private generation 245 chunks | `targeted.log`; this row is JSON/KV measurement, separate from native Host attachment test |
| Fresh Rust reference | Built unchanged source with `test-firebase-auth-emulator`; binary SHA256 `ec11c67352775fe44a0b0c354d2c335e8b68d7066d7bddc7677bc0c3e2e42036` | `rust-reference/build.log`, `source-files.sha256`, `binary.sha256` |
| Fresh Rust-vs-assembled local comparison | Running named 38-scenario subset; thin SDK, second client and replica resume pass; live-query unsubscribe currently fails | `assembly-compare-local-1.log` (final report pending) |
| Actual development deployment | Not yet run | No artifact/config IDs yet |

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

- Fix the observed live-query unsubscribe/late-delivery ordering failure, then
  rerun the unchanged scenario on a fresh fixture.
- Integrate X01 recovery ACK/capacity/placement work through the lead; verify
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
