# Step 2: server-validated reload metadata

Measured 2026-11-05 UTC. Branch `perf/reload-delta`, based exactly on reviewed
integration runtime `e5812dfbcf27eca83ed0c224f40e954df91b4170`. This adds a
bounded metadata shortcut for previously server-validated XOR cursor resumes.
It does not optimize first visits, alter visibility rules or change the wire
contract. No SDK, app, schema, production, staging or CI changes were made.
Integration has not approved or merged this branch.

The saved mixed-cursor reload replay improved modestly and the supported
cursor paths performed zero whole-list evaluations. This is **snapshot-path
CPU plus a production codec model**, not actual whole-app authentication or
WebSocket CPU. The currently shipping collection set and negotiated wire
mode remain unobserved because its managed-auth capture stops before opens.
The 20–50 ms end-to-end runtime reload objective is not proven here; even the
disabled-shortcut replay was already within that range.

## Exact scope and integrity

`replica_resume.rs` retains only a server-computed aggregate, cursor, window
counters and recursive visibility fingerprint. It stores no rows, row hash
maps or encoded snapshot payloads. Defaults are 8,192 entries, 8 MiB charged
bytes and 120 seconds TTL. `GONVEX_RESUME_VALIDATION=0` disables the shortcut;
`GONVEX_RESUME_VALIDATION_ENTRIES`, `_BYTES` and `_TTL_SECS` override limits.
The LRU is runtime-global, with no tenant reservation or fairness guarantee;
eviction only causes authoritative fallback. Runtime drop releases the cache.
Expired entries are removed on matching lookup or later insertion; untouched
expired entries may remain allocated within the fixed capacity.
Module replacement changes the lookup scope immediately; unreachable old
metadata can remain allocated within that same bounded lifecycle.

A preliminary lookup happens **before** `SnapshotCache.entry`, so a valid
resume does not rebuild an evicted base snapshot. This locator cannot
authorize a response. Its versioned key includes project, full database URL,
tenant, module generation/artifact, path, canonical full args, account/member,
membership/admission revisions, directive visibility scope and window shape
(table/key, budgets, ordering, direction and mode); epoch is appended. A short
admitted read rechecks the module identity and exact recursive resolved
fingerprint, active SQL membership, epoch, retained floor, cursor ordering and
visibility-index readiness. The fingerprint format has its own version tag;
it is not assumed to be a universal future permission primitive.

Only a successful authoritative read or fully verified replay installs an
anchor, after its read transaction commits. A client digest never creates
trust. Replay from the server anchor to the requested cursor must reproduce
the supplied digest; replay through the read watermark computes the actual
current digest and deltas. Changed rows still undergo argument filtering,
visibility checks, projection, canonical encoding and hashing. Updates,
inserts, deletes and transitions into/out of visibility update the aggregate
and byte/row counters. Delta replay requires the declared key to be the sole
SQL primary key and to match the visibility key.

Missing proof, altered digest, module/principal/dependency changes, stale or
unknown epoch, pruned history, missing transaction/revision/internal ordinal,
index lag, truncated/boundary windows, replay over 4,096 revisions/rows and
budget/checked arithmetic failures use the existing authoritative path.
Fallback commits/releases the preliminary transaction and explicitly drops
its admission permit before entering the original cache-entry/read path.
Cancellation drops temporary work and permits; older concurrent completions
cannot overwrite a newer-revision anchor.

History validation relies on the existing atomic raw-log writer contract.
Transaction headers, every revision and contiguous ordinals are checked.
The schema has no expected trailing-row count/checksum: arbitrary deletion
of the last change row within an otherwise valid transaction is not certified
detectable. This is not crash-corruption repair. A restart has no metadata and
uses authoritative validation. Native `sum128` requests also use the existing
authoritative path; this implementation accelerates legacy `xor1` only.

## Matched offline measurement

The immutable synthetic source was
`/home/gabriel/work/gonvex-perf/lab-5k-new/baseline-data`. Its entire DB tree,
including WAL/SHM, was previously reflink-copied to the owned
`/mnt/data/work/gonvex-open-cost/offline-data`; only copy routing URLs were
rewritten and copied pending scheduled/outbox work cleared. The constructor
requires an ownership marker. No source DB or endpoint was edited/contacted.
The stored app module hash is
`da98a6665b3b2f834f67e7a5eee99d43708abe36f415dafdfc20bcf47439c069`.

Inputs: normalized `bench/whagons/app-opens-7d97617cf.json`, exact 130 paths
with their args and `$member` substitution, and raw `reload-opens.json`
cursor-presence flags. The final replay uses `BENCH_EXACT_CAPTURE=1`, so it
does not add the historical harness's deduplicated metadata/task request
(131). Saved first visits contained 177–178 opens including navigation and
onboarding duplicates; the first-pass figures below cover only these 130.
This capture predates the current shipping SQLite app, whose count is pending.

Eight admitted synthetic members cover two admins and two ordinary members
in each of the two biggest tenants. Same-role users are not asserted to have
identical visibility; their user-specific collections differ. No cross-user
sharing was introduced. First pass is once per session, in the listed order;
the first member warms its tenant cache and later members also benefit from
prior SQL-oracle work. Reload is the median of three no-write passes. This is
not a concurrent burst or a 1,000-user capacity proof.

Matched final comparisons run the **same optimized executable**, with
`GONVEX_RESUME_VALIDATION=0` versus `1`: unchanged authoritative algorithms
versus the shortcut. `taskset -c 3`, one Tokio worker, two DB readers and two
snapshot slots; unchanged thin-LTO profile/toolchain/RUSTFLAGS. The earlier
frozen unchanged e581 runtime executable remains at
`/mnt/data/work/gonvex-reload-delta/before-bench`, SHA-256
`6cb29196a4df2413e8e521aeb717bf3e62e04d73ca2647226c2356763c88daee`.
It ran the historical 131-request mixed replay and 1,048 exact SQL comparisons;
it is not substituted for the matched final 130-request comparison.
Final executable SHA-256:
`962e1413d1f5ec0f4cf40864c12648c7897d2cf132cf0d70b7ea98e01eae8126`.

Timed regions include real `open_replica`, result/subscription construction
and drop, the production legacy `SnapshotPages` iterator, JSON serialization
and no-context-takeover deflate (window 12, threshold 1,024). Bytes are discarded.
Runtime/module startup and tenant/session admission occur outside timing.
HTTP/auth/token exchange, actual WebSocket orchestration, ingress decoding,
offload scheduling, socket queues/sends, client hydration and persistence are
excluded. Sequential process CPU is user plus system `getrusage`; tracing and
counting-allocator overhead are present in both modes. These are not wire
latency measurements or complete sign-in CPU.

| Synthetic session | First CPU ms, disabled | First CPU ms, enabled | Reload CPU ms, disabled | Reload CPU ms, enabled |
| --- | ---: | ---: | ---: | ---: |
| Tenant 0 admin 0, tenant cold | 698.126 | 568.139 | 25.364 | 23.449 |
| Tenant 0 admin 1 | 67.544 | 66.868 | 24.737 | 21.774 |
| Tenant 0 member 2 | 51.536 | 54.078 | 20.507 | 17.236 |
| Tenant 0 member 3 | 65.388 | 67.419 | 21.846 | 17.939 |
| Tenant 1 admin 0, tenant cold | 280.371 | 248.961 | 22.775 | 20.910 |
| Tenant 1 admin 1 | 53.982 | 55.166 | 26.610 | 19.625 |
| Tenant 1 member 2 | 48.156 | 54.373 | 19.157 | 18.166 |
| Tenant 1 member 3 | 53.879 | 57.506 | 19.604 | 18.308 |

Median across the eight reload medians: **22.311 → 18.967 ms**, about 15%.
One ordered first sample per user does not support a first-visit speedup
claim; some warm first passes became slower. This metadata is not a solution
to the observed ~1.5 CPU-s first visit or ~0.75 CPU-s browser reload.
Raw aggregates and representative traces are in
[results JSON](evidence/reload-delta-results.json); original artifact hashes
are in the [manifest](evidence/reload-delta-artifacts.json).

## Every path, cursor lifecycle and shipping mode

The [130-row per-path table](evidence/reload-delta-paths.md) and
[CSV](evidence/reload-delta-paths.csv) classify the exact saved capture.
For representative admin 0: 87 cursor opens previously validated the entire
cached view; 2 (`members.listMemberTeams`, `roles.memberTeams`) used SQL
validation; all 89 now use server metadata/log validation. The 41 cursorless
requests still produce cached snapshots; none were dropped or deferred.
Across eight sessions, **all 712 cursor opens had zero `cache_view`,
`scan_row_raw` and cache-fill evaluations after the change**. Before: 696
cache-view calls and 44 raw scan calls (the SQL scans returned zero rows).
The trace covers the first reload iteration, independently of the median
phase samples. The focused test also proves no whole-list evaluation after
explicit shared-base eviction. Actual delta catch-up validates changed row
images and is tested separately from this no-write benchmark.

Both final modes performed 1,040 exact projected-row-set comparisons
(130 × 8) against fresh SQL, outside timing. They compare cold authoritative
snapshots; focused resume tests separately compare resulting cursor/digest
against the fresh SQL oracle. These checks are not an exhaustive adversarial
or concurrency proof.

The replay sends **89 XOR cursor digests and 41 cold requests with digest
omitted per session; native sum128 count is zero**. The saved raw capture
contains cursor flags, not digest values. The actual SDK would send its empty
XOR aggregate on cold opens when integrity capability 1 is active; cold
algorithm selection is equivalent here, but ingress is not byte-exact.
No sum128 CPU improvement was measured.

Pinned shipping source (`37ca8278b`, client `0.5.2-sqlite.1`) routes
`replicaOpenRequest` through `windowResumeState` and
`replicaAggregateDigest`, which emits `xor1`; the inspected pinned client JS
SHA-256 is
`4f9ce8be47c21742b49d760ebf0c695da13ab4c2f814d6df5a7d843a416fa2b4`.
This supports an implementation expectation, **not a negotiated live-mode
observation**. Current shipping capture has zero collection opens: managed
auth blocks earlier. Actual mode counts, shipping collection count and
whole-app CPU remain pending with parent/integration.

Why 41 opens lack cursors remains unproven. Of 40 cursorless whole-table
paths, 38 had empty tables: correlation, not a demonstrated cause. Existing
IndexedDB code restores empty window metadata. Source instead exposes a
possible hydration race: `activateScope` starts asynchronously while
`ensureLocalCollections`, send/flush and `windowResumeState` can run before
`replicaReady`. The read-only instrumentation plan is preserved at
`/mnt/data/work/gonvex-gate6-capture/auth-source-investigation.json`
(`hydrationAndCursorPlan`).

Required proof: the same path/args/scope/generation has a valid durable
checkpoint, emits a cursorless open before hydration, then its unchanged
resume state gains a cursor after hydration. Only if observed, propose to
Gabriel a conditional batch-after-hydration change with scope/socket
generation, ownership, disconnect, listener and failure guards. No SDK/app
edits, artificial delay or extra reopen were implemented or authorized here.

## Memory and queue distinctions

After the enabled run: 1,040 metadata entries, **4,139,440 charged bytes**.
Clearing only this cache freed **2,142,312 allocator-live bytes**. Charging
includes retained String capacities, boxed fingerprint, aggregate storage,
a deliberately oversized BTree node per entry and spare-node/container
allowance. It is an admission accounting bound, not measured resident bytes.
Temporary candidate/key/replay allocations and concurrent active futures are
outside that retained-cache charge.

RSS stayed 817,123,328 bytes before/after clear: freed allocations were not
immediately returned to the OS. Disabled run RSS was 751,620,096 bytes;
the run-to-run difference also includes all other allocator/cache history
and must not be attributed to metadata. Eight sessions do not measure the
LRU's saturated 8,192-entry lookup/eviction cost or tenant contention.

Historical queue evidence is preserved on `perf/open-cost` at
`1f116a47bfc5fae439307d8b89806dc72f0b152c`. Its unchanged admission limit
does not bound cold same-key single-flight futures before admission. The
pure-future probe excludes connection/socket/input queues and does not
explain the full observed ~1.2 GB. This branch changes neither queue limits
nor retries and makes no queue-memory speedup claim.

## Verification and known failure

All heavy work used build-headroom and resource admission, actual target
`/mnt/data/work/gonvex-open-cost/target`, unchanged coach-compile/caps.
The owned lab reservation was released at 05:13 UTC, before 05:15; all own
jobs stopped. No further heavy verification is authorized in this window.

- `cargo test -p gonvex-runtime --release --features perf-fixture --lib --test resume_validation resume -- --nocapture`:
  7 selected library tests and 1 integration test passed.
- Optimized full library run: **398 passed, 1 failed, 8 ignored**.
  `connection_future_sizes::connection_future_sizes` reports WebSocket
  future 29,664 bytes versus unchanged 28,672 budget. Exact base
  `e5812dfb` runtime sources under the same options produce the same 29,664
  compile-time layout. This pre-existing optimized/feature-specific failure
  remains unresolved; integration's earlier non-release suite was green.
  Both [full-suite output](evidence/reload-delta-final-tests.log.txt) and
  [baseline layout diagnostic](evidence/reload-delta-layout-baseline.log.txt)
  are committed. The latter intentionally fails compilation to reveal the
  type size; it is diagnostic evidence, not a passing baseline suite.
  Temporary diagnostics and unsuccessful public-admission boxing experiments
  were removed. No stack-budget, profile, toolchain or RUSTFLAGS changes.
- Final `cargo test -p gonvex-runtime --release --features perf-fixture --test resume_validation --test resume_log_only --bench reload_trace --no-run --message-format=json`:
  compile exit 0. Both final integration binaries then passed directly:
  1 new expanded test and 1 existing log-only test; see
  [test output](evidence/reload-delta-integration-tests.log.txt).
- New tests cover zero-scan/nochange, evicted base, tampered digest without
  poisoning proof, different user/tenant/path/full args, module generation,
  update/insert/delete, anchor-to-request-prefix/current-suffix replay,
  retained floor, see-all-spots grant/revoke, cancellation before short-read
  admission and permit recovery. Unit tests cover TTL, zero limits, byte
  eviction, truncated/boundary rejection and older-completion ordering.
  Native sum128 optimization, arbitrary trailing-log corruption and a real
  concurrent permission/multi-session race were not demonstrated by these
  focused tests; no such coverage is claimed.
- `pnpm --config.verify-deps-before-run=false --filter @gonvex/docs typecheck`:
  docs metadata generators, route type generation and TypeScript all passed;
  [output](evidence/reload-delta-generators3.log.txt). No tracked generator
  output changed. No Gonvex schema, agent API, i18n or TS runtime imports
  changed; no additional generator input or runtime import cycle was added.
- Touched Rust files passed `rustfmt --check --edition 2021 --config skip_children=true`;
  `git diff --check` passed. A recursive formatting check also reports existing
  differences in untouched `open_reasons.rs` and `snapshot_cache.rs`; those
  files were not reformatted.

There were no integration conflicts at the stated base. `replica.rs` and
`lib.rs` overlap later runtime work and require parent review before any
integration merge. Visibility/principal-change implementation files were
not edited. Next evidence needed is the unblocked shipping capture and its
actual mode/collection/cursor lifecycle, plus parent-owned full-runtime gate
and a future separately admitted verification window for remaining checks.

## Follow-up: deterministic permission race, source prepared

Reviewed implementation baseline remains immutable at
`cc91423b3a7a4616fa34517abbe4371887574fe1`. The follow-up adds only a
`resume_permission_race` integration-test target and this report update;
there is no runtime fix or refactor. **The new test has not been compiled or
executed yet.** Source was prepared while the permission child owns the
05:14–05:43 UTC reservation; no build/test job was started. Verification must
wait for an explicit next lab grant, regardless of lock-file presence.

The three cases separately revoke `members.see_all_spots`, soft-delete the
member's `memberTeams` links, and downgrade the member's SQL role/permissions.
Each establishes a server anchor and proves a preceding unchanged resume
does zero whole-list evaluations. The original admitted session and request
scope remain unchanged so this cannot pass by simply changing the locator.
The test holds every real snapshot admission slot, polls the actual resume
until the public queue count confirms it is waiting, commits the dependency
write through the fixture's separate writer, then releases the permits.
There are no sleep-based race windows or feed-applier sleeps in this target.

It compares the queued result's complete messages and subscription presence
with the same request through the authoritative SQL path. Revocation must
produce only `needHashes`, without a stale `ready`, subscription or row
payload. Then the existing SDK client model sends its keys/hashes: both
paths' messages, subscription scope, resolved principal/fingerprint and
revision must agree, and applying the response must leave exactly the fresh
SQL row set with valid digest and every actually revoked row absent. A
nonempty revoked set prevents a vacuous fixture pass. Because `needHashes`
has no wire cursor, the test brackets the queued read with the unchanged
post-commit DB revision and explicitly checks that revision on subsequent
ready/subscription/oracle results. Admission permits and queue entries must
be conserved after completion. One test binary serializes the three cases
and avoids interference with the other tests' global counters/admission.

A second, **during-read index-advancement** interleaving is not implemented:
source inspection found no existing deterministic pause hook between the
admitted read snapshot, resolved parent/index binding and final readiness
check. Public admission permits stop before that interval; holding reader
pool slots also stops before the SQL snapshot. No production hook, scheduler
guess or timing sleep was added. The supported contract remains correctness
at the transaction's read revision, with exact index-readiness guards before
and after replay; it is not latest-permission-at-send-time atomicity. The
socket's later feed/revalidation ordering is outside this offline test.
The in-read race remains an explicit unexercised guarantee, not passed
coverage.

Two static cost limits also remain unmeasured: `remember` walks the whole
entry map for expiry and may walk it again for LRU eviction under a global
mutex; history `COUNT`/grouping can scan a large transaction before rejecting
more than 4,096 rows. The retained/replayed-row cap does not bound that SQL
scan's CPU. Eight-session results do not measure these saturated costs.
No broad cache/log refactor was made; quantify them only in a later admitted
window.
