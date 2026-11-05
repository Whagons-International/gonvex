# Full-app heap growth investigation

Investigation dated 2026-11-04/05. Branch `perf/heap-growth`, from `636e3c45`;
allocator/cache instrumentation `09799a27`, delivery memo fix `ba54cee4`.
No staging traffic, deployment, release-branch merge or CI work was performed.

## Finding and limits

The lab reproduces high anonymous RSS and repeated principal-triggered replica
reopens. It does not reproduce an unbounded live-heap leak over the measured
interval. The unmodified-base 30-user run rises to approximately 950 MiB anonymous
RSS at minute 15, then stays at 946–957 MiB through minute 40 while principal
reopens grow from 1,047 to 2,418. This cannot rule out a slower leak in staging.

The missing memory has several owners: module artifacts, shared parent ACL
indexes, snapshot-cache row trees and views, subscription visibility, and a
large resident-minus-live remainder. The delivery memo has bounded accumulation
that is especially long-lived at low write rates. Its budget counted encoded
text but retained cloned `ServerMessage` value trees too. The sampled
`delivery::prewarm` allocation sites grow from 5.9 to 19.4 MiB between minutes
5 and 30, even though the released feed backlog is zero. These are allocation
sites, not a traversal of current ownership; stack classification is approximate.

## Reproduction and evidence

Existing lab: `/home/gabriel/work/gonvex-perf/lab-hg`. Separate `gvhg-*`
containers, copied fixture/data and Whagons app `7d97617cf`; no staging requests.
30 thin-client users, production tenant mix, real page-load collection list
(130 opens), task-detail window churn, principal-triggered needHashes reopens,
presence v2 and writes. Runtime: 2 CPUs pinned 2,10, 3 GiB cap; generator:
CPUs 3,11. This is a memory reproduction, **not** the 1 CPU/2 GiB 1k gate.

Samples are in `<run>/samples.jsonl`, environment in `<run>/runtime-env`,
binary SHA in `<run>/commit`, generator logs in `<run>/gen.log`.
`heap/je-30u` and `heap/jefix-30u` contain jemalloc profiles every 300 seconds
with `prof:true,lg_prof_sample:19` (512 KiB sample interval). `bin-je` is
`09799a27` plus allocator-stat read wiring; `bin-fix-je` is `ba54cee4`.

The previous soak driver did not acquire its own exclusive lock: it only
paused on another holder. Historical runs therefore have uncertain foreign
build/load contamination; the WIP and trim runs also have recorded lock pauses.
WIP ended early during a pause (31.3 sampled wall-clock minutes, not the intended
40 active minutes). Trim spans 51.9 wall-clock minutes with pauses. Purge and
jemalloc spans are 32.3 minutes. Treat all cross-run RSS differences as exploratory,
not isolated A/B performance proof. Thin-client workload fidelity is also limited
by the harness retry behavior under investigation in the integration series.

### Recovery of the interrupted fix run

`jefix-30u` runtime started 2026-11-05 03:08:57 UTC; generator at 03:09:01.
The provider failed at 03:10:32. The containers survived but the controller and
sampling process died. Last original sample: 03:10:32; sampling restarted at
03:14:55 (263 seconds between samples). The lock was absent on recovery and was
created with exclusive-create at 03:14:35; no existing holder was overwritten.
A foreign development runtime was present; no cargo/rustc process was found on
recovery. Contamination during the earlier unlocked interval cannot be excluded.
The same containers continued; neither the workload nor the data was reset.
This is an interrupted observation, **not** a clean continuous 30-minute soak.
It ended at 03:41:08 with runtime and generator stopped; ownership/state assertions
preceded lock release at 03:41:48 UTC. Direct notifications went to open-cost,
reopen, integration, then parent in that order. No new soak was launched.

57 samples span 32.1 wall-clock minutes, including the missing interval. Final
principal reopens: 1,731; expired memo keys: 945. Late metrics show 30 connections
and 3,938 subscriptions. The collector's `ws.subs` field incorrectly counted
connection entries; use `/dev/metrics` `websocket.subscriptions` instead.
The last-ten median memo estimate is 0.28 MiB (five tenants); this is not the
sampled profile owner total. Anonymous RSS is higher than in the earlier jemalloc
run despite similar live Rust bytes. Total RSS reduction is **not demonstrated**;
native allocator retention, profiling overhead and uncounted native allocations
remain possible contributors.

## Exploratory allocator comparison

MiB; medians of the last ten unpaused samples, which can include churn and cache
expiry. Different live cache contents prevent causal attribution of the RSS delta.
The live counter covers Rust global-allocator requested bytes; SQLite and V8
allocations through their native allocators are separate. Their totals and Rust
live bytes must not be interpreted as a complete RSS decomposition.

| Run | Anonymous RSS | Rust live | Cache estimate | SQLite used | V8 physical |
| --- | ---: | ---: | ---: | ---: | ---: |
| base-30u (636e3c45, mimalloc) | 955.8 | unavailable | 84.0 | 178.5 | 76.6 |
| wip-30u (09799a27, mimalloc) | 911.3 | 351.5 | 77.3 | 176.0 | 69.8 |
| trim-30u (60 s trim) | 808.1 | 285.9 | 29.5 | 175.0 | 72.3 |
| purge0-30u (purge delay 0) | 803.1 | 251.0 | 52.4 | 174.5 | 76.6 |
| je-30u (jemalloc) | 761.7 | 251.3 | 52.4 | 172.4 | 68.4 |
| jefix-30u (interrupted, jemalloc) | 931.1 | 253.6 | 54.3 | 177.0 | 79.8 |

At a WIP sample the largest mimalloc mapping is 507 MiB while counted live Rust
allocations are 363 MiB process-wide. This supports significant allocator
retention/overhead but is not an exact arena accounting: requested sizes exclude
allocator rounding, and mapping boundaries do not identify every allocation.
Trim is off by default. The trim thread calls `mi_collect(true)` from its own
thread and `malloc_trim(0)`; it cannot promise to collect every live worker heap.
No allocator setting is recommended for production on these unmatched runs alone.
Jemalloc mallctl stats return zero in these binaries (stats not compiled in);
zero is unavailable data, not an empty heap. The independent live counter works.

## Sampled owners before the delivery fix

MiB sampled live allocations, exclusive first-matching stack buckets. Delivery
frames take precedence over outer feed-apply frames. Earlier `owners.txt` grouped
delivery under feed; the table below corrects that classification. Sampling
estimates need not match exact counted requested bytes.

| Owner | Minute 5 | Minute 30 |
| --- | ---: | ---: |
| Module artifact / module host | 69.2 | 70.8 |
| Parent ACL indexes | 61.6 | 61.6 |
| Snapshot entry rows | 79.0 | 60.1 |
| Snapshot views / ACL decode | 2.5 | 1.5 |
| Visibility / principal resolution | 22.5 | 18.5 |
| Delivery memo / prewarm | 5.9 | 19.4 |
| Change feed / event shares (excluding delivery) | 7.0 | 13.5 |
| Replica subscription state | 8.2 | 9.3 |
| Websocket connection | 3.3 | 4.3 |
| Store / SQLite Rust glue | 8.2 | 8.2 |
| Other Gonvex | 18.7 | 18.7 |
| Unresolved stacks | 2.0 | 3.5 |
| Total | 288.1 | 289.3 |

The snapshot-cache bucket falls after expiry, while the memo grows. Event shares
live count stays approximately 16–20, feed backlog bytes/entries return to zero,
and publications equal releases. Closed windows, replica subscription state and
visibility resolution show no monotonic growth in this interval. Small profiler
and presence allocations can fall under other buckets; they are not separately
proved absent. Principal cache is separately reported by existing metrics.

## Interrupted fix run: sampled comparison

Same classification rules and 512 KiB sampling interval as above. MiB sampled
live allocations at approximately minute 30. These are unmatched, interrupted
exploratory runs, not a controlled A/B comparison. In particular the visibility
and subscription buckets differ substantially between runs. Do not attribute
all cross-run differences to the memo change.

| Allocation-site bucket | Before memo fix | Fixed run |
| --- | ---: | ---: |
| Module artifact / module host | 70.8 | 66.7 |
| Parent ACL indexes | 61.6 | 67.1 |
| Snapshot entry rows | 60.1 | 68.8 |
| Snapshot views / ACL decode | 1.5 | 1.0 |
| Visibility / principal resolution | 18.5 | 40.2 |
| Delivery memo / prewarm | 19.4 | 1.2 |
| Change feed / event shares (excluding delivery) | 13.5 | 11.0 |
| Replica subscription state | 9.3 | 14.4 |
| Websocket connection | 4.3 | 3.3 |
| Store / SQLite Rust glue | 8.2 | 6.0 |
| Other Gonvex | 18.7 | 12.9 |
| Unresolved stacks | 3.5 | 2.5 |
| Total | 289.3 | 295.0 |

The fixed `delivery::prewarm` bucket across minutes 5/10/15/20/25/30 is
30.5 / 1.7 / 1.2 / 1.2 / 1.7 / 1.2 MiB. Thus startup is large; the remaining
five observations stay small rather than accumulating with elapsed time.
The old sequence is 5.9 / 5.9 / 8.4 / 9.9 / 10.4 / 19.4 MiB. A prewarm
allocation can remain referenced outside the memo (for example by pending
outbound data); its stack does not prove that the memo currently owns it.
The direct `memory.deliveryMemo.estimatedBytes` counter measures the memo's
estimate, while the sampled bucket measures all still-live allocations made
under those frames. Neither is an exact heap traversal.

Symbolization used a concrete compile-only grant from open-cost, pinned CPU 6
with CPUQuota 100%, and ended at 03:43:59 UTC before the 03:45 deadline. Runtime
and load generator were already stopped. The resource receipt reports exit 0,
23 seconds wall time and approximately 170 MiB peak memory. No measurement
ran concurrently under that grant.

Artifacts: `/mnt/data/work/gonvex-heap-evidence/{before-owners.txt,after-owners.txt}`,
`classify.py`, six collapsed fix profiles, late `/proc` memory maps, aggregate
metrics and `heap-comparison.html`. Original raw profiles and samples remain
under `lab-hg`. To reproduce classification, run
`jeprof --collapsed --show_bytes <matching-binary> <profile>` for each dump,
then `python3 classify.py <collapsed-files-in-time-order>`.

## Changes and contract

- Wrap each binary allocator in a sharded live requested-byte/allocation counter;
  report `memory.allocator` with allocator-specific state when available.
- Drop snapshot-cache entry payloads older than the existing `max_age` at install
  time. They already cannot be served after that age. Inactive empty slots are
  also pruned; sparse markers and in-progress builds can retain slot metadata.
- Expire delivery memo keys after 30 seconds on the next memo access
  (`GONVEX_DELIVERY_MEMO_SECS=0` disables age expiry). Count kept message trees,
  encoded and compressed text using `text.len() * 4 + 256` per variant toward
  the existing 8 MiB/tenant budget. This is an estimate, not a strict heap bound.
  The memo registry itself keeps tenant entries; tenant lifecycle cleanup is
  outside this patch. Expiry also waits for another access on quiet tenants.
- Add `memory.deliveryMemo` tenant/key/estimated-byte/expired totals and
  `memory.parentAcl` index/row totals. Existing snapshot-cache, principal-cache,
  event-share, feed-backlog and websocket metrics remain available.

No client/protocol or visibility contract changes. An expired memo item is
encoded again; caching frequency and allocator-counter overhead can affect CPU.
The shared payload remains content-checked before reuse. Timing impact is not
established by these memory runs.

## Capacity implications

Do not multiply total 30-user RSS by 1,000/30: module/cache/ACL owners are shared,
whereas visibility/subscription state depends on user permissions and paths.
The sampled replica + websocket buckets alone are about 13.6 MiB/30 users
(~0.45 MiB/user), or ~453 MiB/1,000 users if linear. This omits per-user visibility,
queues and native overhead; it is a component estimate, not a total forecast.

26 active tenant memos at their 8 MiB estimated cap represent 208 MiB estimated
retention. Actual bytes can differ; 30-second expiry usually lowers occupancy.
Parent ACL index memory depends on tenant row counts and projections, not only
number of users. The lab cannot supply a trustworthy total projection for 26
production tenants without their row distribution and realistic 1k measurements.
The 1k baseline remains the source of capacity evidence; this branch is not merged
into its unchanged runtime during the gate series.

## Verification status (2026-11-05 deferred grant)

**Full validation is incomplete. No integration merge is authorized.** Runtime
source remains `ba54cee4` on the older `636e3c45` base; no newer integration
changes were merged and no runtime/compiler/allocator changes were made in this
validation window. No new load or soak ran. The lock was claimed at 06:15:53 UTC
and explicitly extended through 06:45:53 UTC; all jobs are bounded before that
cutoff. The test target is private `/mnt/data/work/gonvex-heap-validation-target`.
The copied cache took 7m05s; compile caps remained unchanged. Every heavy phase
passed `build-headroom` and ran through `coach-resources`.

The actual command was:

```sh
cargo test --offline --locked -p gonvex-runtime --features perf-fixture --lib --tests -- --test-threads=1
```

Output went directly to a log and cargo's exit was captured in a separate file,
without a filter/tee pipeline. Attempts were retained separately:

1. The original hard timeout fired at 06:35:07 during compilation. Process-tree
   exit **124**, cargo exit unavailable, no tests executed. This is incomplete,
   not a pass or a baseline failure.
2. The resumed full command compiled and returned actual cargo exit **101**:
   **390 passed, 1 failed, 7 ignored** in the library. The failure was
   `action_calls::fetch_tests::a_body_timeout_is_reported_as_a_timeout_not_a_decode_failure`:
   its 200 ms request timed out at `.send().await.unwrap()`, before the body
   assertion. Cargo stopped before integration tests.
3. The full command was repeated with `--no-fail-fast` so that the earlier
   failure could not prevent the integration oracles from running. Results
   below include only completed suites; the first failure is not erased.

| Completed suite | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| runtime library | 391 | 0 | 7 |
| runtime binary | 1 | 0 | 0 |
| auth_contract | 4 | 0 | 0 |
| burst_control | 1 | 0 | 0 |
| call_watermark_latency | 1 | 0 | 0 |
| class_sharing_oracle | 2 | 0 | 0 |
| conn_idle | 3 | 0 | 0 |
| delivery_oracle | 1 | 0 | 0 |
| feed_backlog | 1 | 0 | 0 |
| membership_projection | 1 | 0 | 0 |
| offload_equivalence | 1 | 0 | 0 |
| operator_contract | 2 | 0 | 0 |
| perf_fixture_smoke | 1 | 0 | 0 |
| presence_v2 | 9 | 0 | 0 |
| principal_cache_oracle | 1 | 0 | 0 |

Attempt 3 process-tree exit: **124**; actual cargo exit:
**unavailable (interrupted)**.
Last incomplete suite: **raw_rows_oracle**. Integration suites without a
completed result: `raw_rows_oracle`, `redeploy_cache`, `replica_state`, `resume_log_only`, `slow_reader_delivery`, `snapshot_cache_oracle`, `targeted_wake`, `visibility_fanout`.
A partial result is not a full-suite pass. The seven ignored library tests are
three bridge benchmarks and four tests requiring parent-owned native-exit,
authenticated SDK/browser/storage or process-crash coordinators; no ignore was
added by this patch.

The HTTP-timeout failure was checked independently using an immutable
`git archive 636e3c45` and a separate private reflinked target. The exact test
passed with actual cargo exit **0** (394 filtered tests). `action_calls.rs` is
byte-identical between the base and heap branch. Therefore the original failure
was **not reproduced as a baseline failure**; its cause remains unproven.
The repeat library run passing does not justify silently waiving the failed
attempt. Baseline source was not edited, and the baseline was not merged.

The docs generator command `pnpm --filter @gonvex/docs typecheck` passed with
actual exit **0** (`fumadocs-mdx`, Next route types, TypeScript). It produced no
tracked generated-file changes. No Gonvex function/schema, template API or i18n
source changed; CI/package-script inspection found no other applicable standalone
generator for this Rust/report diff. A conservative `crate::` module-reference
SCC comparison against `636e3c45` found unchanged cycles; the only added library
edge is `operations -> heap_stats`, and `heap_stats` has no outgoing crate edges.
The binary also calls that leaf module. No JavaScript runtime files changed.
This graph comparison includes test code and is not full Rust name resolution;
the leaf's one-way imports also establish that it adds no runtime cycle.
The new module's format check and branch whitespace check passed.

Evidence is under `/mnt/data/work/gonvex-heap-evidence/validation/`:
`runtime-full.log`, each attempt's separate status/log, `baseline-timeout-test.log`,
`baseline-proof.json`, `docs-typecheck.log`, `cycle-comparison.json` and
`validation-summary.json`. Historical targeted output before `ba54cee4` reported
38 passed; the new full-run evidence above supersedes any earlier pending or
wrapper-only validation claim. **Total RSS improvement remains unproven.**
