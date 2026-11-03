# Apple M4 realistic Whagons load results

Measured on 2026-11-03 UTC. Apple M4 ARM64, 10 CPU cores, 24 GB RAM.
Docker Desktop remained at 10 CPUs / 14 GiB. These absolute numbers do not
represent the 12-core x86 production server.

All traffic stayed on literal loopback inside `gvmac-load_isolated`, an internal
Docker network with no published ports. No production/customer data or live
service was used. PostgreSQL 16 had 100 connection slots, fsync and
synchronous_commit enabled, with a checkpoint before every run. Caps were
PG/runtime 4 CPUs and 4 GiB each, generator 2 CPUs and 2 GiB, auth 1 CPU and
768 MiB, Valkey 0.5 CPU and 256 MiB.

The published SDK was 0.5.2-staging.31 with the app's exact patch. Real SDK
sessions subscribed to task and nine metadata collections. Independent 30-second
heartbeats and bursty foreground operations used the observed-seven weighted
profile. All 26 synthetic tenants ran scheduled jobs, but active users were
weighted across seven tenants. This is not an all-26-active-user experiment.
Fixtures used 9,000/5,000/2,000 tasks plus 100–280 in other tenants and 256 prepared
ordinary member identities. See the README for workload approximations.

## Completed capacity points

Every row completed a 180-second full-user plateau. All except the one-user
baseline used a simultaneous login burst. PASS requires zero client/runtime
errors and PG rejections, all requested users present throughout, and save and
task-delivery p95 both below 1,000 ms. Zero memory-pressure stops and OOM kills
were recorded for these completed cases. All client error and connection
rejection counts were zero. Runtime errors include scheduled work.

Pre-279 uses shared admission 20, open/idle 2/2, idle expiry 1,000 ms.
Defaults use 20, 2/2, 60,000 ms. Shared4 changes admission to 4 only.
Open1/idle1 and open2/idle1 use admission 20 and 60,000 ms expiry.
Application SQL churn excludes the persistent sampler and health probes.
Admission mean includes uncontended attempts; max is since runtime startup.

| Case | Result | Save p95 ms | Task delivery p95 ms | Runtime errors | PG peak sessions | App sessions/s | Admission mean/max ms |
|---|---|---:|---:|---:|---:|---:|---:|
| pre279-n1 | PASS | 396.2 | 389 | 0 | 57 | 7.101 | 6.16 / 412.1 |
| pre279-n2 | PASS | 386.4 | 379 | 0 | 58 | 5.947 | 2.73 / 371.8 |
| pre279-n5-valid | FAIL | 445.6 | 441 | 16 | 60 | 6.913 | 71.95 / 10034.3 |
| pre279-n10 | FAIL | 454.5 | 750 | 19 | 62 | 6.909 | 101.27 / 10032.5 |
| defaults279-n10 | PASS | 367.0 | 480 | 0 | 62 | 0.000 | 1.46 / 455.0 |
| defaults279-n12 | PASS | 696.6 | 908 | 0 | 62 | 0.000 | 2.02 / 365.8 |
| defaults279-n16 | FAIL | 691.3 | 890 | 16 | 62 | 0.000 | 57.46 / 10108.0 |
| defaults279-n20 | FAIL | 4654.0 | 4774 | 36 | 62 | 0.000 | 153.07 / 10243.5 |
| defaults279-n26 | FAIL | 604.9 | 1804 | 0 | 63 | 0.000 | 7.47 / 1006.7 |
| shared4-n10-resumed | PASS | 464.3 | 636 | 0 | 62 | 0.000 | 3.69 / 471.3 |
| shared4-n16 | FAIL | 915.2 | 1087 | 0 | 62 | 0.000 | 2.82 / 423.4 |
| shared4-n16-repeat | FAIL | 704.6 | 1022 | 0 | 62 | 0.000 | 2.67 / 497.2 |
| shared4-n20 | PASS | 668.1 | 921 | 0 | 62 | 0.000 | 2.40 / 639.2 |
| shared4-n20-repeat | FAIL | 730.9 | 1175 | 0 | 62 | 0.000 | 2.36 / 681.4 |
| shared4-n26-resumed | FAIL | 935.0 | 1632 | 0 | 63 | 0.000 | 7.14 / 1448.0 |
| open1-idle1-n20 | FAIL | 820.7 | 2150 | 39 | 35 | 0.000 | 182.01 / 10343.1 |
| open2-idle1-n20 | FAIL | 509.7 | 1041 | 15 | 39 | 51.896 | 47.52 / 10048.1 |

## CPU and memory

CPU is mean user/system cores during the plateau, not percent. Memory is peak
cgroup memory.current including cache, not RSS. Full aggregate series also retain
generator CPU, health/session churn, account counts and PSI summaries.

| Case | PG user/system cores | Runtime user/system cores | PG peak GiB | Runtime peak GiB | Generator peak GiB |
|---|---:|---:|---:|---:|---:|
| pre279-n1 | 0.039 / 0.041 | 0.031 / 0.014 | 2.28 | 1.81 | 0.23 |
| pre279-n2 | 0.035 / 0.035 | 0.033 / 0.013 | 2.27 | 1.83 | 0.24 |
| pre279-n5-valid | 0.043 / 0.045 | 0.041 / 0.018 | 2.30 | 1.99 | 0.27 |
| pre279-n10 | 0.042 / 0.044 | 0.049 / 0.019 | 2.41 | 2.38 | 0.30 |
| defaults279-n10 | 0.033 / 0.018 | 0.041 / 0.016 | 2.60 | 2.29 | 0.30 |
| defaults279-n12 | 0.059 / 0.030 | 0.093 / 0.031 | 2.46 | 2.62 | 0.32 |
| defaults279-n16 | 0.064 / 0.030 | 0.137 / 0.036 | 2.54 | 2.81 | 0.39 |
| defaults279-n20 | 0.073 / 0.033 | 0.172 / 0.040 | 2.55 | 3.07 | 0.39 |
| defaults279-n26 | 0.067 / 0.029 | 0.205 / 0.045 | 2.73 | 3.39 | 0.47 |
| shared4-n10-resumed | 0.043 / 0.026 | 0.072 / 0.026 | 2.55 | 2.33 | 0.32 |
| shared4-n16 | 0.062 / 0.031 | 0.131 / 0.036 | 2.47 | 2.82 | 0.36 |
| shared4-n16-repeat | 0.061 / 0.031 | 0.119 / 0.035 | 2.43 | 2.83 | 0.38 |
| shared4-n20 | 0.067 / 0.033 | 0.149 / 0.041 | 2.47 | 3.08 | 0.41 |
| shared4-n20-repeat | 0.075 / 0.036 | 0.175 / 0.049 | 2.46 | 3.06 | 0.43 |
| shared4-n26-resumed | 0.082 / 0.035 | 0.257 / 0.051 | 2.61 | 3.28 | 0.47 |
| open1-idle1-n20 | 0.057 / 0.030 | 0.151 / 0.040 | 2.40 | 3.06 | 0.40 |
| open2-idle1-n20 | 0.112 / 0.143 | 0.138 / 0.039 | 2.17 | 3.10 | 0.42 |

## Interpretation and exclusions

Before #279, highest passing tested concurrency was 2 and first failing was 5.
Three and four users were not tested. The historical two-of-ten sign-in failure
did not reproduce on this prepared fixture. Baseline failures instead included
`agents.dispatch` and `deepResearch.tick` waiting about 10 seconds for module
isolates. Foreground latency alone would incorrectly qualify those cases.

With #279 defaults, highest passing was 12 and first failing was 16. Defaults at
16 met both latency thresholds but logged 16 runtime errors; at 20 both latency
thresholds failed and there were 36 runtime errors. At 26 delivery p95 failed
without runtime errors. A higher user count does not force the same failure
mode in this stochastic workload.

Shared admission 4 passed at 20 in its first run but failed at 16 in its first
run. This is non-monotonic evidence, not a clean 20-to-26 capacity bracket.
Random think times, small foreground sample counts and scheduled-job phase vary.
The 16-user repeat also failed delivery at 1,022 ms; the 20-user repeat failed at 1,175 ms. Shared4's highest observed passing point is 20, lowest failing point is 16, and the next higher failing point is 26. Repeat results are retained separately. Never erase a failed point because a
later run passes. The highest observed passing point is not a guaranteed limit.

The first bottleneck is admission and scheduled/foreground execution contention,
not exhausted PostgreSQL physical sessions or average CPU saturation. Captured
20-user diagnostics show scheduled isolate-deadline failures and slow SQL
acquisition around 10 seconds. SQL admission is only one queue; changing it does
not reserve module-host capacity for foreground work. Zero errors still does not
ensure task-delivery p95 passes, as the shared4 16/26 runs demonstrate.

Keep open/idle matched at 2/2 and expiry at 60 seconds. Open=2/idle=1 generated
51.9 application connections/s; matched settings generated zero after #279.
Open=1/idle=1 avoided churn but failed at 20 users. Keep shared=20 as the default recommendation. Shared=4 reduced scheduled errors
in these cases, but its 20-user pass failed to repeat and both 16-user cases
failed delivery. It is an experimental contention setting, not a qualified
20-user recommendation. The best recommended settings remain #279 defaults,
with 12 highest passing and 16 first failing tested users.
Do not treat these M4 observations as authorization to change production.
A further runtime change should coordinate bounded scheduler/foreground admission
at both SQL and module-host queues, with fairness and cancellation tests.

`shared4-n10` was interrupted for restart and remains PARTIAL, excluded from
capacity. `pre279-n5` was invalidated by build overlap; only `pre279-n5-valid`
is included. No builds overlapped any included plateau. Resource teardown exit
137 events without an OOM event are not OOM measurements.

Provenance is in `mac-m4-provenance.json`. The corrected candidate image ID is
`sha256:1b72a867fc3196bb8989359c2f62578afd24110bec815c85283070b2bd489b54`.
The exact archive manifests were retained outside Git and their hashes are in
provenance. One candidate integration-test file differed from the source commit;
all runtime build inputs matched. PR #279 is merged and is not duplicated here.
The PR base also contains later visibility changes absent from the pinned image;
these measurements describe the pinned image, not a rebuild of current staging.
The AArch64 x86-only SYS_fork/SYS_vfork cfg guard was required to build both
baseline and candidate.

Raw telemetry and synthetic fixture state remain on the Mac under
`~/loadtest/gvmac-state`, with a final archive backed up beside the resume note.
Only aggregate summaries/provenance are committed. The original workstation
experiment and incident directory were not modified or used.

## Validation and real API evidence

After the final plateau and isolated Compose teardown, the ARM64 Docker test
build passed `gonvex-postgres --lib admission_metrics_include_success_and_timeout`
and `gonvex-runtime --lib idle_timeout`, one and two tests respectively. The
minimal test context initially lacked a shared protocol fixture; adding the
fixture fixed that build setup. `Dockerfile.tests` includes the required fixture.

```sh
docker build -f bench/whagons/Dockerfile.tests -t gvmac-tests:verify .
node --test bench/whagons/safety.test.mjs
python3 bench/whagons/summary.test.py
cargo fmt --all --manifest-path rust/Cargo.toml -- --check
```

The three Node guard tests and three Python qualification tests passed.
Python compilation, shell syntax and git diff whitespace checks passed.
The exact docs typecheck pipeline, fumadocs-mdx, next typegen and tsc --noEmit,
passed through a capped heavy-workloads unit after build-headroom passed.
No tracked generator output changed. Relative JS/TS runtime-import cycle
comparison against staging found zero cycles before and after; the only added
runtime edge is the benchmark importing its leaf safety module.
The complete changed-file classifier reports non-UI.

Real SDK/API excerpt from `shared4-n20`, retained here without identifiers:

```text
LOAD_LABEL=shared4-n20 LOAD_USERS=20 LOAD_SHARED=4 LOAD_MAX_OPEN=2
LOAD_MAX_IDLE=2 LOAD_IDLE_MS=60000 LOAD_RAMP_SECONDS=0 run-mac-case.sh
connectedUsers=20 targetUsers=20 steadySeconds=180 errors={}
clientSloPassed=true save.samples=50 save.p95=668.1004
 taskDelivery.samples=62 taskDelivery.p95=921
/dev/metrics plateau: min authenticated accounts=20, samples=17
PostgreSQL peak sessions=62, application churn=0/s, runtime errors=0
```

The same settings failed the repeat at delivery p95=1,175 ms. Both outputs are
included in the aggregates. This excerpt proves exercised SDK saves and task
publications, not a repeatable 20-user guarantee.

Full raw state was archived before `compose down --volumes`. The event reader
was stopped afterward. A final Docker inventory contained only the three
unrelated containers, all healthy, and no gvmac network or volume.
