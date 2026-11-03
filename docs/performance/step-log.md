# perf/integrate-5k step log

One section per measured run on the 5k-program lab (`gvperf-*` containers, compose
project `gvperf-5k`, lab root `/home/gabriel/work/gonvex-perf/lab-5k`). Raw artifacts for
each case `<label>` live in the lab root: `<label>.json` (+ `.shard-*`, `.mac-host.jsonl`,
`.rtt.log`), `<label>/host.jsonl` (runtime cgroup and processes, proxy and tunnel
cgroups), `<label>/dev-metrics.jsonl`, `<label>/profile/*.json` (`/dev/profile` every
10 s), `<label>/profile-delta.txt` (`top.py --delta` over the plateau),
`<label>/oncpu.svg` and `<label>/offcpu.svg` (30 s each, in the plateau).

## Setup (all runs)

- **Runtime** on arch3 in a 1 vCPU / 2 GiB container (`cpus: 1`, `mem_limit: 2g`, no
  swap), host-built `local-load` profile (release + debug assertions, which the
  emulator-auth feature requires; same profile as `bench/whagons/build.sh`),
  `GONVEX_SQLITE_SYNCHRONOUS=full`, SQLite files on a Docker volume on arch3's Btrfs
  NVMe root. Module host as a separate process in the same cgroup (base).
- **Generator** on the MacBook (`ssh mac`, 10 cores, 24 GB), Node 26.13.0,
  `/Users/whagons/loadtest/gvperf-5k`, the same Whagons app/SDK as the lab
  (`@gonvex/client 0.5.2-staging.31` + patch `b75f898a`, manifest sha256 `5aed45d4…`).
  Thin clients (`LOAD_CLIENT=thin`, legacy JSON rows, the SDK's own negotiation) plus
  `LOAD_SDK_USERS=8` real-SDK users; visibility oracle sessions use the real SDK; open
  loop (latency from planned start).
- **Transport:** `remote-proxy.py` (container `gvperf-remote-proxy` in the lab network
  namespace) exposes the runtime and auth emulator as Unix sockets; an SSH reverse
  tunnel (`systemd-run --user` unit, so its cgroup is measured) makes them
  `127.0.0.1:18080/18099` on the Mac. No published ports. Their CPU/RSS is reported per
  run. RTT Mac→arch3 is Tailscale (`ping` p50 ≈ 5 ms, p95 ≈ 70–100 ms: the link has a
  heavy tail that adds to every client-side percentile).
- **Fixture:** `bench/whagons/lab-setup.sh 500` (26 tenants, production task counts,
  staffed: 2 admins / 3 approvers / members with teams and spots per tenant, 500 load
  users), snapshot in `baseline-data/`; every case starts from an exact copy of it.
- **Pass rule:** steady plateau p95 < 500 ms and p99 < 1 s per kind, < 1 % errors,
  all users connected, no oracle mismatch, no OOM.
- **Rerun one case:**
  ```sh
  source /home/gabriel/work/gonvex-perf/lab-5k/env.sh
  cd /home/gabriel/work/gonvex-perf/integrate/bench/whagons
  # full workload
  LOAD_LABEL=<new-label> LOAD_CLIENT=thin LOAD_SDK_USERS=8 LOAD_USERS=200 LOAD_SHARDS=2 \
    LOAD_SECONDS=180 LOAD_RAMP_SECONDS=60 ./run-remote-case.sh
  # single path
  LOAD_PREPARED_USERS=500 LOAD_LABEL=<new-label> LOAD_SCRIPT=paths.ts LOAD_SCENARIO=heartbeat \
    LOAD_RATE=35 LOAD_USERS=200 LOAD_SECONDS=120 LOAD_RAMP_SECONDS=30 LOAD_PERF_AT=70 ./run-remote-case.sh
  python3 step-report.py $LOAD_LAB_ROOT <label> --profile-out $LOAD_LAB_ROOT/<label>/profile-delta.txt
  ```
  Wrap in `coach-resources run --unit gvperf-<label> --build-dir $LOAD_LAB_ROOT --kind fixture-perf -- bash -c '…'`
  as the lab drivers do (`lab-5k/series.sh`, `plan2.sh`).

Thin-client conformance on this base (`node thin-conformance.ts` in the lab,
`LOAD_PREPARED_USERS=500`): 13/13 steps pass with legacy rows and 13/13 with
`LOAD_NATIVE_ROWS=true`; `unsupported` empty in both.

---

## Step 0: base + profiler

Commit `4932845595` / `b07805b1` (bench-only changes after the binary's commit
`ad764cc4` = lab 72e6d30 + perf/fixture + perf/profiler a7bfff3e). Runtime binary
sha256 `e7e1901eb5319c41e2434bfee3112ea478a75f032834040b2fd252478c38d14e`.

Note: `base-path-heartbeat-25/50` and `base-path-edit-2` ran while low-priority
builds were running on arch3 (12:25–12:37); they are superseded by the `base2-*`
reruns below and are not used for conclusions. Runtime restarts between cases were
`docker stop` with a 20 s grace (the base runtime ignored SIGTERM, so these were hard
kills); every case restores `baseline-data` first, so no state carries over.

### base-thin-200: full workload, 200 users — FAIL (memory, then CPU)

thin + 8 SDK users, 2 Mac shards, 60 s ramp, 180 s plateau.

| kind | samples | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|
| reducer | 3159 | 1,283 | 18,423 | 46,628 |
| heartbeat | 2781 | 1,119 | 16,415 | 45,013 |
| delivery | 47611 | 1,090 | 13,242 | 24,178 |
| query | 86 | 966 | 21,345 | 29,538 |
| taskDelivery | 3181 | 2,721 | 21,754 | 41,824 |
| save | 365 | 3,970 | 28,234 | 58,230 |
| permission | 13 | 2,156 | 44,512 | 44,512 |
| visibility | 8 | 13,060 | 61,508 | 61,508 |

- users 200/200, error rate 1.25 %, oracle: 9 checks, 8 converged, **1 mismatch
  (0 leaked, 793 missing after 15 s)**, i.e. a member's replica had not caught up
  within 15 s under this backlog (delivery p99 24 s), not proven a visibility bug.
  Thin `unsupported`: 8 × `replica.error: authenticate with an active tenant before
  opening a Replica Collection` (opens racing a membership-changed re-auth; the SDK
  users logged the matching reducer error).
- runtime cgroup: CPU 0.80 cores (throttled 1,072/1,781 periods, 54 s of 178 s);
  memory at the 2 GiB limit (plateau peak 2,048 MiB, anon 1,530 MiB),
  `memory.events max` 1,583, `sock_throttled` 17; runtime RSS 1,429 MiB, module host
  395 MiB.
- transport: proxy 0.06 cores / 29 MiB, ssh tunnel 0.13 cores / 18 MiB. Generator:
  Mac 87 CPU s, peak RSS 800 MiB (2 shards). RTT p50 5.3 / p95 75 / max 125 ms.
- profile (plateau): heartbeat `fn.total` p50 630 ms / p99 7.9 s, of which
  `invoke.js` p50 523 ms / p99 7.75 s (the module host process waiting for CPU and
  isolates; `budget_exceeded … waiting for a module isolate` errors at 200 users);
  `commit.fsync` p50 28 ms / p99 433 ms; `db.writer_wait` p99 369 ms;
  `delivery.queue_wait` p50 18 ms / p99 38.7 s; `ws.inbound_wait` p99 13.6 s;
  `tokio.sched_lag` p50 1.4 ms / p99 113 ms / max 2.3 s.
- **Binding constraint:** memory first (cgroup pinned at 2 GiB, reclaim and socket
  throttling), then CPU (one vCPU shared by the runtime and the V8 module host, 60 %
  of periods throttled). Generator and transport are not limiting.

### base-cron-200: 200 users, 20 min plateau across 13:00 — runtime OOM-killed

Started 12:43 to cross the hour. The runtime was **OOM-killed at 12:47** (2 GiB, at the
end of the ramp; `OOMKilled=true`, exit 137); the generator then retried for 30 min
and was stopped. Partial results only (`base-cron-200.json.shard-*.partial.json`):
heartbeat p50 10 s / p99 40 s, `budget_exceeded … waiting for a module isolate` 637×,
`control.support.heartbeat timed out` 148×. Same workload as base-thin-200, which
survived at the limit; at 200 users the base is on the OOM edge. The :00 window was
re-measured at 150 users (below).

### Single paths (paths.ts, 200 thin users with the standard collections)

Offered Poisson rate, 30 s ramp, 120 s plateau, latency from planned start.

| case | offered/s | p50 ms | p95 ms | p99 ms | runtime CPU | throttled periods | result |
|---|---:|---:|---:|---:|---:|---:|---|
| base2-path-heartbeat-25 | 25 | 31 | 130 | 215 | 0.39 | 39/1183 | PASS |
| base2-path-heartbeat-35 | 35 | 52 | 169 | 333 | 0.49 | 56/1181 | PASS |
| base2-path-heartbeat-50 | 50 | 631 | 4,917 | 7,917 | 0.90 | 680/1190 | FAIL |
| base-path-edit-2 (150 users, largest tenant; ran during a build) | 2 | save 78 / taskDelivery 142 | 290 / 331 | 549 / 551 | 0.27 | 36/1153 | PASS |
| base-path-edit-5 (150 users, largest tenant) | 5 | save 174 / taskDelivery 242 | 726 / 777 | 1,084 / 985 | 0.62 | 120/1178 | FAIL |
| base2-path-snapshot-2 (5-collection detail window) | 2 | 43 | 124 | 129 | 0.10 | 10/1126 | PASS |

Knees on base: **heartbeat between 35 and 50/s** (5k users need ≈ 370 heartbeat-class
calls/s), **edits into the largest tenant between 2 and 5/s** at ~80 receivers per
edit (5k needs ≈ 23/s into ~1,950 receivers), snapshot opens ≥ 2/s (higher rates
pending). Sign-in bursts are slow on every scenario: `initialLoad` p50 0.3–20 s for
200 users signing in over 30 s.

Runtime memory with 200 idle-ish users connected: 1.6–1.9 GiB cgroup (anon
1.3 GiB; runtime RSS 1.2–1.4 GiB, module host 310–380 MiB).

### Where a slow heartbeat or save spends its time (base + profiler)

From `profile-delta.txt` (plateau window), p50 / p99:

| stage | heartbeat @35/s | heartbeat @50/s | save @5/s (edit) |
|---|---|---|---|
| fn.total | 30 ms / 231 ms | 476 ms / 2.60 s | 102 ms / 2.21 s |
| fn.prepare | 0.4 / 1.0 ms | 0.7 / 1.6 ms | 0.6 / 1.2 ms |
| invoke.connect | 0.07 / 12.6 ms | 2.4 / 143 ms | 0.16 / 208 ms |
| invoke.first_frame | 0.12 / 1.9 ms | 0.01 / 30 ms | 0.16 / 4.2 ms |
| invoke.js | 0.5 / 68 ms | **377 ms / 2.34 s** | 11.6 ms / **2.20 s** |
| invoke.host_calls | 0.3 / 17 ms | 0.9 / 108 ms | 6.7 / 210 ms |
| db.writer_wait | 0.06 / 8.4 ms | 0.27 / 98 ms | 0.23 / 161 ms |
| db.sql | 0.12 / 1.5 ms | 0.19 / 17 ms | 0.09 / 3.7 ms (22 calls) |
| fn.finish | 25 / 163 ms | 46 / 556 ms | 25 / 470 ms |
| commit.finalize | 0.4 / 5.9 ms | 1.0 / 69 ms | 0.8 / 24 ms |
| commit.commit | 0.14 / 36 ms | 0.26 / 37 ms | 1.1 / 94 ms |
| **commit.fsync** | **24 / 159 ms** | 37 / 552 ms | 22 / 470 ms |
| commit.publish | 9 / 46 µs | 16 / 55 µs | 22 / 53 µs |

Delivery and scheduler, p50 / p99 (max):

| stage | heartbeat @35/s | heartbeat @50/s | edit @5/s | thin-200 |
|---|---|---|---|---|
| feed.notify_lag | 0.26 / 125 ms | 13 / 882 ms | 1.7 / 252 ms | 1.9 / 393 ms |
| delivery.queue_wait | 7 / 66 ms (472) | 35 ms / 1.8 s (7.5 s) | 68 / 366 ms | 18 ms / 38.7 s |
| delivery.visibility | 0.07 / 0.24 ms | 0.10 / 0.57 ms | 0.42 / 1.0 ms | |
| ws.inbound_wait | 0.006 / 5.8 ms | 0.009 ms / 3.85 s | 0.01 / 50 ms | 0.17 ms / 13.6 s |
| tokio.sched_lag | 1.2 / 17 ms (471) | 3.4 / 111 ms (561) | 1.2 / 149 ms | 1.4 / 113 ms (2.3 s) |
| worker busy (1 s) | 42 % | 91 % | | |

Long polls (> 5 ms) in those windows are almost all `ws_connection` (a connection
task applying a feed event / encoding), plus watchdog `probe_stalled` entries of
25–34 ms; stalls counted: 180 at 35/s, 801 at 50/s.

**Answer.** Below the knee a heartbeat or save is **fsync-bound**: `commit.fsync`
(`synchronous=full`, Btrfs `fdatasync` ≈ 25 ms p50) is ~80 % of the mean and the
largest p99 contributor (p99/total p99 ≈ 70 % at 35/s). The tokio worker is not the
limiter there (`sched_lag` p99 17 ms, inbound wait p99 6 ms). At the knee the tail
moves to **`invoke.js`**: the V8 module host is a second process in the same 1-vCPU
cgroup, and once the runtime + module host need more than one core (0.9 cores, 57 %
of periods throttled at 50 heartbeats/s, ~10 ms CPU per heartbeat call across both
processes) JS waits for CPU and for an isolate (p99 2.3 s), and the single tokio worker
falls behind with it (`sched_lag` p99 111 ms, `ws.inbound_wait` p99 3.9 s,
`delivery.queue_wait` p99 1.8 s). So the "single tokio worker stalls" hypothesis is
**not the first-order cause**: stalls are a consequence of CPU saturation (and, at 200
users in the full workload, of memory pressure), not the trigger. Off-CPU
(`base2-path-heartbeat-50/offcpu.svg`): SQLite worker threads dominate blocked time
(waiting for work); threads named `tokio-rt-worker` (the worker and the blocking pool
share this name) spend 12 s of 30 s in Btrfs `fdatasync` (`btrfs_sync_log`).
