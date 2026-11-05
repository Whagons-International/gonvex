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

### Cron :00 window at 150 users (base-cron-150, base + profiler)

200 users OOM-killed the base runtime before :00 (`base-cron-200` above), so the :00
window was measured at **150 users** (thin + 8 SDK, 2 Mac shards, 60 s ramp, plateau
13:49:52–14:06:52 crossing 14:00:00, on-CPU perf 13:59:45–14:00:15, off-CPU
14:00:15–14:00:45). Binary as Step 0.

Harness note: both cron runs hung in generator teardown after the plateau (a load job
awaiting a reply that never came); `base-cron-200` was killed with no raw samples. For
`base-cron-150` the raw samples were salvaged from the live shard processes through the
Node inspector (`<lab>/base-cron-150.salvage.shard-{0,1}`, same fields as a shard
result) before killing them; the merged summary is therefore partial. run.ts now bounds
teardown (`LOAD_TEARDOWN_MS`, default 90 s, stuck jobs counted as errors) and its SIGTERM
partial result carries raw samples.

User latency per op kind, minute before vs two minutes after 14:00
(`window-report.py <lab>/cron150 x <14:00 epoch> 60 120`):

| kind | before n | before p50 | before p95 | before p99 | after n | after p50 | after p95 | after p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| heartbeat | 682 | 167 | 4,708 | 5,849 | 1262 | 216 | 29,576 | 35,656 |
| reducer | 728 | 167 | 4,737 | 5,836 | 1354 | 220 | 30,166 | 36,445 |
| save | 43 | 215 | 4,739 | 7,077 | 88 | 250 | 32,974 | 44,671 |
| delivery | 9236 | 123 | 4,176 | 5,893 | 15937 | 157 | 22,035 | 28,200 |
| taskDelivery | 220 | 269 | 6,328 | 6,408 | 542 | 246 | 29,625 | 41,651 |
| query | 11 | 152 | 4,805 | 4,805 | 22 | 205 | 21,724 | 27,713 |
| permission | 3 | 169 | 180 | 180 | 4 | 1,173 | 12,343 | 12,343 |
| reconnect | 342 | 153 | 352 | 1,104 | 480 | 137 | 357 | 788 |

Per minute (heartbeat / save p95, ms): 13:54 1,942 / 2,980; **13:55 16,144 / 25,703**
(the */5 jobs); 13:56 737 / 1,236; 13:57 2,140 / 1,713; 13:58 1,370 / 1,929; 13:59
4,708 / 4,739; **14:00 31,804 / 39,407; 14:01 25,985 / 32,974; 14:02 52,023 / 58,677**;
14:03 651 / 2,746; 14:04 327 / 368 (users leaving). Base fails at 150 users even
outside the herd (p95 0.7–4.7 s); the herd adds three minutes of 25–60 s.

Runtime side (`/dev/profile` deltas; dumps nearest :58:00, :00:00, :02:00, :05:00 in
`base-cron-150/profile/`), p50 / p99 (max):

| stage | 13:58–14:00 | 14:00–14:02 | 14:02–14:05 |
|---|---|---|---|
| worker busy / globalQ peak | 62 % / 13 | 51 % / 107 | 33 % / 107 |
| fn.total heartbeat | 54 ms / 1.43 s | 123 ms / 18.7 s | 57 ms / 5.97 s |
| fn.total action | 0.8 ms / 328 ms | 1.8 ms / 32.5 s | 0.4 ms / 2.85 s |
| invoke.js heartbeat | 3.0 ms / 1.25 s | 25 ms / 17.5 s | 2.4 ms / 5.87 s |
| db.writer_wait heartbeat | 0.3 / 151 ms | 0.7 ms / 403 ms (18.3 s) | 0.2 / 101 ms |
| commit.fsync heartbeat | 33 ms / 1.07 s | 22 / 403 ms | 31 / 193 ms |
| delivery.queue_wait | 7 ms / 612 ms | 8 ms / 4.21 s | 7 ms / 15.3 s |
| ws.inbound_wait | 0.01 ms / 1.05 s | 0.01 ms / 20.1 s | 0.01 ms / 6.24 s |
| snapshot.permit_wait | 21 / 184 ms | 20 / 198 ms (29 s) | 8 ms / 10.0 s (38 s) |
| tokio.sched_lag | 1.3 / 65 ms (1.9 s) | 1.5 / 94 ms (**17.2 s**) | 1.2 / 55 ms (**37.5 s**) |

Runtime cgroup (2 GiB, 1 vCPU): anon 1,399 MiB / page cache 313 MiB at 14:00:00;
`memory.events max` 344 → 914 (14:00:01) → 1,966 (14:01) → 6,324 (14:02) → 24,116
(14:04); page cache evicted to 15 MiB at 14:01; **module host RSS 395 → 788 MiB** by
14:02 (the :00 Actions), runtime RSS 1.0–1.3 GiB. On-CPU 13:59:45–14:00:15: 26 % in
`native_queued_spin_lock_slowpath` under `try_to_free_pages → shrink_node →
evict_folios` (direct reclaim) plus `clear_highpages`/LRU frames, 63 % of samples on
`sqlx-sqlite-worker` threads (they fault and reclaim), 33 % on `tokio-rt-worker`.

Long polls: the worst single poll of the run was the **`outbox` task, 33.7 s, ending
14:02:40** (the action-outbox drain spawned after each reducer commit,
`execution.rs` `drain_action_outbox`), then `ws_connection` 706 ms (14:01:12) and `feed`
493 ms (14:02:04). With one tokio worker that one poll is the 17–37 s `sched_lag` max and
the 14:02 spike (every socket waits). Whether that poll was blocked in reclaim/page
faults or in synchronous work inside the Action path is not determined (perf did not
cover 14:02); it needs a perf capture at 14:01–14:03 on the next :00 run.

**Binding constraint at :00 (base): memory.** The :00 Actions double the module host's
RSS, the cgroup sits at `memory.max`, direct reclaim eats the vCPU, the module host
cannot answer (`invoke.js` p99 17.5 s, `budget_exceeded … waiting for a module isolate`),
and one 33.7 s `outbox` poll stalls the single worker. Inputs for the scheduled-load
workstream: Action memory in the module host at :00, and the outbox drain must not run
on the network worker.

---

## Steps 1–4: merges on the old app (Whagons lab pin, legacy presence clients)

Each step is the previous one plus the named merge, built from a local tag
(`gvperf-step-<name>`, `local-load` profile) in its own worktree/target dir
(`lab-5k/build-steps2.sh`); binary SHA-256 in `lab-5k/bin-<step>/sha256`. Same lab, data
and workload as Step 0 (`bench/whagons/run-remote-case.sh`, thin + 8 SDK users, 2 shards
per 200 users, 60 s ramp, 180 s plateau unless noted). Runtime env adds
`GONVEX_PRESENCE_LEGACY_BEAT_PATHS=presence.beat`,
`GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS=presence.transition` (old clients' beats are
answered in Rust), V8 in process (default). staging/replica-state/offload ran with the
ws-buffers container sysctls (`tcp_[rw]mem 4096 16384 262144`, `tcp_notsent_lowat`,
`somaxconn`); from ws-fix on they are dropped (a0a33ff4 caps per socket).

| step (commit) | runtime sha256 |
|---|---|
| staging = deploy/sqlite-staging 216976c9 + ws-buffers 684330d8 + gonvex_capture fix (cde57d16) | `a672023b0adc…` |
| + replica-state ccdc4e8b (9cc6e6fa) | `91b3db14ce29…` |
| + offload b4edd647, thin client answers replica.needHashes (ab922f45) | `d2820e332d05…` |
| + ws-buffers a0a33ff4 (bc5a0be8, local tag only) | see `bin-ws-fix/sha256` |
| + scheduled-load edffa51a (2d539a28, local tag only) | see `bin-sched-load/sha256` |

| step | case | users | heartbeat p95/p99 | save p95/p99 | delivery p95/p99 | taskDelivery p95/p99 | errors | connected | mem peak | CPU (throttled periods) | oracle | result |
|---|---|---:|---|---|---|---|---:|---|---:|---|---|---|
| base | base-thin-200 | 200 | 16,415/45,013 | 28,234/58,230 | 13,242/24,178 | 21,754/41,824 | 1.25 % | 200/200 | 2,048 MiB | 0.80 (1072/1781) | 1 mism./9 | FAIL |
| staging+ws-buffers 684330d8 | staging-thin-200 | 200 | 3,940/16,848 | 16,603/24,621 | 9,471/17,053 | 13,067/20,090 | 0.70 % | 200/200 | 1,978 MiB | 0.61 (816/1766) | 0 mism./9 | FAIL |
| +replica-state | rstate-thin-200 | 200 | 292/7,853 | 3,508/24,744 | 1,801/6,692 | 6,708/20,185 | 0.64 % | 200/200 | 1,532 MiB | 0.37 (417/1683) | 0 mism./9 | FAIL |
| +offload | offload-thin-200 | 200 | 869/10,534 | 3,451/20,486 | 6,726/16,666 | 1,025/12,576 | 0.66 % | 196/200 | 1,710 MiB | 0.34 (415/1647) | 0 mism./9 | FAIL |
| +offload | offload-thin-500b | 500 | 81,468/113,928 | 130,017/156,035 | 25,504/33,677 | 76,522/110,711 | 10.54 % | 500/500 | 1,821 MiB | 1.00 (1756/1758) | 4 mism./7 | FAIL |
| +ws-buffers a0a33ff4 | wsfix-thin-200b | 200 | 1,045/13,937 | 8,712/16,055 | 7,216/13,597 | 7,999/16,693 | 1.44 % | 200/200 | 1,598 MiB | 0.41 (425/1451) | 0 mism./8 | FAIL |
| +scheduled-load | sched-thin-200 | 200 | 500/12,220 | 3,135/20,446 | 6,478/15,142 | 1,922/14,323 | 0.58 % | 200/200 | 1,544 MiB | 0.39 (524/1729) | 0 mism./9 | FAIL |

Notes: `offload-thin-500b` is the rerun with per-shard tunnels (the first 500-user try stalled at
241 connections: a single reverse SSH tunnel ends in one macOS `sshd-session`, capped at 256
open files; the generator now opens one `ssh -L` per shard from the Mac). `wsfix-thin-200b`
ran 150 s plateau / 45 s ramp on a quiet host; a first `wsfix-thin-200` overlapped the
new-app lab setup and is not used. `sched-thin-200` overlapped the new-lab user prepare
(host io PSI 5 %). All oracle checks so far: 0 leaked rows; the 500-user mismatches are
missing rows after 15 s while delivery queues were 18–30 s deep.

Runtime side (plateau `top.py --delta`), p50 / p99:

| stage | staging | +replica-state | +offload | +offload @500 | +ws a0a33ff4 | +scheduled-load |
|---|---|---|---|---|---|---|
| fn.total presence | 47 ms / 4.23 s | 22 ms / 2.68 s | 19 / 302 ms | 123 ms / 2.42 s | 15 ms / 1.50 s | 22 / 671 ms |
| fn.total reducer | 116 ms / 4.94 s | 38 ms / 2.68 s | 27 / 638 ms | 183 ms / 3.43 s | 29 ms / 1.21 s | 34 / 940 ms |
| snapshot.permit_wait | 4.78 / 21.9 s | 2.73 / 17.6 s | 3.25 / 19.0 s | 31.5 / 77.3 s | 4.45 / 15.5 s | 3.52 / 19.2 s |
| delivery.queue_wait | 17 ms / 24.6 s | 7 ms / 16.9 s | 0.8 ms / 20.0 s | 18.1 / 116 s | 1.1 ms / 19.1 s | 1.5 ms / 21.1 s |
| ws.inbound_wait | 0.03 ms / 8.95 s | 0.02 / 302 ms | 0.01 / 423 ms | 0.01 ms / 3.71 s | 0.01 / 487 ms | 0.01 / 532 ms |
| tokio.sched_lag | 1.2 / 88 ms (2.3 s) | 1.2 / 50 ms | 1.2 / 44 ms | 1.3 / 80 ms | 1.2 / 48 ms | 1.2 / 48 ms (1.8 s) |

**Binding constraint after these steps: replica snapshot admission.** Memory fell from the
2 GiB limit (staging, runtime RSS 1.48 GiB with V8 in process) to 1.5–1.7 GiB with
replica-state; reducers are no longer the tail at 200 users (offload: heartbeat `fn.total`
p99 220 ms, `sched_lag` p99 44 ms). What remains: 2 snapshot permits
(`GONVEX_REPLICA_SNAPSHOT_CONCURRENCY` default = CPU count, min 2), ~10 opens/s at 200 users
queue for 3–5 s at p50, and a connection's replies and deliveries wait behind its pending
opens (`delivery.queue_wait` p99 17–24 s while the worker is 3 % busy). At 500 users the CPU
saturates as well (1.00 core, 1,756/1,758 periods throttled; permit wait p50 31 s). The
replica-state snapshot path no longer records `snapshot.query/intern/build`, so the time
inside a permit is not broken down; flamegraph user frames do not resolve past libc
(DWARF unwinding stops at `[libc.so.6]`, ~25–30 % self time) — not fixed yet.

## Cron :00 window at 200 users on the merged build (sched-cron-200)

Binary: scheduled-load step (2d539a28). Plateau 15:46:14–16:06:15, on- and off-CPU perf in
parallel 15:59:00–16:04:00 (`LOAD_PERF_PARALLEL=1 LOAD_PERF_SECONDS=300`, off-CPU switch
timeline kept in `sched-cron-200/offcpu.switches.txt`). Host: a delivery-v3 build ran in
parallel (other cores). **Generator caveat:** runtime access tokens live 15 min and
run.ts did not refresh them, so every session dropped 15 min after sign-in (16:00:15–16:01:46).
The minute before :00 is clean; after 16:00:15 users were leaving. The same expiry explains
both earlier "teardown hangs" (base-cron-150 users dropped 14:04, 15 min after sign-in).
run.ts now refreshes sessions (410f39bf).

| minute | heartbeat n / p95 / p99 | save p95 / p99 | delivery p95 / p99 |
|---|---|---|---|
| 15:54 | 913 / 81 / 129 | 168 / 435 | 158 / 445 |
| 15:55 | 934 / 199 / 475 | 223 / 806 | 467 / 616 |
| 15:56 | 906 / 140 / 221 | 296 / 551 | 213 / 520 |
| 15:57 | 914 / 86 / 114 | 219 / 239 | 134 / 258 |
| 15:58 | 915 / 90 / 196 | 191 / 263 | 160 / 274 |
| 15:59 | 905 / 86 / 179 | 242 / 503 | 158 / 249 |
| 16:00 (users leaving from :15) | 679 / 108 / 415 | 432 / 585 | 416 / 724 |

Runtime across :00 (where base had a 33.7 s `outbox` poll and reclaim): memory flat
(anon 894–913 MiB, page cache 351–420 MiB, `memory.events max` 0, RSS 1.07–1.09 GiB, one
process), longest poll of the whole run 369 ms (`ws_connection`, 15:55:18), `poll.outbox`
max 55 ms, 15:59:50–16:00:15: `fn.total cron` p99 84 ms (max 1.53 s), `tokio.sched_lag`
p99 6.6 ms (max 503 ms). Off-CPU: the longest `tokio-rt-worker`-named blocks (≤ 3 s) are
blocking-pool threads parked on a futex; none in reclaim or page-fault paths. **Answer to
"reclaim or synchronous code":** on the merged build the stall does not occur. On base it
coincided with the cgroup at `memory.max` (module host 395→788 MiB at :00, direct reclaim
26 % of on-CPU samples), and it disappears once memory has headroom (V8 in process +
replica-state, ~1.5 GiB) and scheduled-load staggers crons. No synchronous long-running code
showed on the outbox path in this capture. That is consistent with reclaim, but it does not
prove the base stall was reclaim alone: the base run had no perf over 14:02.

## Step 5: perf/delivery-v3 adb6bcd9 (0faccf31) — oracle gate

Merged onto 75f180d0 (offload line) because delivery-v3 already carries deploy/sqlite-staging
f482033c, perf/delivery, replica-state, visibility-masks, sqlite-direct, offload and a rebuild
of ws-buffers a0a33ff4. Conflicts: Rust hunks taken from delivery-v3 (its side was the
superset in each); thin client keeps `presenceV2` (opt-in `presence: 1`) and legacy-rows
default, and adds delivery-v3's `batchFrames` (`LOAD_THIN_BATCH_FRAMES=0` disables). The
earlier ws-fix and sched-load step builds are superseded (local tags only). Tests on 0faccf31:
`cargo test -p gonvex-runtime -p gonvex-store -p gonvex-websocket`: 482 passed, 0 failed,
6 ignored. Runtime sha256 in `lab-5k/bin-delivery-v3/sha256`.

Oracle gate (old-app lab, 100 users: thin + 16 real-SDK users, 2 shards, 45 s ramp, 600 s
plateau; admins change permissions every 120–300 s: team add/remove, spot add/remove, share,
assign, setSpot, plus one 50-task reorganization per shard; each change is followed by a
fresh SDK session's snapshot that the target's long-lived replica must match within 15 s):

| case | checks | converged | no change | mismatched (leaked / missing) | thin frames in | thin bytes in | heartbeat p95/p99 | delivery p50/p95/p99 | save p95/p99 |
|---|---:|---:|---:|---|---:|---:|---|---|---|
| dv3-oracle-100 (batch frames on) | 23 | 18 | 5 | 0 (0 / 0) | 42,273 | 757 MB | 101 / 1,398 | 80 / 486 / 5,513 | 153 / 404 |
| dv3-oracle-100-nobatch (`LOAD_THIN_BATCH_FRAMES=0`) | 23 | 18 | 5 | 0 (0 / 0) | 119,030 | 756 MB | 55 / 164 | 34 / 198 / 4,882 | 133 / 346 |

**Gate: pass** (zero leaked, zero missing, `client.thin.unsupported` empty in both). Batch frames
cut frames received 2.8× for the same bytes. Cost: with batching, reducer/heartbeat p99 is
1.4 s vs 164 ms. A reducer resolves only after the replica watermark covering its commit,
and a commit that carries no rows for the caller (heartbeats) gets that watermark in a
data-free batch on the 1 s tick. Delivery p50 also rises 34 → 80 ms (50 ms data tick).
Runtime: 0.12–0.13 cores, 1.38–1.46 GiB peak.

## Step 6: perf/action-depth 3f5a2ed2 (6a934356) + deploy/sqlite-staging bc1f0a06 (d87f934e)

action-depth carries scheduled-load edffa51a, whose store/host-call changes overlap
sqlite-direct's direct SQL path. Ported, not hand-merged blindly:
`TenantTransaction` keeps `writer_stats`/`outbox_written`; `commit_with_outcome` wraps the
direct commit and returns `CommitOutcome {revision, changes, members_changed,
outbox_written}`; `feed::finalize_direct` takes the per-database feed-known flag, probes for
staged rows first and returns the change count; the direct outbox insert sets
`outbox_written`; cron `rows_read` is counted in the profiled host-call path (SQL answered
inline on the isolate thread is not counted). The lab's encoded-row-size cache (af869fb1, via
scheduled-load) targets replica.rs functions the replica-state rewrite removed, so it is not on
this line. bc1f0a06 is admin-cli/export only (runtime binary unchanged); its Postgres test
uses `capture::install`, our `check_restart.py` registers a no-op `gonvex_capture` for the
same root cause. Tests on 6a934356: 497 passed, 0 failed, 6 ignored (admin-cli's Postgres
export test not run: needs Postgres).

### Cron :00 window at 200 users after action-depth (ad-cron-200)

Binary: action-depth step (6a934356, sha256 `b075a23e6f1b…`), old-app lab, 200 users (thin + 8 SDK),
plateau 16:46:18–17:06:20 (20 min; run.ts token refresh in place: all 200 sessions held past
the 15-min mark), on- and off-CPU perf 16:59–17:04. Plateau overall: users 200/200, errors
0.24 %, oracle 61 checks / 0 mismatched; heartbeat p50/p95/p99 9 / 138 / 1,534 ms, save
100 / 194 / 984, delivery 88 / 184 / 2,443, taskDelivery 103 / 202 / 1,863 (p99s include the
16:52 reconnect wave and a 17:03 link hiccup, see below). Runtime: 0.12 cores, 225 of 11,511
periods throttled, peak 1,467 MiB, RSS 0.91 → 1.11 GiB over 20 min, `memory.events max` 0.

Before/after for the :00 herd (heartbeat n / p95 / p99, ms):

| minute | base @150 (base-cron-150) | action-depth @200 (ad-cron-200) |
|---|---|---|
| :55 | 16,144 / 27,586 (*/5 jobs) | 935 / 100 / 164 |
| :58 | 1,370 / 2,099 | 916 / 113 / 152 |
| :59 | 4,708 / 5,849 | 907 / 117 / 174 |
| :00 | 31,804 / 36,592 | 914 / 115 / 185 |
| :01 | 25,985 / 29,439 | 923 / 121 / 175 |
| :02 | 52,023 / 58,778 | 940 / 123 / 195 |
| :03 | 651 / 1,310 | 954 / 184 / 1,572 (link: ping timeout ~17:03:10, RTT to 1.5 s) |

`window-report.py` (minute before vs two minutes after 17:00): heartbeat p95/p99 117/174 →
118/181, save 199/316 → 184/194, delivery 162/296 → 174/197, taskDelivery 213/318 → 192/210.
Runtime between 17:02:50 and 17:04:00 (the client blip): `delivery.queue_wait` p99 3.8 ms,
`tokio.sched_lag` p99 2 ms, no long poll, so the 17:03 tail is the Mac↔arch3 link.

| | base @150 | action-depth @200 |
|---|---|---|
| runtime memory at :00 → +2 min | anon 1,399 → 1,816 MiB, cgroup at 2 GiB, `memory.events max` 344 → 6,324 | anon 808 → 823 MiB, `max` 0 |
| process RSS at :00 → +2 min | runtime 1.29 → 1.15 GiB + module host 395 → 788 MiB | one process (V8 in process) 1.08 → 1.10 GiB |
| top-level Actions | — | `action.running` peak 1, `action.waiting` peak 0 |
| longest poll | `outbox` 33.7 s (14:02:40) | 454 ms (`other`, 16:45:18, sign-in ramp); none > 30 ms after 16:50 |
| `tokio.sched_lag` max, :00–:02 | 17.2 s | p99 2 ms, max 462 ms (whole plateau) |
| `fn.total cron` p99 / max | — | 60 ms / 1.06 s |
| scheduler | — | 2,755 completed, 0 failed, lag 0 |

Isolate count is not exported by this build (no gauge). The `action.running` gauge shows the
one-root-Action cap holding.

**Idle RSS across :00 (new-app lab, Whagons split module, no users, action-depth binary,
started 16:24:47):** process RSS 546 MB (16:25) → 640 MB (17:00) → 615–618 MB (17:02–17:06),
HWM 672 MB, 84 threads; cgroup anon 408 → 522 MB. For comparison, Coordinator's staging data
point for the same shape before action-depth: 528 MB → ~990 MB by :00 → ~1.07 GB after :13 (HWM
1.11 GB). Idle RSS stays roughly flat across :00 with action-depth (+~95 MB in the first 35 min,
no step at :00). Sampler log: `lab-5k-new/idle-rss-action-depth.log` (to 17:20).

---

## sqlite.1 candidate: `f23a2194`

**perf/integrate-5k `f23a2194` = sqlite.1 candidate (green).** Contents: deploy/sqlite-staging
bc1f0a06 (incl. f482033c, f3ef8ae1), perf/delivery-v3 adb6bcd9 (delivery 12d4ba07,
replica-state ccdc4e8b, visibility-masks 2d031e4f, sqlite-direct 6e08c242, offload b4edd647,
ws-buffers websocket crate a0a33ff4 rebuild + review fixes, profiler, fixture), perf/action-depth
3f5a2ed2 (scheduled-load edffa51a, ported onto sqlite-direct, see Step 6), check_restart.py
`gonvex_capture` control fix, bench changes. All four upstream heads are ancestors (checked 17:40,
no newer commits). Runtime code is identical to the measured `bin-action-depth`
(sha256 `b075a23e6f1b…`; `git diff 6a934356 f23a2194 -- rust ':!rust/crates/admin-cli'` is
empty).

| gate | result |
|---|---|
| `cargo test --workspace --no-fail-fast` (debug, featureless) | **633 passed, 0 failed, 7 ignored** (41 test binaries; quiet host). A first run while a 500-user case was loading the host failed one timing test, `gonvex-store writer_durability::checkpoints_run_off_the_writer_and_the_wal_stays_bounded` (WAL 4.86 MB vs ~4.1 MB bound; the background checkpoint thread was starved); it passes 3/3 alone and in the quiet full run. Flaky under host load, flagged. |
| oracle, batch frames on (rc1-oracle-100: 100 users, 16 SDK, 600 s, permission changes) | 23 checks, 18 converged, 5 no change, **0 leaked / 0 missing**; 26,246 `replica.batch` frames; `unsupported` {}; errors 0.08 %; heartbeat p95/p99 115/201 ms, save 176/198, delivery 193/3,949 |
| oracle, `LOAD_THIN_BATCH_FRAMES=0` (rc1-oracle-100-nobatch) | 23 / 18 / 5, **0 leaked / 0 missing**; 0 batches, 118,929 frames; `unsupported` {}; errors 0.08 %; heartbeat 87/540, save 137/1,842, delivery 260/3,470 |
| 200-user thin run vs base (ad-cron-200, 20 min plateau, vs base-thin-200) | heartbeat p50/p95/p99 9/138/1,534 vs 1,119/16,415/45,013 ms; save 100/194/984 vs 3,970/28,234/58,230; delivery 88/184/2,443 vs 1,090/13,242/24,178; errors 0.24 vs 1.25 %; memory peak 1,467 vs 2,048 MiB; CPU 0.12 vs 0.80 cores; oracle 61 checks / 0 mismatched |
| :00 window vs base (ad-cron-200 @200 vs base-cron-150 @150) | heartbeat p95 at :00/:01/:02 = 115/121/123 ms vs 31.8/26.0/52.0 s; no memory events (base: cgroup at 2 GiB); longest poll < 30 ms after the ramp (base: 33.7 s `outbox`) |

Not a pass of the 5k rule yet: at 200 users p99s are 1.0–2.4 s on the old app (the 16:52
reconnect wave and a 17:03 link drop are inside the window), and 500 users fail on CPU
(below).

Known behaviour to pass on: with batch frames, a reducer whose commit carries no rows for its
caller resolves at the next data-free batch (1 s tick). It showed as heartbeat p99 1.4 s in the
delivery-v3-only gate, and not in the candidate gate (201 ms). Not investigated further.

### 500 users on the candidate (ad-thin-500b, old app)

users 500/500, errors 3.97 %, oracle 8 checks / 0 mismatched; heartbeat p50/p95/p99
85 / 46,101 / 64,299 ms, save 25,702 / 79,408 / 105,221 (offload step @500: 577 / 81,468 /
113,928 and 64,143 / 130,017 / 156,035). Runtime 0.99 cores (1,709/1,753 periods throttled),
peak 1,659 MiB. `snapshot.build` p50 18.4 s / p99 48.8 s (now instrumented again) and
`snapshot.permit_wait` p50 19 s; `delivery.queue_wait` p50 9.4 s. **Binding constraint at 500:
CPU in snapshot builds** (sign-in burst plus detail windows); reducers are 70–90 ms p50.
`thin unsupported`: 155 × `replica.error: authenticate with an active tenant before opening a
Replica Collection` (opens racing re-auth under the backlog). First try (ad-thin-500) lost one
shard's tunnel to a Tailscale drop (17:12); tunnels now retry.

---

## Step-ups on the candidate, new-app lab

Lab `lab-5k-new` (containers `gvnew-*`, project `gvperf-new`): Whagons worktree
`whagons-integrate-5k` (perf/integrate-5k = staging f7faec3e5 + presence-v2 96e1f1f59 +
bundle-split 51501c890; `module.split: true`, manifest artifact `8e7aa7b4d461`, local SDK
`@gonvex/client 0.5.2-sqlite.0` built from this branch, package.json pin edited locally to it),
`lab-setup.sh 5000` (26 tenants, 5,000 prepared users, staffed, verify passed), baseline in
`lab-5k-new/baseline-data`. Generator: Mac `app-new` (same generated api/manifest + local SDK),
`LOAD_PRESENCE_V2=1` (thin clients advertise `presence: 1`, transitions are `presence.set`, no
heartbeat Reducers, presence delivery timed from `presence.patch`; the 8 SDK users use the v2
SDK), batch frames on. Binary = candidate runtime (`bin-action-depth`). The old-app runtime is
stopped during these runs.

### new-500: 500 users — FAIL (CPU, then memory)

| kind | samples | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| save | 1,009 | 35,662 | 106,272 | 122,059 |
| reducer | 1,020 | 35,003 | 106,226 | 122,058 |
| taskDelivery | 8,893 | 12,932 | 63,384 | 89,225 |
| delivery (presence) | 35,112 | 652 | 1,173 | 2,497 |
| query | 65 | 17,418 | 27,296 | 28,904 |

users 500/500, errors 29.6 % (265 task updates not delivered before teardown, 159 detail
queries never current), oracle 8 checks: 1 mismatch = **0 leaked**, 696 missing after 15 s
(16 s delivery backlog), 2 oracle snapshots never completed. Runtime: 1.00 core (1,757/1,757
periods throttled, 407 s throttled in 176 s), cgroup at 2,048 MiB (RSS 1.71 GiB, anon 1.23 GiB).

Profiler (plateau): `snapshot.build` p50 28.7 s / p99 72.8 s with `snapshot.permit_wait`
p50 34.6 s (the build includes its permit wait); inside the permit `snapshot.visibility` p50
7.5 ms, `snapshot.begin` 0.4 ms, `snapshot.acl` 0.24 ms; `delivery.queue_wait` p50 15.9 s,
`delivery.visibility` p50 0.8 ms / p99 113 ms; `fn.total reducer` p50 893 ms / p99 5.3 s;
`fn.total cron` p50 818 ms; `commit.fsync` p50 13 ms; `tokio.sched_lag` p99 93 ms;
`ws.encode` p50 1 µs, `ws.compress` p50 103 µs.

**Where the CPU goes** (on-CPU perf 30 s in the plateau, 199 Hz, self time; user frames
resolved through a symfs that mirrors the container paths; call chains still do not unwind
because perf recorded no build-id for `/binhost/gonvex-runtime`, so this is self time by
thread and code category, not inclusive time per stage):

| thread (≈ stage) | total | SQLite | JSON encode/decode | replica maps/hash | visibility | deflate | V8 | alloc/drop | libc copies | other runtime | kernel |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `gonvex-offload` (snapshot builds, batch item encode) | 46.4 | 5.8 | 9.6 | 9.0 | 1.4 | 0.0 | 0.0 | 2.3 | 5.3 | 4.7 | 8.3 |
| `gonvex-module-isolate` (V8 + inline SQL) | 24.8 | 2.9 | 1.5 | 0.5 | 0.0 | 0.0 | 3.5 | 0.8 | 1.9 | 2.2 | 11.5 |
| `tokio-rt-worker` (sockets, delivery) | 16.9 | 0.6 | 3.9 | 0.5 | 0.0 | 1.3 | 0.0 | 0.8 | 1.1 | 2.2 | 6.4 |
| `sqlx-sqlite-worker` | 8.4 | 2.8 | 0 | 0 | 0 | 0 | 0 | 0.1 | 0.7 | 1.0 | 3.8 |
| other | 4.0 | 0.5 | 0.4 | 0.1 | 0 | 0 | 0 | 0.3 | 0.2 | 0.6 | 1.9 |
| **all** | 100 | 12.5 | 15.4 | 10.1 | 1.4 | 1.3 | 3.5 | 4.3 | 9.2 | 10.7 | 32.0 |

Top self symbols: `sqlite3VdbeExec` 4.8 %, SipHash (`std` `HashMap` default hasher) 3.3 % +
`RandomState::hash_one` 1.9 %, `replica::json_byte_len` 2.9 %, `serde_json format_escaped_str`
2.7 %, `replica::collapse_changes` 1.6 %, `replica::write_stable_string` 1.1 %, indexmap
insert/clone/drop ~2 %. Kernel is spread (syscall entry/exit, page zeroing, `pread` copies,
some compaction, btrfs writes), not reclaim-dominated. Reading: snapshot builds are the CPU
(≈ 46 % of samples), and inside them JSON row decode/encode plus per-row hashing/size
bookkeeping cost about 3× the SQLite query; visibility is negligible (1.4 %). Delivery on
the network worker is ~17 % (JSON + deflate), V8 proper ~3.5 % (the isolate thread's time is
mostly syscalls and inline SQL). Targets: cached snapshots / log-only resume
(perf/resume-lazy) and raw rows (perf/delivery-v4) address the offload-pool share; v1→v2
visibility does not move CPU here.

### new-200 (reference, candidate, new app)

users 200/200, oracle 9 / 0 mismatched; save p50/p95/p99 168 / 8,019 / 11,266 ms, taskDelivery
159 / 6,002 / 11,302, presence delivery 613 / 1,140 / 2,769, query 104 / 4,802 / 11,256;
0.39 cores, peak 1,723 MiB (RSS 1.27 GiB). Error rate 53 %: 443 × "Task update was not
delivered before teardown" (old app at 200: 17 in 20 min) plus 28 × `budget_exceeded` in the
new app's `taskResources.recordTaskViewByTaskId`. The teardown check counts every user of the
tenant per undelivered edit, so a few edits whose row never matches the expected value in the
new app's `tasks.recentWorkspaceTasks` inflate it; not yet resolved (generator/app
compatibility vs real delivery loss is open). `snapshot.permit_wait` p50 616 ms, `fn.total cron`
p99 940 ms (max 18 s), `fn.total reducer` p50 92 ms / p99 9.9 s.

### Step 7: perf/reader-warm 33975410 (c4b20666) at the failing step (500, new app)

Clean merge on 92cd0a9a. Compose passes `GONVEX_SQLITE_CACHE_BUDGET_MB` (new-lab override).

| case | budget | save p50/p95/p99 | taskDelivery p50/p95/p99 | presence delivery p99 | errors | RSS max | cgroup peak | snapshot.build p50 | delivery.queue_wait p50 | db.reader_begin p99 | oracle (leaked) |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---|
| new-500 (candidate) | – | 35.7 / 106.3 / 122.1 s | 12.9 / 63.4 / 89.2 s | 2,497 ms | 29.6 % | 1,711 MiB | 2,048 | 28.7 s | 15.9 s | 169 ms | 1 mism. (0) |
| rw-500-b256 | 256 MB | 40.0 / 99.9 / 127.5 s | 17.7 / 61.4 / 88.4 s | 1,888 ms | 35.6 % | 1,884 MiB | 2,048 | 31.3 s | 19.1 s | 112 ms | 2 mism. (0) |
| rw-500-b128 | 128 MB | 29.2 / 99.3 / 118.4 s | 9.5 / 57.7 / 81.2 s | 2,360 ms | 19.3 % | 1,764 MiB | 2,049 | 28.9 s | 13.1 s | 127 ms | 2 mism. (0) |

All three are CPU-bound (1.00 core, every period throttled), so p99 is set by the snapshot
backlog and the budget does not move it beyond run-to-run noise. 256 MB costs ~120 MB more
RSS than 128 MB with no latency gain at this load. **Recommendation: keep the default at 128.**
Not measured: the cold-tenant benefit (warm readers at low load, startup), which is what
reader-warm targets; this step cannot show it.

## Step 8: perf/delivery-v4 d7254de0 (a5c1928d, on reader-warm c4b20666) and perf/resume-lazy d93290ec (5a796ad1)

Both merged clean. `cargo test --workspace --no-fail-fast`: a5c1928d 642 passed / 0 failed / 7
ignored; resume-lazy line (fba5b411) 643 / 0 / 7. fba5b411 adds `/dev/metrics` → `memory`:
`sqliteUsedBytes` (`sqlite3_memory_used`), `sqliteBudgetMiB`, `snapshotCache` (entries, bytes,
hits, builds, roll-forwards, fallbacks, evictions), `v8` (isolates, total/used/physical heap,
external, malloced; summed over live isolate workers as of each one's last call).

Oracle gate, delivery-v4 binary (old-app lab, 100 users, 16 SDK, 600 s, permission changes):

| case | checks | conv. | no change | mismatched | batches | frames in | unsupported | errors | heartbeat p95/p99 | save p95/p99 | delivery p95/p99 |
|---|---:|---:|---:|---|---:|---:|---|---:|---|---|---|
| v4-oracle-100 (batch on) | 23 | 18 | 5 | 0 (0/0) | 26,188 | 42,127 | {} | 0.22 % | 54 / 104 | 103 / 1,232 | 136 / 1,638 |
| v4-oracle-100-nobatch | 23 | 18 | 5 | 0 (0/0) | 0 | 118,009 | {} | 0.06 % | 51 / 165 | 171 / 2,500 | 81 / 1,894 |

With delivery-v4's call-completion flush, batch-on heartbeat p99 is 104 ms (delivery-v3 alone: 1,398 ms).

Oracle gate, resume-lazy line (fba5b411 binary, same setup):

| case | checks | conv. | no change | mismatched | batches | frames in | unsupported | errors | heartbeat p95/p99 | save p95/p99 | delivery p95/p99 | taskDelivery p95/p99 | CPU | peak |
|---|---:|---:|---:|---|---:|---:|---|---:|---|---|---|---|---:|---:|
| rl-oracle-100 (batch on) | 23 | 18 | 5 | 0 (0/0) | 26,380 | 42,363 | {} | 0.06 % | 55 / 96 | 98 / 132 | 116 / 847 | 131 / 164 | 0.09 | 1,507 MiB |
| rl-oracle-100-nobatch | 23 | 18 | 5 | 0 (0/0) | 0 | 118,294 | {} | 0.04 % | 46 / 72 | 106 / 148 | 55 / 711 | 96 / 118 | 0.09 | 1,436 MiB |

**Gate: pass.** First runs with every kind's p99 under 1 s (100 users, old app). Memory at the
end of rl-oracle-100: SQLite allocator 187 MB (budget 128 MiB is a soft limit), snapshot cache
81 MB / 691 entries (2,119 hits, 84 roll-forwards, 0 fallbacks, 0 evictions), V8 80 MB in 2 isolates.

### rl-500: 500 users, new app, resume-lazy line — FAIL (CPU; snapshot cache at its cap)

| kind | samples | p50 | p95 | p99 | candidate new-500 p50 / p99 |
|---|---:|---:|---:|---:|---|
| save | 881 | 24,851 | 83,415 | 110,202 | 35,662 / 122,059 |
| taskDelivery | 8,538 | 8,965 | 50,347 | 71,910 | 12,932 / 89,225 |
| presence delivery | 35,047 | 661 | 2,342 | 9,250 | 652 / 2,497 |
| query | 104 | 14,224 | 28,489 | 29,844 | 17,418 / 28,904 |
| reconnect | 339 | 209 | 621 | 1,290 | – |

users 500/500; oracle 7 checks, 2 mismatched, both **0 leaked** (277 and 697 missing, backlog);
errors 65 % (1,567 × "Task update was not delivered before teardown", see new-200 note; 28 ×
`budget_exceeded` in `taskResources.recordTaskViewByTaskId`). Runtime 1.00 core (1,758/1,758
periods throttled), cgroup at 2,048 MiB. Profiler: `snapshot.build` p50 15.4 s (28.7 s on the
candidate) / p99 39.4 s, `snapshot.permit_wait` p50 23.8 s, `snapshot.visibility` p50 5.7 ms,
`delivery.queue_wait` p50 5.0 s (15.9 s) / p99 76.8 s, `fn.total reducer` p50 3.1 s,
`tokio.sched_lag` p99 83 ms.

Memory by owner (`/dev/metrics memory` + runtime cgroup), 500 users connected:

| time | conn | SQLite alloc | snapshot cache (entries, evictions) | V8 heaps (isolates) | cgroup anon | page cache | rest of anon |
|---|---:|---:|---|---|---:|---:|---:|
| 19:22:57 (idle) | 0 | 127 MiB | 0 | 47 MiB (2) | 398 MiB | 346 MiB | ~224 MiB |
| 19:24:18 | 500 | 136 | 166 MiB (578, 0) | 54 (2) | 1,246 | 553 | ~890 |
| 19:25:40 | 502 | 131 | 249 MiB (618, 469) | 54 (2) | 1,675 | 348 | ~1,240 |
| 19:26:23 | 500 | 135 | 253 MiB (997, 639) | 28 (1) | 1,430 | 585 | ~1,010 |

The 256 MiB cache fills within ~2 min at 500 users and then evicts (1,591 evictions by the end
of the case), so opens fall back to builds. SQLite stays at its budget. V8 is small (28–55 MiB).
The largest share is the rest of the Rust heap, ~1.0–1.25 GiB at 500 users: per-connection
replica state plus queued deliveries behind a 5 s / 77 s (p50/p99) backlog.

CPU (on-CPU perf, self time by thread × category, as for new-500):

| thread | total | SQLite | JSON | replica/cache/hash | visibility | deflate | V8 | alloc | libc | other | kernel |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `sqlx-sqlite-worker` | 34.1 | 17.5 | 0 | 0 | 0 | 0 | 0 | 0.1 | 2.1 | 2.4 | 12.1 |
| `gonvex-offload` | 26.6 | 0.4 | 4.8 | 6.1 | 3.3 | 0.1 | 0 | 1.7 | 2.5 | 1.8 | 5.9 |
| `gonvex-module-isolate` | 20.4 | 1.7 | 0.8 | 0.3 | 0 | 0 | 2.3 | 0.3 | 0.9 | 1.8 | 12.3 |
| `tokio-rt-worker` | 17.5 | 0.3 | 0.7 | 6.8 | 0.1 | 1.2 | 0 | 0.9 | 0.8 | 2.0 | 4.7 |
| **all** | 100 | 20.0 | 6.3 | 13.2 | 3.4 | 1.3 | 2.3 | 3.1 | 6.3 | 8.1 | 36.5 |

vs the candidate (new-500): JSON 15.4 → 6.3 %, offload pool 46 → 27 %, SQLite worker threads 8 →
34 % (`sqlite3VdbeExec` 11.3 % self). SipHash (`std` default hasher) is 5.8 % self, plus
`RandomState::hash_one` ~2.6 %, much of it on the tokio worker (`hash_one<&(&str, &str)>`).
V8 proper stays at 2.3 %. **Binding constraint at 500 (new app): CPU, now in SQLite reads**
(sqlx worker threads; which statements is not attributed: call chains do not unwind) with the
snapshot cache thrashing at 256 MiB, and memory at the 2 GiB limit, mostly Rust heap outside
the cache/SQLite/V8.

### Visibility plans in the lab apps: v2 throughout (no v1 runs)

Checked 19:5x for the question "were the candidate runs on legacy v1 plans?": **no.** Both lab
apps load 340 plans, all `version: 2` with `permissionTables` (the runtime counts a plan as
legacy when `version != 2`, `modules.rs`), and neither runtime log contains "DEPRECATED legacy
visibility manifest":

| lab | app | plans | v2 | deprecation log |
|---|---|---:|---:|---|
| lab-5k (old app, Steps 0–6, candidate gates, ad-cron-200) | `whagons-sqlite-lab` 55969f041 | 340 | 340 | none |
| lab-5k-new (new app, new-*/rw-*/rl-500) | `whagons-integrate-5k` f7faec3e5 (staging before the #1623 revert) | 340 | 340 | none |

So every step so far ran v2 masks; the 3.7–9.4 s v1 delivery-visibility cost seen on Whagons
staging is not in these numbers. Whagons perf/visibility-v2 f38c1dd28 merged into the local
`whagons-integrate-5k` (lab-only commits: SDK pin, merge taking the staging-deploy line's
package.json/lock/patch; generated files regenerated): plans unchanged (0 of 340 differ), one
function signature changed (`workspaces.create`), artifact 8e7aa7b4d461 → 83669147bb63. Synced
into the new-lab baseline (`POST /dev/sync`, generation 2, 1,982 functions; old baseline kept as
`baseline-data-8e7aa7b4`), Mac `app-new` refreshed with it and SDK packages rebuilt from this
branch. Lab runs after this point use 83669147bb63.

### rl-500b: 500 users, resume-lazy line, artifact 83669147 (visibility-v2 merge) — FAIL

save p50/p95/p99 41,370 / 95,803 / 115,663 ms (rl-500 with 8e7aa7b4: 24,851 / 83,415 /
110,202), taskDelivery 8,440 / 61,733 / 84,399, presence delivery 697 / 6,771 / 15,932,
reconnect 193 / 307 / 405; errors 92 % (6,304 × task update not delivered before teardown,
113 × `budget_exceeded` in `taskResources.recordTaskViewByTaskId`, was 28); oracle 5 checks, 2
mismatched. Runtime 1.00 core, cgroup at 2,048 MiB. `fn.total reducer` p50 **8.7 s** / p99 10.1 s
(rl-500: 3.1 s), i.e. reducers run into the 10 s deadline; `snapshot.build` p50 4.7 s (mean
15.9 s), `delivery.queue_wait` p50 12.6 s. One run only. The app change (16 commits of the
staging-deploy line arrived with f38c1dd28; visibility plans are unchanged) makes the reducer
path heavier at this load; which function is not yet isolated (slow traces in
`rl-500b/profile/*.json`, `top.py --file`).

### Slow reducers on the 83669147 artifact: not an app or runtime regression (offline analysis)

Question: rl-500b (artifact 83669147) showed reducer `fn.total` p50 8.7 s vs 3.1 s (rl-500,
8e7aa7b4), 113 vs 28 `budget_exceeded`. Answer: same reducer cost per invoke; the difference is
queueing at CPU saturation.

1. **The budget:** all `budget_exceeded` in both runs are `invocation deadline elapsed while
   waiting for a module isolate`: the 10 s time budget spent queued for an isolate, not a
   host-call or row budget. They hit every task reducer (rl-500b: recordTaskViewByTaskId 113,
   tasks.create 56, updateDescription 53, createNote 45, update 34, reschedule 32, rename 26,
   assignUser 26, …); recordTaskViewByTaskId leads only because it is the most frequent call.
   Committed reducers fell 5.1/s → 2.7/s (fewer got an isolate in time); per completed slow
   trace, SQL (p50 6–7 ms) and finish (40–79 ms) are similar.
2. **App diff:** of the 13 non-merge commits between f7faec3e5 and the merge, the net source
   change under `gonvex/` (excluding generated files and tests) is `forms.ts` +11 (publish
   validation, #1631), `workspaces.ts` +22 (default-space task type, #1632) and one line of a
   generated catalog; no migrations. `gonvex/taskResources/commands.ts`
   (recordTaskViewByTaskId), task create/update, triggers, indexes: unchanged. The SDK change
   bundled with the rebuild is manifest/type-only (cron `dueWhen`; no cron uses it). Scheduled
   jobs ran the same mix in both runs.
3. **Micro-run on the real bundle** (`bridge_bench_whagons`, in process, `taskset -c 5`, copy of the
   largest new-lab tenant: 8,851 tasks, 1,951 active members; 200 calls per function; runs A1 =
   8e7aa7b4, B1 = 83669147, A2 = 8e7aa7b4):

| function | host calls / invoke | invoke p50 A1 / B1 / A2 (ms) | bridge+JS mean A1 / B1 / A2 (ms) |
|---|---|---|---|
| taskResources.recordTaskViewByTaskId | 12 (11 query, 1 write) | 0.82 / 0.84 / 0.80 | 0.37 / 0.38 / 0.36 |
| tasks.update | 15 (13 query, 2 write) | 1.33 / 1.44 / 1.28 | 0.61 / 0.66 / 0.57 |
| tasks.updateDescription | 15 | 1.32 / 1.29 / 1.26 | 0.52 / 0.51 / 0.49 |
| taskResources.createNote | 15 | 1.06 / 1.01 / 1.04 | 0.61 / 0.58 / 0.62 |
| tasks.create | 31 (28 query, 2 write, 1 schedule) | 1.50 / 1.50 / 1.55 | 0.85 / 0.82 / 0.86 |

   Module load 577 / 548 / 560 ms; RSS after load 159 / 158 / 149 MB. (Some calls fail with
   "Task not found" for tasks outside the admin's spaces; same in all three runs.)

**Reading:** a reducer costs ~1–1.5 ms of isolate time on either artifact; under load it waits
3–9 s for the isolate because the single vCPU is saturated by snapshot builds and SQLite reads
(the V8 isolate thread gets ~20–25 % of samples, mostly kernel), so the rl-500 vs rl-500b gap is
run-to-run variance of a saturated queue, not the artifact. Fix belongs to neither the Whagons
reducers nor the reducer path: it is the CPU budget at 500 (cache-heap / snapshot work).
Instrumentation gaps found: in the in-process V8 path `invoke.*` stages (JS time, host-call
count) and `/dev/metrics` per-function `calls`/`averageDurationMs` stay 0, and slow traces carry
the class label but not the function; that is why the per-function split above needed the bench.
`bridge_bench` gained `taskResources.recordTaskViewByTaskId` and `BRIDGE_BENCH_ONLY`.

## Harness: perf/delivery-loss 295c3aed merged (corrected task-delivery check), clock offset per run

**Flag for every earlier new-app run (new-200, new-500, rw-500-b256, rw-500-b128, rl-500, rl-500b):
their error rates include false alarms.** "Task update was not delivered before teardown" was a
harness defect, not lost updates (docs/performance/delivery-loss.md): run.ts compared an arch3
server timestamp with a Mac generator timestamp across clock skew, still expected failed saves,
had no teardown grace and some impossible view expectations, and each false alarm was counted
once per user of the tenant. Those runs' error rates (29.6 %, 35.6 %, 19.3 %, 65 %, 92 %, 53 % at
200) are therefore not meaningful; their latency, CPU, memory and oracle results are unaffected.
Old-app runs carried the same defect at a smaller skew (e.g. 17 such errors in ad-cron-200).
delivery-loss reports 200 users on the new app at 0 undelivered, 0.44 % errors, oracle pass.

From here on every case records the Mac − arch3 clock offset before and after the generator
(`bench/whagons/clock-offset.py`: one SSH session, 15 round trips, lowest-RTT sample, precision
≈ RTT/2; `<case>/clock-offset-{before,after}.json`, printed by step-report.py). First reading
20:2x: Mac −23.8 ms vs arch3 (RTT 4.0 ms).

## Step 9: perf/profiler-v2 a0e35f80 (merged at bfc31ae5's parent; clean)

`cargo test --workspace --no-fail-fast` on bfc31ae5: 647 passed, 0 failed, 7 ignored. Binary
`lab-5k/bin-profiler-v2`. Semantics: `/dev/metrics functions.<path>` `calls`/`errors`/
`averageDurationMs`/`lastDurationMs` are now server-side counts since process start (`source:
"server"`, plus `cancelled`, `maxDurationMs`, `totalDurationMs`); browser telemetry moved under
`functions.<path>.client`. Readers: `bench/whagons/step-report.py` and `summarize.py` do not read
these fields (unchanged); `apps/dashboard/src/App.tsx` reads them: type extended with the new
optional fields, totals use the exact `totalDurationMs` when present, the per-function card says
"Function Calls (server, since start)" for server counts (`tsc --noEmit` passes). Idle smoke test
on the new-app baseline (no generator): `invoke.isolate_wait`, `module_host.isolate_waiting`,
background stages (`sqlite.checkpoint`, `v8.gc_minor/major`, `v8.isolate_start/stop`,
`mem.malloc_trim`), the v8 isolate table, function names in slow traces and the "functions by
total time" table all appear in `top.py --file`. The next 500-user run reports them.

## Step 10: perf/cron-writer ce9294bf (890ae2ad) + Whagons perf/grant-projection-incremental 605f5ca6 (artifact 1118d6c7)

gonvex merge clean; `cargo test --workspace --no-fail-fast` on 890ae2ad: 653 passed, 0 failed, 8
ignored; binary `lab-5k/bin-cron-writer`. SDK packages rebuilt from 890ae2ad (module-sdk
`updateDataRows` now updates by primary key: `UPDATE … WHERE <key> IN (SELECT key FROM
json_each($1))`, present in the built `module.reducers.js` and `module.js`). Isolate guard
defaults: priority user > outbox/system > scheduled (scheduled moves up after 5 s), scheduled
Reducer slice 3 s (`GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS`), separate scheduled isolate off.

Whagons (local `whagons-integrate-5k`, lab-only commits): previous regenerated artifact committed,
605f5ca69 merged (chunked `grantProjection.reconcile`, 100 members/step, rescheduled; cheaper
`notifyIfUnassigned`), only generated files conflicted and were regenerated with codegen from
the 890ae2ad packages: artifact **1118d6c7e970** (340 v2 plans, 1,982 functions, no deprecation
log). Synced into the new-lab baseline (`gonvex_runtime_manifests` = 1118d6c7…; previous
baseline kept as `baseline-data-83669147`), Mac `app-new` refreshed. Next 500-user run: after
cache-heap (+ omit-nulls).

## Step 11: perf/cache-heap 36c440d6 (19803c3e) + Whagons perf/notification-fanout c67531834 (artifact da98a666)

gonvex merge clean; `cargo test --workspace --no-fail-fast` on 19803c3e: 656 passed, 0 failed, 8
ignored; binary `lab-5k/bin-cache-heap`. Lab runtime env (both labs' compose overrides):
`GONVEX_OFFLOAD_NICE=10`, `GONVEX_SQLITE_READER_NICE=10` (code defaults stay 0). omit-nulls not
merged. Whagons: c67531834 merged into the local app (one source conflict in
`tasks/bulkCommands.ts`, both sides narrowed `memberTeams` to the workspace's teams; took the
branch's `readDataValues` form; generated files regenerated), artifact **da98a6665b3b**
(`insertNotificationRows` in the reducer bundle), synced into the new-lab baseline (previous kept
as `baseline-data-1118d6c7`), Mac `app-new` refreshed.

### Load-model realism (notification-fanout finding)

The default generator mix is heavier than the production telemetry sample (3,783 traces, 13
accounts, 349 heartbeat person-minutes; Whagons docs/performance/notification-fanout.md):

- **task creates ~7× production**: ~0.097 creates per person-minute in the lab vs 0.014 in the sample;
- **never assigns at create**, so every create schedules `tasks.notifyIfUnassigned` to every member of
  the workspace's teams (~1,364 rows per create in the largest tenant);
- **workspace chat sends** (4 % of actions; the sample has none), each notifying the same audience;
- notifications were 81–91 % of change-log bytes in the large tenants.

The default mix stays as is for comparability (Gabriel asked for the heavier mix). run.ts now has
`LOAD_MIX=production`: both create actions weighted at 0.014/0.097 of the default
(`LOAD_CREATE_SCALE` overrides), chat sends off, `LOAD_CREATE_ASSIGN_FRACTION` (default 0: the
sample does not record assignees at create) gives that share of creates the creator as assignee.
Each step with the default mix gets one companion run with `LOAD_MIX=production` at the same user
count.

Oracle gate on 19803c3e (old-app lab, 100 users, 16 SDK, 600 s; nice env set):

| case | checks | conv. | no change | mismatched | batches | frames in | unsupported | errors | heartbeat p95/p99 | save p95/p99 | delivery p95/p99 | clock offset |
|---|---:|---:|---:|---|---:|---:|---|---:|---|---|---|---|
| ch-oracle-100 (batch on) | 23 | 18 | 5 | 0 (0/0) | 26,256 | 42,197 | {} | 0.06 % | 60 / 146 | 139 / 816 | 178 / 1,407 | −15.8 / −30.3 ms |
| ch-oracle-100-nobatch | 23 | 18 | 5 | 0 (0/0) | 0 | 118,267 | {} | 0.06 % | 161 / 1,756 | 522 / 2,591 | 1,702 / 3,128 | −30.5 / −32.1 ms |

**Gate: pass.** (The no-batch run's tail is worse than earlier no-batch gates: 360 throttled
periods vs 109; host io PSI was elevated by other labs. Not investigated.)

### ch-500 (default mix) and ch-500-prod (production mix), 500 users, new app, artifact da98a666

| | ch-500 (default mix) | ch-500-prod (`LOAD_MIX=production`) |
|---|---|---|
| users connected | 500/500 | 500/500 |
| errors | 11.54 % (140 × task detail queries never current, 3 × reducer timeout 20 s) | 0.08 % |
| oracle | 8 checks, 1 mismatch (0 leaked) | 8 checks, 0 mismatched |
| save p50 / p95 / p99 | 19,105 / 79,437 / 106,054 | 83 / 493 / 1,109 |
| taskDelivery | 8,165 / 53,811 / 77,969 | 138 / 506 / 1,086 |
| presence delivery | 729 / 2,125 / 4,565 | 522 / 1,000 / 1,060 |
| query | 12,121 / 28,759 / 29,858 | 55 / 910 / 2,087 |
| reconnect | 288 / 1,003 / 2,389 | 79 / 238 / 367 |
| tasks.create calls (plateau) | 124 | 24 |
| runtime CPU (throttled periods) | 1.00 (1,755/1,757) | 0.41 (195/1,754) |
| cgroup peak / anon peak | 2,048 / 1,280 MiB | 1,833 / 892 MiB |
| `invoke.isolate_wait` reducer p50 / p99 / max | 855 ms / 6.80 s / 7.19 s (94 % of invoke time) | 0.04 / 336 / 624 ms (73 %) |
| `invoke.isolate_wait` cron p50 / p99 / max | 1.6 ms / 10.0 s / 10.0 s (57 %) | 0.02 / 7.3 / 361 ms |
| scheduled vs user | cron 0.5/s (`fn.total` mean 1.96 s, p99 10 s), reducer 5.8/s (mean 1.72 s) | cron 2.4/s (mean 30 ms), reducer 5.2/s (mean 90 ms) |
| 3 s scheduled slice errors | none (one `notifyIfUnassigned` hit the 10 s deadline) | none |
| `snapshot.build` p50 / p99 | 12.4 s / 44.6 s | 9 ms / 2.76 s |
| `delivery.queue_wait` p50 / p99 | 8.4 s / 105.8 s | 1.4 ms / 2.39 s |
| clock offset Mac − arch3 | −32.3 / −32.7 ms | −33.8 / −35.1 ms |

Background work (ms of wall clock per s), ch-500: SQLite checkpoint 84.4 (0.71/s, 4,472 pages/s)
+ inline checkpoint 25.8, V8 GC minor 56.9 (27/s) + major 6.9, malloc_trim 0.6, snapshot cache
build 130,570 (7.7 builds/s; summed wall time of concurrent builds, stretched by starvation),
offload busy 680. ch-500-prod: checkpoint 45.2, inline 2.2, GC minor 3.6, major 0.2, cache build
1,195 (8.0/s), offload busy 142.

Top functions by server time, ch-500: recordTaskViewByTaskId 12.5 % (195 calls, mean 1.26 s),
tasks.create 12.0 % (124, 1.90 s), createNote 10.7 % (103, 2.05 s), notifyIfUnassigned 9.3 % (93,
1.96 s, 1 error), updateDescription 8.7 %, update 7.7 %, chat.sendWorkspaceChat 5.6 %, start 5.0 %.
The means are mostly isolate wait (94 %).

Heap by owner (ch-500 plateau, `/dev/metrics memory`): SQLite allocator 126–142 MiB (budget 128
soft), snapshot cache 74–77 MiB with **0 evictions** (cache-heap fixed the thrash; 1,763 entries,
mostly small per-task detail keys), V8 heaps 26–56 MiB, rest of anon ~0.9–0.95 GiB (anon peak
1,280 MiB). The 2 GiB cgroup figure is page cache (file 600–1,000 MiB, reclaimable); jemalloc
call-site profiling (cache-heap's `jemalloc-prof` build) not run here.

CPU by thread (on-CPU perf, self time, symfs-resolved), ch-500:

| thread | total | SQLite | JSON | replica/cache/hash | visibility | deflate | V8 | alloc | libc | other | kernel |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `tokio-rt-worker` | 36.3 | 0.6 | 4.5 | 6.0 | 0 | 1.9 | 0 | 3.6 | 2.7 | 3.8 | 13.0 |
| `gonvex-offload` | 34.5 | 0.4 | 4.1 | 7.2 | 4.9 | 0 | 0 | 3.0 | 3.4 | 2.2 | 9.3 |
| `gonvex-module-isolate` | 17.4 | 1.4 | 1.3 | 0.4 | 0 | 0 | 3.9 | 0.3 | 2.2 | 1.8 | 6.2 |
| `sqlx-sqlite-worker` | 9.2 | 2.5 | 0 | 0 | 0 | 0 | 0 | 0.2 | 1.0 | 1.2 | 4.3 |
| **all** | 100 | 5.0 | 10.0 | 13.6 | 4.9 | 2.0 | 3.9 | 7.1 | 9.5 | 9.3 | 34.7 |

Top self symbols: SipHash 3.2 %, `indexmap::Bucket<String, Value>::clone` 2.1 %, foldhash
`hash_one<&(&str,&str)>` 1.9 %, `drop_glue<serde_json::Value>` 1.6 %, std `hash_one<&String>` 1.4 %,
`mi_free` 1.35 %, `HashMap<(&str,&str),usize>::insert` 1.0 %, `BTreeMap<(&str,&str),
replica::CollapsedChange>` 0.9 %, deflate 0.8 %, `visibility_class::CompiledPlan::eval` 0.7 %.
SQLite CPU fell from 34 % of samples (rl-500) to 9 % (cache-heap + reader nice).

**Binding constraint at 500, default mix: CPU on the network worker and the offload pool:**
feed/delivery work (change collapsing, `serde_json::Value` clones and drops, hashing) and
per-open snapshot/visibility work, with a delivery backlog of 8 s at p50 (perf/feed-backlog,
perf/open-visibility). Reducers are cheap (~1 ms) but wait for the isolate. **With the production
mix (≈1/5 of the creates, no chat sends) the same 500 users run at 0.41 cores** and miss the
pass rule only narrowly: presence delivery p95 1,000 ms (p50 522 ms ≈ half the 1 s presence-v2
tick, so this kind cannot reach p95 < 500 ms by design), query p95 910 ms, and p99s of
1.06–1.11 s on save/taskDelivery/presence. Not stepped to 1k: 500 fails in both mixes.

### Production-mix step-up: 1,000 users (ch-1k-prod-b) — FAIL; 2.5k not run

Same build/artifact (cache-heap 19803c3e, da98a666), `LOAD_MIX=production`, 8 shards, 90 s ramp,
180 s plateau. A first try (`ch-1k-prod`, invalid) was limited by the lab transport: the
single-loop `remote-proxy.py` sat at 0.95–1.0 core from the ramp on (it used 0.04 core at 500, so
likely a spinning connection rather than load), 477 users never connected and the runtime idled
at 0.22 cores (oracle: 10 mismatches, all 0 leaked). The proxy now preforks `PROXY_WORKERS` (4)
asyncio workers on the shared listening sockets; in the rerun it used 0.08 core.

| | ch-500-prod | **ch-1k-prod-b** |
|---|---|---|
| users connected | 500/500 | 1,000/1,000 |
| errors | 0.08 % | 82 % (6,362 × "save still pending at teardown", 523 metadata / 494 detail subscriptions never current, 453 undelivered, 60 jobs pending 90 s after plateau) |
| oracle | 8 / 0 mismatched | 6 checks, 2 mismatched (0 leaked) |
| save p50 / p95 / p99 | 83 / 493 / 1,109 | 91,198 / 188,567 / 216,314 |
| taskDelivery | 138 / 506 / 1,086 | 55,507 / 150,932 / 176,554 |
| presence delivery | 522 / 1,000 / 1,060 | 697 / 1,292 / 2,501 |
| reconnect | 79 / 238 / 367 | 390 / 1,108 / 1,678 |
| runtime CPU (throttled) | 0.41 (195/1,754) | 1.00 (1,751/1,761) |
| cgroup peak / anon peak | 1,833 / 892 MiB | 2,048 / 1,491 MiB |
| `fn.total` reducer p50 / p99 | 58 / 610 ms | 176 ms / 2.00 s |
| `invoke.isolate_wait` reducer p50 / p99 / max | 0.04 / 336 / 624 ms | 63 ms / 1.83 s / 2.13 s (84 %) |
| `invoke.isolate_wait` cron p50 / p99 / max | 0.02 / 7 / 361 ms | 0.03 ms / 2.15 s / 3.01 s (50 %) |
| scheduled vs user | 2.4/s vs 5.2/s | 2.6/s vs 10.6/s |
| 3 s slice errors | none | none (cron `fn.total` max 3.01 s = the slice) |
| `snapshot.build` p50 / p99 | 9 ms / 2.76 s | 33.0 s / 75.2 s (66 builds/s) |
| `delivery.queue_wait` p50 / p99 | 1.4 ms / 2.39 s | 23.9 s / 124.7 s |
| clock offset Mac − arch3 | −33.8 / −35.1 ms | −16.9 / −15.9 ms |

Saves look slow to clients (p50 91 s) although `fn.total` reducer p99 is 2.0 s: a save resolves
only when the replica watermark covering its commit arrives, and that waits behind the 24 s
delivery backlog. Background work (ms/s): checkpoint 70.5 (5,557 pages/s) + inline 4.8, V8 GC
minor 15.7 + major 1.4, cache builds 5.8/s, offload busy 736. Top functions by server time:
recordTaskViewByTaskId 19.4 % (2.4/s, mean 311 ms), createNote 8.7 %, updateDescription 8.6 %,
update 6.5 %, assignUser 5.8 %, notifyIfUnassigned 4.3 % (31 calls, mean 948 ms),
grantProjection.reconcile 3.6 % (47 steps, mean 525 ms).

Heap by owner (plateau): SQLite 131–141 MiB, snapshot cache 72–88 MiB (0 evictions), V8 50–57 MiB,
rest of anon ~1.2 GiB (anon 1,413–1,473 MiB; delivery backlog).

CPU by thread (self time):

| thread | total | SQLite | JSON | replica/cache/hash | visibility | deflate | V8 | alloc | libc | other | kernel |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `gonvex-offload` | 46.8 | 0.6 | 8.2 | 10.6 | 6.7 | 0 | 0 | 3.6 | 4.6 | 3.5 | 9.0 |
| `tokio-rt-worker` | 30.6 | 0.7 | 1.7 | 4.9 | 0.1 | 3.7 | 0 | 2.1 | 2.2 | 5.3 | 9.8 |
| `sqlx-sqlite-worker` | 12.0 | 2.9 | 0 | 0 | 0 | 0 | 0 | 0.3 | 1.6 | 1.2 | 5.9 |
| `gonvex-module-isolate` | 8.0 | 0.9 | 0.5 | 0.1 | 0 | 0 | 1.8 | 0.2 | 0.8 | 1.2 | 2.5 |
| **all** | 100 | 5.3 | 10.7 | 15.8 | 6.8 | 3.8 | 1.8 | 6.6 | 9.5 | 11.9 | 28.6 |

Top self symbols: SipHash 4.5 % + std `hash_one<&String>` 2.4 % + foldhash `hash_one` 1.2 %,
deflate 3.0 %, `visibility_class::CompiledPlan::eval` 1.1 %, `drop_glue<Value>` 1.1 %, indexmap
clone 0.9 %. **Knee of the production mix: between 500 (near pass) and 1,000 (clear CPU
failure).** Binding constraint at 1k: CPU in per-open snapshot builds on the offload pool
(66 builds/s at ~1k connected; JSON, hashing, visibility evaluation), then delivery
(compression, hashing) on the network worker; the 24 s delivery backlog is also the largest
heap owner. Off-CPU not analysed for this step (on-CPU saturated: 1,751/1,761 periods
throttled).

### Batch-off oracle tail (ch-oracle-100-nobatch), cheap look

vs the earlier batch-off gate (rl-oracle-100-nobatch, before cache-heap): system CPU 0.09 vs 0.03
cores, throttled periods 360 vs 57, presence `fn.total` p99 4.72 s vs 50 ms, reducer p99 2.68 s vs
59 ms, isolate wait max 0.9–1.3 s, `ws.inbound_wait` p99 331 ms vs 0.16 ms. The tail is CPU
bursts in the cgroup (more syscalls for unbatched frames plus the new reader/offload nice
settings changing scheduling inside the 1-vCPU cgroup), not delivery. One run each; not
pursued further.

## Step 12: perf/open-visibility e0f44750 + perf/fast-hash ee06c759 (f42ecfc0)

Both merged clean on 0b3d595a (feed-backlog not yet pushed; runs below are without it). The
vendored `rust/vendor/serde_json` is patched in through `[patch.crates-io]` in `rust/Cargo.toml`;
the lab builds host binaries from the step worktree, so `Dockerfile.runtime` is not involved.
Tests on f42ecfc0: `cargo test --workspace --no-fail-fast` 657 passed / 0 failed / 8 ignored;
`cargo test -p gonvex-runtime --features perf-fixture --no-fail-fast` 407 passed / 0 failed / 9
ignored (all oracles, perf fixture v2 rebuilt). Binary `lab-5k/bin-open-vis`.

### Do snapshot builds continue after their open is cancelled? (1k, ch-1k-prod-b)

- `snapshot.build` is a scope timer (it records when the build future is dropped, cancelled or
  not): 66.3/s at 1k; the stages after the global permit (`snapshot.acl`, `.begin`,
  `.visibility`) run 39.5/s. So **~26.8/s builds ended before they got a permit**: dropped while
  waiting (p50 permit wait 36 s), before any SQL or visibility work. This matches the "builds
  that produced no entry"; those opens cost a queue slot and a later re-open, not build CPU.
- Code path: `replica.close` calls `ReplicaOpenQueue::remove` (aborts the `Abortable` around a
  waiting or running preparation), socket teardown `clear()`s the queue (drops the offloaded
  tasks), and the shared cache build runs inline in the opener's future (no detached spawn):
  a cancelled build stops at its next `.await`.
- Not measurable from the current profile: CPU spent after cancellation inside synchronous
  sections (row decode/visibility between awaits), and a shared build discarded when the opener
  holding the per-key build lock is aborted (the next opener rebuilds). `snapshot.send` is not a
  completion proxy (5.4/s vs 40/s builds even at 500 production-mix, where nothing times out).
  To settle it, count preparations aborted after permit acquisition (in `ReplicaOpenQueue::remove`
  for an active entry) and cache builds whose holder was dropped.

### Step 12 runs (open-visibility + fast-hash, without feed-backlog)

Oracle gate (old-app lab, 100 users, 16 SDK, permission changes): **pass both modes**.

| case | checks | conv. | no change | mismatched | batches | unsupported | errors | heartbeat p95/p99 | save p95/p99 | delivery p95/p99 |
|---|---:|---:|---:|---|---:|---|---:|---|---|---|
| ov-oracle-100 (batch on) | 23 | 16 | 7 | 0 | 27,013 | {} | 0.08 % | 61 / 171 | 106 / 223 | 140 / 1,185 |
| ov-oracle-100-nobatch | 23 | 14 | 8 | 0 | 0 | {} | 0.09 % | 59 / 221 | 140 / 595 | 218 / 2,527 |

(1 × `tasks.create` "Workspace not found" right after a permission change, both runs: app behaviour. The
batch-off tail of the cache-heap gate is gone: heartbeat p99 221 ms vs 1,756 ms.)

**ov-1k-prod was invalid (transport):** the runtime saw ~1,890 sockets for ~970 accounts (2×,
with real subscriptions: 14k vs 9k) and the Mac tunnels logged `connect: resource temporarily
unavailable` on the proxy socket. `asyncio.start_unix_server(sock=…)` re-listens with its default
backlog 100, overriding the proxy's 4096; eight tunnels opening sockets at once overflowed it and
client retries left extra server sockets. Fixed (backlog 4096, 80e4c7e7); step-report.py now
prints sockets per account and flags > 1.1. Rerun: ov-1k-prod-b (1,004 sockets / 1,000 accounts).

| | ch-500 (cache-heap) | **ov-500** (default mix) | ch-1k-prod-b (cache-heap) | **ov-1k-prod-b** (production mix) |
|---|---|---|---|---|
| users / sockets per account | 500 / 1.00 | 500 / 1.00 | 1,000 / 1.00 | 1,000 / 1.00 |
| errors | 11.5 % | 15.6 % (223 metadata subscriptions never current) | 82 % | 21 % (406 detail queries never current) |
| oracle | 1 mism. (0 leaked) | 1 mism. (0 leaked) | 2 mism. (0 leaked) | 3 mism. (0 leaked; 236 / 1,080 / 2,316 missing) |
| save p50 / p95 / p99 | 19.1 / 79.4 / 106.1 s | 1.80 / 27.1 / 37.7 s | 91.2 / 188.6 / 216.3 s | 39.9 / 109.7 / 136.8 s |
| taskDelivery p50 / p95 / p99 | 8.2 / 53.8 / 78.0 s | 1.33 / 17.0 / 30.4 s | 55.5 / 150.9 / 176.6 s | 17.4 / 70.8 / 99.5 s |
| presence delivery p50 / p95 / p99 | 0.73 / 2.1 / 4.6 s | 0.68 / 1.5 / 2.6 s | 0.70 / 1.3 / 2.5 s | 0.67 / 1.3 / 3.0 s |
| query p50 / p95 | 12.1 / 28.8 s | 6.4 / 22.6 s | – (8 samples) | 17.7 / 29.2 s |
| runtime CPU | 1.00 | 1.00 | 1.00 | 1.00 |
| anon peak | 1,280 MiB | 1,154 MiB | 1,491 MiB | 1,374 MiB |
| reducer isolate wait p50 / p99 / max | 855 ms / 6.8 / 7.2 s | 465 ms / 2.08 / 2.42 s | 63 ms / 1.83 / 2.13 s | 12 ms / 1.48 / 2.19 s |
| cron isolate wait p99 / max | 10 / 10 s | 4.2 / 5.8 s | 2.15 / 3.0 s | 1.54 / 5.4 s |
| `snapshot.permit_wait` p50 | 15.1 s | 3.5 s | 35.8 s | 32.9 s |
| `delivery.queue_wait` p50 / p99 | 8.4 / 105.8 s | 0.11 / 28.9 s | 23.9 / 124.7 s | 15.4 / 114.8 s |
| clock offset Mac − arch3 | −32 ms | −61 / −65 ms | −17 / −16 ms | −66 / −71 ms |

No 3 s slice errors (scheduled `fn.total` max 6.0 s at 500 and 5.4 s at 1k are isolate wait
before the slice; slices count from the lease). Background work, ov-1k-prod-b (ms/s): checkpoint
66.4 + inline 6.9, GC minor 15.3 + major 0.9, offload busy 723. Top functions by server time
(1k): recordTaskViewByTaskId 16.9 % (2.4/s, mean 250 ms), createNote 11.1 %, updateDescription
8.9 %, update 7.6 %, start 6.6 %; means are 85 % isolate wait.

Snapshot builds without an entry at 1k: `snapshot.build` 73.3/s vs post-permit stages
(`snapshot.acl`) 52.6/s: **20.7/s builds end before getting a permit** (dropped while waiting,
permit wait p50 33 s) and do no SQL or visibility work. So the wasted share is queue slots and
later re-opens, not build CPU.

Heap by owner, ov-1k-prod-b plateau: SQLite 121–138 MiB, snapshot cache 73–94 MiB, principal cache
5–10 MiB, V8 53–55 MiB, rest of anon ~1.0 GiB (anon 1,041–1,241 MiB); page cache 580–630 MiB.

CPU by thread (self time):

| thread | ov-500 | ov-1k-prod-b | of which (1k): JSON / replica+hash / visibility / deflate / kernel |
|---|---:|---:|---|
| `gonvex-offload` (snapshot builds, batch items) | 35.1 | 44.4 | 10.5 / 6.5 / 7.4 / 0 / 8.2 |
| `tokio-rt-worker` (sockets, delivery) | 20.9 | 29.5 | 2.8 / 3.6 / 0.2 / 3.0 / 9.2 |
| `sqlx-sqlite-worker` | 12.4 | 12.6 | SQLite 3.0, kernel 6.6 |
| `gonvex-module-isolate` | 26.5 | 10.0 | V8 2.0, kernel 3.6 |

Top self symbols (1k): `indexmap::Core<String, Value>::insert_full` 2.2 %, indexmap bucket clones
1.7 %, `mi_free` 1.3 %, deflate 2.5 %, indexmap lookups 1.2 %, `format_escaped_str` 1.1 %, foldhash
1.1 %, `sqlite3VdbeExec` 1.1 %, `drop_glue<Value>` 1.1 %. SipHash is gone (fast-hash). At 500
(default mix) the top symbol is change collapsing (`BTreeMap<(&str,&str), CollapsedChange>` 2.3 %).

**Binding constraint (both):** CPU. At 1k (production mix) per-open snapshot builds on the
offload pool (44 %: building `serde_json::Value` rows, visibility, hashing) and delivery on the
network worker (30 %); snapshot permits are the queue (p50 33 s), and the delivery backlog
(p50 15 s) is the largest heap owner. At 500 (default mix) the same plus change collapsing and
V8 (26 %). Not passing at either step; 2.5k not run.

## Step 13: snapshot-raw 61911345 + feed-backlog 0d2b261a + resume-fix c25141f3 (ebb65fdd)

Merges: snapshot-raw on 3db7f121 (clean); feed-backlog (conflicts: runtime `Cargo.toml` bench/test
lists, both kept; `replica.rs` duplicate `resume_changes` dropped, snapshot-raw had moved it to
`raw_log.rs` with the same per-table query); resume-fix (`Cargo.toml` test list). Tests:
snapshot-raw alone 658/0 workspace, 408/0 perf-fixture; feed-backlog + snapshot-raw 658/0, 409/0;
**ebb65fdd: workspace 658 passed / 0 failed / 8 ignored; perf-fixture 410 / 0 / 9**, incl.
`resume_log_only::valid_cursor_resumes_read_no_full_set_even_on_a_sparse_key`. Pushes over HTTPS
(`gh` credential) since the SSH agent is gone. An attempt to start the new-lab runtime before the
build had installed its binaries created `bin-resume-fix` root-owned (install failed); fixed
before any run (binary sha256 `54e689f9ee4f…`). New-lab runtime now starts from its host path
(`LOAD_RUNTIME_COMMAND`, empty entrypoint) so host perf can open it.

Oracle gate (old app, 100 users): rf-oracle-100 23 checks / 0 mismatched / `unsupported` {} /
0.06 % errors, heartbeat p95/p99 52/110, save 86/110, delivery 95/642, taskDelivery 118/149 ms;
nobatch 23 / 0 / {} / 0.10 %, heartbeat 45/68, save 107/137, delivery 57/240 ms. Runtime 0.07 cores.

| case | users | errors | oracle | save p50/p95/p99 | taskDelivery | presence delivery | query | reconnect | cores (throttled) | anon peak |
|---|---:|---:|---|---|---|---|---|---|---|---:|
| rf-500 (default mix) | 500 | 0.17 % | 8 / 0 | 142 / 965 / 1,929 | 214 / 875 / 1,799 | 545 / 1,006 / 1,119 | 53 / 617 / 3,613 | 48 / 104 / 224 | 0.67 (815/1,775) | 1,215 MiB |
| rf-500-prod | 500 | 0.08 % | 8 / 0 | 30 / 92 / 233 | 87 / 155 / 296 | 533 / 1,003 / 1,063 | 52 / 105 / 568 | 46 / 81 / 133 | 0.31 (96/1,778) | 997 MiB |
| rf-1k-prod-1 | 1,000 | 0.89 % | 9 / 0 | 50 / 12,434 / 38,176 | 111 / 7,794 / 37,083 | 583 / 1,118 / 25,886 | 53 / 9,859 / 12,960 | 53 / 115 / 188 | 0.64 (539/1,763) | 1,191 MiB |
| rf-1k-prod-2 | 1,000 | 0.04 % | 9 / 0 | 48 / 4,225 / 9,510 | 111 / 1,023 / 4,536 | 591 / 1,023 / 1,197 | 53 / 5,438 / 8,435 | 56 / 184 / 494 | 0.64 (543/1,772) | 1,174 MiB |
| rf-1k-prod-3 | 1,000 | 0.04 % | 9 / 0 | 44 / 2,053 / 8,804 | 106 / 540 / 3,976 | 554 / 1,013 / 1,108 | 53 / 5,717 / 9,116 | 55 / 135 / 190 | 0.65 (554/1,762) | 1,168 MiB |
| **1k median (spread)** | | 0.04 % (0.04–0.89) | | 48 / 4,225 (2,053–12,434) / 9,510 (8,804–38,176) | 111 / 1,023 (540–7,794) / 4,536 | 583 / 1,023 / 1,197 | 53 / 5,717 / 9,116 | | 0.64 | |

All runs: 1.00 sockets per account; clock offset Mac − arch3 −40 to −56 ms.
**Production mix at 500 passes every kind except presence delivery** (p95 1,003 / p99 1,063 ms,
bounded by the 1 s presence-v2 tick; pending Gabriel's decision). 1k keeps up on average (p50s
fine, errors ≤ 0.9 %, 0 mismatches) but fails on p95/p99 tails: snapshot permit wait p90 5–8 s
(bursts: 31 % of periods throttled at 0.64 cores), delivery queue p99 7–11 s. Run-to-run spread
at 1k is ~3–6× in tails (run 1 vs runs 2–3), consistent with feed-backlog's observation.
reducer isolate wait p50 31–32 µs, p99 248–445 ms.

Resume opens: `snapshot.query` is still not recorded on these paths, so use the cache counters.
Fallbacks (an open with a cache entry that still scanned SQL) per reconnect: ov-500 (before
resume-fix) 117 / 514 reconnects = 0.23; rf-500 96 / 1,214 = 0.08; rf-500-prod 42 / 1,269 = 0.03;
reconnect p50 / p99 138 ms / 1.3 s (ov-1k) → 48 / 224 ms. sparseSkips (narrow members on indexed
SQL by design) ~1,700 per run.
## CPU cost model (production mix, resume-fix + feed-backlog + snapshot-raw line, ebb65fdd)

Method (`bench/whagons/cost-model.py`, `bench/whagons/cpu-attribution.py`): one 500-user
production-mix run with a frame-pointer build of the same commit (`rf-500-prod-fp`,
`-C force-frame-pointers=yes`, `PERF_CALLGRAPH=fp`; DWARF unwinding stops after one frame on
these binaries, so earlier "CPU by thread" tables were self time only). Each on-CPU stack is
assigned to the first matching work category (inclusive), scaled to the cgroup's measured
cores over the plateau (0.309 cores). Unit rates come from the `/dev/profile` dumps of the
same plateau (stage counts, per-function invocation counters) and the generator's samples.
Scaling is calibrated on the median 1k production-mix run (rf-1k-prod-2, 0.633 cores, 31 %
of periods throttled, so 1k demand is at or slightly above that).

| work (500 users, production mix) | share | cores | unit rate | cost per unit |
|---|---:|---:|---|---|
| delivery: feed apply per connection (offload pool 81 %) | 28.9 % | 0.089 | 829 deliveries/s from 5.9 changes/s (≈141 receivers per change) | 0.108 ms per delivery |
| delivery: socket encode / compress / write | 8.4 % | 0.026 | same | 0.031 ms per delivery |
| SQLite worker threads (caller not on the stack: snapshot reads, log catch-up) | 18.6 % | 0.058 | 42 opens/s | ≤ 1.4 ms per open if all of it is snapshot reads |
| per-connection socket task (inbound frames, auth, bookkeeping) | 9.6 % | 0.030 | 500 connections | 60 µs/s per connected user |
| reducers / actions (V8 + inline SQL) | 11.2 % | 0.035 | 32.6 calls/s (24.6/s `pushNotifications.deliverScheduled`, 4.3/s user saves/views) | 1.07 ms per call (user saves 0.9–1.6 ms, bridge_bench) |
| snapshot opens (build, cache, visibility; offload 90 %) | 7.4 % | 0.023 | 42 opens/s (login/reconnect 9 collections, detail window 8) | 0.54 ms per open (+ up to 1.4 ms SQLite reads); ≈ 5–17 ms per login or detail window |
| commit / checkpoint / storage | 4.2 % | 0.013 | ~10 writes/s | ≈ 1.3 ms per write incl. checkpoints (checkpoint 14.3 ms/s) |
| scheduled functions (scheduler thread) | 3.9 % | 0.012 | 14 crons, staggered | fixed ≈ 0.012 cores |
| presence (v2 ticks and patches) | 2.4 % | 0.007 | 500 users, transitions ~1/100 s | 15 µs/s per user |
| runtime scheduling / kernel / other | 5.3 % | 0.016 | – | overhead |
| **total** | 100 % | **0.309** | | |

Per user action (500 users, production mix): a save costs ≈ 1.1 ms of V8 + ≈ 1.3 ms of
commit/storage, plus its delivery: **≈ 20 ms per change at 141 receivers** (feed apply +
socket), i.e. delivery is ~90 % of a save's total CPU. A view (detail window, 8 collections)
≈ 4–12 ms; a sign-in or reconnect (9 collections) ≈ 5–17 ms. Background (checkpoints, GC,
crons) ≈ 0.03 cores, roughly constant. GC 1.1 ms/s.

**Scaling 500 → 1k (production mix):** deliveries ×3.67 (≈ N², every change reaches ~2× the
receivers) but CPU per thread only ×1.86 (offload), ×1.97 (network worker), ×1.86 (SQLite),
×3.1 (V8: 0.029 → 0.090 cores; push-notification actions ×1.41, user calls ×1.9; isolate
contention not separated), total ×2.04. Per-tick batching (one frame per connection per tick)
and class-shared decisions keep delivery CPU close to linear in connections up to 1k: per
delivery cost fell from 0.139 ms to 0.071 ms.

### Projection (cores of runtime CPU demand)

| users | production mix (linear from 500→1k) | production mix if delivery turns quadratic past 1k | default mix (linear from rf-500) | dominant line item |
|---:|---:|---:|---:|---|
| 500 | 0.31 (measured) | 0.31 | 0.67 (measured) | prod: delivery (37 %); default: V8 push/notification work (33 %) |
| 1,000 | 0.63–0.65 (measured, 31 % throttled) | 0.65 | ≈ 1.4 | delivery (offload + socket) |
| 2,500 | ≈ 1.6 | ≈ 2.4 | ≈ 3.4 | delivery; then SQLite reads and per-connection socket work |
| 5,000 | ≈ 3.2 | ≈ 6–8 | ≈ 6.7 | delivery (and V8 notification work in the default mix) |

Linear: total(N) = total(1k) × N/1k. Quadratic case: the 1k delivery share (≈ 0.25 cores) ×
(N/1k)², the rest linear. Default mix: rf-500 (0.67 cores, unsaturated: 54 % of periods
throttled in bursts) × N/500; its extra over production is V8 (0.22 vs 0.03 cores:
`pushNotifications.deliverScheduled` 75/s vs 25/s, `notifyIfUnassigned` 0.75/s) and SQLite/other
threads (notification rows written and delivered).

**How far is 5k on 1 vCPU?** With ~30 % headroom for bursts (the 1k runs throttle 31 % of
periods at 0.64 cores average), the budget is ≈ 0.7 cores. Production mix at 5k needs ≈ 3.2
cores at today's per-unit costs (linear) → **≈ 4.5× less CPU per user overall**, of which
delivery (≈ 37 % now, growing) must shrink the most: ≈ 5× per delivered change (feed apply
per connection is the largest single item: 0.108 ms × receivers), SQLite reads per open ≈ 4×,
per-connection socket work ≈ 4×. The default mix additionally needs the notification path
(push-notification actions, unassigned-task fan-out) cut ≈ 7× or moved off the 1 vCPU.
Not measured: a 2.5k run (1k does not pass yet), the quadratic case past 1k (only two points),
a frame-pointer run at 1k, and the per-class vs per-connection split of delivery (no counter).

## Step 14: burst-control 5ca42585 (fast-forward of 165b5a2f)

Merge: fast-forward to 5ca42585 (no conflicts). Tests (`targets/integrate-burst-control`): workspace
**661 passed / 0 failed / 8 ignored**, perf-fixture **414 / 0 / 9** (incl. `tests/burst_control.rs`).
Lab: `compose.new.yaml` now passes `GONVEX_FEED_DURING_ADMISSION` / `GONVEX_FEED_MERGE_LAG` from
`LOAD_FEED_DURING_ADMISSION` / `LOAD_FEED_MERGE_LAG` (defaults 1/1 = code defaults); each case's
`runtime-env` records the values used.

Oracle gate (old app, 100 users): bc-oracle-100 23 checks / 0 mismatched / `unsupported` {} / 0.08 %
errors, heartbeat p95/p99 49/68, save 89/121, delivery 94/119, taskDelivery 119/172 ms; nobatch 23 / 0
/ {} / 0.08 %, heartbeat 43/59, save 109/205, delivery 49/72 ms. Runtime 0.08 cores.

**Contamination.** The first series (bc-1k-prod-1..3, bc-500, bc-1k-prod-fp) overlapped another thread's
CPU-heavy arch3 work (delivery-coverage: `cargo test` full suites 05:29–05:45, clippy and benches
05:45–05:55, a lab build 05:55–05:57): run 1↔tfull1, run 2↔tfull2, run 3 (the collapse: 0.91 cores,
3.7 % errors, 1 visibility check "0 leaked, 236 missing after 15 s" while opens waited 21–25 s)↔tfull3,
bc-500↔bench8, fp run↔lab build. They are kept below but not used for medians. The repeat series
(`qb-*`) waits before each case until no other thread's coach-resources unit is active and the 1-min
load is < 2, and logs host `/proc/stat` (host busy 2.2–2.6 of 14 cores on every qb case: runtime,
proxy, monitor and perf only). Toggles on and off interleaved.

Production mix, 1,000 users (180 s plateau; all 1,000/1,000 connected, 1.00 sockets per account, 0
mismatched). Columns: save, taskDelivery, presence delivery, query = window open, reconnect, p50/p95/p99 ms;
snapshot permit-wait p90; delivery queue-wait p99.

| case | burst control | errors | save | taskDelivery | presence | query | reconnect | cores (throttled) | permit p90 | queue p99 |
|---|---|---:|---|---|---|---|---|---|---:|---:|
| qb-1k-on-1 | on | 0.04 % | 60 / 1,228 / 4,713 | 121 / 1,118 / 1,761 | 620 / 1,174 / 1,615 | 59 / 8,172 / 10,266 | 57 / 187 / 525 | 0.69 (708/1,764) | 9.7 s | 183 ms |
| qb-1k-on-2 | on | 0.04 % | 58 / 1,267 / 4,406 | 122 / 1,206 / 2,283 | 615 / 1,228 / 1,704 | 57 / 9,778 / 12,807 | 54 / 146 / 307 | 0.70 (732/1,772) | 9.3 s | 272 ms |
| qb-1k-on-3 | on | 0.09 % | 71 / 1,458 / 7,854 | 131 / 1,271 / 1,967 | 639 / 1,345 / 1,849 | 105 / 10,521 / 15,268 | 60 / 157 / 363 | 0.72 (775/1,765) | 10.0 s | 247 ms |
| **on, median (spread)** | | 0.04 % | 60 / **1,267** (1,228–1,458) / **4,713** (4,406–7,854) | 122 / 1,206 / 1,967 | 620 / 1,228 / 1,704 | 59 / **9,778** (8,172–10,521) / 12,807 | 57 / 157 / 363 | 0.70 | 9.7 s | 247 ms |
| qb-1k-off-1 | off (both 0) | 0.13 % | 69 / 12,963 / 19,600 | 130 / 6,168 / 11,879 | 567 / 1,025 / 1,148 | 106 / 13,876 / 18,046 | 53 / 132 / 192 | 0.70 (744/1,763) | 10.6 s | 17.2 s |
| qb-1k-off-2 | off (both 0) | 0.13 % | 65 / 8,888 / 14,301 | 125 / 2,188 / 7,503 | 553 / 1,028 / 1,118 | 100 / 9,388 / 14,274 | 54 / 143 / 211 | 0.69 (698/1,766) | 9.2 s | 13.9 s |
| bc-1k-prod-nomerge | admission on, merge 0 | 0.22 % | 111 / 6,492 / 18,010 | 154 / 914 / 1,473 | 577 / 1,053 / 1,277 | 3,075 / 14,744 / 25,472 | 55 / 119 / 190 | 0.75 (866/1,772) | 10.1 s | 381 ms |
| bc-1k-prod-off (quiet, before the qb gate) | off | 1.98 % | 10,686 / 48,747 / 70,648 | 3,710 / 25,937 / 40,240 | 610 / 1,086 / 1,434 | 14,430 / 27,356 / 29,475 | 73 / 282 / 689 | 0.93 (1,448/1,759) | 15.3 s | 38.9 s |
| bc-1k-prod-1 (contaminated) | on | 0.17 % | 123 / 1,740 / 9,413 | 166 / 1,378 / 1,897 | 642 / 1,320 / 1,704 | 255 / 12,037 / 17,131 | 56 / 140 / 326 | 0.79 | 14.0 s | 386 ms |
| bc-1k-prod-2 (contaminated) | on | 0.04 % | 82 / 2,333 / 11,721 | 137 / 1,323 / 2,672 | 604 / 1,157 / 1,597 | 102 / 12,418 / 16,938 | 56 / 138 / 207 | 0.76 | 9.5 s | 202 ms |
| bc-1k-prod-3 (contaminated) | on | 3.68 % | 650 / 29,058 / 57,516 | 428 / 2,322 / 2,993 | 673 / 1,610 / 2,399 | 18,949 / 24,555 / 28,021 | 63 / 268 / 519 | 0.91 | 21.0 s | 497 ms |
| rf-1k-prod median (step 13) | – | 0.04 % | 48 / 4,225 / 9,510 | 111 / 1,023 / 4,536 | 591 / 1,023 / 1,197 | 53 / 5,438 / 8,435 | 56 / 184 / 494 | 0.64 | 5.1 s | 7.4 s |

**Burst control does what it says at 1k:** delivery queue-wait p99 14–17 s → 0.18–0.27 s, save p95
9–13 s (off, same binary) → 1.2–1.5 s, p99 14–20 s → 4.4–7.9 s, 0 mismatches, errors ≤ 0.09 %. **1k
still fails:** save p95 1.27 s, window opens p95 8–10 s (snapshot permit wait p90 ≈ 9.7 s, CPU-bound
as expected: 0.70 cores, 40–44 % of periods throttled), and presence delivery p95 rises from ≈ 1.02 s
to 1.17–1.35 s (no longer just the 1 s tick). Not a Coach milestone; 2.5k not attempted.

**Burst control costs CPU: merged feed events lose cross-connection sharing.** Default mix 500, clean
A/B on the same binary (bc-500-off 06:00, bc-500-b 06:04, no other thread's work running):

| case | errors | save | taskDelivery | query | cores (throttled) |
|---|---:|---|---|---|---|
| bc-500-off (both 0) | 0.17 % | 155 / 772 / 1,192 | 195 / 851 / 1,168 | 53 / 317 / 1,390 | **0.65** (770/1,782) |
| bc-500-b (defaults) | 0.17 % | 394 / 1,268 / 1,738 | 494 / 1,316 / 1,910 | 109 / 830 / 1,458 | **0.79** (1,055/1,772) |
| bc-500 (defaults, contaminated) | 0.17 % | 674 / 2,220 / 2,932 | 747 / 1,998 / 2,734 | 206 / 1,341 / 2,118 | 0.90 (1,414/1,771) |
| rf-500 (step 13) | 0.17 % | 142 / 965 / 1,929 | 214 / 875 / 1,799 | 53 / 617 / 3,613 | 0.67 (815/1,775) |

With the toggles on, the runtime spends +0.14 cores (+22 %) on the same load (network threads 0.09
→ 0.21–0.24 cores, offload 0.11 → 0.17–0.23), and every latency is worse at 500. Mechanism (code and
counters):
- `feed_share::EventShares` keys a shared decision set on epoch, revision **and the address of the
  broadcast's change list** (`feed_share.rs:218`). `merge_waiting_feed` builds a new
  `Arc<Vec<change>>` per connection (`change_feed::merge_transactions`), so a merged event never
  finds the shares the other connections built for the same transactions and evaluates visibility
  alone. Shares created / found: bc-500-off 850 / 260,605 (307 hits per share), bc-500-b ≈ 55k / 138k
  (2.5); at 1k quiet: off 22–41k / 1.11–1.13 M (27–50), on 118–135k / 0.78–0.83 M (5.7–7.0).
- The encoded-item memo (`TenantFrames::item`, key revision + kind, variants compared by full
  `ServerMessage` equality, `MAX_VARIANTS` 16) gets one variant per distinct merged range, so lookups
  compare several large messages and then encode privately anyway.
- `merge_waiting_feed` checks mergeability by calling `merge_transactions` on each new pair, which
  clones both change lists and drops the result, then merges the whole set again (O(n²) clones per
  lagging connection).
Frame-pointer profile at 1k (bc-1k-prod-fp; partly overlapped another thread's lab build, so shares
are indicative): feed apply per connection 43 % of 0.80 cores vs 28.9 % at 500 before; inside it
`merge_waiting_feed` 7.4 %, `merge_transactions` 6.6 %, Arc drop / drop glue 4.2 %, clones 4.4 %,
row projection / stable encoding 2.7 % (0.5 % before).
Suggested fix (for burst-control's owner): merge by reference, not by copy. Keep the original
`Arc` change lists of the merged events and apply them through their existing shares (one share per
source event); check mergeability from (epoch, revision) without building a merged event; or key
shares by (epoch, revision range) and publish one canonical merged event per range. The feed during
admission alone (merge 0, `bc-1k-prod-nomerge`) gives a delivery queue p99 of 0.38 s but save p95 6.5 s,
so the merge is what fixes saves at 1k; it needs to stop costing sharing.

Resume: fallbacks per reconnect stay low (`snapshotCache.fallbacks` 64 per 1,237 reconnects in
bc-500-off); reconnect p50 47–60 ms, p99 ≤ 525 ms in every quiet run.

## Step 15: milestone-gate round (conn-idle, delivery-coverage, burst-control r2, nice 10, presence-edge, wakeup-skip): gate not passed yet

Merges on perf/integrate-5k: conn-idle 5f05f94d (813860f5; Cargo.toml bench list), delivery-coverage
7750e394 (a0f3e3a2, clean), burst-control round 2 a12845ab (c834535b; feed-loop conflict:
delivery-coverage's `fan_out_waiting` runs per event inside the by-reference batch loop), nice 10 code
default (88c98b1f), absolute nice + no inheritance (8ec09dfc), presence-edge beb0f8d4 (f98ca1a8,
clean), delivery-coverage 37d95866 wakeup-skip (f6463024; `wake_dirty` added to the round-2 feed arm).
Tests on f6463024: workspace **671 / 0 / 8**, perf-fixture **423 / 0 / 9**; round-15 head c834535b
664 / 0, 417 / 0. Lab: the new-lab compose passes `LOAD_FEED_LAG_MERGE`, `LOAD_FEED_TARGETED_WAKE`,
`LOAD_FEED_WAKE_FLUSH_MS` (code defaults). Every case below held `LAB_RUN_ACTIVE`; host busy cores over
the plateau from `/proc/stat` (last column, of 14; a run is clean at ≤ 3.5; other projects' agents on
this host, e.g. a duckdb C++ build and a `gonvex codegen`, are outside the lock).

**Per-thread nice, gate build at 1k (`ps -L -o tid,ni,comm`, g2-1k-prod-1):** gonvex-offload ×3 ni 10
(none at 19), gonvex-scheduled ×3 ni 5, sqlx-sqlite-worker ×35 ni 10 (readers) and ×27 ni 0 (one
writer per database), gonvex-module-isolate ×2 / watchdog ×2 / reaper ni 0, gonvex-sqlite-checkpoint
ni 0, gonvex-spawner ni 0, tokio-rt-worker ×6 ni 0, V8 platform ×15 ni 0, main ni 0. Absolute nice: the
offload pool and its blocking threads set base + 10 on start (a pool thread started by a pool thread
no longer stacks to 19). Lowering a nice value needs CAP_SYS_NICE (lab and staging containers: CapEff
0, RLIMIT_NICE 0), so base-priority threads are created by a base-priority spawner
(`gonvex_profiler::thread_priority::spawn_at`: isolates, watchdogs, checkpoint, scheduled lane) and
writer pools open on the network runtime; tests `profiler thread_priority` and
`store tests/thread_priority.rs` (a database first opened from a niced runtime: writer worker at
base, reader at base + reader nice).

Oracle gate (round-15 build c834535b, old app, 100 users): batch on 23 checks / 0 mismatched / {} /
0.10 % / PASS; batch off 23 / 0 / {} / 0.08 % but latency FAIL (heartbeat p95 1,053 ms, delivery p95
2.7 s, 0.23 cores vs 0.08 before; permit wait p90 ≈ 1 s at 100 users). Not yet repeated on f6463024.

Production mix (save, taskDelivery, presence, query = window open, reconnect: p50 / p95 / p99 ms;
snapshot permit wait p90; delivery queue p99):

| case | build | errors | mism. | save | taskDelivery | presence | query | reconnect | cores (thr.) | permit p90 | queue p99 | host busy |
|---|---|---:|---:|---|---|---|---|---|---|---:|---:|---:|
| ab-500-prod-prebc2 | a0f3e3a2 (ci + dc + bc r1) | 0.08 % | 0 | 53 / 146 / 394 | 113 / 199 / 447 | 566 / 1,011 / 1,229 | 52 / 106 / 362 | 55 / 116 / 182 | 0.26 (58) | 8 ms | 57 ms | 1.6 |
| r15-500-prod-fp | c834535b fp | **31.1 %** | 0 | 36 / 21,791 / 46,903 | 99 / 22,523 / 42,625 | 588 / 1,098 / 2,841 | 52 / 21,337 / 27,541 | 61 / 292 / 1,408 | 0.52 (683) | 27.6 s | 536 ms | 2.0 |
| g2-500-prod-smoke | f6463024 | 0.08 % | 0 | 65 / 548 / 1,458 | 139 / 609 / 888 | 53 / 285 / 617 | 52 / 615 / 1,579 | 60 / 174 / 257 | 0.35 (150) | 331 ms | 632 ms | 5.4 ✗ |
| g2-1k-prod-1 | f6463024 | 0.05 % | 0 | 121 / 4,591 / 10,945 | 192 / 974 / 1,308 | 144 / 357 / 543 | 118 / 6,486 / 10,127 | 73 / 198 / 325 | 0.62 (558) | 4.07 s | 428 ms | 4.0 ✗ |
| g2-1k-prod-default-b | f6463024 | 0.05 % | 0 | 61 / **540 / 1,133** | 139 / 550 / 858 | 121 / **304 / 378** | 53 / 378 / 1,315 | 77 / 202 / 289 | 0.54 (338) | 804 ms | 436 ms | 3.0 |
| g2-1k-prod-flush1000-b | f6463024, flush 1 s | 0.05 % | 0 | 84 / 550 / 1,636 | 161 / 549 / 825 | 136 / 324 / 462 | 100 / 1,364 / 2,270 | 80 / 248 / 450 | 0.53 (365) | 1.89 s | 331 ms | 3.2 |
| g2-1k-prod-fp | f6463024 fp | 0.04 % | 0 | 122 / 1,586 / 9,202 | 194 / 922 / 1,313 | 144 / 349 / 547 | 103 / 6,582 / 9,391 | 83 / 232 / 402 | 0.56 (448) | 3.90 s | 395 ms | 3.3 |
| g2-1k-prod-nowake | f6463024, targeted wake 0 | **45.5 %** | **2** | 461 / 75,875 / 97,379 | 489 / 34,868 / 50,373 | 194 / 2,099 / 6,146 | 282 / 21,728 / 27,841 | 287 / 836 / 2,830 | 0.89 (1,419) | 25.7 s | 31.7 s | 3.7 |

- **Presence is fixed**: p95 285–357 ms, p99 378–547 ms at 1k (was ≈ 1.0–1.3 s).
- **Watermark lag of targeted wakeups does not inflate the metrics**: flush 1 s is no better than the
  5 s default (save 550 / 1,636 vs 540 / 1,133; query worse), so the default stays.
- **Without targeted wakeups the build spirals at 1k** (snapshot admission sheds 1,455 builds, delivery
  queue 30–50 s, 2 visibility mismatches "missing" while opens time out): the overload spiral
  delivery-coverage saw once at 500. With them, no spiral in 5 runs.
- The best clean run misses the gate on save p95/p99 (540 / 1,133), taskDelivery p95 (550) and
  query p99 (1,315); the others fail on window opens (p95 6.5 s) and saves behind them.

**Binding constraint: snapshot admission grants slots to futures nobody polls.** Permit hold × rate
gives slot utilization 16–30 % at 1k (default-b 16 %, fp 24 %, run 1 30 %; burst-control round 1
qb-1k-on-2 30 %), yet permit wait p90 is 0.8–4 s, with CPU at 0.54–0.62 cores and 19–31 % of periods
throttled. `SnapshotAdmission::release` hands a freed slot to the head waiter's oneshot and counts it
as used at once; the waiter turns it into a build only when its future is polled. That future lives in
the connection's `select!` loop (`replica_opens`), which does not poll it while the loop body awaits
feed work (`fan_out_waiting` on another connection's shape claim, a 64-event by-reference batch
apply, a socket send). `permit_hold` starts after the poll, so the granted-but-idle time shows only
as everyone else's permit wait. Saves follow: a client does not accept a watermark while one of its
collections opens (SDK rule), so save tails track open tails (fp run: query p99 9.4 s, save p99
9.2 s). Fix direction: run admission + build in a spawned task (independent of the connection loop),
or grant on poll (hand over only to a waiter that is being polled, else to the next one).

Secondary costs (frame-pointer attribution, g2-1k-prod-fp, 0.56 cores; `cpu-attribution.py`):
feed apply 18.2 % (shared fan-out path on the network threads 8.1 %), SQLite workers 17.1 % (statement
execution 7.4 %, **statement prepare 4.3 %**; 10.3 % in the spiralling r15 run: reader connections
close after 15 s idle and reopen with a cold statement cache), socket write 15.7 % (kernel send 9.6 %),
V8 15.4 % (SQL host calls 9.5 %, JS 4.9 %), snapshot open 12.1 % (visibility/ACL 8.3 %), inbound
8.6 %, commit/fsync 4.1 %, scheduler 3.2 %, presence 2.1 %; kernel 31 % overall. Reducer isolate wait
p99 0.5–1.0 s: `pushNotifications.deliverScheduled` runs at 36/s against ≈ 8 user reducer calls/s on
the module host.

## Step 16 (in progress, paused for the arch3 reboot 2026-11-04 ~15:40): gate6 on 7d01af5b

Merges since step 15 (96c408c0): busy-database budget-close guard 94eec46f (my droppable commit,
since folded into staging-findings), staging-findings 001678f4 → ef9bc21d → 636e3c45 (reader idle
120 s, single-flight reader opens, snapshot cache keyed by content, replica-open counters by reason),
admission-fix a47512f7 (snapshot slots claimed when the waiter runs), ci-fixes f9aec228 (rustfmt,
clippy, deterministic tests). Head **7d01af5b**. Tests on 7d01af5b: workspace 677 / 0 / 8;
perf-fixture 427 / **1** / 9: `live_query::tests::exact_count_parameter_type_changes_match_rows_on_one_connection`
("the row and count statements must both remain cached") failed once in the full suite and passed
5 / 5 alone and 22 / 22 with its module: order-dependent (process-wide SQLite state), not a gate
blocker; queued. rustfmt drift after the ci-fixes merge in files staging-findings changed later
(open_reasons.rs, replica.rs, snapshot_cache.rs, store reader_warm_bench.rs, reader_open_single_flight.rs)
is left to the CI thread.

**Real-app page loads (lab, Whagons web client at the lab pin 7d97617cf, load-user-0, idle lab).**
Vite (e2e mode) + headless Chromium against the lab runtime through the proxy sockets; sent
`replica.openMany` frames counted, runtime cgroup CPU and /dev/profile read around each phase
(scripts in `/home/gabriel/work/gonvex-perf/app-opens/`):

| phase | replica opens | runtime CPU | snapshot.build (count / Σ ms) |
|---|---:|---:|---|
| first visit (sign-in, /welcome, /workspace/all), ×3 | 177–178 | 1.49–1.52 s | 177–178 / 726–1,580 ms |
| warm reload of /workspace/all, ×3 | 130 | 0.72–0.79 s | 130 / 1,278–1,337 ms |

126 of the 130 reload opens are argument-less whole-collection `__local.*` collections; 4 take
one argument (userId = member id, workspaceId 'all', associableType 'task'); 89 of 130 carry a
resume cursor. The lab thin and SDK users open 9 collections at sign-in (8 metadata +
recentWorkspaceTasks) plus ≈ 8 per detail window: **the gate so far modelled ≈ 1/15 of a real
page load**. `LOAD_APP_OPENS=app-opens-7d97617cf.json` (51b419b3) makes every generator session
open the captured set (deduplicated with the metadata ones) at sign-in and reconnect (cold, no
cursors: conservative vs a warm reload). One page load per user per hour adds ≈ 36 opens/s and
≈ 0.21 cores at 1k (warm-reload cost). The lab tenants had an empty control-plane `domain`; the
browser run set `synthetic-0` on the live lab control DB only (reset from the baseline at each case).
Lab readers per tenant file: `GONVEX_DB_MAX_OPEN_CONNS` unset → `available_parallelism` (1 under the
1-CPU quota) clamped to 2; staging's 16 lets 16 readers per tenant open in a burst (2.9 MB schema
parse each). Recommendation: unset on staging (2 on 1 vCPU), with single-flight opens bounding bursts.

Lab changes: runtime pinned to one physical core (`cpuset 7,15`, quota 1 CPU), proxy on 4–6,12–14
(`compose.new.yaml`, `LOAD_RUNTIME_CPUSET` / `LOAD_PROXY_CPUSET`); each case records its cpuset;
host busy is scaled to 16 logical CPUs (earlier step-15 numbers used 14: ≈ 12 % low). `LAB_RUN_ACTIVE`
is held per case and only the holder removes it.

Gate6 status: builds (normal + frame-pointer, `lab-5k/bin-gate6`, `bin-gate6-fp`) and tests done;
**no gate case run yet** (host load 20–66 from other projects for most of the afternoon). Planned
series (`lab-5k-new/chain-gate6m.sh`): 100-user app-set smoke → oracle on/off → production 1k with
`LOAD_APP_OPENS` until 3 clean runs (host busy ≤ 4.0/16) → :00 window with the app set.

### Step 16 results (after the reboot): gate6 on 7d01af5b with the real app collection set: FAIL

Lab back up cleanly after the reboot (base services of both labs restarted; no stale lock). The
quiet-host gate now judges contamination by foreign CPU on the runtime's pinned core (cpus 7,15)
instead of whole-host load, since other projects keep the host at load 4–10.

| case | users | errors | oracle | result |
|---|---:|---:|---|---|
| g6-100-app-smoke (app set) | 100/100 | 1.29 % (3 known addMemberTeam) | 0 mismatched | save p95/p99 966 / 6,348 ms, 0.38 cores (thin 100 users: ≈ 0.08), host busy 7/16 |
| g6-oracle-100 (thin) | 100/100 | 0.10 % | 23 / 0 / {} | PASS |
| g6-oracle-100-nobatch (thin) | 100/100 | 0.10 % | 23 / 0 / {} | latency FAIL (as on c834535b) |
| **g6-1k-app-1 (production, app set)** | **758/1,000** | **100 %** | – | **collapse during the 90 s sign-in ramp**; series stopped |

g6-1k-app-1, ramp (the runtime stopped answering /dev/profile before the plateau; deltas over the
85 s of ramp): snapshot builds admitted 441/s with slot hold 4.1 ms mean (**≈ 92 % slot use**, the
admission fix works: grant lag p99 0.19 ms, budget wait ≈ 0), permit wait p50 1.4 s / p99 10.5 s,
waiting builds at the 4,096 cap (shed: "too many waiting builds" and the 30 s deadline), offload
runtime holding 4,101 tasks; runtime CPU 0.98–1.00 cores from t+8 s; anon 645 → ≈ 1,900 MiB in
80 s (memory.events max 56,408, no OOM kill seen); 1.10 sockets per account (transport retries).
Demand: 1,000 users × ≈ 140 opens in a 90 s ramp ≈ 1,550 opens/s vs ≈ 440/s capacity on 1 vCPU.

**Reopen churn (app set, 100 users):** `spots.list` reopened 2,373 times in 1.7 min (14.9 per socket
per minute), every one after a server `needHashes` with reason `needHashesPrincipal`
(`replica_mask.rs` resync) answered by a full-integrity reopen: the same pattern as staging's
listMembers reopens. `spots` visibility reads `members.see_all_spots` (members is a permission
table), a likely trigger; not traced.

**Generator gap:** the thin client does not retry `replica.error` (the SDK retries with 250 ms ×
2ⁿ backoff up to 5 s, `packages/client/src/index.ts` scheduleReplicaRetry), so a shed subscription
stays failed; the 1.05 M error count is error notifications, not retries.

### Step 17 preparation: retrying harness, realistic ramp; unchanged gate6 runtime

Runtime baseline remains **7d01af5b**, binary SHA-256
`d8283384ed1df4ddff65c46fed0e31075a8a0978fc9ade608d8cac9d597d5f32`
(`lab-5k/bin-gate6/gonvex-runtime`, identical to Step 16's recorded binary).
Harness retry commit **8f92ee58** adds the SDK failed-open lifecycle plus bounded
80–100% jitter on 250 ms doubling to 5 s, separate failure/retry/recovery counters,
and cleanup tests. No runtime/app/protocol change.

Revised baseline: 1,000 users across **900 s**, then **300 s plateau**; production
mix/skew and the real app collection set; 1 reload/user/hour uniformly phased
(0.278 reload/s at 1k), replacing the prior 30% simultaneous reconnect wave.
Cold snapshots on reload conservatively exceed warm cursor reload cost. Separate
ramp and common-plateau latency/error scopes; all errors retained; repeated watcher
notifications of one error object counted once. Record collections ready at
teardown in addition to authenticated users. Three quiet runs plus :00 plateau,
oracle and leak checks remain required. Runtime-restart storm is NON-GATING.

Lab order authorized by parent: existing heap-growth recovery → short open-cost
window → short reopen window → baseline. Honor current LAB_RUN_ACTIVE owner;
no builds/measurements overlap, no runtime merges during the series. Heap
ba54cee4 tests are unverified; do not integrate it on prior wrapper exit status.

Harness follow-up **e242abe6** covers retry resumption when a listener returns
inside retention. Focused verification: `node --test bench/whagons/*.test.mjs`
**28/28**, `python3 bench/whagons/summary.test.py` **5/5**, and
`node --experimental-strip-types --check bench/whagons/run.ts` pass. Realistic
reloads have no legacy midpoint-wave exemption in capacity connection checks.
Baseline runner prepared at `/mnt/data/work/gonvex-gate6-baseline/case.sh`:
atomic exclusive lock, pre-run pinned-core quiet check, resource/headroom gate,
per-case data under `/mnt/data`, unchanged gate6 binaries. Not launched while
heap holds its recovered reservation (expected end 03:41 UTC).
