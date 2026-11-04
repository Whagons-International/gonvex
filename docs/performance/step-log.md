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
