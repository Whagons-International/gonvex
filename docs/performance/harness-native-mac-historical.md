# Native Mac historical baseline: runtime OOM, no completed 1k capacity point

The independent native Mac generator passed the full-payload timing and cached
checkpoint qualification. The subsequent density-qualified historical 1000-target
case failed at **620.447 seconds**, when the kernel killed the frozen Gonvex
runtime in its exact **2-GiB** child cgroup. No load rerun followed that OOM.
This is a historical failure observation, not a completed 1000-user result,
current-staging acceptance, final expanded-stack pass or 5k capacity claim.

Before the kernel event, 633 users attempted sign-in, 630 authenticated and 526
had recorded full initial readiness. Durable stop receipts contain 635 attempts,
630 authentications and 526 initial readiness observations; two attempts started
after the kernel event. All 1000 user IDs/planned timestamps remain recorded.
Both reached 75-user bursts started entirely inside their original five-second
windows. The later 150-user burst, 300s steady plateau and 1000-user cached deploy
refresh were not reached. Initial `readyAt` is not a claim of final current state.

## Native generator qualification

The verified executable is the existing Node **v26.13.0**, not the Mac's default
v25.4.0. Hardware reports 24 GiB RAM and ten physical cores (four performance,
six efficiency). Eight native workers use 512-MiB V8 heap bounds, an owned
6-GiB sampled RSS guard and a host-pressure stop below 15% available memory.
The job runs as a normal owned one-shot LaunchAgent, with default nice 0 and
`KeepAlive=false`; no other job, CPU policy, stack, container or UI was changed.
No Docker generator, browser or GPU process was started.

The full-payload replay uses all **132** captured shipping collection intents:
338 real synthetic frames, 8,851 tasks and 16,042 total window rows per attempt.
Every worker proves an actual approved SDK cold instance and a fresh cached SDK
instance against the corresponding thin instances. All 132 warm windows must
advertise their correct complete checkpoint cursor/digest, keep the actual rows,
and match the full projected-row oracle. All eight SDK/thin cold and cached
reference comparisons passed, including complete thin checkpoint inventories.

For **150 offered arrivals over the unchanged five seconds**, all 150 completed
all windows and matched the SDK oracle. No start missed the offered window.
Arrival lateness p95/p99 was **2.65/23.56 ms**; full readiness from planned arrival
p95/p99 was **204.60/258.96 ms**. Full independent row comparisons ran after a
global timing barrier. This qualifies this driver history, not application
permission coverage or 1000-user resident/refresh behavior.

An early SSH-launched replay inherited background nice 5 and failed timing.
Exploratory twelve-worker and repeated calibrations also remain preserved; they
are not passing results. `launchctl submit` unexpectedly restarted completed
calibrations. Its owned label and processes were removed, and live jobs use
explicit one-shot plists. The append-only IPC journal preserves three complete
normal-launcher calibrations: two passed, a later one failed. The first passing
run is recovered with exact journal line ranges, full messages and source-log
SHA in `qualified-native-replay.json` / `recovered-native-qualifications.json`.
The later overwritten summary and failed run remain unchanged; no failure was
relabelled as passing. No calibration ran alongside the selected live case.

## Transport and live prerequisites

Sixteen owned SSH reverse forwards expose only loopback Mac ports to Unix sockets
inside the isolated synthetic arch3 namespace: separate runtime/auth forwards
for each of the eight workers. Mac SSH processes therefore do not combine both
routes' descriptor demand in one session. The owned bridge uses a 4096-listener
backlog and a 65,536-descriptor process limit. The selected arch3 unit explicitly
inherits that descriptor limit; CPU/RAM limits remain unchanged. No global
kernel, other-worker cap or shared Mac job setting was changed.

Before the selected case, all eight native density probes held **125 runtime
and 125 auth connections each**, completed their three health-request rounds
and reported zero errors: 2000 simultaneous route connections in total. This
component probe prices/proves forwarding density, not app or authentication SLO.
The application/module prerequisites then passed through these exact forwards:
132/132 cold and cached collections for admin/ordinary identities, actual warm
SDK `__local.userPresence` opens/cursors, fresh SDK full-row comparisons after a
real shipping share and revoke, and removal of the old row. Preflight grant/revoke
convergence was 572/549 ms; it is narrower than full permission acceptance.
All accounts used ordinary password authentication and verified synthetic fixtures.

There was one earlier Mac-driven attempt that became **INVALID_TRANSPORT**:
the first bridge's inherited 1024-descriptor limit exhausted near 490 connections.
Its process died and the initial liveness check failed to notice promptly.
That attempt is preserved separately at `mac-baseline-20261105-onecase` / Mac
`live3`, with all eight durable partials. It makes no runtime-capacity claim.
The bridge limit/liveness checks were fixed, and the explicit density probe caught
a listener-backlog failure before another application series could start.
The selected density-qualified case below is the only application series after
those transport checks passed. Earlier missing frozen SDK dependency files and
an auto-restarting launcher caused preflight failures, not offered 1k app cases.

## Historical SLO and row-oracle failures

| Window | Attempts / late starts | Arrival p95/p99 ms | Auth p95/p99 ms | Full initial readiness / p95/p99 ms |
| --- | --- | ---: | ---: | --- |
| burst-75-a | 75/75; 0 late | 351.00/610.67 | 884.00/951.67 | 75/75; 11714.00/13877.00 |
| burst-75-b | 75/75; 0 late | 26.67/638.67 | 4152.00/4299.67 | 75/75; 77221.33/110135.67 |

Both windows fail the existing p95<500ms/p99<1000ms readiness rules; authentication
also fails p95. Late completions remain attributed to their originating offered
window. Each reached burst's start-window error ledger records zero errors, but
this does not qualify the unreached steady phase or erase later failures.
Across the interrupted mixed ramp, the ledger has **182 error events**, an
observed **6.2543%** error rate, snapshot-admission refusals, module-isolate budget
failures and terminal connection losses. There are **40 unfinished operations**.
The third burst and complete steady/reload SLOs are unqualified.

There are zero reported checkpoint-integrity failures, but **four visibility
oracle mismatches**. One reported **1071 extra rows versus a fresh SDK observer**;
three timed out with zero extra/missing rows at the later diagnostic read.
These are retained mandatory failures. They have not been investigated at matched
revisions, so this report does not claim a confirmed cross-principal data leak
or dismiss the discrepancy as harmless timing. The zero-leak/missing-row gate
did not pass. No explicit steady share/revoke cycles or deploy-refresh events
were reached; preflight proofs do not substitute for them.

All eight actual workers acknowledged fsynced partial results on coordinated
stop. Raw errors, pending operations, sign-ins, artifact pins and samples survive.
Two legacy summary fields need care: partial `notAuthenticated` used the wrong
field name, and the merged steady `errorRate=0` refers to an empty future scope.
Counts here come from actual `authAt` records; no steady success is claimed.
The branch fixes the aborted authentication count for future runs. This metadata
fix was not swapped into the measured run. The old same-workstation timing note
in the raw result is also inapplicable: these were cross-host measurements.

## Runtime memory, CPU and network attribution

Kernel evidence names `gonvex-runtime`, PID 3192249, and the exact owned
`...gonvex-harness-native-mac-density2.service/runtime` cgroup. The victim had
2,083,212 KiB anonymous RSS and 41,332 KiB file RSS. Configured runtime
`memory.max` is 2,147,483,648 bytes; its recorded peak was 2,147,729,408 bytes
(a small charge/kill overshoot, not a raised limit). The lane's peak was
2,778,562,560 bytes, well below its 6-GiB maximum. Aggregate memory events record
one OOM kill. Runtime/V8 affinity was checked at **7,15**, with **100000/100000**
CPU quota; the full arch3 lane was **200000/100000**, swap disabled.

Over the common 616.339s sampled offered interval, runtime/V8 used 422.917 CPU-s
(mean 0.686 cores). Auth used 2.570, bridge 20.572, sampler 2.104, driver/control
5.131 and remaining owned SSH/helper work about 38.047 CPU-s. Total lane work
was 491.340 CPU-s, mean 0.797 cores. The selected failure is therefore not the
old two-CPU generator ceiling or a 6-GiB aggregate OOM.

The final profiler snapshot had **1372 snapshot-permit waiters and two active
snapshots**. Snapshot permit-wait p99 was 14.052s, snapshot build p99 14.064s,
delivery queue-wait p99 535.26ms and offload queue-wait p99 191.40ms. These are
concurrent wall spans, not CPU measurements and must not be added. They establish
admission/delivery backlog alongside the runtime memory failure; allocation
provenance was not measured, so no particular queued object is proven to cause
all retained memory.

The separate Mac generator used 489.169 CPU-s across approximately 625 seconds;
its sampled concurrent RSS peak was **4,552,491,008 bytes**. Individual-worker
peak RSS values sum to 4,637,376,512 bytes; that sum is not a simultaneous peak.
Per-worker event-loop, GC, CPU and cache/hash counters are preserved in partials.
Native compute was separately qualified before load; runtime CPU includes its
own backend/V8 work and cannot be reassigned to the Mac.

Mac→arch3 Tailscale ping had 20/20 replies, min/mean/max 3.109/13.354/87.799ms.
SSH RPC/clock samples and forwarding CPU/byte counters are retained separately;
SSH RPC time is not packet RTT. The coarse clock probe was a separate tool-side
control read and is not included in the lane CPU totals; application/auth/bridge/
forwarding workload remained inside the actual lane caps. Clock-offset intervals intersected approximately
[-60.78,+76.35]ms with the coarse SSH probe. No network delay or clock estimate
was subtracted from reported latency to manufacture a pass. Within-Mac planned
arrival/auth/readiness timings avoid cross-host clock subtraction. Row-timestamp
delivery measurements remain raw cross-host values with that uncertainty.

There is no isolated runtime CPU-per-delivery, completed cached-reload CPU point
or 1000-user steady tenant-size distribution. **No 5k core projection** is made.

## Pins, remaining work and handoff

- Measured harness/framework head: `43ab6950af7d61704a145c1a8d985d02cb5210fe`.
- Runtime source: `2b7e2ff9f6950ea73a4659c284c9204262314574`; binary
  `8367959c2ec47c6480b3fa27baa119d47364ac5c37f9045f8e26b07dfc22abf8`.
- Module-host binary: `a6decd429c53d3fa6df5ff6735eee45c550b42063113935eddb39918eaae4fe3`.
- Shipping app: `37ca8278be4365951816d05c3312abc66df51dd4`; module
  `d39570bbfc685c71f377f30033063f3853e239b37278db7d59b7c7372bdebbd3`.
  Actual stored module hash and full manifest except synthetic routing match the
  declared app; stored manifest SHA is `c1efbf195f9a7d56e1a10a3edc5f437c6dbe20be411f1582a7287fab993296bb`.
- App manifest SHA: `7676b14074cef430d1b74bcf4bc0d3414669f87d5e130e5f7fe7603cb345e353`.
- Required 132-intent fixture SHA: `f6e151af66b445edb796ab3a2c4d28e474426789b5502ff7326829d907e65f62`.
- Approved candidate-2 SDK JS: `60b2673135e0ffd245b2219fb8a18cd2f6add3ac98ccbfa6af9cb6f5ae20eb0a`;
  all 128 composition members remain pinned. Frozen shipping SDK dependencies
  were copied as framework code, not replaced by aliases/stubs or a PG artifact.
- Twenty-six physical-schema hashes, Native Node binary SHA, generated API/
  contract hashes, complete source pins and stopped synthetic database/WAL hashes
  are retained. No pre-case full database byte hash was captured for this new
  clone; its source clone, module/schema/fixture identity and after-case bytes
  are explicit, rather than inventing an initial data hash.
- Artifact manifest SHA: `329486c2a5aa2242a80652a6fe7aa518d0f53a909f2fb202f6d8eaf578b86a3e`.

| Requirement | Status |
| --- | --- |
| Independent native Mac full-payload/cached 132-window model | Qualified replay; actual SDK references and arrival/readiness rules passed |
| Forwarding density and helper process limits | Qualified before selected case; previous failed component attempts kept separate |
| Historical reached 75/5s offered arrivals | Both complete and inside their original windows |
| Historical application SLO, zero mismatched rows, runtime memory | Failed; runtime OOM and retained oracle/SLO failures |
| Third 150 burst, 1000 ready users, 300s steady, 1000 cached refreshes | Not reached/unqualified |
| Current staging SQLite app/module/schema/open pairing | Still blocked; no current-staging claim from shipping fixture |
| Complete permission/provider primitives | Still blocked without a real complete handoff; shipping preflight is narrower |
| Negotiated starter SDK/network capability | Still blocked; no private fixture substitution or new branch consumption |
| 5k capacity/core estimate | Unqualified; no projection |

A next runtime investigation should examine snapshot backlog/retained allocations
and the visibility discrepancy with matched revisions. It requires a separately
reviewed candidate and explicit source pins; this lane did not merge permission,
starter, provider or heap branches into the historical binary. No further series
was started after the runtime OOM. The original arch3-invalid report remains
unchanged and separate.

Final verification passes **57 Node + 5 Python tests**, docs generators/typecheck,
and runtime-import-cycle comparison 0→0. New report/docs and the one-field aborted
counter correction are the only repository changes. No CI, PR, deployment,
production/staging endpoint, browser, other Mac stack or customer-data action was
performed. Only framework/harness code and existing synthetic fixtures went to
the new owned Mac directory. All owned Mac workers/LaunchAgents/forwards and
arch3 runtime/helpers are stopped; owner locks are released at final handoff.

Evidence roots:
`/mnt/data/work/gonvex-harness/mac-native-20261105` and
`/mnt/data/work/gonvex-harness/mac-baseline-20261105-density2`.
The final handoff receipt records the pushed head and report SHA.
