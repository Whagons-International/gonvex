# Isolated Whagons load experiment on Apple M4

This experiment runs the Rust runtime and real published Whagons SDK sessions
inside one internal Docker network namespace. It publishes no ports. All app,
Firebase emulator and PostgreSQL traffic uses literal loopback addresses.
Never import production/customer data or target a deployed service.

The measured machine is an Apple M4 ARM64, 10 cores, 24 GB RAM, with Docker
Desktop at 10 CPUs / 14 GiB. Absolute numbers do not represent the 12-core x86
production server. See `results/mac-m4.md` for measured points and provenance.

## Workload and qualification

The aggregate profile contains counts and timing distributions from 3,783
observed client traces, 13 accounts and seven tenants. It does not establish
complete traffic rates or concurrent users. `aggregate.py` emits aggregate data
only. Never commit its private input.

The fixture has 26 synthetic tenant databases, with 9,000/5,000/2,000 tasks in
the largest and 100–280 in the rest. Active user selection weights seven tenants
6:5:4:3:2:2:1. All 26 tenants have scheduled jobs. This is not an all-26-active-user
experiment. `LOAD_ALL_TENANTS_ACTIVE=true` is a separate, unmeasured profile.

Each ordinary member has a real authenticated SDK session and normalized local
replica. Sessions subscribe to `tasks.recentWorkspaceTasks` plus nine metadata
collections. Independent presence/support heartbeats run every 30 seconds,
skipping overlapping requests. Foreground actions follow the aggregate call mix
and interpolated think times. Those timings are workload assumptions, not a
replay of private trace rows. External AI, file, push and telephony calls are
excluded. No integration credentials are installed.

Saves await authoritative responses with local optimistic execution disabled.
Task delivery requires the expected row state. Initial snapshots, presence
delivery, business saves and automatic heartbeats have separate latency series.
Nearest-rank p95 is measured for operations started during the full-user plateau.

A capacity point requires a complete 180-second plateau, all requested accounts
still authenticated, nonempty save/task-delivery samples, both p95 values below
1,000 ms, zero client/runtime errors and zero PostgreSQL connection rejections.
A partial, missing-telemetry, build-overlap, memory-pressure or OOM-stopped case
cannot pass. The monitor uses the workstation's existing 5% full-memory-PSI gate.
Report highest passing and first failing tested points without interpolation.

## Reproduction

Use separate `~/loadtest/gvmac-gonvex` and `gvmac-app` clean source archives.
The app needs its generated manifest and exact SDK pin/patch. The driver checks
the installed SDK version and patch hash. Build images before starting any load:

```sh
export PATH=/opt/homebrew/bin:/usr/local/bin:$PATH
cd ~/loadtest/gvmac-gonvex
docker build -f bench/whagons/Dockerfile.mac -t gvmac-runtime:279 .
docker build -f bench/whagons/Dockerfile.generator -t gvmac-generator:local ~/loadtest
export LOAD_LAB_ROOT=$HOME/loadtest/gvmac-state LOAD_NPM_CACHE=$HOME/loadtest/gvmac-npm
mkdir -p "$LOAD_LAB_ROOT/runtime"
docker compose -f bench/whagons/compose.mac.yaml up -d --wait
# New fixture only. Never repeat setup when resuming existing cases.
docker compose -f bench/whagons/compose.mac.yaml run --rm generator setup
bash bench/whagons/prepare-mac.sh
LOAD_LABEL=unique-case LOAD_USERS=10 LOAD_RAMP_SECONDS=0 \
  LOAD_SHARED=20 LOAD_MAX_OPEN=2 LOAD_MAX_IDLE=2 LOAD_IDLE_MS=60000 \
  bash bench/whagons/run-mac-case.sh
```

Builds inherit release optimization and enable debug assertions solely for the
Firebase emulator guard. AArch64 needs the x86-only `SYS_fork`/`SYS_vfork`
seccomp entries guarded with `cfg(target_arch = "x86_64")`.

The case runner refuses existing labels, recreates only its runtime, checks
resource ownership/internal networking/durability, resets synthetic task state,
and runs a checkpoint before measurement. It preserves existing identities.
Reset does not remove newly created tasks, so counts can grow slightly across
cases. Random think times and scheduled-job phase vary between cases; these are
single plateaus, not repeated statistical capacity estimates.

PG/runtime are capped at 4 CPUs/4 GiB each; the generator at 2 CPUs/2 GiB.
PostgreSQL 16 uses `max_connections=100`, fsync and synchronous_commit enabled.
The persistent sampler includes itself and dedicated listeners in physical SQL
session counts. Application connection churn excludes sampler and health probes.
CPU values are mean cores, memory is peak cgroup memory.current including cache.
Admission mean includes uncontended attempts; max is since runtime startup.
Neither includes later physical SQL connection setup.

Preserve raw state outside Git, then remove only this experiment's resources:

```sh
docker compose -f bench/whagons/compose.mac.yaml down --volumes
```

Never prune or stop unrelated containers. On arch3, any separate heavy build or
install must use `/mnt/data`, `build-headroom`, and `coach-resources`.

Run `node --test bench/whagons/safety.test.mjs` and
`python3 bench/whagons/summary.test.py` for the destination/privacy/qualification
checks. Rust unit tests cover admission counters and idle-timeout configuration.

After measurements stop, `docker build -f bench/whagons/Dockerfile.tests
-t gvmac-tests:verify .` reproduces the focused ARM64 Rust unit checks on the Mac.
Do not build this image during a measured plateau.
