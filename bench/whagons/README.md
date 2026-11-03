# Whagons local workload

This benchmark uses the Whagons generated references and published Gonvex SDK.
It requires a separate Whagons staging checkout. No application functions belong
in this framework repository.

The supplied trace aggregate covers 3,783 sampled client traces, 13 accounts and
7 tenants. It establishes the observed call mix and approximately 30-second
presence/support heartbeats. It does **not** establish complete concurrency or
unbiased calls per minute. `aggregate.py` reads a private CSV and emits aggregate
counts only. Never check in its input.

Workload assumptions:

- 26 synthetic tenants. Task targets are 9,000, 5,000, 2,000 and 100–280 for the
  other tenants. These are synthetic sizes, not a production export.
- Tenant selection uses observed account-count rank weights 6:5:4:3:2:2:1.
  The remaining tenants have weight zero in the observed profile. Set
  `LOAD_ALL_TENANTS_ACTIVE=true` for an additional stress profile that gives each
  remaining tenant weight one. Do not combine these profiles in a capacity table.
- Presence and support heartbeats run every 30 seconds without overlapping a
  previous heartbeat on the same client.
- Presence transitions use exponential intervals averaging 101 seconds, based
  on 287 observed transitions per 964 beats. These are synthetic idle/active
  transitions; the traces do not identify the underlying browser event.
- Foreground think times interpolate the observed aggregate gap quantiles of
  5.16 / 12.73 / 44.77 / 735.97 seconds at p25 / p50 / p75 / p95. The tail is
  capped at 1,800 seconds and the interpolation is scaled by 1.1, an explicit
  approximation to 0.361 foreground calls per observed heartbeat-person-minute.
  This preserves short bursts and long pauses without copying any trace rows. Selected actions
  are details 72, preferences 29, tag 7, start 6, create 5, transition 4, comment 3.
  External AI, files, push and telephony actions are excluded.
- Every user owns a real SDK connection and normalized replica. The task
  collection remains subscribed, alongside presence, members, team memberships,
  workspaces, statuses, tags and workspace preferences. These tenants fit the
  declared 10,000-row task replica budget. Above that budget the driver needs
  the workspace live-query fallback before larger datasets can be tested.
- `prepare` creates emulator Accounts and test Members before measurement.
  `run` signs in again and selects the tenant through the real session exchange.
  It does not call private setup endpoints during measurement.

Run the stack in an **internal Docker network**, sharing a network namespace so
runtime and emulator can both bind literal loopback addresses. The destination
guard accepts only literal loopback URLs. Use PostgreSQL 16 with
`max_connections=100`. Do not copy a production env file. Supply only synthetic
credentials, `GONVEX_ENV=test`, `FIREBASE_AUTH_EMULATOR_HOST=127.0.0.1:9099`, and a
runtime compiled with `gonvex-runtime/test-firebase-auth-emulator` using the
`local-load` Cargo profile. It inherits release optimizations and retains debug
assertions only to keep the emulator guard available. An unoptimized `dev` build
is useful for functional checks but cannot establish production capacity.
The unsigned-token path deliberately does not exist in release builds.

The recorded local experiment uses 4 CPUs/4 GiB each for PostgreSQL and runtime,
2 CPUs/2 GiB for the SDK generator, 1 CPU/768 MiB for auth, and 0.5 CPU/256 MiB for
Valkey. Runtime CPU includes the V8 child. Valkey is present but current Rust
runtime health reports it unused. The workstation disk and other workloads can
limit this experiment. Do not call its result a production capacity guarantee.

## Driver

Run with an existing `tsx` from the app checkout. Set `WHAGONS_CHECKOUT`,
`LOAD_STATE`, `LOAD_ADMIN_KEY`, `LOAD_URL` and `LOAD_AUTH_URL`. Both URLs must be
literal loopback addresses *inside the isolated network namespace*.

1. `tsx bench/whagons/run.ts setup` deploys the app manifest and creates the 26
   tenant structures using the existing gated fixture Action/internal Reducer.
   It seeds 50 tasks per tenant to stay inside the test Reducer deadline. A dummy
   Microsoft credential key and synthetic Stripe placeholders let scheduled
   Actions return `configured:false` or `not_subscribed` for these tenants. No
   integrations, subscriptions or real integration keys are installed.
2. `python bench/whagons/fill.py STATE` fills the disposable `gvload-pg` databases
   to the target sizes and creates a completed status. This script accepts no
   database endpoint and talks only to the named local container.
3. `LOAD_USERS=100 tsx bench/whagons/run.ts prepare` creates test users and filler
   members. Run `staff.py STATE` afterward to join measured Accounts to canonical
   tenant Members, assign team membership and ordinary task permissions, and
   allow the synthetic members to see all fixture spots. Keep these prerequisites
   outside the measured interval. The Member email column is not an identity key.
4. Restore and verify PostgreSQL `fsync=on`, `synchronous_commit=on`, and run
   `CHECKPOINT` after fixture creation. Keep them enabled during every load step.
5. `LOAD_USERS=26 LOAD_RAMP_SECONDS=60 LOAD_SECONDS=180 LOAD_RESULT=step.json tsx
   bench/whagons/run.ts` runs a staggered sign-in burst followed by the plateau.
6. Run `monitor.py OUTPUT_DIRECTORY` alongside each step. It stops the generator
   if host memory full PSI avg10 exceeds the workstation's existing 5% gate.

Run every build and load through `coach-resources`, using a build directory on
`/mnt/data`. Run `build-headroom` before **each** step and retry refused gates.
Stop only this experiment's containers after the final step. No production or
shared QA stack is a valid target.

## Measurements and qualification

Initial snapshots are measured separately and never counted as delivery of edits
that preceded the subscription. Business saves and automatic heartbeats have
separate latency series; the combined reducer series is also retained.

Latency uses nearest-rank p50/p95/p99. Reducers await authoritative server
responses; local optimistic execution is disabled for timing. Subscription
snapshots and subsequent task publications are separate series. Presence delivery
uses server row timestamps and the sender's start time on the same workstation.
Task delivery correlates observed row timestamps with pending task operations.
Presence delivery alone cannot establish task update delivery capacity.

The monitor keeps one PostgreSQL connection for `pg_stat_database.sessions`
sampling, so sampling itself does not create one new SQL session every second.
Count physical client connections, including its own connection and dedicated
listeners. PostgreSQL background workers are excluded from this series. Derive session churn from differences over wall time.
Collect Docker cgroup `user_usec`, `system_usec`, and memory separately for PG and
runtime. Admission timing measures semaphore admission, including uncontended
attempts, not the later SQL connection establishment time.

A passing capacity point requires the requested users connected, no client errors,
no Postgres connection rejections, nonempty save/task-update samples, p95 saves
below 1,000 ms, and p95 task delivery below 1,000 ms. A memory-pressure stop, missing
samples, generator OOM, incomplete ramp or lost subscription is not a passing
point. Compare identical snapshots and workloads across pre-261, 261 defaults,
and timeout/max/admission experiments. Report the highest passing **tested**
point and first failing point, not an interpolated maximum.

## Reproducing the local stack

The Compose file is workstation-only. It runs locally built Linux binaries with
read-only host loader/library mounts. It publishes no ports. All traffic,
including Firebase sign-in, stays on loopback inside one isolated network
namespace. No credentials or settings from an existing deployment are needed.

Set these paths before invoking Compose:

```sh
export LOAD_LAB_ROOT=/mnt/data/work/gonvex-load-lab
export LOAD_BIN_DIR=$LOAD_LAB_ROOT/candidate-bin
export LOAD_NPM_CACHE=$HOME/.npm
export WHAGONS_CHECKOUT=/absolute/path/to/pinned/whagons-staging
export LOAD_DEPS_ROOT=$(realpath "$WHAGONS_CHECKOUT/node_modules")
mkdir -p "$LOAD_LAB_ROOT/runtime"
```

Use an empty lab directory for a new experiment. The app checkout must have its
matching generated manifest and installed dependencies. Firebase Tools 15.11.0
must already be cached locally; its container uses `npx --offline`. The Firebase
project is `demo-whagons-e2e`. Auth data is exported on clean shutdown for a later
restart with the same synthetic identities.

Build `build.sh` through `coach-resources` with the lab as its build directory.
The recorded two-job optimized build used a 6 GiB request and two CPU shares.
The same script accepts `LOAD_GONVEX_REPO` for the pre-261 checkout and supplies
the identical optimization profile through Cargo configuration. Use separate
binary and Cargo target directories for each revision. Record binary SHA-256
and check that revision-specific metrics are present or absent before measuring.
Stop/recreate the runtime when switching
binary directories or pool settings.

```sh
docker compose -f bench/whagons/compose.yaml up -d --wait
# Every setup/run below must also be wrapped in coach-resources on arch3.
docker compose -f bench/whagons/compose.yaml run --name gvload-generator --rm generator setup
python bench/whagons/fill.py "$LOAD_LAB_ROOT/fixture.json"
LOAD_USERS=100 docker compose -f bench/whagons/compose.yaml run --name gvload-generator --rm generator prepare
python bench/whagons/staff.py "$LOAD_LAB_ROOT/fixture.json"
docker compose -f bench/whagons/compose.yaml run --name gvload-generator --rm generator verify
# Before each measured run, reset task state and verify durability and max_connections.
python bench/whagons/reset.py "$LOAD_LAB_ROOT/fixture.json"
LOAD_LABEL=case-26 LOAD_USERS=26 docker compose -f bench/whagons/compose.yaml run --name gvload-generator --rm generator
python bench/whagons/summarize.py "$LOAD_LAB_ROOT" case-26
# At completion; the bind-mounted synthetic data remains in the lab directory.
docker compose -f bench/whagons/compose.yaml down
```

For automated preflight, runtime recreation, sampler cleanup and aggregation,
use `LOAD_LABEL=case-26 LOAD_USERS=26 bench/whagons/run-case.sh` through
`coach-resources`. Export the same Compose variables first. It refuses to
overwrite a summary and verifies the internal network and owned containers.
Compose recreates the runtime when its settings change; pressure retries with
identical settings retain the warm runtime. Force a runtime recreation after
replacing binaries within an existing binary directory.

Run `monitor.py "$LOAD_LAB_ROOT/case-26"` alongside a manual measured run, with a trap
to stop it afterward. The explicit generator name is required for its pressure
stop. Keep Postgres `fsync` and `synchronous_commit` enabled during measurements.
Optional faster fixture creation with durability disabled must finish with both
restored and a checkpoint before any measured traffic. `LOAD_IDLE_MS`,
`LOAD_MAX_OPEN`, `LOAD_MAX_IDLE` and `LOAD_SHARED` configure the runtime; recreate
only the runtime between settings and await its health before starting users.
