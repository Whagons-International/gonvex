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

Keep the lab directory (`LOAD_LAB_ROOT`) outside the checkout. Set
`LOAD_HEADROOM_CHECK` to a command that takes the lab path and fails when the
host lacks disk or memory headroom; the scripts wait until it passes before each
step. Large cases need `LOAD_SHARDS` (one Node event loop drives about 100
users) and a matching `LOAD_GENERATOR_CPUS`/`LOAD_GENERATOR_MEMORY`.
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
export LOAD_LAB_ROOT=$HOME/work/gonvex-load-lab
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

## PostgreSQL baseline

`results/mac-m4.md` records the PostgreSQL runtime on the same Apple M4 (#283),
measured before the SQLite storage change with the earlier pass rule (zero
errors; save and task-delivery p95 under 1 s). Its Mac runner targeted the
PostgreSQL stack and was replaced by the portable lab above.

## Action mix and fixture (2026-11-03)

Derived from the load-realism audit of 14 days of production client telemetry
(765k events, read-only) and the production-v2 tenant sizes, then made heavier
than production on the expensive paths at Gabriel's direction: editing,
creating, approvals and permission changes. Production peaks near 12
concurrent web clients, so every lab point above that is already a stress
multiple.

**Tenants.** Task counts follow production-v2: 8,850 / 4,038 / 4,010 / 3,273 /
3,073 / 3,043 / 2,178 / 2,130 / 2,005 / 1,866 / 930 / 700, then 14 small
tenants. Synthetic rows carry a ~400-byte description. Users are spread with
the production write skew 39/39/11/6/2/2/1 % over the first seven tenants;
`LOAD_ALL_TENANTS_ACTIVE=true` spreads them evenly over all 26 as a stress
profile.

**Visibility.** `fill.py` adds per tenant 4 parent spots with 3 children
each, teams T1–T3, a second workspace B (teams T2/T3; A has T0/T1) and an
approval category whose rule (`ON_CREATE`) has three user approvers. 30 % of
tasks live in B and 15 % have no spot. `staff.py` makes, per tenant, two
admins (all teams, all spots), three approvers, and members with one or two
teams and one or two parent spots; about one in seven sees all spots.

**Foreground mix** (weights in percent, after the think time measured in
production; heartbeats, support heartbeats and presence transitions run on
their own timers as in the app):

| Group | Actions |
|---|---|
| Reads 25 | task details 15 (subscribes task, notes, logs, views, approvals, findings, assignees, custom fields for a dwell, then unsubscribes; records the view), workspace switch 4, notifications 3, workspace chat 3 |
| Edits 40 | description 8, due date via `tasks.update` 6, rename 4, priority 4, reschedule 4, start 5, status transition 4, tag add/remove 3, spot move 2 |
| Assignments 5 | `taskResources.assignUser` to another member (grants visibility) |
| Creates 20 | task 6 (A or B, with a spot), comment 8, relation 2, workspace chat message 4 |
| Approvals 10 | task in the approval category 6, approver decision 4 |

Not modelled: forms, signatures, findings and uploads need fixture rows or
storage the lab does not seed (forms, storage objects).

**Permission changes.** Each admin makes one change every 2–5 minutes against
an ordinary member in the same generator process: add/remove a spot,
add/remove a team, share a task the member cannot see (half revoked a minute
later), assign it to them, or move one of their tasks to another spot. One
reorganization per run reassigns ~50 tasks to one member and adds them to
another team.

**Visibility oracle.** After every change, a fresh session for the affected
member computes their rows from scratch; their long-lived replica must
converge to exactly that set. `visibility` samples the time from the change to
convergence when the change altered the member's rows. A difference after
15 s (leaked or missing rows) is an error and fails the run regardless of the
error budget. `visibilityChecks` counts checks, convergences, no-op changes and
mismatches.

**Reconnect wave.** About 30 % of users drop and re-open their session within
10 s at the middle of the plateau (`reconnect` measures sign-in to a complete
task replica).

**Measures.** Capacity uses the challenge rule on the steady plateau
(p95 < 500 ms, p99 < 1 s per kind, < 1 % failed operations, every user
connected, no visibility mismatch). `show.py` also reports fixed-load targets
(save and reducer p95 < 50 ms / p99 < 100 ms, delivery p95 < 100 ms, heartbeat
p95 < 20 ms) and the initial task load against a 1 s target. On Docker Desktop
for macOS, `fdatasync` on the state volume measured p50 0.43 ms / p95 1.9 ms,
far below a Linux server's disk, so save latency there understates the cost of
durable commits.

## Thin protocol clients

Ported from `bench/thin-generator` (e15a1ebf) onto this lab's workload.
`LOAD_CLIENT=thin` drives the same `run.ts` workload (tenants, action mix,
timers, permission changes, visibility oracle, reconnect wave, pass rule, raw
samples) through `thin-client.mjs` instead of the full SDK. It sends the SDK's
frames at the same points: auth with the same capabilities, `action.call`,
`reducer.call`, batched `replica.openMany`, `replica.close` after the SDK's
250 ms retention, and re-auth plus full re-open after `membership-changed` or a
dropped connection. Reducer and action promises settle only after the covering
replica or control watermark; watermarks are deferred while a collection
opens; frames that arrive while a snapshot is staging apply after activation.
It skips LocalReplica storage, staging and digest verification, and shares
parsed rows between users (`RowPool`).

Rows: by default both clients use **legacy JSON rows** (`replica.snapshotPage`,
`replica.snapshot`, `replica.transaction`, `replica.delta`; no ACKs). That is
what the Whagons app's SDK (`0.5.2-staging.31` + patch `b75f898a`) negotiates:
it enables native rows only with a staging adapter, which the app does not
configure. `LOAD_NATIVE_ROWS=true` switches thin clients and SDK users (with
in-memory staging, no fsync) to native rows (`replica.snapshotEncodedPage`
with `replica.snapshotAck`, `replica.transactionEncoded`).

Options:

- `LOAD_SDK_USERS=N`: N measured users, spread evenly, keep the real SDK.
- `LOAD_ORACLE_CLIENT=sdk|thin` (default `sdk`): client for visibility-oracle
  sessions.
- `LOAD_OPEN_LOOP=true|false` (default `true` for thin, `false` for sdk): with
  the open loop, foreground actions and heartbeats fire on their planned
  schedule whether or not earlier ones finished, and the first measurement of
  each action counts from its planned start. `LOAD_MAX_INFLIGHT_PER_USER` (16)
  bounds one user's in-flight actions; overflow counts as an error.
- `LOAD_THIN_ACK_DELAY_MS`: delay each native snapshot ACK (slow client storage).
- `LOAD_WIRE_RECORD=/path.jsonl` (`LOAD_WIRE_RECORD_FULL=true` for payloads):
  record every frame either client sends and receives.

Results add `client` (kind, rows, thin counters including `unsupported`, which
must stay empty), `schedule` and `generatorCost` (CPU seconds, RSS samples).

Checks: `node --test thin-client.test.mjs` (protocol unit tests, native and
legacy rows) and `node thin-conformance.ts` in the lab (`LOAD_STATE`,
`LOAD_PREPARED_USERS` = users passed to `prepare`): signs the same admin and
member in through the SDK and the thin client, applies scripted changes from a
third session and compares every collection's keys and projected values plus
the frames each client sent, after every step.

`paths.ts` (run with `node`) connects `LOAD_USERS` thin users with the
standard collections and drives one operation at a fixed Poisson rate
(`LOAD_RATE`/s), timed from each operation's planned start:
`LOAD_SCENARIO=heartbeat` (`presence.beat`), `snapshot` (open and close a
five-collection task detail window) or `edit` (`tasks.rename` in the largest
tenant, plus delivery to every other connected user who sees the task).

Second lab on one host: `LOAD_PREFIX` and `LOAD_COMPOSE_PROJECT` rename the
containers, volume, network and compose project; `LOAD_COMPOSE_OVERRIDE` adds a
compose file (for example host library mounts for host-built binaries).

Corrected gate6 baseline (Gabriel, superseding the 15-minute ramp; runtime commit `7d01af5b`, harness committed separately):
`LOAD_USERS=1000 LOAD_RAMP_SECONDS=90 LOAD_SECONDS=300 LOAD_MIX=production
LOAD_APP_OPENS=app-opens-7d97617cf.json LOAD_RELOADS_PER_USER_HOUR=1`.
The reload rate replaces the legacy 30%-in-10-seconds reconnect stress wave.
Each user has a uniformly phased 1-hour reload period, starting after login:
about 0.278 page loads/s across 1,000 users. Reloads reopen the full captured
collection set cold (conservative relative to the browser's warm cursors).
Gate the common steady plateau separately from the 90-second sign-in burst, retain all
ramp errors/latencies, and require every user's collection set current at teardown.
Three quiet runs, a plateau crossing :00, and oracle/leak checks remain required.
A runtime restart with simultaneous reconnects is a separate, non-gating stress case.
A 900-second sign-in ramp is optional supplemental evidence only; it cannot qualify
the release. The gating sign-in burst stays at 1,000 users in 90 seconds.
The sign-in optimization objective is 60–80 CPU-ms/user (prior first visit about
1.5 CPU-s/user); queue bounds alone cannot meet it.

Thin replica errors follow the SDK's per-subscription exponential retry (250 ms,
doubling to 5 seconds), with 80–100% jitter to disperse failures. There is at most
one retry timer per subscription; success resets attempts; release, socket loss,
reauthentication and client close cancel timers. Recovery clears the error only
on `replica.ready`. `replicaErrors`, `replicaRetries`, `replicaRecoveries` and
`replicaRecoveryMs` count protocol failures, retry attempts, successful recoveries
and aggregate time from first failure to recovery. Recognized failures are not
unsupported protocol frames. Repeated watcher notifications of the same error
object count once. Results retain timestamped errors and separate ramp/steady
errors; steady SLO uses errors and operation starts inside the common plateau.
