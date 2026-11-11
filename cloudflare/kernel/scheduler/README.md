# Kernel scheduler (K11)

Crons, `runAfter`, `runAt`, coalescing `wakeAt` and the action outbox drain, all on the tenant object's one alarm,
with leases, backoff, the durable hop limit, a maximum attempt count and a dead state (IB2). Reference:
`rust/crates/runtime/src/scheduler.rs`, `scheduled_lane.rs`, `execution.rs` (outbox drain), `store/src/lib.rs`
(outbox SQL); CONTRACT E.b, E.c.1, E.timers; notes 0001 (IB2), 0012 R6, 0016 G0-8/G0-9, and 0030 (this package).

```ts
import { Scheduler, TenantAlarm, storageAlarm, storageDueWhen, parseCronSpecs } from "../kernel/scheduler/index.ts";

// In the tenant object's constructor:
const alarm = new TenantAlarm(storageAlarm(ctx.storage));
const scheduler = new Scheduler({
  storage: ctx.storage, project, tenant,
  module: activeGeneration,        // kindOf(path), crons(), artifactHash()      (K04 generations / K19)
  reducers: reducerPath,           // runReducer(run): in-object, lane "scheduled" (K07)
  actions: actionRuntime,          // startAction(run): Action Worker or a Workflow step (K12)
  dueWhen: storageDueWhen(tenantStorage),                                      // K01
});
scheduler.attach(alarm);           // other packages register their own sources on the same alarm

async alarm() { await alarm.fire(); }                       // the object's alarm() handler
await scheduler.notify();                                    // after a commit that wrote the outbox
await scheduler.syncCrons(parseCronSpecs(manifest.module.crons), { tenantRoutes }); // deploy, provision, import (K19, K02, M03)
await scheduler.enqueue({ function, args, runAtUnixMs, provenance, coalesce });      // ctx.scheduler from an Action (K12 RPC)
scheduler.renew(claim); await scheduler.complete(claim, { ok: true });               // a handed-off Action reports back (K12)
scheduler.listDead({ limit, cursor }); await scheduler.requeueDead(id);              // GET /dev/actions/dead, POST …/requeue (K19)
```

| File | What | Rust |
| --- | --- | --- |
| `cron.ts` | Cron expressions with the `cron` 0.15 crate's grammar and semantics (UTC, seconds field, optional year, Sunday = 1, day-of-month AND day-of-week, shorthands, names, `?`, steps) and its error text with the caret | `cron` crate `parsing.rs`, `schedule.rs` |
| `timing.ts` | Stagger key, staggered next run, `next_cron_run` (missed runs skipped), cron job ids, wake keys, outbox backoff | `scheduler.rs`, `store/src/lib.rs` `retry_delay_ms` |
| `specs.ts` | `module.crons` validation with Rust's texts (`dueWhen` admitted as one read statement through K01) | `modules.rs` |
| `provenance.ts` | Stored `InvocationProvenance` parsing (serde's rules), the durable hop, `direct_provenance` | `module-runtime/src/lib.rs`, `execution.rs` |
| `sql.ts` | `gonvex_scheduled_jobs` in the tenant object, `_gonvex_action_dead`, the counted SQL surface | `control_schema.sql:261-301` |
| `jobs.ts`, `outbox.ts` | Job and outbox SQL: coalescing insert, claim, lease renewal, finish, retention, promotion, retry, dead | `scheduler.rs`, `store/src/lib.rs` |
| `scheduler.ts` | `Scheduler`: the three alarm sources, execution, handoff callbacks, cron sync, operator calls | `scheduler.rs`, `execution.rs` |
| `alarm.ts` | `TenantAlarm`: one alarm at the minimum of every source's next due time | E.timers |
| `due-when.ts` | `dueWhen` on a K01 Query invocation | `scheduler.rs` `due_check` |
| `ports.ts` | The interfaces to K04/K19, K07 and K12 | |

## Behavior

- **One alarm.** Every duty is an `AlarmSource` with `nextAt(now)` (an index probe) and `run(now, budget)`. The
  alarm is the minimum over the sources, so a tenant with nothing due is never woken (Rust polls every 30 s and
  sweeps every tenant's outbox every 30 s). `arm()` only moves the alarm earlier; a later alarm fires, finds nothing
  and re-arms. A source that throws backs off (1 s doubling to 60 s) without stopping the others, and a source that
  claims to be due but does nothing cannot spin the object. Sources run in order: outbox (promotions first), jobs,
  retention, so a `runAfter(0)` promoted in an event runs in the same event.
- **Jobs** (`gonvex_scheduled_jobs`, moved from the Control Plane, 0012 R6). Claim order, the 120 s lease and the
  claim token are Rust's. The alarm claims and runs one job at a time (Rust: one scheduled run per tenant at a time);
  a handed-off Action holds its claim and the next job waits for it, or for its lease to run out. `finish` ends the
  run `completed`, or `dead` with `last_error` when it failed (IB2: still one attempt, no retries), and inserts the
  cron's next run in the same transaction. A job whose lease was lost `maxAttempts` times (the object reset under it)
  dies instead of looping (0030 §3). Completed jobs are deleted after 7 days in batches of 5,000 (at most 20 per
  pass, at most every 600 s); dead jobs are kept.
- **Crons.** `syncCrons` replaces every pending cron run with each cron's next run (Rust's `sync_project`), in one
  transaction; Rust's texts for an invalid expression and for project-scope crons on a project without exactly one
  tenant route. Stagger, interval phase, expression offset, job ids, the one-pending-run-per-cron index and
  missed-run skipping are Rust's. `dueWhen` runs as a read-only K01 Query with `$1` = now (Rust binds `Utc::now()`);
  a falsy first column skips the run (it still completes and queues the next), an error fails open.
- **`runAfter` / `runAt` / `wakeAt`.** From a Reducer, Rust and K04 write an outbox row under
  `_gonvex.scheduler.enqueue`; the outbox source promotes up to 100 due rows per event in one transaction
  (one INSERT over `json_each` and one DELETE). `wakeAt` merges into the unclaimed pending wake with the same wake
  key (earlier `run_at` wins); a claimed wake leaves the unique index, so a wake during the run queues one more.
  Replaying a promotion of an existing job id is a no-op. `scheduleStatement` gives the same insert as a statement
  for the Reducer's own transaction (E.b.1's "direct insert"); `enqueue` is the Action path (a direct insert).
  An invalid payload or an unregistered target backs off like any outbox row and dies at the attempt cap.
- **Outbox Actions.** Oldest `available_at, created_at` first, one in flight per tenant, a 300 s lease. A claim
  increments `attempts`. Success deletes the row; failure backs off `1000·2^(attempts-1)` ms capped at 15 min;
  the attempt that reaches `maxAttempts` (default 8) moves the row to `_gonvex_action_dead`, as does a claim after
  the last allowed attempt lost its lease and the hop guard (Rust deletes the row). Renew and complete are guarded
  by the attempt, so a zombie delivery cannot touch a newer claim (S4).
- **Provenance and actors.** Jobs run with the stored provenance one durable hop on (`parentCommandId` ←
  `commandId`, `commandId` = job id, channel `scheduler`, depth 0); stored provenance without a root (cron runs)
  becomes `direct_provenance` of the system session. The actor is the stored `actorAccountId` unless it is empty or
  `_gonvex_system`; the runner re-resolves the Member (Rust's `tenant_session_for_account`). Outbox Actions get
  `commandId` = `outbox-{id}` and channel `system`; actors starting with `_gonvex_` run as the system session. A
  `direct` run for an account actor is completed by the runner with `completeDirect` once it has the session.
- **Handoff (K12).** `startAction` returns `{ok}` for a short Action or `{accepted: true}` when it continues in a
  Workflow step; then the step renews the claim at least every `leaseMs / 2` (`renew`) and reports with `complete`.
  A lost claim makes both return false. Queues are not used (0016 G0-8).
- **Operators.** `listDead` merges dead jobs and dead Actions, newest first, with a cursor; `requeueDead` resets the
  attempts and makes the row pending again (a dead cron run comes back once as a one-shot; the cron already has its
  next run).

## Differences from Rust

Intended (IB2) and recorded in note 0030: the dead state, the attempt cap, the dead-actions table, the lease-lost
cap on jobs, one outbox Action in flight per tenant instead of one drainer per process, attempt-guarded completion,
alarm-driven wakes instead of 30 s polling, retention timing. Also:

1. **Cron error text** is the crate's (`cron "x" has invalid expression: <input>\n<caret>\n<message>`), checked on 20
   invalid expressions; an exotic input could place the caret differently, and only `/dev/sync`'s 422 text shows it.
2. **Wake keys** of args holding floats with 16 or more significant digits can differ: serde_json 1.0.151 parses them
   lossily (`1000000000000000.5` reads as `…0.4`). It matters only for a wake queued in Rust and merged after cutover.
3. **`dueWhen` truthiness** is by the value's storage class. Rust reads the type name sqlx reports, which follows a
   plain column's declared type: a non-NULL `DATETIME`/`DATE`/`TIME`/`NUMERIC` column is always truthy there, and a
   value whose class does not decode as its declared type is falsy. No Whagons cron has a `dueWhen`.

## Verification

`pnpm test` in `cloudflare/` runs `test/*.test.ts` in workerd with real Durable Object SQLite.

- **`test/whagons-day.test.ts` (the `done_when`).** The 14 tenant crons of whagons5-client `2457bb65f`
  (`test/fixtures/whagons-crons.json`, 12 Reducers and 2 Actions) run for 24 simulated hours on the tenant alarm,
  for two tenants (one starting at midnight, one at 13:17:42.250). Every run must happen at exactly the time the Rust
  runtime schedules it, with Rust's job id, through the right runner, and nothing else may run; each cron's next run
  is pending at Rust's time afterwards. Expected times and ids come from `verify/rust-vectors`, which replays
  `sync_project` and `finish` with the Rust functions themselves.
- **`test/parity.test.ts`.** `verify/rust-vectors` compiles `scheduler.rs`'s functions unchanged against `cron`
  0.15.0, `chrono` 0.4.45, `sha2` 0.10.9 and `serde_json` 1.0.151 (the versions in `rust/Cargo.lock`) and prints
  `test/golden/rust-vectors.json`: 56 expressions (parse result and error text), 4 next occurrences and the latest
  occurrence at or before for 14 instants each, 260 staggered/unstaggered/late/narrow-spread next runs, stagger
  keys, job ids, wake keys and the backoff. Regenerate with
  `cargo run --release --manifest-path verify/rust-vectors/Cargo.toml > test/golden/rust-vectors.json`.
- **`test/scheduler.test.ts`.** Promotion and provenance; coalescing, the mid-run wake and replay; invalid rows and
  K04's provenance shape; dead jobs, hops, unregistered functions, dead cron runs and requeue; lost leases and the
  lease-lost cap; `dueWhen` false/true/error and `$1`; delivery order, the backoff sequence, the attempt cap, the
  dead table, listing and requeue; accepted Actions, renewals, lease expiry and zombies; cron sync and Rust's errors;
  missed runs; retention; the real object alarm; a throwing source; query plans (no scan of the job history).
- **Conformance.** Scenario `scheduler` (`cloudflare/conformance/scenarios/scheduler.mjs`, fixture
  `fixtures/app/gonvex/scheduler.ts`): `runAfter(0)`, `runAt(+300 ms)`, three coalescing `wakeAt`, and a
  self-rescheduling Reducer stopped by the hop limit, on the checksummed `board` table. It runs Rust against Rust now
  and on the kernel once the Reducer path (K07) and the tenant object exist.

## Numbers

The Whagons day, local workerd, per tenant (`whagons-day.test.ts` prints them):

| | Per day | Per run |
| --- | --- | --- |
| Cron runs | 6,153 | |
| Alarm events | 6,153 | 1 |
| SQL statements (scheduler only) | 73,836 | 12 |
| Rows read | 252,273 | 41 |
| Rows written | 55,377 | 9 |

Bundle: `kernel/scheduler/index.ts` with what it imports (K01 admission, K03's SHA-256) is 50,848 B minified,
15,844 B gzip (esbuild). Nothing in the Worker entry imports it yet, so `pnpm bundle` does not change.

Every run is its own alarm event: the stagger gives each of the 14 crons its own offset, so the 4 per-minute crons
wake the object 4 times a minute and it never hibernates. That is R06's baseline ("no per-minute wakes"). The rows
written per run are the claim (1 row, the lease index), the finish (status and its partial indexes) and the next
run's insert (row and indexes): about $1.66 per tenant per month at $1 per million rows written. The cost of the
Reducers themselves is K07's.
