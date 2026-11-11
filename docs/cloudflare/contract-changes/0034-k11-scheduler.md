# 0034: K11 scheduler: how IB2 is stored, deviations, and findings for other packages

Date: 2026-11-11. Status: **proposed by K11**, waiting for the lead. Affects: K11, K03, K04, K07, K12, K19, A01,
P03, M03, R02, R06. Implementation: `cloudflare/kernel/scheduler/` (README "Differences from Rust").

## 1. Dead outbox Actions live in their own table (IB2, E.a.5)

E.a.5 says `_gonvex_action_outbox` "gains `max_attempts` and a `dead` status". The table's DDL is Rust's byte for
byte: K03 installs it inside `SYNC_INFRASTRUCTURE_SQL`, whose digest is the `infrastructure` bootstrap receipt, and
its `CHECK (status IN ('pending','processing'))` cannot be altered without a table rebuild, which would also break
G0-10's "identical `sqlite_schema`".

**Proposal (implemented):** the outbox stays Rust's. A row that dies is moved, in the same transaction, to
`_gonvex_action_dead` (the outbox columns plus `attempts`, `last_error`, `dead_at`). The cap is a scheduler setting
(`maxAttempts`, default 8) rather than a column: nothing sets it per row (`ctx.actions.enqueue` has no option).
Requeue moves the row back with `attempts = 0`. `GET /dev/actions/dead` lists dead outbox Actions and dead jobs
together (`Scheduler.listDead`); `POST /dev/actions/dead/{id}/requeue` is `Scheduler.requeueDead`.

## 2. `gonvex_scheduled_jobs` in the tenant object (0012 R6)

Same columns and indexes without `project_id`/`tenant_id`; `status` gains `dead`; the row gains `last_error`
(the dead listing needs it); two new partial indexes: lease expiry (an alarm must know when an abandoned claim becomes
claimable; Rust polls every 30 s) and dead rows (operator listing).

- **A01:** `gonvex_scheduled_jobs`, `_gonvex_action_dead` and their indexes (`SCHEDULER_OBJECTS` in `sql.ts`) belong in
  `addedObjects()`, so P03's export to a Rust file and M02's comparisons leave them out.
- **K02:** call `installScheduler` at provisioning, then `syncCrons`.
- **M03:** at cutover, a tenant's pending one-shot jobs move from the Control Plane's `gonvex_scheduled_jobs` into its
  object (cron rows need not: `syncCrons` recreates them). Pending `_gonvex.scheduler.enqueue` outbox rows in the
  tenant file are promoted by the kernel as Rust would.

## 3. Deviations to rule on

| # | Rust | Kernel | Why |
|---|---|---|---|
| a | A job whose lease expires is re-run forever (a crash loop re-runs every 120 s) | A job claimed more than `maxAttempts` times (its lease lost each time) ends `dead` ("scheduled run lost its lease N times without finishing") | IB2 forbids endless retries; with R25 a CPU-overrun job would otherwise reset the object every 2 minutes forever |
| b | One outbox drainer per process, delivering one Action at a time across all tenants | One outbox Action in flight per tenant (the next waits for the claim to end or expire) | Contract E.c.1 "one in flight per tenant"; equal or better ordering for every tenant |
| c | `complete_action` deletes by id only | Completion, retry and renewal are guarded by the attempt | S4's exactly-once design: a zombie delivery cannot complete a newer claim |
| d | A claim after the last attempt's lease ran out runs it again | It dies ("Action delivery lost its lease after 8 attempts") | Same cap as a failure |
| e | Wakes every ≤ 30 s and sweeps every tenant's outbox every 30 s; prunes completed jobs every 600 s | The alarm is set to the earliest due work only; pruning runs when the oldest completed job turns 7 days old, at most every 600 s | Idle tenants are not woken (R06). Pruning time is not observable |
| f | One scheduled run per tenant at a time (the lane's tenant lock), including inline Actions | Same, and a handed-off Action (Workflow) holds that turn until it completes or its 120 s lease runs out | Keeps E.b.3 "one run per tenant at a time" with Workflow handoff (0016 G0-8) |
| g | Dead rows: n/a | Dead jobs and dead Actions are kept forever, as 0001 says. A cron that fails every minute adds 1,440 dead rows a day (≈ 0.5 MB/day) | **Lead:** keep forever, or prune dead cron runs after N days? |

## 4. Small differences (no ruling needed unless the lead disagrees)

- Cron expression errors reproduce the `cron` crate's text and caret (checked on 20 invalid expressions).
- Wake keys of args holding floats with 16 or more significant digits can differ from Rust (serde_json parses them
  lossily). Only a wake queued in Rust and merged after cutover could notice.
- `dueWhen` truthiness follows the value's storage class; Rust follows sqlx's type name, which uses a plain column's
  declared type (`DATETIME`/`NUMERIC` columns are always truthy when non-NULL there). No Whagons cron has `dueWhen`.

## 5. Findings for other packages

1. **K04 stores the wrong provenance shape in outbox rows (affects K07, blocks the hop limit).** Rust stores the full
   `InvocationProvenance` (`serde_json::to_value`, with `artifactHash`, `channel` as an enum string, `durableHops`,
   `actionStack`, …). K04's `DatabaseHost.provenance()` stores eight fields without `artifactHash`, so serde (and K11,
   which parses with serde's rules) refuses it. Rust would then run a Reducer-scheduled job **as the system**, not as
   the Member, and every hop restarts the count, so **a self-rescheduling chain never ends**. K07 must pass the
   invocation's full provenance to the host, as Rust's host does (`host_calls.rs:354`). Covered by K11's test
   "K04's provenance shape falls back to the system actor as in Rust" and the conformance scenario `scheduler`.
2. **E.b.1's "direct insert in the Reducer's transaction".** K04 still writes `_gonvex.scheduler.enqueue` outbox rows
   (Rust's way); K11 promotes them, which keeps imported Rust rows working. To insert the job directly instead, K04/K07
   run `Scheduler.scheduleStatement(...)` through K01's `session.run` and call `scheduler.notify()` after commit.
3. **The tenant object** (whichever package assembles it) wires: `alarm()` → `TenantAlarm.fire()`; after a commit that
   wrote the outbox → `scheduler.notify()`; deploy (K19) → `syncCrons(specs, { tenantRoutes })`; K12 RPC →
   `enqueue`, `renew`, `complete`; operator routes (K19) → `listDead`, `requeueDead`. Other periodic duties (K03 change
   log pruning, K07 receipt expiry, K06 membership projection, K14 presence) register `AlarmSource`s on the same
   `TenantAlarm`.
4. **R06 / R02 baseline.** Over a simulated day the 14 Whagons crons make 6,153 runs and 6,153 alarm events per tenant:
   the stagger gives every cron its own offset, so the four per-minute crons wake the object four times a minute and
   it never hibernates. The scheduler's own bookkeeping writes 9 rows per run (55,377 rows a day, ≈ $1.66 per tenant
   per month). R06's "no per-minute wakes" starts from these numbers.

## Platform follow-ups (credentials vault down, 2026-11-11)

Everything above ran in local workerd. On the platform, still to measure: a real alarm firing a tenant object at
due times over an hour, alarm retry after a reset mid-run (lease recovery), and alarm-event CPU per cron run.
