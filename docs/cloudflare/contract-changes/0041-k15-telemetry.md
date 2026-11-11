# 0041: Telemetry and errors (K15): sinks, retention and findings

Date: 2026-11-11. Status: **proposed** (K15), for the lead. Numbered 0041: 0039 is K12's (0037) and 0040 is
K21's open PR. Affects: `CONTRACT.md` A.1.7 (`error.envelope`, `telemetry.batch`), A.2 (`error.*`,
`telemetry.*`), C.2 tables 7, 28, 33-35, D.1 "Metrics, logs, errors and caches"; K05, K06, K14, K19, M05.
Code: `cloudflare/kernel/telemetry/`.

The brief's lesson: unbounded telemetry filled real disk on the old server (an 11 GB `telemetry.jsonl`, a
13 GB mutation-log table). So every store below has an explicit age limit or size cap, and the Cloudflare
sinks are used where they serve the purpose better than SQLite rows.

## 1. Sinks: what goes where, and why

| Data | Rust at `0bf3ad13` | Kernel |
| --- | --- | --- |
| Performance events (`telemetry.event`, `telemetry.batch`) | Control Plane SQLite, 14 days, a `DELETE` on every insert | **Analytics Engine is the sink of record**: every accepted event is one data point, written by the gateway (no RPC, no SQLite, the platform's three-month retention). The control object keeps a **bounded SQLite window** (§2) because the Rust-shaped routes need exact rows: the `logs` tail and `functions` of `/dev/metrics`, `/dev/logs/stream` and `DELETE /dev/logs` (de-duplicated by event id, deletable, ordered). |
| Error events and groups | Control Plane SQLite, never pruned | Control object SQLite with retention and caps (§2). Grouping needs an upsert, de-duplication by event id and operator writes (status, priority, assignee); K06's `control.support.listErrors`/`getError` and K19's routes read it. The volume is bounded by the rate limits. |
| Error support sessions (`error.register`) | `gonvex_support_sessions`, one row per connection, never pruned | Same table (K06's), pruned 30 days after last seen unless live (§2). |
| Reducer logs | `gonvex_runtime_reducer_logs` exists, but **nothing writes or reads it** (the P04 doc comment in `reducer_feed.rs` calls it a diagnostic log; `grep` finds no writer) | The kernel writes none either. The table still gets retention, so a future writer cannot grow it without bound. Module `console.*` output (K04's prelude, `[gonvex:module]` prefix, Rust's stdout/stderr) goes to **Workers Logs** (`observability.enabled`), with the platform's retention and daily limits: no store of ours. P04's `_gonvex_reducer_feed` stays with P04/M02 (0015 S0-6). |

**Why not Analytics Engine for everything.** Measured on the platform (`bench/ae-probe/run.mjs`, dataset
`gonvex-cf-dev-k15-telemetry`): 300 rows written in 3 requests of 100 were stored as **26 sampled points**
whose `_sample_interval` sums to exactly 300, queryable after about 50 s (complete within 2 minutes). Totals
are right in aggregate, but small counts are estimates (30 error rows were estimated as 10), ids are not
kept per row, nothing can be deleted, and reads need the SQL API with an account token. That suits
dashboards over large volumes (M05), not the Rust routes. **Why not Workers Logs for events:** a Worker
cannot query it.

**Why the SQLite window is safe.** Performance events are the highest-volume frame in the protocol (one per
call, batched 50 per 5 s per socket). Without a bound, the control object, which also serves every sign-in
and revalidation, would take one RPC and one transaction per batch per socket. Each gateway therefore
forwards at most `GONVEX_TELEMETRY_STORE_PER_MINUTE` (600) rows per project per minute; with 8 gateways that
is at most 80 rows/s into the control object, and Analytics Engine still gets every event. Below that rate
(development, conformance, small tenants) the routes return exactly what Rust returns. Above it, the routes'
`functions` counts and `logs` tail cover a sample, until M05 reads `functions` from Analytics Engine.

## 2. Retention and caps

| Table | Age | Per-project cap | Where |
| --- | --- | --- | --- |
| `gonvex_performance_events` | 14 days (as Rust) | 100,000 rows | each write deletes one page of expired rows (Rust deletes all, unbounded); the cap is checked every 1,000 inserted rows; hourly prune |
| `gonvex_error_events` | 30 days | 20,000 rows | cap checked every 200 new events; hourly |
| `gonvex_error_groups` | 90 days after last seen | 5,000 groups | hourly |
| `gonvex_runtime_reducer_logs` | 14 days | 10,000 rows | hourly |
| `gonvex_support_sessions` | 30 days after last seen, never while the connection is live | none | hourly |
| A group's impact maps (`tenants`, `releases`, `environments`, `accounts`, `devices`) | — | 1,000 keys each | each write |

The hourly prune is the control object's existing alarm (K06's idempotency prune, K19's multiplexing). Deletes
run in pages of 500 rows with a yield between pages, at most 40 pages per table per run; the oldest rows go
first. Two indexes are added for it (`gonvex_cf_error_events_age`, `gonvex_cf_error_groups_age`); the Rust
tables and columns are unchanged.

Worst case at the caps: performance rows about 1 KB each with trace and device (≈100 MB per project), error
events at most 32 KiB each (≤ 640 MB per project, typically 2-5 KB each, ≈ 100 MB), groups at most 32 KiB of
latest event plus capped maps. The 10 GB object limit holds for well over ten busy projects.

Observable effects, all on data older than the windows: an error group not seen for 90 days disappears (its
next event starts a new group, not a regression); the release filter of `/dev/errors/groups` lists releases
of retained events only; `control.support.listSessions` no longer lists sessions idle for 30 days.

**The impact-map cap is a deviation for very wide groups.** Rust's maps grow without bound, and a group seen
by tens of thousands of devices would pass the platform's 2 MB row limit and fail every later event of that
group. With the cap, keys already present keep counting, new keys beyond 1,000 are not added, and
`event_count` is exact. A dashboard showing "devices affected" saturates at 1,000.

## 3. Rate limits

Rust keeps one limiter per process: 120 units per account, else 20 per connection, per project, per 60 s
window, shared by `error.*` frames and valid performance events. The kernel keeps it in the object that owns
the socket (0012 R11), so an account with sockets on several gateways gets the budget once per gateway, and a
gateway that hibernates or restarts starts with fresh windows (as a restarted Rust process does).

## 4. Where the work runs

The gateway validates, rate-limits, scrubs, fingerprints and computes event ids; the control object only
stores. One RPC per `error.envelope` (it needs an ack), one per new or changed `error.register`, one per
telemetry frame that has rows inside the forwarding budget. Inside the control object an envelope is one
storage transaction with three statements (insert events, read the groups touched, upsert them), whatever
its size; no statement binds more than three parameters (`json_each` carries the batch).

Rows read / written, measured in the control object (`test/store.spec.ts`):

| Call | Rows read | Rows written |
| --- | --- | --- |
| `recordErrors`, 1 event, new group | 7 | 8 |
| `recordErrors`, 1 event already stored | 3 | 0 |
| `recordErrors`, 4 events in 2 groups | 21 | 21 |
| `recordErrors`, 20 events in 4 groups | 72 | 96 |
| `recordPerformance`, 50 rows (first write of the project: includes the cap check) | 202 | 150 |

## 5. Accepted differences inherent to JavaScript values

Events arrive as parsed JSON, so two details of Rust's `serde_json::Value` (with `preserve_order`) cannot be
reproduced: integer-like keys (`"0"`, `"42"`) are ordered first in a JS object, and a float written with a
zero fraction (`1.0`) is held as `1` (2 bytes shorter in the 32 KiB / 256 KiB checks and in the stored
payload). Every other key order matches, including `swap_remove` when an anonymous connection loses
`account`, `context` and `breadcrumbs`; the parity test compares stored payloads as text.

An envelope is one transaction: if the store fails, none of its events is kept, where Rust keeps those
written before the failure. The answer (`error store unavailable`) is the same.

## 6. Files outside `kernel/telemetry/` (additive)

- K05 (`kernel/session/`): `session.ts` routes the five frames to `SocketTelemetry` **with or without a
  tenant** (Rust handles them for every socket; the stand-in forwarded them to the tenant port only for
  tenant sessions) and keeps the auth frame's `device` (`telemetry_device`, at most 2 KiB) in the record;
  `host.ts` shares one limiter per object; `ports.ts` adds the optional `Ports.telemetry`;
  `placement/gateway.ts` reads `TELEMETRY_EVENTS` and `GONVEX_TELEMETRY_STORE_PER_MINUTE`.
- K14 (`kernel/presence/socket.ts`): the close-time `connectionState(…, null)` also runs when only an error
  session was registered.
- K06 (`control/src/control-object.ts`): three RPC methods, `errorSessionId` in `ConnectionState` (live
  until close, merged with presence's reports, last seen on close), the prune on the hourly alarm.
- Shared, append-only: `package.json` scripts, the root `tsconfig.json` exclude, the conformance registry
  and README.

## 7. Verification

- Conformance scenario `error-telemetry` passes Rust against Rust (deterministic across runs). It needs
  sockets and the operator routes in one backend, which the kernel does not have yet, so
  `src/error-telemetry-golden.mjs` records Rust's run and `kernel/telemetry/test/parity.spec.ts` replays it:
  24 acks, both rate-limit floods (18 for an anonymous connection, 97 for the account after its earlier
  frames) and 15 route responses equal Rust's. Two injected faults (plain removal instead of `swap_remove`;
  Unicode lowercasing in the secret-key check) each fail it.
- Unit tests cover scrubbing, attribution, fingerprints (Rust's values), chrono's RFC 3339 grammar, the 20
  events / 32 KiB / 256 KiB limits at their boundaries, both rate limits and their windows, session
  de-duplication, batches, the sink and the budget, retention and every cap.

## 8. For the lead

1. Confirm Analytics Engine as the sink of record for performance events and the SQLite window (§1).
2. Confirm the support-session prune (§2): it touches K06's table.
3. Confirm the impact-map cap (§2).
4. **Integration, not in K15's paths:** when the gateway's ports move from the K05 stand-in to the real
   control object, set `Ports.telemetry = controlTelemetryPort(() => control)` in `kernel/session/bindings.ts`.
   Until then the deployed kernel answers `error telemetry is unavailable`. Deployments bind `TELEMETRY_EVENTS`
   to `gonvex-cf-dev-<deployment>-telemetry` (the probe showed a prefixed dataset name is accepted).
5. Platform state: the probe Worker `gonvex-cf-dev-k15-ae-probe` was deleted. The dataset
   `gonvex-cf-dev-k15-telemetry` holds the 300 probe rows until the platform's retention expires them;
   the probe created nothing else.
