# Telemetry and errors (K15)

Error envelopes (`error.register`, `error.envelope`, `error.heartbeat`), performance events
(`telemetry.event`, `telemetry.batch`), their stores and retention, and the data the error-triage routes
(K19, `/dev/errors/*`, `/dev/metrics`, `/dev/logs`) read. The reference is
`rust/crates/runtime/src/telemetry.rs` and `metrics.rs` with the socket loop's telemetry arms
(`lib.rs:3703-3850`) at `0bf3ad13`. Decisions and measurements are in
`docs/cloudflare/contract-changes/0041-k15-telemetry.md`.

```text
client ──error.* / telemetry.*──▶ gateway: SocketTelemetry (validate, rate-limit, scrub, fingerprint)
                                     │  every accepted performance event ──▶ Analytics Engine (TELEMETRY_EVENTS)
                                     └─ RPC: telemetryRegisterSession / telemetryRecordErrors / telemetryRecordPerformance
                                            ▼
                                  control object: TelemetryStore (gonvex_error_*, gonvex_performance_events, …)
                                            ▲  hourly alarm: retention and caps
                                  K19 routes read the same tables
```

| File | What |
| --- | --- |
| `envelope.ts` | The pure port: limits, `authoritative_event` (attribution from the socket, secret scrubbing, bounds, serde `swap_remove` order), fingerprints and generated event ids, chrono's RFC 3339 parser, `error.ack` bytes, performance-event validation. |
| `limiter.ts` | `TelemetryLimiter`: 60 s windows, forgotten after 120 s; 120 per account, 20 per anonymous connection, per project. |
| `socket.ts` | `SocketTelemetry`, one per socket (K05 `Session`), and the host state shared by an object's sockets: the five frames, error-session de-duplication, the forwarding budget. |
| `store.ts` | `TelemetryStore` in the control object: batched writes (`json_each`, no statement per row), retention and per-project caps (`RETENTION`), the hourly prune. |
| `port.ts` | `TelemetryPort` and the adapter over the control object's RPC. |
| `sink.ts`, `env.ts` | The Analytics Engine sink and its data-point layout; the gateway's configuration (`TELEMETRY_EVENTS`, `GONVEX_TELEMETRY_STORE_PER_MINUTE`). |
| `bench/ae-probe/` | Platform probe: a `gonvex-cf-dev-` dataset accepts the sink's writes; when they become queryable; sampling. |
| `test/` | workerd tests (`vitest run --root kernel/telemetry`): the Rust golden replay, envelope, socket, store. |

## Behaviour

- **Attribution.** Project, tenant, session (connection id) and account come from the socket, never from the
  payload. An anonymous connection (no identity) loses `account`, `context` and `breadcrumbs`. A socket
  without a project gets `error telemetry is unavailable`; so does every socket while the gateway has no
  telemetry port (the K05 control stand-in has no store).
- **Limits, in Rust's order.** Project, rate limit, then 1-20 events, each at most 32 KiB serialized, at most
  256 KiB in total. The rate limit is shared by `error.*` frames (one unit each) and performance events (one
  unit per valid event). Windows live in the object that owns the socket (0012 R11).
- **Scrubbing.** Keys containing `password`, `passwd`, `secret`, `token`, `authorization`, `cookie` or `apikey`
  (ASCII case-folded, `-` and `_` removed) are dropped at every depth; strings keep 4,000 characters, arrays
  50 items, objects their first 100 keys; below depth 8 values become `"[Truncated]"`.
- **Grouping.** Fingerprint = first 16 bytes of SHA-256(`name\nmessage\nculprit`). An envelope is one
  storage transaction: events whose id the project already has (or that repeat an earlier id of the envelope)
  are not new; each group's count, first and last seen, latest event and impact maps change as Rust's
  event-by-event upserts would; a resolved group that gets an event is marked a regression.
- **Error support sessions.** `error.register` / `error.heartbeat` upsert `gonvex_support_sessions` (id = the
  connection id) only for a new session or changed attribution; repeats are answered from the socket's
  record. The session is live until the socket closes (K06 `connectionState`).
- **Performance events.** Every accepted event goes to Analytics Engine when `TELEMETRY_EVENTS` is bound. At
  most `GONVEX_TELEMETRY_STORE_PER_MINUTE` (600) rows per project per minute per gateway also go to the
  control object's SQLite window, which is what `/dev/metrics` (`functions`, `logs`), `/dev/logs/stream` and
  `DELETE /dev/logs` read, exactly as in Rust.

## Retention (`RETENTION` in `store.ts`)

| Table | Age | Per-project cap | Enforced |
| --- | --- | --- | --- |
| `gonvex_performance_events` | 14 days (Rust's) | 100,000 rows | each write (one page), cap checked every 1,000 rows; hourly |
| `gonvex_error_events` | 30 days | 20,000 rows | cap checked every 200 new events; hourly |
| `gonvex_error_groups` | 90 days since last seen | 5,000 groups | hourly |
| `gonvex_runtime_reducer_logs` | 14 days | 10,000 rows | hourly (Rust writes none) |
| `gonvex_support_sessions` | 30 days since last seen, never while live | — | hourly |
| impact maps of a group | — | 1,000 keys each | each write |

Deletes run in pages of 500 rows with a yield between pages, at most 40 pages per table per run.

## Verification

- `test/parity.spec.ts` replays `test/golden/rust-error-telemetry.json` (Rust's run of the conformance
  scenario `error-telemetry`, recorded by `cloudflare/conformance/src/error-telemetry-golden.mjs`) through
  `SocketTelemetry` into the real control object, and compares every `error.ack`, both rate-limit floods and
  every route response with Rust's, including the key order of stored events.
- Re-record the golden after a reference change:
  `node cloudflare/conformance/src/error-telemetry-golden.mjs` (needs the Rust binary, README of the harness).
