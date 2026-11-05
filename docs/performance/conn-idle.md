# Per-connection idle cost (perf/conn-idle)

Branch `perf/conn-idle` (from `perf/integrate-5k` 165b5a2f). Step-13 lever B: the step-13 CPU cost
model charged the per-connection socket task ~60 µs/s per connected user (9.6 % of CPU at 500 users,
production mix); at 5k users that alone would be ~0.3 cores.

**Result in one paragraph.** Two per-connection costs scaled badly and are gone.
(1) Every connect, authenticate and disconnect broadcast a `PresenceChanged` event that woke
*every* connection of the project, which is N² wakeups under reconnect churn. Now only the
connections that display connection counts (a live `control.support.listTenants` query, the
dashboard) subscribe to it.
(2) Every authenticated connection revalidated its session on its own 30 s timer with six SQL
statements. Now one process-wide round per 30 s checks all plain sessions with a few set queries.
On a micro-driver with 5,000 authenticated presence-v2 sockets on one CPU, server CPU per
connection fell from 72 to 18.9 µs/s in the lab-like mix (presence transitions every ~101 s per
user plus 0.27 %/s reconnects). That is 0.36 → 0.094 cores, 3.8×, and the cost is now linear in
connections (1k: 17.7 µs/s). Under churn alone it fell 76 → 5.6 µs/s (13.7×); quiet sockets went
10.2 → 2.7 µs/s (3.7×). What remains in the lab-like mix is presence delivery itself: about one
`presence.patch` frame per connection per second, i.e. one `sendto`. That syscall is 53 % of the
remaining CPU, and a third of it is loopback receive work that a real network path does
elsewhere. Presence semantics, Ping/liveness (20 s / 60 s) and the batch tick are unchanged.

## Where an idle connection's CPU went (base 165b5a2f)

Measured with the new driver (below), server pinned to one CPU, production config defaults:

* **Connection-count events woke every connection of the project (N²).** `RuntimeMetrics`
  broadcast `PresenceChanged { project }` on `runtime_events` at every authenticate and every
  disconnect. Every connection task of the project woke, matched the project and called
  `refresh_control_queries`, which then skipped everything unless the connection held a
  `control.support.listTenants` live query (the support console's tenant list with
  `activeSessions`). With reconnects at the lab's rate (≈ 0.27 % of users per second, 1.35/s at
  500 users) that is `2 × r × N` wakeups per second per connection: 5.4/s per connection at 1k
  and 17–18/s at 5k. It was the largest idle cost at 5k: churn alone cost 0.38 cores for 5k sockets.
* **Session revalidation per connection.** Each authenticated socket ran `revalidate_connection`
  every 30 s on its own `interval`: `authenticate_session` = session lookup, `last_seen_at`
  update (on the control writer), tenant route lookup, read transaction with the Member and sync
  clock reads (six statements, each handed to a sqlx worker thread and back, ~10 task polls).
  ≈ 6 µs/s per idle connection: two thirds of the quiet-socket cost.
* Everything else is small or already wake-on-work: the 50 ms batch deadline exists only while a
  batch is pending (`ReplicaBatcher::deadline` is `None` when empty); presence pushes into a slot
  only on a tick with changes; the writer task waits on a `Notify`; the 20 s presence Ping timer
  costs one wake per 20 s (plus the writer and the Pong), ≈ 1 µs/s per connection in total.

## What changed

1. **`PresenceChanged` on its own channel, subscribed only by listeners** (`lib.rs`,
   `metrics.rs` unchanged, `dashboard.rs`). `RuntimeInner::presence_events` carries connection
   count changes. A connection subscribes lazily while it holds a live
   `control.support.listTenants` query and drops the receiver when it no longer does; the
   dashboard's query loops subscribe to both channels. A lagged receiver refreshes once instead of
   dropping the change. `runtime_events` keeps the rare events (Control changed, module reloaded,
   support command).
2. **Batched session revalidation** (`auth_revalidation.rs`, `ControlPlane::valid_session_hashes`,
   `ControlPlane::active_member_accounts`). Plain `gvx_session_` connections (tenant sessions that
   are not anonymous, and control-only sessions) register a watch keyed by (token, project,
   tenant); the loop re-syncs it each iteration (three string compares when unchanged), so a
   re-authentication or a discarded tenant session re-registers or unregisters. One process-wide
   task runs a round every 30 s: per project, one `SELECT ... WHERE token_hash IN json_each(...)`
   per 500 sessions plus one `last_seen_at` update for the valid ones (same 5-minute throttle);
   per tenant, one read transaction listing active Members among the connected accounts. Only
   connections whose session failed are woken (`AuthWatch::revoked`) and get the same
   `auth.error {id: "session-expired", code: "transient"}` as before. Impersonation, anonymous
   public sessions and other token kinds keep the per-connection timer and
   `revalidate_connection`.
3. **No stall-timer registration for writes that complete at once** (`websocket/connection.rs`):
   `watched_write`'s `select!` is `biased` toward the write, so the 1 s stall-check `Sleep` is
   only registered when the write is actually pending.

Semantics kept: the same validity conditions (session not revoked or expired, account enabled,
session project, tenant still resolves, Member active), checked at least every 30 s; a failed
check, including a database error, revokes, as before. Presence Ping 20 s / liveness 60 s and
the batch tick (≤ 50 ms) are unchanged. Differences: a new connection's first revalidation now
happens 0–30 s after it authenticates (was exactly 30 s); revocations still take effect within
30 s plus the round's duration.

## Driver

`benches/conn_idle.rs` (`--features perf-fixture`, harness-less):

    taskset -c 13 cargo bench -p gonvex-runtime --features perf-fixture --bench conn_idle

The runtime runs in the bench process (pinned to one CPU with taskset: one Tokio worker, as in
the 1-vCPU container) with production config defaults (`Config::from_env`). It provisions N
members over 7 tenants with the production skew (39/39/11/6/2/2/1 %) and long-lived session
tokens directly in SQL. A child process (the same binary, `CONN_IDLE_ROLE=client`, on the other
CPUs) opens N sockets, authenticates each with presence v2, sends `presence.set` +
`presence.subscribe` as the thin client does, then only answers Pings, optionally toggling
presence (`CONN_IDLE_TRANSITION_S`, mean seconds per socket; the lab uses 101) and
reconnecting (`CONN_IDLE_RECONNECT_PER_S`, total). After a warm-up it measures the server
process's CPU (`getrusage`) over 60 s (3 Pings and 2 revalidation rounds per socket), per-thread
CPU, and the profiler's per-task poll counts. Scenarios: **quiet** (no transitions, no
reconnects), **presence** (101 s transitions), **churn** (reconnects at 0.27 %/s of N), **lab**
(both).

## Results

A/B interleaved (base binary, then after binary, same conditions; two repetitions each; other
agents' builds were running on the machine, so single runs vary by ~±15 %). µs of server CPU
per connection per second (`getrusage` of the server process over 60 s ÷ connections); cores =
server process CPU; ws polls = `poll.ws_connection` per connection per second (profiler).

| sockets | scenario | base µs/s (run 1 / 2) | after µs/s (run 1 / 2) | speed-up | cores base → after | ws polls/conn/s |
|---:|---|---|---|---:|---|---|
| 1,000 | quiet | 10.6 / 9.1 | 3.6 / 3.6 | 2.8× | 0.010 → 0.004 | 0.33 → 0.10 |
| 1,000 | presence (101 s) | 23.0 / 20.4 | 17.2 / 15.0 | 1.3× | 0.022 → 0.016 | 1.23 → 0.99 |
| 1,000 | churn (2.7 reconnects/s) | 26.7 / 24.4 | 7.3 / 6.3 | 3.8× | 0.026 → 0.007 | 5.33 → 0.12 |
| 1,000 | lab-like (both) | 33.7 / 32.3 | 18.4 / 17.0 | 1.9× | 0.033 → 0.018 | 5.75 → 1.03 |
| 5,000 | quiet | 8.9 / 11.4 | 2.5 / 3.0 | 3.7× | 0.051 → 0.014 | 0.32 → 0.10 |
| 5,000 | churn (13.5 reconnects/s) | 66.2 / 86.7 | 5.6 / 5.6 | 13.7× | 0.382 → 0.028 | 18.4 → 0.13 |
| 5,000 | lab-like (both) | 74.2 / 69.8 | 20.0 / 17.7 | 3.8× | 0.360 → 0.094 | 17.2 → 1.11 |

Every run: all sockets connected, 0 auth errors. Earlier single runs (development) agree:
base lab-like 1k 38.5, 5k 70.7; after fixes 1+2 (without the `biased` write) 1k 19.1, 5k 17.7.

The base cost grows with N in the churn and lab-like scenarios (1k → 5k: 25 → 76 and 33 → 72 µs/s).
After the change it is flat (6–7 → 5.6 and 18 → 19). Breakdown of the remaining 5k lab-like cost
(`perf record -F 999 -g`, 30 s): 53 % `sendto` (TCP transmit and, on loopback, the peer's receive
softirq; nf_conntrack/nf_tables ~9 %), ~34 % user space (connection loop, `Notify`,
tungstenite framing, `clock_gettime` 3.5 % from the always-on profiler and the batcher). The writer
task's ~1.1 polls/s per connection at ~14 µs are the presence frames. With quiet sockets (pings
only) the remaining cost is 2.5–3 µs/s per connection at 5k (0.014 cores for 5k sockets).

What this means for the cost model's 60 µs/s per user at 500 (production mix): this driver
reproduces ~33 µs/s at 1k without any data subscriptions or deliveries. The rest of the lab
figure is per-connection work on change-feed events (every connection of a tenant wakes for every
tenant transaction and runs its fan-out lookup). That belongs to delivery (lever A) and is not
measured here.

## Risks and contract

* No protocol change. Clients see the same `auth.error` for an invalid session.
* A database error during a round revokes every connection in the failed chunk at once (the old
  per-connection checks failed the same way, but spread over 30 s). A reconnect storm after a
  control-database outage is therefore more synchronized than before.
* The round runs on the network runtime; at 5k sessions it is ~10 control statements plus one per
  tenant every 30 s, measured inside the driver's window.

## Tests

* `cargo test -p gonvex-websocket -p gonvex-store -p gonvex-runtime`: 517 passed / 0 failed /
  7 ignored.
* `cargo test -p gonvex-runtime --features perf-fixture`: 413 passed / 0 failed / 9 ignored
  (410 at the base plus the 3 new ones).
* New `tests/conn_idle.rs` (real sockets): a round with valid sessions revokes nothing; revoked,
  disabled-account and expired sessions get `auth.error session-expired` after one round while a
  valid connection stays and its `last_seen_at` is refreshed; revoked connections leave the
  registry; a Member that left ends the session; a control-only owner socket with a live
  `control.support.listTenants` query still gets `presence-change` refreshes on connect and on
  disconnect elsewhere in the project, and its control-only session is batched and stays valid.
* `cargo clippy -p gonvex-runtime --features perf-fixture --benches --tests`: no warnings in the
  new or changed code.

## Not measured / left

* Not measured: the lab (no e2e run; integration owns that), compressed sockets (the tungstenite
  test client does not negotiate permessage-deflate), the round's latency at large session counts
  (its CPU is inside the 60 s windows above), and a revocation-time distribution.
* Not done: coalesced presence Pings. With quiet sockets the whole remaining cost, Pings included,
  is ≤ 3 µs/s per connection; a per-connection 20 s Tokio `Interval` stays registered and is not
  re-armed per loop. A shared tick would save at most one connection-task poll per 20 s
  (~0.1 µs/s) and would burst-wake every socket together.
* Next lever for idle-ish connections in production: feed-event wakeups of connections an event
  does not concern (lever A territory). After that comes the frame rate of presence (1 s tick,
  pending Gabriel's decision).
