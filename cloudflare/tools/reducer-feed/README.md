# Reducer log feed replay (P04)

Shadow running (stage 2) replays every committed Reducer call of a tenant into
a second backend in commit order. The Rust runtime records the calls; this tool
moves them from one Rust runtime into another and checks that both tenants end
up identical.

## What the runtime records

`rust/crates/runtime/src/reducer_feed.rs`. Off unless `GONVEX_REDUCER_FEED=1`.

The existing tables were not enough:

| Table | Has | Missing for replay |
|---|---|---|
| `_gonvex_sync_transactions` | revision, channel, root channel, command ids, actor, on-behalf-of, agent execution | path, arguments, `ctx.now`, entropy, key, artifact, member snapshot, delegation, outcome; no row when a call changed no tracked rows |
| `_gonvex_reducer_idempotency` | key, receipt path, result | only keyed calls; no order, identity, time or entropy |
| `gonvex_runtime_reducer_logs` | diagnostic entries in the Control Plane | not per tenant, not in commit order, not written in the call's transaction |

So each Reducer that commits a write transaction appends one row to the
per-tenant `_gonvex_reducer_feed`, in the same transaction:

- `seq`: commit order (AUTOINCREMENT, never reused)
- `revision`: the revision that commit got, stamped by a trigger on
  `_gonvex_sync_transactions`; NULL when the call changed no tracked rows (a
  receipt-only or outbox-only write)
- `command_id`, `path`, `args`
- `session`: account, member (role, permissions, membership revision),
  delegation, developer session
- `provenance`: channel, root channel, root/parent command ids, actor,
  on-behalf-of, agent execution, thread/turn/tool-call ids, artifact hash,
  depth, durable hops, Action stack, delegation
- `now_unix_ms` (`ctx.now`; the runtime stamps it while recording),
  `intent_entropy` (seeds `Math.random`), `idempotency_key`, `receipt_path`,
  `artifact_hash`
- `result_sha256`: the outcome a replay must reproduce

Rolled-back calls leave no row. A Reducer that made no database call cannot
change state and is not recorded. If the append fails, the call fails: a feed
with holes would let a shadow drift without anyone noticing.

Routes (admin key only, `404` while the flag is off):

- `GET /dev/reducer-feed?project&tenant&after=<seq>&limit=<n≤5000>` returns
  `{entries, next, more, gaps, head}`. `gaps` lists revisions inside the page's
  span that no recorded call made: `control.*` functions, operator edits,
  scheduled-job promotion. A replay cannot reproduce those writes.
- `POST /dev/reducer-feed/replay` (only with `GONVEX_REDUCER_FEED_REPLAY=1`)
  runs `{project, tenant, entries}` in order through the ordinary Reducer path,
  using the recorded session, provenance, clock, entropy, key and receipt path.
  It stops at the first entry that fails, returns a different result, or
  recorded a different artifact than the one this runtime runs.
- `GET /dev/reducer-feed/checksums?project&tenant` returns per-table row
  counts and SHA-256 over every row in key order (`quote()` of each column), from
  one snapshot.

A replaying runtime does not start the scheduler and never wakes the Action
outbox. Crons, scheduled jobs and Actions already ran on the source, and the
Reducer calls they made are in the feed. Never set the replay flag on an
instance that serves users.

## Running it

```sh
# source: records
GONVEX_REDUCER_FEED=1 gonvex-server …
# shadow: same artifact, same base state (fresh setup or a P03 import at the feed's start)
GONVEX_REDUCER_FEED_REPLAY=1 gonvex-server …

GONVEX_SOURCE_ADMIN_KEY=… GONVEX_TARGET_ADMIN_KEY=… \
  node cloudflare/tools/reducer-feed/replay.mjs \
  --source http://127.0.0.1:8080 --target http://127.0.0.1:8081 \
  --project whagons --tenant acme [--after <seq>] [--batch 200]
```

The tool prints a JSON report (calls applied, the cursor to resume from, gaps,
revision mismatches, differing tables). Exit codes: `0` identical, `1` a call
diverged or a request failed, `2` checksums differ, `3` usage. Keys are read
from the environment only. Node 18+, no dependencies.

## Proof (`done_when`)

`cargo test -p gonvex-runtime --lib reducer_feed` runs a scripted day of calls
on runtime A (recording) and replays it on a fresh runtime B (replaying) from
the same base fixture. The day mixes six members on the UI channel, a delegated
API client, Agent-channel calls in a UI-rooted chain, the nightly internal job
on the scheduler channel, idempotent calls and their retries, receipt-only
calls, no-ops and rolled-back failures. The test asserts identical per-table
checksums.

Set `GONVEX_REDUCER_FEED_NODE=node` to also serve both runtimes on sockets and
run this tool into a third fresh runtime. `GONVEX_REDUCER_FEED_DAY_CALLS` scales
the day (default 1,500). `GONVEX_REDUCER_FEED_BENCH=<calls>` runs the
write-overhead measurement (`write_overhead`); use `--release`.

### Measured (arch3, release build, 2026-11-10)

Scripted day, 1,500 calls: 1,324 feed rows (58 without a revision), replayed
in 4 pages. All 32 compared tables are identical, including `tasks` (406
rows), `audit` (935), `comments` (196), `members` (6), `_gonvex_sync_changes`
(2,290) and `_gonvex_reducer_idempotency` (340). The Node tool reproduces the
same result into a third runtime.

Write overhead with the flag on (`GONVEX_REDUCER_FEED_BENCH=3000`, createTask
and bump alternating, feed off and on in blocks of 100):

| | off | on |
|---|---|---|
| per call, mean | 29.18 ms | 29.16 ms (−0.1%) |
| per call, p50 | 19.90 ms | 19.92 ms (+13 µs) |
| writer held, without fsync | 1.0 µs (provenance INSERT) | 7.7 µs (+ feed INSERT and trigger): **+6.7 µs** |
| capture serialization, outside the writer | | 2.7 µs |
| feed row payload | | ~1,006 bytes |

The commit fsync dominates a call, so end to end the feed is not measurable.
What it adds is about 6.7 µs of writer time per recorded Reducer: the single
writer's throughput ceiling drops by that much per commit. Storage grows by
about 1 KB per call; nothing prunes the feed yet.

## Checksum exclusions

| Excluded | Why |
|---|---|
| `_gonvex_reducer_feed` | exists on the source only |
| `sqlite_sequence` | AUTOINCREMENT counters, including the feed's own |
| `sqlite_stat1`, `sqlite_stat4` | planner statistics |
| `_gonvex_action_outbox` | delivery state: the source drains Actions, a shadow does not |
| `_gonvex_control_plane_membership_outbox` | delivery state; a `members` trigger stamps it with SQLite's wall clock |
| `_gonvex_sync_clock.epoch` | random per database |
| `created_at` of `_gonvex_sync_changes`, `_gonvex_sync_transactions`, `_gonvex_reducer_idempotency` | SQLite wall clock at commit |

Everything else must match exactly: every application table, `members`,
receipts, and the change feed's row images, ordinals and revisions.

## Determinism limits (for the shadow runner)

- **SQL that reads the wall clock** (a column `DEFAULT (unixepoch(...))`, or a
  trigger calling `unixepoch`) gives a different value on replay. Modules must
  write times from `ctx.now`. Check every staging tenant schema for this before
  G4.
- **A raw `ctx.db.insert` into a table with a text key**, with no key and no
  `generatedId`, gets `Uuid::new_v4()` from the host. `@gonvex/module-sdk`
  always passes a `generatedId` derived from the intent, so SDK modules are
  deterministic.
- **Writes that are not Reducer calls** show up as `gaps`. Revisions after a
  gap are offset on the shadow (`revisionMismatches`), but row contents still
  match as long as the gap did not touch them.
- **Reducer results** are compared by hash. A module that returns something
  derived from data the source changed outside the feed diverges, and the
  replay stops on that entry.
