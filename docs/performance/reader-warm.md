# Warm readers and cheap startup introspection

Staging logs (2026-11-03, one user) showed bursts of 11–14 `PRAGMA foreign_keys = ON`
statements taking 1–4 s each, reader pool acquire waits up to 4.2 s, and at startup 22 runs of
`SELECT m.name, p.name, p.type …` (4,303 rows) at 1.1–1.9 s each. This branch finds the causes
and fixes both.

All numbers come from `rust/crates/store/examples/reader_warm_bench.rs` on arch3, run over a
reflink copy of the lab runtime's real Whagons data (26 tenant files, identical schema: 396
tables, 902 indexes, 1,127 triggers, 553 KB of schema SQL). The process was pinned to one CPU
(`taskset -c N`) with a current-thread tokio runtime to model the 1-vCPU box. Other agents
were building on the machine at the same time.

```sh
B=$CARGO_TARGET_DIR/release/examples/reader_warm_bench
LAB_DATA=/copy/of/data MODE=setup   taskset -c 0 $B   # per-step connection setup
LAB_DATA=/copy/of/data MODE=startup taskset -c 0 $B   # store half of Runtime::start
LAB_DATA=/copy/of/data MODE=idle GAP_S=60 ROUNDS=3 GONVEX_SQLITE_CACHE_BUDGET_MB=128|256 taskset -c 2 $B
RUST_LOG=sqlx::query=debug ...                         # every statement with its time
```

## Root cause 1: every new reader parses the whole schema

When a connection opens, sqlx sends all its pragmas as one batch. That batch is what the log
showed as `PRAGMA foreign_keys = ON; …`. `foreign_keys` is the first statement and is cheap.
The next pragma that needs the schema (`synchronous`, `cache_size`, …) makes SQLite parse the
whole `sqlite_schema` of the file, which leaves **2.9 MB of parsed schema per connection**.
The waits are CPU and allocator contention, not locks: no writer was running in the `setup`
measurement.

| read-only open (`MODE=setup`) | p50 | p99 |
|---|---|---|
| one at a time, 1 CPU: open_v2 | 0.23 ms | 1.3 ms |
| one at a time, 1 CPU: schema-loading pragma | 27 ms | 40 ms |
| 13 concurrent, 1 CPU: whole setup | 183 ms | 212 ms |
| 13 concurrent, all CPUs: whole setup | 433 ms | 460 ms |

Concurrent opens are slower on many CPUs than on one, because SQLite's memory statistics take a
global mutex on every allocation (needed for `sqlite3_memory_used` and the soft heap limit),
and the schema parse makes thousands of allocations. On staging's slower, throttled vCPU, 11–14
concurrent opens take 1–4 s.

The runtime closed readers 15 s after their last use (`GONVEX_DB_IDLE_TIMEOUT_MS`). Each cron
or page load after a quiet spell therefore reopened readers in every tenant it touched, up to
`max_connections_per_database` (2 on one vCPU) per tenant, all at once. The acquire wait is
that setup time.

## Root cause 2: introspection repeated per tenant, streamed row by row

Per tenant, startup ran the capture catalog query (`capture::load_shapes`, 4,303 rows) three
times: when the writer connects, after `TENANT_IDENTITY_SQL` (any script containing
`TRIGGER`), and at the end of `install_change_feed_locked`. Over 26 tenants plus the control
plane that is 80 runs. Startup also ran a 4,428-row all-columns query and a `pragma_table_list`
query with a correlated `sqlite_schema` scan per table (~70 ms each). In the sqlite3 shell the
catalog query costs 4–32 ms. In process it cost far more, because every result row crosses
sqlx's worker thread separately, and 8 tenants provision at once on one CPU.

## Fixes

**Reader retention** (`registry.rs`, `sweep_readers`). Readers no longer use sqlx's
per-connection idle timer. A sweeper task (one per registry) looks at when each database last
released a reader (`after_release` stamp) and applies these rules:

- read within `idle_timeout` (15 s): keep up to `max_idle_connections_per_database` (this knob
  is now enforced);
- read within `warm_reader_window` (`GONVEX_DB_WARM_READER_MS`, default 600,000 ms = 10 min):
  keep **one warm reader**;
- older: close all readers (`0` disables warm readers, which restores the old behaviour).

**Warm readers count against `GONVEX_SQLITE_CACHE_BUDGET_MB`.** The sweeper measures the
connection heap: SQLite heap minus page-cache bytes (`SQLITE_STATUS_PAGECACHE_OVERFLOW`), i.e.
schemas and statements, which the soft heap limit cannot reclaim. While that heap is above
three quarters of the budget, it closes warm readers of the least recently read databases
first, then the idle extra readers of busy databases (keeping one). The last quarter of the
budget is left for page caches. `sqlite_connection_heap()` is exported.

**Reader patience** (`GONVEX_DB_READER_PATIENCE_MS`, default 25 ms; `0` turns it off).
`begin_transaction(read_only)` normally opens a new reader when every open reader is busy.
Now it first waits up to the patience for one to be released (polling `try_acquire`, which
never opens a connection). A page load's burst of short reads then shares the warm reader
instead of paying 30 ms to seconds for another schema parse. This applies only to
`begin_transaction`. Code that runs queries directly on `pools.reader()` still opens a reader
at once.

**Per-connection setup.** Readers no longer send `PRAGMA synchronous`, which does nothing on
a read-only connection. `foreign_keys` is sqlx's default and costs microseconds. The real setup
cost is the schema parse, which cannot be avoided per connection, so the remedy is not
reconnecting.

**Startup introspection** (`capture.rs`, `provision.rs`):

- Capture catalogs are cached process-wide by a SHA-256 of the `sqlite_schema` rows they depend
  on (every table definition plus every capture trigger), read as one `group_concat` row.
  Tenants with the same schema share one `Arc` catalog. `refresh` compares digests and does
  nothing when the captured tables did not change, so a boot reads a digest per refresh
  instead of rebuilding the catalog. A version counter (`schema_version`) was not used because
  it can repeat after a rolled-back DDL.
- The catalog, all-columns, trigger and receipt reads return one `json_group_array` row instead
  of thousands of rows.
- `application_tables` collects the virtual and shadow table names once (a materialized CTE)
  instead of scanning `sqlite_schema` once per table.

## Results (1 CPU)

| measurement | before | after | change |
|---|---|---|---|
| startup, 26 tenants (`provision_tenant_database` ×26, 8 at a time) | 7.1–8.9 s (4 runs) | 1.36–1.44 s (2 runs) | ~5.5× faster |
| startup, all CPUs | 5.5 s | 1.18 s | 4.6× faster |
| capture catalog builds at startup | 80 | 10 (the first 8 tenants miss the cache concurrently; control plane) | |
| RSS after startup | 307 MiB | 252 MiB | −55 MiB |
| read open after a 60 s gap, 26 tenants × 4 concurrent reads, p50 / p99, budget 256 MB | 478 / 671 ms | 2.5 / 7.6 ms | p99 88× lower |
| same, budget 128 MB (the default) | 478 / 671 ms | 422 / 611 ms | budget keeps ~6 warm readers |
| SQLite heap in steady state (26 tenants), budget 256 | 227 MiB (78 connections) | 164 MiB (26 writers + 26 warm readers) | −63 MiB |
| SQLite heap in steady state, budget 128 | 227 MiB | 151–221 MiB (reopened extras until swept) | |

With 20 s gaps the "before" numbers vary from round to round, because sqlx's reaper sometimes
keeps readers past 15 s. Raw outputs are under `/home/gabriel/work/gonvex-perf/reader-warm-lab/results`.

### The budget decision

26 writers alone hold ~75 MB of parsed schema, which is already more than three quarters of the
default 128 MB budget leaves after writers (96 MB allowance). So under the default budget only
~6 tenants keep a warm reader. Before this change the budget was never enforced on connection
heap: the same workload reached 227 MiB. Recommended setting for ~26 Whagons tenants:
**`GONVEX_SQLITE_CACHE_BUDGET_MB=256`**. Steady SQLite heap is then ~165 MiB, and post-idle
reads stay in single-digit milliseconds. The default is left as is because it belongs to the
SQLite writer workstream; the coordinator should choose.

## Knobs

| env | default | meaning |
|---|---|---|
| `GONVEX_DB_WARM_READER_MS` | 600000 | how long one reader stays open after a database's last read; 0 disables |
| `GONVEX_DB_READER_PATIENCE_MS` | 25 | how long a read transaction waits for a busy reader before opening another |
| `GONVEX_DB_IDLE_TIMEOUT_MS` | 15000 (runtime) | extra readers beyond the warm one close after this |
| `GONVEX_DB_MAX_IDLE_CONNS` | 4 | idle readers kept while a database is busy (now enforced) |
| `GONVEX_SQLITE_CACHE_BUDGET_MB` | 128 | warm readers are evicted (LRU) while connection heap > 3/4 of it |

## Not measured

- The full runtime binary (V8 module host, real crons and page loads) and staging itself. The
  driver reproduces the store-level pattern only.
- Startup of the whole `Runtime::start` (module install, control migrations); only the
  per-tenant store provisioning is timed.
- Off-CPU profiles of the concurrent-open case. The mutex explanation rests on the 1-CPU vs
  all-CPU comparison, not on a futex trace.
- Readers used directly through `pools.reader()` (change feed, control plane) do not use
  patience.
- With a cold tenant, a burst still opens up to `max_connections_per_database` readers,
  because patience (25 ms) is shorter than an open.
