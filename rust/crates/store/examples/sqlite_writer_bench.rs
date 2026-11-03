//! Store-level micro-benchmarks for the tenant writer (workstream 8,
//! `docs/performance/sqlite-writer.md`). No runtime, no V8: one tenant
//! database driven the way the runtime's Reducer path drives it.
//!
//! ```text
//! sqlite_writer_bench thread      # sqlx worker thread vs rusqlite on one thread vs inline raw handle
//! sqlite_writer_bench commit      # reducer-shaped write transactions at 1/10/50/200 writers
//! sqlite_writer_bench cache       # read latency + RSS with 27 tenant files open
//! ```
//!
//! Environment: `BENCH_DIR` (data directory, default under the temp dir),
//! `BENCH_SECONDS` (per point, default 5), `BENCH_WRITERS` (comma list,
//! default `1,10,50,200`), `BENCH_SYNC` (`barrier` = registry default,
//! `full` = synchronous=FULL inside COMMIT, `normal` = no fsync before ack),
//! `BENCH_WORKERS` (tokio workers, default 1 to mirror a 1-vCPU box; pin the
//! process with `taskset -c N` for the same effect on threads).
use gonvex_store::feed::{capture_triggers_sql, finalize, Provenance, SYNC_INFRASTRUCTURE_SQL};
use gonvex_store::rows::table_columns;
use gonvex_store::{begin_transaction, execute_script, Durability, PoolLimits, PoolRegistry};
use sqlx::{Connection, SqliteConnection};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PRESENCE_ROWS: i64 = 2_000;
const TASK_ROWS: i64 = 9_000;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let workers = env_or("BENCH_WORKERS", 1usize);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    let mode = std::env::args().nth(1).unwrap_or_else(|| "thread".into());
    runtime.block_on(async move {
        match mode.as_str() {
            "thread" => thread_model().await,
            "commit" => commit().await,
            "cache" => cache().await,
            other => panic!("unknown mode {other}"),
        }
    });
}

// ---------------------------------------------------------------- measuring

#[derive(Clone, Copy)]
struct Usage {
    cpu_us: i64,
    vcsw: i64,
    ivcsw: i64,
}

fn usage() -> Usage {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let us = |tv: libc::timeval| tv.tv_sec * 1_000_000 + tv.tv_usec;
    Usage {
        cpu_us: us(ru.ru_utime) + us(ru.ru_stime),
        vcsw: ru.ru_nvcsw,
        ivcsw: ru.ru_nivcsw,
    }
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn sqlite_memory_used() -> i64 {
    unsafe { libsqlite3_sys::sqlite3_memory_used() }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index]
}

struct Report {
    label: String,
    ops: u64,
    wall: Duration,
    lat_ns: Vec<u64>,
    before: Usage,
    after: Usage,
}

impl Report {
    fn print(mut self) {
        self.lat_ns.sort_unstable();
        let ops = self.ops.max(1) as f64;
        println!(
            "{:<44} ops={:>7} {:>9.0} op/s  mean={:>8.1}us p50={:>8.1}us p99={:>8.1}us p999={:>8.1}us max={:>8.1}us  cpu/op={:>6.1}us vcsw/op={:>5.2} ivcsw/op={:>5.2}",
            self.label,
            self.ops,
            ops / self.wall.as_secs_f64(),
            self.lat_ns.iter().sum::<u64>() as f64 / ops / 1e3,
            percentile(&self.lat_ns, 0.50) as f64 / 1e3,
            percentile(&self.lat_ns, 0.99) as f64 / 1e3,
            percentile(&self.lat_ns, 0.999) as f64 / 1e3,
            self.lat_ns.last().copied().unwrap_or(0) as f64 / 1e3,
            (self.after.cpu_us - self.before.cpu_us) as f64 / ops,
            (self.after.vcsw - self.before.vcsw) as f64 / ops,
            (self.after.ivcsw - self.before.ivcsw) as f64 / ops,
        );
    }
}

// ------------------------------------------------------------------ fixture

fn bench_dir() -> PathBuf {
    let dir = std::env::var_os("BENCH_DIR").map(PathBuf::from).unwrap_or_else(|| {
        std::env::temp_dir().join(format!("gonvex-writer-bench-{}", std::process::id()))
    });
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const TENANT_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS presence (
  id TEXT PRIMARY KEY, member_id TEXT NOT NULL, status TEXT NOT NULL,
  last_seen INTEGER NOT NULL, data JSON NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY, workspace_id INTEGER NOT NULL, team_id INTEGER NOT NULL,
  title TEXT NOT NULL, description TEXT NOT NULL, status TEXT NOT NULL,
  assignee TEXT, updated_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS tasks_workspace ON tasks (workspace_id, updated_at);
CREATE TABLE IF NOT EXISTS task_events (
  id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, kind TEXT NOT NULL, at INTEGER NOT NULL);
"#;

/// Creates a tenant-shaped database (change feed, capture triggers, presence
/// and `tasks` rows of ~400-byte descriptions) through the writer.
async fn provision(connection: &mut SqliteConnection, tasks: i64, presence: i64) {
    execute_script(connection, SYNC_INFRASTRUCTURE_SQL).await.unwrap();
    execute_script(connection, TENANT_SCHEMA).await.unwrap();
    for (table, key) in [("presence", "id"), ("tasks", "id"), ("task_events", "id")] {
        let columns = table_columns(connection, table).await.unwrap();
        execute_script(connection, &capture_triggers_sql(table, key, &columns)).await.unwrap();
    }
    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    if existing == 0 {
        // Seed without capture (bulk rows are not part of any measured write).
        execute_script(
            connection,
            &format!(
                "BEGIN;
                 INSERT INTO tasks (id, workspace_id, team_id, title, description, status, assignee, updated_at)
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {tasks})
                 SELECT i, i % 40, i % 12, 'Task ' || i, substr(replace(hex(randomblob(200)), '0', ' '), 1, 400),
                        CASE i % 3 WHEN 0 THEN 'open' WHEN 1 THEN 'doing' ELSE 'done' END, 'm' || (i % 300), i
                 FROM n;
                 INSERT INTO presence (id, member_id, status, last_seen)
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {presence})
                 SELECT 'p' || i, 'm' || i, 'online', 0 FROM n;
                 DELETE FROM _gonvex_sync_changes;
                 COMMIT;"
            ),
        )
        .await
        .unwrap();
    }
}

// -------------------------------------------------------------- 1. threads

/// The SQL `feed::finalize` + `reference_integrity::flush` run, on rusqlite
/// (same statements, same order) so the thread models compare like for like.
fn finalize_rusqlite(conn: &rusqlite::Connection, command: &str) -> rusqlite::Result<Option<i64>> {
    finalize_timed(conn, command, &mut None)
}

/// `finalize_rusqlite` with optional per-statement wall time (ns).
fn finalize_timed(conn: &rusqlite::Connection, command: &str, times: &mut Option<[u64; 9]>) -> rusqlite::Result<Option<i64>> {
    let mut clock = Instant::now();
    let mut slot = 0usize;
    let mut tick = |times: &mut Option<[u64; 9]>| {
        if let Some(t) = times.as_mut() {
            t[slot] += clock.elapsed().as_nanos() as u64;
        }
        slot += 1;
        clock = Instant::now();
    };
    let exists = |sql: &str| -> rusqlite::Result<bool> {
        conn.prepare_cached(sql)?.query_row([], |row| row.get(0))
    };
    let _ = exists("SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_gonvex_reference_pending')")?;
    tick(times);
    if !exists("SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_gonvex_sync_changes')")? {
        return Ok(None);
    }
    tick(times);
    conn.prepare_cached("DELETE FROM _gonvex_sync_changes WHERE revision IS NULL AND operation = 'update' AND new_value IS NULL")?.execute([])?;
    tick(times);
    conn.prepare_cached(
        r#"UPDATE _gonvex_sync_changes
           SET old_value = nullif(new_value -> '$[0]', 'null'),
               changed_columns = coalesce(new_value -> '$[2]', '[]'),
               new_value = nullif(new_value -> '$[1]', 'null')
           WHERE revision IS NULL AND json_type(new_value) = 'array'"#,
    )?
    .execute([])?;
    tick(times);
    if !exists("SELECT EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL)")? {
        return Ok(None);
    }
    tick(times);
    let (revision, _epoch): (i64, String) = conn
        .prepare_cached("UPDATE _gonvex_sync_clock SET revision = revision + 1 WHERE singleton = 1 RETURNING revision, epoch")?
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    tick(times);
    conn.prepare_cached(
        r#"UPDATE _gonvex_sync_changes
           SET revision = $1, transaction_id = $1, ordinal = ranked.row_ordinal, command_id = $2
           FROM (SELECT event_id, row_number() OVER (ORDER BY event_id) AS row_ordinal
                 FROM _gonvex_sync_changes WHERE revision IS NULL) AS ranked
           WHERE _gonvex_sync_changes.event_id = ranked.event_id"#,
    )?
    .execute(rusqlite::params![revision, command])?;
    tick(times);
    let mut tables = conn.prepare_cached("SELECT DISTINCT table_name FROM _gonvex_sync_changes WHERE revision = $1 ORDER BY table_name")?;
    let _tables: Vec<String> = tables.query_map([revision], |row| row.get(0))?.collect::<Result<_, _>>()?;
    drop(tables);
    tick(times);
    conn.prepare_cached(
        r#"INSERT INTO _gonvex_sync_transactions
           (revision, transaction_id, root_command_id, origin_command_id, root_invocation_channel,
            invocation_channel, actor_account_id, actor_member_id, on_behalf_of_member_id, agent_execution_id)
           VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (revision) DO NOTHING"#,
    )?
    .execute(rusqlite::params![revision, command, command, "ui", "ui", "acct", "member", None::<String>, None::<String>])?;
    tick(times);
    Ok(Some(revision))
}

const HEARTBEAT: &str = "UPDATE presence SET last_seen = $1, status = $2 WHERE id = $3";
const POINT_READ: &str = "SELECT status, last_seen FROM presence WHERE id = $1";

type Job = Box<dyn FnOnce(&rusqlite::Connection) + Send>;

/// A dedicated thread that owns a connection handle and runs closures.
fn spawn_db_thread(raw: usize) -> std::sync::mpsc::Sender<Job> {
    let (tx, rx) = std::sync::mpsc::channel::<Job>();
    std::thread::spawn(move || {
        let conn = unsafe { rusqlite::Connection::from_handle(raw as *mut _) }.unwrap();
        conn.set_prepared_statement_cache_capacity(64);
        while let Ok(job) = rx.recv() {
            job(&conn);
        }
    });
    tx
}

async fn on_thread<T: Send + 'static>(
    jobs: &std::sync::mpsc::Sender<Job>,
    f: impl FnOnce(&rusqlite::Connection) -> T + Send + 'static,
) -> T {
    let (tx, rx) = tokio::sync::oneshot::channel();
    jobs.send(Box::new(move |conn| {
        let _ = tx.send(f(conn));
    }))
    .unwrap();
    rx.await.unwrap()
}

async fn thread_model() {
    let dir = bench_dir();
    let path = dir.join("thread.db");
    let options = format!("sqlite://{}?mode=rwc", path.display())
        .parse::<sqlx::sqlite::SqliteConnectOptions>()
        .unwrap()
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        // Isolate per-statement overhead from fsync.
        .synchronous(sqlx::sqlite::SqliteSynchronous::Off);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    gonvex_store::capture::install(&mut connection).await.unwrap();
    provision(&mut connection, 1_000, PRESENCE_ROWS).await;
    let n: u64 = env_or("BENCH_OPS", 20_000);
    let provenance = Provenance {
        command_id: Some("cmd".into()),
        ..Provenance::default()
    };

    // Warm statement caches.
    for i in 0..200 {
        sqlx::query(POINT_READ).bind(format!("p{}", i % 100 + 1)).fetch_optional(&mut connection).await.unwrap();
    }

    // (a) point read, one statement per round trip
    let report = |label: &str, ops, wall, lat_ns, before, after| Report {
        label: label.into(), ops, wall, lat_ns, before, after,
    };
    {
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            sqlx::query(POINT_READ).bind(format!("p{}", i % 2000 + 1)).fetch_optional(&mut connection).await.unwrap();
            lat.push(s.elapsed().as_nanos() as u64);
        }
        report("stmt point-read sqlx", n, t0.elapsed(), lat, u0, usage()).print();
    }
    // (b) heartbeat transaction with real finalize, all via sqlx
    {
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            let mut tx = connection.begin().await.unwrap();
            sqlx::query(HEARTBEAT).bind(i as i64).bind("online").bind(format!("p{}", i % 2000 + 1)).execute(&mut *tx).await.unwrap();
            finalize(&mut tx, &provenance).await.unwrap();
            tx.commit().await.unwrap();
            lat.push(s.elapsed().as_nanos() as u64);
        }
        report("txn heartbeat+finalize sqlx (~13 trips)", n, t0.elapsed(), lat, u0, usage()).print();
    }
    // (c) same transaction: BEGIN/stmt/COMMIT via sqlx, finalize inline on
    // the locked raw handle (the narrow production change)
    {
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            let mut tx = connection.begin().await.unwrap();
            sqlx::query(HEARTBEAT).bind(i as i64 + n as i64).bind("online").bind(format!("p{}", i % 2000 + 1)).execute(&mut *tx).await.unwrap();
            {
                let mut handle = tx.lock_handle().await.unwrap();
                let conn = unsafe { rusqlite::Connection::from_handle(handle.as_raw_handle().as_ptr()) }.unwrap();
                finalize_rusqlite(&conn, "cmd").unwrap();
            }
            tx.commit().await.unwrap();
            lat.push(s.elapsed().as_nanos() as u64);
        }
        report("txn heartbeat sqlx + finalize inline raw", n, t0.elapsed(), lat, u0, usage()).print();
    }

    // Hand the raw handle to a dedicated thread (rusqlite) while we hold
    // sqlx's lock, so sqlx's own worker stays parked.
    let mut handle = connection.lock_handle().await.unwrap();
    let raw = handle.as_raw_handle().as_ptr() as usize;
    let jobs = spawn_db_thread(raw);
    // (d) point read, one job per statement on a dedicated thread
    {
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            let id = format!("p{}", i % 2000 + 1);
            on_thread(&jobs, move |c| {
                c.prepare_cached(POINT_READ).unwrap().query_row([id], |r| r.get::<_, String>(0)).ok()
            })
            .await;
            lat.push(s.elapsed().as_nanos() as u64);
        }
        report("stmt point-read rusqlite thread", n, t0.elapsed(), lat, u0, usage()).print();
    }
    // (e) heartbeat txn, one job per statement (same trips as sqlx)
    {
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            on_thread(&jobs, |c| c.execute_batch("BEGIN").unwrap()).await;
            let (v, id) = (i as i64 + 2 * n as i64, format!("p{}", i % 2000 + 1));
            on_thread(&jobs, move |c| c.prepare_cached(HEARTBEAT).unwrap().execute(rusqlite::params![v, "online", id]).unwrap()).await;
            // finalize as 11 separate jobs is what sqlx does; emulate by
            // running each statement as its own job.
            for _ in 0..10 {
                on_thread(&jobs, |c| c.prepare_cached("SELECT 1").unwrap().query_row([], |r| r.get::<_, i64>(0)).unwrap()).await;
            }
            on_thread(&jobs, |c| finalize_rusqlite(c, "cmd").unwrap()).await;
            on_thread(&jobs, |c| c.execute_batch("COMMIT").unwrap()).await;
            lat.push(s.elapsed().as_nanos() as u64);
        }
        report("txn heartbeat rusqlite thread, job/stmt", n, t0.elapsed(), lat, u0, usage()).print();
    }
    // (f) heartbeat txn as ONE job on the dedicated thread
    {
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            let (v, id) = (i as i64 + 3 * n as i64, format!("p{}", i % 2000 + 1));
            on_thread(&jobs, move |c| {
                c.execute_batch("BEGIN").unwrap();
                c.prepare_cached(HEARTBEAT).unwrap().execute(rusqlite::params![v, "online", id]).unwrap();
                finalize_rusqlite(c, "cmd").unwrap();
                c.execute_batch("COMMIT").unwrap();
            })
            .await;
            lat.push(s.elapsed().as_nanos() as u64);
        }
        report("txn heartbeat rusqlite thread, 1 job/txn", n, t0.elapsed(), lat, u0, usage()).print();
    }
    drop(jobs);
    // (g) the whole txn inline on the async thread (lower bound)
    {
        let conn = unsafe { rusqlite::Connection::from_handle(raw as *mut _) }.unwrap();
        let mut lat = Vec::with_capacity(n as usize);
        let (u0, t0) = (usage(), Instant::now());
        for i in 0..n {
            let s = Instant::now();
            conn.execute_batch("BEGIN").unwrap();
            conn.prepare_cached(HEARTBEAT).unwrap().execute(rusqlite::params![i as i64 + 4 * n as i64, "online", format!("p{}", i % 2000 + 1)]).unwrap();
            finalize_rusqlite(&conn, "cmd").unwrap();
            conn.execute_batch("COMMIT").unwrap();
            lat.push(s.elapsed().as_nanos() as u64);
            if i % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
        report("txn heartbeat inline raw (no handoff)", n, t0.elapsed(), lat, u0, usage()).print();
    }
    {
        let conn = unsafe { rusqlite::Connection::from_handle(raw as *mut _) }.unwrap();
        let mut times = Some([0u64; 9]);
        let (mut begin, mut stmt, mut commit) = (0u64, 0u64, 0u64);
        for i in 0..n {
            let s = Instant::now();
            conn.execute_batch("BEGIN").unwrap();
            begin += s.elapsed().as_nanos() as u64;
            let s = Instant::now();
            conn.prepare_cached(HEARTBEAT).unwrap().execute(rusqlite::params![i as i64 + 5 * n as i64, "online", format!("p{}", i % 2000 + 1)]).unwrap();
            stmt += s.elapsed().as_nanos() as u64;
            finalize_timed(&conn, "cmd", &mut times).unwrap();
            let s = Instant::now();
            conn.execute_batch("COMMIT").unwrap();
            commit += s.elapsed().as_nanos() as u64;
        }
        let per = |v: u64| v as f64 / n as f64 / 1e3;
        let t = times.unwrap();
        println!("inline breakdown us/txn: BEGIN {:.1} heartbeat-UPDATE(+trigger+hook) {:.1} | ref_pending? {:.1} has_feed {:.1} delete-noop {:.1} split-json {:.1} pending? {:.1} clock {:.1} rank-UPDATE {:.1} tables {:.1} insert-txn {:.1} | COMMIT {:.1}",
            per(begin), per(stmt), per(t[0]), per(t[1]), per(t[2]), per(t[3]), per(t[4]), per(t[5]), per(t[6]), per(t[7]), per(t[8]), per(commit));
    }
    drop(handle);
}

// ---------------------------------------------------------------- 2. commit

/// One reducer-shaped write transaction through the production path
/// (`begin_transaction` → statements → `commit_with_revision`): 9 of 10 are a
/// heartbeat UPDATE, 1 of 10 a task update of three statements.
async fn reducer_write(pools: &PoolRegistry, url: &str, writer: usize, step: u64) {
    let mut tx = begin_transaction(pools, url, false).await.unwrap();
    tx.set_command_id(&format!("w{writer}-{step}")).await.unwrap();
    let now = step as i64;
    if step % 10 == 0 {
        let id = ((writer as i64 * 7919 + step as i64) % TASK_ROWS) + 1;
        sqlx::query("SELECT status FROM tasks WHERE id = $1").bind(id).fetch_one(&mut **tx.transaction()).await.unwrap();
        sqlx::query("UPDATE tasks SET status = $1, updated_at = $2 WHERE id = $3")
            .bind(if step % 20 == 0 { "doing" } else { "done" }).bind(now).bind(id)
            .execute(&mut **tx.transaction()).await.unwrap();
        sqlx::query("INSERT INTO task_events (task_id, kind, at) VALUES ($1, 'status', $2)")
            .bind(id).bind(now).execute(&mut **tx.transaction()).await.unwrap();
    } else {
        sqlx::query(HEARTBEAT).bind(now).bind("online").bind(format!("p{}", writer as i64 % PRESENCE_ROWS + 1))
            .execute(&mut **tx.transaction()).await.unwrap();
    }
    // Simulated V8 time with the writer held (the Reducer path keeps its
    // write transaction open from the first ctx.db call until it returns).
    let hold: u64 = env_or("BENCH_HOLD_US", 0);
    if hold > 0 {
        tokio::time::sleep(Duration::from_micros(hold)).await;
    }
    tx.commit_with_revision().await.unwrap();
}

async fn commit() {
    let dir = bench_dir();
    let sync = std::env::var("BENCH_SYNC").unwrap_or_else(|_| "barrier".into());
    let seconds: u64 = env_or("BENCH_SECONDS", 5);
    let writers: Vec<usize> = std::env::var("BENCH_WRITERS")
        .unwrap_or_else(|_| "1,10,50,200".into())
        .split(',')
        .map(|w| w.trim().parse().unwrap())
        .collect();
    let durability = if sync == "barrier" { Durability::Full } else { Durability::Normal };
    let pools = PoolRegistry::with_durability(
        PoolLimits { max_total_connections: 256, ..PoolLimits::default() },
        durability,
    );
    let url = gonvex_store::database_url(&dir.join(format!("commit-{sync}.db")));
    {
        let writer = pools.pool(&url).await.unwrap();
        let mut connection = writer.acquire().await.unwrap();
        provision(&mut connection, TASK_ROWS, PRESENCE_ROWS).await;
        if sync == "full" {
            // The writer pool keeps its single connection forever.
            sqlx::query("PRAGMA synchronous = FULL").execute(&mut *connection).await.unwrap();
        }
    }
    // BENCH_RATE: total transactions/s offered (0 = closed loop, as fast as
    // possible). Each writer sleeps writers/rate between its transactions.
    let rate: f64 = env_or("BENCH_RATE", 0.0);
    for &count in &writers {
        let pause = if rate > 0.0 { Duration::from_secs_f64(count as f64 / rate) } else { Duration::ZERO };
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let (u0, t0) = (usage(), Instant::now());
        let mut tasks = Vec::new();
        for writer in 0..count {
            let (pools, url) = (pools.clone(), url.clone());
            tasks.push(tokio::spawn(async move {
                let mut lat = Vec::new();
                let mut step = 0u64;
                while Instant::now() < deadline {
                    step += 1;
                    let s = Instant::now();
                    reducer_write(&pools, &url, writer, step).await;
                    lat.push(s.elapsed().as_nanos() as u64);
                    if !pause.is_zero() {
                        tokio::time::sleep(pause).await;
                    }
                }
                lat
            }));
        }
        let mut lat = Vec::new();
        for task in tasks {
            lat.extend(task.await.unwrap());
        }
        let wall = t0.elapsed();
        use gonvex_store::checkpoint as ck;
        use std::sync::atomic::Ordering::Relaxed;
        println!(
            "  checkpoints so far: background={} (total {} ms, max {} ms) inline={}",
            ck::BACKGROUND_CHECKPOINTS.load(Relaxed),
            ck::BACKGROUND_CHECKPOINT_US.load(Relaxed) / 1000,
            ck::BACKGROUND_CHECKPOINT_MAX_US.load(Relaxed) / 1000,
            ck::INLINE_CHECKPOINTS.load(Relaxed),
        );
        Report {
            label: format!("commit sync={sync} w={count} rate={rate}"),
            ops: lat.len() as u64,
            wall,
            lat_ns: lat,
            before: u0,
            after: usage(),
        }
        .print();
    }
    let wal = std::fs::metadata(gonvex_store::database_path(&url).with_extension("db-wal")).map(|m| m.len()).unwrap_or(0);
    println!("wal bytes at end: {wal}");
    pools.close().await;
}

// ----------------------------------------------------------------- 5. cache

async fn cache() {
    let dir = bench_dir();
    let seconds: u64 = env_or("BENCH_SECONDS", 5);
    let tenants: usize = env_or("BENCH_TENANTS", 27);
    let readers: usize = env_or("BENCH_READERS", 4);
    let big: i64 = env_or("BENCH_TASKS", TASK_ROWS);
    let rss0 = status_kib("VmRSS:");
    let pools = PoolRegistry::with_durability(PoolLimits::default(), Durability::Normal);
    let urls: Vec<String> = (0..tenants)
        .map(|i| gonvex_store::database_url(&dir.join(format!("tenant-{i}.db"))))
        .collect();
    for (i, url) in urls.iter().enumerate() {
        let writer = pools.pool(url).await.unwrap();
        let mut connection = writer.acquire().await.unwrap();
        // One big tenant (the 39 % one), the rest small.
        provision(&mut connection, if i == 0 { big } else { 300 }, if i == 0 { PRESENCE_ROWS } else { 50 }).await;
    }
    let urls = Arc::new(urls);
    // Touch every tenant's readers (snapshot-style scans), then measure the
    // big tenant's read latency with `readers` concurrent readers.
    for url in urls.iter() {
        let pool = pools.reader(url).await.unwrap();
        let mut conns = Vec::new();
        for _ in 0..readers {
            let mut c = pool.acquire().await.unwrap();
            sqlx::query("SELECT * FROM tasks").fetch_all(&mut *c).await.unwrap();
            conns.push(c);
        }
    }
    println!(
        "after opening {tenants} tenants: VmRSS={} KiB (+{} KiB) RssAnon={} KiB RssFile={} KiB sqlite3_memory_used={} KiB",
        status_kib("VmRSS:"), status_kib("VmRSS:") - rss0, status_kib("RssAnon:"), status_kib("RssFile:"), sqlite_memory_used() / 1024
    );
    for (label, sql) in [
        ("read point (task by id)", "SELECT * FROM tasks WHERE id = $1"),
        ("read range (one workspace = 1/40 of tasks)", "SELECT id, title, status, updated_at FROM tasks WHERE workspace_id = $1 ORDER BY updated_at"),
        ("read snapshot (all tasks)", "SELECT * FROM tasks"),
    ] {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let (u0, t0) = (usage(), Instant::now());
        let mut tasks = Vec::new();
        for r in 0..readers {
            let (pools, url) = (pools.clone(), urls[0].clone());
            let big = big;
            tasks.push(tokio::spawn(async move {
                let pool = pools.reader(&url).await.unwrap();
                let mut lat = Vec::new();
                let mut i = r as i64;
                while Instant::now() < deadline {
                    i += 7919;
                    let s = Instant::now();
                    let key = if sql.contains("workspace_id") { i % 40 } else { i % big + 1 };
                    if sql.contains("$1") {
                        sqlx::query(sql).bind(key).fetch_all(&pool).await.unwrap();
                    } else {
                        sqlx::query(sql).fetch_all(&pool).await.unwrap();
                    }
                    lat.push(s.elapsed().as_nanos() as u64);
                }
                lat
            }));
        }
        let mut lat = Vec::new();
        for task in tasks {
            lat.extend(task.await.unwrap());
        }
        Report { label: label.into(), ops: lat.len() as u64, wall: t0.elapsed(), lat_ns: lat, before: u0, after: usage() }.print();
    }
    println!(
        "end: VmRSS={} KiB RssAnon={} KiB RssFile={} KiB sqlite3_memory_used={} KiB highwater={} KiB",
        status_kib("VmRSS:"), status_kib("RssAnon:"), status_kib("RssFile:"), sqlite_memory_used() / 1024,
        unsafe { libsqlite3_sys::sqlite3_memory_highwater(0) } / 1024
    );
    println!("cache budget: {} MiB (GONVEX_SQLITE_CACHE_BUDGET_MB)", gonvex_store::sqlite_cache_budget_mib());
}
