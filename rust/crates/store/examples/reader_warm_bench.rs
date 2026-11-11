//! Reader warmth and startup micro-driver over real tenant files.
//!
//! `LAB_DATA` is a copy of a runtime data directory (`control.db` plus
//! `projects/<id>/tenants/*.db`); the driver never writes outside it.
//! `MODE` selects the measurement:
//!
//! * `setup`: raw connection setup on one tenant file, one statement at a
//!   time (open, each pragma sqlx sends, first query), then `CONCURRENT`
//!   (default 13) simultaneous opens on distinct tenants. Run under
//!   `taskset -c 0` to model a 1-vCPU host.
//! * `startup`: the store half of `Runtime::start`: writer open (capture
//!   catalog) plus `provision_tenant_database` with no new migrations for
//!   every tenant, 8 at a time, as the runtime does.
//! * `idle`: `ROUNDS` (default 4) rounds separated by `GAP_S` (default 20)
//!   seconds of idleness. Each round touches every tenant the way crons and
//!   page loads do: `BURST` (default 4) concurrent read transactions per
//!   tenant, then `FOLLOW` (default 5) single reads 200 ms apart. Prints
//!   read-transaction open latency (pool admission + reader acquire, which
//!   includes connection setup when a reader must be opened) p50/p99/max
//!   per round, open readers and SQLite heap. `READERS` (default 2, the
//!   runtime default on one vCPU) caps readers per database.
//! * `burst`: the staging pattern of 2026-11-04 (a page load or reconnect
//!   opening every subscribed collection at once). Every tenant's writer is
//!   open (as in the runtime); `TARGETS` (default 2) tenants get `BURST`
//!   (default 14) concurrent read transactions per round, each holding its
//!   reader for one query plus `HOLD_MS` (default 5), after `GAP_S` (default
//!   30) idle seconds. Prints read-transaction open (= `snapshot.begin`)
//!   p50/p99/max, reader connections opened, opens over 2 s (sqlx's slow
//!   acquire warning) and SQLite heap. Pool settings come from the
//!   environment (`GONVEX_DB_WARM_READER_MS`, `GONVEX_DB_READER_PATIENCE_MS`,
//!   `GONVEX_DB_READER_OPENS`, `IDLE_MS`, `READERS` default 4).
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gonvex_store::{begin_transaction, ControlPlane, PoolLimits, PoolRegistry, TenantRoute};

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn tenant_files(data: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for project in std::fs::read_dir(data.join("projects")).expect("projects dir") {
        let tenants = project.unwrap().path().join("tenants");
        let Ok(entries) = std::fs::read_dir(&tenants) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|extension| extension == "db") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn summary(label: &str, mut values: Vec<f64>) {
    values.sort_by(f64::total_cmp);
    println!(
        "{label:<34} n={:<5} p50={:>9.3} ms  p99={:>9.3} ms  max={:>9.3} ms  sum={:>9.1} ms",
        values.len(),
        quantile(&values, 0.5),
        quantile(&values, 0.99),
        values.last().copied().unwrap_or(0.0),
        values.iter().sum::<f64>()
    );
}

fn rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map(|pages| pages * 4096.0 / 1048576.0)
        .unwrap_or(0.0)
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

/// One read-only connection opened and set up the way the reader pool does,
/// one statement at a time.
fn setup_breakdown(path: &Path) -> Vec<(&'static str, f64)> {
    use rusqlite::{Connection, OpenFlags};
    let mut steps = Vec::new();
    let mmap = format!(
        "PRAGMA mmap_size = {}",
        env_or("MMAP_BYTES", 268_435_456u64)
    );
    let started = Instant::now();
    // sqlx runs each connection on its own worker thread.
    std::thread::spawn(|| ()).join().unwrap();
    steps.push(("thread spawn+join", ms(started.elapsed())));
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    steps.push(("open_v2", ms(started.elapsed())));
    for (name, sql) in [
        ("foreign_keys = ON", "PRAGMA foreign_keys = ON"),
        ("synchronous = NORMAL", "PRAGMA synchronous = NORMAL"),
        ("busy_timeout = 5000", "PRAGMA busy_timeout = 5000"),
        ("cache_size = -8192", "PRAGMA cache_size = -8192"),
        ("mmap_size (MMAP_BYTES)", mmap.as_str()),
        ("temp_store = MEMORY", "PRAGMA temp_store = MEMORY"),
        ("first SELECT", "SELECT count(*) FROM tasks"),
    ] {
        let step = Instant::now();
        connection.execute_batch(sql).unwrap();
        steps.push((name, ms(step.elapsed())));
    }
    let (mut schema, mut high) = (0, 0);
    unsafe {
        libsqlite3_sys::sqlite3_db_status(
            connection.handle(),
            libsqlite3_sys::SQLITE_DBSTATUS_SCHEMA_USED,
            &mut schema,
            &mut high,
            0,
        );
    }
    steps.push(("total", ms(started.elapsed())));
    steps.push(("schema KiB (not ms)", schema as f64 / 1024.0));
    steps
}

fn mode_setup(files: &[PathBuf]) {
    let repeats: usize = env_or("REPEATS", 20);
    let mut by_step: Vec<(&'static str, Vec<f64>)> = Vec::new();
    for index in 0..repeats {
        for (step, (name, value)) in setup_breakdown(&files[index % files.len()])
            .into_iter()
            .enumerate()
        {
            if by_step.len() <= step {
                by_step.push((name, Vec::new()));
            }
            by_step[step].1.push(value);
        }
    }
    println!("== sequential read-only connection setup ({repeats} opens, distinct files) ==");
    for (name, values) in by_step {
        summary(name, values);
    }
    let concurrent: usize = env_or("CONCURRENT", 13);
    let barrier = Arc::new(std::sync::Barrier::new(concurrent));
    let threads = (0..concurrent)
        .map(|index| {
            let path = files[index % files.len()].clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                setup_breakdown(&path)
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    println!("== {concurrent} concurrent opens (one thread each, like sqlx workers) ==");
    for step in 0..results[0].len() {
        summary(
            results[0][step].0,
            results.iter().map(|steps| steps[step].1).collect(),
        );
    }
}

async fn control(data: &Path, pools: &PoolRegistry) -> ControlPlane {
    let url = format!("sqlite://{}", data.join("control.db").display());
    ControlPlane::connect(&url, pools.clone(), Default::default())
        .await
        .expect("control plane")
}

fn routes(files: &[PathBuf]) -> Vec<TenantRoute> {
    files
        .iter()
        .map(|path| TenantRoute {
            project_id: "lab".into(),
            tenant_id: path.file_stem().unwrap().to_string_lossy().into_owned(),
            database_url: format!("sqlite://{}", path.display()),
        })
        .collect()
}

async fn mode_startup(data: &Path, files: &[PathBuf]) {
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = control(data, &pools).await;
    let started = Instant::now();
    let per_tenant = Arc::new(Mutex::new(Vec::new()));
    let concurrency: usize = env_or("CONCURRENCY", 8);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = Vec::new();
    for route in routes(files) {
        let (control, per_tenant, semaphore) =
            (control.clone(), per_tenant.clone(), semaphore.clone());
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let step = Instant::now();
            control
                .provision_tenant_database(route, Vec::new())
                .await
                .unwrap();
            per_tenant.lock().unwrap().push(ms(step.elapsed()));
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    println!(
        "== startup: {} tenants provisioned ({concurrency} at a time) in {:.1} ms; SQLite heap {:.1} MiB, RSS {:.1} MiB ==",
        files.len(),
        ms(started.elapsed()),
        gonvex_store::sqlite_memory_used() as f64 / 1048576.0,
        rss_mib()
    );
    summary("provision per tenant", per_tenant.lock().unwrap().clone());
    pools.close().await;
}

async fn mode_idle(files: &[PathBuf]) {
    let rounds: usize = env_or("ROUNDS", 4);
    let gap = Duration::from_secs(env_or("GAP_S", 20));
    let burst: usize = env_or("BURST", 4);
    let follow: usize = env_or("FOLLOW", 5);
    // The runtime's defaults: 15 s idle timeout and, on a 1-vCPU host,
    // two readers per database (`default_readers_per_database`).
    let readers: u32 = env_or("READERS", 2);
    let pools = PoolRegistry::new(PoolLimits {
        max_connections_per_database: readers,
        max_idle_connections_per_database: readers,
        idle_timeout: Duration::from_millis(env_or("IDLE_MS", 15_000)),
        ..PoolLimits::default()
    });
    let urls = routes(files)
        .into_iter()
        .map(|route| route.database_url)
        .collect::<Vec<_>>();
    // The runtime opens every tenant (writer) at startup.
    for url in &urls {
        pools.pool(url).await.unwrap();
    }
    println!(
        "== idle: {} tenants, {rounds} rounds, {}s gaps, burst {burst}, follow {follow}; after writers: heap {:.1} MiB (writer schemas {:.1} MiB) ==",
        urls.len(),
        gap.as_secs(),
        gonvex_store::sqlite_memory_used() as f64 / 1048576.0,
        gonvex_store::sqlite_writer_schema_heap() as f64 / 1048576.0
    );
    let mut all_burst = Vec::new();
    let mut all_follow = Vec::new();
    for round in 0..rounds {
        if round > 0 {
            tokio::time::sleep(gap).await;
        }
        let before = pools.snapshot().await.open_connections;
        let opens = Arc::new(Mutex::new(Vec::new()));
        let started = Instant::now();
        let mut tasks = Vec::new();
        for url in &urls {
            for _ in 0..burst {
                let (pools, url, opens) = (pools.clone(), url.clone(), opens.clone());
                tasks.push(tokio::spawn(async move {
                    let step = Instant::now();
                    let mut transaction = begin_transaction(&pools, &url, true).await.unwrap();
                    opens.lock().unwrap().push(ms(step.elapsed()));
                    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
                        .fetch_one(&mut **transaction.transaction())
                        .await
                        .unwrap();
                }));
            }
        }
        for task in tasks {
            task.await.unwrap();
        }
        let burst_ms = ms(started.elapsed());
        let mut follow_opens = Vec::new();
        for _ in 0..follow {
            tokio::time::sleep(Duration::from_millis(200)).await;
            for url in &urls {
                let step = Instant::now();
                let mut transaction = begin_transaction(&pools, url, true).await.unwrap();
                follow_opens.push(ms(step.elapsed()));
                let _: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
                    .fetch_one(&mut **transaction.transaction())
                    .await
                    .unwrap();
            }
        }
        let snapshot = pools.snapshot().await;
        println!(
            "round {round}: connections before {before} after {} | burst wall {burst_ms:.1} ms | heap {:.1} MiB, RSS {:.1} MiB",
            snapshot.open_connections,
            gonvex_store::sqlite_memory_used() as f64 / 1048576.0,
            rss_mib()
        );
        let opens = opens.lock().unwrap().clone();
        summary("  burst read open", opens.clone());
        summary("  follow-up read open", follow_opens.clone());
        if round > 0 {
            all_burst.extend(opens);
            all_follow.extend(follow_opens);
        }
    }
    println!("== rounds after an idle gap ==");
    summary("burst read open", all_burst);
    summary("follow-up read open", all_follow);
    for stage in gonvex_profiler::snapshot() {
        let name = stage.stage.info().name;
        if name.starts_with("db.") {
            println!(
                "profiler {name:<20} n={:<6} p50={:>8.3} ms p99={:>8.3} ms max={:>8.3} ms",
                stage.count,
                stage.quantile(0.5) as f64 / 1e6,
                stage.quantile(0.99) as f64 / 1e6,
                stage.max as f64 / 1e6
            );
        }
    }
    pools.close().await;
}

async fn mode_burst(files: &[PathBuf]) {
    let rounds: usize = env_or("ROUNDS", 4);
    let gap = Duration::from_secs(env_or("GAP_S", 30));
    let burst: usize = env_or("BURST", 14);
    let targets: usize = env_or("TARGETS", 2);
    let hold = Duration::from_millis(env_or("HOLD_MS", 5));
    let readers: u32 = env_or("READERS", 4);
    let limits = PoolLimits {
        max_connections_per_database: readers,
        max_idle_connections_per_database: readers,
        idle_timeout: Duration::from_millis(env_or("IDLE_MS", 15_000)),
        ..PoolLimits::default()
    };
    println!(
        "== burst: {} tenants open, {targets} targets x {burst} reads, {rounds} rounds, {}s gaps | readers {readers}, idle {:?}, warm {:?} x{}, patience {:?}, opens {} ==",
        files.len(),
        gap.as_secs(),
        limits.idle_timeout,
        limits.warm_reader_window,
        limits.warm_readers,
        limits.reader_patience,
        limits.reader_opens,
    );
    let pools = PoolRegistry::new(limits);
    let urls = routes(files)
        .into_iter()
        .map(|route| route.database_url)
        .collect::<Vec<_>>();
    for url in &urls {
        pools.pool(url).await.unwrap();
    }
    let targets = &urls[..targets.min(urls.len())];
    let mut all = Vec::new();
    let (mut all_opens, mut all_slow) = (0, 0);
    for round in 0..rounds {
        tokio::time::sleep(gap).await;
        let opens_before = gonvex_store::sqlite_reader_opens();
        let before = pools.snapshot().await.open_connections;
        let begins = Arc::new(Mutex::new(Vec::new()));
        let started = Instant::now();
        let mut tasks = Vec::new();
        for url in targets {
            for _ in 0..burst {
                let (pools, url, begins) = (pools.clone(), url.clone(), begins.clone());
                tasks.push(tokio::spawn(async move {
                    let step = Instant::now();
                    let mut transaction = begin_transaction(&pools, &url, true).await.unwrap();
                    begins.lock().unwrap().push(ms(step.elapsed()));
                    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
                        .fetch_one(&mut **transaction.transaction())
                        .await
                        .unwrap();
                    tokio::time::sleep(hold).await;
                }));
            }
        }
        for task in tasks {
            task.await.unwrap();
        }
        let wall = ms(started.elapsed());
        let opened = gonvex_store::sqlite_reader_opens() - opens_before;
        let begins = begins.lock().unwrap().clone();
        let slow = begins.iter().filter(|value| **value > 2_000.0).count();
        println!(
            "round {round}: connections before {before} | readers opened {opened} | begins > 2 s {slow} | burst wall {wall:.1} ms | heap {:.1} MiB",
            gonvex_store::sqlite_memory_used() as f64 / 1048576.0
        );
        summary("  read open (snapshot.begin)", begins.clone());
        all.extend(begins);
        all_opens += opened;
        all_slow += slow;
    }
    println!("== all rounds: readers opened {all_opens}, begins > 2 s {all_slow} ==");
    summary("read open (snapshot.begin)", all);
    for stage in gonvex_profiler::snapshot() {
        let name = stage.stage.info().name;
        if name.starts_with("db.reader") {
            println!(
                "profiler {name:<20} n={:<6} p50={:>8.3} ms p99={:>8.3} ms max={:>8.3} ms",
                stage.count,
                stage.quantile(0.5) as f64 / 1e6,
                stage.quantile(0.99) as f64 / 1e6,
                stage.max as f64 / 1e6
            );
        }
    }
    pools.close().await;
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // `RUST_LOG=sqlx::query=debug` prints every statement with its time.
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
    gonvex_profiler::init_from_env();
    let data = PathBuf::from(std::env::var("LAB_DATA").expect("LAB_DATA"));
    let files = tenant_files(&data);
    assert!(
        !files.is_empty(),
        "no tenant files under {}",
        data.display()
    );
    match std::env::var("MODE").unwrap_or_default().as_str() {
        "setup" => mode_setup(&files),
        "startup" => mode_startup(&data, &files).await,
        "burst" => mode_burst(&files).await,
        _ => mode_idle(&files).await,
    }
}
