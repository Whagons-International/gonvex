//! Per-thread nice values of SQLite connection workers do not depend on the
//! thread that opened them: a database first opened from a niced runtime
//! (like the offload pool) still gets its writer worker at the process base
//! and its reader workers at base + GONVEX_SQLITE_READER_NICE. Linux only;
//! own test binary (the reader nice is process-wide).
#![cfg(target_os = "linux")]

use gonvex_profiler::thread_priority as priority;
use gonvex_store::{database_url, PoolLimits, PoolRegistry};

/// (comm, nice) of every thread of this process.
fn threads() -> Vec<(String, i32)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else { continue };
        // pid (comm) state ppid ... field 19 = nice; comm may contain spaces.
        let close = stat.rfind(')').unwrap();
        let comm = stat[stat.find('(').unwrap() + 1..close].to_owned();
        let fields: Vec<&str> = stat[close + 2..].split(' ').collect();
        out.push((comm, fields[16].parse().unwrap()));
    }
    out
}

fn sqlx_workers() -> Vec<i32> {
    threads().into_iter().filter(|(comm, _)| comm.starts_with("sqlx-sqlite")).map(|(_, nice)| nice).collect()
}

#[test]
fn writer_workers_stay_at_base_and_readers_at_reader_nice_whoever_opens_them() {
    std::env::set_var("GONVEX_SQLITE_READER_NICE", "8");
    priority::init();
    let base = priority::base();
    // Readers can only be raised above their creator's value (lowering needs
    // CAP_SYS_NICE), so the reader nice here (8) is above the opener's (6), as
    // in production (reader 10, offload pool 10).
    if base > 11 {
        eprintln!("process base nice {base}: no room to test");
        return;
    }
    // The base runtime writers open on (the network runtime in the binary).
    let home = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
    gonvex_store::reader_priority::set_home_runtime(home.handle().clone());
    // A niced runtime, as the offload pool is.
    let niced = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .on_thread_start(|| {
            priority::set_current_target(6);
        })
        .build()
        .unwrap();
    let dir = std::env::temp_dir().join(format!("gonvex-thread-priority-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = database_url(&dir.join("tenant.db"));
    let registry = PoolRegistry::new(PoolLimits::default());
    let registry = std::sync::Arc::new(registry);
    // Spawned (not block_on, which runs on the calling thread): on the niced worker.
    let (r, u) = (registry.clone(), url.clone());
    niced.block_on(niced.spawn(async move {
        assert_eq!(priority::current(), base + 6, "the opening thread is niced");
        let writer = r.pool(&u).await.unwrap();
        sqlx::query("CREATE TABLE t(id INTEGER PRIMARY KEY)").execute(&writer).await.unwrap();
    }))
    .unwrap();
    let writers = sqlx_workers();
    assert_eq!(writers, vec![base], "writer worker at base, not inherited base + 6: {:?}", threads());
    let (r, u) = (registry.clone(), url.clone());
    niced.block_on(niced.spawn(async move {
        let reader = r.reader(&u).await.unwrap();
        let _: i64 = sqlx::query_scalar("SELECT count(*) FROM t").fetch_one(&reader).await.unwrap();
    }))
    .unwrap();
    let mut workers = sqlx_workers();
    workers.sort();
    assert_eq!(workers, vec![base, base + 8], "writer at base, reader at base + 8 (raised from the creator's base + 6): {:?}", threads());
    home.block_on(registry.close());
    let _ = std::fs::remove_dir_all(&dir);
}
