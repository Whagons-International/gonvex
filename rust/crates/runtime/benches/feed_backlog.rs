//! Change-feed backlog driver: heap held for a stalled connection and the
//! delivery latency of connections that keep up.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench feed_backlog
//!
//! FEED_FAST receivers (default 40) receive every transaction as it is
//! broadcast; FEED_SLOW receivers (default 1) are subscribed but not polled
//! while FEED_WRITES transactions (default 1,500) of FEED_ROWS task updates
//! each (default 50, ~400-byte descriptions, both images in the log) are
//! committed back to back, then drain. Reports live heap (counting allocator)
//! during the writes, fast-receiver latency commit → receive, and what the
//! stalled receivers got (events in order, lag, resets). Builds on the base
//! (broadcast of parsed events) and on perf/feed-backlog alike.
//! Env: BENCH_JSON_OUT, GONVEX_FEED_BACKLOG_BYTES (after only).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::perf_fixture::harness::{
    peak_live_bytes, reset_peak, AllocSnapshot, CountingAlloc,
};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};
use serde_json::json;

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn quantile(values: &mut [u64], q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[((values.len() - 1) as f64 * q).round() as usize]
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    if std::env::var_os("GONVEX_SQLITE_SYNCHRONOUS").is_none() {
        std::env::set_var("GONVEX_SQLITE_SYNCHRONOUS", "normal");
    }
    let fast_count: usize = env("FEED_FAST", 40);
    let slow_count: usize = env("FEED_SLOW", 1);
    let writes: usize = env("FEED_WRITES", 1_500);
    let rows: usize = env("FEED_ROWS", 50);
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    let route = fixture.tenant(0).route.clone();
    let tasks: Vec<String> = fixture
        .tenant(0)
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect();

    // Commit instants by revision, filled by the writer.
    let committed: Arc<Mutex<std::collections::HashMap<u64, Instant>>> = Arc::default();
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::default();
    let mut fast = Vec::new();
    for _ in 0..fast_count {
        let mut receiver = fixture.runtime.perf_subscribe_change_feed(&route).await;
        let committed = Arc::clone(&committed);
        let latencies = Arc::clone(&latencies);
        fast.push(tokio::spawn(async move {
            let mut received = 0usize;
            let mut lagged = 0usize;
            loop {
                match receiver.recv().await {
                    Ok(FeedEvent::Transaction {
                        revision, changes, ..
                    }) => {
                        let now = Instant::now();
                        received += 1;
                        // Touch the images as an apply would.
                        std::hint::black_box(
                            changes
                                .iter()
                                .filter(|c| c.new_value.get("_id").is_some())
                                .count(),
                        );
                        if let Some(at) = committed.lock().unwrap().get(&revision) {
                            latencies
                                .lock()
                                .unwrap()
                                .push(now.saturating_duration_since(*at).as_micros() as u64);
                        }
                    }
                    Ok(FeedEvent::Reset { .. }) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => lagged += 1,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
                if received == usize::MAX {
                    break;
                }
                if received >= writes_target() {
                    break;
                }
            }
            (received, lagged)
        }));
    }
    let mut slow = Vec::new();
    for _ in 0..slow_count {
        slow.push(fixture.runtime.perf_subscribe_change_feed(&route).await);
    }
    WRITES_TARGET.store(writes, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let base = AllocSnapshot::now();
    reset_peak();
    let mut heap_samples = Vec::new();
    let started = Instant::now();
    let mut last_revision = 0;
    for i in 0..writes {
        let placeholders = (0..rows)
            .map(|r| format!("${}", r + 2))
            .collect::<Vec<_>>()
            .join(",");
        let statement =
            format!(r#"UPDATE "tasks" SET "description"=$1 WHERE "_id" IN ({placeholders})"#);
        let mut transaction =
            gonvex_store::begin_transaction(fixture.pools(), &route.database_url, false)
                .await
                .unwrap();
        let mut query =
            sqlx::query(&statement).bind(format!("feed backlog write {i} {}", "x".repeat(380)));
        for r in 0..rows {
            query = query.bind(tasks[(i * rows + r) % tasks.len()].clone());
        }
        query
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let revision = fixture.revision(0).await.unwrap();
        committed.lock().unwrap().insert(revision, Instant::now());
        last_revision = revision;
        if i % 100 == 99 {
            let now = AllocSnapshot::now();
            heap_samples.push(now.live_bytes.saturating_sub(base.live_bytes));
        }
    }
    let write_secs = started.elapsed().as_secs_f64();
    // Fast receivers finish.
    let mut fast_received = Vec::new();
    for handle in fast {
        fast_received.push(
            tokio::time::timeout(Duration::from_secs(120), handle)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let held = AllocSnapshot::now()
        .live_bytes
        .saturating_sub(base.live_bytes);
    let peak = peak_live_bytes().saturating_sub(base.live_bytes);

    // A call committed while the stalled receivers are behind: its result
    // waits for the connection's watermark, i.e. until its feed reaches the
    // call's revision.
    let save_started = Instant::now();
    {
        let mut transaction =
            gonvex_store::begin_transaction(fixture.pools(), &route.database_url, false)
                .await
                .unwrap();
        sqlx::query(r#"UPDATE "tasks" SET "description"=$1 WHERE "_id"=$2"#)
            .bind("feed backlog save")
            .bind(tasks[0].clone())
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
    let save_revision = fixture.revision(0).await.unwrap();
    let last_revision = save_revision;
    // Stalled receivers drain.
    reset_peak();
    let drain_started = Instant::now();
    let mut slow_results = Vec::new();
    let mut drains = Vec::new();
    for mut receiver in slow {
        drains.push(tokio::spawn(async move {
        let (mut events, mut lagged, mut resets, mut ordered, mut last, mut changes) = (0, 0, 0, true, 0u64, 0usize);
        let mut save_ms = None;
        let mut first_lag_ms = None;
        loop {
            match tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await {
                Ok(Ok(FeedEvent::Transaction { revision, changes: c, .. })) => {
                    events += 1;
                    changes += c.len();
                    ordered &= revision > last;
                    last = revision;
                    if revision >= save_revision && save_ms.is_none() {
                        save_ms = Some(save_started.elapsed().as_secs_f64() * 1e3);
                    }
                    if revision >= last_revision {
                        break;
                    }
                }
                Ok(Ok(FeedEvent::Reset { .. })) => resets += 1,
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                    // The base resets the connection here: its subscriptions reopen.
                    lagged += 1;
                    first_lag_ms.get_or_insert(save_started.elapsed().as_secs_f64() * 1e3);
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        json!({"events": events, "changes": changes, "ordered": ordered, "lastRevision": last, "lagged": lagged, "resets": resets, "saveVisibleMs": save_ms, "lagAtMs": first_lag_ms})
        }));
    }
    // Stalled connections catch up concurrently, as on a server.
    for drain in drains {
        slow_results.push(drain.await.unwrap());
    }
    let drain_ms = drain_started.elapsed().as_secs_f64() * 1e3;
    let drain_peak = peak_live_bytes().saturating_sub(base.live_bytes);
    let mut latency = latencies.lock().unwrap().clone();
    let result = json!({
        "bench": "feed_backlog",
        "fast": fast_count, "slowCount": slow_count, "writes": writes, "rowsPerWrite": rows,
        "writeSecs": write_secs,
        "heapDuringWritesMiB": heap_samples.iter().map(|b| *b as f64 / 1048576.0).collect::<Vec<_>>(),
        "heapHeldAfterWritesMiB": held as f64 / 1048576.0,
        "heapPeakMiB": peak as f64 / 1048576.0,
        "fastLatencyUs": {"p50": quantile(&mut latency, 0.5), "p95": quantile(&mut latency, 0.95), "p99": quantile(&mut latency, 0.99), "max": quantile(&mut latency, 1.0), "n": latency.len()},
        "fastReceived": fast_received.iter().map(|r| r.0).sum::<usize>(),
        "fastLagged": fast_received.iter().map(|r| r.1).sum::<usize>(),
        "slow": slow_results,
        "slowDrainMs": drain_ms,
        "heapPeakDrainMiB": drain_peak as f64 / 1048576.0,
        "backlog": backlog_metrics(),
    });
    println!("{result}");
    if let Ok(path) = std::env::var("BENCH_JSON_OUT") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{result}").unwrap();
    }
}

static WRITES_TARGET: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(usize::MAX);
fn writes_target() -> usize {
    WRITES_TARGET.load(std::sync::atomic::Ordering::Relaxed)
}

fn backlog_metrics() -> serde_json::Value {
    gonvex_runtime::change_feed::backlog::metrics()
}
