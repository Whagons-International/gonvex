//! Stall driver: scheduler lag, socket echo and network-thread poll times
//! while the runtime opens snapshots and fans out feed events.
//!
//!   taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench stalls
//!
//! Pin to one CPU (taskset) to model a 1-vCPU container: the network runtime
//! has one worker and the offload pool sizes itself from the CPU affinity.
//! Compare `GONVEX_OFFLOAD=inline` (heavy work on the network thread, the
//! previous behaviour) with `runtime` (default), `blocking`, `threads`.
//!
//! Env: STALL_SCENARIOS (idle,opens,fanout,burst), STALL_SECONDS (8),
//! STALL_OPEN_RATE (opens/s, 10), STALL_UPDATE_RATE (task updates/s, 4),
//! STALL_BURSTS (concurrent opens per burst, "200"), BENCH_JSON_OUT.
//! See docs/performance/offload.md.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gonvex_runtime::offload::{offload, SnapshotAdmission};
use gonvex_runtime::perf_fixture::harness::{
    peak_live_bytes, reset_peak, rss_bytes, AllocSnapshot, CountingAlloc,
};
use gonvex_runtime::perf_fixture::stalls::{self, timed, Summary};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const BIG: usize = 0;
const TASKS: &str = "tasks.recentWorkspaceTasks";

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { tokio::spawn(run()).await.unwrap() });
}

struct Probes {
    stop: Arc<AtomicBool>,
    lag: tokio::task::JoinHandle<Vec<u64>>,
    echo: std::thread::JoinHandle<Vec<u64>>,
    started: Instant,
}

async fn start_probes() -> Probes {
    let stop = Arc::new(AtomicBool::new(false));
    stalls::start_recording();
    Probes {
        lag: stalls::lag_probe(stop.clone()),
        echo: stalls::echo_probe(stop.clone()).await,
        stop,
        started: Instant::now(),
    }
}

async fn stop_probes(probes: Probes, name: &str, mut extra: serde_json::Map<String, Value>) {
    probes.stop.store(true, Relaxed);
    let lag = probes.lag.await.unwrap();
    let echo = tokio::task::spawn_blocking(move || probes.echo.join().unwrap())
        .await
        .unwrap();
    let polls = stalls::stop_recording();
    let polls = Summary::of(polls.into_iter().map(u64::from).collect());
    let lag = Summary::of(lag);
    let echo = Summary::of(echo);
    let pool = offload();
    extra.insert("seconds".into(), json!(probes.started.elapsed().as_secs_f64()));
    let line = json!({
        "name": name,
        "strategy": format!("{:?}", pool.strategy()),
        "offloadThreads": pool.threads(),
        "cpus": std::thread::available_parallelism().map_or(0, |cpus| cpus.get()),
        "lag": lag.to_json(),
        "echo": echo.to_json(),
        "networkPolls": polls.to_json(),
        "extra": extra,
    });
    println!(
        "{name:<28} {:<8} lag p99 {:>7} us max {:>7} us | echo p99 {:>7} us max {:>7} us | poll p99 {:>6} us max {:>7} us >5ms {:>4}",
        format!("{:?}", pool.strategy()),
        lag.p99_us,
        lag.max_us,
        echo.p99_us,
        echo.max_us,
        polls.p99_us,
        polls.max_us,
        polls.over_5ms
    );
    println!("STALL_JSON {line}");
    if let Ok(path) = std::env::var("BENCH_JSON_OUT") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    }
}

async fn run() {
    stalls::mark_network_thread();
    let scenarios = std::env::var("STALL_SCENARIOS").unwrap_or("idle,opens,fanout,burst".into());
    let seconds: f64 = env("STALL_SECONDS", 8.0);
    let fixture = Arc::new(Fixture::build(FixtureOptions::whagons()).await.unwrap());
    let members = fixture.tenant(BIG).members.len();
    let tasks = fixture.tenant(BIG).tasks.len();
    let window = Duration::from_secs_f64(seconds);

    if scenarios.contains("overhead") {
        // Round trip of an empty job through the pool (scheduling overhead).
        let pool = offload();
        for (kind, spawn) in [("spawn", true), ("run", false)] {
            let mut samples = Vec::new();
            for _ in 0..20_000 {
                let started = Instant::now();
                if spawn {
                    pool.spawn(async {}).await;
                } else {
                    pool.run(|| ()).await;
                }
                samples.push(started.elapsed().as_nanos() as u64);
            }
            let summary = Summary::of(samples);
            println!(
                "overhead/{kind:<6} {:?}: p50 {} ns p99 {} ns",
                pool.strategy(),
                summary.p50_us,
                summary.p99_us
            );
            let line = json!({"name": format!("stall/overhead_{kind}"), "strategy": format!("{:?}", pool.strategy()), "ns": summary.to_json()});
            println!("STALL_JSON {line}");
            if let Ok(path) = std::env::var("BENCH_JSON_OUT") {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
                writeln!(file, "{line}").unwrap();
            }
        }
    }

    if scenarios.contains("idle") {
        let probes = start_probes().await;
        tokio::time::sleep(window).await;
        stop_probes(probes, "stall/idle", Default::default()).await;
    }

    if scenarios.contains("opens") {
        // (a) M snapshot opens per second of the 9k-task collection, cycling
        // through all 78 members (4 Admin, ... 21 GrantOnly); not retained.
        let rate: f64 = env("STALL_OPEN_RATE", 10.0);
        // Warm the interner/estimates the way a running server would be.
        for member in 0..members {
            let _ = stalls::socket_open(&fixture.runtime, &fixture.session(BIG, member), TASKS).await;
        }
        let probes = start_probes().await;
        let latencies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        let deadline = Instant::now() + window;
        let mut handles = Vec::new();
        let mut n = 0;
        while Instant::now() < deadline {
            interval.tick().await;
            let fixture = fixture.clone();
            let latencies = latencies.clone();
            let member = n % members;
            n += 1;
            handles.push(tokio::spawn(timed(async move {
                let started = Instant::now();
                let opened =
                    stalls::socket_open(&fixture.runtime, &fixture.session(BIG, member), TASKS)
                        .await
                        .unwrap();
                latencies
                    .lock()
                    .unwrap()
                    .push(started.elapsed().as_micros() as u64);
                opened.rows
            })));
        }
        let mut rows = 0;
        for handle in handles {
            rows += handle.await.unwrap();
        }
        let latencies = std::mem::take(&mut *latencies.lock().unwrap());
        let mut extra = serde_json::Map::new();
        extra.insert("opens".into(), json!(n));
        extra.insert("targetRate".into(), json!(rate));
        extra.insert("rows".into(), json!(rows));
        extra.insert("openLatency".into(), Summary::of(latencies).to_json());
        stop_probes(probes, &format!("stall/opens_{rate}_per_s"), extra).await;
    }

    if scenarios.contains("fanout") {
        // (b) task updates into the 78-member tenant, every member holding
        // all 10 subscriptions, each connection its own task.
        let rate: f64 = env("STALL_UPDATE_RATE", 4.0);
        let connections = fixture.connect_all(BIG).await.unwrap();
        let (sender, _) = tokio::sync::broadcast::channel(256);
        let counters = Arc::new(stalls::FanoutCounters::default());
        let mut loops = Vec::new();
        for connection in connections {
            loops.push(tokio::spawn(timed(stalls::connection_loop(
                fixture.runtime.clone(),
                connection,
                sender.subscribe(),
                counters.clone(),
            ))));
        }
        // Warm the apply-cost averages with two updates.
        for n in 0..2 {
            let event = fixture
                .update_task(BIG, n * 131 % tasks, json!({"description": format!("warm {n}")}))
                .await
                .unwrap();
            let _ = sender.send((Instant::now(), event));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        counters.latency_us.lock().unwrap().clear();
        let probes = start_probes().await;
        let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        let deadline = Instant::now() + window;
        let mut updates = 0;
        while Instant::now() < deadline {
            interval.tick().await;
            let task = (updates * 7919 + 13) % tasks;
            let event = fixture
                .update_task(BIG, task, json!({"description": format!("update {updates}")}))
                .await
                .unwrap();
            let _ = sender.send((Instant::now(), event));
            updates += 1;
        }
        // Let queued deliveries finish inside the window.
        let expected = (updates + 2) * loops.len();
        let drain = Instant::now();
        while counters.deliveries.load(Relaxed) < expected && drain.elapsed() < Duration::from_secs(60) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let latencies = std::mem::take(&mut *counters.latency_us.lock().unwrap());
        let mut extra = serde_json::Map::new();
        extra.insert("updates".into(), json!(updates));
        extra.insert("targetRate".into(), json!(rate));
        extra.insert("deliveries".into(), json!(counters.deliveries.load(Relaxed)));
        extra.insert("messages".into(), json!(counters.messages.load(Relaxed)));
        extra.insert("errors".into(), json!(counters.errors.load(Relaxed)));
        extra.insert("deliveryLatency".into(), Summary::of(latencies).to_json());
        stop_probes(probes, &format!("stall/fanout_{rate}_updates_per_s"), extra).await;
        drop(sender);
        for handle in loops {
            drop(handle.await.unwrap());
        }
    }

    if scenarios.contains("burst") {
        // (c) N concurrent opens of the 9k-task collection across all
        // members, every subscription retained (as reconnecting sockets do).
        let bursts = std::env::var("STALL_BURSTS").unwrap_or("200".into());
        for burst in bursts.split(',').filter_map(|value| value.trim().parse::<usize>().ok()) {
            let _ = SnapshotAdmission::global().take_peak_reserved_bytes();
            let base = reset_peak();
            let rss_before = rss_bytes();
            let before = AllocSnapshot::now();
            let probes = start_probes().await;
            let started = Instant::now();
            let mut handles = Vec::new();
            for n in 0..burst {
                let fixture = fixture.clone();
                handles.push(tokio::spawn(timed(async move {
                    let opened_at = Instant::now();
                    let opened =
                        stalls::socket_open(&fixture.runtime, &fixture.session(BIG, n % members), TASKS)
                            .await
                            .unwrap();
                    (opened, opened_at.elapsed().as_micros() as u64)
                })));
            }
            let mut held = Vec::new();
            let mut latencies = Vec::new();
            let mut rows = 0;
            for handle in handles {
                let (opened, latency) = handle.await.unwrap();
                rows += opened.rows;
                latencies.push(latency);
                held.push(opened.subscription);
            }
            let elapsed = started.elapsed();
            let peak = peak_live_bytes() - base;
            let retained = AllocSnapshot::now().since(before).live_bytes;
            let mut extra = serde_json::Map::new();
            extra.insert("opens".into(), json!(burst));
            extra.insert("rows".into(), json!(rows));
            extra.insert("burstMs".into(), json!(elapsed.as_secs_f64() * 1e3));
            extra.insert("openLatency".into(), Summary::of(latencies).to_json());
            extra.insert("peakHeapBytes".into(), json!(peak));
            extra.insert("retainedHeapBytes".into(), json!(retained));
            extra.insert("transientPeakBytes".into(), json!(peak - retained));
            extra.insert(
                "rssDeltaBytes".into(),
                json!(rss_bytes().zip(rss_before).map(|(after, before)| after as i64 - before as i64)),
            );
            extra.insert(
                "peakReservedSnapshotBytes".into(),
                json!(SnapshotAdmission::global().take_peak_reserved_bytes()),
            );
            extra.insert(
                "snapshotBudgetBytes".into(),
                json!(SnapshotAdmission::global().budget_bytes()),
            );
            stop_probes(probes, &format!("stall/burst_{burst}_opens"), extra).await;
            drop(held);
        }
    }
    match Arc::try_unwrap(fixture) {
        Ok(fixture) => drop(fixture),
        Err(_) => {}
    }
}
