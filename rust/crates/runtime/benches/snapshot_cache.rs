//! Shared snapshot cache (base + change-log delta) at el-rey size: cost per
//! open fresh vs cache hit vs hit + 1k-change delta, resume cost at 0/10/200
//! changes, cache memory, and a reconnect storm before vs after.
//!
//!   taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench snapshot_cache
//!
//! "before" is the unchanged SQL path (`open_replica_fresh`, or the cache
//! switched off for the storm); "after" is the default path through the cache.
//! Env: BENCH_FILTER (open,resume,memory,storm), BENCH_ITERS (5),
//! BENCH_TASKS (10500), BENCH_MEMBERS (78), STORM_OPENS (1500),
//! STORM_SECONDS (10), STORM_MODES (cold,resume), STORM_UPDATE_RATE (4/s),
//! BENCH_JSON_OUT. See docs/performance/resume-and-bounded-replicas.md.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gonvex_protocol::{ReplicaCursor, ReplicaOpenRequest, ServerMessage};
use gonvex_runtime::perf_fixture::harness::{
    peak_live_bytes, reset_peak, rss_bytes, AllocSnapshot, CountingAlloc,
};
use gonvex_runtime::perf_fixture::stalls::{self, timed, Summary};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, SqlValue, TenantSpec, WHAGONS_SUBSCRIPTIONS,
};
use gonvex_store::TenantSession;
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const TENANT: usize = 0;
const TASKS: &str = "tasks.recentWorkspaceTasks";

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn enabled(name: &str) -> bool {
    std::env::var("BENCH_FILTER").map_or(true, |filter| {
        filter.split(',').any(|part| name.contains(part.trim()))
    })
}

fn record(name: &str, values: Value) {
    let line = json!({"name": name, "values": values});
    println!("BENCH_JSON {line}");
    if let Ok(path) = std::env::var("BENCH_JSON_OUT") {
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(file, "{line}");
        }
    }
}

/// Process CPU (user + system) in nanoseconds.
fn process_cpu_ns() -> u64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid out pointer.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let ns = |time: libc::timeval| time.tv_sec as u64 * 1_000_000_000 + time.tv_usec as u64 * 1000;
    ns(usage.ru_utime) + ns(usage.ru_stime)
}

struct Sample {
    wall_ns: u64,
    cpu_ns: u64,
    allocations: u64,
    peak: i64,
}

async fn measure<T>(op: impl std::future::Future<Output = T>) -> (T, Sample) {
    let base = reset_peak();
    let before = AllocSnapshot::now();
    let cpu = process_cpu_ns();
    let started = Instant::now();
    let value = op.await;
    let sample = Sample {
        wall_ns: started.elapsed().as_nanos() as u64,
        cpu_ns: process_cpu_ns() - cpu,
        allocations: AllocSnapshot::now().since(before).allocations,
        peak: peak_live_bytes() - base,
    };
    (value, sample)
}

fn summarize(samples: &[Sample]) -> Value {
    let median = |mut values: Vec<u64>| {
        values.sort_unstable();
        values[values.len() / 2]
    };
    json!({
        "iterations": samples.len(),
        "median_ms": median(samples.iter().map(|s| s.wall_ns).collect()) as f64 / 1e6,
        "cpu_ms_median": median(samples.iter().map(|s| s.cpu_ns).collect()) as f64 / 1e6,
        "allocations_median": median(samples.iter().map(|s| s.allocations).collect()),
        "peak_heap_bytes_max": samples.iter().map(|s| s.peak).max().unwrap_or(0),
    })
}

fn request(path: &str, resume: Option<&(ReplicaCursor, String)>) -> ReplicaOpenRequest {
    let mut request = json!({"id": path, "path": path, "args": {}});
    if let Some((cursor, digest)) = resume {
        request["cursor"] = serde_json::to_value(cursor).unwrap();
        request["digest"] = json!(digest);
    }
    serde_json::from_value(request).unwrap()
}

fn ready_of(messages: &[ServerMessage]) -> Option<(ReplicaCursor, String)> {
    messages.iter().rev().find_map(|message| match message {
        ServerMessage::ReplicaReady { cursor, digest, .. } => {
            Some((cursor.clone(), digest.clone()))
        }
        _ => None,
    })
}

fn rows_of(messages: &[ServerMessage]) -> usize {
    messages
        .iter()
        .map(|message| match message {
            ServerMessage::ReplicaSnapshot { result, .. } => result.len(),
            _ => 0,
        })
        .sum()
}

/// Commits `count` task updates as one transaction (one revision, `count`
/// change-log rows).
async fn update_tasks(fixture: &Fixture, count: usize, salt: usize) {
    let tasks = fixture.tenant(TENANT).tasks.len();
    let offset = (salt * 7919) % tasks.saturating_sub(count).max(1);
    fixture
        .commit(
            TENANT,
            r#"UPDATE "tasks" SET "name" = "name" || '.', "updatedAt" = "updatedAt" + 1
               WHERE "_id" IN (SELECT "_id" FROM "tasks" ORDER BY "_id" LIMIT $1 OFFSET $2)"#,
            vec![SqlValue::Int(count as i64), SqlValue::Int(offset as i64)],
        )
        .await
        .unwrap();
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { tokio::spawn(run()).await.unwrap() });
}

async fn run() {
    stalls::mark_network_thread();
    let tasks: usize = env("BENCH_TASKS", 10_500);
    let members: usize = env("BENCH_MEMBERS", 78);
    let iterations: usize = env("BENCH_ITERS", 5);
    let mut options = FixtureOptions::whagons();
    options.tenants = vec![TenantSpec::new("elrey", tasks, members)];
    let started = Instant::now();
    let fixture = Fixture::build(options).await.unwrap();
    eprintln!(
        "fixture ({tasks} tasks, {members} members) ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let runtime = fixture.runtime.clone();
    let kinds = [
        ("Admin", MemberKind::Admin),
        ("TeamAllSpots", MemberKind::TeamAllSpots),
        ("GrantOnly", MemberKind::GrantOnly),
    ];

    // ---- Cost per open ------------------------------------------------------
    for (label, kind) in kinds {
        let name = format!("open/{TASKS}/{label}");
        if !enabled(&name) {
            continue;
        }
        let session = fixture.session(TENANT, fixture.members_of_kind(TENANT, kind)[0]);
        // Warm parent ACL indexes and SQLite caches.
        let _ = runtime
            .open_replica_fresh(&session, request(TASKS, None))
            .await
            .unwrap();
        let mut fresh = Vec::new();
        let mut rows = 0;
        for _ in 0..iterations {
            let (opened, sample) =
                measure(runtime.open_replica_fresh(&session, request(TASKS, None))).await;
            rows = rows_of(&opened.unwrap().messages);
            fresh.push(sample);
        }
        let mut miss = Vec::new();
        for _ in 0..iterations {
            runtime.clear_snapshot_cache();
            let (_, sample) = measure(runtime.open_replica(&session, request(TASKS, None))).await;
            miss.push(sample);
        }
        let mut hit = Vec::new();
        for _ in 0..iterations {
            let (opened, sample) =
                measure(runtime.open_replica(&session, request(TASKS, None))).await;
            assert_eq!(rows_of(&opened.unwrap().messages), rows);
            hit.push(sample);
        }
        let mut delta = Vec::new();
        for i in 0..iterations {
            update_tasks(&fixture, 1000, i).await;
            let (_, sample) = measure(runtime.open_replica(&session, request(TASKS, None))).await;
            delta.push(sample);
        }
        let mut fresh_after_delta = Vec::new();
        for i in 0..iterations {
            update_tasks(&fixture, 1000, i + 100).await;
            let (_, sample) =
                measure(runtime.open_replica_fresh(&session, request(TASKS, None))).await;
            fresh_after_delta.push(sample);
        }
        record(
            &name,
            json!({
                "rows": rows,
                "fresh": summarize(&fresh),
                "cache_miss_build": summarize(&miss),
                "cache_hit": summarize(&hit),
                "cache_hit_plus_1k_changes": summarize(&delta),
                "fresh_after_1k_changes": summarize(&fresh_after_delta),
                "cache": format!("{:?}", runtime.snapshot_cache_stats()),
            }),
        );
    }

    // ---- Resume from a client cursor -----------------------------------------
    gonvex_profiler::reset();
    for (label, kind) in kinds {
        for changes in [0usize, 10, 200] {
            let name = format!("resume/{TASKS}/{label}/after_{changes}_changes");
            if !enabled(&name) {
                continue;
            }
            let session = fixture.session(TENANT, fixture.members_of_kind(TENANT, kind)[0]);
            let mut before = Vec::new();
            let mut after = Vec::new();
            let mut need_hashes = 0;
            for i in 0..iterations {
                for (samples, fresh) in [(&mut before, true), (&mut after, false)] {
                    let opened = runtime
                        .open_replica(&session, request(TASKS, None))
                        .await
                        .unwrap();
                    let token = ready_of(&opened.messages).unwrap();
                    if changes > 0 {
                        update_tasks(&fixture, changes, i * 2 + usize::from(fresh)).await;
                    }
                    let (opened, sample) = measure(async {
                        if fresh {
                            runtime
                                .open_replica_fresh(&session, request(TASKS, Some(&token)))
                                .await
                        } else {
                            runtime
                                .open_replica(&session, request(TASKS, Some(&token)))
                                .await
                        }
                    })
                    .await;
                    let opened = opened.unwrap();
                    need_hashes += opened
                        .messages
                        .iter()
                        .filter(|message| {
                            matches!(message, ServerMessage::ReplicaNeedHashes { .. })
                        })
                        .count();
                    samples.push(sample);
                }
            }
            record(
                &name,
                json!({
                    "before_sql_scan": summarize(&before),
                    "after_cache_plus_log": summarize(&after),
                    "need_hashes": need_hashes,
                }),
            );
        }
    }

    // Where an open's fixed cost goes (both paths; profiler stages).
    let stages = gonvex_profiler::snapshot()
        .into_iter()
        .filter(|stage| stage.name().starts_with("snapshot.") && stage.count > 0)
        .map(|stage| {
            (
                stage.name().to_owned(),
                json!({"count": stage.count, "mean_us": stage.sum / stage.count / 1000, "p50_us": stage.quantile(0.5) / 1000, "max_us": stage.max / 1000}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    if !stages.is_empty() {
        record(
            "profiler/snapshot_stages_during_resume",
            Value::Object(stages),
        );
    }

    // ---- Memory of cache entries ---------------------------------------------
    if enabled("memory") {
        let session = fixture.session(
            TENANT,
            fixture.members_of_kind(TENANT, MemberKind::Admin)[0],
        );
        let mut entries = serde_json::Map::new();
        for path in WHAGONS_SUBSCRIPTIONS {
            let _ = runtime
                .open_replica_fresh(&session, request(path, None))
                .await
                .unwrap();
            runtime.clear_snapshot_cache();
            let live = AllocSnapshot::now().live_bytes;
            let opened = runtime
                .open_replica(&session, request(path, None))
                .await
                .unwrap();
            let rows = rows_of(&opened.messages);
            drop(opened);
            let measured = AllocSnapshot::now().live_bytes - live;
            let stats = runtime.snapshot_cache_stats();
            entries.insert(
                path.to_string(),
                json!({
                    "rows": rows,
                    "measured_live_bytes": measured,
                    "estimated_bytes": stats.bytes,
                    "bytes_per_row": if rows > 0 { measured / rows as i64 } else { 0 },
                }),
            );
        }
        runtime.clear_snapshot_cache();
        for path in WHAGONS_SUBSCRIPTIONS {
            let _ = runtime
                .open_replica(&session, request(path, None))
                .await
                .unwrap();
        }
        let stats = runtime.snapshot_cache_stats();
        record(
            "memory/entries",
            json!({"per_collection": entries, "all_collections": format!("{stats:?}")}),
        );
    }

    // ---- Reconnect storm -------------------------------------------------------
    if enabled("storm") {
        let opens: usize = env("STORM_OPENS", 1500);
        let seconds: f64 = env("STORM_SECONDS", 10.0);
        let rate: f64 = env("STORM_UPDATE_RATE", 4.0);
        let modes = std::env::var("STORM_MODES").unwrap_or("cold,resume".into());
        let connections = opens / WHAGONS_SUBSCRIPTIONS.len();
        // Visibility classes of the members' task subscriptions.
        let mut held = Vec::new();
        for member in 0..members {
            held.push(
                fixture
                    .connect_paths(TENANT, member, &[TASKS])
                    .await
                    .unwrap(),
            );
        }
        let classes = Fixture::visibility_handles(&mut held, TASKS)
            .into_iter()
            .flatten()
            .map(|member| member.class().id())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        drop(held);
        for mode in modes.split(',') {
            for cache in [false, true] {
                let name = format!(
                    "storm/{mode}/{opens}_opens_{seconds}s/{}",
                    if cache { "after_cache" } else { "before_sql" }
                );
                runtime.set_snapshot_cache_enabled(cache);
                runtime.clear_snapshot_cache();
                // Client state of a resume: each (member, collection)'s ready.
                let mut tokens = BTreeMap::new();
                if mode == "resume" {
                    for member in 0..members {
                        let session = fixture.session(TENANT, member);
                        for path in WHAGONS_SUBSCRIPTIONS {
                            let opened =
                                stalls::socket_open(&runtime, &session, path).await.unwrap();
                            tokens.insert((member, path), opened.ready.unwrap());
                        }
                    }
                    update_tasks(&fixture, 200, 7).await;
                }
                runtime.clear_snapshot_cache();
                let tokens = Arc::new(tokens);
                let before = runtime.snapshot_cache_stats();
                let stop = Arc::new(AtomicBool::new(false));
                let peak_rss = Arc::new(AtomicU64::new(0));
                let rss_base = rss_bytes().unwrap_or(0);
                let sampler = {
                    let stop = stop.clone();
                    let peak_rss = peak_rss.clone();
                    std::thread::spawn(move || {
                        while !stop.load(Relaxed) {
                            peak_rss.fetch_max(rss_bytes().unwrap_or(0), Relaxed);
                            std::thread::sleep(Duration::from_millis(20));
                        }
                    })
                };
                let writer = {
                    let stop = stop.clone();
                    let fixture = &fixture;
                    async move {
                        let mut interval =
                            tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
                        let (mut n, mut errors) = (0, 0);
                        while !stop.load(Relaxed) {
                            interval.tick().await;
                            if let Err(error) = fixture
                                .update_task(
                                    TENANT,
                                    (n * 7919 + 13) % tasks,
                                    json!({"description": format!("storm {n}")}),
                                )
                                .await
                            {
                                eprintln!("storm writer: {error}");
                                errors += 1;
                            }
                            n += 1;
                        }
                        (n, errors)
                    }
                };
                stalls::start_recording();
                let cpu = process_cpu_ns();
                let started = Instant::now();
                let mut handles = Vec::new();
                for connection in 0..connections {
                    let member = connection % members;
                    let session: TenantSession = fixture.session(TENANT, member);
                    let runtime = runtime.clone();
                    let tokens = tokens.clone();
                    let at =
                        Duration::from_secs_f64(seconds * connection as f64 / connections as f64);
                    handles.push(tokio::spawn(timed(gonvex_profiler::instrument(
                        gonvex_profiler::TaskKind::WsConnection,
                        async move {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(started + at))
                                .await;
                            let opens = WHAGONS_SUBSCRIPTIONS.iter().map(|path| {
                                let request = request(path, tokens.get(&(member, *path)));
                                let runtime = runtime.clone();
                                let session = session.clone();
                                async move {
                                    let begun = Instant::now();
                                    let opened =
                                        stalls::socket_open_request(&runtime, &session, request)
                                            .await;
                                    let opened = opened.unwrap();
                                    (
                                        begun.elapsed().as_micros() as u64,
                                        opened.need_hashes,
                                        opened.bytes,
                                    )
                                }
                            });
                            futures_util::future::join_all(opens).await
                        },
                    ))));
                }
                let all = async {
                    let mut latencies = Vec::new();
                    let mut need_hashes = 0;
                    let mut bytes = 0;
                    for handle in handles {
                        for (latency, need, sent) in handle.await.unwrap() {
                            latencies.push(latency);
                            need_hashes += usize::from(need);
                            bytes += sent;
                        }
                    }
                    stop.store(true, Relaxed);
                    (latencies, need_hashes, bytes)
                };
                let ((latencies, need_hashes, bytes), updates) = tokio::join!(all, writer);
                let elapsed = started.elapsed();
                let cpu = process_cpu_ns() - cpu;
                let polls = stalls::stop_recording();
                sampler.join().unwrap();
                let after = runtime.snapshot_cache_stats();
                record(
                    &name,
                    json!({
                        "opens": latencies.len(),
                        "connections": connections,
                        "visibility_classes": classes,
                        "task_updates_during": updates.0,
                        "task_update_errors": updates.1,
                        "complete_s": elapsed.as_secs_f64(),
                        "process_cpu_s": cpu as f64 / 1e9,
                        "peak_rss_mb": peak_rss.load(Relaxed) as f64 / 1048576.0,
                        "peak_rss_over_start_mb": (peak_rss.load(Relaxed) as f64 - rss_base as f64) / 1048576.0,
                        "open_latency": Summary::of(latencies).to_json(),
                        "network_polls": Summary::of(polls.iter().map(|(wall, _)| *wall as u64).collect()).to_json(),
                        "need_hashes": need_hashes,
                        "wire_bytes": bytes,
                        "cache_builds": after.builds - before.builds,
                        "cache_hits": after.hits - before.hits,
                        "cache_roll_forwards": after.roll_forwards - before.roll_forwards,
                        "cache_fallbacks": after.fallbacks - before.fallbacks,
                        "cache_entries": after.entries,
                        "cache_bytes": after.bytes,
                    }),
                );
            }
        }
    }
    runtime.set_snapshot_cache_enabled(true);
    fixture.shutdown().await;
}
