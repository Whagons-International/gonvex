//! Raw snapshot rows (perf/snapshot-raw): where snapshot builds, cache fills
//! and opens, resumes and change collapsing still build `serde_json::Value`
//! rows, and what they cost. Every scenario prints its CPU, allocations and
//! peak heap plus a per-call-site table (`gonvex_runtime::raw_sites`: calls,
//! rows, allocations and time on the calling thread, per scenario iteration).
//!
//!   taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench snapshot_raw
//!
//! Scenarios (BENCH_FILTER substrings):
//! * `open/<kind>/{fresh,miss,hit,hit_32,hit_1k}`: `tasks.recentWorkspaceTasks`
//!   for one member of `kind` (Admin sees every task, TeamSpots ~a team's):
//!   SQL scan without the cache, a cache miss (shared build + view), a hit,
//!   a hit after 32 / 1,000 changed rows (one transaction, applied from the
//!   change log on top of the entry).
//! * `resume/<kind>/{10,200}`: resume from the client's cursor after that many
//!   changed rows (aggregate digest: cache + log).
//! * `login/<kind>/{miss,hit}`: the 11 cold-login collections.
//! * `detail/<kind>/{miss,hit}`: the 8 collections of one task detail window
//!   (keyed entries built inside the open's read, then hits).
//! * `feed/burst_<n>`: every member connected with the 10 login collections;
//!   one transaction updating `n` tasks applied to every connection
//!   (`apply_feed_event`, no encoding).
//!
//! Env: BENCH_TASKS (9000), BENCH_MEMBERS (78), BENCH_ITERS (5),
//! BENCH_JSON_OUT.

use std::time::Instant;

use gonvex_protocol::{ReplicaCursor, ReplicaOpenRequest, ServerMessage};
use gonvex_runtime::perf_fixture::harness::{
    peak_live_bytes, reset_peak, AllocSnapshot, CountingAlloc,
};
use gonvex_runtime::perf_fixture::{
    Encode, Fixture, FixtureOptions, MemberKind, SqlValue, TenantSpec, WHAGONS_COLD_LOGIN,
    WHAGONS_DETAIL_WINDOW,
};
use gonvex_runtime::raw_sites;
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

/// (user, system) CPU of the process in ns.
fn cpu_ns() -> (u64, u64) {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid out pointer.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let ns = |time: libc::timeval| time.tv_sec as u64 * 1_000_000_000 + time.tv_usec as u64 * 1000;
    (ns(usage.ru_utime), ns(usage.ru_stime))
}

#[derive(Default, Clone, Copy)]
struct Sample {
    wall_ns: u64,
    user_ns: u64,
    cpu_ns: u64,
    allocations: u64,
    allocated_bytes: u64,
    peak: i64,
}

async fn measure<T>(op: impl std::future::Future<Output = T>) -> (T, Sample) {
    let base = reset_peak();
    let before = AllocSnapshot::now();
    let (user, system) = cpu_ns();
    let started = Instant::now();
    let value = op.await;
    let wall_ns = started.elapsed().as_nanos() as u64;
    let (user_after, system_after) = cpu_ns();
    let allocated = AllocSnapshot::now().since(before);
    (
        value,
        Sample {
            wall_ns,
            user_ns: user_after - user,
            cpu_ns: user_after - user + system_after - system,
            allocations: allocated.allocations,
            allocated_bytes: allocated.allocated_bytes,
            peak: peak_live_bytes() - base,
        },
    )
}

type Sites = Vec<(&'static str, u64, u64, u64, u64)>;

/// Median sample fields, plus the per-site table per iteration (measured
/// operations only).
struct Scenario {
    samples: Vec<Sample>,
    sites: Sites,
}

impl Scenario {
    fn start() -> Self {
        raw_sites::reset();
        Self {
            samples: Vec::new(),
            sites: raw_sites::snapshot()
                .into_iter()
                .map(|(name, ..)| (name, 0, 0, 0, 0))
                .collect(),
        }
    }

    /// Runs and measures one iteration.
    async fn run<T>(&mut self, op: impl std::future::Future<Output = T>) -> T {
        let before = raw_sites::snapshot();
        let (value, sample) = measure(op).await;
        let after = raw_sites::snapshot();
        for ((total, before), after) in self.sites.iter_mut().zip(before).zip(after) {
            total.1 += after.1 - before.1;
            total.2 += after.2 - before.2;
            total.3 += after.3 - before.3;
            total.4 += after.4 - before.4;
        }
        self.samples.push(sample);
        value
    }

    fn finish(self, name: &str, extra: Value) {
        let n = self.samples.len().max(1) as u64;
        let median = |field: fn(&Sample) -> u64| {
            let mut values: Vec<u64> = self.samples.iter().map(field).collect();
            values.sort_unstable();
            values.get(values.len() / 2).copied().unwrap_or(0)
        };
        let sites = self
            .sites
            .iter()
            .copied()
            .filter(|(_, calls, ..)| *calls > 0)
            .map(|(site, calls, rows, allocations, nanos)| {
                (
                    site.to_owned(),
                    json!({
                        "calls": calls / n,
                        "rows": rows / n,
                        "allocations": allocations / n,
                        "ms": (nanos / n) as f64 / 1e6,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        record(
            name,
            json!({
                "iterations": self.samples.len(),
                "wall_ms": median(|s| s.wall_ns) as f64 / 1e6,
                "user_ms": median(|s| s.user_ns) as f64 / 1e6,
                "cpu_ms": median(|s| s.cpu_ns) as f64 / 1e6,
                "allocations": median(|s| s.allocations),
                "allocated_mb": median(|s| s.allocated_bytes) as f64 / 1e6,
                "peak_heap_mb": self.samples.iter().map(|s| s.peak).max().unwrap_or(0) as f64 / 1e6,
                "sites": sites,
                "extra": extra,
            }),
        );
    }
}

fn request(
    path: &str,
    args: Value,
    resume: Option<&(ReplicaCursor, String)>,
) -> ReplicaOpenRequest {
    let mut request = json!({"id": path, "path": path, "args": args});
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

/// One transaction updating `count` tasks (`count` change-log rows).
async fn update_tasks(
    fixture: &Fixture,
    count: usize,
    salt: usize,
) -> gonvex_runtime::change_feed::FeedEvent {
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
        .unwrap()
}

async fn open(
    fixture: &Fixture,
    session: &TenantSession,
    path: &str,
    args: Value,
) -> Vec<ServerMessage> {
    fixture
        .runtime
        .open_replica(session, request(path, args, None))
        .await
        .unwrap()
        .messages
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
    let tasks: usize = env("BENCH_TASKS", 9_000);
    let members: usize = env("BENCH_MEMBERS", 78);
    let iterations: usize = env("BENCH_ITERS", 5);
    let mut options = FixtureOptions::whagons();
    options.tenants = vec![TenantSpec::new("big", tasks, members)];
    let started = Instant::now();
    let fixture = Fixture::build(options).await.unwrap();
    eprintln!(
        "fixture ({tasks} tasks, {members} members) ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let runtime = fixture.runtime.clone();
    let kinds = [
        ("Admin", MemberKind::Admin),
        ("TeamSpots", MemberKind::TeamSpots),
    ];

    for (label, kind) in kinds {
        let session = fixture.session(TENANT, fixture.members_of_kind(TENANT, kind)[0]);
        // Warm parent ACL indexes, principal cache and SQLite pages.
        let _ = runtime
            .open_replica_fresh(&session, request(TASKS, json!({}), None))
            .await
            .unwrap();

        let name = format!("open/{label}/fresh");
        if enabled(&name) {
            let mut scenario = Scenario::start();
            let mut rows = 0;
            for _ in 0..iterations {
                let opened = scenario
                    .run(runtime.open_replica_fresh(&session, request(TASKS, json!({}), None)))
                    .await;
                rows = rows_of(&opened.unwrap().messages);
            }
            scenario.finish(&name, json!({"rows": rows}));
        }
        let name = format!("open/{label}/miss");
        if enabled(&name) {
            let mut scenario = Scenario::start();
            for _ in 0..iterations {
                runtime.clear_snapshot_cache();
                scenario
                    .run(open(&fixture, &session, TASKS, json!({})))
                    .await;
            }
            scenario.finish(
                &name,
                json!({"cache": format!("{:?}", runtime.snapshot_cache_stats())}),
            );
        }
        let name = format!("open/{label}/hit");
        if enabled(&name) {
            let _ = open(&fixture, &session, TASKS, json!({})).await;
            let mut scenario = Scenario::start();
            for _ in 0..iterations {
                scenario
                    .run(open(&fixture, &session, TASKS, json!({})))
                    .await;
            }
            scenario.finish(&name, json!({}));
        }
        for changes in [32usize, 1000] {
            let name = format!("open/{label}/hit_{changes}");
            if !enabled(&name) {
                continue;
            }
            let mut scenario = Scenario::start();
            for i in 0..iterations {
                // A fresh entry, then `changes` rows the open replays.
                runtime.clear_snapshot_cache();
                let _ = open(&fixture, &session, TASKS, json!({})).await;
                update_tasks(&fixture, changes, i).await;
                scenario
                    .run(open(&fixture, &session, TASKS, json!({})))
                    .await;
            }
            scenario.finish(&name, json!({}));
        }
        for changes in [10usize, 200] {
            let name = format!("resume/{label}/{changes}");
            if !enabled(&name) {
                continue;
            }
            let mut scenario = Scenario::start();
            let mut need_hashes = 0;
            for i in 0..iterations {
                let opened = open(&fixture, &session, TASKS, json!({})).await;
                let token = ready_of(&opened).unwrap();
                update_tasks(&fixture, changes, i + 50).await;
                let opened = scenario
                    .run(runtime.open_replica(&session, request(TASKS, json!({}), Some(&token))))
                    .await;
                need_hashes += opened
                    .unwrap()
                    .messages
                    .iter()
                    .filter(|message| matches!(message, ServerMessage::ReplicaNeedHashes { .. }))
                    .count();
            }
            scenario.finish(&name, json!({"need_hashes": need_hashes}));
        }
        for mode in ["miss", "hit"] {
            let name = format!("login/{label}/{mode}");
            if !enabled(&name) {
                continue;
            }
            let mut scenario = Scenario::start();
            for _ in 0..iterations {
                if mode == "miss" {
                    runtime.clear_snapshot_cache();
                }
                scenario
                    .run(async {
                        for path in WHAGONS_COLD_LOGIN {
                            open(&fixture, &session, path, json!({})).await;
                        }
                    })
                    .await;
            }
            scenario.finish(&name, json!({}));
        }
        for mode in ["miss", "hit"] {
            let name = format!("detail/{label}/{mode}");
            if !enabled(&name) {
                continue;
            }
            let mut scenario = Scenario::start();
            let all = &fixture.tenant(TENANT).tasks;
            for i in 0..iterations {
                let id = all[(i * 131 + 7) % all.len()].id.clone();
                if mode == "miss" {
                    runtime.clear_snapshot_cache();
                }
                scenario
                    .run(async {
                        for (path, argument) in WHAGONS_DETAIL_WINDOW {
                            open(&fixture, &session, path, json!({ argument: id })).await;
                        }
                    })
                    .await;
            }
            scenario.finish(&name, json!({}));
        }
    }

    // ---- Change burst applied to every connection -----------------------------
    let bursts: Vec<usize> = [1usize, 50]
        .into_iter()
        .filter(|n| enabled(&format!("feed/burst_{n}")))
        .collect();
    if !bursts.is_empty() {
        let mut connections = fixture.connect_all(TENANT).await.unwrap();
        for (i, burst) in bursts.into_iter().enumerate() {
            let name = format!("feed/burst_{burst}");
            let mut scenario = Scenario::start();
            let mut messages = 0;
            for iteration in 0..iterations {
                let event = update_tasks(&fixture, burst, 1000 + i * 100 + iteration).await;
                let delivery = scenario
                    .run(fixture.deliver(&mut connections, &event, Encode::None))
                    .await;
                messages = delivery.unwrap().messages;
            }
            scenario.finish(
                &name,
                json!({"connections": connections.len(), "messages": messages}),
            );
        }
    }
}
