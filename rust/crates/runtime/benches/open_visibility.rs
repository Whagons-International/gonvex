//! Per-open visibility resolution: CPU per task detail window (8 collections)
//! and per cold login (11 collections), and a 500-member open driver.
//!
//!   taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench open_visibility
//!
//! Scenarios (BENCH_FILTER substrings):
//! * `sequence/<kind>/<quiet|writes>`: per member of `kind` (each member once,
//!   so its first login is cold): a cold login (11 opens), then 3 detail
//!   windows on different tasks (8 opens each), one open after another.
//!   `writes` commits one task update before every open, so every open reads
//!   at a newer revision than the one before (the loaded case: ~23 task
//!   writes/s in the largest tenant).
//! * `ab`: BENCH_AB_MEMBERS (200) unused members alternate sharing off/on
//!   in one process: cold login + 2 detail windows each.
//! * `driver`: BENCH_DRIVER_MEMBERS members (default every member of the
//!   tenant; build with BENCH_MEMBERS=500) log in over DRIVER_SECONDS (each
//!   once, as concurrent tasks) while detail windows open at DRIVER_DETAIL_RATE/s
//!   for already logged-in members and task updates commit at
//!   DRIVER_WRITE_RATE/s (DRIVER_GRANT_RATE/s of them `taskUsers`
//!   assignments instead). With DRIVER_LOGIN_RATE set, steady state instead:
//!   logins (reconnects) at that rate cycling over every member and detail
//!   windows for any member (the 1k production-mix plateau: 4.4 logins/s,
//!   4.3 detail windows/s). Reports process CPU per open and latency per login /
//!   detail window.
//!
//! Env: BENCH_TASKS (9000), BENCH_MEMBERS (78), BENCH_ITERS (4 members per
//! kind and mode), DRIVER_SECONDS (30), DRIVER_DETAIL_RATE (2), DRIVER_WRITE_RATE (23),
//! DRIVER_GRANT_RATE (0.5), DRIVER_LOGIN_RATE (0 = ramp), BENCH_JSON_OUT.
//! GONVEX_SNAPSHOT_CACHE_KEYED_INLINE=0 restores building per-task detail
//! entries in a separate admitted read before the open. With GONVEX_SQLITE_STATEMENT_PROFILE=1 it also reports SQL
//! statements per login / window (the profile hook costs CPU: take CPU
//! numbers from a run without it). GONVEX_PRINCIPAL_CACHE=0 measures the
//! SQL resolution per open (the code before `principal_cache`).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gonvex_runtime::perf_fixture::harness::{AllocSnapshot, CountingAlloc};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, TenantSpec, WHAGONS_COLD_LOGIN, WHAGONS_DETAIL_WINDOW,
};
use gonvex_store::TenantSession;
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const TENANT: usize = 0;

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

/// Process CPU (user + system, every thread incl. SQLite workers) in ns.
fn process_cpu_ns() -> u64 {
    let (user, system) = process_cpu_split_ns();
    user + system
}

/// (user, system) CPU in ns. User time is the steadier comparison on a
/// shared host: page compaction and migration under host memory pressure
/// are charged as system time to whichever process faults.
fn process_cpu_split_ns() -> (u64, u64) {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid out pointer.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let ns = |time: libc::timeval| time.tv_sec as u64 * 1_000_000_000 + time.tv_usec as u64 * 1000;
    (ns(usage.ru_utime), ns(usage.ru_stime))
}

fn statements() -> (u64, u64) {
    gonvex_store::statement_profile::snapshot()
        .iter()
        .fold((0, 0), |(count, steps), (_, stats)| {
            (count + stats.count, steps + stats.vm_steps)
        })
}

#[derive(Default)]
struct Sample {
    wall_ns: u64,
    cpu_ns: u64,
    user_ns: u64,
    allocations: u64,
    statements: u64,
    vm_steps: u64,
}

async fn measure<T>(op: impl std::future::Future<Output = T>) -> (T, Sample) {
    let before = AllocSnapshot::now();
    let (statements_before, steps_before) = statements();
    let (user, system) = process_cpu_split_ns();
    let started = Instant::now();
    let value = op.await;
    let wall_ns = started.elapsed().as_nanos() as u64;
    let (user_after, system_after) = process_cpu_split_ns();
    let user_ns = user_after - user;
    let cpu_ns = user_ns + system_after - system;
    let (statements_after, steps_after) = statements();
    let sample = Sample {
        wall_ns,
        cpu_ns,
        user_ns,
        allocations: AllocSnapshot::now().since(before).allocations,
        statements: statements_after - statements_before,
        vm_steps: steps_after - steps_before,
    };
    (value, sample)
}

fn median(mut values: Vec<u64>) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

fn percentile(mut values: Vec<u64>, p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[((values.len() as f64 * p) as usize).min(values.len() - 1)]
}

fn summarize(samples: &[Sample]) -> Value {
    json!({
        "n": samples.len(),
        "wall_ms_median": median(samples.iter().map(|s| s.wall_ns).collect()) as f64 / 1e6,
        "cpu_ms_median": median(samples.iter().map(|s| s.cpu_ns).collect()) as f64 / 1e6,
        "user_ms_median": median(samples.iter().map(|s| s.user_ns).collect()) as f64 / 1e6,
        "cpu_ms_mean": samples.iter().map(|s| s.cpu_ns).sum::<u64>() as f64 / 1e6 / samples.len().max(1) as f64,
        "allocations_median": median(samples.iter().map(|s| s.allocations).collect()),
        "sql_statements_median": median(samples.iter().map(|s| s.statements).collect()),
        "sql_vm_steps_median": median(samples.iter().map(|s| s.vm_steps).collect()),
    })
}

impl Sample {
    fn add(&mut self, other: Sample) {
        self.wall_ns += other.wall_ns;
        self.cpu_ns += other.cpu_ns;
        self.user_ns += other.user_ns;
        self.allocations += other.allocations;
        self.statements += other.statements;
        self.vm_steps += other.vm_steps;
    }
}

/// A cold login; the sample covers the opens only (not `writes`' commits).
async fn login(fixture: &Fixture, session: &TenantSession, writes: Option<&AtomicU64>) -> Sample {
    let mut total = Sample::default();
    for path in WHAGONS_COLD_LOGIN {
        if let Some(counter) = writes {
            write_task(fixture, counter.fetch_add(1, Relaxed) as usize).await;
        }
        let (opened, sample) = measure(fixture.open_replica(session, path, path)).await;
        assert!(opened.unwrap().subscription.is_some(), "{path} did not subscribe");
        total.add(sample);
    }
    total
}

async fn detail_window(
    fixture: &Fixture,
    session: &TenantSession,
    task: usize,
    writes: Option<&AtomicU64>,
) -> Sample {
    let tasks = &fixture.tenant(TENANT).tasks;
    let id = tasks[task % tasks.len()].id.clone();
    let mut total = Sample::default();
    for (path, argument) in WHAGONS_DETAIL_WINDOW {
        if let Some(counter) = writes {
            write_task(fixture, counter.fetch_add(1, Relaxed) as usize).await;
        }
        let (opened, sample) =
            measure(fixture.open_replica_args(session, path, path, json!({ argument: id }))).await;
        assert!(opened.unwrap().subscription.is_some(), "{path} did not subscribe");
        total.add(sample);
    }
    total
}

/// One task update (a new revision touching only `tasks`).
async fn write_task(fixture: &Fixture, n: usize) {
    let tasks = fixture.tenant(TENANT).tasks.len();
    fixture
        .update_task(TENANT, (n * 7919) % tasks, json!({"name": format!("bench {n}")}))
        .await
        .unwrap();
}

fn cache_stats(fixture: &Fixture) -> Value {
    serde_json::to_value(fixture.runtime.principal_cache_stats()).unwrap_or(Value::Null)
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
    let iterations: usize = env("BENCH_ITERS", 4);
    let mut options = FixtureOptions::whagons();
    options.tenants[0] = TenantSpec::new("big", tasks, members);
    let started = Instant::now();
    let fixture = Arc::new(Fixture::build(options).await.unwrap());
    eprintln!(
        "fixture ({tasks} tasks, {members} members) ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    // Warm SQLite page caches, parent ACL indexes and the snapshot cache's
    // principal-free entries with one member that no scenario measures.
    let warm = fixture.session(TENANT, members - 1);
    let _ = login(&fixture, &warm, None).await;
    let _ = detail_window(&fixture, &warm, 1, None).await;
    let writes = AtomicU64::new(0);
    let mut used = std::collections::BTreeSet::from([members - 1]);

    let kinds = [
        ("Admin", MemberKind::Admin),
        ("TeamAllSpots", MemberKind::TeamAllSpots),
        ("TeamSpots", MemberKind::TeamSpots),
        ("GrantOnly", MemberKind::GrantOnly),
    ];
    for mode in ["quiet", "writes"] {
        for (label, kind) in kinds {
            let name = format!("sequence/{label}/{mode}");
            if !enabled(&name) {
                continue;
            }
            // Small kinds (4 admins at 78 members) split between the modes.
            let of_kind = fixture.members_of_kind(TENANT, kind);
            let take = if mode == "quiet" {
                iterations.min((of_kind.len() / 2).max(1))
            } else {
                iterations
            };
            let candidates = of_kind
                .into_iter()
                .filter(|member| !used.contains(member))
                .take(take)
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                eprintln!("{name}: no unused members of this kind");
                continue;
            }
            let counter = (mode == "writes").then_some(&writes);
            let (mut logins, mut first, mut later) = (Vec::new(), Vec::new(), Vec::new());
            let stats_before = cache_stats(&fixture);
            for (n, member) in candidates.iter().enumerate() {
                used.insert(*member);
                let session = fixture.session(TENANT, *member);
                logins.push(login(&fixture, &session, counter).await);
                for window in 0..3 {
                    let task = 101 + n * 37 + window * 1013;
                    let sample = detail_window(&fixture, &session, task, counter).await;
                    if window == 0 {
                        first.push(sample);
                    } else {
                        later.push(sample);
                    }
                }
            }
            let values = json!({
                "members": candidates.len(),
                "login_11": summarize(&logins),
                "detail_8_first": summarize(&first),
                "detail_8_later": summarize(&later),
                "principal_cache_before": stats_before,
                "principal_cache_after": cache_stats(&fixture),
            });
            eprintln!("{name}: {values}");
            record(&name, values);
        }
    }

    if enabled("ab") {
        ab(&fixture, &mut used).await;
    }
    if enabled("driver") {
        driver(fixture.clone(), &used).await;
    }
}

/// In-process A/B: unused members alternate between principal sharing off
/// and on (one member each, interleaved), so host load drifts hit both
/// modes alike. Per member: a cold login, then 2 detail windows.
async fn ab(fixture: &Fixture, used: &mut std::collections::BTreeSet<usize>) {
    let members = (0..fixture.tenant(TENANT).members.len())
        .filter(|member| !used.contains(member))
        .take(env("BENCH_AB_MEMBERS", 200))
        .collect::<Vec<_>>();
    let mut samples: [(Vec<Sample>, Vec<Sample>); 2] = Default::default();
    for (n, member) in members.iter().enumerate() {
        used.insert(*member);
        let mode = n % 2;
        fixture.runtime.set_principal_cache_enabled(mode == 1);
        let session = fixture.session(TENANT, *member);
        samples[mode].0.push(login(fixture, &session, None).await);
        for window in 0..2 {
            let task = 211 + n * 41 + window * 977;
            samples[mode].1.push(detail_window(fixture, &session, task, None).await);
        }
    }
    fixture.runtime.set_principal_cache_enabled(true);
    let values = json!({
        "members": members.len(),
        "off": {"login_11": summarize(&samples[0].0), "detail_8": summarize(&samples[0].1)},
        "on": {"login_11": summarize(&samples[1].0), "detail_8": summarize(&samples[1].1)},
        "principal_cache": cache_stats(fixture),
    });
    eprintln!("ab: {values}");
    record("ab", values);
}

async fn driver(fixture: Arc<Fixture>, used: &std::collections::BTreeSet<usize>) {
    let seconds: f64 = env("DRIVER_SECONDS", 30.0);
    let detail_rate: f64 = env("DRIVER_DETAIL_RATE", 2.0);
    let write_rate: f64 = env("DRIVER_WRITE_RATE", 23.0);
    // Of those, assignments (`taskUsers` inserts: a grant input of every
    // task-plan resolution in the tenant, so each one invalidates them all).
    let grant_rate: f64 = env("DRIVER_GRANT_RATE", 0.5);
    // Steady state instead of a ramp: logins (reconnects) at this rate over
    // every member, and detail windows for any member.
    let login_rate: f64 = env("DRIVER_LOGIN_RATE", 0.0);
    let available = (0..fixture.tenant(TENANT).members.len())
        .filter(|member| !used.contains(member))
        .collect::<Vec<_>>();
    let count = env("BENCH_DRIVER_MEMBERS", available.len()).min(available.len());
    let members = available[..count].to_vec();
    if members.is_empty() {
        eprintln!("driver: no unused members");
        return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let logged_in = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
    let login_ns = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let detail_ns = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let writes_done = Arc::new(AtomicU64::new(0));
    let stats_before = cache_stats(&fixture);
    let cpu_before = process_cpu_ns();
    let user_before = process_cpu_split_ns().0;
    let started = Instant::now();

    let writer = {
        let (fixture, stop, done) = (fixture.clone(), stop.clone(), writes_done.clone());
        tokio::spawn(async move {
            let period = Duration::from_secs_f64(1.0 / write_rate.max(0.001));
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            while !stop.load(Relaxed) {
                ticker.tick().await;
                let n = done.fetch_add(1, Relaxed) as usize;
                let every = (write_rate / grant_rate.max(1e-9)).round().max(1.0) as usize;
                if grant_rate > 0.0 && n % every == every - 1 {
                    let tenant = fixture.tenant(TENANT);
                    let task = &tenant.tasks[(n * 7919) % tenant.tasks.len()];
                    let member = &tenant.members[(n * 31) % tenant.members.len()];
                    fixture
                        .commit(
                            TENANT,
                            r#"INSERT INTO "taskUsers"("_id","taskId","userId","workspaceId","createdAt","updatedAt") VALUES($1,$2,$3,$4,1,1)"#,
                            vec![
                                format!("driver-assign-{n}").into(),
                                task.id.clone().into(),
                                member.id.clone().into(),
                                task.workspace_id.clone().into(),
                            ],
                        )
                        .await
                        .unwrap();
                } else {
                    write_task(&fixture, 50_000 + n).await;
                }
            }
        })
    };
    let mut tasks = Vec::new();
    // Logins: evenly over the window, each as its own task (connections).
    let login_period = if login_rate > 0.0 {
        Duration::from_secs_f64(1.0 / login_rate)
    } else {
        Duration::from_secs_f64(seconds / members.len() as f64)
    };
    let all_members = members.clone();
    let detail_period = Duration::from_secs_f64(1.0 / detail_rate.max(0.001));
    let mut next_login = Instant::now();
    let mut next_detail = Instant::now() + detail_period;
    let mut pending: Box<dyn Iterator<Item = usize> + Send> = if login_rate > 0.0 {
        let order = all_members.clone();
        Box::new((0..).map(move |n: usize| order[(n * 7) % order.len()]))
    } else {
        Box::new(members.into_iter())
    };
    let mut detail_n = 0usize;
    let deadline = Instant::now() + Duration::from_secs_f64(seconds);
    while Instant::now() < deadline {
        let now = Instant::now();
        if now >= next_login {
            if let Some(member) = pending.next() {
                let (fixture, logged_in, login_ns) =
                    (fixture.clone(), logged_in.clone(), login_ns.clone());
                tasks.push(tokio::spawn(async move {
                    let session = fixture.session(TENANT, member);
                    let started = Instant::now();
                    let _ = login(&fixture, &session, None).await;
                    login_ns
                        .lock()
                        .unwrap()
                        .push(started.elapsed().as_nanos() as u64);
                    logged_in.lock().unwrap().push(member);
                }));
            }
            next_login += login_period;
        }
        if now >= next_detail {
            let member = if login_rate > 0.0 {
                Some(all_members[(detail_n * 31 + 3) % all_members.len()])
            } else {
                let logged_in = logged_in.lock().unwrap();
                (!logged_in.is_empty()).then(|| logged_in[(detail_n * 31) % logged_in.len()])
            };
            if let Some(member) = member {
                let (fixture, detail_ns) = (fixture.clone(), detail_ns.clone());
                let task = 7 + detail_n * 53;
                tasks.push(tokio::spawn(async move {
                    let session = fixture.session(TENANT, member);
                    let started = Instant::now();
                    let _ = detail_window(&fixture, &session, task, None).await;
                    detail_ns
                        .lock()
                        .unwrap()
                        .push(started.elapsed().as_nanos() as u64);
                }));
                detail_n += 1;
            }
            next_detail += detail_period;
        }
        let wake = next_login.min(next_detail);
        tokio::time::sleep_until(tokio::time::Instant::from_std(wake.max(Instant::now()))).await;
    }
    for task in tasks {
        task.await.unwrap();
    }
    stop.store(true, Relaxed);
    writer.await.unwrap();
    let elapsed = started.elapsed().as_secs_f64();
    let cpu_ms = (process_cpu_ns() - cpu_before) as f64 / 1e6;
    let user_ms = (process_cpu_split_ns().0 - user_before) as f64 / 1e6;
    let logins = login_ns.lock().unwrap().clone();
    let details = detail_ns.lock().unwrap().clone();
    let opens = logins.len() * WHAGONS_COLD_LOGIN.len() + details.len() * WHAGONS_DETAIL_WINDOW.len();
    let ms = |values: &Vec<u64>, p: f64| percentile(values.clone(), p) as f64 / 1e6;
    let values = json!({
        "elapsed_s": elapsed,
        "logins": logins.len(),
        "detail_windows": details.len(),
        "writes": writes_done.load(Relaxed),
        "cpu_ms_total": cpu_ms,
        "user_ms_total": user_ms,
        "cpu_cores": cpu_ms / 1e3 / elapsed,
        "cpu_ms_per_open_incl_writes": cpu_ms / opens.max(1) as f64,
        "login_ms_p50": ms(&logins, 0.5),
        "login_ms_p99": ms(&logins, 0.99),
        "detail_ms_p50": ms(&details, 0.5),
        "detail_ms_p99": ms(&details, 0.99),
        "principal_cache_before": stats_before,
        "principal_cache_after": cache_stats(&fixture),
        "snapshot_cache": {
            "builds": fixture.runtime.snapshot_cache_stats().builds,
            "hits": fixture.runtime.snapshot_cache_stats().hits,
            "keyed_inline_builds": fixture.runtime.snapshot_cache_stats().keyed_inline_builds,
        },
    });
    eprintln!("driver: {values}");
    record("driver", values);
}
