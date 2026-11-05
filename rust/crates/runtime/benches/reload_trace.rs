//! Replay the captured mixed-cursor whole-app collection set against an owned
//! synthetic lab copy. Does not authenticate or open any network endpoint.
use gonvex_protocol::{ReplicaCursor, ReplicaOpenRequest, ServerMessage};
use gonvex_runtime::perf_fixture::harness::{
    peak_live_bytes, reset_peak, AllocSnapshot, CountingAlloc,
};
use gonvex_runtime::perf_fixture::WHAGONS_COLD_LOGIN;
use gonvex_runtime::raw_sites;
use serde_json::{json, Value};
use std::time::Instant;
#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);
fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { tokio::spawn(run_offline()).await.unwrap() });
}
async fn run_offline() {
    use gonvex_runtime::perf_fixture::offline::OfflineFixture;
    let directory = std::path::PathBuf::from(std::env::var("BENCH_OFFLINE_DATA").unwrap());
    let fixture = OfflineFixture::start(&directory).await.unwrap();
    let sessions: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("BENCH_OFFLINE_SESSIONS").unwrap()).unwrap(),
    )
    .unwrap();
    let captured: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("BENCH_OFFLINE_OPENS").unwrap()).unwrap(),
    )
    .unwrap();
    let mut specs = captured["opens"].as_array().unwrap().clone();
    for path in if env("BENCH_EXACT_CAPTURE", 0) == 1 {
        &[][..]
    } else {
        &WHAGONS_COLD_LOGIN[..]
    } {
        if *path == "notifications.listNotifications" || *path == "presence.list" {
            continue;
        }
        if !specs
            .iter()
            .any(|s| s["path"] == *path && s["args"] == json!({}))
        {
            specs.push(json!({"path":path,"args":{}}));
        }
    }
    let cursor_flags: std::collections::BTreeMap<String, bool> =
        std::env::var("BENCH_CURSOR_CAPTURE")
            .ok()
            .map(|path| {
                serde_json::from_slice::<Value>(&std::fs::read(path).unwrap())
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| {
                        (
                            s["path"].as_str().unwrap().to_owned(),
                            s["hasCursor"].as_bool().unwrap(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
    let count = env("BENCH_ITERS", 5);
    for identity in sessions.as_array().unwrap() {
        let session = fixture
            .session(
                identity["project"].as_str().unwrap(),
                identity["tenant"].as_str().unwrap(),
                identity["account"].as_str().unwrap(),
            )
            .await
            .unwrap();
        let label = identity["label"].as_str().unwrap();
        let mut tokens: Vec<Option<(ReplicaCursor, String)>> = Vec::new();
        for phase in ["first", "reload"] {
            let mut scenario = Scenario::start();
            for iteration in 0..if phase == "first" { 1 } else { count } {
                scenario
                    .run(async {
                        let mut next = Vec::new();
                        for (i, spec) in specs.iter().enumerate() {
                            let path = spec["path"].as_str().unwrap();
                            let args: Value = serde_json::from_str(
                                &serde_json::to_string(&spec["args"])
                                    .unwrap()
                                    .replace("$member", &session.member.id),
                            )
                            .unwrap();
                            let token = if phase == "reload" && (cursor_flags.is_empty() || cursor_flags.get(path).copied().unwrap_or(false)) {
                                tokens[i].as_ref()
                            } else {
                                None
                            };
                            let before_sites = raw_sites::snapshot();
                            let before_cpu = cpu_ns();
                            let mut opened = fixture
                                .runtime
                                .open_replica(&session, request(path, args, token))
                                .await
                                .unwrap_or_else(|e| panic!("{path}: {e}"));
                            assert!(
                                opened.subscription.is_some(),
                                "{path} unexpectedly needs hashes"
                            );
                            let open_cpu = cpu_ns();
                            let next_token = ready_of(&opened.messages);
                            let snapshot = opened.messages.iter().any(|m| matches!(m, ServerMessage::ReplicaSnapshot { .. }));
                            let wire_start = cpu_ns();
                            let wire = if env("BENCH_WIRE",0) == 1 { OfflineFixture::encode_wire(std::mem::take(&mut opened.messages)) } else { (0,0) };
                            let wire_end = cpu_ns();
                            if iteration == 0 && env("BENCH_TRACE", 0) == 1 {
                                let after_cpu = open_cpu;
                                let sites: serde_json::Map<_,_> = raw_sites::snapshot().into_iter().zip(&before_sites)
                                    .filter(|(a,b)| a.1>b.1).map(|(a,b)| (a.0.to_owned(),json!({"calls":a.1-b.1,"rows":a.2-b.2}))).collect();
                                record("reload_open", json!({"session":label,"phase":phase,"path":path,"sentCursor":token.is_some(),
                                    "snapshot":snapshot,"jsonBytes":wire.0,"wireBytes":wire.1,"wireCpuNs":wire_end.0+wire_end.1-wire_start.0-wire_start.1,
                                    "cpuNs":after_cpu.0+after_cpu.1-before_cpu.0-before_cpu.1,"sites":sites}));
                            }
                            next.push(next_token);
                            std::hint::black_box(opened);
                        }
                        tokens = next;
                    })
                    .await;
                std::hint::black_box(iteration);
            }
            scenario.finish(
                &format!("offline/{label}/{phase}"),
                json!({"opens":specs.len(),"cursorReload":phase=="reload","mixedCapture":!cursor_flags.is_empty()}),
            );
        }
        if env("BENCH_ORACLE", 0) == 1 {
            let rows = |messages: &[ServerMessage]| {
                let mut rows: Vec<_> = messages
                    .iter()
                    .flat_map(|m| match m {
                        ServerMessage::ReplicaSnapshot { result, .. } => result
                            .iter()
                            .map(|r| serde_json::to_string(&r.to_value()).unwrap())
                            .collect(),
                        _ => Vec::new(),
                    })
                    .collect();
                rows.sort();
                rows
            };
            for spec in &specs {
                let path = spec["path"].as_str().unwrap();
                let args: Value = serde_json::from_str(
                    &serde_json::to_string(&spec["args"])
                        .unwrap()
                        .replace("$member", &session.member.id),
                )
                .unwrap();
                let cached = fixture
                    .runtime
                    .open_replica(&session, request(path, args.clone(), None))
                    .await
                    .unwrap();
                let fresh = fixture
                    .runtime
                    .open_replica_fresh(&session, request(path, args, None))
                    .await
                    .unwrap();
                assert_eq!(
                    rows(&cached.messages),
                    rows(&fresh.messages),
                    "{label} {path}: SQL oracle mismatch"
                );
            }
            record(
                &format!("offline/{label}/oracle"),
                json!({"exactRowComparisons":specs.len()}),
            );
        }
    }
    let metadata = fixture.runtime.resume_validation_stats();
    let memory_before = AllocSnapshot::now();
    let rss_before = gonvex_runtime::perf_fixture::harness::rss_bytes();
    fixture.runtime.clear_resume_validation();
    let freed = memory_before.live_bytes - AllocSnapshot::now().live_bytes;
    record(
        "validation_metadata_memory",
        json!({"entries":metadata.0,"chargedBytes":metadata.1,"freedAllocatorBytes":freed,"rssBefore":rss_before,"rssAfter":gonvex_runtime::perf_fixture::harness::rss_bytes()}),
    );
    fixture.shutdown().await;
}
