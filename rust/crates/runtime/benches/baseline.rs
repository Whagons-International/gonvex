//! Baseline benchmarks on the shared Whagons fixture.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench baseline
//!
//! Env: BENCH_FILTER=<substring>, BENCH_ITERS=<n>, BENCH_WARMUP=<n>,
//! BENCH_JSON_OUT=<file>, BENCH_WORKERS=<tokio workers, default 1>.
//! See docs/performance/perf-fixture.md.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Duration;

use gonvex_protocol::ServerMessage;
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::perf_fixture::harness::{retained, BenchConfig, CountingAlloc, Report};
use gonvex_runtime::perf_fixture::{
    Delivery, Encode, Fixture, FixtureOptions, JsTimeMode, MemberKind, WHAGONS_SUBSCRIPTIONS,
};
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const BIG: usize = 0;
const TASKS: &str = "tasks.recentWorkspaceTasks";

fn main() {
    let workers = std::env::var("BENCH_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

fn changes(event: &FeedEvent) -> &[gonvex_runtime::change_feed::LogChange] {
    match event {
        FeedEvent::Transaction { changes, .. } => changes,
        _ => &[],
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// Stable per-change fan-out fields: apply/encode split, visibility checks.
fn delivery_extra(extra: &mut BTreeMap<String, Value>, deliveries: &[Delivery], members: usize) {
    // Medians include the warmup deliveries (same work as measured ones).
    let Some(last) = deliveries.last() else {
        return;
    };
    let apply = median(deliveries.iter().map(|d| d.apply_ns as f64).collect());
    let encode = median(deliveries.iter().map(|d| d.encode_ns as f64).collect());
    let checks = median(
        deliveries
            .iter()
            .map(|d| d.visibility.in_memory_row_checks as f64)
            .collect(),
    );
    extra.insert("receivers".into(), json!(last.connections_with_messages));
    extra.insert("messages".into(), json!(last.messages));
    extra.insert("jsonBytes".into(), json!(last.json_bytes));
    extra.insert("wireBytes".into(), json!(last.wire_bytes));
    extra.insert("applyNsMedian".into(), json!(apply.round()));
    extra.insert(
        "applyNsPerSubscriber".into(),
        json!((apply / members as f64).round()),
    );
    extra.insert("encodeNsMedian".into(), json!(encode.round()));
    extra.insert(
        "encodeNsPerMessage".into(),
        json!((encode / last.messages.max(1) as f64).round()),
    );
    extra.insert("visibilityChecksPerChange".into(), json!(checks));
    extra.insert(
        "applyNsPerVisibilityCheck".into(),
        json!(if checks > 0.0 {
            (apply / checks).round()
        } else {
            0.0
        }),
    );
    extra.insert(
        "visibilitySqlPerChange".into(),
        json!(last.visibility.visibility_sql_statements),
    );
    extra.insert(
        "collectionSnapshotsPerChange".into(),
        json!(last.visibility.collection_snapshots),
    );
    extra.insert(
        "principalRebuildsPerChange".into(),
        json!(last.visibility.principal_rebuilds),
    );
}

fn reducer_ok(message: &ServerMessage) {
    assert!(
        matches!(message, ServerMessage::ReducerResult { .. }),
        "reducer failed: {message:?}"
    );
}

async fn run() {
    let mut report = Report::new();
    let started = std::time::Instant::now();
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    eprintln!("fixture ready in {:.1}s", started.elapsed().as_secs_f64());
    let tenant = fixture.tenant(BIG);
    let members = tenant.members.len();

    // ---- Snapshot open -------------------------------------------------
    // One member of each visibility kind: the tasks collection alone.
    for kind in [
        MemberKind::Admin,
        MemberKind::TeamAllSpots,
        MemberKind::TeamSpots,
        MemberKind::GrantOnly,
    ] {
        let member = fixture.members_of_kind(BIG, kind)[0];
        let session = fixture.session(BIG, member);
        let rows = fixture.connect(BIG, member).await.unwrap().rows[TASKS];
        let name = format!("snapshot_open/{TASKS}/{kind:?}");
        if let Some(result) = report
            .run_simple(&name, BenchConfig::new(2, 15), 1.0, "open", |_| {
                let session = session.clone();
                let fixture = &fixture;
                async move {
                    let opened = fixture.open_replica(&session, TASKS, TASKS).await.unwrap();
                    std::hint::black_box(opened);
                }
            })
            .await
        {
            result.extra.insert("rows".into(), json!(rows));
        }
    }
    // A member's full subscription set (10 collections), one op.
    let team_member = fixture.members_of_kind(BIG, MemberKind::TeamAllSpots)[0];
    report
        .run_simple(
            "snapshot_open/all_10_subscriptions/TeamAllSpots",
            BenchConfig::new(1, 10),
            WHAGONS_SUBSCRIPTIONS.len() as f64,
            "subscription",
            |_| {
                let fixture = &fixture;
                async move {
                    std::hint::black_box(fixture.connect(BIG, team_member).await.unwrap());
                }
            },
        )
        .await;

    // Retained memory: one member alone, then every member of the tenant
    // (rows are interned across subscriptions of the same database).
    if report.enabled("retained/") {
        let (first, alone) = retained(fixture.connect(BIG, team_member)).await;
        let first = first.unwrap();
        let tasks_rows = first.rows[TASKS];
        let mut extra = BTreeMap::new();
        extra.insert("liveBytes".into(), json!(alone.live_bytes));
        extra.insert("peakHeapBytes".into(), json!(alone.peak_bytes));
        extra.insert("rowsPerSubscription".into(), json!(first.rows));
        extra.insert("rssBytes".into(), json!(alone.rss_bytes));
        extra.insert("snapshotRows".into(), json!(first.snapshot_rows));
        extra.insert("taskRows".into(), json!(tasks_rows));
        report.record("retained/one_member_10_subscriptions/TeamAllSpots", extra);
        drop(first);
    }
    let (connections, all) = retained(fixture.connect_all(BIG)).await;
    let connections = RefCell::new(connections.unwrap());
    {
        let connections = connections.borrow();
        let rows: usize = connections.iter().map(|c| c.snapshot_rows).sum();
        let task_rows: usize = connections.iter().map(|c| c.rows[TASKS]).sum();
        let mut extra = BTreeMap::new();
        extra.insert("members".into(), json!(members));
        extra.insert(
            "subscriptions".into(),
            json!(members * WHAGONS_SUBSCRIPTIONS.len()),
        );
        extra.insert("liveBytes".into(), json!(all.live_bytes));
        extra.insert("peakHeapBytes".into(), json!(all.peak_bytes));
        extra.insert(
            "liveBytesPerMember".into(),
            json!(all.live_bytes.map(|b| b / members as i64)),
        );
        extra.insert(
            "liveBytesPerSubscription".into(),
            json!(all
                .live_bytes
                .map(|b| b / (members * WHAGONS_SUBSCRIPTIONS.len()) as i64)),
        );
        extra.insert("rssBytes".into(), json!(all.rss_bytes));
        extra.insert("snapshotRows".into(), json!(rows));
        extra.insert("taskRows".into(), json!(task_rows));
        report.record("retained/all_members_10_subscriptions", extra);
    }

    // ---- Fan-out --------------------------------------------------------
    // One task update (new description) delivered to all 78 members' full
    // subscription sets. The commit is setup; delivery is timed. Tasks are
    // spotless so every team member of the workspace sees them.
    let spotless: Vec<usize> = tenant
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, task)| task.spot_id.is_none())
        .map(|(i, _)| i)
        .collect();
    let mut sample_event = None;
    for (encode, label) in [
        (Encode::None, "apply"),
        (Encode::Json, "apply+json"),
        (Encode::JsonDeflate, "apply+json+deflate"),
    ] {
        let name = format!("fanout/task_update/{members}_members/{label}");
        let last = RefCell::new(None);
        let stats = RefCell::new(Vec::new());
        if let Some(result) = report
            .run(
                &name,
                BenchConfig::new(3, 30),
                members as f64,
                "subscriber",
                |i| {
                    let fixture = &fixture;
                    let task = spotless[i % spotless.len()];
                    async move {
                        fixture
                            .update_task(BIG, task, json!({"description": format!("fan-out edit {i} {}", "x".repeat(380))}))
                            .await
                            .unwrap()
                    }
                },
                |_, event| {
                    let (fixture, connections, last, stats) = (&fixture, &connections, &last, &stats);
                    async move {
                        let mut connections = connections.borrow_mut();
                        let delivery = fixture.deliver(&mut connections, &event, encode).await.unwrap();
                        stats.borrow_mut().push(delivery);
                        *last.borrow_mut() = Some(event);
                    }
                },
            )
            .await
        {
            delivery_extra(&mut result.extra, &stats.take(), members);
        }
        sample_event = last.into_inner().or(sample_event);
    }
    let name = format!("fanout/presence_beat/{members}_members/apply+json");
    let stats = RefCell::new(Vec::new());
    if let Some(result) = report
        .run(
            &name,
            BenchConfig::new(3, 30),
            members as f64,
            "subscriber",
            |i| {
                let fixture = &fixture;
                async move { fixture.beat_presence(BIG, i % members).await.unwrap() }
            },
            |_, event| {
                let (fixture, connections, stats) = (&fixture, &connections, &stats);
                async move {
                    let mut connections = connections.borrow_mut();
                    let delivery = fixture
                        .deliver(&mut connections, &event, Encode::Json)
                        .await
                        .unwrap();
                    stats.borrow_mut().push(delivery);
                }
            },
        )
        .await
    {
        delivery_extra(&mut result.extra, &stats.take(), members);
        result.extra.insert(
            "note".into(),
            json!("presence row is public: every member receives it"),
        );
    }

    // ---- Visibility evaluation of one change against every subscriber ---
    let task_event = match sample_event {
        Some(event) => event,
        None => fixture
            .update_task(
                BIG,
                spotless[0],
                json!({"description": "visibility sample"}),
            )
            .await
            .unwrap(),
    };
    let change = changes(&task_event)
        .iter()
        .find(|change| change.table == "tasks")
        .cloned()
        .expect("task change");
    let name = format!("visibility/task_update/{members}_members");
    let mut admitted = 0;
    let mut connections = connections.into_inner();
    if let Some(result) = report.run_sync(
        &name,
        BenchConfig::new(10, 300),
        members as f64,
        "subscriber",
        |_| {
            admitted =
                std::hint::black_box(Fixture::evaluate_visibility(&connections, TASKS, &change));
        },
    ) {
        result.extra.insert("admitted".into(), json!(admitted));
    }

    // ---- WebSocket compression of a typical delta payload ----------------
    let mut payload = None;
    for connection in connections.iter_mut().take(1) {
        let event = fixture
            .update_task(
                BIG,
                spotless[1],
                json!({"description": format!("payload sample {}", "y".repeat(380))}),
            )
            .await
            .unwrap();
        let messages = fixture
            .runtime
            .apply_feed_event(&connection.session, &mut connection.subscriptions, event)
            .await
            .unwrap();
        payload = messages
            .iter()
            .map(|m| serde_json::to_string(m).unwrap())
            .max_by_key(String::len);
    }
    if let Some(payload) = payload {
        let mut compressed = 0;
        let name = "ws/compress_text/task_delta";
        if let Some(result) =
            report.run_sync(name, BenchConfig::new(100, 2000), 1.0, "message", |_| {
                compressed = gonvex_websocket::perf_compress_text(payload.as_bytes(), 15, 0)
                    .unwrap()
                    .map_or(payload.len(), |bytes| bytes.len());
            })
        {
            result
                .extra
                .insert("payloadBytes".into(), json!(payload.len()));
            result
                .extra
                .insert("compressedBytes".into(), json!(compressed));
            result.extra.insert(
                "note".into(),
                json!(
                    "threshold 0 so the payload is always compressed; production threshold is 1024"
                ),
            );
        }
    }

    // ---- Reducer round trips through the fake module host --------------
    let session = fixture.session(BIG, team_member);
    let reducers: Vec<(&str, &str, Box<dyn Fn(usize) -> Value>)> = vec![
        ("presence.beat", "tenant", Box::new(|_| json!({}))),
        (
            "tasks.updateDescription",
            "tenant",
            Box::new({
                let ids: Vec<String> = spotless
                    .iter()
                    .map(|&i| tenant.tasks[i].id.clone())
                    .collect();
                move |i| json!({"taskId": ids[i % ids.len()], "description": format!("reducer edit {i}")})
            }),
        ),
        (
            "control.support.heartbeat",
            "control",
            Box::new(|_| json!({"release": "perf-fixture", "environment": "bench"})),
        ),
    ];
    for js_us in [0u64, 1500] {
        fixture
            .host
            .set_js_time(Duration::from_micros(js_us), JsTimeMode::Spin);
        for (path, scope, args) in &reducers {
            if *scope == "control" && js_us > 0 {
                continue; // native: never reaches the module host
            }
            let name = format!("reducer/{path}/js_{js_us}us");
            let calls_before = fixture.host.host_calls();
            let invokes_before = fixture.host.invocations();
            let config = BenchConfig::new(5, 200);
            if let Some(result) = report
                .run_simple(&name, config, 1.0, "call", |i| {
                    let fixture = &fixture;
                    let session = &session;
                    let args = args(i);
                    async move {
                        let reply = if *scope == "control" {
                            fixture
                                .call_control_reducer(session, "bench-connection", path, args)
                                .await
                        } else {
                            fixture.call_reducer(session, path, args).await
                        };
                        reducer_ok(&reply);
                    }
                })
                .await
            {
                let invokes = fixture.host.invocations() - invokes_before;
                if invokes > 0 {
                    result.extra.insert(
                        "hostCallsPerInvoke".into(),
                        json!((fixture.host.host_calls() - calls_before) as f64 / invokes as f64),
                    );
                }
            }
        }
    }

    report.finish();
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find(|line| line.starts_with("model name"))
                .map(|line| line.split(':').nth(1).unwrap_or("").trim().to_owned())
        })
        .unwrap_or_default();
    println!(
        "\nmachine: {} CPUs, {cpu}; tokio workers {}",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        std::env::var("BENCH_WORKERS").unwrap_or_else(|_| "1".into())
    );
    fixture.shutdown().await;
}
