//! `replicaOmitNulls`: wire bytes, encode CPU and memory of Replica rows
//! with and without null-valued columns, on an el-rey-sized tenant (real
//! Whagons 52-column `tasks` schema; the fixture fills 28 of them).
//!
//!   taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench omit_nulls
//!
//! Sections (BENCH_FILTER): `snapshot` (Admin open of
//! `tasks.recentWorkspaceTasks` and every Whagons collection: page bytes raw
//! and per-message DEFLATE as the socket compresses them, the omission pass,
//! page encoding), `delta` (batch frame bytes and encode time per task update,
//! for every member's connection), `memory` (snapshot cache entry vs the same
//! rows without nulls). Env: BENCH_TASKS (10500), BENCH_MEMBERS (78),
//! BENCH_DESCRIPTION (400 bytes), BENCH_ITERS (7), BENCH_UPDATES (40),
//! BENCH_DUMP (directory: page texts for the client decode bench),
//! BENCH_JSON_OUT. See docs/performance/omit-nulls.md.

use std::time::Instant;

use gonvex_protocol::ServerMessage;
use gonvex_runtime::delivery::{BatchConfig, DeliveryHub, ReplicaBatcher, RowEncoding};
use gonvex_runtime::perf_fixture::harness::{AllocSnapshot, CountingAlloc};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, TenantSpec, WHAGONS_SUBSCRIPTIONS,
};
use gonvex_websocket::OutboundBatch;
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const TASKS: &str = "tasks.recentWorkspaceTasks";

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn enabled(name: &str) -> bool {
    std::env::var("BENCH_FILTER").map_or(true, |f| f.split(',').any(|p| name.contains(p.trim())))
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

fn deflated(text: &str) -> usize {
    gonvex_websocket::bench_compress_text(text.as_bytes(), 15)
        .unwrap()
        .map_or(text.len(), |bytes| bytes.len())
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// (pages, raw bytes, deflated bytes, rows, encode ms, omission ms, allocations of the omission)
fn snapshot_bytes(
    opened: &mut gonvex_runtime::replica::ReplicaOpenResult,
    snapshot: &ServerMessage,
    omit: bool,
    iters: usize,
    dump: Option<(&str, &str)>,
) -> Value {
    let rows = match snapshot {
        ServerMessage::ReplicaSnapshot { result, .. } => result.len(),
        _ => 0,
    };
    let mut omission_ms = Vec::new();
    let mut omission_allocs = 0;
    let mut encode_ms = Vec::new();
    let mut pages = Vec::new();
    for _ in 0..iters {
        opened.messages = vec![snapshot.clone()];
        let copy = &mut *opened;
        let before = AllocSnapshot::now();
        let started = Instant::now();
        if omit {
            gonvex_runtime::replica::omit_open_nulls(copy);
        }
        omission_ms.push(started.elapsed().as_secs_f64() * 1e3);
        omission_allocs = AllocSnapshot::now().since(before).allocations;
        let started = Instant::now();
        pages = Fixture::snapshot_page_texts(copy.messages.remove(0));
        encode_ms.push(started.elapsed().as_secs_f64() * 1e3);
    }
    if let Some((dir, name)) = dump {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(
            format!("{dir}/{name}.json"),
            serde_json::to_string(&pages).unwrap(),
        )
        .unwrap();
    }
    let raw: usize = pages.iter().map(String::len).sum();
    let mut deflate_ms = Vec::new();
    let mut compressed = 0;
    for _ in 0..iters.min(3) {
        let started = Instant::now();
        compressed = pages.iter().map(|page| deflated(page)).sum::<usize>();
        deflate_ms.push(started.elapsed().as_secs_f64() * 1e3);
    }
    json!({
        "rows": rows, "pages": pages.len(), "raw_bytes": raw, "deflated_bytes": compressed,
        "deflate_ms": median(deflate_ms),
        "omission_ms": median(omission_ms), "omission_allocations": omission_allocs,
        "page_encode_ms": median(encode_ms),
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let tasks: usize = env("BENCH_TASKS", 10_500);
    let members: usize = env("BENCH_MEMBERS", 78);
    let iters: usize = env("BENCH_ITERS", 7);
    let mut options = FixtureOptions::whagons();
    let mut spec = TenantSpec::new("elrey", tasks, members);
    spec.description_bytes = env("BENCH_DESCRIPTION", 400);
    options.tenants = vec![spec];
    let fixture = Fixture::build(options).await.unwrap();
    let admin = fixture.members_of_kind(0, MemberKind::Admin)[0];
    let session = fixture.session(0, admin);
    let dump = std::env::var("BENCH_DUMP").ok();

    if enabled("snapshot") {
        for path in WHAGONS_SUBSCRIPTIONS {
            // Warm the shared snapshot cache, then a cache hit (the common open).
            fixture.open_replica(&session, path, path).await.unwrap();
            let mut opened = fixture.open_replica(&session, path, path).await.unwrap();
            let dumped = (path == TASKS).then_some(()).and(dump.as_deref());
            let snapshot = opened.messages.first().cloned().expect("snapshot");
            let legacy = snapshot_bytes(
                &mut opened,
                &snapshot,
                false,
                iters,
                dumped.map(|d| (d, "legacy")),
            );
            let omitted = snapshot_bytes(
                &mut opened,
                &snapshot,
                true,
                iters,
                dumped.map(|d| (d, "omit")),
            );
            record(
                "snapshot",
                json!({"path": path, "member": "Admin", "legacy": legacy, "omit": omitted}),
            );
        }
    }

    if enabled("memory") {
        // The shared cache entry holds the rows as SQLite's text (with nulls).
        fixture.runtime.clear_snapshot_cache();
        fixture.open_replica(&session, TASKS, TASKS).await.unwrap();
        let stats = fixture.runtime.snapshot_cache_stats();
        let opened = fixture.open_replica(&session, TASKS, TASKS).await.unwrap();
        let Some(ServerMessage::ReplicaSnapshot { result, .. }) = opened.messages.first() else {
            panic!("snapshot")
        };
        let full: usize = result.iter().map(|row| row.get().len()).sum();
        let stripped: usize = result
            .iter()
            .map(|row| {
                gonvex_protocol::omit_nulls::strip_null_members(row.get())
                    .map_or(row.get().len(), |t| t.len())
            })
            .sum();
        // Cost split of the omission pass: scan + new text, then JsonRow validation.
        let mut scan_ms = Vec::new();
        let mut row_ms = Vec::new();
        for _ in 0..iters {
            let started = Instant::now();
            let texts: Vec<Option<String>> = result
                .iter()
                .map(|row| gonvex_protocol::omit_nulls::strip_null_members(row.get()))
                .collect();
            scan_ms.push(started.elapsed().as_secs_f64() * 1e3);
            let started = Instant::now();
            let rows: Vec<_> = texts
                .into_iter()
                .flatten()
                .map(|text| gonvex_protocol::JsonRow::from_json(text).unwrap())
                .collect();
            row_ms.push(started.elapsed().as_secs_f64() * 1e3);
            drop(rows);
        }
        record(
            "memory",
            json!({"path": TASKS, "rows": result.len(), "cache_stats": format!("{stats:?}"),
                   "row_text_bytes": full, "row_text_bytes_without_nulls": stripped,
                   "strip_scan_ms": median(scan_ms), "json_row_validate_ms": median(row_ms)}),
        );
    }

    if enabled("delta") {
        // Every member's connection with all 10 collections, batch frames.
        let mut connections = Vec::new();
        for member in 0..members {
            connections.push(fixture.connect(0, member).await.unwrap());
        }
        let updates: usize = env("BENCH_UPDATES", 40);
        let tenant_id = fixture.tenants[0].route.tenant_id.clone();
        let task_count = fixture.tenants[0].tasks.len();
        let encodings = [RowEncoding::Json, RowEncoding::OmitNulls];
        // (frames with data, raw bytes, deflated bytes, push+frame ns) per encoding
        let mut totals = [(0usize, 0usize, 0usize, 0u128); 2];
        let mut per_kind = serde_json::Map::new();
        for kind in ["description", "nulled", "valued"] {
            let mut kind_totals = [(0usize, 0usize, 0usize); 2];
            for update in 0..updates {
                let task = (update * 7919 + kind.len() * 131) % task_count;
                let patch = match kind {
                    "description" => json!({"description": format!("edit {update}")}),
                    // A column becomes null, another stays null.
                    "nulled" => json!({"description": null, "flagColor": null}),
                    _ => json!({"description": format!("back {update}"), "flagColor": "red"}),
                };
                let event = fixture.update_task(0, task, patch).await.unwrap();
                for connection in connections.iter_mut() {
                    let messages = fixture
                        .runtime
                        .apply_feed_event(
                            &connection.session,
                            &mut connection.subscriptions,
                            event.clone(),
                        )
                        .await
                        .unwrap();
                    for (index, encoding) in encodings.iter().enumerate() {
                        let hub = DeliveryHub::default();
                        let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
                        batcher.set_enabled(true);
                        let mut batch = OutboundBatch::default();
                        let started = Instant::now();
                        for message in messages.iter().cloned() {
                            let rest = batcher
                                .push(
                                    &mut batch,
                                    &hub,
                                    &tenant_id,
                                    *encoding,
                                    &connection.subscriptions,
                                    message,
                                )
                                .unwrap();
                            assert!(rest.is_none(), "unbatched frame");
                        }
                        let has_data = batcher.has_items();
                        let frame = batcher.take_frame(&mut batch, None);
                        let elapsed = started.elapsed().as_nanos();
                        if let (true, Some(frame)) = (has_data, frame) {
                            let text = std::str::from_utf8(&frame.bytes).unwrap();
                            let (raw, compressed) = (text.len(), deflated(text));
                            totals[index].0 += 1;
                            totals[index].1 += raw;
                            totals[index].2 += compressed;
                            totals[index].3 += elapsed;
                            kind_totals[index].0 += 1;
                            kind_totals[index].1 += raw;
                            kind_totals[index].2 += compressed;
                        }
                    }
                }
            }
            let side = |(frames, raw, compressed): (usize, usize, usize)| {
                json!({"frames": frames, "raw_bytes_per_frame": raw as f64 / frames.max(1) as f64,
                       "deflated_bytes_per_frame": compressed as f64 / frames.max(1) as f64})
            };
            per_kind.insert(
                kind.into(),
                json!({"legacy": side(kind_totals[0]), "omit": side(kind_totals[1])}),
            );
        }
        let side = |(frames, raw, compressed, ns): (usize, usize, usize, u128)| {
            json!({"frames": frames, "raw_bytes": raw, "deflated_bytes": compressed,
                   "raw_bytes_per_frame": raw as f64 / frames.max(1) as f64,
                   "deflated_bytes_per_frame": compressed as f64 / frames.max(1) as f64,
                   "encode_us_per_frame": ns as f64 / 1e3 / frames.max(1) as f64})
        };
        record(
            "delta",
            json!({"updates_per_kind": updates, "connections": members, "kinds": per_kind,
                   "legacy": side(totals[0]), "omit": side(totals[1])}),
        );
    }
    fixture.shutdown().await;
}
