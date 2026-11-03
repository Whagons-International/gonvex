//! Delivery v3 end-to-end benchmarks: the whole per-change feed path of every
//! connection (decide + encode + batch + compress), not only the send path.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench delivery_v3
//!
//! Env: BENCH_FILTER, BENCH_ITERS, BENCH_JSON_OUT (see perf-fixture.md);
//! V3_MEMBERS (default 1950) / V3_TASKS (default 9000) for the large tenant;
//! V3_WRITES (default 46: two simulated seconds at 23 writes/s);
//! V3_BURST (default "200") reconnect burst sizes, comma separated.
//!
//! Benchmarks:
//! - `v3/task_update/78_members/{json,native}`: one task update, for each of
//!   the 78 fixture connections (10 Whagons collections each), in sequence as
//!   the connection loops would: `apply_feed_event`, `ReplicaBatcher::push`
//!   of every frame, the watermark, and the batch frame (compressed, window
//!   12) when it holds changed rows. Row-less batches wait for the idle delay
//!   and are not counted per change (as in `delivery.rs`).
//! - `v3/driver/{N}_members/23_writes_per_s/json`: N members of a 9,000-task
//!   tenant each with `tasks.recentWorkspaceTasks`, task writes at 23/s, 50 ms
//!   ticks; every frame goes through one real compressed loopback WebSocket
//!   (ratchet framing, permessage-deflate). CPU = busy time / simulated time.
//! - `v3/reconnect_burst/{n}`: n connections of the 78-member tenant (members
//!   round-robin, all 10 collections) opened concurrently; peak heap and time.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::delivery::{BatchConfig, DeliveryHub, ReplicaBatcher};
use gonvex_runtime::perf_fixture::harness::{
    peak_live_bytes, reset_peak, AllocSnapshot, CountingAlloc, Report,
};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberConnection, TenantSpec};
use gonvex_websocket::OutboundBatch;
use serde_json::json;

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const BIG: usize = 0;
const WINDOW: u8 = 12;
const THRESHOLD: usize = 1024;
const TASKS: &str = "tasks.recentWorkspaceTasks";

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn revision(event: &FeedEvent) -> u64 {
    match event {
        FeedEvent::Transaction { revision, .. } => *revision,
        FeedEvent::Reset { .. } => 0,
    }
}

#[derive(Default, Clone, Copy)]
struct Wire {
    frames: u64,
    raw_bytes: u64,
    wire_bytes: u64,
    receivers: u64,
}

struct Conn {
    batcher: ReplicaBatcher,
    batch: OutboundBatch,
}

fn conns(count: usize) -> Vec<Conn> {
    (0..count)
        .map(|_| {
            let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
            batcher.set_enabled(true);
            Conn {
                batcher,
                batch: OutboundBatch::default(),
            }
        })
        .collect()
}

/// One connection's whole feed step for `event`: apply, push, watermark.
async fn feed_one(
    fixture: &Fixture,
    hub: &DeliveryHub,
    tenant_id: &str,
    native: bool,
    connection: &mut MemberConnection,
    conn: &mut Conn,
    event: &FeedEvent,
) -> bool {
    let messages = fixture
        .runtime
        .apply_feed_event(&connection.session, &mut connection.subscriptions, event.clone())
        .await
        .unwrap();
    let received = !messages.is_empty();
    for message in messages {
        let rest = conn
            .batcher
            .push(
                &mut conn.batch,
                hub,
                tenant_id,
                native,
                &connection.subscriptions,
                message,
            )
            .unwrap();
        assert!(rest.is_none(), "unexpected unbatched frame {rest:?}");
    }
    conn.batcher.watermark(
        &mut conn.batch,
        tenant_id,
        &connection.subscriptions,
        revision(event),
    );
    received
}

async fn run() {
    let mut report = Report::new();
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    let tenant_id = fixture.tenants[BIG].route.tenant_id.clone();
    let members = fixture.tenants[BIG].members.len();
    let spotless: Vec<usize> = fixture.tenants[BIG]
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, task)| task.spot_id.is_none())
        .map(|(i, _)| i)
        .collect();
    let mut edit = 0usize;

    // ---- 1. One task update, whole feed path of 78 connections ----------
    if report.enabled("v3/task_update/") {
        let mut connections = fixture.connect_all(BIG).await.unwrap();
        for native in [false, true] {
            let rows = if native { "native" } else { "json" };
            let name = format!("v3/task_update/{members}_members/{rows}");
            if !report.enabled(&name) {
                continue;
            }
            let hub = DeliveryHub::default();
            let mut v = conns(members);
            let iterations = env_usize("BENCH_ITERS", 20);
            let warmup = 3;
            let mut samples = Vec::new();
            let mut allocs = AllocSnapshot::default();
            let mut sum = Wire::default();
            for i in 0..warmup + iterations {
                edit += 1;
                let task = spotless[edit % spotless.len()];
                let event = fixture
                    .update_task(BIG, task, json!({"description": format!("v3 edit {edit} {}", "x".repeat(380))}))
                    .await
                    .unwrap();
                let mut wire = Wire::default();
                let before = AllocSnapshot::now();
                let started = Instant::now();
                for (connection, conn) in connections.iter_mut().zip(v.iter_mut()) {
                    if feed_one(&fixture, &hub, &tenant_id, native, connection, conn, &event).await {
                        wire.receivers += 1;
                    }
                    if conn.batcher.has_items() {
                        let raw = conn.batch.len() as u64;
                        if let Some(frame) = conn
                            .batcher
                            .take_frame(&mut conn.batch, Some((WINDOW, THRESHOLD)))
                        {
                            wire.frames += 1;
                            wire.raw_bytes += raw;
                            wire.wire_bytes += frame.bytes.len() as u64;
                        }
                    } else {
                        // Row-less: waits for the idle delay (not counted).
                        let _ = conn.batcher.take_frame(&mut conn.batch, None);
                    }
                }
                let elapsed = started.elapsed();
                let delta = AllocSnapshot::now().since(before);
                if i >= warmup {
                    samples.push(elapsed.as_nanos() as f64);
                    allocs.allocations += delta.allocations;
                    allocs.allocated_bytes += delta.allocated_bytes;
                    sum.frames += wire.frames;
                    sum.raw_bytes += wire.raw_bytes;
                    sum.wire_bytes += wire.wire_bytes;
                    sum.receivers += wire.receivers;
                }
            }
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let n = iterations as f64;
            let stats = hub.tenant(&tenant_id).stats();
            let mut extra = BTreeMap::new();
            extra.insert("medianNs".into(), json!(samples[samples.len() / 2]));
            extra.insert("p90Ns".into(), json!(samples[samples.len() * 9 / 10]));
            extra.insert("nsPerSubscriber".into(), json!(samples[samples.len() / 2] / members as f64));
            extra.insert("allocationsPerChange".into(), json!(allocs.allocations as f64 / n));
            extra.insert("bytesAllocatedPerChange".into(), json!(allocs.allocated_bytes as f64 / n));
            extra.insert("framesPerChange".into(), json!(sum.frames as f64 / n));
            extra.insert("receiversPerChange".into(), json!(sum.receivers as f64 / n));
            extra.insert("jsonBytesPerChange".into(), json!(sum.raw_bytes as f64 / n));
            extra.insert("wireBytesPerChange".into(), json!(sum.wire_bytes as f64 / n));
            extra.insert("memo".into(), json!({
                "hits": stats.hits, "encodes": stats.encodes,
                "privateEncodes": stats.private_encodes, "encodedBytes": stats.encoded_bytes,
            }));
            eprintln!("{name}: {extra:?}");
            report.record(&name, extra);
        }
    }

    // ---- 2. Reconnect burst ----------------------------------------------
    for burst in std::env::var("V3_BURST")
        .unwrap_or_else(|_| "200".into())
        .split(',')
        .filter_map(|n| n.trim().parse::<usize>().ok())
    {
        let name = format!("v3/reconnect_burst/{burst}");
        if !report.enabled(&name) {
            continue;
        }
        let base = reset_peak();
        let before = AllocSnapshot::now();
        let started = Instant::now();
        let fixture_ref = &fixture;
        let opened = futures_util::future::join_all(
            (0..burst).map(|i| async move { fixture_ref.connect(BIG, i % members).await }),
        )
        .await;
        let elapsed = started.elapsed();
        let peak = peak_live_bytes() - base;
        let live = AllocSnapshot::now().since(before).live_bytes;
        let ok = opened.iter().filter(|c| c.is_ok()).count();
        let rows: usize = opened
            .iter()
            .filter_map(|c| c.as_ref().ok())
            .map(|c| c.snapshot_rows)
            .sum();
        drop(opened);
        let mut extra = BTreeMap::new();
        extra.insert("connections".into(), json!(burst));
        extra.insert("opened".into(), json!(ok));
        extra.insert("snapshotRows".into(), json!(rows));
        extra.insert("seconds".into(), json!(elapsed.as_secs_f64()));
        extra.insert("peakHeapBytes".into(), json!(peak));
        extra.insert("retainedBytesAfterOpen".into(), json!(live));
        extra.insert("retainedBytesPerConnection".into(), json!(live / burst.max(1) as i64));
        extra.insert(
            "note".into(),
            json!("fixture.connect (open_replica per collection, snapshot messages built and dropped); concurrent on 1 tokio worker"),
        );
        report.record(&name, extra);
    }

    // ---- 3. 1,950 members at 23 writes/s through a real socket ------------
    let large_members = env_usize("V3_MEMBERS", 1950);
    let large_tasks = env_usize("V3_TASKS", 9000);
    let name = format!("v3/driver/{large_members}_members/23_writes_per_s/json");
    if report.enabled(&name) {
        fixture.shutdown().await;
        let started = Instant::now();
        let fixture = Fixture::build(FixtureOptions {
            tenants: vec![TenantSpec::new("large", large_tasks, large_members)],
            ..FixtureOptions::whagons()
        })
        .await
        .unwrap();
        let tenant_id = fixture.tenants[0].route.tenant_id.clone();
        let mut connections = Vec::with_capacity(large_members);
        for member in 0..large_members {
            connections.push(fixture.connect_paths(0, member, &[TASKS]).await.unwrap());
        }
        eprintln!(
            "driver: {large_members} members opened {TASKS} in {:.1}s",
            started.elapsed().as_secs_f64()
        );
        let writes = env_usize("V3_WRITES", 46);
        let tasks = fixture.tenants[0].tasks.len();
        let mut events = Vec::with_capacity(writes);
        for k in 0..writes {
            let task = (k * 7919) % tasks;
            events.push(
                fixture
                    .update_task(0, task, json!({"description": format!("driver {k} {}", "y".repeat(380))}))
                    .await
                    .unwrap(),
            );
        }
        let (mut socket, drained) = loopback_socket().await;
        let hub = DeliveryHub::default();
        let mut v = conns(large_members);
        let tick = 0.05f64;
        let mut busy = Duration::ZERO;
        let mut frames = 0u64;
        let mut receivers = 0u64;
        let before = AllocSnapshot::now();
        let mut next_tick = tick;
        let mut next_idle = 1.0f64;
        for (k, event) in events.iter().enumerate() {
            let at = k as f64 / 23.0;
            let started = Instant::now();
            while next_tick <= at {
                let idle = next_idle <= next_tick;
                for conn in v.iter_mut() {
                    if conn.batch.is_open() && (idle || conn.batcher.has_items()) {
                        std::mem::swap(socket.batch_mut(), &mut conn.batch);
                        conn.batcher.flush(&mut socket).await.unwrap();
                        std::mem::swap(socket.batch_mut(), &mut conn.batch);
                        frames += 1;
                    }
                }
                if idle {
                    next_idle += 1.0;
                }
                next_tick += tick;
            }
            for (connection, conn) in connections.iter_mut().zip(v.iter_mut()) {
                if feed_one(&fixture, &hub, &tenant_id, false, connection, conn, event).await {
                    receivers += 1;
                }
            }
            busy += started.elapsed();
        }
        let started = Instant::now();
        for conn in v.iter_mut() {
            if conn.batch.is_open() {
                std::mem::swap(socket.batch_mut(), &mut conn.batch);
                conn.batcher.flush(&mut socket).await.unwrap();
                std::mem::swap(socket.batch_mut(), &mut conn.batch);
                frames += 1;
            }
        }
        busy += started.elapsed();
        let allocs = AllocSnapshot::now().since(before);
        drop(socket);
        let wire_bytes = tokio::task::spawn_blocking(move || drained.join().unwrap())
            .await
            .unwrap();
        let simulated = writes as f64 / 23.0;
        let mut extra = BTreeMap::new();
        extra.insert("members".into(), json!(large_members));
        extra.insert("writes".into(), json!(writes));
        extra.insert("framesPerSecond".into(), json!(frames as f64 / simulated));
        extra.insert("receiversPerWrite".into(), json!(receivers as f64 / writes as f64));
        extra.insert("wireBytesPerSecond".into(), json!(wire_bytes as f64 / simulated));
        extra.insert("cpuShareOfOneCore".into(), json!(busy.as_secs_f64() / simulated));
        extra.insert("busyMsPerWrite".into(), json!(busy.as_secs_f64() * 1e3 / writes as f64));
        extra.insert("allocationsPerWrite".into(), json!(allocs.allocations as f64 / writes as f64));
        extra.insert("bytesAllocatedPerWrite".into(), json!(allocs.allocated_bytes as f64 / writes as f64));
        extra.insert(
            "note".into(),
            json!("apply_feed_event + batch push + flush through one compressed loopback socket for every member, in sequence on 1 tokio worker"),
        );
        report.record(&name, extra);
        fixture.shutdown().await;
    }
    report.finish();
}

/// A server-side compressed WebSocket over loopback whose client drains and
/// counts bytes on its own OS thread (joined after the socket is dropped).
async fn loopback_socket() -> (gonvex_websocket::WebSocket, std::thread::JoinHandle<u64>) {
    use std::io::{Read, Write};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |request: axum::extract::Request| {
            let tx = tx.clone();
            async move {
                gonvex_websocket::upgrade(
                    request,
                    true,
                    THRESHOLD,
                    64 << 20,
                    16 << 20,
                    move |socket| async move {
                        if let Some(tx) = tx.lock().unwrap().take() {
                            let _ = tx.send(socket);
                        }
                    },
                )
                .await
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        write!(stream, "GET /ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n").unwrap();
        let mut header = Vec::new();
        let mut byte = [0u8; 1];
        while !header.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
        }
        let mut total = 0u64;
        let mut buffer = vec![0u8; 1 << 16];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) | Err(_) => return total,
                Ok(n) => total += n as u64,
            }
        }
    });
    let socket = rx.await.unwrap();
    assert!(socket.is_compressed());
    (socket, client)
}
