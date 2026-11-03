//! Delivery pipeline benchmarks (workstreams 1-3): encode once, compress once,
//! one frame per connection per tick. See docs/performance/delivery.md.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench delivery
//!
//! Env as in `baseline` (BENCH_FILTER, BENCH_ITERS, BENCH_JSON_OUT), plus
//! DRIVER_SECONDS (simulated seconds of the 1,950-subscriber driver, default 4).
//!
//! `apply_feed_event` (per-connection visibility + replica state, workstream 5)
//! always runs untimed in setup: these benches time only what happens to the
//! frames it returns.
//!
//! Paths:
//! - `legacy`: per connection, per frame `serde_json` (compact transactions or
//!   native canonical rows) and a new zlib compressor per frame above the
//!   threshold, plus one watermark frame per change: the code before this
//!   branch.
//! - `legacy_reuse`: the same frames with this branch's reset (not rebuilt)
//!   per-thread compressor, as old clients now get.
//! - `v2`: `replica.batch`: items memoized per tenant/revision by content and
//!   compressed once per window; one frame per connection per tick.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use gonvex_protocol::ServerMessage;
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::delivery::{
    perf_legacy_encode, BatchConfig, DeliveryHub, ReplicaBatcher, TenantFrames,
};
use gonvex_runtime::perf_fixture::harness::{AllocSnapshot, BenchConfig, CountingAlloc, Report};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberConnection};
use gonvex_websocket::OutboundBatch;
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const BIG: usize = 0;
/// Browsers offer `client_max_window_bits` without a server window: the
/// runtime then selects 12 (`negotiation.rs`).
const WINDOW: u8 = 12;
const THRESHOLD: usize = 1024;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Path {
    Legacy,
    LegacyReuse,
    V2,
}

impl Path {
    fn label(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::LegacyReuse => "legacy_reuse",
            Self::V2 => "v2",
        }
    }
}

#[derive(Default, Clone, Copy, Debug)]
struct Wire {
    frames: u64,
    raw_bytes: u64,
    wire_bytes: u64,
}

impl Wire {
    fn add(&mut self, other: Wire) {
        self.frames += other.frames;
        self.raw_bytes += other.raw_bytes;
        self.wire_bytes += other.wire_bytes;
    }
}

fn revision(event: &FeedEvent) -> u64 {
    match event {
        FeedEvent::Transaction { revision, .. } => *revision,
        FeedEvent::Reset { .. } => 0,
    }
}

/// Untimed: what `apply_feed_event` hands the send path for each connection.
async fn apply_all(
    fixture: &Fixture,
    connections: &mut [MemberConnection],
    event: &FeedEvent,
) -> Vec<Vec<ServerMessage>> {
    let mut out = Vec::with_capacity(connections.len());
    for connection in connections.iter_mut() {
        out.push(
            fixture
                .runtime
                .apply_feed_event(
                    &connection.session,
                    &mut connection.subscriptions,
                    event.clone(),
                )
                .await
                .unwrap(),
        );
    }
    out
}

/// The pre-batch send path for one connection and one change.
fn legacy_send(messages: Vec<ServerMessage>, revision: u64, native: bool, reuse: bool) -> Wire {
    let mut wire = Wire::default();
    let compress = |text: &[u8]| {
        if reuse {
            gonvex_websocket::perf_compress_text(text, WINDOW, THRESHOLD)
        } else {
            gonvex_websocket::perf_compress_text_fresh(text, WINDOW, THRESHOLD)
        }
        .unwrap()
        .map_or(text.len(), |compressed| compressed.len())
    };
    let watermark = ServerMessage::ReplicaWatermark { revision };
    for mut message in messages.into_iter().chain(std::iter::once(watermark)) {
        let text = perf_legacy_encode(&mut message, native, Some(THRESHOLD)).unwrap();
        wire.frames += 1;
        wire.raw_bytes += text.len() as u64;
        wire.wire_bytes += compress(text.as_bytes()) as u64;
    }
    wire
}

struct V2Connection {
    batcher: ReplicaBatcher,
    batch: OutboundBatch,
}

fn v2_connections(count: usize) -> Vec<V2Connection> {
    (0..count)
        .map(|_| {
            let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
            batcher.set_enabled(true);
            V2Connection {
                batcher,
                batch: OutboundBatch::default(),
            }
        })
        .collect()
}

fn v2_push(
    connection: &mut V2Connection,
    hub: &DeliveryHub,
    tenant: &str,
    native: bool,
    subscriptions: &BTreeMap<String, gonvex_runtime::replica::ReplicaSubscription>,
    messages: Vec<ServerMessage>,
    revision: u64,
) {
    for message in messages {
        let rest = connection
            .batcher
            .push(
                &mut connection.batch,
                hub,
                tenant,
                native,
                subscriptions,
                message,
            )
            .unwrap();
        assert!(rest.is_none(), "unexpected unbatched frame {rest:?}");
    }
    connection
        .batcher
        .watermark(&mut connection.batch, tenant, subscriptions, revision);
}

fn v2_flush(connection: &mut V2Connection) -> Wire {
    let raw = connection.batch.len() as u64;
    match connection
        .batcher
        .take_frame(&mut connection.batch, Some((WINDOW, THRESHOLD)))
    {
        Some(frame) => Wire {
            frames: 1,
            raw_bytes: raw,
            wire_bytes: frame.bytes.len() as u64,
        },
        None => Wire::default(),
    }
}

fn memo_extra(
    extra: &mut BTreeMap<String, Value>,
    before: gonvex_runtime::delivery::MemoStats,
    frames: &TenantFrames,
    ops: f64,
) {
    let after = frames.stats();
    extra.insert(
        "memoEncodesPerOp".into(),
        json!((after.encodes - before.encodes) as f64 / ops),
    );
    extra.insert(
        "memoHitsPerOp".into(),
        json!((after.hits - before.hits) as f64 / ops),
    );
    extra.insert(
        "privateEncodesPerOp".into(),
        json!((after.private_encodes - before.private_encodes) as f64 / ops),
    );
    extra.insert(
        "encodedJsonBytesPerOp".into(),
        json!((after.encoded_bytes - before.encoded_bytes) as f64 / ops),
    );
    extra.insert("memoHeapBytes".into(), json!(frames.memo_bytes()));
}

async fn run() {
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    let mut report = Report::new();
    let tenant_id = fixture.tenants[BIG].route.tenant_id.clone();
    let members = fixture.tenants[BIG].members.len();
    let spotless: Vec<usize> = fixture.tenants[BIG]
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, task)| task.spot_id.is_none())
        .map(|(i, _)| i)
        .collect();
    let mut connections = fixture.connect_all(BIG).await.unwrap();
    let mut edit = 0usize;

    // ---- 1. One task update fanned out to 78 members --------------------
    for native in [false, true] {
        let rows = if native { "native" } else { "json" };
        for path in [Path::Legacy, Path::LegacyReuse, Path::V2] {
            let name = format!(
                "delivery/task_update/{members}_members/{}/{rows}",
                path.label()
            );
            if !report.enabled(&name) {
                continue;
            }
            let hub = DeliveryHub::default();
            let frames_memo = hub.tenant(&tenant_id);
            let stats_before = frames_memo.stats();
            let mut v2 = v2_connections(members);
            let mut wires = Vec::new();
            let mut deferred = 0u64;
            let mut inputs = Vec::new();
            let config = BenchConfig::new(3, 20);
            for _ in 0..config.warmup + config.iterations {
                edit += 1;
                let task = spotless[edit % spotless.len()];
                let event = fixture
                    .update_task(
                        BIG,
                        task,
                        json!({"description": format!("delivery edit {edit} {}", "x".repeat(380))}),
                    )
                    .await
                    .unwrap();
                let messages = apply_all(&fixture, &mut connections, &event).await;
                inputs.push((revision(&event), messages));
            }
            let mut inputs = inputs.into_iter();
            let subscriptions: Vec<_> = connections.iter().map(|c| &c.subscriptions).collect();
            let result = report.run_sync(&name, config, members as f64, "subscriber", |_| {
                let (revision, messages) = inputs.next().unwrap();
                let mut wire = Wire::default();
                for (index, messages) in messages.into_iter().enumerate() {
                    match path {
                        Path::Legacy | Path::LegacyReuse => wire.add(legacy_send(
                            messages,
                            revision,
                            native,
                            path == Path::LegacyReuse,
                        )),
                        Path::V2 => {
                            let connection = &mut v2[index];
                            v2_push(
                                connection,
                                &hub,
                                &tenant_id,
                                native,
                                subscriptions[index],
                                messages,
                                revision,
                            );
                            if connection.batcher.has_items() {
                                wire.add(v2_flush(connection));
                            } else {
                                // No changed row for this member (only its
                                // collection's ready/digest and the watermark):
                                // waits for the idle delay, coalesced with
                                // later changes; not counted per change.
                                deferred += 1;
                                let _ = connection.batcher.take_frame(&mut connection.batch, None);
                            }
                        }
                    }
                }
                wires.push(wire);
            });
            if let Some(result) = result {
                let measured = &wires[wires.len().saturating_sub(config.iterations)..];
                let n = measured.len() as f64;
                let sum = measured.iter().fold(Wire::default(), |mut a, w| {
                    a.add(*w);
                    a
                });
                result
                    .extra
                    .insert("framesPerChange".into(), json!(sum.frames as f64 / n));
                result
                    .extra
                    .insert("jsonBytesPerChange".into(), json!(sum.raw_bytes as f64 / n));
                result.extra.insert(
                    "wireBytesPerChange".into(),
                    json!(sum.wire_bytes as f64 / n),
                );
                result
                    .extra
                    .insert("encodeCompressNsPerChange".into(), json!(result.median_ns));
                if path == Path::V2 {
                    let total = (config.warmup + config.iterations) as f64;
                    result.extra.insert(
                        "deferredNoRowChangeConnectionsPerChange".into(),
                        json!(deferred as f64 / total),
                    );
                    memo_extra(&mut result.extra, stats_before, &frames_memo, total);
                }
            }
        }
    }

    // ---- 2. Presence beats: 10 beats inside one 50 ms tick ---------------
    for path in [Path::Legacy, Path::V2] {
        let beats = 10usize;
        let name = format!(
            "delivery/presence_beats/{beats}_per_tick/{members}_members/{}",
            path.label()
        );
        if !report.enabled(&name) {
            continue;
        }
        let hub = DeliveryHub::default();
        let mut v2 = v2_connections(members);
        let config = BenchConfig::new(2, 10);
        let mut inputs = Vec::new();
        for round in 0..config.warmup + config.iterations {
            let mut tick = Vec::new();
            for beat in 0..beats {
                let event = fixture
                    .beat_presence(BIG, (round * beats + beat) % members)
                    .await
                    .unwrap();
                let messages = apply_all(&fixture, &mut connections, &event).await;
                tick.push((revision(&event), messages));
            }
            inputs.push(tick);
        }
        let mut inputs = inputs.into_iter();
        let subscriptions: Vec<_> = connections.iter().map(|c| &c.subscriptions).collect();
        let mut wires = Vec::new();
        let result = report.run_sync(&name, config, members as f64, "subscriber", |_| {
            let tick = inputs.next().unwrap();
            let mut wire = Wire::default();
            for (revision, messages) in tick {
                for (index, messages) in messages.into_iter().enumerate() {
                    match path {
                        Path::V2 => v2_push(
                            &mut v2[index],
                            &hub,
                            &tenant_id,
                            false,
                            subscriptions[index],
                            messages,
                            revision,
                        ),
                        _ => wire.add(legacy_send(messages, revision, false, false)),
                    }
                }
            }
            if path == Path::V2 {
                for connection in &mut v2 {
                    wire.add(v2_flush(connection));
                }
            }
            wires.push(wire);
        });
        if let Some(result) = result {
            let measured = &wires[wires.len().saturating_sub(config.iterations)..];
            let n = measured.len() as f64;
            let sum = measured.iter().fold(Wire::default(), |mut a, w| {
                a.add(*w);
                a
            });
            result.extra.insert("beatsPerTick".into(), json!(beats));
            result
                .extra
                .insert("framesPerTick".into(), json!(sum.frames as f64 / n));
            result
                .extra
                .insert("jsonBytesPerTick".into(), json!(sum.raw_bytes as f64 / n));
            result
                .extra
                .insert("wireBytesPerTick".into(), json!(sum.wire_bytes as f64 / n));
        }
    }

    // ---- 3. Micro-driver: 1,950 subscribers, 23 task writes/s -----------
    // 78 real connections, each replicated 25x (the copies receive exactly
    // the frames their source connection's apply produced; apply itself is
    // not part of this driver). Simulated time: write k at k/23 s; data
    // batches flush at each 50 ms tick, watermark-only batches each 1 s.
    let copies = 25usize;
    let subscribers = members * copies;
    let seconds: f64 = std::env::var("DRIVER_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4.0);
    let writes = (23.0 * seconds).round() as usize;
    let name_of = |path: Path| {
        format!(
            "delivery/driver/{subscribers}_subscribers/23_writes_per_s/{}",
            path.label()
        )
    };
    if [Path::Legacy, Path::V2]
        .iter()
        .any(|path| report.enabled(&name_of(*path)))
    {
        let mut events = Vec::new();
        for _ in 0..writes {
            edit += 1;
            let task = spotless[edit % spotless.len()];
            let event = fixture
                .update_task(
                    BIG,
                    task,
                    json!({"description": format!("driver edit {edit} {}", "x".repeat(380))}),
                )
                .await
                .unwrap();
            let messages = apply_all(&fixture, &mut connections, &event).await;
            events.push((revision(&event), messages));
        }
        let subscriptions: Vec<_> = connections.iter().map(|c| &c.subscriptions).collect();
        for native in [false, true] {
            for path in [Path::Legacy, Path::V2] {
                let name = format!(
                    "{}/{}",
                    name_of(path),
                    if native { "native" } else { "json" }
                );
                if !report.enabled(&name) {
                    continue;
                }
                let hub = DeliveryHub::default();
                let frames_memo = hub.tenant(&tenant_id);
                let mut v2 = v2_connections(subscribers);
                let mut busy = Duration::ZERO;
                let mut wire = Wire::default();
                let alloc_before = AllocSnapshot::now();
                let tick = 0.05f64;
                let mut next_tick = tick;
                let mut next_idle = 1.0f64;
                let flush_due = |v2: &mut Vec<V2Connection>, data_only: bool, wire: &mut Wire| {
                    for connection in v2.iter_mut() {
                        if connection.batch.is_open()
                            && (!data_only || connection.batcher.has_items())
                        {
                            wire.add(v2_flush(connection));
                        }
                    }
                };
                for (k, (revision, messages)) in events.iter().enumerate() {
                    let at = k as f64 / 23.0;
                    if path == Path::V2 {
                        let started = Instant::now();
                        while next_tick <= at {
                            let idle = next_idle <= next_tick;
                            flush_due(&mut v2, !idle, &mut wire);
                            if idle {
                                next_idle += 1.0;
                            }
                            next_tick += tick;
                        }
                        busy += started.elapsed();
                    }
                    // Untimed: each synthetic subscriber's own copy of its frames.
                    let copies_of: Vec<Vec<ServerMessage>> = (0..subscribers)
                        .map(|s| messages[s % members].clone())
                        .collect();
                    let started = Instant::now();
                    for (s, messages) in copies_of.into_iter().enumerate() {
                        match path {
                            Path::V2 => v2_push(
                                &mut v2[s],
                                &hub,
                                &tenant_id,
                                native,
                                subscriptions[s % members],
                                messages,
                                *revision,
                            ),
                            _ => wire.add(legacy_send(messages, *revision, native, false)),
                        }
                    }
                    busy += started.elapsed();
                }
                if path == Path::V2 {
                    let started = Instant::now();
                    flush_due(&mut v2, false, &mut wire);
                    busy += started.elapsed();
                }
                let allocs = AllocSnapshot::now().since(alloc_before);
                let simulated = writes as f64 / 23.0;
                let mut extra = BTreeMap::new();
                extra.insert("subscribers".into(), json!(subscribers));
                extra.insert("writes".into(), json!(writes));
                extra.insert("simulatedSeconds".into(), json!(simulated));
                extra.insert(
                    "framesPerSecond".into(),
                    json!(wire.frames as f64 / simulated),
                );
                extra.insert(
                    "wireBytesPerSecond".into(),
                    json!(wire.wire_bytes as f64 / simulated),
                );
                extra.insert(
                    "jsonBytesPerSecond".into(),
                    json!(wire.raw_bytes as f64 / simulated),
                );
                extra.insert(
                    "cpuMsPerSecond".into(),
                    json!(busy.as_secs_f64() * 1e3 / simulated),
                );
                extra.insert(
                    "cpuShareOfOneCore".into(),
                    json!(busy.as_secs_f64() / simulated),
                );
                extra.insert(
                    "allocationsPerWrite".into(),
                    json!(allocs.allocations as f64 / writes as f64),
                );
                extra.insert(
                    "bytesAllocatedPerWrite".into(),
                    json!(allocs.allocated_bytes as f64 / writes as f64),
                );
                if path == Path::V2 {
                    memo_extra(&mut extra, Default::default(), &frames_memo, writes as f64);
                }
                extra.insert(
                    "note".into(),
                    json!("excludes apply_feed_event (per-connection visibility/replica state) and socket writes"),
                );
                report.record(&name, extra);
            }
        }

        // ---- 3b. The same streams through a real compressed WebSocket --
        // One loopback socket (client drains on its own OS thread); every
        // member's stream of the driver is replayed through it, so the timed
        // work is the production send path: encode, permessage-deflate,
        // ratchet framing and the socket write. x25 = 1,950 subscribers.
        for path in [Path::Legacy, Path::V2] {
            let name = format!(
                "delivery/socket_send/{members}_streams_x{copies}/23_writes_per_s/{}/json",
                path.label()
            );
            if !report.enabled(&name) {
                continue;
            }
            let (mut socket, drained) = loopback_socket().await;
            let hub = DeliveryHub::default();
            let mut busy = Duration::ZERO;
            let mut frames = 0u64;
            let mut debug_frame = std::env::var_os("DELIVERY_DEBUG_FRAME").is_some();
            for member in 0..members {
                let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
                batcher.set_enabled(true);
                let tick = 0.05f64;
                let mut next_tick = tick;
                let mut next_idle = 1.0f64;
                for (k, (revision, messages)) in events.iter().enumerate() {
                    let at = k as f64 / 23.0;
                    let messages = messages[member].clone();
                    let started = Instant::now();
                    match path {
                        Path::V2 => {
                            while next_tick <= at {
                                let idle = next_idle <= next_tick;
                                if socket.batch().is_open() && (idle || batcher.has_items()) {
                                    if debug_frame {
                                        debug_frame = false;
                                        eprintln!("batch raw bytes {}", socket.batch().len());
                                    }
                                    batcher.flush(&mut socket).await.unwrap();
                                    frames += 1;
                                }
                                if idle {
                                    next_idle += 1.0;
                                }
                                next_tick += tick;
                            }
                            for message in messages {
                                assert!(batcher
                                    .push(
                                        socket.batch_mut(),
                                        &hub,
                                        &tenant_id,
                                        false,
                                        subscriptions[member],
                                        message
                                    )
                                    .unwrap()
                                    .is_none());
                            }
                            batcher.watermark(
                                socket.batch_mut(),
                                &tenant_id,
                                subscriptions[member],
                                *revision,
                            );
                        }
                        _ => {
                            for mut message in messages.into_iter().chain(std::iter::once(
                                ServerMessage::ReplicaWatermark {
                                    revision: *revision,
                                },
                            )) {
                                let text = perf_legacy_encode(&mut message, false, Some(THRESHOLD))
                                    .unwrap();
                                socket
                                    .send(axum::extract::ws::Message::Text(text.into()))
                                    .await
                                    .unwrap();
                                frames += 1;
                            }
                        }
                    }
                    busy += started.elapsed();
                }
                let started = Instant::now();
                if socket.batch().is_open() {
                    batcher.flush(&mut socket).await.unwrap();
                    frames += 1;
                }
                busy += started.elapsed();
            }
            drop(socket);
            let wire_bytes = tokio::task::spawn_blocking(move || drained.join().unwrap())
                .await
                .unwrap();
            let simulated = writes as f64 / 23.0;
            let scale = copies as f64 / simulated;
            let mut extra = BTreeMap::new();
            extra.insert("streams".into(), json!(members));
            extra.insert("scaledSubscribers".into(), json!(subscribers));
            extra.insert("framesPerSecond".into(), json!(frames as f64 * scale));
            extra.insert(
                "wireBytesPerSecond".into(),
                json!(wire_bytes as f64 * scale),
            );
            extra.insert(
                "cpuShareOfOneCore".into(),
                json!(busy.as_secs_f64() * scale),
            );
            extra.insert(
                "nsPerFrame".into(),
                json!(busy.as_nanos() as f64 / frames as f64),
            );
            extra.insert(
                "note".into(),
                json!("send path incl. ratchet framing, deflate and loopback socket writes; excludes apply_feed_event; scaled x25 from 78 real streams"),
            );
            report.record(&name, extra);
        }
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
                    gonvex_websocket::UpgradeOptions {
                        compression: true,
                        threshold: THRESHOLD,
                        max_message: 64 << 20,
                        max_frame: 16 << 20,
                        outbound: Default::default(),
                        socket_fd: None,
                    },
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
        assert!(String::from_utf8_lossy(&header).contains("permessage-deflate"));
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
