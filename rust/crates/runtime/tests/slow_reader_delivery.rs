//! Delivery v3 under backpressure: a connection whose client stops reading
//! during sustained writes gets fewer, newer batches, and its outbound queue
//! never exceeds the transport cap.
//!
//! The driver runs the connection loop's delivery policy with the production
//! pieces: a real compressed WebSocket over loopback (bounded outbound queue,
//! `SocketWatch::is_backlogged`), the fixture's replica subscriptions,
//! `apply_feed_event`, `ReplicaBatcher`, and `delivery::merge_waiting_feed`
//! (the function the loop calls to merge the feed events that waited while
//! the socket was behind). While backlogged it takes no feed event and queues
//! no batch; once writable it merges what waited into one event per row.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test slow_reader_delivery`
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use flate2::{Decompress, FlushDecompress};
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::delivery::{merge_waiting_feed, BatchConfig, DeliveryHub, ReplicaBatcher};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind};
use gonvex_websocket::OutboundLimits;
use serde_json::{json, Value};

const WRITES: usize = 300;
const WINDOW: u8 = 12;

fn limits() -> OutboundLimits {
    OutboundLimits {
        max_bytes: 64 << 10,
        max_frames: 64,
        low_water: 16 << 10,
        stall_timeout: Duration::from_secs(30),
    }
}

/// A compressed server socket whose client reads nothing until `release`,
/// with a tiny receive buffer so the server's queue fills quickly; then it
/// reads everything and returns the WebSocket frames (opcode byte, payload).
async fn slow_socket(
    release: Arc<AtomicBool>,
) -> (gonvex_websocket::WebSocket, std::thread::JoinHandle<Vec<(u8, Vec<u8>)>>) {
    use std::io::Write;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |request: axum::extract::Request| {
            let tx = tx.clone();
            async move {
                gonvex_websocket::upgrade(
                    request,
                    gonvex_websocket::UpgradeOptions {
                        compression: true,
                        threshold: 256,
                        max_message: 64 << 20,
                        max_frame: 16 << 20,
                        outbound: limits(),
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
        let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.connect(&address.into()).unwrap();
        let mut stream: std::net::TcpStream = socket.into();
        write!(stream, "GET /ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n").unwrap();
        let mut header = Vec::new();
        let mut byte = [0u8; 1];
        while !header.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
        }
        while !release.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut bytes = Vec::new();
        let _ = stream.read_to_end(&mut bytes);
        let mut frames = Vec::new();
        let mut at = 0;
        while at + 2 <= bytes.len() {
            let opcode = bytes[at];
            let mut len = (bytes[at + 1] & 127) as usize;
            at += 2;
            if len == 126 {
                len = u16::from_be_bytes([bytes[at], bytes[at + 1]]) as usize;
                at += 2;
            } else if len == 127 {
                len = u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap()) as usize;
                at += 8;
            }
            if at + len > bytes.len() {
                break;
            }
            frames.push((opcode, bytes[at..at + len].to_vec()));
            at += len;
        }
        frames
    });
    let socket = rx.await.unwrap();
    assert!(socket.is_compressed());
    (socket, client)
}

fn inflate(body: &[u8]) -> Vec<u8> {
    let mut input = body.to_vec();
    input.extend_from_slice(&[0, 0, 255, 255]);
    let mut codec = Decompress::new_with_window_bits(false, WINDOW);
    let mut out = Vec::with_capacity(body.len() * 8 + 1024);
    loop {
        let consumed = codec.total_in() as usize;
        if out.capacity() == out.len() {
            out.reserve(out.len());
        }
        codec
            .decompress_vec(&input[consumed..], &mut out, FlushDecompress::Sync)
            .unwrap();
        if codec.total_in() as usize == input.len() && out.len() < out.capacity() {
            return out;
        }
    }
}

fn revision(event: &FeedEvent) -> u64 {
    match event {
        FeedEvent::Transaction { revision, .. } => *revision,
        FeedEvent::Reset { .. } => 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_reader_gets_fewer_newer_batches_and_never_exceeds_the_cap() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let tenant_id = fixture.tenants[0].route.tenant_id.clone();
    let admin = fixture.members_of_kind(0, MemberKind::Admin)[0];
    let mut connection = fixture.connect(0, admin).await.unwrap();
    let task = 0usize;
    let task_id = fixture.tenants[0].tasks[task].id.clone();

    let release = Arc::new(AtomicBool::new(false));
    let (mut socket, client) = slow_socket(Arc::clone(&release)).await;
    let watch = socket.watch();
    let (feed_tx, feed_rx) = tokio::sync::broadcast::channel::<FeedEvent>(4096);
    let mut feed = Some(feed_rx);
    let mut pending = None;
    let hub = DeliveryHub::default();
    let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
    batcher.set_enabled(true);

    let mut catching_up = false;
    let mut batches = 0usize;
    let mut merged_events = 0usize;
    let mut applied_events = 0usize;
    let mut max_backlog = 0usize;
    let mut last_revision = 0u64;
    // The cap: queued (plus in-flight) bytes stay within `max_bytes`; only a
    // single frame on an otherwise empty queue may be larger.
    let step = |socket: &gonvex_websocket::WebSocket, max_backlog: &mut usize| {
        let backlog = socket.backlog();
        *max_backlog = (*max_backlog).max(backlog.bytes);
        assert!(
            backlog.bytes <= limits().max_bytes || backlog.frames <= 1,
            "queued {} bytes in {} frames",
            backlog.bytes,
            backlog.frames
        );
        assert!(backlog.frames <= limits().max_frames);
    };
    let mut written = 0usize;
    let started = std::time::Instant::now();
    // Writes land in the feed while the driver delivers; the client starts
    // reading only after all writes (the worst case for the queue).
    while written < WRITES || feed.as_ref().is_some_and(|rx| !rx.is_empty()) || pending.is_some() {
        if written < WRITES {
            for _ in 0..3.min(WRITES - written) {
                written += 1;
                // Incompressible, so the kernel buffers cannot absorb it all.
                let noise: String = (0..24 << 10)
                    .map(|i| {
                        let x = (written as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                            .wrapping_add((i as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                        char::from(b'a' + ((x ^ (x >> 29)) % 26) as u8)
                    })
                    .collect();
                let description = format!("slow {written} {noise}");
                let event = fixture
                    .update_task(0, task, json!({"description": description}))
                    .await
                    .unwrap();
                last_revision = revision(&event);
                feed_tx.send(event).unwrap();
            }
        } else {
            release.store(true, Ordering::Relaxed);
        }
        step(&socket, &mut max_backlog);
        if watch.is_backlogged() {
            // Behind: produce nothing new; wait briefly for the writer.
            catching_up = true;
            let _ = tokio::time::timeout(Duration::from_millis(2), watch.writable()).await;
            continue;
        }
        // One tick: everything waiting, merged when the socket fell behind.
        let mut taken = 0;
        while let Some(event) = pending.take().map(Result::unwrap).or_else(|| {
            feed.as_mut().and_then(|rx| rx.try_recv().ok())
        }) {
            let event = if catching_up {
                let before = revision(&event);
                let merged = merge_waiting_feed(event, &mut feed, &mut pending, &connection.subscriptions, true);
                if revision(&merged) != before {
                    merged_events += 1;
                }
                merged
            } else {
                event
            };
            applied_events += 1;
            let messages = fixture
                .runtime
                .apply_feed_event(&connection.session.clone(), &mut connection.subscriptions, event.clone())
                .await
                .unwrap();
            for message in messages {
                assert!(batcher
                    .push(socket.batch_mut(), &hub, &tenant_id, false, &connection.subscriptions, message)
                    .unwrap()
                    .is_none());
            }
            batcher.watermark(socket.batch_mut(), &tenant_id, &connection.subscriptions, revision(&event));
            taken += 1;
            if taken >= 3 && !catching_up {
                break;
            }
        }
        if feed.as_ref().is_none_or(|rx| rx.is_empty()) && pending.is_none() {
            catching_up = false;
        }
        if socket.batch().is_open() && socket.try_flush_batch().unwrap() {
            batches += 1;
        }
        step(&socket, &mut max_backlog);
        assert!(started.elapsed() < Duration::from_secs(120), "driver stuck");
    }
    if socket.batch().is_open() {
        socket.flush_batch().await.unwrap();
        batches += 1;
    }
    release.store(true, Ordering::Relaxed);
    drop(socket);
    let frames = tokio::task::spawn_blocking(move || client.join().unwrap())
        .await
        .unwrap();

    // Every batch the client received, in order.
    let mut descriptions = Vec::new();
    let mut watermark = 0;
    let mut bindings: HashMap<String, String> = HashMap::new();
    let mut received = 0usize;
    for (opcode, payload) in &frames {
        if opcode & 0x0f != 1 {
            continue;
        }
        let text = if opcode & 0x40 != 0 { inflate(payload) } else { payload.clone() };
        let batch: Value = serde_json::from_slice(&text).unwrap();
        assert_eq!(batch["type"], "replica.batch");
        received += 1;
        for item in batch["items"].as_array().unwrap() {
            match item["type"].as_str().unwrap() {
                "replica.stream" => {
                    bindings.insert(item["stream"].as_str().unwrap().into(), item["id"].as_str().unwrap().into());
                }
                "replica.delta" => {
                    for row in item["upserts"].as_array().into_iter().flatten() {
                        if row["_id"] == json!(task_id) {
                            descriptions.push(row["description"].as_str().unwrap().to_owned());
                        }
                    }
                }
                _ => {}
            }
        }
        watermark = batch["watermark"].as_u64().unwrap_or(watermark);
    }
    eprintln!(
        "{WRITES} writes, {applied_events} applied events ({merged_events} merged), {batches} batches queued, {received} received, max queued {max_backlog} bytes, {} task versions delivered",
        descriptions.len()
    );
    assert_eq!(received, batches);
    // Fewer: the slow client got far fewer batches and row versions than writes.
    assert!(merged_events > 0, "nothing was coalesced");
    assert!(batches < WRITES / 2, "{batches} batches for {WRITES} writes");
    assert!(descriptions.len() < WRITES / 2, "{} versions", descriptions.len());
    // Newer: it ends at the last write, nothing missing at the end.
    assert_eq!(watermark, last_revision);
    assert!(descriptions.last().unwrap().starts_with(&format!("slow {WRITES} ")));
}
