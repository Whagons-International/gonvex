//! Stall micro-driver: what replica work does to the network runtime.
//!
//! Probes, all on the network runtime (the runtime the bench builds; one
//! worker thread, like a 1-vCPU container):
//! - [`lag_probe`]: a task that sleeps 1 ms in a loop and records how late it
//!   wakes (scheduler lag: how long a ready socket waits for the worker).
//! - [`echo_probe`]: a loopback TCP echo served by a network-runtime task and
//!   pinged by a plain OS thread every 5 ms (round trip of a socket message).
//! - [`timed`]: wraps a future and records the duration of every poll that
//!   runs on the network worker thread ([`mark_network_thread`]). The fixture
//!   wraps connection tasks with it, and the offload pool wraps its `Inline`
//!   tasks, so `max poll` covers the heavy work wherever it runs on that thread.
//!
//! Load drivers mirror the websocket loop's calls ([`socket_open`],
//! [`connection_loop`]); they validate the runtime's scheduling, they do not
//! simulate users.
use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use gonvex_protocol::{ReplicaOpenRequest, ServerMessage};
use gonvex_store::TenantSession;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{FixtureResult, MemberConnection};
use crate::change_feed::FeedEvent;
use crate::replica::ReplicaSubscription;
use crate::Runtime;

static NETWORK_THREAD: OnceLock<ThreadId> = OnceLock::new();
static POLLS: Mutex<Vec<(u32, u32)>> = Mutex::new(Vec::new());
static RECORDING: AtomicBool = AtomicBool::new(false);

/// Records the calling thread as the network worker. Call from a task on the
/// network runtime (one worker thread).
pub fn mark_network_thread() {
    let _ = NETWORK_THREAD.set(std::thread::current().id());
}

fn on_network_thread() -> bool {
    NETWORK_THREAD
        .get()
        .is_some_and(|id| *id == std::thread::current().id())
}

/// A future whose polls on the network thread are timed.
pub struct Timed<F>(Pin<Box<F>>);

pub fn timed<F: Future>(future: F) -> Timed<F> {
    Timed(Box::pin(future))
}

impl<F: Future> Future for Timed<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        if !RECORDING.load(Relaxed) || !on_network_thread() {
            return self.0.as_mut().poll(context);
        }
        let started = Instant::now();
        let cpu = thread_cpu_ns();
        let result = self.0.as_mut().poll(context);
        let micros = started.elapsed().as_micros().min(u32::MAX as u128) as u32;
        let cpu_micros = ((thread_cpu_ns() - cpu) / 1000).min(u32::MAX as u64) as u32;
        POLLS.lock().unwrap().push((micros, cpu_micros));
        result
    }
}

/// CPU time of the calling thread (excludes time the OS ran other threads,
/// so it is the work a poll did, independent of machine load).
pub fn thread_cpu_ns() -> u64 {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out pointer.
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };
    now.tv_sec as u64 * 1_000_000_000 + now.tv_nsec as u64
}

/// Starts a measurement window: clears recorded polls.
pub fn start_recording() {
    POLLS.lock().unwrap().clear();
    RECORDING.store(true, Relaxed);
}

/// Ends the window and returns the recorded polls: (wall, thread CPU) in
/// microseconds.
pub fn stop_recording() -> Vec<(u32, u32)> {
    RECORDING.store(false, Relaxed);
    std::mem::take(&mut *POLLS.lock().unwrap())
}

/// Percentile summary of microsecond samples.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub count: usize,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    /// Samples above 5 ms.
    pub over_5ms: usize,
    /// Samples above 50 ms.
    pub over_50ms: usize,
}

impl Summary {
    pub fn of(mut samples: Vec<u64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_unstable();
        let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
        Self {
            count: samples.len(),
            p50_us: at(0.5),
            p99_us: at(0.99),
            p999_us: at(0.999),
            max_us: *samples.last().unwrap(),
            over_5ms: samples.iter().filter(|value| **value > 5_000).count(),
            over_50ms: samples.iter().filter(|value| **value > 50_000).count(),
        }
    }

    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

/// Sleeps 1 ms in a loop until `stop`, returning wake lateness (us).
pub fn lag_probe(stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<Vec<u64>> {
    tokio::spawn(async move {
        let mut lateness = Vec::new();
        while !stop.load(Relaxed) {
            let target = tokio::time::Instant::now() + Duration::from_millis(1);
            tokio::time::sleep_until(target).await;
            lateness.push(target.elapsed().as_micros() as u64);
        }
        lateness
    })
}

/// A loopback echo: the server is a network-runtime task, the client an OS
/// thread that sends 64 bytes every 5 ms and records round trips (us).
pub async fn echo_probe(stop: Arc<AtomicBool>) -> std::thread::JoinHandle<Vec<u64>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(timed(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        let mut buffer = [0u8; 64];
        while stream.read_exact(&mut buffer).await.is_ok() {
            if stream.write_all(&buffer).await.is_err() {
                break;
            }
        }
    }));
    std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        let mut round_trips = Vec::new();
        let mut buffer = [7u8; 64];
        while !stop.load(Relaxed) {
            let started = Instant::now();
            stream.write_all(&buffer).unwrap();
            stream.read_exact(&mut buffer).unwrap();
            round_trips.push(started.elapsed().as_micros() as u64);
            std::thread::sleep(Duration::from_millis(5));
        }
        round_trips
    })
}

/// What [`socket_open`] did.
pub struct SocketOpen {
    pub subscription: Option<ReplicaSubscription>,
    pub rows: usize,
    pub bytes: usize,
    /// The final `replica.ready` (cursor and digest a client persists).
    pub ready: Option<(gonvex_protocol::ReplicaCursor, String)>,
    /// The server asked for the client's hashes.
    pub need_hashes: bool,
}

/// One replica open as the websocket loop runs it: the socket's preparation
/// queue (`enqueue_replica_open`, which runs the build on the offload pool),
/// install, then the paged snapshot encoding of `send_replica_open`. Frames
/// are encoded and dropped instead of written.
pub async fn socket_open(
    runtime: &Runtime,
    session: &TenantSession,
    path: &str,
) -> FixtureResult<SocketOpen> {
    let request: ReplicaOpenRequest =
        serde_json::from_value(json!({"id": path, "path": path, "args": {}}))?;
    socket_open_request(runtime, session, request).await
}

/// [`socket_open`] of any request (a resume carries the client's cursor and
/// digest).
pub async fn socket_open_request(
    runtime: &Runtime,
    session: &TenantSession,
    request: ReplicaOpenRequest,
) -> FixtureResult<SocketOpen> {
    let mut queue = crate::replica_open_queue::ReplicaOpenQueue::default();
    if let Some(error) =
        crate::enqueue_replica_open(runtime, Some(session), request, false, &mut queue).await
    {
        return Err(serde_json::to_string(&error)?.into());
    }
    let outcome = queue
        .next()
        .await
        .ok_or("replica open produced no outcome")?;
    let mut replicas = BTreeMap::new();
    let messages = crate::install_replica_open_outcomes(&mut replicas, vec![outcome]);
    let mut rows = 0;
    let mut bytes = 0;
    let mut ready = None;
    let mut need_hashes = false;
    for message in messages {
        if let ServerMessage::ReplicaReady { cursor, digest, .. } = &message {
            ready = Some((cursor.clone(), digest.clone()));
        }
        need_hashes |= matches!(message, ServerMessage::ReplicaNeedHashes { .. });
        match message {
            ServerMessage::ReplicaSnapshot { ref result, .. } => {
                rows += result.len();
                let mut pages =
                    crate::offload::PageEncoder::new(crate::replica::SnapshotPages::new(message));
                while let Some(page) = pages.next().await {
                    bytes += page?.len();
                }
            }
            ServerMessage::ReplicaError { error, .. } => return Err(error.into()),
            message => bytes += serde_json::to_string(&message)?.len(),
        }
    }
    Ok(SocketOpen {
        subscription: replicas.into_values().next(),
        rows,
        bytes,
        ready,
        need_hashes,
    })
}

/// Counters shared by [`connection_loop`] tasks.
#[derive(Default)]
pub struct FanoutCounters {
    pub deliveries: AtomicUsize,
    pub messages: AtomicUsize,
    pub bytes: AtomicUsize,
    /// Event commit to this connection's last encoded message (us), summed.
    pub latency_us: Mutex<Vec<u64>>,
    pub errors: AtomicU64,
    /// `STALL_BATCH=1`: longest inline fan-out call and longest inline
    /// push + frame of one event on the network thread (us).
    pub fanout_max_us: AtomicU64,
    pub push_max_us: AtomicU64,
}

/// One connection's feed loop as the websocket runs it: receive the event,
/// `apply_feed_event_offloaded`, JSON-encode every message (`send_json`).
pub async fn connection_loop(
    runtime: Runtime,
    mut connection: MemberConnection,
    mut events: tokio::sync::broadcast::Receiver<(Instant, FeedEvent)>,
    counters: Arc<FanoutCounters>,
) -> MemberConnection {
    // `STALL_BATCH=1`: a `replicaBatchFrames` connection, as the socket
    // loop runs it in delivery v4: the shared fan-out outcome inline when
    // its shape has one, else the offloaded full path (recording it).
    let batched = std::env::var("STALL_BATCH").is_ok_and(|value| value.trim() == "1");
    let mut batcher = crate::delivery::ReplicaBatcher::default();
    batcher.set_enabled(batched);
    let mut batch = gonvex_websocket::OutboundBatch::default();
    while let Ok((committed, event)) = events.recv().await {
        if batched {
            let tenant_id = connection.session.route.tenant_id.clone();
            crate::replica::fanout::await_indexes(&connection.subscriptions, &event).await;
            let served = loop {
                // Only the synchronous fan-out call is timed (not waits for
                // another connection's outcome).
                let inline = Instant::now();
                let served = runtime.fan_out(
                    &connection.session,
                    &mut connection.subscriptions,
                    &event,
                    &mut batcher,
                    &mut batch,
                    false,
                );
                counters
                    .fanout_max_us
                    .fetch_max(inline.elapsed().as_micros() as u64, Relaxed);
                match served {
                    crate::replica::fanout::FanOut::Wait(receiver) => {
                        crate::replica::fanout::wait_claim(receiver).await
                    }
                    other => break other,
                }
            };
            let applied = match served {
                crate::replica::fanout::FanOut::Served(result) => {
                    result.map(|_| Vec::new()).map_err(|_| ())
                }
                crate::replica::fanout::FanOut::Wait(_) => unreachable!(),
                crate::replica::fanout::FanOut::Miss { record, claim } => {
                    let applied = runtime
                        .apply_feed_event_offloaded(
                            &connection.session,
                            &mut connection.subscriptions,
                            event.clone(),
                            Some((false, None)),
                            record,
                        )
                        .await
                        .map_err(|_| ());
                    drop(claim);
                    applied
                }
            };
            let Ok(messages) = applied else {
                counters.errors.fetch_add(1, Relaxed);
                continue;
            };
            let pushed = Instant::now();
            let mut bytes = 0;
            for message in messages {
                match batcher.push(
                    &mut batch,
                    &runtime.inner.delivery,
                    &tenant_id,
                    false,
                    &connection.subscriptions,
                    message,
                ) {
                    Ok(None) => {}
                    Ok(Some(message)) => {
                        bytes += serde_json::to_string(&message).map_or(0, |text| text.len())
                    }
                    Err(_) => {
                        counters.errors.fetch_add(1, Relaxed);
                    }
                }
            }
            if let crate::change_feed::FeedEvent::Transaction { revision, .. } = &event {
                batcher.watermark(&mut batch, &tenant_id, &connection.subscriptions, *revision);
            }
            let items = batcher.has_items();
            if let Some(frame) = batcher.take_frame(&mut batch, None) {
                bytes += frame.bytes.len();
            }
            counters
                .push_max_us
                .fetch_max(pushed.elapsed().as_micros() as u64, Relaxed);
            counters.deliveries.fetch_add(1, Relaxed);
            counters.messages.fetch_add(usize::from(items), Relaxed);
            counters.bytes.fetch_add(bytes, Relaxed);
            counters
                .latency_us
                .lock()
                .unwrap()
                .push(committed.elapsed().as_micros() as u64);
            continue;
        }
        match runtime
            .apply_feed_event_offloaded(
                &connection.session,
                &mut connection.subscriptions,
                event,
                None,
                false,
            )
            .await
        {
            Ok(messages) => {
                let mut bytes = 0;
                for message in &messages {
                    bytes += serde_json::to_string(message).map_or(0, |text| text.len());
                }
                counters.deliveries.fetch_add(1, Relaxed);
                counters.messages.fetch_add(messages.len(), Relaxed);
                counters.bytes.fetch_add(bytes, Relaxed);
                counters
                    .latency_us
                    .lock()
                    .unwrap()
                    .push(committed.elapsed().as_micros() as u64);
            }
            Err(_) => {
                counters.errors.fetch_add(1, Relaxed);
            }
        }
    }
    connection
}

/// `apply_feed_event_offloaded` for one fixture connection (the socket path).
pub async fn apply_offloaded(
    runtime: &Runtime,
    connection: &mut MemberConnection,
    event: FeedEvent,
) -> Result<Vec<ServerMessage>, crate::replica::ReplicaError> {
    runtime
        .apply_feed_event_offloaded(
            &connection.session,
            &mut connection.subscriptions,
            event,
            None,
            false,
        )
        .await
}

/// The paged JSON frames of a snapshot message, encoded either through
/// [`crate::offload::PageEncoder`] (`offloaded`) or by iterating the pages
/// on the caller's thread; each frame parsed back, without its random
/// `snapshotId`, so the two can be compared.
pub async fn snapshot_page_frames(message: ServerMessage, offloaded: bool) -> Vec<Value> {
    let pages = crate::replica::SnapshotPages::new(message);
    let mut frames = Vec::new();
    if offloaded {
        let mut encoder = crate::offload::PageEncoder::new(pages);
        while let Some(page) = encoder.next().await {
            frames.push(serde_json::from_str::<Value>(&page.unwrap()).unwrap());
        }
    } else {
        for page in pages {
            frames.push(serde_json::to_value(&page).unwrap());
        }
    }
    for frame in &mut frames {
        frame.as_object_mut().unwrap().remove("snapshotId");
    }
    frames
}
