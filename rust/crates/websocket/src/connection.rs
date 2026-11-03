//! One WebSocket connection: an owned reader task, an owned writer task and a
//! bounded outbound queue between the application and the writer.
//!
//! - The application never blocks on the socket itself. `send` queues a frame
//!   and waits only while the queue is at its byte/frame cap.
//! - The writer closes the connection when a write makes no progress for
//!   `stall_timeout` (a client that stopped reading, or a dead TCP peer), with
//!   an abortive close so the kernel frees the socket's buffers at once.
//! - `is_backlogged`/`writable` let producers coalesce while the client is
//!   behind instead of queueing every intermediate state.
//! - `last_activity` is liveness evidence that does not punish the client for
//!   the server's own backpressure (see its documentation).
use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::extract::ws;
use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, Notify},
    task::AbortHandle,
    time::Instant,
};

/// Close code sent (best effort) when the writer gives up on a connection
/// whose writes made no progress for `stall_timeout`.
pub const CLOSE_SEND_STALLED: u16 = 4008;

/// Per-connection outbound bounds. Bytes are payload bytes before compression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboundLimits {
    /// Queued plus in-flight message bytes. A single larger message is still
    /// accepted when nothing else is queued, so no message is undeliverable.
    pub max_bytes: usize,
    /// Queued plus in-flight frames.
    pub max_frames: usize,
    /// `is_backlogged` is true at or above this many queued bytes, or while a
    /// write is blocked on a full socket.
    pub low_water: usize,
    /// Close the connection when a write makes no progress for this long.
    pub stall_timeout: Duration,
}

impl Default for OutboundLimits {
    fn default() -> Self {
        Self {
            max_bytes: 512 << 10,
            max_frames: 1024,
            low_water: 64 << 10,
            stall_timeout: Duration::from_secs(30),
        }
    }
}

/// Queued outbound work, as seen by producers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Backlog {
    pub bytes: usize,
    pub frames: usize,
    /// The writer is waiting for the socket to accept more bytes.
    pub write_blocked: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError {
    /// The queue is at its cap; coalesce and retry after `writable()`.
    Full(ws::Message),
    Closed,
}

pub(crate) enum Outbound {
    Message(ws::Message),
    /// Send whichever of the two text messages is smaller on the wire.
    BestText { full: String, patch: String },
}

impl Outbound {
    fn bytes(&self) -> usize {
        match self {
            Self::Message(message) => message_bytes(message),
            Self::BestText { full, patch } => full.len() + patch.len(),
        }
    }
}

fn message_bytes(message: &ws::Message) -> usize {
    match message {
        ws::Message::Text(text) => text.len(),
        ws::Message::Binary(bytes) | ws::Message::Ping(bytes) | ws::Message::Pong(bytes) => {
            bytes.len()
        }
        ws::Message::Close(frame) => frame.as_ref().map_or(0, |frame| frame.reason.len() + 2),
    }
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Outbound>,
    /// Queued plus in-flight.
    bytes: usize,
    frames: usize,
    closed: bool,
    /// The application dropped its handle: drain, then close.
    finishing: bool,
}

pub(crate) struct Shared {
    epoch: std::time::Instant,
    limits: OutboundLimits,
    queue: Mutex<Queue>,
    wake_writer: Notify,
    space: Notify,
    /// Milliseconds since `epoch` (+1, so 0 means never).
    inbound_at: AtomicU64,
    drained_at: AtomicU64,
    /// The reader holds a message the application has not taken yet.
    reader_waiting: AtomicBool,
    /// Bytes the socket accepted.
    written: AtomicU64,
    write_blocked: AtomicBool,
    stalled: AtomicBool,
    /// The TCP socket, when known, for an abortive close on stall.
    fd: AtomicI32,
    reader: Mutex<Option<AbortHandle>>,
}

impl Shared {
    pub(crate) fn new(limits: OutboundLimits) -> Arc<Self> {
        Arc::new(Self {
            epoch: std::time::Instant::now(),
            limits,
            queue: Mutex::default(),
            wake_writer: Notify::new(),
            space: Notify::new(),
            inbound_at: AtomicU64::new(0),
            drained_at: AtomicU64::new(0),
            reader_waiting: AtomicBool::new(false),
            written: AtomicU64::new(0),
            write_blocked: AtomicBool::new(false),
            stalled: AtomicBool::new(false),
            fd: AtomicI32::new(-1),
            reader: Mutex::new(None),
        })
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64 + 1
    }

    fn instant(&self, ms: u64) -> Option<Instant> {
        (ms != 0).then(|| Instant::from_std(self.epoch + Duration::from_millis(ms - 1)))
    }

    pub(crate) fn set_fd(&self, fd: i32) {
        self.fd.store(fd, Relaxed);
    }

    pub(crate) fn inbound(&self) {
        self.inbound_at.store(self.now_ms(), Relaxed);
    }

    fn release(&self, bytes: usize) {
        {
            let mut queue = self.queue.lock().unwrap();
            queue.bytes -= bytes;
            queue.frames -= 1;
        }
        self.space.notify_waiters();
    }

    /// Stop the connection: drop queued frames, wake producers, stop the
    /// reader. On a stall the socket is reset (SO_LINGER 0) so unsent data is
    /// discarded and its kernel memory freed immediately.
    fn abort(&self, stalled: bool) {
        {
            let mut queue = self.queue.lock().unwrap();
            queue.closed = true;
            // Only the writer aborts, after releasing its in-flight frame.
            queue.items.clear();
            queue.bytes = 0;
            queue.frames = 0;
        }
        self.space.notify_waiters();
        if stalled {
            self.stalled.store(true, Relaxed);
            let fd = self.fd.load(Relaxed);
            #[cfg(unix)]
            if fd >= 0 {
                // SAFETY: the writer still owns the socket; it is closed only
                // after this returns, when the writer drops its half.
                let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
                let _ = socket2::SockRef::from(&fd).set_linger(Some(Duration::ZERO));
            }
        }
        if let Some(reader) = self.reader.lock().unwrap().take() {
            reader.abort();
        }
    }
}

/// Counts bytes the socket accepted and records when a blocked write resumes:
/// that is the peer acknowledging data, i.e. evidence it is reading.
pub(crate) struct Watched<S> {
    inner: S,
    shared: Arc<Shared>,
}

impl<S> Watched<S> {
    pub(crate) fn new(inner: S, shared: Arc<Shared>) -> Self {
        Self { inner, shared }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Watched<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Watched<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, data);
        match &result {
            Poll::Pending => this.shared.write_blocked.store(true, Relaxed),
            Poll::Ready(Ok(n)) if *n > 0 => {
                this.shared.written.fetch_add(*n as u64, Relaxed);
                if this.shared.write_blocked.swap(false, Relaxed) {
                    this.shared.drained_at.store(this.shared.now_ms(), Relaxed);
                }
            }
            Poll::Ready(_) => {}
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The write half the writer task owns.
pub(crate) enum Writer {
    Ratchet {
        sender: Box<crate::RatchetSender>,
        alternative: Arc<Mutex<Option<Vec<u8>>>>,
    },
    Sink(Pin<Box<dyn Sink<ws::Message, Error = axum::Error> + Send>>),
}

impl Writer {
    async fn write(&mut self, outbound: Outbound) -> Result<(), axum::Error> {
        let _sending = gonvex_profiler::gauge(gonvex_profiler::GaugeId::WsSending).guard();
        let _write = gonvex_profiler::ScopeTimer::new(
            gonvex_profiler::Stage::WsWrite,
            gonvex_profiler::Label::None,
        );
        match self {
            Self::Sink(sink) => {
                let message = match outbound {
                    Outbound::Message(message) => message,
                    Outbound::BestText { full, patch } => ws::Message::Text(
                        if patch.len() < full.len() { patch } else { full }.into(),
                    ),
                };
                sink.send(message).await
            }
            Self::Ratchet {
                sender,
                alternative,
            } => crate::write_ratchet(sender, alternative, outbound)
                .await
                .map_err(axum::Error::new),
        }
    }
}

enum WriteEnd {
    Done(Result<(), axum::Error>),
    Stalled,
}

/// Runs one write; gives up when the socket accepts no bytes for
/// `stall_timeout`. Writers without a byte counter (an axum socket) count
/// only whole messages as progress.
async fn watched_write(shared: &Shared, writer: &mut Writer, outbound: Outbound) -> WriteEnd {
    let stall = shared.limits.stall_timeout;
    let check = (stall / 4).clamp(Duration::from_millis(10), Duration::from_secs(1));
    let write = writer.write(outbound);
    tokio::pin!(write);
    let mut written = shared.written.load(Relaxed);
    let mut progress = Instant::now();
    loop {
        tokio::select! {
            result = &mut write => return WriteEnd::Done(result),
            _ = tokio::time::sleep(check) => {
                let now = shared.written.load(Relaxed);
                if now != written {
                    written = now;
                    progress = Instant::now();
                } else if progress.elapsed() >= stall {
                    return WriteEnd::Stalled;
                }
            }
        }
    }
}

pub(crate) async fn write_loop(shared: Arc<Shared>, mut writer: Writer) {
    loop {
        let outbound = loop {
            let notified = shared.wake_writer.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut queue = shared.queue.lock().unwrap();
                if queue.closed {
                    return;
                }
                if let Some(outbound) = queue.items.pop_front() {
                    break outbound;
                }
                if queue.finishing {
                    queue.closed = true;
                    drop(queue);
                    shared.abort(false);
                    return;
                }
            }
            notified.await;
        };
        let bytes = outbound.bytes();
        let end = watched_write(&shared, &mut writer, outbound).await;
        shared.release(bytes);
        match end {
            WriteEnd::Done(Ok(())) => {}
            WriteEnd::Done(Err(_)) => {
                shared.abort(false);
                return;
            }
            WriteEnd::Stalled => {
                // Best effort: the peer is not reading, so a close frame would
                // only queue behind the unread data. Reset the socket instead.
                tracing_stall(&shared);
                shared.abort(true);
                return;
            }
        }
    }
}

fn tracing_stall(shared: &Shared) {
    let queue = shared.queue.lock().unwrap();
    tracing::warn!(
        stall_timeout = ?shared.limits.stall_timeout,
        queued_bytes = queue.bytes,
        queued_frames = queue.frames,
        close_code = CLOSE_SEND_STALLED,
        "websocket send stalled; resetting the connection"
    );
}

type Inbound = (u64, Result<ws::Message, axum::Error>);

/// Forward one inbound message to the application, recording liveness.
pub(crate) async fn forward(
    shared: &Shared,
    tx: &mpsc::Sender<Inbound>,
    message: Result<ws::Message, axum::Error>,
) -> bool {
    shared.inbound();
    let done = message.is_err() || matches!(message, Ok(ws::Message::Close(_)));
    let read_at = if gonvex_profiler::enabled() {
        gonvex_profiler::now_ns()
    } else {
        0
    };
    shared.reader_waiting.store(true, Relaxed);
    let sent = tx.send((read_at, message)).await.is_ok();
    shared.reader_waiting.store(false, Relaxed);
    sent && !done
}

/// Reader task for a stream-backed socket (tungstenite or axum).
pub(crate) async fn read_stream<S>(shared: Arc<Shared>, mut stream: S, tx: mpsc::Sender<Inbound>)
where
    S: Stream<Item = Result<ws::Message, axum::Error>> + Unpin,
{
    while let Some(message) = stream.next().await {
        if !forward(&shared, &tx, message).await {
            break;
        }
    }
}

/// One WebSocket connection. Dropping it lets the writer flush what is
/// queued (bounded by the caps and the stall timeout), then closes the socket.
pub struct WebSocket {
    threshold: Option<usize>,
    inbound: mpsc::Receiver<Inbound>,
    shared: Arc<Shared>,
}

impl Drop for WebSocket {
    fn drop(&mut self) {
        self.shared.queue.lock().unwrap().finishing = true;
        self.shared.wake_writer.notify_one();
    }
}

impl WebSocket {
    pub(crate) fn start(
        shared: Arc<Shared>,
        threshold: Option<usize>,
        writer: Writer,
        reader: impl FnOnce(Arc<Shared>, mpsc::Sender<Inbound>) -> tokio::task::JoinHandle<()>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let handle = reader(Arc::clone(&shared), tx);
        *shared.reader.lock().unwrap() = Some(handle.abort_handle());
        tokio::spawn(gonvex_profiler::instrument(
            gonvex_profiler::TaskKind::Other,
            write_loop(Arc::clone(&shared), writer),
        ));
        Self {
            threshold,
            inbound: rx,
            shared,
        }
    }

    /// Wrap an axum socket (no compression; progress is counted per message).
    pub fn from_axum(socket: ws::WebSocket, limits: OutboundLimits) -> Self {
        let shared = Shared::new(limits);
        let (sink, stream) = socket.split();
        Self::start(shared, None, Writer::Sink(Box::pin(sink)), |shared, tx| {
            tokio::spawn(read_stream(shared, stream, tx))
        })
    }

    /// The actual negotiated transport threshold; plain fallback cannot compress.
    pub fn compression_threshold(&self) -> Option<usize> {
        self.threshold
    }

    pub async fn next(&mut self) -> Option<Result<ws::Message, axum::Error>> {
        self.inbound.recv().await.map(crate::inbound)
    }

    /// Queue a message. Waits only while the queue is at its cap; fails once
    /// the connection is closed (including after a send stall).
    pub async fn send(&mut self, message: ws::Message) -> Result<(), axum::Error> {
        self.enqueue(Outbound::Message(message)).await
    }

    /// Queue whichever of two complete text messages is smaller on the wire:
    /// compared after compression on a compressed connection (the winner is
    /// not recompressed), as raw bytes on a plain one.
    pub async fn send_best_text(&mut self, full: &str, patch: &str) -> Result<(), axum::Error> {
        self.enqueue(Outbound::BestText {
            full: full.to_owned(),
            patch: patch.to_owned(),
        })
        .await
    }

    /// Queue without waiting. `Full` hands the message back so the caller can
    /// merge it into newer state and retry after `writable()`.
    pub fn try_send(&self, message: ws::Message) -> Result<(), TrySendError> {
        let bytes = message_bytes(&message);
        let mut queue = self.shared.queue.lock().unwrap();
        if queue.closed {
            return Err(TrySendError::Closed);
        }
        if !self.fits(&queue, bytes) {
            return Err(TrySendError::Full(message));
        }
        self.push(&mut queue, Outbound::Message(message), bytes);
        Ok(())
    }

    fn fits(&self, queue: &Queue, bytes: usize) -> bool {
        let limits = &self.shared.limits;
        queue.frames == 0
            || (queue.bytes + bytes <= limits.max_bytes && queue.frames < limits.max_frames)
    }

    fn push(&self, queue: &mut Queue, outbound: Outbound, bytes: usize) {
        queue.items.push_back(outbound);
        queue.bytes += bytes;
        queue.frames += 1;
        self.shared.wake_writer.notify_one();
    }

    async fn enqueue(&mut self, outbound: Outbound) -> Result<(), axum::Error> {
        let bytes = outbound.bytes();
        let mut outbound = Some(outbound);
        loop {
            let space = self.shared.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            {
                let mut queue = self.shared.queue.lock().unwrap();
                if queue.closed {
                    return Err(axum::Error::new(self.closed_error()));
                }
                if self.fits(&queue, bytes) {
                    self.push(&mut queue, outbound.take().unwrap(), bytes);
                    return Ok(());
                }
            }
            space.await;
        }
    }

    fn closed_error(&self) -> io::Error {
        if self.shared.stalled.load(Relaxed) {
            io::Error::new(io::ErrorKind::TimedOut, "WebSocket send stalled")
        } else {
            io::Error::new(io::ErrorKind::BrokenPipe, "WebSocket closed")
        }
    }

    /// A cheap handle for backlog and liveness checks that does not borrow
    /// the socket (usable in the same `select!` as `next()`).
    pub fn watch(&self) -> SocketWatch {
        SocketWatch(Arc::clone(&self.shared))
    }

    pub fn backlog(&self) -> Backlog {
        self.watch().backlog()
    }

    pub fn is_backlogged(&self) -> bool {
        self.watch().is_backlogged()
    }

    pub async fn writable(&self) {
        self.watch().writable().await
    }

    pub fn is_closed(&self) -> bool {
        self.watch().is_closed()
    }

    pub fn send_stalled(&self) -> bool {
        self.watch().send_stalled()
    }

    pub fn last_activity(&self) -> Option<Instant> {
        self.watch().last_activity()
    }

    pub fn limits(&self) -> OutboundLimits {
        self.shared.limits
    }
}

/// Backlog and liveness view of one connection.
#[derive(Clone)]
pub struct SocketWatch(Arc<Shared>);

impl SocketWatch {
    pub fn backlog(&self) -> Backlog {
        let queue = self.0.queue.lock().unwrap();
        Backlog {
            bytes: queue.bytes,
            frames: queue.frames,
            write_blocked: queue.frames > 0 && self.0.write_blocked.load(Relaxed),
        }
    }

    /// The client is behind: producers should merge new state rather than
    /// queue more frames, and send once `writable()` resolves.
    pub fn is_backlogged(&self) -> bool {
        let backlog = self.backlog();
        backlog.bytes >= self.0.limits.low_water || backlog.write_blocked
    }

    /// Resolves when the connection is no longer backlogged, or is closed.
    pub async fn writable(&self) {
        loop {
            let space = self.0.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            if !self.is_backlogged() || self.is_closed() {
                return;
            }
            space.await;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.0.queue.lock().unwrap().closed
    }

    /// The writer gave up on this connection because writes made no progress.
    pub fn send_stalled(&self) -> bool {
        self.0.stalled.load(Relaxed)
    }

    /// Latest evidence that the peer is alive, for an inbound-silence
    /// (liveness) timeout. Counts an inbound frame and a blocked write
    /// resuming (the peer consumed data), and is "now" while the server
    /// itself is the reason for the silence:
    /// - the reader holds a message the application has not taken, or
    /// - frames are still queued for the peer, so its replies (Pongs) may be
    ///   waiting behind that data. Whether such a peer is draining or stuck
    ///   is the send-stall timeout's decision, not the liveness timeout's.
    ///
    /// A client that stopped reading while nothing is queued for it gets no
    /// credit, so it still fails liveness.
    pub fn last_activity(&self) -> Option<Instant> {
        let pending = self.0.queue.lock().unwrap().frames > 0;
        if pending || self.0.reader_waiting.load(Relaxed) {
            return Some(Instant::now());
        }
        let inbound = self.0.inbound_at.load(Relaxed);
        let drained = self.0.drained_at.load(Relaxed);
        self.0.instant(inbound.max(drained))
    }
}

impl Stream for WebSocket {
    type Item = Result<ws::Message, axum::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .inbound
            .poll_recv(cx)
            .map(|item| item.map(crate::inbound))
    }
}

/// tungstenite <-> axum message conversion for the plain transport.
pub(crate) mod plain {
    use super::*;
    use tokio_tungstenite::tungstenite::{self as ts, protocol::frame::coding::CloseCode};

    pub(crate) fn to_tungstenite(message: ws::Message) -> ts::Message {
        // SAFETY (both unchecked conversions): axum's Utf8Bytes is valid UTF-8.
        let text = |text: ws::Utf8Bytes| unsafe { ts::Utf8Bytes::from_bytes_unchecked(Bytes::from(text)) };
        match message {
            ws::Message::Text(value) => ts::Message::Text(text(value)),
            ws::Message::Binary(bytes) => ts::Message::Binary(bytes),
            ws::Message::Ping(bytes) => ts::Message::Ping(bytes),
            ws::Message::Pong(bytes) => ts::Message::Pong(bytes),
            ws::Message::Close(Some(frame)) => {
                ts::Message::Close(Some(ts::protocol::CloseFrame {
                    code: CloseCode::from(frame.code),
                    reason: text(frame.reason),
                }))
            }
            ws::Message::Close(None) => ts::Message::Close(None),
        }
    }

    pub(crate) fn from_tungstenite(message: ts::Message) -> Option<ws::Message> {
        // tungstenite validated UTF-8 already; axum re-checks on conversion.
        let text = |text: ts::Utf8Bytes| ws::Utf8Bytes::try_from(Bytes::from(text)).unwrap_or_default();
        Some(match message {
            ts::Message::Text(value) => ws::Message::Text(text(value)),
            ts::Message::Binary(bytes) => ws::Message::Binary(bytes),
            ts::Message::Ping(bytes) => ws::Message::Ping(bytes),
            ts::Message::Pong(bytes) => ws::Message::Pong(bytes),
            ts::Message::Close(Some(frame)) => ws::Message::Close(Some(ws::CloseFrame {
                code: frame.code.into(),
                reason: text(frame.reason),
            })),
            ts::Message::Close(None) => ws::Message::Close(None),
            ts::Message::Frame(_) => return None,
        })
    }
}
