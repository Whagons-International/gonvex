//! Optional RFC 7692 transport. JSON and the application protocol are unchanged.
mod connection;
mod frame_limits;
mod negotiation;
pub use connection::{
    Backlog, OutboundLimits, SocketWatch, TrySendError, WebSocket, CLOSE_SEND_STALLED,
};
use connection::{Outbound, Shared, Watched, Writer};
use frame_limits::FrameLimits;
use futures_util::{SinkExt, StreamExt};
use std::{
    future::Future,
    io,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{ws, Request},
    http::{header::SEC_WEBSOCKET_EXTENSIONS, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::BytesMut;
use flate2::{Compress, Compression, FlushCompress};
use hyper_util::rt::TokioIo;
use ratchet_ext::{
    Extension, ExtensionDecoder, ExtensionEncoder, FrameHeader, OpCode, RsvBits,
    SplittableExtension,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
mod inflate;
use inflate::inflate_message;

/// Initial read buffer of a connection that did not negotiate compression.
const PLAIN_READ_BUFFER: usize = 4096;

/// Growing writers (`BytesMut` reassembly, `serde_json::to_string`) double
/// their buffer, so a 65 KB message can sit in a 128 KiB allocation. Queued and
/// handed-over messages keep that whole allocation alive, which is what a
/// stalled client pins; above this much slack the message is moved to an
/// exact-size allocation (one copy, only for large messages).
fn has_slack(len: usize, capacity: usize) -> bool {
    capacity - len > (len / 4).max(4096)
}

fn exact(payload: BytesMut) -> bytes::Bytes {
    if has_slack(payload.len(), payload.capacity()) {
        bytes::Bytes::copy_from_slice(&payload)
    } else {
        payload.freeze()
    }
}

/// A text message whose allocation is no larger than its content (see
/// `has_slack`). Use it for large encoded frames before `send`.
pub fn text_message(mut text: String) -> ws::Message {
    if has_slack(text.len(), text.capacity()) {
        text.shrink_to_fit();
    }
    ws::Message::Text(text.into())
}

/// The upgraded byte stream.
trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
type BoxIo = Box<dyn Io>;

/// How to run one upgraded connection.
#[derive(Clone, Copy, Debug)]
pub struct UpgradeOptions {
    /// Offer permessage-deflate to clients that ask for it.
    pub compression: bool,
    pub threshold: usize,
    pub max_message: usize,
    pub max_frame: usize,
    pub outbound: OutboundLimits,
    /// The accepted TCP socket, so a stalled connection can be reset
    /// (SO_LINGER 0) and its kernel buffers freed at once.
    pub socket_fd: Option<i32>,
}

pub async fn upgrade<F, Fut>(mut request: Request, options: UpgradeOptions, callback: F) -> Response
where
    F: FnOnce(WebSocket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let UpgradeOptions {
        compression,
        threshold,
        max_message,
        max_frame,
        outbound,
        socket_fd,
    } = options;
    let selection = if compression {
        match negotiation::negotiate(request.headers()) {
            Ok(selection) => selection,
            Err(()) => return StatusCode::BAD_REQUEST.into_response(),
        }
    } else {
        None
    };
    let Ok(mut response) = ratchet_core::server::response_from_headers(
        request.headers(),
        ratchet_core::NoExtProvider,
        &ratchet_core::SubprotocolRegistry::default(),
    )
    .map(|parts| parts.response) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if let Some(selection) = &selection {
        response
            .headers_mut()
            .insert(SEC_WEBSOCKET_EXTENSIONS, selection.response.clone());
    }
    let upgraded = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let Ok(stream) = upgraded.await else {
            return;
        };
        let shared = Shared::new(outbound);
        if let Some(fd) = socket_fd {
            shared.set_fd(fd);
        }
        let io: BoxIo = Box::new(TokioIo::new(stream));
        let socket = match selection {
            None => plain(shared, io, max_message, max_frame).await,
            Some(selection) => compressed(shared, io, selection, threshold, max_message, max_frame),
        };
        callback(socket).await;
    });
    response.map(|_| axum::body::Body::empty()).into_response()
}

/// No compression: tungstenite, configured exactly as axum's upgrade did.
async fn plain(shared: Arc<Shared>, io: BoxIo, max_message: usize, max_frame: usize) -> WebSocket {
    use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
    let config = WebSocketConfig::default()
        .max_message_size(Some(max_message))
        .max_frame_size(Some(max_frame))
        // tungstenite allocates and zero-fills 128 KiB per connection by
        // default. Frames larger than this still grow the buffer.
        .read_buffer_size(PLAIN_READ_BUFFER);
    let socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
        Watched::new(io, Arc::clone(&shared)),
        Role::Server,
        Some(config),
    )
    .await;
    let (sink, stream) = socket.split();
    let sink = sink
        .sink_map_err(axum::Error::new)
        .with(|message| std::future::ready(Ok(connection::plain::to_tungstenite(message))));
    let stream = stream.filter_map(|message| {
        std::future::ready(match message {
            Ok(message) => connection::plain::from_tungstenite(message).map(Ok),
            Err(error) => Some(Err(axum::Error::new(error))),
        })
    });
    WebSocket::start(shared, None, Writer::Sink(Box::pin(sink)), |shared, tx| {
        tokio::spawn(gonvex_profiler::instrument(
            gonvex_profiler::TaskKind::WsReader,
            connection::read_stream(shared, Box::pin(stream), tx),
        ))
    })
}

type RatchetIo = FrameLimits<Watched<BoxIo>>;
pub(crate) type RatchetSender = ratchet_core::Sender<RatchetIo, Encode>;

fn compressed(
    shared: Arc<Shared>,
    io: BoxIo,
    selection: negotiation::Agreement,
    threshold: usize,
    max_message: usize,
    max_frame: usize,
) -> WebSocket {
    let alternative = Arc::default();
    let extension = Deflate {
        encoder: Encode {
            window_bits: selection.server_window,
            threshold,
            alternative: Arc::clone(&alternative),
        },
        decoder: Decode {
            window_bits: selection.client_window,
            max_message,
            compressed: false,
        },
    };
    // FrameLimits coalesces small frame headers and payloads to avoid
    // delayed-ACK stalls, and frees that buffer after each flush.
    let socket = ratchet_core::WebSocket::from_upgraded(
        ratchet_core::WebSocketConfig {
            max_message_size: max_message,
        },
        FrameLimits::new(Watched::new(io, Arc::clone(&shared)), max_frame),
        Some(extension),
        BytesMut::new(),
        ratchet_core::Role::Server,
    );
    let (sender, receiver) = socket.split().expect("fresh socket");
    let writer = Writer::Ratchet {
        sender: Box::new(sender),
        alternative,
    };
    WebSocket::start(shared, Some(threshold), writer, |shared, tx| {
        // Ratchet reads are not cancellation safe. Keep them in an owned task;
        // the runtime can cancel channel receives without losing a partial frame.
        tokio::spawn(gonvex_profiler::instrument(
            gonvex_profiler::TaskKind::WsReader,
            read_ratchet(shared, receiver, tx),
        ))
    })
}

async fn read_ratchet(
    shared: Arc<Shared>,
    mut receiver: ratchet_core::Receiver<RatchetIo, Decode>,
    tx: mpsc::Sender<(u64, Result<ws::Message, axum::Error>)>,
) {
    let mut payload = BytesMut::new();
    loop {
        let result = match receiver.read(&mut payload).await {
            // Hand over the whole buffer: `split()` would leave an empty
            // view that keeps the message's allocation alive while idle.
            // `exact` drops growth slack before the message waits anywhere.
            Ok(ratchet_core::Message::Text) => {
                match ws::Utf8Bytes::try_from(exact(std::mem::take(&mut payload))) {
                    Ok(text) => Ok(ws::Message::Text(text)),
                    Err(error) => {
                        let _ = receiver
                            .close(ratchet_core::CloseReason::new(
                                ratchet_core::CloseCode::Invalid,
                                None,
                            ))
                            .await;
                        Err(axum::Error::new(error))
                    }
                }
            }
            Ok(ratchet_core::Message::Binary) => {
                Ok(ws::Message::Binary(exact(std::mem::take(&mut payload))))
            }
            Ok(ratchet_core::Message::Ping(bytes)) => Ok(ws::Message::Ping(bytes)),
            Ok(ratchet_core::Message::Pong(bytes)) => Ok(ws::Message::Pong(bytes)),
            Ok(ratchet_core::Message::Close(reason)) => {
                Ok(ws::Message::Close(reason.map(|reason| ws::CloseFrame {
                    code: reason.code.into(),
                    reason: reason.description.unwrap_or_default().into(),
                })))
            }
            Err(error) => Err(axum::Error::new(error)),
        };
        if !connection::forward(&shared, &tx, result).await {
            break;
        }
    }
}

// Cancellation or an early write error must never leave a candidate armed for
// a later, unrelated text message.
struct AlternativeGuard(Arc<Mutex<Option<Vec<u8>>>>);
impl Drop for AlternativeGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap().take();
    }
}

/// Writes one queued item on a compressed connection.
async fn write_ratchet(
    sender: &mut RatchetSender,
    alternative: &Arc<Mutex<Option<Vec<u8>>>>,
    outbound: Outbound,
) -> Result<(), ratchet_core::Error> {
    let message = match outbound {
        Outbound::BestText { full, patch } => {
            let guard = AlternativeGuard(Arc::clone(alternative));
            *alternative.lock().unwrap() = Some(patch.into_bytes());
            let result = sender.write_text(full).await;
            drop(guard);
            return result;
        }
        Outbound::Message(message) => message,
    };
    match message {
        ws::Message::Text(text) => sender.write_text(text.as_str()).await,
        ws::Message::Binary(bytes) => sender.write_binary(bytes).await,
        ws::Message::Ping(bytes) => sender.write_ping(bytes).await,
        ws::Message::Pong(bytes) => sender.write_pong(bytes).await,
        ws::Message::Close(reason) => {
            let reason = reason
                .map(|reason| ratchet_core::CloseReason {
                    code: ratchet_core::CloseCode::try_from(reason.code.to_be_bytes())
                        .unwrap_or(ratchet_core::CloseCode::Protocol),
                    description: Some(reason.reason.to_string()),
                })
                .unwrap_or_else(|| {
                    ratchet_core::CloseReason::new(ratchet_core::CloseCode::Normal, None)
                });
            sender.close(reason).await
        }
    }
}

/// Records how long an inbound message waited between the socket reader and
/// the connection task.
fn inbound(
    (read_at, message): (u64, Result<ws::Message, axum::Error>),
) -> Result<ws::Message, axum::Error> {
    if read_at != 0 {
        gonvex_profiler::record(
            gonvex_profiler::Stage::WsInboundWait,
            gonvex_profiler::Label::None,
            gonvex_profiler::now_ns().saturating_sub(read_at),
        );
    }
    message
}

const COMPRESSION_LEVEL: u32 = 1;

#[derive(Debug)]
struct Deflate {
    encoder: Encode,
    decoder: Decode,
}
#[derive(Debug)]
struct Encode {
    window_bits: u8,
    threshold: usize,
    alternative: Arc<Mutex<Option<Vec<u8>>>>,
}
#[derive(Debug)]
struct Decode {
    window_bits: u8,
    max_message: usize,
    compressed: bool,
}

impl ExtensionEncoder for Encode {
    type Error = io::Error;
    fn encode(&mut self, payload: &mut BytesMut, header: &mut FrameHeader) -> io::Result<()> {
        if header.opcode == OpCode::Text {
            let alternative = self.alternative.lock().unwrap().take();
            if let Some(alternative) = alternative {
                if !header.fin {
                    return Err(io::Error::other(
                        "outgoing alternatives require a complete message",
                    ));
                }
                let full = compress_text(payload, self.window_bits, self.threshold)?;
                let patch = compress_text(&alternative, self.window_bits, self.threshold)?;
                if patch.as_ref().map_or(alternative.len(), Vec::len)
                    < full.as_ref().map_or(payload.len(), Vec::len)
                {
                    header.rsv1 = patch.is_some();
                    *payload = patch.as_deref().unwrap_or(&alternative).into();
                } else if let Some(full) = full {
                    *payload = full.as_slice().into();
                    header.rsv1 = true;
                }
                return Ok(());
            }
        }
        if payload.len() < self.threshold {
            return Ok(());
        }
        if !header.fin || header.opcode.is_continuation() {
            return Err(io::Error::other(
                "outgoing compression requires a complete message",
            ));
        }
        if let Some(bytes) = compress_text(payload, self.window_bits, self.threshold)? {
            *payload = bytes.as_slice().into();
            header.rsv1 = true;
        }
        Ok(())
    }
}

/// Benchmark hook: the exact per-message compressor the encoder runs.
#[doc(hidden)]
pub fn bench_compress_text(payload: &[u8], window_bits: u8) -> io::Result<Option<Vec<u8>>> {
    compress_text(payload, window_bits, 0)
}

fn compress_text(payload: &[u8], window_bits: u8, threshold: usize) -> io::Result<Option<Vec<u8>>> {
    if payload.len() < threshold {
        return Ok(None);
    }
    let _compress = gonvex_profiler::ScopeTimer::new(
        gonvex_profiler::Stage::WsCompress,
        gonvex_profiler::Label::None,
    );
    // No context takeover, including when comparing alternatives.
    let mut codec =
        Compress::new_with_window_bits(Compression::new(COMPRESSION_LEVEL), false, window_bits);
    let mut encoded = Vec::with_capacity(payload.len() / 4);
    let mut scratch = [0; 8192];
    loop {
        let before_in = codec.total_in();
        let before_out = codec.total_out();
        codec
            .compress(
                &payload[before_in as usize..],
                &mut scratch,
                FlushCompress::Sync,
            )
            .map_err(io::Error::other)?;
        let written = (codec.total_out() - before_out) as usize;
        encoded.extend_from_slice(&scratch[..written]);
        if codec.total_in() as usize == payload.len() && written < scratch.len() {
            break;
        }
        if codec.total_in() == before_in && written == 0 {
            return Err(io::Error::other("compressor stalled"));
        }
    }
    if !encoded.ends_with(&[0, 0, 255, 255]) {
        return Err(io::Error::other("missing deflate trailer"));
    }
    encoded.truncate(encoded.len() - 4);
    if encoded.len() < payload.len() {
        return Ok(Some(encoded));
    }
    Ok(None)
}
impl ExtensionDecoder for Decode {
    type Error = io::Error;
    fn decode(&mut self, payload: &mut BytesMut, header: &mut FrameHeader) -> io::Result<()> {
        if header.opcode == OpCode::Continuation {
            if header.rsv1 {
                return Err(io::Error::other("RSV1 on continuation"));
            }
        } else {
            self.compressed = header.rsv1;
        }
        // Ratchet passes the accumulated payload. Inflate once, at FIN, with a
        // limit applied inside the inflater before it allocates expanded output.
        if self.compressed && header.fin {
            let decoded = inflate_message(payload, self.window_bits, self.max_message)?;
            *payload = decoded.as_slice().into();
            self.compressed = false;
        }
        Ok(())
    }
}

impl ExtensionEncoder for Deflate {
    type Error = io::Error;
    fn encode(&mut self, p: &mut BytesMut, h: &mut FrameHeader) -> io::Result<()> {
        self.encoder.encode(p, h)
    }
}
impl ExtensionDecoder for Deflate {
    type Error = io::Error;
    fn decode(&mut self, p: &mut BytesMut, h: &mut FrameHeader) -> io::Result<()> {
        self.decoder.decode(p, h)
    }
}
impl Extension for Deflate {
    fn bits(&self) -> RsvBits {
        RsvBits {
            rsv1: true,
            rsv2: false,
            rsv3: false,
        }
    }
}
impl SplittableExtension for Deflate {
    type SplitEncoder = Encode;
    type SplitDecoder = Decode;
    fn split(self) -> (Encode, Decode) {
        (self.encoder, self.decoder)
    }
}

#[cfg(test)]
mod outbound_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_messages_do_not_keep_growth_slack() {
        // A 65 KB reassembled message in a doubled 128 KiB buffer.
        let mut grown = BytesMut::with_capacity(128 << 10);
        grown.extend_from_slice(&[b'x'; 65_000]);
        let bytes = exact(grown);
        assert_eq!(bytes.len(), 65_000);
        // The copy owns an exact allocation: converting back is free and
        // reports no spare capacity.
        let vec: Vec<u8> = bytes.into();
        assert!(vec.capacity() - vec.len() < 4096);
        // Small slack is kept (no copy for ordinary messages).
        let mut small = BytesMut::with_capacity(4096);
        small.extend_from_slice(b"hello");
        assert_eq!(exact(small).len(), 5);

        let mut text = String::with_capacity(128 << 10);
        text.push_str(&"y".repeat(65_000));
        let ws::Message::Text(text) = text_message(text) else {
            unreachable!()
        };
        let vec: Vec<u8> = bytes::Bytes::from(text).into();
        assert_eq!(vec.len(), 65_000);
        assert!(vec.capacity() - vec.len() < 4096);
    }

    #[test]
    fn alternative_selection_compares_compressed_bytes_and_reuses_the_winner() {
        for (full, patch, expected) in [
            (
                "a".repeat(4096),
                "short but less compressible".repeat(8),
                "a".repeat(4096),
            ),
            ("a".repeat(4096), "ok".to_owned(), "ok".to_owned()),
            (
                "complete".to_owned(),
                "patch".to_owned(),
                "patch".to_owned(),
            ),
        ] {
            let alternative = Arc::new(Mutex::new(Some(patch.into_bytes())));
            let mut encoder = Encode {
                window_bits: 12,
                threshold: 1024,
                alternative: alternative.clone(),
            };
            let mut payload = BytesMut::from(full.as_bytes());
            let mut header = FrameHeader {
                fin: true,
                rsv1: false,
                rsv2: false,
                rsv3: false,
                opcode: OpCode::Text,
            };
            encoder.encode(&mut payload, &mut header).unwrap();
            let decoded = if header.rsv1 {
                inflate_message(&payload, 12, 8192).unwrap()
            } else {
                payload.to_vec()
            };
            assert_eq!(decoded, expected.as_bytes());
            assert!(alternative.lock().unwrap().is_none());
        }
    }
    use axum::{extract::State, routing::get, Router};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        task::JoinHandle,
    };

    async fn echo(
        State((enabled, threshold, limit)): State<(bool, usize, usize)>,
        request: Request,
    ) -> Response {
        upgrade(
            request,
            UpgradeOptions {
                compression: enabled,
                threshold,
                max_message: limit,
                max_frame: limit,
                outbound: OutboundLimits::default(),
                socket_fd: None,
            },
            move |mut socket| async move {
                assert!(matches!(
                    socket.compression_threshold(),
                    None | Some(_) if socket.compression_threshold().is_none_or(|t| t == threshold)
                ));
                loop {
                    // Exercise the same repeated cancellation as the runtime select loop.
                    let frame = match tokio::time::timeout(
                        std::time::Duration::from_millis(3),
                        socket.next(),
                    )
                    .await
                    {
                        Ok(Some(Ok(frame))) => frame,
                        Ok(_) => return,
                        Err(_) => continue,
                    };
                    match frame {
                        ws::Message::Ping(_) | ws::Message::Pong(_) => continue,
                        ws::Message::Close(_) => return,
                        ws::Message::Text(ref text) if text.as_str() == "send-controls" => {
                            for frame in [
                                ws::Message::Ping(vec![b'x'; 125].into()),
                                ws::Message::Pong(vec![b'x'; 125].into()),
                                ws::Message::Close(Some(ws::CloseFrame {
                                    code: 1008,
                                    reason: "x".repeat(123).into(),
                                })),
                            ] {
                                socket.send(frame).await.unwrap();
                            }
                            return;
                        }
                        ws::Message::Text(ref text) if text.as_str().starts_with("close-code:") => {
                            let code = text.as_str()[11..].parse().unwrap();
                            let _ = socket
                                .send(ws::Message::Close(Some(ws::CloseFrame {
                                    code,
                                    reason: "reason".into(),
                                })))
                                .await;
                            return;
                        }
                        ws::Message::Text(ref text) if text.as_str() == "close-policy" => {
                            let _ = socket
                                .send(ws::Message::Close(Some(ws::CloseFrame {
                                    code: 1008,
                                    reason: "policy".into(),
                                })))
                                .await;
                            return;
                        }
                        frame => {
                            if socket.send(frame).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            },
        )
        .await
    }

    async fn connect(enabled: bool, offer: Option<&str>) -> (TcpStream, String, JoinHandle<()>) {
        connect_with_threshold(enabled, offer, 1024).await
    }

    async fn connect_with_threshold(
        enabled: bool,
        offer: Option<&str>,
        threshold: usize,
    ) -> (TcpStream, String, JoinHandle<()>) {
        connect_with_limit(enabled, offer, threshold, 4096).await
    }

    async fn connect_with_limit(
        enabled: bool,
        offer: Option<&str>,
        threshold: usize,
        limit: usize,
    ) -> (TcpStream, String, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/ws", get(echo))
                    .with_state((enabled, threshold, limit)),
            )
            .await
            .unwrap();
        });
        let mut stream = TcpStream::connect(address).await.unwrap();
        let extension = offer
            .map(|v| format!("Sec-WebSocket-Extensions: {v}\r\n"))
            .unwrap_or_default();
        stream.write_all(format!("GET /ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{extension}\r\n").as_bytes()).await.unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            response.push(stream.read_u8().await.unwrap());
        }
        (
            stream,
            String::from_utf8(response).unwrap().to_lowercase(),
            server,
        )
    }

    fn masked(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![opcode];
        if payload.len() < 126 {
            frame.push(128 | payload.len() as u8);
        } else if payload.len() <= 0xffff {
            frame.push(128 | 126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            frame.push(128 | 127);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        frame.extend_from_slice(&[1, 2, 3, 4]);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
        );
        frame
    }

    async fn read(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let opcode = stream.read_u8().await.unwrap();
        let size = stream.read_u8().await.unwrap();
        assert_eq!(size & 128, 0);
        let size = match size {
            126 => stream.read_u16().await.unwrap() as usize,
            127 => stream.read_u64().await.unwrap() as usize,
            n => n as usize,
        };
        let mut payload = vec![0; size];
        stream.read_exact(&mut payload).await.unwrap();
        (opcode, payload)
    }

    struct TestEncoder;
    impl TestEncoder {
        fn prepare_message(&mut self, data: &[u8]) -> io::Result<TestEncoded> {
            let mut codec = Compress::new_with_window_bits(Compression::new(1), false, 12);
            let mut out = vec![0; data.len() * 2 + 128];
            codec
                .compress(data, &mut out, FlushCompress::Sync)
                .map_err(io::Error::other)?;
            assert_eq!(codec.total_in() as usize, data.len());
            out.truncate(codec.total_out() as usize - 4);
            Ok(TestEncoded(out))
        }
    }
    struct TestEncoded(Vec<u8>);
    impl TestEncoded {
        fn commit(self) -> Vec<u8> {
            self.0
        }
    }
    struct TestDecoder;
    impl TestDecoder {
        fn decompress(&mut self, data: &[u8], _: bool, limit: usize) -> io::Result<Vec<u8>> {
            inflate_message(data, 15, limit)
        }
    }
    fn codecs() -> (TestEncoder, TestDecoder) {
        (TestEncoder, TestDecoder)
    }

    #[tokio::test]
    async fn negotiation_and_legacy_fallback() {
        for (enabled, offer, expected) in [
            (false, Some("permessage-deflate"), false),
            (true, None, false),
            (true, Some("unknown-extension"), false),
            (
                true,
                Some("permessage-deflate; client_max_window_bits"),
                true,
            ),
            (
                true,
                Some("permessage-deflate; server_max_window_bits=10"),
                true,
            ),
        ] {
            let (mut stream, headers, server) = connect(enabled, offer).await;
            assert!(headers.starts_with("http/1.1 101"), "{headers}");
            assert_eq!(headers.contains("sec-websocket-extensions:"), expected);
            if expected {
                assert!(headers.contains("server_no_context_takeover"));
                assert!(headers.contains("client_no_context_takeover"));
                assert!(headers.contains("server_max_window_bits="));
            }
            stream
                .write_all(&masked(0x81, b"old JSON client"))
                .await
                .unwrap();
            assert_eq!(read(&mut stream).await, (0x81, b"old JSON client".to_vec()));
            server.abort();
        }
    }

    #[tokio::test]
    async fn threshold_and_independent_messages() {
        let (mut stream, _, server) =
            connect(true, Some("permessage-deflate; client_max_window_bits")).await;
        let (_, mut decoder) = codecs();
        for size in [1023, 1024, 4096, 1024] {
            let payload = vec![b'x'; size];
            stream.write_all(&masked(0x81, &payload)).await.unwrap();
            let (opcode, data) = read(&mut stream).await;
            if size < 1024 {
                assert_eq!(opcode, 0x81);
                assert_eq!(data, payload);
            } else {
                assert_eq!(opcode, 0xc1);
                assert!(data.len() < payload.len() / 2);
                assert_eq!(decoder.decompress(&data, true, 4096).unwrap(), payload);
            }
        }
        // Preserve policy close codes and reasons across the transport adapter.
        stream
            .write_all(&masked(0x81, b"close-policy"))
            .await
            .unwrap();
        assert_eq!(read(&mut stream).await, (0x88, b"\x03\xf0policy".to_vec()));
        server.abort();
    }

    #[tokio::test]
    async fn compressed_fragmented_input_survives_cancelled_receives() {
        let (mut stream, _, server) = connect(true, Some("permessage-deflate")).await;
        let (mut encoder, _) = codecs();
        let data = encoder
            .prepare_message(b"fragmented JSON message")
            .unwrap()
            .commit();
        let split = data.len() / 2;
        let first = masked(0x41, &data[..split]);
        stream.write_all(&first[..3]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        stream.write_all(&first[3..]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        stream.write_all(&masked(0x89, b"ping")).await.unwrap();
        assert_eq!(read(&mut stream).await, (0x8a, b"ping".to_vec()));
        stream
            .write_all(&masked(0x80, &data[split..]))
            .await
            .unwrap();
        assert_eq!(
            read(&mut stream).await,
            (0x81, b"fragmented JSON message".to_vec())
        );
        server.abort();
    }

    #[tokio::test]
    async fn frames_larger_than_the_read_fragment_round_trip() {
        let mut seed = 9u32;
        let text = (0..60_000)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                b"abcdefghijklmnopqrstuvwxyz0123456789 {}:,"[(seed % 41) as usize]
            })
            .collect::<Vec<_>>();
        let (mut stream, headers, server) = connect_with_limit(
            true,
            Some("permessage-deflate; client_max_window_bits"),
            1024,
            1 << 20,
        )
        .await;
        assert!(headers.contains("permessage-deflate"));
        let (mut encoder, mut decoder) = codecs();
        let compressed = encoder.prepare_message(&text).unwrap().commit();
        assert!(compressed.len() > 2 * frame_limits::READ_FRAGMENT);
        let split = compressed.len() / 3 * 2 + 1;
        for frames in [
            vec![masked(0x81, &text)],
            vec![masked(0xc1, &compressed)],
            // A fragmented compressed message with a ping between fragments.
            vec![
                masked(0x41, &compressed[..split]),
                masked(0x89, b"ping"),
                masked(0x80, &compressed[split..]),
            ],
            // Uneven sizes: one byte past a fragment, and a tiny final fragment.
            vec![
                masked(0x01, &text[..frame_limits::READ_FRAGMENT + 1]),
                masked(0x00, &text[frame_limits::READ_FRAGMENT + 1..text.len() - 3]),
                masked(0x80, &text[text.len() - 3..]),
            ],
        ] {
            let mut wire = Vec::new();
            for frame in &frames {
                wire.extend_from_slice(frame);
            }
            // Byte-sized writes exercise partial headers and payloads.
            for chunk in wire.chunks(7919) {
                stream.write_all(chunk).await.unwrap();
            }
            if frames.len() == 3 && frames[1][0] == 0x89 {
                assert_eq!(read(&mut stream).await, (0x8a, b"ping".to_vec()));
            }
            let (opcode, data) = read(&mut stream).await;
            assert_eq!(opcode, 0xc1);
            assert_eq!(decoder.decompress(&data, true, 1 << 20).unwrap(), text);
        }
        server.abort();
    }

    #[tokio::test]
    async fn invalid_text_closes_with_invalid_payload_code() {
        let (mut stream, _, server) = connect(true, Some("permessage-deflate")).await;
        stream.write_all(&masked(0x81, &[0xff])).await.unwrap();
        let (opcode, payload) = read(&mut stream).await;
        assert_eq!(opcode, 0x88);
        assert_eq!(&payload[..2], &1007u16.to_be_bytes());
        server.abort();
    }

    #[tokio::test]
    async fn low_threshold_sends_uncompressed_control_frames() {
        for threshold in [1, 125] {
            let (mut stream, _, server) =
                connect_with_threshold(true, Some("permessage-deflate"), threshold).await;
            let (_, mut decoder) = codecs();
            let payload = vec![b'x'; 125];
            for opcode in [0x81, 0x82] {
                stream.write_all(&masked(opcode, &payload)).await.unwrap();
                let (header, data) = read(&mut stream).await;
                assert_eq!(header, opcode | 0x40);
                assert_eq!(decoder.decompress(&data, true, 4096).unwrap(), payload);
            }
            stream
                .write_all(&masked(0x81, b"send-controls"))
                .await
                .unwrap();
            for opcode in [0x89, 0x8a, 0x88] {
                let frame =
                    tokio::time::timeout(std::time::Duration::from_secs(1), read(&mut stream))
                        .await
                        .unwrap();
                let mut payload = vec![b'x'; 125];
                if opcode == 0x88 {
                    payload[..2].copy_from_slice(&1008u16.to_be_bytes());
                }
                assert_eq!(frame, (opcode, payload));
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn plain_and_negotiated_transport_parity() {
        for offer in [None, Some("permessage-deflate")] {
            let (mut stream, _, server) = connect(true, offer).await;
            // UTF-8 is validated after reassembly, including a split code point.
            stream
                .write_all(&masked(0x01, &[b'a', 0xe2]))
                .await
                .unwrap();
            stream.write_all(&masked(0x89, b"ping")).await.unwrap();
            assert_eq!(read(&mut stream).await, (0x8a, b"ping".to_vec()));
            stream
                .write_all(&masked(0x8a, b"unsolicited pong"))
                .await
                .unwrap();
            stream
                .write_all(&masked(0x80, &[0x82, 0xac, b'z']))
                .await
                .unwrap();
            assert_eq!(read(&mut stream).await, (0x81, "a€z".as_bytes().to_vec()));
            stream.write_all(&masked(0x02, &[1, 2])).await.unwrap();
            stream.write_all(&masked(0x80, &[3, 4])).await.unwrap();
            assert_eq!(read(&mut stream).await, (0x82, vec![1, 2, 3, 4]));
            // Exactly the limit is accepted for an accumulated message.
            stream
                .write_all(&masked(0x02, &vec![b'x'; 2048]))
                .await
                .unwrap();
            stream
                .write_all(&masked(0x80, &vec![b'x'; 2048]))
                .await
                .unwrap();
            let (opcode, data) = read(&mut stream).await;
            if opcode & 0x40 != 0 {
                assert_eq!(inflate_message(&data, 12, 4096).unwrap(), vec![b'x'; 4096]);
            } else {
                assert_eq!(data, vec![b'x'; 4096]);
            }
            stream
                .write_all(&masked(0x81, b"close-policy"))
                .await
                .unwrap();
            assert_eq!(read(&mut stream).await, (0x88, b"\x03\xf0policy".to_vec()));
            server.abort();
        }
    }

    // Both transports reject invalid data without delivering a message. Their
    // wire shutdown differs: tungstenite yields an error; Ratchet sends 1002,
    // and our text adapter sends 1007. Record those differences explicitly.
    #[tokio::test]
    async fn plain_and_negotiated_reject_oversize_and_invalid_text() {
        for offer in [None, Some("permessage-deflate")] {
            for kind in 0..3 {
                let (mut stream, _, server) = connect(true, offer).await;
                match kind {
                    0 => stream
                        .write_all(&masked(0x82, &vec![b'x'; 4097]))
                        .await
                        .unwrap(),
                    1 => {
                        stream
                            .write_all(&masked(0x02, &vec![b'x'; 2048]))
                            .await
                            .unwrap();
                        stream
                            .write_all(&masked(0x80, &vec![b'x'; 2049]))
                            .await
                            .unwrap();
                    }
                    _ => stream.write_all(&masked(0x81, &[0xff])).await.unwrap(),
                }
                let mut received = Vec::new();
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    stream.read_to_end(&mut received),
                )
                .await
                .unwrap();
                if offer.is_none() {
                    assert!(
                        received.is_empty(),
                        "legacy error does not send a data message"
                    );
                } else {
                    assert_eq!(received[0], 0x88);
                    let code = if kind == 2 { 1007u16 } else { 1002u16 };
                    assert_eq!(&received[2..4], &code.to_be_bytes());
                }
                server.abort();
            }
        }
    }

    #[tokio::test]
    async fn bursts_preserve_order_with_slow_consumers() {
        for offer in [None, Some("permessage-deflate")] {
            let (mut stream, _, server) = connect(true, offer).await;
            for index in 0..128u16 {
                stream
                    .write_all(&masked(0x82, &index.to_be_bytes()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            for index in 0..128u16 {
                assert_eq!(
                    read(&mut stream).await,
                    (0x82, index.to_be_bytes().to_vec())
                );
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn peer_close_response_documents_transport_difference() {
        for offer in [None, Some("permessage-deflate")] {
            let (mut stream, _, server) = connect(true, offer).await;
            stream
                .write_all(&masked(0x88, b"\x0f\xa1bye"))
                .await
                .unwrap();
            let mut received = Vec::new();
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read_to_end(&mut received),
            )
            .await
            .unwrap();
            if offer.is_none() {
                // The callback drops immediately, without flushing tungstenite's queued reply.
                assert!(received.is_empty());
            } else {
                // Ratchet immediately acknowledges the code, without the peer's reason.
                assert_eq!(received, b"\x88\x02\x0f\xa1");
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn application_close_codes_match_legacy_transport() {
        for code in [
            1000u16, 1001, 1008, 1009, 1011, 1012, 1013, 3001, 4001, 4999,
        ] {
            for offer in [None, Some("permessage-deflate")] {
                let (mut stream, _, server) = connect(true, offer).await;
                stream
                    .write_all(&masked(0x81, format!("close-code:{code}").as_bytes()))
                    .await
                    .unwrap();
                let (opcode, payload) = read(&mut stream).await;
                assert_eq!(opcode, 0x88);
                assert_eq!(&payload[..2], &code.to_be_bytes());
                assert_eq!(&payload[2..], b"reason");
                server.abort();
            }
        }
    }

    #[tokio::test]
    async fn compressed_expansion_and_invalid_utf8_are_rejected_on_wire() {
        for (input, code) in [(vec![b'x'; 4097], 1002u16), (vec![255; 10], 1007u16)] {
            let (mut stream, _, server) = connect(true, Some("permessage-deflate")).await;
            let (mut encoder, _) = codecs();
            let data = encoder.prepare_message(&input).unwrap().commit();
            stream.write_all(&masked(0xc1, &data)).await.unwrap();
            let (opcode, payload) = read(&mut stream).await;
            assert_eq!(opcode, 0x88);
            assert_eq!(&payload[..2], &code.to_be_bytes());
            server.abort();
        }
    }

    #[tokio::test]
    async fn malformed_negotiation_and_offer_fallback_on_wire() {
        let (_, headers, server) = connect(true, Some("permessage-deflate; client_max_window_bits=16, permessage-deflate; server_max_window_bits=9")).await;
        assert!(headers.starts_with("http/1.1 101"));
        assert!(headers.contains("server_max_window_bits=9"));
        server.abort();
        let (_, headers, server) = connect(
            true,
            Some("permessage-deflate; server_no_context_takeover=1"),
        )
        .await;
        assert!(headers.starts_with("http/1.1 101"));
        assert!(!headers.contains("sec-websocket-extensions:"));
        server.abort();
        let (_, headers, server) = connect(
            true,
            Some("permessage-deflate; client_max_window_bits=\"12"),
        )
        .await;
        assert!(headers.starts_with("http/1.1 400"));
        server.abort();
    }

    #[test]
    fn incompressible_messages_stay_plain() {
        let mut encoder = Encode {
            window_bits: 12,
            threshold: 1024,
            alternative: Arc::default(),
        };
        let mut seed = 42u32;
        let data: Vec<u8> = (0..2048)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                seed as u8
            })
            .collect();
        let mut payload = BytesMut::from(data.as_slice());
        let mut header = FrameHeader {
            fin: true,
            rsv1: false,
            rsv2: false,
            rsv3: false,
            opcode: OpCode::Binary,
        };
        encoder.encode(&mut payload, &mut header).unwrap();
        assert!(!header.rsv1);
        assert_eq!(payload.as_ref(), data);
    }

    #[test]
    fn large_messages_round_trip_across_compressor_output_chunks() {
        let mut encoder = Encode {
            window_bits: 12,
            threshold: 1024,
            alternative: Arc::default(),
        };
        let mut seed = 42u32;
        for size in [1024, 8192, 131072] {
            let data: Vec<u8> = (0..size)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 17;
                    seed ^= seed << 5;
                    (seed % 16) as u8
                })
                .collect();
            let mut payload = BytesMut::from(data.as_slice());
            let mut header = FrameHeader {
                fin: true,
                rsv1: false,
                rsv2: false,
                rsv3: false,
                opcode: OpCode::Binary,
            };
            encoder.encode(&mut payload, &mut header).unwrap();
            assert!(header.rsv1);
            let (_, mut decoder) = codecs();
            assert_eq!(decoder.decompress(&payload, true, size).unwrap(), data);
        }
    }

    #[test]
    fn idle_extensions_hold_only_settings_and_receive_messages_independently() {
        assert!(std::mem::size_of::<Deflate>() <= 48);
        for bits in [8, 9, 12, 15] {
            assert_eq!(inflate_message(&[0], bits, 0).unwrap(), b"");
        }
        let mut decoder = Decode {
            window_bits: 15,
            max_message: 4096,
            compressed: false,
        };
        let mut header = FrameHeader {
            fin: true,
            rsv1: true,
            rsv2: false,
            rsv3: false,
            opcode: OpCode::Text,
        };
        for size in [4096, 1024, 4096] {
            let (mut encoder, _) = codecs();
            let original = vec![b'x'; size];
            let encoded = encoder.prepare_message(&original).unwrap().commit();
            let mut payload = BytesMut::from(encoded.as_slice());
            decoder.decode(&mut payload, &mut header).unwrap();
            assert_eq!(payload.as_ref(), original);
        }
        // RFC 7692 permits BFINAL streams followed by the stripped empty block.
        let mut payload = BytesMut::from(&[0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00][..]);
        decoder.decode(&mut payload, &mut header).unwrap();
        assert_eq!(payload.as_ref(), b"Hello");
        // An ordinary client auth/request frame never creates an inflater.
        header.rsv1 = false;
        let mut payload = BytesMut::from(&b"small client request"[..]);
        decoder.decode(&mut payload, &mut header).unwrap();
        assert_eq!(payload.as_ref(), b"small client request");
    }

    #[test]
    fn expanded_payload_is_bounded_and_repeated_messages_reset() {
        let (mut encoder, _) = codecs();
        let mut decoder = Decode {
            window_bits: 15,
            max_message: 4096,
            compressed: false,
        };
        let mut header = FrameHeader {
            fin: true,
            rsv1: true,
            rsv2: false,
            rsv3: false,
            opcode: OpCode::Text,
        };
        let data = encoder.prepare_message(&vec![b'x'; 8192]).unwrap().commit();
        let mut payload = BytesMut::from(data.as_slice());
        assert!(decoder.decode(&mut payload, &mut header).is_err());
        assert!(payload.len() < 4096);
        // Fresh decoders can read either message independently, with no dictionary.
        for _ in 0..3 {
            let data = encoder.prepare_message(&vec![b'x'; 2048]).unwrap().commit();
            let (_, mut fresh) = codecs();
            assert_eq!(
                fresh.decompress(&data, true, 2048).unwrap(),
                vec![b'x'; 2048]
            );
        }
    }
}
