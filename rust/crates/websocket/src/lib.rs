//! Optional RFC 7692 transport. JSON and the application protocol are unchanged.
mod frame_limits;
mod negotiation;
use frame_limits::FrameLimits;
use futures_util::Stream;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use axum::{
    extract::{ws, FromRequestParts, Request},
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
use tokio::{sync::mpsc, task::JoinHandle};
mod inflate;
use inflate::inflate_message;

pub async fn upgrade<F, Fut>(
    mut request: Request,
    enabled: bool,
    threshold: usize,
    max_message: usize,
    max_frame: usize,
    callback: F,
) -> Response
where
    F: FnOnce(WebSocket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let selection = if enabled {
        match negotiation::negotiate(request.headers()) {
            Ok(selection) => selection,
            Err(()) => return StatusCode::BAD_REQUEST.into_response(),
        }
    } else {
        None
    };
    let Some(selection) = selection else {
        let (mut parts, _) = request.into_parts();
        return match ws::WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(upgrade) => upgrade
                .max_message_size(max_message)
                .max_frame_size(max_frame)
                .on_upgrade(move |socket| callback(WebSocket::Plain(Box::new(socket))))
                .into_response(),
            Err(error) => error.into_response(),
        };
    };
    let Ok(mut response) = ratchet_core::server::response_from_headers(
        request.headers(),
        ratchet_core::NoExtProvider,
        &ratchet_core::SubprotocolRegistry::default(),
    )
    .map(|parts| parts.response) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    response
        .headers_mut()
        .insert(SEC_WEBSOCKET_EXTENSIONS, selection.response.clone());
    let upgraded = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        if let Ok(stream) = upgraded.await {
            let extension = Deflate {
                encoder: Encode {
                    window_bits: selection.server_window,
                    threshold,
                },
                decoder: Decode {
                    window_bits: selection.client_window,
                    max_message,
                    compressed: false,
                },
            };
            // Coalesce small frame headers and payloads to avoid delayed-ACK stalls.
            let socket = ratchet_core::WebSocket::from_upgraded(
                ratchet_core::WebSocketConfig {
                    max_message_size: max_message,
                },
                FrameLimits::new(
                    tokio::io::BufWriter::with_capacity(4096, TokioIo::new(stream)),
                    max_frame,
                ),
                Some(extension),
                BytesMut::new(),
                ratchet_core::Role::Server,
            );
            callback(WebSocket::compressed(socket)).await;
        }
    });
    response.map(|_| axum::body::Body::empty()).into_response()
}

type IoStream = FrameLimits<tokio::io::BufWriter<TokioIo<hyper::upgrade::Upgraded>>>;
type CompressedSocket = ratchet_core::WebSocket<IoStream, Deflate>;

pub enum WebSocket {
    Plain(Box<ws::WebSocket>),
    Compressed(Box<Compressed>),
}

pub struct Compressed {
    sender: ratchet_core::Sender<IoStream, Encode>,
    receiver: mpsc::Receiver<Result<ws::Message, axum::Error>>,
    reader: JoinHandle<()>,
}

impl Drop for Compressed {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Stream for WebSocket {
    type Item = Result<ws::Message, axum::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket.as_mut()).poll_next(cx),
            Self::Compressed(socket) => socket.receiver.poll_recv(cx),
        }
    }
}

impl WebSocket {
    fn compressed(socket: CompressedSocket) -> Self {
        let (sender, mut receiver) = socket.split().expect("fresh socket");
        let (tx, rx) = mpsc::channel(1);
        // Ratchet reads are not cancellation safe. Keep them in an owned task;
        // the runtime can cancel channel receives without losing a partial frame.
        let reader = tokio::spawn(async move {
            let mut payload = BytesMut::new();
            loop {
                let result = match receiver.read(&mut payload).await {
                    Ok(ratchet_core::Message::Text) => {
                        match ws::Utf8Bytes::try_from(payload.split().freeze()) {
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
                        Ok(ws::Message::Binary(payload.split().freeze()))
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
                let done = result.is_err() || matches!(result, Ok(ws::Message::Close(_)));
                if tx.send(result).await.is_err() || done {
                    break;
                }
            }
        });
        Self::Compressed(Box::new(Compressed {
            sender,
            receiver: rx,
            reader,
        }))
    }

    pub async fn next(&mut self) -> Option<Result<ws::Message, axum::Error>> {
        match self {
            Self::Plain(socket) => socket.recv().await,
            Self::Compressed(socket) => socket.receiver.recv().await,
        }
    }

    pub async fn send(&mut self, message: ws::Message) -> Result<(), axum::Error> {
        let Self::Compressed(socket) = self else {
            if let Self::Plain(socket) = self {
                return socket.send(message).await;
            }
            unreachable!();
        };
        let result = match message {
            ws::Message::Text(text) => socket.sender.write_text(text.as_str()).await,
            ws::Message::Binary(bytes) => socket.sender.write_binary(bytes).await,
            ws::Message::Ping(bytes) => socket.sender.write_ping(bytes).await,
            ws::Message::Pong(bytes) => socket.sender.write_pong(bytes).await,
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
                socket.sender.close(reason).await
            }
        };
        result.map_err(axum::Error::new)
    }
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
        if payload.len() < self.threshold {
            return Ok(());
        }
        if !header.fin || header.opcode.is_continuation() {
            return Err(io::Error::other(
                "outgoing compression requires a complete message",
            ));
        }
        // Both directions negotiate no context takeover. Allocate codec state for
        // this call rather than retaining an allocation on idle sockets.
        let mut codec = Compress::new_with_window_bits(
            Compression::new(COMPRESSION_LEVEL),
            false,
            self.window_bits,
        );
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
            *payload = encoded.as_slice().into();
            header.rsv1 = true;
        }
        Ok(())
    }
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
mod tests {
    use super::*;
    use axum::{extract::State, routing::get, Router};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn echo(State((enabled, threshold)): State<(bool, usize)>, request: Request) -> Response {
        upgrade(
            request,
            enabled,
            threshold,
            4096,
            4096,
            |mut socket| async move {
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/ws", get(echo))
                    .with_state((enabled, threshold)),
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
        } else {
            frame.push(128 | 126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
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
