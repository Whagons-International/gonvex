//! Outbound queue caps, coalescing signals, stall close and liveness evidence,
//! over real loopback sockets with small kernel buffers.
use super::*;
use axum::{routing::get, Router};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
};

const KERNEL_BUFFER: usize = 16 << 10;

/// The accepted socket's descriptor, as a server hands it to `upgrade`. The
/// test server also shrinks the socket's send buffer here.
#[derive(Clone, Copy)]
struct Fd(i32);
impl<'a> axum::extract::connect_info::Connected<axum::serve::IncomingStream<'a, tokio::net::TcpListener>>
    for Fd
{
    fn connect_info(stream: axum::serve::IncomingStream<'a, tokio::net::TcpListener>) -> Self {
        let _ = socket2::SockRef::from(stream.io()).set_send_buffer_size(KERNEL_BUFFER);
        Fd(std::os::fd::AsRawFd::as_raw_fd(stream.io()))
    }
}

struct Fixture {
    client: TcpStream,
    socket: WebSocket,
    server: tokio::task::JoinHandle<()>,
}

/// One upgraded connection; the server socket is handed to the test.
async fn fixture(compressed: bool, limits: OutboundLimits) -> Fixture {
    let (sender, receiver) = oneshot::channel::<WebSocket>();
    let sender = Arc::new(Mutex::new(Some(sender)));
    let app = Router::new().route(
        "/ws",
        get(move |request: Request| {
            let sender = Arc::clone(&sender);
            async move {
                let fd = request
                    .extensions()
                    .get::<axum::extract::ConnectInfo<Fd>>()
                    .map(|info| info.0 .0);
                let options = UpgradeOptions {
                    compression: true,
                    threshold: 1024,
                    max_message: 1 << 20,
                    max_frame: 1 << 20,
                    outbound: limits,
                    socket_fd: fd,
                };
                upgrade(request, options, move |socket| async move {
                    if let Some(sender) = sender.lock().unwrap().take() {
                        let _ = sender.send(socket);
                    }
                })
                .await
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<Fd>(),
        )
        .await
        .unwrap();
    });
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(KERNEL_BUFFER as u32).unwrap();
    let mut client = socket.connect(address).await.unwrap();
    let extension = if compressed {
        "Sec-WebSocket-Extensions: permessage-deflate\r\n"
    } else {
        ""
    };
    client.write_all(format!("GET /ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{extension}\r\n").as_bytes()).await.unwrap();
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        response.push(client.read_u8().await.unwrap());
    }
    assert!(response.starts_with(b"HTTP/1.1 101"));
    let socket = receiver.await.unwrap();
    assert_eq!(socket.compression_threshold().is_some(), compressed);
    Fixture {
        client,
        socket,
        server,
    }
}

fn limits(stall: Duration) -> OutboundLimits {
    OutboundLimits {
        max_bytes: 256 << 10,
        max_frames: 64,
        low_water: 32 << 10,
        stall_timeout: stall,
    }
}

/// Incompressible text so compressed and plain connections fill buffers alike.
fn payload(size: usize, seed: u32) -> String {
    let mut seed = seed | 1;
    (0..size)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            (b'a' + (seed % 26) as u8) as char
        })
        .collect()
}

/// Read and discard one server frame (headers are never masked).
async fn read_frame(client: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let opcode = client.read_u8().await?;
    let size = match client.read_u8().await? {
        126 => client.read_u16().await? as usize,
        127 => client.read_u64().await? as usize,
        n => n as usize,
    };
    let mut data = vec![0; size];
    client.read_exact(&mut data).await?;
    Ok((opcode, data))
}

#[tokio::test]
async fn a_client_that_stops_reading_is_capped_then_reset() {
    for compressed in [false, true] {
        let stall = Duration::from_millis(400);
        let mut fixture = fixture(compressed, limits(stall)).await;
        let started = std::time::Instant::now();
        let mut sent = 0usize;
        let mut max_backlog = 0usize;
        // The client never reads. Sends succeed until the kernel buffers and
        // the queue are full, then wait; the writer then resets the socket.
        while fixture
            .socket
            .send(ws::Message::Text(payload(16 << 10, sent as u32).into()))
            .await
            .is_ok()
        {
            sent += 1;
            let backlog = fixture.socket.backlog();
            assert!(backlog.bytes <= 256 << 10, "{backlog:?}");
            assert!(backlog.frames <= 64, "{backlog:?}");
            max_backlog = max_backlog.max(backlog.bytes);
            assert!(started.elapsed() < Duration::from_secs(10), "never closed");
        }
        assert!(fixture.socket.send_stalled());
        assert!(fixture.socket.is_closed());
        assert!(max_backlog >= 128 << 10, "queue was used: {max_backlog}");
        assert!(started.elapsed() >= stall);
        assert_eq!(fixture.socket.backlog().bytes, 0, "queued frames dropped");
        // The reader stopped too, and the peer sees a reset, not a close.
        assert!(fixture.socket.next().await.is_none());
        let mut buffer = vec![0; 1 << 20];
        let reset = loop {
            match fixture.client.read(&mut buffer).await {
                Ok(0) => break false,
                Ok(_) => continue,
                Err(error) => break error.kind() == std::io::ErrorKind::ConnectionReset,
            }
        };
        assert!(reset, "abortive close (SO_LINGER 0)");
        fixture.server.abort();
    }
}

#[tokio::test]
async fn a_slow_reader_is_backlogged_but_never_stalled_and_counts_as_alive() {
    let stall = Duration::from_millis(300);
    let mut fixture = fixture(false, limits(stall)).await;
    let total = 48usize;
    let mut reader = fixture.client;
    // 8 KiB every 15 ms is about 530 KB/s: slower than the server produces.
    let drain = tokio::spawn(async move {
        let mut buffer = [0; 8 << 10];
        let mut read = 0;
        while read < total * (16 << 10) {
            match reader.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(n) => read += n,
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        read
    });
    let mut saw_backlog = false;
    for index in 0..total {
        fixture
            .socket
            .send(ws::Message::Text(payload(16 << 10, index as u32).into()))
            .await
            .expect("a client that keeps reading is never closed");
        saw_backlog |= fixture.socket.is_backlogged();
    }
    assert!(saw_backlog);
    let before = tokio::time::Instant::now();
    fixture.socket.writable().await;
    assert!(!fixture.socket.is_backlogged());
    let activity = fixture.socket.last_activity().expect("drain progress");
    assert!(activity >= before - Duration::from_secs(1));
    assert!(drain.await.unwrap() >= (total - 1) * (16 << 10));
    assert!(!fixture.socket.send_stalled());
    fixture.server.abort();
}

#[tokio::test]
async fn small_writes_into_an_idle_socket_are_not_liveness() {
    let mut fixture = fixture(true, limits(Duration::from_secs(5))).await;
    // A client that reads nothing: Pings fit in the kernel buffer, so they
    // never block and are no evidence that the client is alive.
    for _ in 0..3 {
        fixture.socket.send(ws::Message::Ping(bytes::Bytes::new())).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fixture.socket.last_activity(), None);
    assert!(!fixture.socket.is_backlogged());
    // An inbound frame is.
    let masked = [0x8a, 0x80, 1, 2, 3, 4];
    fixture.client.write_all(&masked).await.unwrap();
    assert!(matches!(
        fixture.socket.next().await,
        Some(Ok(ws::Message::Pong(_)))
    ));
    assert!(fixture.socket.last_activity().is_some());
    fixture.server.abort();
}

#[tokio::test]
async fn a_reader_waiting_on_the_application_is_alive() {
    let mut fixture = fixture(true, limits(Duration::from_secs(5))).await;
    // Two messages: the application takes neither, so the reader blocks
    // with one in hand. Silence after that is the server's backpressure.
    for _ in 0..2 {
        fixture
            .client
            .write_all(&[0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2])
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let first = fixture.socket.last_activity().unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(fixture.socket.last_activity().unwrap() > first);
    for _ in 0..2 {
        assert!(matches!(fixture.socket.next().await, Some(Ok(ws::Message::Text(_)))));
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let settled = fixture.socket.last_activity().unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fixture.socket.last_activity().unwrap(), settled);
    fixture.server.abort();
}

#[tokio::test]
async fn try_send_hands_back_frames_when_full_and_queued_frames_flush_after_drop() {
    let mut fixture = fixture(false, limits(Duration::from_secs(5))).await;
    let mut accepted = 0;
    let rejected = loop {
        match fixture
            .socket
            .try_send(ws::Message::Text(payload(16 << 10, accepted).into()))
        {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(message)) => break message,
            Err(TrySendError::Closed) => panic!("closed"),
        }
        assert!(accepted < 1000);
    };
    assert!(matches!(rejected, ws::Message::Text(_)));
    assert!(fixture.socket.is_backlogged());
    fixture
        .socket
        .send(ws::Message::Close(Some(ws::CloseFrame {
            code: 4009,
            reason: "bye".into(),
        })))
        .await
        .unwrap();
    // Dropping the handle keeps every queued frame, then closes.
    drop(fixture.socket);
    let mut texts = 0;
    loop {
        let (opcode, data) = read_frame(&mut fixture.client).await.unwrap();
        match opcode {
            0x81 => texts += 1,
            0x88 => {
                assert_eq!(&data[..2], 4009u16.to_be_bytes());
                break;
            }
            other => panic!("unexpected opcode {other:#x}"),
        }
    }
    assert_eq!(texts, accepted);
    let mut rest = Vec::new();
    fixture.client.read_to_end(&mut rest).await.unwrap();
    fixture.server.abort();
}
