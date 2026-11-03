//! Accepted TCP sockets: per-socket kernel buffer caps (no sysctl or
//! privilege needed) and the connect info WebSocket upgrades use to reset a
//! stalled connection.
use std::{io, net::SocketAddr};

use axum::{
    extract::connect_info::Connected,
    serve::{IncomingStream, Listener},
};
use tokio::net::{TcpListener, TcpStream};

/// Socket options applied to every accepted connection. `None` keeps the
/// platform default (and, for the buffers, the kernel's autotuning).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SocketTuning {
    /// `SO_SNDBUF`. Caps unsent plus unacknowledged data per socket; the
    /// kernel doubles the value for its bookkeeping. Disables send autotuning
    /// for that socket only.
    pub send_buffer: Option<usize>,
    /// `SO_RCVBUF`. Disables receive autotuning for that socket only.
    pub receive_buffer: Option<usize>,
    /// `TCP_NOTSENT_LOWAT` (Linux). Keeps the unsent part of the send queue
    /// small, so backpressure reaches the connection's writer early.
    pub notsent_lowat: Option<u32>,
}

impl Default for SocketTuning {
    fn default() -> Self {
        Self {
            send_buffer: Some(256 << 10),
            receive_buffer: None,
            notsent_lowat: Some(128 << 10),
        }
    }
}

impl SocketTuning {
    /// Protocol frames already batch their headers and payloads, so Nagle is
    /// off; then the configured buffer caps.
    pub fn apply(&self, stream: &TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;
        let socket = socket2::SockRef::from(stream);
        if let Some(bytes) = self.send_buffer {
            socket.set_send_buffer_size(bytes)?;
        }
        if let Some(bytes) = self.receive_buffer {
            socket.set_recv_buffer_size(bytes)?;
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(bytes) = self.notsent_lowat {
            socket.set_tcp_notsent_lowat(bytes)?;
        }
        Ok(())
    }
}

/// A TCP listener that tunes every accepted socket.
pub struct TunedListener {
    inner: TcpListener,
    tuning: SocketTuning,
}

impl TunedListener {
    pub fn new(inner: TcpListener, tuning: SocketTuning) -> Self {
        Self { inner, tuning }
    }
}

impl Listener for TunedListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, peer) = Listener::accept(&mut self.inner).await;
        if let Err(error) = self.tuning.apply(&stream) {
            tracing::warn!(%error, "failed to apply socket options");
        }
        (stream, peer)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Connect info: the peer address and the accepted socket's descriptor
/// (`into_make_service_with_connect_info::<AcceptedSocket>()`).
#[derive(Clone, Copy, Debug)]
pub struct AcceptedSocket {
    pub peer: SocketAddr,
    pub fd: Option<i32>,
}

impl AcceptedSocket {
    fn of(stream: &TcpStream, peer: SocketAddr) -> Self {
        #[cfg(unix)]
        let fd = Some(std::os::fd::AsRawFd::as_raw_fd(stream));
        #[cfg(not(unix))]
        let fd = {
            let _ = stream;
            None
        };
        Self { peer, fd }
    }
}

impl Connected<IncomingStream<'_, TunedListener>> for AcceptedSocket {
    fn connect_info(stream: IncomingStream<'_, TunedListener>) -> Self {
        Self::of(stream.io(), *stream.remote_addr())
    }
}

impl Connected<IncomingStream<'_, TcpListener>> for AcceptedSocket {
    fn connect_info(stream: IncomingStream<'_, TcpListener>) -> Self {
        Self::of(stream.io(), *stream.remote_addr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepted_sockets_get_the_configured_caps() {
        let tuning = SocketTuning {
            send_buffer: Some(64 << 10),
            receive_buffer: Some(32 << 10),
            notsent_lowat: Some(16 << 10),
        };
        let mut listener = TunedListener::new(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            tuning,
        );
        let address = listener.local_addr().unwrap();
        let (client, (accepted, _)) = tokio::join!(
            TcpStream::connect(address),
            Listener::accept(&mut listener)
        );
        let _client = client.unwrap();
        assert!(accepted.nodelay().unwrap());
        let socket = socket2::SockRef::from(&accepted);
        // Linux reports twice the requested size (bookkeeping overhead).
        assert!(socket.send_buffer_size().unwrap() >= 64 << 10);
        assert!(socket.send_buffer_size().unwrap() <= 2 * (64 << 10));
        assert!(socket.recv_buffer_size().unwrap() <= 2 * (32 << 10));
        #[cfg(target_os = "linux")]
        assert_eq!(socket.tcp_notsent_lowat().unwrap(), 16 << 10);
    }

    #[tokio::test]
    async fn unset_values_keep_platform_defaults() {
        let plain = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = plain.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(address), plain.accept());
        let _client = client.unwrap();
        let (accepted, _) = accepted.unwrap();
        let default_send = socket2::SockRef::from(&accepted).send_buffer_size().unwrap();
        SocketTuning {
            send_buffer: None,
            receive_buffer: None,
            notsent_lowat: None,
        }
        .apply(&accepted)
        .unwrap();
        assert_eq!(
            socket2::SockRef::from(&accepted).send_buffer_size().unwrap(),
            default_send
        );
    }
}
