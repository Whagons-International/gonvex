//! Anonymous browser admission and process-local abuse budgets.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicLimits {
    pub connection_calls: usize,
    pub ip_calls: usize,
    pub payload_bytes: usize,
    pub connections_per_ip: usize,
    pub trusted_proxies: Vec<IpAddr>,
    pub pending_per_ip: usize,
    pub auth_timeout: Duration,
    pub action_timeout: Duration,
}

impl Default for PublicLimits {
    fn default() -> Self {
        Self {
            connection_calls: 30,
            ip_calls: 120,
            payload_bytes: 64 << 10,
            connections_per_ip: 20,
            trusted_proxies: Vec::new(),
            pending_per_ip: 100,
            auth_timeout: Duration::from_secs(10),
            action_timeout: Duration::from_secs(60),
        }
    }
}

pub(crate) struct Window {
    started: Instant,
    count: usize,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            count: 0,
        }
    }
}

impl Window {
    fn allow(&mut self, count: usize, limit: usize) -> bool {
        if self.started.elapsed() >= Duration::from_secs(60) {
            *self = Self::default();
        }
        if count > limit.saturating_sub(self.count) {
            return false;
        }
        self.count += count;
        true
    }
}

#[derive(Default)]
pub(crate) struct PublicLimiter {
    windows: Mutex<BTreeMap<IpAddr, Window>>,
    connections: Mutex<BTreeMap<IpAddr, usize>>,
    pending: Mutex<BTreeMap<IpAddr, usize>>,
}

pub(crate) struct PublicConnection {
    limiter: Arc<PublicLimiter>,
    ip: IpAddr,
    pending: bool,
}

impl Drop for PublicConnection {
    fn drop(&mut self) {
        let mut connections = if self.pending {
            &self.limiter.pending
        } else {
            &self.limiter.connections
        }
        .lock()
        .unwrap();
        if let Some(count) = connections.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                connections.remove(&self.ip);
            }
        }
    }
}

impl PublicLimiter {
    pub(crate) fn admit(self: &Arc<Self>, ip: IpAddr, limit: usize) -> Option<PublicConnection> {
        let mut connections = self.connections.lock().unwrap();
        let count = connections.entry(ip).or_default();
        if *count >= limit {
            return None;
        }
        *count += 1;
        Some(PublicConnection {
            limiter: self.clone(),
            ip,
            pending: false,
        })
    }

    pub(crate) fn admit_pending(
        self: &Arc<Self>,
        ip: IpAddr,
        limit: usize,
    ) -> Option<PublicConnection> {
        let mut pending = self.pending.lock().unwrap();
        let count = pending.entry(ip).or_default();
        if *count >= limit {
            return None;
        }
        *count += 1;
        Some(PublicConnection {
            limiter: self.clone(),
            ip,
            pending: true,
        })
    }

    pub(crate) fn allow(
        &self,
        ip: IpAddr,
        connection: &mut Window,
        count: usize,
        limits: &PublicLimits,
    ) -> bool {
        let mut windows = self.windows.lock().unwrap();
        windows.retain(|_, window| window.started.elapsed() < Duration::from_secs(120));
        let ip_allowed = windows.entry(ip).or_default().allow(count, limits.ip_calls);
        let connection_allowed = connection.allow(count, limits.connection_calls);
        ip_allowed && connection_allowed
    }
}

pub(crate) fn client_ip(
    peer: IpAddr,
    headers: &axum::http::HeaderMap,
    limits: &PublicLimits,
) -> IpAddr {
    if limits.trusted_proxies.contains(&peer) {
        // Trusted proxies must replace this header, never append untrusted input.
        if let Some(ip) = headers
            .get("x-real-ip")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
        {
            return ip;
        }
    }
    peer
}

pub(crate) struct PublicAdmission {
    checked: tokio::sync::Mutex<Instant>,
    revoked: std::sync::atomic::AtomicBool,
    pub(crate) changed: tokio::sync::Notify,
}

impl PublicAdmission {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            checked: tokio::sync::Mutex::new(Instant::now()),
            revoked: std::sync::atomic::AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn monitor<F, Fut>(
        self: &Arc<Self>,
        max_age: Duration,
        validate: F,
    ) -> AdmissionMonitor
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = bool> + Send,
    {
        let admission = self.clone();
        AdmissionMonitor(tokio::spawn(async move {
            loop {
                tokio::time::sleep(max_age).await;
                if !admission.check(max_age, &validate).await {
                    break;
                }
            }
        }))
    }

    pub(crate) fn revoked(&self) -> bool {
        self.revoked.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) async fn check<F, Fut>(&self, max_age: Duration, validate: F) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let mut checked = self.checked.lock().await;
        if self.revoked() {
            return false;
        }
        if checked.elapsed() >= max_age {
            if !validate().await {
                self.revoked
                    .store(true, std::sync::atomic::Ordering::Release);
                self.changed.notify_one();
                return false;
            }
            *checked = Instant::now();
        }
        true
    }
}

pub(crate) struct AdmissionMonitor(pub(crate) tokio::task::JoinHandle<()>);
impl Drop for AdmissionMonitor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) fn same_scope(pinned: &(String, String), project: &str, tenant: &str) -> bool {
    pinned.0 == project && pinned.1 == tenant
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_count_calls_and_release_connection_slots() {
        let limiter = Arc::new(PublicLimiter::default());
        let ip = "127.0.0.1".parse().unwrap();
        let limits = PublicLimits {
            connection_calls: 2,
            ip_calls: 3,
            connections_per_ip: 1,
            ..Default::default()
        };
        let slot = limiter.admit(ip, 1).unwrap();
        assert!(limiter.admit(ip, 1).is_none());
        let mut window = Window::default();
        assert!(limiter.allow(ip, &mut window, 2, &limits));
        assert!(!limiter.allow(ip, &mut window, 1, &limits));
        assert!(!limiter.allow(ip, &mut Window::default(), 1, &limits));
        drop(slot);
        assert!(limiter.admit(ip, 1).is_some());
        window.started = Instant::now() - Duration::from_secs(61);
        assert!(window.allow(2, 2));
    }

    #[tokio::test]
    async fn socket_admission_enforces_connection_ip_and_payload_limits() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{
            connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
        };

        async fn next_json(
            socket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        ) -> serde_json::Value {
            let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            serde_json::from_str(frame.to_text().unwrap()).unwrap()
        }

        let mut config = crate::config::Config::from_env().unwrap();
        config.public_functions = PublicLimits {
            connection_calls: 1,
            ip_calls: 2,
            payload_bytes: 512,
            connections_per_ip: 1,
            ..Default::default()
        };
        let runtime = crate::Runtime::new(config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = runtime
            .router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url = format!("ws://{address}/ws?public=1");
        let auth = Message::Text(
            r#"{"type":"auth","id":"public","project":"p","tenant":"t","public":true}"#.into(),
        );
        let (mut first, _) = connect_async(&url).await.unwrap();
        assert_eq!(next_json(&mut first).await["type"], "session.ready");
        first.send(auth.clone()).await.unwrap();
        assert_eq!(
            next_json(&mut first).await["error"],
            "auth session store is unavailable"
        );
        let rejected = connect_async(&url).await.unwrap_err();
        assert!(
            matches!(rejected, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == 429)
        );
        first.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if runtime
                    .inner
                    .public_limiter
                    .connections
                    .lock()
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let (mut third, _) = connect_async(&url).await.unwrap();
        next_json(&mut third).await;
        third.send(auth.clone()).await.unwrap();
        assert_eq!(
            next_json(&mut third).await["error"],
            "auth session store is unavailable"
        );
        third.send(auth.clone()).await.unwrap();
        let limited = next_json(&mut third).await;
        assert_eq!(limited["id"], "public");
        assert_eq!(limited["error"], "public call rate limit exceeded");
        third.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if runtime
                    .inner
                    .public_limiter
                    .connections
                    .lock()
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let (mut fourth, _) = connect_async(&url).await.unwrap();
        next_json(&mut fourth).await;
        fourth.send(auth).await.unwrap();
        assert_eq!(
            next_json(&mut fourth).await["error"],
            "public call rate limit exceeded"
        );
        fourth.close(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (mut oversized, _) = connect_async(&url).await.unwrap();
        next_json(&mut oversized).await;
        oversized
            .send(Message::Text("x".repeat(513).into()))
            .await
            .unwrap();
        let close = tokio::time::timeout(Duration::from_secs(3), oversized.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(close, Message::Close(Some(frame)) if u16::from(frame.code) == 1009));
        server.abort();
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn unauthenticated_transports_have_slots_deadlines_and_ingestion_limits() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{
            connect_async,
            tungstenite::{
                protocol::frame::{
                    coding::{CloseCode, Data, OpCode},
                    Frame,
                },
                Message,
            },
        };
        let mut config = crate::config::Config::from_env().unwrap();
        config.public_functions.pending_per_ip = 1;
        config.public_functions.auth_timeout = Duration::from_millis(100);
        config.public_functions.payload_bytes = 512;
        let runtime = crate::Runtime::new(config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = runtime
            .router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        for suffix in ["/ws?public=1", "/ws"] {
            let url = format!("ws://{address}{suffix}");
            let (mut first, _) = connect_async(&url).await.unwrap();
            first.next().await.unwrap().unwrap();
            let rejected = connect_async(&url).await.unwrap_err();
            assert!(
                matches!(rejected, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == 429)
            );
            let close = tokio::time::timeout(Duration::from_secs(2), first.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(
                matches!(close, Message::Close(Some(frame)) if frame.code == CloseCode::Policy)
            );
            first.close(None).await.ok();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (mut fragmented, _) = connect_async(format!("ws://{address}/ws?public=1"))
            .await
            .unwrap();
        fragmented.next().await.unwrap().unwrap();
        fragmented
            .send(Message::Frame(Frame::message(
                vec![b'x'; 300],
                OpCode::Data(Data::Text),
                false,
            )))
            .await
            .unwrap();
        fragmented
            .send(Message::Frame(Frame::message(
                vec![b'x'; 300],
                OpCode::Data(Data::Continue),
                true,
            )))
            .await
            .unwrap();
        // The cumulative message limit rejects transport fragments, before JSON
        // assembly or authentication. Tungstenite closes on capacity errors.
        let result = tokio::time::timeout(Duration::from_secs(2), fragmented.next())
            .await
            .unwrap();
        assert!(result.is_none() || matches!(result, Some(Err(_)) | Some(Ok(Message::Close(_)))));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (mut member_transport, _) = connect_async(format!("ws://{address}/ws")).await.unwrap();
        member_transport.next().await.unwrap().unwrap();
        member_transport
            .send(Message::Text(
                r#"{"type":"auth","id":"bypass","project":"p","tenant":"t","public":true}"#.into(),
            ))
            .await
            .unwrap();
        let error = member_transport.next().await.unwrap().unwrap();
        let error: serde_json::Value = serde_json::from_str(error.to_text().unwrap()).unwrap();
        assert_eq!(
            error["error"],
            "public authentication requires the public WebSocket transport"
        );
        server.abort();
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn admission_cache_expires_between_batch_items_and_revocation_is_sticky() {
        let admission = PublicAdmission::new();
        let active = std::sync::atomic::AtomicBool::new(true);
        assert!(
            admission
                .check(Duration::ZERO, || async {
                    active.load(std::sync::atomic::Ordering::Relaxed)
                })
                .await
        );
        active.store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(
            admission
                .check(Duration::from_secs(30), || async { false })
                .await
        );
        *admission.checked.lock().await = Instant::now() - Duration::from_secs(31);
        assert!(
            !admission
                .check(Duration::from_secs(30), || async { false })
                .await
        );
        assert!(admission.revoked());
        // A later valid admission cannot revive this socket.
        assert!(!admission.check(Duration::ZERO, || async { true }).await);
    }

    #[tokio::test]
    async fn admission_monitor_runs_while_execution_is_blocked() {
        let admission = PublicAdmission::new();
        let _monitor = admission.monitor(Duration::from_millis(10), || async { false });
        // This represents a handler holding the serial connection loop. The
        // monitor still revokes admission before that work has completed.
        let blocked_work = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(blocked_work);
        tokio::select! {
            _ = admission.changed.notified() => assert!(admission.revoked()),
            _ = &mut blocked_work => panic!("execution delayed the revocation monitor"),
        }
    }

    #[test]
    fn canonical_public_scope_is_pinned() {
        let pinned = ("p".into(), "t".into());
        assert!(same_scope(&pinned, "p", "t"));
        assert!(!same_scope(&pinned, "other", "t"));
        assert!(!same_scope(&pinned, "p", "other"));
    }

    #[test]
    fn forwarding_headers_require_a_trusted_peer() {
        let peer = "127.0.0.1".parse().unwrap();
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-real-ip", "192.0.2.1".parse().unwrap());
        assert_eq!(client_ip(peer, &headers, &PublicLimits::default()), peer);
        let limits = PublicLimits {
            trusted_proxies: vec![peer],
            ..Default::default()
        };
        assert_eq!(
            client_ip(peer, &headers, &limits),
            "192.0.2.1".parse::<IpAddr>().unwrap()
        );
    }
}
