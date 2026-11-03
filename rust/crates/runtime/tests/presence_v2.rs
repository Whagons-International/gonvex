//! Presence v2 over real WebSockets: transitions, audience scoping, multiple
//! tabs, reconnect grace, Ping liveness and the in-memory support heartbeat.
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};
use gonvex_runtime::control::ControlConnection;
use gonvex_runtime::presence::PresenceConfig;
use gonvex_runtime::Runtime;
use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const TICK: Duration = Duration::from_millis(50);
const GRACE: Duration = Duration::from_millis(600);

fn test_config(database_url: String, presence: PresenceConfig) -> Config {
    let data_dir = gonvex_store::DataDir::parse(&database_url);
    Config {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        control_plane_database_url: Some(database_url.clone()),
        default_database_url: Some(gonvex_store::database_url(data_dir.root())),
        tenant_database_urls: BTreeMap::new(),
        project_database_urls: BTreeMap::new(),
        require_auth: true,
        websocket_compression: false,
        websocket_compression_threshold: 1024,
        control_secret: Some("test-control-secret-that-is-long-enough".to_owned()),
        auth_public_url: Some("http://localhost:8080".to_owned()),
        admin_key: Some("test-admin-key".to_owned()),
        dev_sync_key: None,
        dashboard_account: None,
        dashboard_password: None,
        dashboard_auth_project_id: Some("project".to_owned()),
        google_client_id: None,
        google_client_secret: None,
        database_max_total_connections: 8,
        database_max_connections: 4,
        database_max_idle_connections: 1,
        database_idle_timeout: Duration::from_secs(60),
        module_host: ModuleHostConfig {
            enabled: false,
            binary: None,
            endpoint: None,
            start_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            max_frame_bytes: 1 << 20,
            max_concurrent_calls: 4,
            isolate_pool_size: 1,
            execution_timeout: Duration::from_secs(1),
            action_execution_timeout: Duration::from_secs(900),
        },
        runtime_version: "test".to_owned(),
        sandbox: SandboxConfig::default(),
        storage: StorageConfig::default(),
        service_principals: Vec::new(),
        public_functions: Default::default(),
        presence,
    }
}

struct Fixture {
    runtime: Runtime,
    address: SocketAddr,
    server: tokio::task::JoinHandle<()>,
    pools: PoolRegistry,
    control_url: String,
    _data: tempfile::TempDir,
}

async fn fixture(presence: PresenceConfig) -> Fixture {
    let data = tempfile::tempdir().unwrap();
    let control_url = gonvex_store::database_url(&data.path().join("control.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let fixture = pools.pool(&control_url).await.unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_projects
           (id,name,database_mode,database_url,status,auth_mode)
           VALUES('project','Project','multiTenant',$1,'active','gonvex-native')"#,
    )
    .bind(&control_url)
    .execute(&fixture)
    .await
    .unwrap();
    for tenant in ["tenant-a", "tenant-b"] {
        let tenant_url = gonvex_store::database_url(&data.path().join(format!("{tenant}.db")));
        control
            .clone()
            .provision_tenant_database(
                TenantRoute {
                    project_id: "project".to_owned(),
                    tenant_id: tenant.to_owned(),
                    database_url: tenant_url.clone(),
                },
                Vec::new(),
            )
            .await
            .unwrap();
        pools.close_database(&tenant_url).await;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants
               (relationship_id,project_id,tenant_id,name,database_url,status)
               VALUES($1,'project',$1,$1,$2,'active')"#,
        )
        .bind(tenant)
        .bind(&tenant_url)
        .execute(&fixture)
        .await
        .unwrap();
    }
    let runtime = Runtime::new(test_config(control_url.clone(), presence));
    runtime.start().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Fixture {
        runtime,
        address,
        server,
        pools,
        control_url,
        _data: data,
    }
}

impl Fixture {
    /// Creates a tenant Member and returns (memberId, access token).
    async fn member(&self, tenant: &str, email: &str) -> (String, String) {
        let response = self
            .runtime
            .router()
            .oneshot(
                Request::post("/dev/internal/e2e/members")
                    .header("authorization", "Bearer test-admin-key")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "projectId":"project","tenantId":tenant,"email":email,
                            "name":email,"password":"correct horse battery staple",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let created: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 64 << 10).await.unwrap())
                .unwrap();
        let login = self
            .runtime
            .execute_control_action(
                &ControlConnection {
                    project_id: "project".to_owned(),
                    ..ControlConnection::default()
                },
                "control.auth.passwordLogin",
                &json!({"email":email,"password":"correct horse battery staple"}),
                &format!("login-{email}"),
            )
            .await
            .unwrap();
        (
            created["memberId"].as_str().unwrap().to_owned(),
            login["accessToken"].as_str().unwrap().to_owned(),
        )
    }

    async fn connect(&self, token: &str, tenant: &str, presence: bool) -> Socket {
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", self.address))
            .await
            .unwrap();
        let ready = next_json(&mut socket).await;
        assert_eq!(ready["type"], "session.ready");
        assert_eq!(ready["capabilities"]["presence"], 1);
        let mut capabilities = json!({"preserveSubscriptions":1});
        if presence {
            capabilities["presence"] = json!(1);
        }
        send(
            &mut socket,
            json!({"type":"auth","id":"auth","token":token,"project":"project","tenant":tenant,"capabilities":capabilities}),
        )
        .await;
        loop {
            let frame = next_json(&mut socket).await;
            match frame["type"].as_str() {
                Some("auth.result") => break,
                Some("auth.error") => panic!("auth failed: {frame}"),
                _ => {}
            }
        }
        socket
    }
}

async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn next_json(socket: &mut Socket) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .expect("frame before timeout")
            .expect("socket open")
            .expect("frame");
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

/// The next presence frame, skipping unrelated frames.
async fn next_presence(socket: &mut Socket) -> Value {
    loop {
        let frame = next_json(socket).await;
        if frame["type"]
            .as_str()
            .is_some_and(|kind| kind.starts_with("presence."))
        {
            return frame;
        }
    }
}

/// Asserts no presence frame arrives within `window`.
async fn quiet(socket: &mut Socket, window: Duration) {
    let deadline = tokio::time::Instant::now() + window;
    loop {
        match tokio::time::timeout_at(deadline, socket.next()).await {
            Err(_) => return,
            Ok(Some(Ok(Message::Text(text)))) => {
                let frame: Value = serde_json::from_str(&text).unwrap();
                assert!(
                    !frame["type"].as_str().unwrap_or("").starts_with("presence."),
                    "unexpected presence frame {frame}"
                );
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("socket ended: {other:?}"),
        }
    }
}

fn changes(frame: &Value) -> Vec<(String, String)> {
    assert_eq!(frame["type"], "presence.patch", "{frame}");
    frame["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["memberId"].as_str().unwrap().to_owned(),
                c["state"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn config() -> PresenceConfig {
    PresenceConfig {
        tick: TICK,
        offline_grace: GRACE,
        ..PresenceConfig::default()
    }
}

#[tokio::test]
async fn presence_v2_transitions_scoping_tabs_and_reconnect() {
    let fixture = fixture(config()).await;
    let (alice, alice_token) = fixture.member("tenant-a", "alice@example.test").await;
    let (_bob, bob_token) = fixture.member("tenant-a", "bob@example.test").await;
    let (carol, carol_token) = fixture.member("tenant-b", "carol@example.test").await;

    // Bob watches tenant A. The snapshot answers his subscribe id.
    let mut bob = fixture.connect(&bob_token, "tenant-a", true).await;
    send(&mut bob, json!({"type":"presence.subscribe","id":"watch"})).await;
    let snapshot = next_presence(&mut bob).await;
    assert_eq!(snapshot["type"], "presence.snapshot");
    assert_eq!(snapshot["id"], "watch");
    assert_eq!(snapshot["members"], json!([]));

    // Alice comes online in two tabs; the burst is one coalesced transition.
    let mut tab1 = fixture.connect(&alice_token, "tenant-a", true).await;
    let mut tab2 = fixture.connect(&alice_token, "tenant-a", true).await;
    send(&mut tab1, json!({"type":"presence.set","state":"active"})).await;
    send(&mut tab2, json!({"type":"presence.set","state":"idle"})).await;
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "active".into())]);

    // Carol (tenant B) is invisible to tenant A, and Alice to Carol.
    let mut carol_socket = fixture.connect(&carol_token, "tenant-b", true).await;
    send(&mut carol_socket, json!({"type":"presence.set","state":"active"})).await;
    send(&mut carol_socket, json!({"type":"presence.subscribe","id":"b"})).await;
    let carol_snapshot = next_presence(&mut carol_socket).await;
    assert!(!carol_snapshot.to_string().contains(&alice));
    quiet(&mut bob, TICK * 6).await;
    // A tick may land between Carol's set and subscribe: she is then in the
    // snapshot instead of the first patch.
    if carol_snapshot["members"].as_array().unwrap().is_empty() {
        let carol_patch = next_presence(&mut carol_socket).await;
        assert_eq!(changes(&carol_patch), vec![(carol.clone(), "active".into())]);
    } else {
        assert_eq!(carol_snapshot["members"][0]["memberId"], carol.as_str());
    }

    // Both tabs idle → idle. Tab 1 closes → Alice stays idle via tab 2.
    send(&mut tab1, json!({"type":"presence.set","state":"idle"})).await;
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "idle".into())]);
    tab1.close(None).await.unwrap();
    quiet(&mut bob, GRACE * 2).await;

    // A dropped socket that reconnects within grace shows no flicker.
    drop(tab2);
    tokio::time::sleep(GRACE / 4).await;
    let mut tab3 = fixture.connect(&alice_token, "tenant-a", true).await;
    send(&mut tab3, json!({"type":"presence.set","state":"idle"})).await;
    quiet(&mut bob, GRACE * 2).await;

    // Explicit offline (page hide) is published without waiting for grace.
    send(&mut tab3, json!({"type":"presence.set","state":"offline"})).await;
    let offline = next_presence(&mut bob).await;
    assert_eq!(changes(&offline), vec![(alice.clone(), "offline".into())]);
    // Coming back is immediate, then a real disconnect goes offline after grace.
    send(&mut tab3, json!({"type":"presence.set","state":"active"})).await;
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "active".into())]);
    let closed_at = std::time::Instant::now();
    drop(tab3);
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "offline".into())]);
    assert!(closed_at.elapsed() >= GRACE, "offline only after grace");

    // A client without the capability (old SDK) is never in the feed, and a
    // connection without a tenant Member session cannot subscribe.
    let mut old = fixture.connect(&alice_token, "tenant-a", false).await;
    send(&mut old, json!({"type":"presence.set","state":"active"})).await;
    // presence.set needs no capability; only liveness Pings do. It is accepted.
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "active".into())]);
    drop(old);
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "offline".into())]);

    let (mut anonymous, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", fixture.address))
        .await
        .unwrap();
    next_json(&mut anonymous).await;
    send(&mut anonymous, json!({"type":"presence.subscribe","id":"nope"})).await;
    let error = next_presence(&mut anonymous).await;
    assert_eq!(error["type"], "presence.error");
    assert_eq!(error["id"], "nope");

    fixture.server.abort();
    fixture.runtime.shutdown().await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn unresponsive_presence_connections_are_closed_and_go_offline() {
    let fixture = fixture(PresenceConfig {
        ping_interval: Duration::from_secs(1),
        liveness_timeout: Duration::from_secs(2),
        ..config()
    })
    .await;
    let (alice, alice_token) = fixture.member("tenant-a", "alice@example.test").await;
    let (_bob, bob_token) = fixture.member("tenant-a", "bob@example.test").await;
    let mut bob = fixture.connect(&bob_token, "tenant-a", true).await;
    send(&mut bob, json!({"type":"presence.subscribe","id":"watch"})).await;
    next_presence(&mut bob).await;
    // Alice's socket stops reading: tungstenite answers Pings only while the
    // stream is polled, so the server sees no inbound frame.
    let mut alice_socket = fixture.connect(&alice_token, "tenant-a", true).await;
    send(&mut alice_socket, json!({"type":"presence.set","state":"active"})).await;
    let started = std::time::Instant::now();
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "active".into())]);
    // Bob keeps reading (and so answers Pings); Alice goes offline after
    // liveness timeout + grace, while Bob's own connection survives.
    assert_eq!(changes(&next_presence(&mut bob).await), vec![(alice.clone(), "offline".into())]);
    assert!(started.elapsed() >= Duration::from_secs(2));
    // Alice's server side closed the socket with the liveness reason.
    let mut closed = false;
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_secs(2), alice_socket.next()).await
    {
        match frame {
            Ok(Message::Close(Some(close))) => {
                assert_eq!(close.reason.as_str(), "presence liveness timeout");
                closed = true;
                break;
            }
            Ok(_) => {}
            Err(_) => {
                closed = true;
                break;
            }
        }
    }
    assert!(closed);
    fixture.server.abort();
    fixture.runtime.shutdown().await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn repeated_support_heartbeats_are_answered_without_writes() {
    let fixture = fixture(config()).await;
    let (_alice, alice_token) = fixture.member("tenant-a", "alice@example.test").await;
    let mut socket = fixture.connect(&alice_token, "tenant-a", true).await;
    let control = fixture.pools.pool(&fixture.control_url).await.unwrap();
    let idempotency_rows = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM gonvex_control_idempotency WHERE path='control.support.heartbeat'",
        )
        .fetch_one(&control)
        .await
        .unwrap()
    };
    let mut session_ids = Vec::new();
    for call in 0..3 {
        send(
            &mut socket,
            json!({"type":"reducer.call","id":format!("hb-{call}"),"path":"control.support.heartbeat",
                   "scope":"control","idempotencyKey":format!("hb-{call}"),
                   "args":{"release":"1.2.3","environment":"production"}}),
        )
        .await;
        loop {
            let frame = next_json(&mut socket).await;
            if frame["type"] == "reducer.result" {
                session_ids.push(frame["result"]["sessionId"].as_str().unwrap().to_owned());
            }
            if frame["type"] == "reducer.error" {
                panic!("heartbeat failed: {frame}");
            }
            // The client completes control writes at the control watermark.
            if frame["type"] == "control.watermark" {
                assert_eq!(frame["id"], format!("hb-{call}"));
                break;
            }
        }
    }
    assert!(session_ids.iter().all(|id| id == &session_ids[0]));
    assert_eq!(idempotency_rows().await, 1, "only the first heartbeat writes");
    // A changed release is new attribution and is written.
    send(
        &mut socket,
        json!({"type":"reducer.call","id":"hb-new","path":"control.support.heartbeat",
               "scope":"control","idempotencyKey":"hb-new",
               "args":{"release":"1.2.4","environment":"production"}}),
    )
    .await;
    loop {
        if next_json(&mut socket).await["type"] == "control.watermark" {
            break;
        }
    }
    assert_eq!(idempotency_rows().await, 2);
    let release: String = sqlx::query_scalar("SELECT release FROM gonvex_support_sessions WHERE id=$1")
        .bind(&session_ids[0])
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(release, "1.2.4");
    // Closing the connection flushes last_seen_at lazily (batched).
    let stored_before: i64 =
        sqlx::query_scalar("SELECT last_seen_at FROM gonvex_support_sessions WHERE id=$1")
            .bind(&session_ids[0])
            .fetch_one(&control)
            .await
            .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(socket);
    let flushed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let seen: i64 =
                sqlx::query_scalar("SELECT last_seen_at FROM gonvex_support_sessions WHERE id=$1")
                    .bind(&session_ids[0])
                    .fetch_one(&control)
                    .await
                    .unwrap();
            if seen > stored_before {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(flushed.is_ok(), "last_seen_at flushed after close");
    drop(control);
    fixture.server.abort();
    fixture.runtime.shutdown().await;
    fixture.pools.close().await;
}

async fn register_support_sessions(socket: &mut Socket) -> Vec<String> {
    send(
        socket,
        json!({"type":"reducer.call","id":"hb","path":"control.support.heartbeat","scope":"control",
               "idempotencyKey":"hb","args":{"release":"1.2.3","environment":"production"}}),
    )
    .await;
    let mut support = String::new();
    loop {
        let frame = next_json(socket).await;
        if frame["type"] == "reducer.result" {
            support = frame["result"]["sessionId"].as_str().unwrap().to_owned();
        }
        if frame["type"] == "control.watermark" {
            break;
        }
    }
    // The SDK error reporter's session (id = connection id).
    send(socket, json!({"type":"error.register","id":"err","release":"1.2.3","environment":"production"})).await;
    loop {
        let frame = next_json(socket).await;
        if frame["type"] == "error.ack" {
            assert!(frame.get("error").is_none(), "{frame}");
            break;
        }
    }
    let connection = support.trim_start_matches("support_").to_owned();
    vec![support, connection]
}

async fn last_seen(pool: &sqlx::SqlitePool, id: &str) -> i64 {
    sqlx::query_scalar("SELECT last_seen_at FROM gonvex_support_sessions WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn shutdown_records_end_times_of_open_support_sessions() {
    let fixture = fixture(config()).await;
    let (_alice, alice_token) = fixture.member("tenant-a", "alice@example.test").await;
    let (_bob, bob_token) = fixture.member("tenant-a", "bob@example.test").await;
    let mut alice = fixture.connect(&alice_token, "tenant-a", true).await;
    let mut bob = fixture.connect(&bob_token, "tenant-a", true).await;
    let mut sessions = register_support_sessions(&mut alice).await;
    sessions.extend(register_support_sessions(&mut bob).await);
    let control = fixture.pools.pool(&fixture.control_url).await.unwrap();
    let mut before = Vec::new();
    for id in &sessions {
        before.push(last_seen(&control, id).await);
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    // Sockets stay open: this is a stop signal, not a disconnect.
    let started = std::time::Instant::now();
    assert_eq!(fixture.runtime.flush_support_sessions().await, 4);
    fixture.runtime.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(5));
    for (id, before) in sessions.iter().zip(before) {
        assert!(last_seen(&control, id).await > before, "end time recorded for {id}");
    }
    drop((alice, bob, control));
    fixture.server.abort();
    fixture.pools.close().await;
}

#[tokio::test]
async fn periodic_refresh_updates_open_sessions_without_control_fanout() {
    let fixture = fixture(PresenceConfig {
        support_refresh: Duration::from_secs(1),
        ..config()
    })
    .await;
    let (_alice, alice_token) = fixture.member("tenant-a", "alice@example.test").await;
    let mut alice = fixture.connect(&alice_token, "tenant-a", true).await;
    let sessions = register_support_sessions(&mut alice).await;
    let control = fixture.pools.pool(&fixture.control_url).await.unwrap();
    let idempotency = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gonvex_control_idempotency")
            .fetch_one(&control)
            .await
            .unwrap()
    };
    let registered: Vec<i64> = {
        let mut values = Vec::new();
        for id in &sessions {
            values.push(last_seen(&control, id).await);
        }
        values
    };
    let notifications = fixture.runtime.control_notification_count();
    let rows = idempotency().await;
    // The background task (1 s interval, random first offset) refreshes both
    // sessions while the socket stays open.
    let refreshed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut all = true;
            for (id, registered) in sessions.iter().zip(&registered) {
                all &= last_seen(&control, id).await > *registered;
            }
            if all {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(refreshed.is_ok(), "open sessions refreshed");
    // A direct refresh is one batched write of every open session.
    assert_eq!(fixture.runtime.refresh_support_sessions().await, 2);
    assert_eq!(fixture.runtime.control_notification_count(), notifications, "no Control fan-out");
    assert_eq!(idempotency().await, rows, "no idempotency rows");
    // Nothing was pushed to the open connection either.
    if let Ok(Some(Ok(Message::Text(text)))) =
        tokio::time::timeout(Duration::from_millis(300), alice.next()).await
    {
        panic!("refresh pushed a frame: {text}");
    }
    drop(control);
    fixture.server.abort();
    fixture.runtime.shutdown().await;
    fixture.pools.close().await;
}
