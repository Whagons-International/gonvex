//! conn-idle: batched session revalidation and connection-count events over
//! real WebSockets (docs/performance/conn-idle.md).
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::accept::{AcceptedSocket, TunedListener};
use gonvex_runtime::config::{
    Config, ConnectionConfig, ModuleHostConfig, SandboxConfig, StorageConfig,
};
use gonvex_runtime::control::ControlConnection;
use gonvex_runtime::presence::PresenceConfig;
use gonvex_runtime::Runtime;
use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn test_config(
    database_url: String,
    presence: PresenceConfig,
    connection: ConnectionConfig,
) -> Config {
    let data_dir = gonvex_store::DataDir::parse(&database_url);
    Config {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        control_plane_database_url: Some(database_url),
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
            in_process: false,
            max_heap_bytes: 64 << 20,
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
        connection,
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
    fixture_with(presence, ConnectionConfig::default()).await
}

/// Serves like `main.rs`: tuned sockets and their descriptors as connect info.
async fn fixture_with(presence: PresenceConfig, connection: ConnectionConfig) -> Fixture {
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
    let runtime = Runtime::new(test_config(control_url.clone(), presence, connection));
    runtime.start().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(
            TunedListener::new(listener, connection.socket),
            router.into_make_service_with_connect_info::<AcceptedSocket>(),
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
        let mut capabilities = json!({"preserveSubscriptions":1});
        if presence {
            capabilities["presence"] = json!(1);
        }
        self.connect_with(token, tenant, capabilities).await
    }

    async fn connect_with(&self, token: &str, tenant: &str, capabilities: Value) -> Socket {
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", self.address))
            .await
            .unwrap();
        self.authenticate(&mut socket, token, tenant, capabilities)
            .await;
        socket
    }

    async fn authenticate(
        &self,
        socket: &mut Socket,
        token: &str,
        tenant: &str,
        capabilities: Value,
    ) {
        let ready = next_json(socket).await;
        assert_eq!(ready["type"], "session.ready");
        assert_eq!(ready["capabilities"]["presence"], 1);
        send(
            socket,
            json!({"type":"auth","id":"auth","token":token,"project":"project","tenant":tenant,"capabilities":capabilities}),
        )
        .await;
        loop {
            let frame = next_json(socket).await;
            match frame["type"].as_str() {
                Some("auth.result") => break,
                Some("auth.error") => panic!("auth failed: {frame}"),
                _ => {}
            }
        }
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

async fn control(fixture: &Fixture) -> sqlx::SqlitePool {
    fixture.pools.pool(&fixture.control_url).await.unwrap()
}

/// The next `auth.error`, failing after `within`; other frames are skipped.
async fn auth_error(socket: &mut Socket, within: Duration) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, socket.next()).await {
            Err(_) => return None,
            Ok(Some(Ok(Message::Text(text)))) => {
                let frame: Value = serde_json::from_str(&text).unwrap();
                if frame["type"] == "auth.error" {
                    return Some(frame);
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("socket ended: {other:?}"),
        }
    }
}

#[tokio::test]
async fn batched_revalidation_revokes_only_invalid_sessions() {
    let fixture = fixture(PresenceConfig::default()).await;
    let (_, keep_token) = fixture.member("tenant-a", "keep@example.test").await;
    let (_, revoked_token) = fixture.member("tenant-a", "revoked@example.test").await;
    let (_, disabled_token) = fixture.member("tenant-b", "disabled@example.test").await;
    let (_, expired_token) = fixture.member("tenant-b", "expired@example.test").await;
    let mut keep = fixture.connect(&keep_token, "tenant-a", true).await;
    let mut revoked = fixture.connect(&revoked_token, "tenant-a", true).await;
    let mut disabled = fixture.connect(&disabled_token, "tenant-b", false).await;
    let mut expired = fixture.connect(&expired_token, "tenant-b", false).await;
    // Every plain session connection is registered with the batched round.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fixture.runtime.session_revalidation_counters().0 < 4 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "connections not registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A round with every session valid revokes nothing.
    fixture.runtime.revalidate_sessions_now().await;
    assert_eq!(fixture.runtime.session_revalidation_counters().2, 0);
    for socket in [&mut keep, &mut revoked, &mut disabled, &mut expired] {
        assert!(auth_error(socket, Duration::from_millis(200))
            .await
            .is_none());
    }

    let pool = control(&fixture).await;
    sqlx::query("UPDATE gonvex_auth_sessions SET revoked_at=1 WHERE token_hash=$1")
        .bind(gonvex_store::token_hash(&revoked_token))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE accounts SET disabled_at=1 WHERE email='disabled@example.test'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE gonvex_auth_sessions SET expires_at=1 WHERE token_hash=$1")
        .bind(gonvex_store::token_hash(&expired_token))
        .execute(&pool)
        .await
        .unwrap();
    fixture.runtime.revalidate_sessions_now().await;
    for socket in [&mut revoked, &mut disabled, &mut expired] {
        let error = auth_error(socket, Duration::from_secs(5))
            .await
            .expect("an invalid session is told so");
        assert_eq!(error["id"], "session-expired", "{error}");
        assert_eq!(error["code"], "transient", "{error}");
    }
    assert!(auth_error(&mut keep, Duration::from_millis(300))
        .await
        .is_none());
    assert_eq!(fixture.runtime.session_revalidation_counters().2, 3);
    // Revoked connections are unauthenticated now and leave the registry.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fixture.runtime.session_revalidation_counters().0 != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "revoked connections stay registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The kept session's `last_seen_at` was refreshed by the round.
    let seen: Option<i64> =
        sqlx::query_scalar("SELECT last_seen_at FROM gonvex_auth_sessions WHERE token_hash=$1")
            .bind(gonvex_store::token_hash(&keep_token))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(seen.is_some());
    drop(keep);
    fixture.server.abort();
    fixture.runtime.shutdown().await;
}

/// Disabling a tenant in the Control Plane ends its authenticated sessions
/// at the next revalidation round (at most `auth_revalidation::INTERVAL`
/// later), leaves other tenants' sessions alone, and refuses new sign-ins.
#[tokio::test]
async fn batched_revalidation_revokes_sessions_of_a_disabled_tenant() {
    let fixture = fixture(PresenceConfig::default()).await;
    let (_, kept_token) = fixture.member("tenant-a", "kept@example.test").await;
    let (_, gone_token) = fixture.member("tenant-b", "gone@example.test").await;
    let mut kept = fixture.connect(&kept_token, "tenant-a", false).await;
    let mut gone = fixture.connect(&gone_token, "tenant-b", false).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fixture.runtime.session_revalidation_counters().0 < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "connections not registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pool = control(&fixture).await;
    sqlx::query(
        "UPDATE gonvex_runtime_tenants SET status='disabled' WHERE project_id='project' AND tenant_id='tenant-b'",
    )
    .execute(&pool)
    .await
    .unwrap();
    fixture.runtime.revalidate_sessions_now().await;
    let error = auth_error(&mut gone, Duration::from_secs(5))
        .await
        .expect("a session on a disabled tenant is told so");
    assert_eq!(error["id"], "session-expired", "{error}");
    assert!(auth_error(&mut kept, Duration::from_millis(300))
        .await
        .is_none());
    // The revoked connection holds no tenant session: a call is refused.
    send(
        &mut gone,
        json!({"type":"reducer.call","id":"after-disable","path":"anything","args":{}}),
    )
    .await;
    let refused = next_json(&mut gone).await;
    assert_ne!(refused["type"], "reducer.result", "{refused}");
    // Signing in again to the disabled tenant is refused.
    let (mut again, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", fixture.address))
        .await
        .unwrap();
    assert_eq!(next_json(&mut again).await["type"], "session.ready");
    send(
        &mut again,
        json!({"type":"auth","id":"auth","token":gone_token,"project":"project","tenant":"tenant-b"}),
    )
    .await;
    loop {
        let frame = next_json(&mut again).await;
        match frame["type"].as_str() {
            Some("auth.error") => break,
            Some("auth.result") => panic!("signed in to a disabled tenant: {frame}"),
            _ => {}
        }
    }
    drop(kept);
    fixture.server.abort();
    fixture.runtime.shutdown().await;
}

#[tokio::test]
async fn batched_revalidation_revokes_a_member_that_left() {
    let fixture = fixture(PresenceConfig::default()).await;
    let (member, token) = fixture.member("tenant-a", "leaver@example.test").await;
    let mut socket = fixture.connect(&token, "tenant-a", false).await;
    let route = gonvex_store::TenantRoute {
        project_id: "project".to_owned(),
        tenant_id: "tenant-a".to_owned(),
        database_url: gonvex_store::database_url(&fixture._data.path().join("tenant-a.db")),
    };
    let control = gonvex_store::ControlPlane::connect(
        &fixture.control_url,
        fixture.pools.clone(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    assert!(control
        .active_member_accounts("project", "tenant-a", &["nobody".to_owned()])
        .await
        .unwrap()
        .is_empty());
    let mut tx = control
        .begin_tenant_transaction(&route, false)
        .await
        .unwrap();
    tx.set_command_id("conn-idle-test:leave").await.unwrap();
    sqlx::query("UPDATE members SET status='removed' WHERE id=$1")
        .bind(&member)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    fixture.runtime.revalidate_sessions_now().await;
    // The membership feed may end the session first; either way it ends.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let frame = tokio::time::timeout_at(deadline, socket.next())
            .await
            .expect("session ends")
            .expect("socket open")
            .expect("frame");
        if let Message::Text(text) = frame {
            let frame: Value = serde_json::from_str(&text).unwrap();
            if frame["type"] == "auth.error" || frame["type"] == "membership.changed" {
                break;
            }
        }
    }
    fixture.server.abort();
    fixture.runtime.shutdown().await;
}

#[tokio::test]
async fn list_tenants_still_follows_connection_counts() {
    let fixture = fixture(PresenceConfig::default()).await;
    let pool = control(&fixture).await;
    sqlx::query(
        "UPDATE gonvex_runtime_projects SET owner_email='owner@example.test' WHERE id='project'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let (_, owner_token) = fixture.member("tenant-a", "owner@example.test").await;
    let (_, member_token) = fixture.member("tenant-b", "member@example.test").await;
    // A control-only socket (no tenant), as the support console opens.
    let (mut owner, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", fixture.address))
        .await
        .unwrap();
    assert_eq!(next_json(&mut owner).await["type"], "session.ready");
    send(
        &mut owner,
        json!({"type":"auth","id":"auth","token":owner_token,"project":"project","controlOnly":true}),
    )
    .await;
    loop {
        let frame = next_json(&mut owner).await;
        assert_ne!(frame["type"], "auth.error", "{frame}");
        if frame["type"] == "auth.result" {
            assert_eq!(frame["result"]["tenantId"], "", "{frame}");
            break;
        }
    }
    send(
        &mut owner,
        json!({"type":"query.subscribe","id":"tenants","path":"control.support.listTenants","args":{},"scope":"control"}),
    )
    .await;
    let sessions = |frame: &Value, tenant: &str| {
        frame["result"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["id"] == tenant))
            .and_then(|row| row["activeSessions"].as_u64())
    };
    let initial = loop {
        let frame = next_json(&mut owner).await;
        if frame["type"] == "query.result" && frame["id"] == "tenants" {
            break frame;
        }
    };
    assert_eq!(sessions(&initial, "tenant-b"), Some(0), "{initial}");
    // The control-only session is batched too and stays valid.
    assert_eq!(fixture.runtime.session_revalidation_counters().0, 1);
    fixture.runtime.revalidate_sessions_now().await;
    assert_eq!(fixture.runtime.session_revalidation_counters().2, 0);
    // A connect elsewhere in the project refreshes the owner's live query.
    let member = fixture.connect(&member_token, "tenant-b", false).await;
    loop {
        let frame = next_json(&mut owner).await;
        if frame["type"] == "query.result"
            && frame["id"] == "tenants"
            && sessions(&frame, "tenant-b") == Some(1)
        {
            assert_eq!(frame["reason"], "presence-change", "{frame}");
            break;
        }
    }
    // And so does the disconnect.
    drop(member);
    loop {
        let frame = next_json(&mut owner).await;
        if frame["type"] == "query.result"
            && frame["id"] == "tenants"
            && sessions(&frame, "tenant-b") == Some(0)
        {
            break;
        }
    }
    fixture.server.abort();
    fixture.runtime.shutdown().await;
}
