use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};
use gonvex_runtime::Runtime;
use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use serde_json::Value;
use sqlx::Row;
use tower::ServiceExt;

fn test_config(control_url: String, data_url: String) -> Config {
    Config {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        control_plane_database_url: Some(control_url),
        default_database_url: Some(data_url),
        tenant_database_urls: BTreeMap::new(),
        project_database_urls: BTreeMap::new(),
        require_auth: false,
        websocket_compression: false,
        websocket_compression_threshold: 1024,
        control_secret: Some("test-control-secret-that-is-long-enough".to_owned()),
        auth_public_url: Some("http://localhost:8080".to_owned()),
        admin_key: Some("operator-admin-key".to_owned()),
        dev_sync_key: None,
        dashboard_account: None,
        dashboard_password: None,
        dashboard_auth_project_id: None,
        google_client_id: None,
        google_client_secret: None,
        database_max_total_connections: 8,
        database_max_connections: 4,
        database_max_idle_connections: 1,
        database_idle_timeout: std::time::Duration::from_secs(60),
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
        presence: Default::default(),
        connection: Default::default(),
    }
}

/// A fresh data directory under `GONVEX_TEST_DATA_DIR` when set (CI volume),
/// otherwise a system tempdir.
fn test_data_dir() -> tempfile::TempDir {
    match std::env::var("GONVEX_TEST_DATA_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(root) => {
            std::fs::create_dir_all(&root).unwrap();
            tempfile::tempdir_in(root).unwrap()
        }
        None => tempfile::tempdir().unwrap(),
    }
}

async fn json_response(runtime: &Runtime, mut request: Request<Body>) -> (StatusCode, Value) {
    if !request.headers().contains_key("authorization") {
        request.headers_mut().insert(
            "authorization",
            "Bearer operator-admin-key".parse().unwrap(),
        );
    }
    let response = runtime.router().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let value = serde_json::from_slice(&body).unwrap_or_else(|_| {
        panic!(
            "expected JSON response for {status}, got {}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, value)
}

#[tokio::test]
async fn rust_operator_routes_preserve_data_and_membership_contracts() {
    let data = test_data_dir();
    let data_url = gonvex_store::database_url(data.path());
    let control_url = gonvex_store::database_url(&data.path().join("control.db"));
    let tenant_url =
        gonvex_store::database_url(&data.path().join("projects/project/tenants/tenant.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let control_fixture = pools.pool(&control_url).await.unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_projects
           (id,name,database_mode,database_url,status,auth_mode)
           VALUES('project','Project','multiTenant',$1,'active','gonvex-native')"#,
    )
    .bind(&tenant_url)
    .execute(&control_fixture)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_tenants
           (relationship_id,project_id,tenant_id,name,database_url,status,provisioned)
           VALUES('relationship','project','tenant','Tenant',$1,'active',1)"#,
    )
    .bind(&tenant_url)
    .execute(&control_fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('account','project','person@example.test','Person')",
    )
    .execute(&control_fixture)
    .await
    .unwrap();
    control
        .clone()
        .provision_tenant_database(
            TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: "tenant".to_owned(),
                database_url: tenant_url.clone(),
            },
            Vec::new(),
        )
        .await
        .unwrap();
    let tenant_fixture = pools.pool(&tenant_url).await.unwrap();
    sqlx::query(
        "INSERT INTO members(id,account_id,display_name,role) VALUES('member','account','Person','admin')",
    )
    .execute(&tenant_fixture)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE tasks(id TEXT PRIMARY KEY,title TEXT NOT NULL,owner_id TEXT,metadata JSON NOT NULL DEFAULT '{}',created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)))",
    )
    .execute(&tenant_fixture)
    .await
    .unwrap();
    sqlx::query("INSERT INTO tasks(id,title,owner_id,metadata) VALUES('task-1','First','old',json_object('owner','old'))")
        .execute(&tenant_fixture)
        .await
        .unwrap();

    let runtime = Runtime::new(test_config(control_url.clone(), data_url));
    runtime.start().await.unwrap();
    let scope = "project=project&tenant=tenant";

    let (status, missing_scope) = json_response(
        &runtime,
        Request::get("/dev/data/tables")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{missing_scope}");
    assert_eq!(missing_scope["error"], "project is required");

    let (status, tables) = json_response(
        &runtime,
        Request::get(format!("/dev/data/tables?{scope}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{tables}");
    assert!(
        tables["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| table["name"] == "tasks"),
        "{tables}"
    );
    assert!(!tables["tables"]
        .as_array()
        .unwrap()
        .iter()
        .any(|table| table["name"] == "members"));

    let (status, rows) = json_response(
        &runtime,
        Request::get(format!(
            "/dev/data/tables/tasks/rows?{scope}&sort=title&direction=asc"
        ))
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows["rows"][0]["title"], "First");

    let (status, inserted) = json_response(
        &runtime,
        Request::post(format!("/dev/data/tables/tasks/rows?{scope}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id":"task-2","title":"Second"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{inserted}");
    assert_eq!(inserted["row"]["id"], "task-2");

    let (status, updated) = json_response(
        &runtime,
        Request::patch(format!("/dev/data/tables/tasks/rows/task-2?{scope}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"title":"Updated"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["row"]["title"], "Updated");

    let (status, replacement) = json_response(
        &runtime,
        Request::post(format!("/dev/data/references/replace?{scope}"))
            .header("authorization", "Bearer operator-admin-key")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"replacements":{"old":"new"},"dryRun":false}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replacement}");
    assert_eq!(replacement["textRows"], 1);
    assert_eq!(replacement["jsonRows"], 1);

    let (status, members) = json_response(
        &runtime,
        Request::get("/dev/projects/project/auth/memberships?tenant=tenant")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{members}");
    assert_eq!(members["members"][0]["memberId"], "member");
    assert_eq!(members["members"][0]["email"], "person@example.test");

    let (status, invited) = json_response(
        &runtime,
        Request::put("/dev/projects/project/auth/memberships?tenant=tenant")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"email":"invitee@example.test","role":"member","permissions":{}}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{invited}");
    let invitation_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM gonvex_auth_membership_invitations WHERE project_id='project' AND tenant_id='tenant' AND email='invitee@example.test' AND revoked_at IS NULL",
    )
    .fetch_one(&control_fixture)
    .await
    .unwrap();
    assert_eq!(invitation_count, 1);

    let (status, deleted) = json_response(
        &runtime,
        Request::delete(format!("/dev/data/tables/tasks/rows/task-2?{scope}"))
            .header("authorization", "Bearer operator-admin-key")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks WHERE id='task-2'")
        .fetch_one(&tenant_fixture)
        .await
        .unwrap();
    assert_eq!(remaining, 0);

    let owner: String = sqlx::query_scalar("SELECT owner_id FROM tasks WHERE id='task-1'")
        .fetch_one(&tenant_fixture)
        .await
        .unwrap();
    let metadata: sqlx::types::Json<Value> =
        sqlx::query("SELECT metadata FROM tasks WHERE id='task-1'")
            .fetch_one(&tenant_fixture)
            .await
            .unwrap()
            .get("metadata");
    assert_eq!(owner, "new");
    assert_eq!(metadata.0["owner"], "new");

    runtime.shutdown().await;
    drop((tenant_fixture, control_fixture));
    pools.close().await;
}

#[tokio::test]
async fn dashboard_sessions_follow_account_security_lifecycle() {
    let data = test_data_dir();
    let data_url = gonvex_store::database_url(data.path());
    let url = gonvex_store::database_url(&data.path().join("control.db"));
    let runtime = Runtime::new(test_config(url.clone(), data_url));
    runtime.start().await.unwrap();
    let fixture_pools = PoolRegistry::new(PoolLimits::default());
    let fixture = fixture_pools.pool(&url).await.unwrap();
    async fn save(runtime: &Runtime, password: &str, role: &str) {
        let (status, _) = json_response(runtime, Request::post("/dev/auth/accounts")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({"email":"security@example.test", "name":"Security", "password":password,"role":role}).to_string())).unwrap()).await;
        assert_eq!(status, StatusCode::CREATED);
    }
    async fn login_token(runtime: &Runtime, password: &str) -> String {
        let (status, body) = json_response(
            runtime,
            Request::post("/dev/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"email":"security@example.test", "password":password})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        body["session"]["accessToken"].as_str().unwrap().to_owned()
    }
    async fn valid(runtime: &Runtime, token: &str, expected: StatusCode) {
        let (status, _) = json_response(
            runtime,
            Request::get("/dev/auth/me")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, expected);
    }
    save(&runtime, "original-password", "admin").await;
    let first = login_token(&runtime, "original-password").await;
    valid(&runtime, &first, StatusCode::OK).await;
    sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status,auth_mode) VALUES('security','Security','multiTenant','active','gonvex-native')")
        .execute(&fixture).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = runtime.router();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = format!("ws://{address}/dev/metrics/stream?project=security")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        format!("gonvex-dashboard-auth.{first}").parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .is_text());
    sqlx::query(
        "UPDATE gonvex_dashboard_accounts SET name='Renamed' WHERE email='security@example.test'",
    )
    .execute(&fixture)
    .await
    .unwrap();
    valid(&runtime, &first, StatusCode::OK).await;
    save(&runtime, "replacement-password", "admin").await;
    valid(&runtime, &first, StatusCode::UNAUTHORIZED).await;
    // The rename may still deliver an update frame before the revoked session
    // is closed; skip data frames until the close arrives.
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match socket.next().await {
                Some(Ok(frame)) if frame.is_text() => continue,
                other => return other,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(closed, Some(Ok(ref frame)) if frame.is_close()),
        "expected close, got {closed:?}"
    );
    server.abort();
    let second = login_token(&runtime, "replacement-password").await;
    sqlx::query(
        "UPDATE gonvex_dashboard_accounts SET role='standard' WHERE email='security@example.test'",
    )
    .execute(&fixture)
    .await
    .unwrap();
    valid(&runtime, &second, StatusCode::UNAUTHORIZED).await;
    sqlx::query(
        "UPDATE gonvex_dashboard_accounts SET role='admin' WHERE email='security@example.test'",
    )
    .execute(&fixture)
    .await
    .unwrap();
    valid(&runtime, &second, StatusCode::UNAUTHORIZED).await;
    let third = login_token(&runtime, "replacement-password").await;
    sqlx::query("DELETE FROM gonvex_dashboard_accounts WHERE email='security@example.test'")
        .execute(&fixture)
        .await
        .unwrap();
    valid(&runtime, &third, StatusCode::UNAUTHORIZED).await;
    save(&runtime, "replacement-password", "admin").await;
    valid(&runtime, &third, StatusCode::UNAUTHORIZED).await;
    let fourth = login_token(&runtime, "replacement-password").await;
    sqlx::query("UPDATE gonvex_dashboard_accounts SET password_hash='!disabled' WHERE email='security@example.test'")
        .execute(&fixture).await.unwrap();
    valid(&runtime, &fourth, StatusCode::UNAUTHORIZED).await;
    drop(fixture);
    runtime.shutdown().await;
    fixture_pools.close().await;
}
