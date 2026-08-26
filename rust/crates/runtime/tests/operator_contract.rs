use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};
use gonvex_runtime::Runtime;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use tower::ServiceExt;

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

fn test_config(database_url: String) -> Config {
    Config {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        control_plane_database_url: Some(database_url.clone()),
        default_database_url: Some(database_url),
        tenant_database_urls: BTreeMap::new(),
        project_database_urls: BTreeMap::new(),
        require_auth: false,
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
        },
        runtime_version: "test".to_owned(),
        sandbox: SandboxConfig::default(),
        storage: StorageConfig::default(),
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
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping operator contract test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_operator_control_{nonce}");
    let tenant_schema = format!("gonvex_operator_tenant_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    for schema in [&control_schema, &tenant_schema] {
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
    }
    let control_url = scoped_url(&base_url, &control_schema);
    let tenant_url = scoped_url(&base_url, &tenant_schema);
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let control_fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&control_url)
        .await
        .unwrap();
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
           VALUES('relationship','project','tenant','Tenant',$1,'active',TRUE)"#,
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
    let tenant_fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_url)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO members(id,account_id,display_name,role) VALUES('member','account','Person','admin')",
    )
    .execute(&tenant_fixture)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE tasks(id text PRIMARY KEY,title text NOT NULL,owner_id text,metadata jsonb NOT NULL DEFAULT '{}'::jsonb,created_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(&tenant_fixture)
    .await
    .unwrap();
    sqlx::query("INSERT INTO tasks(id,title,owner_id,metadata) VALUES('task-1','First','old',jsonb_build_object('owner','old'))")
        .execute(&tenant_fixture)
        .await
        .unwrap();

    let runtime = Runtime::new(test_config(control_url.clone()));
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
    tenant_fixture.close().await;
    control_fixture.close().await;
    pools.close().await;
    for schema in [&tenant_schema, &control_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
    admin.close().await;
}
