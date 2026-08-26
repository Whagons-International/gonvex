use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use gonvex_postgres::{token_hash, ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};
use gonvex_runtime::control::ControlConnection;
use gonvex_runtime::Runtime;
use pbkdf2::pbkdf2_hmac;
use serde_json::Value;
use sha2::Sha256;
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
        require_auth: true,
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

#[tokio::test]
async fn refresh_token_reuse_commits_family_revocation() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping auth contract test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("gonvex_auth_contract_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
        .execute(&admin)
        .await
        .unwrap();
    let database_url = scoped_url(&base_url, &schema);
    let runtime = Runtime::new(test_config(database_url.clone()));
    runtime.start().await.unwrap();
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_projects(id,name,status,auth_mode) VALUES('project','Project','active','gonvex-native')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('account','project','person@example.test','Person')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES('project','person@example.test','Person','dev')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO account_identities(account_id,project_id,provider,issuer,subject,email,verified_email) VALUES('account','project','password','gonvex-native','person@example.test','person@example.test',TRUE)",
    )
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_dashboard_accounts(email,name,role,password_hash) VALUES('person@example.test','Person','standard','unused')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    let mut password_hash = [0_u8; 32];
    pbkdf2_hmac::<Sha256>(
        b"correct horse battery staple",
        b"fixed-test-salt",
        1,
        &mut password_hash,
    );
    let encoded_password = format!(
        "pbkdf2_sha256$1${}${}",
        URL_SAFE_NO_PAD.encode(b"fixed-test-salt"),
        URL_SAFE_NO_PAD.encode(password_hash)
    );
    sqlx::query(
        "INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES('project','account',$1)",
    )
    .bind(encoded_password)
    .execute(&fixture)
    .await
    .unwrap();

    let login = runtime
        .execute_control_action(
            &ControlConnection {
                project_id: "project".to_owned(),
                ..ControlConnection::default()
            },
            "control.auth.passwordLogin",
            &serde_json::json!({
                "email":"person@example.test",
                "password":"correct horse battery staple",
            }),
            "login-once",
        )
        .await
        .unwrap();
    let access_token = login["accessToken"].as_str().unwrap();
    let response = runtime
        .router()
        .oneshot(
            Request::get("/dev/auth/me")
                .header("authorization", format!("Bearer {access_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let identity: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 64 << 10).await.unwrap()).unwrap();
    assert_eq!(identity["authentication"], "nativeSession");
    assert_eq!(identity["account"]["email"], "person@example.test");

    let refresh = "gvx_refresh_replayed";
    sqlx::query(
        r#"INSERT INTO gonvex_auth_refresh_tokens
           (token_hash,family_id,project_id,account_id,expires_at,used_at)
           VALUES($1,'family','project','account',now()+interval '1 day',now()-interval '10 seconds')"#,
    )
    .bind(token_hash(refresh))
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_auth_sessions
           (token_hash,project_id,account_id,family_id,expires_at)
           VALUES($1,'project','account','family',now()+interval '1 hour')"#,
    )
    .bind(token_hash("gvx_session_existing"))
    .execute(&fixture)
    .await
    .unwrap();

    let response = runtime
        .router()
        .oneshot(
            Request::post("/auth/token")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"project":"project","grantType":"refresh_token","refreshToken":"{refresh}"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 64 << 10).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid or expired authentication grant");
    let rows = sqlx::query(
        r#"SELECT revoked_at IS NOT NULL AS revoked FROM gonvex_auth_sessions
           WHERE family_id='family'
           UNION ALL
           SELECT revoked_at IS NOT NULL FROM gonvex_auth_refresh_tokens WHERE family_id='family'"#,
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.get::<bool, _>("revoked")));

    fixture.close().await;
    runtime.shutdown().await;
    sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn internal_e2e_actor_creation_uses_native_auth_and_tenant_admission() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping E2E actor contract test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_e2e_control_{nonce}");
    let tenant_schema = format!("gonvex_e2e_tenant_{nonce}");
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
    let route = TenantRoute {
        project_id: "project".to_owned(),
        tenant_id: "tenant".to_owned(),
        database_url: tenant_url.clone(),
    };
    control
        .clone()
        .provision_tenant_database(route.clone(), Vec::new())
        .await
        .unwrap();
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&control_url)
        .await
        .unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_projects
           (id,name,database_mode,database_url,status,auth_mode)
           VALUES('project','Project','multiTenant',$1,'active','gonvex-native')"#,
    )
    .bind(&control_url)
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_tenants
           (relationship_id,project_id,tenant_id,name,database_url,status)
           VALUES('tenant','project','tenant','Tenant',$1,'active')"#,
    )
    .bind(&tenant_url)
    .execute(&fixture)
    .await
    .unwrap();

    let runtime = Runtime::new(test_config(control_url.clone()));
    runtime.start().await.unwrap();
    let request_body = serde_json::json!({
        "projectId":"project",
        "tenantId":"tenant",
        "email":"actor@example.test",
        "name":"Test Actor",
        "password":"correct horse battery staple",
    })
    .to_string();
    let response = runtime
        .router()
        .oneshot(
            Request::post("/dev/internal/e2e/members")
                .header("authorization", "Bearer test-admin-key")
                .header("content-type", "application/json")
                .body(Body::from(request_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let created: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 64 << 10).await.unwrap()).unwrap();
    assert_eq!(created["projectId"], "project");
    assert_eq!(created["tenantId"], "tenant");

    let resumed = runtime
        .router()
        .oneshot(
            Request::post("/dev/internal/e2e/members")
                .header("authorization", "Bearer test-admin-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "projectId":"project",
                        "tenantId":"tenant",
                        "email":"actor@example.test",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed: Value =
        serde_json::from_slice(&to_bytes(resumed.into_body(), 64 << 10).await.unwrap()).unwrap();
    assert_eq!(resumed["accountId"], created["accountId"]);
    assert_eq!(resumed["memberId"], created["memberId"]);

    let login = runtime
        .execute_control_action(
            &ControlConnection {
                project_id: "project".to_owned(),
                ..ControlConnection::default()
            },
            "control.auth.passwordLogin",
            &serde_json::json!({
                "email":"actor@example.test",
                "password":"correct horse battery staple",
            }),
            "e2e-login",
        )
        .await
        .unwrap();
    let session = control
        .authenticate_session(
            login["accessToken"].as_str().unwrap(),
            Some("project"),
            Some("tenant"),
        )
        .await
        .unwrap();
    assert_eq!(session.identity.account.id, created["accountId"]);
    assert_eq!(session.member.id, created["memberId"]);
    assert_eq!(session.member.status, "active");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM accounts WHERE auth_realm_id='project' AND lower(email)='actor@example.test'",
        )
        .fetch_one(&fixture)
        .await
        .unwrap(),
        1
    );

    runtime.shutdown().await;
    fixture.close().await;
    pools.close().await;
    for schema in [&tenant_schema, &control_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
    admin.close().await;
}
