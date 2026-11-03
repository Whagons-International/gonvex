use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use gonvex_store::{
    token_hash, Account, ControlPlane, Member, PoolLimits, PoolRegistry, SessionIdentity,
    TenantRoute, TenantSession,
};
use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};
use gonvex_runtime::control::{ControlConnection, ControlError};
use gonvex_runtime::Runtime;
use pbkdf2::pbkdf2_hmac;
use serde_json::Value;
use sha2::Sha256;
use sqlx::{Row, SqlitePool};
use tempfile::TempDir;
use tower::ServiceExt;

/// A disposable data directory and a fixture writer on its Control Plane file.
/// The runtime under test opens the same file through its own registry.
async fn sqlite_fixture(name: &str) -> (TempDir, String, PoolRegistry, SqlitePool) {
    let data = tempfile::tempdir().unwrap();
    let url = gonvex_store::database_url(&data.path().join(format!("{name}.db")));
    let pools = PoolRegistry::new(PoolLimits::default());
    let fixture = pools.pool(&url).await.unwrap();
    (data, url, pools, fixture)
}

const NOW_MS: &str = "CAST(unixepoch('subsec')*1000 AS INTEGER)";

fn test_config(database_url: String) -> Config {
    // The default database URL names the data directory holding control.db.
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
        database_idle_timeout: std::time::Duration::from_secs(60),
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
    }
}

#[tokio::test]
async fn tenant_directory_allows_project_developers_without_support_administration() {
    let (data, database_url, fixture_pools, fixture) = sqlite_fixture("control").await;
    let runtime = Runtime::new(test_config(database_url.clone()));
    runtime.start().await.unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_projects(id,name,status,owner_email) VALUES('project','Project','active','owner@example.test'),('other','Other','active','other-owner@example.test')",
    ).execute(&fixture).await.unwrap();
    sqlx::query("INSERT INTO accounts(id,email,name) VALUES('dev','dev@example.test','Dev')")
        .execute(&fixture)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES('project','admin@example.test','Admin','admin'),('project','dev@example.test','Dev','dev'),('other','other-dev@example.test','Other developer','dev')",
    ).execute(&fixture).await.unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,domain,database_url,status) VALUES('visible','project','visible','Visible','visible',$1,'active'),('private','other','private','Private','private',$1,'active')",
    ).bind(&database_url).execute(&fixture).await.unwrap();

    for role in [
        "owner",
        "admin",
        "dev",
        "member",
        "tenant-admin",
        "other-dev",
    ] {
        let identity = SessionIdentity {
            project_id: "project".to_owned(),
            account: Account {
                id: role.to_owned(),
                email: format!("{role}@example.test"),
                email_verified: true,
                name: role.to_owned(),
                avatar_url: String::new(),
                provider: "password".to_owned(),
            },
        };
        let connection = ControlConnection {
            project_id: "project".to_owned(),
            identity: Some(identity.clone()),
            tenant: matches!(role, "member" | "tenant-admin").then(|| TenantSession {
                identity,
                route: TenantRoute {
                    project_id: "project".to_owned(),
                    tenant_id: "visible".to_owned(),
                    database_url: database_url.clone(),
                },
                member: Member {
                    id: role.to_owned(),
                    account_id: role.to_owned(),
                    status: "active".to_owned(),
                    display_name: role.to_owned(),
                    avatar_url: String::new(),
                    role: if role == "tenant-admin" {
                        "admin"
                    } else {
                        "member"
                    }
                    .to_owned(),
                    permissions: serde_json::json!({}),
                    membership_revision: 1,
                },
                admission_revision: 0,
                delegation: None,
                developer_session: None,
            }),
            ..ControlConnection::default()
        };
        let args = serde_json::json!({});
        let result = runtime
            .execute_control_query(&connection, "control.support.listTenants", &args)
            .await;
        let live = runtime
            .open_control_query(
                &connection,
                role.to_owned(),
                "control.support.listTenants".to_owned(),
                args.clone(),
            )
            .await;
        if matches!(role, "owner" | "admin" | "dev") {
            let rows = result.unwrap_or_else(|error| panic!("{role} must list tenants: {error}"));
            assert_eq!(
                rows.as_array().unwrap().len(),
                1,
                "{role} must stay within its project"
            );
            assert_eq!(rows[0]["id"], "visible");
            assert_eq!(live.unwrap().1.last_result, rows);
            if role == "dev" {
                let grant = runtime
                    .execute_control_reducer(
                        &connection,
                        "control.developer.enter",
                        &serde_json::json!({"tenantId":"visible"}),
                        "nonmember-entry",
                    )
                    .await
                    .expect("a project developer can enter without provisioning");
                assert!(grant["id"].as_str().unwrap().starts_with("devmode_"));
                for path in [
                    "control.support.listSessions",
                    "control.support.listErrors",
                    "control.project.developers.list",
                ] {
                    assert!(
                        matches!(
                            runtime
                                .execute_control_query(&connection, path, &args)
                                .await,
                            Err(ControlError::ProjectAdminRequired)
                        ),
                        "tenant discovery must not grant {path}"
                    );
                }
                let impersonating = ControlConnection {
                    impersonation_id: "support-grant".to_owned(),
                    ..connection.clone()
                };
                assert!(matches!(
                    runtime
                        .execute_control_query(&impersonating, "control.support.listTenants", &args)
                        .await,
                    Err(ControlError::AdministrationDuringImpersonation)
                ));
            }
        } else {
            assert!(
                matches!(result, Err(ControlError::DeveloperRequired)),
                "{role} must not list tenants"
            );
            assert!(
                matches!(live, Err(ControlError::DeveloperRequired)),
                "{role} must not subscribe to tenants"
            );
        }
    }
    assert!(matches!(
        runtime
            .execute_control_query(
                &ControlConnection {
                    project_id: "project".to_owned(),
                    ..ControlConnection::default()
                },
                "control.support.listTenants",
                &serde_json::json!({})
            )
            .await,
        Err(ControlError::AuthenticationRequired)
    ));
    runtime.shutdown().await;
    fixture_pools.close().await;
    drop(data);
}

#[tokio::test]
async fn refresh_token_reuse_commits_family_revocation() {
    let (data, database_url, fixture_pools, fixture) = sqlite_fixture("control").await;
    let runtime = Runtime::new(test_config(database_url.clone()));
    runtime.start().await.unwrap();
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

    // A browser can deliver the same provider token through session restore
    // and the provider callback at nearly the same time. The shared
    // idempotency key must serialize those calls and return the one committed
    // grant, never observe the conflict without its row.
    let concurrent_args = serde_json::json!({
        "email":"person@example.test",
        "password":"correct horse battery staple",
    });
    let concurrent_connection = ControlConnection {
        project_id: "project".to_owned(),
        ..ControlConnection::default()
    };
    let (first, second) = tokio::join!(
        runtime.execute_control_action(
            &concurrent_connection,
            "control.auth.passwordLogin",
            &concurrent_args,
            "login-concurrent",
        ),
        runtime.execute_control_action(
            &concurrent_connection,
            "control.auth.passwordLogin",
            &concurrent_args,
            "login-concurrent",
        ),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first, second);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM gonvex_control_idempotency WHERE project_id='project' AND idempotency_key='login-concurrent' AND state='completed'",
        )
        .fetch_one(&fixture)
        .await
        .unwrap(),
        1
    );

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
    sqlx::query(&format!(
        r#"INSERT INTO gonvex_auth_refresh_tokens
           (token_hash,family_id,project_id,account_id,expires_at,used_at)
           VALUES($1,'family','project','account',{NOW_MS}+86400000,{NOW_MS}-10000)"#,
    ))
    .bind(token_hash(refresh))
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(&format!(
        r#"INSERT INTO gonvex_auth_sessions
           (token_hash,project_id,account_id,family_id,expires_at)
           VALUES($1,'project','account','family',{NOW_MS}+3600000)"#,
    ))
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

    runtime.shutdown().await;
    fixture_pools.close().await;
    drop(data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_e2e_actor_creation_uses_native_auth_and_tenant_admission() {
    let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new("debug")).with_test_writer().try_init();
    let data = tempfile::tempdir().unwrap();
    let control_url = gonvex_store::database_url(&data.path().join("control.db"));
    let tenant_url = gonvex_store::database_url(&data.path().join("tenant.db"));
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
    // One registry owns a database file in production. Release this fixture
    // registry's tenant connections so only the runtime under test holds them.
    pools.close_database(&tenant_url).await;
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

    sqlx::query("UPDATE gonvex_runtime_projects SET auth_mode='firebase' WHERE id='project'")
        .execute(&fixture)
        .await
        .unwrap();
    let firebase_response = runtime
        .router()
        .oneshot(
            Request::post("/dev/internal/e2e/members")
                .header("authorization", "Bearer test-admin-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "projectId":"project",
                        "tenantId":"tenant",
                        "email":"firebase-actor@example.test",
                        "name":"Firebase Actor",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(firebase_response.status(), StatusCode::OK);
    let firebase_actor: Value = serde_json::from_slice(
        &to_bytes(firebase_response.into_body(), 64 << 10)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(firebase_actor["accountId"].as_str().is_some());
    assert!(firebase_actor["memberId"].as_str().is_some());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM account_identities WHERE account_id=$1 AND provider='password'",
        )
        .bind(firebase_actor["accountId"].as_str().unwrap())
        .fetch_one(&fixture)
        .await
        .unwrap(),
        0
    );

    runtime.shutdown().await;
    pools.close().await;
    drop(data);
}

/// Real sign-ins, including directory routing and tenant-local admission, with
/// only two control connections and two shared SQL slots.
#[tokio::test]
async fn concurrent_signins_complete_with_two_sql_slots() {
    let (_data, scoped, pools, fixture) = sqlite_fixture("control").await;
    let control = ControlPlane::connect(&scoped, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    // Same file for control and tenant also exercises single-database mode.
    control
        .clone()
        .provision_tenant_database(
            TenantRoute {
                project_id: "project".into(),
                tenant_id: "tenant".into(),
                database_url: scoped.clone(),
            },
            Vec::new(),
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,status,auth_mode) VALUES('project','Project','active','gonvex-native')").execute(&fixture).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('tenant','project','tenant','Tenant',$1,'active')").bind(&scoped).execute(&fixture).await.unwrap();
    let mut hash = [0u8; 32];
    pbkdf2_hmac::<Sha256>(b"password", b"salt", 1, &mut hash);
    let encoded = format!(
        "pbkdf2_sha256$1${}${}",
        URL_SAFE_NO_PAD.encode(b"salt"),
        URL_SAFE_NO_PAD.encode(hash)
    );
    for i in 0..20 {
        let id = format!("account-{i}");
        sqlx::query(
            "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES($1,'project',$2,'Person')",
        )
        .bind(&id)
        .bind(format!("person{i}@example.test"))
        .execute(&fixture)
        .await
        .unwrap();
        sqlx::query("INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES('project',$1,$2)").bind(&id).bind(&encoded).execute(&fixture).await.unwrap();
        sqlx::query("INSERT INTO members(id,account_id,status) VALUES($1,$1,'active')")
            .bind(&id)
            .execute(&fixture)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status) VALUES($1,'tenant',$1,'active')").bind(&id).execute(&fixture).await.unwrap();
    }
    let mut config = test_config(scoped);
    config.database_max_connections = 2;
    config.database_max_idle_connections = 2;
    config.database_max_total_connections = 2;
    let runtime = Runtime::new(config);
    runtime.start().await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..20 {
        let runtime = runtime.clone();
        tasks.spawn(async move {
            let connection = ControlConnection { project_id: "project".into(), ..Default::default() };
            let args = serde_json::json!({"email":format!("person{i}@example.test"),"password":"password"});
            let key = format!("login-{i}");
            let result = runtime.execute_control_action(&connection,"control.auth.passwordLogin",&args,&key).await.unwrap();
            assert_eq!(result["activeTenantId"], "tenant");
            assert_eq!(result["tenants"][0]["memberId"], format!("account-{i}"));
            let replay = runtime.execute_control_action(&connection,"control.auth.passwordLogin",&args,&key).await.unwrap();
            assert_eq!(replay["accessToken"], result["accessToken"]);
        });
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("all 20 sign-ins and idempotent replays must complete without starvation");
    sqlx::query("UPDATE members SET status='revoked' WHERE account_id='account-0'")
        .execute(&fixture)
        .await
        .unwrap();
    let replay = runtime
        .execute_control_action(
            &ControlConnection {
                project_id: "project".into(),
                ..Default::default()
            },
            "control.auth.passwordLogin",
            &serde_json::json!({"email":"person0@example.test","password":"password"}),
            "login-0",
        )
        .await
        .unwrap();
    assert_eq!(
        replay["tenants"],
        serde_json::json!([]),
        "replays must revalidate current membership"
    );
    assert_eq!(replay["activeTenantId"], "");
    sqlx::query(
        "UPDATE gonvex_runtime_projects SET owner_email='person1@example.test' WHERE id='project'",
    )
    .execute(&fixture)
    .await
    .unwrap();
    let login = runtime
        .execute_control_action(
            &ControlConnection {
                project_id: "project".into(),
                ..Default::default()
            },
            "control.auth.passwordLogin",
            &serde_json::json!({"email":"person1@example.test","password":"password"}),
            "login-1",
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
    let overview = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.execute_control_query(
            &ControlConnection {
                project_id: "project".into(),
                identity: Some(session.identity),
                ..Default::default()
            },
            "control.support.listTenants",
            &serde_json::json!({}),
        ),
    )
    .await
    .expect("support overview must release control capacity before tenant counts")
    .unwrap();
    assert_eq!(overview[0]["userCount"], 19);
    drop(runtime);
    pools.close().await;
    fixture.close().await;
}
