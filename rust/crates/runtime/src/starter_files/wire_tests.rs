//! Real loopback websocket + separately compiled public SDK; no runtime.start,
//! module host, production endpoint or customer data.
use super::config;
use crate::{
    modules::{FunctionDefinition, ProjectModule},
    replica::permission::starter_adapter::CollectionUse,
    starter_files::{provider, Limits, Store},
    Runtime,
};
use gonvex_store::{permission_indexes as indexes, ControlPlane, TenantRoute, TenantSession};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

struct Environment(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl Environment {
    fn set() -> Self {
        let vars = [
            "GONVEX_PERMISSION_PRIMITIVES_LOCAL",
            "GONVEX_STARTER_BOOTSTRAP",
        ];
        let old = vars.iter().map(|k| (*k, std::env::var_os(k))).collect();
        for k in vars {
            std::env::set_var(k, "1");
        }
        Self(old)
    }
}
impl Drop for Environment {
    fn drop(&mut self) {
        for (k, v) in self.0.drain(..) {
            if let Some(v) = v {
                std::env::set_var(k, v)
            } else {
                std::env::remove_var(k)
            }
        }
    }
}
struct Fixture {
    runtime: Runtime,
    session: TenantSession,
    _data: tempfile::TempDir,
    _artifacts: tempfile::TempDir,
    url: String,
    token: String,
    registration: Arc<crate::replica::permission::LocalSnapshotRegistration>,
    store: Arc<Store>,
    build_ns: u128,
}
async fn fixture() -> Fixture {
    fixture_with(true, 0).await
}
async fn fixture_with(prebuild: bool, extra_rows: usize) -> Fixture {
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let data = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let artifacts = tempfile::tempdir_in("/mnt/data/work/gonvex-starter-bootstrap").unwrap();
    let runtime = Runtime::new(config(false));
    let url = gonvex_store::database_url(&data.path().join("tenant.db"));
    let control = ControlPlane::connect(
        &gonvex_store::database_url(&data.path().join("control.db")),
        runtime.inner.pools.clone(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let cp = control.pools().pool(control.database_url()).await.unwrap();
    sqlx::raw_sql("INSERT INTO gonvex_runtime_projects(id,name,database_mode) VALUES('starter-wire','synthetic','multiTenant'); INSERT INTO accounts(id,auth_realm_id) VALUES('account-a','starter-wire'),('account-b','starter-wire');").execute(&cp).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,provisioned) VALUES('rel','starter-wire','tenant','synthetic',?1,1)").bind(&url).execute(&cp).await.unwrap();
    let token = "gvx_session_starter_synthetic_bob".to_string();
    sqlx::query("INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,expires_at) VALUES(?1,'starter-wire','account-b',?2)").bind(gonvex_store::token_hash(&token)).bind(chrono::Utc::now().timestamp_millis()+600000).execute(&cp).await.unwrap();
    *runtime.inner.control_plane.write().await = Some(control);
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON,display_name TEXT DEFAULT '',avatar_url TEXT DEFAULT '',membership_revision INTEGER DEFAULT 1); INSERT INTO members(id,account_id,status,role,permissions) VALUES('alice','account-a','active','member','{}'),('bob','account-b','active','member','{}'); CREATE TABLE memberships(id TEXT PRIMARY KEY,memberId TEXT,team TEXT); INSERT INTO memberships VALUES('ma','alice','t1'),('mb','bob','t1'); CREATE TABLE tasks(id TEXT PRIMARY KEY,team TEXT,body TEXT,rank INTEGER,secret TEXT); INSERT INTO tasks VALUES('a','t1','first',1,'private-secret'),('b','t2','hidden',2,'private-secret'),('c','t1','last',3,'private-secret'); CREATE TABLE presence(id TEXT PRIMARY KEY,state TEXT);").execute(&mut **tx.transaction()).await.unwrap();
    for table in ["tasks", "memberships", "presence"] {
        let columns = gonvex_store::rows::table_columns(tx.transaction(), table)
            .await
            .unwrap();
        sqlx::raw_sql(&gonvex_store::feed::capture_triggers_sql(
            table, "id", &columns,
        ))
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    }
    gonvex_store::capture::refresh(tx.transaction())
        .await
        .unwrap();
    for i in 0..extra_rows {
        sqlx::query("INSERT INTO tasks VALUES(?1,'t1',?2,?3,'private-secret')")
            .bind(format!("synthetic-{i:05}"))
            .bind("d".repeat(400))
            .bind(i as i64 + 10)
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let function = FunctionDefinition {
        kind: "query".into(),
        internal: false,
        public: false,
        delivery: "replica".into(),
        action_profile: String::new(),
        action_capabilities: Value::Null,
        replica: Some(provider::fixture_definition()),
        dependencies: Value::Null,
        live_query_plan: None,
        interactive: false,
        classification: String::new(),
        description: String::new(),
        tags: vec![],
        confirmation: String::new(),
        args_schema: Value::Null,
        result_schema: Value::Null,
        profile_slot: 0,
    };
    let module = ProjectModule {
        project_id: "starter-wire".into(),
        generation: 1,
        artifact_hash: "starter-wire-source".into(),
        client_contract: None,
        functions: BTreeMap::from([("tasks.all".into(), function)]),
        manifest_functions: json!({}),
        replica_epochs: Arc::default(),
        schema: json!({}),
        visibility: BTreeMap::from([("tasks".into(), provider::fixture_visibility())]),
        permission_grants: None,
        invitation_acceptance_reducer: String::new(),
        migrations: vec![],
        crons: vec![],
    };
    runtime
        .inner
        .modules
        .install_permission_fixture_manifest(module, provider::fixture_manifest())
        .await;
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "starter-wire".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "account-a".into();
    session.member.id = "alice".into();
    session.member.role = "member".into();
    let registration = Arc::new(
        runtime
            .install_local_source_permission_snapshot(&session, "tasks.all")
            .await
            .unwrap(),
    );
    // Existing user's ordinary cold load happens first; new Bob has the same
    // exact shared team atoms, a distinct active account and no client cache.
    let cold =
        serde_json::from_value(json!({"id":"alice-cold","path":"tasks.all","args":{}})).unwrap();
    runtime.open_replica_fresh(&session, cold).await.unwrap();
    let store = Store::open(artifacts.path(), Limits::default()).unwrap();
    let build_start = std::time::Instant::now();
    if prebuild {
        runtime
            .spawn_local_starter_after_miss(
                registration.clone(),
                session.clone(),
                store.clone(),
                CollectionUse::SharedBase,
            )
            .unwrap()
            .await
            .unwrap()
            .unwrap();
    }
    runtime
        .register_local_starter_transport(&session, registration.clone(), store.clone())
        .await
        .unwrap();
    Fixture {
        runtime,
        session,
        _data: data,
        _artifacts: artifacts,
        url,
        token,
        registration,
        store,
        build_ns: build_start.elapsed().as_nanos(),
    }
}
fn evaluated() -> u64 {
    crate::raw_sites::snapshot()
        .iter()
        .filter(|s| {
            matches!(
                s.0,
                "cache_view" | "cache_row_raw" | "scan_row_raw" | "scan_row_parsed"
            )
        })
        .map(|s| s.1)
        .sum()
}
#[tokio::test]
#[ignore = "build candidate client/protocol and set STARTER_SDK_CLIENT_ARTIFACT; requires perf-fixture counters"]
async fn starter_live_sdk_negotiates_promotes_catches_up_and_normally_resumes() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture_with(false, 0).await;
    let mut build_gate = Some(f.store.build_permit().await.unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = f.runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    for mode in [
        "old-client",
        "bootstrap",
        "old-server",
        "revoke-before-piece",
    ] {
        std::env::set_var(
            "GONVEX_STARTER_BOOTSTRAP",
            if mode == "old-server" { "0" } else { "1" },
        );
        let mut child = tokio::process::Command::new("node")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/testdata/starter-live-sdk.mjs"
            ))
            .env(
                "STARTER_SDK_CLIENT_ARTIFACT",
                std::env::var("STARTER_SDK_CLIENT_ARTIFACT")
                    .expect("explicit candidate artifact directory"),
            )
            .arg(format!("ws://{address}/ws"))
            .arg(&f.token)
            .arg(mode)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
        let mut before = 0;
        let mut staged = 0;
        while let Some(line) =
            tokio::time::timeout(std::time::Duration::from_secs(30), lines.next_line())
                .await
                .expect("SDK child line timeout")
                .unwrap()
        {
            println!("SDK {line}");
            if line == "OFFERED" {
                let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
                    .await
                    .unwrap();
                sqlx::query("UPDATE tasks SET team='t2' WHERE id='a'")
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                f.runtime
                    .rebuild_local_source_permission_snapshot(&f.session, &f.registration)
                    .await
                    .unwrap();
                input.write_all(b"CONTINUE\n").await.unwrap();
            } else if line == "RETENTION" {
                let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
                    .await
                    .unwrap();
                sqlx::query("UPDATE presence SET state='retention' WHERE id='p'")
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap();
                indexes::seal(&mut tx, f.registration.index_scope())
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
                    .await
                    .unwrap();
                sqlx::raw_sql("DELETE FROM _gonvex_sync_changes WHERE revision<2; UPDATE _gonvex_sync_clock SET retained_revision=2 WHERE singleton=1").execute(&mut **tx.transaction()).await.unwrap();
                tx.commit().await.unwrap();
                f.runtime
                    .spawn_local_starter_after_miss(
                        f.registration.clone(),
                        f.session.clone(),
                        f.store.clone(),
                        CollectionUse::SharedBase,
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                input.write_all(b"OPEN_OLD\n").await.unwrap();
            } else if line == "STAGED" {
                staged += 1;
                if staged > 1 {
                    input.write_all(b"CONTINUE\n").await.unwrap();
                    continue;
                }
                let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
                    .await
                    .unwrap();
                sqlx::query("INSERT INTO presence VALUES('p','active')")
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap();
                indexes::seal(&mut tx, f.registration.index_scope())
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                input.write_all(b"CONTINUE\n").await.unwrap();
            } else if line == "READY" {
                before = evaluated();
                input.write_all(b"RESUME\n").await.unwrap();
            } else if line == "RESUMED" {
                assert_eq!(evaluated(), before, "ordinary XOR resume read base rows");
            }
        }
        let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success(), "SDK child failed mode {mode}");
        if mode == "old-client" {
            // The normal ready arrived despite a deliberately blocked builder.
            drop(build_gate.take());
            f.runtime.wait_starter_background().await;
            f.runtime.clear_resume_validation();
        }
    }
    server.abort();
    let _ = server.await;
}

async fn bob_connection(
    f: &Fixture,
) -> (
    crate::replica::permission::transport::Connection,
    TenantSession,
) {
    let control = f.runtime.inner.control_plane.read().await.clone().unwrap();
    let session = control
        .authenticate_session(&f.token, Some("starter-wire"), Some("tenant"))
        .await
        .unwrap();
    let authenticated = crate::control::ControlConnection {
        connection_id: "synthetic".into(),
        project_id: "starter-wire".into(),
        identity: Some(session.identity.clone()),
        tenant: Some(session.clone()),
        auth_token: f.token.clone(),
        ..Default::default()
    };
    let mut c = crate::replica::permission::transport::Connection::default();
    assert!(c.bind_auth(&authenticated));
    c.negotiated = true;
    (c, session)
}
fn cold() -> gonvex_protocol::ReplicaOpenRequest {
    serde_json::from_value(json!({"id":"wire","path":"tasks.all","args":{},"starter":{"version":1},"digest":format!("xor1:{}","0".repeat(64))})).unwrap()
}
async fn offered(
    f: &Fixture,
    c: &mut crate::replica::permission::transport::Connection,
    s: &TenantSession,
) -> gonvex_protocol::StarterOffer {
    assert!(f.runtime.offer_starter(c, s, &cold(), false).await.unwrap());
    match c.pop().unwrap() {
        gonvex_protocol::ServerMessage::ReplicaStarterOffer(o) => o,
        _ => panic!("not an offer"),
    }
}
#[tokio::test]
async fn starter_guard_denies_unchanged_atoms_row_move_old_hash_and_allows_fresh_build() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture().await;
    let (mut c, s) = bob_connection(&f).await;
    let offer = offered(&f, &mut c, &s).await;
    let before = f
        .runtime
        .open_local_permission_snapshot(
            &f.registration,
            &s,
            serde_json::from_value(json!({"id":"oracle","path":"tasks.all","args":{}})).unwrap(),
        )
        .await
        .unwrap()
        .identity;
    let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET team='t2' WHERE id='a'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    f.runtime
        .rebuild_local_source_permission_snapshot(&f.session, &f.registration)
        .await
        .unwrap();
    let after = f
        .runtime
        .open_local_permission_snapshot(
            &f.registration,
            &s,
            serde_json::from_value(json!({"id":"oracle","path":"tasks.all","args":{}})).unwrap(),
        )
        .await
        .unwrap()
        .identity;
    assert_eq!(before.exact, after.exact);
    assert_ne!(before.change_serial, after.change_serial);
    assert!(f
        .runtime
        .starter_piece(&mut c, &s, &offer.id, &offer.offer_id, 0)
        .await
        .is_err());
    assert!(
        c.pop().is_none(),
        "revoked row bytes entered the bounded outbound channel"
    );
    assert!(f
        .runtime
        .read_local_starter_piece(
            &f.registration,
            &s,
            &f.store,
            &offer.pieces[0].hash,
            |_, _| panic!("old raw hash authorized a GET"),
            CollectionUse::SharedBase
        )
        .await
        .is_err());
    c.close(&offer.id);
    f.runtime
        .spawn_local_starter_after_miss(
            f.registration.clone(),
            f.session.clone(),
            f.store.clone(),
            CollectionUse::SharedBase,
        )
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let fresh = offered(&f, &mut c, &s).await;
    assert_eq!(fresh.rows, 1);
    assert_ne!(fresh.pieces[0].hash, offer.pieces[0].hash);
    f.runtime
        .starter_piece(&mut c, &s, &fresh.id, &fresh.offer_id, 0)
        .await
        .unwrap();
    assert!(matches!(
        c.pop(),
        Some(gonvex_protocol::ServerMessage::ReplicaStarterPiece { .. })
    ));
}
#[tokio::test]
async fn starter_native_session_member_account_and_retired_auth_guard_every_payload() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture().await;
    let (mut c, s) = bob_connection(&f).await;
    let offer = offered(&f, &mut c, &s).await;
    let control = f.runtime.inner.control_plane.read().await.clone().unwrap();
    let cp = control.pools().pool(control.database_url()).await.unwrap();
    sqlx::query("UPDATE gonvex_auth_sessions SET revoked_at=1 WHERE token_hash=?1")
        .bind(gonvex_store::token_hash(&f.token))
        .execute(&cp)
        .await
        .unwrap();
    assert!(f
        .runtime
        .starter_piece(&mut c, &s, &offer.id, &offer.offer_id, 0)
        .await
        .is_err());
    assert!(c.pop().is_none());
    sqlx::query("UPDATE gonvex_auth_sessions SET revoked_at=NULL WHERE token_hash=?1")
        .bind(gonvex_store::token_hash(&f.token))
        .execute(&cp)
        .await
        .unwrap();
    let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='inactive' WHERE id='bob'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(f
        .runtime
        .starter_piece(&mut c, &s, &offer.id, &offer.offer_id, 0)
        .await
        .is_err());
    assert!(c.pop().is_none());
    let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='active' WHERE id='bob'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("UPDATE accounts SET disabled_at=1 WHERE id='account-b'")
        .execute(&cp)
        .await
        .unwrap();
    assert!(f
        .runtime
        .starter_piece(&mut c, &s, &offer.id, &offer.offer_id, 0)
        .await
        .is_err());
    assert!(c.pop().is_none());
    sqlx::query("UPDATE accounts SET disabled_at=NULL WHERE id='account-b'")
        .execute(&cp)
        .await
        .unwrap();
    f.runtime
        .starter_piece(&mut c, &s, &offer.id, &offer.offer_id, 0)
        .await
        .unwrap();
    c.retire();
    assert!(
        c.pop().is_none(),
        "retired auth leaked an already queued chunk"
    );
    assert!(c.is_empty());
}
#[tokio::test]
async fn starter_complete_certificate_rejects_unmodeled_membership_state_and_unsupported_shapes() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture().await;
    let (mut c, s) = bob_connection(&f).await;
    let mut args = cold();
    args.args = json!({"assignedToMe":true});
    assert!(!f
        .runtime
        .offer_starter(&mut c, &s, &args, false)
        .await
        .unwrap());
    assert!(!f
        .runtime
        .offer_starter(&mut c, &s, &cold(), true)
        .await
        .unwrap());
    let offer = offered(&f, &mut c, &s).await;
    c.close(&offer.id);
    let mut cached = cold();
    cached.cursor = Some(offer.cursor);
    assert!(!matches!(
        f.runtime.offer_starter(&mut c, &s, &cached, false).await,
        Ok(true)
    ));
    assert!(c.pop().is_none());
    let mut tx = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE memberships ADD COLUMN deletedAt INTEGER")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(f
        .runtime
        .register_local_starter_transport(&s, f.registration.clone(), f.store.clone())
        .await
        .is_err());
    assert!(!matches!(
        f.runtime.offer_starter(&mut c, &s, &cold(), false).await,
        Ok(true)
    ));
    assert!(c.pop().is_none());
}

#[tokio::test]
async fn starter_enqueue_linearizes_before_waiting_revoke_writer() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture().await;
    let (mut c, session) = bob_connection(&f).await;
    let offer = offered(&f, &mut c, &session).await;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let gate = barrier.clone();
    *crate::replica::permission::transport::BEFORE_PIECE_ENQUEUE
        .lock()
        .unwrap() = Some(Arc::new(move || {
        entered_tx.lock().unwrap().take().unwrap().send(()).unwrap();
        gate.wait();
    }));
    let runtime = f.runtime.clone();
    let s = session.clone();
    let id = offer.id.clone();
    let oid = offer.offer_id.clone();
    let task = tokio::spawn(async move {
        crate::replica::permission::transport::piece_offloaded(runtime, c, s, id, oid, 0).await
    });
    entered_rx.await.unwrap();
    let revoke = gonvex_store::begin_transaction(&f.runtime.inner.pools, &f.url, false);
    tokio::pin!(revoke);
    assert!(
        futures_util::poll!(&mut revoke).is_pending(),
        "writer entered before guarded enqueue"
    );
    barrier.wait();
    let (mut c, result) = task.await.unwrap();
    result.unwrap();
    *crate::replica::permission::transport::BEFORE_PIECE_ENQUEUE
        .lock()
        .unwrap() = None;
    let mut tx = revoke.await.unwrap();
    sqlx::query("UPDATE tasks SET team='t2' WHERE id='a'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        matches!(
            c.pop(),
            Some(gonvex_protocol::ServerMessage::ReplicaStarterPiece { .. })
        ),
        "authorized enqueue lost its linearization"
    );
    f.runtime
        .rebuild_local_source_permission_snapshot(&f.session, &f.registration)
        .await
        .unwrap();
    assert!(f
        .runtime
        .commit_starter(
            &mut c,
            &session,
            &offer.id,
            &offer.offer_id,
            &offer.cursor,
            &offer.digest
        )
        .await
        .is_err());
    assert!(c.pop().is_none(), "revoked stage became ready");
}

#[tokio::test]
#[ignore = "explicit compiled SDK artifact; synthetic module benchmark, no capacity claim"]
async fn starter_live_module_benchmark() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture_with(true, 9000).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = f.runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut reports = Vec::new();
    let repeats = std::env::var("STARTER_BENCH_REPEATS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(3)
        .clamp(1, 3);
    for repeat in 0..repeats {
        for mode in ["benchmark-normal", "benchmark-starter"] {
            crate::replica::permission::transport::TIMINGS
                .lock()
                .unwrap()
                .clear();
            gonvex_profiler::reset();
            crate::raw_sites::reset();
            crate::starter_files::diagnostics::reset(std::env::var_os("STARTER_DETAIL").is_some());
            crate::starter_files::capture_allocations(
                std::env::var("STARTER_ALLOC_PROFILE").as_deref() != Ok("0"),
            );
            let cpu_start = process_cpu_ns();
            let output = tokio::process::Command::new("node")
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/testdata/starter-live-sdk.mjs"
                ))
                .arg(format!("ws://{address}/ws"))
                .arg(&f.token)
                .arg(mode)
                .env(
                    "STARTER_SDK_CLIENT_ARTIFACT",
                    std::env::var("STARTER_SDK_CLIENT_ARTIFACT").unwrap(),
                )
                .output()
                .await
                .unwrap();
            let server_cpu_ns = process_cpu_ns() - cpu_start;
            let server_allocations = crate::starter_files::capture_allocations(false);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let text = String::from_utf8(output.stdout).unwrap();
            let node: Value =
                serde_json::from_str(text.lines().find_map(|l| l.strip_prefix("BENCH ")).unwrap())
                    .unwrap();
            let timings = crate::replica::permission::transport::TIMINGS
                .lock()
                .unwrap()
                .clone();
            reports.push(json!({"repeat":repeat,"node":node,"raw_sites":crate::raw_sites::snapshot(),"detail":crate::starter_files::diagnostics::snapshot(),"server_stage_wall_ns":timings,"server_process_cpu_ns":server_cpu_ns,"server_alloc_count_requested_bytes":server_allocations,"fast_profiler":gonvex_profiler::snapshot().into_iter().filter(|s|s.count>0).map(|s|json!({"stage":s.name(),"label":format!("{:?}",s.label),"count":s.count,"sum_ns":s.sum,"max_ns":s.max})).collect::<Vec<_>>()}));
        }
    }
    fn files(path: &std::path::Path) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(path).unwrap() {
            let e = e.unwrap();
            if e.path().is_dir() {
                out.extend(files(&e.path()));
            } else {
                out.push((
                    e.file_name().to_string_lossy().into_owned(),
                    e.metadata().unwrap().len(),
                ));
            }
        }
        out
    }
    let disk = files(f._artifacts.path());
    let mut raw_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    for entry in std::fs::read_dir(f._artifacts.path().join("manifests")).unwrap() {
        let entry = entry.unwrap();
        let envelope: Value =
            serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
        if let Ok(m) =
            serde_json::from_value::<crate::starter_files::Manifest>(envelope["manifest"].clone())
        {
            raw_bytes += m.pieces.iter().map(|p| p.raw_bytes).sum::<u64>();
            compressed_bytes += m.pieces.iter().map(|p| p.bytes).sum::<u64>();
        }
    }
    let result = json!({"optimized":!cfg!(debug_assertions),"debug_assertions":cfg!(debug_assertions),"allocation_profile":std::env::var("STARTER_ALLOC_PROFILE").as_deref()!=Ok("0"),"build_ns":f.build_ns,"raw_bytes":raw_bytes,"compressed_bytes":compressed_bytes,"server_resident_bytes":resident_bytes(),"qualification":"synthetic 9002 visible tasks, 400-byte extra descriptions; runtime build profile described by optimized/debug_assertions fields, Node-only codec; warm server and OS, fresh client/storage each run; authority/window/format scan, native session checks, pieces, normal-v2 anchor scan included; timings are noisy wall ns/op, not 1-vCPU capacity; Rust allocation requests across all test-process threads (includes workers and fixture maintenance), excludes SQLite C and Node; RSS is whole Node process peak, SDK charges estimated retained JS, not hard RSS cap", "rows":9002,"reports":reports,"disk_files":disk,"disk_bytes":disk.iter().map(|x|x.1).sum::<u64>()});
    std::fs::write(
        std::env::var("STARTER_BENCH_RESULT").unwrap_or_else(|_| {
            "/mnt/data/work/gonvex-starter-bootstrap/module-benchmark.json".into()
        }),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("STARTER_BENCH {}", result);
    server.abort();
    let _ = server.await;
}

fn process_cpu_ns() -> u128 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) },
        0
    );
    ts.tv_sec as u128 * 1_000_000_000 + ts.tv_nsec as u128
}
fn resident_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()
        .map(|n| n * 1024)
}

#[tokio::test]
async fn starter_connection_budget_includes_pending_piece_buffers_and_releases_on_retirement() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture().await;
    let (mut c, s) = bob_connection(&f).await;
    let mut admitted = 0;
    for i in 0..12 {
        let mut request = cold();
        request.id = format!("budget-{i}");
        if matches!(
            f.runtime.offer_starter(&mut c, &s, &request, false).await,
            Ok(true)
        ) {
            admitted += 1;
            assert!(c.pop().is_some());
        }
        let (count, bytes) = c.budget_stats();
        assert!(count <= 8 && bytes <= 8 << 20);
    }
    assert!(
        admitted > 0 && admitted < 8,
        "per-connection byte cap must bind before count cap"
    );
    c.retire();
    assert_eq!(c.budget_stats(), (0, 0));
    assert!(c.pop().is_none());
}

#[tokio::test]
async fn starter_missing_or_corrupt_pinned_piece_denies_bytes_and_cancels_once() {
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    for missing in [true, false] {
        let f = fixture().await;
        let (mut c, session) = bob_connection(&f).await;
        let offer = offered(&f, &mut c, &session).await;
        let path = f
            ._artifacts
            .path()
            .join("pieces")
            .join(&offer.pieces[0].hash);
        if missing {
            std::fs::remove_file(path).unwrap();
        } else {
            std::fs::write(path, b"corrupt").unwrap();
        }
        assert!(f
            .runtime
            .starter_piece(&mut c, &session, &offer.id, &offer.offer_id, 0)
            .await
            .is_err());
        assert!(
            c.pop().is_none(),
            "unverified encoded bytes reached outbound channel"
        );
        assert!(c.cancel(&offer.id, &offer.offer_id).is_some());
        assert!(
            c.cancel(&offer.id, &offer.offer_id).is_none(),
            "replayed cancellation doubled normal fallback"
        );
    }
}

#[tokio::test]
async fn starter_valid_checksums_cannot_replace_authorized_rows_before_offer() {
    use sha2::{Digest, Sha256};
    let hash = |bytes: &[u8]| {
        Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Environment::set();
    let f = fixture().await;
    let path = std::fs::read_dir(f._artifacts.path().join("manifests"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut envelope: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    // Preserve exact identity/serial/source authority and row count. Even a
    // self-consistent checksummed cache cannot substitute out-of-scope key b.
    let raw=b"{\"id\":\"a\",\"body\":\"first\",\"rank\":1}\n{\"id\":\"b\",\"body\":\"hidden\",\"rank\":2}\n";
    let piece_hash = hash(raw);
    std::fs::write(f._artifacts.path().join("pieces").join(&piece_hash), raw).unwrap();
    envelope["manifest"]["pieces"] = json!([{"hash":piece_hash,"raw_hash":piece_hash,"compressed":false,"bytes":raw.len(),"raw_bytes":raw.len()}]);
    let manifest: crate::starter_files::Manifest =
        serde_json::from_value(envelope["manifest"].clone()).unwrap();
    envelope["checksum"] = json!(hash(&serde_json::to_vec(&manifest).unwrap()));
    std::fs::write(path, serde_json::to_vec(&envelope).unwrap()).unwrap();
    let (mut c, s) = bob_connection(&f).await;
    assert!(!matches!(
        f.runtime.offer_starter(&mut c, &s, &cold(), false).await,
        Ok(true)
    ));
    assert!(
        c.pop().is_none(),
        "a forged cache was offered before authorized SQL equality proof"
    );
}
