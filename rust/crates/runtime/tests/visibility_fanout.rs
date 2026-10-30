//! Production Replica Collection feed budgets, measured after admission.
use base64::Engine as _;
use gonvex_postgres::{
    Account, ControlPlane, Member, PoolLimits, PoolRegistry, RuntimeManifestRecord,
    SessionIdentity, TenantRoute, TenantSession,
};
use gonvex_protocol::ReplicaOpenRequest;
use gonvex_runtime::change_feed::{self, FeedEvent};
use gonvex_runtime::replica::ReplicaSubscription;
use gonvex_runtime::{
    config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig},
    Runtime,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::{prelude::*, Layer};

#[derive(Clone)]
struct SqlCapture(Arc<Mutex<Vec<String>>>);
impl<S: tracing::Subscriber> Layer<S> for SqlCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        struct Visitor(Vec<String>);
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "db.statement" {
                    self.0.push(value.to_owned());
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "db.statement" {
                    self.0.push(format!("{value:?}"));
                }
            }
        }
        let mut visitor = Visitor(Vec::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().extend(visitor.0);
    }
}

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
        module_host: ModuleHostConfig {
            enabled: false,
            binary: None,
            endpoint: None,
            start_timeout: Duration::from_secs(30),
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

fn fixture_manifest() -> (Value, String) {
    let permissions = json!(["memberTeams"]);
    let tasks = json!({"version":2,"table":"tasks","key":"id","permissionTables":permissions,"sets":{
        "teams":{"kind":"principal","table":"memberTeams","select":"teamId","joins":[],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]},
        "assigned":{"kind":"grant","table":"taskUsers","select":"taskId","joins":[],"where":[{"table":"taskUsers","column":"userId","context":"member.id"}]},
        "teamAssigned":{"kind":"grant","table":"taskUsers","select":"taskId","joins":[{"table":"memberTeams","leftColumn":"teamId","rightColumn":"teamId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]}
    },"where":{"operator":"or","children":[{"operator":"inSet","column":"workspaceId","set":"teams"},{"operator":"inSet","column":"id","set":"assigned"},{"operator":"inSet","column":"id","set":"teamAssigned"}]}});
    let notes = json!({"version":2,"table":"taskNotes","key":"id","permissionTables":permissions,"sets":{},"where":{"operator":"inherits","column":"taskId","table":"tasks","key":"id"}});
    let mut functions = json!({});
    for (path, table, columns) in [
        ("tasks.list", "tasks", vec!["id", "workspaceId", "title"]),
        ("notes.list", "taskNotes", vec!["id", "taskId", "body"]),
    ] {
        functions[path] = json!({"kind":"query","handler":"list","file":"test.js","delivery":"replica","args":{"kind":"object","fields":{}},"result":{"kind":"any"},"replica":{"table":table,"key":"id","columns":columns,"maxRows":50,"maxBytes":1048576}});
    }
    let code = "export function list() { return []; }";
    let javascript_hash = format!("{:x}", Sha256::digest(code));
    let mut module = json!({"generation":1,"language":"typescript","entrypoint":"test.js","files":{},"functions":functions,"visibility":{"tasks":tasks,"taskNotes":notes},"crons":[],"invitationAcceptanceReducer":"","javascript":{"path":"test.js","hash":javascript_hash}});
    // This fixture contains only integer JSON numbers, whose canonical encoding
    // agrees with serde_json's sorted-object serialization.
    let hash =
        gonvex_runtime::modules::canonical_artifact_hash(module.as_object().unwrap()).unwrap();
    module["hash"] = json!(hash);
    module["javascript"]["code"] = json!(base64::engine::general_purpose::STANDARD.encode(code));
    (json!({"project":"fanout","module":module}), hash)
}

struct Connection {
    member: usize,
    session: TenantSession,
    subscriptions: BTreeMap<String, ReplicaSubscription>,
}

async fn deliver(
    runtime: &Runtime,
    connections: &mut [Connection],
    event: &FeedEvent,
) -> Vec<usize> {
    let mut worked = Vec::new();
    for connection in connections {
        let before = runtime.visibility_metrics().snapshot();
        runtime
            .apply_feed_event(
                &connection.session,
                &mut connection.subscriptions,
                event.clone(),
            )
            .await
            .unwrap();
        // In-memory row checks are the expected per-connection cost of a
        // change (changed rows x connections). Budgets govern expensive work.
        let after = runtime.visibility_metrics().snapshot();
        if (
            after.visibility_sql_statements,
            after.collection_snapshots,
            after.principal_rebuilds,
            after.live_query_reruns,
        ) != (
            before.visibility_sql_statements,
            before.collection_snapshots,
            before.principal_rebuilds,
            before.live_query_reruns,
        ) {
            worked.push(connection.member);
        }
    }
    worked
}

async fn committed(pools: &PoolRegistry, route: &TenantRoute, after: &mut u64) -> FeedEvent {
    let clock = change_feed::read_clock(pools, route).await.unwrap();
    let changes = change_feed::read_changes(pools, route, *after, clock.revision)
        .await
        .unwrap();
    assert!(
        !changes.is_empty(),
        "fixture write must pass through real change capture"
    );
    assert!(
        changes
            .iter()
            .all(|change| change.revision == clock.revision),
        "one intent per measured feed transaction"
    );
    *after = clock.revision;
    FeedEvent::Transaction {
        database_epoch: clock.epoch,
        revision: clock.revision,
        changes,
    }
}

#[tokio::test]
async fn visibility_fanout_stays_within_statement_budgets() {
    let Ok(base) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
        eprintln!("GONVEX_TEST_POSTGRES_URL absent; PostgreSQL fixture unavailable");
        return;
    };
    let binary = std::env::var("GONVEX_TEST_MODULE_HOST_BINARY")
        .expect("fan-out integration requires the real module-host binary");
    let n: usize = std::env::var("GONVEX_VIS_FANOUT_MEMBERS")
        .unwrap_or_else(|_| "200".into())
        .parse()
        .unwrap();
    assert!(
        n >= 4,
        "fixture needs three grant recipients and one unrelated member"
    );
    let schema = format!("visibility_fanout_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = scoped_url(&base, &schema);
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('fanout','Fanout','multiTenant','active')").execute(&db).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('fanout','fanout','tenant','Tenant',$1,'active')").bind(&url).execute(&db).await.unwrap();
    sqlx::raw_sql(
        r#"
        CREATE TABLE tasks(id text PRIMARY KEY, "workspaceId" text, title text);
        CREATE TABLE "taskNotes"(id text PRIMARY KEY, "taskId" text, body text);
        CREATE TABLE "taskUsers"(id text PRIMARY KEY, "taskId" text, "userId" text, "teamId" text);
        CREATE TABLE "memberTeams"(id text PRIMARY KEY, "memberId" text, "teamId" text);
        INSERT INTO tasks VALUES('hidden','private','Hidden'),('granted','private-grant','Granted');
        INSERT INTO "taskNotes" VALUES('hidden-note','hidden','Private note');
    "#,
    )
    .execute(&db)
    .await
    .unwrap();
    let route = TenantRoute {
        project_id: "fanout".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    };
    control
        .clone()
        .provision_tenant_database(route.clone(), Vec::new())
        .await
        .unwrap();
    for i in 0..n {
        sqlx::query(
            "INSERT INTO members(id,account_id,status,display_name) VALUES($1,$1,'active',$1)",
        )
        .bind(format!("m{i}"))
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("INSERT INTO \"memberTeams\" VALUES($1,$1,'team')")
            .bind(format!("m{i}"))
            .execute(&db)
            .await
            .unwrap();
    }
    for i in 1..=3 {
        sqlx::query("INSERT INTO \"memberTeams\" VALUES($1,$2,'grant-team')")
            .bind(format!("grant-team-{i}"))
            .bind(format!("m{i}"))
            .execute(&db)
            .await
            .unwrap();
    }
    let (manifest, hash) = fixture_manifest();
    control
        .save_runtime_manifest(&RuntimeManifestRecord {
            project_id: "fanout".into(),
            module_hash: hash,
            manifest,
        })
        .await
        .unwrap();
    let mut config = test_config(url.clone());
    config.module_host.enabled = true;
    config.module_host.binary = Some(binary.into());
    let runtime = Runtime::new(config);
    runtime.start().await.unwrap();
    let mut connections = Vec::new();
    // Member 1 has two connections: its permission change must still build only
    // one shared principal, independently of collection/connection count.
    for i in (0..n).chain(std::iter::once(1)) {
        let id = format!("m{i}");
        let session = TenantSession {
            identity: SessionIdentity {
                project_id: "fanout".into(),
                account: Account {
                    id: id.clone(),
                    email: format!("{id}@example.test"),
                    email_verified: true,
                    name: id.clone(),
                    avatar_url: String::new(),
                    provider: "password".into(),
                },
            },
            route: route.clone(),
            member: Member {
                id: id.clone(),
                account_id: id.clone(),
                status: "active".into(),
                display_name: id,
                avatar_url: String::new(),
                role: "member".into(),
                permissions: json!({}),
                membership_revision: 1,
            },
            admission_revision: 0,
            delegation: None,
            developer_session: None,
        };
        let mut subscriptions = BTreeMap::new();
        for (id, path) in [("tasks", "tasks.list"), ("notes", "notes.list")] {
            let request: ReplicaOpenRequest =
                serde_json::from_value(json!({"id":id,"path":path,"args":{}})).unwrap();
            let opened = runtime.open_replica(&session, request).await.unwrap();
            let subscription = opened
                .subscription
                .expect("real collection must remain subscribed");
            assert!(subscription.rows.is_empty());
            subscriptions.insert(id.to_owned(), subscription);
        }
        connections.push(Connection {
            member: i,
            session,
            subscriptions,
        });
    }
    let mut revision = change_feed::read_clock(&pools, &route)
        .await
        .unwrap()
        .revision;
    runtime.visibility_metrics().reset_for_test();
    for statement in [
        "INSERT INTO tasks VALUES('changed','team','Before')",
        "UPDATE tasks SET title='After' WHERE id='changed'",
        "DELETE FROM tasks WHERE id='changed'",
    ] {
        sqlx::query(statement).execute(&db).await.unwrap();
        let event = committed(&pools, &route, &mut revision).await;
        deliver(&runtime, &mut connections, &event).await;
        let counters = runtime.visibility_metrics().snapshot();
        assert_eq!(
            counters.visibility_sql_statements, 0,
            "ordinary task write ran visibility SQL"
        );
        assert_eq!(
            counters.collection_snapshots, 0,
            "ordinary task write rebuilt a collection"
        );
        assert_eq!(counters.principal_rebuilds, 0);
        eprintln!("fanout task-write {statement:?}: {counters:?}");
    }
    assert!(
        runtime.visibility_metrics().snapshot().in_memory_row_checks > 0,
        "instrumentation must count production evaluator work"
    );
    runtime.visibility_metrics().reset_for_test();
    sqlx::query("INSERT INTO \"taskUsers\" VALUES('team-grant','granted',NULL,'grant-team')")
        .execute(&db)
        .await
        .unwrap();
    let event = committed(&pools, &route, &mut revision).await;
    let worked = deliver(&runtime, &mut connections, &event).await;
    assert_eq!(
        worked
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([1, 2, 3]),
        "one team grant must affect exactly its three member recipients"
    );
    assert!(
        worked.iter().all(|member| (1..=3).contains(member)),
        "grant work reached unnamed members: {worked:?}"
    );
    for connection in &connections {
        assert_eq!(
            connection.subscriptions["tasks"]
                .rows
                .contains_key("granted"),
            (1..=3).contains(&connection.member)
        );
    }
    // The granted task row is not in the feed event (only the grant row
    // changed), so each named member's connection fetches it once per open
    // collection. Nothing else runs SQL.
    let named_connections = connections
        .iter()
        .filter(|connection| (1..=3).contains(&connection.member))
        .count() as u64;
    let statements = runtime
        .visibility_metrics()
        .snapshot()
        .visibility_sql_statements;
    eprintln!(
        "fanout team-grant: worked={worked:?} {:?}",
        runtime.visibility_metrics().snapshot()
    );
    assert!(
        statements <= named_connections * 2,
        "grant fetches exceeded one per named connection and collection: {statements}"
    );
    runtime.visibility_metrics().reset_for_test();
    sqlx::query("INSERT INTO \"memberTeams\" VALUES('extra','m1','extra')")
        .execute(&db)
        .await
        .unwrap();
    let event = committed(&pools, &route, &mut revision).await;
    let worked = deliver(&runtime, &mut connections, &event).await;
    assert!(
        worked.iter().all(|member| *member == 1),
        "permission work reached unrelated members: {worked:?}"
    );
    // Each of member 1's connections rebuilds its own collections; nothing
    // runs for any other member. Sharing one principal across a member's
    // connections is a follow-up (cost per device, not per tenant).
    let member_one_collections = connections
        .iter()
        .filter(|connection| connection.member == 1)
        .map(|connection| connection.subscriptions.len() as u64)
        .sum::<u64>();
    let rebuilds = runtime.visibility_metrics().snapshot().principal_rebuilds;
    eprintln!(
        "fanout member-team change: worked={worked:?} {:?}",
        runtime.visibility_metrics().snapshot()
    );
    assert!(
        (1..=member_one_collections).contains(&rebuilds),
        "permission change rebuilt {rebuilds} collections; member 1 holds {member_one_collections}"
    );
    runtime.visibility_metrics().reset_for_test();
    sqlx::query("INSERT INTO \"taskUsers\" VALUES('parent-grant','hidden','m0',NULL)")
        .execute(&db)
        .await
        .unwrap();
    let event = committed(&pools, &route, &mut revision).await;
    for connection in &mut connections {
        let statements = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(SqlCapture(statements.clone()));
        runtime
            .apply_feed_event(
                &connection.session,
                &mut connection.subscriptions,
                event.clone(),
            )
            .with_subscriber(subscriber)
            .await
            .unwrap();
        let child_fetches = statements
            .lock()
            .unwrap()
            .iter()
            .filter(|sql| sql.contains("taskNotes") && sql.to_ascii_uppercase().contains("SELECT"))
            .count();
        if connection.member == 0 {
            assert!(child_fetches > 0, "newly visible parent must execute a real targeted child fetch; SQLx query tracing must be enabled");
        } else {
            assert_eq!(
                child_fetches, 0,
                "child fetch reached unrelated member {}",
                connection.member
            );
        }
    }
    for connection in &connections {
        assert_eq!(
            connection.subscriptions["notes"]
                .rows
                .contains_key("hidden-note"),
            connection.member == 0,
            "parent transition delivered children to the wrong member"
        );
    }
    eprintln!(
        "fanout parent-grant child fetch: {:?}",
        runtime.visibility_metrics().snapshot()
    );
    assert_eq!(
        runtime.visibility_metrics().snapshot().collection_snapshots,
        0,
        "inheritance transition rebuilt an entire collection"
    );
    runtime.shutdown().await;
    db.close().await;
    pools.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
