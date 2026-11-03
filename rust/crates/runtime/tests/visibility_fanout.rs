//! Production Replica Collection feed budgets, measured after admission.
use base64::Engine as _;
use gonvex_store::{
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
use sqlx::sqlite::{Sqlite, SqliteArguments};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

/// Fixture writes commit through a TenantTransaction so the change feed
/// assigns their revision, exactly like a Reducer commit.
async fn write<'q>(
    pools: &PoolRegistry,
    url: &str,
    query: sqlx::query::Query<'q, Sqlite, SqliteArguments<'q>>,
) {
    let mut transaction = gonvex_store::begin_transaction(pools, url, false)
        .await
        .unwrap();
    query.execute(&mut **transaction.transaction()).await.unwrap();
    transaction.commit().await.unwrap();
}

/// Writes a multi-statement fixture script as one committed revision.
async fn script(pools: &PoolRegistry, url: &str, sql: &str) {
    let mut transaction = gonvex_store::begin_transaction(pools, url, false)
        .await
        .unwrap();
    gonvex_store::execute_script(&mut **transaction.transaction(), sql)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

fn sql_capture() -> Arc<Mutex<Vec<String>>> {
    static CAPTURE: std::sync::OnceLock<Arc<Mutex<Vec<String>>>> = std::sync::OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let statements = Arc::new(Mutex::new(Vec::new()));
            tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(SqlCapture(statements.clone())),
            )
            .expect("SQL capture must own the global subscriber");
            statements
        })
        .clone()
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

fn fixture_manifest(legacy: bool) -> (Value, String) {
    let permissions = json!(["memberTeams"]);
    let tasks = json!({"version":2,"table":"tasks","key":"id","permissionTables":permissions,"sets":{
        "teams":{"kind":"principal","table":"memberTeams","select":"teamId","joins":[],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]},
        "assigned":{"kind":"grant","table":"taskUsers","select":"taskId","joins":[],"where":[{"table":"taskUsers","column":"userId","context":"member.id"}]},
        "teamAssigned":{"kind":"grant","table":"taskUsers","select":"taskId","joins":[{"table":"memberTeams","leftColumn":"teamId","rightColumn":"teamId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]}
    },"where":{"operator":"or","children":[{"operator":"inSet","column":"workspaceId","set":"teams"},{"operator":"inSet","column":"id","set":"assigned"},{"operator":"inSet","column":"id","set":"teamAssigned"}]}});
    let notes = json!({"version":2,"table":"taskNotes","key":"id","permissionTables":permissions,"sets":{},"where":{"operator":"inherits","column":"taskId","table":"tasks","key":"id"}});
    let (mut tasks, notes) = if legacy {
        // Today's failure shape: task IDs selected FROM tasks, and child sets
        // that join tasks. Every task write becomes a set dependency change.
        let workspace = json!({"table":"tasks","select":"id","joins":[{"table":"memberTeams","leftColumn":"workspaceId","rightColumn":"teamId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]});
        let assigned = json!({"table":"taskUsers","select":"taskId","joins":[],"where":[{"table":"taskUsers","column":"userId","context":"member.id"}]});
        let team = json!({"table":"taskUsers","select":"taskId","joins":[{"table":"memberTeams","leftColumn":"teamId","rightColumn":"teamId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]});
        let tasks = json!({"table":"tasks","key":"id","sets":{"workspaceTasks":workspace,"assigned":assigned,"teamAssigned":team},"where":{"operator":"or","children":[{"operator":"inSet","column":"id","set":"workspaceTasks"},{"operator":"inSet","column":"id","set":"assigned"},{"operator":"inSet","column":"id","set":"teamAssigned"}]}});
        let notes = json!({"table":"taskNotes","key":"id","sets":{
            "workspaceTasks":{"table":"memberTeams","select":"id","selectFrom":"tasks","joins":[{"table":"tasks","leftColumn":"teamId","rightColumn":"workspaceId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]},
            "assigned":{"table":"taskUsers","select":"taskId","joins":[{"table":"tasks","leftColumn":"taskId","rightColumn":"id"}],"where":[{"table":"taskUsers","column":"userId","context":"member.id"}]},
            "teamAssigned":{"table":"taskUsers","select":"taskId","joins":[{"table":"tasks","leftColumn":"taskId","rightColumn":"id"},{"table":"memberTeams","leftAlias":"taskUsers","leftColumn":"teamId","rightColumn":"teamId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]}
        },"where":{"operator":"or","children":[{"operator":"inSet","column":"taskId","set":"workspaceTasks"},{"operator":"inSet","column":"taskId","set":"assigned"},{"operator":"inSet","column":"taskId","set":"teamAssigned"}]}});
        (tasks, notes)
    } else {
        (tasks, notes)
    };
    if !legacy {
        // These valid mask states used to fail during initial SQL compilation:
        // a role bypass leaves its sets inactive, and a derived set may be empty.
        tasks["sets"]["notedAssignments"] = json!({
            "kind":"derived","from":"assigned","table":"taskNotes",
            "key":"taskId","select":"taskId"
        });
        tasks["sets"]["projectSpaces"] = json!({
            "kind":"derived","from":"assigned","table":"projects",
            "key":"id","select":"workspaceId"
        });
        tasks["where"]["children"].as_array_mut().unwrap().extend([
            json!({"operator":"role","value":"admin"}),
            json!({"operator":"inSet","column":"id","set":"notedAssignments"}),
            json!({"operator":"inSet","column":"workspaceId","set":"projectSpaces"}),
        ]);
    }
    let mut links = notes.clone();
    links["table"] = json!("taskLinks");
    let mut functions = json!({});
    for (path, table, columns) in [
        ("tasks.list", "tasks", vec!["id", "workspaceId", "title"]),
        ("notes.list", "taskNotes", vec!["id", "taskId", "body"]),
        ("links.list", "taskLinks", vec!["id", "taskId", "label"]),
    ] {
        functions[path] = json!({"kind":"query","handler":"list","file":"test.js","delivery":"replica","args":{"kind":"object","fields":{}},"result":{"kind":"any"},"replica":{"table":table,"key":"id","columns":columns,"maxRows":50,"maxBytes":1048576}});
    }
    functions["tasks.withNotes"] = json!({
        "kind":"query","handler":"list","file":"test.js","delivery":"live",
        "args":{"kind":"object","fields":{}},"result":{"kind":"any"},
        "dependencies":{"liveQueryPlan":{
            "table":"tasks","key":"id","columns":["id"],"resultPath":["rows"],
            "where":{"operator":"inRelation","column":"id",
                "relation":{"table":"taskNotes","column":"taskId"}},
            "window":{"offsetArgument":"offset","limitArgument":"limit",
                "defaultLimit":50,"maxLimit":50,"count":"exact"}
        }}
    });
    if !legacy {
        functions["attachments.list"] = json!({"kind":"query","handler":"list","file":"test.js",
            "delivery":"replica","args":{"kind":"object","fields":{}},"result":{"kind":"any"},
            "replica":{"table":"attachments","key":"id","columns":["id","noteId","body"],
                "maxRows":50,"maxBytes":1048576}});
        functions["attachments.window"] = json!({"kind":"query","handler":"list","file":"test.js",
            "delivery":"live","args":{"kind":"object","fields":{}},"result":{"kind":"any"},
            "dependencies":{"liveQueryPlan":{"table":"attachments","key":"id","columns":["id"],
                "resultPath":["rows"],"window":{"offsetArgument":"offset","limitArgument":"limit",
                    "defaultLimit":50,"maxLimit":50,"count":"exact"}}}});
    }
    let code = "export function list() { return []; }";
    let javascript_hash = format!("{:x}", Sha256::digest(code));
    let mut module = json!({"generation":1,"language":"typescript","entrypoint":"test.js","files":{},"functions":functions,"visibility":{"tasks":tasks,"taskNotes":notes,"taskLinks":links},"crons":[],"invitationAcceptanceReducer":"","javascript":{"path":"test.js","hash":javascript_hash}});
    if !legacy {
        module["visibility"]["projects"] = json!({"version":2,"table":"projects","key":"id",
            "permissionTables":permissions,"sets":{},"where":{"operator":"public"}});
        module["visibility"]["attachments"] = json!({"version":2,"table":"attachments","key":"id",
            "permissionTables":permissions,"sets":{},"where":{"operator":"inherits",
                "column":"noteId","table":"taskNotes","key":"id"}});
    }
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

/// The fan-out budgets need the real module host. Outside the dedicated CI
/// step (GONVEX_VIS_FANOUT_REQUIRED=1) a missing binary skips the test.
fn module_host_binary() -> Option<String> {
    match std::env::var("GONVEX_TEST_MODULE_HOST_BINARY") {
        Ok(binary) => Some(binary),
        Err(_) if std::env::var("GONVEX_VIS_FANOUT_REQUIRED").as_deref() == Ok("1") => {
            panic!("GONVEX_VIS_FANOUT_REQUIRED is set but GONVEX_TEST_MODULE_HOST_BINARY is not")
        }
        Err(_) => {
            eprintln!("GONVEX_TEST_MODULE_HOST_BINARY absent; fan-out budgets not measured here");
            None
        }
    }
}

#[tokio::test]
async fn visibility_fanout_stays_within_statement_budgets() {
    run_visibility_fixture(false).await;
}

#[tokio::test]
async fn visibility_fanout_legacy_baseline() {
    run_visibility_fixture(true).await;
}

async fn run_visibility_fixture(legacy: bool) {
    let mode = if legacy { "legacy" } else { "v2" };
    let Some(binary) = module_host_binary() else {
        return;
    };
    let n: usize = std::env::var("GONVEX_VIS_FANOUT_MEMBERS")
        .unwrap_or_else(|_| "200".into())
        .parse()
        .unwrap();
    assert!(
        n >= 7,
        "fixture needs three grant recipients, two ancestor controls and a lagging member"
    );
    let directory = tempfile::tempdir().unwrap();
    // One SQLite file holds both the Control Plane and the tenant tables.
    let url = gonvex_store::database_url(&directory.path().join("fanout.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    write(&pools, &url, sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('fanout','Fanout','multiTenant','active')")).await;
    write(&pools, &url, sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('fanout','fanout','tenant','Tenant',$1,'active')").bind(&url)).await;
    {
        let mut transaction = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        gonvex_store::execute_script(&mut **transaction.transaction(), r#"
            CREATE TABLE tasks(id TEXT PRIMARY KEY, "workspaceId" TEXT, title TEXT);
            CREATE TABLE "taskNotes"(id TEXT PRIMARY KEY, "taskId" TEXT, body TEXT);
            CREATE TABLE projects(id TEXT PRIMARY KEY, "workspaceId" TEXT);
            CREATE TABLE attachments(id TEXT PRIMARY KEY, "noteId" TEXT, body TEXT);
            CREATE TABLE "taskLinks"(id TEXT PRIMARY KEY, "taskId" TEXT, label TEXT);
            CREATE TABLE "taskUsers"(id TEXT PRIMARY KEY, "taskId" TEXT, "userId" TEXT, "teamId" TEXT);
            CREATE TABLE "memberTeams"(id TEXT PRIMARY KEY, "memberId" TEXT, "teamId" TEXT);
            INSERT INTO tasks VALUES('hidden','private','Hidden'),('granted','private-grant','Granted');
            INSERT INTO "taskNotes" VALUES('hidden-note','hidden','Private note');
        "#)
        .await
        .unwrap();
        transaction.commit().await.unwrap();
    }
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
        write(&pools, &url, sqlx::query(
            "INSERT INTO members(id,account_id,status,display_name) VALUES($1,$1,'active',$1)",
        )
        .bind(format!("m{i}"))).await;
        write(&pools, &url, sqlx::query("INSERT INTO \"memberTeams\" VALUES($1,$1,'team')")
            .bind(format!("m{i}"))).await;
    }
    for i in 1..=3 {
        write(&pools, &url, sqlx::query("INSERT INTO \"memberTeams\" VALUES($1,$2,'grant-team')")
            .bind(format!("grant-team-{i}"))
            .bind(format!("m{i}"))).await;
    }
    let (manifest, hash) = fixture_manifest(legacy);
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
    let related = runtime
        .open_live_query(
            &connections[0].session,
            "related".into(),
            "tasks.withNotes".into(),
            json!({}),
        )
        .await
        .expect("a related mask must resolve its own sets and inherited parents");
    assert_eq!(
        related.subscription.last_result["rows"],
        json!([]),
        "hidden related rows must never make a task visible"
    );
    if !legacy {
        write(&pools, &url, sqlx::query("INSERT INTO members(id,account_id,status,display_name,role) VALUES('admin','admin','active','Admin','admin')")).await;
        let mut admin_session = connections[0].session.clone();
        admin_session.identity.account.id = "admin".into();
        admin_session.member.id = "admin".into();
        admin_session.member.account_id = "admin".into();
        admin_session.member.role = "admin".into();
        let request =
            serde_json::from_value(json!({"id":"admin","path":"tasks.list","args":{}})).unwrap();
        let opened = runtime
            .open_replica(&admin_session, request)
            .await
            .expect("role-pruned mask sets must not break admin admission");
        assert_eq!(opened.subscription.unwrap().rows.len(), 2);
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
        "INSERT INTO \"taskNotes\" VALUES('ordinary-note','hidden','Before')",
        "UPDATE \"taskNotes\" SET body='After' WHERE id='ordinary-note'",
        "DELETE FROM \"taskNotes\" WHERE id='ordinary-note'",
    ] {
        runtime.visibility_metrics().reset_for_test();
        write(&pools, &url, sqlx::query(statement)).await;
        let event = committed(&pools, &route, &mut revision).await;
        let worked = deliver(&runtime, &mut connections, &event).await;
        let counters = runtime.visibility_metrics().snapshot();
        if legacy {
            assert!(
                counters.visibility_sql_statements > 0,
                "legacy source dependency must issue visibility SQL"
            );
            assert!(
                counters.collection_snapshots > 0,
                "legacy source dependency must rebuild collections"
            );
            assert_eq!(
                worked
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                n,
                "legacy task writes must reproduce fan-out across every fixture member"
            );
        } else {
            assert_eq!(
                counters.visibility_sql_statements, 0,
                "ordinary task write ran visibility SQL"
            );
            assert_eq!(
                counters.collection_snapshots, 0,
                "ordinary task write rebuilt a collection"
            );
            assert_eq!(counters.principal_rebuilds, 0);
        }
        eprintln!("fanout {mode} task-write {statement:?}: {counters:?}");
    }
    if !legacy {
        assert!(
            runtime.visibility_metrics().snapshot().in_memory_row_checks > 0,
            "instrumentation must count production evaluator work"
        );
    }
    runtime.visibility_metrics().reset_for_test();
    write(&pools, &url, sqlx::query("INSERT INTO \"taskUsers\" VALUES('team-grant','granted',NULL,'grant-team')")).await;
    let event = committed(&pools, &route, &mut revision).await;
    let worked = deliver(&runtime, &mut connections, &event).await;
    if legacy {
        assert!(
            worked.contains(&0),
            "legacy grant dependencies must reach unnamed member m0"
        );
        assert!(runtime.visibility_metrics().snapshot().collection_snapshots > 0);
    } else {
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
    }
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
        "fanout {mode} team-grant: worked={worked:?} {:?}",
        runtime.visibility_metrics().snapshot()
    );
    if !legacy {
        assert!(
            statements <= named_connections * 2,
            "grant fetches exceeded one per named connection and collection: {statements}"
        );
    }
    runtime.visibility_metrics().reset_for_test();
    write(&pools, &url, sqlx::query("INSERT INTO \"memberTeams\" VALUES('extra','m1','extra')")).await;
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
        "fanout {mode} member-team change: worked={worked:?} {:?}",
        runtime.visibility_metrics().snapshot()
    );
    if legacy {
        assert_eq!(
            runtime.visibility_metrics().snapshot().collection_snapshots,
            member_one_collections,
            "legacy membership changes remain scoped to m1 here"
        );
    } else {
        assert!(
        (1..=member_one_collections).contains(&rebuilds),
        "permission change rebuilt {rebuilds} collections; member 1 holds {member_one_collections}"
    );
    }
    runtime.visibility_metrics().reset_for_test();
    write(&pools, &url, sqlx::query("INSERT INTO \"taskUsers\" VALUES('parent-grant','hidden','m0',NULL)")).await;
    let event = committed(&pools, &route, &mut revision).await;
    if legacy {
        let worked = deliver(&runtime, &mut connections, &event).await;
        assert!(
            worked.iter().any(|member| *member != 0),
            "legacy parent grant must issue work for unrelated members"
        );
    } else {
        // Capture through the process-global subscriber. Scoped per-future
        // dispatchers lose SQLx events once the runtime's background feed
        // tasks also run SQLx queries (tracing's cached callsite interest and
        // max level are global). Background feed SQL never names taskNotes.
        let statements = sql_capture();
        for connection in &mut connections {
            statements.lock().unwrap().clear();
            runtime
                .apply_feed_event(
                    &connection.session,
                    &mut connection.subscriptions,
                    event.clone(),
                )
                .await
                .unwrap();
            let child_fetches = statements
                .lock()
                .unwrap()
                .iter()
                .filter(|sql| {
                    sql.contains("taskNotes") && sql.to_ascii_uppercase().contains("SELECT")
                })
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
        "fanout {mode} parent-grant child fetch: {:?}",
        runtime.visibility_metrics().snapshot()
    );
    if legacy {
        assert!(runtime.visibility_metrics().snapshot().collection_snapshots > 0);
    } else {
        assert_eq!(
            runtime.visibility_metrics().snapshot().collection_snapshots,
            0,
            "inheritance transition rebuilt an entire collection"
        );
    }
    for connection in connections.iter().take(2) {
        let related = runtime
            .open_live_query(
                &connection.session,
                "related".into(),
                "tasks.withNotes".into(),
                json!({}),
            )
            .await
            .unwrap();
        assert_eq!(
            related.subscription.last_result["rows"],
            if connection.member == 0 {
                json!([{"id":"hidden"}])
            } else {
                json!([])
            },
            "related visibility must follow only this member's current parent grants"
        );
    }
    if !legacy {
        // Parent ACL index maintenance must not depend on who consumes the
        // feed. Only a Live Query connection sees this parent move; no Replica
        // connection receives it. Before the single-writer applier, only
        // Replica connections advanced the shared index, so it froze here and
        // the next applied event jumped the gap.
        let session = connections[0].session.clone();
        let mut lq_only = BTreeMap::new();
        lq_only.insert(
            "lq-only".to_owned(),
            runtime
                .open_live_query(
                    &session,
                    "lq-only".into(),
                    "tasks.withNotes".into(),
                    json!({}),
                )
                .await
                .unwrap()
                .subscription,
        );
        write(&pools, &url, sqlx::query("UPDATE tasks SET \"workspaceId\"='team' WHERE id='granted'")).await;
        let moved = committed(&pools, &route, &mut revision).await;
        runtime
            .apply_live_query_feed_event(&session, &mut lq_only, &moved)
            .await;
        // A brand-new collection after the gap, then a child of the moved
        // parent: decided in memory against the shared tasks index. taskLinks
        // is independent of the derived taskNotes set tested above.
        let request: ReplicaOpenRequest =
            serde_json::from_value(json!({"id":"late","path":"links.list","args":{}})).unwrap();
        let mut late = BTreeMap::new();
        late.insert(
            "late".to_owned(),
            runtime
                .open_replica(&session, request)
                .await
                .unwrap()
                .subscription
                .unwrap(),
        );
        write(&pools, &url, sqlx::query("INSERT INTO \"taskLinks\" VALUES('granted-link','granted','Now visible')")).await;
        let link = committed(&pools, &route, &mut revision).await;
        runtime.visibility_metrics().reset_for_test();
        runtime
            .apply_feed_event(&session, &mut late, link)
            .await
            .unwrap();
        assert!(
            late["late"].rows.contains_key("granted-link"),
            "a child of a parent moved while only a Live Query consumed the feed must be delivered"
        );
        assert_eq!(
            runtime.visibility_metrics().snapshot().collection_snapshots,
            0,
            "the child was decided in memory, not by a rebuild"
        );

        // Derived and grant revocations must cross both inheritance edges:
        // attachments -> taskNotes -> tasks -> projects.
        script(&pools, &url, r#"
            INSERT INTO tasks VALUES('chain-task','chain-private','Chain');
            INSERT INTO "taskNotes" VALUES('chain-note','chain-task','Chain note');
            INSERT INTO attachments VALUES('chain-attachment','chain-note','Attachment');
            INSERT INTO projects VALUES('chain-project','chain-private');
            INSERT INTO "taskUsers" VALUES('chain-grant','chain-project','m4',NULL);
        "#).await;
        let _ = committed(&pools, &route, &mut revision).await;
        let mut holders = Vec::new();
        for member in [4, 5] {
            let session = connections
                .iter()
                .find(|c| c.member == member)
                .unwrap()
                .session
                .clone();
            let mut replicas = BTreeMap::new();
            replicas.insert(
                "chain".to_owned(),
                runtime
                    .open_replica(
                        &session,
                        serde_json::from_value(
                            json!({"id":"chain","path":"attachments.list","args":{}}),
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .subscription
                    .unwrap(),
            );
            assert_eq!(
                replicas["chain"].rows.contains_key("chain-attachment"),
                member == 4
            );
            let mut queries = BTreeMap::new();
            queries.insert(
                "chain".to_owned(),
                runtime
                    .open_live_query(
                        &session,
                        "chain".into(),
                        "attachments.window".into(),
                        json!({}),
                    )
                    .await
                    .unwrap()
                    .subscription,
            );
            holders.push((member, session, replicas, queries));
        }
        for statement in [
            "UPDATE projects SET \"workspaceId\"='revoked' WHERE id='chain-project'",
            "UPDATE projects SET \"workspaceId\"='chain-private' WHERE id='chain-project'",
            "UPDATE tasks SET \"workspaceId\"='revoked' WHERE id='chain-task'",
            "UPDATE tasks SET \"workspaceId\"='chain-private' WHERE id='chain-task'",
            "DELETE FROM \"taskUsers\" WHERE id='chain-grant'",
        ] {
            write(&pools, &url, sqlx::query(statement)).await;
            let event = committed(&pools, &route, &mut revision).await;
            for (member, session, replicas, queries) in &mut holders {
                runtime.visibility_metrics().reset_for_test();
                runtime
                    .apply_feed_event(session, replicas, event.clone())
                    .await
                    .unwrap();
                runtime
                    .apply_live_query_feed_event(session, queries, &event)
                    .await;
                let visible = *member == 4 && statement.contains("='chain-private'");
                assert_eq!(
                    replicas["chain"].rows.contains_key("chain-attachment"),
                    visible,
                    "two-level inherited replica must follow {statement}"
                );
                assert_eq!(
                    queries["chain"].last_result["rows"],
                    if visible {
                        json!([{"id":"chain-attachment"}])
                    } else {
                        json!([])
                    },
                    "two-level inherited query must follow {statement}"
                );
                if *member == 5 {
                    assert_eq!(
                        runtime
                            .visibility_metrics()
                            .snapshot()
                            .visibility_sql_statements,
                        0,
                        "ancestor grant/derived changes must not wake unrelated members"
                    );
                }
            }
        }

        let member_session = holders[0].1.clone();
        let mut direct_parent = BTreeMap::new();
        direct_parent.insert(
            "direct".to_owned(),
            runtime
                .open_replica(
                    &member_session,
                    serde_json::from_value(json!({"id":"direct","path":"notes.list","args":{}}))
                        .unwrap(),
                )
                .await
                .unwrap()
                .subscription
                .unwrap(),
        );
        assert!(!direct_parent["direct"].rows.contains_key("chain-note"));
        write(&pools, &url, sqlx::query(
            "INSERT INTO \"taskUsers\" VALUES('chain-grant-again','chain-project','m4',NULL)",
        )).await;
        let grant_again = committed(&pools, &route, &mut revision).await;
        runtime
            .apply_feed_event(&member_session, &mut direct_parent, grant_again)
            .await
            .unwrap();
        assert!(
            direct_parent["direct"].rows.contains_key("chain-note"),
            "a direct-parent grant feeding a derived workspace must reveal existing notes"
        );

        // A lagging connection must never receive pre-share sensitive text
        // admitted by a parent index that has already reached the sharing write.
        script(&pools, &url, r#"
            INSERT INTO tasks VALUES('lag-parent','lag-private','Lagged');
            INSERT INTO "memberTeams" VALUES('lag-team','m6','lag-space');
        "#).await;
        let _ = committed(&pools, &route, &mut revision).await;
        let lag_session = connections
            .iter()
            .find(|c| c.member == 6)
            .unwrap()
            .session
            .clone();
        let mut lagged = BTreeMap::new();
        lagged.insert(
            "lagged".to_owned(),
            runtime
                .open_replica(
                    &lag_session,
                    serde_json::from_value(json!({"id":"lagged","path":"notes.list","args":{}}))
                        .unwrap(),
                )
                .await
                .unwrap()
                .subscription
                .unwrap(),
        );
        write(&pools, &url, sqlx::query(
            "INSERT INTO \"taskNotes\" VALUES('lag-note','lag-parent','must-never-be-delivered')",
        )).await;
        let old_note = committed(&pools, &route, &mut revision).await;
        write(&pools, &url, sqlx::query("UPDATE \"taskNotes\" SET body='Redacted before sharing' WHERE id='lag-note'")).await;
        let redaction = committed(&pools, &route, &mut revision).await;
        write(&pools, &url, sqlx::query("UPDATE tasks SET \"workspaceId\"='lag-space' WHERE id='lag-parent'")).await;
        let sharing = committed(&pools, &route, &mut revision).await;
        let clock = change_feed::read_clock(&pools, &route).await.unwrap();
        assert!(
            gonvex_runtime::visibility::indexes_ready(
                &lagged["lagged"].resolved,
                clock.revision,
                &clock.epoch
            )
            .await
        );
        let delivered = runtime
            .apply_feed_event(&lag_session, &mut lagged, old_note)
            .await
            .unwrap();
        assert_eq!(
            lagged["lagged"].rows["lag-note"]["body"],
            "Redacted before sharing"
        );

        assert!(
            !serde_json::to_string(&delivered)
                .unwrap()
                .contains("must-never-be-delivered"),
            "future ACL state must never expose an old event payload"
        );
        for event in [redaction, sharing] {
            runtime
                .apply_feed_event(&lag_session, &mut lagged, event)
                .await
                .unwrap();
        }
        assert_eq!(
            lagged["lagged"].rows["lag-note"]["body"],
            "Redacted before sharing"
        );
        // Completeness can advance on an unrelated commit without changing any
        // ACL rows. A held-row edit must stay entirely in memory in that case.
        write(&pools, &url, sqlx::query("UPDATE \"taskNotes\" SET body='Current edit' WHERE id='lag-note'")).await;
        let edit = committed(&pools, &route, &mut revision).await;
        write(&pools, &url, sqlx::query(
            "INSERT INTO attachments VALUES('unrelated-attachment','chain-note','Unrelated')",
        )).await;
        let _ = committed(&pools, &route, &mut revision).await;
        let clock = change_feed::read_clock(&pools, &route).await.unwrap();
        assert!(
            gonvex_runtime::visibility::indexes_ready(
                &lagged["lagged"].resolved,
                clock.revision,
                &clock.epoch
            )
            .await
        );
        runtime.visibility_metrics().reset_for_test();
        runtime
            .apply_feed_event(&lag_session, &mut lagged, edit)
            .await
            .unwrap();
        assert_eq!(lagged["lagged"].rows["lag-note"]["body"], "Current edit");
        let counters = runtime.visibility_metrics().snapshot();
        assert_eq!(
            counters.collection_snapshots, 0,
            "unrelated later commits must not force snapshots"
        );
        assert_eq!(
            counters.visibility_sql_statements, 0,
            "unrelated later commits must not cause SQL"
        );
        assert_eq!(counters.consistency_rebuilds, 0);
    }
    runtime.shutdown().await;
    pools.close().await;
}
