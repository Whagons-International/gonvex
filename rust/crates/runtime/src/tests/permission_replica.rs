//! OWN local fixture only; no runtime.start(), V8 host or network endpoint.
//! Feature-gated source/index/cold-snapshot regressions with the v2 oracle.
use super::config;
use crate::{
    modules::{FunctionDefinition, ProjectModule, ReplicaCollectionDefinition},
    permissions::{
        compile::{self, Expr},
        model::{Atom, PermissionScope},
        query::NormalizedBinding,
    },
    replica::permission::{projection_version, LocalSnapshotRegistration},
    Runtime,
};
use gonvex_protocol::ReplicaOpenRequest;
use gonvex_store::{permission_indexes as indexes, ControlPlane, TenantRoute, TenantSession};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

struct Environment(Option<std::ffi::OsString>);
impl Drop for Environment {
    fn drop(&mut self) {
        match self.0.take() {
            Some(value) => std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", value),
            None => std::env::remove_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL"),
        }
    }
}
fn atom(key: &str) -> Vec<u8> {
    Atom {
        namespace: "task".into(),
        resource: key.into(),
        principal: None,
    }
    .canonical()
    .unwrap()
}
fn row_facts(keys: &[&str]) -> indexes::SourceChange {
    indexes::SourceChange::Rows {
        source: "tasks".into(),
        facts: keys
            .iter()
            .map(|key| indexes::RowFact {
                universe: "tasks".into(),
                key: (*key).into(),
                atoms: vec![("read".into(), atom(key))],
            })
            .collect(),
    }
}
fn grant_facts(keys: &[&str]) -> indexes::SourceChange {
    indexes::SourceChange::Grants {
        source: "grants".into(),
        facts: keys
            .iter()
            .map(|key| indexes::GrantFact {
                target: indexes::Subject::Member("alice".into()),
                atom: atom(key),
            })
            .collect(),
    }
}
fn request(path: &str, workspace: Value) -> ReplicaOpenRequest {
    serde_json::from_value(json!({"id":"snapshot","path":path,"args":{"workspace":workspace}}))
        .unwrap()
}
async fn compare(
    runtime: &Runtime,
    session: &TenantSession,
    registration: &LocalSnapshotRegistration,
    request: ReplicaOpenRequest,
) -> crate::replica::permission::LocalSnapshotResult {
    let expected = runtime
        .open_replica_fresh(session, request.clone())
        .await
        .unwrap();
    let actual = runtime
        .open_local_permission_snapshot(registration, session, request)
        .await
        .unwrap();
    assert!(
        actual.open.subscription.is_none(),
        "must not install a live route"
    );
    assert_eq!(
        serde_json::to_value(&actual.open.messages).unwrap(),
        serde_json::to_value(&expected.messages).unwrap(),
        "projection/order/hash/window/metadata/messages differ from real v2 cold open"
    );
    assert_eq!(actual.window, expected.subscription.unwrap().window);
    actual
}

#[tokio::test]
async fn local_primitive_real_replica_snapshot_matches_v2_and_refills_boundary() {
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _environment = Environment(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let runtime = Runtime::new(config(false));
    runtime.set_snapshot_cache_enabled(false);
    runtime.set_principal_cache_enabled(false);
    let url = gonvex_store::database_url(&dir.path().join("tenant.db"));
    let control = ControlPlane::connect(
        &gonvex_store::database_url(&dir.path().join("control.db")),
        runtime.inner.pools.clone(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    *runtime.inner.control_plane.write().await = Some(control);
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON); INSERT INTO members VALUES('alice','account','active','member','{}'); CREATE TABLE fixtureGrants(memberId TEXT,taskId TEXT); CREATE TABLE tasks(id TEXT PRIMARY KEY NOT NULL,title TEXT,rank INTEGER,workspace TEXT,payload JSON,enabled BOOLEAN,private TEXT,deletedAt INTEGER); INSERT INTO tasks VALUES('r1','one',10,'A','{\"a\":1}',1,'secret',NULL),('r2','two',10,'A','null',0,'secret',NULL),('r3','hidden',9,'A','[]',1,'secret',NULL),('r4','filtered',7,'B','true',1,'secret',NULL),('r5','deleted',6,'A','1',1,'secret',1),('r6','null rank',NULL,'A',NULL,0,'secret',NULL),('r7','early',8,'A','\"scalar\"',1,'secret',NULL),('r8','later with enough text to cross a small canonical row byte budget',12,'A','[]',1,'secret',NULL)")
        .execute(&mut **tx.transaction()).await.unwrap();
    let instance = indexes::install_local(&mut tx).await.unwrap();
    let epoch: String =
        sqlx::query_scalar("SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1")
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap();
    let schema: String = sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE name='tasks'")
        .fetch_one(&mut **tx.transaction())
        .await
        .unwrap();
    let plan = serde_json::from_value(json!({"version":2,"table":"tasks","key":"id","permissionTables":["fixtureGrants"],
        "sets":{"read":{"kind":"principal","table":"fixtureGrants","select":"taskId","where":[{"column":"memberId","context":"member.id"}]}},
        "where":{"operator":"inSet","column":"id","set":"read"}})).unwrap();
    let mut module = ProjectModule {
        project_id: "local-replica".into(),
        generation: 1,
        artifact_hash: "owned-fixture".into(),
        client_contract: None,
        functions: BTreeMap::new(),
        manifest_functions: json!({}),
        replica_epochs: Arc::default(),
        schema: json!({}),
        visibility: BTreeMap::from([("tasks".into(), plan)]),
        permission_grants: None,
        invitation_acceptance_reducer: String::new(),
        migrations: vec![],
        crons: vec![],
    };
    let mut bindings = Vec::new();
    let all = ["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"];
    let admitted = ["r1", "r2", "r4", "r5", "r6", "r7", "r8"];
    for key in admitted {
        sqlx::query("INSERT INTO fixtureGrants VALUES('alice',?1)")
            .bind(key)
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
    }
    for (index, (direction, max_rows, max_bytes)) in [
        ("asc", 3, 0),
        ("desc", 2, 0),
        ("asc", 0, 180),
        ("asc", 0, 0),
    ]
    .into_iter()
    .enumerate()
    {
        let path = format!("tasks.local{index}");
        let definition = ReplicaCollectionDefinition {
            table: "tasks".into(),
            key: "id".into(),
            columns: ["id", "title", "rank", "payload", "enabled"]
                .map(str::to_owned)
                .to_vec(),
            equal_filters: BTreeMap::from([("workspace".into(), "workspace".into())]),
            exclude_when_set: vec!["deletedAt".into()],
            visibility_tables: vec!["fixtureGrants".into()],
            visibility_plan_hash: "fixture-v2".into(),
            order_by: "rank".into(),
            order_direction: direction.into(),
            mode: "window".into(),
            max_rows,
            max_bytes,
            retention_ms: 0,
        };
        let expression = Expr::SlotAny("read".into());
        let scope = PermissionScope {
            project: "local-replica".into(),
            database_instance: instance.clone(),
            tenant: "local-tenant".into(),
            epoch: epoch.clone(),
            table: "tasks".into(),
            collection: path.clone(),
            policy_version: compile::policy_version("tasks", &expression).unwrap(),
            projection_version: projection_version(&definition, &schema),
            semantics_version: "normalized-atoms-binary-v1".into(),
        };
        let scope_id = indexes::create_scope(
            &mut tx,
            &scope.canonical().unwrap(),
            &instance,
            &epoch,
            "tasks",
        )
        .await
        .unwrap();
        indexes::register_sources(
            &mut tx,
            scope_id,
            &[
                ("row".into(), "tasks".into()),
                ("grant".into(), "grants".into()),
            ],
        )
        .await
        .unwrap();
        indexes::bind_local_rows(&mut tx, scope_id).await.unwrap();
        indexes::apply(
            &mut tx,
            scope_id,
            &[row_facts(&all), grant_facts(&admitted)],
        )
        .await
        .unwrap();
        indexes::seal(&mut tx, scope_id).await.unwrap();
        bindings.push(NormalizedBinding {
            scope_id,
            required_generation: 1,
            authority: Vec::new(),
            scope,
            universe: "tasks".into(),
            expression,
        });
        module.functions.insert(
            path,
            FunctionDefinition {
                kind: "query".into(),
                internal: false,
                public: false,
                delivery: "replica".into(),
                action_profile: String::new(),
                action_capabilities: Value::Null,
                replica: Some(definition),
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
            },
        );
    }
    tx.commit().await.unwrap();
    runtime
        .inner
        .modules
        .install_permission_fixture(module.clone())
        .await;
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "local-replica".into(),
        tenant_id: "local-tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "account".into();
    session.member.id = "alice".into();
    session.member.role = "member".into();
    let mut registrations = Vec::new();
    for binding in &bindings {
        let registration = runtime
            .register_local_permission_snapshot(&session, binding.clone())
            .await
            .unwrap();
        let result = compare(
            &runtime,
            &session,
            &registration,
            request(&binding.scope.collection, json!("A")),
        )
        .await;
        assert_eq!(result.identity.principal, "alice");
        if binding.scope.collection.ends_with('0') {
            assert_eq!(result.window.rows, 3);
            assert_eq!(
                result.window.boundary,
                Some(Box::new((json!(10), json!("r1"))))
            );
        }
        // Preserve SQLite '= NULL' semantics; do not rewrite the filter to IS.
        let empty = compare(
            &runtime,
            &session,
            &registration,
            request(&binding.scope.collection, Value::Null),
        )
        .await;
        assert_eq!(empty.window.rows, 0);
        registrations.push(registration);
    }
    // Revoke two boundary rows and insert/grant another in ONE writer snapshot.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql("DELETE FROM fixtureGrants WHERE taskId IN ('r6','r7'); INSERT INTO fixtureGrants VALUES('alice','r9'); INSERT INTO tasks VALUES('r9','new',11,'A','false',0,'secret',NULL); UPDATE _gonvex_sync_clock SET revision=revision+1 WHERE singleton=1").execute(&mut **tx.transaction()).await.unwrap();
    let next = ["r1", "r2", "r4", "r5", "r8", "r9"];
    for binding in &bindings {
        indexes::apply(
            &mut tx,
            binding.scope_id,
            &[
                row_facts(&["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"]),
                grant_facts(&next),
            ],
        )
        .await
        .unwrap();
        indexes::seal(&mut tx, binding.scope_id).await.unwrap();
    }
    tx.commit().await.unwrap();
    let filled = compare(
        &runtime,
        &session,
        &registrations[0],
        request("tasks.local0", json!("A")),
    )
    .await;
    assert_eq!(filled.window.rows, 3);
    assert_eq!(
        filled.window.boundary,
        Some(Box::new((json!(11), json!("r9"))))
    );
    // A raw projection-only write with NO normalized replacement fails closed,
    // even if its writer bypasses change-feed revision advancement.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET title='unmaintained' WHERE id='r1'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(runtime
        .open_local_permission_snapshot(
            &registrations[0],
            &session,
            request("tasks.local0", json!("A"))
        )
        .await
        .is_err());
    // Rollback restores the previous readiness atomically.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    for binding in &bindings {
        indexes::apply(
            &mut tx,
            binding.scope_id,
            &[row_facts(&[
                "r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9",
            ])],
        )
        .await
        .unwrap();
        indexes::seal(&mut tx, binding.scope_id).await.unwrap();
    }
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("DELETE FROM tasks WHERE id='r2'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let maintained = compare(
        &runtime,
        &session,
        &registrations[0],
        request("tasks.local0", json!("A")),
    )
    .await;
    assert_eq!(
        maintained.identity.exact, filled.identity.exact,
        "content-only row maintenance must not grow/shrink canonical principal set"
    );
    // Primitive authority requires a persisted valid member. Blank/NUL/BLOB
    // member IDs are rejected rather than coerced or treated as an identity.
    for invalid in ["''", "char(0)", "X'01'"] {
        let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
            .await
            .unwrap();
        sqlx::query(&format!(
            "UPDATE members SET id={invalid} WHERE account_id='account'"
        ))
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(runtime
            .open_local_permission_snapshot(
                &registrations[0],
                &session,
                request("tasks.local0", json!("A"))
            )
            .await
            .is_err());
        let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
            .await
            .unwrap();
        sqlx::query("UPDATE members SET id='alice' WHERE account_id='account'")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let mut ephemeral = session.clone();
    ephemeral.developer_session = Some(json!({"fixture":true}));
    assert!(runtime
        .open_local_permission_snapshot(
            &registrations[0],
            &ephemeral,
            request("tasks.local0", json!("A"))
        )
        .await
        .is_err());
    let mut resume = request("tasks.local0", json!("A"));
    resume.cursor = Some(gonvex_protocol::ReplicaCursor { epoch, revision: 0 });
    assert!(runtime
        .open_local_permission_snapshot(&registrations[0], &session, resume)
        .await
        .is_err());
    // A new module needs a new trusted registration, even if keys still match.
    module.generation += 1;
    runtime
        .inner
        .modules
        .install_permission_fixture(module)
        .await;
    assert!(runtime
        .open_local_permission_snapshot(
            &registrations[0],
            &session,
            request("tasks.local0", json!("A"))
        )
        .await
        .is_err());
}

#[tokio::test]
async fn signed_relational_source_identity_grant_serves_real_sql_snapshot() {
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _environment = Environment(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let runtime = Runtime::new(config(false));
    runtime.set_snapshot_cache_enabled(false);
    runtime.set_principal_cache_enabled(false);
    let url = gonvex_store::database_url(&dir.path().join("source.db"));
    let control = ControlPlane::connect(
        &gonvex_store::database_url(&dir.path().join("control.db")),
        runtime.inner.pools.clone(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    *runtime.inner.control_plane.write().await = Some(control);
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON); INSERT INTO members VALUES('alice','account','active','member','{}'),('bob','other-account','active','member','{}'); CREATE TABLE owned(_id TEXT PRIMARY KEY,owner TEXT,title TEXT,rank INTEGER,payload JSON,enabled BOOLEAN); INSERT INTO owned VALUES('a','alice','first',1,'null',1),('b',NULL,'null owner',2,NULL,0),('c','','blank',3,'{}',1),('d',' ','space',4,'[]',0),('e','ALICE','case',5,'true',1),('f','bob','other',6,'1',0),('g','alice','last',7,'\"text\"',1)")
        .execute(&mut **tx.transaction()).await.unwrap();
    tx.commit().await.unwrap();
    let definition:ReplicaCollectionDefinition=serde_json::from_value(json!({"table":"owned","key":"_id","columns":["_id","title","rank","payload","enabled"],"orderBy":"rank","orderDirection":"asc","mode":"window","maxRows":2,"maxBytes":400,"retentionMs":0})).unwrap();
    let plan=serde_json::from_value(json!({"version":2,"table":"owned","key":"_id","sets":{},"where":{"operator":"eqContext","column":"owner","context":"member.id"}})).unwrap();
    let module = ProjectModule {
        project_id: "source-fixture".into(),
        generation: 1,
        artifact_hash: "signed-source-fixture".into(),
        client_contract: None,
        functions: BTreeMap::from([(
            "owned.list".into(),
            FunctionDefinition {
                kind: "query".into(),
                internal: false,
                public: false,
                delivery: "replica".into(),
                action_profile: String::new(),
                action_capabilities: Value::Null,
                replica: Some(definition),
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
            },
        )]),
        manifest_functions: json!({}),
        replica_epochs: Arc::default(),
        schema: json!({}),
        visibility: BTreeMap::from([("owned".into(), plan)]),
        permission_grants: None,
        invitation_acceptance_reducer: String::new(),
        migrations: vec![],
        crons: vec![],
    };
    let manifest: Value =
        serde_json::from_str(include_str!("../../testdata/permission-source-text.json")).unwrap();
    runtime
        .inner
        .modules
        .install_permission_fixture_manifest(module.clone(), manifest.clone())
        .await;
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "source-fixture".into(),
        tenant_id: "source-tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "account".into();
    session.member.id = "alice".into();
    session.member.role = "member".into();
    let registration = runtime
        .install_local_source_permission_snapshot(&session, "owned.list")
        .await
        .unwrap();
    let initial = compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    assert_eq!(initial.window.rows, 2);
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    // Content insert/delete and owner retarget affect rows but NOT identity atoms.
    sqlx::raw_sql("INSERT INTO owned VALUES('h','alice','new',0,'{}',0); DELETE FROM owned WHERE _id='a'; UPDATE owned SET owner='bob' WHERE _id='g'; UPDATE _gonvex_sync_clock SET revision=revision+1 WHERE singleton=1").execute(&mut **tx.transaction()).await.unwrap();
    tx.commit().await.unwrap();
    assert!(runtime
        .open_local_permission_snapshot(&registration, &session, request("owned.list", Value::Null))
        .await
        .is_err());
    runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .unwrap();
    let changed = compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    assert_eq!(changed.window.rows, 1);
    assert_eq!(
        initial.identity.exact, changed.identity.exact,
        "content writes must not change canonical principal identity atoms"
    );
    // A failed typed source rebuild is atomic, and remains unready until fixed.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET owner=X'01' WHERE _id='h'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .is_err());
    assert!(runtime
        .open_local_permission_snapshot(&registration, &session, request("owned.list", Value::Null))
        .await
        .is_err());
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET owner='alice' WHERE _id='h'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .unwrap();
    // isNull is supported; onNull=reject and nullable key must not coerce/omit.
    let bound = crate::permissions::source::Manifest::parse(&manifest)
        .unwrap()
        .bind("owned.list")
        .unwrap();
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET _id=NULL WHERE _id='h'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    assert!(bound.sources[1].load(&mut tx).await.is_err());
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status=NULL WHERE id='alice'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    assert!(bound.sources[0].load(&mut tx).await.is_err());
    tx.rollback().await.unwrap();
    compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    // Membership removal invalidates identity grants and independently rejects
    // persisted admission. No ephemeral developer fallback is claimed.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='disabled' WHERE id='alice'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(runtime
        .open_local_permission_snapshot(&registration, &session, request("owned.list", Value::Null))
        .await
        .is_err());
    runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .unwrap();
    assert!(runtime
        .open_replica_fresh(&session, request("owned.list", Value::Null))
        .await
        .is_err());
    assert!(runtime
        .open_local_permission_snapshot(&registration, &session, request("owned.list", Value::Null))
        .await
        .is_err());
    // Signed source/provider/dependency authority cannot be substituted beneath
    // the same local module generation/artifact, even in a synthetic fixture.
    let mut changed_manifest = manifest.clone();
    changed_manifest["declaration"]["providers"]["self"]["version"] = json!("changed");
    runtime
        .inner
        .modules
        .install_permission_fixture_manifest(module, changed_manifest)
        .await;
    assert!(runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .is_err());
}

#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "The process environment must remain exclusively owned across this whole isolated test, including awaits; the existing fixture tests share this lock."
)]
async fn signed_group_sources_match_v2_overlaps_negatives_and_transaction_changes() {
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _environment = Environment(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let runtime = Runtime::new(config(false));
    runtime.set_snapshot_cache_enabled(false);
    runtime.set_principal_cache_enabled(false);
    let url = gonvex_store::database_url(&dir.path().join("source.db"));
    let control = ControlPlane::connect(
        &gonvex_store::database_url(&dir.path().join("control.db")),
        runtime.inner.pools.clone(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    *runtime.inner.control_plane.write().await = Some(control);
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON); INSERT INTO members VALUES('alice','account','active','member','{}'),('bob','other-account','active','member','{}'); CREATE TABLE owned(_id TEXT PRIMARY KEY,title TEXT,rank INTEGER,payload JSON,enabled BOOLEAN); INSERT INTO owned VALUES('a','first',1,'null',1),('b','second',2,'{}',0),('c','third',3,'[]',1); CREATE TABLE directGrants(_id TEXT PRIMARY KEY,memberId TEXT,taskId TEXT,deletedAt INTEGER); INSERT INTO directGrants VALUES('direct-a','alice','a',NULL),('alternate-a','alice','a',NULL); CREATE TABLE teamGrants(_id TEXT PRIMARY KEY,teamId TEXT,taskId TEXT,deletedAt INTEGER); INSERT INTO teamGrants VALUES('blue-b','blue','b',NULL),('red-b','red','b',NULL),('blue-c','blue','c',NULL); CREATE TABLE memberTeams(_id TEXT PRIMARY KEY,memberId TEXT,teamId TEXT,deletedAt INTEGER); INSERT INTO memberTeams VALUES('blue-alice','alice','blue',NULL),('red-alice','alice','red',NULL); CREATE TABLE otherTeams(_id TEXT PRIMARY KEY,memberId TEXT,teamId TEXT,deletedAt INTEGER); INSERT INTO otherTeams VALUES('other-red-bob','bob','red',NULL); CREATE TABLE denyGrants(_id TEXT PRIMARY KEY,memberId TEXT,taskId TEXT,deletedAt INTEGER)")
        .execute(&mut **tx.transaction()).await.unwrap();
    tx.commit().await.unwrap();
    let definition:ReplicaCollectionDefinition=serde_json::from_value(json!({"table":"owned","key":"_id","columns":["_id","title","rank","payload","enabled"],"orderBy":"rank","orderDirection":"asc","mode":"window","maxRows":2,"maxBytes":400,"retentionMs":0})).unwrap();
    let plan=serde_json::from_value(json!({"version":2,"table":"owned","key":"_id","permissionTables":["directGrants","teamGrants","memberTeams","denyGrants"],"sets":{
        "direct":{"kind":"grant","table":"directGrants","select":"taskId","joins":[],"where":[{"table":"directGrants","column":"memberId","context":"member.id"},{"table":"directGrants","column":"deletedAt","value":{"literal":null}}]},
        "teams":{"kind":"grant","table":"teamGrants","select":"taskId","joins":[{"table":"memberTeams","leftColumn":"teamId","rightColumn":"teamId"}],"where":[{"table":"memberTeams","column":"memberId","context":"member.id"},{"table":"memberTeams","column":"deletedAt","value":{"literal":null}},{"table":"teamGrants","column":"deletedAt","value":{"literal":null}}]},
        "removed":{"kind":"grant","table":"denyGrants","select":"taskId","joins":[],"where":[{"table":"denyGrants","column":"memberId","context":"member.id"},{"table":"denyGrants","column":"deletedAt","value":{"literal":null}}]}
    },"where":{"operator":"and","children":[{"operator":"or","children":[{"operator":"inSet","column":"_id","set":"direct"},{"operator":"inSet","column":"_id","set":"teams"}]},{"operator":"not","children":[{"operator":"inSet","column":"_id","set":"removed"}]}]}})).unwrap();
    let module = ProjectModule {
        project_id: "source-fixture".into(),
        generation: 1,
        artifact_hash: "signed-source-fixture".into(),
        client_contract: None,
        functions: BTreeMap::from([(
            "owned.list".into(),
            FunctionDefinition {
                kind: "query".into(),
                internal: false,
                public: false,
                delivery: "replica".into(),
                action_profile: String::new(),
                action_capabilities: Value::Null,
                replica: Some(definition),
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
            },
        )]),
        manifest_functions: json!({}),
        replica_epochs: Arc::default(),
        schema: json!({}),
        visibility: BTreeMap::from([("owned".into(), plan)]),
        permission_grants: None,
        invitation_acceptance_reducer: String::new(),
        migrations: vec![],
        crons: vec![],
    };
    let manifest: Value =
        serde_json::from_str(include_str!("../../testdata/permission-source-groups.json")).unwrap();
    runtime
        .inner
        .modules
        .install_permission_fixture_manifest(module.clone(), manifest.clone())
        .await;
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "source-fixture".into(),
        tenant_id: "source-tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "account".into();
    session.member.id = "alice".into();
    session.member.role = "member".into();
    let registration = runtime
        .install_local_source_permission_snapshot(&session, "owned.list")
        .await
        .unwrap();
    let initial = compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    assert_eq!(initial.window.rows, 2);
    // A namespaced membership must never grant atoms from another namespace.
    let mut bob = session.clone();
    bob.identity.account.id = "other-account".into();
    bob.member.id = "bob".into();
    assert_eq!(
        compare(
            &runtime,
            &bob,
            &registration,
            request("owned.list", Value::Null)
        )
        .await
        .window
        .rows,
        0
    );
    for (statement, rows) in [
        ("UPDATE directGrants SET deletedAt=1 WHERE _id='direct-a'",2),
        ("DELETE FROM directGrants WHERE _id='alternate-a'",2),
        ("UPDATE memberTeams SET deletedAt=2 WHERE _id='blue-alice'",1),
        ("UPDATE teamGrants SET deletedAt=3 WHERE _id='red-b'",0),
        ("UPDATE teamGrants SET deletedAt=NULL WHERE _id='red-b'",1),
        // A negative flip adds no new positive atom; it must still remove b.
        ("INSERT INTO denyGrants VALUES('remove-b','alice','b',NULL)",0),
        ("DELETE FROM denyGrants",1),
        // Row and grant retarget in the same TX; old/NEW source closure matters.
        ("UPDATE owned SET _id='new-b' WHERE _id='b'; UPDATE teamGrants SET taskId='new-b' WHERE _id='red-b'",1),
    ] {
        let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false).await.unwrap();
        sqlx::raw_sql(statement).execute(&mut **tx.transaction()).await.unwrap();
        tx.commit().await.unwrap();
        assert!(runtime.open_local_permission_snapshot(&registration,&session,request("owned.list",Value::Null)).await.is_err(),"mutation must invalidate old authority before disclosure");
        runtime.rebuild_local_source_permission_snapshot(&session,&registration).await.unwrap();
        assert_eq!(compare(&runtime,&session,&registration,request("owned.list",Value::Null)).await.window.rows,rows);
    }
    let before = compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql("INSERT INTO owned VALUES('ungranted','new row',4,'null',0)")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .unwrap();
    let after = compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    assert_eq!(
        before.identity.exact, after.identity.exact,
        "row-only mutations cannot change principal atom set"
    );
    // Rollback preserves both source rows and ready index authority.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql("DELETE FROM memberTeams; DELETE FROM teamGrants")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    compare(
        &runtime,
        &session,
        &registration,
        request("owned.list", Value::Null),
    )
    .await;
    // A non-TEXT projected member is rejected; failure remains unready/atomic.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE memberTeams SET memberId=X'01' WHERE _id='red-alice'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(runtime
        .rebuild_local_source_permission_snapshot(&session, &registration)
        .await
        .is_err());
    assert!(runtime
        .open_local_permission_snapshot(&registration, &session, request("owned.list", Value::Null))
        .await
        .is_err());
}
