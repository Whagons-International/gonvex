//! OWN local fixture only; no runtime.start(), V8 host or network endpoint.
//! Source-prepared Stage 3 regression. Not yet compiled/executed.
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
#[tokio::test]
async fn starter_backend_streams_real_module_rows_and_revokes() {
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
    let cp_pool = control.pools().pool(control.database_url()).await.unwrap();
    sqlx::raw_sql("INSERT INTO gonvex_runtime_projects(id,name,database_mode) VALUES('local-replica','synthetic','multi'); INSERT INTO accounts(id,auth_realm_id) VALUES('account','local-replica');").execute(&cp_pool).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,provisioned) VALUES('rel','local-replica','local-tenant','synthetic',?1,1)").bind(&url).execute(&cp_pool).await.unwrap();
    *runtime.inner.control_plane.write().await = Some(control);
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON,display_name TEXT DEFAULT '',avatar_url TEXT DEFAULT '',membership_revision INTEGER DEFAULT 1); INSERT INTO members(id,account_id,status,role,permissions) VALUES('alice','account','active','member','{}'); CREATE TABLE fixtureGrants(memberId TEXT,taskId TEXT); CREATE TABLE tasks(id TEXT PRIMARY KEY NOT NULL,title TEXT,rank INTEGER,workspace TEXT,payload JSON,enabled BOOLEAN,private TEXT,deletedAt INTEGER); INSERT INTO tasks VALUES('r1','one',10,'A','{\"a\":1}',1,'secret',NULL),('r2','two',10,'A','null',0,'secret',NULL),('r3','hidden',9,'A','[]',1,'secret',NULL),('r4','filtered',7,'B','true',1,'secret',NULL),('r5','deleted',6,'A','1',1,'secret',1),('r6','null rank',NULL,'A',NULL,0,'secret',NULL),('r7','early',8,'A','\"scalar\"',1,'secret',NULL),('r8','later with enough text to cross a small canonical row byte budget',12,'A','[]',1,'secret',NULL)")
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
    for (index, (direction, max_rows, max_bytes)) in [("asc", 0, 0)].into_iter().enumerate() {
        let path = format!("tasks.local{index}");
        let definition = ReplicaCollectionDefinition {
            table: "tasks".into(),
            key: "id".into(),
            columns: ["id", "title", "rank", "payload", "enabled"]
                .map(str::to_owned)
                .to_vec(),
            equal_filters: BTreeMap::new(),
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

    use crate::replica::permission::starter_adapter::CollectionUse;
    use crate::starter_files::{Limits, Store};
    let registration = Arc::new(
        runtime
            .register_local_permission_snapshot(&session, bindings[0].clone())
            .await
            .unwrap(),
    );
    let artifact_dir = tempfile::tempdir_in("/mnt/data/work/gonvex-starter-files").unwrap();
    let store = Store::open(artifact_dir.path(), Limits::default()).unwrap();
    let handle = runtime
        .spawn_local_starter_after_miss(
            registration.clone(),
            session.clone(),
            store.clone(),
            CollectionUse::SharedBase,
        )
        .unwrap();
    let manifest = handle.await.unwrap().unwrap();
    assert_eq!(manifest.rows, 6);
    let mut ids = std::collections::BTreeSet::new();
    runtime
        .read_local_starter(
            &registration,
            &session,
            &store,
            |p, b| {
                let raw = if p.compressed {
                    let mut framed = b.to_vec();
                    framed.extend_from_slice(&[0, 0, 255, 255]);
                    let mut out = vec![0; p.raw_bytes as usize + 1];
                    let mut decoder = flate2::Decompress::new(false);
                    decoder
                        .decompress(&framed, &mut out, flate2::FlushDecompress::Sync)
                        .unwrap();
                    out.truncate(decoder.total_out() as usize);
                    out
                } else {
                    b.to_vec()
                };
                for line in raw.split(|b| *b == b'\n').filter(|s| !s.is_empty()) {
                    let row: Value = serde_json::from_slice(line)?;
                    ids.insert(row["id"].as_str().unwrap().to_string());
                    assert!(row.get("private").is_none());
                }
                Ok(())
            },
            CollectionUse::SharedBase,
        )
        .await
        .unwrap();
    assert_eq!(
        ids,
        ["r1", "r2", "r4", "r6", "r7", "r8"]
            .map(str::to_owned)
            .into_iter()
            .collect()
    );
    assert!(runtime
        .read_local_starter(
            &registration,
            &session,
            &store,
            |_, _| panic!("member list"),
            CollectionUse::PerUserList
        )
        .await
        .is_err());
    // Same user atom set, but a row moves out of its authorized scope. Old
    // manifest and direct old content-hash requests must disclose zero bytes.
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    let before = crate::permissions::query::prepare_projection(
        &mut tx,
        &session,
        &bindings[0],
        false,
        &bindings[0].scope.projection_version,
    )
    .await
    .unwrap()
    .identity;
    let mut moved = match row_facts(&all) {
        indexes::SourceChange::Rows { facts, .. } => facts,
        _ => unreachable!(),
    };
    moved.iter_mut().find(|r| r.key == "r1").unwrap().atoms = vec![("read".into(), atom("r3"))];
    sqlx::query("UPDATE tasks SET workspace='B' WHERE id='r1'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    indexes::apply(
        &mut tx,
        bindings[0].scope_id,
        &[indexes::SourceChange::Rows {
            source: "tasks".into(),
            facts: moved,
        }],
    )
    .await
    .unwrap();
    indexes::seal(&mut tx, bindings[0].scope_id).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, true)
        .await
        .unwrap();
    let after = crate::permissions::query::prepare_projection(
        &mut tx,
        &session,
        &bindings[0],
        false,
        &bindings[0].scope.projection_version,
    )
    .await
    .unwrap()
    .identity;
    tx.commit().await.unwrap();
    assert_eq!(before.exact, after.exact);
    assert_ne!(before.change_serial, after.change_serial);
    assert!(runtime
        .read_local_starter(
            &registration,
            &session,
            &store,
            |_, _| panic!("old rows disclosed"),
            CollectionUse::SharedBase
        )
        .await
        .is_err());
    assert!(runtime
        .read_local_starter_piece(
            &registration,
            &session,
            &store,
            &manifest.pieces[0].hash,
            |_, _| panic!("raw hash bypass"),
            CollectionUse::SharedBase
        )
        .await
        .is_err());
    let rebuilt = runtime
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
    assert_eq!(rebuilt.rows, 5);
    assert!(runtime
        .read_local_starter_piece(
            &registration,
            &session,
            &store,
            &manifest.pieces[0].hash,
            |_, _| panic!("foreign manifest piece"),
            CollectionUse::SharedBase
        )
        .await
        .is_err());
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='inactive' WHERE id='alice'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(runtime
        .read_local_starter(
            &registration,
            &session,
            &store,
            |_, _| panic!("revoked member"),
            CollectionUse::SharedBase
        )
        .await
        .is_err());
    let mut tx = gonvex_store::begin_transaction(&runtime.inner.pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='active' WHERE id='alice'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("UPDATE accounts SET disabled_at=1 WHERE id='account'")
        .execute(&cp_pool)
        .await
        .unwrap();
    assert!(runtime
        .read_local_starter(
            &registration,
            &session,
            &store,
            |_, _| panic!("disabled account"),
            CollectionUse::SharedBase
        )
        .await
        .is_err());
}
