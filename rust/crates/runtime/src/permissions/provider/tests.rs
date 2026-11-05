use super::*;
use crate::modules::{FunctionDefinition, ModuleRegistry, ProjectModule};
use gonvex_store::{PoolLimits, PoolRegistry, TenantRoute};
use sqlx::Row;
use std::sync::Arc;
mod actual_definitions;
struct Env(Option<std::ffi::OsString>);
impl Drop for Env {
    fn drop(&mut self) {
        match self.0.take() {
            Some(v) => std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", v),
            None => std::env::remove_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL"),
        }
    }
}
fn module(predicate: Value) -> ProjectModule {
    let definition:ReplicaCollectionDefinition=serde_json::from_value(json!({"table":"owned","key":"_id","columns":["_id","owner","body","flag","payload"],"orderBy":"_id","orderDirection":"asc","maxRows":20,"maxBytes":4096,"mode":"eager"})).unwrap();
    ProjectModule {
        project_id: "fixture-project".into(),
        generation: 1,
        artifact_hash: "fixture-artifact".into(),
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
                args_schema: json!({"kind":"object","fields":{}}),
                result_schema: json!({"kind":"any"}),
                profile_slot: 0,
            },
        )]),
        manifest_functions: json!({}),
        replica_epochs: Arc::default(),
        schema: json!({}),
        visibility: BTreeMap::from([(
            "owned".into(),
            serde_json::from_value(
                json!({"version":2,"table":"owned","key":"_id","sets":{},"where":predicate}),
            )
            .unwrap(),
        )]),
        permission_grants: None,
        invitation_acceptance_reducer: String::new(),
        migrations: vec![],
        crons: vec![],
    }
}
async fn keys(
    reg: &CompiledRegistration,
    lease: &ModuleCallLease,
    tx: &mut TenantTransaction,
    session: &TenantSession,
) -> Vec<String> {
    let Fresh::Indexed(mut scan) = reg.fresh(lease, tx, session, &json!({})).await.unwrap() else {
        panic!("expected eligible indexed source")
    };
    let (tx, _, prepared, _) = scan.parts();
    let sql = format!(
        "SELECT row_key FROM ({}) ORDER BY row_key",
        prepared.compiled.sql
    );
    super::super::query::bind(prepared, &sql)
        .fetch_all(&mut **tx.transaction())
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}
async fn v2_keys(
    lease: &ModuleCallLease,
    tx: &mut TenantTransaction,
    session: &TenantSession,
) -> Vec<String> {
    let p = &lease.visibility["owned"];
    let resolved = crate::visibility::resolve(tx, session, p, 0).await.unwrap();
    let mut values = Vec::new();
    let predicate = crate::visibility::compile_predicate(p, &resolved, "r", &mut values).unwrap();
    let sql = format!("SELECT _id FROM owned r WHERE {predicate} ORDER BY _id");
    let sql_rows = crate::sql_parameters::Parameters::scalars(&values)
        .unwrap()
        .bind(sqlx::query(&sql))
        .fetch_all(&mut **tx.transaction())
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect::<Vec<String>>();
    let interpreted =
        sqlx::query("SELECT json_object('_id',_id,'owner',owner) FROM owned ORDER BY _id")
            .fetch_all(&mut **tx.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|r| serde_json::from_str::<Value>(&r.get::<String, _>(0)).unwrap())
            .filter(|r| crate::visibility::row_matches(p, &resolved, r))
            .map(|r| r["_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
    assert_eq!(sql_rows, interpreted);
    sql_rows
}
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "Own the process-local fixture opt-in across all awaits; shared with existing fixture tests."
)]
async fn complete_certificate_maintains_source_rows_before_commit_and_falls_back_when_dirty_or_ineligible(
) {
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Env(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let url = gonvex_store::database_url(&dir.path().join("provider.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON); INSERT INTO members VALUES('alice','a','active','member','{}'),('bob','b','active','member','{}'),('guest','g','active','guest','{}'); CREATE TABLE owned(_id TEXT PRIMARY KEY,owner TEXT,body TEXT,flag BOOLEAN,payload JSON); INSERT INTO owned VALUES('one','alice','first',1,'null'),('two','bob','second',0,'{}'),('three',NULL,'null owner',NULL,NULL); CREATE TABLE unrelated(id TEXT PRIMARY KEY)").execute(&mut **tx.transaction()).await.unwrap();
    indexes::install_local(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let registry = ModuleRegistry::new();
    registry
        .install_permission_fixture(module(
            json!({"operator":"eqContext","column":"owner","context":"member.id"}),
        ))
        .await;
    let lease = registry.project_for_call("fixture-project").await.unwrap();
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "fixture-project".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "a".into();
    session.member.id = "alice".into();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let Compilation::Complete(cert) =
        CompleteProviderCertificate::compile(&lease, &mut tx, &session, "owned.list")
            .await
            .unwrap()
    else {
        panic!("source not compiled")
    };
    let reg = cert.install(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(
        keys(&reg, &lease, &mut tx, &session).await,
        v2_keys(&lease, &mut tx, &session).await
    );
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET owner='bob' WHERE _id='one'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    assert!(matches!(
        reg.fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap(),
        Fresh::NormalV2(Fallback::DomainOrIndexNotReady)
    ));
    assert!(v2_keys(&lease, &mut tx, &session).await.is_empty());
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(keys(&reg, &lease, &mut tx, &session).await, vec!["one"]);
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET owner='bob' WHERE _id='one'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(
        keys(&reg, &lease, &mut tx, &session).await,
        v2_keys(&lease, &mut tx, &session).await
    );
    assert!(keys(&reg, &lease, &mut tx, &session).await.is_empty());
    let state = indexes::state(tx.transaction(), reg.scope_id())
        .await
        .unwrap();
    let delta = indexes::decision_diff::current(
        tx.transaction(),
        reg.scope_id(),
        state.generation,
        state.covered_revision,
        "alice",
    )
    .await
    .unwrap();
    assert_eq!(
        delta,
        indexes::decision_diff::Outcome::Exact {
            deltas: vec![indexes::decision_diff::Delta::Removed("one".into())],
            candidates: 1
        }
    );
    let delta = indexes::decision_diff::current(
        tx.transaction(),
        reg.scope_id(),
        state.generation,
        state.covered_revision,
        "bob",
    )
    .await
    .unwrap();
    assert_eq!(
        delta,
        indexes::decision_diff::Outcome::Exact {
            deltas: vec![indexes::decision_diff::Delta::Added("one".into())],
            candidates: 1
        }
    );
    tx.commit().await.unwrap();
    // Noncanonical values commit through the existing v2 writer. They make the
    // new path ineligible, not a new authorization denial or silent coercion.
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET owner=X'01' WHERE _id='one'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(matches!(
        reg.fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap(),
        Fresh::NormalV2(Fallback::DomainOrIndexNotReady)
    ));
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE owned SET owner='alice' WHERE _id='one'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(keys(&reg, &lease, &mut tx, &session).await, vec!["one"]);
    tx.commit().await.unwrap();
    // A declaration/hash reused with changed actual projection is insufficient.
    drop(lease);
    let mut changed =
        module(json!({"operator":"eqContext","column":"owner","context":"member.id"}));
    changed
        .functions
        .get_mut("owned.list")
        .unwrap()
        .replica
        .as_mut()
        .unwrap()
        .max_rows = 1;
    registry.install_permission_fixture(changed).await;
    let lease = registry.project_for_call("fixture-project").await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(matches!(
        reg.fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap(),
        Fresh::NormalV2(Fallback::ModuleOrProjectionChanged)
    ));
    tx.commit().await.unwrap();
    drop(lease);
    let mut changed =
        module(json!({"operator":"eqContext","column":"owner","context":"member.id"}));
    changed.schema = json!({"dialect":"sqlite","version":2,"fingerprint":"changed-actual-schema-under-same-module-labels"});
    registry.install_permission_fixture(changed).await;
    let lease = registry.project_for_call("fixture-project").await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(matches!(
        reg.fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap(),
        Fresh::NormalV2(Fallback::ModuleOrProjectionChanged)
    ));
    tx.commit().await.unwrap();
}

#[tokio::test]
#[expect(clippy::await_holding_lock, reason = "process-local fixture opt-in")]
async fn permission_domains_preserve_existing_sql_and_interpreted_disagreement_and_normal_fallback()
{
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Env(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let url = gonvex_store::database_url(&dir.path().join("truth-domain.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON); INSERT INTO members VALUES('alice','a','active','member','{\"read\":true}'); CREATE TABLE owned(_id TEXT PRIMARY KEY,owner TEXT,body TEXT,flag BOOLEAN,payload JSON); INSERT INTO owned VALUES('one','alice','first',1,'null')").execute(&mut **tx.transaction()).await.unwrap();
    indexes::install_local(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let registry = ModuleRegistry::new();
    registry
        .install_permission_fixture(module(json!({"operator":"permission","value":"read"})))
        .await;
    let lease = registry.project_for_call("fixture-project").await.unwrap();
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "fixture-project".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "a".into();
    session.member.id = "alice".into();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let Compilation::Complete(cert) =
        CompleteProviderCertificate::compile(&lease, &mut tx, &session, "owned.list")
            .await
            .unwrap()
    else {
        panic!("permission did not compile")
    };
    let reg = cert.install(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    // SQL truthiness, interpreted truthiness, new-provider domain readiness.
    let cases = [
        ("2", false, true, false),
        ("\"TRUE\"", true, false, false),
        ("1.0", false, true, false),
        ("true", true, true, true),
        ("false", false, false, true),
        ("1", true, true, true),
        ("0", false, false, true),
        ("\"true\"", true, true, true),
        ("\"1\"", true, true, true),
        ("\"false\"", false, false, true),
        ("null", false, false, true),
        ("{}", false, false, true),
    ];
    for (raw, sql_expected, memory_expected, eligible) in cases {
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::query("UPDATE members SET permissions=?1 WHERE id='alice'")
            .bind(format!("{{\"read\":{raw},\"unrelated\":2}}"))
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert!(
            matches!(
                reg.fresh(&lease, &mut tx, &session, &json!({}))
                    .await
                    .unwrap(),
                Fresh::NormalV2(Fallback::DomainOrIndexNotReady)
            ),
            "writer must not read old ready index {raw}"
        );
        tx.commit().await.unwrap();
        let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        let plan = &lease.visibility["owned"];
        let resolved = crate::visibility::resolve(&mut tx, &session, plan, 0)
            .await
            .unwrap();
        let memory = crate::visibility::row_matches(plan, &resolved, &json!({"_id":"one"}));
        let mut values = Vec::new();
        let predicate =
            crate::visibility::compile_predicate(plan, &resolved, "r", &mut values).unwrap();
        let sql: i64 = crate::sql_parameters::Parameters::scalars(&values)
            .unwrap()
            .bind(sqlx::query(&format!(
                "SELECT count(*) FROM owned r WHERE {predicate}"
            )))
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap()
            .get(0);
        assert_eq!(sql != 0, sql_expected, "preserve existing SQL {raw}");
        assert_eq!(
            memory, memory_expected,
            "preserve existing interpreted {raw}"
        );
        match reg
            .fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap()
        {
            Fresh::Indexed(_) => {
                assert!(eligible, "ambiguous input must normal-fallback {raw}");
                assert_eq!(
                    !keys(&reg, &lease, &mut tx, &session).await.is_empty(),
                    sql_expected
                );
            }
            Fresh::NormalV2(Fallback::DomainOrIndexNotReady) => {
                assert!(!eligible, "canonical input must recover {raw}")
            }
            _ => panic!("unexpected domain fallback {raw}"),
        }
        tx.commit().await.unwrap();
    }
}

#[tokio::test]
#[expect(clippy::await_holding_lock, reason = "process-local fixture opt-in")]
async fn initially_ineligible_index_repairs_with_full_principal_backfill_and_reset_barrier() {
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Env(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let url = gonvex_store::database_url(&dir.path().join("initially-ineligible.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT,role TEXT,permissions JSON); INSERT INTO members VALUES('alice','a','active','member','{}'); CREATE TABLE owned(_id TEXT PRIMARY KEY,owner TEXT,body TEXT,flag BOOLEAN,payload JSON); INSERT INTO owned VALUES('one',X'01','first',1,'null')").execute(&mut **tx.transaction()).await.unwrap();
    indexes::install_local(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let registry = ModuleRegistry::new();
    registry
        .install_permission_fixture(module(
            json!({"operator":"eqContext","column":"owner","context":"member.id"}),
        ))
        .await;
    let lease = registry.project_for_call("fixture-project").await.unwrap();
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "fixture-project".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "a".into();
    session.member.id = "alice".into();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let Compilation::Complete(cert) =
        CompleteProviderCertificate::compile(&lease, &mut tx, &session, "owned.list")
            .await
            .unwrap()
    else {
        panic!("structural TEXT source not compiled")
    };
    let reg = cert.install(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let initial = indexes::state(tx.transaction(), reg.scope_id())
        .await
        .unwrap();
    assert_eq!(initial.phase, "failed");
    assert!(matches!(
        reg.fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap(),
        Fresh::NormalV2(Fallback::DomainOrIndexNotReady)
    ));
    // The FIRST source revision repairs the domain. There is no prior ready
    // index or principal backfill to incrementally reuse, even with no gap.
    sqlx::query("UPDATE owned SET owner='alice' WHERE _id='one'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    let repaired = indexes::state(tx.transaction(), reg.scope_id())
        .await
        .unwrap();
    assert!(repaired.generation > initial.generation);
    assert_eq!(keys(&reg, &lease, &mut tx, &session).await, vec!["one"]);
    assert_eq!(v2_keys(&lease, &mut tx, &session).await, vec!["one"]);
    assert_eq!(
        indexes::decision_diff::current(
            tx.transaction(),
            reg.scope_id(),
            repaired.generation,
            repaired.covered_revision,
            "alice"
        )
        .await
        .unwrap(),
        indexes::decision_diff::Outcome::NormalV2
    );
    tx.commit().await.unwrap();
    // An incomplete grant replacement is not a complete compiled generation.
    // A subsequent row-only write must rebuild principals as well as rows.
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::query("DELETE FROM _gonvex_permission_supports WHERE scope_id=?1")
        .bind(reg.scope_id())
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    assert!(matches!(
        reg.fresh(&lease, &mut tx, &session, &json!({}))
            .await
            .unwrap(),
        Fresh::NormalV2(Fallback::DomainOrIndexNotReady)
    ));
    sqlx::query("UPDATE owned SET body='changed after incomplete batch' WHERE _id='one'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    let restored = indexes::state(tx.transaction(), reg.scope_id())
        .await
        .unwrap();
    assert!(restored.generation > repaired.generation);
    assert_eq!(
        keys(&reg, &lease, &mut tx, &session).await,
        v2_keys(&lease, &mut tx, &session).await
    );
    assert_eq!(keys(&reg, &lease, &mut tx, &session).await, vec!["one"]);
    tx.commit().await.unwrap();
    // Legacy normalized APIs/receipts cannot replace or seal the source proof.
    for operation in 0..3 {
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        let result = match operation {
            0 => {
                indexes::apply(
                    &mut tx,
                    reg.scope_id(),
                    &[indexes::SourceChange::Grants {
                        source: "compiled.principals".into(),
                        facts: vec![],
                    }],
                )
                .await
            }
            1 => indexes::register_sources(&mut tx, reg.scope_id(), &[]).await,
            _ => indexes::seal(&mut tx, reg.scope_id()).await,
        };
        assert!(result.is_err());
        assert!(tx.commit().await.is_err());
    }
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(keys(&reg, &lease, &mut tx, &session).await, vec!["one"]);
    tx.commit().await.unwrap();
}
