//! Independent source-support and migration oracles for the scoped semijoin.
use super::*;
use sqlx::{Connection, Row};
use std::collections::BTreeSet;

// Frozen accepted73 SQL, including its binding order and finite universe.
const OLD_SQL: &str = "WITH universe AS (SELECT row_key FROM _gonvex_permission_rows WHERE scope_id=?1 AND table_id=?2 AND generation=?3 AND universe_id=?6), p0 AS (SELECT ra.row_key FROM _gonvex_permission_row_atoms ra JOIN _gonvex_permission_set_atoms pa ON pa.scope_id=ra.scope_id AND pa.atom_id=ra.atom_id AND pa.generation=ra.generation WHERE ra.row_key IN (SELECT row_key FROM universe) AND ra.scope_id=?1 AND ra.table_id=?2 AND ra.generation=?3 AND pa.set_id=?4 AND ra.slot=?5) SELECT DISTINCT row_key FROM p0";

async fn execute(
    db: &mut sqlx::SqliteConnection,
    sql: &str,
    scope: i64,
    generation: i64,
    principal: Option<&str>,
) -> BTreeSet<String> {
    sqlx::query(sql)
        .bind(scope)
        .bind("tasks")
        .bind(generation)
        .bind(principal)
        .bind("read")
        .bind("u")
        .fetch_all(db)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}
async fn both(db: &mut sqlx::SqliteConnection, expected: &[&str]) {
    let compiled = compile("u", &Expr::SlotAny("read".into())).unwrap();
    let expected = expected
        .iter()
        .map(|s| s.to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        execute(db, OLD_SQL, 1, 1, Some("alice")).await,
        expected,
        "frozen JOIN oracle disagrees with independent expected sources"
    );
    assert_eq!(
        execute(db, &compiled.sql, 1, 1, Some("alice")).await,
        expected,
        "candidate changed effective support semantics"
    );
}
#[tokio::test]
async fn principal_semijoin_preserves_scope_types_case_overlap_group_paths_and_last_support() {
    let mut db = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../store/src/permission_indexes/schema.sql"
    )))
    .execute(&mut db)
    .await
    .unwrap();
    sqlx::raw_sql("INSERT INTO _gonvex_permission_atoms VALUES(1,1,1,X'01'),(1,1,2,X'02'),(1,1,3,X'03'),(1,1,10,X'41'),(1,1,11,X'61'),(1,1,X'01',X'bb'),(2,1,1,X'01'),(1,2,1,X'01');
    INSERT INTO _gonvex_permission_supports VALUES(1,1,'direct-a','member','alice',1),(1,1,'direct-b','member','alice',1),(1,1,'negative','member','alice',2),(1,1,'group','group','top',3),(1,1,'upper','member','ALICE',11),(1,1,'blob-principal','member',X'616c696365',11),(2,1,'other','member','alice',1),(1,2,'future','member','alice',1);
    INSERT INTO _gonvex_permission_memberships VALUES(1,1,'leaf-a','leaf','member','alice'),(1,1,'leaf-b','leaf','member','alice'),(1,1,'top','top','group','leaf'),(1,1,'cycle','leaf','group','top');
    INSERT INTO _gonvex_permission_row_supports VALUES(1,1,'rows','tasks','u','A'),(1,1,'rows','tasks','u','a'),(1,1,'rows','tasks','u','1'),(1,1,'rows','tasks','u','01'),(1,1,'rows','tasks','u','é'),(1,1,'rows','tasks','u','é'),(1,1,'rows','tasks','u','group'),(1,1,'rows','tasks','u','only-blob'),(1,1,'rows','tasks','u','canon-case'),(1,1,'rows','tasks','foreign','foreign-universe'),(1,1,'rows','other','u','foreign-table'),(2,1,'rows','tasks','u','other-scope'),(1,2,'rows','tasks','u','future-gen');
    INSERT INTO _gonvex_permission_row_atom_supports SELECT scope_id,generation,'primary',table_id,row_key,'read',1 FROM _gonvex_permission_row_supports WHERE row_key NOT IN ('group','only-blob','canon-case');
    INSERT INTO _gonvex_permission_row_atom_supports VALUES(1,1,'backup','tasks','a','read',1),(1,1,'group','tasks','a','read',3),(1,1,'group','tasks','group','read',3),(1,1,'blob','tasks','only-blob','read',X'01'),(1,1,'case','tasks','canon-case','read',11),(1,1,'blob-key','tasks',X'41','read',1),(1,1,'negative','tasks','a','blocked',2);").execute(&mut db).await.unwrap();
    both(&mut db, &["A", "a", "1", "01", "é", "é", "group"]).await;
    let c = compile("u", &Expr::SlotAny("read".into())).unwrap();
    for sql in [OLD_SQL, c.sql.as_str()] {
        assert!(execute(&mut db, sql, 1, 1, None).await.is_empty());
        assert_eq!(
            execute(&mut db, sql, 2, 1, Some("alice")).await,
            BTreeSet::from(["other-scope".into()])
        );
        assert_eq!(
            execute(&mut db, sql, 1, 2, Some("alice")).await,
            BTreeSet::from(["future-gen".into()])
        );
        assert_eq!(
            execute(&mut db, sql, 1, 1, Some("ALICE")).await,
            BTreeSet::from(["canon-case".into()])
        );
    }
    // NULLs are rejected by the physical normalized schema, not silently made
    // into matching atoms/keys by the query rewrite.
    assert!(sqlx::query("INSERT INTO _gonvex_permission_row_atom_supports VALUES(1,1,'null','tasks','A','read',NULL)").execute(&mut db).await.is_err());
    let mut tx = db.begin().await.unwrap();
    sqlx::query("DELETE FROM _gonvex_permission_supports WHERE scope_id=1 AND generation=1 AND source_id='direct-a'").execute(&mut *tx).await.unwrap();
    both(&mut tx, &["A", "a", "1", "01", "é", "é", "group"]).await;
    sqlx::query("DELETE FROM _gonvex_permission_supports WHERE scope_id=1 AND generation=1 AND source_id='direct-b'").execute(&mut *tx).await.unwrap();
    both(&mut tx, &["a", "group"]).await;
    tx.rollback().await.unwrap();
    both(&mut db, &["A", "a", "1", "01", "é", "é", "group"]).await;
    // Removing one row provenance retains the other; removing the last clears
    // it, while group grants and the finite universe continue to apply.
    sqlx::query("DELETE FROM _gonvex_permission_row_atom_supports WHERE scope_id=1 AND generation=1 AND row_key='A'").execute(&mut db).await.unwrap();
    both(&mut db, &["a", "1", "01", "é", "é", "group"]).await;
    let subtract = compile(
        "u",
        &Expr::Subtract {
            universe: "u".into(),
            allow: Box::new(Expr::SlotAny("read".into())),
            remove: Box::new(Expr::SlotAny("blocked".into())),
        },
    )
    .unwrap();
    let mut q = sqlx::query(&subtract.sql)
        .bind(1i64)
        .bind("tasks")
        .bind(1i64)
        .bind("alice");
    for b in &subtract.extra_bindings {
        q = match b {
            Binding::Text(s) => q.bind(s),
            Binding::Blob(b) => q.bind(b),
        };
    }
    let actual: BTreeSet<String> = q
        .fetch_all(&mut db)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        actual,
        ["1", "01", "é", "é", "group"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );
}

#[tokio::test]
async fn principal_semijoin_policy_hash_requires_reinstall_and_rollback_cannot_publish_it() {
    use crate::permissions::{
        model::{Atom, PermissionScope},
        query::{self, NormalizedBinding},
    };
    use gonvex_store::{
        permission_indexes as indexes, PoolLimits, PoolRegistry, TenantRoute, TenantSession,
    };
    let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    struct Env(Option<std::ffi::OsString>);
    impl Drop for Env {
        fn drop(&mut self) {
            if let Some(x) = self.0.take() {
                std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", x)
            } else {
                std::env::remove_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL")
            }
        }
    }
    let _env = Env(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let url = gonvex_store::database_url(&dir.path().join("policy-reinstall.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT,status TEXT); INSERT INTO members VALUES('alice','account','active')").execute(&mut **tx.transaction()).await.unwrap();
    let instance = indexes::install_local(&mut tx).await.unwrap();
    let epoch: String =
        sqlx::query_scalar("SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1")
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap();
    // Independently frozen accepted73 SQL policy hash; it must NOT describe the
    // changed compiler program even when its row relation is equivalent.
    let old = "c9cc51b5900a3519992e5b0dda0aa6d73130be0a2ce4326e7024785fa46c4895";
    let expr = Expr::SlotAny("read".into());
    let current = policy_version("u", &expr).unwrap();
    assert_ne!(
        current, old,
        "compiler SQL changed without invalidating its policy identity"
    );
    assert_ne!(
        policy_version_with_authority("u", &expr, b"source-one").unwrap(),
        policy_version_with_authority("u", &expr, b"source-two").unwrap()
    );
    let scope = PermissionScope {
        project: "fixture-project".into(),
        database_instance: instance.clone(),
        tenant: "fixture-tenant".into(),
        epoch: epoch.clone(),
        table: "tasks".into(),
        collection: "tasks.list".into(),
        policy_version: old.into(),
        projection_version: "keys-binary-v1".into(),
        semantics_version: "normalized-atoms-binary-v1".into(),
    };
    let atom = Atom {
        namespace: "fixture".into(),
        resource: "team".into(),
        principal: None,
    }
    .canonical()
    .unwrap();
    let facts = vec![
        indexes::SourceChange::Grants {
            source: "grants".into(),
            facts: vec![indexes::GrantFact {
                target: indexes::Subject::Member("alice".into()),
                atom: atom.clone(),
            }],
        },
        indexes::SourceChange::Rows {
            source: "rows".into(),
            facts: vec![indexes::RowFact {
                universe: "u".into(),
                key: "t1".into(),
                atoms: vec![("read".into(), atom)],
            }],
        },
    ];
    async fn install(
        tx: &mut gonvex_store::TenantTransaction,
        scope: &PermissionScope,
        facts: &[indexes::SourceChange],
    ) -> i64 {
        let id = indexes::create_scope(
            tx,
            &scope.canonical().unwrap(),
            &scope.database_instance,
            &scope.epoch,
            &scope.table,
        )
        .await
        .unwrap();
        indexes::register_sources(
            tx,
            id,
            &[
                ("grant".into(), "grants".into()),
                ("row".into(), "rows".into()),
            ],
        )
        .await
        .unwrap();
        indexes::apply(tx, id, facts).await.unwrap();
        indexes::seal(tx, id).await.unwrap();
        id
    }
    let old_id = install(&mut tx, &scope, &facts).await;
    tx.commit().await.unwrap();
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: scope.project.clone(),
        tenant_id: scope.tenant.clone(),
        database_url: url.clone(),
    });
    session.identity.account.id = "account".into();
    let old_binding = NormalizedBinding {
        scope_id: old_id,
        required_generation: 1,
        scope: scope.clone(),
        universe: "u".into(),
        expression: expr,
        authority: Vec::new(),
    };
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(
        query::prepare(&mut tx, &session, &old_binding, false)
            .await
            .is_err(),
        "old ready index inherited changed compiler identity"
    );
    tx.rollback().await.unwrap();
    let mut fresh = old_binding.clone();
    fresh.scope.policy_version = current;
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    fresh.scope_id = install(&mut tx, &fresh.scope, &facts).await;
    assert_ne!(fresh.scope_id, old_id);
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(
        query::prepare(&mut tx, &session, &fresh, false)
            .await
            .is_err(),
        "rolled-back reinstall became ready"
    );
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    fresh.scope_id = install(&mut tx, &fresh.scope, &facts).await;
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(
        query::select_keys(&mut tx, &session, &fresh)
            .await
            .unwrap()
            .1,
        vec!["t1"]
    );
    assert!(query::prepare(&mut tx, &session, &old_binding, false)
        .await
        .is_err());
    tx.rollback().await.unwrap();
}
