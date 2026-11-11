//! Writer-only schema (`writer_schema`) through provisioning: objects leave
//! the file's schema, still run for writes, survive idle release, and
//! migrations see them persistent.
use super::*;
use crate::transaction_commit_tests::TestDir;
use crate::{begin_transaction, PoolLimits};
use std::time::Duration;

const SCHEMA: &str = r#"
CREATE TABLE store (id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE audit (id TEXT PRIMARY KEY, note TEXT NOT NULL);
CREATE VIEW items AS SELECT id, name FROM store;
CREATE TRIGGER items_insert INSTEAD OF INSERT ON items
  BEGIN INSERT INTO store(id, name) VALUES (NEW.id, upper(NEW.name)); END;
CREATE VIEW expected_names AS SELECT id, name FROM store WHERE name <> '';
CREATE TRIGGER store_audit AFTER INSERT ON store
  BEGIN INSERT INTO audit(id, note) SELECT 'a' || id, 'saw ' || name FROM expected_names WHERE id = NEW.id; END;
CREATE VIEW shared_ids AS SELECT id FROM store;
CREATE VIEW reader_ids AS SELECT * FROM shared_ids;
INSERT INTO _gonvex_writer_schema_rules(type, pattern) VALUES
  ('trigger', '%'), ('view', 'expected\_%'), ('view', 'shared\_%');
"#;

async fn names(pools: &PoolRegistry, url: &str, sql: &str) -> Vec<String> {
    sqlx::query_scalar(sql)
        .fetch_all(&pools.reader(url).await.unwrap())
        .await
        .unwrap()
}

async fn insert_item(pools: &PoolRegistry, url: &str, id: &str, name: &str) {
    let mut transaction = begin_transaction(pools, url, false).await.unwrap();
    sqlx::query("INSERT INTO items(id, name) VALUES ($1, $2)")
        .bind(id)
        .bind(name)
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

async fn audit(pools: &PoolRegistry, url: &str, id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT note FROM audit WHERE id = 'a' || $1 ORDER BY note")
        .bind(id)
        .fetch_all(&pools.pool(url).await.unwrap())
        .await
        .unwrap()
}

async fn temp_triggers(pools: &PoolRegistry, url: &str) -> Option<i64> {
    let writer = pools.pool(url).await.unwrap();
    let mut connection = writer.try_acquire()?;
    Some(
        sqlx::query_scalar("SELECT count(*) FROM temp.sqlite_schema WHERE type = 'trigger'")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn writer_only_objects_leave_the_file_schema_and_still_run_for_writes() {
    let dir = TestDir::new("gonvex-writer-schema");
    let data = crate::DataDir::new(&dir.0);
    let pools = PoolRegistry::new(PoolLimits {
        idle_timeout: Duration::from_millis(100),
        writer_schema_idle: Duration::from_millis(300),
        ..PoolLimits::default()
    });
    let control = ControlPlane::connect(&data.control_url(), pools.clone(), Default::default())
        .await
        .unwrap();
    let route = TenantRoute {
        project_id: "project".to_owned(),
        tenant_id: "tenant".to_owned(),
        database_url: data.tenant_url("project", "tenant").unwrap(),
    };
    let url = route.database_url.clone();
    let mut migrations = vec![SqlMigration::new(
        "0001_schema.sql".to_owned(),
        MigrationScope::Tenant,
        false,
        SCHEMA.to_owned(),
    )];
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations.clone())
        .await
        .unwrap();

    // Readers parse no module trigger, nor the views only triggers read; a
    // view another kept view names stays.
    let file_objects = names(
        &pools,
        &url,
        "SELECT name FROM sqlite_schema WHERE type IN ('trigger','view') AND name NOT LIKE '\\_gonvex%' ESCAPE '\\' AND name NOT LIKE 'gonvex\\_%' ESCAPE '\\' ORDER BY name",
    )
    .await;
    assert_eq!(file_objects, ["items", "reader_ids", "shared_ids"]);
    let stashed = names(
        &pools,
        &url,
        "SELECT name FROM _gonvex_writer_schema ORDER BY seq",
    )
    .await;
    assert_eq!(stashed, ["expected_names", "items_insert", "store_audit"]);

    // The writer runs them: INSTEAD OF on a view, AFTER on a table reading
    // a writer-only view.
    insert_item(&pools, &url, "1", "a").await;
    assert_eq!(audit(&pools, &url, "1").await, ["saw A"]);

    // An idle writer drops its copies; its next use recreates them first.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(temp_triggers(&pools, &url).await, Some(0));
    let (_, releases) = crate::writer_schema_counters();
    assert!(releases >= 1);
    insert_item(&pools, &url, "2", "b").await;
    assert_eq!(audit(&pools, &url, "2").await, ["saw B"]);

    // A migration sees (and replaces) them in the file's schema; afterwards
    // only the new definition runs, once.
    migrations.push(SqlMigration::new(
        "0002_audit_v2.sql".to_owned(),
        MigrationScope::Tenant,
        false,
        "DROP TRIGGER store_audit;
         CREATE TRIGGER store_audit AFTER INSERT ON store
           BEGIN INSERT INTO audit(id, note) VALUES ('a' || NEW.id, 'v2 ' || NEW.name); END;"
            .to_owned(),
    ));
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations.clone())
        .await
        .unwrap();
    insert_item(&pools, &url, "3", "c").await;
    assert_eq!(audit(&pools, &url, "3").await, ["v2 C"]);
    let file_triggers = names(
        &pools,
        &url,
        "SELECT name FROM sqlite_schema WHERE type = 'trigger' AND name NOT LIKE '\\_gonvex%' ESCAPE '\\' AND name NOT LIKE 'gonvex\\_%' ESCAPE '\\'",
    )
    .await;
    assert!(file_triggers.is_empty(), "{file_triggers:?}");

    // A reboot (no pending migration) keeps them stashed and running.
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations)
        .await
        .unwrap();
    insert_item(&pools, &url, "4", "d").await;
    assert_eq!(audit(&pools, &url, "4").await, ["v2 D"]);
    pools.close().await;
}
