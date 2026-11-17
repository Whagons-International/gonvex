use std::collections::BTreeMap;

use gonvex_store::{
    ControlPlane, DataDir, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
};
use serde_json::Value;
use sqlx::Row;

struct TestDir(std::path::PathBuf);

impl TestDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::var_os("GONVEX_TEST_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("{prefix}-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Row images of wide tables: SQLite caps one function call at 1,000
/// arguments, so images of very wide tables merge several `json_object` calls.
/// Every write commits one revision with typed images and changed columns.
#[tokio::test]
async fn change_feed_records_wide_tables_with_typed_images() {
    let dir = TestDir::new("gonvex-feed");
    let data = DataDir::new(&dir.0);
    let tenant_url = data.tenant_url("project", "tenant").unwrap();
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(
        &data.control_url(),
        pools.clone(),
        BTreeMap::from([("project:tenant".to_owned(), tenant_url.clone())]),
    )
    .await
    .unwrap();

    // 600 text columns plus typed ones: two json_object calls per image.
    let columns = (1..=600)
        .map(|index| format!("c{index:03} TEXT"))
        .collect::<Vec<_>>()
        .join(",");
    let route = TenantRoute {
        project_id: "project".to_owned(),
        tenant_id: "tenant".to_owned(),
        database_url: tenant_url.clone(),
    };
    control
        .clone()
        .provision_tenant_database(
            route.clone(),
            vec![SqlMigration::new(
                "0001_wide.sql".to_owned(),
                MigrationScope::Tenant,
                false,
                format!(
                    "CREATE TABLE wide (id TEXT PRIMARY KEY,done BOOLEAN,meta JSON,n INTEGER,{columns})"
                ),
            )],
        )
        .await
        .unwrap();

    let mut listener = pools.listener(&tenant_url).await.unwrap();
    listener.listen("gonvex_change_feed").await.unwrap();
    let mut revisions = Vec::new();
    for statement in [
        r#"INSERT INTO wide(id,done,meta,n,c001,c600) VALUES('row-1',1,'{"a":[1]}',7,'first','last')"#,
        "UPDATE wide SET c600='changed' WHERE id='row-1'",
        "DELETE FROM wide WHERE id='row-1'",
    ] {
        let mut transaction = control
            .begin_tenant_transaction(&route, false)
            .await
            .unwrap();
        sqlx::query(statement)
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        revisions.push(transaction.commit_with_revision().await.unwrap().unwrap());
        let notification = listener.recv().await.unwrap();
        let payload: Value = serde_json::from_str(notification.payload()).unwrap();
        assert_eq!(payload["revision"], *revisions.last().unwrap());
        assert_eq!(payload["tables"], serde_json::json!(["wide"]));
    }
    assert_eq!(revisions, [1, 2, 3]);

    let tenant = pools.reader(&tenant_url).await.unwrap();
    let changes = sqlx::query(
        r#"SELECT operation,old_value,new_value,changed_columns FROM _gonvex_sync_changes
           WHERE table_name='wide' ORDER BY revision,ordinal"#,
    )
    .fetch_all(&tenant)
    .await
    .unwrap();
    let operations = changes
        .iter()
        .map(|change| change.get::<String, _>("operation"))
        .collect::<Vec<_>>();
    assert_eq!(operations, ["insert", "update", "delete"]);

    let inserted = changes[0].get::<Value, _>("new_value");
    assert_eq!(inserted.as_object().unwrap().len(), 604);
    assert_eq!(inserted["id"], "row-1");
    assert_eq!(inserted["done"], true);
    assert_eq!(inserted["meta"], serde_json::json!({"a":[1]}));
    assert_eq!(inserted["n"], 7);
    assert_eq!(inserted["c001"], "first");
    assert_eq!(inserted["c300"], Value::Null);
    assert_eq!(inserted["c600"], "last");

    assert_eq!(
        changes[1].get::<Value, _>("changed_columns"),
        serde_json::json!(["c600"])
    );
    assert_eq!(changes[1].get::<Value, _>("old_value")["c600"], "last");
    assert_eq!(changes[1].get::<Value, _>("new_value")["c600"], "changed");

    let deleted = changes[2].get::<Value, _>("old_value");
    assert_eq!(deleted.as_object().unwrap().len(), 604);
    assert_eq!(deleted["c600"], "changed");
    assert_eq!(changes[2].get::<Option<Value>, _>("new_value"), None);

    // A transaction that changes no tracked row publishes nothing.
    let mut transaction = control
        .begin_tenant_transaction(&route, false)
        .await
        .unwrap();
    sqlx::query("UPDATE wide SET c001='x' WHERE id='missing'")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    assert_eq!(transaction.commit_with_revision().await.unwrap(), None);
    pools.close().await;
}
