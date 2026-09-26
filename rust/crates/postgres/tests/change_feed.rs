use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_postgres::{
    ControlPlane, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, Row};

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

/// PostgreSQL caps function calls at 100 arguments. The change-feed trigger
/// used to pass every column pair to one `jsonb_build_object`, so any write to
/// a table with more than 50 columns failed.
#[tokio::test]
async fn change_feed_records_tables_wider_than_fifty_columns() {
    let Some(base_url) = env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping change-feed test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_rust_feed_control_{nonce}");
    let tenant_schema = format!("gonvex_rust_feed_tenant_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .expect("connect test PostgreSQL");
    for schema in [&control_schema, &tenant_schema] {
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .expect("create isolated schema");
    }
    let control_url = scoped_url(&base_url, &control_schema);
    let tenant_url = scoped_url(&base_url, &tenant_schema);
    let control = ControlPlane::connect(
        &control_url,
        PoolRegistry::new(PoolLimits::default()),
        BTreeMap::from([("project:tenant".to_owned(), tenant_url.clone())]),
    )
    .await
    .unwrap();

    // 120 data columns plus the key: three jsonb_build_object calls per image.
    let columns = (1..=120)
        .map(|index| format!("c{index:03} TEXT"))
        .collect::<Vec<_>>()
        .join(",");
    control
        .provision_tenant_database(
            TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: "tenant".to_owned(),
                database_url: tenant_url.clone(),
            },
            vec![SqlMigration::new(
                "0001_wide.sql".to_owned(),
                MigrationScope::Tenant,
                false,
                format!("CREATE TABLE wide (id TEXT PRIMARY KEY,{columns})"),
            )],
        )
        .await
        .unwrap();

    let tenant = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_url)
        .await
        .unwrap();
    sqlx::query("INSERT INTO wide(id,c001,c120) VALUES('row-1','first','last')")
        .execute(&tenant)
        .await
        .expect("insert into a 121-column table");
    sqlx::query("UPDATE wide SET c120='changed' WHERE id='row-1'")
        .execute(&tenant)
        .await
        .expect("update a 121-column table");
    sqlx::query("DELETE FROM wide WHERE id='row-1'")
        .execute(&tenant)
        .await
        .expect("delete from a 121-column table");

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
    assert_eq!(inserted.as_object().unwrap().len(), 121);
    assert_eq!(inserted["id"], "row-1");
    assert_eq!(inserted["c001"], "first");
    assert_eq!(inserted["c060"], Value::Null);
    assert_eq!(inserted["c120"], "last");
    assert_eq!(
        changes[0].get::<Vec<String>, _>("changed_columns").len(),
        121
    );

    assert_eq!(
        changes[1].get::<Vec<String>, _>("changed_columns"),
        ["c120"]
    );
    assert_eq!(changes[1].get::<Value, _>("old_value")["c120"], "last");
    assert_eq!(changes[1].get::<Value, _>("new_value")["c120"], "changed");

    let deleted = changes[2].get::<Value, _>("old_value");
    assert_eq!(deleted.as_object().unwrap().len(), 121);
    assert_eq!(deleted["c120"], "changed");
    assert_eq!(changes[2].get::<Option<Value>, _>("new_value"), None);

    tenant.close().await;
    for schema in [&control_schema, &tenant_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
}
