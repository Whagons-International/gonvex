//! A matching bootstrap receipt must not hide sync triggers that another
//! writer replaced afterwards.
//!
//! Staging 2026-10-30 (on PostgreSQL): an older runtime build ran during a
//! rollback and rewrote every table's capture function with its own
//! generator. The current runtime came back, found its receipts and the
//! objects present, and skipped the repair. On SQLite the installer compares
//! the stored trigger definitions with the ones it would install.

use super::*;
use crate::transaction_commit_tests::TestDir;
use crate::{begin_transaction, PoolLimits};

const WIDE_COLUMNS: usize = 53;

#[test]
fn trigger_statements_are_read_verbatim_from_install_scripts() {
    let columns = ["id", "title"]
        .map(|name| crate::rows::ColumnInfo {
            name: name.to_owned(),
            declared: "TEXT".to_owned(),
            not_null: false,
            primary_key: name == "id",
        })
        .to_vec();
    let script = feed::capture_triggers_sql("tasks", "id", &columns);
    let statements = trigger_statements(&script);
    assert_eq!(statements.len(), 3);
    for (statement, name) in statements.iter().zip(feed::capture_trigger_names("tasks")) {
        assert!(
            statement.starts_with(&format!("CREATE TRIGGER \"{name}\"")),
            "{statement}"
        );
        assert!(statement.ends_with("\nEND"), "{statement}");
    }
}

#[tokio::test]
async fn sync_bootstrap_repairs_triggers_replaced_behind_matching_receipts() {
    let dir = TestDir::new("gonvex-foreign-writer");
    let url = crate::database_url(&dir.0.join("tenant.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let columns = (1..=WIDE_COLUMNS)
        .map(|index| format!("c{index:02} TEXT"))
        .collect::<Vec<_>>()
        .join(",");
    let writer = pools.pool(&url).await.unwrap();
    sqlx::raw_sql(&format!(
        "CREATE TABLE wide (id TEXT PRIMARY KEY,{columns})"
    ))
    .execute(&writer)
    .await
    .unwrap();
    let install = || async {
        let _lock = pools.lock(&url, SCHEMA_LOCK).await.unwrap();
        let mut connection = writer.acquire().await.unwrap();
        install_change_feed_locked(&mut connection).await
    };
    let receipts = || async {
        sqlx::query_as::<_, (String, String)>(
            "SELECT name,digest FROM _gonvex_sync_bootstrap ORDER BY name",
        )
        .fetch_all(&writer)
        .await
        .unwrap()
    };
    let write = |id: &'static str| {
        let pools = pools.clone();
        let url = url.clone();
        async move {
            let mut transaction = begin_transaction(&pools, &url, false).await.unwrap();
            sqlx::query("INSERT INTO wide(id,c01,c53) VALUES($1,'first','last')")
                .bind(id)
                .execute(&mut **transaction.transaction())
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
    };
    install().await.unwrap();
    let installed = receipts().await;
    write("before").await;

    // The older writer replaces the insert trigger with one that records no
    // row image and leaves this runtime's receipts untouched.
    sqlx::raw_sql(
        r#"DROP TRIGGER "_gonvex_sync_wide_insert";
           CREATE TRIGGER "_gonvex_sync_wide_insert" AFTER INSERT ON "wide" BEGIN
             INSERT INTO _gonvex_sync_changes (table_name,row_id,operation,changed_columns)
             VALUES ('wide', NEW.id, 'insert', '[]');
           END;"#,
    )
    .execute(&writer)
    .await
    .unwrap();
    assert_eq!(receipts().await, installed);
    write("broken").await;
    let broken: Option<String> =
        sqlx::query_scalar("SELECT new_value FROM _gonvex_sync_changes WHERE row_id='broken'")
            .fetch_one(&writer)
            .await
            .unwrap();
    assert_eq!(broken, None, "the foreign trigger lost the row image");

    install().await.unwrap();
    assert_eq!(
        receipts().await,
        installed,
        "the repair reinstalls the same definitions"
    );
    write("repaired").await;
    let image: serde_json::Value =
        sqlx::query_scalar("SELECT new_value FROM _gonvex_sync_changes WHERE row_id='repaired'")
            .fetch_one(&writer)
            .await
            .unwrap();
    assert_eq!(image.as_object().unwrap().len(), WIDE_COLUMNS + 1);
    assert_eq!(image["c53"], "last");
    let recorded: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT row_id,revision FROM _gonvex_sync_changes WHERE table_name='wide' ORDER BY event_id",
    )
    .fetch_all(&writer)
    .await
    .unwrap();
    assert_eq!(
        recorded
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["before", "broken", "repaired"]
    );
    assert!(
        recorded.iter().all(|(_, revision)| revision.is_some()),
        "{recorded:?}"
    );

    // A warm boot with matching definitions keeps everything as is.
    let version: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&writer)
        .await
        .unwrap();
    install().await.unwrap();
    let after: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&writer)
        .await
        .unwrap();
    assert_eq!(version, after);
    pools.close().await;
}
