//! Warm readers count against `GONVEX_SQLITE_CACHE_BUDGET_MB`: when the
//! connection heap (schemas, statements) passes three quarters of the budget,
//! idle warm readers close. Own test binary: the budget is process-wide.
use gonvex_store::{database_url, sqlite_connection_heap, PoolLimits, PoolRegistry};
use std::time::Duration;

#[tokio::test]
async fn warm_readers_close_when_connection_heap_exceeds_the_budget() {
    // 1 MiB: a few connections holding the schema below exceed it.
    std::env::set_var("GONVEX_SQLITE_CACHE_BUDGET_MB", "1");
    let dir = std::env::temp_dir().join(format!(
        "gonvex-warm-budget-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let registry = PoolRegistry::new(PoolLimits {
        idle_timeout: Duration::from_millis(100),
        warm_reader_window: Duration::from_secs(600),
        ..PoolLimits::default()
    });
    let schema = (0..300)
        .map(|index| {
            format!(
                "CREATE TABLE t{index}(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER, d REAL);
                 CREATE INDEX t{index}_a ON t{index}(a, b);"
            )
        })
        .collect::<String>();
    let mut urls = Vec::new();
    for index in 0..4 {
        let url = database_url(&dir.join(format!("tenant-{index}.db")));
        sqlx::raw_sql(&schema)
            .execute(&registry.pool(&url).await.unwrap())
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM t1")
            .fetch_one(&registry.reader(&url).await.unwrap())
            .await
            .unwrap();
        assert_eq!(count, 0);
        urls.push(url);
    }
    assert!(
        sqlite_connection_heap() > 768 * 1024,
        "schema too small for the test: {}",
        sqlite_connection_heap()
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    // Inside the 10-minute warm window, but the writers alone exceed the
    // allowance, so no warm reader survives.
    assert_eq!(registry.snapshot().await.open_connections, 4);
    registry.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}
