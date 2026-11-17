//! Warm readers count against `GONVEX_SQLITE_CACHE_BUDGET_MB`: when the
//! readers' connection heap (schemas, statements) passes three quarters of the
//! budget, idle warm readers close. Writers' schemas are not counted: closing
//! readers cannot free them. Own test binary: the budget is process-wide.
use gonvex_store::{
    database_url, sqlite_connection_heap, sqlite_writer_schema_heap, PoolLimits, PoolRegistry,
};
use std::time::Duration;

const MIB: i64 = 1024 * 1024;

#[tokio::test]
async fn warm_readers_close_only_when_reader_heap_exceeds_the_budget() {
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
    let schema = (0..1500)
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
    // The schema was created after the writers opened: the sweep re-measures
    // idle writers.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let writers = sqlite_writer_schema_heap();
    let total = sqlite_connection_heap();
    let readers = total - writers;
    assert!(
        writers > 2 * MIB && readers > 2 * MIB,
        "schema too small for the test: writers {writers}, readers {readers}"
    );

    // A budget the warm readers fit in, which the writers alone exceed: every
    // warm reader stays open.
    let budget = (readers * 4 / 3) / MIB + 2;
    assert!(writers + readers > budget * MIB * 3 / 4);
    std::env::set_var("GONVEX_SQLITE_CACHE_BUDGET_MB", budget.to_string());
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(registry.snapshot().await.open_connections, 8);

    // A budget no reader fits in: the warm readers close, the writers stay.
    std::env::set_var("GONVEX_SQLITE_CACHE_BUDGET_MB", "1");
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(registry.snapshot().await.open_connections, 4);
    registry.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}
