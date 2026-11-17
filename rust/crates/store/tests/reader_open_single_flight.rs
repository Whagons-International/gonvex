//! A burst of reads on a database with no open reader opens one reader at a
//! time (`PoolLimits::reader_opens`): the reads wait for the reader being
//! opened and share it, instead of each parsing the schema in parallel (the
//! staging reconnect burst of 2026-11-04: 8 opens at 1.1-2.5 s each). Own
//! test binary: `sqlite_reader_opens` is process-wide.
use gonvex_store::{
    begin_transaction, database_url, sqlite_reader_opens, PoolLimits, PoolRegistry,
};
use std::time::Duration;

async fn burst_opens(reader_opens: u32) -> u64 {
    let dir = std::env::temp_dir().join(format!(
        "gonvex-reader-open-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let registry = PoolRegistry::new(PoolLimits {
        max_connections_per_database: 4,
        max_idle_connections_per_database: 4,
        reader_patience: Duration::from_millis(25),
        reader_opens,
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
    let url = database_url(&dir.join("tenant.db"));
    sqlx::raw_sql(&schema)
        .execute(&registry.pool(&url).await.unwrap())
        .await
        .unwrap();
    let before = sqlite_reader_opens();
    let mut reads = Vec::new();
    for _ in 0..12 {
        let (registry, url) = (registry.clone(), url.clone());
        reads.push(tokio::spawn(async move {
            let mut transaction = begin_transaction(&registry, &url, true).await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM t1")
                .fetch_one(&mut **transaction.transaction())
                .await
                .unwrap();
            assert_eq!(count, 0);
        }));
    }
    for read in reads {
        read.await.unwrap();
    }
    let opened = sqlite_reader_opens() - before;
    registry.close().await;
    let _ = std::fs::remove_dir_all(&dir);
    opened
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cold_burst_of_short_reads_opens_one_reader() {
    let opened = burst_opens(2).await;
    assert!(
        opened <= 2,
        "12 short reads on a cold database opened {opened} readers"
    );
}
