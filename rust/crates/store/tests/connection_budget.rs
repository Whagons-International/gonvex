//! SQLite registry budget: one writer plus a bounded, idle-closing reader pool
//! per database file, and listeners that hold no connection.
use gonvex_store::{database_url, PoolLimits, PoolRegistry};
use std::time::Duration;

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
    fn url(&self, name: &str) -> String {
        database_url(&self.0.join(format!("{name}.db")))
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn idle_readers_close_and_databases_stay_within_their_budget() {
    let dir = TestDir::new("gonvex-budget");
    let registry = PoolRegistry::new(PoolLimits {
        max_connections_per_database: 2,
        max_idle_connections_per_database: 2,
        idle_timeout: Duration::from_millis(100),
        warm_reader_window: Duration::ZERO,
        ..PoolLimits::default()
    });
    let mut listeners = Vec::new();
    for index in 0..26 {
        let url = dir.url(&format!("budget-{index}"));
        let mut listener = registry.listener(&url).await.unwrap();
        listener.listen("budget_test").await.unwrap();
        listeners.push(listener);
        let reader = registry.reader(&url).await.unwrap();
        let a = reader.acquire().await.unwrap();
        let b = reader.acquire().await.unwrap();
        drop((a, b));
    }
    let snapshot = registry.snapshot().await;
    assert_eq!(snapshot.pools, 26);
    assert_eq!(
        snapshot.listener_connections, 0,
        "listeners hold no connection"
    );
    assert!(snapshot.open_connections <= 26 * 3, "{snapshot:?}");
    assert_eq!(snapshot.max_open_connections, 26 * 3);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let snapshot = registry.snapshot().await;
    // Only the always-open writers remain.
    assert_eq!(snapshot.open_connections, 26, "{snapshot:?}");
    drop(listeners);
    registry.close().await;
    assert_eq!(registry.snapshot().await.pools, 0);
}

#[tokio::test]
async fn one_warm_reader_outlives_the_idle_timeout_until_the_warm_window_ends() {
    let dir = TestDir::new("gonvex-warm");
    let registry = PoolRegistry::new(PoolLimits {
        max_connections_per_database: 3,
        max_idle_connections_per_database: 3,
        idle_timeout: Duration::from_millis(400),
        warm_reader_window: Duration::from_millis(2000),
        ..PoolLimits::default()
    });
    let urls = (0..4)
        .map(|index| dir.url(&format!("warm-{index}")))
        .collect::<Vec<_>>();
    for url in &urls {
        let reader = registry.reader(url).await.unwrap();
        let held = (
            reader.acquire().await.unwrap(),
            reader.acquire().await.unwrap(),
            reader.acquire().await.unwrap(),
        );
        drop(held);
    }
    assert_eq!(registry.snapshot().await.open_connections, 4 * 4);
    // Past the idle timeout, each database keeps exactly one warm reader.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(registry.snapshot().await.open_connections, 4 + 4);
    // A read within the window keeps it warm without opening another.
    let reader = registry.reader(&urls[0]).await.unwrap();
    let one: i64 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&reader)
        .await
        .unwrap();
    assert_eq!(one, 1);
    assert_eq!(registry.snapshot().await.open_connections, 4 + 4);
    // Past the window, unread databases close their warm reader; the one
    // read 1 s later is still inside its window.
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert_eq!(registry.snapshot().await.open_connections, 4 + 1);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(registry.snapshot().await.open_connections, 4);
    registry.close().await;
}

#[tokio::test]
async fn each_database_has_exactly_one_writer_connection() {
    let dir = TestDir::new("gonvex-writer");
    // Long enough to open a new database file on a loaded machine; the held
    // writer below still has to time out within it.
    let registry = PoolRegistry::new(PoolLimits {
        acquire_timeout: Duration::from_millis(1000),
        ..PoolLimits::default()
    });
    let url = dir.url("tenant");
    let writer = registry.pool(&url).await.unwrap();
    let held = writer.acquire().await.unwrap();
    assert!(matches!(
        writer.acquire().await,
        Err(sqlx::Error::PoolTimedOut)
    ));
    // Another file's writer is independent; the same URL spelled differently
    // resolves to the same database.
    registry
        .pool(&dir.url("other"))
        .await
        .unwrap()
        .acquire()
        .await
        .unwrap();
    let alias = format!("sqlite:{}", dir.0.join("tenant.db").display());
    assert!(matches!(
        registry.pool(&alias).await.unwrap().acquire().await,
        Err(sqlx::Error::PoolTimedOut)
    ));
    drop(held);
    writer.acquire().await.unwrap();
    // Readers are read-only.
    let reader = registry.reader(&url).await.unwrap();
    assert!(sqlx::query("CREATE TABLE t(x)")
        .execute(&reader)
        .await
        .is_err());
    registry.close().await;
}

#[tokio::test]
async fn regexp_is_available_on_writers_and_readers() {
    let dir = TestDir::new("gonvex-regexp");
    let registry = PoolRegistry::new(PoolLimits::default());
    let url = dir.url("tenant");
    for pool in [
        registry.pool(&url).await.unwrap(),
        registry.reader(&url).await.unwrap(),
    ] {
        let matched: bool = sqlx::query_scalar("SELECT 'task-42' REGEXP $1")
            .bind("^task-[0-9]+$")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(matched);
    }
    registry.close().await;
}
