//! A failed statement must keep the transaction from committing, as Postgres
//! aborted the whole transaction on any error.
use gonvex_store::{begin_transaction, DatabaseError, PoolLimits, PoolRegistry};

struct TestDir(std::path::PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::var_os("GONVEX_TEST_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("abort-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn url(&self) -> String {
        format!("sqlite://{}", self.0.join("t.db").display())
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn marked_failure_refuses_commit_and_rolls_back() {
    let dir = TestDir::new();
    let url = dir.url();
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::query("INSERT INTO t VALUES (1)")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.mark_failed("constraint failed");
    match tx.commit().await {
        Err(DatabaseError::TransactionAborted(reason)) => assert_eq!(reason, "constraint failed"),
        other => panic!("expected TransactionAborted, got {other:?}"),
    }
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name = 't')",
    )
    .fetch_one(&pools.pool(&url).await.unwrap())
    .await
    .unwrap();
    assert!(!exists, "nothing from the aborted transaction may persist");
}

#[tokio::test]
async fn cleared_failure_commits() {
    let dir = TestDir::new();
    let url = dir.url();
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.mark_failed("recovered via ROLLBACK TO");
    tx.clear_failure();
    tx.commit().await.unwrap();
}
