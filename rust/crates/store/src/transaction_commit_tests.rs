use super::*;

struct Fixture {
    _dir: TestDir,
    pools: PoolRegistry,
    url: String,
}

/// A unique directory under the system temp dir, removed on drop.
pub(crate) struct TestDir(pub(crate) std::path::PathBuf);

impl TestDir {
    pub(crate) fn new(prefix: &str) -> Self {
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

impl Fixture {
    async fn new() -> Self {
        let dir = TestDir::new("gonvex-commit");
        let url = database_url(&dir.0.join("tenant.db"));
        let pools = PoolRegistry::new(PoolLimits::default());
        sqlx::raw_sql(
            "CREATE TABLE records(id INTEGER PRIMARY KEY);
             CREATE TABLE parents(id INTEGER PRIMARY KEY);
             CREATE TABLE children(id INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED);",
        )
        .execute(&pools.pool(&url).await.unwrap())
        .await
        .unwrap();
        Self {
            _dir: dir,
            pools,
            url,
        }
    }

    async fn begin(&self, read_only: bool) -> TenantTransaction {
        begin_transaction(&self.pools, &self.url, read_only)
            .await
            .unwrap()
    }

    async fn count(&self, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&self.pools.reader(&self.url).await.unwrap())
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn failed_statements_do_not_commit_their_own_effects() {
    let fixture = Fixture::new().await;
    // SQLite aborts only the failing statement; the transaction stays usable
    // and commits the statements that succeeded. The failed one left nothing.
    for statement in [
        "INSERT INTO records VALUES (1)",
        "SELECT * FROM missing_table",
    ] {
        sqlx::query("DELETE FROM records")
            .execute(&self_pool(&fixture).await)
            .await
            .unwrap();
        let mut transaction = fixture.begin(false).await;
        sqlx::query("INSERT INTO records VALUES (1)")
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        assert!(sqlx::query(statement)
            .execute(&mut **transaction.transaction())
            .await
            .is_err());
        transaction.rollback().await.unwrap();
        assert_eq!(fixture.count("records").await, 0, "after {statement}");
    }
}

async fn self_pool(fixture: &Fixture) -> SqlitePool {
    fixture.pools.pool(&fixture.url).await.unwrap()
}

#[tokio::test]
async fn healthy_commit_and_explicit_rollback_keep_their_semantics() {
    let fixture = Fixture::new().await;
    for (commit, expected) in [(true, 1i64), (false, 1i64)] {
        let mut transaction = fixture.begin(false).await;
        sqlx::query("INSERT INTO records VALUES ($1)")
            .bind(if commit { 1 } else { 2 })
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        if commit {
            transaction.commit().await.unwrap();
        } else {
            transaction.rollback().await.unwrap();
        }
        assert_eq!(fixture.count("records").await, expected);
    }
}

#[tokio::test]
async fn snapshot_can_rollback_after_a_caught_sql_error() {
    let fixture = Fixture::new().await;
    for statement in [
        "SELECT * FROM missing_table",
        "INSERT INTO records VALUES (1)",
    ] {
        let mut transaction = fixture.begin(true).await;
        assert!(transaction.read_only());
        assert!(
            sqlx::query(statement)
                .execute(&mut **transaction.transaction())
                .await
                .is_err(),
            "read-only snapshot accepted {statement}"
        );
        transaction.rollback().await.unwrap();
        assert_eq!(fixture.count("records").await, 0);
    }
}

#[tokio::test]
async fn deferred_constraints_still_fail_at_commit() {
    let fixture = Fixture::new().await;
    let mut transaction = fixture.begin(false).await;
    sqlx::query("INSERT INTO children VALUES (99)")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    assert!(transaction.commit().await.is_err());
    assert_eq!(fixture.count("children").await, 0);
    // The writer is usable again after the failed commit rolled back.
    let mut transaction = fixture.begin(false).await;
    sqlx::query("INSERT INTO records VALUES (7)")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(fixture.count("records").await, 1);
}

#[tokio::test]
async fn readers_keep_one_snapshot_while_the_writer_commits() {
    let fixture = Fixture::new().await;
    let mut snapshot = fixture.begin(true).await;
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
        .fetch_one(&mut **snapshot.transaction())
        .await
        .unwrap();
    let mut writer = fixture.begin(false).await;
    sqlx::query("INSERT INTO records VALUES (1)")
        .execute(&mut **writer.transaction())
        .await
        .unwrap();
    writer.commit().await.unwrap();
    let during: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
        .fetch_one(&mut **snapshot.transaction())
        .await
        .unwrap();
    assert_eq!(before, during, "a read transaction sees one WAL snapshot");
    snapshot.commit().await.unwrap();
    assert_eq!(fixture.count("records").await, 1);
}

#[tokio::test]
async fn aborted_transaction_cannot_report_successful_commit() {
    let fixture = Fixture::new().await;
    sqlx::query("INSERT INTO records VALUES (1)")
        .execute(&self_pool(&fixture).await)
        .await
        .unwrap();
    // A ROLLBACK conflict resolution ends the whole transaction inside SQLite.
    let mut transaction = fixture.begin(false).await;
    sqlx::query("INSERT INTO records VALUES (2)")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    assert!(sqlx::query("INSERT OR ROLLBACK INTO records VALUES (1)")
        .execute(&mut **transaction.transaction())
        .await
        .is_err());
    assert!(
        transaction.commit().await.is_err(),
        "a transaction SQLite already rolled back reported a successful commit"
    );
    assert_eq!(fixture.count("records").await, 1);
}
