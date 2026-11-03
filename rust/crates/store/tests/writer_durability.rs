//! The tenant writer's commit path: concurrent write transactions share WAL
//! syncs (the `sync_barrier` group commit) while each keeps its own atomicity,
//! revision and error isolation; checkpoints run off the writer with a
//! bounded WAL; the process-wide SQLite cache budget is applied.
use gonvex_store::feed::{capture_triggers_sql, SYNC_INFRASTRUCTURE_SQL};
use gonvex_store::rows::table_columns;
use gonvex_store::{
    begin_transaction, database_url, execute_script, Durability, PoolLimits, PoolRegistry,
};
use std::sync::atomic::Ordering;
use std::sync::Once;

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

/// Small checkpoint thresholds so a test crosses them quickly. Every test in
/// this binary sets the same values before opening a registry.
fn small_wal() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("GONVEX_SQLITE_CHECKPOINT_PAGES", "20");
        std::env::set_var("GONVEX_SQLITE_WAL_CAP_PAGES", "200");
    });
}

async fn tenant(durability: Durability) -> (TestDir, PoolRegistry, String) {
    small_wal();
    let dir = TestDir::new("gonvex-writer");
    let url = database_url(&dir.0.join("tenant.db"));
    let pools = PoolRegistry::with_durability(PoolLimits::default(), durability);
    let writer = pools.pool(&url).await.unwrap();
    let mut connection = writer.acquire().await.unwrap();
    execute_script(&mut connection, SYNC_INFRASTRUCTURE_SQL)
        .await
        .unwrap();
    execute_script(
        &mut connection,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
    )
    .await
    .unwrap();
    let columns = table_columns(&mut connection, "t").await.unwrap();
    execute_script(&mut connection, &capture_triggers_sql("t", "id", &columns))
        .await
        .unwrap();
    drop(connection);
    (dir, pools, url)
}

async fn values(pools: &PoolRegistry, url: &str) -> Vec<(i64, String)> {
    sqlx::query_as("SELECT id, v FROM t ORDER BY id")
        .fetch_all(&pools.reader(url).await.unwrap())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_commits_share_syncs_and_keep_their_own_revisions() {
    let (_dir, pools, url) = tenant(Durability::Full).await;
    let syncs = gonvex_store::WAL_SYNCS.load(Ordering::Relaxed);
    let mut tasks = Vec::new();
    for id in 0..60i64 {
        let (pools, url) = (pools.clone(), url.clone());
        tasks.push(tokio::spawn(async move {
            let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
            tx.set_command_id(&format!("cmd-{id}")).await.unwrap();
            sqlx::query("INSERT INTO t VALUES ($1, 'x')")
                .bind(id)
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            tx.commit_with_revision().await.unwrap().unwrap()
        }));
    }
    let mut revisions = Vec::new();
    for task in tasks {
        revisions.push(task.await.unwrap());
    }
    let syncs = gonvex_store::WAL_SYNCS.load(Ordering::Relaxed) - syncs;
    revisions.sort_unstable();
    revisions.dedup();
    assert_eq!(revisions.len(), 60, "one revision per transaction");
    assert!(
        syncs >= 1,
        "Full durability syncs the WAL before acknowledging"
    );
    assert!(
        syncs < 60,
        "concurrent commits shared WAL syncs ({syncs} for 60)"
    );
    let reader = pools.reader(&url).await.unwrap();
    let per_revision: Vec<(i64, i64)> =
        sqlx::query_as("SELECT revision, count(*) FROM _gonvex_sync_changes GROUP BY revision")
            .fetch_all(&reader)
            .await
            .unwrap();
    assert_eq!(per_revision.len(), 60);
    assert!(
        per_revision.iter().all(|(_, rows)| *rows == 1),
        "each revision holds only its own change"
    );
    let commands: i64 =
        sqlx::query_scalar("SELECT count(DISTINCT command_id) FROM _gonvex_sync_changes")
            .fetch_one(&reader)
            .await
            .unwrap();
    assert_eq!(commands, 60, "provenance stays per transaction");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_or_cancelled_transaction_does_not_affect_concurrent_commits() {
    let (_dir, pools, url) = tenant(Durability::Full).await;
    let mut tasks = Vec::new();
    for id in 0..30i64 {
        let (pools, url) = (pools.clone(), url.clone());
        tasks.push(tokio::spawn(async move {
            let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
            sqlx::query("INSERT INTO t VALUES ($1, 'kept')")
                .bind(id)
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            match id % 3 {
                // A statement failed: the whole transaction is refused.
                1 => {
                    tx.mark_failed("test failure");
                    assert!(tx.commit().await.is_err());
                    false
                }
                // Dropped without commit (a cancelled reducer future).
                2 => {
                    drop(tx);
                    false
                }
                _ => {
                    tx.commit().await.unwrap();
                    true
                }
            }
        }));
    }
    let mut expected = Vec::new();
    for (id, task) in tasks.into_iter().enumerate() {
        if task.await.unwrap() {
            expected.push((id as i64, "kept".to_owned()));
        }
    }
    assert_eq!(values(&pools, &url).await, expected);
    // The writer is still usable and the feed has no orphaned changes.
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    sqlx::query("INSERT INTO t VALUES (1000, 'after')")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE revision IS NULL")
            .fetch_one(&pools.reader(&url).await.unwrap())
            .await
            .unwrap();
    assert_eq!(pending, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_run_off_the_writer_and_the_wal_stays_bounded() {
    let (dir, pools, url) = tenant(Durability::Normal).await;
    let background = gonvex_store::checkpoint::BACKGROUND_CHECKPOINTS.load(Ordering::Relaxed);
    let padding = "p".repeat(3000);
    for id in 0..600i64 {
        let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
        sqlx::query("INSERT INTO t VALUES ($1, $2)")
            .bind(id)
            .bind(&padding)
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    // The checkpoint thread works asynchronously.
    for _ in 0..100 {
        if gonvex_store::checkpoint::BACKGROUND_CHECKPOINTS.load(Ordering::Relaxed) > background {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        gonvex_store::checkpoint::BACKGROUND_CHECKPOINTS.load(Ordering::Relaxed) > background,
        "the background thread checkpointed"
    );
    // 600 commits of ~5 pages each would reach ~3,000 frames without
    // checkpoints. The cap (200 frames) is approximate: an inline checkpoint
    // that meets a running background one retries on the next commit, and
    // the WAL restarts only when the writer's next transaction starts on a
    // fully copied WAL, so allow a few multiples of it.
    let wal = std::fs::metadata(dir.0.join("tenant.db-wal"))
        .unwrap()
        .len();
    let page: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(&pools.reader(&url).await.unwrap())
        .await
        .unwrap();
    assert!(
        wal <= (1_000 * (page + 24) + 32) as u64,
        "WAL stays near its cap: {wal} bytes"
    );
    assert_eq!(values(&pools, &url).await.len(), 600);
    // Closing the database closes the checkpoint thread's connection too; a
    // fresh registry reads everything back from the file.
    pools.close_database(&url).await;
    let reopened = PoolRegistry::with_durability(PoolLimits::default(), Durability::Normal);
    assert_eq!(values(&reopened, &url).await.len(), 600);
    reopened.close().await;
}

#[tokio::test]
async fn the_sqlite_cache_budget_is_a_process_wide_soft_heap_limit() {
    let _registry = PoolRegistry::with_durability(PoolLimits::default(), Durability::Normal);
    let budget = gonvex_store::sqlite_cache_budget_mib();
    assert_eq!(budget, 128, "default budget for a 2 GB box");
    // A negative argument reads the limit without changing it.
    let limit = unsafe { libsqlite3_sys::sqlite3_soft_heap_limit64(-1) };
    assert_eq!(limit, (budget * 1024 * 1024) as i64);
}
