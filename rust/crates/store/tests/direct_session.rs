//! Direct sessions on a transaction's connection (`gonvex_store::direct`):
//! statements run on the caller's thread with cached prepared statements,
//! while transaction semantics stay those of `TenantTransaction` (rollback,
//! cancellation and error isolation, revision publication after commit, and
//! cache invalidation when the schema changes).
use std::time::Duration;

use gonvex_store::direct::{Bind, OwnedBind};
use gonvex_store::feed::{capture_triggers_sql, CHANGE_FEED_CHANNEL, SYNC_INFRASTRUCTURE_SQL};
use gonvex_store::rows::{decode_value, table_columns, JsonKind};
use gonvex_store::{
    begin_transaction, database_url, execute_script, Durability, PoolLimits, PoolRegistry,
};
use serde_json::{json, Value};
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

async fn tenant(durability: Durability) -> (TestDir, PoolRegistry, String) {
    let dir = TestDir::new("gonvex-direct");
    let url = database_url(&dir.0.join("tenant.db"));
    let pools = PoolRegistry::with_durability(PoolLimits::default(), durability);
    let writer = pools.pool(&url).await.unwrap();
    let mut connection = writer.acquire().await.unwrap();
    execute_script(&mut connection, SYNC_INFRASTRUCTURE_SQL).await.unwrap();
    execute_script(
        &mut connection,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL UNIQUE)",
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

async fn insert_direct(pools: &PoolRegistry, url: &str, id: i64, value: &str) -> Option<u64> {
    let mut tx = begin_transaction(pools, url, false).await.unwrap();
    tx.set_command_id(&format!("cmd-{id}")).await.unwrap();
    let session = tx.direct().await.unwrap();
    let changed = session
        .run(|conn| conn.execute("INSERT INTO t VALUES ($1, $2)", &[Bind::Integer(id), Bind::Text(value)]))
        .unwrap()
        .unwrap();
    assert_eq!(changed, 1);
    tx.commit_with_revision().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_writes_commit_through_the_change_feed_and_reuse_statements() {
    let (_dir, pools, url) = tenant(Durability::Normal).await;
    let first = insert_direct(&pools, &url, 1, "a").await.unwrap();
    let after_first = pools.writer_statement_stats(&url).await.unwrap();
    for id in 2..=20 {
        insert_direct(&pools, &url, id, &format!("v{id}")).await.unwrap();
    }
    let stats = pools.writer_statement_stats(&url).await.unwrap();
    // Every statement after the first transaction (the insert, finalize and
    // the schema check) came from the cache.
    assert_eq!(stats.prepared, after_first.prepared, "{stats:?}");
    assert!(stats.reused >= 19 * 8, "{stats:?}");
    assert_eq!(values(&pools, &url).await.len(), 20);
    // Change rows carry the revision, ordinal and command as before.
    let reader = pools.reader(&url).await.unwrap();
    let rows = sqlx::query(
        "SELECT revision, ordinal, command_id, new_value FROM _gonvex_sync_changes WHERE row_id = '1'",
    )
    .fetch_all(&reader)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>(0) as u64, first);
    assert_eq!(rows[0].get::<i64, _>(1), 1);
    assert_eq!(rows[0].get::<String, _>(2), "cmd-1");
    let image: Value = serde_json::from_str(&rows[0].get::<String, _>(3)).unwrap();
    assert_eq!(image, json!({"id": 1, "v": "a"}));
    let provenance: (String, String) = sqlx::query_as(
        "SELECT root_command_id, invocation_channel FROM _gonvex_sync_transactions WHERE revision = $1",
    )
    .bind(first as i64)
    .fetch_one(&reader)
    .await
    .unwrap();
    assert_eq!(provenance, ("cmd-1".to_owned(), "system".to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlx_and_direct_statements_share_one_transaction() {
    let (_dir, pools, url) = tenant(Durability::Normal).await;
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    let session = tx.direct().await.unwrap();
    session
        .run(|conn| conn.execute("INSERT INTO t VALUES (1, 'direct')", &[]))
        .unwrap()
        .unwrap();
    // Using sqlx ends the lease; the same transaction sees the direct write.
    let seen: i64 = sqlx::query_scalar("SELECT count(*) FROM t")
        .fetch_one(&mut **tx.transaction())
        .await
        .unwrap();
    assert_eq!(seen, 1);
    assert!(!session.active());
    assert!(session.run(|_| ()).is_none());
    sqlx::query("INSERT INTO t VALUES (2, 'sqlx')")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    // A new lease sees both.
    let session = tx.direct().await.unwrap();
    let count = session
        .run(|conn| conn.query_i64("SELECT count(*) FROM t", &[]))
        .unwrap()
        .unwrap();
    assert_eq!(count, Some(2));
    tx.commit().await.unwrap();
    assert_eq!(values(&pools, &url).await, vec![(1, "direct".into()), (2, "sqlx".into())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_cancel_and_errors_stay_inside_their_transaction() {
    let (_dir, pools, url) = tenant(Durability::Full).await;
    insert_direct(&pools, &url, 1, "kept").await.unwrap();

    // Explicit rollback after direct writes.
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    let session = tx.direct().await.unwrap();
    session
        .run(|conn| conn.execute("INSERT INTO t VALUES (2, 'rolled back')", &[]))
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(!session.active());

    // A cancelled transaction (dropped mid-way with its lease held, e.g. a
    // reducer future dropped at its deadline) leaves nothing behind and does
    // not wedge the writer: sqlx's worker resumes and rolls it back.
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    let session = tx.direct().await.unwrap();
    session
        .run(|conn| conn.execute("INSERT INTO t VALUES (3, 'cancelled')", &[]))
        .unwrap()
        .unwrap();
    drop(tx);
    assert!(session.run(|_| ()).is_none());

    // A failed statement reports sqlx's error text and code and leaves the
    // transaction usable; a transaction marked failed refuses to commit.
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    let session = tx.direct().await.unwrap();
    session
        .run(|conn| conn.execute("INSERT INTO t VALUES (4, 'partial')", &[]))
        .unwrap()
        .unwrap();
    let error = session
        .run(|conn| conn.execute("INSERT INTO t VALUES (5, 'kept')", &[]))
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "error returned from database: (code: 2067) UNIQUE constraint failed: t.v"
    );
    assert!(error.as_database_error().unwrap().is_unique_violation());
    tx.mark_failed(error.to_string());
    assert!(matches!(
        tx.commit_with_revision().await,
        Err(gonvex_store::DatabaseError::TransactionAborted(_))
    ));

    // Interleaved with committing transactions, only the committed rows stay.
    let mut tasks = Vec::new();
    for id in 10..30i64 {
        let (pools, url) = (pools.clone(), url.clone());
        tasks.push(tokio::spawn(async move {
            let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
            let session = tx.direct().await.unwrap();
            session
                .run(|conn| conn.execute("INSERT INTO t VALUES ($1, $2)", &[Bind::Integer(id), Bind::Text(&format!("v{id}"))]))
                .unwrap()
                .unwrap();
            match id % 3 {
                0 => tx.rollback().await.unwrap(),
                1 => drop(tx),
                _ => {
                    tx.commit_with_revision().await.unwrap().unwrap();
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let mut expected = vec![(1, "kept".to_owned())];
    expected.extend((10..30).filter(|id| id % 3 == 2).map(|id| (id, format!("v{id}"))));
    assert_eq!(values(&pools, &url).await, expected);
    // No orphaned change rows: every staged change got a revision.
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE revision IS NULL")
        .fetch_one(&pools.reader(&url).await.unwrap())
        .await
        .unwrap();
    assert_eq!(pending, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revisions_publish_after_commit_and_in_commit_order() {
    let (_dir, pools, url) = tenant(Durability::Full).await;
    let mut listener = pools.listener(&url).await.unwrap();
    listener.listen(CHANGE_FEED_CHANNEL).await.unwrap();
    let mut tasks = Vec::new();
    for id in 0..24i64 {
        let (pools, url) = (pools.clone(), url.clone());
        tasks.push(tokio::spawn(async move {
            insert_direct(&pools, &url, id, &format!("v{id}")).await.unwrap()
        }));
    }
    let mut returned = Vec::new();
    for task in tasks {
        returned.push(task.await.unwrap());
    }
    returned.sort_unstable();
    let base = returned[0];
    assert_eq!(returned, (base..base + 24).collect::<Vec<_>>());
    let reader = pools.reader(&url).await.unwrap();
    let mut published = Vec::new();
    while published.len() < 24 {
        let notification = tokio::time::timeout(Duration::from_secs(10), listener.recv())
            .await
            .unwrap()
            .unwrap();
        let payload: Value = serde_json::from_str(notification.payload()).unwrap();
        let revision = payload["revision"].as_u64().unwrap();
        // Published only once committed: a fresh reader already sees it.
        let clock: i64 = sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock")
            .fetch_one(&reader)
            .await
            .unwrap();
        assert!(clock as u64 >= revision);
        let changes: i64 =
            sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE revision = $1")
                .bind(revision as i64)
                .fetch_one(&reader)
                .await
                .unwrap();
        assert_eq!(changes, 1);
        assert_eq!(payload["tables"], json!(["t"]));
        published.push(revision);
    }
    published.sort_unstable();
    assert_eq!(published, (base..base + 24).collect::<Vec<_>>());
    // Each transaction's revision covers its own row only, assigned in commit
    // order (the writer serializes commits).
    let order: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT revision, event_id FROM _gonvex_sync_changes ORDER BY event_id",
    )
    .fetch_all(&reader)
    .await
    .unwrap();
    assert!(order.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_migration_invalidates_cached_statements() {
    let (_dir, pools, url) = tenant(Durability::Normal).await;
    insert_direct(&pools, &url, 1, "a").await.unwrap();
    let select = |pools: PoolRegistry, url: String| async move {
        let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
        let session = tx.direct().await.unwrap();
        let columns = session
            .run(|conn| {
                let statement = conn.prepare("SELECT * FROM t").unwrap();
                (0..statement.column_count())
                    .map(|index| {
                        (
                            statement.column_name(index).to_owned(),
                            statement.column_decltype(index).map(str::to_owned),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap();
        tx.rollback().await.unwrap();
        columns
    };
    let before = select(pools.clone(), url.clone()).await;
    assert_eq!(
        before,
        vec![("id".into(), Some("INTEGER".into())), ("v".into(), Some("TEXT".into()))]
    );
    let prepared = pools.writer_statement_stats(&url).await.unwrap().prepared;
    // A migration through sqlx on the writer adds a column.
    let writer = pools.pool(&url).await.unwrap();
    execute_script(
        &mut writer.acquire().await.unwrap(),
        "ALTER TABLE t ADD COLUMN meta JSON",
    )
    .await
    .unwrap();
    let after = select(pools.clone(), url.clone()).await;
    assert_eq!(after.len(), 3);
    assert_eq!(after[2], ("meta".into(), Some("JSON".into())));
    let stats = pools.writer_statement_stats(&url).await.unwrap();
    assert!(stats.prepared > prepared, "the cache was cleared and re-prepared: {stats:?}");

    // DDL through a session clears the cache too.
    let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
    let session = tx.direct().await.unwrap();
    session
        .run(|conn| {
            conn.execute("ALTER TABLE t ADD COLUMN flag BOOLEAN", &[]).unwrap();
            conn.schema_changed();
            let statement = conn.prepare("SELECT * FROM t").unwrap();
            assert_eq!(statement.column_count(), 4);
            assert_eq!(statement.column_decltype(3), Some("BOOLEAN"));
        })
        .unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_json_matches_the_sqlx_row_decoding() {
    let (_dir, pools, url) = tenant(Durability::Normal).await;
    let writer = pools.pool(&url).await.unwrap();
    execute_script(
        &mut writer.acquire().await.unwrap(),
        r#"CREATE TABLE k (id TEXT PRIMARY KEY, meta JSON, flag BOOLEAN, n BIGINT, r REAL, note TEXT, raw BLOB);
           INSERT INTO k VALUES
             ('a', '{"z":1,"a":[1,2.5,"x"],"s":"q\"uote"}', 1, 9007199254740993, 1.0, 'line
two', x'00ff10'),
             ('b', 'not json', 0, -4, 1e300, '{"looks":"json"}', NULL),
             ('c', NULL, NULL, NULL, NULL, NULL, NULL),
             ('d', '"text"', 2.5, 7, -0.5, 'true', x''),
             ('e', '[]', 0.0, 0, 3.0e-7, 'ünïcødé ✓', x'41');"#,
    )
    .await
    .unwrap();
    let kinds = [
        JsonKind::Text,
        JsonKind::Json,
        JsonKind::Boolean,
        JsonKind::Integer,
        JsonKind::Real,
        JsonKind::Text,
        JsonKind::Text,
    ];
    let reader = pools.reader(&url).await.unwrap();
    let rows = sqlx::query("SELECT * FROM k ORDER BY id")
        .fetch_all(&reader)
        .await
        .unwrap();
    let expected = rows
        .iter()
        .map(|row| {
            (0..kinds.len())
                .map(|index| decode_value(row.try_get_raw(index).unwrap(), kinds[index]).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut tx = begin_transaction(&pools, &url, true).await.unwrap();
    let session = tx.direct().await.unwrap();
    let actual = session
        .run(|conn| {
            let mut statement = conn.prepare("SELECT * FROM k ORDER BY id").unwrap();
            let mut rows = Vec::new();
            while statement.step().unwrap() {
                rows.push(
                    (0..kinds.len())
                        .map(|index| {
                            let mut out = Vec::new();
                            statement.write_json(index, kinds[index], &mut out).unwrap();
                            // Byte-identical to serializing the decoded value.
                            let value: Value = serde_json::from_slice(&out).unwrap();
                            assert_eq!(out, serde_json::to_vec(&value).unwrap());
                            value
                        })
                        .collect::<Vec<_>>(),
                );
            }
            rows
        })
        .unwrap();
    assert_eq!(actual, expected);

    // Streamed JSON rows match what sqlx returns for the same statement.
    let sql = "SELECT json_object('id', id, 'note', note) FROM k ORDER BY id".to_owned();
    let via_sqlx: Vec<Option<String>> = sqlx::query_scalar(&sql).fetch_all(&reader).await.unwrap();
    let mut stream = tx
        .stream_json_rows(sql, vec![OwnedBind::Integer(1)])
        .await
        .unwrap();
    let mut streamed = Vec::new();
    while let Some(batch) = stream.next_batch().await {
        for row in batch.unwrap().rows() {
            streamed.push(String::from_utf8(row.to_vec()).unwrap());
        }
    }
    drop(stream);
    assert_eq!(
        streamed,
        via_sqlx.into_iter().flatten().collect::<Vec<_>>()
    );
    tx.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_row_stream_stops_its_query() {
    let (_dir, pools, url) = tenant(Durability::Normal).await;
    let writer = pools.pool(&url).await.unwrap();
    execute_script(
        &mut writer.acquire().await.unwrap(),
        "CREATE TABLE big (n INTEGER); WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 50000) INSERT INTO big SELECT n FROM s;",
    )
    .await
    .unwrap();
    let mut tx = begin_transaction(&pools, &url, true).await.unwrap();
    let mut stream = tx
        .stream_json_rows("SELECT json_object('n', n) FROM big".to_owned(), Vec::new())
        .await
        .unwrap();
    let first = stream.next_batch().await.unwrap().unwrap();
    assert_eq!(first.rows().next().unwrap(), br#"{"n":1}"#);
    drop(stream);
    // Releasing the lease waits for the interrupted query, then sqlx works.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM big")
        .fetch_one(&mut **tx.transaction())
        .await
        .unwrap();
    assert_eq!(count, 50_000);
    tx.commit().await.unwrap();
}
