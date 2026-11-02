//! Local SQLite load: 26 tenant files, each with one writer and a reader pool,
//! driven by simulated users that read a grid and commit small reducer-shaped
//! writes. Prints throughput and the registry snapshot.
//!
//! `LOAD_USERS` (default 300), `LOAD_SECONDS` (default 10) and
//! `GONVEX_DATA_DIR` (default: a fresh directory under the temp dir).
use gonvex_store::{begin_transaction, DataDir, PoolLimits, PoolRegistry};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let users: usize = std::env::var("LOAD_USERS")
        .unwrap_or("300".into())
        .parse()
        .unwrap();
    let seconds: u64 = std::env::var("LOAD_SECONDS")
        .unwrap_or("10".into())
        .parse()
        .unwrap();
    let root = std::env::var("GONVEX_DATA_DIR")
        .map(Into::into)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("gonvex-load-{}", uuid::Uuid::new_v4().simple()))
        });
    let data = DataDir::new(&root);
    let _lock = data.lock().expect("data directory lock");
    let pools = PoolRegistry::new(PoolLimits::default());
    let urls = (0..26)
        .map(|index| data.tenant_url("load", &format!("tenant-{index}")).unwrap())
        .collect::<Vec<_>>();
    for url in &urls {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS tasks(id INTEGER PRIMARY KEY, title TEXT, done BOOLEAN NOT NULL DEFAULT 0);
             INSERT INTO tasks(title)
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000)
             SELECT 'task ' || i FROM n WHERE NOT EXISTS (SELECT 1 FROM tasks);",
        )
        .execute(&pools.pool(url).await.unwrap())
        .await
        .unwrap();
    }
    let reads = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut tasks = Vec::new();
    for user in 0..users {
        let (pools, url) = (pools.clone(), urls[user % urls.len()].clone());
        let (reads, writes) = (reads.clone(), writes.clone());
        tasks.push(tokio::spawn(async move {
            let mut step = 0u64;
            while Instant::now() < deadline {
                step += 1;
                let read_only = step % 5 != 0;
                let mut transaction = begin_transaction(&pools, &url, read_only).await.unwrap();
                if read_only {
                    sqlx::query("SELECT id, title, done FROM tasks ORDER BY id LIMIT 100")
                        .fetch_all(&mut **transaction.transaction())
                        .await
                        .unwrap();
                    transaction.commit().await.unwrap();
                    reads.fetch_add(1, Ordering::Relaxed);
                } else {
                    sqlx::query("UPDATE tasks SET done = NOT done WHERE id = $1")
                        .bind((user as i64 * 7 + step as i64) % 1000 + 1)
                        .execute(&mut **transaction.transaction())
                        .await
                        .unwrap();
                    transaction.commit().await.unwrap();
                    writes.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let (reads, writes) = (
        reads.load(Ordering::Relaxed),
        writes.load(Ordering::Relaxed),
    );
    println!(
        "users={users} seconds={seconds} reads/s={} writes/s={} snapshot={:?}",
        reads / seconds,
        writes / seconds,
        pools.snapshot().await
    );
    pools.close().await;
    if std::env::var("GONVEX_DATA_DIR").is_err() {
        let _ = std::fs::remove_dir_all(&root);
    }
}
