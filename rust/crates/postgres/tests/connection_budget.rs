//! Disposable numeric-loopback fixture only. SQL admission tests stay unchanged.
use gonvex_postgres::{PoolLimits, PoolRegistry};
use std::time::Duration;
fn local_url() -> Option<String> {
    let value = std::env::var("GONVEX_TEST_POSTGRES_URL").ok()?;
    assert!(matches!(
        url::Url::parse(&value).unwrap().host_str(),
        Some("127.0.0.1" | "::1")
    ));
    Some(value)
}
fn limits(idle: u32) -> PoolLimits {
    PoolLimits {
        max_idle_connections_per_database: idle,
        idle_timeout: Duration::from_millis(100),
        ..PoolLimits::default()
    }
}
#[tokio::test]
async fn idle_databases_cannot_accumulate_outside_physical_budget() {
    let Some(base) = local_url() else { return };
    let registry = PoolRegistry::new(limits(1));
    let mut listeners = Vec::new();
    for index in 0..26 {
        let url = format!("{base}&application_name=budget-{index}");
        let mut listener = registry.listener(&url).await.unwrap();
        listener.listen("budget_test").await.unwrap();
        listeners.push(listener);
        let pool = registry.pool(&url).await.unwrap();
        let a = pool.acquire().await.unwrap();
        let b = pool.acquire().await.unwrap();
        drop((a, b));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let snapshot = registry.snapshot().await;
    assert_eq!(snapshot.listener_connections, 26);
    assert!(snapshot.open_connections <= 78);
    assert_eq!(snapshot.idle, 0, "idle timeout closes tenant connections");
    drop(listeners);
    registry.close().await;
}
#[tokio::test]
async fn max_idle_zero_closes_released_connections() {
    let Some(base) = local_url() else { return };
    let registry = PoolRegistry::new(PoolLimits {
        idle_timeout: Duration::from_secs(60),
        ..limits(0)
    });
    let pool = registry.pool(&base).await.unwrap();
    sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.size() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    registry.close().await;
}
#[tokio::test]
async fn concurrent_releases_enforce_max_idle() {
    let Some(base) = local_url() else { return };
    let registry = PoolRegistry::new(PoolLimits {
        idle_timeout: Duration::from_secs(60),
        ..limits(1)
    });
    let pool = registry.pool(&base).await.unwrap();
    let a = pool.acquire().await.unwrap();
    let b = pool.acquire().await.unwrap();
    drop((a, b));
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.size() > 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(pool.num_idle() <= 1);
    registry.close().await;
}
#[tokio::test]
async fn listeners_are_counted_without_pinning_sql_admission() {
    let Some(base) = local_url() else { return };
    let registry = PoolRegistry::new(PoolLimits {
        max_total_connections: 1,
        ..limits(1)
    });
    let mut listener = registry.listener(&base).await.unwrap();
    listener.listen("budget_test").await.unwrap();
    assert_eq!(registry.snapshot().await.listener_connections, 1);
    let _permit = registry.admit(&base).await.unwrap();
    let pool = registry.pool(&base).await.unwrap();
    sqlx::query("SELECT pg_notify('budget_test', 'ok')")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .unwrap()
            .unwrap()
            .payload(),
        "ok"
    );
    drop(listener);
    assert_eq!(registry.snapshot().await.listener_connections, 0);
    registry.close().await;
}
