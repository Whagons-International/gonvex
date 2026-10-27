//! Native behavior tests. No metrics atomics or guard internals are inspected.
//! SQLx cases are explicitly ignored unless a parent supplies a test database.

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::{DatabaseError, PoolLimits, PoolRegistry};

fn registry(timeout: Duration) -> PoolRegistry {
    PoolRegistry::new(PoolLimits {
        max_total_connections: 1,
        acquire_timeout: timeout,
        ..PoolLimits::default()
    })
}

// Poll the real acquisition once without spawning a task or consuming its
// result. A successful call proves the operation actually suspended.
async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "expected a blocked acquisition"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn admission_immediate_does_not_count_a_wait() {
    let pools = registry(Duration::from_secs(5));
    let permit = pools.admit().await.unwrap();
    let snapshot = pools.snapshot().await;
    assert_eq!(snapshot.admission_active, 1);
    assert_eq!(snapshot.admission_wait.acquired_count, 1);
    assert_eq!(snapshot.admission_wait.wait_count, 0);
    assert_eq!(snapshot.admission_wait.current_waiters, 0);
    assert_eq!(snapshot.admission_wait.wait_duration_ms, 0);
    drop(permit);
    assert_eq!(pools.snapshot().await.admission_active, 0);
}

#[tokio::test]
async fn admission_preserves_native_cooperative_yield_with_available_capacity() {
    tokio::spawn(async {
        let pools = registry(Duration::from_secs(5));
        // Exhaust this task's current budget without yielding to the executor.
        for _ in 0..1024 {
            if !tokio::task::coop::has_budget_remaining() {
                break;
            }
            tokio::task::consume_budget().await;
        }
        assert!(!tokio::task::coop::has_budget_remaining());
        let mut acquire = Box::pin(pools.admit());
        assert_pending(acquire.as_mut()).await;
        let permit = acquire.await.unwrap();
        let snapshot = pools.snapshot().await;
        assert_eq!(snapshot.admission_wait.wait_count, 1);
        assert_eq!(snapshot.admission_wait.current_waiters, 0);
        assert_eq!(snapshot.admission_wait.acquired_count, 1);
        drop(permit);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn admission_queued_acquire_is_counted_once() {
    let pools = registry(Duration::from_secs(5));
    let held = pools.admit().await.unwrap();
    let mut queued = Box::pin(pools.admit());
    assert_pending(queued.as_mut()).await;
    assert_pending(queued.as_mut()).await;
    let waiting = pools.snapshot().await;
    assert_eq!(waiting.admission_wait.current_waiters, 1);
    assert_eq!(waiting.admission_wait.wait_count, 1);
    tokio::time::sleep(Duration::from_millis(2)).await;
    drop(held);
    let acquired = queued.await.unwrap();
    let done = pools.snapshot().await;
    assert_eq!(done.admission_wait.current_waiters, 0);
    assert_eq!(done.admission_wait.acquired_count, 2);
    assert_eq!(done.admission_wait.wait_count, 1);
    assert!(done.admission_wait.wait_duration_ms >= 1);
    assert_eq!(done.admission_wait.cancelled_count, 0);
    // The acquired permit retains its ordinary lifetime.
    assert_eq!(done.admission_active, 1);
    drop(acquired);
}

#[tokio::test]
async fn admission_cancel_balances_waiters_and_preserves_capacity() {
    let pools = registry(Duration::from_secs(5));
    let held = pools.admit().await.unwrap();
    let mut cancelled = Box::pin(pools.admit());
    assert_pending(cancelled.as_mut()).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    drop(cancelled);
    let done = pools.snapshot().await;
    assert_eq!(done.admission_wait.current_waiters, 0);
    assert_eq!(done.admission_wait.cancelled_count, 1);
    assert_eq!(done.admission_wait.timeout_count, 0);
    assert!(done.admission_wait.wait_duration_ms >= 1);
    assert_eq!(done.admission_active, 1);
    drop(held);
    let _next = pools.admit().await.unwrap();
}

#[tokio::test]
async fn admission_timeout_is_not_cancellation() {
    let pools = registry(Duration::from_millis(5));
    let held = pools.admit().await.unwrap();
    let mut queued = Box::pin(pools.admit());
    assert_pending(queued.as_mut()).await;
    assert!(matches!(queued.await, Err(DatabaseError::AdmissionTimeout)));
    let done = pools.snapshot().await;
    assert_eq!(done.admission_wait.wait_count, 1);
    assert_eq!(done.admission_wait.current_waiters, 0);
    assert_eq!(done.admission_wait.timeout_count, 1);
    assert_eq!(done.admission_wait.cancelled_count, 0);
    assert!(done.admission_wait.wait_duration_ms >= 1);
    drop(held);
    let _next = pools.admit().await.unwrap();
}

#[tokio::test]
async fn admission_keeps_fifo_order_after_cancellation() {
    let pools = registry(Duration::from_secs(5));
    let held = pools.admit().await.unwrap();
    let mut first = Box::pin(pools.admit());
    let mut cancelled = Box::pin(pools.admit());
    let mut last = Box::pin(pools.admit());
    assert_pending(first.as_mut()).await;
    assert_pending(cancelled.as_mut()).await;
    assert_pending(last.as_mut()).await;
    drop(cancelled);
    drop(held);
    // A newcomer cannot steal the permit reserved for the first waiter.
    let mut newcomer = Box::pin(pools.admit());
    assert_pending(newcomer.as_mut()).await;
    assert_pending(last.as_mut()).await;
    let first_permit = first.await.unwrap();
    assert_pending(last.as_mut()).await;
    drop(first_permit);
    let last_permit = last.await.unwrap();
    assert_pending(newcomer.as_mut()).await;
    drop(last_permit);
    let _newcomer_permit = newcomer.await.unwrap();
    let done = pools.snapshot().await;
    assert_eq!(done.admission_wait.current_waiters, 0);
    assert_eq!(done.admission_wait.cancelled_count, 1);
    assert_eq!(done.admission_wait.wait_count, 4);
    assert_eq!(done.admission_wait.acquired_count, 4);
}

#[tokio::test]
async fn admission_closed_is_an_error_without_a_false_wait() {
    let pools = registry(Duration::from_secs(5));
    pools.admission.close();
    assert!(matches!(
        pools.admit().await,
        Err(DatabaseError::AdmissionTimeout)
    ));
    let done = pools.snapshot().await;
    // Preserve the existing public error mapping, but diagnose closure correctly.
    assert_eq!(done.admission_wait.error_count, 1);
    assert_eq!(done.admission_wait.timeout_count, 0);
    assert_eq!(done.admission_wait.wait_count, 0);
}

async fn sqlx_pool(timeout: Duration) -> PgPool {
    let url = std::env::var("GONVEX_POOL_METRICS_TEST_DATABASE_URL")
        .expect("parent must supply an isolated test Postgres URL");
    PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
        // Test fixture only: create a deterministic immediate-ready idle path.
        // Production test_before_acquire remains SQLx's default true.
        .test_before_acquire(false)
        .acquire_timeout(timeout)
        .connect(&url)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated GONVEX_POOL_METRICS_TEST_DATABASE_URL"]
async fn sqlx_immediate_idle_acquire_does_not_count_a_wait() {
    let pool = sqlx_pool(Duration::from_secs(5)).await;
    let pools = registry(Duration::from_secs(5));
    let mut warm = pool.acquire().await.unwrap();
    warm.return_to_pool().await;
    tokio::task::yield_now().await;
    let connection = pools.acquire(&pool).await.unwrap();
    let done = pools.snapshot().await;
    assert_eq!(done.sqlx_acquire_wait.acquired_count, 1);
    assert_eq!(done.sqlx_acquire_wait.wait_count, 0);
    assert_eq!(done.sqlx_acquire_wait.current_waiters, 0);
    assert_eq!(done.admission_wait.acquired_count, 0);
    drop(connection);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires isolated GONVEX_POOL_METRICS_TEST_DATABASE_URL"]
async fn sqlx_queued_acquire_keeps_admission_and_connection_ownership() {
    let pool = sqlx_pool(Duration::from_secs(5)).await;
    let pools = registry(Duration::from_secs(5));
    let admission = pools.admit().await.unwrap();
    let mut held = pool.acquire().await.unwrap();
    let mut queued = Box::pin(pools.acquire(&pool));
    assert_pending(queued.as_mut()).await;
    assert_pending(queued.as_mut()).await;
    let waiting = pools.snapshot().await;
    assert_eq!(waiting.sqlx_acquire_wait.current_waiters, 1);
    assert_eq!(waiting.sqlx_acquire_wait.wait_count, 1);
    assert_eq!(waiting.admission_active, 1);
    assert_eq!(waiting.admission_wait.wait_count, 0);
    tokio::time::sleep(Duration::from_millis(2)).await;
    held.return_to_pool().await;
    let connection = queued.await.unwrap();
    let done = pools.snapshot().await;
    assert_eq!(done.sqlx_acquire_wait.current_waiters, 0);
    assert_eq!(done.sqlx_acquire_wait.acquired_count, 1);
    assert!(done.sqlx_acquire_wait.wait_duration_ms >= 1);
    assert_eq!(
        done.sqlx_acquire_wait.max_wait_duration_ms,
        done.sqlx_acquire_wait.wait_duration_ms
    );
    assert_eq!(pool.num_idle(), 0);
    assert_eq!(done.admission_active, 1);
    drop(connection);
    drop(admission);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires isolated GONVEX_POOL_METRICS_TEST_DATABASE_URL"]
async fn sqlx_cancel_balances_waiters_without_releasing_held_resources() {
    let pool = sqlx_pool(Duration::from_secs(5)).await;
    let pools = registry(Duration::from_secs(5));
    let admission = pools.admit().await.unwrap();
    let mut held = pool.acquire().await.unwrap();
    let mut cancelled = Box::pin(pools.acquire(&pool));
    assert_pending(cancelled.as_mut()).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    drop(cancelled);
    let done = pools.snapshot().await;
    assert_eq!(done.sqlx_acquire_wait.current_waiters, 0);
    assert_eq!(done.sqlx_acquire_wait.cancelled_count, 1);
    assert_eq!(done.sqlx_acquire_wait.timeout_count, 0);
    assert!(done.sqlx_acquire_wait.wait_duration_ms >= 1);
    assert_eq!(done.admission_active, 1);
    assert_eq!(pool.num_idle(), 0);
    held.return_to_pool().await;
    let connection = pools.acquire(&pool).await.unwrap();
    drop(connection);
    drop(admission);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires isolated GONVEX_POOL_METRICS_TEST_DATABASE_URL"]
async fn sqlx_timeout_is_not_cancellation() {
    let pool = sqlx_pool(Duration::from_millis(20)).await;
    let pools = registry(Duration::from_secs(5));
    let mut held = pool.acquire().await.unwrap();
    let mut queued = Box::pin(pools.acquire(&pool));
    assert_pending(queued.as_mut()).await;
    assert!(matches!(queued.await, Err(sqlx::Error::PoolTimedOut)));
    let done = pools.snapshot().await;
    assert_eq!(done.sqlx_acquire_wait.wait_count, 1);
    assert_eq!(done.sqlx_acquire_wait.current_waiters, 0);
    assert_eq!(done.sqlx_acquire_wait.timeout_count, 1);
    assert_eq!(done.sqlx_acquire_wait.cancelled_count, 0);
    assert!(done.sqlx_acquire_wait.wait_duration_ms >= 1);
    held.return_to_pool().await;
    let connection = pools.acquire(&pool).await.unwrap();
    drop(connection);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires isolated GONVEX_POOL_METRICS_TEST_DATABASE_URL"]
async fn sqlx_closed_is_an_error_without_a_false_wait() {
    let pool = sqlx_pool(Duration::from_secs(5)).await;
    let pools = registry(Duration::from_secs(5));
    pool.close().await;
    assert!(matches!(
        pools.acquire(&pool).await,
        Err(sqlx::Error::PoolClosed)
    ));
    let done = pools.snapshot().await;
    assert_eq!(done.sqlx_acquire_wait.error_count, 1);
    assert_eq!(done.sqlx_acquire_wait.wait_count, 0);
}

#[tokio::test]
#[ignore = "requires isolated GONVEX_POOL_METRICS_TEST_DATABASE_URL"]
async fn measured_transaction_and_query_release_connections_as_before() {
    let pool = sqlx_pool(Duration::from_secs(5)).await;
    let pools = registry(Duration::from_secs(5));
    let mut transaction = pools.begin(&pool).await.unwrap();
    sqlx::query("SELECT 1")
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    // The macro must accept scalar queries and temporary SQL strings, release
    // before returning, and leave acquisition errors unchanged.
    let value: i32 = crate::pool_query!(
        &pools,
        &pool,
        sqlx::query_scalar(&format!("SELECT {}::int", 7)),
        fetch_one
    )
    .await
    .unwrap();
    assert_eq!(value, 7);
    let clone = pools.clone();
    assert_eq!(clone.snapshot().await.sqlx_acquire_wait.acquired_count, 2);
    assert_eq!(clone.snapshot().await.admission_wait.acquired_count, 0);
    pool.close().await;
}
