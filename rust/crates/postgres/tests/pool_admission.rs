use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use gonvex_postgres::{ControlPlane, DatabaseError, PoolLimits, PoolRegistry, TenantRoute};
use sqlx::{postgres::PgPoolOptions, PgPool};

struct Fixture {
    control: ControlPlane,
    pools: PoolRegistry,
    admin: PgPool,
    schemas: Vec<String>,
    routes: Vec<TenantRoute>,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let base = std::env::var("GONVEX_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base)
            .await
            .unwrap();
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let schemas = (0..4)
            .map(|i| format!("admission_{nonce}_{i}"))
            .collect::<Vec<_>>();
        let mut urls = Vec::new();
        for schema in &schemas {
            sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
                .execute(&admin)
                .await
                .unwrap();
            let separator = if base.contains('?') { '&' } else { '?' };
            urls.push(format!("{base}{separator}options=-csearch_path%3D{schema}"));
        }
        let pools = PoolRegistry::new(PoolLimits {
            max_total_connections: 2,
            max_connections_per_database: 1,
            acquire_timeout: Duration::from_millis(500),
            ..PoolLimits::default()
        });
        let control = ControlPlane::connect(&urls[0], pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let routes = urls
            .into_iter()
            .skip(1)
            .enumerate()
            .map(|(i, database_url)| TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: format!("tenant-{i}"),
                database_url,
            })
            .collect();
        Some(Self {
            control,
            pools,
            admin,
            schemas,
            routes,
        })
    }

    async fn begin(
        &self,
        index: usize,
        read_only: bool,
    ) -> Result<gonvex_postgres::TenantTransaction, DatabaseError> {
        self.control
            .begin_tenant_transaction(&self.routes[index], read_only)
            .await
    }

    async fn close(self) {
        self.pools.close().await;
        for schema in self.schemas {
            sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
                .execute(&self.admin)
                .await
                .unwrap();
        }
        self.admin.close().await;
    }
}

#[tokio::test]
async fn busy_tenant_waiters_leave_shared_capacity_for_other_tenants_and_control() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let held = fixture.begin(0, true).await.unwrap();
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let mut waiter = Box::pin(tokio::task::unconstrained(fixture.begin(0, true)));
        assert!(matches!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        waiters.push(waiter);
    }
    assert_eq!(
        fixture.pools.snapshot().await.admission_active,
        1,
        "requests waiting for a tenant slot must not reserve shared capacity"
    );
    let mut healthy = fixture.begin(1, true).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 42")
            .fetch_one(&mut **healthy.transaction())
            .await
            .unwrap(),
        42
    );
    healthy.rollback().await.unwrap();
    fixture.control.ping().await.unwrap();
    drop(waiters);
    held.rollback().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    fixture.close().await;
}

#[tokio::test]
async fn cancelling_a_shared_queue_waiter_releases_its_database_slot() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let first = fixture.begin(0, true).await.unwrap();
    let second = fixture.begin(1, true).await.unwrap();
    let mut waiting = Box::pin(tokio::task::unconstrained(fixture.begin(2, true)));
    assert!(matches!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(fixture.pools.snapshot().await.admission_active, 2);
    assert_eq!(
        fixture
            .pools
            .pool(&fixture.routes[2].database_url)
            .await
            .unwrap()
            .size(),
        0,
        "waiting for shared admission must not open more physical connections"
    );
    drop(waiting);
    first.rollback().await.unwrap();
    let third = fixture.begin(2, true).await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 2);
    third.commit().await.unwrap();
    second.rollback().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    fixture.close().await;
}

#[tokio::test]
async fn database_queue_timeout_does_not_leak_shared_or_database_capacity() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let held = fixture.begin(0, false).await.unwrap();
    let result = fixture.begin(0, true).await;
    assert!(matches!(result, Err(DatabaseError::AdmissionTimeout)));
    assert_eq!(fixture.pools.snapshot().await.admission_active, 1);
    held.rollback().await.unwrap();
    let recovered = fixture.begin(0, true).await.unwrap();
    recovered.commit().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    fixture.close().await;
}

#[tokio::test]
async fn control_listener_does_not_occupy_the_only_sql_connection() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let listener = fixture
        .control
        .listener(&["pool_admission_test"])
        .await
        .unwrap();
    fixture.control.ping().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    drop(listener);
    fixture.close().await;
}

#[tokio::test]
async fn shared_queue_timeout_returns_the_reserved_database_slot() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let first = fixture.begin(0, false).await.unwrap();
    let second = fixture.begin(1, false).await.unwrap();
    let result = fixture.begin(2, true).await;
    assert!(matches!(result, Err(DatabaseError::AdmissionTimeout)));
    assert_eq!(fixture.pools.snapshot().await.admission_active, 2);
    first.rollback().await.unwrap();
    let recovered = fixture.begin(2, true).await.unwrap();
    recovered.rollback().await.unwrap();
    second.rollback().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    fixture.close().await;
}

#[tokio::test]
async fn closing_one_database_cancels_its_queue_without_locking_other_pools() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let held = fixture.begin(0, true).await.unwrap();
    let mut waiter = Box::pin(tokio::task::unconstrained(fixture.begin(0, true)));
    assert!(matches!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    let mut closing = Box::pin(tokio::task::unconstrained(
        fixture
            .pools
            .close_database(&fixture.routes[0].database_url),
    ));
    assert!(matches!(
        closing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert!(matches!(waiter.await, Err(DatabaseError::AdmissionTimeout)));
    let healthy = fixture.begin(1, true).await.unwrap();
    healthy.rollback().await.unwrap();
    held.rollback().await.unwrap();
    closing.await;
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    fixture.close().await;
}
