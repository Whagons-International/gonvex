//! SQLite registry semantics: shared admission, one writer per file, readers
//! that see a snapshot and never wait for the writer, in-process
//! notifications, keyed locks and the data-directory lock.
use std::collections::BTreeMap;
use std::time::Duration;

use gonvex_store::{ControlPlane, DataDir, DatabaseError, PoolLimits, PoolRegistry, TenantRoute};

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

struct Fixture {
    _dir: TestDir,
    control: ControlPlane,
    pools: PoolRegistry,
    routes: Vec<TenantRoute>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TestDir::new("gonvex-admission");
        let data = DataDir::new(&dir.0);
        let pools = PoolRegistry::new(PoolLimits {
            max_total_connections: 2,
            max_connections_per_database: 2,
            acquire_timeout: Duration::from_millis(2000),
            ..PoolLimits::default()
        });
        let control = ControlPlane::connect(&data.control_url(), pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let mut routes = Vec::new();
        for index in 0..3 {
            let route = TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: format!("tenant-{index}"),
                database_url: data
                    .tenant_url("project", &format!("tenant-{index}"))
                    .unwrap(),
            };
            sqlx::query("CREATE TABLE records(id INTEGER PRIMARY KEY)")
                .execute(&pools.pool(&route.database_url).await.unwrap())
                .await
                .unwrap();
            routes.push(route);
        }
        Self {
            _dir: dir,
            control,
            pools,
            routes,
        }
    }

    async fn begin(
        &self,
        index: usize,
        read_only: bool,
    ) -> Result<gonvex_store::TenantTransaction, DatabaseError> {
        self.control
            .begin_tenant_transaction(&self.routes[index], read_only)
            .await
    }
}

#[tokio::test]
async fn shared_admission_timeout_does_not_leak_capacity() {
    let fixture = Fixture::new().await;
    let first = fixture.begin(0, false).await.unwrap();
    let second = fixture.begin(1, false).await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 2);
    let result = fixture.begin(2, true).await;
    assert!(matches!(result, Err(DatabaseError::AdmissionTimeout)));
    assert_eq!(fixture.pools.snapshot().await.admission_active, 2);
    first.rollback().await.unwrap();
    let recovered = fixture.begin(2, true).await.unwrap();
    recovered.rollback().await.unwrap();
    second.rollback().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
}

#[tokio::test]
async fn a_second_writer_waits_while_readers_proceed_on_a_snapshot() {
    let fixture = Fixture::new().await;
    let mut writer = fixture.begin(0, false).await.unwrap();
    sqlx::query("INSERT INTO records VALUES (1)")
        .execute(&mut **writer.transaction())
        .await
        .unwrap();
    // The only writer connection is busy: a second write transaction times
    // out and returns its admission permit.
    let blocked = fixture.begin(0, false).await;
    assert!(
        matches!(blocked, Err(DatabaseError::Sql(sqlx::Error::PoolTimedOut))),
        "{:?}",
        blocked.err()
    );
    assert_eq!(fixture.pools.snapshot().await.admission_active, 1);
    // A reader is admitted and sees only committed state.
    let mut reader = fixture.begin(0, true).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
        .fetch_one(&mut **reader.transaction())
        .await
        .unwrap();
    assert_eq!(count, 0);
    reader.rollback().await.unwrap();
    writer.commit().await.unwrap();
    let mut reader = fixture.begin(0, true).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
        .fetch_one(&mut **reader.transaction())
        .await
        .unwrap();
    assert_eq!(count, 1);
    reader.commit().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
}

#[tokio::test]
async fn control_listener_receives_in_process_notifications() {
    let fixture = Fixture::new().await;
    let mut listener = fixture
        .control
        .listener(&["pool_admission_test"])
        .await
        .unwrap();
    fixture.control.ping().await.unwrap();
    fixture
        .control
        .notify("ignored_channel", "nope")
        .await
        .unwrap();
    fixture
        .control
        .notify("pool_admission_test", "ready")
        .await
        .unwrap();
    let notification = tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification.channel(), "pool_admission_test");
    assert_eq!(notification.payload(), "ready");
    assert_eq!(fixture.pools.snapshot().await.admission_active, 0);
    // Notifications are per database file.
    let mut tenant = fixture
        .pools
        .listener(&fixture.routes[0].database_url)
        .await
        .unwrap();
    tenant.listen("pool_admission_test").await.unwrap();
    fixture
        .control
        .notify("pool_admission_test", "control only")
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), tenant.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn keyed_locks_serialize_one_key_on_one_database() {
    let fixture = Fixture::new().await;
    let url = &fixture.routes[0].database_url;
    let held = fixture.pools.lock(url, "schema").await.unwrap();
    // Other keys and other databases are independent.
    fixture.pools.lock(url, "other").await.unwrap();
    fixture
        .pools
        .lock(&fixture.routes[1].database_url, "schema")
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        fixture.pools.lock(url, "schema")
    )
    .await
    .is_err());
    drop(held);
    tokio::time::timeout(Duration::from_secs(1), fixture.pools.lock(url, "schema"))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn closing_a_database_releases_it_and_reopening_works() {
    let fixture = Fixture::new().await;
    let url = fixture.routes[2].database_url.clone();
    let before = fixture.pools.snapshot().await.pools;
    fixture.pools.close_database(&url).await;
    assert_eq!(fixture.pools.snapshot().await.pools, before - 1);
    let mut transaction = fixture.begin(2, false).await.unwrap();
    sqlx::query("INSERT INTO records VALUES (5)")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(fixture.pools.snapshot().await.pools, before);
}

#[tokio::test]
async fn the_data_directory_has_one_owner() {
    let dir = TestDir::new("gonvex-lock");
    let data = DataDir::new(dir.0.join("data"));
    let lock = data.lock().unwrap();
    assert_eq!(lock.path(), dir.0.join("data").join(".gonvex.lock"));
    assert!(matches!(data.lock(), Err(DatabaseError::DataDirLocked(_))));
    drop(lock);
    let again = data.lock().unwrap();
    assert!(std::fs::read_to_string(again.path())
        .unwrap()
        .contains(&std::process::id().to_string()));
}
