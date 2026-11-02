use super::*;

struct Fixture {
    admin: PgPool,
    pool: PgPool,
    schema: String,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let Some(url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping transaction commit test");
            return None;
        };
        let schema = format!("gonvex_commit_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{separator}options=-csearch_path%3D{schema}");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&scoped)
            .await
            .unwrap();
        Some(Self {
            admin,
            pool,
            schema,
        })
    }

    async fn begin(&self) -> TenantTransaction {
        TenantTransaction {
            transaction: self.pool.begin().await.unwrap(),
            _admission: AdmissionPermit {
                _shared: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
                _database: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
            },
            read_only: false,
        }
    }

    async fn close(self) {
        self.pool.close().await;
        sqlx::query(&format!(r#"DROP SCHEMA "{}" CASCADE"#, self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

#[tokio::test]
async fn aborted_transaction_cannot_report_successful_commit() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    sqlx::query("CREATE TABLE records(id integer PRIMARY KEY)")
        .execute(&fixture.pool)
        .await
        .unwrap();
    for statement in [
        "INSERT INTO records VALUES (1)",
        "SELECT 1/0",
        "SELECT * FROM missing_table",
    ] {
        sqlx::query("TRUNCATE records")
            .execute(&fixture.pool)
            .await
            .unwrap();
        let mut transaction = fixture.begin().await;
        sqlx::query("INSERT INTO records VALUES (1)")
            .execute(&mut *transaction.transaction)
            .await
            .unwrap();
        assert!(sqlx::query(statement)
            .execute(&mut *transaction.transaction)
            .await
            .is_err());
        assert!(
            transaction.commit().await.is_err(),
            "aborted transaction reported successful commit after {statement}"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    fixture.close().await;
}

#[tokio::test]
async fn healthy_commit_and_explicit_rollback_keep_their_semantics() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    sqlx::query("CREATE TABLE records(id integer PRIMARY KEY)")
        .execute(&fixture.pool)
        .await
        .unwrap();
    for (commit, expected) in [(true, 1i64), (false, 1i64)] {
        let mut transaction = fixture.begin().await;
        sqlx::query("INSERT INTO records VALUES ($1)")
            .bind(if commit { 1 } else { 2 })
            .execute(&mut *transaction.transaction)
            .await
            .unwrap();
        if commit {
            transaction.commit().await.unwrap();
        } else {
            transaction.rollback().await.unwrap();
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(count, expected);
    }
    fixture.close().await;
}

#[tokio::test]
async fn snapshot_can_rollback_after_a_caught_sql_error() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    sqlx::query("CREATE TABLE records(id integer PRIMARY KEY)")
        .execute(&fixture.pool)
        .await
        .unwrap();
    for statement in [
        "SELECT * FROM missing_table",
        "INSERT INTO records VALUES (1)",
    ] {
        let mut transaction = fixture.begin().await;
        transaction.read_only = true;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *transaction.transaction)
            .await
            .unwrap();
        assert!(sqlx::query(statement)
            .execute(&mut *transaction.transaction)
            .await
            .is_err());
        transaction.rollback().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM records")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    fixture.close().await;
}

#[tokio::test]
async fn deferred_constraints_still_fail_at_commit() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    sqlx::query("CREATE TABLE parents(id integer PRIMARY KEY)")
        .execute(&fixture.pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE children(id integer REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .execute(&fixture.pool)
    .await
    .unwrap();
    let mut transaction = fixture.begin().await;
    sqlx::query("INSERT INTO children VALUES (99)")
        .execute(&mut *transaction.transaction)
        .await
        .unwrap();
    assert!(transaction.commit().await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM children")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    fixture.close().await;
}
