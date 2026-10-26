//! Ordinary Rust regressions for bugs found during the Bend experiment.
use super::*;
use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry};
use sqlx::{postgres::PgPoolOptions, PgPool};

struct Fixture {
    admin: PgPool,
    control: ControlPlane,
    schema: String,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let Ok(url) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
            eprintln!("PostgreSQL host regression skipped: set GONVEX_TEST_POSTGRES_URL");
            return None;
        };
        let schema = format!("gonvex_parameter_host_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query(&format!(
            "CREATE TABLE \"{schema}\".tasks(id bigint PRIMARY KEY,title text NOT NULL)"
        ))
        .execute(&admin)
        .await
        .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{separator}options=-csearch_path%3D{schema}");
        let control = ControlPlane::connect(
            &scoped,
            PoolRegistry::new(PoolLimits::default()),
            BTreeMap::new(),
        )
        .await
        .unwrap();
        Some(Self {
            admin,
            control,
            schema,
        })
    }

    async fn calls(&self) -> DatabaseHostCalls {
        DatabaseHostCalls::new(
            self.control.begin_control_transaction(false).await.unwrap(),
            DatabaseCapability::Reducer,
        )
    }

    async fn reset(&self) {
        sqlx::query(&format!("TRUNCATE \"{}\".tasks", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        sqlx::query(&format!("INSERT INTO \"{}\".tasks VALUES (0,'zero'),(1,'one'),(4702394921427289928,'upper'),(7017280452245743464,'lower')",self.schema))
            .execute(&self.admin).await.unwrap();
    }

    async fn rows(&self) -> Vec<(i64, String)> {
        sqlx::query_as(&format!(
            "SELECT id,title FROM \"{}\".tasks ORDER BY id",
            self.schema
        ))
        .fetch_all(&self.admin)
        .await
        .unwrap()
    }

    async fn close(self) {
        sqlx::query(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

#[tokio::test]
async fn crud_type_changes_cannot_commit_unrelated_row_writes() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    for operation in ["update", "delete", "deleteMany"] {
        fixture.reset().await;
        let before = fixture.rows().await;
        let mut calls = fixture.calls().await;
        let failure = match operation {
            "update" => {
                calls
                    .update(
                        "tasks",
                        "id",
                        serde_json::json!(0),
                        serde_json::json!({"title":"first"}),
                    )
                    .await
                    .unwrap();
                calls
                    .update(
                        "tasks",
                        "id",
                        serde_json::json!("abcdefgh"),
                        serde_json::json!({"title":"wrong"}),
                    )
                    .await
            }
            "delete" => {
                calls
                    .delete("tasks", "id", serde_json::json!(0))
                    .await
                    .unwrap();
                calls
                    .delete("tasks", "id", serde_json::json!("abcdefgh"))
                    .await
            }
            _ => {
                calls
                    .delete_many("tasks", "id", serde_json::json!([0, 1]))
                    .await
                    .unwrap();
                calls
                    .delete_many("tasks", "id", serde_json::json!(["abcdefgh", "ABCDEFGH"]))
                    .await
            }
        };
        assert!(
            failure.is_err(),
            "{operation} must reject an incompatible ID type"
        );
        assert!(
            calls.finish(true).await.is_err(),
            "{operation} must not report a successful commit after the caught SQL error"
        );
        assert_eq!(
            fixture.rows().await,
            before,
            "{operation} changed unrelated or earlier rows"
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn raw_query_type_changes_and_described_text_arrays_preserve_values() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let mut calls = fixture.calls().await;
    for value in [
        serde_json::json!(0),
        serde_json::json!("abcdefgh"),
        serde_json::json!({"hello":"world"}),
        serde_json::json!(1.25),
        Value::Null,
    ] {
        assert_eq!(
            calls
                .query("SELECT $1 AS n", Value::Array(vec![value.clone()]))
                .await
                .unwrap(),
            serde_json::json!([{"n":value}])
        );
    }
    for cast in ["text[]", "varchar[]", "char[]"] {
        assert_eq!(
            calls
                .query(
                    &format!("SELECT $1::{cast} AS n"),
                    serde_json::json!([["a", "é"]])
                )
                .await
                .unwrap(),
            serde_json::json!([{"n":["a","é"]}])
        );
    }
    calls.finish(false).await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn query_admission_accepts_postgres_literals_and_comments() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let transaction = fixture
        .control
        .begin_control_transaction(true)
        .await
        .unwrap();
    let mut calls = DatabaseHostCalls::new(transaction, DatabaseCapability::Query);
    for (statement, expected) in [
        (
            "SELECT $$semi;colon$$ AS n",
            serde_json::json!("semi;colon"),
        ),
        ("SELECT $$'$$ AS n", serde_json::json!("'")),
        ("SELECT 42 AS n; -- trailing comment", serde_json::json!(42)),
        (r"SELECT E'a\';b' AS n", serde_json::json!("a';b")),
        ("-- leading comment\nSELECT 42 AS n", serde_json::json!(42)),
        (";;/* prefix */ SELECT 42 AS n;", serde_json::json!(42)),
        (
            "SELECT $é$semi;colon$é$ AS n",
            serde_json::json!("semi;colon"),
        ),
        ("SELECT E'a'\n'b\\';c' AS n", serde_json::json!("ab';c")),
        (
            "SELECT E'a' -- continuation\r'b\\';c' AS n",
            serde_json::json!("ab';c"),
        ),
    ] {
        assert_eq!(
            calls.query(statement, Value::Null).await.unwrap(),
            serde_json::json!([{"n": expected}]),
            "{statement:?}"
        );
    }
    calls.finish(true).await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn carriage_return_batch_rejects_before_postgres_and_keeps_transaction_usable() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let mut calls = fixture.calls().await;
    calls
        .query("INSERT INTO tasks VALUES (2,'valid')", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        calls
            .query("SELECT 1 -- comment\r; DELETE FROM tasks", Value::Null)
            .await
            .unwrap_err(),
        "a database statement may not contain more than one statement"
    );
    assert_eq!(
        calls
            .query("SELECT count(*) AS n FROM tasks", Value::Null)
            .await
            .unwrap(),
        serde_json::json!([{"n":1}])
    );
    calls.finish(true).await.unwrap();
    assert_eq!(fixture.rows().await, vec![(2, "valid".to_owned())]);
    fixture.close().await;
}
