//! Ordinary Rust regressions for bugs found during the Bend experiment,
//! running on SQLite.
use super::tests::TestDatabase;
use super::*;
use gonvex_store::rows::table_columns;

const TASKS: &str = "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL)";

struct Fixture {
    database: TestDatabase,
}

impl Fixture {
    async fn new() -> Self {
        Self {
            database: TestDatabase::new(TASKS).await,
        }
    }

    async fn calls(&self) -> DatabaseHostCalls {
        self.database.calls(DatabaseCapability::Reducer).await
    }

    async fn query_calls(&self) -> DatabaseHostCalls {
        self.database.calls(DatabaseCapability::Query).await
    }

    async fn reset(&self) {
        let pool = self.database.pools.pool(&self.database.url).await.unwrap();
        gonvex_store::execute_script(&mut pool.acquire().await.unwrap(), "DELETE FROM tasks; INSERT INTO tasks VALUES (0,'zero'),(1,'one'),(4702394921427289928,'upper'),(7017280452245743464,'lower')")
            .await
            .unwrap();
    }

    async fn rows(&self) -> Vec<(i64, String)> {
        let pool = self
            .database
            .pools
            .reader(&self.database.url)
            .await
            .unwrap();
        sqlx::query_as("SELECT id,title FROM tasks ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn crud_type_changes_cannot_commit_unrelated_row_writes() {
    // SQLite never coerces a non-numeric text id into an INTEGER key, so the
    // second call matches no row instead of touching an unrelated one.
    let fixture = Fixture::new().await;
    for operation in ["update", "delete", "deleteMany"] {
        fixture.reset().await;
        let before = fixture.rows().await;
        let mut calls = fixture.calls().await;
        let (first, second) = match operation {
            "update" => (
                calls
                    .update(
                        "tasks",
                        "id",
                        serde_json::json!(0),
                        serde_json::json!({"title":"first"}),
                    )
                    .await
                    .unwrap(),
                calls
                    .update(
                        "tasks",
                        "id",
                        serde_json::json!("abcdefgh"),
                        serde_json::json!({"title":"wrong"}),
                    )
                    .await
                    .unwrap(),
            ),
            "delete" => (
                calls
                    .delete("tasks", "id", serde_json::json!(0))
                    .await
                    .unwrap(),
                calls
                    .delete("tasks", "id", serde_json::json!("abcdefgh"))
                    .await
                    .unwrap(),
            ),
            _ => (
                calls
                    .delete_many("tasks", "id", serde_json::json!([0, 1]))
                    .await
                    .unwrap(),
                calls
                    .delete_many("tasks", "id", serde_json::json!(["abcdefgh", "ABCDEFGH"]))
                    .await
                    .unwrap(),
            ),
        };
        assert!(
            second.is_null() || second == serde_json::json!({"deleted": 0}),
            "{operation} with an incompatible id must match nothing: {second:?}"
        );
        calls.finish(true).await.unwrap();
        let expected = before
            .into_iter()
            .filter_map(|(id, title)| match (operation, id) {
                ("update", 0) => Some((0, "first".to_owned())),
                ("delete", 0) | ("deleteMany", 0 | 1) => None,
                _ => Some((id, title)),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            fixture.rows().await,
            expected,
            "{operation} changed unrelated rows; first={first:?}"
        );
    }
}

#[tokio::test]
async fn caught_sql_errors_cannot_commit_earlier_writes() {
    let fixture = Fixture::new().await;
    fixture.reset().await;
    let before = fixture.rows().await;
    let mut calls = fixture.calls().await;
    calls
        .update(
            "tasks",
            "id",
            serde_json::json!(0),
            serde_json::json!({"title":"first"}),
        )
        .await
        .unwrap();
    assert!(calls
        .query("INSERT INTO tasks VALUES (0,'duplicate')", Value::Null)
        .await
        .is_err());
    assert!(calls
        .query("SELECT 1 AS n", Value::Null)
        .await
        .unwrap_err()
        .contains("current transaction is aborted"));
    assert!(calls
        .delete("tasks", "id", serde_json::json!(1))
        .await
        .unwrap_err()
        .contains("current transaction is aborted"));
    assert!(
        calls
            .finish(true)
            .await
            .unwrap_err()
            .contains("current transaction is aborted"),
        "a caught SQL error must not become a successful commit"
    );
    assert_eq!(fixture.rows().await, before);

    // The store itself refuses to commit a transaction whose SQL failed.
    let mut calls = fixture.calls().await;
    calls
        .delete("tasks", "id", serde_json::json!(1))
        .await
        .unwrap();
    assert!(calls
        .query("INSERT INTO tasks VALUES (0,'duplicate')", Value::Null)
        .await
        .is_err());
    let transaction = calls.transaction.take().unwrap();
    assert!(matches!(
        transaction.commit_with_revision().await,
        Err(gonvex_store::DatabaseError::TransactionAborted(_))
    ));
    assert_eq!(fixture.rows().await, before);
}

#[tokio::test]
async fn rollback_to_savepoint_recovers_from_a_caught_sql_error() {
    let fixture = Fixture::new().await;
    let mut calls = fixture.calls().await;
    for statement in ["INSERT INTO tasks VALUES (2,'kept')", "SAVEPOINT attempt"] {
        calls.query(statement, Value::Null).await.unwrap();
    }
    assert!(calls
        .query("INSERT INTO tasks VALUES (2,'duplicate')", Value::Null)
        .await
        .is_err());
    calls
        .query("ROLLBACK TO SAVEPOINT attempt", Value::Null)
        .await
        .unwrap();
    calls
        .query("RELEASE SAVEPOINT attempt", Value::Null)
        .await
        .unwrap();
    calls.finish(true).await.unwrap();
    assert_eq!(fixture.rows().await, vec![(2, "kept".to_owned())]);
}

#[tokio::test]
async fn raw_query_type_changes_and_array_parameters_preserve_values() {
    let fixture = Fixture::new().await;
    let mut calls = fixture.calls().await;
    // One cached statement, many parameter types.
    for value in [
        serde_json::json!(0),
        serde_json::json!("abcdefgh"),
        serde_json::json!(1.25),
        serde_json::json!(-9007199254740993_i64),
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
    // Objects and arrays bind as JSON text; booleans as 0/1.
    let object = serde_json::json!({"hello":"world"});
    assert_eq!(
        calls
            .query(
                "SELECT json_extract($1, '$.hello') AS n, $2 AS b",
                serde_json::json!([object, true]),
            )
            .await
            .unwrap(),
        serde_json::json!([{"n":"world","b":1}])
    );
    for (value, expected) in [
        (serde_json::json!(["a", "é"]), serde_json::json!(["a", "é"])),
        (serde_json::json!([1, 2]), serde_json::json!([1, 2])),
        (serde_json::json!([{"x": 1}]), serde_json::json!([{"x": 1}])),
    ] {
        let statement =
            "SELECT value AS n, t.id FROM json_each($1) LEFT JOIN tasks t ON 0 ORDER BY key";
        for _ in 0..2 {
            let rows = calls
                .query(statement, Value::Array(vec![value.clone()]))
                .await
                .unwrap();
            let values = rows
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    assert!(row["id"].is_null());
                    row["n"].clone()
                })
                .collect::<Vec<_>>();
            assert_eq!(Value::Array(values), expected);
        }
    }
    calls.finish(false).await.unwrap();
}

#[tokio::test]
async fn query_admission_accepts_sqlite_literals_and_comments() {
    let fixture = Fixture::new().await;
    let mut calls = fixture.query_calls().await;
    for (statement, expected) in [
        ("SELECT 'semi;colon' AS n", serde_json::json!("semi;colon")),
        ("SELECT '''' AS n", serde_json::json!("'")),
        ("SELECT 42 AS n; -- trailing comment", serde_json::json!(42)),
        (r"SELECT 'a\'';b' AS n", serde_json::json!(r"a\';b")),
        ("-- leading comment\nSELECT 42 AS n", serde_json::json!(42)),
        (";;/* prefix */ SELECT 42 AS n;", serde_json::json!(42)),
        ("SELECT 42 AS [n;]", serde_json::json!(42)),
        ("SELECT 'é;' AS n", serde_json::json!("é;")),
    ] {
        let rows = calls.query(statement, Value::Null).await.unwrap();
        let value = rows[0]
            .as_object()
            .unwrap()
            .values()
            .next()
            .cloned()
            .unwrap();
        assert_eq!(value, expected, "{statement:?}");
    }
    assert!(calls
        .query("INSERT INTO tasks VALUES (9,'query write')", Value::Null)
        .await
        .unwrap_err()
        .contains("may only read"));
    calls.finish(true).await.unwrap();
    assert!(fixture.rows().await.is_empty());
}

#[tokio::test]
async fn line_comment_batches_follow_sqlite_rules_and_keep_transaction_usable() {
    let fixture = Fixture::new().await;
    let mut calls = fixture.calls().await;
    calls
        .query("INSERT INTO tasks VALUES (2,'valid')", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        calls
            .query("SELECT 1 -- comment\n; DELETE FROM tasks", Value::Null)
            .await
            .unwrap_err(),
        "a database statement may not contain more than one statement"
    );
    // A carriage return does not end a SQLite line comment, so the DELETE
    // is commented out rather than run as a second statement.
    assert_eq!(
        calls
            .query("SELECT 1 AS n -- comment\r; DELETE FROM tasks", Value::Null)
            .await
            .unwrap(),
        serde_json::json!([{"n":1}])
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
}

#[tokio::test]
async fn raw_transaction_boundaries_cannot_escape_invocation_rollback() {
    let fixture = Fixture::new().await;
    for boundary in [
        "COMMIT",
        "END TRANSACTION",
        "ROLLBACK",
        "BEGIN",
        "BEGIN IMMEDIATE",
        ";;/* prefix */ COMMIT",
        "-- prefix\nEND",
        "ATTACH DATABASE ':memory:' AS escape",
        "PRAGMA foreign_keys = OFF",
    ] {
        let mut calls = fixture.calls().await;
        calls
            .query(
                "INSERT INTO tasks VALUES (2,'before boundary')",
                Value::Null,
            )
            .await
            .unwrap();
        let error = calls.query(boundary, Value::Null).await.unwrap_err();
        assert!(
            error.contains("transaction boundaries") || error.contains("not allowed"),
            "{boundary}: {error}"
        );
        // A local rejection must not abort the transaction or lose earlier work.
        assert_eq!(
            calls
                .query("SELECT count(*) AS n FROM tasks", Value::Null)
                .await
                .unwrap(),
            serde_json::json!([{"n":1}])
        );
        calls.finish(false).await.unwrap();
        assert!(
            fixture.rows().await.is_empty(),
            "{boundary} escaped outer rollback"
        );
    }
    let mut calls = fixture.calls().await;
    for statement in [
        "INSERT INTO tasks VALUES (2,'kept')",
        "SAVEPOINT app",
        "INSERT INTO tasks VALUES (3,'rolled back')",
        "ROLLBACK TO SAVEPOINT app",
        "RELEASE SAVEPOINT app",
    ] {
        calls.query(statement, Value::Null).await.unwrap();
    }
    calls.finish(true).await.unwrap();
    assert_eq!(fixture.rows().await, vec![(2, "kept".to_owned())]);
}

#[tokio::test]
async fn caught_query_sql_errors_can_return_a_fallback_after_snapshot_rollback() {
    let fixture = Fixture::new().await;
    for statement in [
        "SELECT abs(-9223372036854775808)",
        "SELECT * FROM missing_table",
    ] {
        let mut calls = fixture.query_calls().await;
        assert!(calls.query(statement, Value::Null).await.is_err());
        calls
            .finish(true)
            .await
            .expect("a Query fallback should survive snapshot rollback");
    }
    assert!(fixture.rows().await.is_empty());
}

// Change-feed regressions: the capture triggers stage rows and the committing
// TenantTransaction assigns one revision with its provenance.
struct SyncFixture {
    database: TestDatabase,
}

#[derive(Debug)]
struct SyncFacts {
    rows: Vec<(i64, String)>,
    changes: Vec<Value>,
    transactions: Vec<Value>,
    revision: i64,
    retained: i64,
}

impl SyncFixture {
    async fn new() -> Self {
        let database = TestDatabase::new(TASKS).await;
        let pool = database.pools.pool(&database.url).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        gonvex_store::execute_script(&mut connection, gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
            .await
            .unwrap();
        let columns = table_columns(&mut connection, "tasks").await.unwrap();
        gonvex_store::execute_script(
            &mut connection,
            &gonvex_store::feed::capture_triggers_sql("tasks", "id", &columns),
        )
        .await
        .unwrap();
        Self { database }
    }

    async fn calls(&self, command: &str) -> DatabaseHostCalls {
        let mut calls = self.database.calls(DatabaseCapability::Reducer).await;
        calls
            .transaction_mut()
            .set_command_id(command)
            .await
            .unwrap();
        calls
    }

    async fn facts(&self) -> SyncFacts {
        let pool = self
            .database
            .pools
            .reader(&self.database.url)
            .await
            .unwrap();
        let json_rows = |sql: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, String>(sql)
                    .fetch_all(&pool)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|text| serde_json::from_str::<Value>(&text).unwrap())
                    .collect::<Vec<_>>()
            }
        };
        let rows = sqlx::query_as("SELECT id,title FROM tasks ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
        let changes = json_rows(
            "SELECT json_object('table_name',table_name,'row_id',row_id,'operation',operation,'old_value',json(old_value),'new_value',json(new_value),'revision',revision,'ordinal',ordinal,'command_id',command_id,'transaction_id',transaction_id) FROM _gonvex_sync_changes ORDER BY event_id",
        )
        .await;
        let transactions = json_rows(
            "SELECT json_object('revision',revision,'transaction_id',transaction_id,'origin_command_id',origin_command_id,'root_command_id',root_command_id,'invocation_channel',invocation_channel,'root_invocation_channel',root_invocation_channel) FROM _gonvex_sync_transactions ORDER BY revision",
        )
        .await;
        let (revision, retained) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT revision,retained_revision FROM _gonvex_sync_clock WHERE singleton = 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        SyncFacts {
            rows,
            changes,
            transactions,
            revision,
            retained,
        }
    }
}

impl SyncFacts {
    fn pristine(&self) -> bool {
        self.rows.is_empty()
            && self.changes.is_empty()
            && self.transactions.is_empty()
            && self.revision == 0
            && self.retained == 0
    }

    fn captures(&self, expected: &[(i64, &str)], command: &str) -> bool {
        let expected_rows = expected
            .iter()
            .map(|(id, title)| (*id, (*title).to_owned()))
            .collect::<Vec<_>>();
        if self.rows != expected_rows
            || self.changes.len() != expected.len()
            || self.transactions.len() != 1
            || self.revision <= 0
            || self.retained != 0
        {
            return false;
        }
        let meta = &self.transactions[0];
        if meta["revision"] != self.revision
            || meta["origin_command_id"] != command
            || meta["root_command_id"] != command
            || meta["invocation_channel"] != "system"
            || meta["root_invocation_channel"] != "system"
            || meta["transaction_id"].as_i64().is_none()
        {
            return false;
        }
        self.changes
            .iter()
            .zip(expected)
            .enumerate()
            .all(|(index, (change, (id, title)))| {
                let expected_id = id.to_string();
                change["table_name"] == "tasks"
                    && change["row_id"].as_str() == Some(expected_id.as_str())
                    && change["operation"] == "insert"
                    && change["old_value"].is_null()
                    && change["new_value"] == serde_json::json!({"id":id,"title":title})
                    && change["revision"] == self.revision
                    && change["ordinal"] == (index + 1) as i64
                    && change["command_id"] == command
                    && change["transaction_id"] == meta["transaction_id"]
            })
    }
}

async fn run(mut calls: DatabaseHostCalls, statements: &[&str]) -> Result<(), String> {
    for statement in statements {
        if let Err(error) = calls.query(statement, Value::Null).await {
            calls.finish(false).await?;
            return Err(error);
        }
    }
    calls.finish(true).await
}

#[tokio::test]
async fn sync_finalization_batches_capture_every_insert_in_one_revision() {
    let fixture = SyncFixture::new().await;
    assert!(fixture.facts().await.pristine());
    run(
        fixture.calls("sync_batch").await,
        &[
            "INSERT INTO tasks VALUES (1,'A'),(2,'B')",
            "INSERT INTO tasks VALUES (3,'C')",
        ],
    )
    .await
    .unwrap();
    let facts = fixture.facts().await;
    assert!(
        facts.captures(&[(1, "A"), (2, "B"), (3, "C")], "sync_batch"),
        "{facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_savepoint_rollback_discards_staged_changes() {
    let fixture = SyncFixture::new().await;
    run(
        fixture.calls("sync_savepoint").await,
        &[
            "INSERT INTO tasks VALUES (1,'A')",
            "SAVEPOINT before_b",
            "INSERT INTO tasks VALUES (2,'B')",
            "ROLLBACK TO SAVEPOINT before_b",
            "RELEASE SAVEPOINT before_b",
            "INSERT INTO tasks VALUES (3,'C')",
        ],
    )
    .await
    .unwrap();
    let facts = fixture.facts().await;
    assert!(
        facts.captures(&[(1, "A"), (3, "C")], "sync_savepoint"),
        "{facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_rollback_leaves_no_changes_for_the_next_commit() {
    let fixture = SyncFixture::new().await;
    let mut calls = fixture.calls("sync_rolled_back").await;
    calls
        .query("INSERT INTO tasks VALUES (1,'rolled back')", Value::Null)
        .await
        .unwrap();
    calls.finish(false).await.unwrap();
    assert!(fixture.facts().await.pristine());
    // A failed commit (caught SQL error) also leaves nothing staged.
    let failed = run(
        fixture.calls("sync_failed").await,
        &[
            "INSERT INTO tasks VALUES (1,'A')",
            "INSERT INTO tasks VALUES (1,'duplicate')",
        ],
    )
    .await;
    assert!(failed.is_err());
    assert!(fixture.facts().await.pristine());
    run(
        fixture.calls("sync_next_write").await,
        &["INSERT INTO tasks VALUES (2,'next transaction')"],
    )
    .await
    .unwrap();
    let facts = fixture.facts().await;
    assert!(
        facts.captures(&[(2, "next transaction")], "sync_next_write"),
        "{facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_reports_the_committed_revision() {
    let fixture = SyncFixture::new().await;
    let mut calls = fixture.calls("sync_revision").await;
    calls
        .insert("tasks", serde_json::json!({"id": 1, "title": "A"}), None)
        .await
        .unwrap();
    let transaction = calls.transaction.take().unwrap();
    assert_eq!(transaction.commit_with_revision().await.unwrap(), Some(1));
    let mut calls = fixture.calls("sync_read_only").await;
    calls
        .query("SELECT count(*) AS n FROM tasks", Value::Null)
        .await
        .unwrap();
    let transaction = calls.transaction.take().unwrap();
    assert_eq!(transaction.commit_with_revision().await.unwrap(), None);
    assert_eq!(fixture.facts().await.revision, 1);
}
