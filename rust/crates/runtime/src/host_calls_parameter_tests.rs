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

#[tokio::test]
async fn raw_transaction_boundaries_cannot_escape_invocation_rollback() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    for boundary in [
        "COMMIT",
        "END WORK",
        "ROLLBACK",
        "ABORT",
        "BEGIN",
        "START TRANSACTION",
        "PREPARE TRANSACTION 'escape'",
        ";;/* prefix */ COMMIT",
        "-- prefix\rEND",
    ] {
        let mut calls = fixture.calls().await;
        calls
            .query(
                "INSERT INTO tasks VALUES (2,'before boundary')",
                Value::Null,
            )
            .await
            .unwrap();
        assert!(
            calls
                .query(boundary, Value::Null)
                .await
                .unwrap_err()
                .contains("transaction boundaries"),
            "{boundary}"
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
    fixture.close().await;
}

#[tokio::test]
async fn caught_query_sql_errors_can_return_a_fallback_after_snapshot_rollback() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    for statement in ["SELECT 1/0", "SELECT * FROM missing_table"] {
        let transaction = fixture
            .control
            .begin_control_transaction(true)
            .await
            .unwrap();
        let mut calls = DatabaseHostCalls::new(transaction, DatabaseCapability::Query);
        assert!(calls.query(statement, Value::Null).await.is_err());
        calls
            .finish(true)
            .await
            .expect("a Query fallback should survive snapshot rollback");
    }
    assert!(fixture.rows().await.is_empty());
    fixture.close().await;
}

// Provisioned tenant regressions use their own fixture. The parameter Fixture
// above intentionally remains unchanged.
struct SyncFixture {
    admin: PgPool,
    pools: PoolRegistry,
    control: Option<ControlPlane>,
    route: gonvex_postgres::TenantRoute,
    schema: String,
}

impl SyncFixture {
    async fn new() -> Result<Option<Self>, String> {
        let url = match std::env::var("GONVEX_TEST_POSTGRES_URL") {
            Ok(url) => url,
            Err(std::env::VarError::NotPresent) => {
                eprintln!("PostgreSQL sync regression skipped: set GONVEX_TEST_POSTGRES_URL");
                return Ok(None);
            }
            Err(error) => return Err(error.to_string()),
        };
        let schema = format!("gonvex_sync_host_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .map_err(|error| error.to_string())?;
        if let Err(error) = sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
        {
            admin.close().await;
            return Err(error.to_string());
        }
        let pools = PoolRegistry::new(PoolLimits {
            max_total_connections: 1,
            max_connections_per_database: 1,
            max_idle_connections_per_database: 1,
            // Match the registry's 30-minute connection lifetime instead of
            // letting its default one-second idle timeout obscure reuse.
            idle_timeout: std::time::Duration::from_secs(30 * 60),
            ..PoolLimits::default()
        });
        let mut fixture = Self {
            admin,
            pools,
            control: None,
            route: gonvex_postgres::TenantRoute {
                project_id: schema.clone(),
                tenant_id: schema.clone(),
                database_url: String::new(),
            },
            schema,
        };
        let setup = fixture.setup(&url).await;
        if let Err(error) = setup {
            let cleanup = fixture.close().await;
            return Err(format!(
                "sync fixture setup failed: {error}; cleanup: {cleanup:?}"
            ));
        }
        Ok(Some(fixture))
    }

    async fn setup(&mut self, base_url: &str) -> Result<(), String> {
        let mut url = url::Url::parse(base_url).map_err(|error| error.to_string())?;
        // Preserve any supplied connection options, then select only this
        // schema. Use one canonical URL for both ControlPlane and TenantRoute.
        let pairs = url.query_pairs().into_owned().collect::<Vec<_>>();
        let mut options = String::new();
        url.set_query(None);
        for (key, value) in pairs {
            if key == "options" {
                options.push_str(&value);
                options.push(' ');
            } else {
                url.query_pairs_mut().append_pair(&key, &value);
            }
        }
        options.push_str(&format!("-csearch_path={}", self.schema));
        url.query_pairs_mut().append_pair("options", &options);
        self.route.database_url = url.to_string();
        sqlx::query(&format!(
            "CREATE TABLE \"{}\".tasks(id bigint PRIMARY KEY,title text NOT NULL)",
            self.schema
        ))
        .execute(&self.admin)
        .await
        .map_err(|error| error.to_string())?;
        let control = ControlPlane::connect(
            &self.route.database_url,
            self.pools.clone(),
            BTreeMap::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
        self.control = Some(control.clone());
        // Provisioning holds one admission permit. Its identity script,
        // migrations and feed install check out connections sequentially;
        // reference-integrity installation uses the already-held connection.
        control
            .provision_tenant_database(self.route.clone(), Vec::new())
            .await
            .map_err(|error| error.to_string())
    }

    async fn calls(&self, command: &str) -> Result<DatabaseHostCalls, String> {
        let control = self
            .control
            .as_ref()
            .ok_or("sync fixture is not provisioned")?;
        let mut transaction = control
            .begin_tenant_transaction(&self.route, false)
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .set_command_id(command)
            .await
            .map_err(|error| error.to_string())?;
        Ok(DatabaseHostCalls::new(
            transaction,
            DatabaseCapability::Reducer,
        ))
    }

    async fn backend_pid(&self, calls: &mut DatabaseHostCalls) -> Result<i64, String> {
        let result = calls
            .query(
                "SELECT pg_backend_pid() AS pid,current_schema() AS schema",
                Value::Null,
            )
            .await?;
        if result[0]["schema"] != self.schema {
            return Err(format!(
                "host transaction selected the wrong schema: {result:?}"
            ));
        }
        result[0]["pid"]
            .as_i64()
            .filter(|pid| *pid > 0)
            .ok_or_else(|| format!("host did not return a backend PID: {result:?}"))
    }

    async fn facts(&self) -> Result<SyncFacts, String> {
        let schema = &self.schema;
        // The admin pool is independent of the one-connection host pool and
        // every relation here is schema-qualified.
        let rows = sqlx::query_as::<_, (i64, String)>(&format!(
            "SELECT id,title FROM \"{schema}\".tasks ORDER BY id"
        ))
        .fetch_all(&self.admin)
        .await
        .map_err(|error| error.to_string())?;
        let changes = sqlx::query_scalar::<_, Value>(&format!(
            "SELECT to_jsonb(c) FROM \"{schema}\"._gonvex_sync_changes c ORDER BY event_id"
        ))
        .fetch_all(&self.admin)
        .await
        .map_err(|error| error.to_string())?;
        let transactions = sqlx::query_scalar::<_, Value>(&format!(
            "SELECT to_jsonb(t) FROM \"{schema}\"._gonvex_sync_transactions t ORDER BY revision"
        ))
        .fetch_all(&self.admin)
        .await
        .map_err(|error| error.to_string())?;
        let (revision, retained) = sqlx::query_as::<_, (i64, i64)>(&format!(
            "SELECT revision,retained_revision FROM \"{schema}\"._gonvex_sync_clock WHERE singleton"
        ))
        .fetch_one(&self.admin)
        .await
        .map_err(|error| error.to_string())?;
        let metadata = sqlx::query_scalar::<_, Value>(&format!(
            r#"SELECT jsonb_build_object(
                'stage',count(*) FILTER (WHERE p.proname='gonvex_sync_tasks_stage'
                    AND t.tgenabled='O' AND t.tgtype=29),
                'finalizer',count(*) FILTER (WHERE p.proname='gonvex_sync_finalize_transaction'
                    AND t.tgenabled='O' AND t.tgtype=29 AND t.tgdeferrable AND t.tginitdeferred),
                'bootstrap',(SELECT count(*) FROM "{schema}"._gonvex_sync_bootstrap
                    WHERE name IN ('infrastructure','table:tasks')))
               FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
               JOIN pg_namespace n ON n.oid=c.relnamespace
               JOIN pg_proc p ON p.oid=t.tgfoid
               WHERE n.nspname=$1 AND c.relname='tasks' AND NOT t.tgisinternal"#
        ))
        .bind(schema)
        .fetch_one(&self.admin)
        .await
        .map_err(|error| error.to_string())?;
        Ok(SyncFacts {
            rows,
            changes,
            transactions,
            revision,
            retained,
            metadata,
        })
    }

    async fn close(self) -> Result<(), String> {
        // Close the physical host connection, including its session GUC,
        // before dropping any functions or tables it might still reference.
        self.pools.close().await;
        drop(self.control);
        let result = sqlx::query(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string());
        self.admin.close().await;
        result
    }
}

#[derive(Debug)]
struct SyncFacts {
    rows: Vec<(i64, String)>,
    changes: Vec<Value>,
    transactions: Vec<Value>,
    revision: i64,
    retained: i64,
    metadata: Value,
}

impl SyncFacts {
    fn pristine(&self) -> bool {
        self.rows.is_empty()
            && self.changes.is_empty()
            && self.transactions.is_empty()
            && self.revision == 0
            && self.retained == 0
            && self.metadata == serde_json::json!({"stage":1,"finalizer":1,"bootstrap":2})
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

#[derive(Debug)]
struct SyncAttempt {
    query_error: Option<(String, String)>,
    finish: Result<(), String>,
    explicit_rollback: bool,
    transient: bool,
}

fn sync_postgres_error(error: &str) -> bool {
    // SQLx's Database variant has this display prefix. Host APIs return only
    // strings, so require it plus a sync-specific refusal below. An arbitrary
    // SQL/setup error or a transport error cannot count as a safe rejection.
    error.starts_with("error returned from database: ")
}

fn sync_guard_rejection(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    let sync = error.contains("gonvex.sync_revision")
        || error.contains("sync finaliz")
        || error.contains("sync guard")
        || error.contains("sync revision")
        || error.contains("set constraints")
        || error.contains("constraint mode");
    sync && [
        "not allowed",
        "must",
        "cannot",
        "invalid",
        "reserved",
        "reject",
        "unsafe",
        "unsupported",
        "prohibit",
        "denied",
        "interfer",
    ]
    .iter()
    .any(|word| error.contains(*word))
}

impl SyncAttempt {
    async fn run(mut calls: DatabaseHostCalls, statements: &[&str]) -> Self {
        let fault = calls.transient_fault();
        let mut query_error = None;
        for statement in statements {
            if let Err(error) = calls.query(statement, Value::Null).await {
                query_error = Some(((*statement).to_owned(), error));
                break;
            }
        }
        // A local admission refusal leaves the SQL transaction usable, so the
        // invocation must roll back explicitly. For PostgreSQL errors, still
        // ask finish(true) to prove that the caught error cannot become a
        // successful commit. Unexpected errors follow the same cleanup path
        // but are rejected by safe_rejection().
        let explicit_rollback = query_error
            .as_ref()
            .is_some_and(|(_, error)| !sync_postgres_error(error));
        let finish = calls.finish(!explicit_rollback).await;
        Self {
            query_error,
            finish,
            explicit_rollback,
            transient: fault.observed(),
        }
    }

    fn committed(&self) -> bool {
        self.query_error.is_none() && self.finish.is_ok() && !self.transient
    }

    fn safe_rejection(&self) -> bool {
        if self.transient {
            return false;
        }
        match &self.query_error {
            Some((_, error)) if sync_guard_rejection(error) => {
                if self.explicit_rollback {
                    self.finish.is_ok()
                } else {
                    self.finish.as_ref().err().is_some_and(|commit_error| {
                        sync_postgres_error(commit_error)
                            && (commit_error.contains("current transaction is aborted")
                                || sync_guard_rejection(commit_error))
                    })
                }
            }
            None => self
                .finish
                .as_ref()
                .err()
                .is_some_and(|error| sync_postgres_error(error) && sync_guard_rejection(error)),
            _ => false,
        }
    }
}

#[tokio::test]
async fn sync_finalization_late_write_has_no_unpublished_events() {
    let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
        return;
    };
    let evidence = async {
        let initial = fixture.facts().await?;
        let calls = fixture.calls("sync_guard_late_write").await?;
        let attempt = SyncAttempt::run(
            calls,
            &[
                "INSERT INTO tasks VALUES (1,'A')",
                "SET CONSTRAINTS ALL IMMEDIATE",
                "SET CONSTRAINTS ALL DEFERRED",
                "INSERT INTO tasks VALUES (2,'B')",
            ],
        )
        .await;
        let final_facts = fixture.facts().await?;
        Ok::<_, String>((initial, attempt, final_facts))
    }
    .await;
    // Collect evidence first and always close before any safety assertion,
    // including the expected baseline mismatch.
    let cleanup = fixture.close().await;
    cleanup.expect("sync fixture cleanup");
    let (initial, attempt, facts) = evidence.expect("sync regression database/setup failure");
    assert!(
        initial.pristine(),
        "sync fixture must start with zero counters and installed triggers: {initial:?}"
    );
    assert!(
        (attempt.committed() && facts.captures(&[(1, "A"), (2, "B")], "sync_guard_late_write"))
            || (attempt.safe_rejection() && facts.pristine()),
        "sync_finalization_late_write_has_no_unpublished_events: attempt={attempt:?}; facts={facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_session_guard_does_not_poison_next_transaction() {
    let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
        return;
    };
    let evidence = async {
        let initial = fixture.facts().await?;
        let mut calls = fixture.calls("sync_guard_poison").await?;
        let poison_pid = fixture.backend_pid(&mut calls).await?;
        let poison = SyncAttempt::run(
            calls,
            &["SELECT set_config('gonvex.sync_revision','1',false)"],
        )
        .await;
        let after_poison = fixture.facts().await?;
        let mut calls = fixture.calls("sync_guard_next_write").await?;
        let next_pid = fixture.backend_pid(&mut calls).await?;
        let next =
            SyncAttempt::run(calls, &["INSERT INTO tasks VALUES (1,'next transaction')"]).await;
        let final_facts = fixture.facts().await?;
        Ok::<_, String>((
            initial,
            poison_pid,
            poison,
            after_poison,
            next_pid,
            next,
            final_facts,
        ))
    }
    .await;
    let cleanup = fixture.close().await;
    cleanup.expect("sync fixture cleanup");
    let (initial, poison_pid, poison, after_poison, next_pid, next, facts) =
        evidence.expect("sync regression database/setup failure");
    assert!(
        initial.pristine(),
        "sync fixture must start with zero counters and installed triggers: {initial:?}"
    );
    assert_eq!(
        poison_pid, next_pid,
        "session regression requires physical host connection reuse"
    );
    assert!(
        (poison.committed() || poison.safe_rejection()) && after_poison.pristine(),
        "guard-only invocation must commit without data effects or explicitly reject and roll back: {poison:?}; {after_poison:?}"
    );
    // A guard-setting refusal may be safe, but ordinary later transactions
    // must still make progress on this same connection.
    assert!(
        next.committed() && facts.captures(&[(1, "next transaction")], "sync_guard_next_write"),
        "sync_finalization_session_guard_does_not_poison_next_transaction: poison_pid={poison_pid}; next_pid={next_pid}; poison={poison:?}; next={next:?}; facts={facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_edge_batches_capture_every_insert() {
    for early_flush in [false, true] {
        let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
            return;
        };
        let evidence = async {
            let initial = fixture.facts().await?;
            let mut statements = vec![
                "INSERT INTO tasks VALUES (1,'A'),(2,'B')",
                "INSERT INTO tasks VALUES (3,'C')",
            ];
            if early_flush {
                statements.push("SET CONSTRAINTS ALL IMMEDIATE");
            }
            let attempt =
                SyncAttempt::run(fixture.calls("sync_guard_batch").await?, &statements).await;
            Ok::<_, String>((initial, attempt, fixture.facts().await?))
        }
        .await;
        fixture.close().await.expect("sync fixture cleanup");
        let (initial, attempt, facts) = evidence.expect("sync regression database/setup failure");
        assert!(initial.pristine(), "{initial:?}");
        assert!(
            attempt.committed()
                && facts.captures(&[(1, "A"), (2, "B"), (3, "C")], "sync_guard_batch"),
            "early_flush={early_flush}; attempt={attempt:?}; facts={facts:?}"
        );
    }
}

#[tokio::test]
async fn sync_finalization_edge_rollback_before_flush_reopens_capture() {
    let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
        return;
    };
    let evidence = async {
        let initial = fixture.facts().await?;
        let attempt = SyncAttempt::run(
            fixture.calls("sync_guard_rollback_before").await?,
            &[
                "INSERT INTO tasks VALUES (1,'A')",
                "SAVEPOINT before_flush",
                "SET CONSTRAINTS ALL IMMEDIATE",
                "ROLLBACK TO SAVEPOINT before_flush",
                "RELEASE SAVEPOINT before_flush",
                "INSERT INTO tasks VALUES (2,'B')",
            ],
        )
        .await;
        Ok::<_, String>((initial, attempt, fixture.facts().await?))
    }
    .await;
    fixture.close().await.expect("sync fixture cleanup");
    let (initial, attempt, facts) = evidence.expect("sync regression database/setup failure");
    assert!(initial.pristine(), "{initial:?}");
    assert!(
        attempt.committed() && facts.captures(&[(1, "A"), (2, "B")], "sync_guard_rollback_before"),
        "attempt={attempt:?}; facts={facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_edge_rollback_after_flush_keeps_phase_closed() {
    let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
        return;
    };
    let evidence = async {
        let initial = fixture.facts().await?;
        let mut calls = fixture.calls("sync_guard_rollback_after").await?;
        let fault = calls.transient_fault();
        let probe = async {
            for statement in [
                "INSERT INTO tasks VALUES (1,'A')",
                "SET CONSTRAINTS ALL IMMEDIATE",
                "SET CONSTRAINTS ALL DEFERRED",
                "SAVEPOINT after_flush",
            ] {
                calls.query(statement, Value::Null).await?;
            }
            let mut rejections = Vec::new();
            // A second refused write after recovery proves ROLLBACK TO did
            // not reopen the phase that was closed before this savepoint.
            for statement in [
                "INSERT INTO tasks VALUES (2,'B')",
                "INSERT INTO tasks VALUES (3,'C')",
            ] {
                match calls.query(statement, Value::Null).await {
                    Err(error) if sync_postgres_error(&error) && sync_guard_rejection(&error) => {
                        rejections.push(error);
                    }
                    Err(error) => return Err(format!("unexpected error for {statement}: {error}")),
                    Ok(value) => {
                        return Err(format!("late write succeeded: {statement}: {value:?}"))
                    }
                }
                calls
                    .query("ROLLBACK TO SAVEPOINT after_flush", Value::Null)
                    .await?;
            }
            calls
                .query("RELEASE SAVEPOINT after_flush", Value::Null)
                .await?;
            Ok::<_, String>(rejections)
        }
        .await;
        // Unexpected errors or successes roll back explicitly, then fail
        // below after cleanup. Only the recognized SQL refusals may commit A.
        let finish = calls.finish(probe.is_ok()).await;
        Ok::<_, String>((
            initial,
            probe,
            finish,
            fault.observed(),
            fixture.facts().await?,
        ))
    }
    .await;
    fixture.close().await.expect("sync fixture cleanup");
    let (initial, probe, finish, transient, facts) =
        evidence.expect("sync regression database/setup failure");
    assert!(initial.pristine(), "{initial:?}");
    assert!(
        probe.as_ref().is_ok_and(|errors| errors.len() == 2)
            && finish.is_ok()
            && !transient
            && facts.captures(&[(1, "A")], "sync_guard_rollback_after"),
        "probe={probe:?}; finish={finish:?}; transient={transient}; facts={facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_edge_immediate_before_write_never_orphans_events() {
    let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
        return;
    };
    let evidence = async {
        let initial = fixture.facts().await?;
        let attempt = SyncAttempt::run(
            fixture.calls("sync_guard_initial_immediate").await?,
            &[
                "SET CONSTRAINTS ALL IMMEDIATE",
                "INSERT INTO tasks VALUES (1,'A'),(2,'B')",
                "INSERT INTO tasks VALUES (3,'C')",
            ],
        )
        .await;
        Ok::<_, String>((initial, attempt, fixture.facts().await?))
    }
    .await;
    fixture.close().await.expect("sync fixture cleanup");
    let (initial, attempt, facts) = evidence.expect("sync regression database/setup failure");
    assert!(initial.pristine(), "{initial:?}");
    // Do not assume the relative execution order of immediate row triggers.
    assert!(
        (attempt.committed()
            && facts.captures(
                &[(1, "A"), (2, "B"), (3, "C")],
                "sync_guard_initial_immediate"
            ))
            || (attempt.safe_rejection() && facts.pristine()),
        "attempt={attempt:?}; facts={facts:?}"
    );
}

#[tokio::test]
async fn sync_finalization_edge_guc_changes_cannot_reopen_phase() {
    for setting in [
        "SELECT set_config('gonvex.sync_revision','',true)",
        "SELECT set_config('gonvex.sync_revision','987654321',true)",
    ] {
        let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
            return;
        };
        let evidence = async {
            let initial = fixture.facts().await?;
            let attempt = SyncAttempt::run(
                fixture.calls("sync_guard_guc_after_flush").await?,
                &[
                    "INSERT INTO tasks VALUES (1,'A')",
                    "SET CONSTRAINTS ALL IMMEDIATE",
                    "SET CONSTRAINTS ALL DEFERRED",
                    setting,
                    "INSERT INTO tasks VALUES (2,'B')",
                ],
            )
            .await;
            Ok::<_, String>((initial, attempt, fixture.facts().await?))
        }
        .await;
        fixture.close().await.expect("sync fixture cleanup");
        let (initial, attempt, facts) = evidence.expect("sync regression database/setup failure");
        assert!(initial.pristine(), "{initial:?}");
        assert!(
            attempt.safe_rejection() && facts.pristine(),
            "setting={setting}; attempt={attempt:?}; facts={facts:?}"
        );
    }
}

#[tokio::test]
async fn sync_finalization_edge_deferred_trigger_late_write_fails_atomically() {
    let Some(fixture) = SyncFixture::new().await.expect("sync fixture setup") else {
        return;
    };
    let evidence = async {
        let schema = &fixture.schema;
        // Trusted fixture DDL uses the admin pool. Both objects belong only
        // to this schema and DROP SCHEMA CASCADE removes them on every path.
        let function_sql = format!(
            r#"CREATE FUNCTION "{schema}".sync_guard_commit_writer() RETURNS trigger
            LANGUAGE plpgsql AS $$
            DECLARE finalized_metadata jsonb;
            BEGIN
              SELECT to_jsonb(t) INTO finalized_metadata
                FROM "{schema}"._gonvex_sync_transactions t
                WHERE transaction_id=txid_current()::bigint;
              IF finalized_metadata IS NULL THEN
                RAISE EXCEPTION 'edge deferred trigger ran before finalization';
              END IF;
              BEGIN
                INSERT INTO "{schema}".tasks VALUES (2,'B');
              EXCEPTION WHEN SQLSTATE '55000' THEN
                -- Keep the actual metadata in the host-visible error even
                -- though COMMIT will roll back the entire transaction.
                RAISE EXCEPTION USING ERRCODE='55000',
                  MESSAGE=SQLERRM || '; edge deferred trigger metadata=' || finalized_metadata::text;
              END;
              RETURN NULL;
            END; $$"#
        );
        sqlx::query(&function_sql)
            .execute(&fixture.admin)
            .await
            .map_err(|error| error.to_string())?;
        let trigger_sql = format!(
            r#"CREATE CONSTRAINT TRIGGER zz_sync_guard_commit_writer
            AFTER INSERT ON "{schema}".tasks DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW WHEN (NEW.id=1)
            EXECUTE FUNCTION "{schema}".sync_guard_commit_writer()"#
        );
        sqlx::query(&trigger_sql)
            .execute(&fixture.admin)
            .await
            .map_err(|error| error.to_string())?;
        let trigger_metadata = sqlx::query_scalar::<_, Value>(&format!(
            r#"SELECT jsonb_build_object(
                'after_finalizer',t.tgname::text COLLATE "C">f.tgname::text COLLATE "C",
                'deferred',t.tgdeferrable AND t.tginitdeferred AND t.tgenabled='O',
                'trigger',pg_get_triggerdef(t.oid),
                'function',pg_get_functiondef(t.tgfoid),
                'finalizer',pg_get_triggerdef(f.oid),
                'finalizer_function',pg_get_functiondef(f.tgfoid))
               FROM pg_trigger t JOIN pg_trigger f ON f.tgrelid=t.tgrelid
               WHERE t.tgrelid='"{schema}".tasks'::regclass
                 AND t.tgname='zz_sync_guard_commit_writer'
                 AND f.tgname='gonvex_sync_tasks_finalize_trigger'"#
        ))
        .fetch_one(&fixture.admin)
        .await
        .map_err(|error| error.to_string())?;
        let initial = fixture.facts().await?;
        let attempt = SyncAttempt::run(
            fixture.calls("sync_guard_commit_trigger").await?,
            &["INSERT INTO tasks VALUES (1,'A')"],
        )
        .await;
        Ok::<_, String>((
            initial,
            trigger_metadata,
            function_sql,
            trigger_sql,
            attempt,
            fixture.facts().await?,
        ))
    }
    .await;
    fixture.close().await.expect("sync fixture cleanup");
    let (initial, trigger_metadata, function_sql, trigger_sql, attempt, facts) =
        evidence.expect("sync regression database/setup failure");
    assert!(initial.pristine(), "{initial:?}");
    let finalized_metadata = attempt
        .finish
        .as_ref()
        .err()
        .and_then(|error| error.split_once("; edge deferred trigger metadata="))
        .and_then(|(_, json)| serde_json::from_str::<Value>(json).ok());
    let trace = format!(
        "trigger_metadata={trigger_metadata:?}; function_sql={function_sql}; trigger_sql={trigger_sql}; attempt={attempt:?}; facts={facts:?}"
    );
    eprintln!("sync finalization deferred-trigger evidence: {trace}");
    assert!(
        trigger_metadata["after_finalizer"] == true
            && trigger_metadata["deferred"] == true
            && attempt.query_error.is_none()
            && attempt.safe_rejection()
            && finalized_metadata.as_ref().is_some_and(|meta| {
                meta["revision"] == 1
                    && meta["transaction_id"].as_i64().is_some_and(|txid| txid > 0)
                    && meta["origin_command_id"] == "sync_guard_commit_trigger"
                    && meta["root_command_id"] == "sync_guard_commit_trigger"
                    && meta["invocation_channel"] == "system"
                    && meta["root_invocation_channel"] == "system"
            })
            && facts.pristine(),
        "{trace}"
    );
}
