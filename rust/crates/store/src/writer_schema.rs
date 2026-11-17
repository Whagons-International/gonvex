//! Writer-only schema objects.
//!
//! SQLite parses every view and trigger of a file on every connection that
//! opens it. Triggers only run on writes, and every write to a tenant file
//! goes through its single writer connection, so a module can declare its
//! triggers (and views that only those triggers read) writer-only:
//!
//! ```sql
//! INSERT INTO _gonvex_writer_schema_rules(type, pattern) VALUES
//!   ('trigger', '%'), ('view', 'referenceExpected\_%');
//! ```
//!
//! After tenant migrations, matching objects move from the file's schema to
//! `_gonvex_writer_schema`, and the writer recreates them as TEMP objects on
//! its own connection (TEMP triggers fire for that connection's writes to
//! main tables and views). Readers then parse only tables, indexes, read
//! views and framework triggers. Framework objects (`_gonvex*`, `gonvex_*`)
//! and capture triggers never move. A writer that has not written for
//! `GONVEX_DB_WRITER_SCHEMA_IDLE_MS` drops its TEMP copies and recreates them
//! before its next use (`Database::writer` hooks in `registry`).
//!
//! Migrations always see the persistent form: before a batch with pending
//! migrations the objects are restored to the file's schema, and moved again
//! after it.

use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::{Connection, SqliteConnection};

use crate::DatabaseError;

const TABLES_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _gonvex_writer_schema_rules(
  type TEXT NOT NULL CHECK(type IN ('trigger','view')),
  pattern TEXT NOT NULL,
  PRIMARY KEY(type, pattern)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_writer_schema(
  seq INTEGER PRIMARY KEY,
  type TEXT NOT NULL CHECK(type IN ('trigger','view')),
  name TEXT NOT NULL UNIQUE,
  sql TEXT NOT NULL
) STRICT;
"#;

tokio::task_local! {
    /// Set while provisioning works on a tenant file: its writer defers the
    /// TEMP copies until a write needs them. Provisioning writes only
    /// framework tables, and pending migrations run with the objects
    /// restored to the file's schema.
    static SCHEMA_WORK: ();
}

/// Runs `work` with writer-only schema installs deferred (see [`SCHEMA_WORK`]).
pub(crate) async fn schema_work<F: std::future::Future>(work: F) -> F::Output {
    SCHEMA_WORK.scope((), work).await
}

/// Whether the current task is provisioning (installs are deferred).
pub(crate) fn deferred() -> bool {
    SCHEMA_WORK.try_with(|_| ()).is_ok()
}

static INSTALLS: AtomicU64 = AtomicU64::new(0);
static RELEASES: AtomicU64 = AtomicU64::new(0);

/// Writer-only schema installs and idle releases since start.
pub fn writer_schema_counters() -> (u64, u64) {
    (
        INSTALLS.load(Ordering::Relaxed),
        RELEASES.load(Ordering::Relaxed),
    )
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `CREATE TRIGGER|VIEW …` as stored in `sqlite_schema` (normalized: no TEMP,
/// no IF NOT EXISTS) to its idempotent TEMP form.
fn temp_sql(kind: &str, sql: &str) -> Result<String, DatabaseError> {
    let trimmed = sql.trim_start();
    let keyword = if kind == "view" { "VIEW" } else { "TRIGGER" };
    let mut words = trimmed.splitn(3, char::is_whitespace);
    let (create, object, rest) = (words.next(), words.next(), words.next());
    match (create, object, rest) {
        (Some(c), Some(o), Some(rest))
            if c.eq_ignore_ascii_case("CREATE") && o.eq_ignore_ascii_case(keyword) =>
        {
            Ok(format!("CREATE TEMP {keyword} IF NOT EXISTS {rest}"))
        }
        _ => Err(sqlx::Error::Protocol(format!(
            "writer-only {kind} has unexpected SQL: {}",
            trimmed.chars().take(80).collect::<String>()
        ))
        .into()),
    }
}

async fn stash_exists(connection: &mut SqliteConnection) -> Result<bool, DatabaseError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='_gonvex_writer_schema')",
    )
    .fetch_one(&mut *connection)
    .await?)
}

/// Creates the TEMP copies of `rows` (`(type, sql)` in stash order: a view
/// before the triggers created after it).
async fn create_temp(
    connection: &mut SqliteConnection,
    rows: &[(String, String)],
) -> Result<(), DatabaseError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut script = String::new();
    for (kind, sql) in rows {
        script.push_str(&temp_sql(kind, sql)?);
        script.push_str(";\n");
    }
    let mut transaction = connection.begin().await?;
    sqlx::Executor::execute(&mut *transaction, sqlx::raw_sql(&script)).await?;
    transaction.commit().await?;
    Ok(())
}

/// Recreates every stashed object as TEMP on this (writer) connection.
/// Idempotent. Returns the number of stashed objects.
pub(crate) async fn install(connection: &mut SqliteConnection) -> Result<usize, DatabaseError> {
    if !stash_exists(connection).await? {
        return Ok(0);
    }
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT type, sql FROM _gonvex_writer_schema ORDER BY seq")
            .fetch_all(&mut *connection)
            .await?;
    create_temp(connection, &rows).await?;
    if !rows.is_empty() {
        INSTALLS.fetch_add(1, Ordering::Relaxed);
    }
    Ok(rows.len())
}

/// Drops this connection's TEMP copies (triggers first). Their parsed
/// definitions are freed; [`install`] recreates them.
pub(crate) async fn release(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    let objects: Vec<(String, String)> = sqlx::query_as(
        "SELECT type, name FROM temp.sqlite_schema WHERE type IN ('trigger','view')
           AND name IN (SELECT name FROM main._gonvex_writer_schema)
         ORDER BY type='view', name",
    )
    .fetch_all(&mut *connection)
    .await?;
    if objects.is_empty() {
        return Ok(());
    }
    let mut script = String::new();
    for (kind, name) in &objects {
        let keyword = if kind == "view" { "VIEW" } else { "TRIGGER" };
        script.push_str(&format!("DROP {keyword} IF EXISTS temp.{};\n", quote(name)));
    }
    let mut transaction = connection.begin().await?;
    sqlx::Executor::execute(&mut *transaction, sqlx::raw_sql(&script)).await?;
    transaction.commit().await?;
    RELEASES.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Moves the stashed objects back into the file's schema so migrations see
/// (and may change or drop) them. Drops this connection's TEMP copies first:
/// an object must never exist in both forms, or its trigger would fire twice.
pub async fn restore(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    if !stash_exists(connection).await? {
        return Ok(());
    }
    release(connection).await?;
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT type, sql FROM _gonvex_writer_schema ORDER BY seq")
            .fetch_all(&mut *connection)
            .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let mut script = String::new();
    for (_, sql) in &rows {
        script.push_str(sql);
        script.push_str(";\n");
    }
    script.push_str("DELETE FROM _gonvex_writer_schema;\n");
    let mut transaction = connection.begin().await?;
    sqlx::Executor::execute(&mut *transaction, sqlx::raw_sql(&script)).await?;
    transaction.commit().await?;
    Ok(())
}

/// Moves the objects matching `_gonvex_writer_schema_rules` out of the file's
/// schema, creating their TEMP copies on this connection when it holds the
/// others (`installed`). A view stays when an object that remains in the
/// file's schema names it (a reader could use it). Returns the number of
/// objects moved.
pub(crate) async fn stash(
    connection: &mut SqliteConnection,
    installed: bool,
) -> Result<usize, DatabaseError> {
    crate::execute_script(&mut *connection, TABLES_SQL).await?;
    let candidates: Vec<(i64, String, String, String)> = sqlx::query_as(
        r#"SELECT s.rowid, s.type, s.name, s.sql FROM main.sqlite_schema s
           WHERE s.type IN ('trigger','view') AND s.sql IS NOT NULL
             AND s.name NOT LIKE '\_gonvex%' ESCAPE '\'
             AND s.name NOT LIKE 'gonvex\_%' ESCAPE '\'
             AND NOT (s.type = 'trigger' AND s.sql LIKE '%' || $1 || '(%')
             AND EXISTS (SELECT 1 FROM _gonvex_writer_schema_rules r
                         WHERE r.type = s.type AND s.name LIKE r.pattern ESCAPE '\')
           ORDER BY s.rowid"#,
    )
    .bind(crate::capture::CAPTURE_FUNCTION)
    .fetch_all(&mut *connection)
    .await?;
    if candidates.is_empty() {
        return Ok(0);
    }
    let mut moving: std::collections::BTreeSet<String> = candidates
        .iter()
        .map(|(_, _, name, _)| name.clone())
        .collect();
    // Remaining objects that mention a candidate view keep it in the file.
    let remaining: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, coalesce(sql,'') FROM main.sqlite_schema WHERE type IN ('view','trigger')",
    )
    .fetch_all(&mut *connection)
    .await?;
    loop {
        let kept: Vec<String> = candidates
            .iter()
            .filter(|(_, kind, name, _)| kind == "view" && moving.contains(name))
            .filter(|(_, _, view, _)| {
                remaining
                    .iter()
                    .any(|(name, sql)| !moving.contains(name) && mentions(sql, view))
            })
            .map(|(_, _, name, _)| name.clone())
            .collect();
        if kept.is_empty() {
            break;
        }
        for name in kept {
            moving.remove(&name);
        }
    }
    let moved: Vec<&(i64, String, String, String)> = candidates
        .iter()
        .filter(|(_, _, name, _)| moving.contains(name))
        .collect();
    if moved.is_empty() {
        return Ok(0);
    }
    let mut script = String::new();
    // Views first, so the TEMP copies are created in an order that also
    // reproduces them in the file's schema on restore.
    let ordered = moved
        .iter()
        .filter(|row| row.1 == "view")
        .chain(moved.iter().filter(|row| row.1 == "trigger"));
    for (_, kind, name, sql) in ordered.clone() {
        script.push_str(&format!(
            "INSERT INTO _gonvex_writer_schema(type,name,sql) VALUES('{kind}',{},{});\n",
            literal(name),
            literal(sql)
        ));
    }
    for (_, _, name, _) in moved.iter().filter(|row| row.1 == "trigger") {
        script.push_str(&format!("DROP TRIGGER main.{};\n", quote(name)));
    }
    for (_, _, name, _) in moved.iter().filter(|row| row.1 == "view") {
        script.push_str(&format!("DROP VIEW main.{};\n", quote(name)));
    }
    let mut transaction = connection.begin().await?;
    sqlx::Executor::execute(&mut *transaction, sqlx::raw_sql(&script)).await?;
    transaction.commit().await?;
    if installed {
        let rows: Vec<(String, String)> = ordered
            .map(|(_, kind, _, sql)| (kind.clone(), sql.clone()))
            .collect();
        create_temp(connection, &rows).await?;
    }
    Ok(moved.len())
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Whether `sql` names `name` as an identifier (quoted, bracketed or bare).
fn mentions(sql: &str, name: &str) -> bool {
    if sql.contains(&quote(name))
        || sql.contains(&format!("[{name}]"))
        || sql.contains(&format!("`{name}`"))
    {
        return true;
    }
    let bytes = sql.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    let mut from = 0;
    while let Some(at) = sql[from..].find(name) {
        let start = from + at;
        let end = start + name.len();
        let before = start == 0 || !ident(bytes[start - 1]);
        let after = end >= bytes.len() || !ident(bytes[end]);
        if before && after {
            return true;
        }
        from = start + 1;
    }
    false
}

/// Whether any tenant-scope migration of `migrations` is not yet recorded.
pub(crate) async fn migrations_pending(
    connection: &mut SqliteConnection,
    migrations: &[crate::SqlMigration],
) -> Result<bool, DatabaseError> {
    let recorded: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='gonvex_migrations')",
    )
    .fetch_one(&mut *connection)
    .await?;
    for migration in migrations
        .iter()
        .filter(|migration| migration.scope == crate::MigrationScope::Tenant)
    {
        if !recorded {
            return Ok(true);
        }
        let applied: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM gonvex_migrations WHERE name=$1)")
                .bind(&migration.name)
                .fetch_one(&mut *connection)
                .await?;
        if !applied {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_sql_rewrites_only_the_leading_keywords() {
        assert_eq!(
            temp_sql("trigger", "CREATE TRIGGER \"t\" AFTER INSERT ON x BEGIN SELECT 'CREATE TRIGGER'; END").unwrap(),
            "CREATE TEMP TRIGGER IF NOT EXISTS \"t\" AFTER INSERT ON x BEGIN SELECT 'CREATE TRIGGER'; END"
        );
        assert_eq!(
            temp_sql("view", "create view v AS SELECT 1").unwrap(),
            "CREATE TEMP VIEW IF NOT EXISTS v AS SELECT 1"
        );
        assert!(temp_sql(
            "view",
            "CREATE TRIGGER v AFTER INSERT ON x BEGIN SELECT 1; END"
        )
        .is_err());
    }

    #[test]
    fn mentions_matches_identifiers_not_prefixes() {
        assert!(mentions(
            "SELECT * FROM \"referenceExpected_tasks\" e",
            "referenceExpected_tasks"
        ));
        assert!(mentions(
            "SELECT * FROM referenceExpected_tasks",
            "referenceExpected_tasks"
        ));
        assert!(!mentions(
            "SELECT * FROM referenceExpected_tasksX",
            "referenceExpected_tasks"
        ));
        assert!(!mentions("SELECT 1", "referenceExpected_tasks"));
    }
}
