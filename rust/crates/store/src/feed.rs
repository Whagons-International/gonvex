//! Change capture and revisioning on SQLite.
//!
//! Row triggers stage changes with `revision IS NULL`. The committing
//! transaction assigns one revision to all of them before `COMMIT`
//! ([`finalize`]), and the registry publishes it after `COMMIT`. One writer
//! connection per database means every staged row belongs to the committing
//! transaction.

use serde_json::json;
use sqlx::{Row, SqliteConnection};

use crate::rows::{json_object_expr, quote_identifier, ColumnInfo};
use crate::DatabaseError;

pub const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

pub const CHANGE_FEED_CHANNEL: &str = "gonvex_change_feed";

/// Infrastructure every tenant/project database carries.
pub const SYNC_INFRASTRUCTURE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _gonvex_sync_clock (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  epoch TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0,
  retained_revision INTEGER NOT NULL DEFAULT 0
);
INSERT INTO _gonvex_sync_clock (singleton, epoch, revision)
VALUES (1, lower(hex(randomblob(16))), 0) ON CONFLICT (singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS _gonvex_sync_changes (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  transaction_id INTEGER NOT NULL DEFAULT 0,
  revision INTEGER,
  ordinal INTEGER,
  command_id TEXT,
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  old_value JSON,
  new_value JSON,
  changed_columns JSON NOT NULL DEFAULT '[]',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_revision
  ON _gonvex_sync_changes (revision, ordinal) WHERE revision IS NOT NULL;
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_pending
  ON _gonvex_sync_changes (event_id) WHERE revision IS NULL;
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_created_at
  ON _gonvex_sync_changes (created_at);
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_command
  ON _gonvex_sync_changes (command_id, revision) WHERE command_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS _gonvex_sync_transactions (
  revision INTEGER PRIMARY KEY,
  transaction_id INTEGER NOT NULL DEFAULT 0,
  root_command_id TEXT NOT NULL,
  origin_command_id TEXT NOT NULL,
  root_invocation_channel TEXT NOT NULL DEFAULT 'system',
  invocation_channel TEXT NOT NULL,
  actor_account_id TEXT,
  actor_member_id TEXT,
  on_behalf_of_member_id TEXT,
  agent_execution_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS _gonvex_action_outbox (
  id TEXT PRIMARY KEY,
  action_path TEXT NOT NULL,
  args JSON NOT NULL,
  actor_user_id TEXT,
  actor_email TEXT,
  provenance JSON NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  locked_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_pending
  ON _gonvex_action_outbox (available_at, created_at) WHERE status = 'pending';
"#;

/// Provenance recorded with a revision (replaces the `gonvex.*` GUCs).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Provenance {
    pub command_id: Option<String>,
    pub root_command_id: Option<String>,
    pub root_channel: Option<String>,
    pub channel: Option<String>,
    pub actor_account_id: Option<String>,
    pub actor_member_id: Option<String>,
    pub on_behalf_of_member_id: Option<String>,
    pub agent_execution_id: Option<String>,
}

/// A committed revision, published after `COMMIT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitNotice {
    pub epoch: String,
    pub revision: u64,
    pub tables: Vec<String>,
}

impl CommitNotice {
    pub fn payload(&self) -> String {
        let payload =
            json!({"epoch": self.epoch, "revision": self.revision, "tables": self.tables});
        let text = payload.to_string();
        if text.len() > 7000 {
            json!({"epoch": self.epoch, "revision": self.revision}).to_string()
        } else {
            text
        }
    }
}

/// Assigns one revision to the transaction's staged changes. Returns `None`
/// when the transaction changed no tracked rows, or when the database has no
/// change feed (the Control Plane).
pub async fn finalize(
    connection: &mut SqliteConnection,
    provenance: &Provenance,
) -> Result<Option<CommitNotice>, DatabaseError> {
    let has_feed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_gonvex_sync_changes')",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !has_feed {
        return Ok(None);
    }
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL)",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !pending {
        return Ok(None);
    }
    let clock = sqlx::query(
        "UPDATE _gonvex_sync_clock SET revision = revision + 1 WHERE singleton = 1 RETURNING revision, epoch",
    )
    .fetch_one(&mut *connection)
    .await?;
    let revision: i64 = clock.get(0);
    let epoch: String = clock.get(1);
    sqlx::query(
        r#"UPDATE _gonvex_sync_changes
           SET revision = $1, transaction_id = $1, ordinal = ranked.row_ordinal, command_id = $2
           FROM (SELECT event_id, row_number() OVER (ORDER BY event_id) AS row_ordinal
                 FROM _gonvex_sync_changes WHERE revision IS NULL) AS ranked
           WHERE _gonvex_sync_changes.event_id = ranked.event_id"#,
    )
    .bind(revision)
    .bind(provenance.command_id.as_deref().filter(|id| !id.is_empty()))
    .execute(&mut *connection)
    .await?;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT table_name FROM _gonvex_sync_changes WHERE revision = $1 ORDER BY table_name",
    )
    .bind(revision)
    .fetch_all(&mut *connection)
    .await?;
    let command = provenance.command_id.clone().unwrap_or_default();
    let root = provenance
        .root_command_id
        .clone()
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| command.clone());
    let channel = provenance
        .channel
        .clone()
        .filter(|channel| !channel.is_empty())
        .unwrap_or_else(|| "system".to_owned());
    let root_channel = provenance
        .root_channel
        .clone()
        .filter(|channel| !channel.is_empty())
        .unwrap_or_else(|| channel.clone());
    let empty_none = |value: &Option<String>| value.clone().filter(|value| !value.is_empty());
    sqlx::query(
        r#"INSERT INTO _gonvex_sync_transactions
           (revision, transaction_id, root_command_id, origin_command_id, root_invocation_channel,
            invocation_channel, actor_account_id, actor_member_id, on_behalf_of_member_id, agent_execution_id)
           VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (revision) DO NOTHING"#,
    )
    .bind(revision)
    .bind(root)
    .bind(command)
    .bind(root_channel)
    .bind(channel)
    .bind(empty_none(&provenance.actor_account_id))
    .bind(empty_none(&provenance.actor_member_id))
    .bind(empty_none(&provenance.on_behalf_of_member_id))
    .bind(empty_none(&provenance.agent_execution_id))
    .execute(&mut *connection)
    .await?;
    Ok(Some(CommitNotice {
        epoch,
        revision: revision as u64,
        tables,
    }))
}

/// Columns per `json_array` call and per OR group: bundled SQLite allows 127
/// function arguments, 500 compound-select terms and expression depth 1000.
const COLUMNS_PER_GROUP: usize = 100;

/// JSON array of the names of columns an UPDATE changed, in column order.
pub fn changed_columns_expr(columns: &[ColumnInfo]) -> String {
    if columns.is_empty() {
        return "'[]'".to_owned();
    }
    let groups = columns
        .chunks(COLUMNS_PER_GROUP)
        .enumerate()
        .map(|(index, group)| {
            let cases = group
                .iter()
                .map(|column| {
                    let name = quote_identifier(&column.name);
                    format!(
                        "CASE WHEN OLD.{name} IS NOT NEW.{name} THEN '{}' END",
                        column.name.replace('\'', "''")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let offset = index * COLUMNS_PER_GROUP;
            format!("SELECT key + {offset} AS position, value FROM json_each(json_array({cases}))")
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    format!(
        "(SELECT json_group_array(value) FROM (SELECT value FROM ({groups}) WHERE value IS NOT NULL ORDER BY position))"
    )
}

/// `OLD.c IS NOT NEW.c OR …` as a shallow tree of OR groups.
pub fn any_column_changed_expr(columns: &[ColumnInfo]) -> String {
    if columns.is_empty() {
        return "0".to_owned();
    }
    columns
        .chunks(COLUMNS_PER_GROUP)
        .map(|group| {
            let terms = group
                .iter()
                .map(|column| {
                    let name = quote_identifier(&column.name);
                    format!("OLD.{name} IS NOT NEW.{name}")
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            format!("({terms})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Trigger names for one table's change capture.
pub fn capture_trigger_names(table: &str) -> [String; 3] {
    ["insert", "update", "delete"].map(|operation| format!("_gonvex_sync_{table}_{operation}"))
}

/// `AFTER INSERT/UPDATE/DELETE` triggers that stage row images for `table`.
/// `key` is the primary-key column used as `row_id`. Updates that change no
/// column are not staged.
pub fn capture_triggers_sql(table: &str, key: &str, columns: &[ColumnInfo]) -> String {
    let quoted_table = quote_identifier(table);
    let literal_table = table.replace('\'', "''");
    let kinds = columns
        .iter()
        .map(|column| (column.name.clone(), column.kind()))
        .collect::<Vec<_>>();
    let new_image = json_object_expr("NEW", &kinds);
    let old_image = json_object_expr("OLD", &kinds);
    let key_new = format!("CAST(NEW.{} AS TEXT)", quote_identifier(key));
    let key_old = format!("CAST(OLD.{} AS TEXT)", quote_identifier(key));
    let changed = changed_columns_expr(columns);
    let any_change = any_column_changed_expr(columns);
    let [insert, update, delete] = capture_trigger_names(table).map(|name| quote_identifier(&name));
    format!(
        r#"DROP TRIGGER IF EXISTS {insert};
DROP TRIGGER IF EXISTS {update};
DROP TRIGGER IF EXISTS {delete};
CREATE TRIGGER {insert} AFTER INSERT ON {quoted_table} BEGIN
  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, old_value, new_value, changed_columns)
  VALUES ('{literal_table}', {key_new}, 'insert', NULL, {new_image}, '[]');
END;
CREATE TRIGGER {update} AFTER UPDATE ON {quoted_table} WHEN {any_change} BEGIN
  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, old_value, new_value, changed_columns)
  VALUES ('{literal_table}', {key_new}, 'update', {old_image}, {new_image}, {changed});
END;
CREATE TRIGGER {delete} AFTER DELETE ON {quoted_table} BEGIN
  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, old_value, new_value, changed_columns)
  VALUES ('{literal_table}', {key_old}, 'delete', {old_image}, NULL, '[]');
END;
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::table_columns;
    use sqlx::Connection;

    #[tokio::test]
    async fn finalize_assigns_one_revision_with_typed_row_images() {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(SYNC_INFRASTRUCTURE_SQL)
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::raw_sql(
            r#"CREATE TABLE tasks ("_id" TEXT PRIMARY KEY, title TEXT, done BOOLEAN, meta JSON, n BIGINT)"#,
        )
        .execute(&mut connection)
        .await
        .unwrap();
        let columns = table_columns(&mut connection, "tasks").await.unwrap();
        sqlx::raw_sql(&capture_triggers_sql("tasks", "_id", &columns))
            .execute(&mut connection)
            .await
            .unwrap();
        let mut tx = connection.begin().await.unwrap();
        sqlx::query(r#"INSERT INTO tasks VALUES ('a', 'A', 1, '{"x":[1]}', 5)"#)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(r#"UPDATE tasks SET done = 0 WHERE "_id" = 'a'"#)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(r#"UPDATE tasks SET done = 0 WHERE "_id" = 'a'"#)
            .execute(&mut *tx)
            .await
            .unwrap();
        let notice = finalize(
            &mut tx,
            &Provenance {
                command_id: Some("cmd-1".into()),
                ..Provenance::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(notice.revision, 1);
        assert_eq!(notice.tables, vec!["tasks".to_owned()]);
        let rows = sqlx::query(
            "SELECT operation, ordinal, new_value, changed_columns, command_id FROM _gonvex_sync_changes ORDER BY event_id",
        )
        .fetch_all(&mut connection)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "a no-op update is not staged");
        let image: serde_json::Value = serde_json::from_str(&rows[0].get::<String, _>(2)).unwrap();
        assert_eq!(
            image,
            json!({"_id":"a","title":"A","done":true,"meta":{"x":[1]},"n":5})
        );
        assert_eq!(rows[1].get::<String, _>(3), r#"["done"]"#);
        assert_eq!(rows[1].get::<i64, _>(1), 2);
        assert_eq!(rows[1].get::<String, _>(4), "cmd-1");
        let mut tx = connection.begin().await.unwrap();
        assert!(finalize(&mut tx, &Provenance::default())
            .await
            .unwrap()
            .is_none());
    }
}
