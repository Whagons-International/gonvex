//! Exact normalized visibility differences in ONE writer transaction.
//! Initial correctness baseline evaluates the full finite scope in SQL before
//! and after. It does not claim small candidate sets or cheap global changes.
use gonvex_store::{permission_indexes as indexes, TenantSession, TenantTransaction};
use sqlx::Row;

use super::query::{self, Error, NormalizedBinding, ReadIdentity};

pub(super) struct VisibilityDelta {
    pub before: ReadIdentity,
    pub after: ReadIdentity,
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

/// Supports only normalized fact/grant replacements. Application-table writes,
/// group expansion and parent binding maintenance require explicit adapters;
/// they are not silently inferred by this API. Membership revocation remains
/// authoritative admission failure rather than a guessed empty principal.
pub(super) async fn apply_and_diff(
    tx: &mut TenantTransaction,
    session: &TenantSession,
    binding: &NormalizedBinding,
    changes: &[indexes::SourceChange],
) -> Result<VisibilityDelta, Error> {
    if tx.read_only() {
        return Err(Error::Rejected("permission diff requires a writer"));
    }
    let name = format!(
        "_gonvex_permission_before_{}",
        uuid::Uuid::new_v4().simple()
    );
    let result = run(tx, session, binding, changes, &name).await;
    let cleanup = sqlx::query(&format!("DROP TABLE IF EXISTS temp.{name}"))
        .execute(&mut **tx.transaction())
        .await;
    match (result, cleanup) {
        (Ok(delta), Ok(_)) => Ok(delta),
        (Err(error), _) => {
            tx.mark_failed("permission diff failed");
            Err(error)
        }
        (_, Err(error)) => {
            tx.mark_failed("permission diff cleanup failed");
            Err(error.into())
        }
    }
}

async fn run(
    tx: &mut TenantTransaction,
    session: &TenantSession,
    binding: &NormalizedBinding,
    changes: &[indexes::SourceChange],
    before_table: &str,
) -> Result<VisibilityDelta, Error> {
    let before = query::prepare(tx, session, binding, false).await?;
    // Save the true pre-state BEFORE any grant/row mutation. SQLite holds keys,
    // not cloned JSON rows; source provenance stays in the persistent indexes.
    sqlx::query(&format!(
        "CREATE TEMP TABLE {before_table}(row_key TEXT PRIMARY KEY COLLATE BINARY) WITHOUT ROWID"
    ))
    .execute(&mut **tx.transaction())
    .await?;
    let capture = format!(
        "INSERT INTO temp.{before_table}(row_key) {}",
        before.compiled.sql
    );
    query::bind(&before, &capture)
        .execute(&mut **tx.transaction())
        .await?;
    indexes::apply(tx, binding.scope_id, changes).await?;
    indexes::seal(tx, binding.scope_id).await?;
    let after = query::prepare(tx, session, binding, true).await?;
    let add_sql = format!("SELECT row_key FROM (SELECT row_key FROM ({}) EXCEPT SELECT row_key FROM temp.{before_table}) ORDER BY row_key COLLATE BINARY", after.compiled.sql);
    let remove_sql = format!("SELECT row_key FROM (SELECT row_key FROM temp.{before_table} EXCEPT SELECT row_key FROM ({})) ORDER BY row_key COLLATE BINARY", after.compiled.sql);
    let added = query::bind(&after, &add_sql)
        .fetch_all(&mut **tx.transaction())
        .await?
        .into_iter()
        .map(|row| row.try_get(0))
        .collect::<Result<Vec<String>, _>>()?;
    let removed = query::bind(&after, &remove_sql)
        .fetch_all(&mut **tx.transaction())
        .await?
        .into_iter()
        .map(|row| row.try_get(0))
        .collect::<Result<Vec<String>, _>>()?;
    Ok(VisibilityDelta {
        before: before.identity,
        after: after.identity,
        added,
        removed,
    })
}
