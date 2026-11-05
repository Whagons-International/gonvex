//! Actual indexed SQL execution for the normalized LOCAL binding subset.
//! This is an internal backend path, not yet a replica/WebSocket selector.
use gonvex_store::{permission_indexes as indexes, TenantSession, TenantTransaction};
use sqlx::Row;

use super::{
    compile::{self, Binding, CompiledSet, Expr},
    model::{ExactPermissionSet, PermissionScope},
};

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("unsupported or unready permission primitive: {0}")]
    Rejected(&'static str),
    #[error(transparent)]
    Store(#[from] gonvex_store::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

#[derive(Clone)]
pub(super) struct NormalizedBinding {
    pub scope_id: i64,
    pub required_generation: i64,
    pub scope: PermissionScope,
    pub universe: String,
    pub expression: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ReadIdentity {
    pub exact: ExactPermissionSet,
    pub generation: i64,
    pub change_serial: i64,
    pub source_revision: i64,
    pub principal: String,
}

pub(super) struct Prepared {
    pub compiled: CompiledSet,
    pub identity: ReadIdentity,
    pub scope_id: i64,
    pub table: String,
}

/// Must be called inside the SAME transaction used to execute `compiled`.
/// Allowing pending is confined to the writer's post-batch differential read.
pub(super) async fn prepare(
    tx: &mut TenantTransaction,
    session: &TenantSession,
    binding: &NormalizedBinding,
    writer_pending: bool,
) -> Result<Prepared, Error> {
    if session.is_anonymous() || session.developer_session.is_some() {
        return Err(Error::Rejected(
            "normalized binding requires a persisted active member",
        ));
    }
    if binding.scope.project != session.route.project_id
        || binding.scope.project != session.identity.project_id
        || binding.scope.tenant != session.route.tenant_id
        || binding.scope.semantics_version != "normalized-atoms-binary-v1"
    {
        return Err(Error::Rejected(
            "scope or supported equality semantics mismatch",
        ));
    }
    let policy = compile::policy_version(&binding.universe, &binding.expression)
        .map_err(|_| Error::Rejected("unsupported normalized policy"))?;
    if binding.scope.policy_version != policy
        || binding.scope.projection_version != "keys-binary-v1"
    {
        return Err(Error::Rejected(
            "policy identity or key-only projection binding mismatch",
        ));
    }
    let actual = indexes::require_local(tx.transaction()).await?;
    let expected = std::fs::canonicalize(gonvex_store::database_path(&session.route.database_url))
        .map_err(|_| Error::Rejected("session database path is not a local fixture"))?;
    if actual != expected {
        return Err(Error::Rejected("transaction belongs to another database"));
    }
    let state = indexes::state(tx.transaction(), binding.scope_id).await?;
    let clock = sqlx::query("SELECT epoch,revision FROM _gonvex_sync_clock WHERE singleton=1")
        .fetch_one(&mut **tx.transaction())
        .await?;
    let epoch: String = clock.try_get("epoch")?;
    let revision: i64 = clock.try_get("revision")?;
    let canonical_scope = binding
        .scope
        .canonical()
        .map_err(|_| Error::Rejected("invalid scope"))?;
    let ready =
        state.phase == "ready" || (writer_pending && !tx.read_only() && state.phase == "pending");
    if !ready
        || state.generation <= 0
        || state.generation != binding.required_generation
        || state.covered_revision != revision
        || state.epoch != epoch
        || binding.scope.epoch != epoch
        || state.canonical_scope != canonical_scope
        || state.table != binding.scope.table
        || state.instance_id != binding.scope.database_instance
        || indexes::instance(tx.transaction()).await? != state.instance_id
    {
        return Err(Error::Rejected(
            "index incomplete, stale or from another generation/scope",
        ));
    }
    let incomplete: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_permission_sources WHERE scope_id=?1 AND generation=?2 AND complete=0)")
        .bind(binding.scope_id).bind(state.generation).fetch_one(&mut **tx.transaction()).await?;
    if incomplete != 0 {
        return Err(Error::Rejected("normalized index backfill is incomplete"));
    }
    // Identity comes from the authoritative membership row in this snapshot,
    // never a client argument, role label or cached session Member value.
    let principal: String =
        sqlx::query_scalar("SELECT id FROM members WHERE account_id=?1 AND status='active'")
            .bind(&session.identity.account.id)
            .fetch_optional(&mut **tx.transaction())
            .await?
            .ok_or(Error::Rejected("active membership is required"))?;
    let keys: Vec<Vec<u8>> = sqlx::query_scalar("SELECT a.canonical_key FROM _gonvex_permission_set_atoms pa JOIN _gonvex_permission_atoms a ON a.scope_id=pa.scope_id AND a.generation=pa.generation AND a.atom_id=pa.atom_id WHERE pa.scope_id=?1 AND pa.generation=?2 AND pa.set_id=?3 ORDER BY a.canonical_key")
        .bind(binding.scope_id).bind(state.generation).bind(&principal).fetch_all(&mut **tx.transaction()).await?;
    let exact = ExactPermissionSet::from_keys(&binding.scope, keys)
        .map_err(|_| Error::Rejected("invalid exact permission set"))?;
    let compiled = compile::compile(&binding.universe, &binding.expression)
        .map_err(|_| Error::Rejected("unsupported normalized scope expression"))?;
    Ok(Prepared {
        compiled,
        identity: ReadIdentity {
            exact,
            generation: state.generation,
            change_serial: state.change_serial,
            source_revision: revision,
            principal,
        },
        scope_id: binding.scope_id,
        table: state.table,
    })
}

pub(super) fn bind<'q>(
    prepared: &'q Prepared,
    sql: &'q str,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    let mut query = sqlx::query(sql)
        .bind(prepared.scope_id)
        .bind(&prepared.table)
        .bind(prepared.identity.generation)
        .bind(&prepared.identity.principal);
    for value in &prepared.compiled.extra_bindings {
        query = match value {
            Binding::Text(value) => query.bind(value),
            Binding::Blob(value) => query.bind(value),
        };
    }
    query
}

/// Returned keys are binary-ordered/deduplicated normalized relation IDs only.
/// Collection projection, arbitrary value ordering, windows and live delivery
/// are deliberately not claimed by this initial internal read path.
pub(super) async fn select_keys(
    tx: &mut TenantTransaction,
    session: &TenantSession,
    binding: &NormalizedBinding,
) -> Result<(ReadIdentity, Vec<String>), Error> {
    let prepared = prepare(tx, session, binding, false).await?;
    let sql = format!(
        "SELECT row_key FROM ({}) ORDER BY row_key COLLATE BINARY",
        prepared.compiled.sql
    );
    let rows = bind(&prepared, &sql)
        .fetch_all(&mut **tx.transaction())
        .await?;
    let keys = rows
        .into_iter()
        .map(|row| row.try_get(0))
        .collect::<Result<Vec<String>, _>>()?;
    Ok((prepared.identity, keys))
}
