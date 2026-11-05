//! Independent normalized permission indexes for owned LOCAL fixtures only.
//! No normal provisioning path installs this schema. Row/group/role adapters,
//! automatic application-table maintenance and live-feed publication are not
//! implemented by this normalized relation store.
use std::path::{Path, PathBuf};

use sqlx::{Row, SqliteConnection};

use crate::{DatabaseError, TenantTransaction};

pub const LOCAL_FIXTURE_ROOT: &str = "/mnt/data/work/gonvex-permission-fixtures";

fn invalid(message: &str) -> DatabaseError {
    DatabaseError::Decode(message.into())
}

/// Check the ACTUAL connection path, not a caller-supplied route or URL. This
/// deliberately excludes in-memory, staging and production databases.
pub async fn require_local(connection: &mut SqliteConnection) -> Result<PathBuf, DatabaseError> {
    if std::env::var("GONVEX_PERMISSION_PRIMITIVES_LOCAL").as_deref() != Ok("1") {
        return Err(invalid("local permission primitive opt-in is required"));
    }
    let path: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name='main'")
            .fetch_one(&mut *connection)
            .await?;
    let path = std::fs::canonicalize(path).map_err(|e| DatabaseError::Io(e.to_string()))?;
    let root = std::fs::canonicalize(Path::new(LOCAL_FIXTURE_ROOT))
        .map_err(|e| DatabaseError::Io(e.to_string()))?;
    if root != Path::new(LOCAL_FIXTURE_ROOT)
        || path == root
        || !path.starts_with(&root)
        || !path.is_file()
    {
        return Err(invalid(
            "permission primitives require a dedicated local fixture database",
        ));
    }
    Ok(path)
}

#[cfg(unix)]
fn physical_key(path: &Path) -> Result<String, DatabaseError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path).map_err(|e| DatabaseError::Io(e.to_string()))?;
    Ok(format!("{}:{}", metadata.dev(), metadata.ino()))
}
#[cfg(not(unix))]
fn physical_key(_path: &Path) -> Result<String, DatabaseError> {
    Err(invalid(
        "local permission fixture identity requires Unix file identity",
    ))
}

async fn writer(tx: &mut TenantTransaction) -> Result<(), DatabaseError> {
    if tx.read_only() {
        return Err(invalid(
            "permission index mutation requires a writer transaction",
        ));
    }
    require_local(tx.transaction()).await?;
    Ok(())
}

/// Explicit fixture setup; NEVER invoked by ordinary store provisioning.
async fn install_local_inner(tx: &mut TenantTransaction) -> Result<String, DatabaseError> {
    writer(tx).await?;
    sqlx::raw_sql(include_str!("permission_indexes/schema.sql"))
        .execute(&mut **tx.transaction())
        .await?;
    sqlx::query("INSERT INTO _gonvex_permission_identity(singleton,instance_id,physical_key) VALUES(1,?1,?2) ON CONFLICT DO NOTHING")
        .bind(uuid::Uuid::new_v4().to_string()).bind(physical_key(&require_local(tx.transaction()).await?)?).execute(&mut **tx.transaction()).await?;
    // Every mutation dirties readiness even when a caller bypasses the helper.
    // Sealing is explicit after the final operation in a supported batch.
    for table in [
        "_gonvex_permission_atoms",
        "_gonvex_permission_supports",
        "_gonvex_permission_memberships",
        "_gonvex_permission_row_supports",
        "_gonvex_permission_row_atom_supports",
    ] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            let image = if operation == "DELETE" { "OLD" } else { "NEW" };
            let name = format!("{table}_dirty_{}", operation.to_ascii_lowercase());
            let mut sql = format!("CREATE TRIGGER IF NOT EXISTS {name} AFTER {operation} ON {table} BEGIN UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id={image}.scope_id;");
            if operation == "UPDATE" {
                sql.push_str(" UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id=OLD.scope_id AND OLD.scope_id<>NEW.scope_id;");
            }
            sql.push_str(" END;");
            sqlx::raw_sql(&sql).execute(&mut **tx.transaction()).await?;
        }
    }
    // Fact triggers also clear the source's completion receipt. An incomplete
    // replacement cannot be promoted by merely toggling scope readiness.
    for (table, kind) in [
        ("_gonvex_permission_supports", "grant"),
        ("_gonvex_permission_memberships", "membership"),
        ("_gonvex_permission_row_supports", "row"),
        ("_gonvex_permission_row_atom_supports", "row"),
    ] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            let image = if operation == "DELETE" { "OLD" } else { "NEW" };
            if operation != "DELETE" {
                let guard = format!("CREATE TRIGGER IF NOT EXISTS {table}_declared_{} BEFORE {operation} ON {table} BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM _gonvex_permission_sources s WHERE s.scope_id=NEW.scope_id AND s.generation=NEW.generation AND s.kind='{kind}' AND s.source_id=NEW.source_id) THEN RAISE(ABORT,'undeclared normalized permission source') END; END;", operation.to_ascii_lowercase());
                sqlx::raw_sql(&guard)
                    .execute(&mut **tx.transaction())
                    .await?;
            }
            let name = format!("{table}_source_{}", operation.to_ascii_lowercase());
            let mut sql = format!("CREATE TRIGGER IF NOT EXISTS {name} AFTER {operation} ON {table} BEGIN UPDATE _gonvex_permission_sources SET complete=0 WHERE scope_id={image}.scope_id AND generation={image}.generation AND kind='{kind}' AND source_id={image}.source_id;");
            if operation == "UPDATE" {
                sql.push_str(&format!(" UPDATE _gonvex_permission_sources SET complete=0 WHERE scope_id=OLD.scope_id AND generation=OLD.generation AND kind='{kind}' AND source_id=OLD.source_id;"));
            }
            sql.push_str(" END;");
            sqlx::raw_sql(&sql).execute(&mut **tx.transaction()).await?;
        }
    }
    sqlx::raw_sql("CREATE TRIGGER IF NOT EXISTS _gonvex_permission_atom_immutable BEFORE UPDATE ON _gonvex_permission_atoms WHEN OLD.canonical_key IS NOT NEW.canonical_key OR OLD.atom_id IS NOT NEW.atom_id OR OLD.scope_id IS NOT NEW.scope_id OR OLD.generation IS NOT NEW.generation BEGIN SELECT RAISE(ABORT,'permission atom identities are immutable'); END;")
        .execute(&mut **tx.transaction()).await?;
    instance(tx.transaction()).await
}

pub async fn instance(connection: &mut SqliteConnection) -> Result<String, DatabaseError> {
    let path = require_local(connection).await?;
    let row=sqlx::query("SELECT instance_id,physical_key FROM _gonvex_permission_identity WHERE singleton=1 AND schema_version=1")
        .fetch_one(connection).await?;
    let installed_key: String = row.try_get("physical_key")?;
    if installed_key != physical_key(&path)? {
        return Err(invalid(
            "copied/replaced permission database requires an explicit new instance and rebuild",
        ));
    }
    Ok(row.try_get("instance_id")?)
}

/// Scope key is canonical bytes from the runtime's typed identity, including
/// project/physical instance/tenant/epoch/table/collection/policy semantics.
async fn create_scope_inner(
    tx: &mut TenantTransaction,
    canonical: &[u8],
    instance_id: &str,
    epoch: &str,
    table: &str,
) -> Result<i64, DatabaseError> {
    writer(tx).await?;
    let actual = instance(tx.transaction()).await?;
    let actual_epoch: String =
        sqlx::query_scalar("SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1")
            .fetch_one(&mut **tx.transaction())
            .await?;
    if canonical.is_empty() || actual != instance_id || actual_epoch != epoch || table.is_empty() {
        return Err(invalid("permission scope identity mismatch"));
    }
    let id = sqlx::query_scalar("INSERT INTO _gonvex_permission_scopes(canonical_scope,instance_id,epoch,table_id,generation,phase) VALUES(?1,?2,?3,?4,1,'building') ON CONFLICT(canonical_scope) DO UPDATE SET canonical_scope=excluded.canonical_scope RETURNING scope_id")
        .bind(canonical).bind(instance_id).bind(epoch).bind(table).fetch_one(&mut **tx.transaction()).await?;
    Ok(id)
}

#[derive(Clone, Debug)]
pub struct IndexState {
    pub canonical_scope: Vec<u8>,
    pub instance_id: String,
    pub epoch: String,
    pub table: String,
    pub generation: i64,
    pub phase: String,
    pub covered_revision: i64,
    pub change_serial: i64,
}

pub async fn state(
    connection: &mut SqliteConnection,
    scope: i64,
) -> Result<IndexState, DatabaseError> {
    require_local(connection).await?;
    let row = sqlx::query("SELECT * FROM _gonvex_permission_scopes WHERE scope_id=?1")
        .bind(scope)
        .fetch_one(connection)
        .await?;
    Ok(IndexState {
        canonical_scope: row.try_get("canonical_scope")?,
        instance_id: row.try_get("instance_id")?,
        epoch: row.try_get("epoch")?,
        table: row.try_get("table_id")?,
        generation: row.try_get("generation")?,
        phase: row.try_get("phase")?,
        covered_revision: row.try_get("covered_revision")?,
        change_serial: row.try_get("change_serial")?,
    })
}

async fn atom(
    tx: &mut TenantTransaction,
    scope: i64,
    generation: i64,
    key: &[u8],
) -> Result<i64, DatabaseError> {
    if key.is_empty() {
        return Err(invalid("empty canonical atom"));
    }
    // Single tenant writer prevents races on scoped IDs. No process-local bit
    // offset is persisted or used as permission-set identity.
    Ok(sqlx::query_scalar("INSERT INTO _gonvex_permission_atoms(scope_id,generation,atom_id,canonical_key) SELECT ?1,?2,coalesce(max(atom_id),0)+1,?3 FROM _gonvex_permission_atoms WHERE scope_id=?1 AND generation=?2 ON CONFLICT(scope_id,generation,canonical_key) DO UPDATE SET canonical_key=excluded.canonical_key RETURNING atom_id")
        .bind(scope).bind(generation).bind(key).fetch_one(&mut **tx.transaction()).await?)
}

/// Required normalized sources are declared before a backfill/load. Their
/// completion receipts are written only after an entire replacement succeeds.
async fn register_sources_inner(
    tx: &mut TenantTransaction,
    scope: i64,
    sources: &[(String, String)],
) -> Result<(), DatabaseError> {
    writer(tx).await?;
    let state = state(tx.transaction(), scope).await?;
    for (kind, source) in sources {
        if !matches!(kind.as_str(), "grant" | "row" | "membership") || source.is_empty() {
            tx.mark_failed("invalid permission source declaration");
            return Err(invalid("invalid permission source declaration"));
        }
        sqlx::query("INSERT OR IGNORE INTO _gonvex_permission_sources(scope_id,generation,kind,source_id) VALUES(?1,?2,?3,?4)")
            .bind(scope).bind(state.generation).bind(kind).bind(source).execute(&mut **tx.transaction()).await?;
    }
    sqlx::query("UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id=?1")
        .bind(scope).execute(&mut **tx.transaction()).await?;
    Ok(())
}

#[derive(Clone, Debug)]
pub enum Subject {
    Member(String),
    Group(String),
}
impl Subject {
    fn parts(&self) -> (&str, &str) {
        match self {
            Self::Member(id) => ("member", id),
            Self::Group(id) => ("group", id),
        }
    }
}
#[derive(Clone, Debug)]
pub struct GrantFact {
    pub target: Subject,
    pub atom: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct MembershipFact {
    pub group: String,
    pub member: Subject,
}
#[derive(Clone, Debug)]
pub struct RowFact {
    pub universe: String,
    pub key: String,
    pub atoms: Vec<(String, Vec<u8>)>,
}
#[derive(Clone, Debug)]
pub enum SourceChange {
    /// Empty replacements are revokes. Other source IDs remain intact.
    Grants {
        source: String,
        facts: Vec<GrantFact>,
    },
    Rows {
        source: String,
        facts: Vec<RowFact>,
    },
    Memberships {
        source: String,
        facts: Vec<MembershipFact>,
    },
}

/// All replacements share the caller's writer transaction. Distinct sources
/// retain alternate supports; SQL views deduplicate effective membership.
pub async fn apply(
    tx: &mut TenantTransaction,
    scope: i64,
    changes: &[SourceChange],
) -> Result<(), DatabaseError> {
    let result = apply_inner(tx, scope, changes).await;
    if result.is_err() {
        tx.mark_failed("permission index batch failed");
    }
    result
}
async fn apply_inner(
    tx: &mut TenantTransaction,
    scope: i64,
    changes: &[SourceChange],
) -> Result<(), DatabaseError> {
    writer(tx).await?;
    let state = state(tx.transaction(), scope).await?;
    if state.generation <= 0 {
        return Err(invalid("invalid permission index generation"));
    }
    sqlx::query("UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id=?1")
        .bind(scope).execute(&mut **tx.transaction()).await?;
    for change in changes {
        let (kind, source) = match change {
            SourceChange::Grants { source, .. } => ("grant", source),
            SourceChange::Rows { source, .. } => ("row", source),
            SourceChange::Memberships { source, .. } => ("membership", source),
        };
        let declared: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_permission_sources WHERE scope_id=?1 AND generation=?2 AND kind=?3 AND source_id=?4)")
            .bind(scope).bind(state.generation).bind(kind).bind(source).fetch_one(&mut **tx.transaction()).await?;
        if declared != 1 {
            return Err(invalid("undeclared normalized permission source"));
        }
        match change {
            SourceChange::Grants { source, facts } => {
                if source.is_empty() {
                    return Err(invalid("grant source is required"));
                }
                sqlx::query("DELETE FROM _gonvex_permission_supports WHERE scope_id=?1 AND generation=?2 AND source_id=?3")
                    .bind(scope).bind(state.generation).bind(source).execute(&mut **tx.transaction()).await?;
                for fact in facts {
                    let (target_kind, target_id) = fact.target.parts();
                    if target_id.is_empty() {
                        return Err(invalid("grant target is required"));
                    }
                    let atom = atom(tx, scope, state.generation, &fact.atom).await?;
                    sqlx::query(
                        "INSERT OR IGNORE INTO _gonvex_permission_supports VALUES(?1,?2,?3,?4,?5,?6)",
                    )
                    .bind(scope)
                    .bind(state.generation)
                    .bind(source)
                    .bind(target_kind)
                    .bind(target_id)
                    .bind(atom)
                    .execute(&mut **tx.transaction())
                    .await?;
                }
            }
            SourceChange::Memberships { source, facts } => {
                if source.is_empty() {
                    return Err(invalid("membership source is required"));
                }
                sqlx::query("DELETE FROM _gonvex_permission_memberships WHERE scope_id=?1 AND generation=?2 AND source_id=?3")
                    .bind(scope).bind(state.generation).bind(source).execute(&mut **tx.transaction()).await?;
                for fact in facts {
                    let (kind, id) = fact.member.parts();
                    if fact.group.is_empty() || id.is_empty() {
                        return Err(invalid("group and member identity required"));
                    }
                    sqlx::query("INSERT OR IGNORE INTO _gonvex_permission_memberships VALUES(?1,?2,?3,?4,?5,?6)")
                        .bind(scope).bind(state.generation).bind(source).bind(&fact.group).bind(kind).bind(id).execute(&mut **tx.transaction()).await?;
                }
            }
            SourceChange::Rows { source, facts } => {
                if source.is_empty() {
                    return Err(invalid("row source is required"));
                }
                for table in [
                    "_gonvex_permission_row_supports",
                    "_gonvex_permission_row_atom_supports",
                ] {
                    sqlx::query(&format!(
                        "DELETE FROM {table} WHERE scope_id=?1 AND generation=?2 AND source_id=?3"
                    ))
                    .bind(scope)
                    .bind(state.generation)
                    .bind(source)
                    .execute(&mut **tx.transaction())
                    .await?;
                }
                for fact in facts {
                    if fact.key.is_empty() || fact.universe.is_empty() {
                        return Err(invalid("row key and finite universe are required"));
                    }
                    sqlx::query("INSERT OR IGNORE INTO _gonvex_permission_row_supports VALUES(?1,?2,?3,?4,?5,?6)")
                        .bind(scope).bind(state.generation).bind(source).bind(&state.table).bind(&fact.universe).bind(&fact.key).execute(&mut **tx.transaction()).await?;
                    for (slot, key) in &fact.atoms {
                        if slot.is_empty() {
                            return Err(invalid("row atom slot is required"));
                        }
                        let atom = atom(tx, scope, state.generation, key).await?;
                        sqlx::query("INSERT OR IGNORE INTO _gonvex_permission_row_atom_supports VALUES(?1,?2,?3,?4,?5,?6,?7)")
                            .bind(scope).bind(state.generation).bind(source).bind(&state.table).bind(&fact.key).bind(slot).bind(atom).execute(&mut **tx.transaction()).await?;
                    }
                }
            }
        }
        sqlx::query("UPDATE _gonvex_permission_sources SET complete=1 WHERE scope_id=?1 AND generation=?2 AND kind=?3 AND source_id=?4")
            .bind(scope).bind(state.generation).bind(kind).bind(source).execute(&mut **tx.transaction()).await?;
    }
    let cycle: i64 = sqlx::query_scalar("WITH RECURSIVE links(root,node) AS (SELECT group_id,member_id FROM _gonvex_permission_memberships WHERE scope_id=?1 AND generation=?2 AND member_kind='group' UNION SELECT l.root,m.member_id FROM links l JOIN _gonvex_permission_memberships m ON m.group_id=l.node WHERE m.scope_id=?1 AND m.generation=?2 AND m.member_kind='group') SELECT EXISTS(SELECT 1 FROM links WHERE root=node)")
        .bind(scope).bind(state.generation).fetch_one(&mut **tx.transaction()).await?;
    if cycle != 0 {
        return Err(invalid("cyclic primitive group membership"));
    }
    Ok(())
}

/// Mark a fully maintained NORMALIZED binding pending publication. No helper
/// infers source-table/parent/role adapters or their completeness. Calling this
/// on a partial backfill is forbidden; its trusted binder owns that proof.
async fn seal_inner(tx: &mut TenantTransaction, scope: i64) -> Result<(), DatabaseError> {
    writer(tx).await?;
    let changed = sqlx::query("UPDATE _gonvex_permission_scopes SET phase='pending',covered_revision=(SELECT revision FROM _gonvex_sync_clock WHERE singleton=1) WHERE scope_id=?1 AND NOT EXISTS(SELECT 1 FROM _gonvex_permission_sources s WHERE s.scope_id=?1 AND s.generation=_gonvex_permission_scopes.generation AND s.complete=0) AND instance_id=(SELECT instance_id FROM _gonvex_permission_identity WHERE singleton=1) AND epoch=(SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1)")
        .bind(scope).execute(&mut **tx.transaction()).await?.rows_affected();
    if changed != 1 {
        tx.mark_failed("permission index seal identity mismatch");
        return Err(invalid("permission index seal identity mismatch"));
    }
    Ok(())
}

/// Runs after change-feed finalization and before COMMIT, on the same writer.
/// Any later fact edit dirties the scope via triggers and prevents publication.
/// Unrelated/stale scopes remain unready rather than claiming false coverage.
pub(crate) async fn finalize_local(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    if std::env::var("GONVEX_PERMISSION_PRIMITIVES_LOCAL").as_deref() != Ok("1") {
        return Ok(());
    }
    let installed: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_permission_scopes')").fetch_one(&mut *connection).await?;
    if installed == 0 {
        return Ok(());
    }
    instance(connection).await?;
    sqlx::query("UPDATE _gonvex_permission_scopes SET phase='ready',covered_revision=(SELECT revision FROM _gonvex_sync_clock WHERE singleton=1) WHERE phase='pending' AND NOT EXISTS(SELECT 1 FROM _gonvex_permission_sources s WHERE s.scope_id=_gonvex_permission_scopes.scope_id AND s.generation=_gonvex_permission_scopes.generation AND s.complete=0) AND instance_id=(SELECT instance_id FROM _gonvex_permission_identity WHERE singleton=1) AND epoch=(SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1)")
        .execute(connection).await?;
    Ok(())
}

pub async fn install_local(tx: &mut TenantTransaction) -> Result<String, DatabaseError> {
    let result = install_local_inner(tx).await;
    if result.is_err() {
        tx.mark_failed("permission index install_local failed");
    }
    result
}

pub async fn create_scope(
    tx: &mut TenantTransaction,
    canonical: &[u8],
    instance_id: &str,
    epoch: &str,
    table: &str,
) -> Result<i64, DatabaseError> {
    let result = create_scope_inner(tx, canonical, instance_id, epoch, table).await;
    if result.is_err() {
        tx.mark_failed("permission index create_scope failed");
    }
    result
}

pub async fn register_sources(
    tx: &mut TenantTransaction,
    scope: i64,
    sources: &[(String, String)],
) -> Result<(), DatabaseError> {
    let result = register_sources_inner(tx, scope, sources).await;
    if result.is_err() {
        tx.mark_failed("permission index register_sources failed");
    }
    result
}

pub async fn seal(tx: &mut TenantTransaction, scope: i64) -> Result<(), DatabaseError> {
    let result = seal_inner(tx, scope).await;
    if result.is_err() {
        tx.mark_failed("permission index seal failed");
    }
    result
}

fn local_row_guards(scope: i64, table: &str) -> Result<Vec<(String, String)>, DatabaseError> {
    if scope <= 0 || table.is_empty() || table.starts_with("_gonvex_") {
        return Err(invalid("invalid local permission source table"));
    }
    let table = crate::rows::quote_identifier(table);
    Ok(["INSERT", "UPDATE", "DELETE"].into_iter().map(|op| {
        let name = format!("_gonvex_permission_binding_{scope}_{}", op.to_ascii_lowercase());
        let sql = format!("CREATE TRIGGER {name} AFTER {op} ON {table} BEGIN UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id={scope}; UPDATE _gonvex_permission_sources SET complete=0 WHERE scope_id={scope} AND kind='row'; END");
        (name, sql)
    }).collect())
}

/// Explicit OWN fixture setup, never provisioning or a runtime read migration.
/// Every actual source-row write invalidates readiness/row-source receipts in
/// the same transaction; all declared row sources must then be replaced/sealed.
/// This conservative guard also covers projection/filter/order-only writes.
pub async fn bind_local_rows(tx: &mut TenantTransaction, scope: i64) -> Result<(), DatabaseError> {
    let result = async {
        writer(tx).await?;
        let table = state(tx.transaction(), scope).await?.table;
        for (name, sql) in local_row_guards(scope, &table)? {
            let actual: Option<String> = sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1",
            )
            .bind(name)
            .fetch_optional(&mut **tx.transaction())
            .await?;
            if let Some(actual) = actual {
                if actual != sql {
                    return Err(invalid("local row guard changed"));
                }
            } else {
                sqlx::raw_sql(&sql).execute(&mut **tx.transaction()).await?;
            }
        }
        verify_local_rows(tx.transaction(), scope, &table).await
    }
    .await;
    if result.is_err() {
        tx.mark_failed("local source-row binding failed");
    }
    result
}

/// Verify exact guard SQL in the SAME snapshot as the served row/index read.
/// Missing/replaced guards fail closed. This does not infer row atoms from an
/// arbitrary application model: the explicit normalized writer still owns them.
pub async fn verify_local_rows(
    connection: &mut SqliteConnection,
    scope: i64,
    table: &str,
) -> Result<(), DatabaseError> {
    require_local(connection).await?;
    for (name, expected) in local_row_guards(scope, table)? {
        let actual: Option<String> = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1 AND tbl_name=?2",
        )
        .bind(name)
        .bind(table)
        .fetch_optional(&mut *connection)
        .await?;
        if actual.as_deref() != Some(expected.as_str()) {
            return Err(invalid(
                "local permission source-row guard missing or changed",
            ));
        }
    }
    let sources: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_permission_sources s JOIN _gonvex_permission_scopes p ON p.scope_id=s.scope_id AND p.generation=s.generation WHERE s.scope_id=?1 AND s.kind='row'")
        .bind(scope).fetch_one(connection).await?;
    if sources == 0 {
        return Err(invalid(
            "local row binding needs a declared normalized row source",
        ));
    }
    Ok(())
}

fn local_source_guards(
    scope: i64,
    table: &str,
    source: &str,
    kind: &str,
) -> Result<Vec<(String, String)>, DatabaseError> {
    if scope <= 0
        || source.is_empty()
        || table.is_empty()
        || table.starts_with("_gonvex_")
        || !matches!(kind, "grant" | "row" | "membership")
    {
        return Err(invalid("invalid local permission source guard"));
    }
    let table = crate::rows::quote_identifier(table);
    let encoded = source
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let source = format!("'{}'", source.replace('\'', "''"));
    Ok(["INSERT","UPDATE","DELETE"].into_iter().map(|op|{
        let name=format!("_gonvex_permission_src_{scope}_{encoded}_{kind}_{}",op.to_ascii_lowercase());
        let sql=format!("CREATE TRIGGER {name} AFTER {op} ON {table} BEGIN UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id={scope}; UPDATE _gonvex_permission_sources SET complete=0 WHERE scope_id={scope} AND kind='{kind}' AND source_id={source}; END");
        (name,sql)
    }).collect())
}
/// Explicit local source-table binding, including authoritative identity/grant
/// inputs. Whole source rebuild is required after ANY write, OLD or NEW key.
pub async fn bind_local_source(
    tx: &mut TenantTransaction,
    scope: i64,
    table: &str,
    source: &str,
    kind: &str,
) -> Result<(), DatabaseError> {
    let result = async {
        writer(tx).await?;
        for (name, sql) in local_source_guards(scope, table, source, kind)? {
            let actual: Option<String> = sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1",
            )
            .bind(&name)
            .fetch_optional(&mut **tx.transaction())
            .await?;
            if let Some(actual) = actual {
                if actual != sql {
                    return Err(invalid("local source guard changed"));
                }
            } else {
                sqlx::raw_sql(&sql).execute(&mut **tx.transaction()).await?;
            }
        }
        verify_local_source(tx.transaction(), scope, table, source, kind).await
    }
    .await;
    if result.is_err() {
        tx.mark_failed("local source guard binding failed");
    }
    result
}
pub async fn verify_local_source(
    connection: &mut SqliteConnection,
    scope: i64,
    table: &str,
    source: &str,
    kind: &str,
) -> Result<(), DatabaseError> {
    require_local(connection).await?;
    for (name, expected) in local_source_guards(scope, table, source, kind)? {
        let actual: Option<String> = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1 AND tbl_name=?2",
        )
        .bind(name)
        .bind(table)
        .fetch_optional(&mut *connection)
        .await?;
        if actual.as_deref() != Some(expected.as_str()) {
            return Err(invalid("local source guard missing or changed"));
        }
    }
    Ok(())
}
