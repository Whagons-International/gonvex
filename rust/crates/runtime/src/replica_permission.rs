//! Internal LOCAL snapshot experiment. No wire handler calls this module.
//! Registration never installs schema, and the returned open has NO live
//! subscription. A commit/admission lease barrier is required before activation.
use super::*;
use crate::permissions::query::{self, NormalizedBinding, ReadIdentity};
use futures_util::TryStreamExt;
use gonvex_store::permission_indexes as indexes;

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalSnapshotError {
    #[error("local primitive snapshot rejected: {0}")]
    Rejected(&'static str),
    #[error(transparent)]
    Replica(#[from] ReplicaError),
    #[error(transparent)]
    Permission(#[from] query::Error),
    #[error(transparent)]
    Store(#[from] gonvex_store::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

/// Owned, non-client registration. Exact module/physical schema and normalized
/// binding are pinned; possession alone never bypasses same-read readiness.
/// Deliberately no Runtime registry, app migration, SDK or network selector.
pub(crate) struct LocalSnapshotRegistration {
    binding: NormalizedBinding,
    definition: ReplicaCollectionDefinition,
    generation: u64,
    artifact_hash: String,
    schema: String,
    source_schemas: Vec<(String, String)>,
    source_guards: Vec<(String, String, String)>,
    manifest_identity: Option<Vec<u8>>,
}

pub(crate) struct LocalSnapshotResult {
    pub open: ReplicaOpenResult,
    pub window: ReplicaWindow,
    pub identity: ReadIdentity,
}

/// Includes typing and collation (the complete source DDL), projection, ordering,
/// filters and both budgets. This identity is part of the canonical exact set.
pub(crate) fn projection_version(definition: &ReplicaCollectionDefinition, schema: &str) -> String {
    let mut bytes = b"gonvex-local-replica-projection-v1".to_vec();
    for part in [
        serde_json::to_vec(definition).expect("serializable definition"),
        schema.as_bytes().to_vec(),
    ] {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&part);
    }
    hex_digest(&bytes)
}

async fn source_schema(
    tx: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
) -> Result<String, LocalSnapshotError> {
    indexes::require_local(tx.transaction()).await?;
    let schema: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
            .bind(&definition.table)
            .fetch_optional(&mut **tx.transaction())
            .await?
            .ok_or(LocalSnapshotError::Rejected(
                "source must be a real local table",
            ))?;
    let ddl = schema.to_ascii_uppercase();
    if ddl.contains("COLLATE") || ddl.contains("VIRTUAL") {
        return Err(LocalSnapshotError::Rejected(
            "explicit collations and virtual tables unsupported",
        ));
    }
    let columns = gonvex_store::rows::table_columns(tx.transaction(), &definition.table).await?;
    let key = columns
        .iter()
        .find(|c| c.name == definition.key)
        .ok_or(LocalSnapshotError::Rejected("source key missing"))?;
    if !key.primary_key
        || key.declared.to_ascii_uppercase() != "TEXT"
        || columns.iter().filter(|c| c.primary_key).count() != 1
    {
        return Err(LocalSnapshotError::Rejected(
            "requires one non-null TEXT primary key with binary collation",
        ));
    }
    let mut projected = BTreeSet::new();
    for name in definition
        .columns
        .iter()
        .chain(definition.equal_filters.keys())
        .chain(definition.exclude_when_set.iter())
        .chain([&definition.order_by, &definition.key])
    {
        quote(name)?;
        let column = columns
            .iter()
            .find(|c| c.name == *name)
            .ok_or(LocalSnapshotError::Rejected("collection column absent"))?;
        if column.declared.to_ascii_uppercase().contains("BLOB") {
            return Err(LocalSnapshotError::Rejected(
                "BLOB projection/filter/order unsupported",
            ));
        }
    }
    for column in &definition.columns {
        if !projected.insert(column) {
            return Err(LocalSnapshotError::Rejected("duplicate projection column"));
        }
    }
    if !projected.contains(&definition.key)
        || !projected.contains(&definition.order_by)
        || !matches!(definition.order_direction.as_str(), "asc" | "desc")
        || definition.max_bytes < 0
        || definition.max_rows > (i64::MAX - 1) as usize
    {
        return Err(LocalSnapshotError::Rejected(
            "requires projected key/order, explicit direction and valid budgets",
        ));
    }
    let order = columns
        .iter()
        .find(|c| c.name == definition.order_by)
        .expect("checked above");
    if order.kind() == gonvex_store::rows::JsonKind::Json {
        return Err(LocalSnapshotError::Rejected(
            "JSON ordering adapter unsupported",
        ));
    }
    // SQL validation only, not a Rust authorization loop. TEXT affinity still
    // permits BLOB storage; reject it rather than silently converting row IDs.
    let invalid: i64 = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM {} WHERE typeof({}) <> 'text' OR {}='' OR instr({},char(0))<>0)",
        quote(&definition.table)?,
        quote(&definition.key)?,
        quote(&definition.key)?,
        quote(&definition.key)?
    ))
    .fetch_one(&mut **tx.transaction())
    .await?;
    if invalid != 0 {
        return Err(LocalSnapshotError::Rejected("non-text or empty source key"));
    }
    Ok(schema)
}

impl Runtime {
    /// Explicit backend binding to a REAL loaded collection in an owned fixture.
    /// This performs reads only; store installation/binding is separately explicit.
    pub(crate) async fn register_local_permission_snapshot(
        &self,
        session: &TenantSession,
        binding: NormalizedBinding,
    ) -> Result<LocalSnapshotRegistration, LocalSnapshotError> {
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let function = module
            .functions
            .get(&binding.scope.collection)
            .filter(|f| f.kind == "query" && f.delivery == "replica")
            .ok_or_else(|| ReplicaError::FunctionMissing(binding.scope.collection.clone()))?;
        let definition = function
            .replica
            .clone()
            .ok_or(LocalSnapshotError::Rejected(
                "collection definition missing",
            ))?;
        if definition.table != binding.scope.table {
            return Err(LocalSnapshotError::Rejected("source/collection mismatch"));
        }
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut tx = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        let schema = source_schema(&mut tx, &definition).await?;
        indexes::verify_local_rows(tx.transaction(), binding.scope_id, &definition.table).await?;
        query::prepare_projection(
            &mut tx,
            session,
            &binding,
            false,
            &projection_version(&definition, &schema),
        )
        .await?;
        tx.commit().await?;
        Ok(LocalSnapshotRegistration {
            binding,
            definition,
            generation: module.generation,
            artifact_hash: module.artifact_hash.clone(),
            schema,
            source_schemas: Vec::new(),
            source_guards: Vec::new(),
            manifest_identity: None,
        })
    }

    /// Cold snapshot ONLY. No feed install, resume, network handler, cached
    /// principal, shared encoder or per-set delivery is activated by this call.
    pub(crate) async fn open_local_permission_snapshot(
        &self,
        registration: &LocalSnapshotRegistration,
        session: &TenantSession,
        request: ReplicaOpenRequest,
    ) -> Result<LocalSnapshotResult, LocalSnapshotError> {
        if request.path != registration.binding.scope.collection
            || request.cursor.is_some()
            || request.digest.is_some()
            || !request.hashes.is_empty()
            || !request.keys.is_empty()
            || request.full_integrity
        {
            return Err(LocalSnapshotError::Rejected(
                "only an exact registered cold snapshot is supported",
            ));
        }
        let definition = &registration.definition;
        let keyed = definition
            .equal_filters
            .values()
            .any(|arg| request.args.get(arg).is_some());
        let class = if keyed {
            format!("{}\0keyed", definition.table)
        } else {
            definition.table.clone()
        };
        let permit = crate::replica_open_queue::admission_wait(
            crate::offload::SnapshotAdmission::global().admit(
                format!(
                    "{}\0{}\0{}",
                    session.route.database_url, class, session.member.id
                ),
                &class,
            ),
        )
        .await
        .map_err(ReplicaError::from)?;
        // Lease freezes module activation through the read/commit; read begins
        // AFTER admission, so a queued preparation cannot serve a stale clock.
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let current = module
            .functions
            .get(&request.path)
            .filter(|f| f.kind == "query" && f.delivery == "replica")
            .and_then(|f| f.replica.as_ref())
            .ok_or(LocalSnapshotError::Rejected("collection removed"))?;
        if module.generation != registration.generation
            || module.artifact_hash != registration.artifact_hash
            || serde_json::to_vec(current).expect("definition")
                != serde_json::to_vec(definition).expect("definition")
        {
            return Err(LocalSnapshotError::Rejected(
                "module changed since local registration",
            ));
        }
        if let Some(expected) = &registration.manifest_identity {
            let actual = self
                .inner
                .modules
                .permission_manifest(&module)
                .await
                .ok_or(LocalSnapshotError::Rejected("source manifest removed"))?;
            if stable_json(&actual).as_bytes() != expected.as_slice() {
                return Err(LocalSnapshotError::Rejected(
                    "source manifest authority changed",
                ));
            }
        }
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut tx = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        let schema = source_schema(&mut tx, definition).await?;
        if schema != registration.schema {
            return Err(LocalSnapshotError::Rejected(
                "physical source schema changed",
            ));
        }
        indexes::verify_local_rows(
            tx.transaction(),
            registration.binding.scope_id,
            &definition.table,
        )
        .await?;
        verify_source_registration(&mut tx, registration).await?;
        let clock = clock_in_transaction(&mut tx).await?;
        let prepared = query::prepare_projection(
            &mut tx,
            session,
            &registration.binding,
            false,
            &projection_version(definition, &schema),
        )
        .await?;
        if prepared.identity.source_revision != i64::try_from(clock.revision).unwrap_or(-1) {
            return Err(LocalSnapshotError::Rejected(
                "clock/index read revision mismatch",
            ));
        }
        let mut digest = ReplicaAggregate::new(false);
        let mut result = Vec::new();
        let mut inline = Some(BTreeMap::new());
        let window = scan_local_window(&mut tx, definition, &prepared, &request.args, |row| {
            digest.add(&row.hash);
            if let Some(hashes) = &mut inline {
                if hashes.len() < INLINE_SNAPSHOT_HASH_LIMIT {
                    hashes.insert(row.key.to_string(), row.hash.to_string());
                } else {
                    inline = None;
                }
            }
            result.push(row.row);
            Ok(())
        })
        .await?;
        tx.commit().await?;
        permit.observe_json_bytes(window.bytes as usize);
        // Legacy metadata is used solely to compare the existing cold messages.
        // It is NOT an authority to resume/share a primitive permission set.
        let directive = module.replica_directive(
            &session.route.tenant_id,
            &session.route.database_url,
            &session.identity.account.id,
            &session.member.permissions,
        );
        let cursor = cursor_for(&clock, definition, &directive.visibility_scope);
        let messages = vec![
            replica_snapshot_message(
                &request,
                definition,
                result,
                inline,
                window.truncated,
                cursor.clone(),
                &directive.scope,
                &directive.visibility_scope,
                &digest.digest(),
            ),
            replica_ready(
                &request.id,
                &request.path,
                definition,
                cursor,
                digest.digest(),
                window.truncated,
            ),
        ];
        Ok(LocalSnapshotResult {
            open: ReplicaOpenResult {
                messages,
                subscription: None,
            },
            window,
            identity: prepared.identity,
        })
    }
}

async fn scan_local_window(
    tx: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    prepared: &query::Prepared,
    args: &Value,
    mut visit: impl FnMut(ScannedRow) -> Result<(), ReplicaError>,
) -> Result<ReplicaWindow, LocalSnapshotError> {
    let args = args.as_object().ok_or(ReplicaError::InvalidArguments)?;
    let mut values = Vec::new();
    let mut predicates = definition
        .exclude_when_set
        .iter()
        .map(|column| quote(column).map(|c| format!("r.{c} IS NULL")))
        .collect::<Result<Vec<_>, _>>()?;
    let offset = 4 + prepared.compiled.extra_bindings.len();
    for (column, argument) in &definition.equal_filters {
        let value = args.get(argument).ok_or(ReplicaError::InvalidArguments)?;
        values.push(
            crate::sql_parameters::Parameter::scalar(value)
                .map_err(ReplicaError::InvalidIdentifier)?,
        );
        predicates.push(format!("r.{} = ?{}", quote(column)?, offset + values.len()));
    }
    let columns = declared_columns(tx, definition, definition.columns.iter()).await?;
    let direction = if definition.order_direction == "asc" {
        "ASC"
    } else {
        "DESC"
    };
    // Authorization is a distinct indexed SQL key relation BEFORE projection,
    // filtering, ordering and LIMIT. No full-list Rust authorization loop.
    let mut sql = format!("SELECT {} FROM {} r JOIN ({}) admitted ON r.{} COLLATE BINARY=admitted.row_key COLLATE BINARY WHERE {} ORDER BY r.{} {direction},r.{} {direction}",
        gonvex_store::rows::json_object_expr("r", &columns), quote(&definition.table)?, prepared.compiled.sql,
        quote(&definition.key)?, if predicates.is_empty() { "1".to_owned() } else { predicates.join(" AND ") }, quote(&definition.order_by)?, quote(&definition.key)?);
    if definition.max_rows > 0 {
        values.push(crate::sql_parameters::Parameter::Integer(
            (definition.max_rows + 1) as i64,
        ));
        sql.push_str(&format!(" LIMIT ?{}", offset + values.len()));
    }
    let mut query = query::bind(prepared, &sql);
    for parameter in values {
        query = parameter.bind(query);
    }
    // SQLx streams through this snapshot's connection. Do not collect the full
    // projection before byte admission; a byte budget can stop at the next row.
    let mut stream = query.fetch(&mut **tx.transaction());
    let mut collector = WindowCollector::new(definition);
    while let Some(row) = stream.try_next().await? {
        if collector.window.rows.is_multiple_of(32) {
            tokio::task::yield_now().await;
        }
        let json: String = row.try_get(0)?;
        if !collector.push(&json, &mut visit)? {
            break;
        }
    }
    drop(stream);
    Ok(collector.finish())
}

async fn verify_source_registration(
    tx: &mut TenantTransaction,
    registration: &LocalSnapshotRegistration,
) -> Result<(), LocalSnapshotError> {
    if indexes::instance(tx.transaction()).await? != registration.binding.scope.database_instance {
        return Err(LocalSnapshotError::Rejected(
            "source registration belongs to another physical database",
        ));
    }
    let state = indexes::state(tx.transaction(), registration.binding.scope_id).await?;
    if state.generation != registration.binding.required_generation
        || state.canonical_scope
            != registration
                .binding
                .scope
                .canonical()
                .map_err(|_| LocalSnapshotError::Rejected("invalid source scope"))?
    {
        return Err(LocalSnapshotError::Rejected(
            "source scope/generation changed",
        ));
    }
    for (table, expected) in &registration.source_schemas {
        let schema: Option<String> =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
                .bind(table)
                .fetch_optional(&mut **tx.transaction())
                .await?;
        if schema.as_deref() != Some(expected.as_str()) {
            return Err(LocalSnapshotError::Rejected(
                "source schema authority changed",
            ));
        }
    }
    for (table, source, kind) in &registration.source_guards {
        indexes::verify_local_source(
            tx.transaction(),
            registration.binding.scope_id,
            table,
            source,
            kind,
        )
        .await?;
    }
    Ok(())
}

impl Runtime {
    /// Explicit OWN fixture installation/backfill from the signed module's
    /// source declaration, never called by provisioning, open or a wire handler.
    /// This is the only manifest→source facts→registered SQL snapshot adapter.
    pub(crate) async fn install_local_source_permission_snapshot(
        &self,
        session: &TenantSession,
        path: &str,
    ) -> Result<LocalSnapshotRegistration, LocalSnapshotError> {
        if session.is_anonymous()
            || session.developer_session.is_some()
            || session.identity.project_id != session.route.project_id
            || session.route.tenant_id.is_empty()
        {
            return Err(LocalSnapshotError::Rejected(
                "requires matching persisted local member route",
            ));
        }
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let raw = self
            .inner
            .modules
            .permission_manifest(&module)
            .await
            .ok_or(LocalSnapshotError::Rejected(
                "no signed primitive source manifest",
            ))?;
        let bound = crate::permissions::source::Manifest::parse(&raw)?.bind(path)?;
        let definition = module
            .functions
            .get(path)
            .filter(|f| f.kind == "query" && f.delivery == "replica")
            .and_then(|f| f.replica.as_ref())
            .ok_or_else(|| ReplicaError::FunctionMissing(path.into()))?
            .clone();
        if bound.collection.table != definition.table || bound.collection.key != definition.key {
            return Err(LocalSnapshotError::Rejected(
                "source binding does not match real collection",
            ));
        }
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut tx = control
            .begin_tenant_transaction(&session.route, false)
            .await?;
        let result=async{
            let schema=source_schema(&mut tx,&definition).await?;
            let mut source_schemas=BTreeMap::new();let mut changes=Vec::new();
            for source in &bound.sources {
                let ddl:String=sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
                    .bind(source.source_table()).fetch_one(&mut **tx.transaction()).await?;
                source_schemas.insert(source.source_table().to_owned(),ddl);
                changes.push(source.load(&mut tx).await?);
            }
            let source_schemas=source_schemas.into_iter().collect::<Vec<_>>();
            let manifest_identity=stable_json(&raw).into_bytes();
            let authority=stable_json(&serde_json::json!({"format":"source-authority-v1","moduleGeneration":module.generation,
                "artifactHash":module.artifact_hash,"manifest":*raw,"sourceSchemas":source_schemas})).into_bytes();
            let instance=indexes::install_local(&mut tx).await?;
            let clock=clock_in_transaction(&mut tx).await?;
            let scope=crate::permissions::model::PermissionScope {
                project:session.route.project_id.clone(),database_instance:instance,tenant:session.route.tenant_id.clone(),epoch:clock.epoch,
                table:definition.table.clone(),collection:path.into(),
                policy_version:crate::permissions::compile::policy_version_with_authority(&bound.collection.universe,&bound.expression,&authority)
                    .map_err(|_|LocalSnapshotError::Rejected("invalid source policy"))?,
                projection_version:projection_version(&definition,&schema),semantics_version:"normalized-atoms-binary-v1".into(),
            };
            let scope_id=indexes::create_scope(&mut tx,&scope.canonical().map_err(|_|LocalSnapshotError::Rejected("invalid source scope"))?,&scope.database_instance,&scope.epoch,&scope.table).await?;
            let sources=bound.sources.iter().map(|source|(source.kind().to_owned(),source.id().to_owned())).collect::<Vec<_>>();
            indexes::register_sources(&mut tx,scope_id,&sources).await?;
            indexes::bind_local_rows(&mut tx,scope_id).await?;
            let mut source_guards=Vec::new();
            for source in &bound.sources {
                indexes::bind_local_source(&mut tx,scope_id,source.source_table(),source.id(),source.kind()).await?;
                source_guards.push((source.source_table().into(),source.id().into(),source.kind().into()));
            }
            indexes::apply(&mut tx,scope_id,&changes).await?;
            indexes::seal(&mut tx,scope_id).await?;
            let generation=indexes::state(tx.transaction(),scope_id).await?.generation;
            Ok::<_,LocalSnapshotError>(LocalSnapshotRegistration{
                binding:NormalizedBinding{scope_id,required_generation:generation,scope,universe:bound.collection.universe,expression:bound.expression,authority},
                definition,generation:module.generation,artifact_hash:module.artifact_hash.clone(),schema,source_schemas,source_guards,manifest_identity:Some(manifest_identity),
            })
        }.await;
        match result {
            Ok(registration) => {
                tx.commit().await?;
                Ok(registration)
            }
            Err(error) => {
                tx.mark_failed("local source compiler/backfill failed");
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    /// Same-writer full logical-source replacement, including true old+new
    /// dependencies through conservative guards. No incremental cost claim.
    pub(crate) async fn rebuild_local_source_permission_snapshot(
        &self,
        session: &TenantSession,
        registration: &LocalSnapshotRegistration,
    ) -> Result<(), LocalSnapshotError> {
        if session.route.project_id != registration.binding.scope.project
            || session.identity.project_id != registration.binding.scope.project
            || session.route.tenant_id != registration.binding.scope.tenant
        {
            return Err(LocalSnapshotError::Rejected(
                "source rebuild route mismatch",
            ));
        }
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let raw = self
            .inner
            .modules
            .permission_manifest(&module)
            .await
            .ok_or(LocalSnapshotError::Rejected("source manifest removed"))?;
        if module.generation != registration.generation
            || module.artifact_hash != registration.artifact_hash
            || registration.manifest_identity.as_deref() != Some(stable_json(&raw).as_bytes())
        {
            return Err(LocalSnapshotError::Rejected(
                "source authority changed; rebind required",
            ));
        }
        let bound = crate::permissions::source::Manifest::parse(&raw)?
            .bind(&registration.binding.scope.collection)?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut tx = control
            .begin_tenant_transaction(&session.route, false)
            .await?;
        let result = async {
            verify_source_registration(&mut tx, registration).await?;
            indexes::verify_local_rows(
                tx.transaction(),
                registration.binding.scope_id,
                &registration.definition.table,
            )
            .await?;
            let mut changes = Vec::new();
            for source in &bound.sources {
                changes.push(source.load(&mut tx).await?);
            }
            indexes::apply(&mut tx, registration.binding.scope_id, &changes).await?;
            indexes::seal(&mut tx, registration.binding.scope_id).await?;
            Ok::<_, LocalSnapshotError>(())
        }
        .await;
        match result {
            Ok(()) => {
                tx.commit().await?;
                Ok(())
            }
            Err(error) => {
                tx.mark_failed("local source rebuild failed");
                tx.rollback().await?;
                Err(error)
            }
        }
    }
}

/// Read-only adapter boundary for independently compiled, fresh validated scans.
/// Caller owns the SAME transaction for certificate validation and this visit.
/// This wrapper grants no readiness, provider completeness or authority itself.
#[cfg(feature = "permission-primitives")]
pub(crate) async fn visit_indexed_collection_window(
    tx: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    prepared: &query::Prepared,
    args: &Value,
    visit: impl FnMut(ScannedRow) -> Result<(), ReplicaError>,
) -> Result<ReplicaWindow, LocalSnapshotError> {
    scan_local_window(tx, definition, prepared, args, visit).await
}
