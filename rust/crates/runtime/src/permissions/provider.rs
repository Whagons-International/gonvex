//! Sealed loaded-module equivalence certificates. A declared version, digest or
//! source completion receipt cannot construct one. The compiler derives the
//! complete supported program from the ACTUAL leased v2 policy and exact schema.
//! Unsupported/domain/admission cases explicitly select the normal v2 path.
use super::{
    compile::CompiledSet,
    model::{ExactPermissionSet, PermissionScope},
    query::{Prepared, ReadIdentity},
};
use crate::modules::{ModuleCallLease, ReplicaCollectionDefinition};
use gonvex_store::permission_indexes::program::{Context, Policy, Program};
use gonvex_store::{permission_indexes as indexes, TenantSession, TenantTransaction};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Fallback {
    UnsupportedPolicy { path: String, operator: String },
    UnsupportedSchema(String),
    UnsupportedAdmission,
    DomainOrIndexNotReady,
    ModuleOrProjectionChanged,
    MissingCollection,
    MissingIndex,
    UnorderedSource,
    Arguments,
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Store(#[from] gonvex_store::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Classification {
    SharedBase,
    MemberList,
}
/// No Deserialize/public constructor/complete flag. Owned backend compilation
/// is the only way to create this proof and all its typed program fields.
#[derive(Clone)]
pub(crate) struct CompleteProviderCertificate {
    program: Program,
    source_scope: PermissionScope,
    definition: ReplicaCollectionDefinition,
    function_authority: Value,
    path: String,
    project: String,
    generation: u64,
    artifact: String,
    classification: Classification,
}
pub(crate) enum Compilation {
    Complete(Box<CompleteProviderCertificate>),
    NormalV2(Fallback),
}
pub(crate) struct CompiledRegistration {
    certificate: CompleteProviderCertificate,
    scope_id: i64,
    generation: i64,
}
pub(crate) enum Fresh<'tx> {
    Indexed(Box<ValidatedScan<'tx>>),
    NormalV2(Fallback),
}
/// Module activation lease remains held through the actual scan. Caller owns
/// the SAME read TX and any writer/publication lease through enqueue/commit.
pub(crate) struct ValidatedScan<'tx> {
    transaction: &'tx mut TenantTransaction,
    pub prepared: Prepared,
    pub definition: ReplicaCollectionDefinition,
    pub args: Value,
    pub source_scope: PermissionScope,
    pub query_scope: PermissionScope,
    pub classification: Classification,
    _module: ModuleCallLease,
}
impl ValidatedScan<'_> {
    /// The only scan entry exposes the exact transaction that was validated.
    /// No reopening/substituting a connection between admission and row bytes.
    pub fn parts(
        &mut self,
    ) -> (
        &mut TenantTransaction,
        &ReplicaCollectionDefinition,
        &Prepared,
        &Value,
    ) {
        (
            self.transaction,
            &self.definition,
            &self.prepared,
            &self.args,
        )
    }
}

fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}
fn canonical<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("typed compiler data serializes")
}
async fn ddl(tx: &mut TenantTransaction, table: &str) -> Result<Option<String>, Error> {
    Ok(
        sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1")
            .bind(table)
            .fetch_optional(&mut **tx.transaction())
            .await?,
    )
}
fn expression(plan: &crate::visibility::VisibilityPlan) -> Result<Policy, Fallback> {
    fn lower(
        p: &crate::visibility::VisibilityExpression,
        depth: usize,
    ) -> Result<Policy, Fallback> {
        let unsupported = || Fallback::UnsupportedPolicy {
            path: "where".into(),
            operator: p.operator.clone(),
        };
        if depth > 64
            || p.on_missing.is_some()
            || p.predicate.is_some()
            || p.via.is_some()
            || !p.parent_where.is_empty()
            || !p.table.is_empty()
            || !p.key.is_empty()
            || !p.set.is_empty()
        {
            return Err(unsupported());
        }
        let scalar = p.children.is_empty();
        let no_value = p.value.is_none();
        let no_fields = p.column.is_empty() && p.context.is_empty();
        Ok(match p.operator.as_str() {
            "public" if scalar && no_value && no_fields => Policy::Admitted,
            "permission" | "role" if scalar && no_fields => {
                let name = p
                    .value
                    .as_ref()
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(unsupported)?
                    .to_owned();
                if p.operator == "permission" {
                    Policy::Permission { name }
                } else {
                    Policy::Role { name }
                }
            }
            "eq" if scalar && p.context.is_empty() && !p.column.is_empty() => {
                let object = p
                    .value
                    .as_ref()
                    .and_then(Value::as_object)
                    .filter(|o| o.len() == 1 && o.contains_key("literal"))
                    .ok_or_else(unsupported)?;
                let value = match &object["literal"] {
                    Value::Null => None,
                    Value::String(s) => Some(s.clone()),
                    _ => return Err(unsupported()),
                };
                Policy::RowLiteral {
                    column: p.column.clone(),
                    value,
                }
            }
            "eqContext" if scalar && no_value && !p.column.is_empty() => Policy::RowContext {
                column: p.column.clone(),
                context: match p.context.as_str() {
                    "member.id" => Context::Member,
                    "account.id" => Context::Account,
                    "tenant.id" => Context::Tenant,
                    _ => return Err(unsupported()),
                },
            },
            "and" | "or" if no_value && no_fields && !p.children.is_empty() => {
                let children = p
                    .children
                    .iter()
                    .map(|c| lower(c, depth + 1))
                    .collect::<Result<Vec<_>, _>>()?;
                if p.operator == "and" {
                    Policy::All { children }
                } else {
                    Policy::Any { children }
                }
            }
            "not" if no_value && no_fields && p.children.len() == 1 => Policy::Not {
                child: Box::new(lower(&p.children[0], depth + 1)?),
            },
            _ => return Err(unsupported()),
        })
    }
    if plan.version != Some(2) || !plan.sets.is_empty() {
        return Err(Fallback::UnsupportedPolicy {
            path: "where".into(),
            operator: plan.predicate.operator.clone(),
        });
    }
    lower(&plan.predicate, 0)
}
impl CompleteProviderCertificate {
    /// `module` is a real activation lease returned by ModuleRegistry. Neither
    /// the serialized declaration nor a caller-supplied normalized binding is
    /// trusted as an equivalence assertion.
    pub async fn compile(
        module: &ModuleCallLease,
        tx: &mut TenantTransaction,
        session: &TenantSession,
        path: &str,
    ) -> Result<Compilation, Error> {
        let actual = indexes::require_local(tx.transaction()).await?;
        if std::fs::canonicalize(gonvex_store::database_path(&session.route.database_url))
            .ok()
            .as_ref()
            != Some(&actual)
        {
            return Ok(Compilation::NormalV2(Fallback::ModuleOrProjectionChanged));
        }
        let Some(function) = module
            .functions
            .get(path)
            .filter(|f| f.kind == "query" && f.delivery == "replica")
        else {
            return Ok(Compilation::NormalV2(Fallback::MissingCollection));
        };
        let Some(definition) = function.replica.clone() else {
            return Ok(Compilation::NormalV2(Fallback::MissingCollection));
        };
        let Some(plan) = module.visibility.get(&definition.table) else {
            return Ok(Compilation::NormalV2(Fallback::UnsupportedPolicy {
                path: "visibility".into(),
                operator: "missing".into(),
            }));
        };
        let policy = match expression(plan) {
            Ok(p) => p,
            Err(e) => return Ok(Compilation::NormalV2(e)),
        };
        if module.project_id != session.route.project_id
            || module.project_id != session.identity.project_id
            || definition.table != plan.table
            || definition.key != plan.key
        {
            return Ok(Compilation::NormalV2(Fallback::ModuleOrProjectionChanged));
        }
        let Some(row_schema) = ddl(tx, &definition.table).await? else {
            return Ok(Compilation::NormalV2(Fallback::UnsupportedSchema(
                "missing source table".into(),
            )));
        };
        let Some(member_schema) = ddl(tx, "members").await? else {
            return Ok(Compilation::NormalV2(Fallback::UnsupportedSchema(
                "missing persisted members".into(),
            )));
        };
        // No collation assumptions are inferred from TEXT. Explicit collations
        // require their own SQL/interpreted equivalence domain and adapter.
        if row_schema.to_ascii_uppercase().contains("COLLATE")
            || row_schema.to_ascii_uppercase().contains("VIRTUAL")
            || row_schema.to_ascii_uppercase().contains("WITHOUT ROWID")
            || member_schema.to_ascii_uppercase().contains("WITHOUT ROWID")
            || member_schema.to_ascii_uppercase().contains("COLLATE")
        {
            return Ok(Compilation::NormalV2(Fallback::UnsupportedSchema(
                "collation/virtual source".into(),
            )));
        }
        let columns =
            gonvex_store::rows::table_columns(tx.transaction(), &definition.table).await?;
        let members = gonvex_store::rows::table_columns(tx.transaction(), "members").await?;
        let by_name = columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect::<BTreeMap<_, _>>();
        if columns.iter().filter(|c| c.primary_key).count() != 1
            || !columns.iter().any(|c| {
                c.primary_key && c.name == definition.key && c.declared.eq_ignore_ascii_case("TEXT")
            })
            || !["id", "account_id", "status"].iter().all(|name| {
                members
                    .iter()
                    .any(|c| c.name == *name && c.declared.eq_ignore_ascii_case("TEXT"))
            })
        {
            return Ok(Compilation::NormalV2(Fallback::UnsupportedSchema(
                "identity/key physical types".into(),
            )));
        }
        let mut required: BTreeSet<String> = definition
            .columns
            .iter()
            .chain(definition.equal_filters.keys())
            .chain(definition.exclude_when_set.iter())
            .cloned()
            .collect();
        required.insert(definition.key.clone());
        if !definition.order_by.is_empty() {
            required.insert(definition.order_by.clone());
        }
        for (column, _) in policy.contexts() {
            required.insert(column.to_owned());
            if !by_name
                .get(column)
                .is_some_and(|c| c.declared.eq_ignore_ascii_case("TEXT"))
            {
                return Ok(Compilation::NormalV2(Fallback::UnsupportedSchema(
                    "context equality is not TEXT/BINARY".into(),
                )));
            }
        }
        required.extend(
            policy
                .literals()
                .into_iter()
                .map(|(column, _)| column.to_owned()),
        );
        if required.iter().any(|c| !by_name.contains_key(c.as_str()))
            || !definition.columns.contains(&definition.key)
            || (!definition.order_by.is_empty()
                && !matches!(definition.order_direction.as_str(), "asc" | "desc"))
            || definition.max_bytes < 0
            || definition.max_rows > (i64::MAX - 1) as usize
        {
            return Ok(Compilation::NormalV2(Fallback::UnsupportedSchema(
                "projection/filter/order/budget".into(),
            )));
        }
        let installed:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_permission_identity')").fetch_one(&mut **tx.transaction()).await?;
        if installed == 0 {
            return Ok(Compilation::NormalV2(Fallback::MissingIndex));
        }
        let instance = match indexes::instance(tx.transaction()).await {
            Ok(instance) => instance,
            Err(gonvex_store::DatabaseError::Decode(_)) => {
                return Ok(Compilation::NormalV2(Fallback::MissingIndex))
            }
            Err(e) => return Err(e.into()),
        };
        let epoch: String =
            sqlx::query_scalar("SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1")
                .fetch_one(&mut **tx.transaction())
                .await?;
        // A compiler/kernel change invalidates old materializations as well as
        // policy changes. This is an additional invalidation input; the sealed
        // proof still requires the actual typed module and physical sources.
        let kernel = hash(
            concat!(
                include_str!("../../../store/src/permission_indexes/program.rs"),
                include_str!("../../../store/src/permission_indexes/decision.rs"),
                include_str!("../../../store/src/permission_indexes/decision.sql"),
                include_str!("../../../store/src/permission_indexes/literal.rs")
            )
            .as_bytes(),
        );
        let authority = canonical(
            &json!({"format":"indexed-relational-policy-v1","kernel":kernel,"equivalence":"actual-v2-sql-and-interpreted-v1","project":module.project_id,"moduleGeneration":module.generation,"artifact":module.artifact_hash,"moduleSchema":module.schema,"admission":"persisted-active-member","policy":plan,"normalized":policy,"sourceSchemas":{"members":member_schema,"rows":row_schema},"allRootColumns":columns.iter().map(|c|(&c.name,&c.declared,c.not_null,c.primary_key)).collect::<Vec<_>>()}),
        );
        let program = Program {
            format: 1,
            table: definition.table.clone(),
            key: definition.key.clone(),
            tenant: session.route.tenant_id.clone(),
            policy,
            row_schema: row_schema.clone(),
            member_schema,
            authority: authority.clone(),
            projection_inputs: columns.iter().map(|c| c.name.clone()).collect(),
        };
        program.validate()?;
        let source_scope = PermissionScope {
            project: module.project_id.clone(),
            database_instance: instance,
            tenant: session.route.tenant_id.clone(),
            epoch,
            table: definition.table.clone(),
            collection: format!("__authorization.{}", definition.table),
            policy_version: hash(&authority),
            projection_version: hash(row_schema.as_bytes()),
            semantics_version: "indexed-relational-policy-v1".into(),
        };
        let function_authority = json!({"kind":function.kind,"delivery":function.delivery,"public":function.public,"internal":function.internal,"args":function.args_schema,"result":function.result_schema,"dependencies":function.dependencies,"definition":definition});
        let classification = if program
            .policy
            .contexts()
            .iter()
            .any(|(_, context)| matches!(context, Context::Member | Context::Account))
            || !definition.equal_filters.is_empty()
        {
            Classification::MemberList
        } else {
            Classification::SharedBase
        };
        Ok(Compilation::Complete(Box::new(Self {
            program,
            source_scope,
            definition,
            function_authority,
            path: path.into(),
            project: module.project_id.clone(),
            generation: module.generation,
            artifact: module.artifact_hash.clone(),
            classification,
        })))
    }
    pub fn canonical_program(&self) -> Vec<u8> {
        canonical(&self.program)
    }
    pub fn normalized_program(&self) -> &Program {
        &self.program
    }
    pub fn source_authority(&self) -> &[u8] {
        &self.program.authority
    }
    pub fn function_authority(&self) -> &Value {
        &self.function_authority
    }
    pub fn activation_identity(&self) -> (&str, u64, &str) {
        (&self.project, self.generation, &self.artifact)
    }
    pub fn source_scope(&self) -> &PermissionScope {
        &self.source_scope
    }
    pub fn definition(&self) -> &ReplicaCollectionDefinition {
        &self.definition
    }
    pub fn classification(&self) -> Classification {
        self.classification
    }
    pub async fn install(
        self: Box<Self>,
        tx: &mut TenantTransaction,
    ) -> Result<CompiledRegistration, Error> {
        let canonical = self.source_scope.canonical().expect("compiler scope");
        let id = indexes::create_scope(
            tx,
            &canonical,
            &self.source_scope.database_instance,
            &self.source_scope.epoch,
            &self.source_scope.table,
        )
        .await?;
        let state = indexes::state(tx.transaction(), id).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1")
                .fetch_one(&mut **tx.transaction())
                .await?;
        let reusable = state.phase == "ready"
            && state.covered_revision == revision
            && indexes::program::verify(tx.transaction(), id, &self.program)
                .await
                .is_ok();
        if !reusable {
            indexes::program::install(tx, id, &self.program).await?;
        }
        let generation = indexes::state(tx.transaction(), id).await?.generation;
        Ok(CompiledRegistration {
            certificate: *self,
            scope_id: id,
            generation,
        })
    }
}
impl CompiledRegistration {
    pub fn certificate(&self) -> &CompleteProviderCertificate {
        &self.certificate
    }
    pub fn scope_id(&self) -> i64 {
        self.scope_id
    }
    /// Caller acquires the real module lease before opening `tx` (module/tenant
    /// lock order). The returned scan retains that lease; no hidden new lock.
    pub async fn fresh<'tx>(
        &self,
        module: &ModuleCallLease,
        tx: &'tx mut TenantTransaction,
        session: &TenantSession,
        args: &Value,
    ) -> Result<Fresh<'tx>, Error> {
        let c = &self.certificate;
        if session.is_anonymous() || session.identity.account.id.is_empty() {
            return Ok(Fresh::NormalV2(Fallback::UnsupportedAdmission));
        }
        if module.project_id != c.project
            || module.generation != c.generation
            || module.artifact_hash != c.artifact
        {
            return Ok(Fresh::NormalV2(Fallback::ModuleOrProjectionChanged));
        }
        let current =
            match CompleteProviderCertificate::compile(module, tx, session, &c.path).await? {
                Compilation::Complete(p) => p,
                Compilation::NormalV2(r) => return Ok(Fresh::NormalV2(r)),
            };
        if current.program != c.program
            || current.function_authority != c.function_authority
            || current.source_scope != c.source_scope
        {
            return Ok(Fresh::NormalV2(Fallback::ModuleOrProjectionChanged));
        }
        if indexes::program::verify(tx.transaction(), self.scope_id, &c.program)
            .await
            .is_err()
        {
            return Ok(Fresh::NormalV2(Fallback::DomainOrIndexNotReady));
        }
        let state = indexes::state(tx.transaction(), self.scope_id).await?;
        let incomplete: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_permission_sources WHERE scope_id=?1 AND generation=?2 AND complete=0) OR EXISTS(SELECT 1 FROM _gonvex_permission_program_changes WHERE scope_id=?1) OR (SELECT count(*) FROM _gonvex_permission_sources WHERE scope_id=?1 AND generation=?2)<>2").bind(self.scope_id).bind(state.generation).fetch_one(&mut **tx.transaction()).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1")
                .fetch_one(&mut **tx.transaction())
                .await?;
        if incomplete != 0
            || state.phase != "ready"
            || state.generation <= 0
            || state.covered_revision != revision
            || state.canonical_scope != c.source_scope.canonical().expect("source scope")
            || state.table != c.source_scope.table
            || state.instance_id != c.source_scope.database_instance
            || state.epoch != c.source_scope.epoch
        {
            return Ok(Fresh::NormalV2(Fallback::DomainOrIndexNotReady));
        }
        let principal: Option<String> =
            sqlx::query_scalar("SELECT id FROM members WHERE account_id=?1 AND status='active'")
                .bind(&session.identity.account.id)
                .fetch_optional(&mut **tx.transaction())
                .await?;
        let Some(principal) = principal else {
            return Ok(Fresh::NormalV2(Fallback::UnsupportedAdmission));
        };
        if !args.is_object()
            || c.definition
                .equal_filters
                .values()
                .any(|a| args.get(a).is_none())
        {
            return Ok(Fresh::NormalV2(Fallback::Arguments));
        }
        if c.definition.order_by.is_empty() {
            return Ok(Fresh::NormalV2(Fallback::UnorderedSource));
        }
        let keys:Vec<Vec<u8>>=sqlx::query_scalar("SELECT a.canonical_key FROM _gonvex_permission_set_atoms p JOIN _gonvex_permission_atoms a ON a.scope_id=p.scope_id AND a.generation=p.generation AND a.atom_id=p.atom_id WHERE p.scope_id=?1 AND p.generation=?2 AND p.set_id=?3 ORDER BY a.canonical_key").bind(self.scope_id).bind(state.generation).bind(&principal).fetch_all(&mut **tx.transaction()).await?;
        let mut query_scope = c.source_scope.clone();
        query_scope.collection = c.path.clone();
        query_scope.projection_version = hash(&canonical(
            &json!({"function":c.function_authority,"args":args,"schema":c.program.row_schema}),
        ));
        query_scope.policy_version = hash(&canonical(
            &json!({"source":c.program.authority,"definition":c.function_authority}),
        ));
        let exact =
            ExactPermissionSet::from_keys(&query_scope, keys).expect("validated canonical set");
        let sql = indexes::decision::select_rows()
            .replace("?3", "?4")
            .replace("?2", "?3");
        let prepared = Prepared {
            compiled: CompiledSet {
                sql,
                extra_bindings: vec![],
            },
            identity: ReadIdentity {
                exact,
                generation: state.generation,
                change_serial: state.change_serial,
                source_revision: revision,
                principal,
            },
            scope_id: self.scope_id,
            table: c.definition.table.clone(),
        };
        Ok(Fresh::Indexed(Box::new(ValidatedScan {
            transaction: tx,
            prepared,
            definition: c.definition.clone(),
            args: args.clone(),
            source_scope: c.source_scope.clone(),
            query_scope,
            classification: c.classification,
            _module: module.clone(),
        })))
    }
}

#[cfg(test)]
mod tests;
