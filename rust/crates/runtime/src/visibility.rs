//! Centralized structured row visibility.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use gonvex_postgres::{DatabaseError, TenantSession, TenantTransaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::Row;
use thiserror::Error;

use crate::sql_parameters::Parameters;
use crate::visibility_index::ParentAclIndex;
use crate::visibility_mask::{match_expression, scalar};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityPlan {
    #[serde(default)]
    pub version: Option<u8>,
    #[serde(default)]
    pub permission_tables: Vec<String>,
    pub table: String,
    pub key: String,
    #[serde(default)]
    pub sets: BTreeMap<String, VisibilitySet>,
    #[serde(rename = "where")]
    pub predicate: VisibilityExpression,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilitySet {
    #[serde(default)]
    pub kind: String,
    pub table: String,
    #[serde(default)]
    pub alias: String,
    pub select: String,
    #[serde(default)]
    pub select_from: String,
    #[serde(default)]
    pub joins: Vec<VisibilityJoin>,
    #[serde(default, rename = "where")]
    pub constraints: Vec<VisibilityConstraint>,
    /// `derived` only: the set whose tokens are mapped through `table`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,
    /// `derived` only: the indexed table's key column the tokens name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityJoin {
    pub table: String,
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub left_alias: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityConstraint {
    #[serde(default)]
    pub table: String,
    pub column: String,
    #[serde(default)]
    pub context: String,
    #[serde(default)]
    pub value: Option<Value>,
    /// `derived` filters only: the indexed row's column must be one of this
    /// member's tokens in the named principal or grant set of the same plan.
    #[serde(default, rename = "inSet", skip_serializing_if = "String::is_empty")]
    pub in_set: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityExpression {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_missing: Option<Box<VisibilityExpression>>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
    /// `inherits`/`inheritsAny` only: literal filters on the parent row.
    #[serde(default, rename = "where", skip_serializing_if = "Vec::is_empty")]
    pub parent_where: Vec<VisibilityConstraint>,
    /// `inheritsAny` only: the association rows linking a row to its parents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<VisibilityVia>,
    pub operator: String,
    #[serde(default)]
    pub column: String,
    #[serde(default)]
    pub context: String,
    #[serde(default)]
    pub set: String,
    #[serde(default)]
    pub value: Option<Value>,
    #[serde(default)]
    pub children: Vec<VisibilityExpression>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityVia {
    pub table: String,
    pub child_column: String,
    pub parent_column: String,
}

/// Literal constraints on an indexed row: equality, or IS NULL for a null literal.
pub fn literal_constraints_hold(constraints: &[VisibilityConstraint], row: &Value) -> bool {
    constraints.iter().all(|constraint| {
        let literal = constraint
            .value
            .as_ref()
            .and_then(|value| value.get("literal"));
        match (row.get(&constraint.column), literal) {
            (Some(Value::Null) | None, Some(Value::Null)) => true,
            (Some(value), Some(literal)) => value == literal,
            _ => false,
        }
    })
}

#[derive(Clone, Debug)]
pub struct ResolvedVisibility {
    // Set only by verified developer admission when no active Member exists.
    pub(crate) ephemeral_member: bool,
    pub revision: u64,
    pub direct: BTreeMap<String, String>,
    pub role: String,
    pub permissions: Value,
    pub sets: BTreeMap<String, BTreeSet<String>>,
    pub fingerprint: String,
    /// Version 2 `inherits` parents, keyed by parent table: the parent's plan,
    /// this member's resolution of it, and the tenant-shared parent ACL index.
    pub inherited: BTreeMap<String, InheritedParent>,
    /// Version 2 `derived` sets: the tenant-shared index each one maps through.
    pub indexes: BTreeMap<String, Arc<std::sync::RwLock<ParentAclIndex>>>,
}

/// Everything an in-memory `inherits` check needs. Holds no database handle.
#[derive(Clone)]
pub struct InheritedParent {
    pub plan: Arc<VisibilityPlan>,
    pub resolved: Arc<ResolvedVisibility>,
    pub index: Arc<std::sync::RwLock<ParentAclIndex>>,
}

impl InheritedParent {
    /// The parent's projected ACL row, or None when the parent does not exist.
    pub fn row(&self, key: &str) -> Option<Arc<Value>> {
        self.index.read().ok()?.get(key)
    }
}

impl std::fmt::Debug for InheritedParent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InheritedParent")
            .field("table", &self.plan.table)
            .field("resolved", &self.resolved)
            .finish()
    }
}

/// Grant-set join tokens live beside ordinary sets under this suffix, e.g. the
/// member's team ids for a grant set joined to memberTeams.
pub const GRANT_JOIN_SUFFIX: &str = "::join";

/// Parent ACL indexes shared by every connection of a tenant database. Each
/// index is loaded once and then advanced by feed transactions in revision
/// order; applying a revision a second time is a no-op.
#[derive(Default)]
pub struct ParentAclRegistry {
    indexes: std::sync::Mutex<BTreeMap<String, Arc<std::sync::RwLock<ParentAclIndex>>>>,
    loading: tokio::sync::Mutex<()>,
}

fn parent_index_key(session: &TenantSession, table: &str, key: &str) -> String {
    format!(
        "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
        session.route.project_id, session.route.tenant_id, session.route.database_url, table, key
    )
}

fn tenant_index_prefix(session: &TenantSession) -> String {
    format!(
        "{}\u{0}{}\u{0}{}\u{0}",
        session.route.project_id, session.route.tenant_id, session.route.database_url
    )
}

/// Columns a tenant index of `table` keyed by `key` projects: what the table's
/// own plan reads when inherited, and what every derived set maps through.
pub fn index_columns(
    plans: &BTreeMap<String, VisibilityPlan>,
    table: &str,
    key: &str,
) -> BTreeSet<String> {
    let mut columns = BTreeSet::from([key.to_owned()]);
    if let Some(plan) = plans.get(table) {
        if plan.key == key {
            columns.extend(acl_columns(plan));
        }
    }
    fn parent_filters(
        expression: &VisibilityExpression,
        table: &str,
        columns: &mut BTreeSet<String>,
    ) {
        if matches!(expression.operator.as_str(), "inherits" | "inheritsAny")
            && expression.table == table
        {
            columns.extend(
                expression
                    .parent_where
                    .iter()
                    .map(|constraint| constraint.column.clone()),
            );
        }
        for child in &expression.children {
            parent_filters(child, table, columns);
        }
        if let Some(fallback) = &expression.on_missing {
            parent_filters(fallback, table, columns);
        }
    }
    for plan in plans.values() {
        if plans.get(table).is_some_and(|parent| parent.key == key) {
            parent_filters(&plan.predicate, table, &mut columns);
        }
        for expression in inherits_any(plan) {
            let Some(via) = &expression.via else { continue };
            if via.table == table && via.child_column == key {
                columns.insert(via.parent_column.clone());
            }
            if via.table == table && via.parent_column == key {
                columns.insert(via.child_column.clone());
            }
        }
        for set in plan.sets.values() {
            if set.kind == "derived" && set.table == table && set.key == key {
                columns.insert(set.select.clone());
                columns.extend(
                    set.constraints
                        .iter()
                        .map(|constraint| constraint.column.clone()),
                );
            }
        }
    }
    columns
}

impl ParentAclRegistry {
    async fn get_or_load(
        &self,
        transaction: &mut TenantTransaction,
        session: &TenantSession,
        plans: &BTreeMap<String, VisibilityPlan>,
        table: &str,
        key_column: &str,
        revision: u64,
    ) -> Result<Arc<std::sync::RwLock<ParentAclIndex>>, VisibilityError> {
        let key = parent_index_key(session, table, key_column);
        if let Some(index) = self.indexes.lock().expect("parent ACL registry").get(&key) {
            return Ok(index.clone());
        }
        let _loading = self.loading.lock().await;
        if let Some(index) = self.indexes.lock().expect("parent ACL registry").get(&key) {
            return Ok(index.clone());
        }
        let columns = index_columns(plans, table, key_column);
        let projection = columns
            .iter()
            .map(|column| {
                Ok(format!(
                    "{}, r.{}",
                    sql_text_literal(column),
                    quote(column)?
                ))
            })
            .collect::<Result<Vec<_>, VisibilityError>>()?
            .join(", ");
        let id_column = row_id_column(plans, table);
        let statement = format!(
            "SELECT r.{}::text, r.{}::text, jsonb_build_object({projection})::text FROM {} AS r",
            quote(&id_column)?,
            quote(key_column)?,
            quote(table)?
        );
        let rows = sqlx::query(&statement)
            .fetch_all(&mut **transaction.transaction())
            .await?;
        let mut projected = Vec::new();
        for row in rows {
            let id: Option<String> = row.try_get(0)?;
            let key: Option<String> = row.try_get(1)?;
            let value: String = row.try_get(2)?;
            if let (Some(id), Some(key)) = (id, key) {
                projected.push((
                    id,
                    key,
                    serde_json::from_str(&value).map_err(|error| {
                        VisibilityError::Invalid(format!("parent ACL row: {error}"))
                    })?,
                ));
            }
        }
        let index = Arc::new(std::sync::RwLock::new(ParentAclIndex::from_rows(
            revision, projected,
        )));
        self.indexes
            .lock()
            .expect("parent ACL registry")
            .insert(key, index.clone());
        Ok(index)
    }

    /// Advance every loaded parent index of this tenant database by one feed
    /// transaction. Called by each connection; only the first call applies it.
    pub fn apply(
        &self,
        session: &TenantSession,
        plans: &BTreeMap<String, VisibilityPlan>,
        revision: u64,
        changes: &[crate::change_feed::LogChange],
    ) {
        let prefix = tenant_index_prefix(session);
        let indexes = self
            .indexes
            .lock()
            .expect("parent ACL registry")
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .filter_map(|(key, index)| {
                let (table, key_column) = key[prefix.len()..].split_once('\u{0}')?;
                Some((table.to_owned(), key_column.to_owned(), index.clone()))
            })
            .collect::<Vec<_>>();
        for (table, key_column, index) in indexes {
            let columns = index_columns(plans, &table, &key_column);
            let updates = changes
                .iter()
                .filter(|change| change.table == table)
                .map(|change| {
                    let row = change.new_value.as_object().and_then(|row| {
                        let key = match row.get(&key_column) {
                            None | Some(Value::Null) => return None,
                            Some(value) => scalar(Some(value)),
                        };
                        Some((
                            key,
                            Value::Object(
                                columns
                                    .iter()
                                    .map(|column| {
                                        (
                                            column.clone(),
                                            row.get(column).cloned().unwrap_or(Value::Null),
                                        )
                                    })
                                    .collect(),
                            ),
                        ))
                    });
                    (change.row_id.clone(), row)
                })
                .collect::<Vec<_>>();
            if let Ok(mut index) = index.write() {
                index.apply_rows(revision, updates);
            }
        }
    }

    /// A database reset invalidates every projection of that tenant database.
    pub fn reset(&self, session: &TenantSession) {
        let prefix = tenant_index_prefix(session);
        self.indexes
            .lock()
            .expect("parent ACL registry")
            .retain(|key, _| !key.starts_with(&prefix));
    }
}

/// The column the change feed uses as a table's row id: its plan key, or the
/// Gonvex default `_id` for tables without a visibility plan.
fn row_id_column(plans: &BTreeMap<String, VisibilityPlan>, table: &str) -> String {
    plans
        .get(table)
        .map(|plan| plan.key.clone())
        .unwrap_or_else(|| "_id".to_owned())
}

fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Row columns a plan's predicate reads, including inherited foreign keys and
/// the key itself. These are the only columns a parent ACL index projects.
pub fn acl_columns(plan: &VisibilityPlan) -> BTreeSet<String> {
    fn visit(expression: &VisibilityExpression, columns: &mut BTreeSet<String>) {
        if matches!(
            expression.operator.as_str(),
            "eq" | "eqContext" | "inSet" | "inherits"
        ) && !expression.column.is_empty()
        {
            columns.insert(expression.column.clone());
        }
        for child in &expression.children {
            visit(child, columns);
        }
        if let Some(fallback) = &expression.on_missing {
            visit(fallback, columns);
        }
    }
    let mut columns = BTreeSet::from([plan.key.clone()]);
    visit(&plan.predicate, &mut columns);
    columns
}

/// Index name of an `inheritsAny` association, keyed by one of its columns.
pub fn via_index_name(via: &VisibilityVia, column: &str) -> String {
    format!("via\u{0}{}\u{0}{}", via.table, column)
}

/// Every `inheritsAny` expression of a plan.
pub fn inherits_any(plan: &VisibilityPlan) -> Vec<&VisibilityExpression> {
    fn visit<'a>(expression: &'a VisibilityExpression, output: &mut Vec<&'a VisibilityExpression>) {
        if expression.operator == "inheritsAny" {
            output.push(expression);
        }
        for child in &expression.children {
            visit(child, output);
        }
        if let Some(fallback) = &expression.on_missing {
            visit(fallback, output);
        }
    }
    let mut output = Vec::new();
    visit(&plan.predicate, &mut output);
    output
}

/// Tables a version 2 plan inherits from, directly (not transitively).
pub fn inherited_tables(plan: &VisibilityPlan) -> BTreeSet<String> {
    fn visit(expression: &VisibilityExpression, tables: &mut BTreeSet<String>) {
        if matches!(expression.operator.as_str(), "inherits" | "inheritsAny") {
            tables.insert(expression.table.clone());
        }
        for child in &expression.children {
            visit(child, tables);
        }
        if let Some(fallback) = &expression.on_missing {
            visit(fallback, tables);
        }
    }
    let mut tables = BTreeSet::new();
    visit(&plan.predicate, &mut tables);
    tables
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VisibilityDependencies {
    columns: BTreeMap<String, BTreeSet<String>>,
}

impl VisibilityDependencies {
    pub fn tables(&self) -> BTreeSet<String> {
        self.columns.keys().cloned().collect()
    }

    pub fn change_affects(&self, table: &str, operation: &str, changed_columns: &[String]) -> bool {
        let Some(relevant_columns) = self.columns.get(table) else {
            return false;
        };
        if !operation.eq_ignore_ascii_case("update") {
            return true;
        }
        changed_columns
            .iter()
            .any(|column| relevant_columns.contains(column))
    }
}

#[derive(Debug, Error)]
pub enum VisibilityError {
    #[error("invalid visibility plan: {0}")]
    Invalid(String),
    #[error("active tenant member is required for visibility")]
    MemberRequired,
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

impl VisibilityPlan {
    pub fn validate(&self) -> Result<(), VisibilityError> {
        identifier(&self.table)?;
        identifier(&self.key)?;
        if self.version.is_some_and(|version| version != 2) {
            return Err(VisibilityError::Invalid(
                "visibility version must be 2".into(),
            ));
        }
        let permissions: BTreeSet<_> = self.permission_tables.iter().collect();
        for table in &self.permission_tables {
            identifier(table)?;
        }
        if permissions.len() != self.permission_tables.len() {
            return Err(VisibilityError::Invalid(
                "permissionTables contains duplicates".into(),
            ));
        }
        for (name, set) in &self.sets {
            if self.version == Some(2) {
                let fix =
                    "use a permission-only principal set, a single-source grant set, or inherits";
                let invalid = |reason: String| {
                    VisibilityError::Invalid(format!("set {name:?}: {reason}; {fix}"))
                };
                if set.kind != "principal" && set.kind != "grant" && set.kind != "derived" {
                    return Err(invalid("kind must be principal, grant or derived".into()));
                }
                if set.kind == "derived" {
                    identifier(&set.key)?;
                    let Some(source) = self.sets.get(&set.from) else {
                        return Err(invalid(format!("derived from unknown set {:?}", set.from)));
                    };
                    if source.kind == "derived"
                        && self
                            .sets
                            .get(&source.from)
                            .is_some_and(|inner| inner.kind == "derived")
                    {
                        return Err(invalid("derived chains are limited to two sets".into()));
                    }
                    if set.from == *name || (source.kind == "derived" && source.from == *name) {
                        return Err(invalid("derived sets must be acyclic".into()));
                    }
                    if set.table == self.table {
                        return Err(invalid("a derived set maps through another table".into()));
                    }
                    if !set.joins.is_empty() {
                        return Err(invalid("a derived set has no joins".into()));
                    }
                    for constraint in &set.constraints {
                        if !constraint.table.is_empty() {
                            return Err(invalid(
                                "derived filters name indexed-row columns only".into(),
                            ));
                        }
                        if !constraint.in_set.is_empty()
                            && !self.sets.get(&constraint.in_set).is_some_and(|other| {
                                other.kind == "principal" || other.kind == "grant"
                            })
                        {
                            return Err(invalid(format!(
                                "derived inSet {:?} must name a principal or grant set",
                                constraint.in_set
                            )));
                        }
                    }
                }
                // A grant set may read its own source table (for example the
                // assignment rows of tasks a member is assigned to): its rows
                // are patched in memory and route only to the members they
                // name. A principal set over its own source would make every
                // source write a principal change, which is the fan-out V2 bans.
                // A source that is itself a declared permission table (for
                // example spots, whose ancestry is its own tree) changes only
                // by rare administrative writes, so its principal sets may
                // read it.
                let source_is_permission = permissions.contains(&self.table);
                if !source_is_permission
                    && ((set.kind != "grant" && set.table == self.table)
                        || set.joins.iter().any(|join| join.table == self.table))
                {
                    return Err(invalid("reads its own source table".into()));
                }
                if set.kind == "principal" && !permissions.contains(&set.table) {
                    return Err(invalid(format!(
                        "undeclared permission table {:?}",
                        set.table
                    )));
                }
                for join in set.joins.iter().filter(|_| set.kind != "derived") {
                    if !permissions.contains(&join.table) {
                        return Err(invalid(format!("joins data table {:?}", join.table)));
                    }
                }
                if set.kind == "grant"
                    && !set.constraints.iter().any(|constraint| {
                        matches!(constraint.context.as_str(), "member.id" | "account.id")
                    })
                {
                    return Err(invalid(
                        "grant requires member.id or account.id anchor".into(),
                    ));
                }
                if set
                    .constraints
                    .iter()
                    .any(|constraint| constraint.context == "tenant.id")
                {
                    return Err(invalid("tenant.id context is forbidden".into()));
                }
            }
            identifier(name)?;
            validate_set(set)?;
        }
        validate_expression(&self.predicate, &self.sets)
    }

    pub fn dependencies(&self) -> BTreeSet<String> {
        self.dependency_columns().tables()
    }

    pub fn dependency_columns(&self) -> VisibilityDependencies {
        self.dependency_columns_for_sets(None)
    }

    /// Role-only branches can make row/set branches irrelevant for this member.
    /// Member admission and role changes remain dependencies in every case.
    /// Row decisions need membership values only for branches not settled by
    /// the admitted role. SQL-only snapshots leave these values on the server.
    pub fn needs_materialized_sets(&self, resolved: &ResolvedVisibility) -> bool {
        let sets = active_sets(self, &resolved.role);
        !sets.is_empty()
    }

    pub fn resolved_dependency_columns(
        &self,
        resolved: &ResolvedVisibility,
    ) -> VisibilityDependencies {
        let sets = active_sets(self, &resolved.role);
        self.dependency_columns_for_sets(Some(&sets))
    }

    /// Conservative row routing for legacy declarations. A changed occurrence
    /// can be skipped only when its own context constraint proves both old and
    /// new rows belong to another principal. Joins without such an anchor keep
    /// their dependency until migrated to permission-token reachability.
    pub fn change_concerns_member(
        &self,
        resolved: &ResolvedVisibility,
        table: &str,
        old: &Value,
        new: &Value,
    ) -> bool {
        if old.is_null() && new.is_null() {
            return true;
        }
        if table == "members" {
            for row in [old, new].into_iter().filter(|row| !row.is_null()) {
                match row.get("id").and_then(Value::as_str) {
                    Some(id)
                        if Some(id) != resolved.direct.get("member.id").map(String::as_str) => {}
                    _ => return true,
                }
            }
        }
        let active = active_sets(self, &resolved.role);
        for name in active {
            let set = &self.sets[&name];
            let base = if set.alias.is_empty() {
                set.table.as_str()
            } else {
                set.alias.as_str()
            };
            let occurrences =
                std::iter::once((set.table.as_str(), base)).chain(set.joins.iter().map(|join| {
                    (
                        join.table.as_str(),
                        if join.alias.is_empty() {
                            join.table.as_str()
                        } else {
                            join.alias.as_str()
                        },
                    )
                }));
            for (_, alias) in occurrences.filter(|(source, _)| *source == table) {
                let anchors = set
                    .constraints
                    .iter()
                    .filter(|constraint| {
                        (constraint.table == alias
                            || (constraint.table.is_empty() && alias == base))
                            && !constraint.context.is_empty()
                    })
                    .collect::<Vec<_>>();
                if anchors.is_empty() {
                    return true;
                }
                for row in [old, new].into_iter().filter(|row| !row.is_null()) {
                    // A missing feed column is unknown, never evidence to skip.
                    let excludes = anchors.iter().any(|anchor| {
                        match (
                            row.get(&anchor.column),
                            resolved.direct.get(&anchor.context),
                        ) {
                            (Some(value), Some(expected)) => scalar(Some(value)) != *expected,
                            _ => false,
                        }
                    });
                    if !excludes {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Version 2: does this change alter the member's principal, i.e. a
    /// principal set, a grant set's permission join, or the member row itself?
    /// Grant source rows are not principal inputs; they are patched in memory.
    /// Inherited parents are checked recursively with their own resolution.
    pub fn principal_change_concerns(
        &self,
        resolved: &ResolvedVisibility,
        table: &str,
        operation: &str,
        changed_columns: &[String],
        old: &Value,
        new: &Value,
    ) -> bool {
        if table == "members" {
            let member = resolved.direct.get("member.id").map(String::as_str);
            let names_member = [old, new]
                .into_iter()
                .filter(|row| !row.is_null())
                .any(|row| {
                    row.get("id")
                        .and_then(Value::as_str)
                        .is_none_or(|id| Some(id) == member)
                });
            let relevant = !operation.eq_ignore_ascii_case("update")
                || changed_columns.iter().any(|column| {
                    matches!(
                        column.as_str(),
                        "id" | "account_id" | "status" | "role" | "permissions"
                    )
                });
            if names_member && relevant {
                return true;
            }
        }
        let active = active_sets(self, &resolved.role);
        for name in &active {
            let set = &self.sets[name];
            let base = if set.alias.is_empty() {
                set.table.as_str()
            } else {
                set.alias.as_str()
            };
            let mut occurrences = Vec::new();
            if set.kind != "grant" {
                occurrences.push((set.table.as_str(), base));
            }
            for join in &set.joins {
                occurrences.push((
                    join.table.as_str(),
                    if join.alias.is_empty() {
                        join.table.as_str()
                    } else {
                        join.alias.as_str()
                    },
                ));
            }
            if !occurrences.iter().any(|(source, _)| *source == table) {
                continue;
            }
            let dependencies =
                self.dependency_columns_for_sets(Some(&BTreeSet::from([name.clone()])));
            if !dependencies.change_affects(table, operation, changed_columns) {
                continue;
            }
            for (_, alias) in occurrences.iter().filter(|(source, _)| *source == table) {
                let anchors = set
                    .constraints
                    .iter()
                    .filter(|constraint| {
                        !constraint.context.is_empty()
                            && (constraint.table == *alias
                                || (constraint.table.is_empty() && *alias == base))
                    })
                    .collect::<Vec<_>>();
                // Unanchored permission occurrences (for example workspaceTeams
                // reached through memberTeams) stay conservative: rare admin
                // writes rebuild, ordinary data writes never reach this path.
                if anchors.is_empty() {
                    return true;
                }
                for row in [old, new].into_iter().filter(|row| !row.is_null()) {
                    let excludes = anchors.iter().any(|anchor| {
                        match (
                            row.get(&anchor.column),
                            resolved.direct.get(&anchor.context),
                        ) {
                            (Some(value), Some(expected)) => scalar(Some(value)) != *expected,
                            _ => false,
                        }
                    });
                    if !excludes {
                        return true;
                    }
                }
            }
        }
        resolved.inherited.values().any(|parent| {
            parent.plan.principal_change_concerns(
                &parent.resolved,
                table,
                operation,
                changed_columns,
                old,
                new,
            )
        })
    }

    /// Active grant sets of a version 2 plan, by name.
    pub fn active_grant_sets<'a>(
        &'a self,
        resolved: &ResolvedVisibility,
    ) -> Vec<(&'a String, &'a VisibilitySet)> {
        let active = active_sets(self, &resolved.role);
        self.sets
            .iter()
            .filter(|(name, set)| set.kind == "grant" && active.contains(*name))
            .collect()
    }

    /// Does some `inherits` of this plan admit children of `row`, a row of the
    /// inherited `table`: its parent filters hold and the parent is visible?
    pub fn parent_admits(&self, table: &str, parent: &InheritedParent, row: &Value) -> bool {
        fn visit(
            expression: &VisibilityExpression,
            table: &str,
            parent: &InheritedParent,
            row: &Value,
        ) -> bool {
            (matches!(expression.operator.as_str(), "inherits" | "inheritsAny")
                && expression.table == table
                && literal_constraints_hold(&expression.parent_where, row)
                && row_matches(&parent.plan, &parent.resolved, row))
                || expression
                    .children
                    .iter()
                    .any(|child| visit(child, table, parent, row))
                || expression
                    .on_missing
                    .as_ref()
                    .is_some_and(|fallback| visit(fallback, table, parent, row))
        }
        !row.is_null() && visit(&self.predicate, table, parent, row)
    }

    /// Child columns that reference each directly inherited parent table.
    pub fn inherited_columns(&self) -> BTreeMap<String, BTreeSet<String>> {
        fn visit(
            expression: &VisibilityExpression,
            output: &mut BTreeMap<String, BTreeSet<String>>,
        ) {
            if expression.operator == "inherits" {
                output
                    .entry(expression.table.clone())
                    .or_default()
                    .insert(expression.column.clone());
            }
            for child in &expression.children {
                visit(child, output);
            }
            if let Some(fallback) = &expression.on_missing {
                visit(fallback, output);
            }
        }
        let mut output = BTreeMap::new();
        visit(&self.predicate, &mut output);
        output
    }

    fn dependency_columns_for_sets(
        &self,
        active: Option<&BTreeSet<String>>,
    ) -> VisibilityDependencies {
        let mut columns = BTreeMap::<String, BTreeSet<String>>::new();
        columns.insert(
            "members".to_owned(),
            BTreeSet::from([
                "id".to_owned(),
                "account_id".to_owned(),
                "status".to_owned(),
                "role".to_owned(),
                "permissions".to_owned(),
            ]),
        );
        for (name, set) in &self.sets {
            if active.is_some_and(|names| !names.contains(name)) {
                continue;
            }
            let base_alias = if set.alias.is_empty() {
                set.table.as_str()
            } else {
                set.alias.as_str()
            };
            let mut aliases = BTreeMap::from([(base_alias.to_owned(), set.table.clone())]);
            let mut previous = base_alias.to_owned();
            for join in &set.joins {
                let left_alias = if join.left_alias.is_empty() {
                    previous.as_str()
                } else {
                    join.left_alias.as_str()
                };
                if let Some(left_table) = aliases.get(left_alias) {
                    columns
                        .entry(left_table.clone())
                        .or_default()
                        .insert(join.left_column.clone());
                }
                columns
                    .entry(join.table.clone())
                    .or_default()
                    .insert(join.right_column.clone());
                let alias = if join.alias.is_empty() {
                    join.table.as_str()
                } else {
                    join.alias.as_str()
                };
                aliases.insert(alias.to_owned(), join.table.clone());
                previous = alias.to_owned();
            }
            let select_from = if set.select_from.is_empty() {
                base_alias
            } else {
                set.select_from.as_str()
            };
            if let Some(select_table) = aliases.get(select_from) {
                columns
                    .entry(select_table.clone())
                    .or_default()
                    .insert(set.select.clone());
            }
            for constraint in &set.constraints {
                let table = if constraint.table.is_empty() {
                    Some(&set.table)
                } else {
                    aliases.get(&constraint.table)
                };
                if let Some(table) = table {
                    columns
                        .entry(table.clone())
                        .or_default()
                        .insert(constraint.column.clone());
                }
            }
        }
        VisibilityDependencies { columns }
    }
}

/// Validate cross-plan contracts once when admitting a module, before any
/// connection can observe an incomplete inheritance chain.
pub fn validate_manifest(plans: &BTreeMap<String, VisibilityPlan>) -> Result<(), VisibilityError> {
    fn parents<'a>(
        expression: &'a VisibilityExpression,
        output: &mut Vec<&'a VisibilityExpression>,
    ) {
        if expression.operator == "inherits" {
            output.push(expression);
        }
        for child in &expression.children {
            parents(child, output);
        }
        if let Some(fallback) = &expression.on_missing {
            parents(fallback, output);
        }
    }
    fn visit(
        name: &str,
        plans: &BTreeMap<String, VisibilityPlan>,
        path: &mut BTreeSet<String>,
        depth: usize,
    ) -> Result<(), VisibilityError> {
        if depth > 4 {
            return Err(VisibilityError::Invalid(format!(
                "inheritance exceeds four edges at {name:?}"
            )));
        }
        if !path.insert(name.to_owned()) {
            return Err(VisibilityError::Invalid(format!(
                "inheritance cycle at {name:?}"
            )));
        }
        let plan = &plans[name];
        let mut references = Vec::new();
        parents(&plan.predicate, &mut references);
        for reference in references {
            let parent = plans.get(&reference.table).ok_or_else(|| {
                VisibilityError::Invalid(format!(
                    "plan {name:?} inherits unknown parent {:?}",
                    reference.table
                ))
            })?;
            if plan.version != Some(2) || parent.version != Some(2) {
                return Err(VisibilityError::Invalid(format!(
                    "plan {name:?}: inherits requires version 2 child and parent"
                )));
            }
            if reference.key != parent.key {
                return Err(VisibilityError::Invalid(format!(
                    "plan {name:?}: inherited key {:?} differs from parent key {:?}",
                    reference.key, parent.key
                )));
            }
            visit(&reference.table, plans, path, depth + 1)?;
        }
        path.remove(name);
        Ok(())
    }
    let mut declaration: Option<BTreeSet<&String>> = None;
    for (name, plan) in plans {
        plan.validate()?;
        if plan.version == Some(2) {
            let tables = plan.permission_tables.iter().collect::<BTreeSet<_>>();
            if declaration
                .as_ref()
                .is_some_and(|expected| *expected != tables)
            {
                return Err(VisibilityError::Invalid(format!(
                    "plan {name:?}: permissionTables must agree across version 2 plans"
                )));
            }
            declaration = Some(tables);
        }
        visit(name, plans, &mut BTreeSet::new(), 0)?;
    }
    Ok(())
}

// Only constants and the already admitted role are evaluated here. Row values,
// set contents and permissions stay unknown; their dependencies are retained.
// This never changes the SQL predicate or the row visibility decision.
fn role_only_value(expression: &VisibilityExpression, role: &str) -> Option<bool> {
    match expression.operator.as_str() {
        "public" => Some(true),
        "role" => expression
            .value
            .as_ref()?
            .as_str()
            .map(|expected| expected == role),
        "and" | "or" if !expression.children.is_empty() => {
            let is_and = expression.operator == "and";
            let mut unknown = false;
            for child in &expression.children {
                match role_only_value(child, role) {
                    Some(value) if value != is_and => return Some(value),
                    None => unknown = true,
                    _ => {}
                }
            }
            if unknown {
                None
            } else {
                Some(is_and)
            }
        }
        "not" if expression.children.len() == 1 => {
            role_only_value(&expression.children[0], role).map(|value| !value)
        }
        _ => None,
    }
}

/// Sets a predicate references for this role, plus every set a `derived`
/// set maps from, transitively.
fn active_sets(plan: &VisibilityPlan, role: &str) -> BTreeSet<String> {
    let mut active = BTreeSet::new();
    active_visibility_sets(&plan.predicate, role, &mut active);
    let mut pending = active.iter().cloned().collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        if let Some(set) = plan.sets.get(&name) {
            if set.kind == "derived" {
                let needed = std::iter::once(set.from.clone())
                    .chain(
                        set.constraints
                            .iter()
                            .map(|constraint| constraint.in_set.clone()),
                    )
                    .filter(|name| !name.is_empty())
                    .collect::<Vec<_>>();
                for name in needed {
                    if active.insert(name.clone()) {
                        pending.push(name);
                    }
                }
            }
        }
    }
    active
}

fn active_visibility_sets(
    expression: &VisibilityExpression,
    role: &str,
    sets: &mut BTreeSet<String>,
) {
    if role_only_value(expression, role).is_some() {
        return;
    }
    if matches!(expression.operator.as_str(), "inSet" | "nonEmpty") {
        sets.insert(expression.set.clone());
    }
    for child in &expression.children {
        active_visibility_sets(child, role, sets);
    }
    if let Some(fallback) = &expression.on_missing {
        active_visibility_sets(fallback, role, sets);
    }
}

pub async fn resolve(
    transaction: &mut TenantTransaction,
    session: &TenantSession,
    plan: &VisibilityPlan,
    revision: u64,
) -> Result<ResolvedVisibility, VisibilityError> {
    resolve_member(transaction, session, plan, revision, true).await
}

/// SQL windows enforce set membership in their SQL predicate. Never fetch the
/// entire tenant membership into Rust before executing a bounded query.
pub async fn resolve_for_query(
    transaction: &mut TenantTransaction,
    session: &TenantSession,
    plan: &VisibilityPlan,
    revision: u64,
) -> Result<ResolvedVisibility, VisibilityError> {
    resolve_member(transaction, session, plan, revision, false).await
}

/// Related SQL predicates share the same admitted member and database snapshot.
pub fn resolve_related_query(
    plan: &VisibilityPlan,
    member: &ResolvedVisibility,
) -> Result<ResolvedVisibility, VisibilityError> {
    plan.validate()?;
    let mut resolved = member.clone();
    resolved.sets.clear();
    resolved.fingerprint = visibility_fingerprint(
        plan,
        &resolved.direct,
        &resolved.role,
        &resolved.permissions,
        &resolved.sets,
    );
    Ok(resolved)
}

async fn resolve_member(
    transaction: &mut TenantTransaction,
    session: &TenantSession,
    plan: &VisibilityPlan,
    revision: u64,
    materialize: bool,
) -> Result<ResolvedVisibility, VisibilityError> {
    plan.validate()?;
    let row = sqlx::query(
        r#"SELECT id, role, permissions
           FROM members WHERE account_id = $1 AND status = 'active'"#,
    )
    .bind(&session.identity.account.id)
    .fetch_optional(&mut **transaction.transaction())
    .await?;
    let (member_id, role, permissions, ephemeral_member) = match row {
        Some(row) => (
            row.get::<String, _>("id"),
            row.get::<String, _>("role"),
            row.try_get::<Json<Value>, _>("permissions")?.0,
            false,
        ),
        // Admission already verified this self developer grant and project
        // authority. Use its ephemeral principal without creating a Member.
        // Persisted active members above retain their real role/permissions.
        None if session.developer_session.is_some()
            && !session.is_anonymous()
            && session.member.id == format!("developer:{}", session.identity.account.id)
            && session.member.account_id == session.identity.account.id
            && session.member.status == "active" =>
        {
            (
                session.member.id.clone(),
                session.member.role.clone(),
                session.member.permissions.clone(),
                true,
            )
        }
        None => return Err(VisibilityError::MemberRequired),
    };
    let direct = BTreeMap::from([
        ("account.id".to_owned(), session.identity.account.id.clone()),
        ("member.id".to_owned(), member_id),
        ("tenant.id".to_owned(), session.route.tenant_id.clone()),
    ]);
    let mut sets = BTreeMap::new();
    let mut active = BTreeSet::new();
    if materialize || plan.version == Some(2) {
        active.extend(active_sets(plan, &role));
    }
    for (name, set) in &plan.sets {
        if !active.contains(name) || set.kind == "derived" {
            continue;
        }
        let (statement, parameters) = compile_set(set, &direct)?;
        let parameters = Parameters::scalars(&parameters).map_err(VisibilityError::Invalid)?;
        let statement_key = parameters.statement(&statement);
        let query = parameters.bind(sqlx::query(statement_key.as_ref()));
        let rows = query.fetch_all(&mut **transaction.transaction()).await?;
        let values = rows
            .into_iter()
            .filter_map(|row| row.try_get::<Option<String>, _>(0).ok().flatten())
            .collect();
        sets.insert(name.clone(), values);
        if plan.version == Some(2) && set.kind == "grant" && !set.joins.is_empty() {
            if let Some(join_set) = grant_join_set(set) {
                let (statement, parameters) = compile_set(&join_set, &direct)?;
                let parameters =
                    Parameters::scalars(&parameters).map_err(VisibilityError::Invalid)?;
                let statement_key = parameters.statement(&statement);
                let query = parameters.bind(sqlx::query(statement_key.as_ref()));
                let rows = query.fetch_all(&mut **transaction.transaction()).await?;
                sets.insert(
                    format!("{name}{GRANT_JOIN_SUFFIX}"),
                    rows.into_iter()
                        .filter_map(|row| row.try_get::<Option<String>, _>(0).ok().flatten())
                        .collect(),
                );
            }
        }
    }
    let fingerprint = visibility_fingerprint(plan, &direct, &role, &permissions, &sets);
    Ok(ResolvedVisibility {
        ephemeral_member,
        revision,
        direct,
        role,
        permissions,
        sets,
        fingerprint,
        inherited: BTreeMap::new(),
        indexes: BTreeMap::new(),
    })
}

/// Resolve a version 2 plan and, recursively, every plan it inherits from for
/// the same member, attaching the tenant-shared parent ACL indexes. Legacy
/// plans resolve exactly as before.
pub fn resolve_with_parents<'a>(
    transaction: &'a mut TenantTransaction,
    session: &'a TenantSession,
    plan: &'a VisibilityPlan,
    revision: u64,
    plans: &'a BTreeMap<String, VisibilityPlan>,
    registry: &'a ParentAclRegistry,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ResolvedVisibility, VisibilityError>> + Send + 'a>,
> {
    Box::pin(async move {
        let mut resolved = resolve_member(transaction, session, plan, revision, true).await?;
        if plan.version != Some(2) {
            return Ok(resolved);
        }
        for table in inherited_tables(plan) {
            let parent = plans.get(&table).ok_or_else(|| {
                VisibilityError::Invalid(format!("unknown inherited parent {table:?}"))
            })?;
            let parent_resolved =
                resolve_with_parents(transaction, session, parent, revision, plans, registry)
                    .await?;
            let index = registry
                .get_or_load(
                    transaction,
                    session,
                    plans,
                    &parent.table,
                    &parent.key,
                    revision,
                )
                .await?;
            resolved.inherited.insert(
                table,
                InheritedParent {
                    plan: Arc::new(parent.clone()),
                    resolved: Arc::new(parent_resolved),
                    index,
                },
            );
        }
        for expression in inherits_any(plan) {
            let Some(via) = &expression.via else { continue };
            for column in [&via.child_column, &via.parent_column] {
                let index = registry
                    .get_or_load(transaction, session, plans, &via.table, column, revision)
                    .await?;
                resolved.indexes.insert(via_index_name(via, column), index);
            }
        }
        for name in active_sets(plan, &resolved.role) {
            let set = &plan.sets[&name];
            if set.kind != "derived" {
                continue;
            }
            let index = registry
                .get_or_load(transaction, session, plans, &set.table, &set.key, revision)
                .await?;
            resolved.indexes.insert(name, index);
        }
        recompute_derived(plan, &mut resolved);
        Ok(resolved)
    })
}

/// Recompute every derived set of a version 2 plan in memory from its source
/// set and the tenant-shared index. Returns (set, added, removed) per change.
pub fn recompute_derived(
    plan: &VisibilityPlan,
    resolved: &mut ResolvedVisibility,
) -> Vec<(String, BTreeSet<String>, BTreeSet<String>)> {
    let mut changes: Vec<(String, BTreeSet<String>, BTreeSet<String>)> = Vec::new();
    // Chains are at most two deep: two passes reach a fixed point.
    for _ in 0..2 {
        for (name, set) in &plan.sets {
            if set.kind != "derived" {
                continue;
            }
            let Some(index) = resolved.indexes.get(name).cloned() else {
                continue;
            };
            let source = resolved.sets.get(&set.from).cloned().unwrap_or_default();
            let Ok(index) = index.read() else {
                continue;
            };
            let sets = &resolved.sets;
            let direct = &resolved.direct;
            let next = source
                .iter()
                .flat_map(|token| index.get_all(token))
                .filter(|row| {
                    set.constraints.iter().all(|constraint| {
                        let value = row.get(&constraint.column);
                        if !constraint.context.is_empty() {
                            return value.is_some_and(|value| {
                                !value.is_null()
                                    && direct.get(&constraint.context) == Some(&scalar(Some(value)))
                            });
                        }
                        if !constraint.in_set.is_empty() {
                            return value.is_some_and(|value| {
                                !value.is_null()
                                    && sets
                                        .get(&constraint.in_set)
                                        .is_some_and(|tokens| tokens.contains(&scalar(Some(value))))
                            });
                        }
                        literal_constraints_hold(std::slice::from_ref(constraint), row)
                    })
                })
                .filter_map(|row| match row.get(&set.select) {
                    None | Some(Value::Null) => None,
                    Some(value) => Some(scalar(Some(value))),
                })
                .collect::<BTreeSet<_>>();
            let previous = resolved.sets.get(name).cloned().unwrap_or_default();
            if previous != next {
                let added = next.difference(&previous).cloned().collect::<BTreeSet<_>>();
                let removed = previous.difference(&next).cloned().collect::<BTreeSet<_>>();
                if let Some((_, all_added, all_removed)) =
                    changes.iter_mut().find(|(changed, _, _)| changed == name)
                {
                    all_added.extend(added);
                    all_removed.extend(removed);
                } else {
                    changes.push((name.clone(), added, removed));
                }
                resolved.sets.insert(name.clone(), next);
            }
        }
    }
    changes
}

/// Does a change to `table` touch a row some derived set maps one of its
/// source tokens through? Only then can a derived set change in memory.
pub fn derived_change_concerns(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    table: &str,
    row_id: &str,
    old: &Value,
    new: &Value,
) -> bool {
    plan.sets.iter().any(|(name, set)| {
        set.kind == "derived"
            && set.table == table
            && resolved.indexes.contains_key(name)
            && resolved.sets.get(&set.from).is_some_and(|source| {
                [old, new].into_iter().any(|row| {
                    row.get(&set.key)
                        .map(|value| scalar(Some(value)))
                        .is_some_and(|key| source.contains(&key))
                }) || source.contains(row_id)
            })
    })
}

/// For a grant set joined to permission tables, the permission side as its own
/// principal set selecting the first join's key. None when a later join hangs
/// off the grant row itself, which cannot be decided from permission tokens.
pub fn grant_join_set(set: &VisibilitySet) -> Option<VisibilitySet> {
    let base = if set.alias.is_empty() {
        &set.table
    } else {
        &set.alias
    };
    let first = set.joins.first()?;
    if !first.left_alias.is_empty() && &first.left_alias != base {
        return None;
    }
    if set
        .joins
        .iter()
        .skip(1)
        .any(|join| &join.left_alias == base)
    {
        return None;
    }
    if !set.select_from.is_empty() && &set.select_from != base {
        return None;
    }
    let first_alias = if first.alias.is_empty() {
        first.table.clone()
    } else {
        first.alias.clone()
    };
    Some(VisibilitySet {
        kind: "principal".to_owned(),
        from: String::new(),
        key: String::new(),
        table: first.table.clone(),
        alias: first.alias.clone(),
        select: first.right_column.clone(),
        select_from: String::new(),
        joins: set
            .joins
            .iter()
            .skip(1)
            .map(|join| VisibilityJoin {
                left_alias: if join.left_alias.is_empty() {
                    String::new()
                } else {
                    join.left_alias.clone()
                },
                ..join.clone()
            })
            .collect(),
        constraints: set
            .constraints
            .iter()
            .filter(|constraint| !constraint.table.is_empty() && &constraint.table != base)
            .map(|constraint| VisibilityConstraint {
                in_set: String::new(),
                table: if constraint.table == first_alias {
                    String::new()
                } else {
                    constraint.table.clone()
                },
                ..constraint.clone()
            })
            .collect(),
    })
}

/// How one grant-table row bears on a member's grant set.
#[derive(Debug, PartialEq, Eq)]
pub enum GrantContribution {
    /// The row grants this token to the member.
    Token(String),
    /// The row grants nothing to the member.
    None,
    /// The set's shape cannot be decided from the row; re-resolve it.
    Unknown,
}

/// Decide in memory whether a grant row contributes to this member's set. Uses
/// only the row, the member's direct context and its resolved join tokens.
pub fn grant_contribution(
    name: &str,
    set: &VisibilitySet,
    resolved: &ResolvedVisibility,
    row: &Value,
) -> GrantContribution {
    let Some(row) = row.as_object() else {
        return GrantContribution::None;
    };
    let base = if set.alias.is_empty() {
        &set.table
    } else {
        &set.alias
    };
    if !set.select_from.is_empty() && &set.select_from != base {
        return GrantContribution::Unknown;
    }
    for constraint in &set.constraints {
        if !constraint.table.is_empty() && &constraint.table != base {
            continue;
        }
        let Some(value) = row.get(&constraint.column) else {
            return GrantContribution::Unknown;
        };
        let holds = if constraint.context.is_empty() {
            match constraint
                .value
                .as_ref()
                .and_then(|value| value.get("literal"))
            {
                Some(literal) => value == literal,
                None => return GrantContribution::Unknown,
            }
        } else {
            match resolved.direct.get(&constraint.context) {
                Some(expected) => scalar(Some(value)) == *expected,
                None => return GrantContribution::Unknown,
            }
        };
        if !holds {
            return GrantContribution::None;
        }
    }
    if let Some(first) = set.joins.first() {
        let Some(tokens) = resolved.sets.get(&format!("{name}{GRANT_JOIN_SUFFIX}")) else {
            return GrantContribution::Unknown;
        };
        match row.get(&first.left_column) {
            Some(value) if tokens.contains(&scalar(Some(value))) => {}
            Some(_) => return GrantContribution::None,
            None => return GrantContribution::Unknown,
        }
    }
    match row.get(&set.select) {
        Some(Value::Null) => GrantContribution::None,
        Some(value) => GrantContribution::Token(scalar(Some(value))),
        None => GrantContribution::Unknown,
    }
}

pub fn compile_predicate(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    row_alias: &str,
    parameters: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    compile_expression(
        &plan.predicate,
        plan,
        resolved,
        row_alias,
        parameters,
        &mut None,
    )
}

pub fn row_matches(plan: &VisibilityPlan, resolved: &ResolvedVisibility, row: &Value) -> bool {
    let Some(row) = row.as_object() else {
        return false;
    };
    match_expression(&plan.predicate, resolved, row)
}

fn validate_set(set: &VisibilitySet) -> Result<(), VisibilityError> {
    identifier(&set.table)?;
    identifier(&set.select)?;
    let base_alias = if set.alias.is_empty() {
        &set.table
    } else {
        identifier(&set.alias)?;
        &set.alias
    };
    let mut aliases = BTreeSet::from([base_alias.clone()]);
    let mut physical = BTreeMap::from([(set.table.clone(), 1usize)]);
    for join in &set.joins {
        identifier(&join.table)?;
        identifier(&join.left_column)?;
        identifier(&join.right_column)?;
        let alias = if join.alias.is_empty() {
            &join.table
        } else {
            identifier(&join.alias)?;
            &join.alias
        };
        if !aliases.insert(alias.clone()) {
            return Err(VisibilityError::Invalid(format!(
                "visibility alias {alias:?} is repeated"
            )));
        }
        *physical.entry(join.table.clone()).or_default() += 1;
    }
    if physical.iter().any(|(table, count)| {
        *count > 1
            && ((table == &set.table && set.alias.is_empty())
                || set
                    .joins
                    .iter()
                    .any(|join| &join.table == table && join.alias.is_empty()))
    }) {
        return Err(VisibilityError::Invalid(
            "every occurrence of a repeated visibility table requires an alias".to_owned(),
        ));
    }
    for constraint in &set.constraints {
        identifier(&constraint.column)?;
        if !constraint.table.is_empty() {
            identifier(&constraint.table)?;
            if !aliases.contains(&constraint.table) {
                return Err(VisibilityError::Invalid(format!(
                    "unknown constraint alias {:?}",
                    constraint.table
                )));
            }
        }
        let has_context = !constraint.context.is_empty();
        let has_value = constraint.value.is_some();
        let has_set = !constraint.in_set.is_empty();
        if [has_context, has_value, has_set]
            .into_iter()
            .filter(|present| *present)
            .count()
            != 1
        {
            return Err(VisibilityError::Invalid(
                "visibility constraint requires exactly one of context, value or inSet".to_owned(),
            ));
        }
        if has_set && set.kind != "derived" {
            return Err(VisibilityError::Invalid(
                "inSet constraints are allowed only on derived sets".to_owned(),
            ));
        }
        if has_context {
            context_key(&constraint.context)?;
        } else if has_value {
            visibility_literal(constraint.value.as_ref())?;
        }
    }
    Ok(())
}

fn validate_expression(
    expression: &VisibilityExpression,
    sets: &BTreeMap<String, VisibilitySet>,
) -> Result<(), VisibilityError> {
    match expression.operator.as_str() {
        "public" => {}
        "permission" | "role"
            if expression
                .value
                .as_ref()
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty()) =>
        {
            return Err(VisibilityError::Invalid(format!(
                "{} requires a value",
                expression.operator
            )))
        }
        "permission" | "role" => {}
        "eq" => {
            identifier(&expression.column)?;
            visibility_literal(expression.value.as_ref())?;
        }
        "eqContext" => {
            identifier(&expression.column)?;
            context_key(&expression.context)?;
        }
        "inherits" | "inheritsAny" => {
            identifier(&expression.column)?;
            if expression.operator == "inheritsAny" {
                let via = expression.via.as_ref().ok_or_else(|| {
                    VisibilityError::Invalid("inheritsAny requires via".to_owned())
                })?;
                identifier(&via.table)?;
                identifier(&via.child_column)?;
                identifier(&via.parent_column)?;
            }
            identifier(&expression.table)?;
            identifier(&expression.key)?;
            for constraint in &expression.parent_where {
                identifier(&constraint.column)?;
                if !constraint.context.is_empty() || constraint.value.is_none() {
                    return Err(VisibilityError::Invalid(
                        "inherited parent filters are literals only".to_owned(),
                    ));
                }
                visibility_literal(constraint.value.as_ref())?;
            }
        }
        "inSet" | "nonEmpty" => {
            if expression.operator == "inSet" {
                identifier(&expression.column)?;
            }
            if !sets.contains_key(&expression.set) {
                return Err(VisibilityError::Invalid(format!(
                    "unknown visibility set {:?}",
                    expression.set
                )));
            }
        }
        "and" | "or" if expression.children.is_empty() => {
            return Err(VisibilityError::Invalid(format!(
                "{} requires children",
                expression.operator
            )))
        }
        "and" | "or" => {}
        "not" if expression.children.len() == 1 => {}
        "not" => {
            return Err(VisibilityError::Invalid(
                "not requires one child".to_owned(),
            ))
        }
        operator => {
            return Err(VisibilityError::Invalid(format!(
                "unsupported operator {operator:?}"
            )))
        }
    }
    for child in &expression.children {
        validate_expression(child, sets)?;
    }
    if let Some(fallback) = &expression.on_missing {
        if expression.operator != "inherits" {
            return Err(VisibilityError::Invalid(
                "onMissing is allowed only on inherits".into(),
            ));
        }
        validate_expression(fallback, sets)?;
    }
    Ok(())
}

fn compile_expression(
    expression: &VisibilityExpression,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    row_alias: &str,
    parameters: &mut Vec<Value>,
    account_parameter: &mut Option<usize>,
) -> Result<String, VisibilityError> {
    let account_argument = |parameters: &mut Vec<Value>, slot: &mut Option<usize>| {
        let index = *slot.get_or_insert_with(|| {
            parameters.push(Value::String(resolved.direct["account.id"].clone()));
            parameters.len()
        });
        format!("${index}")
    };
    let argument = |parameters: &mut Vec<Value>, value: Value| {
        parameters.push(value);
        format!("${}", parameters.len())
    };
    Ok(match expression.operator.as_str() {
        "public" => "TRUE".to_owned(),
        "inherits" => {
            static PARENT_ALIAS: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let parent = resolved.inherited.get(&expression.table).ok_or_else(|| {
                VisibilityError::Invalid(format!(
                    "inherited parent {:?} was not resolved",
                    expression.table
                ))
            })?;
            let next = || {
                format!(
                    "_gonvex_parent_{}",
                    PARENT_ALIAS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                )
            };
            let alias = next();
            let column = quote(&expression.column)?;
            let key = quote(&expression.key)?;
            let table = quote(&expression.table)?;
            let parent_predicate = compile_expression(
                &parent.plan.predicate,
                &parent.plan,
                &parent.resolved,
                &alias,
                parameters,
                account_parameter,
            )?;
            let mut parent_filters = String::new();
            for constraint in &expression.parent_where {
                parent_filters.push_str(" AND ");
                parent_filters.push_str(&compile_literal_equality(
                    &alias,
                    &constraint.column,
                    visibility_literal(constraint.value.as_ref())?,
                    parameters,
                )?);
            }
            let visible = format!(
                "EXISTS (SELECT 1 FROM {table} AS {alias} WHERE {alias}.{key}::text = {row_alias}.{column}::text{parent_filters} AND ({parent_predicate}))"
            );
            match &expression.on_missing {
                None => visible,
                Some(fallback) => {
                    let missing = next();
                    let fallback = compile_expression(
                        fallback,
                        plan,
                        resolved,
                        row_alias,
                        parameters,
                        account_parameter,
                    )?;
                    format!(
                        "({visible} OR ({row_alias}.{column} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {table} AS {missing} WHERE {missing}.{key}::text = {row_alias}.{column}::text) AND ({fallback})))"
                    )
                }
            }
        }
        "inheritsAny" => {
            static VIA_ALIAS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let via = expression
                .via
                .as_ref()
                .ok_or_else(|| VisibilityError::Invalid("inheritsAny requires via".to_owned()))?;
            let parent = resolved.inherited.get(&expression.table).ok_or_else(|| {
                VisibilityError::Invalid(format!(
                    "inherited parent {:?} was not resolved",
                    expression.table
                ))
            })?;
            let suffix = VIA_ALIAS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let link = format!("_gonvex_via_{suffix}");
            let alias = format!("_gonvex_any_parent_{suffix}");
            let parent_predicate = compile_expression(
                &parent.plan.predicate,
                &parent.plan,
                &parent.resolved,
                &alias,
                parameters,
                account_parameter,
            )?;
            let mut parent_filters = String::new();
            for constraint in &expression.parent_where {
                parent_filters.push_str(" AND ");
                parent_filters.push_str(&compile_literal_equality(
                    &alias,
                    &constraint.column,
                    visibility_literal(constraint.value.as_ref())?,
                    parameters,
                )?);
            }
            format!(
                "EXISTS (SELECT 1 FROM {} AS {link} JOIN {} AS {alias} ON {alias}.{}::text = {link}.{}::text WHERE {link}.{}::text = {row_alias}.{}::text{parent_filters} AND ({parent_predicate}))",
                quote(&via.table)?,
                quote(&expression.table)?,
                quote(&expression.key)?,
                quote(&via.parent_column)?,
                quote(&via.child_column)?,
                quote(&expression.column)?,
            )
        }
        "nonEmpty" if plan.version == Some(2) => {
            let values = resolved.sets.get(&expression.set).ok_or_else(|| {
                VisibilityError::Invalid(format!(
                    "mask principal set {:?} was not resolved",
                    expression.set
                ))
            })?;
            if values.is_empty() {
                "FALSE".to_owned()
            } else {
                "TRUE".to_owned()
            }
        }
        "nonEmpty" => {
            let set = plan.sets.get(&expression.set).ok_or_else(|| {
                VisibilityError::Invalid(format!("unknown set {:?}", expression.set))
            })?;
            let subquery = compile_set_with_parameters(set, &resolved.direct, parameters)?;
            format!("EXISTS ({subquery})")
        }
        // SQL snapshots and in-memory deltas must use the same admitted
        // principal. Ordinary members keep their live database role checks.
        "permission" | "role" if resolved.ephemeral_member => {
            if match_expression(expression, resolved, &serde_json::Map::new()) {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        "permission" => {
            let account = account_argument(parameters, account_parameter);
            let permission = argument(
                parameters,
                Value::String(
                    expression
                        .value
                        .as_ref()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                ),
            );
            format!(
                "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = {account} AND _gonvex_member.status = 'active' AND lower(COALESCE(_gonvex_member.permissions ->> {permission}::text, '')) IN ('true', '1'))"
            )
        }
        "role" => {
            let account = account_argument(parameters, account_parameter);
            let role = argument(
                parameters,
                Value::String(
                    expression
                        .value
                        .as_ref()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                ),
            );
            format!(
                "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = {account} AND _gonvex_member.status = 'active' AND _gonvex_member.role = {role})"
            )
        }
        "eqContext" => {
            let column = quote(&expression.column)?;
            let value = argument(
                parameters,
                Value::String(resolved.direct[&expression.context].clone()),
            );
            format!("{row_alias}.{column} = {value}")
        }
        "eq" => compile_literal_equality(
            row_alias,
            &expression.column,
            visibility_literal(expression.value.as_ref())?,
            parameters,
        )?,
        "inSet" if plan.version == Some(2) => {
            let column = quote(&expression.column)?;
            let values = resolved.sets.get(&expression.set).ok_or_else(|| {
                VisibilityError::Invalid(format!(
                    "mask principal set {:?} was not resolved",
                    expression.set
                ))
            })?;
            let tokens = Value::Array(values.iter().cloned().map(Value::String).collect());
            let parameter = argument(parameters, tokens);
            format!("{row_alias}.{column}::text = ANY(ARRAY(SELECT jsonb_array_elements_text({parameter}::jsonb)))")
        }
        "inSet" => {
            let column = quote(&expression.column)?;
            let set = plan.sets.get(&expression.set).ok_or_else(|| {
                VisibilityError::Invalid(format!("unknown set {:?}", expression.set))
            })?;
            let subquery = compile_set_with_parameters(set, &resolved.direct, parameters)?;
            format!("{row_alias}.{column} IN ({subquery})")
        }
        "and" | "or" => {
            let mut children = Vec::new();
            for child in &expression.children {
                children.push(format!(
                    "({})",
                    compile_expression(
                        child,
                        plan,
                        resolved,
                        row_alias,
                        parameters,
                        account_parameter
                    )?
                ));
            }
            let joined = children.join(if expression.operator == "and" {
                " AND "
            } else {
                " OR "
            });
            format!("({joined})")
        }
        // Boolean double negation is the original admission decision. Avoid
        // nested SQL truth tests, which can hide an indexable equality.
        "not" if expression.children[0].operator == "not" => compile_expression(
            &expression.children[0].children[0],
            plan,
            resolved,
            row_alias,
            parameters,
            account_parameter,
        )?,
        "not" if expression.children[0].operator == "eq" => compile_literal_comparison(
            row_alias,
            &expression.children[0].column,
            visibility_literal(expression.children[0].value.as_ref())?,
            parameters,
            true,
        )?,
        // Visibility uses Boolean admission: a nullable equality that does not
        // admit a row is false to the Rust evaluator. SQL NOT would preserve
        // UNKNOWN and disagree under negation. IS NOT TRUE complements the
        // child's admission result, including UNKNOWN, without evaluating it twice.
        "not" => format!(
            "({}) IS NOT TRUE",
            compile_expression(
                &expression.children[0],
                plan,
                resolved,
                row_alias,
                parameters,
                account_parameter
            )?
        ),
        _ => "FALSE".to_owned(),
    })
}

fn compile_set(
    set: &VisibilitySet,
    direct: &BTreeMap<String, String>,
) -> Result<(String, Vec<Value>), VisibilityError> {
    let mut parameters = Vec::new();
    let query = compile_set_with_parameters(set, direct, &mut parameters)?;
    Ok((query, parameters))
}

fn compile_set_with_parameters(
    set: &VisibilitySet,
    direct: &BTreeMap<String, String>,
    parameters: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    let base_alias = if set.alias.is_empty() {
        set.table.as_str()
    } else {
        set.alias.as_str()
    };
    let mut aliases = BTreeMap::from([(base_alias.to_owned(), "v0".to_owned())]);
    let mut from = format!("{} AS v0", quote(&set.table)?);
    let mut previous = base_alias.to_owned();
    for (index, join) in set.joins.iter().enumerate() {
        let logical = if join.alias.is_empty() {
            &join.table
        } else {
            &join.alias
        };
        let left_logical = if join.left_alias.is_empty() {
            &previous
        } else {
            &join.left_alias
        };
        let left_alias = aliases.get(left_logical).ok_or_else(|| {
            VisibilityError::Invalid(format!("unknown left alias {left_logical:?}"))
        })?;
        let sql_alias = format!("v{}", index + 1);
        from.push_str(&format!(
            " JOIN {} AS {sql_alias} ON {left_alias}.{} = {sql_alias}.{}",
            quote(&join.table)?,
            quote(&join.left_column)?,
            quote(&join.right_column)?
        ));
        aliases.insert(logical.clone(), sql_alias);
        previous = logical.clone();
    }
    let select_from = if set.select_from.is_empty() {
        base_alias
    } else {
        &set.select_from
    };
    let selected_alias = aliases
        .get(select_from)
        .ok_or_else(|| VisibilityError::Invalid(format!("unknown select alias {select_from:?}")))?;
    let mut query = format!(
        "SELECT DISTINCT {selected_alias}.{}::text FROM {from}",
        quote(&set.select)?
    );
    let mut predicates = Vec::new();
    for constraint in &set.constraints {
        let alias = if constraint.table.is_empty() {
            "v0"
        } else {
            aliases
                .get(&constraint.table)
                .ok_or_else(|| {
                    VisibilityError::Invalid(format!(
                        "unknown constraint alias {:?}",
                        constraint.table
                    ))
                })?
                .as_str()
        };
        let has_context = !constraint.context.is_empty();
        let has_value = constraint.value.is_some();
        if has_context == has_value {
            return Err(VisibilityError::Invalid(
                "visibility constraint requires exactly one of context or value".to_owned(),
            ));
        }
        if has_context {
            context_key(&constraint.context)?;
            parameters.push(Value::String(direct[&constraint.context].clone()));
            predicates.push(format!(
                "{alias}.{} = ${}",
                quote(&constraint.column)?,
                parameters.len()
            ));
        } else {
            predicates.push(compile_literal_equality(
                alias,
                &constraint.column,
                visibility_literal(constraint.value.as_ref())?,
                parameters,
            )?);
        }
    }
    if !predicates.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&predicates.join(" AND "));
    }
    Ok(query)
}

fn visibility_literal(value: Option<&Value>) -> Result<&Value, VisibilityError> {
    let object = value.and_then(Value::as_object).ok_or_else(|| {
        VisibilityError::Invalid("visibility literal must be an object".to_owned())
    })?;
    if object.len() != 1 || !object.contains_key("literal") {
        return Err(VisibilityError::Invalid(
            "visibility literal must contain only literal".to_owned(),
        ));
    }
    Ok(&object["literal"])
}

fn compile_literal_equality(
    row_alias: &str,
    column: &str,
    literal: &Value,
    parameters: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    compile_literal_comparison(row_alias, column, literal, parameters, false)
}

fn compile_literal_comparison(
    row_alias: &str,
    column: &str,
    literal: &Value,
    parameters: &mut Vec<Value>,
    negated: bool,
) -> Result<String, VisibilityError> {
    let column = quote(column)?;
    if literal.is_null() {
        let predicate = if negated { "IS NOT NULL" } else { "IS NULL" };
        return Ok(format!("{row_alias}.{column} {predicate}"));
    }
    parameters.push(literal.clone());
    let operator = if negated { "IS DISTINCT FROM" } else { "=" };
    Ok(format!(
        "{row_alias}.{column} {operator} ${}",
        parameters.len()
    ))
}

fn visibility_fingerprint(
    plan: &VisibilityPlan,
    direct: &BTreeMap<String, String>,
    role: &str,
    permissions: &Value,
    sets: &BTreeMap<String, BTreeSet<String>>,
) -> String {
    let payload = serde_json::json!({
        "plan": plan,
        "direct": direct,
        "role": role,
        "permissions": permissions,
        "sets": sets,
    });
    Sha256::digest(serde_json::to_vec(&payload).unwrap_or_default())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn context_key(value: &str) -> Result<(), VisibilityError> {
    if matches!(value, "account.id" | "member.id" | "tenant.id") {
        Ok(())
    } else {
        Err(VisibilityError::Invalid(format!(
            "unsupported context {value:?}"
        )))
    }
}

fn identifier(value: &str) -> Result<&str, VisibilityError> {
    if !value.is_empty()
        && value.len() <= 63
        && value.chars().enumerate().all(|(index, character)| {
            character.is_ascii_alphabetic()
                || character == '_'
                || (index > 0 && character.is_ascii_digit())
        })
    {
        Ok(value)
    } else {
        Err(VisibilityError::Invalid(format!(
            "invalid SQL identifier {value:?}"
        )))
    }
}

fn quote(value: &str) -> Result<String, VisibilityError> {
    identifier(value).map(|value| format!("\"{value}\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_empty_flags_retain_dependencies_and_follow_set_contents() {
        let plan = dependency_plan(serde_json::json!({"operator":"nonEmpty","set":"assigned"}));
        plan.validate().unwrap();
        let mut member = dependency_member("member");
        assert!(plan.resolved_dependency_columns(&member).change_affects(
            "taskUsers",
            "insert",
            &[]
        ));
        assert!(!row_matches(&plan, &member, &serde_json::json!({})));
        member
            .sets
            .insert("assigned".to_owned(), BTreeSet::from(["task-1".to_owned()]));
        assert!(row_matches(&plan, &member, &serde_json::json!({})));
        let mut parameters = Vec::new();
        assert!(compile_predicate(&plan, &member, "r", &mut parameters)
            .unwrap()
            .starts_with("EXISTS ("));
        let unknown = dependency_plan(serde_json::json!({"operator":"nonEmpty","set":"missing"}));
        assert!(unknown.validate().is_err());
    }

    #[tokio::test]
    async fn nullable_negation_matches_postgres_admission() {
        use sqlx::postgres::PgPoolOptions;

        let Ok(database_url) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping nullable visibility test");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        sqlx::raw_sql(
            "CREATE TEMP TABLE visibility_negation_values ON COMMIT DROP AS \
             SELECT row_number() OVER ()::text AS id, s.status, e.enabled \
             FROM (VALUES (NULL::text), ('blocked'), ('allowed')) AS s(status) \
             CROSS JOIN (VALUES (NULL::boolean), (false), (true)) AS e(enabled)",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        let rows: Vec<Json<Value>> = sqlx::query_scalar(
            "SELECT to_jsonb(r) FROM visibility_negation_values AS r ORDER BY id",
        )
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        let status =
            serde_json::json!({"operator":"eq", "column":"status", "value":{"literal":"blocked"}});
        let enabled =
            serde_json::json!({"operator":"eq", "column":"enabled", "value":{"literal":true}});
        let null =
            serde_json::json!({"operator":"eq", "column":"status", "value":{"literal":null}});
        let negate = |child: Value| serde_json::json!({"operator":"not", "children":[child]});
        let mut expressions = vec![
            status.clone(),
            enabled.clone(),
            null.clone(),
            negate(status.clone()),
            negate(enabled.clone()),
            negate(null),
            negate(negate(status.clone())),
        ];
        for operator in ["and", "or"] {
            let combined = serde_json::json!({"operator":operator, "children":[status, enabled]});
            expressions.push(combined.clone());
            expressions.push(negate(combined));
            expressions.push(serde_json::json!({"operator":operator, "children":[negate(status.clone()), enabled]}));
        }
        let resolved = ResolvedVisibility {
            inherited: Default::default(),
            indexes: Default::default(),
            ephemeral_member: false,
            revision: 1,
            direct: BTreeMap::new(),
            role: String::new(),
            permissions: Value::Null,
            sets: BTreeMap::new(),
            fingerprint: String::new(),
        };
        for expression in expressions {
            let plan: VisibilityPlan = serde_json::from_value(serde_json::json!({
                "table":"visibility_negation_values", "key":"id", "where":expression
            }))
            .unwrap();
            plan.validate().unwrap();
            let expected: Vec<String> = rows
                .iter()
                .filter(|row| row_matches(&plan, &resolved, &row.0))
                .map(|row| row.0["id"].as_str().unwrap().to_owned())
                .collect();
            let mut values = Vec::new();
            let predicate = compile_predicate(&plan, &resolved, "r", &mut values).unwrap();
            let statement = format!(
                "SELECT r.id FROM visibility_negation_values AS r WHERE {predicate} ORDER BY r.id"
            );
            let parameters = Parameters::scalars(&values).unwrap();
            let key = parameters.statement(&statement);
            let actual: Vec<String> = parameters
                .bind(sqlx::query(&key))
                .fetch_all(&mut *tx)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.get("id"))
                .collect();
            assert_eq!(
                actual, expected,
                "predicate={predicate}; expression={expression}"
            );
        }
        tx.rollback().await.unwrap();
        pool.close().await;
    }

    #[tokio::test]
    async fn developer_visibility_without_membership_preserves_admission() {
        use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
        use sqlx::postgres::PgPoolOptions;
        use std::time::{SystemTime, UNIX_EPOCH};

        let Some(database_url) = std::env::var("GONVEX_TEST_POSTGRES_URL").ok() else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping visibility contract test");
            return;
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let schema = format!("gonvex_developer_visibility_{nonce}");
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let pools = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(&scoped_url, pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let fixture = PgPoolOptions::new()
            .max_connections(1)
            .connect(&scoped_url)
            .await
            .unwrap();
        sqlx::raw_sql(
            r#"
            CREATE TABLE members(id text PRIMARY KEY, account_id text, status text,
                                 role text, permissions jsonb);
            INSERT INTO members VALUES('revoked-member','developer-account','revoked','owner','{}');
        "#,
        )
        .execute(&fixture)
        .await
        .unwrap();
        let before: Value =
            sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM members m")
                .fetch_one(&fixture)
                .await
                .unwrap();

        // Host-owned result of verified grant admission; token/authority checks
        // are covered by postgres tenant_isolation and runtime auth_contract.
        let route = TenantRoute {
            project_id: "project".to_owned(),
            tenant_id: "tenant".to_owned(),
            database_url: scoped_url,
        };
        let mut session = TenantSession::anonymous(route.clone());
        session.identity.account.id = "developer-account".to_owned();
        session.member.id = "developer:developer-account".to_owned();
        session.member.account_id = "developer-account".to_owned();
        session.member.status = "active".to_owned();
        session.member.role = "admin".to_owned();
        session.member.permissions = serde_json::json!({"developer":true});
        session.developer_session = Some(serde_json::json!({"grantId":"devmode_verified"}));
        let plan: VisibilityPlan = serde_json::from_value(serde_json::json!({
            "table":"members", "key":"id", "where":{"operator":"role","value":"admin"}
        }))
        .unwrap();

        for materialize in [true, false] {
            let mut tx = control
                .begin_tenant_transaction(&route, true)
                .await
                .unwrap();
            let resolved = if materialize {
                resolve(&mut tx, &session, &plan, 7).await
            } else {
                resolve_for_query(&mut tx, &session, &plan, 7).await
            }
            .expect("verified nonmember developer must resolve replica and query visibility");
            assert_eq!(resolved.direct["member.id"], "developer:developer-account");
            assert_eq!(resolved.role, "admin");
            assert!(row_matches(
                &plan,
                &resolved,
                &serde_json::json!({"id":"revoked-member"})
            ));
            // Execute compiled SQL: accepting a plan alone missed the empty
            // directory regression when role checks required a persisted row.
            for predicate in [
                serde_json::json!({"operator":"role","value":"admin"}),
                serde_json::json!({"operator":"role","value":"owner"}),
                serde_json::json!({"operator":"permission","value":"developer"}),
                serde_json::json!({"operator":"permission","value":"view-users"}),
                serde_json::json!({"operator":"eqContext","column":"id","context":"member.id"}),
                serde_json::json!({"operator":"and","children":[
                    {"operator":"role","value":"admin"},
                    {"operator":"not","children":[{"operator":"role","value":"owner"}]},
                    {"operator":"permission","value":"developer"}
                ]}),
            ] {
                let check: VisibilityPlan = serde_json::from_value(serde_json::json!({
                    "table":"members", "key":"id", "where":predicate
                }))
                .unwrap();
                let related = resolve_related_query(&check, &resolved).unwrap();
                let expected = row_matches(
                    &check,
                    &related,
                    &serde_json::json!({"id":"revoked-member"}),
                );
                assert_sql_visibility(&mut tx, &check, &related, expected).await;
            }
            tx.rollback().await.unwrap();
        }

        // A synthetic-looking member ID alone never authorizes ordinary or
        // support-impersonation sessions. Nor does metadata replace admission.
        for invalid in [0, 1, 2] {
            let mut denied = session.clone();
            match invalid {
                0 => denied.developer_session = None,
                1 => denied.member.id = "revoked-member".to_owned(),
                _ => denied.member.account_id = "other-account".to_owned(),
            }
            let mut tx = control
                .begin_tenant_transaction(&route, true)
                .await
                .unwrap();
            assert!(matches!(
                resolve(&mut tx, &denied, &plan, 7).await,
                Err(VisibilityError::MemberRequired)
            ));
            tx.rollback().await.unwrap();
        }
        let after: Value =
            sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM members m")
                .fetch_one(&fixture)
                .await
                .unwrap();
        assert_eq!(
            before, after,
            "visibility must not create or reactivate members"
        );

        sqlx::query("UPDATE members SET status='active',role='member',permissions='{}'")
            .execute(&fixture)
            .await
            .unwrap();
        let mut tx = control
            .begin_tenant_transaction(&route, true)
            .await
            .unwrap();
        let resolved = resolve(&mut tx, &session, &plan, 8).await.unwrap();
        assert_eq!(resolved.direct["member.id"], "revoked-member");
        assert_eq!(resolved.role, "member");
        assert!(!resolved.ephemeral_member);
        assert_sql_visibility(&mut tx, &plan, &resolved, false).await;
        let permission_plan: VisibilityPlan = serde_json::from_value(serde_json::json!({
            "table":"members", "key":"id", "where":{"operator":"permission","value":"developer"}
        }))
        .unwrap();
        assert_sql_visibility(&mut tx, &permission_plan, &resolved, false).await;
        assert!(!row_matches(
            &plan,
            &resolved,
            &serde_json::json!({"id":"revoked-member"})
        ));
        tx.rollback().await.unwrap();

        // Ordinary member reads still require a currently active row, even
        // when the declared table visibility itself is public.
        let public: VisibilityPlan = serde_json::from_value(serde_json::json!({
            "table":"members", "key":"id", "where":{"operator":"public"}
        }))
        .unwrap();
        let mut tx = control
            .begin_tenant_transaction(&route, false)
            .await
            .unwrap();
        let ordinary = resolve(&mut tx, &session, &public, 8).await.unwrap();
        assert!(!ordinary.ephemeral_member);
        assert_read_visibility(&mut tx, &public, &ordinary, true).await;
        sqlx::query("UPDATE members SET status='revoked'")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert_read_visibility(&mut tx, &public, &ordinary, false).await;
        tx.rollback().await.unwrap();
        fixture.close().await;
        pools.close().await;
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }

    async fn assert_sql_visibility(
        tx: &mut TenantTransaction,
        plan: &VisibilityPlan,
        resolved: &ResolvedVisibility,
        expected: bool,
    ) {
        let mut parameters = Vec::new();
        let predicate = compile_predicate(plan, resolved, "r", &mut parameters).unwrap();
        let statement = format!("SELECT r.id FROM members AS r WHERE {predicate} ORDER BY r.id");
        let parameters = Parameters::scalars(&parameters).unwrap();
        let statement_key = parameters.statement(&statement);
        let query = parameters.bind(sqlx::query(statement_key.as_ref()));
        let rows = query.fetch_all(&mut **tx.transaction()).await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<String, _>("id"))
                .collect::<Vec<_>>(),
            if expected {
                vec!["revoked-member".to_owned()]
            } else {
                vec![]
            },
            "compiled SQL visibility must match the admitted principal and row predicate"
        );

        assert_read_visibility(tx, plan, resolved, expected).await;
    }

    async fn assert_read_visibility(
        tx: &mut TenantTransaction,
        plan: &VisibilityPlan,
        resolved: &ResolvedVisibility,
        expected: bool,
    ) {
        let expected_rows = if expected {
            vec![serde_json::json!({"id":"revoked-member"})]
        } else {
            vec![]
        };
        let replica = serde_json::from_value(serde_json::json!({
            "table":"members", "key":"id", "columns":["id"],
            "orderBy":"id", "orderDirection":"asc", "maxRows":5000
        }))
        .unwrap();
        let (snapshot, truncated) =
            crate::replica::query_rows(tx, &replica, plan, resolved, &serde_json::json!({}))
                .await
                .unwrap();
        assert!(!truncated);
        assert_eq!(
            snapshot, expected_rows,
            "replica snapshot must preserve verified support admission"
        );

        let query = serde_json::from_value(serde_json::json!({
            "table":"members", "key":"id", "columns":["id"]
        }))
        .unwrap();
        let result = crate::live_query::execute_in_transaction(
            tx,
            &query,
            plan,
            resolved,
            &serde_json::Map::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            Value::Array(expected_rows),
            "live query must preserve verified support admission"
        );
    }

    fn dependency_plan(predicate: Value) -> VisibilityPlan {
        serde_json::from_value(serde_json::json!({
            "table": "tasks", "key": "id",
            "sets": {
                "assigned": {"table": "taskUsers", "select": "taskId", "where": [{"column": "userId", "context": "member.id"}]},
                "shared": {"table": "taskShares", "select": "taskId", "where": [{"column": "memberId", "context": "member.id"}]}
            },
            "where": predicate
        })).unwrap()
    }

    fn dependency_member(role: &str) -> ResolvedVisibility {
        ResolvedVisibility {
            inherited: Default::default(),
            indexes: Default::default(),
            ephemeral_member: false,
            revision: 1,
            direct: BTreeMap::from([("member.id".to_owned(), "member-1".to_owned())]),
            role: role.to_owned(),
            permissions: Value::Null,
            sets: BTreeMap::new(),
            fingerprint: String::new(),
        }
    }

    #[test]
    fn anchored_dependency_routes_inserts_deletes_and_reassignments() {
        let plan =
            dependency_plan(serde_json::json!({"operator":"inSet","column":"id","set":"assigned"}));
        let member = dependency_member("member");
        let own = serde_json::json!({"userId":"member-1"});
        let other = serde_json::json!({"userId":"member-2"});
        for (old, new) in [
            (Value::Null, other.clone()),
            (other.clone(), Value::Null),
            (other.clone(), other.clone()),
        ] {
            assert!(!plan.change_concerns_member(&member, "taskUsers", &old, &new));
        }
        for (old, new) in [
            (Value::Null, own.clone()),
            (own.clone(), Value::Null),
            (own.clone(), other.clone()),
            (other.clone(), own.clone()),
        ] {
            assert!(plan.change_concerns_member(&member, "taskUsers", &old, &new));
        }
        assert!(plan.change_concerns_member(
            &member,
            "taskUsers",
            &Value::Null,
            &serde_json::json!({})
        ));
        assert!(plan.change_concerns_member(&member, "taskUsers", &Value::Null, &Value::Null));
        assert!(!plan.change_concerns_member(
            &member,
            "members",
            &Value::Null,
            &serde_json::json!({"id":"member-2"})
        ));
        assert!(plan.change_concerns_member(
            &member,
            "members",
            &serde_json::json!({"id":"member-1"}),
            &Value::Null
        ));
    }

    #[test]
    fn unanchored_join_dependencies_are_never_skipped() {
        let mut plan =
            dependency_plan(serde_json::json!({"operator":"inSet","column":"id","set":"assigned"}));
        plan.sets.get_mut("assigned").unwrap().constraints.clear();
        assert!(plan.change_concerns_member(
            &dependency_member("member"),
            "taskUsers",
            &Value::Null,
            &serde_json::json!({"userId":"member-2"})
        ));
    }

    #[test]
    fn admitted_role_skips_irrelevant_sets_but_never_member_revocation() {
        let plan = dependency_plan(serde_json::json!({"operator":"or","children":[
            {"operator":"role","value":"admin"},
            {"operator":"inSet","column":"id","set":"assigned"}
        ]}));
        plan.validate().unwrap();
        let mut admin = dependency_member("admin");
        let dependencies = plan.resolved_dependency_columns(&admin);
        assert!(!dependencies.change_affects("taskUsers", "insert", &[]));
        for column in ["status", "role", "permissions", "account_id"] {
            assert!(dependencies.change_affects("members", "update", &[column.to_owned()]));
        }
        let task = serde_json::json!({"id":"task-1"});
        assert!(row_matches(&plan, &admin, &task));
        admin
            .sets
            .insert("assigned".to_owned(), BTreeSet::from(["task-1".to_owned()]));
        assert!(row_matches(&plan, &admin, &task));
        // After the admitted role changes, the previously inactive dependency
        // is necessary again and revoking the assignment hides the row.
        admin.role = "member".to_owned();
        assert!(plan.resolved_dependency_columns(&admin).change_affects(
            "taskUsers",
            "delete",
            &[]
        ));
        admin.sets.clear();
        assert!(!row_matches(&plan, &admin, &task));
        assert!(!plan.resolved_dependency_columns(&admin).change_affects(
            "taskShares",
            "insert",
            &[]
        ));
    }

    #[test]
    fn nested_role_branches_prune_only_irrelevant_dependencies() {
        let plan = dependency_plan(serde_json::json!({"operator":"or","children":[
            {"operator":"and","children":[{"operator":"role","value":"reviewer"},{"operator":"inSet","column":"id","set":"assigned"}]},
            {"operator":"and","children":[{"operator":"not","children":[{"operator":"role","value":"admin"}]},{"operator":"inSet","column":"id","set":"shared"}]}
        ]}));
        plan.validate().unwrap();
        let admin = plan.resolved_dependency_columns(&dependency_member("admin"));
        assert_eq!(admin.tables(), BTreeSet::from(["members".to_owned()]));
        let member = plan.resolved_dependency_columns(&dependency_member("member"));
        assert!(!member.change_affects("taskUsers", "insert", &[]));
        assert!(member.change_affects("taskShares", "insert", &[]));
        let reviewer = plan.resolved_dependency_columns(&dependency_member("reviewer"));
        assert!(reviewer.change_affects("taskUsers", "insert", &[]));
        assert!(reviewer.change_affects("taskShares", "delete", &[]));
    }

    #[test]
    fn permission_and_row_predicates_keep_their_set_dependencies() {
        for predicate in [
            serde_json::json!({"operator":"permission","value":"view-all"}),
            serde_json::json!({"operator":"eq","column":"public","value":{"literal":true}}),
            serde_json::json!({"operator":"eqContext","column":"owner","context":"member.id"}),
        ] {
            let plan = dependency_plan(serde_json::json!({"operator":"or","children":[predicate,
                {"operator":"inSet","column":"id","set":"assigned"}
            ]}));
            plan.validate().unwrap();
            for role in ["admin", "member"] {
                assert!(plan
                    .resolved_dependency_columns(&dependency_member(role))
                    .change_affects("taskUsers", "insert", &[]));
            }
        }
    }

    #[test]
    fn bounded_sql_visibility_keeps_membership_in_sql_without_materialized_ids() {
        let plan =
            dependency_plan(serde_json::json!({"operator":"inSet","column":"id","set":"assigned"}));
        let member = dependency_member("member");
        let related = resolve_related_query(&plan, &member).unwrap();
        assert!(related.sets.is_empty());
        assert_eq!(related.revision, member.revision);
        let mut parameters = Vec::new();
        let sql = compile_predicate(&plan, &related, "r", &mut parameters).unwrap();
        assert!(sql.contains("r.\"id\" IN (SELECT DISTINCT"));
        assert!(sql.contains("\"taskUsers\" AS v0"));
        assert_eq!(parameters, vec![Value::String("member-1".to_owned())]);
        // Set changes still invalidate the exact server window. An empty Rust
        // set must never imply public access or suppress a dependency.
        assert!(plan.resolved_dependency_columns(&related).change_affects(
            "taskUsers",
            "delete",
            &[]
        ));
        assert_ne!(related.fingerprint, member.fingerprint);
    }

    #[test]
    fn self_join_requires_aliases_and_compiles_both_occurrences() {
        let set = VisibilitySet {
            kind: String::new(),
            from: String::new(),
            key: String::new(),
            table: "memberTeams".to_owned(),
            alias: "viewerTeams".to_owned(),
            select: "memberId".to_owned(),
            select_from: "peerTeams".to_owned(),
            joins: vec![VisibilityJoin {
                table: "memberTeams".to_owned(),
                alias: "peerTeams".to_owned(),
                left_alias: "viewerTeams".to_owned(),
                left_column: "teamId".to_owned(),
                right_column: "teamId".to_owned(),
            }],
            constraints: vec![VisibilityConstraint {
                in_set: String::new(),
                table: "viewerTeams".to_owned(),
                column: "memberId".to_owned(),
                context: "member.id".to_owned(),
                value: None,
            }],
        };
        validate_set(&set).unwrap();
        let (sql, parameters) = compile_set(
            &set,
            &BTreeMap::from([("member.id".to_owned(), "member-1".to_owned())]),
        )
        .unwrap();
        assert!(sql.contains("\"memberTeams\" AS v0 JOIN \"memberTeams\" AS v1"));
        assert!(sql.contains("v0.\"teamId\" = v1.\"teamId\""));
        assert_eq!(parameters, vec![Value::String("member-1".to_owned())]);
    }

    #[test]
    fn literal_predicates_compile_for_source_and_joined_rows() {
        let set = VisibilitySet {
            kind: String::new(),
            from: String::new(),
            key: String::new(),
            table: "boardMessages".to_owned(),
            alias: String::new(),
            select: "id".to_owned(),
            select_from: String::new(),
            joins: vec![VisibilityJoin {
                table: "boards".to_owned(),
                alias: String::new(),
                left_alias: String::new(),
                left_column: "boardId".to_owned(),
                right_column: "id".to_owned(),
            }],
            constraints: vec![VisibilityConstraint {
                in_set: String::new(),
                table: "boards".to_owned(),
                column: "visibility".to_owned(),
                context: String::new(),
                value: Some(serde_json::json!({"literal": "public"})),
            }],
        };
        validate_set(&set).unwrap();
        let (sql, parameters) = compile_set(&set, &BTreeMap::new()).unwrap();
        assert!(sql.contains("v1.\"visibility\" = $1"));
        assert_eq!(parameters, vec![Value::String("public".to_owned())]);

        let plan = VisibilityPlan {
            version: None,
            permission_tables: Vec::new(),
            table: "boards".to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::new(),
            predicate: VisibilityExpression {
                on_missing: None,
                parent_where: Vec::new(),
                via: None,
                table: String::new(),
                key: String::new(),
                operator: "eq".to_owned(),
                column: "deletedAt".to_owned(),
                context: String::new(),
                set: String::new(),
                value: Some(serde_json::json!({"literal": null})),
                children: Vec::new(),
            },
        };
        let resolved = ResolvedVisibility {
            inherited: Default::default(),
            indexes: Default::default(),
            ephemeral_member: false,
            revision: 1,
            direct: BTreeMap::new(),
            role: String::new(),
            permissions: Value::Null,
            sets: BTreeMap::new(),
            fingerprint: String::new(),
        };
        let mut predicate_parameters = Vec::new();
        let predicate =
            compile_predicate(&plan, &resolved, "row", &mut predicate_parameters).unwrap();
        assert_eq!(predicate, "row.\"deletedAt\" IS NULL");
        assert!(predicate_parameters.is_empty());
        assert!(row_matches(
            &plan,
            &resolved,
            &serde_json::json!({"deletedAt": null})
        ));
    }

    #[test]
    fn composite_predicates_are_grouped_before_callers_append_filters() {
        let plan = VisibilityPlan {
            version: None,
            permission_tables: Vec::new(),
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::new(),
            predicate: VisibilityExpression {
                on_missing: None,
                parent_where: Vec::new(),
                via: None,
                table: String::new(),
                key: String::new(),
                operator: "or".to_owned(),
                column: String::new(),
                context: String::new(),
                set: String::new(),
                value: None,
                children: vec![
                    VisibilityExpression {
                        on_missing: None,
                        parent_where: Vec::new(),
                        via: None,
                        table: String::new(),
                        key: String::new(),
                        operator: "role".to_owned(),
                        column: String::new(),
                        context: String::new(),
                        set: String::new(),
                        value: Some(Value::String("admin".to_owned())),
                        children: Vec::new(),
                    },
                    VisibilityExpression {
                        on_missing: None,
                        parent_where: Vec::new(),
                        via: None,
                        table: String::new(),
                        key: String::new(),
                        operator: "eqContext".to_owned(),
                        column: "createdBy".to_owned(),
                        context: "member.id".to_owned(),
                        set: String::new(),
                        value: None,
                        children: Vec::new(),
                    },
                ],
            },
        };
        let resolved = ResolvedVisibility {
            inherited: Default::default(),
            indexes: Default::default(),
            ephemeral_member: false,
            revision: 1,
            direct: BTreeMap::from([
                ("account.id".to_owned(), "account-1".to_owned()),
                ("member.id".to_owned(), "member-1".to_owned()),
                ("tenant.id".to_owned(), "tenant-1".to_owned()),
            ]),
            role: "admin".to_owned(),
            permissions: Value::Null,
            sets: BTreeMap::new(),
            fingerprint: String::new(),
        };
        let mut parameters = Vec::new();
        let predicate = compile_predicate(&plan, &resolved, "row", &mut parameters).unwrap();
        let statement = format!("WHERE {predicate} AND row.\"id\" = $4");

        assert!(statement.starts_with("WHERE ((EXISTS"));
        assert!(statement.contains(") OR (row.\"createdBy\" = $3)) AND row.\"id\" = $4"));
    }

    #[test]
    fn dependency_columns_resolve_aliases_to_physical_tables() {
        let plan = VisibilityPlan {
            version: None,
            permission_tables: Vec::new(),
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::from([(
                "audienceTasks".to_owned(),
                VisibilitySet {
                    kind: String::new(),
                    from: String::new(),
                    key: String::new(),
                    table: "taskAudiences".to_owned(),
                    alias: "taskAudience".to_owned(),
                    select: "taskId".to_owned(),
                    select_from: "taskAudience".to_owned(),
                    joins: vec![VisibilityJoin {
                        table: "memberAudiences".to_owned(),
                        alias: "memberAudience".to_owned(),
                        left_alias: "taskAudience".to_owned(),
                        left_column: "audienceId".to_owned(),
                        right_column: "audienceId".to_owned(),
                    }],
                    constraints: vec![VisibilityConstraint {
                        in_set: String::new(),
                        table: "memberAudience".to_owned(),
                        column: "memberId".to_owned(),
                        context: "member.id".to_owned(),
                        value: None,
                    }],
                },
            )]),
            predicate: VisibilityExpression {
                on_missing: None,
                parent_where: Vec::new(),
                via: None,
                table: String::new(),
                key: String::new(),
                operator: "inSet".to_owned(),
                column: "id".to_owned(),
                context: String::new(),
                set: "audienceTasks".to_owned(),
                value: None,
                children: Vec::new(),
            },
        };

        let dependencies = plan.dependency_columns();
        assert_eq!(
            dependencies.columns["taskAudiences"],
            BTreeSet::from(["audienceId".to_owned(), "taskId".to_owned()]),
        );
        assert_eq!(
            dependencies.columns["memberAudiences"],
            BTreeSet::from(["audienceId".to_owned(), "memberId".to_owned()]),
        );
        assert_eq!(
            dependencies.columns["members"],
            BTreeSet::from([
                "account_id".to_owned(),
                "id".to_owned(),
                "permissions".to_owned(),
                "role".to_owned(),
                "status".to_owned(),
            ]),
        );
        assert!(dependencies.change_affects("memberAudiences", "update", &["memberId".to_owned()],));
        assert!(!dependencies.change_affects(
            "memberAudiences",
            "update",
            &["displayName".to_owned()],
        ));
    }
    #[test]
    fn nested_member_checks_share_only_the_account_context_binding() {
        let plan = dependency_plan(serde_json::json!({"operator":"or","children":[
            {"operator":"role","value":"admin"},
            {"operator":"not","children":[{"operator":"role","value":"owner"}]},
            {"operator":"and","children":[{"operator":"permission","value":"read"},{"operator":"permission","value":"write"}]}
        ]}));
        let mut member = dependency_member("admin");
        member.direct.insert("account.id".into(), "admin".into());
        let mut parameters = vec![serde_json::json!("prefix")];
        let sql = compile_predicate(&plan, &member, "r", &mut parameters).unwrap();
        assert_eq!(
            parameters,
            vec![
                serde_json::json!("prefix"),
                serde_json::json!("admin"),
                serde_json::json!("admin"),
                serde_json::json!("owner"),
                serde_json::json!("read"),
                serde_json::json!("write")
            ]
        );
        assert_eq!(sql.matches("_gonvex_member.account_id = $2").count(), 4);
        assert_eq!(sql.matches("_gonvex_member.status = 'active'").count(), 4);
        assert!(sql.contains("_gonvex_member.role = $3"));
        assert!(sql.contains("permissions ->> $5::text"));
    }
}

#[cfg(test)]
mod mask_tests {
    use super::*;
    use serde_json::json;

    fn plan(value: Value) -> VisibilityPlan {
        serde_json::from_value(value).unwrap()
    }

    fn member(id: &str, sets: &[(&str, &[&str])]) -> ResolvedVisibility {
        ResolvedVisibility {
            ephemeral_member: false,
            revision: 1,
            direct: BTreeMap::from([
                ("member.id".to_owned(), id.to_owned()),
                ("account.id".to_owned(), format!("acct-{id}")),
                ("tenant.id".to_owned(), "t".to_owned()),
            ]),
            role: "member".to_owned(),
            permissions: json!({}),
            sets: sets
                .iter()
                .map(|(name, values)| {
                    (
                        name.to_string(),
                        values.iter().map(|value| value.to_string()).collect(),
                    )
                })
                .collect(),
            fingerprint: String::new(),
            inherited: BTreeMap::new(),
            indexes: BTreeMap::new(),
        }
    }

    fn tasks_plan() -> VisibilityPlan {
        plan(json!({
            "version": 2, "table": "tasks", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams"],
            "sets": {
                "workspaces": {"kind": "principal", "table": "workspaceTeams", "select": "workspaceId",
                    "joins": [{"table": "memberTeams", "leftColumn": "teamId", "rightColumn": "teamId"}],
                    "where": [{"table": "memberTeams", "column": "memberId", "context": "member.id"}]},
                "assigned": {"kind": "grant", "table": "taskUsers", "select": "taskId", "joins": [],
                    "where": [{"table": "taskUsers", "column": "userId", "context": "member.id"},
                              {"table": "taskUsers", "column": "deletedAt", "value": {"literal": null}}]},
                "teamContexts": {"kind": "grant", "table": "taskWorkspaceContexts", "select": "taskId",
                    "joins": [{"table": "memberTeams", "leftColumn": "targetTeamId", "rightColumn": "teamId"}],
                    "where": [{"table": "memberTeams", "column": "memberId", "context": "member.id"}]}
            },
            "where": {"operator": "or", "children": [
                {"operator": "inSet", "column": "workspaceId", "set": "workspaces"},
                {"operator": "inSet", "column": "_id", "set": "assigned"},
                {"operator": "inSet", "column": "_id", "set": "teamContexts"}
            ]}
        }))
    }

    #[test]
    fn grant_rows_are_decided_from_the_row_and_member_alone() {
        let plan = tasks_plan();
        let alice = member("alice", &[("teamContexts::join", &["team-a"])]);
        let set = &plan.sets["assigned"];
        let row = |user: &str| json!({"_id": "tu-1", "taskId": "task-1", "userId": user, "deletedAt": null});
        assert_eq!(
            grant_contribution("assigned", set, &alice, &row("alice")),
            GrantContribution::Token("task-1".to_owned())
        );
        assert_eq!(
            grant_contribution("assigned", set, &alice, &row("bob")),
            GrantContribution::None
        );
        assert_eq!(
            grant_contribution(
                "assigned",
                set,
                &alice,
                &json!({"_id": "tu-1", "taskId": "task-1", "userId": "alice", "deletedAt": 5})
            ),
            GrantContribution::None,
            "a soft-deleted assignment grants nothing"
        );
        assert_eq!(
            grant_contribution("assigned", set, &alice, &json!({"taskId": "task-1"})),
            GrantContribution::Unknown,
            "a missing constrained column is unknown, never evidence"
        );
        let team = &plan.sets["teamContexts"];
        assert_eq!(
            grant_contribution(
                "teamContexts",
                team,
                &alice,
                &json!({"taskId": "task-2", "targetTeamId": "team-a"})
            ),
            GrantContribution::Token("task-2".to_owned())
        );
        assert_eq!(
            grant_contribution(
                "teamContexts",
                team,
                &alice,
                &json!({"taskId": "task-2", "targetTeamId": "team-b"})
            ),
            GrantContribution::None
        );
    }

    #[test]
    fn grant_join_set_selects_the_members_join_tokens() {
        let plan = tasks_plan();
        let join = grant_join_set(&plan.sets["teamContexts"]).unwrap();
        assert_eq!(join.table, "memberTeams");
        assert_eq!(join.select, "teamId");
        assert!(join.joins.is_empty());
        assert_eq!(join.constraints.len(), 1);
        assert_eq!(join.constraints[0].context, "member.id");
        let (sql, parameters) = compile_set(&join, &member("alice", &[]).direct).unwrap();
        assert!(sql.starts_with("SELECT DISTINCT v0.\"teamId\"::text FROM \"memberTeams\" AS v0"));
        assert_eq!(parameters, vec![json!("alice")]);
    }

    #[test]
    fn only_permission_rows_naming_the_member_rebuild_its_principal() {
        let plan = tasks_plan();
        let alice = member("alice", &[]);
        let concerns = |table: &str, operation: &str, old: Value, new: Value| {
            plan.principal_change_concerns(&alice, table, operation, &[], &old, &new)
        };
        // Data and grant writes are never principal changes.
        assert!(!concerns(
            "tasks",
            "insert",
            Value::Null,
            json!({"_id": "t", "workspaceId": "w"})
        ));
        assert!(!concerns(
            "taskUsers",
            "insert",
            Value::Null,
            json!({"userId": "alice", "taskId": "t"})
        ));
        // Another member's team membership is not Alice's principal.
        assert!(!concerns(
            "memberTeams",
            "insert",
            Value::Null,
            json!({"memberId": "bob", "teamId": "x"})
        ));
        assert!(concerns(
            "memberTeams",
            "insert",
            Value::Null,
            json!({"memberId": "alice", "teamId": "x"})
        ));
        assert!(concerns(
            "memberTeams",
            "delete",
            json!({"memberId": "alice", "teamId": "x"}),
            Value::Null
        ));
        // Bob's own member row (role change) concerns only Bob.
        let bob_row = json!({"id": "bob", "role": "admin"});
        assert!(!plan.principal_change_concerns(
            &alice,
            "members",
            "update",
            &["role".to_owned()],
            &bob_row,
            &bob_row
        ));
        let alice_row = json!({"id": "alice", "role": "admin"});
        assert!(plan.principal_change_concerns(
            &alice,
            "members",
            "update",
            &["role".to_owned()],
            &alice_row,
            &alice_row
        ));
        assert!(!plan.principal_change_concerns(
            &alice,
            "members",
            "update",
            &["lastSeenAt".to_owned()],
            &alice_row,
            &alice_row
        ));
    }

    fn parent(rows: &[(&str, Value)]) -> InheritedParent {
        InheritedParent {
            plan: Arc::new(tasks_plan()),
            resolved: Arc::new(member(
                "alice",
                &[
                    ("workspaces", &["w1"]),
                    ("assigned", &["t-assigned"]),
                    ("teamContexts", &[]),
                ],
            )),
            index: Arc::new(std::sync::RwLock::new(ParentAclIndex::from_snapshot(
                1,
                rows.iter()
                    .map(|(key, row)| (key.to_string(), row.clone()))
                    .collect(),
            ))),
        }
    }

    #[test]
    fn inherited_rows_follow_the_parent_in_memory() {
        let child = plan(json!({
            "version": 2, "table": "taskNotes", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams"], "sets": {},
            "where": {"operator": "inherits", "column": "taskId", "table": "tasks", "key": "_id",
                      "onMissing": {"operator": "role", "value": "system_admin"}}
        }));
        let mut alice = member("alice", &[]);
        alice.inherited.insert(
            "tasks".to_owned(),
            parent(&[
                (
                    "t-visible",
                    json!({"_id": "t-visible", "workspaceId": "w1"}),
                ),
                ("t-hidden", json!({"_id": "t-hidden", "workspaceId": "w2"})),
                (
                    "t-assigned",
                    json!({"_id": "t-assigned", "workspaceId": "w2"}),
                ),
            ]),
        );
        let note = |task: Value| json!({"_id": "n", "taskId": task});
        assert!(row_matches(&child, &alice, &note(json!("t-visible"))));
        assert!(!row_matches(&child, &alice, &note(json!("t-hidden"))));
        assert!(
            row_matches(&child, &alice, &note(json!("t-assigned"))),
            "a parent grant reaches its children"
        );
        assert!(
            !row_matches(&child, &alice, &note(Value::Null)),
            "a null foreign key denies"
        );
        assert!(
            !row_matches(&child, &alice, &note(json!("t-gone"))),
            "orphans need onMissing"
        );
        alice.role = "system_admin".to_owned();
        assert!(
            row_matches(&child, &alice, &note(json!("t-gone"))),
            "onMissing admits orphans"
        );
        assert!(
            !row_matches(&child, &alice, &note(json!("t-hidden"))),
            "onMissing never overrides a hidden parent"
        );
        // Parent changes apply once and are seen by every child check.
        alice.role = "member".to_owned();
        alice.inherited["tasks"].index.write().unwrap().apply(
            2,
            [(
                "t-hidden".to_owned(),
                Some(json!({"_id": "t-hidden", "workspaceId": "w1"})),
            )],
        );
        assert!(row_matches(&child, &alice, &note(json!("t-hidden"))));
    }

    #[test]
    fn derived_sets_map_grants_through_the_shared_index_in_memory() {
        let categories = plan(json!({
            "version": 2, "table": "categories", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams"],
            "sets": {
                "assignedTasks": {"kind": "grant", "table": "taskUsers", "select": "taskId", "joins": [],
                    "where": [{"table": "taskUsers", "column": "userId", "context": "member.id"}]},
                "assignedTaskCategories": {"kind": "derived", "from": "assignedTasks", "table": "tasks",
                    "key": "_id", "select": "categoryId", "joins": [],
                    "where": [{"column": "deletedAt", "value": {"literal": null}}]}
            },
            "where": {"operator": "inSet", "column": "_id", "set": "assignedTaskCategories"}
        }));
        categories.validate().unwrap();
        let mut alice = member("alice", &[("assignedTasks", &["t1", "t2"])]);
        let index = Arc::new(std::sync::RwLock::new(ParentAclIndex::from_snapshot(
            1,
            BTreeMap::from([
                (
                    "t1".to_owned(),
                    json!({"_id": "t1", "categoryId": "c1", "deletedAt": null}),
                ),
                (
                    "t2".to_owned(),
                    json!({"_id": "t2", "categoryId": "c2", "deletedAt": 7}),
                ),
                (
                    "t3".to_owned(),
                    json!({"_id": "t3", "categoryId": "c3", "deletedAt": null}),
                ),
            ]),
        )));
        alice
            .indexes
            .insert("assignedTaskCategories".to_owned(), index.clone());
        let first = recompute_derived(&categories, &mut alice);
        assert_eq!(first.len(), 1);
        assert_eq!(
            alice.sets["assignedTaskCategories"],
            BTreeSet::from(["c1".to_owned()]),
            "a soft-deleted granted task contributes no category"
        );
        assert!(row_matches(&categories, &alice, &json!({"_id": "c1"})));
        assert!(
            !row_matches(&categories, &alice, &json!({"_id": "c3"})),
            "unassigned tasks grant nothing"
        );
        // A recategorised task moves the grant, and only for members holding it.
        assert!(derived_change_concerns(
            &categories,
            &alice,
            "tasks",
            "t1",
            &json!({"_id": "t1"}),
            &json!({"_id": "t1"})
        ));
        assert!(!derived_change_concerns(
            &categories,
            &alice,
            "tasks",
            "t3",
            &json!({"_id": "t3"}),
            &json!({"_id": "t3"})
        ));
        index.write().unwrap().apply(
            2,
            [(
                "t1".to_owned(),
                Some(json!({"_id": "t1", "categoryId": "c9", "deletedAt": null})),
            )],
        );
        let moved = recompute_derived(&categories, &mut alice);
        assert_eq!(
            moved,
            vec![(
                "assignedTaskCategories".to_owned(),
                BTreeSet::from(["c9".to_owned()]),
                BTreeSet::from(["c1".to_owned()])
            )]
        );
        assert!(
            recompute_derived(&categories, &mut alice).is_empty(),
            "recompute is idempotent"
        );
    }

    #[test]
    fn derived_sets_reject_unsafe_shapes() {
        let with = |set: Value| {
            plan(json!({
                "version": 2, "table": "categories", "key": "_id", "permissionTables": ["memberTeams"],
                "sets": {
                    "assignedTasks": {"kind": "grant", "table": "taskUsers", "select": "taskId", "joins": [],
                        "where": [{"table": "taskUsers", "column": "userId", "context": "member.id"}]},
                    "derived": set
                },
                "where": {"operator": "inSet", "column": "_id", "set": "derived"}
            }))
        };
        let base = json!({"kind": "derived", "from": "assignedTasks", "table": "tasks", "key": "_id", "select": "categoryId", "joins": [], "where": []});
        with(base.clone()).validate().unwrap();
        let mut own = base.clone();
        own["table"] = json!("categories");
        assert!(
            with(own).validate().is_err(),
            "a derived set never maps through its own table"
        );
        let mut unknown = base.clone();
        unknown["from"] = json!("missing");
        assert!(with(unknown).validate().is_err());
        let mut context = base.clone();
        context["where"] = json!([{"column": "createdBy", "context": "member.id"}]);
        with(context).validate().unwrap();
        let mut joined = base.clone();
        joined["where"] = json!([{"table": "other", "column": "x", "value": {"literal": 1}}]);
        assert!(
            with(joined).validate().is_err(),
            "derived filters name indexed-row columns only"
        );
    }

    #[test]
    fn inherited_parent_filters_and_permission_self_reads() {
        // F1: a soft-deleted parent hides children even if the parent plan
        // itself would still admit it.
        let child = plan(json!({
            "version": 2, "table": "crmActivities", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams"], "sets": {},
            "where": {"operator": "inherits", "column": "taskId", "table": "tasks", "key": "_id",
                      "where": [{"column": "deletedAt", "value": {"literal": null}}]}
        }));
        child.validate().unwrap();
        let mut alice = member("alice", &[]);
        alice.inherited.insert(
            "tasks".to_owned(),
            parent(&[
                (
                    "live",
                    json!({"_id": "live", "workspaceId": "w1", "deletedAt": null}),
                ),
                (
                    "gone",
                    json!({"_id": "gone", "workspaceId": "w1", "deletedAt": 5}),
                ),
            ]),
        );
        assert!(row_matches(&child, &alice, &json!({"taskId": "live"})));
        assert!(!row_matches(&child, &alice, &json!({"taskId": "gone"})));
        let mut parameters = Vec::new();
        let sql = compile_predicate(&child, &alice, "r", &mut parameters).unwrap();
        assert!(sql.contains("\"deletedAt\" IS NULL"));
        let parent = &alice.inherited["tasks"];
        assert!(!child.parent_admits(
            "tasks",
            parent,
            &json!({"_id": "gone", "workspaceId": "w1", "deletedAt": 5})
        ));
        // F2: a permission-table source may read its own tree; a data source may not.
        let spots = |permission: &str| {
            plan(json!({
                "version": 2, "table": "spots", "key": "_id", "permissionTables": [permission, "memberSpots"],
                "sets": {"tree": {"kind": "principal", "table": "spots", "alias": "child", "select": "_id",
                    "joins": [{"table": "memberSpots", "leftColumn": "parentId", "rightColumn": "spotId"}],
                    "where": [{"table": "memberSpots", "column": "memberId", "context": "member.id"}]}},
                "where": {"operator": "inSet", "column": "_id", "set": "tree"}
            }))
        };
        spots("spots").validate().unwrap();
        assert!(spots("other").validate().is_err());
    }

    #[test]
    fn derived_sets_correlate_rows_by_a_non_unique_key() {
        // F3: workspaces of my live approval contexts on tasks I must approve.
        let workspaces = plan(json!({
            "version": 2, "table": "workspaces", "key": "_id",
            "permissionTables": ["memberTeams"],
            "sets": {
                "teams": {"kind": "principal", "table": "memberTeams", "select": "teamId", "joins": [],
                    "where": [{"table": "memberTeams", "column": "memberId", "context": "member.id"}]},
                "pending": {"kind": "grant", "table": "taskApprovalInstances", "select": "taskId", "joins": [],
                    "where": [{"table": "taskApprovalInstances", "column": "approverUserId", "context": "member.id"},
                              {"table": "taskApprovalInstances", "column": "status", "value": {"literal": "pending"}}]},
                "mine": {"kind": "derived", "from": "pending", "table": "taskWorkspaceContexts", "key": "taskId",
                    "select": "workspaceId", "joins": [],
                    "where": [{"column": "kind", "value": {"literal": "approval"}},
                              {"column": "revokedAt", "value": {"literal": null}},
                              {"column": "targetUserId", "context": "member.id"}]},
                "team": {"kind": "derived", "from": "pending", "table": "taskWorkspaceContexts", "key": "taskId",
                    "select": "workspaceId", "joins": [],
                    "where": [{"column": "kind", "value": {"literal": "approval"}},
                              {"column": "targetTeamId", "inSet": "teams"}]}
            },
            "where": {"operator": "or", "children": [
                {"operator": "inSet", "column": "_id", "set": "mine"},
                {"operator": "inSet", "column": "_id", "set": "team"}]}
        }));
        workspaces.validate().unwrap();
        let mut alice = member("alice", &[("pending", &["t1"]), ("teams", &["approvers"])]);
        let contexts = Arc::new(std::sync::RwLock::new(ParentAclIndex::from_rows(
            1,
            [
                (
                    "c1".to_owned(),
                    "t1".to_owned(),
                    json!({"kind": "approval", "revokedAt": null, "targetUserId": "alice", "workspaceId": "w-alice"}),
                ),
                (
                    "c2".to_owned(),
                    "t1".to_owned(),
                    json!({"kind": "approval", "revokedAt": null, "targetUserId": "bob", "workspaceId": "w-bob"}),
                ),
                (
                    "c3".to_owned(),
                    "t1".to_owned(),
                    json!({"kind": "approval", "targetTeamId": "approvers", "workspaceId": "w-team"}),
                ),
                (
                    "c4".to_owned(),
                    "t2".to_owned(),
                    json!({"kind": "approval", "revokedAt": null, "targetUserId": "alice", "workspaceId": "w-other"}),
                ),
            ],
        )));
        alice.indexes.insert("mine".to_owned(), contexts.clone());
        alice.indexes.insert("team".to_owned(), contexts);
        recompute_derived(&workspaces, &mut alice);
        assert_eq!(
            alice.sets["mine"],
            BTreeSet::from(["w-alice".to_owned()]),
            "only my context on my pending task"
        );
        assert_eq!(
            alice.sets["team"],
            BTreeSet::from(["w-team".to_owned()]),
            "team targets follow my teams"
        );
        let mut bad = workspaces.clone();
        bad.sets.get_mut("team").unwrap().constraints[1].in_set = "mine".to_owned();
        assert!(
            bad.validate().is_err(),
            "derived inSet names a principal or grant set"
        );
    }

    #[test]
    fn inherits_any_admits_rows_linked_to_any_visible_parent() {
        // F4: a document is visible when any associated task is visible.
        let documents = plan(json!({
            "version": 2, "table": "documents", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams"], "sets": {},
            "where": {"operator": "inheritsAny", "column": "_id", "table": "tasks", "key": "_id",
                "via": {"table": "documentAssociations", "childColumn": "documentId", "parentColumn": "taskId"}}
        }));
        documents.validate().unwrap();
        let mut alice = member("alice", &[]);
        alice.inherited.insert(
            "tasks".to_owned(),
            parent(&[
                (
                    "t-visible",
                    json!({"_id": "t-visible", "workspaceId": "w1"}),
                ),
                ("t-hidden", json!({"_id": "t-hidden", "workspaceId": "w2"})),
            ]),
        );
        let via = documents.predicate.via.clone().unwrap();
        let rows = [
            (
                "a1".to_owned(),
                json!({"documentId": "d-shared", "taskId": "t-hidden"}),
            ),
            (
                "a2".to_owned(),
                json!({"documentId": "d-shared", "taskId": "t-visible"}),
            ),
            (
                "a3".to_owned(),
                json!({"documentId": "d-private", "taskId": "t-hidden"}),
            ),
        ];
        for column in [&via.child_column, &via.parent_column] {
            let index = ParentAclIndex::from_rows(
                1,
                rows.iter()
                    .map(|(id, row)| (id.clone(), scalar(row.get(column.as_str())), row.clone())),
            );
            alice.indexes.insert(
                via_index_name(&via, column),
                Arc::new(std::sync::RwLock::new(index)),
            );
        }
        assert!(row_matches(&documents, &alice, &json!({"_id": "d-shared"})));
        assert!(!row_matches(
            &documents,
            &alice,
            &json!({"_id": "d-private"})
        ));
        assert!(!row_matches(
            &documents,
            &alice,
            &json!({"_id": "d-unlinked"})
        ));
        let mut parameters = Vec::new();
        let sql = compile_predicate(&documents, &alice, "r", &mut parameters).unwrap();
        assert!(sql.contains("FROM \"documentAssociations\" AS _gonvex_via_"));
        assert!(sql.contains(".\"documentId\"::text = r.\"_id\"::text"));
    }

    #[test]
    fn inherited_sql_mirrors_the_in_memory_rule() {
        let child = plan(json!({
            "version": 2, "table": "taskNotes", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams"], "sets": {},
            "where": {"operator": "inherits", "column": "taskId", "table": "tasks", "key": "_id",
                      "onMissing": {"operator": "role", "value": "system_admin"}}
        }));
        let mut alice = member("alice", &[]);
        alice.inherited.insert("tasks".to_owned(), parent(&[]));
        let mut parameters = Vec::new();
        let sql = compile_predicate(&child, &alice, "r", &mut parameters).unwrap();
        assert!(sql.contains("EXISTS (SELECT 1 FROM \"tasks\" AS _gonvex_parent_"));
        assert!(sql.contains("::text = r.\"taskId\"::text"));
        assert!(sql.contains("r.\"taskId\" IS NOT NULL AND NOT EXISTS"));
        assert!(
            sql.contains("= ANY(ARRAY(SELECT jsonb_array_elements_text("),
            "parent tokens bind as arrays"
        );
        assert!(
            !sql.contains("\"memberTeams\""),
            "no permission-table subqueries per row"
        );
    }
}
