//! Structured one-shot and Live Query execution.
//!
//! Query JavaScript is metadata only on this path. The trusted host compiles
//! the declared plan and injects the table's visibility predicate so a module
//! cannot bypass row authorization with arbitrary SQL.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use gonvex_postgres::{TenantSession, TenantTransaction};
use gonvex_protocol::{ServerMessage, SubscriptionRevision};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::Row;
use thiserror::Error;

use crate::change_feed::{FeedEvent, LogChange};
use crate::host_calls::bind_value;
use crate::visibility::{self, ResolvedVisibility, VisibilityDependencies, VisibilityPlan};
use crate::Runtime;

#[derive(Clone, Debug)]
pub struct LiveQuerySubscription {
    pub id: String,
    pub path: String,
    pub args: Value,
    pub plan: LiveQueryPlan,
    pub visibility: VisibilityPlan,
    pub visibility_dependencies: VisibilityDependencies,
    pub related_dependencies: Vec<VisibilityDependencies>,
    pub required_revision: u64,
    pub computed_revision: u64,
    pub epoch: String,
    pub last_result: Value,
}

pub struct LiveQueryOpenResult {
    pub message: ServerMessage,
    pub subscription: LiveQuerySubscription,
}

fn change_affects_live_query(
    source: &str,
    dependencies: &VisibilityDependencies,
    change: &LogChange,
) -> bool {
    change.table == source
        || dependencies.change_affects(&change.table, &change.operation, &change.changed_columns)
}

#[derive(Clone)]
struct QuerySnapshot {
    result: Value,
    revision: u64,
    epoch: String,
}

#[derive(Clone, Default)]
pub struct SharedLiveQueryCache {
    entries: Arc<tokio::sync::Mutex<BTreeMap<String, QuerySnapshot>>>,
    locks: Arc<tokio::sync::Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl SharedLiveQueryCache {
    pub async fn clear(&self) {
        self.entries.lock().await.clear();
        self.locks.lock().await.clear();
    }

    async fn lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().await;
        if locks.len() > 2_048 {
            locks.clear();
        }
        locks
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn get(&self, key: &str) -> Option<QuerySnapshot> {
        self.entries.lock().await.get(key).cloned()
    }

    async fn insert(&self, key: String, snapshot: QuerySnapshot) {
        let mut entries = self.entries.lock().await;
        if entries.len() >= 1_024 {
            entries.clear();
        }
        entries.insert(key, snapshot);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveQueryPlan {
    pub table: String,
    pub key: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub result_path: Vec<String>,
    #[serde(default, rename = "where")]
    pub predicate: Option<LiveExpression>,
    pub search: Option<LiveSearch>,
    pub filters: Option<LiveFilters>,
    pub sort: Option<LiveSort>,
    pub window: Option<LiveWindow>,
    /// Server-maintained one-to-one read projection. Values and visibility
    /// still come from the base entity; projection rows only select/order IDs.
    #[serde(default)]
    pub index: Option<LiveIndex>,
    #[serde(default)]
    pub server_only: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveExpression {
    pub operator: String,
    #[serde(default)]
    pub column: String,
    pub value: Option<LiveValue>,
    pub value_to: Option<LiveValue>,
    #[serde(default)]
    pub children: Vec<LiveExpression>,
    pub relation: Option<LiveRelation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveRelation {
    pub table: String,
    pub column: String,
    #[serde(default, rename = "where")]
    pub predicate: Option<Box<LiveExpression>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveValue {
    #[serde(default)]
    pub context: String,
    #[serde(default)]
    pub argument: String,
    #[serde(default)]
    pub literal: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSearch {
    pub argument: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub boolean_terms: bool,
    /// Search documents owned by this entity, keyed by its canonical ID.
    #[serde(default)]
    pub sources: Vec<LiveSearchSource>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSearchSource {
    pub table: String,
    pub key: String,
    pub column: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveIndex {
    pub table: String,
    pub key: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub sort_columns: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveFilters {
    pub argument: String,
    pub allowed_columns: Vec<String>,
    pub allowed_operators: Vec<String>,
    #[serde(default)]
    pub column_types: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSort {
    #[serde(default)]
    pub column_argument: String,
    #[serde(default)]
    pub direction_argument: String,
    pub default_column: String,
    pub default_direction: String,
    pub allowed_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveWindow {
    pub offset_argument: String,
    pub limit_argument: String,
    pub default_limit: usize,
    pub max_limit: usize,
    #[serde(default)]
    pub count: String,
}

#[derive(Debug, Error)]
pub enum LiveQueryError {
    #[error("structured query arguments must be an object")]
    InvalidArguments,
    #[error("visibility plan required for table {0:?}")]
    VisibilityMissing(String),
    #[error("invalid structured query: {0}")]
    Invalid(String),
    #[error(transparent)]
    Database(#[from] gonvex_postgres::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Visibility(#[from] visibility::VisibilityError),
}

impl LiveQueryPlan {
    fn index_change_affects(&self, table: &str) -> bool {
        relation_tables(self.predicate.as_ref()).contains(table)
            || self.index.as_ref().is_some_and(|index| index.table == table || index.dependencies.iter().any(|dependency| dependency == table))
            || self.search.as_ref().is_some_and(|search| search.sources.iter().any(|source| source.table == table || source.dependencies.iter().any(|dependency| dependency == table)))
    }

    pub fn validate(&self) -> Result<(), LiveQueryError> {
        quote(&self.table)?;
        quote(&self.key)?;
        if self.columns.is_empty() {
            return Err(LiveQueryError::Invalid(format!(
                "query for {:?} must declare columns",
                self.table
            )));
        }
        let mut columns = BTreeSet::new();
        for column in &self.columns {
            quote(column)?;
            columns.insert(column.as_str());
        }
        if !columns.contains(self.key.as_str()) {
            return Err(LiveQueryError::Invalid(format!(
                "query for {:?} must include key {:?} in columns",
                self.table, self.key
            )));
        }
        if self.server_only {
            return Err(LiveQueryError::Invalid(
                "server-only arbitrary reactive predicates are not supported".to_owned(),
            ));
        }
        if let Some(expression) = &self.predicate {
            validate_expression(expression)?;
        }
        if let Some(search) = &self.search {
            require_name(&search.argument, "search argument")?;
            for column in &search.columns {
                quote(column)?;
            }
            for source in &search.sources {
                quote(&source.table)?;
                quote(&source.key)?;
                quote(&source.column)?;
                for dependency in &source.dependencies { quote(dependency)?; }
            }
        }
        if let Some(index) = &self.index {
            quote(&index.table)?;
            quote(&index.key)?;
            for column in &index.columns { quote(column)?; }
            for dependency in &index.dependencies { quote(dependency)?; }
            for (logical, columns) in &index.sort_columns {
                quote(logical)?;
                if columns.is_empty() { return Err(LiveQueryError::Invalid("index sort must contain columns".to_owned())); }
                for column in columns { quote(column)?; }
            }
        }
        if let Some(filters) = &self.filters {
            require_name(&filters.argument, "filter argument")?;
            for column in &filters.allowed_columns {
                quote(column)?;
            }
            for operator in &filters.allowed_operators {
                if !valid_filter_operator(operator) {
                    return Err(LiveQueryError::Invalid(format!(
                        "unsupported allowed filter operator {operator:?}"
                    )));
                }
            }
            for (column, column_type) in &filters.column_types {
                if !filters.allowed_columns.contains(column) {
                    return Err(LiveQueryError::Invalid(format!(
                        "filter column type references unallowed column {column:?}"
                    )));
                }
                if !matches!(column_type.as_str(), "text" | "number") {
                    return Err(LiveQueryError::Invalid(format!(
                        "filter column {column:?} has unsupported type {column_type:?}"
                    )));
                }
            }
        }
        if let Some(sort) = &self.sort {
            quote(&sort.default_column)?;
            if !matches!(sort.default_direction.as_str(), "asc" | "desc") {
                return Err(LiveQueryError::Invalid(
                    "default sort direction must be asc or desc".to_owned(),
                ));
            }
            for column in &sort.allowed_columns {
                quote(column)?;
            }
            if !sort.allowed_columns.contains(&sort.default_column) {
                return Err(LiveQueryError::Invalid(
                    "default sort column must be allowed".to_owned(),
                ));
            }
        }
        if let Some(window) = &self.window {
            require_name(&window.offset_argument, "window offset argument")?;
            require_name(&window.limit_argument, "window limit argument")?;
            if window.default_limit == 0 || window.max_limit == 0 || window.max_limit > 10_000 {
                return Err(LiveQueryError::Invalid(
                    "window limits must be between 1 and 10000".to_owned(),
                ));
            }
            if !window.count.is_empty() && window.count != "exact" {
                return Err(LiveQueryError::Invalid(
                    "window count must be exact".to_owned(),
                ));
            }
        }
        for part in &self.result_path {
            require_name(part, "result path segment")?;
        }
        Ok(())
    }
}

impl Runtime {
    pub async fn execute_structured_live_query(
        &self,
        session: &TenantSession,
        plan: &LiveQueryPlan,
        args: &Value,
    ) -> Result<Value, LiveQueryError> {
        plan.validate()?;
        let module = self
            .inner
            .modules
            .project(&session.identity.project_id)
            .await
            .ok_or_else(|| LiveQueryError::Invalid("project module is not loaded".to_owned()))?;
        let visibility_plan = module
            .visibility
            .get(&plan.table)
            .ok_or_else(|| LiveQueryError::VisibilityMissing(plan.table.clone()))?;
        self.execute_live_query_plan(session, plan, visibility_plan, args)
            .await
            .map(|snapshot| snapshot.result)
    }

    pub async fn open_live_query(
        &self,
        session: &TenantSession,
        id: String,
        path: String,
        args: Value,
    ) -> Result<LiveQueryOpenResult, LiveQueryError> {
        let module = self
            .inner
            .modules
            .project(&session.identity.project_id)
            .await
            .ok_or_else(|| LiveQueryError::Invalid("project module is not loaded".to_owned()))?;
        let function = module
            .functions
            .get(&path)
            .filter(|function| function.kind == "query" && function.delivery == "live")
            .ok_or_else(|| {
                LiveQueryError::Invalid(format!("live query {path:?} is not registered"))
            })?;
        if function.internal {
            return Err(LiveQueryError::Invalid(format!(
                "internal query {path:?} cannot be subscribed by a client"
            )));
        }
        let plan = function.live_query_plan.clone().ok_or_else(|| {
            LiveQueryError::Invalid(format!(
                "live query {path:?} requires a structured live query plan"
            ))
        })?;
        plan.validate()?;
        let visibility_plan = module
            .visibility
            .get(&plan.table)
            .cloned()
            .ok_or_else(|| LiveQueryError::VisibilityMissing(plan.table.clone()))?;
        let snapshot = self
            .execute_live_query_plan(session, &plan, &visibility_plan, &args)
            .await?;
        let message = query_message(
            &id,
            &path,
            snapshot.result.clone(),
            "initial",
            &snapshot.epoch,
            snapshot.revision,
        );
        Ok(LiveQueryOpenResult {
            message,
            subscription: LiveQuerySubscription {
                id,
                path,
                args,
                visibility_dependencies: visibility_plan.dependency_columns(),
                related_dependencies: relation_tables(plan.predicate.as_ref()).iter().filter_map(|table| module.visibility.get(table)).map(VisibilityPlan::dependency_columns).collect(),
                plan,
                visibility: visibility_plan,
                required_revision: snapshot.revision,
                computed_revision: snapshot.revision,
                epoch: snapshot.epoch,
                last_result: snapshot.result,
            },
        })
    }

    pub async fn apply_live_query_feed_event(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, LiveQuerySubscription>,
        event: &FeedEvent,
    ) -> Vec<ServerMessage> {
        let (event_revision, changes, reset) = match event {
            FeedEvent::Transaction {
                revision, changes, ..
            } => (*revision, changes.as_slice(), false),
            FeedEvent::Reset { .. } => (0, &[][..], true),
        };
        let mut messages = Vec::new();
        for subscription in subscriptions.values_mut() {
            if !reset
                && (event_revision <= subscription.computed_revision
                    || !changes.iter().any(|change| {
                        change_affects_live_query(
                            &subscription.plan.table,
                            &subscription.visibility_dependencies,
                            change,
                        ) || subscription.plan.index_change_affects(&change.table)
                        || subscription.related_dependencies.iter().any(|dependencies| dependencies.change_affects(&change.table, &change.operation, &change.changed_columns))
                    }))
            {
                continue;
            }
            if !reset {
                subscription.required_revision = subscription.required_revision.max(event_revision);
            }
            match self
                .execute_live_query_plan(
                    session,
                    &subscription.plan,
                    &subscription.visibility,
                    &subscription.args,
                )
                .await
            {
                Ok(snapshot) => {
                    if snapshot.revision < subscription.required_revision {
                        continue;
                    }
                    let changed = snapshot.result != subscription.last_result
                        || snapshot.epoch != subscription.epoch;
                    subscription.computed_revision = snapshot.revision;
                    subscription.epoch = snapshot.epoch;
                    if changed {
                        subscription.last_result = snapshot.result.clone();
                        messages.push(query_message(
                            &subscription.id,
                            &subscription.path,
                            snapshot.result,
                            if reset { "reconnect" } else { "change" },
                            &subscription.epoch,
                            subscription.computed_revision,
                        ));
                    }
                }
                Err(error) => messages.push(ServerMessage::QueryError {
                    id: subscription.id.clone(),
                    path: Some(subscription.path.clone()),
                    error: error.to_string(),
                }),
            }
        }
        messages
    }

    async fn execute_live_query_plan(
        &self,
        session: &TenantSession,
        plan: &LiveQueryPlan,
        visibility_plan: &VisibilityPlan,
        args: &Value,
    ) -> Result<QuerySnapshot, LiveQueryError> {
        let args = args.as_object().ok_or(LiveQueryError::InvalidArguments)?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| LiveQueryError::Invalid("Control Plane is unavailable".to_owned()))?;
        let mut transaction = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        let resolved = visibility::resolve(&mut transaction, session, visibility_plan).await?;
        let mut related = BTreeMap::new();
        let tables = relation_tables(plan.predicate.as_ref());
        if !tables.is_empty() {
            let module = self.inner.modules.project(&session.identity.project_id).await
                .ok_or_else(|| LiveQueryError::Invalid("project module is not loaded".to_owned()))?;
            for table in tables {
                let related_plan = module.visibility.get(&table)
                    .ok_or_else(|| LiveQueryError::VisibilityMissing(table.clone()))?;
                let related_resolved = visibility::resolve(&mut transaction, session, related_plan).await?;
                related.insert(table, (related_plan.clone(), related_resolved));
            }
        }
        let fingerprint = std::iter::once(resolved.fingerprint.as_str()).chain(related.values().map(|(_, item)| item.fingerprint.as_str())).collect::<Vec<_>>().join(":");

        let clock =
            sqlx::query("SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton = true")
                .fetch_one(&mut **transaction.transaction())
                .await?;
        let epoch: String = clock.get("epoch");
        let revision = clock.get::<i64, _>("revision").max(0) as u64;
        let key = canonical_group_key(session, plan, args, &fingerprint, &epoch, revision);
        let singleflight = self.inner.live_query_cache.lock(&key).await;
        let _guard = singleflight.lock().await;
        if let Some(snapshot) = self.inner.live_query_cache.get(&key).await {
            transaction.commit().await?;
            return Ok(snapshot);
        }
        let result =
            execute_in_transaction(&mut transaction, plan, visibility_plan, &resolved, args, &related)
                .await?;
        let result_bytes = serde_json::to_vec(&result)
            .map_err(|error| LiveQueryError::Invalid(error.to_string()))?
            .len();
        if result_bytes > 32 << 20 {
            return Err(LiveQueryError::Invalid(
                "live query result exceeds the 32 MiB limit".to_owned(),
            ));
        }
        transaction.commit().await?;
        let snapshot = QuerySnapshot {
            result,
            epoch,
            revision,
        };
        self.inner
            .live_query_cache
            .insert(key, snapshot.clone())
            .await;
        Ok(snapshot)
    }
}

fn canonical_group_key(
    session: &TenantSession,
    plan: &LiveQueryPlan,
    args: &Map<String, Value>,
    visibility_fingerprint: &str,
    epoch: &str,
    revision: u64,
) -> String {
    use sha2::{Digest as _, Sha256};
    let payload = serde_json::json!({
        "project":session.identity.project_id,
        "tenant":session.route.tenant_id,
        "plan":plan,
        "args":args,
        "visibility":visibility_fingerprint,
        "epoch":epoch,
        "revision":revision,
    });
    Sha256::digest(serde_json::to_vec(&payload).unwrap_or_default())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn query_message(
    id: &str,
    path: &str,
    result: Value,
    reason: &str,
    epoch: &str,
    revision: u64,
) -> ServerMessage {
    ServerMessage::QueryResult {
        id: id.to_owned(),
        payload: BTreeMap::from([
            ("path".to_owned(), Value::String(path.to_owned())),
            ("result".to_owned(), result),
            ("reason".to_owned(), Value::String(reason.to_owned())),
            (
                "subscriptionRevision".to_owned(),
                serde_json::to_value(SubscriptionRevision {
                    epoch: epoch.to_owned(),
                    sequence: revision,
                })
                .unwrap_or(Value::Null),
            ),
        ]),
    }
}

async fn execute_in_transaction(
    transaction: &mut TenantTransaction,
    plan: &LiveQueryPlan,
    visibility_plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    args: &Map<String, Value>,
    related: &BTreeMap<String, (VisibilityPlan, ResolvedVisibility)>,
) -> Result<Value, LiveQueryError> {
    let mut parameters = Vec::new();
    let visibility =
        visibility::compile_predicate(visibility_plan, resolved, "r", &mut parameters)?;
    parameters.push(Value::String(resolved.direct["account.id"].clone()));
    let mut predicates = vec![
        visibility,
        format!(
            "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = ${} AND _gonvex_member.status = 'active')",
            parameters.len()
        ),
    ];
    if let Some(expression) = &plan.predicate {
        predicates.push(index_predicate(plan, compile_related_expression(expression, args, &mut parameters, "r", related, &resolved.direct)?)?);
    }
    if let Some(filters) = &plan.filters {
        let filter = compile_filters(filters, args, &mut parameters, "r")?;
        if !filter.is_empty() {
            predicates.push(index_predicate(plan, filter)?);
        }
    }
    if let Some(search) = &plan.search {
        // Push simple projection filters into related search documents. Repeating
        // membership subqueries there can scan the entire projection for a tiny
        // document match; the outer predicate already enforces that membership.
        let index_filter=predicates.iter().filter(|predicate| predicate.contains("g.") && !predicate.contains("r.") && !predicate.contains("SELECT ")).cloned().collect::<Vec<_>>().join(" AND ");
        let predicate = compile_search_filtered(plan, search, args, &mut parameters, &index_filter)?;
        if !predicate.is_empty() { predicates.push(predicate); }
    }
    let (offset, limit) = window(plan.window.as_ref(), args);
    let order = compile_order(plan, args)?;
    let source = compile_source(plan)?;
    let columns = plan
        .columns
        .iter()
        .map(|column| quote(column).map(|quoted| format!("r.{quoted} AS {quoted}")))
        .collect::<Result<Vec<_>, _>>()?;
    let base_parameters = parameters.clone();
    let mut statement = format!(
        "SELECT row_to_json(_gonvex_live_row)::text FROM (SELECT {} FROM {} WHERE {}",
        columns.join(", "),
        source,
        predicates.join(" AND ")
    );
    statement.push_str(&format!(" ORDER BY {order}"));
    if limit > 0 {
        parameters.push(Value::from(limit as u64));
        statement.push_str(&format!(" LIMIT ${}", parameters.len()));
    }
    if offset > 0 {
        parameters.push(Value::from(offset as u64));
        statement.push_str(&format!(" OFFSET ${}", parameters.len()));
    }
    statement.push_str(") AS _gonvex_live_row");
    let mut query = sqlx::query(&statement);
    for parameter in &parameters {
        query = bind_value(query, parameter).map_err(LiveQueryError::Invalid)?;
    }
    let rows = query.fetch_all(&mut **transaction.transaction()).await?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let raw: String = row.get(0);
            serde_json::from_str(&raw).map_err(|error| LiveQueryError::Invalid(error.to_string()))
        })
        .collect::<Result<Vec<Value>, _>>()?;
    let total = if plan
        .window
        .as_ref()
        .is_some_and(|window| window.count == "exact" && !plan.result_path.is_empty())
    {
        let statement = format!(
            "SELECT count(*) FROM {} WHERE {}",
            source,
            predicates.join(" AND ")
        );
        let mut query = sqlx::query_scalar::<_, i64>(&statement);
        for parameter in &base_parameters {
            query = bind_scalar(query, parameter)?;
        }
        Some(
            query
                .fetch_one(&mut **transaction.transaction())
                .await?
                .max(0) as u64,
        )
    } else {
        None
    };
    Ok(shape_result(rows, &plan.result_path, total, offset, limit))
}

fn compile_expression(
    expression: &LiveExpression,
    args: &Map<String, Value>,
    parameters: &mut Vec<Value>,
    row_alias: &str,
) -> Result<String, LiveQueryError> {
    compile_related_expression(expression, args, parameters, row_alias, &BTreeMap::new(), &BTreeMap::new())
}

fn relation_tables(expression: Option<&LiveExpression>) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    if let Some(expression) = expression {
        if let Some(relation) = &expression.relation {
            result.insert(relation.table.clone());
            result.extend(relation_tables(relation.predicate.as_deref()));
        }
        for child in &expression.children { result.extend(relation_tables(Some(child))); }
    }
    result
}

fn compile_related_expression(
    expression: &LiveExpression, args: &Map<String, Value>, parameters: &mut Vec<Value>, row_alias: &str,
    related: &BTreeMap<String, (VisibilityPlan, ResolvedVisibility)>, context: &BTreeMap<String, String>,
) -> Result<String, LiveQueryError> {
    match expression.operator.as_str() {
        "inRelation" => {
            let relation = expression.relation.as_ref().ok_or_else(|| LiveQueryError::Invalid("inRelation requires a relation".to_owned()))?;
            let (plan, resolved) = related.get(&relation.table).ok_or_else(|| LiveQueryError::VisibilityMissing(relation.table.clone()))?;
            let alias = format!("{row_alias}_related");
            let guard = visibility::compile_predicate(plan, resolved, &alias, parameters)?;
            let predicate = relation.predicate.as_ref().map(|child| compile_related_expression(child, args, parameters, &alias, related, context))
                .transpose()?.unwrap_or_else(|| "TRUE".to_owned());
            return Ok(format!("{row_alias}.{}::text IN (SELECT {alias}.{}::text FROM {} AS {alias} WHERE ({guard}) AND ({predicate}))",
                quote(&expression.column)?, quote(&relation.column)?, quote(&relation.table)?));
        }
        "and" | "or" => {
            if expression.children.is_empty() {
                return Ok("FALSE".to_owned());
            }
            let parts = expression
                .children
                .iter()
                .map(|child| {
                    compile_related_expression(child, args, parameters, row_alias, related, context)
                        .map(|part| format!("({part})"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(format!("({})", parts.join(if expression.operator == "and" {
                " AND "
            } else {
                " OR "
            })));
        }
        "not" => {
            if expression.children.len() != 1 {
                return Err(LiveQueryError::Invalid(
                    "live not expression requires one child".to_owned(),
                ));
            }
            return Ok(format!(
                "NOT ({})",
                compile_related_expression(&expression.children[0], args, parameters, row_alias, related, context)?
            ));
        }
        "server" => {
            return Err(LiveQueryError::Invalid(
                "server-only arbitrary reactive predicates are not supported".to_owned(),
            ));
        }
        _ => {}
    }
    let left = format!("{row_alias}.{}", quote(&expression.column)?);
    let value = if let Some(value) = expression.value.as_ref().filter(|value| !value.context.is_empty()) {
        Value::String(context.get(&value.context).ok_or_else(|| LiveQueryError::Invalid("Missing query identity context".to_owned()))?.clone())
    } else { live_value(expression.value.as_ref(), args) };
    let add = |parameters: &mut Vec<Value>, value: Value| {
        parameters.push(value);
        format!("${}", parameters.len())
    };
    Ok(match expression.operator.as_str() {
        "eq" if value.is_null() => format!("{left} IS NULL"),
        "neq" if value.is_null() => format!("{left} IS NOT NULL"),
        "eq" => format!("{left} = {}", add(parameters, value)),
        "neq" => format!("{left} <> {}", add(parameters, value)),
        "gt" => format!("{left} > {}", add(parameters, value)),
        "gte" => format!("{left} >= {}", add(parameters, value)),
        "lt" => format!("{left} < {}", add(parameters, value)),
        "lte" => format!("{left} <= {}", add(parameters, value)),
        "range" => format!(
            "{left} BETWEEN {} AND {}",
            add(parameters, value),
            add(parameters, live_value(expression.value_to.as_ref(), args))
        ),
        "arrayContains" => format!("COALESCE({left}::jsonb, '[]'::jsonb) @> {}::jsonb", add(parameters, Value::Array(vec![value]))),
        "contains" => format!(
            "strpos(COALESCE({left}::text, ''), {}::text) > 0",
            add(parameters, Value::String(argument_text(Some(&value))))
        ),
        "containsInsensitive" => format!(
            "strpos(lower(COALESCE({left}::text, '')), lower({}::text)) > 0",
            add(parameters, Value::String(argument_text(Some(&value))))
        ),
        "in" => {
            let Value::Array(values) = value else {
                return Ok("FALSE".to_owned());
            };
            if values.is_empty() {
                "FALSE".to_owned()
            } else {
                let values = values
                    .into_iter()
                    .map(|value| add(parameters, value))
                    .collect::<Vec<_>>();
                format!("{left} IN ({})", values.join(", "))
            }
        }
        operator => {
            return Err(LiveQueryError::Invalid(format!(
                "unsupported live query operator {operator:?}"
            )));
        }
    })
}

fn compile_source(plan: &LiveQueryPlan) -> Result<String, LiveQueryError> {
    let source = format!("{} AS r", quote(&plan.table)?);
    match &plan.index {
        Some(index) => Ok(format!("{source} JOIN {} AS g ON g.{} = r.{}::text", quote(&index.table)?, quote(&index.key)?, quote(&plan.key)?)),
        None => Ok(source),
    }
}

fn index_predicate(plan: &LiveQueryPlan, mut sql: String) -> Result<String, LiveQueryError> {
    if let Some(index) = &plan.index {
        // compile_source equates the projected key with the source key's text
        // representation. Keep related membership predicates on that same index
        // so PostgreSQL can filter before joining source rows.
        sql = sql.replace(
            &format!("r.{}::text", quote(&plan.key)?),
            &format!("g.{}", quote(&index.key)?),
        );
        for column in &index.columns {
            let name = quote(column)?;
            sql = sql.replace(&format!("r.{name}"), &format!("g.{name}"));
        }
    }
    Ok(sql)
}

fn compile_order(plan: &LiveQueryPlan, args: &Map<String, Value>) -> Result<String, LiveQueryError> {
    let (column, direction) = sort(plan.sort.as_ref(), args);
    let mut terms = Vec::new();
    if let Some(columns) = plan.index.as_ref().and_then(|index| index.sort_columns.get(&column)) {
        for column in columns { terms.push(format!("g.{} {}", quote(column)?, direction.to_uppercase())); }
    } else if !column.is_empty() {
        terms.push(index_predicate(plan, format!("r.{} {}", quote(&column)?, direction.to_uppercase()))?);
    }
    if column != plan.key { terms.push(format!("r.{} ASC", quote(&plan.key)?)); }
    Ok(terms.join(", "))
}

fn search_terms(input: &str, boolean_terms: bool) -> (Vec<String>, &'static str) {
    let input = input.trim().to_lowercase();
    let operator = if boolean_terms && input.contains("$and") { "$and" } else if boolean_terms && input.contains("$or") { "$or" } else { "" };
    let terms = if operator.is_empty() { vec![input] } else { input.split(operator).map(|term| term.trim().to_owned()).collect() };
    (terms.into_iter().filter(|term| !term.is_empty()).collect(), if operator == "$or" { " OR " } else { " AND " })
}

fn search_pattern(term: &str) -> String {
    format!("%{}%", term.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))
}

#[cfg(test)]
fn compile_search(plan: &LiveQueryPlan, search: &LiveSearch, args: &Map<String, Value>, parameters: &mut Vec<Value>) -> Result<String, LiveQueryError> {
    compile_search_filtered(plan,search,args,parameters,"")
}

fn compile_search_filtered(plan: &LiveQueryPlan, search: &LiveSearch, args: &Map<String, Value>, parameters: &mut Vec<Value>, index_filter:&str) -> Result<String, LiveQueryError> {
    let (terms, joiner) = search_terms(&argument_text(args.get(&search.argument)), search.boolean_terms);
    let mut predicates = Vec::new();
    let mut document_sets=Vec::new();
    let direct_index=plan.index.as_ref().filter(|index| search.sources.iter().any(|source| source.table==index.table && source.key==index.key));
    for term in terms {
        let pattern = push(parameters, Value::String(search_pattern(&term)));
        let mut alternatives = Vec::new();
        for column in &search.columns {
            alternatives.push(format!("r.{}::text ILIKE {pattern} ESCAPE '\\'", quote(column)?));
        }
        if !search.sources.is_empty() {
            // A union of indexed candidate IDs avoids a correlated OR EXISTS
            // that would scan the entire entity table for every search term.
            if let Some(index)=direct_index {
                for source in search.sources.iter().filter(|source|source.table==index.table && source.key==index.key){alternatives.push(format!("g.{} ILIKE {pattern} ESCAPE '\\'",quote(&source.column)?));}
            }
            let sources = search.sources.iter().filter(|source| !direct_index.is_some_and(|index|source.table==index.table && source.key==index.key)).map(|source| {
                let (alias, join, filter) = match &plan.index {
                    Some(index) if !index_filter.is_empty() && index.table == source.table => ("g",String::new(),format!(" AND ({index_filter})")),
                    Some(index) if !index_filter.is_empty() => ("d",format!(" JOIN {} AS g ON g.{} = d.{}",quote(&index.table)?,quote(&index.key)?,quote(&source.key)?),format!(" AND ({index_filter})")),
                    _ => ("d",String::new(),String::new()),
                };
                Ok(format!("SELECT {alias}.{} FROM {} AS {alias}{join} WHERE {alias}.{} ILIKE {pattern} ESCAPE '\\'{filter}",quote(&source.key)?,quote(&source.table)?,quote(&source.column)?))
            }).collect::<Result<Vec<_>, LiveQueryError>>()?;
            if !sources.is_empty(){
                let documents=sources.join(" UNION ");
                if let Some(index)=direct_index {
                    // An InitPlan array lets PostgreSQL combine the document
                    // trigram index with the projection key index, and lets a
                    // broad search stop early on the requested ordering index.
                    alternatives.push(format!("g.{} = ANY(ARRAY({documents}))",quote(&index.key)?));
                } else if search.columns.is_empty(){document_sets.push(format!("({documents})"));}
                else{alternatives.push(format!("r.{}::text IN ({documents})",quote(&plan.key)?));}
            }
        }
        if !alternatives.is_empty() { predicates.push(format!("({})", alternatives.join(" OR "))); }
    }
    if !document_sets.is_empty(){return Ok(format!("r.{}::text IN ({})",quote(&plan.key)?,document_sets.join(if joiner==" OR " {" UNION "} else {" INTERSECT "})));}
    Ok(if predicates.is_empty() { String::new() } else { format!("({})", predicates.join(joiner)) })
}

fn compile_filters(
    definition: &LiveFilters,
    args: &Map<String, Value>,
    parameters: &mut Vec<Value>,
    row_alias: &str,
) -> Result<String, LiveQueryError> {
    let Some(raw) = args.get(&definition.argument) else {
        return Ok(String::new());
    };
    if raw.is_null() {
        return Ok(String::new());
    }
    let filters = raw.as_array().ok_or_else(|| {
        LiveQueryError::Invalid(format!(
            "live query filter argument {:?} must be an array",
            definition.argument
        ))
    })?;
    let allowed_columns = definition
        .allowed_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let allowed_operators = definition
        .allowed_operators
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut predicates = Vec::new();
    for (index, raw) in filters.iter().enumerate() {
        let filter = raw.as_object().ok_or_else(|| {
            LiveQueryError::Invalid(format!("live query filter {index} must be an object"))
        })?;
        let column = filter
            .get("column")
            .and_then(Value::as_str)
            .filter(|column| allowed_columns.contains(column))
            .ok_or_else(|| {
                LiveQueryError::Invalid(format!(
                    "live query filter {index} has an unallowed column"
                ))
            })?;
        let operator = filter
            .get("operator")
            .and_then(Value::as_str)
            .filter(|operator| allowed_operators.contains(operator))
            .ok_or_else(|| {
                LiveQueryError::Invalid(format!(
                    "live query filter {index} has an unallowed operator"
                ))
            })?;
        let value = filter.get("value").and_then(Value::as_str).ok_or_else(|| {
            LiveQueryError::Invalid(format!("live query filter {index} value must be a string"))
        })?;
        let value_to = filter.get("valueTo").and_then(Value::as_str);
        if filter.contains_key("valueTo") && value_to.is_none() {
            return Err(LiveQueryError::Invalid(format!(
                "live query filter {index} valueTo must be a string"
            )));
        }
        let left = format!("{row_alias}.{}", quote(column)?);
        let text = format!("COALESCE({left}::text, '')");
        let column_type = definition
            .column_types
            .get(column)
            .map(String::as_str)
            .unwrap_or("text");
        let ordered_comparison = matches!(
            operator,
            "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange"
        );
        let numeric_comparison = ordered_comparison && column_type == "number";
        let parameter_value = if numeric_comparison {
            parse_filter_number(value, index)?
        } else if matches!(operator, "contains" | "notContains" | "startsWith" | "endsWith") {
            let escaped = value.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
            Value::String(match operator {
                "startsWith" => format!("{escaped}%"),
                "endsWith" => format!("%{escaped}"),
                _ => format!("%{escaped}%"),
            })
        } else {
            Value::String(value.to_owned())
        };
        let value_arg = if matches!(operator, "empty" | "notEmpty" | "oneOf") {
            String::new()
        } else {
            push(parameters, parameter_value)
        };
        let comparison_left = if column_type == "number" {
            &left
        } else {
            &text
        };
        let predicate = match operator {
            "contains" | "startsWith" | "endsWith" if value.is_empty() => format!("{text} ILIKE {value_arg} ESCAPE '\\'"),
            "notContains" if value.is_empty() => format!("{text} NOT ILIKE {value_arg} ESCAPE '\\'"),
            "contains" => format!("{left}::text ILIKE {value_arg} ESCAPE '\\'"),
            "notContains" => {
                format!("({left} IS NULL OR {left}::text NOT ILIKE {value_arg} ESCAPE '\\')")
            }
            "equals" if !value.is_empty() => format!("{left}::text = {value_arg}::text"),
            "equals" => format!("{text} = {value_arg}::text"),
            "notEquals" => format!("{text} <> {value_arg}::text"),
            "startsWith" | "endsWith" => format!("{left}::text ILIKE {value_arg} ESCAPE '\\'"),
            "empty" => format!("{text} = ''"),
            "notEmpty" => format!("{text} <> ''"),
            "oneOf" => {
                let values: Vec<Value> = serde_json::from_str(value).map_err(|_| {
                    LiveQueryError::Invalid(format!(
                        "live query filter {index} oneOf value must be a JSON array"
                    ))
                })?;
                if values.is_empty() {
                    "FALSE".to_owned()
                } else {
                    let includes_empty = values.iter().any(|value| argument_text(Some(value)).is_empty());
                    let values = values
                        .into_iter()
                        .map(|value| push(parameters, Value::String(argument_text(Some(&value)))))
                        .collect::<Vec<_>>();
                    let expression = if includes_empty { text.clone() } else { format!("{left}::text") };
                    format!("{expression} IN ({})", values.join(", "))
                }
            }
            "lessThan" => format!("{comparison_left} < {value_arg}"),
            "lessThanOrEqual" => format!("{comparison_left} <= {value_arg}"),
            "greaterThan" => format!("{comparison_left} > {value_arg}"),
            "greaterThanOrEqual" => format!("{comparison_left} >= {value_arg}"),
            "inRange" => {
                let value_to = value_to.ok_or_else(|| {
                    LiveQueryError::Invalid(format!(
                        "live query filter {index} inRange requires valueTo"
                    ))
                })?;
                let value_to = if column_type == "number" {
                    parse_filter_number(value_to, index)?
                } else {
                    Value::String(value_to.to_owned())
                };
                format!(
                    "{comparison_left} BETWEEN {value_arg} AND {}",
                    push(parameters, value_to)
                )
            }
            _ => {
                return Err(LiveQueryError::Invalid(format!(
                    "live query filter {index} has an unsupported operator"
                )));
            }
        };
        predicates.push(format!("({predicate})"));
    }
    Ok(predicates.join(" AND "))
}

fn parse_filter_number(value: &str, index: usize) -> Result<Value, LiveQueryError> {
    if let Ok(value) = value.parse::<i64>() {
        return Ok(Value::from(value));
    }
    let value = value
        .parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64);
    value.map(Value::Number).ok_or_else(|| {
        LiveQueryError::Invalid(format!(
            "live query filter {index} requires a finite numeric value"
        ))
    })
}

fn sort(definition: Option<&LiveSort>, args: &Map<String, Value>) -> (String, String) {
    let Some(definition) = definition else {
        return (String::new(), String::new());
    };
    let column = args
        .get(&definition.column_argument)
        .and_then(Value::as_str)
        .filter(|value| {
            definition
                .allowed_columns
                .iter()
                .any(|column| column == value)
        })
        .unwrap_or(&definition.default_column)
        .to_owned();
    let direction = args
        .get(&definition.direction_argument)
        .and_then(Value::as_str)
        .map(str::to_lowercase)
        .filter(|value| value == "asc" || value == "desc")
        .unwrap_or_else(|| definition.default_direction.clone());
    (column, direction)
}

fn window(definition: Option<&LiveWindow>, args: &Map<String, Value>) -> (usize, usize) {
    let Some(definition) = definition else {
        return (0, 0);
    };
    let offset = nonnegative(args.get(&definition.offset_argument)).unwrap_or(0);
    let limit = nonnegative(args.get(&definition.limit_argument))
        .filter(|limit| *limit > 0)
        .unwrap_or(definition.default_limit)
        .min(definition.max_limit);
    (offset, limit)
}

fn shape_result(
    rows: Vec<Value>,
    path: &[String],
    total: Option<u64>,
    offset: usize,
    limit: usize,
) -> Value {
    if path.is_empty() {
        return Value::Array(rows);
    }
    let mut leaf = Map::new();
    leaf.insert(path[path.len() - 1].clone(), Value::Array(rows));
    if let Some(total) = total {
        leaf.insert("total".to_owned(), Value::from(total));
        leaf.insert("offset".to_owned(), Value::from(offset as u64));
        leaf.insert("limit".to_owned(), Value::from(limit as u64));
    }
    let mut value = Value::Object(leaf);
    for segment in path[..path.len() - 1].iter().rev() {
        value = Value::Object(Map::from_iter([(segment.clone(), value)]));
    }
    value
}

fn live_value(value: Option<&LiveValue>, args: &Map<String, Value>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    if !value.argument.is_empty() {
        args.get(&value.argument).cloned().unwrap_or(Value::Null)
    } else {
        value.literal.clone()
    }
}

fn nonnegative(value: Option<&Value>) -> Option<usize> {
    match value {
        Some(Value::Number(value)) => value.as_u64().and_then(|value| usize::try_from(value).ok()),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
}

fn argument_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
    }
}

fn push(parameters: &mut Vec<Value>, value: Value) -> String {
    parameters.push(value);
    format!("${}", parameters.len())
}

fn validate_expression(expression: &LiveExpression) -> Result<(), LiveQueryError> {
    for value in [expression.value.as_ref(), expression.value_to.as_ref()].into_iter().flatten() {
        if !value.context.is_empty() && !["account.id", "member.id", "tenant.id"].contains(&value.context.as_str()) {
            return Err(LiveQueryError::Invalid("Invalid query identity context".to_owned()));
        }
    }
    if expression.operator == "inRelation" {
        quote(&expression.column)?;
        let relation = expression.relation.as_ref().ok_or_else(|| LiveQueryError::Invalid("inRelation requires a relation".to_owned()))?;
        quote(&relation.table)?; quote(&relation.column)?;
        if let Some(child) = &relation.predicate { validate_expression(child)?; }
        return Ok(());
    }
    match expression.operator.as_str() {
        "and" | "or" if expression.children.is_empty() => {
            return Err(LiveQueryError::Invalid(format!(
                "{} requires children",
                expression.operator
            )));
        }
        "and" | "or" => {}
        "not" if expression.children.len() == 1 => {}
        "not" => {
            return Err(LiveQueryError::Invalid("not requires one child".to_owned()));
        }
        "server" => {
            return Err(LiveQueryError::Invalid(
                "server-only arbitrary reactive predicates are not supported".to_owned(),
            ));
        }
        "eq"
        | "neq"
        | "gt"
        | "gte"
        | "lt"
        | "lte"
        | "range"
        | "in"
        | "contains"
        | "containsInsensitive"
        | "arrayContains" => {
            quote(&expression.column)?;
        }
        operator => {
            return Err(LiveQueryError::Invalid(format!(
                "unsupported live query operator {operator:?}"
            )));
        }
    }
    for child in &expression.children {
        validate_expression(child)?;
    }
    Ok(())
}

fn valid_filter_operator(operator: &str) -> bool {
    matches!(
        operator,
        "contains"
            | "notContains"
            | "equals"
            | "notEquals"
            | "startsWith"
            | "endsWith"
            | "empty"
            | "notEmpty"
            | "oneOf"
            | "lessThan"
            | "lessThanOrEqual"
            | "greaterThan"
            | "greaterThanOrEqual"
            | "inRange"
    )
}

fn quote(value: &str) -> Result<String, LiveQueryError> {
    if value.is_empty()
        || value.len() > 128
        || !value.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        })
    {
        return Err(LiveQueryError::Invalid(format!(
            "SQL identifier {value:?} is invalid"
        )));
    }
    Ok(format!("\"{value}\""))
}

fn require_name(value: &str, label: &str) -> Result<(), LiveQueryError> {
    if value.trim().is_empty() {
        Err(LiveQueryError::Invalid(format!("{label} is required")))
    } else {
        Ok(())
    }
}

fn bind_scalar<'query>(
    query: sqlx::query::QueryScalar<'query, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    value: &Value,
) -> Result<
    sqlx::query::QueryScalar<'query, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    LiveQueryError,
> {
    Ok(match value {
        Value::Null => query.bind(Option::<String>::None),
        Value::Bool(value) => query.bind(*value),
        Value::String(value) => query.bind(value.clone()),
        Value::Number(value) if value.is_i64() => query.bind(value.as_i64().unwrap_or_default()),
        Value::Number(value) if value.is_u64() => query.bind(
            i64::try_from(value.as_u64().unwrap_or_default())
                .map_err(|_| LiveQueryError::Invalid("integer exceeds bigint".to_owned()))?,
        ),
        Value::Number(value) => query.bind(
            value
                .as_f64()
                .ok_or_else(|| LiveQueryError::Invalid("number is not finite".to_owned()))?,
        ),
        Value::Array(_) | Value::Object(_) => query.bind(sqlx::types::Json(value.clone())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_change(table: &str, changed_columns: &[&str]) -> LogChange {
        LogChange {
            revision: 2,
            ordinal: 0,
            origin_command_id: String::new(),
            table: table.to_owned(),
            row_id: "1".to_owned(),
            operation: "update".to_owned(),
            changed_columns: changed_columns
                .iter()
                .map(|column| (*column).to_owned())
                .collect(),
            old_value: Value::Null,
            new_value: Value::Null,
            provenance: Default::default(),
        }
    }

    #[test]
    fn live_query_ignores_unreferenced_visibility_dependency_columns() {
        let visibility: VisibilityPlan = serde_json::from_value(serde_json::json!({
            "table": "taskComments",
            "key": "id",
            "sets": {
                "createdTasks": {
                    "table": "tasks",
                    "select": "id",
                    "joins": [],
                    "where": [
                        {"column": "createdBy", "context": "member.id"}
                    ]
                }
            },
            "where": {"operator": "inSet", "column": "taskId", "set": "createdTasks"}
        }))
        .unwrap();
        let dependencies = visibility.dependency_columns();

        assert!(!change_affects_live_query(
            "taskComments",
            &dependencies,
            &feed_change("tasks", &["name"]),
        ));
        assert!(change_affects_live_query(
            "taskComments",
            &dependencies,
            &feed_change("tasks", &["createdBy"]),
        ));
        assert!(change_affects_live_query(
            "taskComments",
            &dependencies,
            &feed_change("taskComments", &["body"]),
        ));
    }

    #[test]
    fn explicit_null_literal_is_preserved() {
        let plan: LiveQueryPlan = serde_json::from_value(serde_json::json!({
            "table": "tasks",
            "key": "id",
            "columns": ["id"],
            "where": {"operator":"eq","column":"deletedAt","value":{"literal":null}}
        }))
        .unwrap();
        assert_eq!(plan.predicate.unwrap().value.unwrap().literal, Value::Null);
    }

    #[test]
    fn related_scopes_compile_as_guarded_subqueries_and_track_changes() {
        let expression: LiveExpression = serde_json::from_value(serde_json::json!({
            "operator":"inRelation", "column":"id", "relation": {
                "table":"routing", "column":"itemId", "where": {"operator":"and", "children":[
                    {"operator":"eq", "column":"space", "value":{"argument":"space"}},
                    {"operator":"eq", "column":"member", "value":{"context":"member.id"}},
                    {"operator":"eq", "column":"revoked", "value":{"literal":null}}
                ]}
            }
        })).unwrap();
        validate_expression(&expression).unwrap();
        let plan: VisibilityPlan = serde_json::from_value(serde_json::json!({
            "table":"routing", "key":"id", "where":{"operator":"eqContext","column":"owner","context":"member.id"}
        })).unwrap();
        let resolved = ResolvedVisibility { revision: 1, direct: BTreeMap::from([("member.id".to_owned(),"member-a".to_owned())]), role:"member".to_owned(), permissions:Value::Null, sets:BTreeMap::new(), fingerprint:"scope-a".to_owned() };
        let related = BTreeMap::from([("routing".to_owned(), (plan, resolved.clone()))]);
        let args = serde_json::json!({"space":"inbox", "member.id":"attacker"});
        let mut parameters = Vec::new();
        let sql = compile_related_expression(&expression, args.as_object().unwrap(), &mut parameters, "r", &related, &resolved.direct).unwrap();
        assert!(sql.contains("IN (SELECT r_related.\"itemId\"::text FROM \"routing\" AS r_related"), "{sql}");
        assert!(sql.contains("r_related.\"owner\""), "Related-table visibility must be injected: {sql}");
        assert!(sql.contains("r_related.\"revoked\" IS NULL"));
        assert!(parameters.contains(&Value::String("member-a".to_owned())));
        assert!(!parameters.contains(&Value::String("attacker".to_owned())));
        assert_eq!(relation_tables(Some(&expression)), BTreeSet::from(["routing".to_owned()]));
        assert!(matches!(compile_related_expression(&expression, args.as_object().unwrap(), &mut Vec::new(), "r", &BTreeMap::new(), &resolved.direct), Err(LiveQueryError::VisibilityMissing(_))));
    }

    #[test]
    fn related_scopes_reject_unsafe_identifiers_and_unknown_contexts() {
        for expression in [
            serde_json::json!({"operator":"inRelation","column":"id","relation":{"table":"routing; DROP TABLE items","column":"itemId"}}),
            serde_json::json!({"operator":"eq","column":"owner","value":{"context":"client.member"}}),
            serde_json::json!({"operator":"inRelation","column":"id"}),
        ] { assert!(validate_expression(&serde_json::from_value(expression).unwrap()).is_err()); }
    }

    #[test]
    fn null_equality_compiles_to_is_null_without_a_parameter() {
        let expression = LiveExpression {
            operator: "eq".to_owned(),
            column: "deletedAt".to_owned(),
            value: Some(LiveValue {
                argument: String::new(),
                context: String::new(),
                literal: Value::Null,
            }),
            value_to: None,
            children: Vec::new(),
            relation: None,
        };
        let mut parameters = Vec::new();
        let sql = compile_expression(&expression, &Map::new(), &mut parameters, "r").unwrap();
        assert_eq!(sql, "r.\"deletedAt\" IS NULL");
        assert!(parameters.is_empty());
    }

    #[test]
    fn ordered_filters_bind_declared_numeric_columns_as_numbers() {
        let filters: LiveFilters = serde_json::from_value(serde_json::json!({
            "argument": "filters",
            "allowedColumns": ["id", "name"],
            "allowedOperators": ["greaterThanOrEqual"],
            "columnTypes": {"id": "number"}
        }))
        .unwrap();
        let args = serde_json::json!({
            "filters": [{"column": "id", "operator": "greaterThanOrEqual", "value": "42"}]
        });
        let mut parameters = Vec::new();

        let sql =
            compile_filters(&filters, args.as_object().unwrap(), &mut parameters, "r").unwrap();

        assert_eq!(sql, "(r.\"id\" >= $1)");
        assert_eq!(parameters, vec![Value::from(42)]);
    }

    #[test]
    fn ordered_filters_default_to_explicit_text_comparisons() {
        let filters: LiveFilters = serde_json::from_value(serde_json::json!({
            "argument": "filters",
            "allowedColumns": ["name"],
            "allowedOperators": ["lessThan"]
        }))
        .unwrap();
        let args = serde_json::json!({
            "filters": [{"column": "name", "operator": "lessThan", "value": "M"}]
        });
        let mut parameters = Vec::new();

        let sql =
            compile_filters(&filters, args.as_object().unwrap(), &mut parameters, "r").unwrap();

        assert_eq!(sql, "(COALESCE(r.\"name\"::text, '') < $1)");
        assert_eq!(parameters, vec![Value::String("M".to_owned())]);
    }

    #[test]
    fn rejects_arbitrary_server_predicates_and_invalid_columns() {
        let mut plan = LiveQueryPlan {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            columns: vec!["id".to_owned()],
            result_path: Vec::new(),
            predicate: Some(LiveExpression {
                operator: "server".to_owned(),
                column: String::new(),
                value: None,
                value_to: None,
                children: Vec::new(),
            relation: None,
            }),
            search: None,
            filters: None,
            sort: None,
            window: None,
            server_only: false,
            index: None,
        };
        assert!(plan.validate().is_err());
        plan.predicate = None;
        plan.columns.push("id; DROP TABLE tasks".to_owned());
        assert!(plan.validate().is_err());
    }

    #[test]
    fn indexed_search_preserves_phrases_boolean_precedence_and_literal_wildcards() {
        assert_eq!(search_terms(" Status $AND high $or low ", true), (vec!["status".to_owned(), "high $or low".to_owned()], " AND "));
        assert_eq!(search_terms("hello world", true).0, vec!["hello world"]);
        assert_eq!(search_pattern("50%_\\"), "%50\\%\\_\\\\%");
        let plan: LiveQueryPlan = serde_json::from_value(serde_json::json!({
            "table":"items", "key":"_id", "columns":["_id","name"],
            "search":{"argument":"search","columns":[],"booleanTerms":true,"sources":[
                {"table":"itemDocuments","key":"itemId","column":"searchText","dependencies":["labels"]},
                {"table":"itemRelatedDocuments","key":"itemId","column":"searchText"}
            ]},
            "index":{"table":"itemGrid","key":"itemId","columns":["name"],"sortColumns":{"statusId":["statusOrder","statusName"]},"dependencies":["statuses"]},
            "sort":{"defaultColumn":"statusId","defaultDirection":"asc","allowedColumns":["statusId"]}
        })).unwrap();
        plan.validate().unwrap();
        let mut parameters=Vec::new();
        let args=serde_json::json!({"search":"pump $or repair"});
        let sql=compile_search(&plan,plan.search.as_ref().unwrap(),args.as_object().unwrap(),&mut parameters).unwrap();
        assert!(sql.contains(" UNION "));
        assert!(sql.matches(" UNION ").count() >= 3);
        assert!(!sql.contains("strpos"));
        assert!(!sql.contains("EXISTS"));
        assert_eq!(parameters,vec![Value::String("%pump%".into()),Value::String("%repair%".into())]);
        assert_eq!(compile_order(&plan,args.as_object().unwrap()).unwrap(),"g.\"statusOrder\" ASC, g.\"statusName\" ASC, r.\"_id\" ASC");
        assert_eq!(index_predicate(&plan,"r.\"name\" = $1 AND r.\"owner\" = $2".into()).unwrap(),"g.\"name\" = $1 AND r.\"owner\" = $2");
        assert_eq!(index_predicate(&plan, "r.\"_id\"::text IN (SELECT member_id FROM related) OR r.\"name\" = $1".into()).unwrap(), "g.\"itemId\" IN (SELECT member_id FROM related) OR g.\"name\" = $1");
        assert_eq!(index_predicate(&plan, "r.\"_id\" = $1".into()).unwrap(), "r.\"_id\" = $1");
        assert!(plan.index_change_affects("labels"));
        assert!(plan.index_change_affects("statuses"));
        assert!(!plan.index_change_affects("unrelated"));
    }

    #[test]
    fn export_indexed_query_benchmark_statements() {
        let Ok(path)=std::env::var("GONVEX_QUERY_BENCH_SQL") else { return; };
        let plan:LiveQueryPlan=serde_json::from_value(serde_json::json!({
            "table":"tasks","key":"_id","columns":["_id","name"],
            "index":{"table":"taskGridRows","key":"taskId","columns":["workspaceId","priorityId","statusId"],"sortColumns":{"priorityId":["priorityOrder","priorityName","updatedAt"],"statusId":["statusOrder","statusName","updatedAt"]}},
            "search":{"argument":"search","columns":[],"booleanTerms":true,"sources":[{"table":"taskGridRows","key":"taskId","column":"searchText"},{"table":"taskRelatedSearchDocuments","key":"taskId","column":"searchText"}]},
            "sort":{"columnArgument":"sortColumn","directionArgument":"sortDirection","defaultColumn":"id","defaultDirection":"desc","allowedColumns":["id","priorityId","statusId"]}
        })).unwrap();
        let mut output=String::from("SET statement_timeout='20s';\n");
        for (name,args,filter) in [
            ("rare",serde_json::json!({"search":"needle"}),"TRUE"),
            ("and_priority",serde_json::json!({"search":"pump $and needle","sortColumn":"priorityId","sortDirection":"asc"}),"g.\"workspaceId\" = 'workspace-0' AND g.\"statusId\" = 'new'"),
            ("or_status",serde_json::json!({"search":"needle tag $or needle custom","sortColumn":"statusId","sortDirection":"asc"}),"TRUE"),
            ("common_filtered",serde_json::json!({"search":"inspection","sortColumn":"priorityId","sortDirection":"asc"}),"g.\"workspaceId\" = 'workspace-1' AND g.\"statusId\" = 'progress'"),
            ("no_search_status",serde_json::json!({"sortColumn":"statusId","sortDirection":"asc"}),"g.\"workspaceId\" = 'workspace-1'"),
        ] {
            let mut parameters=Vec::new();
            let search=compile_search_filtered(&plan,plan.search.as_ref().unwrap(),args.as_object().unwrap(),&mut parameters,filter).unwrap();
            let predicate=if search.is_empty(){filter.to_owned()}else{format!("{filter} AND {search}")};
            let source=compile_source(&plan).unwrap();
            let order=compile_order(&plan,args.as_object().unwrap()).unwrap();
            for (kind,mut sql) in [
                ("window",format!("SELECT r.\"_id\",r.name FROM {source} WHERE {predicate} ORDER BY {order} LIMIT 100")),
                ("count",format!("SELECT count(*) FROM {source} WHERE {predicate}")),
            ] {
                for (index,value) in parameters.iter().enumerate().rev(){sql=sql.replace(&format!("${}",index+1),&format!("'{}'",value.as_str().unwrap().replace('\'',"''")));}
                for run in 0..3 {output.push_str(&format!("SELECT 'CASE:{name}:{kind}:{run}';\nEXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql};\n"));}
            }
        }
        std::fs::write(path,output).unwrap();
    }

    /// Export application-supplied plans for disposable PostgreSQL benchmarks.
    /// The fixture supplies only public related-table visibility: this measures
    /// indexed filtering/search/order, not tenant admission or visibility cost.
    #[test]
    fn related_scope_disjunction_stays_grouped_with_filters_and_visibility() {
        let expression: LiveExpression = serde_json::from_value(serde_json::json!({
            "operator": "or", "children": [
                {"operator": "eq", "column": "workspaceId", "value": {"literal": "workspace-a"}},
                {"operator": "eq", "column": "workspaceId", "value": {"literal": "workspace-b"}}
            ]
        })).unwrap();
        let sql = compile_related_expression(&expression, &Map::new(), &mut Vec::new(), "r", &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(format!("visible AND {sql} AND matches_search"),
            "visible AND ((r.\"workspaceId\" = $1) OR (r.\"workspaceId\" = $2)) AND matches_search");
    }

    #[test]
    fn export_external_live_query_benchmark_statements() {
        let Ok(fixture_path) = std::env::var("GONVEX_QUERY_BENCH_FIXTURE") else { return; };
        let fixture: Value = serde_json::from_slice(&std::fs::read(fixture_path).unwrap()).unwrap();
        let plan: LiveQueryPlan = serde_json::from_value(fixture["plan"].clone()).unwrap();
        plan.validate().unwrap();
        let context: BTreeMap<String, String> = serde_json::from_value(fixture["context"].clone()).unwrap();
        let related = relation_tables(plan.predicate.as_ref()).into_iter().map(|table| {
            let guard: VisibilityPlan = serde_json::from_value(serde_json::json!({
                "table": table, "key": "_id", "where": {"operator": "public"}
            })).unwrap();
            let resolved = ResolvedVisibility { revision: 1, direct: context.clone(), role: "member".into(),
                permissions: Value::Null, sets: BTreeMap::new(), fingerprint: "benchmark".into() };
            (table, (guard, resolved))
        }).collect::<BTreeMap<_, _>>();
        let mut output = String::from("SET statement_timeout='20s';\nSET max_parallel_workers_per_gather=0;\n");
        for case in fixture["cases"].as_array().unwrap() {
            let args = case["args"].as_object().unwrap();
            let name = case["name"].as_str().unwrap();
            assert!(name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
            let mut parameters = Vec::new();
            let mut predicates = Vec::new();
            if let Some(expression) = &plan.predicate {
                predicates.push(index_predicate(&plan, compile_related_expression(expression, args, &mut parameters, "r", &related, &context).unwrap()).unwrap());
            }
            if let Some(filters) = &plan.filters {
                let predicate = compile_filters(filters, args, &mut parameters, "r").unwrap();
                if !predicate.is_empty() { predicates.push(index_predicate(&plan, predicate).unwrap()); }
            }
            if let Some(search) = &plan.search {
                let index_filter = predicates.iter().filter(|predicate| predicate.contains("g.") && !predicate.contains("r.") && !predicate.contains("SELECT ")).cloned().collect::<Vec<_>>().join(" AND ");
                let predicate = compile_search_filtered(&plan, search, args, &mut parameters, &index_filter).unwrap();
                if !predicate.is_empty() { predicates.push(predicate); }
            }
            let predicate = if predicates.is_empty() { "TRUE".into() } else { predicates.join(" AND ") };
            let source = compile_source(&plan).unwrap();
            let order = compile_order(&plan, args).unwrap();
            for (kind, mut sql) in [
                ("window", format!("SELECT r.{} FROM {source} WHERE {predicate} ORDER BY {order} LIMIT 100", quote(&plan.key).unwrap())),
                ("count", format!("SELECT count(*) FROM {source} WHERE {predicate}")),
            ] {
                for (index, value) in parameters.iter().enumerate().rev() {
                    let literal = match value {
                        Value::Null => "NULL".into(),
                        Value::Bool(_) | Value::Number(_) => value.to_string(),
                        _ => format!("'{}'", value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string()).replace('\'', "''")),
                    };
                    sql = sql.replace(&format!("${}", index + 1), &literal);
                }
                for run in 0..3 { output.push_str(&format!("SELECT 'CASE:{name}:{kind}:{run}';\nEXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql};\n")); }
            }
        }
        std::fs::write(fixture["output"].as_str().unwrap(), output).unwrap();
    }

    #[test]
    fn shapes_exact_windows_at_result_path() {
        assert_eq!(
            shape_result(
                vec![serde_json::json!({"id":"task-1"})],
                &["page".to_owned(), "rows".to_owned()],
                Some(10),
                2,
                3,
            ),
            serde_json::json!({"page":{"rows":[{"id":"task-1"}],"total":10,"offset":2,"limit":3}})
        );
    }
}
