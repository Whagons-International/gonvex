//! Replica Collection snapshots, resumable membership, and transaction routing.

use std::collections::{BTreeMap, BTreeSet};

use gonvex_postgres::{TenantSession, TenantTransaction};
use gonvex_protocol::{
    PublicInvocationProvenance, ReplicaChange, ReplicaCursor, ReplicaOpenRequest, ServerMessage,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use thiserror::Error;

use crate::change_feed::{FeedEvent, LogChange, ReplicaClock};
use crate::host_calls::bind_value;
use crate::modules::ReplicaCollectionDefinition;
use crate::visibility::{self, ResolvedVisibility, VisibilityPlan};
use crate::Runtime;

#[derive(Debug, Error)]
pub enum ReplicaError {
    #[error("project has no active TypeScript module")]
    ModuleMissing,
    #[error("replica function {0:?} is not registered")]
    FunctionMissing(String),
    #[error("visibility plan required for table {0:?}")]
    VisibilityMissing(String),
    #[error("replica arguments must be an object")]
    InvalidArguments,
    #[error("replica row is missing key {0:?}")]
    MissingKey(String),
    #[error("replica returned duplicate key {0:?}")]
    DuplicateKey(String),
    #[error("replica SQL identifier {0:?} is invalid")]
    InvalidIdentifier(String),
    #[error(transparent)]
    Database(#[from] gonvex_postgres::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Visibility(#[from] visibility::VisibilityError),
}

pub struct ReplicaOpenResult {
    pub messages: Vec<ServerMessage>,
    pub subscription: Option<ReplicaSubscription>,
}

pub struct ReplicaSubscription {
    pub id: String,
    pub path: String,
    pub args: Value,
    pub definition: ReplicaCollectionDefinition,
    pub visibility: VisibilityPlan,
    pub visibility_scope: String,
    pub cursor: ReplicaCursor,
    pub rows: BTreeMap<String, Value>,
    pub hashes: BTreeMap<String, String>,
    pub truncated: bool,
    pub resolved: ResolvedVisibility,
}

struct Snapshot {
    rows: Vec<Value>,
    by_key: BTreeMap<String, Value>,
    hashes: BTreeMap<String, String>,
    truncated: bool,
    clock: ReplicaClock,
    resolved: ResolvedVisibility,
}

// Small and normal application collections can carry their integrity map with
// the initial rows. This lets clients persist the cold snapshot once instead of
// hashing and rewriting it again when the immediately following ready arrives.
// Large replicas keep the digest/need-hashes resume path to bound wire overhead.
const INLINE_SNAPSHOT_HASH_LIMIT: usize = 2_048;

impl Runtime {
    pub async fn open_replica(
        &self,
        session: &TenantSession,
        request: ReplicaOpenRequest,
    ) -> Result<ReplicaOpenResult, ReplicaError> {
        let module = self
            .inner
            .modules
            .project(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let function = module
            .functions
            .get(&request.path)
            .filter(|function| function.kind == "query" && function.delivery == "replica")
            .ok_or_else(|| ReplicaError::FunctionMissing(request.path.clone()))?;
        let definition = function
            .replica
            .clone()
            .ok_or_else(|| ReplicaError::FunctionMissing(request.path.clone()))?;
        let visibility = module
            .visibility
            .get(&definition.table)
            .cloned()
            .ok_or_else(|| ReplicaError::VisibilityMissing(definition.table.clone()))?;
        let directive = module.replica_directive(
            &session.route.tenant_id,
            &session.route.database_url,
            &session.identity.account.id,
            &session.member.permissions,
        );
        let snapshot = self
            .replica_snapshot(session, &definition, &visibility, &request.args)
            .await?;
        let cursor = cursor_for(&snapshot.clock, &definition, &directive.visibility_scope);
        let digest = hashes_digest(&snapshot.hashes);
        let mut messages = Vec::new();
        let resumable = request.cursor.as_ref().is_some_and(|client| {
            client.epoch == cursor.epoch
                && client.revision <= cursor.revision
                && client.revision >= snapshot.clock.retained_revision
        });
        if resumable {
            if !request.digest.as_deref().unwrap_or_default().is_empty()
                && !request.full_integrity
                && request.hashes.is_empty()
            {
                if request.digest.as_deref() != Some(digest.as_str()) {
                    messages.push(ServerMessage::ReplicaNeedHashes {
                        id: request.id,
                        path: Some(request.path),
                    });
                    return Ok(ReplicaOpenResult {
                        messages,
                        subscription: None,
                    });
                }
            } else {
                let (upserts, deleted) = diff_rows(
                    &snapshot.by_key,
                    &snapshot.hashes,
                    &request.hashes,
                    &request.keys,
                );
                if !upserts.is_empty()
                    || !deleted.is_empty()
                    || request.cursor.as_ref() != Some(&cursor)
                {
                    messages.push(replica_delta(
                        &request.id,
                        &request.path,
                        cursor.clone(),
                        upserts,
                        deleted,
                        digest.clone(),
                        snapshot.truncated,
                    ));
                }
            }
        } else {
            messages.push(replica_snapshot_message(
                &request,
                &definition,
                &snapshot,
                cursor.clone(),
            ));
        }
        messages.push(replica_ready(
            &request.id,
            &request.path,
            &definition,
            cursor.clone(),
            digest,
            snapshot.truncated,
        ));
        Ok(ReplicaOpenResult {
            messages,
            subscription: Some(ReplicaSubscription {
                id: request.id,
                path: request.path,
                args: request.args,
                definition,
                visibility,
                visibility_scope: directive.visibility_scope,
                cursor,
                rows: snapshot.by_key,
                hashes: snapshot.hashes,
                truncated: snapshot.truncated,
                resolved: snapshot.resolved,
            }),
        })
    }

    async fn replica_snapshot(
        &self,
        session: &TenantSession,
        definition: &ReplicaCollectionDefinition,
        plan: &VisibilityPlan,
        args: &Value,
    ) -> Result<Snapshot, ReplicaError> {
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut transaction = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        let resolved = visibility::resolve(&mut transaction, session, plan).await?;
        let clock = clock_in_transaction(&mut transaction).await?;
        let rows = query_rows(&mut transaction, definition, plan, &resolved, args).await?;
        transaction.commit().await?;
        let (rows, truncated) = apply_budgets(rows, definition)?;
        let mut by_key = BTreeMap::new();
        let mut hashes = BTreeMap::new();
        for row in &rows {
            let key = row_key(row, &definition.key)?;
            if by_key.insert(key.clone(), row.clone()).is_some() {
                return Err(ReplicaError::DuplicateKey(key));
            }
            hashes.insert(key, row_hash(row));
        }
        Ok(Snapshot {
            rows,
            by_key,
            hashes,
            truncated,
            clock,
            resolved,
        })
    }

    pub async fn apply_feed_event(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: FeedEvent,
    ) -> Result<Vec<ServerMessage>, ReplicaError> {
        let FeedEvent::Transaction {
            database_epoch,
            revision,
            changes,
        } = event
        else {
            return Ok(subscriptions
                .values()
                .map(|subscription| ServerMessage::ReplicaReset {
                    id: subscription.id.clone(),
                    path: Some(subscription.path.clone()),
                    reason: "change-feed-reset".to_owned(),
                })
                .collect());
        };
        let mut messages = Vec::new();
        let mut entity_changes = BTreeMap::<(String, String), ReplicaChange>::new();
        for subscription in subscriptions.values_mut() {
            if revision <= subscription.cursor.revision {
                continue;
            }
            let dependencies = subscription.visibility.dependency_columns();
            let (source_changed, dependency_changed) = table_change_relevance(
                &subscription.definition.table,
                &dependencies,
                changes.iter(),
            );
            // Budgets and progressive delivery only make a source-table or
            // visibility-dependency change window-sensitive. Re-running every
            // bounded collection for every unrelated transaction made one task
            // update queue full snapshots for the entire application replica,
            // leaving the changed task stale behind minutes of irrelevant SQL.
            if !source_changed && !dependency_changed {
                // Keep the server-side subscription cursor current for resume
                // bookkeeping, but do not emit one `replica.ready` per open
                // collection. The rows and visibility inputs did not change,
                // so the collection remains authoritative at its existing
                // client cursor. Per-collection ready fan-out made each client
                // re-hash and persist every window for unrelated presence,
                // preference, and notification transactions.
                subscription.cursor.revision = revision;
                continue;
            }
            let source_changes = changes
                .iter()
                .filter(|change| change.table == subscription.definition.table)
                .collect::<Vec<_>>();
            if dependency_changed
                || source_update_requires_snapshot(
                    &subscription.rows,
                    subscription.truncated,
                    &subscription.definition,
                    &subscription.args,
                    &subscription.visibility,
                    &subscription.resolved,
                    source_changes.iter().copied(),
                )?
            {
                let snapshot = self
                    .replica_snapshot(
                        session,
                        &subscription.definition,
                        &subscription.visibility,
                        &subscription.args,
                    )
                    .await?;
                let (upserts, deleted) = diff_rows(
                    &snapshot.by_key,
                    &snapshot.hashes,
                    &subscription.hashes,
                    &subscription.rows.keys().cloned().collect::<Vec<_>>(),
                );
                for row in &upserts {
                    let key = row_key(row, &subscription.definition.key)?;
                    let existed = subscription.rows.contains_key(&key);
                    entity_changes.insert(
                        (subscription.definition.table.clone(), key.clone()),
                        ReplicaChange {
                            entity: subscription.definition.table.clone(),
                            id: key,
                            operation: if existed { "update" } else { "insert" }.to_owned(),
                            old_value: None,
                            new_value: Some(row.clone()),
                            changed_columns: Vec::new(),
                        },
                    );
                }
                for key in &deleted {
                    entity_changes.insert(
                        (subscription.definition.table.clone(), key.clone()),
                        ReplicaChange {
                            entity: subscription.definition.table.clone(),
                            id: key.clone(),
                            operation: "delete".to_owned(),
                            old_value: subscription.rows.get(key).cloned(),
                            new_value: None,
                            changed_columns: Vec::new(),
                        },
                    );
                }
                subscription.rows = snapshot.by_key;
                subscription.hashes = snapshot.hashes;
                subscription.truncated = snapshot.truncated;
                subscription.resolved = snapshot.resolved;
                subscription.cursor.revision = snapshot.clock.revision;
                if !upserts.is_empty() || !deleted.is_empty() {
                    messages.push(replica_delta(
                        &subscription.id,
                        &subscription.path,
                        subscription.cursor.clone(),
                        upserts,
                        deleted,
                        hashes_digest(&subscription.hashes),
                        subscription.truncated,
                    ));
                }
            } else {
                let mut upserts = BTreeMap::new();
                let mut deleted = BTreeSet::new();
                for change in source_changes {
                    let old_visible = row_in_collection(
                        &change.old_value,
                        &subscription.args,
                        &subscription.definition,
                        &subscription.visibility,
                        &subscription.resolved,
                    );
                    let new_visible = row_in_collection(
                        &change.new_value,
                        &subscription.args,
                        &subscription.definition,
                        &subscription.visibility,
                        &subscription.resolved,
                    );
                    let operation = match (old_visible, new_visible) {
                        (true, true) => Some("update"),
                        (false, true) => Some("insert"),
                        (true, false) => Some("delete"),
                        (false, false) => None,
                    };
                    let Some(operation) = operation else {
                        continue;
                    };
                    let old_projection = old_visible
                        .then(|| project_row(&change.old_value, &subscription.definition))
                        .transpose()?;
                    let new_projection = new_visible
                        .then(|| project_row(&change.new_value, &subscription.definition))
                        .transpose()?;
                    if new_visible {
                        let new_projection = new_projection
                            .as_ref()
                            .expect("visible source rows have a projection");
                        subscription
                            .rows
                            .insert(change.row_id.clone(), new_projection.clone());
                        subscription
                            .hashes
                            .insert(change.row_id.clone(), row_hash(new_projection));
                        upserts.insert(change.row_id.clone(), new_projection.clone());
                    } else {
                        subscription.rows.remove(&change.row_id);
                        subscription.hashes.remove(&change.row_id);
                        deleted.insert(change.row_id.clone());
                    }
                    entity_changes.insert(
                        (change.table.clone(), change.row_id.clone()),
                        ReplicaChange {
                            entity: change.table.clone(),
                            id: change.row_id.clone(),
                            operation: operation.to_owned(),
                            old_value: old_projection,
                            new_value: new_projection,
                            changed_columns: change.changed_columns.clone(),
                        },
                    );
                }
                subscription.cursor.revision = revision;
                if !upserts.is_empty() || !deleted.is_empty() {
                    messages.push(replica_delta(
                        &subscription.id,
                        &subscription.path,
                        subscription.cursor.clone(),
                        upserts.into_values().collect(),
                        deleted.into_iter().collect(),
                        hashes_digest(&subscription.hashes),
                        subscription.truncated,
                    ));
                }
            }
            messages.push(replica_ready(
                &subscription.id,
                &subscription.path,
                &subscription.definition,
                subscription.cursor.clone(),
                hashes_digest(&subscription.hashes),
                subscription.truncated,
            ));
        }
        if !entity_changes.is_empty() {
            let origin_command_id = changes.iter().find_map(|change| {
                (!change.origin_command_id.is_empty()).then(|| change.origin_command_id.clone())
            });
            messages.insert(
                0,
                ServerMessage::ReplicaTransaction {
                    cursor: ReplicaCursor {
                        epoch: database_epoch,
                        revision,
                    },
                    origin_command_id,
                    provenance: changes.first().map(|change| PublicInvocationProvenance {
                        root_command_id: change.provenance.root_command_id.clone(),
                        root_channel: Some(change.provenance.root_channel.clone()),
                        channel: change.provenance.channel.clone(),
                        actor_account_id: change.provenance.actor_account_id.clone(),
                        actor_member_id: change.provenance.actor_member_id.clone(),
                        on_behalf_of_member_id: change.provenance.on_behalf_of_member_id.clone(),
                        agent_execution_id: change.provenance.agent_execution_id.clone(),
                    }),
                    changes: entity_changes.into_values().collect(),
                },
            );
        }
        Ok(messages)
    }
}

async fn clock_in_transaction(
    transaction: &mut TenantTransaction,
) -> Result<ReplicaClock, ReplicaError> {
    let row = sqlx::query(
        "SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = true",
    )
    .fetch_one(&mut **transaction.transaction())
    .await?;
    Ok(ReplicaClock {
        epoch: row.get("epoch"),
        revision: row.get::<i64, _>("revision").max(0) as u64,
        retained_revision: row.get::<i64, _>("retained_revision").max(0) as u64,
    })
}

async fn query_rows(
    transaction: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    args: &Value,
) -> Result<Vec<Value>, ReplicaError> {
    let args = args.as_object().ok_or(ReplicaError::InvalidArguments)?;
    let mut parameters = Vec::new();
    let visibility = visibility::compile_predicate(plan, resolved, "r", &mut parameters)?;
    parameters.push(Value::String(resolved.direct["account.id"].clone()));
    let mut predicates = vec![
        visibility,
        format!(
            "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = ${} AND _gonvex_member.status = 'active')",
            parameters.len()
        ),
    ];
    for column in &definition.exclude_when_set {
        predicates.push(format!("r.{} IS NULL", quote(column)?));
    }
    for (column, argument) in &definition.equal_filters {
        let value = args
            .get(argument)
            .ok_or(ReplicaError::InvalidArguments)?
            .clone();
        parameters.push(value);
        predicates.push(format!("r.{} = ${}", quote(column)?, parameters.len()));
    }
    let columns = definition
        .columns
        .iter()
        .map(|column| quote(column).map(|quoted| format!("r.{quoted} AS {quoted}")))
        .collect::<Result<Vec<_>, _>>()?;
    let mut statement = format!(
        "SELECT row_to_json(_gonvex_visible_row)::text FROM (SELECT {} FROM {} AS r WHERE {}",
        columns.join(", "),
        quote(&definition.table)?,
        predicates.join(" AND ")
    );
    if !definition.order_by.is_empty() {
        let direction = if definition.order_direction == "asc" {
            "ASC"
        } else {
            "DESC"
        };
        statement.push_str(&format!(
            " ORDER BY r.{} {direction}",
            quote(&definition.order_by)?
        ));
    }
    if definition.max_rows > 0 {
        parameters.push(Value::from((definition.max_rows + 1) as u64));
        statement.push_str(&format!(" LIMIT ${}", parameters.len()));
    }
    statement.push_str(") AS _gonvex_visible_row");
    let mut query = sqlx::query(&statement);
    for parameter in &parameters {
        query = bind_value(query, parameter).map_err(ReplicaError::InvalidIdentifier)?;
    }
    let rows = query.fetch_all(&mut **transaction.transaction()).await?;
    rows.into_iter()
        .map(|row| {
            let raw: String = row.get(0);
            serde_json::from_str(&raw)
                .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))
        })
        .collect()
}

fn apply_budgets(
    rows: Vec<Value>,
    definition: &ReplicaCollectionDefinition,
) -> Result<(Vec<Value>, bool), ReplicaError> {
    let mut kept = Vec::new();
    let mut bytes = 0i64;
    let mut seen = BTreeSet::new();
    for row in rows {
        let key = row_key(&row, &definition.key)?;
        if !seen.insert(key.clone()) {
            return Err(ReplicaError::DuplicateKey(key));
        }
        if definition.max_rows > 0 && kept.len() >= definition.max_rows {
            return Ok((kept, true));
        }
        let row_bytes = serde_json::to_vec(&row).unwrap_or_default().len() as i64;
        if definition.max_bytes > 0 && bytes + row_bytes > definition.max_bytes {
            return Ok((kept, true));
        }
        bytes += row_bytes;
        kept.push(row);
    }
    Ok((kept, false))
}

fn row_in_collection(
    row: &Value,
    args: &Value,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> bool {
    let (Some(row), Some(args)) = (row.as_object(), args.as_object()) else {
        return false;
    };
    if definition
        .exclude_when_set
        .iter()
        .any(|column| row.get(column).is_some_and(|value| !value.is_null()))
    {
        return false;
    }
    if definition
        .equal_filters
        .iter()
        .any(|(column, argument)| row.get(column) != args.get(argument))
    {
        return false;
    }
    visibility::row_matches(plan, resolved, &Value::Object(row.clone()))
}

/// Returns whether a source-table transaction can no longer be represented by
/// incremental row changes. A complete collection remains complete when the
/// projected post-transaction rows fit its budgets, including for progressive
/// delivery. Once a collection is truncated, its missing rows are unknown and
/// the next source change must rebuild the window from PostgreSQL.
fn source_update_requires_snapshot<'a>(
    rows: &BTreeMap<String, Value>,
    truncated: bool,
    definition: &ReplicaCollectionDefinition,
    args: &Value,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    changes: impl Iterator<Item = &'a LogChange>,
) -> Result<bool, ReplicaError> {
    if truncated {
        return Ok(true);
    }

    // With no budgets, a complete collection can always apply source changes
    // directly. The delivery mode controls initial loading, not correctness
    // of subsequent source-table deltas.
    if definition.max_rows == 0 && definition.max_bytes <= 0 {
        return Ok(false);
    }

    let mut projected = rows.clone();
    for change in changes {
        let old_visible = row_in_collection(&change.old_value, args, definition, plan, resolved);
        let new_visible = row_in_collection(&change.new_value, args, definition, plan, resolved);
        if new_visible {
            projected.insert(
                change.row_id.clone(),
                project_row(&change.new_value, definition)?,
            );
        } else if old_visible || projected.contains_key(&change.row_id) {
            projected.remove(&change.row_id);
        }
    }

    if definition.max_rows > 0 && projected.len() > definition.max_rows {
        return Ok(true);
    }
    if definition.max_bytes > 0 {
        let bytes = projected
            .values()
            .map(|row| serde_json::to_vec(row).unwrap_or_default().len() as i64)
            .sum::<i64>();
        if bytes > definition.max_bytes {
            return Ok(true);
        }
    }
    Ok(false)
}

fn project_row(
    row: &Value,
    definition: &ReplicaCollectionDefinition,
) -> Result<Value, ReplicaError> {
    let object = row.as_object().ok_or(ReplicaError::InvalidArguments)?;
    let mut projected = serde_json::Map::new();
    for column in &definition.columns {
        if let Some(value) = object.get(column) {
            projected.insert(column.clone(), value.clone());
        }
    }
    if !projected.contains_key(&definition.key) {
        return Err(ReplicaError::MissingKey(definition.key.clone()));
    }
    Ok(Value::Object(projected))
}

fn row_key(row: &Value, key: &str) -> Result<String, ReplicaError> {
    match row.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        Some(Value::Bool(value)) => Ok(value.to_string()),
        _ => Err(ReplicaError::MissingKey(key.to_owned())),
    }
}

fn row_hash(row: &Value) -> String {
    hex_digest(stable_json(row).as_bytes())
}

fn hashes_digest(hashes: &BTreeMap<String, String>) -> String {
    let pairs = Value::Array(
        hashes
            .iter()
            .map(|(key, hash)| {
                Value::Array(vec![
                    Value::String(key.clone()),
                    Value::String(hash.clone()),
                ])
            })
            .collect(),
    );
    hex_digest(stable_json(&pairs).as_bytes())
}

fn stable_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value.to_string()
            } else if let Some(value) = value.as_u64() {
                value.to_string()
            } else if let Some(value) = value.as_f64() {
                ryu_js::Buffer::new().format(value).to_owned()
            } else {
                "null".to_owned()
            }
        }
        Value::String(value) => serde_json::to_string(value)
            .unwrap_or_else(|_| "\"\"".to_owned())
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029"),
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(stable_json).collect::<Vec<_>>().join(",")
        ),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|key| format!(
                        "{}:{}",
                        stable_json(&Value::String(key.clone())),
                        stable_json(&values[key])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn table_change_relevance<'a>(
    source: &str,
    dependencies: &visibility::VisibilityDependencies,
    changes: impl Iterator<Item = &'a LogChange>,
) -> (bool, bool) {
    let mut source_changed = false;
    let mut dependency_changed = false;
    for change in changes {
        if change.table == source {
            source_changed = true;
        }
        if dependencies.change_affects(&change.table, &change.operation, &change.changed_columns) {
            dependency_changed = true;
        }
    }
    (source_changed, dependency_changed)
}

fn cursor_for(
    clock: &ReplicaClock,
    _definition: &ReplicaCollectionDefinition,
    _visibility_scope: &str,
) -> ReplicaCursor {
    ReplicaCursor {
        // Transaction frames and every collection window share the durable
        // database epoch. Collection identity and visibility are already
        // isolated by the Local Replica scope/signature, while hashes prove
        // the current row set on resume. Using a per-collection hash here made
        // every normal transaction look like a database replacement to the
        // client and cleared unrelated windows.
        epoch: clock.epoch.clone(),
        revision: clock.revision,
    }
}

fn diff_rows(
    current_rows: &BTreeMap<String, Value>,
    current_hashes: &BTreeMap<String, String>,
    client_hashes: &BTreeMap<String, String>,
    client_keys: &[String],
) -> (Vec<Value>, Vec<String>) {
    let upserts = current_rows
        .iter()
        .filter(|(key, _)| client_hashes.get(*key) != current_hashes.get(*key))
        .map(|(_, row)| row.clone())
        .collect();
    let deleted = client_keys
        .iter()
        .filter(|key| !current_rows.contains_key(*key))
        .cloned()
        .collect();
    (upserts, deleted)
}

fn replica_snapshot_message(
    request: &ReplicaOpenRequest,
    definition: &ReplicaCollectionDefinition,
    snapshot: &Snapshot,
    cursor: ReplicaCursor,
) -> ServerMessage {
    let mut metadata = BTreeMap::from([
        ("path".to_owned(), Value::String(request.path.clone())),
        (
            "orderBy".to_owned(),
            Value::String(definition.order_by.clone()),
        ),
        (
            "orderDirection".to_owned(),
            Value::String(definition.order_direction.clone()),
        ),
        ("mode".to_owned(), Value::String(definition.mode.clone())),
        (
            "maxRows".to_owned(),
            Value::from(definition.max_rows as u64),
        ),
        ("maxBytes".to_owned(), Value::from(definition.max_bytes)),
        ("truncated".to_owned(), Value::Bool(snapshot.truncated)),
    ]);
    if snapshot.hashes.len() <= INLINE_SNAPSHOT_HASH_LIMIT {
        metadata.insert(
            "hashes".to_owned(),
            serde_json::to_value(&snapshot.hashes).unwrap_or_default(),
        );
        metadata.insert(
            "digest".to_owned(),
            Value::String(hashes_digest(&snapshot.hashes)),
        );
    }
    ServerMessage::ReplicaSnapshot {
        id: request.id.clone(),
        result: snapshot.rows.clone(),
        cursor,
        key: definition.key.clone(),
        metadata,
    }
}

fn replica_delta(
    id: &str,
    path: &str,
    cursor: ReplicaCursor,
    upserts: Vec<Value>,
    deleted: Vec<String>,
    digest: String,
    truncated: bool,
) -> ServerMessage {
    ServerMessage::ReplicaDelta {
        id: id.to_owned(),
        cursor,
        payload: BTreeMap::from([
            ("path".to_owned(), Value::String(path.to_owned())),
            ("upserts".to_owned(), Value::Array(upserts)),
            (
                "deleted".to_owned(),
                serde_json::to_value(deleted).unwrap_or_default(),
            ),
            ("originCommandIds".to_owned(), Value::Array(Vec::new())),
            ("digest".to_owned(), Value::String(digest)),
            ("truncated".to_owned(), Value::Bool(truncated)),
        ]),
    }
}

fn replica_ready(
    id: &str,
    path: &str,
    definition: &ReplicaCollectionDefinition,
    cursor: ReplicaCursor,
    digest: String,
    truncated: bool,
) -> ServerMessage {
    ServerMessage::ReplicaReady {
        id: id.to_owned(),
        cursor,
        digest,
        metadata: BTreeMap::from([
            ("path".to_owned(), Value::String(path.to_owned())),
            ("mode".to_owned(), Value::String(definition.mode.clone())),
            ("truncated".to_owned(), Value::Bool(truncated)),
        ]),
    }
}

fn quote(value: &str) -> Result<String, ReplicaError> {
    if !value.is_empty()
        && value.len() <= 63
        && value.chars().enumerate().all(|(index, character)| {
            character.is_ascii_alphabetic()
                || character == '_'
                || (index > 0 && character.is_ascii_digit())
        })
    {
        Ok(format!("\"{value}\""))
    } else {
        Err(ReplicaError::InvalidIdentifier(value.to_owned()))
    }
}

fn hex_digest(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn visibility_with_task_dependency(source: &str) -> VisibilityPlan {
        VisibilityPlan {
            table: source.to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::from([(
                "createdTasks".to_owned(),
                crate::visibility::VisibilitySet {
                    table: "tasks".to_owned(),
                    alias: String::new(),
                    select: "id".to_owned(),
                    select_from: String::new(),
                    joins: Vec::new(),
                    constraints: vec![crate::visibility::VisibilityConstraint {
                        table: String::new(),
                        column: "createdBy".to_owned(),
                        context: "member.id".to_owned(),
                        value: None,
                    }],
                },
            )]),
            predicate: crate::visibility::VisibilityExpression {
                operator: "inSet".to_owned(),
                column: if source == "tasks" { "id" } else { "taskId" }.to_owned(),
                context: String::new(),
                set: "createdTasks".to_owned(),
                value: None,
                children: Vec::new(),
            },
        }
    }

    fn routing_change(table: &str, operation: &str, changed_columns: &[&str]) -> LogChange {
        LogChange {
            revision: 2,
            ordinal: 0,
            origin_command_id: String::new(),
            table: table.to_owned(),
            row_id: "1".to_owned(),
            operation: operation.to_owned(),
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
    fn dependency_updates_only_recompute_visibility_for_referenced_columns() {
        let dependencies = visibility_with_task_dependency("taskComments").dependency_columns();
        let name = routing_change("tasks", "update", &["name"]);
        let created_by = routing_change("tasks", "update", &["createdBy"]);
        let inserted = routing_change("tasks", "insert", &["name"]);

        assert_eq!(
            table_change_relevance("taskComments", &dependencies, std::iter::once(&name)),
            (false, false),
        );
        assert_eq!(
            table_change_relevance("taskComments", &dependencies, std::iter::once(&created_by),),
            (false, true),
        );
        assert_eq!(
            table_change_relevance("taskComments", &dependencies, std::iter::once(&inserted),),
            (false, true),
        );
    }

    #[test]
    fn source_table_can_also_be_a_visibility_dependency() {
        let dependencies = visibility_with_task_dependency("tasks").dependency_columns();
        let name = routing_change("tasks", "update", &["name"]);
        let created_by = routing_change("tasks", "update", &["createdBy"]);

        assert_eq!(
            table_change_relevance("tasks", &dependencies, std::iter::once(&name)),
            (true, false),
        );
        assert_eq!(
            table_change_relevance("tasks", &dependencies, std::iter::once(&created_by)),
            (true, true),
        );
    }

    fn public_visibility() -> (VisibilityPlan, ResolvedVisibility) {
        (
            VisibilityPlan {
                table: "tasks".to_owned(),
                key: "id".to_owned(),
                sets: BTreeMap::new(),
                predicate: crate::visibility::VisibilityExpression {
                    operator: "public".to_owned(),
                    column: String::new(),
                    context: String::new(),
                    set: String::new(),
                    value: None,
                    children: Vec::new(),
                },
            },
            ResolvedVisibility {
                revision: 1,
                direct: BTreeMap::new(),
                role: String::new(),
                permissions: Value::Null,
                sets: BTreeMap::new(),
                fingerprint: String::new(),
            },
        )
    }

    fn source_change(old_value: Value, new_value: Value) -> LogChange {
        LogChange {
            revision: 2,
            ordinal: 0,
            origin_command_id: String::new(),
            table: "tasks".to_owned(),
            row_id: "1".to_owned(),
            operation: "UPDATE".to_owned(),
            changed_columns: vec!["title".to_owned()],
            old_value,
            new_value,
            provenance: Default::default(),
        }
    }

    #[test]
    fn complete_progressive_collection_stays_incremental_within_budgets() -> Result<(), ReplicaError>
    {
        let definition = ReplicaCollectionDefinition {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            columns: vec!["id".to_owned(), "title".to_owned()],
            equal_filters: BTreeMap::new(),
            exclude_when_set: Vec::new(),
            visibility_tables: Vec::new(),
            visibility_plan_hash: String::new(),
            order_by: String::new(),
            order_direction: String::new(),
            mode: "progressive".to_owned(),
            max_rows: 2,
            max_bytes: 1_000,
            retention_ms: 0,
        };
        let (plan, resolved) = public_visibility();
        let old = serde_json::json!({"id": "1", "title": "old"});
        let new = serde_json::json!({"id": "1", "title": "new"});
        let rows = BTreeMap::from([("1".to_owned(), old.clone())]);
        let change = source_change(old, new);

        assert!(!source_update_requires_snapshot(
            &rows,
            false,
            &definition,
            &Value::Object(serde_json::Map::new()),
            &plan,
            &resolved,
            std::iter::once(&change),
        )?);
        Ok(())
    }

    #[test]
    fn incremental_projection_ignores_unrequested_columns_for_delta_and_budget(
    ) -> Result<(), ReplicaError> {
        let definition = ReplicaCollectionDefinition {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            columns: vec!["id".to_owned(), "title".to_owned()],
            equal_filters: BTreeMap::new(),
            exclude_when_set: Vec::new(),
            visibility_tables: Vec::new(),
            visibility_plan_hash: String::new(),
            order_by: String::new(),
            order_direction: String::new(),
            mode: "progressive".to_owned(),
            max_rows: 2,
            max_bytes: 30,
            retention_ms: 0,
        };
        let (plan, resolved) = public_visibility();
        let old = serde_json::json!({"id": "1", "title": "old", "internalNotes": "old"});
        let new = serde_json::json!({
            "id": "1",
            "title": "new",
            "internalNotes": "a source column that is not replicated"
        });
        let rows = BTreeMap::from([("1".to_owned(), project_row(&old, &definition)?)]);
        let change = source_change(old, new.clone());

        assert!(!source_update_requires_snapshot(
            &rows,
            false,
            &definition,
            &Value::Object(serde_json::Map::new()),
            &plan,
            &resolved,
            std::iter::once(&change),
        )?);
        assert_eq!(
            project_row(&new, &definition)?,
            serde_json::json!({"id": "1", "title": "new"}),
        );
        Ok(())
    }

    #[test]
    fn complete_collection_snapshots_when_projected_rows_cross_row_budget(
    ) -> Result<(), ReplicaError> {
        let definition = ReplicaCollectionDefinition {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            columns: vec!["id".to_owned(), "title".to_owned()],
            equal_filters: BTreeMap::new(),
            exclude_when_set: Vec::new(),
            visibility_tables: Vec::new(),
            visibility_plan_hash: String::new(),
            order_by: String::new(),
            order_direction: String::new(),
            mode: "progressive".to_owned(),
            max_rows: 1,
            max_bytes: 1_000,
            retention_ms: 0,
        };
        let (plan, resolved) = public_visibility();
        let old = serde_json::json!({"id": "1", "title": "old"});
        let new = serde_json::json!({"id": "2", "title": "new"});
        let rows = BTreeMap::from([("1".to_owned(), old.clone())]);
        let mut change = source_change(old, new);
        change.row_id = "2".to_owned();

        assert!(source_update_requires_snapshot(
            &rows,
            false,
            &definition,
            &Value::Object(serde_json::Map::new()),
            &plan,
            &resolved,
            std::iter::once(&change),
        )?);
        Ok(())
    }

    #[test]
    fn complete_collection_snapshots_when_projected_rows_cross_byte_budget(
    ) -> Result<(), ReplicaError> {
        let definition = ReplicaCollectionDefinition {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            columns: vec!["id".to_owned(), "title".to_owned()],
            equal_filters: BTreeMap::new(),
            exclude_when_set: Vec::new(),
            visibility_tables: Vec::new(),
            visibility_plan_hash: String::new(),
            order_by: String::new(),
            order_direction: String::new(),
            mode: "progressive".to_owned(),
            max_rows: 2,
            max_bytes: 30,
            retention_ms: 0,
        };
        let (plan, resolved) = public_visibility();
        let old = serde_json::json!({"id": "1", "title": "old"});
        let new = serde_json::json!({"id": "1", "title": "a title too large"});
        let rows = BTreeMap::from([("1".to_owned(), old.clone())]);
        let change = source_change(old, new);

        assert!(source_update_requires_snapshot(
            &rows,
            false,
            &definition,
            &Value::Object(serde_json::Map::new()),
            &plan,
            &resolved,
            std::iter::once(&change),
        )?);
        Ok(())
    }

    #[test]
    fn transition_filter_is_old_and_new_aware() {
        let definition = ReplicaCollectionDefinition {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            columns: vec!["id".to_owned(), "workspaceId".to_owned()],
            equal_filters: BTreeMap::from([("workspaceId".to_owned(), "workspace".to_owned())]),
            exclude_when_set: Vec::new(),
            visibility_tables: Vec::new(),
            visibility_plan_hash: String::new(),
            order_by: String::new(),
            order_direction: String::new(),
            mode: String::new(),
            max_rows: 0,
            max_bytes: 0,
            retention_ms: 0,
        };
        let plan = VisibilityPlan {
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::new(),
            predicate: crate::visibility::VisibilityExpression {
                operator: "public".to_owned(),
                column: String::new(),
                context: String::new(),
                set: String::new(),
                value: None,
                children: Vec::new(),
            },
        };
        let resolved = ResolvedVisibility {
            revision: 1,
            direct: BTreeMap::new(),
            role: String::new(),
            permissions: Value::Null,
            sets: BTreeMap::new(),
            fingerprint: String::new(),
        };
        assert!(row_in_collection(
            &serde_json::json!({"id":"1","workspaceId":"a"}),
            &serde_json::json!({"workspace":"a"}),
            &definition,
            &plan,
            &resolved,
        ));
        assert!(!row_in_collection(
            &serde_json::json!({"id":"1","workspaceId":"b"}),
            &serde_json::json!({"workspace":"a"}),
            &definition,
            &plan,
            &resolved,
        ));

        let cursor = cursor_for(
            &ReplicaClock {
                epoch: "database-epoch".to_owned(),
                revision: 42,
                retained_revision: 0,
            },
            &definition,
            "visibility-scope",
        );
        assert_eq!(cursor.epoch, "database-epoch");
        assert_eq!(cursor.revision, 42);
    }

    #[test]
    fn integrity_hashes_match_the_client_canonical_json_contract() {
        let row = serde_json::json!({
            "z": 1,
            "a": {"é": 2, "\u{2028}": "line", "nested": null},
            "arr": [1.5, true, "x"],
        });
        assert_eq!(
            stable_json(&row),
            "{\"a\":{\"nested\":null,\"é\":2,\"\\u2028\":\"line\"},\"arr\":[1.5,true,\"x\"],\"z\":1}"
        );
        assert_eq!(
            row_hash(&row),
            "edd9d315212b227e52a73002b9c74b74a6199636e46ac150784918a41776b8eb"
        );

        let hashes = BTreeMap::from([
            ("é".to_owned(), "b".to_owned()),
            ("a".to_owned(), "c".to_owned()),
            ("\u{2028}".to_owned(), "d".to_owned()),
        ]);
        assert_eq!(
            hashes_digest(&hashes),
            "6c26345fc47f53ca1caae1985bcb1547d3f6099dfe999b8b58e516086c42bf4f"
        );
    }
}
