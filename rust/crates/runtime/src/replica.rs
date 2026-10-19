//! Replica Collection snapshots, resumable membership, and transaction routing.

use std::collections::{BTreeMap, BTreeSet};

use futures_util::{Stream, StreamExt};
use gonvex_postgres::{TenantSession, TenantTransaction};
use gonvex_protocol::replica_digest::ReplicaSetDigest;
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
    #[error("{0}")]
    PublicSession(String),
    #[error("project has no active TypeScript module")]
    ModuleMissing,
    #[error("replica function {0:?} is not registered")]
    FunctionMissing(String),
    #[error("visibility plan required for table {0:?}")]
    VisibilityMissing(String),
    #[error("sum128-v1 replica integrity was not negotiated")]
    DigestNotNegotiated,
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
    pub aggregate: Option<ReplicaAggregate>,
    pub truncated: bool,
    pub resolved: ResolvedVisibility,
}

impl ReplicaSubscription {
    pub(crate) fn require_legacy_digest(&mut self) {
        if matches!(self.aggregate, Some(ReplicaAggregate::Sum(_))) {
            self.aggregate = Some(ReplicaAggregate::Legacy(aggregate_bytes(&self.hashes)));
        }
    }
}

pub enum ReplicaAggregate {
    Legacy([u8; 32]),
    Sum(Box<ReplicaSetDigest>),
}
impl ReplicaAggregate {
    fn from_hashes(hashes: &BTreeMap<String, String>, sum: bool) -> Self {
        if sum {
            let mut state = Box::<ReplicaSetDigest>::default();
            for hash in hashes.values() {
                state.add(hash).expect("server-generated SHA-256");
            }
            Self::Sum(state)
        } else {
            Self::Legacy(aggregate_bytes(hashes))
        }
    }
    fn change(&mut self, hash: &str, remove: bool) {
        match self {
            Self::Legacy(bytes) => xor_hash(bytes, hash),
            Self::Sum(state) => if remove {
                state.remove(hash)
            } else {
                state.add(hash)
            }
            .expect("server-generated row hash and membership"),
        }
    }
    fn digest(&self) -> String {
        match self {
            Self::Legacy(bytes) => format_aggregate(bytes),
            Self::Sum(state) => state.digest(),
        }
    }
    fn is_sum(&self) -> bool {
        matches!(self, Self::Sum(_))
    }
}

struct Snapshot {
    rows: Vec<Value>,
    by_key: BTreeMap<String, Value>,
    hashes: BTreeMap<String, String>,
    truncated: bool,
    clock: ReplicaClock,
    resolved: ResolvedVisibility,
    resume_changes: Vec<LogChange>,
}

// Small and normal application collections can carry their integrity map with
// the initial rows. This lets clients persist the cold snapshot once instead of
// hashing and rewriting it again when the immediately following ready arrives.
// Large replicas keep the digest/need-hashes resume path to bound wire overhead.
const INLINE_SNAPSHOT_HASH_LIMIT: usize = 2_048;

impl Runtime {
    pub(crate) async fn execute_replica_collection_query(
        &self,
        session: &TenantSession,
        definition: &ReplicaCollectionDefinition,
        visibility: &VisibilityPlan,
        args: &Value,
    ) -> Result<Value, ReplicaError> {
        let snapshot = self
            .replica_snapshot(session, definition, visibility, args)
            .await?;
        Ok(Value::Array(snapshot.rows))
    }

    pub async fn open_replica(
        &self,
        session: &TenantSession,
        request: ReplicaOpenRequest,
    ) -> Result<ReplicaOpenResult, ReplicaError> {
        self.open_replica_negotiated(session, request, false).await
    }

    pub(crate) async fn open_replica_negotiated(
        &self,
        session: &TenantSession,
        request: ReplicaOpenRequest,
        sum_enabled: bool,
    ) -> Result<ReplicaOpenResult, ReplicaError> {
        let sum = requested_sum_digest(&request, sum_enabled)?;
        if session.is_anonymous() {
            return Err(ReplicaError::PublicSession(
                "authenticate with an active tenant before opening a Replica Collection".to_owned(),
            ));
        }
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
        let aggregate = request
            .digest
            .as_deref()
            .is_some_and(|digest| digest.starts_with("xor1:") || sum);
        let mut snapshot = self
            .replica_snapshot_for_resume(
                session,
                &definition,
                &visibility,
                &request.args,
                if aggregate {
                    request.cursor.as_ref().map(|cursor| cursor.revision)
                } else {
                    None
                },
            )
            .await?;
        let cursor = cursor_for(&snapshot.clock, &definition, &directive.visibility_scope);
        let resume_aggregate =
            aggregate.then(|| ReplicaAggregate::from_hashes(&snapshot.hashes, sum));
        let digest = if let Some(aggregate) = &resume_aggregate {
            aggregate.digest()
        } else {
            hashes_digest(&snapshot.hashes)
        };
        let mut messages = Vec::new();
        let resumable = cursor_resumable(
            request.cursor.as_ref(),
            &cursor,
            snapshot.clock.retained_revision,
        );
        if resumable {
            if !request.digest.as_deref().unwrap_or_default().is_empty()
                && !request.full_integrity
                && request.hashes.is_empty()
            {
                let prior_hashes = if !aggregate || request.cursor.as_ref() == Some(&cursor) {
                    Some(snapshot.hashes.clone())
                } else {
                    resume_hashes(
                        &snapshot.hashes,
                        &snapshot.resume_changes,
                        &request.args,
                        &definition,
                        &visibility,
                        &snapshot.resolved,
                    )?
                };
                if let Some(message) =
                    verify_resume_hashes(&request, prior_hashes.as_ref(), aggregate)
                {
                    messages.push(message);
                    return Ok(ReplicaOpenResult {
                        messages,
                        subscription: None,
                    });
                }
                if aggregate {
                    let prior_hashes = prior_hashes.unwrap();
                    let keys = prior_hashes.keys().cloned().collect::<Vec<_>>();
                    let (upserts, deleted) =
                        diff_rows(&snapshot.by_key, &snapshot.hashes, &prior_hashes, &keys);
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
                &mut snapshot,
                cursor.clone(),
                &directive.scope,
                &directive.visibility_scope,
                &digest,
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
                aggregate: resume_aggregate,
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
        self.replica_snapshot_for_resume(session, definition, plan, args, None)
            .await
    }

    async fn replica_snapshot_for_resume(
        &self,
        session: &TenantSession,
        definition: &ReplicaCollectionDefinition,
        plan: &VisibilityPlan,
        args: &Value,
        after: Option<u64>,
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
        let clock = clock_in_transaction(&mut transaction).await?;
        let resolved = if definition.max_rows > 0 || definition.max_bytes > 0 {
            visibility::resolve_for_query(&mut transaction, session, plan, clock.revision).await?
        } else {
            visibility::resolve(&mut transaction, session, plan, clock.revision).await?
        };
        let (rows, truncated) =
            query_rows(&mut transaction, definition, plan, &resolved, args).await?;
        let resume_changes = if let Some(after) =
            after.filter(|after| *after >= clock.retained_revision && *after < clock.revision)
        {
            let mut tables = plan.resolved_dependency_columns(&resolved).tables();
            tables.insert(definition.table.clone());
            resume_changes(&mut transaction, after, clock.revision, tables).await?
        } else {
            Vec::new()
        };
        transaction.commit().await?;
        let mut by_key = BTreeMap::new();
        let mut hashes = BTreeMap::new();
        for (index, row) in rows.iter().enumerate() {
            if index % 32 == 0 {
                tokio::task::yield_now().await;
            }
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
            resume_changes,
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
            let dependencies = subscription
                .visibility
                .resolved_dependency_columns(&subscription.resolved);
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
                || bounded_sql_visibility(
                    &subscription.definition,
                    &subscription.visibility,
                    &subscription.resolved,
                )
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
                if subscription.aggregate.is_some() {
                    let sum = subscription.aggregate.as_ref().unwrap().is_sum();
                    subscription.aggregate =
                        Some(ReplicaAggregate::from_hashes(&subscription.hashes, sum));
                }
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
                        subscription_digest(subscription),
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
                    let new_hash = new_projection.as_ref().map(row_hash);
                    if let Some(aggregate) = &mut subscription.aggregate {
                        if let Some(previous) = subscription.hashes.get(&change.row_id) {
                            aggregate.change(previous, true);
                        }
                        if let Some(hash) = &new_hash {
                            aggregate.change(hash, false);
                        }
                    }
                    if new_visible {
                        let new_projection = new_projection
                            .as_ref()
                            .expect("visible source rows have a projection");
                        subscription
                            .rows
                            .insert(change.row_id.clone(), new_projection.clone());
                        subscription
                            .hashes
                            .insert(change.row_id.clone(), new_hash.unwrap());
                    } else {
                        subscription.rows.remove(&change.row_id);
                        subscription.hashes.remove(&change.row_id);
                    }
                    accumulate_projection_delta(
                        &mut upserts,
                        &mut deleted,
                        &change.row_id,
                        new_projection.as_ref(),
                    );
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
                        subscription_digest(subscription),
                        subscription.truncated,
                    ));
                }
            }
            messages.push(replica_ready(
                &subscription.id,
                &subscription.path,
                &subscription.definition,
                subscription.cursor.clone(),
                subscription_digest(subscription),
                subscription.truncated,
            ));
        }
        if !entity_changes.is_empty() {
            normalize_entity_changes(
                &mut entity_changes,
                subscriptions.values().map(|subscription| {
                    (subscription.definition.table.as_str(), &subscription.rows)
                }),
            );
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

// Each subscription owns a projection and membership. The normalized entity
// owns their union; the last subscription visited must not erase another one's
// fields or delete an entity still present in a different collection.
fn normalize_entity_changes<'a>(
    changes: &mut BTreeMap<(String, String), ReplicaChange>,
    collections: impl Iterator<Item = (&'a str, &'a BTreeMap<String, Value>)>,
) {
    let collections: Vec<_> = collections.collect();
    for ((table, id), change) in changes.iter_mut() {
        let mut fields = serde_json::Map::new();
        for (collection_table, rows) in &collections {
            if *collection_table != table {
                continue;
            }
            if let Some(row) = rows.get(id).and_then(Value::as_object) {
                fields.extend(row.clone());
            }
        }
        if fields.is_empty() {
            change.operation = "delete".to_owned();
            change.new_value = None;
        } else {
            change.operation = if change.old_value.is_some() {
                "update"
            } else {
                "insert"
            }
            .to_owned();
            change.new_value = Some(Value::Object(fields));
        }
    }
}

async fn resume_changes(
    transaction: &mut TenantTransaction,
    after: u64,
    through: u64,
    tables: BTreeSet<String>,
) -> Result<Vec<LogChange>, ReplicaError> {
    let rows = sqlx::query(
            "SELECT revision, ordinal, table_name, row_id, operation, changed_columns, old_value, new_value
             FROM _gonvex_sync_changes
             WHERE revision > $1 AND revision <= $2 AND table_name = ANY($3)
             ORDER BY revision, ordinal",
        ).bind(after as i64).bind(through as i64).bind(tables.into_iter().collect::<Vec<_>>()).fetch_all(&mut **transaction.transaction()).await?;
    Ok(rows
        .into_iter()
        .map(|row| LogChange {
            revision: row.get::<i64, _>("revision") as u64,
            ordinal: row.get("ordinal"),
            table: row.get("table_name"),
            row_id: row.get("row_id"),
            operation: row.get("operation"),
            changed_columns: row
                .get::<Option<Vec<String>>, _>("changed_columns")
                .unwrap_or_default(),
            old_value: row
                .get::<Option<Value>, _>("old_value")
                .unwrap_or(Value::Null),
            new_value: row
                .get::<Option<Value>, _>("new_value")
                .unwrap_or(Value::Null),
            origin_command_id: String::new(),
            provenance: Default::default(),
        })
        .collect())
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
) -> Result<(Vec<Value>, bool), ReplicaError> {
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
    // Decode and admit one row at a time. SQL still uses maxRows + 1;
    // a byte-limited source must not first allocate its entire SQL result.
    let rows = query.fetch(&mut **transaction.transaction()).map(|row| {
        let row = row?;
        let raw: String = row.get(0);
        serde_json::from_str(&raw)
            .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))
    });
    collect_snapshot_rows(rows, definition).await
}

async fn collect_snapshot_rows(
    rows: impl Stream<Item = Result<Value, ReplicaError>>,
    definition: &ReplicaCollectionDefinition,
) -> Result<(Vec<Value>, bool), ReplicaError> {
    futures_util::pin_mut!(rows);
    let mut kept = Vec::new();
    let mut bytes = 0i64;
    let mut seen = BTreeSet::new();
    while let Some(row) = rows.next().await {
        if kept.len() % 32 == 0 {
            tokio::task::yield_now().await;
        }
        let row = row?;
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
fn bounded_sql_visibility(
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> bool {
    (definition.max_rows > 0 || definition.max_bytes > 0) && plan.needs_materialized_sets(resolved)
}

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

fn requested_sum_digest(
    request: &ReplicaOpenRequest,
    negotiated: bool,
) -> Result<bool, ReplicaError> {
    let sum = request
        .digest
        .as_deref()
        .is_some_and(|digest| digest.starts_with("sum128-v1:"));
    if sum && !negotiated {
        return Err(ReplicaError::DigestNotNegotiated);
    }
    Ok(sum)
}

fn row_hash(row: &Value) -> String {
    hex_digest(stable_json(row).as_bytes())
}

fn cursor_resumable(
    client: Option<&ReplicaCursor>,
    current: &ReplicaCursor,
    retained: u64,
) -> bool {
    client.is_some_and(|client| {
        client.epoch == current.epoch
            && client.revision <= current.revision
            && client.revision >= retained
    })
}

fn verify_resume_hashes(
    request: &ReplicaOpenRequest,
    hashes: Option<&BTreeMap<String, String>>,
    aggregate: bool,
) -> Option<ServerMessage> {
    if hashes.is_some_and(|hashes| {
        request.digest.as_deref()
            == Some(
                if aggregate {
                    if request
                        .digest
                        .as_deref()
                        .is_some_and(|digest| digest.starts_with("sum128-v1:"))
                    {
                        ReplicaAggregate::from_hashes(hashes, true).digest()
                    } else {
                        aggregate_digest(hashes)
                    }
                } else {
                    hashes_digest(hashes)
                }
                .as_str(),
            )
    }) {
        None
    } else {
        Some(ServerMessage::ReplicaNeedHashes {
            id: request.id.clone(),
            path: Some(request.path.clone()),
        })
    }
}

fn xor_hash(aggregate: &mut [u8; 32], hash: &str) {
    for (index, byte) in aggregate.iter_mut().enumerate() {
        *byte ^= u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16).unwrap_or(0);
    }
}

fn aggregate_bytes(hashes: &BTreeMap<String, String>) -> [u8; 32] {
    let mut aggregate = [0u8; 32];
    for hash in hashes.values() {
        xor_hash(&mut aggregate, hash);
    }
    aggregate
}

fn aggregate_digest(hashes: &BTreeMap<String, String>) -> String {
    format_aggregate(&aggregate_bytes(hashes))
}

fn format_aggregate(aggregate: &[u8; 32]) -> String {
    format!(
        "xor1:{}",
        aggregate
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn subscription_digest(subscription: &ReplicaSubscription) -> String {
    if let Some(aggregate) = &subscription.aggregate {
        aggregate.digest()
    } else {
        hashes_digest(&subscription.hashes)
    }
}

// Reconstruct the cursor's membership from retained before-images. Visibility
// dependency changes require reconciliation rather than trusting old visibility.
fn resume_hashes(
    current: &BTreeMap<String, String>,
    changes: &[LogChange],
    args: &Value,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> Result<Option<BTreeMap<String, String>>, ReplicaError> {
    if bounded_sql_visibility(definition, plan, resolved) {
        // Retained before-images cannot evaluate an unmaterialized SQL set.
        // Reconcile the bounded snapshot instead of trusting missing IDs.
        return Ok(None);
    }
    let dependencies = plan.resolved_dependency_columns(resolved);
    if table_change_relevance(&definition.table, &dependencies, changes.iter()).1 {
        return Ok(None);
    }
    let mut prior = current.clone();
    for change in changes
        .iter()
        .rev()
        .filter(|change| change.table == definition.table)
    {
        prior.remove(&change.row_id);
        if row_in_collection(&change.old_value, args, definition, plan, resolved) {
            prior.insert(
                change.row_id.clone(),
                row_hash(&project_row(&change.old_value, definition)?),
            );
        }
    }
    Ok(Some(prior))
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
    snapshot: &mut Snapshot,
    cursor: ReplicaCursor,
    scope: &str,
    visibility_scope: &str,
    digest: &str,
) -> ServerMessage {
    let mut metadata = BTreeMap::from([
        ("scope".to_owned(), Value::String(scope.to_owned())),
        (
            "visibilityScope".to_owned(),
            Value::String(visibility_scope.to_owned()),
        ),
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
    metadata.insert("pageDigest".to_owned(), Value::String(digest.to_owned()));
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
        result: std::mem::take(&mut snapshot.rows),
        cursor,
        key: definition.key.clone(),
        metadata,
    }
}

pub(crate) fn legacy_snapshot_frame(mut message: ServerMessage) -> ServerMessage {
    if let ServerMessage::ReplicaSnapshot { metadata, .. } = &mut message {
        metadata.remove("pageDigest");
        metadata.remove("scope");
        metadata.remove("visibilityScope");
    }
    message
}

const SNAPSHOT_PAGE_BYTES: usize = 256 * 1024;
const SNAPSHOT_PAGE_ROWS: usize = 512;

// Rows are moved from the materialized authorized snapshot, not copied into
// a second collection of pages. Only the current page is encoded by the socket.
pub(crate) struct SnapshotPages {
    id: String,
    snapshot_id: String,
    cursor: ReplicaCursor,
    key: String,
    metadata: BTreeMap<String, Value>,
    digest: Value,
    rows: std::iter::Peekable<std::vec::IntoIter<Value>>,
    sequence: u64,
    finished: bool,
    native_rows: bool,
}

impl SnapshotPages {
    pub(crate) fn new(message: ServerMessage) -> Self {
        let ServerMessage::ReplicaSnapshot {
            id,
            result,
            cursor,
            key,
            mut metadata,
        } = message
        else {
            unreachable!("snapshot paging requires a snapshot");
        };
        metadata.remove("hashes");
        let digest = metadata
            .remove("pageDigest")
            .or_else(|| metadata.remove("digest"))
            .expect("snapshot digest");
        metadata.remove("digest");
        Self {
            id,
            snapshot_id: uuid::Uuid::new_v4().to_string(),
            cursor,
            key,
            metadata,
            digest,
            rows: result.into_iter().peekable(),
            sequence: 0,
            finished: false,
            native_rows: false,
        }
    }

    pub(crate) fn native(message: ServerMessage) -> Self {
        let mut pages = Self::new(message);
        pages.native_rows = true;
        pages
    }

    pub(crate) fn identity(&self) -> (&str, &str) {
        (&self.id, &self.snapshot_id)
    }

    pub(crate) fn start(&self) -> ServerMessage {
        let mut metadata = self.metadata.clone();
        metadata.retain(|name, _| {
            matches!(
                name.as_str(),
                "path"
                    | "scope"
                    | "visibilityScope"
                    | "orderBy"
                    | "orderDirection"
                    | "mode"
                    | "maxRows"
                    | "maxBytes"
                    | "truncated"
            )
        });
        metadata.insert("digest".to_owned(), self.digest.clone());
        ServerMessage::ReplicaSnapshotStart {
            snapshot_id: self.snapshot_id.clone(),
            id: self.id.clone(),
            cursor: self.cursor.clone(),
            key: self.key.clone(),
            metadata,
        }
    }

    fn page(&self, result: Vec<Value>, last: bool) -> ServerMessage {
        let mut metadata = self.metadata.clone();
        if last {
            metadata.insert("digest".to_owned(), self.digest.clone());
        }
        if self.native_rows {
            return ServerMessage::ReplicaSnapshotEncodedPage {
                id: self.id.clone(),
                snapshot_id: self.snapshot_id.clone(),
                cursor: self.cursor.clone(),
                key: self.key.clone(),
                sequence: self.sequence,
                last,
                canonical_rows: result.iter().map(stable_json).collect(),
                metadata,
            };
        }
        ServerMessage::ReplicaSnapshotPage {
            id: self.id.clone(),
            snapshot_id: self.snapshot_id.clone(),
            cursor: self.cursor.clone(),
            key: self.key.clone(),
            sequence: self.sequence,
            last,
            result,
            metadata,
        }
    }
}

fn json_byte_len(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Number(number) => number.to_string().len(),
        Value::String(value) => json_string_byte_len(value),
        Value::Array(values) => {
            2 + values.iter().map(json_byte_len).sum::<usize>() + values.len().saturating_sub(1)
        }
        Value::Object(values) => {
            2 + values
                .iter()
                .map(|(key, value)| json_string_byte_len(key) + 1 + json_byte_len(value))
                .sum::<usize>()
                + values.len().saturating_sub(1)
        }
    }
}

fn json_string_byte_len(value: &str) -> usize {
    // Count JSON escapes without allocating a temporary encoded row buffer.
    2 + value.len()
        + value
            .bytes()
            .map(|byte| match byte {
                b'"' | b'\\' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 1,
                0..=0x1f => 5,
                _ => 0,
            })
            .sum::<usize>()
}

impl Iterator for SnapshotPages {
    type Item = ServerMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        // Reserve the final digest and the longer false marker in every page.
        let mut bytes = serde_json::to_vec(&self.page(Vec::new(), true))
            .unwrap()
            .len()
            + 1;
        let mut rows = Vec::new();
        while let Some(row) = self.rows.peek() {
            let row_bytes = (if self.native_rows {
                json_string_byte_len(&stable_json(row))
            } else {
                json_byte_len(row)
            }) + usize::from(!rows.is_empty());
            if !rows.is_empty()
                && (bytes + row_bytes
                    > if self.native_rows {
                        64 * 1024
                    } else {
                        SNAPSHOT_PAGE_BYTES
                    }
                    || rows.len() == SNAPSHOT_PAGE_ROWS)
            {
                break;
            }
            // A single oversized row remains indivisible, matching legacy JSON.
            bytes += row_bytes;
            rows.push(self.rows.next().unwrap());
        }
        self.finished = self.rows.peek().is_none();
        let page = self.page(rows, self.finished);
        self.sequence += 1;
        Some(page)
    }
}

/// Native control rows stay canonical strings through JS envelope decoding.
/// In particular int64 values and source hashes must not pass through f64 JSON.
pub(crate) fn native_control_message(
    message: &ServerMessage,
) -> Result<Option<ServerMessage>, &'static str> {
    match message {
        ServerMessage::ReplicaDelta {
            id,
            cursor,
            payload,
        } => {
            let rows = match payload.get("upserts") {
                Some(Value::Array(rows)) => rows.as_slice(),
                None => &[],
                _ => return Err("invalid native delta rows"),
            };
            let mut encoded: BTreeMap<String, Value> = payload
                .iter()
                .filter(|(key, _)| key.as_str() != "upserts")
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            encoded.insert(
                "canonicalUpserts".into(),
                Value::Array(
                    rows.iter()
                        .map(|row| Value::String(stable_json(row)))
                        .collect(),
                ),
            );
            Ok(Some(ServerMessage::ReplicaDelta {
                id: id.clone(),
                cursor: cursor.clone(),
                payload: encoded,
            }))
        }
        ServerMessage::ReplicaTransaction {
            cursor,
            origin_command_id,
            provenance,
            changes,
        } => {
            let canonical_changes = changes
                .iter()
                .map(|change| {
                    let mut value = serde_json::to_value(change)
                        .map_err(|_| "native transaction encoding failed")?;
                    // Canonical storage merges against its own prior row. The old
                    // projection is unnecessary and could double every wire row.
                    if let Value::Object(object) = &mut value {
                        object.remove("oldValue");
                    }
                    Ok(stable_json(&value))
                })
                .collect::<Result<Vec<_>, &'static str>>()?;
            Ok(Some(ServerMessage::ReplicaTransactionEncoded {
                cursor: cursor.clone(),
                origin_command_id: origin_command_id.clone(),
                provenance: provenance.clone(),
                canonical_changes,
            }))
        }
        _ => Ok(None),
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

fn accumulate_projection_delta(
    upserts: &mut BTreeMap<String, Value>,
    deleted: &mut BTreeSet<String>,
    id: &str,
    row: Option<&Value>,
) {
    if let Some(row) = row {
        deleted.remove(id);
        upserts.insert(id.to_owned(), row.clone());
    } else {
        upserts.remove(id);
        deleted.insert(id.to_owned());
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

    #[tokio::test]
    async fn snapshot_stream_stops_after_row_or_byte_budget_lookahead() {
        let rows = vec![
            serde_json::json!({"id":"a"}),
            serde_json::json!({"id":"b"}),
            serde_json::json!({"id":"c"}),
        ];
        for (max_rows, max_bytes) in [
            (2, 0),
            (0, serde_json::to_vec(&rows[0]).unwrap().len() as i64 * 2),
        ] {
            let definition: ReplicaCollectionDefinition = serde_json::from_value(serde_json::json!({
                "table":"tasks", "key":"id", "columns":["id"], "maxRows":max_rows, "maxBytes":max_bytes
            })).unwrap();
            let source = futures_util::stream::iter(rows.clone().into_iter().map(Ok)).chain(
                futures_util::stream::once(async { panic!("read beyond budget lookahead") }),
            );
            let (kept, truncated) = collect_snapshot_rows(source, &definition).await.unwrap();
            assert_eq!(kept, rows[..2]);
            assert!(truncated);
        }
    }

    #[tokio::test]
    async fn snapshot_stream_distinguishes_exact_boundary_from_truncation_and_errors() {
        let row = serde_json::json!({"id":"a", "text":"café 東京"});
        let definition: ReplicaCollectionDefinition = serde_json::from_value(serde_json::json!({
            "table":"tasks", "key":"id", "columns":["id","text"], "maxRows":1,
            "maxBytes":serde_json::to_vec(&row).unwrap().len()
        }))
        .unwrap();
        let (kept, truncated) =
            collect_snapshot_rows(futures_util::stream::iter([Ok(row.clone())]), &definition)
                .await
                .unwrap();
        assert_eq!(kept, vec![row.clone()]);
        assert!(!truncated);
        assert!(matches!(
            collect_snapshot_rows(
                futures_util::stream::iter([Ok(row.clone()), Ok(row)]),
                &definition
            )
            .await,
            Err(ReplicaError::DuplicateKey(_))
        ));
        assert!(matches!(
            collect_snapshot_rows(
                futures_util::stream::iter([Err(ReplicaError::InvalidArguments)]),
                &definition
            )
            .await,
            Err(ReplicaError::InvalidArguments)
        ));
    }

    fn paged_fixture(rows: Vec<Value>) -> ServerMessage {
        let hashes = rows
            .iter()
            .map(|row| (row_key(row, "id").unwrap(), row_hash(row)))
            .collect();
        ServerMessage::ReplicaSnapshot {
            id: "open".into(),
            result: rows,
            cursor: ReplicaCursor {
                epoch: "e".into(),
                revision: 10,
            },
            key: "id".into(),
            metadata: BTreeMap::from([
                ("pageDigest".into(), Value::String(hashes_digest(&hashes))),
                ("scope".into(), Value::String("tenant".into())),
                ("visibilityScope".into(), Value::String("visible".into())),
                ("path".into(), Value::String("tasks.all".into())),
                ("mode".into(), Value::String("eager".into())),
                ("orderBy".into(), Value::String("id".into())),
                ("orderDirection".into(), Value::String("asc".into())),
                ("maxRows".into(), Value::from(20_000)),
                ("maxBytes".into(), Value::from(100_000_000)),
                ("truncated".into(), Value::Bool(false)),
            ]),
        }
    }

    #[test]
    fn native_control_rows_preserve_exact_wire_values_and_omit_old_projection() {
        let row = serde_json::json!({"id":"a","n":9007199254740993u64,"text":"café 東京 😀\u{2028}","nested":{"null":null}});
        let cursor = ReplicaCursor {
            epoch: "epoch".into(),
            revision: 17,
        };
        let delta = ServerMessage::ReplicaDelta {
            id: "open".into(),
            cursor: cursor.clone(),
            payload: BTreeMap::from([
                ("upserts".into(), Value::Array(vec![row.clone()])),
                ("deleted".into(), serde_json::json!(["gone"])),
                ("digest".into(), Value::String("receipt".into())),
            ]),
        };
        let Some(ServerMessage::ReplicaDelta { payload, .. }) =
            native_control_message(&delta).unwrap()
        else {
            panic!("encoded delta")
        };
        assert!(!payload.contains_key("upserts"));
        assert_eq!(
            payload["canonicalUpserts"],
            serde_json::json!([stable_json(&row)])
        );
        assert_eq!(payload["deleted"], serde_json::json!(["gone"]));
        assert!(
            matches!(delta, ServerMessage::ReplicaDelta {ref payload,..} if payload.contains_key("upserts"))
        );
        let transaction:ServerMessage=serde_json::from_value(serde_json::json!({
            "type":"replica.transaction","cursor":cursor,"originCommandId":"command",
            "changes":[{"entity":"tasks","id":"a","operation":"update","changedColumns":["n","text"],"oldValue":{"n":1},"newValue":row}]
        })).unwrap();
        let Some(ServerMessage::ReplicaTransactionEncoded {
            canonical_changes,
            origin_command_id,
            cursor,
            ..
        }) = native_control_message(&transaction).unwrap()
        else {
            panic!("encoded transaction")
        };
        assert_eq!(origin_command_id.as_deref(), Some("command"));
        assert_eq!(cursor.revision, 17);
        let encoded = &canonical_changes[0];
        assert!(encoded.contains("9007199254740993"));
        let change: Value = serde_json::from_str(encoded).unwrap();
        assert_eq!(change["newValue"]["n"].as_u64(), Some(9007199254740993));
        assert!(change.get("oldValue").is_none());
        assert_eq!(change["changedColumns"], serde_json::json!(["n", "text"]));
        assert!(
            native_control_message(&ServerMessage::ReplicaWatermark { revision: 17 })
                .unwrap()
                .is_none()
        );
        let invalid = ServerMessage::ReplicaDelta {
            id: "bad".into(),
            cursor,
            payload: BTreeMap::from([("upserts".into(), Value::Null)]),
        };
        assert!(native_control_message(&invalid).is_err());
    }

    #[test]
    fn native_snapshot_wire_fixture_matches_canonical_hashes() {
        let count = std::env::var("GONVEX_NATIVE_FRAME_ROWS")
            .ok()
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or(1200);
        assert!((1200..=100000).contains(&count));
        let mut rows:Vec<Value>=(0..count).map(|id|serde_json::json!({"id":format!("row-{id:04}"),"text":"café 東京 😀".repeat(20),"nested":{"é":"nfc","é":"nfd"}})).collect();
        rows.push(serde_json::json!({"id":"controls","text":"\u{2028}\u{2029}\0\n\t\r\u{1f}","value":9007199254740993u64}));
        let mut sum = gonvex_protocol::replica_digest::ReplicaSetDigest::default();
        let hashes: Vec<_> = rows.iter().map(row_hash).collect();
        for hash in &hashes {
            sum.add(hash).unwrap();
        }
        let mut snapshot = paged_fixture(rows.clone());
        if let ServerMessage::ReplicaSnapshot { metadata, .. } = &mut snapshot {
            metadata.insert("pageDigest".into(), Value::String(sum.digest()));
            metadata.insert("maxRows".into(), Value::from(rows.len() as u64));
        }
        let pages: Vec<_> = SnapshotPages::native(snapshot).collect();
        assert!(pages.len() > 1);
        let mut observed = Vec::new();
        for (sequence, page) in pages.iter().enumerate() {
            assert!(serde_json::to_vec(page).unwrap().len() <= 64 * 1024);
            let ServerMessage::ReplicaSnapshotEncodedPage {
                canonical_rows,
                sequence: actual,
                last,
                metadata,
                ..
            } = page
            else {
                panic!("native frame");
            };
            assert_eq!(*actual, sequence as u64);
            assert_eq!(*last, sequence == pages.len() - 1);
            assert_eq!(metadata.contains_key("digest"), *last);
            observed.extend(canonical_rows.iter().map(|row| hex_digest(row.as_bytes())));
        }
        assert_eq!(observed, hashes);
        if let Ok(path) = std::env::var("GONVEX_NATIVE_FRAME_LINES") {
            use std::io::Write;
            let mut output = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
            for page in &pages {
                serde_json::to_writer(&mut output, page).unwrap();
                output.write_all(b"\n").unwrap();
            }
            output.flush().unwrap();
            std::fs::write(format!("{path}.manifest.json"),serde_json::to_vec(&serde_json::json!({"rows":rows.len(),"frames":pages.len(),"digest":sum.digest()})).unwrap()).unwrap();
        }
        if let Ok(path) = std::env::var("GONVEX_NATIVE_FRAME_FIXTURE") {
            std::fs::write(path,serde_json::to_vec(&serde_json::json!({"frames":pages,"rows":rows,"hashes":hashes,"digest":sum.digest()})).unwrap()).unwrap();
        }
    }

    #[test]
    fn snapshot_pages_json_byte_len_matches_encoder() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            serde_json::json!(-0.0),
            serde_json::json!(1.5e30),
            serde_json::json!(u64::MAX),
            serde_json::json!(i64::MIN),
            serde_json::json!("é😀\\\"\n\t\r\u{0008}\u{000c}\u{001f}\u{0000}\u{2028}"),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!({"escaped\"key": [null, true, false, 1.2, {"nested":"é😀"}]}),
        ];
        for value in values {
            assert_eq!(
                json_byte_len(&value),
                serde_json::to_vec(&value).unwrap().len(),
                "{value}"
            );
        }
    }

    #[test]
    fn snapshot_pages_legacy_frame_preserves_original_metadata() {
        let frame = paged_fixture(vec![serde_json::json!({"id":"a"})]);
        let ServerMessage::ReplicaSnapshot {
            metadata, result, ..
        } = legacy_snapshot_frame(frame)
        else {
            panic!("snapshot");
        };
        assert_eq!(result.len(), 1);
        assert!(!metadata.contains_key("pageDigest"));
        assert!(!metadata.contains_key("scope"));
        assert!(!metadata.contains_key("visibilityScope"));
        assert_eq!(metadata["path"], "tasks.all");
    }

    #[test]
    fn snapshot_pages_byte_and_row_limits_20k_measurement() {
        let rows: Vec<Value> = (0..20_000)
            .map(|i| serde_json::json!({"id":format!("row-{i:05}"),"body":"é".repeat(500)}))
            .collect();
        let snapshot = paged_fixture(rows);
        let legacy = legacy_snapshot_frame(snapshot.clone());
        let encoded = serde_json::to_string(&legacy).unwrap();
        drop(legacy);
        let before = encoded.len();
        let before_buffer = encoded.capacity();
        drop(encoded);
        let mut maximum = 0;
        let mut maximum_buffer = 0;
        let mut total = 0;
        let mut count = 0;
        let mut rows = 0;
        let mut identity = None;
        for page in SnapshotPages::new(snapshot) {
            let encoded = serde_json::to_string(&page).unwrap();
            let bytes = encoded.len();
            maximum_buffer = maximum_buffer.max(encoded.capacity());
            assert!(bytes <= SNAPSHOT_PAGE_BYTES);
            maximum = maximum.max(bytes);
            total += bytes;
            let ServerMessage::ReplicaSnapshotPage {
                snapshot_id,
                sequence,
                last,
                result,
                metadata,
                cursor,
                ..
            } = page
            else {
                panic!("page");
            };
            assert_eq!(sequence, count);
            assert_eq!(cursor.revision, 10);
            assert!(result.len() <= SNAPSHOT_PAGE_ROWS);
            if let Some(identity) = &identity {
                assert_eq!(&snapshot_id, identity);
            } else {
                identity = Some(snapshot_id);
            }
            assert_eq!(metadata.contains_key("digest"), last);
            rows += result.len();
            count += 1;
        }
        assert_eq!(rows, 20_000);
        assert!(maximum < before / 50);
        println!(
            "20k JSON wire: monolithic={before} bytes, max_page={maximum} bytes, total_pages={total} bytes, pages={count}; peak single encoded String capacity: before={before_buffer} bytes, after={maximum_buffer} bytes; resident/staged rows and transport queues excluded"
        );
    }

    #[test]
    fn snapshot_pages_materialized_generation_ignores_concurrent_changes() {
        let source: Vec<Value> = (0..1_200)
            .map(|i| serde_json::json!({"id":i,"value":0}))
            .collect();
        let snapshot = paged_fixture(source.clone());
        let mut pages = SnapshotPages::new(snapshot);
        let first = pages.next().unwrap();
        // A writer can commit after the repeatable-read transaction is closed.
        // Pages own that generation; writes are later replayed by the feed.
        let writer = std::thread::spawn(move || {
            let mut source = source;
            for row in &mut source {
                row["value"] = Value::from(1);
            }
            source
        });
        let mut count = 0;
        for page in std::iter::once(first).chain(pages) {
            let ServerMessage::ReplicaSnapshotPage { result, cursor, .. } = page else {
                panic!("page");
            };
            assert_eq!(cursor.revision, 10);
            assert!(result.iter().all(|row| row["value"] == 0));
            count += result.len();
        }
        assert_eq!(count, 1_200);
        assert!(writer.join().unwrap().iter().all(|row| row["value"] == 1));
    }

    #[test]
    fn snapshot_pages_interruption_empty_and_oversized_rows() {
        let source: Vec<Value> = (0..1_200).map(|i| serde_json::json!({"id":i})).collect();
        let mut interrupted = SnapshotPages::new(paged_fixture(source.clone()));
        let ServerMessage::ReplicaSnapshotPage {
            snapshot_id: old_id,
            last,
            ..
        } = interrupted.next().unwrap()
        else {
            panic!("page");
        };
        assert!(!last);
        drop(interrupted);
        let restarted: Vec<_> = SnapshotPages::new(paged_fixture(source)).collect();
        let ServerMessage::ReplicaSnapshotPage {
            snapshot_id,
            sequence,
            ..
        } = &restarted[0]
        else {
            panic!("page");
        };
        assert_ne!(snapshot_id, &old_id);
        assert_eq!(*sequence, 0);
        let empty: Vec<_> = SnapshotPages::new(paged_fixture(vec![])).collect();
        assert!(
            matches!(&empty[0], ServerMessage::ReplicaSnapshotPage { last: true, result, metadata, .. } if result.is_empty() && metadata.contains_key("digest"))
        );
        assert_eq!(empty.len(), 1);
        let large = serde_json::json!({"id":"large", "value":"x".repeat(SNAPSHOT_PAGE_BYTES)});
        let pages: Vec<_> = SnapshotPages::new(paged_fixture(vec![
            large,
            serde_json::json!({"id":"small"}),
        ]))
        .collect();
        assert_eq!(pages.len(), 2);
        assert!(
            matches!(&pages[0], ServerMessage::ReplicaSnapshotPage { result, last: false, .. } if result.len() == 1)
        );
    }

    #[test]
    fn sdk_resume_expired_or_wrong_epoch_cursor_requires_snapshot() {
        let current = ReplicaCursor {
            epoch: "e".into(),
            revision: 10,
        };
        assert!(cursor_resumable(
            Some(&ReplicaCursor {
                epoch: "e".into(),
                revision: 5
            }),
            &current,
            5
        ));
        assert!(!cursor_resumable(
            Some(&ReplicaCursor {
                epoch: "e".into(),
                revision: 4
            }),
            &current,
            5
        ));
        assert!(!cursor_resumable(
            Some(&ReplicaCursor {
                epoch: "old".into(),
                revision: 10
            }),
            &current,
            5
        ));
        assert!(!cursor_resumable(None, &current, 5));
    }

    #[test]
    fn sdk_resume_delta_verifies_before_images_and_detects_mismatch() -> Result<(), ReplicaError> {
        let definition = ReplicaCollectionDefinition {
            table: "tasks".into(),
            key: "id".into(),
            columns: vec!["id".into(), "title".into()],
            equal_filters: BTreeMap::new(),
            exclude_when_set: Vec::new(),
            visibility_tables: Vec::new(),
            visibility_plan_hash: String::new(),
            order_by: String::new(),
            order_direction: String::new(),
            mode: "eager".into(),
            max_rows: 100,
            max_bytes: 10000,
            retention_ms: 0,
        };
        let (plan, resolved) = public_visibility();
        let old = serde_json::json!({"id":"1", "title":"old"});
        let new = serde_json::json!({"id":"1", "title":"new"});
        let inserted = serde_json::json!({"id":"2", "title":"inserted"});
        let removed = serde_json::json!({"id":"3", "title":"removed"});
        let original = BTreeMap::from([
            ("1".into(), row_hash(&old)),
            ("3".into(), row_hash(&removed)),
        ]);
        let current = BTreeMap::from([
            ("1".into(), row_hash(&new)),
            ("2".into(), row_hash(&inserted)),
        ]);
        let mut insertion = source_change(Value::Null, inserted.clone());
        insertion.row_id = "2".into();
        let mut deletion = source_change(removed, Value::Null);
        deletion.row_id = "3".into();
        let intermediate = serde_json::json!({"id":"1", "title":"intermediate"});
        let changes = vec![
            source_change(old, intermediate.clone()),
            source_change(intermediate, new.clone()),
            insertion,
            deletion,
        ];
        let prior = resume_hashes(
            &current,
            &changes,
            &serde_json::json!({}),
            &definition,
            &plan,
            &resolved,
        )?
        .unwrap();
        assert_eq!(aggregate_digest(&prior), aggregate_digest(&original));
        let mut request: ReplicaOpenRequest = serde_json::from_value(serde_json::json!({
            "id":"r", "path":"tasks.list", "args":{}, "digest":aggregate_digest(&original)
        }))
        .unwrap();
        assert!(verify_resume_hashes(&request, Some(&prior), true).is_none());
        request.digest = Some(aggregate_digest(&BTreeMap::new()));
        assert!(matches!(
            verify_resume_hashes(&request, Some(&prior), true),
            Some(ServerMessage::ReplicaNeedHashes { .. })
        ));
        assert!(matches!(
            verify_resume_hashes(&request, None, true),
            Some(ServerMessage::ReplicaNeedHashes { .. })
        ));
        let (upserts, deleted) = diff_rows(
            &BTreeMap::from([("1".into(), new.clone()), ("2".into(), inserted.clone())]),
            &current,
            &prior,
            &["1".into(), "3".into()],
        );
        assert_eq!(upserts, vec![new, inserted]);
        assert_eq!(deleted, vec!["3"]);
        // A bounded snapshot may omit an unchanged row displaced since the
        // cursor. Its reconstructed checksum must request reconciliation.
        let bounded_prior = BTreeMap::from([("3".into(), original["3"].clone())]);
        request.digest = Some(aggregate_digest(&original));
        assert!(matches!(
            verify_resume_hashes(&request, Some(&bounded_prior), true),
            Some(ServerMessage::ReplicaNeedHashes { .. })
        ));
        let dependency = visibility_with_task_dependency("tasks");
        assert!(resume_hashes(
            &current,
            &[routing_change("tasks", "update", &["createdBy"])],
            &serde_json::json!({}),
            &definition,
            &dependency,
            &resolved
        )?
        .is_none());
        Ok(())
    }

    #[test]
    fn negotiated_sum_digest_resume_and_incremental_changes() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../packages/protocol/fixtures/replica-set-digest.json"
        ))
        .unwrap();
        let mut hashes = BTreeMap::new();
        let mut aggregate = ReplicaAggregate::from_hashes(&hashes, true);
        for (i, mutation) in fixture["mutations"].as_array().unwrap().iter().enumerate() {
            let hash = mutation["hash"].as_str().unwrap();
            let remove = mutation["op"] == "remove";
            aggregate.change(hash, remove);
            if remove {
                hashes.retain(|_, value| value != hash);
            } else {
                hashes.insert(i.to_string(), hash.to_owned());
            }
            assert_eq!(
                aggregate.digest(),
                ReplicaAggregate::from_hashes(&hashes, true).digest()
            );
        }
        assert_eq!(aggregate.digest(), fixture["digest"].as_str().unwrap());
        let mut request:ReplicaOpenRequest=serde_json::from_value(serde_json::json!({"id":"sum","path":"tasks.all","args":{},"digest":aggregate.digest()})).unwrap();
        assert!(matches!(
            requested_sum_digest(&request, false),
            Err(ReplicaError::DigestNotNegotiated)
        ));
        assert!(requested_sum_digest(&request, true).unwrap());
        assert!(verify_resume_hashes(&request, Some(&hashes), true).is_none());
        request.digest = Some(format!("sum128-v1:{}", "00".repeat(16)));
        assert!(matches!(
            verify_resume_hashes(&request, Some(&hashes), true),
            Some(ServerMessage::ReplicaNeedHashes { .. })
        ));
        request.digest = Some(aggregate_digest(&hashes));
        assert!(!requested_sum_digest(&request, false).unwrap());
        assert!(verify_resume_hashes(&request, Some(&hashes), true).is_none());
    }

    #[test]
    fn sdk_resume_aggregate_matches_client_xor_fixture() {
        let hashes = BTreeMap::from([("a".into(), "01".repeat(32)), ("b".into(), "03".repeat(32))]);
        assert_eq!(
            aggregate_digest(&hashes),
            format!("xor1:{}", "02".repeat(32))
        );
        assert_eq!(
            aggregate_digest(&BTreeMap::new()),
            format!("xor1:{}", "00".repeat(32))
        );
        let mut aggregate = aggregate_bytes(&hashes);
        xor_hash(&mut aggregate, &hashes["a"]);
        xor_hash(&mut aggregate, &"05".repeat(32));
        assert_eq!(
            format_aggregate(&aggregate),
            aggregate_digest(&BTreeMap::from([
                ("a".into(), "05".repeat(32)),
                ("b".into(), "03".repeat(32)),
            ]))
        );
        xor_hash(&mut aggregate, &hashes["b"]);
        assert_eq!(
            format_aggregate(&aggregate),
            format!("xor1:{}", "05".repeat(32))
        );
    }

    #[test]
    fn repeated_writes_publish_only_the_final_collection_membership() {
        let mut upserts = BTreeMap::new();
        let mut deleted = BTreeSet::new();
        let intermediate = serde_json::json!({"id":"a","categoryId":null,"deletedAt":null});
        // One atomic cleanup first clears references and then soft-deletes the
        // same row. Sending both entries resurrects its intermediate projection
        // on clients and makes the transaction's integrity digest disagree.
        accumulate_projection_delta(&mut upserts, &mut deleted, "a", Some(&intermediate));
        accumulate_projection_delta(&mut upserts, &mut deleted, "a", None);
        assert!(upserts.is_empty());
        assert_eq!(deleted, BTreeSet::from(["a".to_owned()]));
        // A later write in the same intent can also restore visibility.
        accumulate_projection_delta(&mut upserts, &mut deleted, "a", Some(&intermediate));
        assert!(deleted.is_empty());
        assert_eq!(upserts.get("a"), Some(&intermediate));
    }

    #[test]
    fn normalized_changes_union_visible_projections_and_preserve_other_memberships() {
        let key = ("tasks".to_owned(), "a".to_owned());
        let mut changes = BTreeMap::from([(
            key.clone(),
            ReplicaChange {
                entity: "tasks".to_owned(),
                id: "a".to_owned(),
                operation: "delete".to_owned(),
                old_value: Some(serde_json::json!({"id":"a","status":"new"})),
                new_value: None,
                changed_columns: vec!["status".to_owned()],
            },
        )]);
        let empty = BTreeMap::new();
        let names = BTreeMap::from([("a".to_owned(), serde_json::json!({"id":"a","name":"Task"}))]);
        let statuses = BTreeMap::from([(
            "a".to_owned(),
            serde_json::json!({"id":"a","status":"working","note":null}),
        )]);
        normalize_entity_changes(
            &mut changes,
            [("tasks", &empty), ("tasks", &names), ("tasks", &statuses)].into_iter(),
        );
        assert_eq!(changes[&key].operation, "update");
        assert_eq!(
            changes[&key].new_value,
            Some(serde_json::json!({"id":"a","name":"Task","status":"working","note":null}))
        );
        normalize_entity_changes(&mut changes, [("tasks", &empty)].into_iter());
        assert_eq!(changes[&key].operation, "delete");
        assert_eq!(changes[&key].new_value, None);
    }

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
