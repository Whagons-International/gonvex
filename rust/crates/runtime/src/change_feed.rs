//! One durable change-feed reader per active tenant database.

use std::collections::BTreeMap;
use std::time::Duration;

use gonvex_store::{PoolRegistry, TenantRoute};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use tokio::sync::{watch, RwLock};

pub mod backlog;
pub use backlog::FeedReceiver;
use backlog::FeedStore;

#[derive(Clone, Debug)]
pub struct ReplicaClock {
    pub epoch: String,
    pub revision: u64,
    pub retained_revision: u64,
}

#[derive(Clone, Debug)]
pub struct LogChange {
    pub revision: u64,
    pub ordinal: i32,
    pub origin_command_id: String,
    pub table: String,
    pub row_id: String,
    pub operation: String,
    pub changed_columns: Vec<String>,
    /// Row images, shared: merged catch-up events and refilled copies
    /// reference the same parsed JSON instead of deep-copying it.
    pub old_value: std::sync::Arc<Value>,
    pub new_value: std::sync::Arc<Value>,
    pub provenance: TransactionProvenance,
}

#[derive(Clone, Debug, Default)]
pub struct TransactionProvenance {
    pub root_command_id: String,
    pub root_channel: String,
    pub channel: String,
    pub actor_account_id: Option<String>,
    pub actor_member_id: Option<String>,
    pub on_behalf_of_member_id: Option<String>,
    pub agent_execution_id: Option<String>,
}

#[derive(Clone, Debug)]
pub enum FeedEvent {
    Transaction {
        database_epoch: String,
        revision: u64,
        /// Shared by every receiver of the broadcast: a connection's copy of
        /// the event is a reference count, not a deep copy of every row image.
        changes: std::sync::Arc<Vec<LogChange>>,
    },
    Reset {
        reason: String,
    },
}

/// Delivery v3, slow connections: consecutive transactions of one database
/// epoch as one event (changes in commit order, the last revision). Applying
/// it collapses every row to its first before-image and last after-image, so
/// a connection that fell behind receives each row's newest state once
/// instead of every intermediate one. The caller must only merge when no
/// subscription's cursor lies inside the merged range (a subscription skips
/// whole events at or before its cursor). `None` unless all are
/// transactions of one epoch in ascending revision order.
pub fn merge_transactions(events: &[FeedEvent]) -> Option<FeedEvent> {
    let mut changes = Vec::new();
    let mut epoch: Option<&str> = None;
    let mut last = 0u64;
    for event in events {
        let FeedEvent::Transaction {
            database_epoch,
            revision,
            changes: event_changes,
        } = event
        else {
            return None;
        };
        if epoch.is_some_and(|epoch| epoch != database_epoch) || *revision <= last {
            return None;
        }
        epoch = Some(database_epoch);
        last = *revision;
        changes.extend(event_changes.iter().cloned());
    }
    Some(FeedEvent::Transaction {
        database_epoch: epoch?.to_owned(),
        revision: last,
        changes: std::sync::Arc::new(changes),
    })
}

/// The first revision of a feed event (`None` for a reset).
pub fn first_revision(event: &FeedEvent) -> Option<u64> {
    match event {
        FeedEvent::Transaction {
            revision, changes, ..
        } => Some(
            changes
                .iter()
                .map(|change| change.revision)
                .min()
                .unwrap_or(*revision)
                .min(*revision),
        ),
        FeedEvent::Reset { .. } => None,
    }
}

pub struct ChangeFeedHub {
    pools: PoolRegistry,
    feeds: RwLock<BTreeMap<String, std::sync::Arc<FeedStore>>>,
    shutdown: watch::Sender<bool>,
}

impl ChangeFeedHub {
    pub fn new(pools: PoolRegistry) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            pools,
            feeds: RwLock::new(BTreeMap::new()),
            shutdown,
        }
    }

    pub async fn subscribe(&self, route: &TenantRoute) -> FeedReceiver {
        let key = route.database_url.clone();
        if let Some(store) = self.feeds.read().await.get(&key) {
            return store.subscribe();
        }
        let mut feeds = self.feeds.write().await;
        if let Some(store) = feeds.get(&key) {
            return store.subscribe();
        }
        let store = FeedStore::new(self.pools.clone(), route.clone());
        feeds.insert(key, std::sync::Arc::clone(&store));
        // Capture the durable clock before returning the first receiver. Any
        // later Replica snapshot is necessarily at this revision or newer,
        // while the listener plus deliver_committed below closes the gap for commits
        // after it. Starting at retained_revision replayed the full 30-day log
        // into a brand-new connection, delaying current changes and potentially
        // overflowing the broadcast channel before subscriptions had opened.
        let receiver;
        let initial_revision = match read_clock(&self.pools, route).await {
            Ok(clock) => {
                store.set_initial_revision(clock.revision);
                receiver = store.subscribe();
                clock.revision
            }
            Err(error) => {
                receiver = store.subscribe();
                store.reset(format!("change feed clock unavailable: {error}"), None);
                0
            }
        };
        let route = route.clone();
        let pools = self.pools.clone();
        let shutdown = self.shutdown.subscribe();
        tokio::spawn(gonvex_profiler::instrument(
            gonvex_profiler::TaskKind::Feed,
            async move {
                run_feed(route, pools, store, shutdown, initial_revision).await;
            },
        ));
        receiver
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Connections subscribed to this database's feed, or 0 when the map is
    /// busy (profiler attribution only; never waits).
    pub fn receiver_count(&self, database_url: &str) -> usize {
        self.feeds
            .try_read()
            .ok()
            .and_then(|feeds| feeds.get(database_url).map(|store| store.receiver_count()))
            .unwrap_or(0)
    }
}

async fn run_feed(
    route: TenantRoute,
    pools: PoolRegistry,
    sender: std::sync::Arc<FeedStore>,
    mut shutdown: watch::Receiver<bool>,
    mut last_revision: u64,
) {
    let mut last_prune = std::time::Instant::now() - Duration::from_secs(60 * 60);
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut listener = match pools.listener(&route.database_url).await {
            Ok(listener) => listener,
            Err(error) => {
                sender.reset(format!("change feed listener unavailable: {error}"), None);
                if wait_or_shutdown(&mut shutdown, Duration::from_secs(1)).await {
                    return;
                }
                continue;
            }
        };
        if let Err(error) = listener
            .listen(gonvex_store::feed::CHANGE_FEED_CHANNEL)
            .await
        {
            sender.reset(format!("change feed LISTEN failed: {error}"), None);
            if wait_or_shutdown(&mut shutdown, Duration::from_secs(1)).await {
                return;
            }
            continue;
        }
        let recovery_started = std::time::Instant::now();
        let recovery_after = last_revision;
        match deliver_committed(&pools, &route, last_revision, &sender).await {
            Ok(revision) => {
                last_revision = revision;
                tracing::debug!(
                    project = %route.project_id,
                    tenant = %route.tenant_id,
                    wake_source = "reconnect",
                    prior_revision = recovery_after,
                    delivered_revision = revision,
                    delivery_ms = recovery_started.elapsed().as_secs_f64() * 1_000.0,
                    "change feed delivery"
                );
            }
            Err(error) => {
                sender.reset(
                    format!("change feed startup recovery failed: {error}"),
                    None,
                );
                continue;
            }
        }
        loop {
            let (wake_source, notified_revision) = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                    continue;
                }
                notification = listener.recv() => {
                    match notification {
                        Ok(notification) => {
                            let revision = serde_json::from_str::<Value>(notification.payload())
                                .ok()
                                .and_then(|payload| payload.get("revision").and_then(Value::as_u64));
                            ("notify", revision)
                        }
                        // The in-process channel overflowed: notifications were
                        // dropped, so repair from the durable log right away.
                        Err(gonvex_store::DatabaseError::ListenerLagged) => ("lagged", None),
                        Err(error) => {
                            tracing::warn!(
                                project = %route.project_id,
                                tenant = %route.tenant_id,
                                %error,
                                "change feed listener disconnected"
                            );
                            if wait_or_shutdown(&mut shutdown, Duration::from_secs(1)).await {
                                return;
                            }
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => ("repair", None),
            };
            if let Some(lag) = notified_revision.and_then(|revision| {
                gonvex_profiler::since_stamp(gonvex_profiler::stamp_key(
                    route.database_url.trim(),
                    revision,
                ))
            }) {
                gonvex_profiler::record(
                    gonvex_profiler::Stage::FeedNotifyLag,
                    gonvex_profiler::Label::None,
                    lag,
                );
            }
            let delivery_started = std::time::Instant::now();
            let delivery_after = last_revision;
            match deliver_committed(&pools, &route, last_revision, &sender).await {
                Ok(revision) => {
                    gonvex_profiler::record_duration(
                        gonvex_profiler::Stage::FeedRead,
                        gonvex_profiler::Label::None,
                        delivery_started.elapsed(),
                    );
                    last_revision = revision;
                    tracing::debug!(
                        project = %route.project_id,
                        tenant = %route.tenant_id,
                        wake_source,
                        notified_revision,
                        prior_revision = delivery_after,
                        delivered_revision = revision,
                        delivery_ms = delivery_started.elapsed().as_secs_f64() * 1_000.0,
                        "change feed delivery"
                    );
                }
                Err(error) => {
                    sender.reset(format!("change feed recovery failed: {error}"), None);
                    break;
                }
            }
            if last_prune.elapsed() >= Duration::from_secs(60 * 60) {
                if let Err(error) = prune_retained(&pools, &route).await {
                    tracing::warn!(project=%route.project_id, tenant=%route.tenant_id, %error, "prune change feed");
                }
                last_prune = std::time::Instant::now();
            }
        }
    }
}

/// Change-feed retention: 30 days, in milliseconds.
const RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;

pub(crate) async fn prune_retained(
    pools: &PoolRegistry,
    route: &TenantRoute,
) -> Result<(), gonvex_store::DatabaseError> {
    let _lock = pools
        .lock(&route.database_url, "_gonvex_sync_changes_prune")
        .await?;
    let pool = pools.pool(&route.database_url).await?;
    let _admission = pools.admit(&route.database_url).await?;
    let mut transaction = pool.begin().await?;
    let pruned: Option<i64> = sqlx::query_scalar(
        r#"SELECT max(revision) FROM _gonvex_sync_changes
           WHERE created_at < CAST(unixepoch('subsec') * 1000 AS INTEGER) - $1
             AND revision IS NOT NULL"#,
    )
    .bind(RETENTION_MS)
    .fetch_one(&mut *transaction)
    .await?;
    if let Some(revision) = pruned {
        sqlx::query(
            r#"DELETE FROM _gonvex_sync_changes
               WHERE created_at < CAST(unixepoch('subsec') * 1000 AS INTEGER) - $1
                 AND revision IS NOT NULL AND revision <= $2"#,
        )
        .bind(RETENTION_MS)
        .bind(revision)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM _gonvex_sync_transactions WHERE revision <= $1 AND NOT EXISTS (SELECT 1 FROM _gonvex_sync_changes changes WHERE changes.revision=_gonvex_sync_transactions.revision)",
        )
        .bind(revision)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE _gonvex_sync_clock SET retained_revision=max(retained_revision,$1) WHERE singleton=1",
        )
        .bind(revision)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

async fn deliver_committed(
    pools: &PoolRegistry,
    route: &TenantRoute,
    after: u64,
    sender: &FeedStore,
) -> Result<u64, gonvex_store::DatabaseError> {
    let clock = read_clock(pools, route).await?;
    if after < clock.retained_revision {
        sender.reset(
            "change feed cursor expired".to_owned(),
            Some(clock.revision),
        );
        return Ok(clock.revision);
    }
    if clock.revision <= after {
        return Ok(clock.revision);
    }
    let changes = read_changes(pools, route, after, clock.revision).await?;
    let epoch: std::sync::Arc<str> = std::sync::Arc::from(clock.epoch.as_str());
    let mut current_revision = 0;
    let mut batch = Vec::new();
    for change in changes {
        if current_revision != 0 && change.revision != current_revision {
            stamp_broadcast(&clock.epoch, current_revision);
            sender.publish(&epoch, current_revision, std::mem::take(&mut batch));
        }
        current_revision = change.revision;
        batch.push(change);
    }
    if !batch.is_empty() {
        stamp_broadcast(&clock.epoch, current_revision);
        sender.publish(&epoch, current_revision, batch);
    }
    Ok(clock.revision)
}

/// Marks when a transaction was broadcast so each connection can measure its
/// own queue wait and delivery time ([`broadcast_key`]).
fn stamp_broadcast(epoch: &str, revision: u64) {
    gonvex_profiler::stamp(broadcast_key(epoch, revision));
}

/// Profiler stamp key of a broadcast feed transaction.
pub(crate) fn broadcast_key(epoch: &str, revision: u64) -> u64 {
    gonvex_profiler::stamp_key(epoch, revision)
}

pub async fn read_clock(
    pools: &PoolRegistry,
    route: &TenantRoute,
) -> Result<ReplicaClock, gonvex_store::DatabaseError> {
    let pool = pools.reader(&route.database_url).await?;
    let _admission = pools.admit(&route.database_url).await?;
    let row = sqlx::query(
        "SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = 1",
    )
    .fetch_one(&pool)
    .await?;
    let revision: i64 = row.get("revision");
    let retained: i64 = row.get("retained_revision");
    Ok(ReplicaClock {
        epoch: row.get("epoch"),
        revision: revision.max(0) as u64,
        retained_revision: retained.max(0) as u64,
    })
}

pub async fn read_changes(
    pools: &PoolRegistry,
    route: &TenantRoute,
    after: u64,
    through: u64,
) -> Result<Vec<LogChange>, gonvex_store::DatabaseError> {
    let pool = pools.reader(&route.database_url).await?;
    let _admission = pools.admit(&route.database_url).await?;
    let rows = sqlx::query(&format!(
        "{CHANGE_COLUMNS} WHERE changes.revision > $1 AND changes.revision <= $2 ORDER BY changes.revision, changes.ordinal"
    ))
    .bind(after as i64)
    .bind(through as i64)
    .fetch_all(&pool)
    .await?;
    rows.iter().map(log_change_from_row).collect()
}

/// Change rows joined with their transaction provenance. `changed_columns`,
/// `old_value` and `new_value` are JSON text written by the capture triggers.
pub(crate) const CHANGE_COLUMNS: &str = r#"SELECT changes.revision, changes.ordinal, COALESCE(changes.command_id, '') AS command_id,
                  changes.table_name, changes.row_id, changes.operation,
                  COALESCE(changes.changed_columns, '[]') AS changed_columns,
                  changes.old_value AS old_value,
                  changes.new_value AS new_value,
                  COALESCE(tx.root_command_id,COALESCE(changes.command_id,'')) AS root_command_id,
                  COALESCE(tx.root_invocation_channel,tx.invocation_channel,'system') AS root_invocation_channel,
                  COALESCE(tx.invocation_channel,'system') AS invocation_channel,
                  tx.actor_account_id,tx.actor_member_id,tx.on_behalf_of_member_id,tx.agent_execution_id
           FROM _gonvex_sync_changes changes
           LEFT JOIN _gonvex_sync_transactions tx ON tx.revision=changes.revision"#;

fn json_text(row: &SqliteRow, column: &str) -> Result<Value, gonvex_store::DatabaseError> {
    let text: Option<String> = row.try_get(column)?;
    match text {
        None => Ok(Value::Null),
        Some(text) => serde_json::from_str(&text)
            .map_err(|error| gonvex_store::DatabaseError::Decode(format!("{column}: {error}"))),
    }
}

pub(crate) fn log_change_from_row(
    row: &SqliteRow,
) -> Result<LogChange, gonvex_store::DatabaseError> {
    let changed_columns = match json_text(row, "changed_columns")? {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    };
    Ok(LogChange {
        revision: row.try_get::<i64, _>("revision")?.max(0) as u64,
        ordinal: row.try_get::<Option<i64>, _>("ordinal")?.unwrap_or(0) as i32,
        origin_command_id: row.try_get("command_id")?,
        table: row.try_get("table_name")?,
        row_id: row.try_get("row_id")?,
        operation: row.try_get("operation")?,
        changed_columns,
        old_value: std::sync::Arc::new(json_text(row, "old_value")?),
        new_value: std::sync::Arc::new(json_text(row, "new_value")?),
        provenance: TransactionProvenance {
            root_command_id: row.try_get("root_command_id")?,
            root_channel: row.try_get("root_invocation_channel")?,
            channel: row.try_get("invocation_channel")?,
            actor_account_id: row.try_get("actor_account_id")?,
            actor_member_id: row.try_get("actor_member_id")?,
            on_behalf_of_member_id: row.try_get("on_behalf_of_member_id")?,
            agent_execution_id: row.try_get("agent_execution_id")?,
        },
    })
}
