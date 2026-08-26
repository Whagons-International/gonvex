//! One durable change-feed reader per active tenant database.

use std::collections::BTreeMap;
use std::time::Duration;

use gonvex_postgres::{PoolRegistry, TenantRoute};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::Row;
use tokio::sync::{broadcast, watch, RwLock};

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
    pub old_value: Value,
    pub new_value: Value,
    pub provenance: TransactionProvenance,
}

#[derive(Clone, Debug, Default)]
pub struct TransactionProvenance {
    pub root_command_id: String,
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
        changes: Vec<LogChange>,
    },
    Reset {
        reason: String,
    },
}

pub struct ChangeFeedHub {
    pools: PoolRegistry,
    feeds: RwLock<BTreeMap<String, broadcast::Sender<FeedEvent>>>,
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

    pub async fn subscribe(&self, route: &TenantRoute) -> broadcast::Receiver<FeedEvent> {
        let key = format!("{}\0{}", route.project_id, route.tenant_id);
        if let Some(sender) = self.feeds.read().await.get(&key) {
            return sender.subscribe();
        }
        let mut feeds = self.feeds.write().await;
        if let Some(sender) = feeds.get(&key) {
            return sender.subscribe();
        }
        let (sender, receiver) = broadcast::channel(1024);
        feeds.insert(key, sender.clone());
        let route = route.clone();
        let pools = self.pools.clone();
        let shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            run_feed(route, pools, sender, shutdown).await;
        });
        receiver
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

async fn run_feed(
    route: TenantRoute,
    pools: PoolRegistry,
    sender: broadcast::Sender<FeedEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut last_prune = std::time::Instant::now() - Duration::from_secs(60 * 60);
    let mut last_revision = match read_clock(&pools, &route).await {
        // Start at the durable retention boundary rather than "now". A
        // subscription snapshot can race listener startup; replay plus the
        // subscription cursor is what proves no commit was skipped.
        Ok(clock) => clock.retained_revision,
        Err(error) => {
            let _ = sender.send(FeedEvent::Reset {
                reason: format!("change feed clock unavailable: {error}"),
            });
            0
        }
    };
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut listener = match PgListener::connect(&route.database_url).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = sender.send(FeedEvent::Reset {
                    reason: format!("change feed listener unavailable: {error}"),
                });
                if wait_or_shutdown(&mut shutdown, Duration::from_secs(1)).await {
                    return;
                }
                continue;
            }
        };
        if let Err(error) = listener.listen("gonvex_change_feed").await {
            let _ = sender.send(FeedEvent::Reset {
                reason: format!("change feed LISTEN failed: {error}"),
            });
            if wait_or_shutdown(&mut shutdown, Duration::from_secs(1)).await {
                return;
            }
            continue;
        }
        match deliver_committed(&pools, &route, last_revision, &sender).await {
            Ok(revision) => last_revision = revision,
            Err(error) => {
                let _ = sender.send(FeedEvent::Reset {
                    reason: format!("change feed startup recovery failed: {error}"),
                });
                continue;
            }
        }
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                notification = listener.recv() => {
                    if notification.is_err() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            match deliver_committed(&pools, &route, last_revision, &sender).await {
                Ok(revision) => last_revision = revision,
                Err(error) => {
                    let _ = sender.send(FeedEvent::Reset {
                        reason: format!("change feed recovery failed: {error}"),
                    });
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

async fn prune_retained(
    pools: &PoolRegistry,
    route: &TenantRoute,
) -> Result<(), gonvex_postgres::DatabaseError> {
    let pool = pools.pool(&route.database_url).await?;
    let _admission = pools.admit().await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('_gonvex_sync_changes_prune'))")
        .execute(&mut *transaction)
        .await?;
    let pruned: Option<i64> = sqlx::query_scalar(
        r#"WITH removed AS (
             DELETE FROM _gonvex_sync_changes
             WHERE created_at < clock_timestamp() - interval '30 days'
             RETURNING revision
           ) SELECT max(revision) FROM removed"#,
    )
    .fetch_one(&mut *transaction)
    .await?;
    if let Some(revision) = pruned {
        sqlx::query(
            "DELETE FROM _gonvex_sync_transactions WHERE revision <= $1 AND NOT EXISTS (SELECT 1 FROM _gonvex_sync_changes changes WHERE changes.revision=_gonvex_sync_transactions.revision)",
        )
        .bind(revision)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE _gonvex_sync_clock SET retained_revision=greatest(retained_revision,$1) WHERE singleton=true",
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
    sender: &broadcast::Sender<FeedEvent>,
) -> Result<u64, gonvex_postgres::DatabaseError> {
    let clock = read_clock(pools, route).await?;
    if after < clock.retained_revision {
        let _ = sender.send(FeedEvent::Reset {
            reason: "change feed cursor expired".to_owned(),
        });
        return Ok(clock.revision);
    }
    if clock.revision <= after {
        return Ok(clock.revision);
    }
    let changes = read_changes(pools, route, after, clock.revision).await?;
    let mut current_revision = 0;
    let mut batch = Vec::new();
    for change in changes {
        if current_revision != 0 && change.revision != current_revision {
            let _ = sender.send(FeedEvent::Transaction {
                database_epoch: clock.epoch.clone(),
                revision: current_revision,
                changes: std::mem::take(&mut batch),
            });
        }
        current_revision = change.revision;
        batch.push(change);
    }
    if !batch.is_empty() {
        let _ = sender.send(FeedEvent::Transaction {
            database_epoch: clock.epoch,
            revision: current_revision,
            changes: batch,
        });
    }
    Ok(clock.revision)
}

pub async fn read_clock(
    pools: &PoolRegistry,
    route: &TenantRoute,
) -> Result<ReplicaClock, gonvex_postgres::DatabaseError> {
    let pool = pools.pool(&route.database_url).await?;
    let _admission = pools.admit().await?;
    let row = sqlx::query(
        "SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = true",
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
) -> Result<Vec<LogChange>, gonvex_postgres::DatabaseError> {
    let pool = pools.pool(&route.database_url).await?;
    let _admission = pools.admit().await?;
    let rows = sqlx::query(
        r#"SELECT changes.revision, changes.ordinal, COALESCE(changes.command_id, '') AS command_id,
                  table_name, row_id, operation,
                  COALESCE(changed_columns, ARRAY[]::text[]) AS changed_columns,
                  COALESCE(old_value, 'null'::jsonb) AS old_value,
                  COALESCE(new_value, 'null'::jsonb) AS new_value,
                  COALESCE(tx.root_command_id,COALESCE(changes.command_id,'')) AS root_command_id,
                  COALESCE(tx.invocation_channel,'system') AS invocation_channel,
                  tx.actor_account_id,tx.actor_member_id,tx.on_behalf_of_member_id,tx.agent_execution_id
           FROM _gonvex_sync_changes changes
           LEFT JOIN _gonvex_sync_transactions tx ON tx.revision=changes.revision
           WHERE changes.revision > $1 AND changes.revision <= $2
           ORDER BY changes.revision, changes.ordinal"#,
    )
    .bind(after as i64)
    .bind(through as i64)
    .fetch_all(&pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| LogChange {
            revision: row.get::<i64, _>("revision").max(0) as u64,
            ordinal: row.get("ordinal"),
            origin_command_id: row.get("command_id"),
            table: row.get("table_name"),
            row_id: row.get("row_id"),
            operation: row.get("operation"),
            changed_columns: row.get("changed_columns"),
            old_value: row.get("old_value"),
            new_value: row.get("new_value"),
            provenance: TransactionProvenance {
                root_command_id: row.get("root_command_id"),
                channel: row.get("invocation_channel"),
                actor_account_id: row.get("actor_account_id"),
                actor_member_id: row.get("actor_member_id"),
                on_behalf_of_member_id: row.get("on_behalf_of_member_id"),
                agent_execution_id: row.get("agent_execution_id"),
            },
        })
        .collect())
}
