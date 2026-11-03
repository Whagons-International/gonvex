//! Projection wakes follow the visibility of their root entity, not their hints.
use crate::visibility::{self, VisibilityPlan};
use crate::visibility_index::ParentAclIndex;
use gonvex_postgres::{TenantSession, TenantTransaction};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

type SourceSlot = Arc<tokio::sync::Mutex<Option<SourceProof>>>;

#[derive(Default)]
pub(super) struct ProjectionCatalog {
    // One current proof per tenant route; module replacement clears the map.
    entries: tokio::sync::Mutex<BTreeMap<String, SourceSlot>>,
    required_roots: Mutex<BTreeMap<String, BTreeSet<(String, String)>>>,
}

struct SourceProof {
    epoch: String,
    module_hash: String,
    tracked: BTreeSet<String>,
}

impl ProjectionCatalog {
    pub async fn clear(&self) {
        self.entries.lock().await.clear();
        self.required_roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub fn root_required(
        &self,
        module_hash: &str,
        plans: &BTreeMap<String, VisibilityPlan>,
        key: &(String, String),
    ) -> Result<bool, visibility::VisibilityError> {
        let mut roots = self
            .required_roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(needed) = roots.get(module_hash) {
            return Ok(needed.contains(key));
        }
        let mut needed = BTreeSet::new();
        for plan in plans.values() {
            needed.extend(visibility::required_indexes(plans, plan)?);
        }
        let result = needed.contains(key);
        roots.insert(module_hash.into(), needed);
        Ok(result)
    }

    pub async fn tracked(
        &self,
        transaction: &mut TenantTransaction,
        session: &TenantSession,
        epoch: &str,
        module_hash: &str,
        sources: &BTreeSet<String>,
        metrics: &crate::visibility_metrics::VisibilityMetrics,
    ) -> Result<BTreeSet<String>, sqlx::Error> {
        let key = serde_json::to_string(&(
            &session.route.project_id,
            &session.route.tenant_id,
            &session.route.database_url,
        ))
        .expect("a string-only route key is serializable");
        let slot = self.entries.lock().await.entry(key).or_default().clone();
        let mut proof = slot.lock().await;
        if let Some(proof) = proof
            .as_ref()
            .filter(|proof| proof.epoch == epoch && proof.module_hash == module_hash)
        {
            return Ok(proof.tracked.clone());
        }
        let tables = sources.iter().cloned().collect::<Vec<_>>();
        let names = |suffix| {
            tables
                .iter()
                .map(|table| artifact_name(table, suffix))
                .collect::<Vec<_>>()
        };
        // Match the CDC trigger contract installed after tenant migrations.
        // Views and untracked relations retain conservative dependency wakes.
        metrics.record_visibility_sql_statement();
        let rows = sqlx::query_scalar::<_, String>(r#"
            SELECT source.table_name
            FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
              source(table_name, stage_function, stage_trigger, finalize_trigger)
            JOIN pg_class relation ON relation.relname = source.table_name
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = current_schema() AND relation.relkind IN ('r','p')
              AND EXISTS (SELECT FROM pg_trigger
                WHERE tgrelid=relation.oid AND tgname=source.stage_trigger
                  AND tgenabled='O' AND tgtype=29
                  AND tgfoid=to_regprocedure(format('%I.%I()',current_schema(),source.stage_function)))
              AND EXISTS (SELECT FROM pg_trigger
                WHERE tgrelid=relation.oid AND tgname=source.finalize_trigger
                  AND tgenabled='O' AND tgtype=29 AND tgdeferrable AND tginitdeferred
                  AND tgfoid=to_regprocedure(format('%I.gonvex_sync_finalize_transaction()',current_schema())))
        "#).bind(&tables).bind(names("stage")).bind(names("stage_trigger"))
            .bind(names("finalize_trigger"))
            .fetch_all(&mut **transaction.transaction()).await?;
        let tracked = rows.into_iter().collect::<BTreeSet<_>>();
        *proof = Some(SourceProof {
            epoch: epoch.into(),
            module_hash: module_hash.into(),
            tracked: tracked.clone(),
        });
        Ok(tracked)
    }
}

// Same deterministic identifier contract as postgres::install_table_trigger.
fn artifact_name(table: &str, suffix: &str) -> String {
    use sha2::{Digest, Sha256};
    let full = format!("gonvex_sync_{table}_{suffix}");
    if full.len() <= 63 {
        return full;
    }
    let hash = Sha256::digest(full.as_bytes());
    let suffix = hash[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{}_{suffix}", &full[..50])
}

#[derive(Clone, Debug, Default)]
pub(super) struct ProjectionRouting {
    pub tracked: bool,
    pub root_index: Option<Arc<RwLock<ParentAclIndex>>>,
    pub(super) held_ids: Arc<BTreeSet<String>>,
}

impl ProjectionRouting {
    pub async fn ready(&self, revision: u64, epoch: &str) -> bool {
        match &self.root_index {
            Some(index) => crate::visibility_index::ready_at(index, revision, epoch).await,
            None => true,
        }
    }
}
