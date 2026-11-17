//! Projection wakes follow the visibility of their root entity, not their hints.
use crate::visibility::{self, VisibilityPlan};
use crate::visibility_index::ParentAclIndex;
use gonvex_store::{TenantSession, TenantTransaction};
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
        // A source is tracked when it is a table carrying all three change
        // capture triggers installed after tenant migrations (see
        // gonvex_store::feed). Views and untracked relations retain
        // conservative dependency wakes.
        let candidates = sources
            .iter()
            .map(|table| {
                let [insert, update, delete] = gonvex_store::feed::capture_trigger_names(table);
                serde_json::json!([table, insert, update, delete])
            })
            .collect::<Vec<_>>();
        metrics.record_visibility_sql_statement();
        let rows = sqlx::query_scalar::<_, String>(
            r#"SELECT source.value->>0
               FROM json_each($1) AS source
               WHERE EXISTS (SELECT 1 FROM sqlite_schema
                             WHERE type = 'table' AND name = source.value->>0)
                 AND (SELECT count(*) FROM sqlite_schema AS t
                      WHERE t.type = 'trigger' AND t.tbl_name = source.value->>0
                        AND t.name IN (source.value->>1, source.value->>2, source.value->>3)
                        AND t.sql LIKE '%' || $2 || '(%') = 3"#,
        )
        .bind(serde_json::Value::Array(candidates).to_string())
        .bind(gonvex_store::CAPTURE_FUNCTION)
        .fetch_all(&mut **transaction.transaction())
        .await?;
        let tracked = rows.into_iter().collect::<BTreeSet<_>>();
        *proof = Some(SourceProof {
            epoch: epoch.into(),
            module_hash: module_hash.into(),
            tracked: tracked.clone(),
        });
        Ok(tracked)
    }
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
