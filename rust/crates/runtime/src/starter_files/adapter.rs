//! SQL streaming and fresh-authority admission for private local starters.
use super::*;
use crate::starter_files::diagnostics::{Scope, Site};
use crate::starter_files::{Authority, Build, Identity, Manifest, Piece, Store};
use std::{io, sync::Arc};

/// Supplied only by a reviewed trusted collection binder, never client args.
/// Unclassified and member-list providers cannot participate in shared files.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CollectionUse {
    SharedBase,
    PerUserList,
    Unsupported,
}

fn reject(_: io::Error) -> LocalSnapshotError {
    LocalSnapshotError::Rejected("starter unavailable; use normal cold open")
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
pub(super) async fn authority(
    tx: &mut TenantTransaction,
    registration: &LocalSnapshotRegistration,
    session: &TenantSession,
) -> Result<Authority, LocalSnapshotError> {
    // Equal filters may encode assigned-to-me/own-record lists. Until a reviewed
    // binder proves otherwise, every parameterized list uses normal opens.
    if !registration.definition.equal_filters.is_empty() {
        return Err(LocalSnapshotError::Rejected(
            "parameterized starter unsupported",
        ));
    }
    let timing = Scope::new(Site::AuthoritySchema);
    let schema = source_schema(tx, &registration.definition).await?;
    if schema != registration.schema {
        return Err(LocalSnapshotError::Rejected("starter schema changed"));
    }
    drop(timing);
    let timing = Scope::new(Site::AuthoritySources);
    verify_source_registration(tx, registration).await?;
    drop(timing);
    let timing = Scope::new(Site::AuthorityGuards);
    indexes::verify_local_rows(
        tx.transaction(),
        registration.binding.scope_id,
        &registration.definition.table,
    )
    .await?;
    drop(timing);
    let timing = Scope::new(Site::AuthorityPrepare);
    let prepared = query::prepare_projection(
        tx,
        session,
        &registration.binding,
        false,
        &projection_version(&registration.definition, &schema),
    )
    .await?;
    drop(timing);
    let _stamp = Scope::new(Site::AuthorityStamp);
    let clock = clock_in_transaction(tx).await?;
    let now = now_ms();
    Ok(Authority {
        identity: Identity::new(
            &prepared.identity.exact,
            format!("{}:{}", registration.generation, registration.artifact_hash),
            prepared.identity.generation,
            prepared.identity.change_serial,
            serde_json::to_vec(&(
                &registration.binding.authority,
                &registration.manifest_identity,
                &registration.source_schemas,
                &registration.source_guards,
            ))
            .expect("source authority serializes"),
        ),
        epoch: clock.epoch,
        revision: clock.revision,
        retained_revision: clock.retained_revision,
        index_ready: true,
        dependency_complete: true,
        active_account: true,
        active_member: true,
        supported_shared_collection: true,
        now_ms: now,
        valid_until_ms: now.saturating_add(60_000),
    })
}
impl Runtime {
    /// Explicit post-normal-load hook for reviewed local bindings only. Returning
    /// the handle never waits for SQL, compression, or disk. Aborting
    /// the handle cancels its queued build through the offload guard.
    pub(crate) fn spawn_local_starter_after_miss(
        &self,
        registration: Arc<LocalSnapshotRegistration>,
        session: TenantSession,
        store: Arc<Store>,
        collection_use: CollectionUse,
    ) -> Option<tokio::task::JoinHandle<Result<Manifest, LocalSnapshotError>>> {
        let queue_permit = store.try_queue_permit()?;
        let runtime = self.clone();
        Some(tokio::spawn(async move {
            let _queue_permit = queue_permit;
            crate::offload::offload()
                .spawn(async move {
                    if crate::offload::offload().is_inline()
                        || collection_use != CollectionUse::SharedBase
                    {
                        return Err(LocalSnapshotError::Rejected(
                            "starter provider unsupported/member list",
                        ));
                    }
                    let control = runtime
                        .inner
                        .control_plane
                        .read()
                        .await
                        .clone()
                        .ok_or(ReplicaError::ModuleMissing)?;
                    let mut tx = control
                        .begin_tenant_transaction(&session.route, true)
                        .await?;
                    let a = authority(&mut tx, &registration, &session).await?;
                    tx.commit().await?;
                    if let Ok(manifest) = runtime
                        .read_local_starter(
                            &registration,
                            &session,
                            &store,
                            |_, _| Ok(()),
                            collection_use,
                        )
                        .await
                    {
                        return Ok(manifest);
                    }
                    if !store.enqueue_after_miss(&a) {
                        return Err(LocalSnapshotError::Rejected(
                            "starter queue full or singleflight",
                        ));
                    }
                    let build = store
                        .take(&a.identity)
                        .ok_or(LocalSnapshotError::Rejected("starter cancelled"))?;
                    let _permit = store.build_permit().await.map_err(reject)?;
                    runtime
                        .build_local_starter(&registration, &session, build, collection_use)
                        .await
                })
                .await
        }))
    }

    /// Local experiment only. Run by a low-priority background worker AFTER a
    /// successful normal first load/miss, never awaited on the open request.
    pub(crate) async fn build_local_starter(
        &self,
        registration: &LocalSnapshotRegistration,
        session: &TenantSession,
        mut build: Build,
        collection_use: CollectionUse,
    ) -> Result<Manifest, LocalSnapshotError> {
        if collection_use != CollectionUse::SharedBase {
            return Err(LocalSnapshotError::Rejected(
                "shared starter requires reviewed base-collection binder",
            ));
        }
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        // Validate account at start. Final access/publication also serializes
        // against the landlord writer below, so account disable cannot race it.
        control
            .tenant_session_for_account(
                &session.identity.project_id,
                &session.route.tenant_id,
                &session.identity.account.id,
            )
            .await?;
        let mut tx = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        let a = authority(&mut tx, registration, session).await?;
        build.begin(&a).map_err(reject)?;
        let prepared = query::prepare_projection(
            &mut tx,
            session,
            &registration.binding,
            false,
            &registration.binding.scope.projection_version,
        )
        .await?;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        scan_local_window(
            &mut tx,
            &registration.definition,
            &prepared,
            &serde_json::json!({}),
            |row| {
                if registration.transport_proof.is_some() {
                    rows += 1;
                    bytes = bytes.saturating_add(
                        serde_json::to_vec(&row.row)
                            .map_err(|_| ReplicaError::InvalidArguments)?
                            .len(),
                    );
                    if rows > crate::starter_files::format::MAX_ROWS as usize
                        || bytes > crate::starter_files::format::MAX_TOTAL as usize
                    {
                        return Err(ReplicaError::InvalidArguments);
                    }
                }
                build
                    .row(&row.row)
                    .map_err(|_| ReplicaError::InvalidArguments)
            },
        )
        .await?;
        tx.commit().await?;
        self.with_local_starter_admission(registration, session, |fresh| {
            build.publish(|| Ok(fresh.clone()))
        })
        .await
    }

    /// Encoded pieces only; subscription envelopes and session cursors are not
    /// shared. The returned manifest stamps the catchup starting cursor.
    pub(crate) async fn read_local_starter(
        &self,
        registration: &LocalSnapshotRegistration,
        session: &TenantSession,
        store: &Arc<Store>,
        visit: impl FnMut(&Piece, &[u8]) -> io::Result<()>,
        collection_use: CollectionUse,
    ) -> Result<Manifest, LocalSnapshotError> {
        if collection_use != CollectionUse::SharedBase {
            return Err(LocalSnapshotError::Rejected(
                "member-list/unsupported provider uses normal open",
            ));
        }
        self.with_local_starter_admission(registration, session, |fresh| {
            store.read(&fresh.identity, || Ok(fresh.clone()), visit)
        })
        .await
    }

    pub(crate) async fn read_local_starter_piece(
        &self,
        registration: &LocalSnapshotRegistration,
        session: &TenantSession,
        store: &Arc<Store>,
        requested_hash: &str,
        visit: impl FnMut(&Piece, &[u8]) -> io::Result<()>,
        collection_use: CollectionUse,
    ) -> Result<Manifest, LocalSnapshotError> {
        if collection_use != CollectionUse::SharedBase {
            return Err(LocalSnapshotError::Rejected(
                "piece requires reviewed shared base collection",
            ));
        }
        self.with_local_starter_admission(registration, session, |fresh| {
            store.read_selected_piece(&fresh.identity, requested_hash, || Ok(fresh.clone()), visit)
        })
        .await
    }

    pub(super) async fn with_local_starter_admission<T>(
        &self,
        registration: &LocalSnapshotRegistration,
        session: &TenantSession,
        use_starter: impl FnOnce(&Authority) -> io::Result<T>,
    ) -> Result<T, LocalSnapshotError> {
        self.with_local_starter_admission_guarded(registration, session, None, use_starter)
            .await
    }

    pub(super) async fn with_local_starter_admission_guarded<T>(
        &self,
        registration: &LocalSnapshotRegistration,
        session: &TenantSession,
        native_auth: Option<(&str, &str, &str)>,
        use_starter: impl FnOnce(&Authority) -> io::Result<T>,
    ) -> Result<T, LocalSnapshotError> {
        let module_timing = Scope::new(Site::AdmissionModule);
        // Module guard freezes activation until all bytes/publication complete.
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        if module.generation != registration.generation
            || module.artifact_hash != registration.artifact_hash
        {
            return Err(LocalSnapshotError::Rejected("starter module changed"));
        }
        let current = module
            .functions
            .get(&registration.binding.scope.collection)
            .and_then(|f| f.replica.as_ref())
            .ok_or(LocalSnapshotError::Rejected("starter collection removed"))?;
        if serde_json::to_vec(current).unwrap()
            != serde_json::to_vec(&registration.definition).unwrap()
        {
            return Err(LocalSnapshotError::Rejected("starter projection changed"));
        }
        if let Some(expected) = &registration.manifest_identity {
            let manifest = self
                .inner
                .modules
                .permission_manifest(&module)
                .await
                .ok_or(LocalSnapshotError::Rejected("starter policy removed"))?;
            if stable_json(&manifest).as_bytes() != expected {
                return Err(LocalSnapshotError::Rejected("starter policy changed"));
            }
        }
        if let Some(proof) = &registration.transport_proof {
            let raw = self
                .inner
                .modules
                .permission_manifest(&module)
                .await
                .ok_or(LocalSnapshotError::Rejected("transport source removed"))?;
            let plan = module
                .visibility
                .get(&registration.definition.table)
                .ok_or(LocalSnapshotError::Rejected("transport policy removed"))?;
            if serde_json::to_vec(&raw).unwrap().as_slice() != proof.manifest()
                || serde_json::to_vec(plan).unwrap().as_slice() != proof.plan()
                || serde_json::to_vec(&registration.definition)
                    .unwrap()
                    .as_slice()
                    != proof.definition()
            {
                return Err(LocalSnapshotError::Rejected(
                    "complete provider authority changed",
                ));
            }
        }
        drop(module_timing);
        let native_timing = Scope::new(Site::AdmissionNative);
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        // One writer per SQLite DB. Landlord -> tenant is the fixed order.
        // These read-only uses of writer connections serialize account disable
        // and tenant revocation through publication / synchronous piece visitor.
        let pool = control.pools().pool(control.database_url()).await?;
        let mut landlord = pool.begin().await?;
        let active: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts WHERE id=?1 AND auth_realm_id=?2 AND disabled_at IS NULL)")
            .bind(&session.identity.account.id).bind(&session.identity.project_id).fetch_one(&mut *landlord).await?;
        let tenant_active: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM gonvex_runtime_tenants WHERE tenant_id=?1 AND project_id=?2 AND status='active' AND deleted_at IS NULL AND database_url=?3)")
            .bind(&session.route.tenant_id).bind(&session.identity.project_id).bind(&session.route.database_url).fetch_one(&mut *landlord).await?;
        if tenant_active != 1 {
            return Err(LocalSnapshotError::Rejected("starter tenant inactive"));
        }
        if active != 1 {
            return Err(LocalSnapshotError::Rejected("starter account disabled"));
        }
        if session.route.database_url == control.database_url() {
            return Err(LocalSnapshotError::Rejected(
                "landlord cannot be a starter tenant",
            ));
        }
        let auth_expiry = if let Some((hash, project, account)) = native_auth {
            if project != session.identity.project_id || account != session.identity.account.id {
                return Err(LocalSnapshotError::Rejected("starter auth scope mismatch"));
            }
            let expiry:Option<i64>=sqlx::query_scalar("SELECT expires_at FROM gonvex_auth_sessions WHERE token_hash=?1 AND project_id=?2 AND account_id=?3 AND revoked_at IS NULL AND expires_at>CAST(unixepoch('subsec')*1000 AS INTEGER)").bind(hash).bind(project).bind(account).fetch_optional(&mut *landlord).await?;
            Some(
                expiry
                    .filter(|n| *n > 0)
                    .ok_or(LocalSnapshotError::Rejected(
                        "starter native session revoked/expired",
                    ))? as u64,
            )
        } else {
            None
        };
        let mut tx = control
            .begin_tenant_transaction(&session.route, false)
            .await?;
        drop(native_timing);
        let mut fresh = authority(&mut tx, registration, session).await?;
        if let Some(expiry) = auth_expiry {
            fresh.valid_until_ms = fresh.valid_until_ms.min(expiry);
        }
        let result = use_starter(&fresh).map_err(reject);
        // No writes are intended; rollback prevents accidental change publication.
        tx.rollback().await?;
        landlord.rollback().await?;
        result
    }
}
