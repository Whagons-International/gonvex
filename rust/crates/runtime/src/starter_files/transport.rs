//! Authenticated connection-scoped bootstrap. No public hash/file endpoint.
use super::starter_adapter::authority;
use super::*;
use crate::starter_files::diagnostics::{Scope, Site};
use crate::starter_files::{format, provider, Manifest, Store};
use base64::Engine;
use gonvex_protocol::{StarterOffer, StarterPiece};
use serde_json::json;
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

const GLOBAL_LIMIT: usize = 64 << 20;
static OFFER_BYTES: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
pub(crate) fn reset_budget_peak() {
    PEAK_BYTES.store(OFFER_BYTES.load(Ordering::Acquire), Ordering::Release);
}
#[cfg(test)]
pub(crate) fn process_budget_stats() -> (usize, usize, usize) {
    (
        OFFER_BYTES.load(Ordering::Acquire),
        PEAK_BYTES.load(Ordering::Acquire),
        GLOBAL_LIMIT,
    )
}
fn record_budget_peak() {
    #[cfg(test)]
    PEAK_BYTES.fetch_max(OFFER_BYTES.load(Ordering::Acquire), Ordering::Relaxed);
}
struct Reservation(AtomicUsize);
impl Reservation {
    fn bytes(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
    // Resizing is private to a not-yet-published singleflight factory.
    fn resize(&self, next: usize) -> io::Result<()> {
        let old = self.bytes();
        if next > old {
            OFFER_BYTES
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    v.checked_add(next - old).filter(|v| *v <= GLOBAL_LIMIT)
                })
                .map_err(|_| io::Error::other("artifact process budget"))?;
        } else {
            OFFER_BYTES.fetch_sub(old - next, Ordering::AcqRel);
        }
        record_budget_peak();
        self.0.store(next, Ordering::Release);
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        OFFER_BYTES.fetch_sub(self.bytes(), Ordering::AcqRel);
    }
}
fn reserve(n: usize) -> Result<Arc<Reservation>, LocalSnapshotError> {
    OFFER_BYTES
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            v.checked_add(n).filter(|v| *v <= GLOBAL_LIMIT)
        })
        .map_err(|_| LocalSnapshotError::Rejected("starter process budget"))?;
    record_budget_peak();
    Ok(Arc::new(Reservation(AtomicUsize::new(n))))
}
#[path = "transport_artifact.rs"]
mod artifact;

#[derive(Clone)]
struct Registered {
    registration: Arc<LocalSnapshotRegistration>,
    store: Arc<Store>,
    proof: Arc<provider::CompleteProvider>,
    column_types: BTreeMap<String, String>,
}
#[derive(Default)]
pub(crate) struct Registry(
    Mutex<BTreeMap<String, Registered>>,
    Mutex<Vec<tokio::task::JoinHandle<Result<Manifest, LocalSnapshotError>>>>,
    Mutex<artifact::Cache>,
);
fn registry_key(session: &TenantSession, path: &str) -> String {
    serde_json::json!([
        session.route.project_id,
        session.route.tenant_id,
        session.route.database_url,
        path
    ])
    .to_string()
}
struct Offer {
    registered: Registered,
    manifest: Arc<Manifest>,
    artifact: Arc<artifact::ValidatedArtifact>,
    public: StarterOffer,
    request: ReplicaOpenRequest,
    next: usize,
    generation: u64,
    _reservation: Arc<Reservation>,
}
pub(crate) struct Envelope {
    pub generation: u64,
    pub message: ServerMessage,
    _reservation: Arc<Reservation>,
}
#[derive(Clone)]
struct NativeAuth {
    hash: String,
    project: String,
    account: String,
}
impl NativeAuth {
    fn parts(&self) -> (&str, &str, &str) {
        (&self.hash, &self.project, &self.account)
    }
}
pub(crate) struct Connection {
    offers: BTreeMap<String, Offer>,
    pub generation: u64,
    pub negotiated: bool,
    auth: Option<NativeAuth>,
    tx: tokio::sync::mpsc::Sender<Envelope>,
    rx: tokio::sync::mpsc::Receiver<Envelope>,
}
impl Default for Connection {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        Self {
            offers: BTreeMap::new(),
            generation: 0,
            negotiated: false,
            auth: None,
            tx,
            rx,
        }
    }
}
impl Connection {
    pub fn bind_auth(&mut self, connection: &crate::control::ControlConnection) -> bool {
        self.auth = connection
            .identity
            .as_ref()
            .filter(|_| connection.auth_token.starts_with("gvx_session_"))
            .and_then(|identity| {
                connection
                    .tenant
                    .as_ref()
                    .filter(|s| !s.is_anonymous() && s.developer_session.is_none())?;
                Some(NativeAuth {
                    hash: gonvex_store::token_hash(&connection.auth_token),
                    project: identity.project_id.clone(),
                    account: identity.account.id.clone(),
                })
            });
        self.auth.is_some()
    }
    #[cfg(test)]
    pub(crate) fn budget_stats(&self) -> (usize, usize) {
        (
            self.offers.len(),
            self.offers
                .values()
                .map(|o| o._reservation.bytes() + o.artifact.charge())
                .sum(),
        )
    }
    pub fn is_empty(&self) -> bool {
        self.offers.is_empty()
    }
    pub fn retire(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.offers.clear();
        self.negotiated = false;
        self.auth = None;
        while self.rx.try_recv().is_ok() {}
    }
    pub fn close(&mut self, id: &str) {
        self.offers.remove(id);
        while self.rx.try_recv().is_ok() {}
    }
    pub fn fallback_request(&self, id: &str) -> Option<ReplicaOpenRequest> {
        let mut r = self.offers.get(id)?.request.clone();
        r.starter = None;
        Some(r)
    }
    pub fn pop(&mut self) -> Option<ServerMessage> {
        while let Ok(e) = self.rx.try_recv() {
            if e.generation == self.generation {
                return Some(e.message);
            }
        }
        None
    }
    pub fn cancel(&mut self, id: &str, offer_id: &str) -> Option<ReplicaOpenRequest> {
        if self
            .offers
            .get(id)
            .is_some_and(|o| o.public.offer_id == offer_id)
        {
            let mut request = self.offers.remove(id)?.request;
            request.starter = None;
            Some(request)
        } else {
            None
        }
    }
    pub fn expired(&mut self) -> Vec<(String, String, ReplicaOpenRequest)> {
        let now = now_ms();
        let ids: Vec<_> = self
            .offers
            .iter()
            .filter(|(_, o)| o.public.expires_at <= now)
            .map(|(id, _)| id.clone())
            .collect();
        ids.into_iter()
            .filter_map(|id| {
                let mut o = self.offers.remove(&id)?;
                o.request.starter = None;
                Some((id, o.public.offer_id, o.request))
            })
            .collect()
    }
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn reject(_: io::Error) -> LocalSnapshotError {
    LocalSnapshotError::Rejected("starter validation failed; normal open required")
}
#[cfg(test)]
pub(crate) static BEFORE_PIECE_ENQUEUE: Mutex<Option<Arc<dyn Fn() + Send + Sync>>> =
    Mutex::new(None);
fn enqueue(
    tx: &tokio::sync::mpsc::Sender<Envelope>,
    generation: u64,
    reservation: Arc<Reservation>,
    message: ServerMessage,
) -> io::Result<()> {
    #[cfg(test)]
    if matches!(message, ServerMessage::ReplicaStarterPiece { .. }) {
        let hook = BEFORE_PIECE_ENQUEUE.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }
    tx.try_send(Envelope {
        generation,
        message,
        _reservation: reservation,
    })
    .map_err(|_| io::Error::other("starter outbound credit exhausted"))
}
fn checked_hash(row: &Value, types: &BTreeMap<String, String>) -> io::Result<String> {
    let object = row
        .as_object()
        .ok_or_else(|| io::Error::other("row object"))?;
    for (key, kind) in types {
        let value = object
            .get(key)
            .ok_or_else(|| io::Error::other("projected column"))?;
        let valid = value.is_null()
            || match kind.as_str() {
                "text" => value.is_string(),
                "integer" => value
                    .as_i64()
                    .is_some_and(|n| n.unsigned_abs() <= 9_007_199_254_740_991),
                "real" => value.as_f64().is_some_and(f64::is_finite),
                "boolean" => value.is_boolean(),
                "json" => true,
                _ => false,
            };
        if !valid {
            return Err(io::Error::other("projected type unsupported"));
        }
    }
    Ok(row_hash(row))
}
impl Runtime {
    #[cfg(test)]
    pub(crate) fn hold_starter_factory(&self) -> impl Drop + '_ {
        self.inner.starter_transport.2.lock().unwrap()
    }
    #[cfg(test)]
    pub(crate) fn clear_starter_artifact_cache(&self) {
        self.inner.starter_transport.2.lock().unwrap().clear();
    }
    #[cfg(test)]
    pub(crate) fn starter_artifact_entries(&self) -> usize {
        self.inner.starter_transport.2.lock().unwrap().len()
    }
    /// Explicit trusted local provider registration, never an RPC/schema install.
    pub(crate) async fn register_local_starter_transport(
        &self,
        session: &TenantSession,
        registration: Arc<LocalSnapshotRegistration>,
        store: Arc<Store>,
    ) -> Result<(), LocalSnapshotError> {
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let raw = self
            .inner
            .modules
            .permission_manifest(&module)
            .await
            .ok_or(LocalSnapshotError::Rejected(
                "complete source manifest required",
            ))?;
        if registration.manifest_identity.as_deref() != Some(stable_json(&raw).as_bytes()) {
            return Err(LocalSnapshotError::Rejected(
                "legacy normalized receipt is not a provider certificate",
            ));
        }
        let plan = module
            .visibility
            .get(&registration.definition.table)
            .ok_or(ReplicaError::VisibilityMissing(
                registration.definition.table.clone(),
            ))?;
        let proof = provider::prove(
            &raw,
            &registration.binding.scope.collection,
            &registration.definition,
            plan,
        )
        .ok_or(LocalSnapshotError::Rejected(
            "provider NOT complete for loaded policy/collection",
        ))?;
        let mut owned = (*registration).clone();
        owned.transport_proof = Some(proof.clone());
        let registration = Arc::new(owned);
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut tx = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        authority(&mut tx, &registration, session).await?;
        let columns =
            gonvex_store::rows::table_columns(tx.transaction(), &registration.definition.table)
                .await?;
        let membership_columns =
            gonvex_store::rows::table_columns(tx.transaction(), &proof.grant_table()).await?;
        if membership_columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<BTreeSet<_>>()
            != *proof.grant_columns()
        {
            return Err(LocalSnapshotError::Rejected(
                "membership schema has unmodeled status/deletion/other inputs",
            ));
        }
        let column_types = registration
            .definition
            .columns
            .iter()
            .map(|key| {
                let c = columns
                    .iter()
                    .find(|c| c.name == *key)
                    .ok_or(LocalSnapshotError::Rejected("projected column absent"))?;
                let kind = match c.kind() {
                    gonvex_store::rows::JsonKind::Json => "json",
                    gonvex_store::rows::JsonKind::Boolean => "boolean",
                    gonvex_store::rows::JsonKind::Integer => "integer",
                    gonvex_store::rows::JsonKind::Real => "real",
                    gonvex_store::rows::JsonKind::Text => "text",
                };
                Ok((key.clone(), kind.into()))
            })
            .collect::<Result<_, LocalSnapshotError>>()?;
        tx.commit().await?;
        let mut registry = self.inner.starter_transport.0.lock().unwrap();
        if registry.len() >= 128 {
            return Err(LocalSnapshotError::Rejected(
                "starter provider registry full",
            ));
        }
        registry.insert(
            registry_key(session, &registration.binding.scope.collection),
            Registered {
                registration,
                store,
                proof: Arc::new(proof),
                column_types,
            },
        );
        Ok(())
    }
    pub(crate) async fn offer_starter(
        &self,
        connection: &mut Connection,
        session: &TenantSession,
        request: &ReplicaOpenRequest,
        sum: bool,
    ) -> Result<bool, LocalSnapshotError> {
        if !connection.negotiated
            || sum
            || request.starter.as_ref().is_some_and(|s| s.version != 1)
            || (request.starter.is_none() && request.cursor.is_none())
            || request.args != json!({})
            || request.full_integrity
            || !request.keys.is_empty()
            || !request.hashes.is_empty()
            || connection.offers.len() >= 8
        {
            return Ok(false);
        }
        let Some(auth) = connection.auth.clone() else {
            return Ok(false);
        };
        let registered = self
            .inner
            .starter_transport
            .0
            .lock()
            .unwrap()
            .get(&registry_key(session, &request.path))
            .cloned();
        let Some(registered) = registered else {
            return Ok(false);
        };
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let mut inspect = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        // Cursor-bearing opens retain their normal wire contract. Only an
        // authoritatively expired/old-epoch checkpoint may switch to bootstrap;
        // a valid (or future/invalid) current-epoch cursor never touches files.
        let clock = clock_in_transaction(&mut inspect).await?;
        if request
            .cursor
            .as_ref()
            .is_some_and(|c| c.epoch == clock.epoch && c.revision >= clock.retained_revision)
        {
            inspect.commit().await?;
            return Ok(false);
        }
        let inspected = authority(&mut inspect, &registered.registration, session).await?;
        let prepared = query::prepare_projection(
            &mut inspect,
            session,
            &registered.registration.binding,
            false,
            &registered.registration.binding.scope.projection_version,
        )
        .await?;
        let sql_timing = Scope::new(Site::OfferSql);
        let mut inspected_digest = ReplicaAggregate::new(false);
        let inspected_window = scan_local_window(
            &mut inspect,
            &registered.registration.definition,
            &prepared,
            &request.args,
            |row| {
                inspected_digest.add(&row.hash);
                Ok(())
            },
        )
        .await?;
        let inspected_digest = inspected_digest.digest();
        drop(sql_timing);
        inspect.commit().await?;
        if inspected_window.truncated
            || inspected_window.boundary.is_some()
            || registered.registration.definition.max_rows > format::MAX_ROWS as usize
            || registered.registration.definition.max_bytes > format::MAX_TOTAL as i64
        {
            return Ok(false);
        }
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        if serde_json::to_vec(
            &module
                .visibility
                .get(&registered.registration.definition.table),
        )
        .unwrap()
            != serde_json::to_vec(&Some(
                serde_json::from_slice::<VisibilityPlan>(&registered.proof.plan())
                    .map_err(|_| LocalSnapshotError::Rejected("provider plan"))?,
            ))
            .unwrap()
        {
            return Ok(false);
        }
        let directive = module.replica_directive(
            &session.route.tenant_id,
            &session.route.database_url,
            &session.identity.account.id,
            &session.member.permissions,
        );
        drop(module);
        let connection_bytes: usize = connection
            .offers
            .values()
            .map(|o| o._reservation.bytes() + o.artifact.charge())
            .sum();
        let generation = connection.generation;
        let tx = connection.tx.clone();
        let (artifact, public, reservation) = self
            .with_local_starter_admission_guarded(
                &registered.registration,
                session,
                Some(auth.parts()),
                |fresh| {
                    if fresh.identity != inspected.identity {
                        return Err(io::Error::other("source changed after window inspection"));
                    }
                    if request.cursor.as_ref().is_some_and(|c| {
                        c.epoch == fresh.epoch && c.revision >= fresh.retained_revision
                    }) {
                        return Err(io::Error::other("usable cache must resume normally"));
                    }
                    // No SQL-proof memo: this offer independently scanned the
                    // current authorized set above, and must compare it again
                    // before reusing ANY process-only encoded proof.
                    let read_timing = Scope::new(Site::OfferRead);
                    let artifact = self
                        .inner
                        .starter_transport
                        .2
                        .try_lock()
                        .map_err(|_| io::Error::other("artifact singleflight busy"))?
                        .get_or_validate(
                            &registered,
                            fresh,
                            inspected_window.rows as u64,
                            &inspected_digest,
                        )?;
                    drop(read_timing);
                    let manifest = artifact.manifest();
                    let verified = artifact.verified();
                    let d = &registered.registration.definition;
                    // Reserve descriptor clones, pending base64 and serialization
                    // BEFORE allocating them. Encoded Arc/context have a separate
                    // shared charge held until their last active/cache owner drops.
                    let envelope_bytes = (1usize << 20)
                        .checked_add(registered.proof.definition().len().saturating_mul(4))
                        .and_then(|n| n.checked_add(manifest.pieces.len().saturating_mul(1024)))
                        .ok_or_else(|| io::Error::other("offer size"))?;
                    if connection_bytes
                        .checked_add(envelope_bytes)
                        .and_then(|n| n.checked_add(artifact.charge()))
                        .is_none_or(|n| n > 8 << 20)
                    {
                        return Err(io::Error::other("starter connection budget"));
                    }
                    let reservation =
                        reserve(envelope_bytes).map_err(|_| io::Error::other("offer budget"))?;
                    let public = StarterOffer {
                        id: request.id.clone(),
                        offer_id: uuid::Uuid::new_v4().to_string(),
                        version: 1,
                        expires_at: now_ms().saturating_add(60_000).min(manifest.expires_ms),
                        cursor: ReplicaCursor {
                            epoch: manifest.epoch.clone(),
                            revision: manifest.revision,
                        },
                        key: d.key.clone(),
                        path: request.path.clone(),
                        scope: directive.scope.clone(),
                        visibility_scope: directive.visibility_scope.clone(),
                        order_by: d.order_by.clone(),
                        order_direction: d.order_direction.clone(),
                        mode: d.mode.clone(),
                        max_rows: d.max_rows,
                        max_bytes: d.max_bytes,
                        truncated: false,
                        digest: verified.digest.clone(),
                        rows: verified.rows,
                        serialized_definition: String::from_utf8(
                            registered.proof.definition().to_vec(),
                        )
                        .map_err(io::Error::other)?,
                        args: request.args.clone(),
                        format: 1,
                        encoding: manifest.identity.encoding.clone(),
                        column_types: registered.column_types.clone(),
                        pieces: manifest
                            .pieces
                            .iter()
                            .enumerate()
                            .map(|(i, p)| StarterPiece {
                                index: i as u32,
                                hash: p.hash.clone(),
                                raw_hash: p.raw_hash.clone(),
                                compressed: p.compressed,
                                bytes: p.bytes as u32,
                                raw_bytes: p.raw_bytes as u32,
                            })
                            .collect(),
                    };
                    if now_ms() >= fresh.valid_until_ms {
                        return Err(io::Error::other("starter session expired before enqueue"));
                    }
                    enqueue(
                        &tx,
                        generation,
                        reservation.clone(),
                        ServerMessage::ReplicaStarterOffer(public.clone()),
                    )?;
                    Ok((artifact, public, reservation))
                },
            )
            .await?;
        connection.offers.insert(
            request.id.clone(),
            Offer {
                registered,
                manifest: artifact.manifest().clone(),
                artifact,
                public,
                request: request.clone(),
                next: 0,
                generation,
                _reservation: reservation,
            },
        );
        Ok(true)
    }
    pub(crate) async fn starter_piece(
        &self,
        connection: &mut Connection,
        session: &TenantSession,
        id: &str,
        offer_id: &str,
        index: u32,
    ) -> Result<(), LocalSnapshotError> {
        let offer = connection
            .offers
            .get_mut(id)
            .ok_or(LocalSnapshotError::Rejected("offer absent"))?;
        if offer.generation != connection.generation
            || offer.public.offer_id != offer_id
            || offer.public.expires_at <= now_ms()
            || index as usize != offer.next
        {
            return Err(LocalSnapshotError::Rejected("offer credit/owner/expiry"));
        }
        offer
            .manifest
            .pieces
            .get(index as usize)
            .ok_or(LocalSnapshotError::Rejected("piece index"))?;
        let tx = connection.tx.clone();
        let generation = connection.generation;
        let reservation = offer._reservation.clone();
        let auth = connection
            .auth
            .clone()
            .ok_or(LocalSnapshotError::Rejected("native session required"))?;
        self.with_local_starter_admission_guarded(
            &offer.registered.registration,
            session,
            Some(auth.parts()),
            |fresh| {
                let _read_timing = Scope::new(Site::PieceRead);
                let bytes = offer
                    .artifact
                    .bytes(&offer.registered, fresh, index as usize)?;
                if now_ms() >= fresh.valid_until_ms || now_ms() >= offer.public.expires_at {
                    return Err(io::Error::other("starter session expired before enqueue"));
                }
                enqueue(
                    &tx,
                    generation,
                    reservation.clone(),
                    ServerMessage::ReplicaStarterPiece {
                        id: id.into(),
                        offer_id: offer_id.into(),
                        index,
                        data: base64::engine::general_purpose::STANDARD.encode(bytes),
                    },
                )
            },
        )
        .await?;
        offer.next += 1;
        Ok(())
    }
}

impl Runtime {
    pub(crate) async fn commit_starter(
        &self,
        connection: &mut Connection,
        session: &TenantSession,
        id: &str,
        offer_id: &str,
        requested: &ReplicaCursor,
        client_digest: &str,
    ) -> Result<ReplicaSubscription, LocalSnapshotError> {
        let offer = connection
            .offers
            .remove(id)
            .ok_or(LocalSnapshotError::Rejected("offer absent"))?;
        if offer.generation != connection.generation
            || offer.public.offer_id != offer_id
            || offer.public.expires_at <= now_ms()
            || offer.next != offer.manifest.pieces.len()
            || requested != &offer.public.cursor
            || client_digest != offer.public.digest
        {
            return Err(LocalSnapshotError::Rejected(
                "bootstrap commit owner/credit/integrity",
            ));
        }
        let registered = &offer.registered;
        let reg = &registered.registration;
        let module = self
            .inner
            .modules
            .project_for_call(&session.identity.project_id)
            .await
            .ok_or(ReplicaError::ModuleMissing)?;
        let plan =
            module
                .visibility
                .get(&reg.definition.table)
                .ok_or(LocalSnapshotError::Rejected(
                    "collection visibility removed",
                ))?;
        let raw = self
            .inner
            .modules
            .permission_manifest(&module)
            .await
            .ok_or(LocalSnapshotError::Rejected("source manifest removed"))?;
        if module.generation != reg.generation
            || module.artifact_hash != reg.artifact_hash
            || serde_json::to_vec(plan).unwrap().as_slice() != registered.proof.plan()
            || serde_json::to_vec(&raw).unwrap().as_slice() != registered.proof.manifest()
            || module
                .functions
                .get(&offer.request.path)
                .and_then(|f| f.replica.as_ref())
                .is_none_or(|d| {
                    serde_json::to_vec(d).unwrap().as_slice() != registered.proof.definition()
                })
        {
            return Err(LocalSnapshotError::Rejected(
                "loaded provider/policy changed",
            ));
        }
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        let pool = control.pools().pool(control.database_url()).await?;
        let mut landlord = pool.begin().await?;
        let active:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts a JOIN gonvex_runtime_tenants t ON t.project_id=a.auth_realm_id WHERE a.id=?1 AND a.auth_realm_id=?2 AND a.disabled_at IS NULL AND t.tenant_id=?3 AND t.status='active' AND t.deleted_at IS NULL AND t.database_url=?4)").bind(&session.identity.account.id).bind(&session.identity.project_id).bind(&session.route.tenant_id).bind(&session.route.database_url).fetch_one(&mut *landlord).await?;
        if active != 1 {
            return Err(LocalSnapshotError::Rejected(
                "bootstrap account/member route revoked",
            ));
        }
        let auth = connection
            .auth
            .as_ref()
            .ok_or(LocalSnapshotError::Rejected("native session required"))?;
        if auth.project != session.identity.project_id
            || auth.account != session.identity.account.id
        {
            return Err(LocalSnapshotError::Rejected("native session scope changed"));
        }
        let expiry:Option<i64>=sqlx::query_scalar("SELECT expires_at FROM gonvex_auth_sessions WHERE token_hash=?1 AND project_id=?2 AND account_id=?3 AND revoked_at IS NULL AND expires_at>CAST(unixepoch('subsec')*1000 AS INTEGER)").bind(&auth.hash).bind(&auth.project).bind(&auth.account).fetch_optional(&mut *landlord).await?;
        let auth_expiry = expiry
            .filter(|n| *n > 0)
            .ok_or(LocalSnapshotError::Rejected(
                "native session revoked/expired",
            ))? as u64;
        let mut transaction = control
            .begin_tenant_transaction(&session.route, false)
            .await?;
        let fresh = authority(&mut transaction, reg, session).await?;
        if fresh.identity != offer.manifest.identity
            || fresh.epoch != offer.manifest.epoch
            || fresh.retained_revision > offer.manifest.revision
        {
            return Err(LocalSnapshotError::Rejected(
                "bootstrap serial/scope/retention changed",
            ));
        }
        let clock = clock_in_transaction(&mut transaction).await?;
        // This policy certificate forbids all parents/joins/negative branches;
        // nevertheless obtain the NORMAL v2 resolved principal and SQL window,
        // not a normalized-set fingerprint or a client-provided digest.
        let resolve_timing = Scope::new(Site::CommitResolve);
        let resolved = visibility::resolve_with_parents(
            &mut transaction,
            session,
            plan,
            clock.revision,
            &module.visibility,
            &visibility::PreparedParentAcl::default(),
            &self.inner.visibility_metrics,
            None,
        )
        .await
        .map_err(ReplicaError::from)?;
        drop(resolve_timing);
        let sql_timing = Scope::new(Site::CommitSql);
        let mut digest = ReplicaAggregate::new(false);
        let window = scan_window(
            &mut transaction,
            &reg.definition,
            plan,
            &resolved,
            &offer.request.args,
            |row| {
                digest.add(&row.hash);
                Ok(())
            },
        )
        .await?;
        crate::starter_files::diagnostics::add(Site::SqlRows, window.rows as u64);
        drop(sql_timing);
        if window.truncated
            || window.boundary.is_some()
            || window.rows as u64 != offer.manifest.rows
            || digest.digest() != offer.public.digest
        {
            return Err(LocalSnapshotError::Rejected(
                "normal v2 membership/window proof failed",
            ));
        }
        // Data equality at the final read clock proves that conservative serial
        // coverage permits no changed collection rows. Unrelated revisions are
        // caught up by the final ready cursor; relevant mutations fall back.
        let directive = module.replica_directive(
            &session.route.tenant_id,
            &session.route.database_url,
            &session.identity.account.id,
            &session.member.permissions,
        );
        let cursor = cursor_for(&clock, &reg.definition, &directive.visibility_scope);
        let locator = resume::key(
            session,
            &offer.request,
            &module,
            &reg.definition,
            &directive.visibility_scope,
            false,
        );
        let fingerprint = resume::fingerprint(&resolved);
        // Authoritative read commits before anchor installation. The landlord
        // writer lease excludes account disable; the tenant writer connection
        // remains exclusively held until COMMIT returns. No network await.
        let durability_timing = Scope::new(Site::CommitDurability);
        transaction.commit().await?;
        drop(durability_timing);
        // Reacquire and revalidate before enqueue: a tenant write may win the
        // commit->enqueue boundary. The cached anchor is installed only after
        // this final same-serial read, under the fresh writer lease.
        let mut final_tx = control
            .begin_tenant_transaction(&session.route, false)
            .await?;
        let last = authority(&mut final_tx, reg, session).await?;
        if last.identity != fresh.identity || last.revision != clock.revision {
            return Err(LocalSnapshotError::Rejected(
                "bootstrap changed before enqueue",
            ));
        }
        if now_ms() >= auth_expiry || now_ms() >= offer.public.expires_at {
            return Err(LocalSnapshotError::Rejected(
                "native session expired before ready enqueue",
            ));
        }
        let ready = replica_ready(
            id,
            &offer.request.path,
            &reg.definition,
            cursor.clone(),
            digest.digest(),
            false,
        );
        enqueue(
            &connection.tx,
            connection.generation,
            offer._reservation.clone(),
            ready,
        )
        .map_err(reject)?;
        self.inner.resume_validation.remember(
            resume::epoch_key(&locator, &cursor.epoch),
            resume::Anchor {
                cursor: cursor.clone(),
                digest: digest.clone(),
                window: window.clone(),
                fingerprint: fingerprint.into(),
            },
        );
        final_tx.rollback().await?;
        landlord.rollback().await?;
        Ok(ReplicaSubscription {
            id: id.into(),
            path: offer.request.path,
            args: offer.request.args,
            definition: Arc::new(reg.definition.clone()),
            visibility: Arc::new(plan.clone()),
            visibility_scope: directive.visibility_scope,
            cursor,
            resolved,
            digest,
            window,
            mask: Default::default(),
            concern_tables: None,
            parent_acl_columns: None,
            wake_member: None,
            fanout: None,
        })
    }
}

#[cfg(test)]
pub(crate) static TIMINGS: Mutex<Vec<(&'static str, u128)>> = Mutex::new(Vec::new());
#[cfg(test)]
struct Timing(&'static str, std::time::Instant);
#[cfg(test)]
impl Drop for Timing {
    fn drop(&mut self) {
        let mut data = TIMINGS.lock().unwrap();
        if data.len() < 8192 {
            data.push((self.0, self.1.elapsed().as_nanos()));
        }
    }
}
pub(crate) async fn prepare_offloaded(
    runtime: Runtime,
    mut connection: Connection,
    session: TenantSession,
    request: ReplicaOpenRequest,
    sum: bool,
) -> (Connection, Result<bool, LocalSnapshotError>) {
    crate::offload::offload()
        .spawn(async move {
            #[cfg(test)]
            let _timing = Timing("offer_authority_window_format", std::time::Instant::now());
            let result = runtime
                .offer_starter(&mut connection, &session, &request, sum)
                .await;
            (connection, result)
        })
        .await
}
pub(crate) async fn piece_offloaded(
    runtime: Runtime,
    mut connection: Connection,
    session: TenantSession,
    id: String,
    offer_id: String,
    index: u32,
) -> (Connection, Result<(), LocalSnapshotError>) {
    crate::offload::offload()
        .spawn(async move {
            #[cfg(test)]
            let _timing = Timing("piece_authority_read_encode", std::time::Instant::now());
            let result = runtime
                .starter_piece(&mut connection, &session, &id, &offer_id, index)
                .await;
            (connection, result)
        })
        .await
}
pub(crate) async fn commit_offloaded(
    runtime: Runtime,
    mut connection: Connection,
    session: TenantSession,
    id: String,
    offer_id: String,
    cursor: ReplicaCursor,
    digest: String,
) -> (Connection, Result<ReplicaSubscription, LocalSnapshotError>) {
    crate::offload::offload()
        .spawn(async move {
            #[cfg(test)]
            let _timing = Timing(
                "commit_authority_v2_anchor_catchup",
                std::time::Instant::now(),
            );
            let result = runtime
                .commit_starter(&mut connection, &session, &id, &offer_id, &cursor, &digest)
                .await;
            (connection, result)
        })
        .await
}

impl Runtime {
    /// Called only after an ordinary complete snapshot has finished preparing.
    /// The open never awaits this bounded singleflight/background build.
    pub(crate) fn schedule_starter_after_normal_snapshot(
        &self,
        session: &TenantSession,
        request: &ReplicaOpenRequest,
        opened: &ReplicaOpenResult,
    ) {
        if !crate::starter_transport_enabled()
            || request.args != json!({})
            || !opened
                .messages
                .iter()
                .any(|m| matches!(m, ServerMessage::ReplicaSnapshot { .. }))
            || opened.subscription.as_ref().is_none_or(|s| {
                s.window.truncated
                    || s.window.rows > format::MAX_ROWS as usize
                    || s.window.bytes > format::MAX_TOTAL as i64
            })
        {
            return;
        }
        let registered = self
            .inner
            .starter_transport
            .0
            .lock()
            .unwrap()
            .get(&registry_key(session, &request.path))
            .cloned();
        let Some(registered) = registered else {
            return;
        };
        let mut jobs = self.inner.starter_transport.1.lock().unwrap();
        jobs.retain(|j| !j.is_finished());
        if jobs.len() >= 16 {
            return;
        }
        if let Some(job) = self.spawn_local_starter_after_miss(
            registered.registration,
            session.clone(),
            registered.store,
            super::starter_adapter::CollectionUse::SharedBase,
        ) {
            jobs.push(job);
        }
    }
    #[cfg(test)]
    pub(crate) async fn wait_starter_background(&self) {
        let jobs = std::mem::take(&mut *self.inner.starter_transport.1.lock().unwrap());
        for job in jobs {
            job.await
                .expect("background task")
                .expect("background starter");
        }
    }
}
