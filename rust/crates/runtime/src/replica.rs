//! Replica Collection snapshots, resumable membership, and transaction routing.
//!
//! A subscription keeps no copy of the rows its client holds (see
//! `docs/performance/replica-state.md`). It keeps the client's cursor, its
//! resolved principal, the integrity aggregate of the client's rows and the
//! window counters of a bounded collection. Feed deltas come from the change
//! feed's before and after images: visible(old) / visible(new) decide insert,
//! update or delete, and the aggregate subtracts the old row's hash and adds
//! the new one. Anything that cannot be decided from the images (a principal
//! change, a truncated window that moves, an index that is not at the event's
//! revision) asks the client for its row hashes (`replica.needHashes`) and
//! reconciles that one collection against SQL on the reopen.

#[path = "replica_mask.rs"]
mod mask;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use gonvex_protocol::replica_digest::ReplicaSetDigest;
use gonvex_protocol::{
    PublicInvocationProvenance, ReplicaChange, ReplicaCursor, ReplicaOpenRequest, ServerMessage,
};
use gonvex_store::{TenantSession, TenantTransaction};
use mask::{ApplyContext, Resync, Step};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use thiserror::Error;

use crate::change_feed::{FeedEvent, LogChange, ReplicaClock};
use crate::modules::ReplicaCollectionDefinition;
use crate::replica_cursor::cursor_resumable;
use crate::sql_parameters::Parameters;
use crate::visibility::{self, ResolvedVisibility, VisibilityPlan};
use crate::Runtime;

type ParentOverlay = Arc<Vec<(String, String, Option<Arc<Value>>)>>;
type SharedDecision<'a> = (
    &'a crate::feed_share::EventShare,
    &'a (u64, usize, Arc<str>, Arc<str>),
);

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
    #[error("targeted replica fetch advanced beyond its resolved principal")]
    SnapshotAdvanced,
    #[error("replica row is missing key {0:?}")]
    MissingKey(String),
    #[error("replica returned duplicate key {0:?}")]
    DuplicateKey(String),
    #[error("replica SQL identifier {0:?} is invalid")]
    InvalidIdentifier(String),
    #[error(transparent)]
    Database(#[from] gonvex_store::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Visibility(#[from] visibility::VisibilityError),
}

pub struct ReplicaOpenResult {
    pub messages: Vec<ServerMessage>,
    pub subscription: Option<ReplicaSubscription>,
}

/// One open Replica Collection of one connection. Holds no rows: the client's
/// membership is defined by `resolved` (and `window` for a bounded
/// collection), and `digest` is the integrity aggregate of the rows the
/// client holds at `cursor`.
pub struct ReplicaSubscription {
    pub id: String,
    pub path: String,
    pub args: Value,
    /// Shared by every subscription of this collection (module generation).
    pub definition: Arc<ReplicaCollectionDefinition>,
    /// Shared by every subscription and inherited parent using this plan.
    pub visibility: Arc<VisibilityPlan>,
    pub visibility_scope: String,
    pub cursor: ReplicaCursor,
    pub resolved: ResolvedVisibility,
    pub(crate) digest: ReplicaAggregate,
    pub(crate) window: ReplicaWindow,
    /// Compiled visibility binding of `resolved` (`visibility_class`),
    /// cleared whenever an event changes `resolved` in memory.
    pub(crate) mask: crate::visibility_class::HandleCache,
    /// Every table a change to which can concern this subscription
    /// (`replica_mask::concern_tables`), computed on first use.
    pub(crate) concern_tables: Option<Arc<std::collections::HashSet<String>>>,
    /// Per inherited parent table: the parent ACL index columns (the only
    /// parent columns its visibility reads), computed on first use.
    pub(crate) parent_acl_columns: Option<Arc<BTreeMap<String, BTreeSet<String>>>>,
}

/// Counters of the rows a client holds, and for a truncated ordered window
/// the (order value, key) of its last row: rows ordered after it are not held.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicaWindow {
    pub rows: usize,
    /// Canonical JSON bytes of the held projections (budget accounting).
    pub bytes: i64,
    pub truncated: bool,
    pub boundary: Option<Box<(Value, Value)>>,
}

impl ReplicaSubscription {
    /// Rows the client holds.
    pub fn row_count(&self) -> usize {
        self.window.rows
    }

    pub fn truncated(&self) -> bool {
        self.window.truncated
    }

    /// The integrity digest of the rows the client holds.
    pub fn digest(&self) -> String {
        self.digest.digest()
    }

    pub(crate) fn require_legacy_digest(&mut self) {
        if let ReplicaAggregate::Sum(state) = &self.digest {
            self.digest = ReplicaAggregate::Legacy(legacy_bytes(state));
        }
    }

    fn bounded(&self) -> bool {
        self.definition.max_rows > 0 || self.definition.max_bytes > 0
    }

    fn over_budget(&self) -> bool {
        (self.definition.max_rows > 0 && self.window.rows > self.definition.max_rows)
            || (self.definition.max_bytes > 0 && self.window.bytes > self.definition.max_bytes)
    }

    /// Whether a truncated window's boundary admits `row` (a full or
    /// projected source row). None when the window has no usable boundary.
    fn window_admits(&self, row: &Value) -> Option<bool> {
        if !self.window.truncated {
            return Some(true);
        }
        if self.definition.order_by.is_empty() {
            return None;
        }
        let boundary = self.window.boundary.as_deref()?;
        Some(within_boundary(&self.definition, boundary, row))
    }

    /// Does this collection's client window hold `row` after the event?
    /// None when that cannot be decided without the client's membership.
    fn holds(&self, row: &Value) -> Option<bool> {
        if row.is_null() {
            return Some(false);
        }
        if !row_in_collection(
            row,
            &self.args,
            &self.definition,
            &self.visibility,
            &self.resolved,
        ) {
            return Some(false);
        }
        self.window_admits(row)
    }
}

/// Integrity aggregate over the hashes of the rows a client holds: `xor1`
/// (the SDK) or negotiated `sum128-v1`. Both are reversible, so a row's old
/// hash is removed and its new hash added without the row set.
#[derive(Clone)]
pub enum ReplicaAggregate {
    Legacy([u8; 32]),
    Sum(Box<ReplicaSetDigest>),
}

impl ReplicaAggregate {
    fn new(sum: bool) -> Self {
        if sum {
            Self::Sum(Box::default())
        } else {
            Self::Legacy([0; 32])
        }
    }

    fn add(&mut self, hash: &str) {
        self.change(hash, false)
    }

    fn remove(&mut self, hash: &str) {
        self.change(hash, true)
    }

    fn change(&mut self, hash: &str, remove: bool) {
        match self {
            Self::Legacy(bytes) => xor_hash(bytes, hash),
            Self::Sum(state) => {
                // A removal of a row this aggregate never added cannot happen
                // for a client that applied every delta; if it does, the
                // digest disagrees and the client reconciles.
                let _ = if remove {
                    state.remove(hash)
                } else {
                    state.add(hash)
                };
            }
        }
    }

    fn digest(&self) -> String {
        match self {
            Self::Legacy(bytes) => format_aggregate(bytes),
            Self::Sum(state) => state.digest(),
        }
    }
}

fn legacy_bytes(state: &ReplicaSetDigest) -> [u8; 32] {
    let digest = state.legacy_digest();
    let hex = digest.trim_start_matches("xor1:");
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap_or(0);
    }
    bytes
}

/// One shared copy of a collection definition per (project, module
/// generation, path) for every subscription of it.
fn shared_definition(
    project: &str,
    generation: u64,
    path: &str,
    definition: &ReplicaCollectionDefinition,
) -> Arc<ReplicaCollectionDefinition> {
    type Definitions = std::collections::HashMap<
        (String, u64, String),
        std::sync::Weak<ReplicaCollectionDefinition>,
    >;
    static DEFINITIONS: std::sync::OnceLock<std::sync::Mutex<Definitions>> =
        std::sync::OnceLock::new();
    let mut definitions = DEFINITIONS.get_or_init(Default::default).lock().unwrap();
    let key = (project.to_owned(), generation, path.to_owned());
    if let Some(shared) = definitions.get(&key).and_then(std::sync::Weak::upgrade) {
        return shared;
    }
    definitions.retain(|_, shared| shared.strong_count() > 0);
    let shared = Arc::new(definition.clone());
    definitions.insert(key, Arc::downgrade(&shared));
    shared
}

/// Free snapshot permits right now (profiler gauge).
pub(crate) fn snapshot_permits_available() -> usize {
    crate::offload::SnapshotAdmission::global().available_permits()
}

/// One row of a collection scan: its key, decoded projection and canonical
/// hash. The scan visitor owns it (moves, no clones).
pub(crate) struct ScannedRow {
    pub key: String,
    pub row: Value,
    pub hash: String,
}

/// A read transaction positioned at one clock with the member's principal.
struct CollectionRead {
    transaction: TenantTransaction,
    clock: ReplicaClock,
    resolved: ResolvedVisibility,
    permit: crate::offload::SnapshotPermit,
    /// Snapshot-active gauge, held as long as the permit.
    _active: gonvex_profiler::GaugeGuard,
}

// Small and normal application collections can carry their integrity map with
// the initial rows. This lets clients persist the cold snapshot once instead of
// hashing and rewriting it again when the immediately following ready arrives.
// Large replicas keep the digest/need-hashes resume path to bound wire overhead.
const INLINE_SNAPSHOT_HASH_LIMIT: usize = 2_048;

enum DigestRequest {
    /// `xor1:` / `sum128-v1:` aggregate with no hashes: resume from the log.
    Aggregate,
    /// A legacy whole-set SHA-256 digest with no hashes.
    WholeSet,
    /// Keys and hashes (or nothing): reconcile against SQL.
    Reconcile,
}

impl Runtime {
    pub(crate) async fn execute_replica_collection_query(
        &self,
        session: &TenantSession,
        definition: &ReplicaCollectionDefinition,
        visibility: &VisibilityPlan,
        args: &Value,
    ) -> Result<Value, ReplicaError> {
        let mut read = self
            .begin_collection_read(session, definition, visibility)
            .await?;
        let mut rows = Vec::new();
        scan_window(
            &mut read.transaction,
            definition,
            visibility,
            &read.resolved,
            args,
            |row| {
                rows.push(row.row);
                Ok(())
            },
        )
        .await?;
        read.transaction.commit().await?;
        Ok(Value::Array(rows))
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
            .as_ref()
            .ok_or_else(|| ReplicaError::FunctionMissing(request.path.clone()))?;
        let definition = shared_definition(
            &module.project_id,
            module.generation,
            &request.path,
            definition,
        );
        let visibility = module
            .visibility
            .get(&definition.table)
            .map(visibility::shared_plan)
            .ok_or_else(|| ReplicaError::VisibilityMissing(definition.table.clone()))?;
        let directive = module.replica_directive(
            &session.route.tenant_id,
            &session.route.database_url,
            &session.identity.account.id,
            &session.member.permissions,
        );
        drop(module);
        let requested = match request.digest.as_deref() {
            Some(digest)
                if !digest.is_empty() && !request.full_integrity && request.hashes.is_empty() =>
            {
                if digest.starts_with("xor1:") || sum {
                    DigestRequest::Aggregate
                } else {
                    DigestRequest::WholeSet
                }
            }
            _ => DigestRequest::Reconcile,
        };
        let need_hashes = |request: &ReplicaOpenRequest| ReplicaOpenResult {
            messages: vec![ServerMessage::ReplicaNeedHashes {
                id: request.id.clone(),
                path: Some(request.path.clone()),
            }],
            subscription: None,
        };
        let mut read = self
            .begin_collection_read(session, &definition, &visibility)
            .await?;
        let cursor = cursor_for(&read.clock, &definition, &directive.visibility_scope);
        let resumable = cursor_resumable(
            request.cursor.as_ref(),
            &cursor,
            read.clock.retained_revision,
        );
        let mut digest = ReplicaAggregate::new(sum);
        let mut messages = Vec::new();
        let window;
        if !resumable {
            // Cold open: stream rows into the snapshot message. Nothing is
            // retained after it is sent.
            let mut rows = Vec::new();
            let mut inline = Some(BTreeMap::new());
            window = scan_window(
                &mut read.transaction,
                &definition,
                &visibility,
                &read.resolved,
                &request.args,
                |row| {
                    digest.add(&row.hash);
                    if let Some(hashes) = &mut inline {
                        if hashes.len() < INLINE_SNAPSHOT_HASH_LIMIT {
                            hashes.insert(row.key, row.hash);
                        } else {
                            inline = None;
                        }
                    }
                    rows.push(Arc::new(row.row));
                    Ok(())
                },
            )
            .await?;
            read.permit.observe_json_bytes(window.bytes.max(0) as usize);
            read.transaction.commit().await?;
            messages.push(replica_snapshot_message(
                &request,
                &definition,
                rows,
                inline,
                window.truncated,
                cursor.clone(),
                &directive.scope,
                &directive.visibility_scope,
                &digest.digest(),
            ));
        } else {
            match requested {
                DigestRequest::Aggregate => {
                    let after = request.cursor.as_ref().map_or(0, |cursor| cursor.revision);
                    let changes = if after < read.clock.revision {
                        let mut tables = visibility
                            .resolved_dependency_columns(&read.resolved)
                            .tables();
                        tables.insert(definition.table.clone());
                        resume_changes(&mut read.transaction, after, read.clock.revision, tables)
                            .await?
                    } else {
                        Vec::new()
                    };
                    let dependencies = visibility.resolved_dependency_columns(&read.resolved);
                    if bounded_sql_visibility(&definition, &visibility, &read.resolved)
                        || table_change_relevance(&definition.table, &dependencies, changes.iter())
                            .1
                    {
                        // Visibility inputs moved since the cursor: the log's
                        // row images cannot say what the client holds.
                        tracing::debug!(target: "gonvex_runtime::replica", path = %request.path, "resume reconciles: visibility inputs changed since the cursor");
                        read.transaction.commit().await?;
                        return Ok(need_hashes(&request));
                    }
                    // The current aggregate is computed from SQL (it verifies
                    // the client's claim); no row is retained.
                    window = scan_window(
                        &mut read.transaction,
                        &definition,
                        &visibility,
                        &read.resolved,
                        &request.args,
                        |row| {
                            digest.add(&row.hash);
                            Ok(())
                        },
                    )
                    .await?;
                    read.permit.observe_json_bytes(window.bytes.max(0) as usize);
                    read.transaction.commit().await?;
                    let mut prior = digest.clone();
                    let mut upserts = BTreeMap::new();
                    let mut deleted = BTreeSet::new();
                    for (id, change) in collapse_changes(&changes) {
                        if id.0 != definition.table {
                            continue;
                        }
                        let image = |row: &Value| -> Result<Option<(Value, String)>, ReplicaError> {
                            if !row_in_collection(
                                row,
                                &request.args,
                                &definition,
                                &visibility,
                                &read.resolved,
                            ) {
                                return Ok(None);
                            }
                            let projection = project_row(row, &definition)?;
                            let hash = row_hash(&projection);
                            Ok(Some((projection, hash)))
                        };
                        let old = image(change.old)?;
                        let new = image(change.new)?;
                        if let Some((_, hash)) = &new {
                            prior.remove(hash);
                        }
                        if let Some((_, hash)) = &old {
                            prior.add(hash);
                        }
                        match (old, new) {
                            (Some((_, old)), Some((row, new))) if old != new => {
                                upserts.insert(id.1.to_owned(), row);
                            }
                            (None, Some((row, _))) => {
                                upserts.insert(id.1.to_owned(), row);
                            }
                            (Some(_), None) => {
                                deleted.insert(id.1.to_owned());
                            }
                            _ => {}
                        }
                    }
                    if (window.truncated && (!upserts.is_empty() || !deleted.is_empty()))
                        || request.digest.as_deref() != Some(prior.digest().as_str())
                    {
                        tracing::debug!(target: "gonvex_runtime::replica", path = %request.path, "resume reconciles: client digest does not match the log");
                        return Ok(need_hashes(&request));
                    }
                    if !upserts.is_empty()
                        || !deleted.is_empty()
                        || request.cursor.as_ref() != Some(&cursor)
                    {
                        messages.push(replica_delta(
                            &request.id,
                            &request.path,
                            cursor.clone(),
                            upserts.into_values().collect(),
                            deleted.into_iter().collect(),
                            digest.digest(),
                            window.truncated,
                        ));
                    }
                }
                DigestRequest::WholeSet => {
                    let mut hashes = BTreeMap::new();
                    window = scan_window(
                        &mut read.transaction,
                        &definition,
                        &visibility,
                        &read.resolved,
                        &request.args,
                        |row| {
                            digest.add(&row.hash);
                            hashes.insert(row.key, row.hash);
                            Ok(())
                        },
                    )
                    .await?;
                    read.permit.observe_json_bytes(window.bytes.max(0) as usize);
                    read.transaction.commit().await?;
                    if request.digest.as_deref() != Some(hashes_digest(&hashes).as_str()) {
                        return Ok(need_hashes(&request));
                    }
                }
                DigestRequest::Reconcile => {
                    // The client sent its keys and hashes: send only rows
                    // whose hash differs and the keys SQL no longer returns.
                    let mut upserts = Vec::new();
                    let mut seen = BTreeSet::new();
                    window = scan_window(
                        &mut read.transaction,
                        &definition,
                        &visibility,
                        &read.resolved,
                        &request.args,
                        |row| {
                            digest.add(&row.hash);
                            if request.hashes.get(&row.key) != Some(&row.hash) {
                                upserts.push(row.row);
                            }
                            seen.insert(row.key);
                            Ok(())
                        },
                    )
                    .await?;
                    read.permit.observe_json_bytes(window.bytes.max(0) as usize);
                    read.transaction.commit().await?;
                    let deleted = request
                        .keys
                        .iter()
                        .filter(|key| !seen.contains(key.as_str()))
                        .cloned()
                        .collect::<Vec<_>>();
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
                            digest.digest(),
                            window.truncated,
                        ));
                    }
                }
            }
        }
        messages.push(replica_ready(
            &request.id,
            &request.path,
            &definition,
            cursor.clone(),
            digest.digest(),
            window.truncated,
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
                resolved: read.resolved,
                digest,
                window,
                mask: Default::default(),
                concern_tables: None,
                parent_acl_columns: None,
            }),
        })
    }

    /// Opens a read transaction for one collection scan and resolves the
    /// member's principal at its clock.
    async fn begin_collection_read(
        &self,
        session: &TenantSession,
        definition: &ReplicaCollectionDefinition,
        plan: &VisibilityPlan,
    ) -> Result<CollectionRead, ReplicaError> {
        // Each WebSocket queues up to REPLICA_OPEN_CONCURRENCY preparations,
        // so a sign-in burst could otherwise build hundreds of full snapshots
        // at once. Snapshot builds are CPU and memory bound: more of them than
        // cores only multiplies peak memory, so bound them process-wide.
        use gonvex_profiler::{gauge, GaugeId, Label, ScopeTimer, Stage, Timer};
        let _build = ScopeTimer::new(Stage::SnapshotBuild, Label::None);
        let mut timer = Timer::start();
        // Bounded by count and by an estimated byte budget
        // (`crate::offload::SnapshotAdmission`).
        let permit = {
            let _waiting = gauge(GaugeId::SnapshotWaiting).guard();
            crate::offload::SnapshotAdmission::global()
                .admit(
                    format!(
                        "{}\u{0}{}\u{0}{}",
                        session.route.database_url, definition.table, session.member.id
                    ),
                    &definition.table,
                )
                .await
        };
        let active = gauge(GaugeId::SnapshotActive).guard();
        timer.lap(Stage::SnapshotPermitWait, Label::None);
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::ModuleMissing)?;
        // Parent ACL indexes load in their own transactions, before this
        // snapshot holds a pool admission.
        let module = if plan.version == Some(2) {
            Some(
                self.inner
                    .modules
                    .project(&session.identity.project_id)
                    .await
                    .ok_or(ReplicaError::ModuleMissing)?,
            )
        } else {
            None
        };
        let acl = match &module {
            Some(module) => {
                self.inner
                    .parent_acl
                    .ensure(
                        &self.inner.change_feeds,
                        &control,
                        session,
                        &module.visibility,
                        &[plan],
                    )
                    .await?
            }
            None => visibility::PreparedParentAcl::default(),
        };
        timer.lap(Stage::SnapshotAcl, Label::None);
        let mut transaction = control
            .begin_tenant_transaction(&session.route, true)
            .await?;
        let clock = clock_in_transaction(&mut transaction).await?;
        timer.lap(Stage::SnapshotBegin, Label::None);
        self.inner.visibility_metrics.record_collection_snapshot();
        self.inner
            .visibility_metrics
            .record_visibility_sql_statement();
        let resolved = if let Some(module) = &module {
            visibility::resolve_with_parents(
                &mut transaction,
                session,
                plan,
                clock.revision,
                &module.visibility,
                &acl,
                &self.inner.visibility_metrics,
            )
            .await?
        } else if definition.max_rows > 0 || definition.max_bytes > 0 {
            visibility::resolve_for_query(&mut transaction, session, plan, clock.revision).await?
        } else {
            visibility::resolve(&mut transaction, session, plan, clock.revision).await?
        };
        timer.lap(Stage::SnapshotVisibility, Label::None);
        Ok(CollectionRead {
            transaction,
            clock,
            resolved,
            permit,
            _active: active,
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
            // Parent ACL appliers poison their own indexes on Reset; a
            // connection never resets shared state another one may hold.
            return Ok(subscriptions
                .values()
                .map(|subscription| ServerMessage::ReplicaReset {
                    id: subscription.id.clone(),
                    path: Some(subscription.path.clone()),
                    reason: "change-feed-reset".to_owned(),
                })
                .collect());
        };
        let collapsed = collapse_changes(&changes);
        let mut messages = Vec::new();
        let mut touches = BTreeMap::<(String, String), EntityTouch>::new();
        let mut resynced = BTreeSet::new();
        // Change rows decoded once and class defaults evaluated once per
        // visibility class for the whole event (`changes` outlives it).
        let mut memo = crate::visibility_class::EvalMemo::default();
        // Delivery v3: the same, plus per-class row decisions, shared by every
        // connection of the tenant (`feed_share`).
        let share = self
            .inner
            .delivery
            .tenant(&session.route.tenant_id)
            .events
            .share(&database_epoch, revision, &changes);
        let mut shared_stats = crate::visibility_class::EvalStats::default();
        for subscription in subscriptions.values_mut() {
            if revision <= subscription.cursor.revision {
                continue;
            }
            let step = if subscription.visibility.version == Some(2) {
                if !visibility::indexes_ready(&subscription.resolved, revision, &database_epoch)
                    .await
                {
                    // In-memory mask evaluation needs every bound parent ACL
                    // index complete through this revision.
                    Step::Resync(Resync::Consistency)
                } else {
                    self.mask_step(
                        session,
                        subscription,
                        revision,
                        &changes,
                        &collapsed,
                        share.as_deref(),
                    )
                    .await?
                }
            } else {
                legacy_step(subscription, &changes)
            };
            let context = match step {
                Step::Skip => {
                    // Rows and visibility inputs did not change; keep the
                    // cursor current without a per-collection ready.
                    subscription.cursor.revision = revision;
                    continue;
                }
                Step::Resync(reason) => {
                    self.resync(subscription, reason, &mut messages, &mut resynced);
                    continue;
                }
                Step::Apply(context) => context,
            };
            let shared = share
                .as_deref()
                .filter(|_| shareable_plan(&subscription.visibility));
            let applied = match shared {
                Some(share) => {
                    let mut guard = share.memo.lock().unwrap();
                    let before = guard.stats;
                    let applied = apply_rows_with(
                        subscription,
                        &context,
                        &collapsed,
                        Some((&mut guard, session.route.database_url.as_str())),
                        Some(share),
                    );
                    let after = guard.stats;
                    drop(guard);
                    shared_stats.compiled_checks += after.compiled_checks - before.compiled_checks;
                    shared_stats.class_evaluations +=
                        after.class_evaluations - before.class_evaluations;
                    shared_stats.member_evaluations +=
                        after.member_evaluations - before.member_evaluations;
                    shared_stats.fallback_checks += after.fallback_checks - before.fallback_checks;
                    applied
                }
                None => apply_rows_with(
                    subscription,
                    &context,
                    &collapsed,
                    Some((&mut memo, session.route.database_url.as_str())),
                    None,
                ),
            };
            let Some(delta) = applied? else {
                self.resync(subscription, Resync::Window, &mut messages, &mut resynced);
                continue;
            };
            subscription.cursor.revision = revision;
            for (key, touch) in delta.touches {
                touches
                    .entry(key)
                    .or_default()
                    .merge(&subscription.id, touch);
            }
            if delta.upserts.is_empty() && delta.deleted.is_empty() {
                // The client's rows and digest are unchanged: send nothing.
                // Its cursor stays where it is (or advances with the
                // connection watermark); a resume from that cursor replays
                // the log and verifies the digest against SQL, so an older
                // cursor is always safe. Most members cannot see a given
                // task, so a per-revision ready to each of them was the
                // largest per-connection delivery cost.
                continue;
            }
            messages.push(replica_delta(
                &subscription.id,
                &subscription.path,
                subscription.cursor.clone(),
                delta.upserts.into_values().collect(),
                delta.deleted.into_iter().collect(),
                subscription.digest.digest(),
                subscription.window.truncated,
            ));
            messages.push(replica_ready(
                &subscription.id,
                &subscription.path,
                &subscription.definition,
                subscription.cursor.clone(),
                subscription.digest.digest(),
                subscription.window.truncated,
            ));
        }
        // The client reopens these with their row hashes; until then they
        // receive nothing.
        subscriptions.retain(|id, _| !resynced.contains(id));
        let entity_changes = normalize_entity_changes(touches, &collapsed, subscriptions);
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
                    changes: entity_changes,
                },
            );
        }
        self.inner.visibility_metrics.record_eval_stats(&memo.stats);
        self.inner
            .visibility_metrics
            .record_eval_stats(&shared_stats);
        Ok(messages)
    }

    fn resync(
        &self,
        subscription: &ReplicaSubscription,
        reason: Resync,
        messages: &mut Vec<ServerMessage>,
        resynced: &mut BTreeSet<String>,
    ) {
        match reason {
            Resync::Principal => self.inner.visibility_metrics.record_principal_rebuild(),
            Resync::Consistency | Resync::Window => {
                self.inner.visibility_metrics.record_consistency_rebuild()
            }
        }
        messages.push(ServerMessage::ReplicaNeedHashes {
            id: subscription.id.clone(),
            path: Some(subscription.path.clone()),
        });
        resynced.insert(subscription.id.clone());
    }
}

/// One row's net change in a transaction: its first before-image and last
/// after-image (intermediate states are never published).
pub(crate) struct CollapsedChange<'a> {
    pub old: &'a Value,
    pub new: &'a Value,
    pub changed_columns: BTreeSet<&'a str>,
}

pub(crate) type CollapsedChanges<'a> = BTreeMap<(&'a str, &'a str), CollapsedChange<'a>>;

pub(crate) fn collapse_changes(changes: &[LogChange]) -> CollapsedChanges<'_> {
    let mut collapsed = CollapsedChanges::new();
    for change in changes {
        let entry = collapsed
            .entry((change.table.as_str(), change.row_id.as_str()))
            .or_insert_with(|| CollapsedChange {
                old: &change.old_value,
                new: &change.new_value,
                changed_columns: BTreeSet::new(),
            });
        entry.new = &change.new_value;
        entry
            .changed_columns
            .extend(change.changed_columns.iter().map(String::as_str));
    }
    collapsed
}

/// Version 1 plans: SQL visibility. Source changes are decided in memory;
/// a visibility input change reconciles the collection.
fn legacy_step(subscription: &ReplicaSubscription, changes: &[LogChange]) -> Step {
    let dependencies = subscription
        .visibility
        .resolved_dependency_columns(&subscription.resolved);
    let (source_changed, dependency_changed) = table_change_relevance(
        &subscription.definition.table,
        &dependencies,
        changes.iter().filter(|change| {
            change.table == subscription.definition.table
                || subscription.visibility.change_concerns_member(
                    &subscription.resolved,
                    &change.table,
                    &change.old_value,
                    &change.new_value,
                )
        }),
    );
    if !source_changed && !dependency_changed {
        return Step::Skip;
    }
    if dependency_changed {
        return Step::Resync(Resync::Principal);
    }
    if bounded_sql_visibility(
        &subscription.definition,
        &subscription.visibility,
        &subscription.resolved,
    ) {
        return Step::Resync(Resync::Window);
    }
    Step::Apply(ApplyContext::default())
}

/// What one subscription did with one entity (table, key) in this event.
#[derive(Default)]
pub(crate) struct EntityTouch {
    /// Some subscription sent an upsert or delete for it.
    changed: bool,
    /// Some subscription held it before the event.
    old_held: bool,
    old_value: Option<serde_json::Map<String, Value>>,
    changed_columns: BTreeSet<String>,
    /// Per subscription id: its projection when it holds the row after the
    /// event, None when it does not.
    decided: BTreeMap<String, Option<Value>>,
}

impl EntityTouch {
    fn merge(&mut self, subscription: &str, touch: RowTouch) {
        self.changed |= touch.changed;
        if let Some(old) = touch.old {
            self.old_held = true;
            if let Value::Object(old) = old {
                self.old_value
                    .get_or_insert_with(Default::default)
                    .extend(old);
            }
        }
        self.changed_columns.extend(touch.changed_columns);
        self.decided.insert(subscription.to_owned(), touch.new);
    }
}

pub(crate) struct RowTouch {
    changed: bool,
    old: Option<Value>,
    new: Option<Value>,
    changed_columns: Vec<String>,
}

#[derive(Default)]
pub(crate) struct SubscriptionDelta {
    upserts: BTreeMap<String, Value>,
    deleted: BTreeSet<String>,
    touches: Vec<((String, String), RowTouch)>,
}

/// Applies the event's source rows (and rows a visibility change affected)
/// to one subscription's digest and window. None when the change cannot be
/// represented without the client's membership (the caller reconciles).
#[cfg(test)]
fn apply_rows(
    subscription: &mut ReplicaSubscription,
    context: &ApplyContext,
    collapsed: &CollapsedChanges<'_>,
) -> Result<Option<SubscriptionDelta>, ReplicaError> {
    apply_rows_with(subscription, context, collapsed, None, None)
}

/// Whether a plan's row decisions may be shared per visibility class: version
/// 2 plans without inherited (parent-row) visibility.
fn shareable_plan(plan: &VisibilityPlan) -> bool {
    plan.version == Some(2)
        && plan.inherited_columns().is_empty()
        && visibility::inherits_any(plan).is_empty()
}

/// `compiled`: the event's memo and the tenant database (class scope). With
/// it, rows are decided by the subscription's compiled visibility class.
fn apply_rows_with(
    subscription: &mut ReplicaSubscription,
    context: &ApplyContext,
    collapsed: &CollapsedChanges<'_>,
    mut compiled: Option<(&mut crate::visibility_class::EvalMemo, &str)>,
    share: Option<&crate::feed_share::EventShare>,
) -> Result<Option<SubscriptionDelta>, ReplicaError> {
    if context.before.is_some() {
        // The event changed this principal in memory: rebind.
        subscription.mask.clear();
    }
    let handle = compiled.as_ref().map(|(_, scope)| {
        subscription
            .mask
            .get(&subscription.visibility, &subscription.resolved, scope)
    });
    // Per-class decisions apply only when nothing about this subscription
    // but its class decides: the principal did not move in this event, no
    // affected-row read, complete window, compiled plan.
    let class_key = match (share, handle.as_ref()) {
        (Some(_), Some(crate::visibility_class::MemberHandle::Compiled(member)))
            if context.before.is_none()
                && context.overlay.is_none()
                && !subscription.window.truncated =>
        {
            Some((
                member.class().id(),
                Arc::as_ptr(&subscription.definition) as usize,
                Arc::<str>::from(
                    serde_json::to_string(&subscription.args)
                        .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))?,
                ),
                Arc::<str>::from(subscription.definition.table.as_str()),
            ))
        }
        _ => None,
    };
    let mut compiled = match (compiled.as_mut(), handle.as_ref()) {
        (Some((memo, _)), Some(handle)) => Some(Compiled {
            memo,
            handle,
            share: share.zip(class_key.as_ref()),
        }),
        _ => None,
    };
    let mut delta = SubscriptionDelta::default();
    let table = subscription.definition.table.clone();
    for ((_, id), change) in collapsed
        .range((table.as_str(), "")..)
        .take_while(|((source, _), _)| *source == table)
    {
        let changed_columns = change
            .changed_columns
            .iter()
            .map(|column| (*column).to_owned())
            .collect();
        if !row_step(
            subscription,
            context,
            id,
            change.old,
            change.new,
            changed_columns,
            &mut delta,
            compiled.as_mut(),
            true,
        )? {
            return Ok(None);
        }
    }
    for (id, row) in &context.affected {
        if !row_step(
            subscription,
            context,
            id,
            row,
            row,
            Vec::new(),
            &mut delta,
            compiled.as_mut(),
            false,
        )? {
            return Ok(None);
        }
    }
    if subscription.bounded() && !subscription.window.truncated && subscription.over_budget() {
        return Ok(None);
    }
    Ok(Some(delta))
}

/// What one row change means for one collection window. A pure function of
/// the collection (definition, arguments, plan), the principal before and
/// after the event, and the window: every subscription of the same
/// collection and visibility class gets the same decision, so a delivery
/// layer can decide once per class and apply the result per subscriber.
#[derive(Clone, Debug, PartialEq)]
pub enum RowDecision {
    /// The client neither held nor may hold the row.
    Outside,
    /// The client held the row and keeps it unchanged.
    Unchanged { row: Value },
    /// The client gains or replaces the row (`old` is what it held).
    Upsert {
        old: Option<(Value, String, i64)>,
        row: Value,
        hash: String,
        bytes: i64,
    },
    /// The client loses the row it held.
    Delete {
        old: Value,
        hash: String,
        bytes: i64,
    },
    /// The window cannot be maintained from the images (reconcile).
    Reconcile,
}

/// Decides one row change (`old`/`new` full or projected source images,
/// `Value::Null` when absent) for a window. `before` is the principal the
/// client's rows were authorized with (with `overlay` restoring the parents'
/// before-images); `after` the principal after the event.
#[allow(clippy::too_many_arguments)]
pub fn decide_row(
    definition: &ReplicaCollectionDefinition,
    args: &Value,
    plan: &VisibilityPlan,
    before: &ResolvedVisibility,
    after: &ResolvedVisibility,
    overlay: &Option<ParentOverlay>,
    window: &ReplicaWindow,
    old: &Value,
    new: &Value,
) -> Result<RowDecision, ReplicaError> {
    decide_row_with(
        definition,
        args,
        plan,
        before,
        after,
        overlay,
        window,
        old,
        new,
        &mut |_, _| None,
    )
}

/// `decide_row` with a compiled visibility check: `compiled(row, is_old)`
/// answers the row's visibility (collection filters are applied here), or
/// None to use the interpreted evaluator.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decide_row_with(
    definition: &ReplicaCollectionDefinition,
    args: &Value,
    plan: &VisibilityPlan,
    before: &ResolvedVisibility,
    after: &ResolvedVisibility,
    overlay: &Option<ParentOverlay>,
    window: &ReplicaWindow,
    old: &Value,
    new: &Value,
    compiled: &mut dyn FnMut(&Value, bool) -> Option<bool>,
) -> Result<RowDecision, ReplicaError> {
    let old_visible = !old.is_null()
        && collection_filters_admit(old, args, definition)
        && match compiled(old, true) {
            Some(visible) => visible,
            None => visibility::with_parent_overlay(overlay, || visible(plan, before, old)),
        };
    let new_visible = !new.is_null()
        && collection_filters_admit(new, args, definition)
        && match compiled(new, false) {
            Some(visible) => visible,
            None => visible(plan, after, new),
        };
    if !old_visible && !new_visible {
        return Ok(RowDecision::Outside);
    }
    let (mut old_in, mut new_in) = (old_visible, new_visible);
    if window.truncated {
        let admits = |row: &Value| {
            if definition.order_by.is_empty() {
                return None;
            }
            window
                .boundary
                .as_deref()
                .map(|boundary| within_boundary(definition, boundary, row))
        };
        let (Some(old_admitted), Some(new_admitted)) = (admits(old), admits(new)) else {
            return Ok(RowDecision::Reconcile);
        };
        old_in &= old_admitted;
        new_in &= new_admitted;
        if old_in != new_in {
            // The window gains or loses a row: its other edge moves too.
            return Ok(RowDecision::Reconcile);
        }
        if !old_in {
            return Ok(RowDecision::Outside);
        }
        let boundary = window.boundary.as_deref().expect("admitted");
        if new.get(&definition.key) == Some(&boundary.1)
            && new.get(&definition.order_by) != Some(&boundary.0)
        {
            return Ok(RowDecision::Reconcile);
        }
    }
    let canonical = |row: Value| {
        let canonical = stable_json(&row);
        (
            hex_digest(canonical.as_bytes()),
            canonical.len() as i64,
            row,
        )
    };
    let old = old_in
        .then(|| project_row(old, definition).map(canonical))
        .transpose()?;
    let new = new_in
        .then(|| project_row(new, definition).map(canonical))
        .transpose()?;
    Ok(match (old, new) {
        (Some((old_hash, _, row)), Some((hash, _, _))) if old_hash == hash => {
            RowDecision::Unchanged { row }
        }
        (old, Some((hash, bytes, row))) => RowDecision::Upsert {
            old: old.map(|(hash, bytes, row)| (row, hash, bytes)),
            row,
            hash,
            bytes,
        },
        (Some((hash, bytes, old)), None) => RowDecision::Delete { old, hash, bytes },
        (None, None) => RowDecision::Outside,
    })
}

/// One row's transition for one subscription. Returns false when the window
/// cannot be maintained from the images alone.
/// A subscription's compiled binding and the event's memo.
struct Compiled<'a> {
    memo: &'a mut crate::visibility_class::EvalMemo,
    handle: &'a crate::visibility_class::MemberHandle,
    /// The event share and this subscription's (class, definition, args,
    /// table) when its decisions may be shared per class.
    share: Option<SharedDecision<'a>>,
}

#[allow(clippy::too_many_arguments)]
fn row_step(
    subscription: &mut ReplicaSubscription,
    context: &ApplyContext,
    id: &str,
    old: &Value,
    new: &Value,
    changed_columns: Vec<String>,
    delta: &mut SubscriptionDelta,
    compiled: Option<&mut Compiled<'_>>,
    memoize: bool,
) -> Result<bool, ReplicaError> {
    // The binding is of the principal after the event; the old image is
    // decided by it only when the principal and the parents did not move.
    let same_principal = context.before.is_none() && context.overlay.is_none();
    let mut compiled = compiled;
    // A class-shared decision: only for change rows (memoized images) that
    // hit no member atom of this subscription's member.
    let shared_key = match compiled.as_deref_mut() {
        Some(Compiled {
            memo,
            handle: crate::visibility_class::MemberHandle::Compiled(member),
            share: Some((share, (class, definition, args, table))),
        }) if memoize && same_principal => {
            let hit = (!old.is_null() && member.hit_memo(memo, old))
                || (!new.is_null() && member.hit_memo(memo, new));
            (!hit).then(|| {
                (
                    *share,
                    crate::feed_share::DecisionKey {
                        class: *class,
                        definition: *definition,
                        args: Arc::clone(args),
                        table: Arc::clone(table),
                        row: Arc::from(id),
                    },
                )
            })
        }
        _ => None,
    };
    let cached = shared_key
        .as_ref()
        .and_then(|(share, key)| share.decision(key));
    let mut check = |row: &Value, is_old: bool| -> Option<bool> {
        use crate::visibility_class::{MemberHandle, RowView};
        let compiled = compiled.as_deref_mut()?;
        if is_old && !same_principal {
            return None;
        }
        match compiled.handle {
            MemberHandle::Compiled(member) if memoize => {
                Some(member.visible_memo(compiled.memo, row))
            }
            MemberHandle::Compiled(member) => {
                compiled.memo.stats.compiled_checks += 1;
                Some(member.visible(RowView::Value(row)))
            }
            MemberHandle::Fallback => {
                compiled.memo.stats.fallback_checks += 1;
                None
            }
        }
    };
    let decision = match cached {
        Some(decision) => RowDecision::clone(&decision),
        None => {
            let decision = decide_row_with(
                &subscription.definition,
                &subscription.args,
                &subscription.visibility,
                context.before.as_deref().unwrap_or(&subscription.resolved),
                &subscription.resolved,
                &context.overlay,
                &subscription.window,
                old,
                new,
                &mut check,
            )?;
            if let Some((share, key)) = shared_key {
                share.insert(key, Arc::new(decision.clone()));
            }
            decision
        }
    };
    let (changed, old, new) = match decision {
        RowDecision::Outside => return Ok(true),
        RowDecision::Reconcile => return Ok(false),
        RowDecision::Unchanged { row } => (false, Some(row.clone()), Some(row)),
        RowDecision::Upsert {
            old,
            row,
            hash,
            bytes,
        } => {
            let old = old.map(|(old, hash, bytes)| {
                subscription.digest.remove(&hash);
                subscription.window.rows = subscription.window.rows.saturating_sub(1);
                subscription.window.bytes -= bytes;
                old
            });
            subscription.digest.add(&hash);
            subscription.window.rows += 1;
            subscription.window.bytes += bytes;
            delta.deleted.remove(id);
            delta.upserts.insert(id.to_owned(), row.clone());
            (true, old, Some(row))
        }
        RowDecision::Delete { old, hash, bytes } => {
            subscription.digest.remove(&hash);
            subscription.window.rows = subscription.window.rows.saturating_sub(1);
            subscription.window.bytes -= bytes;
            delta.upserts.remove(id);
            delta.deleted.insert(id.to_owned());
            (true, Some(old), None)
        }
    };
    delta.touches.push((
        (subscription.definition.table.clone(), id.to_owned()),
        RowTouch {
            changed,
            old,
            new,
            changed_columns,
        },
    ));
    Ok(true)
}

/// One normalized entity change per (table, key): the union of the
/// projections every collection of this connection holds after the event.
/// A collection that did not decide the row in this event is asked through
/// its in-memory membership; an entity is deleted only when no collection of
/// the connection holds it (the SDK removes a deleted entity from every
/// window of that entity).
fn normalize_entity_changes(
    touches: BTreeMap<(String, String), EntityTouch>,
    collapsed: &CollapsedChanges<'_>,
    subscriptions: &BTreeMap<String, ReplicaSubscription>,
) -> Vec<ReplicaChange> {
    let mut changes = Vec::new();
    for ((table, id), touch) in touches {
        if !touch.changed {
            continue;
        }
        let image = collapsed
            .get(&(table.as_str(), id.as_str()))
            .map(|change| change.new);
        let mut fields = serde_json::Map::new();
        let mut unknown = false;
        for subscription in subscriptions
            .values()
            .filter(|subscription| subscription.definition.table == table)
        {
            if let Some(decided) = touch.decided.get(&subscription.id) {
                if let Some(Value::Object(row)) = decided {
                    fields.extend(row.clone());
                }
                continue;
            }
            match image.map(|row| (row, subscription.holds(row))) {
                Some((row, Some(true))) => {
                    if let Ok(Value::Object(row)) = project_row(row, &subscription.definition) {
                        fields.extend(row);
                    }
                }
                Some((_, Some(false))) => {}
                _ => unknown = true,
            }
        }
        let changed_columns = touch.changed_columns.into_iter().collect();
        if fields.is_empty() {
            if unknown {
                // Another collection may still hold it: its delta decides.
                continue;
            }
            changes.push(ReplicaChange {
                entity: table,
                id,
                operation: "delete".to_owned(),
                old_value: touch.old_value.map(Value::Object),
                new_value: None,
                changed_columns,
            });
        } else {
            changes.push(ReplicaChange {
                entity: table,
                id,
                operation: if touch.old_held { "update" } else { "insert" }.to_owned(),
                old_value: touch.old_value.map(Value::Object),
                new_value: Some(Value::Object(fields)),
                changed_columns,
            });
        }
    }
    changes
}

async fn resume_changes(
    transaction: &mut TenantTransaction,
    after: u64,
    through: u64,
    tables: BTreeSet<String>,
) -> Result<Vec<LogChange>, ReplicaError> {
    let tables = serde_json::to_string(&tables.into_iter().collect::<Vec<_>>())
        .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))?;
    let rows = sqlx::query(
            "SELECT revision, ordinal, table_name, row_id, operation, changed_columns, old_value, new_value
             FROM _gonvex_sync_changes
             WHERE revision > $1 AND revision <= $2 AND table_name IN (SELECT value FROM json_each($3))
             ORDER BY revision, ordinal",
        ).bind(after as i64).bind(through as i64).bind(tables).fetch_all(&mut **transaction.transaction()).await?;
    rows.iter()
        .map(|row| {
            Ok(LogChange {
                revision: row.try_get::<i64, _>("revision")?.max(0) as u64,
                ordinal: row.try_get::<Option<i64>, _>("ordinal")?.unwrap_or(0) as i32,
                table: row.try_get("table_name")?,
                row_id: row.try_get("row_id")?,
                operation: row.try_get("operation")?,
                changed_columns: match log_json(row, "changed_columns")? {
                    Value::Array(values) => values
                        .into_iter()
                        .filter_map(|value| value.as_str().map(str::to_owned))
                        .collect(),
                    _ => Vec::new(),
                },
                old_value: log_json(row, "old_value")?,
                new_value: log_json(row, "new_value")?,
                origin_command_id: String::new(),
                provenance: Default::default(),
            })
        })
        .collect()
}

/// Row images and changed columns are JSON text written by the triggers.
fn log_json(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<Value, ReplicaError> {
    match row.try_get::<Option<String>, _>(column)? {
        Some(text) => serde_json::from_str(&text)
            .map_err(|error| ReplicaError::InvalidIdentifier(format!("{column}: {error}"))),
        None => Ok(Value::Null),
    }
}

async fn clock_in_transaction(
    transaction: &mut TenantTransaction,
) -> Result<ReplicaClock, ReplicaError> {
    let row = sqlx::query(
        "SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = 1",
    )
    .fetch_one(&mut **transaction.transaction())
    .await?;
    Ok(ReplicaClock {
        epoch: row.get("epoch"),
        revision: row.get::<i64, _>("revision").max(0) as u64,
        retained_revision: row.get::<i64, _>("retained_revision").max(0) as u64,
    })
}

/// The visible rows of a collection and whether a budget truncated them.
#[cfg(test)]
pub(super) async fn query_rows(
    transaction: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    args: &Value,
) -> Result<(Vec<Value>, bool), ReplicaError> {
    let mut rows = Vec::new();
    let window = scan_window(transaction, definition, plan, resolved, args, |row| {
        rows.push(row.row);
        Ok(())
    })
    .await?;
    Ok((rows, window.truncated))
}

/// Streams the collection's visible rows (in order, within its budgets) to
/// `visit` one at a time and returns the resulting window. Nothing but the
/// visitor's own state outlives a row.
pub(crate) async fn scan_window(
    transaction: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    args: &Value,
    visit: impl FnMut(ScannedRow) -> Result<(), ReplicaError>,
) -> Result<ReplicaWindow, ReplicaError> {
    let args = args.as_object().ok_or(ReplicaError::InvalidArguments)?;
    let mut parameters = Vec::new();
    let visibility = visibility::compile_predicate(plan, resolved, "r", &mut parameters)?;
    let mut predicates = vec![visibility];
    // Verified support principals have no persisted Member. Keep the live
    // membership check for ordinary sessions, including concurrent revocation.
    if !resolved.ephemeral_member {
        parameters.push(Value::String(resolved.direct["account.id"].clone()));
        predicates.push(format!(
            "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = ${} AND _gonvex_member.status = 'active')",
            parameters.len()
        ));
    }
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
    let columns = declared_columns(transaction, definition, definition.columns.iter()).await?;
    let mut statement = format!(
        "SELECT {} FROM {} AS r WHERE {}",
        gonvex_store::rows::json_object_expr("r", &columns),
        quote(&definition.table)?,
        predicates.join(" AND ")
    );
    if !definition.order_by.is_empty() {
        let direction = if definition.order_direction == "asc" {
            "ASC"
        } else {
            "DESC"
        };
        // The key breaks ties so a truncated window has one exact boundary.
        statement.push_str(&format!(
            " ORDER BY r.{} {direction}, r.{} {direction}",
            quote(&definition.order_by)?,
            quote(&definition.key)?
        ));
    }
    if definition.max_rows > 0 {
        parameters.push(Value::from((definition.max_rows + 1) as u64));
        statement.push_str(&format!(" LIMIT ${}", parameters.len()));
    }
    let parameters = Parameters::scalars(&parameters).map_err(ReplicaError::InvalidIdentifier)?;
    // Rows stream as JSON text from a blocking thread on the snapshot's own
    // reader connection (`TenantTransaction::stream_json_rows`), in batches,
    // and are decoded and admitted one at a time. SQL still uses
    // maxRows + 1; a byte-limited source must not first allocate its entire
    // SQL result.
    let rows = transaction
        .stream_json_rows(statement, parameters.into_owned_binds())
        .await?;
    let rows = futures_util::stream::unfold(rows, |mut rows| async move {
        rows.next_batch().await.map(|batch| (batch, rows))
    })
    .flat_map(|batch| {
        futures_util::stream::iter(match batch {
            Ok(batch) => batch
                .rows()
                .map(|row| {
                    serde_json::from_slice::<Value>(row)
                        .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))
                })
                .collect::<Vec<_>>(),
            Err(error) => vec![Err(ReplicaError::from(error))],
        })
    });
    collect_window(rows, definition, visit).await
}

/// `columns` with their declared kinds (JSON, BOOLEAN decide the encoding).
async fn declared_columns<'a>(
    transaction: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    columns: impl Iterator<Item = &'a String>,
) -> Result<Vec<(String, gonvex_store::rows::JsonKind)>, ReplicaError> {
    let declared = gonvex_store::rows::table_columns(transaction.transaction(), &definition.table)
        .await?
        .into_iter()
        .map(|column| (column.name.clone(), column.kind()))
        .collect::<BTreeMap<_, _>>();
    columns
        .map(|column| {
            quote(column)?;
            Ok((
                column.clone(),
                declared
                    .get(column)
                    .copied()
                    .unwrap_or(gonvex_store::rows::JsonKind::Text),
            ))
        })
        .collect()
}

async fn collect_window(
    rows: impl Stream<Item = Result<Value, ReplicaError>>,
    definition: &ReplicaCollectionDefinition,
    mut visit: impl FnMut(ScannedRow) -> Result<(), ReplicaError>,
) -> Result<ReplicaWindow, ReplicaError> {
    futures_util::pin_mut!(rows);
    let mut window = ReplicaWindow::default();
    let mut seen = std::collections::HashSet::new();
    let mut last = None;
    let ordered = !definition.order_by.is_empty();
    while let Some(row) = rows.next().await {
        if window.rows % 32 == 0 {
            tokio::task::yield_now().await;
        }
        let row = row?;
        let key = row_key(&row, &definition.key)?;
        if !seen.insert(key.clone()) {
            return Err(ReplicaError::DuplicateKey(key));
        }
        if definition.max_rows > 0 && window.rows >= definition.max_rows {
            window.truncated = true;
            break;
        }
        let canonical = stable_json(&row);
        let bytes = canonical.len() as i64;
        if definition.max_bytes > 0 && window.bytes + bytes > definition.max_bytes {
            window.truncated = true;
            break;
        }
        window.rows += 1;
        window.bytes += bytes;
        if ordered {
            last = Some((
                row.get(&definition.order_by)
                    .cloned()
                    .unwrap_or(Value::Null),
                row.get(&definition.key).cloned().unwrap_or(Value::Null),
            ));
        }
        visit(ScannedRow {
            key,
            hash: hex_digest(canonical.as_bytes()),
            row,
        })?;
    }
    if window.truncated {
        window.boundary = last.map(Box::new);
    }
    Ok(window)
}

/// SQLite's ordering of the JSON scalars a collection orders by: NULL, then
/// numbers (booleans are integers), then text (binary collation).
fn sql_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    fn class(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(_) | Value::Number(_) => 1,
            Value::String(_) => 2,
            _ => 3,
        }
    }
    fn number(value: &Value) -> f64 {
        match value {
            Value::Bool(value) => f64::from(u8::from(*value)),
            Value::Number(value) => value.as_f64().unwrap_or_default(),
            _ => 0.0,
        }
    }
    class(left)
        .cmp(&class(right))
        .then_with(|| match (left, right) {
            (Value::String(left), Value::String(right)) => left.as_bytes().cmp(right.as_bytes()),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Bool(_) | Value::Number(_), Value::Bool(_) | Value::Number(_)) => {
                number(left).total_cmp(&number(right))
            }
            _ => left.to_string().cmp(&right.to_string()),
        })
}

/// Does `row` sort at or before the window's last row?
fn within_boundary(
    definition: &ReplicaCollectionDefinition,
    boundary: &(Value, Value),
    row: &Value,
) -> bool {
    let null = Value::Null;
    let order = sql_order(row.get(&definition.order_by).unwrap_or(&null), &boundary.0)
        .then_with(|| sql_order(row.get(&definition.key).unwrap_or(&null), &boundary.1));
    if definition.order_direction == "asc" {
        order.is_le()
    } else {
        order.is_ge()
    }
}

fn row_in_collection(
    row: &Value,
    args: &Value,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> bool {
    collection_filters_admit(row, args, definition) && visible(plan, resolved, row)
}

/// The collection's own filters (exclusions and argument equalities).
fn collection_filters_admit(
    row: &Value,
    args: &Value,
    definition: &ReplicaCollectionDefinition,
) -> bool {
    let (Some(object), Some(args)) = (row.as_object(), args.as_object()) else {
        return false;
    };
    if definition
        .exclude_when_set
        .iter()
        .any(|column| object.get(column).is_some_and(|value| !value.is_null()))
    {
        return false;
    }
    if definition
        .equal_filters
        .iter()
        .any(|(column, argument)| object.get(column) != args.get(argument))
    {
        return false;
    }
    true
}

/// The one call into the in-memory visibility evaluator (workstream 7 swaps
/// its implementation in here).
#[inline]
fn visible(plan: &VisibilityPlan, resolved: &ResolvedVisibility, row: &Value) -> bool {
    visibility::row_matches(plan, resolved, row)
}

/// Version 1 plans whose sets stay in SQL cannot decide bounded windows in
/// memory.
fn bounded_sql_visibility(
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> bool {
    plan.version != Some(2)
        && (definition.max_rows > 0 || definition.max_bytes > 0)
        && plan.needs_materialized_sets(resolved)
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

fn xor_hash(aggregate: &mut [u8; 32], hash: &str) {
    for (index, byte) in aggregate.iter_mut().enumerate() {
        *byte ^= u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16).unwrap_or(0);
    }
}

#[cfg(test)]
fn aggregate_bytes<K: Ord, V: AsRef<str>>(hashes: &BTreeMap<K, V>) -> [u8; 32] {
    let mut aggregate = [0u8; 32];
    for hash in hashes.values() {
        xor_hash(&mut aggregate, hash.as_ref());
    }
    aggregate
}

#[cfg(test)]
fn aggregate_digest<K: Ord, V: AsRef<str>>(hashes: &BTreeMap<K, V>) -> String {
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

fn hashes_digest<K: Ord + AsRef<str>, V: AsRef<str>>(hashes: &BTreeMap<K, V>) -> String {
    // Same bytes as stable_json([[key, hash], ...]) without building Values.
    let mut out = String::with_capacity(2 + hashes.len() * 100);
    out.push('[');
    for (index, (key, hash)) in hashes.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('[');
        write_stable_string(&mut out, key.as_ref());
        out.push(',');
        write_stable_string(&mut out, hash.as_ref());
        out.push(']');
    }
    out.push(']');
    hex_digest(out.as_bytes())
}

fn stable_json(value: &Value) -> String {
    let mut out = String::new();
    write_stable_json(&mut out, value);
    out
}

/// Canonical JSON (sorted object keys, JS number formatting, U+2028/U+2029
/// escaped) written into one buffer. Row hashes hash these bytes, so the
/// output must not change; it runs for every row of every snapshot.
fn write_stable_json(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => {
            use std::fmt::Write as _;
            if let Some(value) = value.as_i64() {
                let _ = write!(out, "{value}");
            } else if let Some(value) = value.as_u64() {
                let _ = write!(out, "{value}");
            } else if let Some(value) = value.as_f64() {
                out.push_str(ryu_js::Buffer::new().format(value));
            } else {
                out.push_str("null");
            }
        }
        Value::String(value) => write_stable_string(out, value),
        Value::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_stable_json(out, value);
            }
            out.push(']');
        }
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_stable_string(out, key);
                out.push(':');
                write_stable_json(out, &values[key]);
            }
            out.push('}');
        }
    }
}

fn write_stable_string(out: &mut String, value: &str) {
    let start = out.len();
    // serde_json escapes quotes, backslashes and control characters and
    // writes valid UTF-8, so the buffer stays a valid String.
    let bytes = unsafe { out.as_mut_vec() };
    if serde_json::to_writer(&mut *bytes, value).is_err() {
        bytes.truncate(start);
        bytes.extend_from_slice(b"\"\"");
    }
    if out[start..].contains(['\u{2028}', '\u{2029}']) {
        let tail = out.split_off(start);
        out.push_str(
            &tail
                .replace('\u{2028}', "\\u2028")
                .replace('\u{2029}', "\\u2029"),
        );
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

#[allow(clippy::too_many_arguments)]
fn replica_snapshot_message(
    request: &ReplicaOpenRequest,
    definition: &ReplicaCollectionDefinition,
    rows: Vec<Arc<Value>>,
    inline_hashes: Option<BTreeMap<String, String>>,
    truncated: bool,
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
        ("truncated".to_owned(), Value::Bool(truncated)),
    ]);
    metadata.insert("pageDigest".to_owned(), Value::String(digest.to_owned()));
    if let Some(hashes) = inline_hashes {
        metadata.insert("digest".to_owned(), Value::String(hashes_digest(&hashes)));
        metadata.insert(
            "hashes".to_owned(),
            Value::Object(
                hashes
                    .into_iter()
                    .map(|(key, hash)| (key, Value::String(hash)))
                    .collect(),
            ),
        );
    }
    ServerMessage::ReplicaSnapshot {
        id: request.id.clone(),
        result: rows,
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
    rows: std::iter::Peekable<std::vec::IntoIter<Arc<Value>>>,
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

    /// Rows not yet paged.
    pub(crate) fn remaining_rows(&self) -> usize {
        self.rows.len()
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

    fn page(&self, result: Vec<Arc<Value>>, last: bool) -> ServerMessage {
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
                canonical_rows: result.iter().map(|row| stable_json(row)).collect(),
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

pub(crate) fn json_byte_len(value: &Value) -> usize {
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

    fn paged_fixture(rows: Vec<Value>) -> ServerMessage {
        let hashes = rows
            .iter()
            .map(|row| (row_key(row, "id").unwrap(), row_hash(row)))
            .collect();
        ServerMessage::ReplicaSnapshot {
            id: "open".into(),
            result: rows.into_iter().map(Arc::new).collect(),
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
    fn sdk_resume_aggregate_matches_client_xor_fixture() {
        let hashes: BTreeMap<String, String> =
            BTreeMap::from([("a".into(), "01".repeat(32)), ("b".into(), "03".repeat(32))]);
        assert_eq!(
            aggregate_digest(&hashes),
            format!("xor1:{}", "02".repeat(32))
        );
        assert_eq!(
            aggregate_digest(&BTreeMap::<String, String>::new()),
            format!("xor1:{}", "00".repeat(32))
        );
        let mut aggregate = aggregate_bytes(&hashes);
        xor_hash(&mut aggregate, &hashes["a"]);
        xor_hash(&mut aggregate, &"05".repeat(32));
        assert_eq!(
            format_aggregate(&aggregate),
            aggregate_digest(&BTreeMap::<String, String>::from([
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

    fn visibility_with_task_dependency(source: &str) -> VisibilityPlan {
        VisibilityPlan {
            active_sets_cache: Default::default(),
            version: None,
            permission_tables: Vec::new(),
            table: source.to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::from([(
                "createdTasks".to_owned(),
                crate::visibility::VisibilitySet {
                    kind: String::new(),
                    from: String::new(),
                    key: String::new(),
                    table: "tasks".to_owned(),
                    alias: String::new(),
                    select: "id".to_owned(),
                    select_from: String::new(),
                    joins: Vec::new(),
                    constraints: vec![crate::visibility::VisibilityConstraint {
                        in_set: String::new(),
                        table: String::new(),
                        column: "createdBy".to_owned(),
                        context: "member.id".to_owned(),
                        value: None,
                    }],
                },
            )]),
            predicate: crate::visibility::VisibilityExpression {
                on_missing: None,
                predicate: None,
                parent_where: Vec::new(),
                via: None,
                table: String::new(),
                key: String::new(),
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
                active_sets_cache: Default::default(),
                version: None,
                permission_tables: Vec::new(),
                table: "tasks".to_owned(),
                key: "id".to_owned(),
                sets: BTreeMap::new(),
                predicate: crate::visibility::VisibilityExpression {
                    on_missing: None,
                    predicate: None,
                    parent_where: Vec::new(),
                    via: None,
                    table: String::new(),
                    key: String::new(),
                    operator: "public".to_owned(),
                    column: String::new(),
                    context: String::new(),
                    set: String::new(),
                    value: None,
                    children: Vec::new(),
                },
            },
            ResolvedVisibility {
                inherited: Default::default(),
                indexes: Default::default(),
                ephemeral_member: false,
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
            active_sets_cache: Default::default(),
            version: None,
            permission_tables: Vec::new(),
            table: "tasks".to_owned(),
            key: "id".to_owned(),
            sets: BTreeMap::new(),
            predicate: crate::visibility::VisibilityExpression {
                on_missing: None,
                predicate: None,
                parent_where: Vec::new(),
                via: None,
                table: String::new(),
                key: String::new(),
                operator: "public".to_owned(),
                column: String::new(),
                context: String::new(),
                set: String::new(),
                value: None,
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

    fn collection(value: Value) -> ReplicaCollectionDefinition {
        let mut definition = serde_json::json!({
            "table":"tasks", "key":"id", "columns":["id","title","workspaceId","rank"]
        });
        definition
            .as_object_mut()
            .unwrap()
            .extend(value.as_object().unwrap().clone());
        serde_json::from_value(definition).unwrap()
    }

    /// A subscription whose client holds `rows` (projections).
    fn holding(
        definition: ReplicaCollectionDefinition,
        args: Value,
        rows: &[Value],
    ) -> ReplicaSubscription {
        let (plan, resolved) = public_visibility();
        let mut digest = ReplicaAggregate::new(false);
        let mut window = ReplicaWindow::default();
        for row in rows {
            let canonical = stable_json(row);
            digest.add(&hex_digest(canonical.as_bytes()));
            window.rows += 1;
            window.bytes += canonical.len() as i64;
        }
        ReplicaSubscription {
            id: "s".into(),
            path: "tasks.list".into(),
            args,
            definition: Arc::new(definition),
            visibility: Arc::new(plan),
            visibility_scope: String::new(),
            cursor: ReplicaCursor {
                epoch: "e".into(),
                revision: 1,
            },
            resolved,
            digest,
            window,
            mask: Default::default(),
            concern_tables: None,
            parent_acl_columns: None,
        }
    }

    fn row_change(id: &str, old: Value, new: Value) -> LogChange {
        let mut change = source_change(old, new);
        change.row_id = id.into();
        change
    }

    /// Applies one event to `subscription` and to a model of its client's
    /// rows; checks the digest and counters against the model.
    fn apply_to_model(
        subscription: &mut ReplicaSubscription,
        client: &mut BTreeMap<String, Value>,
        changes: &[LogChange],
    ) -> Option<(Vec<String>, Vec<String>)> {
        let collapsed = collapse_changes(changes);
        let delta = apply_rows(subscription, &ApplyContext::default(), &collapsed).unwrap()?;
        for (id, row) in &delta.upserts {
            client.insert(id.clone(), row.clone());
        }
        for id in &delta.deleted {
            client.remove(id);
        }
        let hashes = client
            .values()
            .map(|row| (row_key(row, "id").unwrap(), row_hash(row)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(subscription.digest.digest(), aggregate_digest(&hashes));
        assert_eq!(subscription.window.rows, client.len());
        assert_eq!(
            subscription.window.bytes,
            client
                .values()
                .map(|row| stable_json(row).len() as i64)
                .sum::<i64>()
        );
        Some((
            delta.upserts.into_keys().collect(),
            delta.deleted.into_iter().collect(),
        ))
    }

    #[test]
    fn feed_deltas_follow_before_and_after_images_without_held_rows() {
        let definition =
            collection(serde_json::json!({"equalFilters":{"workspaceId":"workspace"}}));
        let row = |id: &str, title: &str, workspace: &str| serde_json::json!({"id":id, "title":title, "workspaceId":workspace, "rank":1});
        let mut client = BTreeMap::from([
            ("1".to_owned(), row("1", "one", "a")),
            ("2".to_owned(), row("2", "two", "a")),
        ]);
        let held = client.values().cloned().collect::<Vec<_>>();
        let mut subscription = holding(definition, serde_json::json!({"workspace":"a"}), &held);
        let (upserts, deleted) = apply_to_model(
            &mut subscription,
            &mut client,
            &[
                row_change("1", row("1", "one", "a"), row("1", "uno", "a")),
                row_change("3", Value::Null, row("3", "three", "a")),
                row_change("2", row("2", "two", "a"), Value::Null),
                // Never held (other workspace): no delete is published.
                row_change("9", row("9", "nine", "b"), Value::Null),
                // Leaves the collection by moving workspace.
                row_change("4", row("4", "four", "b"), row("4", "four", "c")),
            ],
        )
        .unwrap();
        assert_eq!(upserts, ["1", "3"]);
        assert_eq!(deleted, ["2"]);
        let (upserts, deleted) = apply_to_model(
            &mut subscription,
            &mut client,
            &[row_change("1", row("1", "uno", "a"), row("1", "uno", "b"))],
        )
        .unwrap();
        assert!(upserts.is_empty());
        assert_eq!(deleted, ["1"]);
    }

    #[test]
    fn unprojected_changes_and_net_noops_publish_nothing() {
        let definition = collection(serde_json::json!({}));
        let row = serde_json::json!({"id":"1", "title":"t", "workspaceId":"a", "rank":1});
        let mut client = BTreeMap::from([("1".to_owned(), row.clone())]);
        let mut subscription = holding(
            definition,
            serde_json::json!({}),
            std::slice::from_ref(&row),
        );
        let mut wider = row.clone();
        wider["internalNotes"] = "not replicated".into();
        let mut renamed = row.clone();
        renamed["title"] = "intermediate".into();
        let mut deleted = renamed.clone();
        deleted["title"] = "gone".into();
        let (upserts, removed) = apply_to_model(
            &mut subscription,
            &mut client,
            &[
                row_change("1", row.clone(), wider),
                // Inserted and deleted within one transaction.
                row_change("5", Value::Null, renamed.clone()),
                row_change("5", renamed, Value::Null),
            ],
        )
        .unwrap();
        assert!(upserts.is_empty() && removed.is_empty());
        // One atomic cleanup first edits and then soft-deletes the same row:
        // only the final membership is published.
        let mut definition = collection(serde_json::json!({"excludeWhenSet":["deletedAt"]}));
        definition.columns.push("deletedAt".into());
        let mut subscription = holding(
            definition,
            serde_json::json!({}),
            std::slice::from_ref(&row),
        );
        let mut client = BTreeMap::from([("1".to_owned(), row.clone())]);
        let mut edited = row.clone();
        edited["title"] = "edited".into();
        let mut soft_deleted = edited.clone();
        soft_deleted["deletedAt"] = 5.into();
        let (upserts, removed) = apply_to_model(
            &mut subscription,
            &mut client,
            &[
                row_change("1", row, edited.clone()),
                row_change("1", edited, soft_deleted),
            ],
        )
        .unwrap();
        assert!(upserts.is_empty());
        assert_eq!(removed, ["1"]);
    }

    #[test]
    fn complete_bounded_collection_stays_incremental_until_a_budget_is_crossed() {
        let row = |id: &str, title: &str| serde_json::json!({"id":id, "title":title, "workspaceId":"a", "rank":1});
        let held = [row("1", "one")];
        let mut client = BTreeMap::from([("1".to_owned(), row("1", "one"))]);
        let mut subscription = holding(
            collection(serde_json::json!({"maxRows":2, "maxBytes":1000, "mode":"progressive"})),
            serde_json::json!({}),
            &held,
        );
        assert!(apply_to_model(
            &mut subscription,
            &mut client,
            &[row_change("2", Value::Null, row("2", "two"))]
        )
        .is_some());
        // A third row crosses maxRows: the window must be rebuilt.
        assert!(apply_to_model(
            &mut subscription,
            &mut client.clone(),
            &[row_change("3", Value::Null, row("3", "three"))]
        )
        .is_none());
        let mut subscription = holding(
            collection(serde_json::json!({"maxRows":5, "maxBytes":80})),
            serde_json::json!({}),
            &held,
        );
        let mut client = BTreeMap::from([("1".to_owned(), row("1", "one"))]);
        assert!(apply_to_model(
            &mut subscription,
            &mut client,
            &[row_change(
                "1",
                row("1", "one"),
                row("1", "a title that is far too large for the budget")
            )]
        )
        .is_none());
    }

    #[test]
    fn truncated_window_moves_only_within_its_boundary() {
        let row = |id: &str, rank: i64, title: &str| serde_json::json!({"id":id, "title":title, "workspaceId":"a", "rank":rank});
        let definition =
            collection(serde_json::json!({"maxRows":2, "orderBy":"rank", "orderDirection":"asc"}));
        let held = [row("1", 1, "one"), row("2", 2, "two")];
        let mut client = held
            .iter()
            .map(|row| (row_key(row, "id").unwrap(), row.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut subscription = holding(definition, serde_json::json!({}), &held);
        subscription.window.truncated = true;
        subscription.window.boundary = Some(Box::new((2.into(), "2".into())));
        // Beyond the boundary: the client never held it.
        let (upserts, deleted) = apply_to_model(
            &mut subscription,
            &mut client,
            &[
                row_change("7", row("7", 7, "x"), row("7", 8, "y")),
                row_change("8", row("8", 9, "z"), Value::Null),
            ],
        )
        .unwrap();
        assert!(upserts.is_empty() && deleted.is_empty());
        // In place inside the window.
        let (upserts, _) = apply_to_model(
            &mut subscription,
            &mut client,
            &[row_change("1", row("1", 1, "one"), row("1", 1, "uno"))],
        )
        .unwrap();
        assert_eq!(upserts, ["1"]);
        // Entering, leaving, or moving the boundary row rebuilds the window.
        for change in [
            row_change("0", Value::Null, row("0", 0, "zero")),
            row_change("1", row("1", 1, "uno"), row("1", 5, "uno")),
            row_change("1", row("1", 1, "uno"), Value::Null),
            row_change("2", row("2", 2, "two"), row("2", 0, "two")),
        ] {
            assert!(apply_to_model(&mut subscription, &mut client.clone(), &[change]).is_none());
        }
        // An unordered truncated window cannot place any visible change.
        let mut unordered = holding(
            collection(serde_json::json!({"maxRows":2})),
            serde_json::json!({}),
            &held,
        );
        unordered.window.truncated = true;
        assert!(apply_to_model(
            &mut unordered,
            &mut client,
            &[row_change("9", Value::Null, row("9", 9, "x"))]
        )
        .is_none());
    }

    #[test]
    fn sql_order_matches_sqlite_storage_classes() {
        use std::cmp::Ordering::*;
        assert_eq!(sql_order(&Value::Null, &1.into()), Less);
        assert_eq!(sql_order(&2.into(), &"1".into()), Less);
        assert_eq!(sql_order(&1.5.into(), &2.into()), Less);
        assert_eq!(sql_order(&true.into(), &1.into()), Equal);
        assert_eq!(sql_order(&"B".into(), &"a".into()), Less);
        let definition = collection(serde_json::json!({"orderBy":"rank", "orderDirection":"desc"}));
        let boundary = (Value::from(5), Value::from("m"));
        let row = |rank: i64, id: &str| serde_json::json!({"id":id, "rank":rank});
        assert!(within_boundary(&definition, &boundary, &row(6, "a")));
        assert!(within_boundary(&definition, &boundary, &row(5, "z")));
        assert!(within_boundary(&definition, &boundary, &row(5, "m")));
        assert!(!within_boundary(&definition, &boundary, &row(5, "a")));
        assert!(!within_boundary(&definition, &boundary, &row(4, "z")));
    }

    #[test]
    fn normalized_changes_keep_entities_another_collection_holds() {
        let row = |workspace: &str| serde_json::json!({"id":"1", "title":"t", "workspaceId":workspace, "rank":1, "extra":"x"});
        let mut narrow =
            collection(serde_json::json!({"equalFilters":{"workspaceId":"workspace"}}));
        narrow.columns = vec!["id".into(), "workspaceId".into()];
        let wide = collection(serde_json::json!({"equalFilters":{"workspaceId":"workspace"}}));
        let mut a = holding(
            narrow,
            serde_json::json!({"workspace":"a"}),
            &[serde_json::json!({"id":"1", "workspaceId":"a"})],
        );
        a.id = "a".into();
        let mut b = holding(wide, serde_json::json!({"workspace":"b"}), &[]);
        b.id = "b".into();
        let mut subscriptions = BTreeMap::from([("a".to_owned(), a), ("b".to_owned(), b)]);
        let normalize = |subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
                         change: LogChange| {
            let changes = [change];
            let collapsed = collapse_changes(&changes);
            let mut touches = BTreeMap::<(String, String), EntityTouch>::new();
            for subscription in subscriptions.values_mut() {
                let delta = apply_rows(subscription, &ApplyContext::default(), &collapsed)
                    .unwrap()
                    .unwrap();
                for (key, touch) in delta.touches {
                    touches
                        .entry(key)
                        .or_default()
                        .merge(&subscription.id, touch);
                }
            }
            normalize_entity_changes(touches, &collapsed, subscriptions)
        };
        // Leaves collection a for collection b: an update with b's fields.
        let changes = normalize(&mut subscriptions, row_change("1", row("a"), row("b")));
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].operation, "update");
        assert_eq!(
            changes[0].new_value,
            Some(serde_json::json!({"id":"1", "title":"t", "workspaceId":"b", "rank":1}))
        );
        // Leaves every collection of the connection: a delete.
        let changes = normalize(&mut subscriptions, row_change("1", row("b"), row("c")));
        assert_eq!(changes[0].operation, "delete");
        assert_eq!(changes[0].new_value, None);
        // A collection that did not decide the row is asked in memory.
        let narrow = (*subscriptions["a"].definition).clone();
        let wide = (*subscriptions["b"].definition).clone();
        let mut a = holding(
            narrow,
            serde_json::json!({"workspace":"a"}),
            &[serde_json::json!({"id":"1", "workspaceId":"a"})],
        );
        a.id = "a".into();
        let mut b = holding(wide, serde_json::json!({"workspace":"b"}), &[]);
        b.id = "b".into();
        let changes = [row_change("1", row("a"), row("b"))];
        let collapsed = collapse_changes(&changes);
        let mut touches = BTreeMap::<(String, String), EntityTouch>::new();
        let delta = apply_rows(&mut a, &ApplyContext::default(), &collapsed)
            .unwrap()
            .unwrap();
        for (key, touch) in delta.touches {
            touches.entry(key).or_default().merge("a", touch);
        }
        let both = BTreeMap::from([("a".to_owned(), a), ("b".to_owned(), b)]);
        let changes = normalize_entity_changes(touches, &collapsed, &both);
        assert_eq!(changes[0].operation, "update");
        assert_eq!(changes[0].new_value.as_ref().unwrap()["title"], "t");
    }

    #[test]
    fn negotiated_sum_digest_is_maintained_incrementally() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../packages/protocol/fixtures/replica-set-digest.json"
        ))
        .unwrap();
        let mut hashes: BTreeMap<String, String> = BTreeMap::new();
        let mut aggregate = ReplicaAggregate::new(true);
        let fresh = |hashes: &BTreeMap<String, String>| {
            let mut fresh = ReplicaAggregate::new(true);
            for hash in hashes.values() {
                fresh.add(hash);
            }
            fresh.digest()
        };
        for (i, mutation) in fixture["mutations"].as_array().unwrap().iter().enumerate() {
            let hash = mutation["hash"].as_str().unwrap();
            if mutation["op"] == "remove" {
                aggregate.remove(hash);
                hashes.retain(|_, value| value != hash);
            } else {
                aggregate.add(hash);
                hashes.insert(i.to_string(), hash.to_owned());
            }
            assert_eq!(aggregate.digest(), fresh(&hashes));
        }
        assert_eq!(aggregate.digest(), fixture["digest"].as_str().unwrap());
        // A renegotiation without sum128 keeps the same membership as xor1.
        let ReplicaAggregate::Sum(state) = &aggregate else {
            unreachable!()
        };
        assert_eq!(
            format_aggregate(&legacy_bytes(state)),
            aggregate_digest(&hashes)
        );
        let request: ReplicaOpenRequest = serde_json::from_value(serde_json::json!({
            "id":"sum","path":"tasks.all","args":{},"digest":aggregate.digest()
        }))
        .unwrap();
        assert!(matches!(
            requested_sum_digest(&request, false),
            Err(ReplicaError::DigestNotNegotiated)
        ));
        assert!(requested_sum_digest(&request, true).unwrap());
    }

    async fn collect(
        rows: impl Stream<Item = Result<Value, ReplicaError>>,
        definition: &ReplicaCollectionDefinition,
    ) -> Result<(Vec<Value>, ReplicaWindow), ReplicaError> {
        let mut kept = Vec::new();
        let window = collect_window(rows, definition, |row| {
            assert_eq!(row.hash, row_hash(&row.row));
            kept.push(row.row);
            Ok(())
        })
        .await?;
        Ok((kept, window))
    }

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
                "table":"tasks", "key":"id", "columns":["id"], "maxRows":max_rows, "maxBytes":max_bytes,
                "orderBy":"id", "orderDirection":"asc"
            })).unwrap();
            let source = futures_util::stream::iter(rows.clone().into_iter().map(Ok)).chain(
                futures_util::stream::once(async { panic!("read beyond budget lookahead") }),
            );
            let (kept, window) = collect(source, &definition).await.unwrap();
            assert_eq!(kept, rows[..2]);
            assert!(window.truncated);
            assert_eq!(window.rows, 2);
            assert_eq!(window.boundary, Some(Box::new(("b".into(), "b".into()))));
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
        let (kept, window) = collect(futures_util::stream::iter([Ok(row.clone())]), &definition)
            .await
            .unwrap();
        assert_eq!(kept, vec![row.clone()]);
        assert!(!window.truncated);
        assert_eq!(window.bytes, stable_json(&row).len() as i64);
        assert!(matches!(
            collect(
                futures_util::stream::iter([Ok(row.clone()), Ok(row)]),
                &definition
            )
            .await,
            Err(ReplicaError::DuplicateKey(_))
        ));
        assert!(matches!(
            collect(
                futures_util::stream::iter([Err(ReplicaError::InvalidArguments)]),
                &definition
            )
            .await,
            Err(ReplicaError::InvalidArguments)
        ));
    }

    #[test]
    fn subscription_state_is_small() {
        // Everything a subscription keeps besides its plan and principal.
        let state = std::mem::size_of::<ReplicaCursor>()
            + std::mem::size_of::<ReplicaAggregate>()
            + std::mem::size_of::<ReplicaWindow>();
        assert!(state <= 128, "{state}");
    }
}
