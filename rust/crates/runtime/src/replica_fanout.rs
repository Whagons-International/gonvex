//! Delivery v4: per-tenant fan-out. One committed transaction is applied in
//! full by the first connection of each *shape*; every other connection of
//! that shape appends the shared, already-encoded items and updates only its
//! own digest, window counters and cursor. See
//! `docs/performance/delivery-v4.md`.
//!
//! A connection's shape is, per subscription in id order, whether the event
//! reaches it and, for the collections of tables the event changed, whether
//! its compiled visibility admits each changed row's old and new image (two
//! bits per row: its class default, computed once per class and row),
//! its collection definition (address), arguments and stream identity.
//! `decide_row` is a function of exactly those visibility answers, the
//! definition, the arguments, the images and the window, so members of
//! different classes that see the changed rows the same way share one
//! outcome (most members of a tenant see none of a given write). A
//! connection has a shape only when its result for this event is a function
//! of exactly that:
//!
//! - every subscription that the event reaches has a version 2 plan without
//!   inherited visibility and a compiled binding, and no table its plan reads
//!   besides the source rows (members, any set or join table, permission
//!   tables) changed: its step is `Skip` (source untouched) or `Apply` with an
//!   unchanged principal, no overlay and no affected-row read;
//! - its member is *hit* by no changed row (no member atom true on the old or
//!   new image), so every visibility check equals its class default;
//! - its windows are complete (not truncated) and its shared indexes are at
//!   the event's revision (not waiting, not ahead);
//! - its streams have distinct identities (default tokens on the socket).
//!
//! Members that need individual decisions (hit rows, principal changes,
//! truncated windows, inherited plans, waiting indexes) take the full path,
//! as before. The full path of the first connection of a shape records the
//! outcome only when it ran clean (no resync); the frames other connections
//! append are then byte-identical to what their own full path would produce
//! (`tests/class_sharing_oracle.rs`).
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use bytes::Bytes;
use gonvex_websocket::{OutboundBatch, SharedText};

use super::*;
use crate::delivery::{DeliveryError, ReplicaBatcher};
use crate::feed_share::EventShare;

static HITS: AtomicU64 = AtomicU64::new(0);
static RECORDED: AtomicU64 = AtomicU64::new(0);
static DATA_HITS: AtomicU64 = AtomicU64::new(0);


/// (connections served without the full path, outcomes recorded, of the
/// served connections those whose outcome decided rows), process totals
/// (tests and benchmarks).
pub fn stats() -> (u64, u64, u64) {
    (
        HITS.load(Ordering::Relaxed),
        RECORDED.load(Ordering::Relaxed),
        DATA_HITS.load(Ordering::Relaxed),
    )
}

/// `GONVEX_REPLICA_FANOUT=0` turns the shared fan-out off; it is also off
/// whenever class sharing is (`feed_share::enabled`).
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    crate::feed_share::enabled()
        && *ENABLED.get_or_init(|| {
            std::env::var("GONVEX_REPLICA_FANOUT")
                .map(|value| value.trim() != "0")
                .unwrap_or(true)
        })
}

/// Per subscription, computed once: what its shape entry and its slow-table
/// check need.
pub(crate) struct FanoutKeys {
    /// Interned argument JSON: equal arguments share one allocation, so a
    /// shape compares its address.
    args: Arc<str>,
    identity: u64,
    /// Tables whose change can move this plan's principal, sets or grants
    /// (one set per plan, shared).
    slow: Arc<HashSet<String>>,
    /// Version 2 plan without inherited visibility (`shareable_plan`).
    shareable: bool,
}

/// One shared value per key while any holder keeps it (`Weak` registry).
fn interned<K: std::hash::Hash + Eq, V: ?Sized>(
    registry: &std::sync::Mutex<std::collections::HashMap<K, std::sync::Weak<V>>>,
    key: K,
    make: impl FnOnce() -> Arc<V>,
) -> Arc<V> {
    let mut registry = registry.lock().unwrap();
    if let Some(value) = registry.get(&key).and_then(std::sync::Weak::upgrade) {
        return value;
    }
    if registry.len() > 1024 {
        registry.retain(|_, value| value.strong_count() > 0);
    }
    let value = make();
    registry.insert(key, Arc::downgrade(&value));
    value
}

impl FanoutKeys {
    pub(crate) fn identity(&self) -> u64 {
        self.identity
    }
}

fn keys(subscription: &mut ReplicaSubscription) -> Arc<FanoutKeys> {
    if let Some(keys) = &subscription.fanout {
        return Arc::clone(keys);
    }
    type Registry<K, V> = std::sync::Mutex<std::collections::HashMap<K, std::sync::Weak<V>>>;
    static SLOW: OnceLock<Registry<usize, HashSet<String>>> = OnceLock::new();
    static ARGS: OnceLock<Registry<String, str>> = OnceLock::new();
    let plan = &subscription.visibility;
    // Keyed by the shared plan's address; the entry holds no reference to
    // the plan, so an address reused by another plan after the old one is
    // gone is only a problem while the old set lives: it is held by keys of
    // subscriptions of the old plan only, and plans are never mutated.
    let slow = interned(
        SLOW.get_or_init(Default::default),
        Arc::as_ptr(plan) as usize,
        || {
            let mut slow = HashSet::new();
            slow.insert("members".to_owned());
            for set in plan.sets.values() {
                slow.insert(set.table.clone());
                for join in &set.joins {
                    slow.insert(join.table.clone());
                }
            }
            slow.extend(plan.permission_tables.iter().cloned());
            Arc::new(slow)
        },
    );
    let args = serde_json::to_string(&subscription.args).unwrap_or_default();
    let args = interned(ARGS.get_or_init(Default::default), args.clone(), || Arc::from(args));
    let keys = Arc::new(FanoutKeys {
        args,
        identity: crate::delivery::stream_identity(subscription),
        slow,
        shareable: plan.version == Some(2) && shareable_plan(plan),
    });
    subscription.fanout = Some(Arc::clone(&keys));
    keys
}

/// The event's changed tables and its collapsed rows (first before-image,
/// last after-image, as `collapse_changes`), by change index into the share.
pub(crate) struct Layout {
    tables: HashSet<String>,
    rows: Vec<(String, usize, usize)>,
}

fn layout(changes: &[LogChange]) -> Layout {
    let mut index = std::collections::HashMap::<(&str, &str), usize>::new();
    let mut rows: Vec<(String, usize, usize)> = Vec::new();
    for (position, change) in changes.iter().enumerate() {
        match index.get(&(change.table.as_str(), change.row_id.as_str())) {
            Some(row) => rows[*row].2 = position,
            None => {
                index.insert((change.table.as_str(), change.row_id.as_str()), rows.len());
                rows.push((change.table.clone(), position, position));
            }
        }
    }
    Layout {
        tables: changes.iter().map(|change| change.table.clone()).collect(),
        rows,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum State {
    /// Cursor at or after the event: untouched.
    Ahead,
    /// The event does not change its source table: cursor advances.
    Skip,
    /// Its source rows are decided by its class.
    Apply,
}

/// An `Arc` compared and hashed by address. Holding it keeps the address
/// from being reused while a shape that names it exists.
pub(crate) struct ByAddress<T: ?Sized>(Option<Arc<T>>);

impl<T: ?Sized> Clone for ByAddress<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: ?Sized> ByAddress<T> {
    fn address(&self) -> usize {
        self.0
            .as_ref()
            .map_or(0, |value| Arc::as_ptr(value) as *const u8 as usize)
    }
}

impl<T: ?Sized> PartialEq for ByAddress<T> {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}

impl<T: ?Sized> Eq for ByAddress<T> {}

impl<T: ?Sized> Hash for ByAddress<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address().hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct ShapeEntry {
    state: State,
    /// Per changed row of its table (in event order, two bits each): whether
    /// its compiled visibility admits the old and the new image.
    visible: u64,
    definition: ByAddress<crate::modules::ReplicaCollectionDefinition>,
    /// The interned argument JSON.
    args: ByAddress<str>,
    identity: u64,
}

pub(crate) type Shape = Vec<ShapeEntry>;

/// What one subscription of a shape does with the event.
pub(crate) enum SubOutcome {
    /// Nothing (cursor ahead).
    None,
    /// Cursor advances, nothing is sent.
    Advance,
    /// Digest/window update and a delta + ready pair.
    Delta(Box<DeltaOutcome>),
}

pub(crate) struct DeltaOutcome {
    /// (remove, hash) in application order.
    ops: Vec<(bool, String)>,
    rows: isize,
    bytes: i64,
    /// Stream-addressed delta without `digest` / `truncated`.
    delta: ServerMessage,
    /// Stream-addressed ready with an empty digest.
    ready: ServerMessage,
    truncated: bool,
}

/// The shared result of one event for every connection of one shape.
pub(crate) struct Outcome {
    transaction: Option<ServerMessage>,
    subs: Vec<SubOutcome>,
    /// Encoded items, per row encoding (`[json, native]`), built once.
    texts: [OnceLock<Option<Texts>>; 2],
}

struct Texts {
    transaction: Option<Arc<SharedText>>,
    /// Per sub: the delta item and the ready text around its digest.
    subs: Vec<Option<(Arc<SharedText>, Bytes, Bytes)>>,
}

impl Outcome {
    fn texts(&self, native: bool) -> Option<&Texts> {
        self.texts[usize::from(native)]
            .get_or_init(|| {
                let transaction = match &self.transaction {
                    Some(message) => Some(Arc::new(
                        crate::delivery::encode_shared(message, native, false).ok()?,
                    )),
                    None => None,
                };
                let mut subs = Vec::with_capacity(self.subs.len());
                for sub in &self.subs {
                    subs.push(match sub {
                        SubOutcome::Delta(delta) => {
                            let item = crate::delivery::encode_shared(&delta.delta, native, true).ok()?;
                            let ready = serde_json::to_string(&delta.ready).ok()?;
                            const DIGEST: &str = "\"digest\":\"";
                            let at = ready.find(&format!("{DIGEST}\""))? + DIGEST.len();
                            Some((
                                Arc::new(item),
                                Bytes::copy_from_slice(ready[..at].as_bytes()),
                                Bytes::copy_from_slice(ready[at..].as_bytes()),
                            ))
                        }
                        _ => None,
                    });
                }
                Some(Texts { transaction, subs })
            })
            .as_ref()
    }
}

/// Whether every index bound into `resolved` is at `revision` of `epoch`
/// right now (no waiting): the synchronous form of `indexes_ready`.
fn indexes_ready_now(resolved: &ResolvedVisibility, revision: u64, epoch: &str) -> bool {
    resolved.inherited.is_empty()
        && resolved
            .indexes
            .values()
            .all(|index| crate::visibility_index::ready_now(index, revision, epoch) == Some(true))
}

/// The connection's shape for this event, or None when some subscription
/// needs its own decisions (see the module documentation). Does not change
/// any subscription state except its cached keys.
pub(crate) fn shape(
    subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
    share: &EventShare,
    epoch: &str,
    revision: u64,
) -> Option<Shape> {
    let layout = share.layout.get_or_init(|| layout(share.changes()));
    let changes = share.changes();
    // Cheap static checks first: a connection with a collection the fan-out
    // never serves (inherited plans, legacy plans) is rejected before any
    // per-row work.
    for subscription in subscriptions.values_mut() {
        if revision > subscription.cursor.revision && !keys(subscription).shareable {
            return None;
        }
    }
    let mut shape: Shape = Vec::with_capacity(subscriptions.len());
    let mut memo = None;
    for subscription in subscriptions.values_mut() {
        let keys = keys(subscription);
        if shape.iter().any(|entry| entry.identity == keys.identity) {
            return None;
        }
        let touched = layout.tables.contains(&subscription.definition.table);
        let state = if revision <= subscription.cursor.revision {
            State::Ahead
        } else {
            if !keys.shareable
                || layout.tables.iter().any(|table| keys.slow.contains(table))
                || ((!subscription.resolved.indexes.is_empty()
                    || !subscription.resolved.inherited.is_empty())
                    && !indexes_ready_now(&subscription.resolved, revision, epoch))
            {
                return None;
            }
            if touched {
                // Shared deltas carry the representative's cursor.
                if subscription.cursor.epoch != epoch
                    || (!subscription.resolved.indexes.is_empty()
                        && visibility::indexes_ahead(&subscription.resolved, revision))
                {
                    return None;
                }
                State::Apply
            } else {
                State::Skip
            }
        };
        if !touched {
            shape.push(ShapeEntry {
                state,
                visible: 0,
                definition: ByAddress(None),
                args: ByAddress(None),
                identity: keys.identity,
            });
            continue;
        }
        // A collection of a changed table: decided (Apply) or asked whether
        // it holds the row (normalization); both read its class only when
        // its member is not hit and its window is complete.
        if subscription.window.truncated {
            return None;
        }
        let Some(crate::visibility_class::MemberHandle::Compiled(member)) = subscription.mask.peek()
        else {
            return None;
        };
        let memo = memo.get_or_insert_with(|| share.memo.lock().unwrap());
        let mut visible = 0u64;
        let mut row = 0;
        for (table, first, last) in &layout.rows {
            if *table != subscription.definition.table {
                continue;
            }
            if row == 32 {
                return None;
            }
            let old = &changes[*first].old_value;
            let new = &changes[*last].new_value;
            // Not hit: the class default, evaluated once per class and row.
            let old_visible = !old.is_null() && member.class_default_memo(memo, old)?;
            let new_visible = !new.is_null() && member.class_default_memo(memo, new)?;
            visible |= (u64::from(old_visible) | u64::from(new_visible) << 1) << (2 * row);
            row += 1;
        }
        shape.push(ShapeEntry {
            state,
            visible,
            definition: ByAddress(Some(Arc::clone(&subscription.definition))),
            args: ByAddress(Some(Arc::clone(&keys.args))),
            identity: keys.identity,
        });
    }
    Some(shape)
}

/// Records what the full path did for one subscription of a shape.
#[derive(Default)]
pub(crate) struct Recorder {
    pub clean: bool,
    pub subs: BTreeMap<String, SubOutcome>,
    pub transaction: Option<ServerMessage>,
}

impl Recorder {
    pub(crate) fn new() -> Self {
        Self {
            clean: true,
            ..Default::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delta(
        &mut self,
        subscription: &ReplicaSubscription,
        ops: Vec<(bool, String)>,
        rows: isize,
        bytes: i64,
        delta: &ServerMessage,
        ready: &ServerMessage,
    ) {
        let token = format!("~{:016x}", crate::delivery::stream_identity(subscription));
        let ServerMessage::ReplicaDelta {
            cursor, payload, ..
        } = delta
        else {
            self.clean = false;
            return;
        };
        let mut payload = payload.clone();
        payload.remove("digest");
        payload.remove("truncated");
        let ServerMessage::ReplicaReady {
            cursor: ready_cursor,
            metadata,
            ..
        } = ready
        else {
            self.clean = false;
            return;
        };
        self.subs.insert(
            subscription.id.clone(),
            SubOutcome::Delta(Box::new(DeltaOutcome {
                ops,
                rows,
                bytes,
                delta: ServerMessage::ReplicaDelta {
                    id: token.clone(),
                    cursor: cursor.clone(),
                    payload,
                },
                ready: ServerMessage::ReplicaReady {
                    id: token,
                    cursor: ready_cursor.clone(),
                    digest: String::new(),
                    metadata: metadata.clone(),
                },
                truncated: subscription.window.truncated,
            })),
        );
    }

    /// The shared outcome, aligned with `subscriptions` (the representative's,
    /// after the event).
    pub(crate) fn finish(
        mut self,
        subscriptions: &BTreeMap<String, ReplicaSubscription>,
        shape: &Shape,
    ) -> Option<Outcome> {
        if !self.clean || subscriptions.len() != shape.len() {
            return None;
        }
        let subs = subscriptions
            .keys()
            .zip(shape)
            .map(|(id, entry)| match entry.state {
                State::Ahead => SubOutcome::None,
                State::Skip => SubOutcome::Advance,
                State::Apply => self.subs.remove(id).unwrap_or(SubOutcome::Advance),
            })
            .collect();
        let transaction = self.transaction.map(|message| match message {
            ServerMessage::ReplicaTransaction {
                cursor,
                origin_command_id,
                provenance,
                mut changes,
            } => {
                for change in &mut changes {
                    change.old_value = None;
                }
                ServerMessage::ReplicaTransaction {
                    cursor,
                    origin_command_id,
                    provenance,
                    changes,
                }
            }
            other => other,
        });
        Some(Outcome {
            transaction,
            subs,
            texts: Default::default(),
        })
    }
}

impl Runtime {
    /// Delivery v4: serves a batch connection from the shared outcome of its
    /// shape, when one was recorded for this event. None: not served (the
    /// caller runs the full path, which may record the outcome). On
    /// `Some(Ok)` the connection's frames for the event are in `batch`
    /// (except its watermark, which the caller adds as before).
    /// [`Self::fan_out_hit`], also saying on a miss whether the full path
    /// should record an outcome (the connection has a shape).
    pub(crate) fn fan_out(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: &FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        native: bool,
    ) -> FanOut {
        let mut eligible = false;
        match self.fan_out_inner(session, subscriptions, event, batcher, batch, native, &mut eligible) {
            Some(result) => FanOut::Served(result),
            None => FanOut::Miss { record: eligible },
        }
    }

    pub fn fan_out_hit(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: &FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        native: bool,
    ) -> Option<Result<(), DeliveryError>> {
        let mut eligible = false;
        self.fan_out_inner(session, subscriptions, event, batcher, batch, native, &mut eligible)
    }

    #[allow(clippy::too_many_arguments)]
    fn fan_out_inner(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: &FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        native: bool,
        eligible: &mut bool,
    ) -> Option<Result<(), DeliveryError>> {
        let FeedEvent::Transaction {
            database_epoch,
            revision,
            changes,
        } = event
        else {
            return None;
        };
        if !enabled() || !batcher.enabled() || subscriptions.is_empty() {
            return None;
        }
        let tenant = self.inner.delivery.tenant(&session.route.tenant_id);
        let share = tenant.events.share(database_epoch, *revision, changes)?;
        let shape = shape(subscriptions, &share, database_epoch, *revision)?;
        *eligible = true;
        let outcome = if shape.iter().any(|entry| entry.state == State::Apply) {
            Some(share.outcomes.lock().unwrap().get(&shape).cloned()?)
        } else {
            // Nothing decides a row: cursors advance, no frame (the full path
            // would produce none either).
            None
        };
        if let Some(outcome) = &outcome {
            // Checks before any change: bounded windows stay in budget and
            // the socket's stream tokens are the default ones.
            for ((id, subscription), sub) in subscriptions.iter().zip(&outcome.subs) {
                if let SubOutcome::Delta(delta) = sub {
                    if !batcher.default_binding(id, crate::delivery::stream_identity(subscription)) {
                        return None;
                    }
                    if subscription.bounded() {
                        let rows = subscription.window.rows as isize + delta.rows;
                        let bytes = subscription.window.bytes + delta.bytes;
                        if (subscription.definition.max_rows > 0
                            && rows > subscription.definition.max_rows as isize)
                            || (subscription.definition.max_bytes > 0
                                && bytes > subscription.definition.max_bytes)
                        {
                            return None;
                        }
                    }
                }
            }
        }
        HITS.fetch_add(1, Ordering::Relaxed);
        let Some(outcome) = outcome else {
            for (subscription, entry) in subscriptions.values_mut().zip(&shape) {
                if entry.state == State::Skip {
                    subscription.cursor.revision = *revision;
                }
            }
            return Some(Ok(()));
        };
        DATA_HITS.fetch_add(1, Ordering::Relaxed);
        let Some(texts) = outcome.texts(native) else {
            return Some(Err(DeliveryError));
        };
        let tenant_id = session.route.tenant_id.as_str();
        if let Some(text) = &texts.transaction {
            batcher.push_shared(batch, tenant_id, subscriptions, Arc::clone(text));
        }
        // Window state first (mutable), then the frames (the batcher reads
        // the subscriptions).
        let mut digests: Vec<(usize, String)> = Vec::new();
        for (index, subscription) in subscriptions.values_mut().enumerate() {
            match &outcome.subs[index] {
                SubOutcome::None => {}
                SubOutcome::Advance => subscription.cursor.revision = *revision,
                SubOutcome::Delta(delta) => {
                    for (remove, hash) in &delta.ops {
                        if *remove {
                            subscription.digest.remove(hash);
                        } else {
                            subscription.digest.add(hash);
                        }
                    }
                    subscription.window.rows =
                        (subscription.window.rows as isize + delta.rows).max(0) as usize;
                    subscription.window.bytes += delta.bytes;
                    subscription.cursor.revision = *revision;
                    digests.push((index, subscription.digest.digest()));
                }
            }
        }
        for (index, digest) in digests {
            let id = subscriptions.keys().nth(index).expect("subscription").as_str();
            {
                {
                    let SubOutcome::Delta(delta) = &outcome.subs[index] else {
                        unreachable!()
                    };
                    let (item, prefix, suffix) = texts.subs[index].as_ref().expect("delta text");
                    let mut tail = String::with_capacity(digest.len() + 32);
                    tail.push_str(",\"digest\":\"");
                    tail.push_str(&digest);
                    tail.push_str("\",\"truncated\":");
                    tail.push_str(if delta.truncated { "true" } else { "false" });
                    tail.push('}');
                    if !batcher.push_delta(batch, tenant_id, subscriptions, id, Arc::clone(item), tail) {
                        return Some(Err(DeliveryError));
                    }
                    let mut ready = Vec::with_capacity(prefix.len() + digest.len() + suffix.len());
                    ready.extend_from_slice(prefix);
                    ready.extend_from_slice(digest.as_bytes());
                    ready.extend_from_slice(suffix);
                    if !batcher.push_ready(batch, tenant_id, subscriptions, id, Bytes::from(ready)) {
                        return Some(Err(DeliveryError));
                    }
                }
            }
        }
        Some(Ok(()))
    }
}

/// Result of [`Runtime::fan_out`].
pub(crate) enum FanOut {
    /// The connection's frames are in its batch.
    Served(Result<(), DeliveryError>),
    /// Not served; `record`: the full path should record the outcome.
    Miss { record: bool },
}

/// Records `outcome` for `shape` (first writer wins).
pub(crate) fn record(share: &EventShare, shape: Shape, outcome: Outcome) {
    let mut outcomes = share.outcomes.lock().unwrap();
    if !outcomes.contains_key(&shape) {
        RECORDED.fetch_add(1, Ordering::Relaxed);
        outcomes.insert(shape, Arc::new(outcome));
    }
}

/// Hash of a shape (diagnostics).
#[allow(dead_code)]
pub(crate) fn shape_hash(shape: &Shape) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    shape.hash(&mut hasher);
    hasher.finish()
}

impl Runtime {
    /// One batch connection's whole feed step for `event` (benchmarks and
    /// tests; the connection loop does the same with its gates): the shared
    /// outcome when its shape has one, else the full path (recording the
    /// outcome) with every frame pushed. Returns the frames that are not
    /// batchable (sent the usual way by the caller).
    #[allow(clippy::too_many_arguments)]
    pub async fn deliver_feed_event(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        hub: &crate::delivery::DeliveryHub,
        native: bool,
    ) -> Result<Vec<ServerMessage>, ReplicaError> {
        let delivery = |_| ReplicaError::InvalidIdentifier("replica batch delivery failed".to_owned());
        let record = match self.fan_out(session, subscriptions, &event, batcher, batch, native) {
            FanOut::Served(result) => return result.map(|_| Vec::new()).map_err(delivery),
            FanOut::Miss { record } => record,
        };
        let messages = self
            .apply_feed_event_with(session, subscriptions, event, record)
            .await?;
        let mut rest = Vec::new();
        for message in messages {
            if let Some(message) = batcher
                .push(batch, hub, &session.route.tenant_id, native, subscriptions, message)
                .map_err(delivery)?
            {
                rest.push(message);
            }
        }
        Ok(rest)
    }
}
