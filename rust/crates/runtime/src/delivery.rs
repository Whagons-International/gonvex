//! Replica delivery v2 (`replicaBatchFrames`): encode once, compress once, one
//! frame per connection per tick. See `docs/performance/delivery.md`.
//!
//! Interface. Input: the replica frames a committed change produces for one
//! connection (today `Runtime::apply_feed_event`; later the per-(class,
//! collection) deltas of the feed path), plus the connection's subscriptions.
//! Output: shared, already-encoded items appended to the connection's pending
//! batch, which the transport sends as one WebSocket message per tick.
//!
//! Encode once. Items never contain per-socket data: a collection delta or
//! ready frame names its *stream* (a token for the collection path and
//! arguments) instead of the client's subscription id, and the socket binds
//! subscription ids to streams with a small inline `replica.stream` item the
//! first time it uses one. Encoded items are memoized per tenant and revision
//! by content: a connection reuses an item only when its own visibility-filtered
//! frame is equal (`==`) to the memoized one, so sharing never depends on a
//! class assignment being right. No connection can receive bytes it would not
//! have produced itself, whatever the class ids are.
//!
//! Compress once. `gonvex_websocket::SharedText` keeps the item's DEFLATE form
//! per negotiated window; a batch is assembled from those segments.
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use gonvex_protocol::ServerMessage;
use gonvex_websocket::{OutboundBatch, Segment, SharedText, WebSocket};
use tokio::time::Instant;

use crate::replica::ReplicaSubscription;

/// Most variants (distinct contents) kept for one (revision, item) key. More
/// than this means the content differs per connection; those encode privately.
const MAX_VARIANTS: usize = 16;
/// Encoded bytes a tenant's memo keeps (texts, compressed forms and frames).
const MAX_MEMO_BYTES: usize = 8 << 20;

/// Batching knobs (`GONVEX_REPLICA_BATCH_TICK_MS`, default 50;
/// `GONVEX_REPLICA_BATCH_IDLE_MS`, default 1000; `GONVEX_REPLICA_BATCH_MAX_BYTES`,
/// default 512 KiB).
#[derive(Clone, Copy, Debug)]
pub struct BatchConfig {
    /// A batch holding replica data is sent at most this long after it opened.
    pub tick: Duration,
    /// A batch without changed rows (only `replica.ready` items and/or a
    /// watermark) waits this long, unless a call waits for that revision.
    pub idle: Duration,
    /// A batch is sent as soon as it holds this many uncompressed bytes.
    pub max_bytes: usize,
}

impl BatchConfig {
    pub fn from_env() -> Self {
        fn var(name: &str) -> Option<u64> {
            std::env::var(name).ok()?.trim().parse().ok()
        }
        Self {
            tick: Duration::from_millis(var("GONVEX_REPLICA_BATCH_TICK_MS").unwrap_or(50)),
            idle: Duration::from_millis(var("GONVEX_REPLICA_BATCH_IDLE_MS").unwrap_or(1000)),
            max_bytes: var("GONVEX_REPLICA_BATCH_MAX_BYTES").unwrap_or(512 << 10) as usize,
        }
    }

    fn global() -> Self {
        static CONFIG: std::sync::OnceLock<BatchConfig> = std::sync::OnceLock::new();
        *CONFIG.get_or_init(Self::from_env)
    }
}

/// An item could not be encoded or the batch could not be written; the
/// connection closes, as when a plain send fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryError;

/// Process-wide memo registry, one memo per tenant.
#[derive(Default)]
pub struct DeliveryHub {
    tenants: Mutex<HashMap<String, Arc<TenantFrames>>>,
}

impl DeliveryHub {
    pub fn tenant(&self, tenant_id: &str) -> Arc<TenantFrames> {
        let mut tenants = self.tenants.lock().unwrap();
        if let Some(frames) = tenants.get(tenant_id) {
            return Arc::clone(frames);
        }
        let frames = Arc::new(TenantFrames::default());
        tenants.insert(tenant_id.to_owned(), Arc::clone(&frames));
        frames
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ItemKind {
    Transaction,
    /// A delta's shared part: everything except the per-connection
    /// `digest` and `truncated`, encoded without its closing brace.
    Delta(Arc<str>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ItemKey {
    revision: u64,
    native: bool,
    kind: ItemKind,
}

struct Variant {
    message: ServerMessage,
    text: Arc<SharedText>,
    bytes: usize,
}

#[derive(Default)]
struct Memo {
    entries: crate::fast_hash::FastMap<ItemKey, Vec<Variant>>,
    order: VecDeque<ItemKey>,
    bytes: usize,
}

/// Counters of one tenant memo (benchmarks and dashboards).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoStats {
    pub hits: u64,
    pub encodes: u64,
    pub private_encodes: u64,
    pub encoded_bytes: u64,
}

/// Encoded replica items of one tenant, shared by its connections.
#[derive(Default)]
pub struct TenantFrames {
    /// Delivery v3: per-event shared decisions (`feed_share`).
    pub events: crate::feed_share::EventShares,
    memo: Mutex<Memo>,
    hits: AtomicU64,
    encodes: AtomicU64,
    private_encodes: AtomicU64,
    encoded_bytes: AtomicU64,
}

impl TenantFrames {
    pub fn stats(&self) -> MemoStats {
        MemoStats {
            hits: self.hits.load(Ordering::Relaxed),
            encodes: self.encodes.load(Ordering::Relaxed),
            private_encodes: self.private_encodes.load(Ordering::Relaxed),
            encoded_bytes: self.encoded_bytes.load(Ordering::Relaxed),
        }
    }

    /// Heap bytes held by memoized items (texts plus compressed forms).
    pub fn memo_bytes(&self) -> usize {
        self.memo.lock().unwrap().bytes
    }

    /// The shared encoding of `message` (already stream-addressed). Equal
    /// messages of the same revision and item share one `SharedText`.
    fn item(&self, key: ItemKey, message: ServerMessage) -> Result<Arc<SharedText>, DeliveryError> {
        {
            let memo = self.memo.lock().unwrap();
            if let Some(variants) = memo.entries.get(&key) {
                if let Some(variant) = variants.iter().find(|variant| variant.message == message) {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(Arc::clone(&variant.text));
                }
            }
        }
        let mut text = encode_item(&message, key.native)?;
        if matches!(key.kind, ItemKind::Delta(_)) {
            // The connection appends `,"digest":..,"truncated":..}`.
            if text.pop() != Some('}') {
                return Err(DeliveryError);
            }
        }
        let text = Arc::new(SharedText::new(text));
        self.encoded_bytes
            .fetch_add(text.len() as u64, Ordering::Relaxed);
        let mut memo = self.memo.lock().unwrap();
        let variants = memo.entries.entry(key.clone()).or_default();
        if variants.len() >= MAX_VARIANTS {
            self.private_encodes.fetch_add(1, Ordering::Relaxed);
            return Ok(text);
        }
        self.encodes.fetch_add(1, Ordering::Relaxed);
        // The compressed forms (about a third of the text) come later; count them now.
        let bytes = text.len() * 2 + 256;
        let first = variants.is_empty();
        variants.push(Variant {
            message,
            text: Arc::clone(&text),
            bytes,
        });
        memo.bytes += bytes;
        if first {
            memo.order.push_back(key);
        }
        while memo.bytes > MAX_MEMO_BYTES {
            let Some(oldest) = memo.order.pop_front() else {
                break;
            };
            if let Some(evicted) = memo.entries.remove(&oldest) {
                memo.bytes -= evicted.iter().map(|variant| variant.bytes).sum::<usize>();
            }
        }
        Ok(text)
    }
}

/// The pre-batch per-connection encoding of one replica frame, as the
/// websocket loop sends it to `replicaNoOldValues` (or native-rows) clients.
#[cfg(feature = "perf-fixture")]
#[doc(hidden)]
pub fn perf_legacy_encode(
    message: &mut ServerMessage,
    native: bool,
    threshold: Option<usize>,
) -> Option<String> {
    if native {
        if let Ok(Some(encoded)) = crate::replica::native_control_message(message) {
            return serde_json::to_string(&encoded).ok();
        }
    } else if let Ok(Some(encoded)) =
        gonvex_protocol::encode_compact_replica_transaction(message, threshold)
    {
        return Some(encoded);
    }
    serde_json::to_string(message).ok()
}

/// The shared item text of `message` as `TenantFrames::item` builds it
/// (a delta without its closing brace).
pub(crate) fn encode_shared(
    message: &ServerMessage,
    native: bool,
    delta: bool,
) -> Result<SharedText, DeliveryError> {
    let mut text = encode_item(message, native)?;
    if delta && text.pop() != Some('}') {
        return Err(DeliveryError);
    }
    Ok(SharedText::new(text))
}

fn encode_item(message: &ServerMessage, native: bool) -> Result<String, DeliveryError> {
    if native {
        if let Some(encoded) = crate::replica::native_control_message(message).map_err(|_| DeliveryError)? {
            return serde_json::to_string(&encoded).map_err(|_| DeliveryError);
        }
    }
    serde_json::to_string(message).map_err(|_| DeliveryError)
}

/// Encodes (and, for `window`, compresses) the shared items of one
/// connection's feed frames ahead of `ReplicaBatcher::push`, so the push on
/// the network thread finds them memoized. Runs on the offload pool with the
/// feed apply. Uses the default stream token; a socket that needed a suffixed
/// token simply encodes at push time.
pub fn prewarm(
    hub: &DeliveryHub,
    tenant_id: &str,
    native: bool,
    window: Option<u8>,
    replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
    messages: &[ServerMessage],
) {
    let frames = hub.tenant(tenant_id);
    for message in messages {
        let item = match message {
            ServerMessage::ReplicaTransaction {
                cursor,
                origin_command_id,
                provenance,
                changes,
            } => {
                let mut changes = changes.clone();
                for change in &mut changes {
                    change.old_value = None;
                }
                (
                    ItemKey {
                        revision: cursor.revision,
                        native,
                        kind: ItemKind::Transaction,
                    },
                    ServerMessage::ReplicaTransaction {
                        cursor: cursor.clone(),
                        origin_command_id: origin_command_id.clone(),
                        provenance: provenance.clone(),
                        changes,
                    },
                )
            }
            ServerMessage::ReplicaDelta {
                id,
                cursor,
                payload,
            } => {
                let Some(subscription) = replicas.get(id) else {
                    continue;
                };
                let token: Arc<str> = Arc::from(format!("~{:016x}", stream_identity(subscription)));
                let mut payload = payload.clone();
                payload.remove("digest");
                payload.remove("truncated");
                (
                    ItemKey {
                        revision: cursor.revision,
                        native,
                        kind: ItemKind::Delta(Arc::clone(&token)),
                    },
                    ServerMessage::ReplicaDelta {
                        id: token.to_string(),
                        cursor: cursor.clone(),
                        payload,
                    },
                )
            }
            _ => continue,
        };
        if let Ok(text) = frames.item(item.0, item.1) {
            if let Some(window) = window {
                let _ = text.deflated(window);
            }
        }
    }
}

/// Delivery v3: `event` merged with the transactions already waiting in the
/// receiver (`crate::change_feed::merge_transactions`), when every subscription's
/// cursor is before them. A waiting event that cannot be merged (a reset,
/// another epoch, lag) is left in `pending` for the next turn.
pub fn merge_waiting_feed(
    event: crate::change_feed::FeedEvent,
    receiver: &mut Option<crate::change_feed::FeedReceiver>,
    pending: &mut Option<Result<crate::change_feed::FeedEvent, tokio::sync::broadcast::error::RecvError>>,
    replicas: &std::collections::BTreeMap<String, crate::replica::ReplicaSubscription>,
    allowed: bool,
) -> crate::change_feed::FeedEvent {
    const MAX_MERGED: usize = 256;
    let (Some(receiver), Some(first)) = (receiver.as_mut(), crate::change_feed::first_revision(&event)) else {
        return event;
    };
    if !allowed
        || pending.is_some()
        || receiver.is_empty()
        || replicas.values().any(|subscription| subscription.cursor.revision >= first)
    {
        return event;
    }
    let mut events = vec![event];
    while events.len() < MAX_MERGED {
        match receiver.try_recv() {
            Ok(next @ crate::change_feed::FeedEvent::Transaction { .. }) => {
                events.push(next);
                if crate::change_feed::merge_transactions(&events[events.len() - 2..]).is_none() {
                    *pending = events.pop().map(Ok);
                    break;
                }
            }
            Ok(other) => {
                *pending = Some(Ok(other));
                break;
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                *pending = Some(Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)));
                break;
            }
            Err(_) => break,
        }
    }
    if events.len() == 1 {
        return events.pop().unwrap();
    }
    // Pairwise checked above: one epoch, ascending revisions.
    crate::change_feed::merge_transactions(&events).expect("mergeable transactions")
}

/// Stream token of a collection: path and arguments, never the socket.
pub(crate) fn stream_identity(subscription: &ReplicaSubscription) -> u64 {
    // Path and arguments never change for a subscription: delivery v4 caches
    // the identity with its fan-out keys.
    if let Some(keys) = &subscription.fanout {
        return keys.identity();
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    subscription.path.hash(&mut hasher);
    // serde_json's canonical rendering of the same `Value` is stable.
    serde_json::to_string(&subscription.args)
        .unwrap_or_default()
        .hash(&mut hasher);
    hasher.finish()
}

struct Binding {
    token: Arc<str>,
    identity: u64,
}

/// Per-connection state of delivery v2.
pub struct ReplicaBatcher {
    enabled: bool,
    config: BatchConfig,
    tenant_id: String,
    frames: Option<Arc<TenantFrames>>,
    // Per connection, bounded by its own subscriptions: foldhash.
    bindings: crate::fast_hash::FastMap<String, Binding>,
    token_owner: crate::fast_hash::FastMap<Arc<str>, String>,
    // State of the batch currently open on the socket.
    open: bool,
    opened_at: Option<Instant>,
    items: usize,
    /// Transaction and delta items (changed rows) in the open batch.
    data: usize,
    watermark: Option<u64>,
    last_watermark: u64,
    /// Revisions call results on this connection wait for (their
    /// `replica.watermark`), until a sent batch's watermark covers them.
    waiting: std::collections::BTreeSet<u64>,
}

impl Default for ReplicaBatcher {
    fn default() -> Self {
        Self::with_config(BatchConfig::global())
    }
}

impl ReplicaBatcher {
    pub fn with_config(config: BatchConfig) -> Self {
        Self {
            enabled: false,
            config,
            tenant_id: String::new(),
            frames: None,
            bindings: Default::default(),
            token_owner: Default::default(),
            open: false,
            opened_at: None,
            items: 0,
            data: 0,
            watermark: None,
            last_watermark: 0,
            waiting: std::collections::BTreeSet::new(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Whether the open batch holds changed rows (transaction or delta
    /// items). A batch of only `replica.ready` items and a watermark carries
    /// no row change for this connection and waits for the idle delay.
    pub fn has_items(&self) -> bool {
        self.open && self.data > 0
    }

    /// Data items (transaction / delta) in the open batch (delivery
    /// coverage: did a feed event add any).
    pub fn data_items(&self) -> usize {
        self.data
    }

    /// Negotiated at authentication (client `replicaBatchFrames: 1`).
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Forget stream bindings (tenant scope discarded). The client drops its
    /// bindings with the socket; rebinding an id simply overwrites.
    pub fn clear_bindings(&mut self) {
        self.bindings.clear();
        self.token_owner.clear();
        self.waiting.clear();
    }

    /// A call result waits for `revision`: as soon as the open batch's
    /// watermark covers it, the batch is due (no tick, no idle delay). Every
    /// waiting revision is tracked on its own, so a later call never delays
    /// an earlier one whose revision is already covered.
    pub fn urgent(&mut self, revision: u64) {
        if revision > 0 {
            self.waiting.insert(revision);
        }
    }

    /// Whether the open batch's watermark covers a revision a call result
    /// waits for: send it now.
    pub fn call_waiting(&self) -> bool {
        self.open
            && self.watermark.is_some_and(|watermark| {
                self.waiting
                    .first()
                    .is_some_and(|first| *first <= watermark)
            })
    }

    /// When the open batch must be sent: now when a call waits for its
    /// watermark, after the tick when it holds data, else after the idle
    /// delay.
    pub fn deadline(&mut self, socket: &OutboundBatch) -> Option<Instant> {
        self.sync(socket);
        let opened_at = self.opened_at?;
        Some(if self.call_waiting() {
            opened_at
        } else if self.data > 0 {
            opened_at + self.config.tick
        } else {
            opened_at + self.config.idle
        })
    }

    pub async fn flush(&mut self, socket: &mut WebSocket) -> Result<(), DeliveryError> {
        let result = socket.flush_batch().await.map_err(|_| DeliveryError);
        self.sync(socket.batch());
        result
    }

    /// Takes the open batch without a socket (benchmarks): the frame a
    /// connection with `deflate` = (window, threshold) would send.
    pub fn take_frame(
        &mut self,
        batch: &mut OutboundBatch,
        deflate: Option<(u8, usize)>,
    ) -> Option<gonvex_websocket::BatchFrame> {
        let frame = batch.take_frame(deflate);
        self.sync(batch);
        frame
    }

    /// Notices a batch the transport already sent (any other send flushes it).
    fn sync(&mut self, socket: &OutboundBatch) {
        if self.open && !socket.is_open() {
            self.open = false;
            self.opened_at = None;
            self.items = 0;
            self.data = 0;
            if let Some(watermark) = self.watermark.take() {
                self.last_watermark = self.last_watermark.max(watermark);
                self.waiting = self.waiting.split_off(&(watermark + 1));
            }
        }
    }

    fn ensure_open(
        &mut self,
        socket: &mut OutboundBatch,
        tenant_id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
    ) {
        self.sync(socket);
        if self.open {
            return;
        }
        let prefix = format!(
            "{{\"type\":\"replica.batch\",\"tenant\":{},\"class\":\"{:016x}\",\"from\":{},\"items\":[",
            serde_json::to_string(tenant_id).unwrap_or_else(|_| "\"\"".into()),
            class_id(replicas),
            self.last_watermark,
        );
        socket.open(Bytes::from(prefix), b"}");
        socket.set_tail(Some(Bytes::from_static(b"]")));
        self.open = true;
        self.opened_at = Some(Instant::now());
        self.items = 0;
        self.data = 0;
        self.watermark = None;
    }

    fn frames(&mut self, tenant_id: &str, hub: &DeliveryHub) -> Arc<TenantFrames> {
        if self.tenant_id != tenant_id || self.frames.is_none() {
            self.tenant_id = tenant_id.to_owned();
            self.frames = Some(hub.tenant(tenant_id));
        }
        Arc::clone(self.frames.as_ref().unwrap())
    }

    fn push_segment(&mut self, socket: &mut OutboundBatch, segment: Segment) {
        if self.items > 0 {
            socket.push(Segment::Inline(Bytes::from_static(b",")));
        }
        socket.push(segment);
        self.items += 1;
    }

    /// The stream token of subscription `id`, binding it on this socket first
    /// when needed. `None` when the id has no subscription (closed meanwhile).
    fn bind(
        &mut self,
        socket: &mut OutboundBatch,
        id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
    ) -> Option<Arc<str>> {
        let subscription = replicas.get(id)?;
        let identity = stream_identity(subscription);
        if let Some(binding) = self.bindings.get(id) {
            if binding.identity == identity {
                return Some(Arc::clone(&binding.token));
            }
        }
        if let Some(old) = self.bindings.remove(id) {
            self.token_owner.remove(&old.token);
        }
        // One token per subscription on a socket: a second subscription of the
        // same collection and arguments gets its own (unshared) token.
        let mut token: Arc<str> = Arc::from(format!("~{identity:016x}"));
        let mut suffix = 1;
        while let Some(owner) = self.token_owner.get(&token) {
            if owner != id && replicas.contains_key(owner.as_str()) {
                suffix += 1;
                token = Arc::from(format!("~{identity:016x}.{suffix}"));
                continue;
            }
            break;
        }
        if let Some(previous) = self.token_owner.insert(Arc::clone(&token), id.to_owned()) {
            if previous != id {
                self.bindings.remove(&previous);
            }
        }
        self.bindings.insert(
            id.to_owned(),
            Binding {
                token: Arc::clone(&token),
                identity,
            },
        );
        let binding = format!(
            "{{\"type\":\"replica.stream\",\"id\":{},\"stream\":\"{token}\"}}",
            serde_json::to_string(id).ok()?,
        );
        self.push_segment(socket, Segment::Inline(Bytes::from(binding)));
        Some(token)
    }

    /// Appends one replica frame of a committed change. Returns the message
    /// back when it is not a batchable replica frame; the caller sends it the
    /// usual way (which first sends the pending batch, keeping order).
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        socket: &mut OutboundBatch,
        hub: &DeliveryHub,
        tenant_id: &str,
        native: bool,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
        message: ServerMessage,
    ) -> Result<Option<ServerMessage>, DeliveryError> {
        let (revision, kind, message) = match message {
            ServerMessage::ReplicaTransaction {
                cursor,
                origin_command_id,
                provenance,
                mut changes,
            } => {
                // Batch clients apply transactions from their local base.
                for change in &mut changes {
                    change.old_value = None;
                }
                (
                    cursor.revision,
                    ItemKind::Transaction,
                    ServerMessage::ReplicaTransaction {
                        cursor,
                        origin_command_id,
                        provenance,
                        changes,
                    },
                )
            }
            ServerMessage::ReplicaDelta {
                id,
                cursor,
                mut payload,
            } => {
                self.ensure_open(socket, tenant_id, replicas);
                let Some(token) = self.bind(socket, &id, replicas) else {
                    return Ok(Some(ServerMessage::ReplicaDelta {
                        id,
                        cursor,
                        payload,
                    }));
                };
                // The collection digest and truncation describe this
                // connection's whole window (they differ between members who
                // see different rows); the changed rows do not. Share the rows,
                // append the window state per connection.
                let mut tail = String::with_capacity(128);
                for key in ["digest", "truncated"] {
                    if let Some(value) = payload.remove(key) {
                        tail.push_str(",\"");
                        tail.push_str(key);
                        tail.push_str("\":");
                        tail.push_str(&serde_json::to_string(&value).map_err(|_| DeliveryError)?);
                    }
                }
                tail.push('}');
                let revision = cursor.revision;
                let message = ServerMessage::ReplicaDelta {
                    id: token.to_string(),
                    cursor,
                    payload,
                };
                let frames = self.frames(tenant_id, hub);
                let text = frames.item(
                    ItemKey {
                        revision,
                        native,
                        kind: ItemKind::Delta(token),
                    },
                    message,
                )?;
                self.push_segment(socket, Segment::Shared(text));
                socket.push(Segment::Inline(Bytes::from(tail)));
                self.data += 1;
                return Ok(None);
            }
            ServerMessage::ReplicaReady {
                id,
                cursor,
                digest,
                metadata,
            } => {
                self.ensure_open(socket, tenant_id, replicas);
                let Some(token) = self.bind(socket, &id, replicas) else {
                    return Ok(Some(ServerMessage::ReplicaReady {
                        id,
                        cursor,
                        digest,
                        metadata,
                    }));
                };
                // Small and per connection (window digest): encoded inline.
                let text = serde_json::to_string(&ServerMessage::ReplicaReady {
                    id: token.to_string(),
                    cursor,
                    digest,
                    metadata,
                })
                .map_err(|_| DeliveryError)?;
                self.push_segment(socket, Segment::Inline(Bytes::from(text)));
                return Ok(None);
            }
            other => return Ok(Some(other)),
        };
        self.ensure_open(socket, tenant_id, replicas);
        let frames = self.frames(tenant_id, hub);
        let text = frames.item(
            ItemKey {
                revision,
                native,
                kind,
            },
            message,
        )?;
        self.push_segment(socket, Segment::Shared(text));
        self.data += 1;
        Ok(None)
    }

    /// Whether subscription `id` (of `identity`) is or would be bound to the
    /// default stream token `~{identity:016x}` on this socket (delivery v4
    /// shared items are encoded with it). Assumes distinct identities among
    /// the connection's subscriptions.
    pub(crate) fn default_binding(&self, id: &str, identity: u64) -> bool {
        self.bindings
            .get(id)
            .is_none_or(|binding| binding.identity != identity || binding.token.len() == 17)
    }

    /// Delivery v4: appends a shared transaction item.
    pub(crate) fn push_shared(
        &mut self,
        socket: &mut OutboundBatch,
        tenant_id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
        text: Arc<SharedText>,
    ) {
        self.ensure_open(socket, tenant_id, replicas);
        self.push_segment(socket, Segment::Shared(text));
        self.data += 1;
    }

    /// Delivery v4: appends a shared delta item and this connection's tail,
    /// exactly as `push` of the delta would. False when `id` has no
    /// subscription.
    pub(crate) fn push_delta(
        &mut self,
        socket: &mut OutboundBatch,
        tenant_id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
        id: &str,
        text: Arc<SharedText>,
        tail: String,
    ) -> bool {
        self.ensure_open(socket, tenant_id, replicas);
        if self.bind(socket, id, replicas).is_none() {
            return false;
        }
        self.push_segment(socket, Segment::Shared(text));
        socket.push(Segment::Inline(Bytes::from(tail)));
        self.data += 1;
        true
    }

    /// Delivery v4: appends this connection's ready item (already encoded).
    pub(crate) fn push_ready(
        &mut self,
        socket: &mut OutboundBatch,
        tenant_id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
        id: &str,
        text: Bytes,
    ) -> bool {
        self.ensure_open(socket, tenant_id, replicas);
        if self.bind(socket, id, replicas).is_none() {
            return false;
        }
        self.push_segment(socket, Segment::Inline(text));
        true
    }

    /// Appends a non-replica item (delivery v3: the connection's drained
    /// `presence.patch`) to the open batch, opening one when needed. It counts
    /// as data: the batch goes at the next tick, so presence keeps its own
    /// cadence (one drain per presence tick) without a frame of its own.
    pub fn push_item(
        &mut self,
        socket: &mut OutboundBatch,
        tenant_id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
        text: String,
    ) {
        self.ensure_open(socket, tenant_id, replicas);
        self.push_segment(socket, Segment::Inline(Bytes::from(text)));
        self.data += 1;
    }

    /// Records the connection's watermark for the open batch (opening a
    /// watermark-only batch when needed).
    pub fn watermark(
        &mut self,
        socket: &mut OutboundBatch,
        tenant_id: &str,
        replicas: &std::collections::BTreeMap<String, ReplicaSubscription>,
        revision: u64,
    ) {
        self.ensure_open(socket, tenant_id, replicas);
        let revision = self
            .watermark
            .map_or(revision, |current| current.max(revision));
        self.watermark = Some(revision);
        socket.set_tail(Some(Bytes::from(format!("],\"watermark\":{revision}"))));
    }

    /// Whether the open batch reached its size bound.
    pub fn full(&self, socket: &OutboundBatch) -> bool {
        socket.len() >= self.config.max_bytes
    }
}

/// The connection's visibility class for the batch header: the
/// `VisibilityClass::stable_id()` of its subscriptions' compiled bindings
/// (identical across processes and restarts). A connection whose
/// subscriptions are all bound to one class sends that class id as is; one
/// spanning several classes (one per plan) sends a hash of their sorted ids;
/// an uncompiled (fallback) binding contributes its principal fingerprint.
/// Informational (edge layer, diagnostics): which bytes a connection gets is
/// decided per subscription by its own class or its own decision.
fn class_id(replicas: &std::collections::BTreeMap<String, ReplicaSubscription>) -> u64 {
    use crate::visibility_class::MemberHandle;
    let id = |subscription: &ReplicaSubscription| match subscription.mask.peek() {
        Some(MemberHandle::Compiled(member)) => member.class().stable_id(),
        _ => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            subscription.resolved.fingerprint.hash(&mut hasher);
            hasher.finish()
        }
    };
    if replicas.len() == 1 {
        return replicas.values().next().map_or(0, id);
    }
    let mut ids: Vec<u64> = replicas.values().map(id).collect();
    ids.sort_unstable();
    ids.dedup();
    if let [only] = ids.as_slice() {
        return *only;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    ids.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batcher() -> ReplicaBatcher {
        let mut batcher = ReplicaBatcher::with_config(BatchConfig {
            tick: Duration::from_millis(50),
            idle: Duration::from_secs(1),
            max_bytes: 512 << 10,
        });
        batcher.set_enabled(true);
        batcher
    }

    #[tokio::test]
    async fn a_watermark_a_call_waits_for_is_due_at_once() {
        let replicas = std::collections::BTreeMap::new();
        let mut batch = OutboundBatch::default();
        let mut batcher = batcher();
        // No call waiting: a data-free batch waits for the idle delay.
        batcher.watermark(&mut batch, "t", &replicas, 4);
        let opened = Instant::now();
        let deadline = batcher.deadline(&batch).unwrap();
        assert!(deadline >= opened + Duration::from_millis(900));
        assert!(!batcher.call_waiting());
        batcher.take_frame(&mut batch, None).unwrap();
        // A call waits for revision 5: the batch carrying watermark 5 is due
        // now, not after the 50 ms tick.
        batcher.urgent(5);
        batcher.watermark(&mut batch, "t", &replicas, 5);
        assert!(batcher.call_waiting());
        assert!(batcher.deadline(&batch).unwrap() <= Instant::now());
        batcher.take_frame(&mut batch, None).unwrap();
        assert!(!batcher.call_waiting());
    }

    #[tokio::test]
    async fn a_later_call_never_delays_an_earlier_covered_one() {
        let replicas = std::collections::BTreeMap::new();
        let mut batch = OutboundBatch::default();
        let mut batcher = batcher();
        batcher.urgent(5);
        batcher.urgent(9);
        // Watermark 5 covers the first call (the second still waits): due now.
        batcher.watermark(&mut batch, "t", &replicas, 5);
        assert!(batcher.call_waiting());
        assert!(batcher.deadline(&batch).unwrap() <= Instant::now());
        batcher.take_frame(&mut batch, None).unwrap();
        // Revision 9 still waits; watermark 7 does not cover it.
        batcher.watermark(&mut batch, "t", &replicas, 7);
        assert!(!batcher.call_waiting());
        batcher.watermark(&mut batch, "t", &replicas, 9);
        assert!(batcher.call_waiting());
        batcher.take_frame(&mut batch, None).unwrap();
        batcher.watermark(&mut batch, "t", &replicas, 10);
        assert!(!batcher.call_waiting(), "every waiting call was covered");
    }
}
