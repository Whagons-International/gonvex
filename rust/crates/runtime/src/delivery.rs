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
    /// A batch holding only a watermark waits this long (unless a call is
    /// waiting for that revision, then `tick`).
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
    entries: HashMap<ItemKey, Vec<Variant>>,
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
    fn item(&self, key: ItemKey, message: ServerMessage) -> Result<Arc<SharedText>, ()> {
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
                return Err(());
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

fn encode_item(message: &ServerMessage, native: bool) -> Result<String, ()> {
    if native {
        if let Some(encoded) = crate::replica::native_control_message(message).map_err(|_| ())? {
            return serde_json::to_string(&encoded).map_err(|_| ());
        }
    }
    serde_json::to_string(message).map_err(|_| ())
}

/// Stream token of a collection: path and arguments, never the socket.
fn stream_identity(subscription: &ReplicaSubscription) -> u64 {
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
    bindings: HashMap<String, Binding>,
    token_owner: HashMap<Arc<str>, String>,
    // State of the batch currently open on the socket.
    open: bool,
    opened_at: Option<Instant>,
    items: usize,
    watermark: Option<u64>,
    last_watermark: u64,
    urgent_through: u64,
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
            bindings: HashMap::new(),
            token_owner: HashMap::new(),
            open: false,
            opened_at: None,
            items: 0,
            watermark: None,
            last_watermark: 0,
            urgent_through: 0,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Whether the open batch holds replica items (not only a watermark).
    pub fn has_items(&self) -> bool {
        self.open && self.items > 0
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
    }

    /// A call result waits for `revision`: a watermark-only batch covering it
    /// is sent at the next tick instead of after the idle delay.
    pub fn urgent(&mut self, revision: u64) {
        self.urgent_through = self.urgent_through.max(revision);
    }

    /// When the open batch must be sent.
    pub fn deadline(&mut self, socket: &OutboundBatch) -> Option<Instant> {
        self.sync(socket);
        let opened_at = self.opened_at?;
        let urgent = self
            .watermark
            .is_some_and(|watermark| self.urgent_through > 0 && watermark >= self.urgent_through);
        Some(if self.items > 0 || urgent {
            opened_at + self.config.tick
        } else {
            opened_at + self.config.idle
        })
    }

    pub async fn flush(&mut self, socket: &mut WebSocket) -> Result<(), ()> {
        let result = socket.flush_batch().await.map_err(|_| ());
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
            if let Some(watermark) = self.watermark.take() {
                self.last_watermark = self.last_watermark.max(watermark);
                if watermark >= self.urgent_through {
                    self.urgent_through = 0;
                }
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
    ) -> Result<Option<ServerMessage>, ()> {
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
                        tail.push_str(&serde_json::to_string(&value).map_err(|_| ())?);
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
                .map_err(|_| ())?;
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
        Ok(None)
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

/// Stand-in visibility class of a connection until shared visibility classes
/// land: a hash of its subscriptions' resolved visibility fingerprints. It is
/// informational (frame header); sharing is decided by content equality.
fn class_id(replicas: &std::collections::BTreeMap<String, ReplicaSubscription>) -> u64 {
    let mut fingerprints: Vec<&str> = replicas
        .values()
        .map(|subscription| subscription.resolved.fingerprint.as_str())
        .collect();
    fingerprints.sort_unstable();
    fingerprints.dedup();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    fingerprints.hash(&mut hasher);
    hasher.finish()
}
