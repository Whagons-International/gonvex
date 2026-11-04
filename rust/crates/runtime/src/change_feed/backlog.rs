//! Byte-bounded change-feed backlog.
//!
//! The broadcast ring of a tenant feed carries light signals (revision, epoch
//! and a [`Weak`] reference to the parsed change images). The strong
//! references live in a per-tenant [`FeedStore`] whose size is bounded by a
//! process-wide byte budget (`GONVEX_FEED_BACKLOG_BYTES`, default 64 MiB of
//! estimated heap) and by [`STORE_MAX_ENTRIES`] transactions. A connection
//! that keeps up upgrades the weak reference: every receiver shares one
//! parsed copy, as before. A connection that fell behind the budget finds the
//! images evicted and re-reads them from the durable change log from its own
//! cursor ([`FeedReceiver`]), instead of pinning them in memory for every
//! other connection of the tenant. A connection whose ring position was
//! overwritten (`Lagged`) resumes the same way rather than resetting its
//! subscriptions.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use gonvex_store::{PoolRegistry, TenantRoute};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use super::{FeedEvent, LogChange};

/// Signals the broadcast ring holds per tenant (`GONVEX_FEED_RING_CAPACITY`,
/// default 4,096). Signals are ~40 bytes, so the ring is sized to make
/// `Lagged` rare; lag is resumed from the log anyway.
fn ring_capacity() -> usize {
    static CAPACITY: OnceLock<usize> = OnceLock::new();
    *CAPACITY.get_or_init(|| {
        std::env::var("GONVEX_FEED_RING_CAPACITY")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|capacity| *capacity > 0)
            .unwrap_or(4096)
    })
}
/// Transactions the store keeps at most, whatever their size.
const STORE_MAX_ENTRIES: usize = 1024;
/// Revisions one log refill reads at most (bounds a lagging connection's
/// transient copy).
const FILL_REVISIONS: u64 = 16;
const DEFAULT_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// One element of the broadcast ring.
#[derive(Clone, Debug)]
pub(crate) enum FeedSignal {
    Transaction {
        database_epoch: Arc<str>,
        revision: u64,
        changes: Weak<Vec<LogChange>>,
    },
    Reset {
        reason: Arc<str>,
    },
}

/// Process-wide counters of the backlog (`/dev/metrics` → `memory.feedBacklog`).
#[derive(Default)]
struct Counters {
    bytes: AtomicUsize,
    entries: AtomicUsize,
    peak_bytes: AtomicUsize,
    published: AtomicU64,
    evicted: AtomicU64,
    fills: AtomicU64,
    fill_revisions: AtomicU64,
    fill_events: AtomicU64,
    fill_changes: AtomicU64,
    fill_ns: AtomicU64,
    lag_resumes: AtomicU64,
    fill_resets: AtomicU64,
    shared_events: AtomicU64,
    refill_bytes: AtomicUsize,
}

fn counters() -> &'static Counters {
    static COUNTERS: OnceLock<Counters> = OnceLock::new();
    COUNTERS.get_or_init(Counters::default)
}

fn budget_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("GONVEX_FEED_BACKLOG_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_BUDGET_BYTES)
    })
}

/// The backlog counters as JSON for the metrics endpoint.
pub fn metrics() -> Value {
    let c = counters();
    serde_json::json!({
        "budgetBytes": budget_bytes(),
        "bytes": c.bytes.load(Ordering::Relaxed),
        "peakBytes": c.peak_bytes.load(Ordering::Relaxed),
        "entries": c.entries.load(Ordering::Relaxed),
        "published": c.published.load(Ordering::Relaxed),
        "evicted": c.evicted.load(Ordering::Relaxed),
        "fills": c.fills.load(Ordering::Relaxed),
        "fillRevisions": c.fill_revisions.load(Ordering::Relaxed),
        "fillEvents": c.fill_events.load(Ordering::Relaxed),
        "fillChanges": c.fill_changes.load(Ordering::Relaxed),
        "fillMs": c.fill_ns.load(Ordering::Relaxed) as f64 / 1e6,
        "lagResumes": c.lag_resumes.load(Ordering::Relaxed),
        "fillResets": c.fill_resets.load(Ordering::Relaxed),
        "sharedRefillEvents": c.shared_events.load(Ordering::Relaxed),
        "refillBudgetBytes": refill_budget_bytes(),
        "refillBytes": c.refill_bytes.load(Ordering::Relaxed),
    })
}

/// Estimated heap bytes of one parsed change (its strings and JSON images).
pub(crate) fn change_bytes(change: &LogChange) -> usize {
    fn heap(value: &Value) -> usize {
        match value {
            Value::Null | Value::Bool(_) | Value::Number(_) => 0,
            Value::String(text) => text.capacity(),
            Value::Array(items) => {
                items.capacity() * std::mem::size_of::<Value>() + items.iter().map(heap).sum::<usize>()
            }
            // A map node per entry: key, value and tree overhead.
            Value::Object(map) => map
                .iter()
                .map(|(key, item)| {
                    key.capacity()
                        + std::mem::size_of::<String>()
                        + std::mem::size_of::<Value>()
                        + 16
                        + heap(item)
                })
                .sum(),
        }
    }
    let provenance = &change.provenance;
    std::mem::size_of::<LogChange>()
        + change.origin_command_id.capacity()
        + change.table.capacity()
        + change.row_id.capacity()
        + change.operation.capacity()
        + change
            .changed_columns
            .iter()
            .map(|column| column.capacity() + std::mem::size_of::<String>())
            .sum::<usize>()
        + heap(&change.old_value)
        + heap(&change.new_value)
        + provenance.root_command_id.capacity()
        + provenance.root_channel.capacity()
        + provenance.channel.capacity()
        + [
            &provenance.actor_account_id,
            &provenance.actor_member_id,
            &provenance.on_behalf_of_member_id,
            &provenance.agent_execution_id,
        ]
        .iter()
        .map(|text| text.as_ref().map_or(0, String::capacity))
        .sum::<usize>()
}

struct StoreState {
    entries: VecDeque<(u64, Arc<Vec<LogChange>>, usize)>,
    bytes: usize,
    /// Every revision at or below it has been broadcast (or predates the feed).
    latest: u64,
}

/// The strong half of one tenant feed: recent parsed transactions within the
/// byte budget, the ring sender and what a receiver needs to refill.
pub(crate) struct FeedStore {
    pools: PoolRegistry,
    route: TenantRoute,
    sender: broadcast::Sender<FeedSignal>,
    state: Mutex<StoreState>,
    refilled: Mutex<Refilled>,
    /// Refills of one tenant run one at a time, so receivers at the same
    /// position read the log once and share the result.
    fill_lock: tokio::sync::Mutex<()>,
}

/// Events receivers re-read from the log, kept for receivers at about the
/// same position: `next[cursor] = (revision, images)` says no transaction
/// lies in `(cursor, revision)` and `revision` is the next one (`None`: no
/// transaction at all through `revision`). Strong references, oldest dropped
/// first while the process-wide refill bytes exceed
/// `GONVEX_FEED_REFILL_BYTES` (default 32 MiB).
#[derive(Default)]
struct Refilled {
    next: std::collections::HashMap<u64, (u64, Option<Arc<Vec<LogChange>>>)>,
    /// Insertion order: (cursor key, revision, bytes).
    order: VecDeque<(u64, u64, usize)>,
    bytes: usize,
}

fn refill_budget_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("GONVEX_FEED_REFILL_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(32 * 1024 * 1024)
    })
}

impl Refilled {
    fn insert(&mut self, cursor: u64, revision: u64, images: Option<Arc<Vec<LogChange>>>) {
        let bytes = images.as_ref().map_or(16, |changes| {
            changes.iter().map(change_bytes).sum::<usize>() + 32
        });
        self.next.insert(cursor, (revision, images));
        self.order.push_back((cursor, revision, bytes));
        self.bytes += bytes;
        counters().refill_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record(&mut self, after: u64, fill: &Fill) {
        let mut cursor = after;
        for event in &fill.events {
            if let FeedEvent::Transaction { revision, changes, .. } = event {
                self.insert(cursor, *revision, Some(Arc::clone(changes)));
                cursor = *revision;
            }
        }
        if fill.through > cursor {
            self.insert(cursor, fill.through, None);
        }
        let budget = refill_budget_bytes();
        let c = counters();
        while c.refill_bytes.load(Ordering::Relaxed) > budget {
            let Some((cursor, revision, bytes)) = self.order.pop_front() else {
                break;
            };
            if self.next.get(&cursor).is_some_and(|entry| entry.0 == revision) {
                self.next.remove(&cursor);
            }
            self.bytes -= bytes;
            c.refill_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
    }

    fn clear(&mut self) {
        counters().refill_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.next.clear();
        self.order.clear();
        self.bytes = 0;
    }

    /// Kept events after `after` through at most `through`, and the revision
    /// they cover through.
    fn take(&self, epoch: &str, after: u64, through: u64) -> (Vec<FeedEvent>, u64) {
        let mut events = Vec::new();
        let mut cursor = after;
        while cursor < through {
            match self.next.get(&cursor) {
                Some((revision, Some(changes))) if *revision <= through => {
                    events.push(FeedEvent::Transaction {
                        database_epoch: epoch.to_owned(),
                        revision: *revision,
                        changes: Arc::clone(changes),
                    });
                    cursor = *revision;
                }
                Some((revision, None)) => cursor = (*revision).min(through),
                _ => break,
            }
        }
        (events, cursor)
    }
}

impl FeedStore {
    pub(crate) fn new(pools: PoolRegistry, route: TenantRoute) -> Arc<Self> {
        let (sender, _) = broadcast::channel(ring_capacity());
        Arc::new(Self {
            pools,
            route,
            sender,
            state: Mutex::new(StoreState {
                entries: VecDeque::new(),
                bytes: 0,
                latest: 0,
            }),
            refilled: Mutex::new(Refilled::default()),
            fill_lock: tokio::sync::Mutex::new(()),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The revision the feed reader starts after: receivers subscribed before
    /// any transaction is published resume from it.
    pub(crate) fn set_initial_revision(&self, revision: u64) {
        let mut state = self.state();
        state.latest = state.latest.max(revision);
    }

    pub(crate) fn subscribe(self: &Arc<Self>) -> FeedReceiver {
        // One lock with publish: the receiver's cursor is exactly the last
        // revision broadcast before its first signal.
        let state = self.state();
        let inner = self.sender.subscribe();
        FeedReceiver {
            inner: Inner::Hub(Box::new(HubReceiver {
                inner,
                store: Arc::clone(self),
                last: Some(state.latest),
                epoch: None,
                ready: VecDeque::new(),
                stashed: None,
                gap_until: None,
                fill: None,
            })),
        }
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    pub(crate) fn publish(&self, database_epoch: &Arc<str>, revision: u64, changes: Vec<LogChange>) {
        let bytes = changes.iter().map(change_bytes).sum::<usize>()
            + std::mem::size_of::<Vec<LogChange>>()
            + 32;
        let changes = Arc::new(changes);
        let signal = FeedSignal::Transaction {
            database_epoch: Arc::clone(database_epoch),
            revision,
            changes: Arc::downgrade(&changes),
        };
        let budget = budget_bytes();
        let c = counters();
        let mut state = self.state();
        state.entries.push_back((revision, changes, bytes));
        state.bytes += bytes;
        let total = c.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        c.peak_bytes.fetch_max(total, Ordering::Relaxed);
        c.entries.fetch_add(1, Ordering::Relaxed);
        c.published.fetch_add(1, Ordering::Relaxed);
        // The newest transaction always stays: receivers that keep up never
        // read the log, whatever its size.
        while state.entries.len() > 1
            && (state.entries.len() > STORE_MAX_ENTRIES
                || c.bytes.load(Ordering::Relaxed) > budget)
        {
            let Some((_, _, evicted)) = state.entries.pop_front() else {
                break;
            };
            state.bytes -= evicted;
            c.bytes.fetch_sub(evicted, Ordering::Relaxed);
            c.entries.fetch_sub(1, Ordering::Relaxed);
            c.evicted.fetch_add(1, Ordering::Relaxed);
        }
        state.latest = state.latest.max(revision);
        let _ = self.sender.send(signal);
    }

    pub(crate) fn reset(&self, reason: String, revision: Option<u64>) {
        self.refilled().clear();
        let mut state = self.state();
        if let Some(revision) = revision {
            state.latest = state.latest.max(revision);
        }
        let _ = self.sender.send(FeedSignal::Reset {
            reason: Arc::from(reason),
        });
    }

    fn refilled(&self) -> std::sync::MutexGuard<'_, Refilled> {
        self.refilled.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `(latest broadcast revision, oldest revision still held)`.
    fn bounds(&self) -> (u64, Option<u64>) {
        let state = self.state();
        (state.latest, state.entries.front().map(|entry| entry.0))
    }
}

impl Drop for FeedStore {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        let c = counters();
        c.bytes.fetch_sub(state.bytes, Ordering::Relaxed);
        c.entries.fetch_sub(state.entries.len(), Ordering::Relaxed);
        self.refilled
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

type FillResult = Result<Fill, String>;

struct Fill {
    epoch: String,
    events: Vec<FeedEvent>,
    through: u64,
}

struct HubReceiver {
    inner: broadcast::Receiver<FeedSignal>,
    store: Arc<FeedStore>,
    /// Last revision this receiver has yielded or proven empty; `None` after a
    /// reset (lag then propagates as before).
    last: Option<u64>,
    epoch: Option<Arc<str>>,
    ready: VecDeque<FeedEvent>,
    /// A signal taken from the ring that waits for a refill to cover it.
    stashed: Option<FeedSignal>,
    /// After `Lagged`: the log must cover revisions through this one before
    /// ring signals are trusted again.
    gap_until: Option<u64>,
    /// A running refill. Spawned so that a cancelled `recv` (it runs inside
    /// `select!`) never restarts or loses it.
    fill: Option<tokio::task::JoinHandle<FillResult>>,
}

/// A tenant feed receiver: yields every committed transaction once, in
/// revision order, whether its images were still in memory or re-read from
/// the change log. `recv` is cancel-safe.
pub struct FeedReceiver {
    inner: Inner,
}

enum Inner {
    /// A plain channel of resolved events (tests and benches).
    Plain(broadcast::Receiver<FeedEvent>),
    Hub(Box<HubReceiver>),
}

impl From<broadcast::Receiver<FeedEvent>> for FeedReceiver {
    fn from(receiver: broadcast::Receiver<FeedEvent>) -> Self {
        Self {
            inner: Inner::Plain(receiver),
        }
    }
}

impl std::fmt::Debug for FeedReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedReceiver").field("len", &self.len()).finish()
    }
}

enum Step {
    Yield(Result<FeedEvent, RecvError>),
    /// A refill must run before the next event.
    Fill { after: u64, through: u64 },
    Continue,
}

impl HubReceiver {
    /// One step on a signal (or lag) taken from the ring.
    fn on_signal(&mut self, signal: Result<FeedSignal, RecvError>) -> Step {
        match signal {
            Err(RecvError::Closed) => Step::Yield(Err(RecvError::Closed)),
            Err(RecvError::Lagged(skipped)) => {
                let Some(last) = self.last else {
                    return Step::Yield(Err(RecvError::Lagged(skipped)));
                };
                counters().lag_resumes.fetch_add(1, Ordering::Relaxed);
                // The ring now starts at its oldest signal; everything before
                // it comes from the log.
                let until = match self.inner.try_recv() {
                    Ok(next) => {
                        let until = match &next {
                            FeedSignal::Transaction { revision, .. } => revision.saturating_sub(1),
                            FeedSignal::Reset { .. } => self.store.bounds().0,
                        };
                        self.stashed = Some(next);
                        until
                    }
                    Err(_) => self.store.bounds().0,
                };
                self.gap_until = Some(until.max(last));
                Step::Continue
            }
            Ok(FeedSignal::Reset { reason }) => {
                self.last = None;
                self.gap_until = None;
                Step::Yield(Ok(FeedEvent::Reset {
                    reason: reason.to_string(),
                }))
            }
            Ok(
                signal @ FeedSignal::Transaction {
                    revision,
                    ..
                },
            ) => {
                if self.last.is_some_and(|last| revision <= last) {
                    // Already yielded from the log.
                    return Step::Continue;
                }
                let FeedSignal::Transaction {
                    database_epoch,
                    changes,
                    ..
                } = &signal
                else {
                    unreachable!()
                };
                if let Some(changes) = changes.upgrade() {
                    self.last = Some(revision);
                    self.epoch = Some(Arc::clone(database_epoch));
                    return Step::Yield(Ok(FeedEvent::Transaction {
                        database_epoch: database_epoch.to_string(),
                        revision,
                        changes,
                    }));
                }
                // Evicted: re-read it (and the evicted ones after it) from
                // the log, from this receiver's cursor.
                let after = self.last.unwrap_or(revision.saturating_sub(1));
                let (latest, oldest) = self.store.bounds();
                let mut through = (after + FILL_REVISIONS).min(latest.max(revision));
                if let Some(oldest) = oldest.filter(|oldest| *oldest > revision) {
                    through = through.min(oldest - 1);
                }
                if self.epoch.is_none() {
                    self.epoch = Some(Arc::clone(database_epoch));
                }
                self.stashed = Some(signal);
                Step::Fill {
                    after,
                    through: through.max(after + 1),
                }
            }
        }
    }

    /// Next step before reading the ring: queued events, a pending gap.
    fn before_ring(&mut self) -> Option<Step> {
        if let Some(event) = self.ready.pop_front() {
            return Some(Step::Yield(Ok(event)));
        }
        if let (Some(until), Some(last)) = (self.gap_until, self.last) {
            if last >= until {
                self.gap_until = None;
            } else {
                return Some(Step::Fill {
                    after: last,
                    through: (last + FILL_REVISIONS).min(until),
                });
            }
        }
        None
    }

    fn start_fill(&mut self, after: u64, through: u64) {
        if let Some(epoch) = self.epoch.clone() {
            let (events, covered) = self.store.refilled().take(&epoch, after, through);
            if covered > after {
                counters()
                    .shared_events
                    .fetch_add(events.len() as u64, Ordering::Relaxed);
                self.last = Some(covered);
                self.ready.extend(events);
                return;
            }
        }
        let store = Arc::clone(&self.store);
        let epoch = self.epoch.clone();
        self.fill = Some(tokio::spawn(gonvex_profiler::instrument(
            gonvex_profiler::TaskKind::Feed,
            async move {
                let _single = store.fill_lock.lock().await;
                if let Some(epoch) = epoch {
                    let (events, covered) = store.refilled().take(&epoch, after, through);
                    if covered > after {
                        counters()
                            .shared_events
                            .fetch_add(events.len() as u64, Ordering::Relaxed);
                        return Ok(Fill {
                            epoch: epoch.to_string(),
                            events,
                            through: covered,
                        });
                    }
                }
                let fill = read_fill(&store.pools, &store.route, after, through).await?;
                store.refilled().record(after, &fill);
                Ok(fill)
            },
        )));
    }

    fn finish_fill(&mut self, result: Result<FillResult, tokio::task::JoinError>) -> Step {
        let result = result.map_err(|error| error.to_string()).and_then(|result| result);
        let c = counters();
        match result {
            Ok(fill) if self.epoch.as_deref().is_none_or(|epoch| epoch == fill.epoch) => {
                c.fill_events.fetch_add(fill.events.len() as u64, Ordering::Relaxed);
                if self.epoch.is_none() {
                    self.epoch = Some(Arc::from(fill.epoch.as_str()));
                }
                self.last = Some(fill.through);
                self.ready.extend(fill.events);
                Step::Continue
            }
            outcome => {
                c.fill_resets.fetch_add(1, Ordering::Relaxed);
                let reason = match outcome {
                    Ok(_) => "change feed epoch changed".to_owned(),
                    Err(error) => error,
                };
                self.last = None;
                self.gap_until = None;
                self.ready.clear();
                Step::Yield(Ok(FeedEvent::Reset { reason }))
            }
        }
    }

    async fn recv(&mut self) -> Result<FeedEvent, RecvError> {
        loop {
            if let Some(fill) = self.fill.as_mut() {
                let result = fill.await;
                self.fill = None;
                match self.finish_fill(result) {
                    Step::Yield(event) => return event,
                    _ => continue,
                }
            }
            let step = match self.before_ring() {
                Some(step) => step,
                None => {
                    let signal = match self.stashed.take() {
                        Some(signal) => Ok(signal),
                        None => self.inner.recv().await,
                    };
                    self.on_signal(signal)
                }
            };
            match step {
                Step::Yield(event) => return event,
                Step::Fill { after, through } => self.start_fill(after, through),
                Step::Continue => {}
            }
        }
    }

    fn try_recv(&mut self) -> Result<FeedEvent, TryRecvError> {
        loop {
            if self.fill.is_some() {
                return Err(TryRecvError::Empty);
            }
            let step = match self.before_ring() {
                Some(step) => step,
                None => {
                    let signal = match self.stashed.take() {
                        Some(signal) => Ok(signal),
                        None => match self.inner.try_recv() {
                            Ok(signal) => Ok(signal),
                            Err(TryRecvError::Empty) => return Err(TryRecvError::Empty),
                            Err(TryRecvError::Closed) => Err(RecvError::Closed),
                            Err(TryRecvError::Lagged(skipped)) => Err(RecvError::Lagged(skipped)),
                        },
                    };
                    self.on_signal(signal)
                }
            };
            match step {
                Step::Yield(Ok(event)) => return Ok(event),
                Step::Yield(Err(RecvError::Closed)) => return Err(TryRecvError::Closed),
                Step::Yield(Err(RecvError::Lagged(skipped))) => {
                    return Err(TryRecvError::Lagged(skipped))
                }
                Step::Fill { after, through } => {
                    // Refills are async: start it, the next `recv` takes it.
                    self.start_fill(after, through);
                    return Err(TryRecvError::Empty);
                }
                Step::Continue => {}
            }
        }
    }

    fn pending(&self) -> usize {
        self.ready.len()
            + usize::from(self.stashed.is_some())
            + usize::from(self.fill.is_some())
            + usize::from(self.gap_until.is_some())
    }
}

impl FeedReceiver {
    /// The next event. Cancel-safe: a signal taken from the ring and a running
    /// refill stay in the receiver.
    pub async fn recv(&mut self) -> Result<FeedEvent, RecvError> {
        match &mut self.inner {
            Inner::Plain(receiver) => receiver.recv().await,
            Inner::Hub(receiver) => receiver.recv().await,
        }
    }

    /// The next event if one is available without waiting (a refill never
    /// completes here; it starts and `recv` returns it).
    pub fn try_recv(&mut self) -> Result<FeedEvent, TryRecvError> {
        match &mut self.inner {
            Inner::Plain(receiver) => receiver.try_recv(),
            Inner::Hub(receiver) => receiver.try_recv(),
        }
    }

    /// Signals waiting in the ring plus events this receiver holds or is
    /// refilling (an upper bound on the events `recv` returns without waiting
    /// for a new commit).
    pub fn len(&self) -> usize {
        match &self.inner {
            Inner::Plain(receiver) => receiver.len(),
            Inner::Hub(receiver) => receiver.inner.len() + receiver.pending(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

async fn read_fill(
    pools: &PoolRegistry,
    route: &TenantRoute,
    after: u64,
    through: u64,
) -> FillResult {
    let started = std::time::Instant::now();
    let clock = super::read_clock(pools, route)
        .await
        .map_err(|error| format!("change feed refill failed: {error}"))?;
    if after < clock.retained_revision {
        return Err("change feed cursor expired".to_owned());
    }
    let through = through.min(clock.revision.max(after));
    let changes = if through > after {
        super::read_changes(pools, route, after, through)
            .await
            .map_err(|error| format!("change feed refill failed: {error}"))?
    } else {
        Vec::new()
    };
    let c = counters();
    c.fills.fetch_add(1, Ordering::Relaxed);
    c.fill_revisions.fetch_add(through - after, Ordering::Relaxed);
    c.fill_changes.fetch_add(changes.len() as u64, Ordering::Relaxed);
    let events = group_transactions(&clock.epoch, changes);
    c.fill_ns
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    gonvex_profiler::record_duration(
        gonvex_profiler::Stage::FeedRead,
        gonvex_profiler::Label::None,
        started.elapsed(),
    );
    Ok(Fill {
        epoch: clock.epoch,
        events,
        through,
    })
}

/// Change rows in log order as one event per revision.
pub(crate) fn group_transactions(epoch: &str, changes: Vec<LogChange>) -> Vec<FeedEvent> {
    let mut events = Vec::new();
    let mut batch: Vec<LogChange> = Vec::new();
    for change in changes {
        if batch.last().is_some_and(|last| last.revision != change.revision) {
            let revision = batch[0].revision;
            events.push(FeedEvent::Transaction {
                database_epoch: epoch.to_owned(),
                revision,
                changes: Arc::new(std::mem::take(&mut batch)),
            });
        }
        batch.push(change);
    }
    if let Some(first) = batch.first() {
        let revision = first.revision;
        events.push(FeedEvent::Transaction {
            database_epoch: epoch.to_owned(),
            revision,
            changes: Arc::new(batch),
        });
    }
    events
}

impl Drop for HubReceiver {
    fn drop(&mut self) {
        if let Some(fill) = self.fill.take() {
            fill.abort();
        }
    }
}
