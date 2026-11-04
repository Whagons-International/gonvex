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
use crate::fast_hash::{FastMap, FastSet};
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

macro_rules! paths {
    ($($variant:ident => $name:literal, $doc:literal;)*) => {
        /// Delivery coverage: how one batch connection's feed step for one
        /// transaction was decided (shared fan-out or its own full path, and
        /// why). Counted once per connection per event in the connection
        /// loop (`count`), exported in `/dev/profile` (`deliveryCoverage`).
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Path { $($variant),* }
        /// (name, description) per path, indexed by `Path as usize`.
        pub const PATHS: &[(&str, &str)] = &[$(($name, $doc)),*];
    };
}

paths! {
    SharedAdvance => "shared.advance", "Served from its shape: nothing to decide, cursors advance (no outcome needed)";
    SharedData => "shared.data", "Served from its shape's recorded outcome (decided rows appended)";
    Representative => "full.representative", "Has a shape but no outcome yet: the full path runs and records it";
    ServeRejected => "full.serve_rejected", "Outcome exists but a bounded window would leave its budget, or a non-default stream token";
    NotBatch => "full.not_batch", "Batching off, a snapshot still being delivered, or a non-transaction event";
    NoShare => "full.no_share", "No tenant event share (sharing off, or the event left the share ring)";
    Inherited => "full.inherited", "A reached subscription has an inherited plan the shape cannot follow (inheritsAny, nested inherits)";
    Legacy => "full.legacy", "A reached subscription has a version 1 (SQL) plan";
    DuplicateStream => "full.duplicate_stream", "Two subscriptions share one stream identity";
    PrincipalMoved => "full.principal_moved", "A changed row moves this member's principal, grants, derived sets or ancestors (the mask step's per-change checks): it re-resolves or re-decides affected rows";
    ParentAcl => "full.parent_acl", "A parent row of an inherited plan changed visibility columns or was deleted (children may flip)";
    ParentAndSource => "full.parent_and_source", "An inherited plan's source rows and a parent row that was created or changed visibility columns in one transaction (decided against the parents' before-images)";
    IndexWaiting => "full.index_waiting", "A bound shared index is not at the event's revision yet";
    CursorEpoch => "full.cursor_epoch", "Cursor of another epoch, or a bound index already ahead of the event";
    Truncated => "full.truncated", "A reached collection of a changed table has a truncated window";
    NotCompiled => "full.not_compiled", "The member's visibility is not compiled (fallback evaluation)";
    Hit => "full.hit", "A changed row hits a member atom (personal grant, assignment, own row): decided individually";
    ManyRows => "full.many_rows", "More than 32 changed rows of one collection's table";
    Empty => "empty", "No replica subscriptions (nothing to decide)";
    Unclean => "full.unclean", "Its shape's first connection could not record an outcome (resync, principal change): decided alone";
}

const PATH_COUNT: usize = PATHS.len();
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static PATH_COUNTS: [AtomicU64; PATH_COUNT] = [ZERO; PATH_COUNT];

static WAITS: AtomicU64 = AtomicU64::new(0);
static WAKEUPS: AtomicU64 = AtomicU64::new(0);
static MERGED: AtomicU64 = AtomicU64::new(0);
static MERGED_SHARED: AtomicU64 = AtomicU64::new(0);
static NOOP_WAKEUPS: AtomicU64 = AtomicU64::new(0);

/// Counts one batch connection's feed arm for one transaction; `noop`: it
/// added no data item (nothing but its watermark to send).
pub fn count_wakeup(noop: bool) {
    WAKEUPS.fetch_add(1, Ordering::Relaxed);
    if noop {
        NOOP_WAKEUPS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Counts one connection's feed step (`Path`).
pub fn count(path: Path) {
    PATH_COUNTS[path as usize].fetch_add(1, Ordering::Relaxed);
}

/// Counts per path since start, indexed by `Path as usize`.
pub fn path_counts() -> [u64; PATH_COUNT] {
    std::array::from_fn(|index| PATH_COUNTS[index].load(Ordering::Relaxed))
}

/// `/dev/profile` `deliveryCoverage`: per path its count, plus the shared
/// and individual (full path) per-connection time histograms.
pub fn coverage_json() -> serde_json::Value {
    let counts = path_counts();
    let shared: u64 = counts[..2].iter().sum();
    let total: u64 = counts.iter().sum();
    serde_json::json!({
        "total": total,
        "shared": shared,
        "sharedFraction": if total == 0 { 0.0 } else { shared as f64 / total as f64 },
        "waits": WAITS.load(Ordering::Relaxed),
        "wake": crate::wake::metrics(),
        "merged": MERGED.load(Ordering::Relaxed),
        "mergedShared": MERGED_SHARED.load(Ordering::Relaxed),
        "wakeups": WAKEUPS.load(Ordering::Relaxed),
        "noopWakeups": NOOP_WAKEUPS.load(Ordering::Relaxed),
        "paths": PATHS.iter().zip(counts).map(|((name, doc), count)| serde_json::json!({
            "path": name, "count": count, "doc": doc,
        })).collect::<Vec<_>>(),
    })
}

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

/// Per subscription, computed once: what its shape entry needs.
/// check need.
pub(crate) struct FanoutKeys {
    /// Interned argument JSON: equal arguments share one allocation, so a
    /// shape compares its address.
    args: Arc<str>,
    identity: u64,
    shareable: Shareable,
    /// Tables whose change may move this member's principal, grants or
    /// derived sets (`members`, every set and join table of its plan and its
    /// parents' plans), with the checks that can fire for each: only their
    /// changes need the per-member checks (`replica_mask::member_unmoved`).
    /// One map per combination of plans, shared.
    moving: Arc<FastMap<String, mask::MovingChecks>>,
}

/// Whether and how a subscription's plan can take the shared path.
pub(crate) enum Shareable {
    /// Version 2 plan without inherited visibility (`shareable_plan`).
    Plain,
    /// Version 2 plan with plain `inherits` parents (no `inheritsAny`, no
    /// parent that inherits again): its source rows are decided by its class
    /// through the shared parent index; parent rows are followed below.
    Inherited(Inherited),
    /// Never (why).
    No(Path),
}

pub(crate) struct Inherited {
    /// Per direct parent table, its ACL index columns: an update that
    /// changes none of them cannot flip whether the parent admits children.
    acl: Arc<BTreeMap<String, BTreeSet<String>>>,
}

impl Shareable {
    fn no(&self) -> Option<Path> {
        match self {
            Self::No(path) => Some(*path),
            _ => None,
        }
    }
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
    static ARGS: OnceLock<Registry<String, str>> = OnceLock::new();
    static MOVING: OnceLock<Registry<Vec<usize>, FastMap<String, mask::MovingChecks>>> =
        OnceLock::new();
    let shareable = shareable(subscription);
    // Shared plans never mutate; a key of addresses is valid while the set
    // lives (held only by keys of subscriptions of these plans). Followed
    // parents never inherit again, so the plans determine the set.
    let plans: Vec<&VisibilityPlan> = std::iter::once(subscription.visibility.as_ref())
        .chain(subscription.resolved.inherited.values().map(|parent| parent.plan.as_ref()))
        .collect();
    let moving = interned(
        MOVING.get_or_init(Default::default),
        plans
            .iter()
            .map(|plan| *plan as *const VisibilityPlan as usize)
            .collect(),
        || {
            // Mirrors where each check of `member_unmoved` can fire:
            // `principal_change_concerns` reads `members` and the tables of
            // non-derived sets (a grant set only through its joins),
            // derived checks the derived sets' tables, `grant_delta` the
            // grant sets' tables; each also for the parents' plans.
            let mut moving = FastMap::<String, mask::MovingChecks>::default();
            moving.entry("members".to_owned()).or_default().principal = true;
            for plan in &plans {
                for set in plan.sets.values() {
                    let entry = moving.entry(set.table.clone()).or_default();
                    match set.kind.as_str() {
                        "derived" => entry.derived = true,
                        "grant" => entry.grant = true,
                        _ => entry.principal = true,
                    }
                    if set.kind != "derived" {
                        for join in &set.joins {
                            moving.entry(join.table.clone()).or_default().principal = true;
                        }
                    }
                }
            }
            Arc::new(moving)
        },
    );
    let args = serde_json::to_string(&subscription.args).unwrap_or_default();
    let args = interned(ARGS.get_or_init(Default::default), args.clone(), || {
        Arc::from(args)
    });
    let keys = Arc::new(FanoutKeys {
        args,
        identity: crate::delivery::stream_identity(subscription),
        shareable,
        moving,
    });
    subscription.fanout = Some(Arc::clone(&keys));
    keys
}

fn shareable(subscription: &mut ReplicaSubscription) -> Shareable {
    let plan = &subscription.visibility;
    if plan.version != Some(2) {
        return Shareable::No(Path::Legacy);
    }
    if shareable_plan(plan) {
        return Shareable::Plain;
    }
    let resolved = &subscription.resolved;
    let followed = visibility::inherits_any(plan).is_empty()
        && plan
            .inherited_columns()
            .keys()
            .all(|table| resolved.inherited.contains_key(table))
        && resolved.inherited.values().all(|parent| {
            parent.resolved.inherited.is_empty()
                && parent.plan.inherited_columns().is_empty()
                && visibility::inherits_any(&parent.plan).is_empty()
        });
    if !followed {
        return Shareable::No(Path::Inherited);
    }
    Shareable::Inherited(Inherited {
        acl: mask::subscription_parent_acl_columns(subscription),
    })
}

/// The event's changed tables and its collapsed rows (first before-image,
/// last after-image, as `collapse_changes`), by change index into the share.
pub(crate) struct Layout {
    tables: FastSet<String>,
    rows: Vec<(String, usize, usize)>,
}

fn layout(changes: &[LogChange]) -> Layout {
    let mut index = FastMap::<(&str, &str), usize>::with_capacity_and_hasher(
        changes.len(),
        Default::default(),
    );
    let mut tables = FastSet::<String>::default();
    let mut rows: Vec<(String, usize, usize)> = Vec::new();
    for (position, change) in changes.iter().enumerate() {
        match index.get(&(change.table.as_str(), change.row_id.as_str())) {
            Some(row) => rows[*row].2 = position,
            None => {
                index.insert((change.table.as_str(), change.row_id.as_str()), rows.len());
                if !tables.contains(change.table.as_str()) {
                    tables.insert(change.table.clone());
                }
                rows.push((change.table.clone(), position, position));
            }
        }
    }
    Layout { tables, rows }
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

/// The outcome key: the entries of the subscriptions the event decides or
/// asks (collections of changed tables: `Apply`, or `Ahead` for
/// normalization), in subscription id order. Subscriptions it only advances
/// (`Skip`) or leaves (`Ahead` of untouched tables) are not part of it:
/// connections that differ only in those (a task detail window open on
/// another task) share one outcome.
pub(crate) type Shape = Vec<ShapeEntry>;

/// A connection's shape for one event: the outcome key and, per
/// subscription in id order, its state and whether its entry is in the key.
pub(crate) struct Shaped {
    pub(crate) key: Shape,
    states: Vec<(State, bool)>,
}

impl Shaped {
    fn decides(&self) -> bool {
        self.key.iter().any(|entry| entry.state == State::Apply)
    }
}

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
                            let item =
                                crate::delivery::encode_shared(&delta.delta, native, true).ok()?;
                            let ready = serde_json::to_string(&delta.ready).ok()?;
                            const DIGEST: &str = "\"digest\":\"";
                            let at = ready.find(&format!("{DIGEST}\""))? + DIGEST.len();
                            Some((
                                Arc::new(item),
                                Bytes::copy_from_slice(&ready.as_bytes()[..at]),
                                Bytes::copy_from_slice(&ready.as_bytes()[at..]),
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

/// Whether every index bound into `resolved` (and its inherited parents',
/// with the parent indexes themselves) is at `revision` of `epoch` right now
/// (no waiting): the synchronous form of `visibility::indexes_ready`.
fn indexes_ready_now(resolved: &ResolvedVisibility, revision: u64, epoch: &str) -> bool {
    let ready = |index: &std::sync::RwLock<crate::visibility_index::ParentAclIndex>| {
        crate::visibility_index::ready_now(index, revision, epoch) == Some(true)
    };
    resolved.indexes.values().all(|index| ready(index))
        && resolved.inherited.values().all(|parent| {
            ready(&parent.index) && indexes_ready_now(&parent.resolved, revision, epoch)
        })
}

/// The union of the changed columns of a collapsed row's changes
/// (`changes[first..=last]` of that table and row): None when some change
/// names none (unknown).
fn changed_columns(changes: &[LogChange], first: usize, last: usize) -> Option<Vec<&str>> {
    let (table, row) = (&changes[first].table, &changes[first].row_id);
    let mut columns = Vec::new();
    for change in &changes[first..=last] {
        if change.table != *table || change.row_id != *row {
            continue;
        }
        if change.changed_columns.is_empty() {
            return None;
        }
        columns.extend(change.changed_columns.iter().map(String::as_str));
    }
    (!columns.is_empty()).then_some(columns)
}

/// The connection's shape for this event, or why some subscription needs
/// its own decisions (see the module documentation). Does not change any
/// subscription state except its cached keys.
///
/// `scope` is the tenant database (class scope): a subscription that the
/// full path would decide (`Apply`) is bound to its compiled class here as
/// the full path binds it, so both leave the same binding behind.
pub(crate) fn shape(
    subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
    share: &EventShare,
    epoch: &str,
    revision: u64,
    scope: &str,
) -> Result<Shaped, Path> {
    let layout = share.layout.get_or_init(|| layout(share.changes()));
    let changes = share.changes();
    // Cheap static checks first: a connection with a collection the fan-out
    // never serves (inherited plans, legacy plans) is rejected before any
    // per-row work.
    for subscription in subscriptions.values_mut() {
        if revision > subscription.cursor.revision {
            if let Some(path) = keys(subscription).shareable.no() {
                return Err(path);
            }
        }
    }
    let mut shape: Vec<ShapeEntry> = Vec::with_capacity(subscriptions.len());
    let mut memo = None;
    for subscription in subscriptions.values_mut() {
        let keys = keys(subscription);
        if shape.iter().any(|entry| entry.identity == keys.identity) {
            return Err(Path::DuplicateStream);
        }
        let touched = layout.tables.contains(&subscription.definition.table);
        // A collection whose own filters (argument equalities, exclusions)
        // admit neither image of any changed row of its table decides no row
        // and holds none of them (a task detail window on another task): it
        // only advances, so it stays out of the outcome key.
        let filtered_out = touched
            && layout
                .rows
                .iter()
                .filter(|(table, _, _)| *table == subscription.definition.table)
                .all(|(_, first, last)| {
                    let admits = |row: &serde_json::Value| {
                        !row.is_null()
                            && collection_filters_admit(row, &subscription.args, &subscription.definition)
                    };
                    !admits(&changes[*first].old_value) && !admits(&changes[*last].new_value)
                });
        let state = if revision <= subscription.cursor.revision {
            State::Ahead
        } else {
            if let Some(path) = keys.shareable.no() {
                return Err(path);
            }
            // The mask step's per-change checks, per member: a change of a
            // table its visibility reads that moves its principal, grants,
            // derived sets or ancestors needs its own decisions.
            let concern = mask::subscription_concern_tables(subscription);
            // Per distinct changed table first (an event has few), per
            // change only for tables that can move a principal.
            let concerned = layout.tables.iter().any(|table| concern.contains(table.as_str()));
            let moving = concerned
                && layout
                    .tables
                    .iter()
                    .any(|table| concern.contains(table.as_str()) && keys.moving.contains_key(table.as_str()));
            for (position, change) in changes.iter().enumerate() {
                if !moving || !concern.contains(change.table.as_str()) {
                    continue;
                }
                if let Some(checks) = keys.moving.get(change.table.as_str()) {
                    let mut checks = *checks;
                    if checks.principal {
                        // Member-independent: once per event and plans.
                        let key = (Arc::as_ptr(&keys.moving) as *const u8 as usize, position);
                        let known = share.principal_possible.lock().unwrap().get(&key).copied();
                        checks.principal = known.unwrap_or_else(|| {
                            let possible = std::iter::once(subscription.visibility.as_ref())
                                .chain(
                                    subscription
                                        .resolved
                                        .inherited
                                        .values()
                                        .map(|parent| parent.plan.as_ref()),
                                )
                                .any(|plan| {
                                    plan.principal_change_possible(
                                        &change.table,
                                        &change.operation,
                                        &change.changed_columns,
                                    )
                                });
                            share.principal_possible.lock().unwrap().insert(key, possible);
                            possible
                        });
                    }
                    if !mask::member_unmoved(subscription, change, checks) {
                        return Err(Path::PrincipalMoved);
                    }
                }
            }
            if (!subscription.resolved.indexes.is_empty()
                || !subscription.resolved.inherited.is_empty())
                && !indexes_ready_now(&subscription.resolved, revision, epoch)
            {
                return Err(Path::IndexWaiting);
            }
            if !concerned {
                // Nothing its visibility reads changed: the mask step skips it.
                shape.push(ShapeEntry {
                    state: State::Skip,
                    visible: 0,
                    definition: ByAddress(None),
                    args: ByAddress(None),
                    identity: keys.identity,
                });
                continue;
            }
            if let Shareable::Inherited(inherited) = &keys.shareable {
                // Parent rows: the mask step re-decides a parent's children
                // only when the parent's admission flips for this member.
                let mut parent_changed = false;
                // Every changed parent is an update of columns its ACL index
                // does not read: its before-image (the mask step's overlay)
                // and the shared index agree on everything visibility reads.
                let mut parents_unmoved = true;
                for (table, first, last) in &layout.rows {
                    let Some(acl) = inherited.acl.get(table) else {
                        continue;
                    };
                    parent_changed = true;
                    let old = &changes[*first].old_value;
                    let new = &changes[*last].new_value;
                    if old.is_null() {
                        // A parent created by this transaction has no
                        // children beyond the transaction's own rows.
                        parents_unmoved = false;
                        continue;
                    }
                    if !new.is_null()
                        && changed_columns(changes, *first, *last)
                            .is_some_and(|columns| columns.iter().all(|column| !acl.contains(*column)))
                    {
                        // No visibility column moved (principal unchanged:
                        // `member_unmoved` above).
                        continue;
                    }
                    parents_unmoved = false;
                    let crate::visibility_class::MemberHandle::Compiled(member) = subscription
                        .mask
                        .get(&subscription.visibility, &subscription.resolved, scope)
                    else {
                        return Err(Path::NotCompiled);
                    };
                    let memo = memo.get_or_insert_with(|| share.memo.lock().unwrap());
                    match member.parent_default_flip(memo, table, old, new) {
                        Some(false) => {}
                        Some(true) => return Err(Path::ParentAcl),
                        None => return Err(Path::Hit),
                    }
                }
                if parent_changed && touched && !parents_unmoved && !filtered_out {
                    // The mask step decides the source rows against the
                    // parents' before-images (overlay), which differ from the
                    // index on what visibility reads.
                    return Err(Path::ParentAndSource);
                }
                if parent_changed {
                    // The mask step applies it (no rows): it binds the mask.
                    subscription
                        .mask
                        .get(&subscription.visibility, &subscription.resolved, scope);
                }
            }
            if touched {
                // Shared deltas carry the representative's cursor.
                if subscription.cursor.epoch != epoch
                    || ((!subscription.resolved.indexes.is_empty()
                        || !subscription.resolved.inherited.is_empty())
                        && visibility::indexes_ahead(&subscription.resolved, revision))
                {
                    return Err(Path::CursorEpoch);
                }
                if filtered_out {
                    // Applied with no row by the full path, which binds the
                    // mask; the cursor advances.
                    subscription
                        .mask
                        .get(&subscription.visibility, &subscription.resolved, scope);
                    shape.push(ShapeEntry {
                        state: State::Skip,
                        visible: 0,
                        definition: ByAddress(None),
                        args: ByAddress(None),
                        identity: keys.identity,
                    });
                    continue;
                }
                State::Apply
            } else {
                State::Skip
            }
        };
        if !touched || filtered_out {
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
            return Err(Path::Truncated);
        }
        let member = if state == State::Apply {
            // Decided by the full path, which binds the mask.
            subscription
                .mask
                .get(&subscription.visibility, &subscription.resolved, scope)
        } else {
            subscription.mask.peek().cloned().ok_or(Path::NotCompiled)?
        };
        let crate::visibility_class::MemberHandle::Compiled(member) = member else {
            return Err(Path::NotCompiled);
        };
        let memo = memo.get_or_insert_with(|| share.memo.lock().unwrap());
        let mut visible = 0u64;
        let mut row = 0;
        for (table, first, last) in &layout.rows {
            if *table != subscription.definition.table {
                continue;
            }
            if row == 32 {
                return Err(Path::ManyRows);
            }
            let old = &changes[*first].old_value;
            let new = &changes[*last].new_value;
            // Not hit: the class default, evaluated once per class and row.
            let old_visible =
                !old.is_null() && member.class_default_memo(memo, old).ok_or(Path::Hit)?;
            let new_visible =
                !new.is_null() && member.class_default_memo(memo, new).ok_or(Path::Hit)?;
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
    // Entries of touched tables name their definition; the others are
    // `Skip` or `Ahead` of untouched tables.
    let states = shape
        .iter()
        .map(|entry| (entry.state, entry.definition.0.is_some()))
        .collect();
    let key = shape
        .into_iter()
        .filter(|entry| entry.definition.0.is_some())
        .collect();
    Ok(Shaped { key, states })
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

    /// The shared outcome, aligned with the shape's key (the
    /// representative's subscriptions, after the event).
    pub(crate) fn finish(
        mut self,
        subscriptions: &BTreeMap<String, ReplicaSubscription>,
        shape: &Shaped,
    ) -> Option<Outcome> {
        if !self.clean || subscriptions.len() != shape.states.len() {
            return None;
        }
        let subs = subscriptions
            .keys()
            .zip(&shape.states)
            .filter(|(_, (_, keyed))| *keyed)
            .map(|(id, (state, _))| match state {
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
    /// shape, when one was recorded for this event. `Miss`: not served (the
    /// caller runs the full path, which may record the outcome). On
    /// `Served(Ok)` the connection's frames for the event are in `batch`
    /// (except its watermark, which the caller adds as before). Counts the
    /// decision (`Path`) once.
    pub(crate) fn fan_out(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: &FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        native: bool,
    ) -> FanOut {
        let mut timer = gonvex_profiler::Timer::start();
        // A lagging connection's merged catch-up event (burst control): more
        // than one revision in one event.
        let merged = matches!(event, FeedEvent::Transaction { revision, .. }
            if crate::change_feed::first_revision(event).is_some_and(|first| first < *revision));
        let result = self.fan_out_inner(session, subscriptions, event, batcher, batch, native);
        if merged && !matches!(result, Err(Missed::Wait(_))) {
            MERGED.fetch_add(1, Ordering::Relaxed);
            if result.is_ok() {
                MERGED_SHARED.fetch_add(1, Ordering::Relaxed);
            }
        }
        match result {
            Ok((path, result)) => {
                timer.lap(
                    gonvex_profiler::Stage::DeliveryShared,
                    gonvex_profiler::Label::None,
                );
                count(path);
                FanOut::Served(result)
            }
            Err(Missed::Claimed(claim)) => {
                count(Path::Representative);
                FanOut::Miss {
                    record: true,
                    claim: Some(claim),
                }
            }
            Err(Missed::Wait(receiver)) => FanOut::Wait(receiver),
            Err(Missed::Path(path)) => {
                count(path);
                FanOut::Miss {
                    record: false,
                    claim: None,
                }
            }
        }
    }

    /// [`Self::fan_out`], waiting for another connection of the same shape
    /// when it is computing the outcome (never returns `Wait`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fan_out_waiting(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: &FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        native: bool,
    ) -> FanOut {
        await_indexes(subscriptions, event).await;
        for _ in 0..4 {
            match self.fan_out(session, subscriptions, event, batcher, batch, native) {
                FanOut::Wait(receiver) => wait_claim(receiver).await,
                other => return other,
            }
        }
        count(Path::Unclean);
        FanOut::Miss {
            record: false,
            claim: None,
        }
    }

    /// Served: (`SharedAdvance` / `SharedData`, result); else why not, or the
    /// claim on the shape's outcome, or a claim to wait for.
    #[allow(clippy::too_many_arguments)]
    fn fan_out_inner(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: &FeedEvent,
        batcher: &mut ReplicaBatcher,
        batch: &mut OutboundBatch,
        native: bool,
    ) -> Result<(Path, Result<(), DeliveryError>), Missed> {
        let FeedEvent::Transaction {
            database_epoch,
            revision,
            changes,
        } = event
        else {
            return Err(Missed::Path(Path::NotBatch));
        };
        if subscriptions.is_empty() {
            return Err(Missed::Path(Path::Empty));
        }
        if !enabled() || !batcher.enabled() {
            return Err(Missed::Path(Path::NotBatch));
        }
        let tenant = self.inner.delivery.tenant(&session.route.tenant_id);
        let share = tenant
            .events
            .share(database_epoch, *revision, changes)
            .ok_or(Missed::Path(Path::NoShare))?;
        let shaped = shape(
            subscriptions,
            &share,
            database_epoch,
            *revision,
            &session.route.database_url,
        )
        .map_err(Missed::Path)?;
        let states = &shaped.states;
        let shape = &shaped.key;
        let outcome = if shaped.decides() {
            let found = share.outcomes.lock().unwrap().get(shape).cloned();
            match found {
                Some(outcome) => Some(outcome),
                None => {
                    let mut claims = share.claims.lock().unwrap();
                    // Re-check under the claims lock: a claim is released
                    // after its outcome is recorded.
                    if let Some(outcome) = share.outcomes.lock().unwrap().get(shape).cloned() {
                        drop(claims);
                        Some(outcome)
                    } else {
                        return Err(match claims.get(shape) {
                            Some(ClaimState::Pending(receiver)) => Missed::Wait(receiver.clone()),
                            Some(ClaimState::Failed) => Missed::Path(Path::Unclean),
                            None => {
                                let (done, receiver) = tokio::sync::watch::channel(false);
                                claims.insert(shape.clone(), ClaimState::Pending(receiver));
                                drop(claims);
                                Missed::Claimed(Claim {
                                    share: Arc::clone(&share),
                                    shape: Some(shape.clone()),
                                    done,
                                })
                            }
                        });
                    }
                }
            }
        } else {
            // Nothing decides a row: cursors advance, no frame (the full path
            // would produce none either).
            None
        };
        if let Some(outcome) = &outcome {
            // Checks before any change: bounded windows stay in budget and
            // the socket's stream tokens are the default ones.
            let mut position = 0;
            for (index, (id, subscription)) in subscriptions.iter().enumerate() {
                if !states[index].1 {
                    continue;
                }
                let sub = &outcome.subs[position];
                position += 1;
                if let SubOutcome::Delta(delta) = sub {
                    if !batcher.default_binding(id, crate::delivery::stream_identity(subscription))
                    {
                        return Err(Missed::Path(Path::ServeRejected));
                    }
                    if subscription.bounded() {
                        let rows = subscription.window.rows as isize + delta.rows;
                        let bytes = subscription.window.bytes + delta.bytes;
                        if (subscription.definition.max_rows > 0
                            && rows > subscription.definition.max_rows as isize)
                            || (subscription.definition.max_bytes > 0
                                && bytes > subscription.definition.max_bytes)
                        {
                            return Err(Missed::Path(Path::ServeRejected));
                        }
                    }
                }
            }
        }
        HITS.fetch_add(1, Ordering::Relaxed);
        let Some(outcome) = outcome else {
            for (subscription, (state, _)) in subscriptions.values_mut().zip(states) {
                if *state == State::Skip {
                    subscription.cursor.revision = *revision;
                }
            }
            return Ok((Path::SharedAdvance, Ok(())));
        };
        DATA_HITS.fetch_add(1, Ordering::Relaxed);
        let Some(texts) = outcome.texts(native) else {
            return Ok((Path::SharedData, Err(DeliveryError)));
        };
        let tenant_id = session.route.tenant_id.as_str();
        if let Some(text) = &texts.transaction {
            batcher.push_shared(batch, tenant_id, subscriptions, Arc::clone(text));
        }
        // Window state first (mutable), then the frames (the batcher reads
        // the subscriptions).
        // (outcome position, subscription position, digest)
        let mut digests: Vec<(usize, usize, String)> = Vec::new();
        let mut at = 0;
        for (index, subscription) in subscriptions.values_mut().enumerate() {
            if !states[index].1 {
                if states[index].0 == State::Skip {
                    subscription.cursor.revision = *revision;
                }
                continue;
            }
            let position = at;
            at += 1;
            match &outcome.subs[position] {
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
                    digests.push((position, index, subscription.digest.digest()));
                }
            }
        }
        for (position, index, digest) in digests {
            let id = subscriptions
                .keys()
                .nth(index)
                .expect("subscription")
                .as_str();
            {
                {
                    let SubOutcome::Delta(delta) = &outcome.subs[position] else {
                        unreachable!()
                    };
                    let (item, prefix, suffix) =
                        texts.subs[position].as_ref().expect("delta text");
                    let mut tail = String::with_capacity(digest.len() + 32);
                    tail.push_str(",\"digest\":\"");
                    tail.push_str(&digest);
                    tail.push_str("\",\"truncated\":");
                    tail.push_str(if delta.truncated { "true" } else { "false" });
                    tail.push('}');
                    if !batcher.push_delta(
                        batch,
                        tenant_id,
                        subscriptions,
                        id,
                        Arc::clone(item),
                        tail,
                    ) {
                        return Ok((Path::SharedData, Err(DeliveryError)));
                    }
                    let mut ready = Vec::with_capacity(prefix.len() + digest.len() + suffix.len());
                    ready.extend_from_slice(prefix);
                    ready.extend_from_slice(digest.as_bytes());
                    ready.extend_from_slice(suffix);
                    if !batcher.push_ready(batch, tenant_id, subscriptions, id, Bytes::from(ready))
                    {
                        return Ok((Path::SharedData, Err(DeliveryError)));
                    }
                }
            }
        }
        Ok((Path::SharedData, Ok(())))
    }
}

/// Targeted wakeups (`crate::wake`): what this connection's replica
/// subscriptions could be affected by, from the same per-subscription state
/// the shape reads (concern tables, moving tables, parent ACL columns, the
/// compiled binding), as an immutable snapshot for the tenant dispatcher.
pub(crate) fn wake_interest(
    subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
    session: &TenantSession,
) -> crate::wake::Interest {
    let scope = session.route.database_url.as_str();
    let mut subs = Vec::with_capacity(subscriptions.len());
    for subscription in subscriptions.values_mut() {
        let keys = keys(subscription);
        let concern = mask::subscription_concern_tables(subscription);
        let plans: Vec<Arc<VisibilityPlan>> = std::iter::once(Arc::clone(&subscription.visibility))
            .chain(
                subscription
                    .resolved
                    .inherited
                    .values()
                    .map(|parent| Arc::clone(&parent.plan)),
            )
            .collect();
        // `derived_change_concerns` (own plan) and its ancestor form
        // (parents): derived sets bound to an index, with their source
        // tokens.
        let mut derived = Vec::new();
        let levels = std::iter::once((subscription.visibility.as_ref(), &subscription.resolved)).chain(
            subscription
                .resolved
                .inherited
                .values()
                .map(|parent| (parent.plan.as_ref(), parent.resolved.as_ref())),
        );
        for (plan, resolved) in levels {
            for (name, set) in &plan.sets {
                if set.kind == "derived" && resolved.indexes.contains_key(name) {
                    if let Some(source) = resolved.sets.get(&set.from) {
                        derived.push((set.table.clone(), set.key.clone(), Arc::new(source.clone())));
                    }
                }
            }
        }
        let (always, parents) = match &keys.shareable {
            Shareable::Plain => (false, None),
            Shareable::Inherited(inherited) => (false, Some(Arc::clone(&inherited.acl))),
            Shareable::No(_) => (true, None),
        };
        // Source rows by class alone: plain plans without shared indexes.
        let member = if matches!(keys.shareable, Shareable::Plain)
            && subscription.resolved.indexes.is_empty()
            && subscription.resolved.inherited.is_empty()
        {
            let handle = match subscription.mask.peek() {
                Some(handle) => handle.clone(),
                None => subscription
                    .wake_member
                    .get_or_insert_with(|| {
                        crate::visibility_class::member_visibility(
                            &subscription.visibility,
                            &subscription.resolved,
                            scope,
                        )
                    })
                    .clone(),
            };
            match handle {
                crate::visibility_class::MemberHandle::Compiled(member) => Some(member),
                crate::visibility_class::MemberHandle::Fallback => None,
            }
        } else {
            None
        };
        subs.push(crate::wake::SubInterest {
            table: subscription.definition.table.clone(),
            concern,
            moving: Arc::clone(&keys.moving),
            plans,
            derived,
            parents,
            member,
            args: subscription.args.clone(),
            definition: Arc::clone(&subscription.definition),
            always,
        });
    }
    crate::wake::Interest {
        account_id: session.identity.account.id.clone(),
        member_id: session.member.id.clone(),
        admission_revision: session.admission_revision,
        subs,
    }
}

/// Targeted wakeups: a cheap key of everything [`wake_interest`] reads that
/// can change while subscribed (the subscription set, definitions, fan-out
/// keys and the compiled bindings; a principal, grant or derived change
/// clears the binding, so it changes the key). Equal keys: the interest is
/// unchanged and need not be rebuilt.
pub(crate) fn wake_key(subscriptions: &BTreeMap<String, ReplicaSubscription>) -> u64 {
    let mut hasher = crate::visibility_class::FxHasher::default();
    let handle = |handle: Option<&crate::visibility_class::MemberHandle>| match handle {
        Some(crate::visibility_class::MemberHandle::Compiled(member)) => Arc::as_ptr(member) as usize,
        Some(crate::visibility_class::MemberHandle::Fallback) => 1,
        None => 0,
    };
    for (id, subscription) in subscriptions {
        id.hash(&mut hasher);
        (Arc::as_ptr(&subscription.definition) as usize).hash(&mut hasher);
        subscription
            .fanout
            .as_ref()
            .map_or(0, |keys| Arc::as_ptr(keys) as usize)
            .hash(&mut hasher);
        handle(subscription.mask.peek()).hash(&mut hasher);
        handle(subscription.wake_member.as_ref()).hash(&mut hasher);
    }
    hasher.finish()
}

/// Targeted wakeups (tests and benches): whether the tenant feed would skip
/// this connection for `event` (decide nothing, send nothing).
pub fn wake_skips(
    subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
    session: &TenantSession,
    event: &FeedEvent,
) -> bool {
    let FeedEvent::Transaction {
        revision, changes, ..
    } = event
    else {
        return false;
    };
    let interest = wake_interest(subscriptions, session);
    let mut memo = crate::visibility_class::EvalMemo::default();
    !interest.affected(*revision, changes, &mut memo)
}

/// Waits until every shared index bound into a subscription that `event`
/// reaches is at the event's revision (or will never be: poisoned, other
/// epoch, unmaintained), as the full path's `visibility::indexes_ready`
/// would before deciding. A parent index applier that has not applied the
/// event yet otherwise sends the connection down the full path
/// (`full.index_waiting`) only to wait there. No await when all are ready.
pub(crate) async fn await_indexes(
    subscriptions: &BTreeMap<String, ReplicaSubscription>,
    event: &FeedEvent,
) {
    let FeedEvent::Transaction {
        database_epoch,
        revision,
        ..
    } = event
    else {
        return;
    };
    fn pending<'a>(
        resolved: &'a ResolvedVisibility,
        revision: u64,
        epoch: &str,
        out: &mut Vec<&'a Arc<std::sync::RwLock<crate::visibility_index::ParentAclIndex>>>,
    ) {
        for index in resolved.indexes.values() {
            if crate::visibility_index::ready_now(index, revision, epoch).is_none() {
                out.push(index);
            }
        }
        for parent in resolved.inherited.values() {
            if crate::visibility_index::ready_now(&parent.index, revision, epoch).is_none() {
                out.push(&parent.index);
            }
            pending(&parent.resolved, revision, epoch, out);
        }
    }
    let mut waiting = Vec::new();
    for subscription in subscriptions.values() {
        if *revision > subscription.cursor.revision && subscription.visibility.version == Some(2) {
            pending(&subscription.resolved, *revision, database_epoch, &mut waiting);
        }
    }
    for index in waiting {
        crate::visibility_index::ready_at(index, *revision, database_epoch).await;
    }
}

/// Why [`Runtime::fan_out_inner`] did not serve a connection.
pub(crate) enum Missed {
    Path(Path),
    Claimed(Claim),
    Wait(tokio::sync::watch::Receiver<bool>),
}

/// Result of [`Runtime::fan_out`].
pub(crate) enum FanOut {
    /// The connection's frames are in its batch.
    Served(Result<(), DeliveryError>),
    /// Not served; `record`: the full path should record the outcome. Hold
    /// `claim` until the full path has finished (it wakes the connections
    /// waiting for this shape's outcome).
    Miss {
        record: bool,
        claim: Option<Claim>,
    },
    /// Another connection of this shape is computing its outcome: wait for
    /// it (`wait_claim`), then call `fan_out` again.
    Wait(tokio::sync::watch::Receiver<bool>),
}

/// A shape's claim in its event share (`EventShare::claims`).
pub(crate) enum ClaimState {
    Pending(tokio::sync::watch::Receiver<bool>),
    Failed,
}

/// Held by the connection computing a shape's outcome. Dropping it (the full
/// path finished, failed or was cancelled) wakes the waiters; when no outcome
/// was recorded the shape is marked failed, so later connections of it take
/// their own path without waiting.
pub(crate) struct Claim {
    share: Arc<EventShare>,
    shape: Option<Shape>,
    done: tokio::sync::watch::Sender<bool>,
}

impl Drop for Claim {
    fn drop(&mut self) {
        if let Some(shape) = self.shape.take() {
            let recorded = self.share.outcomes.lock().unwrap().contains_key(&shape);
            let mut claims = self.share.claims.lock().unwrap();
            if recorded {
                claims.remove(&shape);
            } else {
                claims.insert(shape, ClaimState::Failed);
            }
        }
        let _ = self.done.send(true);
    }
}

/// Waits until the claim behind `receiver` is released.
pub(crate) async fn wait_claim(mut receiver: tokio::sync::watch::Receiver<bool>) {
    WAITS.fetch_add(1, Ordering::Relaxed);
    let _ = receiver.wait_for(|done| *done).await;
}

/// Records `outcome` for `shape` (first writer wins).
pub(crate) fn record(share: &EventShare, shape: Shape, outcome: Outcome) {
    if let std::collections::hash_map::Entry::Vacant(entry) =
        share.outcomes.lock().unwrap().entry(shape)
    {
        RECORDED.fetch_add(1, Ordering::Relaxed);
        entry.insert(Arc::new(outcome));
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
        let delivery =
            |_| ReplicaError::InvalidIdentifier("replica batch delivery failed".to_owned());
        let (record, claim) = match self
            .fan_out_waiting(session, subscriptions, &event, batcher, batch, native)
            .await
        {
            FanOut::Served(result) => return result.map(|_| Vec::new()).map_err(delivery),
            FanOut::Miss { record, claim } => (record, claim),
            FanOut::Wait(_) => unreachable!("fan_out_waiting waits"),
        };
        if claim.is_some() {
            // As the offloaded full path of the connection loop does: other
            // connections run (and wait for this claim) meanwhile.
            tokio::task::yield_now().await;
        }
        let messages = self
            .apply_feed_event_with(session, subscriptions, event, record)
            .await?;
        drop(claim);
        let mut rest = Vec::new();
        for message in messages {
            if let Some(message) = batcher
                .push(
                    batch,
                    hub,
                    &session.route.tenant_id,
                    native,
                    subscriptions,
                    message,
                )
                .map_err(delivery)?
            {
                rest.push(message);
            }
        }
        Ok(rest)
    }
}
