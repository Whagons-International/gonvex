//! Targeted feed wakeups: skip no-op wakeups (docs/performance/delivery-coverage.md §9).
//!
//! Every connection of a tenant used to wake for every transaction of the
//! tenant and run its feed arm to find that, most of the time, nothing
//! concerned it (61 % of wakeups at 500 users). Here the tenant feed decides
//! once per transaction, *before* the transaction's signal is broadcast,
//! which idle connections it could affect ([`WakeRegistry::dispatch`]), marks
//! the revision on their [`Slot`] and wakes only those. An idle connection
//! ("targeted mode": batch delivery, replica subscriptions only, no call
//! waiting for its watermark, no snapshot or open in flight, not catching up)
//! waits on its slot instead of the broadcast ring; when it wakes it takes the
//! queued signals in order, runs the feed arm for marked revisions and skips
//! the others without deciding anything (its cursors stay older, which is
//! always safe: a resume replays the log). Skipped revisions are acknowledged
//! lazily: one tenant timer per idle delay (the delay a watermark-only batch
//! already waited) wakes the connections that skipped something to drain and
//! push their watermark.
//!
//! The decision is conservative ([`Interest::affected`]): a transaction is
//! skipped for a connection only when, for every subscription, every changed
//! row is provably irrelevant: its table is not one the subscription's
//! visibility reads; or it cannot move the member's principal, grants or
//! derived sets (the mask step's own checks, member-independent or from the
//! member's derived source tokens); or it is a parent update of no
//! visibility column; or it is a source row the collection's own filters or
//! the member's compiled class reject in both images. Anything else
//! (inherited source rows, plans with shared indexes, uncompiled members,
//! legacy plans, grant rows, membership changes) wakes the connection. Any
//! change of the connection's interest, and entering targeted mode, treats
//! every revision already dispatched as marked.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use serde_json::Value;

use crate::change_feed::LogChange;
use crate::fast_hash::{FastMap, FastSet};
use crate::visibility::VisibilityPlan;
use crate::visibility_class::{EvalMemo, MemberVisibility};

static DISPATCHED: AtomicU64 = AtomicU64::new(0);
static TARGETED: AtomicU64 = AtomicU64::new(0);
static SKIPPED_MARKS: AtomicU64 = AtomicU64::new(0);
static SKIPPED: AtomicU64 = AtomicU64::new(0);
static FLUSHES: AtomicU64 = AtomicU64::new(0);
static FLUSH_WAKES: AtomicU64 = AtomicU64::new(0);
static ENTERS: AtomicU64 = AtomicU64::new(0);
static DISPATCH_NS: AtomicU64 = AtomicU64::new(0);

/// `GONVEX_FEED_TARGETED_WAKE=0` turns targeted wakeups off (every
/// connection waits on the broadcast ring as before).
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("GONVEX_FEED_TARGETED_WAKE")
            .map(|value| value.trim() != "0")
            .unwrap_or(true)
    })
}

/// `/dev/profile` `deliveryCoverage.wake`.
pub fn metrics() -> Value {
    serde_json::json!({
        "enabled": enabled(),
        "dispatched": DISPATCHED.load(Ordering::Relaxed),
        "targeted": TARGETED.load(Ordering::Relaxed),
        "notTargeted": SKIPPED_MARKS.load(Ordering::Relaxed),
        "skipped": SKIPPED.load(Ordering::Relaxed),
        "flushes": FLUSHES.load(Ordering::Relaxed),
        "flushWakes": FLUSH_WAKES.load(Ordering::Relaxed),
        "enters": ENTERS.load(Ordering::Relaxed),
        "dispatchNs": DISPATCH_NS.load(Ordering::Relaxed),
    })
}

/// Counts one transaction a connection skipped without its feed arm.
pub(crate) fn count_skip() {
    SKIPPED.fetch_add(1, Ordering::Relaxed);
}

/// What one connection's replica subscriptions could be affected by.
pub(crate) struct Interest {
    pub(crate) account_id: String,
    pub(crate) member_id: String,
    pub(crate) admission_revision: u64,
    pub(crate) subs: Vec<SubInterest>,
}

pub(crate) struct SubInterest {
    pub(crate) table: String,
    /// Every table its visibility reads (`replica_mask::concern_tables`).
    pub(crate) concern: Arc<FastSet<String>>,
    /// Tables whose change may move its principal / grants / derived sets.
    pub(crate) moving: Arc<FastMap<String, crate::replica::MovingChecksPub>>,
    /// Its plan and its parents' plans.
    pub(crate) plans: Vec<Arc<VisibilityPlan>>,
    /// Derived sets of its plan and parents: (table, key column, source
    /// tokens), as `derived_change_concerns` reads them.
    pub(crate) derived: Vec<(String, String, Arc<BTreeSet<String>>)>,
    /// Inherited plan: parent table → ACL index columns.
    pub(crate) parents: Option<Arc<BTreeMap<String, BTreeSet<String>>>>,
    /// Compiled binding when its source rows are decided by class alone
    /// (plain version 2 plan, no shared index); None: any source row wakes.
    pub(crate) member: Option<Arc<MemberVisibility>>,
    pub(crate) args: Value,
    pub(crate) definition: Arc<crate::modules::ReplicaCollectionDefinition>,
    /// Legacy or unfollowed plan: any transaction wakes.
    pub(crate) always: bool,
}

impl Interest {
    /// Whether the transaction could change anything this connection sees
    /// (false: its feed arm would decide nothing and send nothing).
    pub(crate) fn affected(&self, revision: u64, changes: &[LogChange], memo: &mut EvalMemo) -> bool {
        if revision > self.admission_revision && self.membership_affects(changes) {
            return true;
        }
        self.subs.iter().any(|sub| sub.affected(changes, memo))
    }

    /// `lib.rs` `membership_affects` for this session.
    fn membership_affects(&self, changes: &[LogChange]) -> bool {
        changes
            .iter()
            .filter(|change| {
                change.table == "members"
                    && change.changed_columns.iter().any(|column| {
                        matches!(
                            column.as_str(),
                            "account_id" | "status" | "role" | "permissions" | "membership_revision"
                        )
                    })
            })
            .any(|change| {
                [&change.old_value, &change.new_value].iter().any(|row| {
                    row.get("account_id").and_then(Value::as_str) == Some(self.account_id.as_str())
                        || row.get("id").and_then(Value::as_str) == Some(self.member_id.as_str())
                })
            })
    }
}

impl SubInterest {
    fn affected(&self, changes: &[LogChange], memo: &mut EvalMemo) -> bool {
        if self.always {
            return true;
        }
        for change in changes {
            let table = change.table.as_str();
            if !self.concern.contains(table) && table != self.table {
                continue;
            }
            let (old, new) = (&*change.old_value, &*change.new_value);
            if let Some(checks) = self.moving.get(table) {
                if checks.grant {
                    return true;
                }
                if checks.derived && self.derived_touched(table, &change.row_id, old, new) {
                    return true;
                }
                if checks.principal
                    && self.plans.iter().any(|plan| {
                        plan.principal_change_possible(table, &change.operation, &change.changed_columns)
                    })
                {
                    return true;
                }
            }
            if let Some(acl) = self.parents.as_ref().and_then(|parents| parents.get(table)) {
                // A parent row: only a visibility-column update or a delete
                // can flip its children.
                if old.is_null() {
                    // Created: no children beyond this transaction's rows.
                } else if new.is_null()
                    || change.changed_columns.is_empty()
                    || change.changed_columns.iter().any(|column| acl.contains(column))
                {
                    return true;
                }
            }
            if table != self.table {
                continue;
            }
            let admits = |row: &Value| {
                !row.is_null() && crate::replica::collection_filters_admit(row, &self.args, &self.definition)
            };
            if !admits(old) && !admits(new) {
                // The collection's own filters reject both images.
                continue;
            }
            let Some(member) = &self.member else {
                return true;
            };
            for row in [old, new] {
                if admits(row) && member.class_default_memo(memo, row) != Some(false) {
                    // Visible by its class, or a member atom hits it.
                    return true;
                }
            }
        }
        false
    }

    /// `derived_change_concerns` (and the ancestor form) from the member's
    /// source tokens.
    fn derived_touched(&self, table: &str, row_id: &str, old: &Value, new: &Value) -> bool {
        self.derived.iter().any(|(derived_table, key, tokens)| {
            derived_table == table
                && ([old, new].into_iter().any(|row| {
                    row.get(key)
                        .map(|value| crate::visibility_mask::scalar(Some(value)))
                        .is_some_and(|key| tokens.contains(&key))
                }) || tokens.contains(row_id))
        })
    }
}

/// One connection's wake slot.
pub struct Slot {
    notify: tokio::sync::Notify,
    state: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    /// Some: targeted mode (the dispatcher decides for it).
    interest: Option<Arc<Interest>>,
    /// Revisions dispatched to it, ascending, not yet taken.
    marked: VecDeque<u64>,
    /// Every revision at or below is treated as marked (dispatched before
    /// its current interest was known).
    full_until: u64,
    /// It has skipped (unmarked) signals queued since its last flush.
    skipped: bool,
}

const MAX_MARKS: usize = 1024;

impl Slot {
    /// Waits until the dispatcher or a flush wakes this connection.
    pub(crate) async fn notified(&self) {
        self.notify.notified().await
    }

    /// Whether the connection must run its feed arm for `revision` (taken in
    /// order). Revisions it was not marked for are skipped.
    pub(crate) fn take(&self, revision: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if revision <= state.full_until {
            while state.marked.front().is_some_and(|marked| *marked <= revision) {
                state.marked.pop_front();
            }
            return true;
        }
        while state.marked.front().is_some_and(|marked| *marked < revision) {
            state.marked.pop_front();
        }
        if state.marked.front() == Some(&revision) {
            state.marked.pop_front();
            return true;
        }
        false
    }

    pub(crate) fn notify_one(&self) {
        self.notify.notify_one();
    }

    /// Whether it is in targeted mode.
    pub(crate) fn active(&self) -> bool {
        self.state.lock().unwrap().interest.is_some()
    }
}

/// The wake slots of one tenant feed.
#[derive(Default)]
pub(crate) struct WakeRegistry {
    state: Mutex<RegistryState>,
    flush_scheduled: AtomicBool,
}

#[derive(Default)]
struct RegistryState {
    slots: Vec<Weak<Slot>>,
    /// The last revision dispatched.
    latest: u64,
}

impl WakeRegistry {
    pub(crate) fn slot(&self) -> Arc<Slot> {
        let slot = Arc::new(Slot {
            notify: tokio::sync::Notify::new(),
            state: Mutex::default(),
        });
        let mut state = self.state.lock().unwrap();
        state.slots.retain(|slot| slot.strong_count() > 0);
        state.slots.push(Arc::downgrade(&slot));
        slot
    }

    /// Targeted mode with `interest` (replacing any earlier one). Atomic with
    /// dispatch: every revision dispatched so far counts as marked.
    pub(crate) fn enter(&self, slot: &Slot, interest: Arc<Interest>) {
        let registry = self.state.lock().unwrap();
        let mut state = slot.state.lock().unwrap();
        if state.interest.is_none() {
            ENTERS.fetch_add(1, Ordering::Relaxed);
        }
        state.interest = Some(interest);
        state.full_until = state.full_until.max(registry.latest);
        state.marked.clear();
    }

    /// Back to the broadcast ring (every signal taken and decided as before).
    pub(crate) fn leave(&self, slot: &Slot) {
        let _registry = self.state.lock().unwrap();
        let mut state = slot.state.lock().unwrap();
        state.interest = None;
        state.marked.clear();
    }

    /// Before a transaction's signal is broadcast: marks the slots it could
    /// affect and returns them (to notify after the broadcast).
    pub(crate) fn dispatch(
        self: &Arc<Self>,
        revision: u64,
        changes: &[LogChange],
    ) -> Vec<Arc<Slot>> {
        let started = std::time::Instant::now();
        let mut registry = self.state.lock().unwrap();
        registry.latest = registry.latest.max(revision);
        let mut memo = EvalMemo::default();
        let mut wake = Vec::new();
        let mut skipped = false;
        registry.slots.retain(|slot| slot.strong_count() > 0);
        for slot in registry.slots.iter().filter_map(Weak::upgrade) {
            let mut state = slot.state.lock().unwrap();
            let Some(interest) = state.interest.clone() else {
                continue;
            };
            DISPATCHED.fetch_add(1, Ordering::Relaxed);
            if interest.affected(revision, changes, &mut memo) {
                TARGETED.fetch_add(1, Ordering::Relaxed);
                state.marked.push_back(revision);
                if state.marked.len() > MAX_MARKS {
                    state.marked.clear();
                    state.full_until = revision;
                }
                drop(state);
                wake.push(slot);
            } else {
                SKIPPED_MARKS.fetch_add(1, Ordering::Relaxed);
                state.skipped = true;
                skipped = true;
            }
        }
        drop(registry);
        if skipped {
            self.schedule_flush();
        }
        DISPATCH_NS.fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        wake
    }

    /// Wakes every slot (a reset, or the feed closing).
    pub(crate) fn wake_all(&self) {
        let registry = self.state.lock().unwrap();
        for slot in registry.slots.iter().filter_map(Weak::upgrade) {
            slot.notify.notify_one();
        }
    }

    /// One timer per tenant: after the batch idle delay (the delay a
    /// watermark-only batch waits anyway), wake the connections that skipped
    /// transactions so they take the signals and push their watermark.
    fn schedule_flush(self: &Arc<Self>) {
        if self.flush_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.flush_scheduled.store(false, Ordering::Release);
            self.flush();
            return;
        };
        let registry = Arc::downgrade(self);
        handle.spawn(async move {
            tokio::time::sleep(crate::delivery::BatchConfig::from_env().idle).await;
            if let Some(registry) = registry.upgrade() {
                registry.flush_scheduled.store(false, Ordering::Release);
                registry.flush();
            }
        });
    }

    fn flush(&self) {
        FLUSHES.fetch_add(1, Ordering::Relaxed);
        let registry = self.state.lock().unwrap();
        for slot in registry.slots.iter().filter_map(Weak::upgrade) {
            let mut state = slot.state.lock().unwrap();
            if std::mem::take(&mut state.skipped) {
                drop(state);
                FLUSH_WAKES.fetch_add(1, Ordering::Relaxed);
                slot.notify.notify_one();
            }
        }
    }
}
