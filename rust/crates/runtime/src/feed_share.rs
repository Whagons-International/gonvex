//! Delivery v3: work on one committed transaction that every connection of a
//! tenant shares. See `docs/performance/delivery-v3.md`.
//!
//! A feed event's row images are one `Arc<Vec<LogChange>>` per broadcast, so
//! their addresses are the same in every connection's copy of the event. An
//! [`EventShare`] holds that `Arc` (the addresses stay valid while it lives)
//! and, keyed by those addresses:
//!
//! - one [`EvalMemo`]: each change row's ACL columns are decoded once and each
//!   visibility class's default is evaluated once for the whole tenant, not
//!   once per connection;
//! - per-class row decisions: `replica::decide_row` for a (visibility class,
//!   collection, arguments, row) is computed once and reused by every
//!   subscription of that class that is not *hit* by the row (no member atom
//!   of its own is true on the old or new image) and whose window is complete.
//!   Those are exactly the subscriptions whose decision is a function of the
//!   class: the subscription's own decision would be identical (`decide_row`
//!   is pure in definition, args, plan, principal, window and images, and a
//!   non-hit member's compiled visibility is its class default).
//!
//! Plans that inherit visibility from parent rows never use the share (their
//! answer also depends on the parent index at evaluation time).
//!
//! Safety: keys use the process-local class id (`VisibilityClass::id`, never
//! reused), the collection definition's address, the exact argument JSON and
//! the row id. A connection only reuses a decision computed for its own class;
//! hit members, principal changes, truncated windows, affected-row reads and
//! fallback (interpreted) plans decide alone, as before.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::change_feed::LogChange;
use crate::replica::RowDecision;
use crate::visibility_class::EvalMemo;

static SHARED_DECISIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Row decisions reused from another subscription of the same class
/// (process total; tests and benchmarks).
pub fn shared_decisions() -> u64 {
    SHARED_DECISIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Events kept per tenant (connections lag each other by a few events at most;
/// a miss only means deciding alone).
const KEEP_EVENTS: usize = 4;

static OVERRIDE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Forces sharing on or off (`None`: environment). Tests and benchmarks
/// compare shared and per-connection decisions in one process.
#[doc(hidden)]
pub fn set_sharing(enabled: Option<bool>) {
    let value = match enabled {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    };
    OVERRIDE.store(value, std::sync::atomic::Ordering::Relaxed);
}

/// `GONVEX_REPLICA_CLASS_SHARING=0` turns sharing off (each connection decides
/// alone, the delivery v2 behaviour).
pub fn enabled() -> bool {
    match OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        1 => return true,
        2 => return false,
        _ => {}
    }
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("GONVEX_REPLICA_CLASS_SHARING")
            .map(|value| value.trim() != "0")
            .unwrap_or(true)
    })
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct DecisionKey {
    pub class: u64,
    pub definition: usize,
    pub args: Arc<str>,
    pub table: Arc<str>,
    pub row: Arc<str>,
}

/// Shared work for one committed transaction of one tenant database.
pub struct EventShare {
    epoch: String,
    revision: u64,
    changes: Arc<Vec<LogChange>>,
    pub(crate) memo: Mutex<EvalMemo>,
    decisions: Mutex<HashMap<DecisionKey, Arc<RowDecision>>>,
    /// Shared copies of change images (parent before-image overlays), by
    /// address in `changes`.
    images: Mutex<HashMap<usize, Arc<serde_json::Value>>>,
    /// Delivery v4: the event's tables and collapsed rows, and the shared
    /// outcome per connection shape (`replica_fanout`).
    pub(crate) layout: std::sync::OnceLock<crate::replica::fanout::Layout>,
    pub(crate) outcomes:
        Mutex<HashMap<crate::replica::fanout::Shape, Arc<crate::replica::fanout::Outcome>>>,
}

impl EventShare {
    pub(crate) fn decision(&self, key: &DecisionKey) -> Option<Arc<RowDecision>> {
        let found = self.decisions.lock().unwrap().get(key).cloned();
        if found.is_some() {
            SHARED_DECISIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        found
    }

    pub(crate) fn changes(&self) -> &Arc<Vec<LogChange>> {
        &self.changes
    }

    pub(crate) fn insert(&self, key: DecisionKey, decision: Arc<RowDecision>) {
        self.decisions.lock().unwrap().insert(key, decision);
    }

    /// One shared copy of `image`, a row image of this event's changes.
    pub(crate) fn image(&self, image: &serde_json::Value) -> Arc<serde_json::Value> {
        let address = image as *const serde_json::Value as usize;
        Arc::clone(
            self.images
                .lock()
                .unwrap()
                .entry(address)
                .or_insert_with(|| Arc::new(image.clone())),
        )
    }

    /// Decisions shared so far (tests and benchmarks).
    pub fn decisions(&self) -> usize {
        self.decisions.lock().unwrap().len()
    }
}

/// Recent event shares of one tenant.
#[derive(Default)]
pub struct EventShares {
    recent: Mutex<VecDeque<Arc<EventShare>>>,
    hits: std::sync::atomic::AtomicU64,
}

impl EventShares {
    /// The share of this event (created by its first connection). Identified
    /// by epoch, revision and the address of the broadcast's change list.
    pub(crate) fn share(
        &self,
        epoch: &str,
        revision: u64,
        changes: &Arc<Vec<LogChange>>,
    ) -> Option<Arc<EventShare>> {
        if !enabled() {
            return None;
        }
        let mut recent = self.recent.lock().unwrap();
        if let Some(share) = recent.iter().find(|share| {
            share.revision == revision
                && share.epoch == epoch
                && Arc::ptr_eq(&share.changes, changes)
        }) {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Some(Arc::clone(share));
        }
        let share = Arc::new(EventShare {
            epoch: epoch.to_owned(),
            revision,
            changes: Arc::clone(changes),
            memo: Mutex::default(),
            decisions: Mutex::default(),
            images: Mutex::default(),
            layout: Default::default(),
            outcomes: Mutex::default(),
        });
        recent.push_back(Arc::clone(&share));
        while recent.len() > KEEP_EVENTS {
            recent.pop_front();
        }
        Some(share)
    }

    /// How often a connection found the event already shared.
    pub fn hits(&self) -> u64 {
        self.hits.load(std::sync::atomic::Ordering::Relaxed)
    }
}
