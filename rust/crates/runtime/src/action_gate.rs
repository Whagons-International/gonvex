//! Process-wide Action admission and the single Action-outbox drainer.
//!
//! An Action holds a complete-bundle isolate (tens of MB) for as long as it
//! runs, including while it awaits `fetch`. On 1 vCPU / 2 GB, memory binds
//! before CPU, so root Actions are admitted through one priority permit
//! (`GONVEX_ACTION_CONCURRENCY`, default 1 when `available_parallelism() <= 2`,
//! else 2): direct user Actions first, then outbox deliveries, then
//! scheduler-started Actions. FIFO within a class. A waiting Action holds no
//! isolate. Nested Actions (an Action's `ctx.functions` calls) run inside
//! their root's permit.
//!
//! The outbox drainer is one task on the scheduled-work thread. Commits that
//! wrote the outbox and the periodic sweep only queue the tenant (deduplicated)
//! and wake it. For each tenant it promotes due scheduled-job rows (cheap
//! writes, no permit), then takes the Action permit and only then claims one
//! Action, so a backlog stays in SQLite rather than in memory and a claimed
//! row never waits for a permit while its lease runs down. After one Action
//! the tenant goes to the back of the queue, so one tenant's backlog cannot
//! starve the others.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{oneshot, Notify};

use gonvex_store::TenantRoute;

use gonvex_store::TenantSession;

/// Priority classes, highest first.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActionClass {
    User = 0,
    Outbox = 1,
    Cron = 2,
}

const CLASSES: usize = 3;

pub(crate) fn default_action_concurrency() -> usize {
    let cpus = std::thread::available_parallelism().map_or(1, |cpus| cpus.get());
    if cpus <= 2 {
        1
    } else {
        2
    }
}

struct GateState {
    available: usize,
    waiters: [VecDeque<oneshot::Sender<()>>; CLASSES],
}

pub(crate) struct ActionGate {
    capacity: usize,
    state: Mutex<GateState>,
    running: [AtomicUsize; CLASSES],
    admitted: [AtomicUsize; CLASSES],
}

impl ActionGate {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        let capacity = capacity.max(1);
        Arc::new(Self {
            capacity,
            state: Mutex::new(GateState {
                available: capacity,
                waiters: Default::default(),
            }),
            running: Default::default(),
            admitted: Default::default(),
        })
    }

    pub(crate) fn from_env() -> Arc<Self> {
        let capacity = std::env::var("GONVEX_ACTION_CONCURRENCY")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(default_action_concurrency);
        Self::new(capacity)
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn admit(self: &Arc<Self>, class: ActionClass) -> ActionPermit {
        self.running[class as usize].fetch_add(1, Ordering::Relaxed);
        self.admitted[class as usize].fetch_add(1, Ordering::Relaxed);
        gonvex_profiler::gauge(gonvex_profiler::GaugeId::ActionRunning).add(1);
        ActionPermit {
            gate: Arc::clone(self),
            class,
        }
    }

    /// Waits for a permit. Cancel-safe: a permit handed to a waiter that is
    /// dropped goes to the next waiter.
    pub(crate) async fn acquire(self: &Arc<Self>, class: ActionClass) -> ActionPermit {
        let receiver = {
            let mut state = self.lock();
            if state.available > 0 && state.waiters.iter().all(VecDeque::is_empty) {
                state.available -= 1;
                drop(state);
                return self.admit(class);
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters[class as usize].push_back(sender);
            receiver
        };
        let _waiting = gonvex_profiler::gauge(gonvex_profiler::GaugeId::ActionWaiting).guard();
        let mut wait = Waiting {
            gate: self,
            receiver: Some(receiver),
        };
        let receiver = wait.receiver.as_mut().expect("receiver until handed over");
        // The sender is only dropped after a successful send or by `release`
        // skipping a closed receiver, so an error cannot happen while waiting.
        let _ = receiver.await;
        wait.receiver = None;
        self.admit(class)
    }

    /// A permit if one is free and nobody waits.
    #[cfg(test)]
    pub(crate) fn try_acquire(self: &Arc<Self>, class: ActionClass) -> Option<ActionPermit> {
        let mut state = self.lock();
        if state.available > 0 && state.waiters.iter().all(VecDeque::is_empty) {
            state.available -= 1;
            drop(state);
            return Some(self.admit(class));
        }
        None
    }

    /// Hands the permit to the highest-priority live waiter, else frees it.
    fn release(&self) {
        let mut state = self.lock();
        for class in 0..CLASSES {
            while let Some(waiter) = state.waiters[class].pop_front() {
                if waiter.send(()).is_ok() {
                    return;
                }
            }
        }
        state.available = (state.available + 1).min(self.capacity);
    }

    /// `(running, waiting, admitted since start)` per class, User first.
    pub(crate) fn snapshot(&self) -> [(usize, usize, usize); CLASSES] {
        let state = self.lock();
        std::array::from_fn(|class| {
            (
                self.running[class].load(Ordering::Relaxed),
                state.waiters[class].len(),
                self.admitted[class].load(Ordering::Relaxed),
            )
        })
    }
}

struct Waiting<'a> {
    gate: &'a ActionGate,
    receiver: Option<oneshot::Receiver<()>>,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if let Some(mut receiver) = self.receiver.take() {
            receiver.close();
            // Handed over after all: pass it on.
            if receiver.try_recv().is_ok() {
                self.gate.release();
            }
        }
    }
}

/// One admitted root Action. Released on drop.
pub(crate) struct ActionPermit {
    gate: Arc<ActionGate>,
    class: ActionClass,
}

impl ActionPermit {
    #[cfg(test)]
    pub(crate) fn class(&self) -> ActionClass {
        self.class
    }
}

impl std::fmt::Debug for ActionPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActionPermit")
            .field("class", &self.class)
            .finish()
    }
}

impl Drop for ActionPermit {
    fn drop(&mut self) {
        self.gate.running[self.class as usize].fetch_sub(1, Ordering::Relaxed);
        gonvex_profiler::gauge(gonvex_profiler::GaugeId::ActionRunning).add(-1);
        self.gate.release();
    }
}

/// Tenants with outbox work, in drain order, deduplicated by database.
#[derive(Default)]
pub(crate) struct OutboxQueue {
    state: Mutex<QueueState>,
    wake: Notify,
    started: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct QueueState {
    order: VecDeque<TenantSession>,
    queued: HashSet<String>,
}

impl OutboxQueue {
    fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Queues a tenant at the back unless it is already queued. Returns
    /// whether it was added.
    pub(crate) fn push(&self, session: TenantSession) -> bool {
        let added = {
            let mut state = self.lock();
            if state.queued.insert(key(&session.route)) {
                state.order.push_back(session);
                true
            } else {
                false
            }
        };
        if added {
            self.wake.notify_one();
        }
        added
    }

    pub(crate) fn pop(&self) -> Option<TenantSession> {
        let mut state = self.lock();
        let session = state.order.pop_front()?;
        state.queued.remove(&key(&session.route));
        Some(session)
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().order.len()
    }

    pub(crate) async fn notified(&self) {
        self.wake.notified().await
    }

    /// True exactly once: the caller starts the drainer.
    pub(crate) fn claim_start(&self) -> bool {
        !self.started.swap(true, Ordering::AcqRel)
    }
}

fn key(route: &TenantRoute) -> String {
    route.database_url.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn pending(
        gate: &Arc<ActionGate>,
        class: ActionClass,
    ) -> tokio::task::JoinHandle<ActionClass> {
        let gate = Arc::clone(gate);
        let task = tokio::spawn(async move {
            let permit = gate.acquire(class).await;
            let class = permit.class();
            tokio::time::sleep(Duration::from_millis(5)).await;
            drop(permit);
            class
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        task
    }

    #[tokio::test]
    async fn one_action_at_a_time_in_priority_order_fifo_within_a_class() {
        let gate = ActionGate::new(1);
        let held = gate.acquire(ActionClass::Cron).await;
        assert!(gate.try_acquire(ActionClass::User).is_none(), "capacity 1");
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for (label, class) in [
            ("cron-1", ActionClass::Cron),
            ("outbox-1", ActionClass::Outbox),
            ("user-1", ActionClass::User),
            ("outbox-2", ActionClass::Outbox),
            ("user-2", ActionClass::User),
        ] {
            let gate = Arc::clone(&gate);
            let order = Arc::clone(&order);
            tasks.push(tokio::spawn(async move {
                let permit = gate.acquire(class).await;
                assert_eq!(gate.snapshot().iter().map(|c| c.0).sum::<usize>(), 1);
                order.lock().unwrap().push(label);
                tokio::time::sleep(Duration::from_millis(2)).await;
                drop(permit);
            }));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let snapshot = gate.snapshot();
        assert_eq!(snapshot[ActionClass::User as usize].1, 2);
        assert_eq!(snapshot[ActionClass::Outbox as usize].1, 2);
        assert_eq!(snapshot[ActionClass::Cron as usize], (1, 1, 1));
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            ["user-1", "user-2", "outbox-1", "outbox-2", "cron-1"]
        );
        assert!(
            gate.try_acquire(ActionClass::Cron).is_some(),
            "permit freed"
        );
    }

    #[tokio::test]
    async fn a_cancelled_waiter_does_not_lose_the_permit() {
        let gate = ActionGate::new(1);
        let held = gate.acquire(ActionClass::User).await;
        let cancelled = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                let _permit = gate.acquire(ActionClass::User).await;
                unreachable!("aborted before admission");
            })
        };
        let later = pending(&gate, ActionClass::Cron).await;
        cancelled.abort();
        let _ = cancelled.await;
        drop(held);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), later)
                .await
                .expect("the next waiter is admitted")
                .unwrap(),
            ActionClass::Cron
        );
        // Handed to a waiter whose future is dropped right after the send.
        let held = gate.acquire(ActionClass::User).await;
        let mut waiter = Box::pin(gate.acquire(ActionClass::Outbox));
        assert!(futures_util::poll!(waiter.as_mut()).is_pending());
        drop(held); // sends to `waiter`
        drop(waiter); // never polled again
        assert!(gate.try_acquire(ActionClass::User).is_some());
    }

    #[tokio::test]
    async fn capacity_two_admits_two() {
        let gate = ActionGate::new(2);
        let a = gate.acquire(ActionClass::Cron).await;
        let b = gate.acquire(ActionClass::Cron).await;
        assert!(gate.try_acquire(ActionClass::User).is_none());
        drop(a);
        assert!(gate.try_acquire(ActionClass::User).is_some());
        drop(b);
    }
}
