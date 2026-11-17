//! Priority admission in front of an isolate pool.
//!
//! An isolate pool has as many permits as isolates (one on a 1-vCPU host).
//! A FIFO semaphore let a herd of cron and one-shot jobs queue ahead of user
//! Reducers, which then spent their whole invocation deadline (10 s, counted
//! from arrival) waiting for the isolate. Waiters here are served by rank:
//! arrival time plus a lane delay, user calls first, then outbox and other
//! system work, then scheduled work. The delay bounds starvation: a scheduled
//! call that has waited longer than its lane delay ranks ahead of user calls
//! that arrived after it, so a saturated server still makes progress on its
//! schedule. See docs/performance/cron-writer.md.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gonvex_module_runtime::{InvocationChannel, InvocationContext};
use tokio::sync::oneshot;

/// Who is waiting for an isolate. Lower lanes are served first.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Lane {
    /// UI, API and agent calls: someone is waiting on the result.
    User,
    /// Outbox Actions and the calls they make (`System` channel).
    System,
    /// Crons and one-shot `runAfter`/`runAt` jobs, and the calls they make.
    Scheduled,
}

impl Lane {
    /// The lane of an invocation, from its channel and its root channel, so a
    /// Reducer that a scheduled Action calls stays scheduled work.
    pub fn of(context: &InvocationContext) -> Self {
        let info = &context.invocation;
        if info.channel == InvocationChannel::Scheduler
            || info.root_channel == InvocationChannel::Scheduler
        {
            Lane::Scheduled
        } else if info.channel == InvocationChannel::System
            || info.root_channel == InvocationChannel::System
        {
            Lane::System
        } else {
            Lane::User
        }
    }

    /// How much later than a user call arriving at the same time this lane
    /// is served.
    fn delay(self, priority: bool) -> Duration {
        match (priority, self) {
            (false, _) | (true, Lane::User) => Duration::ZERO,
            (true, Lane::System) => Duration::from_secs(1),
            (true, Lane::Scheduled) => Duration::from_secs(5),
        }
    }
}

/// The pool is closed; no permit will be granted.
#[derive(Debug)]
pub(crate) struct Closed;

struct State {
    available: usize,
    /// Waiters by (rank in nanoseconds since `epoch`, arrival sequence).
    waiters: BTreeMap<(u128, u64), oneshot::Sender<AdmissionPermit>>,
    sequence: u64,
}

pub(crate) struct Admission {
    state: Mutex<State>,
    epoch: Instant,
    /// Off: every lane has the same rank delay, which is FIFO.
    priority: bool,
}

/// One unit of pool capacity. Dropping it hands the capacity to the
/// best-ranked waiter, or returns it to the pool.
pub(crate) struct AdmissionPermit {
    admission: Option<Arc<Admission>>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if let Some(admission) = self.admission.take() {
            admission.release();
        }
    }
}

impl Admission {
    pub(crate) fn new(permits: usize, priority: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                available: permits,
                waiters: BTreeMap::new(),
                sequence: 0,
            }),
            epoch: Instant::now(),
            priority,
        })
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.state.lock().expect("admission lock").available
    }

    /// Waits for a permit in `lane`. Cancel-safe: a dropped waiter leaves the
    /// queue, and a permit granted to it in the meantime is passed on.
    pub(crate) async fn acquire(self: &Arc<Self>, lane: Lane) -> Result<AdmissionPermit, Closed> {
        let (key, granted) = {
            let mut state = self.state.lock().expect("admission lock");
            // Capacity is handed straight to waiters on release, so free
            // capacity means nobody is waiting.
            if state.available > 0 {
                state.available -= 1;
                return Ok(self.permit());
            }
            let rank = (self.epoch.elapsed() + lane.delay(self.priority)).as_nanos();
            state.sequence += 1;
            let key = (rank, state.sequence);
            let (sender, granted) = oneshot::channel();
            state.waiters.insert(key, sender);
            (key, granted)
        };
        let waiting = Waiting {
            admission: self,
            key,
        };
        let permit = granted.await.map_err(|_| Closed)?;
        drop(waiting);
        Ok(permit)
    }

    /// `acquire` in the user lane, for tests that hold the pool directly.
    #[cfg(test)]
    pub(crate) async fn acquire_owned(self: Arc<Self>) -> Result<AdmissionPermit, Closed> {
        self.acquire(Lane::User).await
    }

    fn permit(self: &Arc<Self>) -> AdmissionPermit {
        AdmissionPermit {
            admission: Some(Arc::clone(self)),
        }
    }

    fn release(self: &Arc<Self>) {
        let mut state = self.state.lock().expect("admission lock");
        while let Some((_, sender)) = state.waiters.pop_first() {
            match sender.send(self.permit()) {
                Ok(()) => return,
                // The waiter is gone; do not let the returned permit release
                // (and lock) again, try the next waiter.
                Err(mut unclaimed) => unclaimed.admission = None,
            }
        }
        state.available += 1;
    }
}

/// Removes a canceled waiter from the queue.
struct Waiting<'a> {
    admission: &'a Arc<Admission>,
    key: (u128, u64),
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let sender = self
            .admission
            .state
            .lock()
            .expect("admission lock")
            .waiters
            .remove(&self.key);
        // Dropped outside the lock. A permit already sent sits in the
        // receiver, which the caller drops, and that permit releases itself.
        drop(sender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    /// Queues one waiter per lane behind a held permit and records the order
    /// in which they are admitted.
    async fn order(priority: bool, lanes: &[Lane]) -> Vec<Lane> {
        let admission = Admission::new(1, priority);
        let held = admission.acquire(Lane::User).await.unwrap();
        let admitted = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for &lane in lanes {
            let admission = Arc::clone(&admission);
            let admitted = Arc::clone(&admitted);
            tasks.push(tokio::spawn(async move {
                let permit = admission.acquire(lane).await.unwrap();
                admitted.lock().unwrap().push(lane);
                tokio::task::yield_now().await;
                drop(permit);
            }));
            // Let the waiter register before the next one arrives.
            tokio::task::yield_now().await;
        }
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        let order = admitted.lock().unwrap().clone();
        assert_eq!(admission.available_permits(), 1);
        order
    }

    #[test]
    fn user_calls_are_admitted_before_queued_scheduled_and_system_work() {
        runtime().block_on(async {
            use Lane::*;
            let lanes = [Scheduled, Scheduled, System, User, Scheduled, User];
            assert_eq!(
                order(true, &lanes).await,
                vec![User, User, System, Scheduled, Scheduled, Scheduled]
            );
            // Without priority the queue is FIFO.
            assert_eq!(order(false, &lanes).await, lanes.to_vec());
        });
    }

    #[test]
    fn scheduled_work_that_waited_past_its_lane_delay_is_not_starved() {
        runtime().block_on(async {
            let admission = Admission::new(1, true);
            let held = admission.acquire(Lane::User).await.unwrap();
            let scheduled = tokio::spawn({
                let admission = Arc::clone(&admission);
                async move {
                    admission
                        .acquire(Lane::Scheduled)
                        .await
                        .map(|_| Instant::now())
                }
            });
            tokio::task::yield_now().await;
            // Rank of the scheduled waiter is arrival + 5 s: pretend the user
            // call arrives later than that by moving the epoch back.
            {
                let mut state = admission.state.lock().unwrap();
                let (key, sender) = state.waiters.pop_first().unwrap();
                state
                    .waiters
                    .insert((key.0.saturating_sub(6_000_000_000), key.1), sender);
            }
            let user = tokio::spawn({
                let admission = Arc::clone(&admission);
                async move { admission.acquire(Lane::User).await.map(|_| Instant::now()) }
            });
            tokio::task::yield_now().await;
            drop(held);
            let scheduled_at = scheduled.await.unwrap().unwrap();
            let user_at = user.await.unwrap().unwrap();
            assert!(
                scheduled_at <= user_at,
                "an aged scheduled waiter goes first"
            );
        });
    }

    #[test]
    fn canceled_waiters_leave_the_queue_and_never_strand_capacity() {
        runtime().block_on(async {
            let admission = Admission::new(1, true);
            let held = admission.acquire(Lane::User).await.unwrap();
            // A waiter that times out while queued.
            assert!(
                tokio::time::timeout(Duration::from_millis(5), admission.acquire(Lane::User))
                    .await
                    .is_err()
            );
            assert!(admission.state.lock().unwrap().waiters.is_empty());
            // A waiter whose permit is granted but who is canceled before it
            // runs: the permit passes on instead of being lost.
            let admission2 = Arc::clone(&admission);
            let waiter = tokio::spawn(async move { admission2.acquire(Lane::Scheduled).await });
            tokio::task::yield_now().await;
            drop(held);
            waiter.abort();
            let _ = waiter.await;
            let again = tokio::time::timeout(Duration::from_secs(1), admission.acquire(Lane::User))
                .await
                .expect("capacity returned")
                .unwrap();
            drop(again);
            assert_eq!(admission.available_permits(), 1);
        });
    }
}
