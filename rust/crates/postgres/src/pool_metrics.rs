//! Acquisition diagnostics shared by clones of one runtime PoolRegistry.
//!
//! Poll the original future once. Only its first Pending starts a wait, so an
//! immediate Ready acquisition performs no metrics clock reads. Both stages poll
//! their original acquisition futures, preserving cooperative scheduling behavior.
//! Admission duration can include cooperative executor yields.
//! SQLx Pending can also mean ping/connect I/O;
//! SQLx 0.8.6 exposes no public observer for its internal capacity queue alone.
//! Do not use SQLx try_acquire here: SQLx's fast path skips normal idle checks.
//!
//! Wait duration runs from the first Pending to completion or drop, including
//! executor scheduling delay. Active waits contribute duration only when settled.
//! Nanoseconds accumulate before conversion to whole milliseconds, retaining
//! sub-ms waits in the total. Duration totals saturate rather than wrap.
//!
//! Immediate completion costs one relaxed outcome-counter increment. A queued
//! attempt also updates wait/current counters, reads Instant on entry and exit,
//! and updates duration/max counters. The metrics add no allocations, tasks,
//! polling loops or timers. Snapshots read relaxed atomics independently and
//! are approximate, not linearizable; live snapshots need not balance counts.

use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::task::Poll;
use std::time::Instant;

use serde::Serialize;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolWaitSnapshot {
    /// Attempts that returned Pending at least once, regardless of outcome.
    pub wait_count: u64,
    /// Settled waits only, including timeout, error and cancellation.
    pub wait_duration_ms: u64,
    /// Polled attempts suspended in this stage, not a tenant/lane queue count.
    pub current_waiters: u64,
    /// All successful acquisitions, including immediate acquisitions.
    pub acquired_count: u64,
    /// Native deadline expired; external future drop is cancellation instead.
    pub timeout_count: u64,
    /// Pending futures dropped before returning an outcome, including unwind.
    pub cancelled_count: u64,
    /// Other acquire errors, such as a closed semaphore or pool.
    pub error_count: u64,
    pub max_wait_duration_ms: u64,
}

#[derive(Default)]
pub(crate) struct WaitMetrics {
    wait_count: AtomicU64,
    wait_duration_ns: AtomicU64,
    current_waiters: AtomicU64,
    acquired_count: AtomicU64,
    timeout_count: AtomicU64,
    cancelled_count: AtomicU64,
    error_count: AtomicU64,
    max_wait_duration_ns: AtomicU64,
}

pub(crate) enum Outcome {
    Acquired,
    TimedOut,
    Error,
}

impl WaitMetrics {
    #[allow(clippy::manual_async_fn)]
    pub(crate) fn track<'a, F, C>(
        &'a self,
        future: F,
        classify: C,
    ) -> impl Future<Output = F::Output> + Send + 'a
    where
        F: Future + Send + 'a,
        F::Output: Send + 'a,
        C: FnOnce(&F::Output) -> Outcome + Send + 'a,
    {
        async move {
            // The guard lives inside this future. Dropping it balances the gauge
            // synchronously, even when the caller cancels without polling again.
            let mut attempt = Attempt {
                metrics: self,
                started: None,
                finished: false,
            };
            let mut future = std::pin::pin!(future);
            let result = poll_fn(|cx| match future.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    if attempt.started.is_none() {
                        attempt.started = Some(Instant::now());
                        self.wait_count.fetch_add(1, Relaxed);
                        self.current_waiters.fetch_add(1, Relaxed);
                    }
                    Poll::Pending
                }
            })
            .await;
            match classify(&result) {
                Outcome::Acquired => &self.acquired_count,
                Outcome::TimedOut => &self.timeout_count,
                Outcome::Error => &self.error_count,
            }
            .fetch_add(1, Relaxed);
            attempt.finished = true;
            result
        }
    }

    pub(crate) fn snapshot(&self) -> PoolWaitSnapshot {
        PoolWaitSnapshot {
            wait_count: self.wait_count.load(Relaxed),
            wait_duration_ms: self.wait_duration_ns.load(Relaxed) / 1_000_000,
            current_waiters: self.current_waiters.load(Relaxed),
            acquired_count: self.acquired_count.load(Relaxed),
            timeout_count: self.timeout_count.load(Relaxed),
            cancelled_count: self.cancelled_count.load(Relaxed),
            error_count: self.error_count.load(Relaxed),
            max_wait_duration_ms: self.max_wait_duration_ns.load(Relaxed) / 1_000_000,
        }
    }
}

struct Attempt<'a> {
    metrics: &'a WaitMetrics,
    started: Option<Instant>,
    finished: bool,
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            // Only queued waits pay for these updates. Counters need no ordering
            // relationship with application data or ownership of the resource.
            let _ = self
                .metrics
                .wait_duration_ns
                .fetch_update(Relaxed, Relaxed, |total| {
                    Some(total.saturating_add(elapsed))
                });
            self.metrics
                .max_wait_duration_ns
                .fetch_max(elapsed, Relaxed);
            if !self.finished {
                self.metrics.cancelled_count.fetch_add(1, Relaxed);
            }
            self.metrics.current_waiters.fetch_sub(1, Relaxed);
        }
    }
}

/// Internal integration helper for finite SQLx queries. The connection is owned
/// by the query future and drops before it returns, as with the pool Executor.
/// A match keeps temporaries used by query builders alive across acquisition.
/// Admission remains the caller's responsibility; this adds no admission permit.
#[doc(hidden)]
#[macro_export]
macro_rules! pool_query {
    ($registry:expr, $pool:expr, $query:expr, $method:ident) => {
        $crate::__pool_query_send(async {
            match $query {
                query => {
                    let mut connection = ($registry).acquire($pool).await?;
                    query.$method(&mut *connection).await
                }
            }
        })
    };
}

#[cfg(test)]
mod tests;
