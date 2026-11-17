//! A nonwaiting bound on ordinary replica opens, including cache single-flight.
//!
//! Acquired only after the validated resume path has exited. No semaphore waiter
//! is constructed here. Clearing/disabling the cache cannot replace this owner.
use crate::offload::SnapshotAdmissionError;
use std::sync::atomic::{
    AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};
use std::sync::{Arc, OnceLock};

pub(crate) struct OpenAdmission {
    state: Arc<State>,
}

struct State {
    limit: usize,
    active: AtomicUsize,
    peak: AtomicUsize,
    refused: AtomicUsize,
}

impl Default for OpenAdmission {
    fn default() -> Self {
        static GLOBAL: OnceLock<Arc<State>> = OnceLock::new();
        let state = GLOBAL.get_or_init(|| {
            let number = |name: &str| std::env::var(name).ok()?.trim().parse::<usize>().ok();
            let active = crate::offload::SnapshotAdmission::global().capacity();
            // Bound arithmetic and profiler signed-gauge representation. Invalid
            // budgets fall back to 256; an explicit zero allows active slots only.
            let max = tokio::sync::Semaphore::MAX_PERMITS;
            let waiting = number("GONVEX_REPLICA_OPEN_MAX_WAITING")
                .filter(|v| active.checked_add(*v).is_some_and(|n| n <= max))
                .unwrap_or(256.min(max.saturating_sub(active)));

            Self::new(active.saturating_add(waiting)).state
        });
        Self {
            state: Arc::clone(state),
        }
    }
}

impl OpenAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(State {
                // Keep ownership and the signed profiler gauge representable even
                // for an absurd explicit active-slot configuration.
                limit: limit.min(tokio::sync::Semaphore::MAX_PERMITS),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                refused: AtomicUsize::new(0),
            }),
        }
    }

    /// No allocation, lock, async wait, or lost-wakeup state. Acquire/release
    /// orders gauge removal before capacity reuse, including on weakly ordered
    /// CPUs. The counters never publish snapshot contents.
    pub(crate) fn try_acquire(&self) -> Result<OpenPermit<'_>, SnapshotAdmissionError> {
        let state = &*self.state;
        let mut count = state.active.load(Relaxed);
        let previous = loop {
            if count >= state.limit {
                break None;
            }
            match state
                .active
                .compare_exchange_weak(count, count + 1, Acquire, Relaxed)
            {
                Ok(_) => break Some(count),
                Err(observed) => count = observed,
            }
        };
        match previous {
            Some(n) => {
                state.peak.fetch_max(n + 1, Relaxed);
                Ok(OpenPermit {
                    admission: state,
                    _gauge: Some(gonvex_profiler::cold_open_guard()),
                    _lifetime: gonvex_profiler::ScopeTimer::new(
                        gonvex_profiler::Stage::ColdOpenLifetime,
                        gonvex_profiler::Label::None,
                    ),
                })
            }
            None => {
                state.refused.fetch_add(1, Relaxed);
                gonvex_profiler::record(
                    gonvex_profiler::Stage::ColdOpenRefused,
                    gonvex_profiler::Label::None,
                    1,
                );
                Err(SnapshotAdmissionError("too many pending replica opens"))
            }
        }
    }

    pub(crate) fn stats(&self) -> (usize, usize, usize, usize) {
        let state = &*self.state;
        (
            state.limit,
            state.active.load(Relaxed),
            state.peak.load(Relaxed),
            state.refused.load(Relaxed),
        )
    }
}

#[must_use]
pub(crate) struct OpenPermit<'a> {
    admission: &'a State,
    _gauge: Option<gonvex_profiler::GaugeGuard>,
    _lifetime: gonvex_profiler::ScopeTimer,
}
impl Drop for OpenPermit<'_> {
    fn drop(&mut self) {
        // Release the observed gauge before publishing free capacity. Otherwise
        // a racing replacement could briefly count both owners above the cap.
        drop(self._gauge.take());
        self.admission.active.fetch_sub(1, Release);
    }
}

#[cfg(test)]
#[path = "open_admission_tests.rs"]
mod tests;
