//! Attributes scheduled work to its cron for the profiler's cron table
//! (`gonvex_profiler::cron`).
//!
//! The scheduler runs each claimed job inside [`scope`]. Execution code adds
//! to the run's accumulator through a task-local (one thread-local read, no
//! allocation, no lock); nested Reducers that an Action awaits inline count
//! toward the same run. Work outside a scheduled run is unaffected:
//! [`active`] is false and every `add` is a no-op.

use std::cell::Cell;
use std::future::Future;

use gonvex_profiler::cron::CronSample;

#[derive(Default)]
pub(crate) struct ScheduledRun {
    pub invoke_ns: Cell<u64>,
    pub writer_hold_ns: Cell<u64>,
    pub host_calls: Cell<u64>,
    pub rows_read: Cell<u64>,
    pub rows_written: Cell<u64>,
    pub revisions: Cell<u64>,
    pub fanout: Cell<u64>,
    /// Module-host requests in progress (an Action's nested Reducers run
    /// inside its request; only the outermost one counts as V8 time).
    invoke_depth: Cell<u32>,
}

/// Times the outermost module-host request of a scheduled run.
pub(crate) struct InvokeTimer(u64);

impl InvokeTimer {
    #[inline]
    pub(crate) fn start(scheduled: bool) -> Self {
        if !scheduled {
            return Self(0);
        }
        let outermost = SCHEDULED
            .try_with(|run| {
                let depth = run.invoke_depth.get();
                run.invoke_depth.set(depth + 1);
                depth == 0
            })
            .unwrap_or(false);
        Self(if outermost {
            gonvex_profiler::now_ns().max(1)
        } else {
            u64::MAX
        })
    }
}

impl Drop for InvokeTimer {
    fn drop(&mut self) {
        if self.0 == 0 {
            return;
        }
        let started = self.0;
        let _ = SCHEDULED.try_with(|run| {
            run.invoke_depth
                .set(run.invoke_depth.get().saturating_sub(1));
            if started != u64::MAX {
                let elapsed = gonvex_profiler::now_ns().saturating_sub(started);
                run.invoke_ns
                    .set(run.invoke_ns.get().saturating_add(elapsed));
            }
        });
    }
}

tokio::task_local! {
    static SCHEDULED: ScheduledRun;
}

/// Whether the current task is a scheduled run.
#[inline]
pub(crate) fn active() -> bool {
    SCHEDULED.try_with(|_| ()).is_ok()
}

/// Adds to the current run's accumulator, if any.
#[inline]
pub(crate) fn add(field: impl FnOnce(&ScheduledRun) -> &Cell<u64>, value: u64) {
    if value == 0 {
        return;
    }
    let _ = SCHEDULED.try_with(|run| {
        let cell = field(run);
        cell.set(cell.get().saturating_add(value));
    });
}

/// Runs `future` as one scheduled run and returns its output with the run's
/// totals (`total_ns`, `start_delay_ns`, `error` and `skipped` are the
/// caller's).
pub(crate) async fn scope<F: Future>(future: F) -> (F::Output, CronSample) {
    SCHEDULED
        .scope(ScheduledRun::default(), async move {
            let output = future.await;
            let sample = SCHEDULED.with(|run| CronSample {
                invoke_ns: run.invoke_ns.get(),
                writer_hold_ns: run.writer_hold_ns.get(),
                host_calls: run.host_calls.get(),
                rows_read: run.rows_read.get(),
                rows_written: run.rows_written.get(),
                revisions: run.revisions.get(),
                fanout: run.fanout.get(),
                ..CronSample::default()
            });
            (output, sample)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn adds_land_only_inside_a_scheduled_run() {
        assert!(!active());
        add(|run| &run.rows_written, 5);
        let ((), sample) = scope(async {
            assert!(active());
            add(|run| &run.rows_written, 2);
            add(|run| &run.rows_written, 3);
            add(|run| &run.revisions, 1);
        })
        .await;
        assert_eq!(sample.rows_written, 5);
        assert_eq!(sample.revisions, 1);
        assert!(!active());
        let ((), sample) = scope(async {
            let outer = InvokeTimer::start(true);
            {
                let _nested = InvokeTimer::start(true);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            drop(outer);
        })
        .await;
        assert!(sample.invoke_ns >= 2_000_000, "{}", sample.invoke_ns);
        assert!(sample.invoke_ns < 1_000_000_000, "nested time counted once");
    }
}
