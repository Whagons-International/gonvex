//! The low-priority execution lane for scheduled work (crons and one-shot
//! `runAt`/`wakeAt` jobs).
//!
//! * Scheduled runs execute on their own OS thread (a current-thread tokio
//!   runtime, `gonvex-scheduled`), never on the tokio worker that polls
//!   sockets. On Linux the thread runs at a higher nice value
//!   (`GONVEX_SCHEDULER_NICE`, default 5), so under CPU contention the kernel
//!   prefers the network thread. In-runtime work of a run (host-call
//!   marshalling, SQL hand-off, the in-process V8 bridge where it exists)
//!   happens on this thread.
//! * A process-wide cap on concurrent runs (`GONVEX_SCHEDULER_CONCURRENCY`,
//!   default 2) and a per-tenant cap of one. The scheduler claims only as many
//!   jobs as it has free permits, so a claimed job never waits behind the cap
//!   while its lease runs down.
//! * User traffic first: before a run starts it waits while user Reducers,
//!   Queries or Actions are executing (`USER_CALLS`), up to
//!   `GONVEX_SCHEDULER_YIELD_MS` (default 2000) per run, so a busy server
//!   still makes progress on its schedule.
//!
//! The lane sits in front of the module-host bridge (out-of-process V8) and
//! the tenant writer alike: it decides when a run may start, not how it calls
//! V8.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// User (non-scheduled) invocations executing now.
static USER_CALLS: AtomicI64 = AtomicI64::new(0);
static USER_IDLE: OnceLock<Notify> = OnceLock::new();

fn user_idle() -> &'static Notify {
    USER_IDLE.get_or_init(Notify::new)
}

/// Counts one user invocation for its lifetime. A relaxed add on entry and a
/// release sub on exit; the last call to finish wakes waiting scheduled runs.
pub(crate) struct UserCall(bool);

impl UserCall {
    #[inline]
    pub(crate) fn enter(scheduled: bool) -> Self {
        if scheduled {
            return Self(false);
        }
        USER_CALLS.fetch_add(1, Ordering::Relaxed);
        Self(true)
    }
}

impl Drop for UserCall {
    #[inline]
    fn drop(&mut self) {
        if self.0 && USER_CALLS.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(notify) = USER_IDLE.get() {
                notify.notify_waiters();
            }
        }
    }
}

pub(crate) fn user_calls() -> i64 {
    USER_CALLS.load(Ordering::Acquire)
}

/// Waits until no user invocation is executing, at most `limit`. Returns how
/// long it waited.
pub(crate) async fn yield_to_users(limit: Duration) -> Duration {
    let started = std::time::Instant::now();
    loop {
        let notified = user_idle().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if user_calls() <= 0 {
            return started.elapsed();
        }
        let left = limit.saturating_sub(started.elapsed());
        if left.is_zero() {
            return started.elapsed();
        }
        // The notify covers the common case; the short timeout covers a
        // decrement that raced the registration above.
        let _ = tokio::time::timeout(left.min(Duration::from_millis(50)), notified).await;
    }
}

#[derive(Clone, Debug)]
pub struct LaneConfig {
    pub concurrency: usize,
    pub yield_limit: Duration,
    pub nice: i32,
    pub dedicated_thread: bool,
}

impl LaneConfig {
    pub fn from_env() -> Self {
        let number = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<i64>().ok())
        };
        Self {
            concurrency: number("GONVEX_SCHEDULER_CONCURRENCY")
                .filter(|value| *value > 0)
                .map_or(2, |value| value as usize),
            yield_limit: Duration::from_millis(
                number("GONVEX_SCHEDULER_YIELD_MS")
                    .filter(|value| *value >= 0)
                    .map_or(2_000, |value| value as u64),
            ),
            nice: number("GONVEX_SCHEDULER_NICE").map_or(5, |value| value.clamp(0, 19) as i32),
            dedicated_thread: !matches!(
                std::env::var("GONVEX_SCHEDULER_THREAD")
                    .ok()
                    .as_deref()
                    .map(str::trim),
                Some("0" | "false" | "off")
            ),
        }
    }
}

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            concurrency: 2,
            yield_limit: Duration::from_millis(2_000),
            nice: 5,
            dedicated_thread: true,
        }
    }
}

pub(crate) struct ScheduledLane {
    config: LaneConfig,
    permits: Arc<Semaphore>,
    tenants: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    handle: OnceLock<Option<tokio::runtime::Handle>>,
}

impl ScheduledLane {
    pub(crate) fn new(config: LaneConfig) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(config.concurrency)),
            config,
            tenants: Mutex::new(HashMap::new()),
            handle: OnceLock::new(),
        }
    }

    pub(crate) fn config(&self) -> &LaneConfig {
        &self.config
    }

    /// Takes every free run permit (at least one, waiting for it).
    pub(crate) async fn acquire_free(&self) -> Vec<OwnedSemaphorePermit> {
        let mut permits = Vec::new();
        if let Ok(first) = self.permits.clone().acquire_owned().await {
            permits.push(first);
        }
        while let Ok(more) = self.permits.clone().try_acquire_owned() {
            permits.push(more);
        }
        permits
    }

    /// The per-tenant run lock (one scheduled run per tenant at a time).
    pub(crate) fn tenant_lock(&self, tenant: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut tenants = self
            .tenants
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if tenants.len() > 4_096 {
            tenants.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        tenants.entry(tenant.to_owned()).or_default().clone()
    }

    /// Runs `future` on the scheduled-work thread (or the current runtime
    /// when the dedicated thread is off or could not start).
    pub(crate) fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self.handle.get_or_init(|| self.start_thread()) {
            Some(handle) => {
                handle.spawn(future);
            }
            None => {
                tokio::spawn(future);
            }
        }
    }

    fn start_thread(&self) -> Option<tokio::runtime::Handle> {
        if !self.config.dedicated_thread {
            return None;
        }
        let nice = self.config.nice;
        let (sender, receiver) = std::sync::mpsc::channel();
        let started = std::thread::Builder::new()
            .name("gonvex-scheduled".to_owned())
            .spawn(move || {
                lower_thread_priority(nice);
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("gonvex-scheduled")
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        tracing::error!(error = %error, "start the scheduled-work runtime");
                        let _ = sender.send(None);
                        return;
                    }
                };
                let _ = sender.send(Some(runtime.handle().clone()));
                runtime.block_on(std::future::pending::<()>());
            });
        match started {
            Ok(_) => receiver.recv().ok().flatten(),
            Err(error) => {
                tracing::error!(error = %error, "start the scheduled-work thread");
                None
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn lower_thread_priority(nice: i32) {
    extern "C" {
        fn setpriority(which: i32, who: u32, prio: i32) -> i32;
        fn gettid() -> i32;
    }
    if nice <= 0 {
        return;
    }
    // PRIO_PROCESS with a thread id sets that thread's nice value (Linux).
    // Raising one's own nice value needs no privilege.
    let result = unsafe { setpriority(0, gettid() as u32, nice) };
    if result != 0 {
        tracing::warn!(nice, "could not lower the scheduled-work thread priority");
    }
}

#[cfg(not(target_os = "linux"))]
fn lower_thread_priority(_nice: i32) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scheduled_runs_wait_for_user_calls_up_to_the_limit() {
        let call = UserCall::enter(false);
        let waited = yield_to_users(Duration::from_millis(30)).await;
        assert!(waited >= Duration::from_millis(30), "{waited:?}");
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(call);
        });
        let waited = yield_to_users(Duration::from_secs(5)).await;
        assert!(
            waited < Duration::from_secs(2),
            "woken when the user call ended: {waited:?}"
        );
        release.await.unwrap();
        // Scheduled work does not count as a user call.
        let _scheduled = UserCall::enter(true);
        assert!(yield_to_users(Duration::from_secs(5)).await < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn permits_cap_concurrent_runs_and_tenants_serialize() {
        let lane = ScheduledLane::new(LaneConfig {
            concurrency: 2,
            dedicated_thread: false,
            ..LaneConfig::default()
        });
        let first = lane.acquire_free().await;
        assert_eq!(first.len(), 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), lane.acquire_free())
                .await
                .is_err(),
            "no permit while two runs hold them"
        );
        drop(first);
        assert_eq!(lane.acquire_free().await.len(), 2);
        let a = lane.tenant_lock("tenant-a");
        let guard = a.clone().lock_owned().await;
        assert!(lane.tenant_lock("tenant-a").try_lock().is_err());
        assert!(lane.tenant_lock("tenant-b").try_lock().is_ok());
        drop(guard);
    }

    #[test]
    fn the_dedicated_thread_runs_spawned_work() {
        let lane = ScheduledLane::new(LaneConfig::default());
        let (sender, receiver) = std::sync::mpsc::channel();
        lane.spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let _ = sender.send(std::thread::current().name().map(str::to_owned));
        });
        let name = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(name.as_deref(), Some("gonvex-scheduled"));
    }
}
