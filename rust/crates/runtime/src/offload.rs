//! Heavy work off the network runtime, and bounded snapshot admission.
//!
//! The process's main tokio runtime serves every socket. In a 1-vCPU container
//! it has a single worker thread, so a 175 ms replica snapshot build or a
//! 1-2 ms-per-socket feed application on it delays every heartbeat queued
//! behind it. Replica snapshot preparation (SQL, decode, intern, visibility),
//! feed application (visibility and delta building) and snapshot page encoding
//! run on this pool instead: a small, separate tokio runtime whose threads run
//! at a lower CPU priority (nice), so the kernel switches to the network
//! thread as soon as a socket becomes ready instead of waiting for the next
//! cooperative yield.
//!
//! Environment:
//! - `GONVEX_OFFLOAD`: `runtime` (default), `inline` (no offload; the previous
//!   behaviour), `blocking` (`spawn_blocking` bounded by a semaphore) or
//!   `threads` (fixed OS threads, one job at a time each). The alternatives
//!   exist for the comparison in docs/performance/offload.md.
//! - `GONVEX_OFFLOAD_THREADS`: pool threads (default: available CPUs, 1..=4).
//! - `GONVEX_OFFLOAD_NICE`: nice increment of pool threads (default 0: off).
//!   Measured: nice 10 cut pool throughput 2-20x on a shared host (hierarchical
//!   CFS weighs a cgroup by the nice weight of its runnable threads), while the
//!   network thread was already isolated at nice 0 (docs/performance/offload.md).
//! - `GONVEX_OFFLOAD_MIN_US`: feed applications whose recent maximum for the
//!   same set of tables is below this run inline (default 250 us): a pool round
//!   trip costs ~6-12 us plus a session clone, more than a small presence delta.
//! - `GONVEX_REPLICA_SNAPSHOT_CONCURRENCY`: concurrent snapshot builds
//!   (default: available CPUs, min 2).
//! - `GONVEX_REPLICA_SNAPSHOT_BUDGET_BYTES`: estimated decoded snapshot heap
//!   in flight across all builds (default 128 MiB). Estimated as 5x the rows'
//!   JSON size (a 9,000-row Admin task snapshot: ~16 MB JSON, +76 MB heap).
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::future::{AbortHandle, Abortable, FutureExt};
use gonvex_protocol::ServerMessage;
use gonvex_store::TenantSession;
use serde_json::Value;

use crate::change_feed::FeedEvent;
use crate::replica::{ReplicaError, ReplicaSubscription};
use crate::Runtime;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// Run on the caller's runtime (no offload).
    Inline,
    /// A separate multi-thread tokio runtime with `threads` workers.
    Runtime,
    /// `spawn_blocking` + `Handle::block_on`, at most `threads` at once.
    Blocking,
    /// `threads` dedicated OS threads, each driving one job at a time.
    Threads,
}

impl Strategy {
    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "inline" | "off" | "0" => Some(Self::Inline),
            "runtime" | "" | "1" => Some(Self::Runtime),
            "blocking" => Some(Self::Blocking),
            "threads" => Some(Self::Threads),
            _ => None,
        }
    }
}

type Job = Box<dyn FnOnce() + Send>;

pub struct Offload {
    strategy: Strategy,
    threads: usize,
    inline_below: Duration,
    runtime: Option<tokio::runtime::Runtime>,
    blocking: Arc<tokio::sync::Semaphore>,
    jobs: Option<Mutex<std::sync::mpsc::Sender<Job>>>,
    /// Decaying maximum feed-application time per table set
    /// (see `GONVEX_OFFLOAD_MIN_US`).
    apply_cost: Mutex<HashMap<Box<str>, Duration>>,
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
}

fn cpus() -> usize {
    std::thread::available_parallelism().map_or(1, |cpus| cpus.get())
}

/// Lowers the calling thread's CPU priority by `increment` (Linux: per thread).
fn lower_priority(increment: i32) {
    #[cfg(target_os = "linux")]
    if increment > 0 {
        // SAFETY: plain syscalls on the calling thread's id.
        unsafe {
            let tid = libc::gettid() as libc::id_t;
            let current = libc::getpriority(libc::PRIO_PROCESS, tid);
            libc::setpriority(libc::PRIO_PROCESS, tid, (current + increment).min(19));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = increment;
}

/// The process-wide pool, configured from the environment on first use.
pub fn offload() -> &'static Offload {
    static POOL: OnceLock<Offload> = OnceLock::new();
    POOL.get_or_init(|| {
        let strategy = std::env::var("GONVEX_OFFLOAD")
            .ok()
            .and_then(|value| Strategy::parse(&value))
            .unwrap_or(Strategy::Runtime);
        let threads = env_usize("GONVEX_OFFLOAD_THREADS")
            .filter(|threads| *threads > 0)
            .unwrap_or_else(|| cpus().clamp(1, 4));
        let nice = env_usize("GONVEX_OFFLOAD_NICE").unwrap_or(0) as i32;
        let inline_below =
            Duration::from_micros(env_usize("GONVEX_OFFLOAD_MIN_US").unwrap_or(250) as u64);
        Offload::new(strategy, threads, nice, inline_below)
    })
}

impl Offload {
    pub fn new(strategy: Strategy, threads: usize, nice: i32, inline_below: Duration) -> Self {
        let runtime = (strategy == Strategy::Runtime).then(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .thread_name("gonvex-offload")
                .enable_all()
                .on_thread_start(move || lower_priority(nice))
                .build()
                .expect("offload runtime")
        });
        let jobs = (strategy == Strategy::Threads).then(|| {
            let (sender, receiver) = std::sync::mpsc::channel::<Job>();
            let receiver = Arc::new(Mutex::new(receiver));
            for index in 0..threads {
                let receiver = receiver.clone();
                std::thread::Builder::new()
                    .name(format!("gonvex-offload-{index}"))
                    .spawn(move || {
                        lower_priority(nice);
                        loop {
                            let job = receiver.lock().unwrap().recv();
                            match job {
                                Ok(job) => job(),
                                Err(_) => return,
                            }
                        }
                    })
                    .expect("offload thread");
            }
            Mutex::new(sender)
        });
        Self {
            strategy,
            threads,
            inline_below,
            runtime,
            blocking: Arc::new(tokio::sync::Semaphore::new(threads)),
            jobs,
            apply_cost: Mutex::default(),
        }
    }

    pub fn strategy(&self) -> Strategy {
        self.strategy
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn is_inline(&self) -> bool {
        self.strategy == Strategy::Inline
    }

    /// Runs `future` on the pool. Dropping the handle cancels it. With
    /// `Inline` it is a task on the caller's runtime (`tokio::spawn`).
    pub fn spawn<F>(&self, future: F) -> Offloaded<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.strategy {
            Strategy::Inline => {
                // The stall driver times inline jobs on the network thread.
                #[cfg(feature = "perf-fixture")]
                let future = crate::perf_fixture::stalls::timed(future);
                Offloaded::Join(tokio::spawn(future))
            }
            Strategy::Runtime => {
                Offloaded::Join(self.runtime.as_ref().unwrap().spawn(Tracked::new(future)))
            }
            Strategy::Blocking | Strategy::Threads => {
                let future = Tracked::new(future);
                let (abort, registration) = AbortHandle::new_pair();
                let (sender, receiver) = tokio::sync::oneshot::channel();
                let handle = tokio::runtime::Handle::current();
                let job = move || {
                    let result = handle.block_on(
                        AssertUnwindSafe(Abortable::new(future, registration)).catch_unwind(),
                    );
                    match result {
                        Ok(Ok(value)) => {
                            let _ = sender.send(Ok(value));
                        }
                        Err(panic) => {
                            let _ = sender.send(Err(panic));
                        }
                        // Cancelled: the handle is gone.
                        Ok(Err(_)) => {}
                    }
                };
                if self.strategy == Strategy::Threads {
                    self.submit(Box::new(job));
                } else {
                    let permits = self.blocking.clone();
                    tokio::spawn(async move {
                        let _permit = permits.acquire_owned().await;
                        let _ = tokio::task::spawn_blocking(job).await;
                    });
                }
                Offloaded::Channel(receiver, abort)
            }
        }
    }

    /// Runs synchronous `work` on the pool. With `Inline` it runs now.
    pub fn run<T, W>(&self, work: W) -> Offloaded<T>
    where
        W: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        match self.strategy {
            Strategy::Inline => {
                Offloaded::Ready(Some(std::panic::catch_unwind(AssertUnwindSafe(work))))
            }
            Strategy::Runtime => {
                let work = tracked_work(work);
                Offloaded::Join(self.runtime.as_ref().unwrap().spawn(
                    gonvex_profiler::instrument(gonvex_profiler::TaskKind::Other, async move {
                        work()
                    }),
                ))
            }
            Strategy::Blocking | Strategy::Threads => {
                let work = tracked_work(work);
                let (sender, receiver) = tokio::sync::oneshot::channel();
                let (abort, _registration) = AbortHandle::new_pair();
                let job = move || {
                    let _ = sender.send(std::panic::catch_unwind(AssertUnwindSafe(work)));
                };
                if self.strategy == Strategy::Threads {
                    self.submit(Box::new(job));
                } else {
                    let permits = self.blocking.clone();
                    tokio::spawn(async move {
                        let _permit = permits.acquire_owned().await;
                        let _ = tokio::task::spawn_blocking(job).await;
                    });
                }
                Offloaded::Channel(receiver, abort)
            }
        }
    }

    /// Runs `work` on the pool without waiting for it (never cancelled).
    pub fn detach<W: FnOnce() + Send + 'static>(&self, work: W) {
        if self.strategy == Strategy::Inline {
            return work();
        }
        let work = tracked_work(work);
        match self.strategy {
            Strategy::Inline => work(),
            Strategy::Runtime => {
                drop(self.runtime.as_ref().unwrap().spawn(async move { work() }));
            }
            Strategy::Blocking => match tokio::runtime::Handle::try_current() {
                Ok(handle) => drop(handle.spawn_blocking(work)),
                Err(_) => work(),
            },
            Strategy::Threads => self.submit(Box::new(work)),
        }
    }

    fn submit(&self, job: Job) {
        self.jobs
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .send(job)
            .expect("offload threads are never stopped");
    }

    /// Whether a feed application touching `tables` is worth a pool round
    /// trip: unknown table sets are offloaded until measured.
    fn offload_apply(&self, tables: &str) -> bool {
        self.apply_cost
            .lock()
            .unwrap()
            .get(tables)
            .is_none_or(|cost| *cost >= self.inline_below)
    }

    fn record_apply(&self, tables: &str, elapsed: Duration) {
        let mut costs = self.apply_cost.lock().unwrap();
        if costs.len() > 4096 && !costs.contains_key(tables) {
            costs.clear();
        }
        let cost = costs.entry(tables.into()).or_insert(elapsed);
        // A slowly decaying maximum, not an average: the same table set is
        // cheap for most connections and tens of milliseconds for a few (an
        // average let those run inline). One slow sample keeps the set
        // offloaded for ~5,000 applications at the default threshold.
        *cost = elapsed.max(*cost - *cost / 1024);
    }
}

impl Drop for Offload {
    fn drop(&mut self) {
        // Pools built by tests are dropped inside async contexts.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// Profiler accounting of one pool job: `offload.queue_wait` (submitted ->
/// first run), `offload.busy` (time spent running), `offload.run` (first run
/// -> done) and the `offload.queued` / `offload.running` gauges. Inert (no
/// clock reads) with the profiler off.
struct JobTrack {
    submitted: u64,
    started: u64,
    busy: u64,
}

impl JobTrack {
    fn submit() -> Self {
        let submitted = if gonvex_profiler::enabled() {
            gonvex_profiler::gauge(gonvex_profiler::GaugeId::OffloadQueued).add(1);
            gonvex_profiler::now_ns()
        } else {
            0
        };
        Self {
            submitted,
            started: 0,
            busy: 0,
        }
    }

    /// Start of a run slice; returns its clock (0: not tracking).
    #[inline]
    fn begin(&mut self) -> u64 {
        if self.submitted == 0 {
            return 0;
        }
        let now = gonvex_profiler::now_ns();
        if self.started == 0 {
            self.started = now;
            gonvex_profiler::record(
                gonvex_profiler::Stage::OffloadQueueWait,
                gonvex_profiler::Label::None,
                now.saturating_sub(self.submitted),
            );
            gonvex_profiler::gauge(gonvex_profiler::GaugeId::OffloadQueued).add(-1);
            gonvex_profiler::gauge(gonvex_profiler::GaugeId::OffloadRunning).add(1);
        }
        now
    }

    #[inline]
    fn end(&mut self, began: u64) {
        if began != 0 {
            self.busy += gonvex_profiler::now_ns().saturating_sub(began);
        }
    }
}

impl Drop for JobTrack {
    fn drop(&mut self) {
        if self.submitted == 0 {
            return;
        }
        if self.started == 0 {
            // Cancelled before it ran.
            gonvex_profiler::gauge(gonvex_profiler::GaugeId::OffloadQueued).add(-1);
            return;
        }
        gonvex_profiler::gauge(gonvex_profiler::GaugeId::OffloadRunning).add(-1);
        gonvex_profiler::record(
            gonvex_profiler::Stage::OffloadBusy,
            gonvex_profiler::Label::None,
            self.busy,
        );
        gonvex_profiler::record(
            gonvex_profiler::Stage::OffloadRun,
            gonvex_profiler::Label::None,
            gonvex_profiler::now_ns().saturating_sub(self.started),
        );
    }
}

/// A pool future with its [`JobTrack`].
struct Tracked<F> {
    inner: F,
    track: JobTrack,
}

impl<F> Tracked<F> {
    fn new(inner: F) -> Self {
        Self {
            inner,
            track: JobTrack::submit(),
        }
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: `inner` is structurally pinned: it is never moved out of
        // `self`, and `Tracked` has no Drop impl or `Unpin` override.
        let this = unsafe { self.get_unchecked_mut() };
        let began = this.track.begin();
        let result = unsafe { Pin::new_unchecked(&mut this.inner) }.poll(context);
        this.track.end(began);
        result
    }
}

/// A synchronous pool job with its [`JobTrack`] (run time is busy time).
fn tracked_work<T>(work: impl FnOnce() -> T + Send) -> impl FnOnce() -> T + Send {
    let mut track = JobTrack::submit();
    move || {
        let began = track.begin();
        let value = work();
        track.end(began);
        value
    }
}

/// A pool job's result. Dropping it cancels the job (for futures).
pub enum Offloaded<T> {
    Join(tokio::task::JoinHandle<T>),
    Channel(
        tokio::sync::oneshot::Receiver<std::thread::Result<T>>,
        AbortHandle,
    ),
    Ready(Option<std::thread::Result<T>>),
}

// The output is only moved, never pinned.
impl<T> Unpin for Offloaded<T> {}

impl<T> Future for Offloaded<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        let result = match &mut *self {
            Self::Join(handle) => match std::task::ready!(Pin::new(handle).poll(context)) {
                Ok(value) => Ok(value),
                Err(error) if error.is_panic() => Err(error.into_panic()),
                Err(error) => panic!("offloaded task failed: {error}"),
            },
            Self::Channel(receiver, _) => match std::task::ready!(Pin::new(receiver).poll(context))
            {
                Ok(result) => result,
                Err(_) => panic!("offloaded task was dropped"),
            },
            Self::Ready(result) => result.take().expect("offloaded result polled twice"),
        };
        Poll::Ready(result.unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
    }
}

impl<T> Drop for Offloaded<T> {
    fn drop(&mut self) {
        match self {
            Self::Join(handle) => handle.abort(),
            Self::Channel(_, abort) => abort.abort(),
            Self::Ready(_) => {}
        }
    }
}

impl Runtime {
    /// `apply_feed_event` for one connection, on the pool when it is heavy.
    /// The subscriptions move to the pool job and back; the caller awaits, so
    /// per-connection ordering is unchanged.
    pub(crate) async fn apply_feed_event_offloaded(
        &self,
        session: &TenantSession,
        subscriptions: &mut BTreeMap<String, ReplicaSubscription>,
        event: FeedEvent,
        prewarm: Option<(bool, Option<u8>)>,
        record: bool,
    ) -> Result<Vec<ServerMessage>, ReplicaError> {
        let pool = offload();
        let tables = feed_tables(&event);
        if pool.is_inline() || subscriptions.is_empty() || !pool.offload_apply(&tables) {
            let started = Instant::now();
            let result = self.apply_feed_event_with(session, subscriptions, event, record).await;
            if !pool.is_inline() {
                pool.record_apply(&tables, started.elapsed());
            }
            return result;
        }
        let mut owned = std::mem::take(subscriptions);
        let runtime = self.clone();
        let session = session.clone();
        let (owned, result, elapsed) = pool
            .spawn(gonvex_profiler::instrument(
                gonvex_profiler::TaskKind::Feed,
                async move {
                    let started = Instant::now();
                    let result = runtime.apply_feed_event_with(&session, &mut owned, event, record).await;
                    // Delivery v3: encode and compress the shared items here,
                    // not at push time on the network thread.
                    if let (Ok(messages), Some((native, window))) = (&result, prewarm) {
                        crate::delivery::prewarm(
                            &runtime.inner.delivery,
                            &session.route.tenant_id,
                            native,
                            window,
                            &owned,
                            messages,
                        );
                    }
                    (owned, result, started.elapsed())
                },
            ))
            .await;
        pool.record_apply(&tables, elapsed);
        *subscriptions = owned;
        result
    }
}

/// Frees `value` on the pool. Dropping a replica subscription frees its row
/// maps and, for its last holder, the interned rows: up to ~100 ms of CPU for
/// a 9,000-row collection, which must not run on the network thread.
pub fn retire<T: Send + 'static>(value: T) {
    offload().detach(move || drop(value));
}

/// A socket's replica subscriptions. `clear`, `remove` and dropping the map
/// hand the removed subscriptions to [`retire`]; everything else is the map.
pub(crate) struct RetiringMap<K: Ord + Send + 'static, V: Send + 'static>(BTreeMap<K, V>);

impl<K: Ord + Send + 'static, V: Send + 'static> Default for RetiringMap<K, V> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<K: Ord + Send + 'static, V: Send + 'static> RetiringMap<K, V> {
    pub(crate) fn clear(&mut self) {
        if !self.0.is_empty() {
            retire(std::mem::take(&mut self.0));
        }
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.remove(key).map(retire).is_some()
    }
}

impl<K: Ord + Send + 'static, V: Send + 'static> std::ops::Deref for RetiringMap<K, V> {
    type Target = BTreeMap<K, V>;
    fn deref(&self) -> &BTreeMap<K, V> {
        &self.0
    }
}

impl<K: Ord + Send + 'static, V: Send + 'static> std::ops::DerefMut for RetiringMap<K, V> {
    fn deref_mut(&mut self) -> &mut BTreeMap<K, V> {
        &mut self.0
    }
}

impl<K: Ord + Send + 'static, V: Send + 'static> Drop for RetiringMap<K, V> {
    fn drop(&mut self) {
        self.clear();
    }
}

/// The distinct tables of a feed event, as the apply-cost key.
fn feed_tables(event: &FeedEvent) -> String {
    match event {
        FeedEvent::Transaction { changes, .. } => {
            let mut tables: Vec<&str> = changes.iter().map(|change| change.table.as_str()).collect();
            tables.sort_unstable();
            tables.dedup();
            tables.join(",")
        }
        FeedEvent::Reset { .. } => String::new(),
    }
}

/// Encodes snapshot pages one at a time. Large remainders are encoded on the
/// pool (the iterator moves there and back), so at most one encoded page per
/// socket exists at a time, as before; the network thread only writes it.
pub(crate) struct PageEncoder {
    pages: Option<crate::replica::SnapshotPages>,
}

const INLINE_PAGE_ROWS: usize = 64;

impl PageEncoder {
    pub(crate) fn new(pages: crate::replica::SnapshotPages) -> Self {
        Self { pages: Some(pages) }
    }

    pub(crate) async fn next(&mut self) -> Option<Result<String, serde_json::Error>> {
        let mut pages = self.pages.take()?;
        let pool = offload();
        let (pages, page) = if pool.is_inline() || pages.remaining_rows() < INLINE_PAGE_ROWS {
            let page = pages.next().map(|page| serde_json::to_string(&page));
            (pages, page)
        } else {
            pool.run(move || {
                let page = pages.next().map(|page| serde_json::to_string(&page));
                (pages, page)
            })
            .await
        };
        if page.is_some() {
            self.pages = Some(pages);
        }
        page
    }
}

/// Encodes `message` as JSON, on the pool for large snapshots and pages.
pub(crate) async fn encode_snapshot_json(
    message: ServerMessage,
) -> Result<String, serde_json::Error> {
    let rows = match &message {
        ServerMessage::ReplicaSnapshot { result, .. }
        | ServerMessage::ReplicaSnapshotPage { result, .. } => result.len(),
        ServerMessage::ReplicaSnapshotEncodedPage { canonical_rows, .. } => canonical_rows.len(),
        _ => 0,
    };
    let large = rows >= INLINE_PAGE_ROWS;
    if !large || offload().is_inline() {
        return serde_json::to_string(&message);
    }
    offload().run(move || serde_json::to_string(&message)).await
}

// ---------------------------------------------------------------------------
// Snapshot admission: a count bound plus an estimated byte budget.

/// Bounds concurrent replica snapshot builds by count and by the estimated
/// decoded heap they hold before interning (the transient part of a
/// snapshot build; the interned rows are retained by subscriptions anyway).
/// Each build reserves its estimate, the last observed size of the same
/// collection for the same member, else the largest observed size of the
/// table, else 1 MiB. Both semaphores are FIFO. A build larger than the whole
/// budget reserves the whole budget, so it runs alone instead of never.
pub struct SnapshotAdmission {
    count: tokio::sync::Semaphore,
    budget: tokio::sync::Semaphore,
    budget_kib: usize,
    estimates: Mutex<Estimates>,
    reserved_kib: AtomicUsize,
    peak_reserved_kib: AtomicUsize,
}

#[derive(Default)]
struct Estimates {
    by_key: HashMap<Box<str>, usize>,
    by_table: HashMap<Box<str>, usize>,
}

const UNKNOWN_SNAPSHOT_BYTES: usize = 8 << 20;
/// Decoded heap per byte of row JSON (`serde_json::Value` maps with string
/// keys). Perf fixture: an Admin's 9,000 task rows are ~16 MB of JSON and the
/// open peaks at +76 MB of heap (docs/performance/perf-fixture.md).
const HEAP_PER_JSON_BYTE: usize = 5;
const ESTIMATE_ENTRIES: usize = 65_536;

pub struct SnapshotPermit {
    admission: &'static SnapshotAdmission,
    _count: tokio::sync::SemaphorePermit<'static>,
    _bytes: tokio::sync::SemaphorePermit<'static>,
    reserved_kib: usize,
    key: Box<str>,
    table: Box<str>,
    admitted: Instant,
}

impl SnapshotAdmission {
    pub fn new(count: usize, budget_bytes: usize) -> Self {
        let budget_kib = (budget_bytes / 1024).clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            count: tokio::sync::Semaphore::new(count.max(1)),
            budget: tokio::sync::Semaphore::new(budget_kib),
            budget_kib,
            estimates: Mutex::default(),
            reserved_kib: AtomicUsize::new(0),
            peak_reserved_kib: AtomicUsize::new(0),
        }
    }

    /// The process-wide admission, configured from the environment.
    pub fn global() -> &'static Self {
        static ADMISSION: OnceLock<SnapshotAdmission> = OnceLock::new();
        ADMISSION.get_or_init(|| {
            let count = env_usize("GONVEX_REPLICA_SNAPSHOT_CONCURRENCY")
                .filter(|value| *value > 0)
                .unwrap_or_else(|| cpus().max(2));
            let budget = env_usize("GONVEX_REPLICA_SNAPSHOT_BUDGET_BYTES")
                .filter(|value| *value > 0)
                .unwrap_or(128 << 20);
            Self::new(count, budget)
        })
    }

    /// The bytes a build of `key` (in `table`) is expected to hold.
    pub fn estimate(&self, key: &str, table: &str) -> usize {
        let estimates = self.estimates.lock().unwrap();
        estimates
            .by_key
            .get(key)
            .or_else(|| estimates.by_table.get(table))
            .copied()
            .unwrap_or(UNKNOWN_SNAPSHOT_BYTES)
    }

    /// The largest estimate recorded for `table`, if any.
    pub fn table_estimate(&self, table: &str) -> Option<usize> {
        self.estimates.lock().unwrap().by_table.get(table).copied()
    }

    /// Waits for a count slot, then for the estimated bytes of `key`. The
    /// estimate is taken after the count wait, so a burst queued before any
    /// build finished still uses the sizes those builds observe.
    pub async fn admit(&'static self, key: String, table: &str) -> SnapshotPermit {
        let count = self
            .count
            .acquire()
            .await
            .expect("snapshot semaphore is never closed");
        let estimate = self.estimate(&key, table);
        let reserved_kib = estimate.div_ceil(1024).clamp(1, self.budget_kib);
        let bytes = self
            .budget
            .acquire_many(reserved_kib as u32)
            .await
            .expect("snapshot budget is never closed");
        let reserved = self.reserved_kib.fetch_add(reserved_kib, Relaxed) + reserved_kib;
        self.peak_reserved_kib.fetch_max(reserved, Relaxed);
        SnapshotPermit {
            admission: self,
            _count: count,
            _bytes: bytes,
            reserved_kib,
            key: key.into(),
            table: table.into(),
            admitted: Instant::now(),
        }
    }

    /// Highest reserved bytes since the last call (and reset to current).
    pub fn take_peak_reserved_bytes(&self) -> usize {
        let current = self.reserved_kib.load(Relaxed);
        self.peak_reserved_kib.swap(current, Relaxed) * 1024
    }

    /// Free count permits right now.
    pub fn available_permits(&self) -> usize {
        self.count.available_permits()
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget_kib * 1024
    }
}

impl SnapshotPermit {
    /// Records a finished streaming build of `json_bytes` of row JSON as the
    /// next estimate (same heap factor as [`Self::observe`]).
    pub fn observe_json_bytes(&self, json_bytes: usize) {
        self.record(json_bytes * HEAP_PER_JSON_BYTE);
    }

    /// Records the decoded size of the finished build as the next estimate.
    pub fn observe(&self, rows: &[Value]) {
        self.record(estimate_rows_bytes(rows));
    }

    fn record(&self, bytes: usize) {
        let mut estimates = self.admission.estimates.lock().unwrap();
        if estimates.by_key.len() >= ESTIMATE_ENTRIES {
            estimates.by_key.clear();
        }
        estimates.by_key.insert(self.key.clone(), bytes);
        let table = estimates.by_table.entry(self.table.clone()).or_insert(0);
        *table = (*table).max(bytes);
    }
}

impl Drop for SnapshotPermit {
    fn drop(&mut self) {
        self.admission.reserved_kib.fetch_sub(self.reserved_kib, Relaxed);
        gonvex_profiler::record_duration(
            gonvex_profiler::Stage::SnapshotPermitHold,
            gonvex_profiler::Label::None,
            self.admitted.elapsed(),
        );
    }
}

/// Estimated decoded heap of `rows`: their JSON size (extrapolated from up
/// to 64 sampled rows) times [`HEAP_PER_JSON_BYTE`].
fn estimate_rows_bytes(rows: &[Value]) -> usize {
    if rows.is_empty() {
        return 0;
    }
    let step = rows.len().div_ceil(64);
    let (count, bytes) = rows
        .iter()
        .step_by(step)
        .fold((0usize, 0usize), |(count, bytes), row| {
            (count + 1, bytes + crate::replica::json_byte_len(row))
        });
    bytes * rows.len() / count * HEAP_PER_JSON_BYTE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn leak(admission: SnapshotAdmission) -> &'static SnapshotAdmission {
        Box::leak(Box::new(admission))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_jobs_record_queue_wait_busy_and_run() {
        use gonvex_profiler::{Label, Stage};
        gonvex_profiler::set_enabled(true);
        let count = |stage: Stage| -> u64 {
            gonvex_profiler::snapshot()
                .into_iter()
                .filter(|row| row.stage == stage && row.label == Label::None)
                .map(|row| row.count)
                .sum()
        };
        let before = [Stage::OffloadQueueWait, Stage::OffloadBusy, Stage::OffloadRun].map(count);
        for strategy in [Strategy::Runtime, Strategy::Blocking, Strategy::Threads] {
            let pool = Offload::new(strategy, 1, 0, Duration::ZERO);
            assert_eq!(pool.run(|| 7).await, 7);
            let value = pool
                .spawn(async {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    8
                })
                .await;
            assert_eq!(value, 8);
            std::mem::forget(pool);
        }
        let after = [Stage::OffloadQueueWait, Stage::OffloadBusy, Stage::OffloadRun].map(count);
        for (before, after) in before.iter().zip(after) {
            assert!(after >= before + 6, "{before} -> {after}");
        }
        let run = gonvex_profiler::snapshot()
            .into_iter()
            .find(|row| row.stage == Stage::OffloadRun)
            .unwrap();
        assert!(run.max >= 2_000_000, "{}", run.max);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn burst_never_exceeds_the_byte_budget() {
        // 64 KiB budget, generous count: bytes are the binding bound.
        let admission = leak(SnapshotAdmission::new(64, 64 * 1024));
        // Teach sizes: 40 KiB for "big" keys, 4 KiB for the others.
        let big = vec![Value::String("x".repeat(40 * 1024 / HEAP_PER_JSON_BYTE - 2))];
        let small = vec![Value::String("x".repeat(4 * 1024 / HEAP_PER_JSON_BYTE - 2))];
        for n in 0..10 {
            let key = format!("member-{n}");
            let permit = admission.admit(key, "tasks").await;
            permit.observe(if n % 3 == 0 { &big } else { &small });
        }
        let held = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for burst in 0..200 {
            let held = held.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                let n = burst % 10;
                let permit = admission.admit(format!("member-{n}"), "tasks").await;
                let bytes = if n % 3 == 0 { 40 * 1024 } else { 4 * 1024 };
                let now = held.fetch_add(bytes, Relaxed) + bytes;
                peak.fetch_max(now, Relaxed);
                tokio::time::sleep(Duration::from_micros(200)).await;
                held.fetch_sub(bytes, Relaxed);
                drop(permit);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let peak = peak.load(Relaxed);
        assert!(peak <= 64 * 1024, "peak {peak} exceeds the budget");
        // More than one build ran at a time: the budget is not a mutex.
        assert!(peak > 40 * 1024, "peak {peak}: builds never overlapped");
        assert!(admission.take_peak_reserved_bytes() <= admission.budget_bytes());
    }

    #[tokio::test]
    async fn oversized_build_runs_alone_instead_of_never() {
        let admission = leak(SnapshotAdmission::new(8, 16 * 1024));
        let huge = vec![Value::String("x".repeat(100 * 1024))];
        admission.admit("huge".into(), "t").await.observe(&huge);
        let first = admission.admit("huge".into(), "t").await;
        // The whole budget is held: a small build waits.
        let waiting = admission.admit("small".into(), "other");
        tokio::pin!(waiting);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err());
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("admitted after the oversized build");
    }

    #[tokio::test]
    async fn unknown_keys_fall_back_to_the_table_maximum() {
        let admission = leak(SnapshotAdmission::new(2, 1 << 20));
        assert_eq!(admission.estimate("a", "tasks"), UNKNOWN_SNAPSHOT_BYTES);
        let rows: Vec<Value> = (0..1000).map(|n| serde_json::json!({ "n": n })).collect();
        admission.admit("a".into(), "tasks").await.observe(&rows);
        let exact: usize = rows.iter().map(crate::replica::json_byte_len).sum::<usize>()
            * HEAP_PER_JSON_BYTE;
        let estimate = admission.estimate("b", "tasks");
        assert!(estimate.abs_diff(exact) * 10 < exact, "{estimate} vs {exact}");
        assert_eq!(admission.estimate("a", "tasks"), estimate);
    }

    async fn strategy_round_trip(strategy: Strategy) {
        let pool = Offload::new(strategy, 2, 0, Duration::ZERO);
        assert_eq!(pool.spawn(async { 40 + 2 }).await, 42);
        assert_eq!(pool.run(|| "sync".to_owned()).await, "sync");
        // Dropping a handle cancels the future.
        let (exit, stopped) = tokio::sync::oneshot::channel::<()>();
        let (started, ready) = tokio::sync::oneshot::channel::<()>();
        let handle = pool.spawn(async move {
            let _exit = exit;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        drop(handle);
        assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
            .await
            .unwrap()
            .is_err());
        // Panics reach the awaiting caller.
        let panicked = AssertUnwindSafe(pool.spawn(async { panic!("boom") }))
            .catch_unwind()
            .await;
        assert!(panicked.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn every_strategy_runs_cancels_and_propagates_panics() {
        for strategy in [
            Strategy::Inline,
            Strategy::Runtime,
            Strategy::Blocking,
            Strategy::Threads,
        ] {
            strategy_round_trip(strategy).await;
        }
    }
}
