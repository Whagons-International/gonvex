//! Bounded preparation jobs owned by one socket. Dropping or clearing the queue
//! cancels every owned task; no detached task can publish into a later admission.
use futures_util::{
    future::{AbortHandle, Abortable, BoxFuture},
    stream::FuturesUnordered,
    FutureExt, StreamExt,
};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

/// Which of a socket's preparations may hold a read clock (burst control,
/// docs/performance/burst-control.md).
///
/// A preparation counts as *reading* from its first poll until the socket
/// takes its result (and installs the subscription in the same step), except
/// while it waits for snapshot admission ([`admission_wait`]): no read
/// transaction or clock of this open exists then. The socket applies feed
/// events to its installed subscriptions while no preparation is reading,
/// so a connection whose opens queue for seconds behind the process-wide
/// snapshot permits still receives its deliveries and call watermarks.
///
/// Ordering argument: a preparation marks itself reading (SeqCst) before it
/// reads its clock; the socket checks [`OpenProgress::reading`] (SeqCst)
/// after it received a feed event, which is broadcast only after its commit.
/// If the check sees no reader, any clock read later sees that commit, so the
/// new subscription's snapshot already contains the event it would skip.
#[derive(Default)]
pub(crate) struct OpenProgress {
    reading: AtomicUsize,
    settled: tokio::sync::Notify,
}

/// `GONVEX_FEED_DURING_ADMISSION=0`: count waiting preparations as reading
/// too (the socket defers its feed while any open is pending, as before
/// burst control).
fn feed_during_admission() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("GONVEX_FEED_DURING_ADMISSION").map_or(true, |value| value.trim() != "0")
    })
}

impl OpenProgress {
    pub fn reading(&self) -> bool {
        self.reading.load(SeqCst) > 0
    }
    /// Resolves after a preparation stopped reading (spurious wake-ups are
    /// possible; re-check [`Self::reading`]).
    pub async fn settled(&self) {
        self.settled.notified().await
    }
}

/// One preparation's reading state.
pub(crate) struct OpenState {
    progress: Arc<OpenProgress>,
    reading: AtomicBool,
}

impl OpenState {
    fn set(&self, reading: bool) {
        if self.reading.swap(reading, SeqCst) == reading {
            return;
        }
        if reading {
            self.progress.reading.fetch_add(1, SeqCst);
        } else if self.progress.reading.fetch_sub(1, SeqCst) == 1 {
            self.progress.settled.notify_one();
        }
    }
}

/// Marks its preparation reading while alive; dropped when the socket takes
/// the result (or the preparation is cancelled).
pub(crate) struct Reading(Arc<OpenState>);

impl Reading {
    fn new(state: Arc<OpenState>) -> Self {
        state.set(true);
        Self(state)
    }
}

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

tokio::task_local! {
    static OPEN: Arc<OpenState>;
}

/// Awaits `admission` (a snapshot permit) with the calling preparation, if
/// any, counted as not reading. Call it only where the preparation holds no
/// read transaction or clock of its own.
pub(crate) async fn admission_wait<F: Future>(admission: F) -> F::Output {
    let state = OPEN
        .try_with(Arc::clone)
        .ok()
        .filter(|_| feed_during_admission());
    if let Some(state) = &state {
        state.set(false);
    }
    let output = admission.await;
    if let Some(state) = &state {
        state.set(true);
    }
    output
}

/// Per-socket bound on pending preparations. Large tenants (El Rey) open far more
/// than 256 windows on a fresh replica; hitting the bound failed the page render.
const MAX_ENTRIES: usize = 8192;
pub(crate) const MAX_BYTES: usize = 32 * 1024 * 1024;

type Completion<T> = (String, u64, bool, T);
/// Preparations run on the offload pool (`crate::offload`), not on the
/// socket's runtime; dropping one cancels it.
type Preparation<T> = crate::offload::Offloaded<(bool, Arc<CompletionSlot<T>>)>;
// The socket's cancellation owner must be able to release an already finished
// value without polling its JoinHandle (for example while processing a close).
// The executor completion keeps only this small slot, not an inaccessible value.
struct CompletionSlot<T>(Mutex<CompletionState<T>>);
struct CompletionState<T> {
    cancelled: bool,
    value: Option<(Completion<T>, Reading)>,
}
impl<T> CompletionSlot<T> {
    fn new() -> Self {
        Self(Mutex::new(CompletionState {
            cancelled: false,
            value: None,
        }))
    }
    fn finish(&self, result: Option<Completion<T>>, reading: Reading) {
        let value = result.map(|result| (result, reading));
        let mut state = self.0.lock().unwrap();
        if !state.cancelled {
            state.value = value;
            return;
        }
        // Drop the rejected payload outside the lock. Cancellation and publish
        // are serialized, so a racing completion cannot recreate a closed value.
        drop(state);
        drop(value);
    }
    fn cancel(&self) {
        let value = {
            let mut state = self.0.lock().unwrap();
            state.cancelled = true;
            state.value.take()
        };
        drop(value);
    }
    fn take(&self) -> Option<(Completion<T>, Reading)> {
        self.0.lock().unwrap().value.take()
    }
}
struct Entry<T> {
    ticket: u64,
    bytes: usize,
    abort: AbortHandle,
    slot: Arc<CompletionSlot<T>>,
}
struct Waiting<T> {
    bulk: bool,
    slot: Arc<CompletionSlot<T>>,
    future: BoxFuture<'static, Option<Completion<T>>>,
}
pub(crate) struct ReplicaOpenQueue<T> {
    active: FuturesUnordered<Preparation<T>>,
    waiting: VecDeque<(String, Waiting<T>)>,
    entries: BTreeMap<String, Entry<T>>,
    bytes: usize,
    ticket: u64,
    bulk_active: usize,
    progress: Arc<OpenProgress>,
}
impl<T: Send + 'static> Default for ReplicaOpenQueue<T> {
    fn default() -> Self {
        Self {
            active: FuturesUnordered::new(),
            waiting: VecDeque::new(),
            entries: BTreeMap::new(),
            bytes: 0,
            ticket: 0,
            bulk_active: 0,
            progress: Arc::default(),
        }
    }
}
impl<T: Send + 'static> ReplicaOpenQueue<T> {
    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.waiting.is_empty()
    }
    /// Whether a preparation may hold a read clock ([`OpenProgress`]).
    pub fn reading(&self) -> bool {
        if !feed_during_admission() {
            return !self.is_empty();
        }
        self.progress.reading()
    }
    pub fn progress(&self) -> Arc<OpenProgress> {
        Arc::clone(&self.progress)
    }
    pub fn clear(&mut self) {
        for entry in self.entries.values() {
            entry.slot.cancel();
        }
        self.active.clear();
        self.waiting.clear();
        self.entries.clear();
        self.bytes = 0;
        self.bulk_active = 0;
    }
    pub fn remove(&mut self, id: &str) {
        if let Some(entry) = self.entries.remove(id) {
            self.bytes -= entry.bytes;
            entry.abort.abort();
            entry.slot.cancel();
        }
        self.waiting.retain(|(key, _)| key != id);
    }
    /// Includes replacements without cancelling the old job until admission.
    pub fn has_capacity(&self, id: &str, bytes: usize) -> bool {
        let replaced = self.entries.get(id);
        let count = self.entries.len() - usize::from(replaced.is_some());
        let retained_bytes = self.bytes - replaced.map_or(0, |entry| entry.bytes);
        count < MAX_ENTRIES && bytes <= MAX_BYTES - retained_bytes
    }
    pub fn push(
        &mut self,
        id: String,
        bytes: usize,
        bulk: bool,
        future: BoxFuture<'static, T>,
    ) -> Result<(), &'static str> {
        if !self.has_capacity(&id, bytes) {
            return Err("pending replica preparation allowance exceeded");
        }
        self.remove(&id);
        self.ticket = self.ticket.wrapping_add(1);
        let ticket = self.ticket;
        let (abort, registration) = AbortHandle::new_pair();
        self.bytes += bytes;
        let slot = Arc::new(CompletionSlot::new());
        self.entries.insert(
            id.clone(),
            Entry {
                ticket,
                bytes,
                abort,
                slot: Arc::clone(&slot),
            },
        );
        let key = id.clone();
        let future = Abortable::new(
            async move { (key, ticket, bulk, future.await) },
            registration,
        )
        .map(Result::ok)
        .boxed();
        self.waiting.push_back((id, Waiting { bulk, future, slot }));
        Ok(())
    }
    fn start(&mut self) {
        while self.active.len() < crate::REPLICA_OPEN_CONCURRENCY {
            // Keep one preparation slot available to a newly arriving small
            // source. Broad declared budgets can describe empty or tiny tables;
            // lend them the other slots instead of idling most of the pool.
            let next = self
                .waiting
                .iter()
                .position(|(_, job)| !job.bulk)
                .or_else(|| {
                    (self.bulk_active < crate::REPLICA_OPEN_CONCURRENCY - 1)
                        .then(|| self.waiting.iter().position(|(_, job)| job.bulk))
                        .flatten()
                });
            let Some(index) = next else { break };
            let (_, job) = self.waiting.remove(index).unwrap();
            if job.bulk {
                self.bulk_active += 1;
            }
            let bulk = job.bulk;
            let state = Arc::new(OpenState {
                progress: Arc::clone(&self.progress),
                reading: AtomicBool::new(false),
            });
            // The socket may await a query that needs a connection held by a
            // preparation. Keep SQL progress independent of polling next().
            // The queue owns cancellation and remains the only publisher.
            self.active
                .push(crate::offload::offload().spawn(gonvex_profiler::instrument(
                    gonvex_profiler::TaskKind::Snapshot,
                    async move {
                        // Reading from the first poll; returned with the result
                        // so it stays reading until the socket installs it.
                        let reading = Reading::new(Arc::clone(&state));
                        let result = OPEN.scope(state, job.future).await;
                        job.slot.finish(result, reading);
                        (bulk, job.slot)
                    },
                )));
        }
    }
    pub async fn next(&mut self) -> Option<T> {
        loop {
            self.start();
            // `_reading` drops when this returns: the caller installs the
            // value before its next await, so no feed event is applied
            // between this preparation's clock and its installation.
            let (bulk, slot) = self.active.next().await?;
            if bulk {
                self.bulk_active -= 1;
            }
            if let Some(((id, ticket, _, value), _reading)) = slot.take() {
                if self
                    .entries
                    .get(&id)
                    .is_some_and(|entry| entry.ticket == ticket)
                {
                    let entry = self.entries.remove(&id).unwrap();
                    self.bytes -= entry.bytes;
                    return Some(value);
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn close_releases_completed_payload_and_read_fence_without_polling() {
        struct Payload(Option<tokio::sync::oneshot::Sender<()>>, Vec<u8>);
        impl Drop for Payload {
            fn drop(&mut self) {
                if let Some(s) = self.0.take() {
                    let _ = s.send(());
                }
            }
        }
        for replace in [false, true] {
            let mut queue = ReplicaOpenQueue::default();
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let (dropped, drop_ack) = tokio::sync::oneshot::channel();
            queue
                .push(
                    "id".into(),
                    1024,
                    false,
                    async move {
                        entered.send(()).unwrap();
                        released.await.unwrap();
                        Payload(Some(dropped), vec![7; 1 << 20])
                    }
                    .boxed(),
                )
                .unwrap();
            {
                let next = queue.next();
                tokio::pin!(next);
                tokio::select! {_=&mut next=>panic!("gate closed"),r=started=>r.unwrap()}
            }
            release.send(()).unwrap();
            // Wait until the offload job published into its connection-owned slot.
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while queue.entries["id"].slot.0.lock().unwrap().value.is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(queue.reading());
            if replace {
                queue
                    .push(
                        "id".into(),
                        1024,
                        false,
                        std::future::pending::<Payload>().boxed(),
                    )
                    .unwrap();
            } else {
                queue.remove("id");
            }
            tokio::time::timeout(std::time::Duration::from_millis(100), drop_ack)
                .await
                .expect("completed payload survived close/replacement without polling")
                .unwrap();
            assert!(!queue.reading(), "closed completion still defers feed");
            queue.clear();
        }
    }
    #[tokio::test]
    async fn cancellation_drops_started_work_without_polling_completion() {
        struct Exit(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Exit {
            fn drop(&mut self) {
                if let Some(exit) = self.0.take() {
                    let _ = exit.send(());
                }
            }
        }
        for mode in ["clear", "remove", "drop"] {
            let (started, ready) = tokio::sync::oneshot::channel();
            let (exit, stopped) = tokio::sync::oneshot::channel();
            let mut queue = Some(ReplicaOpenQueue::default());
            queue
                .as_mut()
                .unwrap()
                .push(
                    "owned".into(),
                    128,
                    false,
                    async move {
                        let _exit = Exit(Some(exit));
                        let _ = started.send(());
                        std::future::pending::<()>().await;
                    }
                    .boxed(),
                )
                .unwrap();
            {
                let pending = queue.as_mut().unwrap().next();
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("preparation completed before cancellation"),
                    ready = ready => ready.unwrap(),
                }
            }
            match mode {
                "clear" => queue.as_mut().unwrap().clear(),
                "remove" => queue.as_mut().unwrap().remove("owned"),
                _ => {
                    queue.take();
                }
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), stopped)
                .await
                .expect("cancelled work survived without completion polling")
                .unwrap();
        }
    }
    #[tokio::test]
    async fn publishes_small_source_without_waiting_for_bulk_and_reserves_capacity() {
        let mut queue = ReplicaOpenQueue::default();
        for n in 0..8 {
            queue
                .push(
                    format!("bulk-{n}"),
                    1024,
                    true,
                    std::future::pending::<usize>().boxed(),
                )
                .unwrap();
        }
        queue
            .push("metadata".into(), 1024, false, async { 42 }.boxed())
            .unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), queue.next())
                .await
                .unwrap(),
            Some(42)
        );
        assert_eq!(queue.bulk_active, crate::REPLICA_OPEN_CONCURRENCY - 1);
        assert_eq!(queue.waiting.len(), 1);
        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(queue.bytes, 0);
    }
    #[tokio::test]
    async fn lends_unused_capacity_without_blocking_a_later_small_source() {
        let mut queue = ReplicaOpenQueue::default();
        for n in 0..crate::REPLICA_OPEN_CONCURRENCY {
            let future = async move {
                // A later directory can have a broad declared budget but finish
                // immediately while earlier SQL snapshots remain suspended.
                if n == crate::REPLICA_OPEN_CONCURRENCY - 2 {
                    42
                } else {
                    std::future::pending::<usize>().await
                }
            };
            queue
                .push(format!("broad-{n}"), 1024, true, future.boxed())
                .unwrap();
        }
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), queue.next())
                .await
                .unwrap(),
            Some(42)
        );
        // Fill the lent capacity with suspended broad work, then admit a newly
        // arriving small source independently. Neither assertion reads slots.
        queue.start();
        queue
            .push("later-small".into(), 1024, false, async { 99 }.boxed())
            .unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), queue.next())
                .await
                .unwrap(),
            Some(99)
        );
    }
    #[tokio::test]
    async fn close_and_replacement_never_publish_obsolete_work() {
        let mut queue = ReplicaOpenQueue::default();
        queue
            .push("same".into(), 100, false, async { 1 }.boxed())
            .unwrap();
        queue.start();
        queue
            .push("same".into(), 100, false, async { 2 }.boxed())
            .unwrap();
        assert_eq!(queue.next().await, Some(2));
        assert_eq!(queue.bytes, 0);
        queue
            .push("closed".into(), 100, false, async { 3 }.boxed())
            .unwrap();
        queue.start();
        queue.remove("closed");
        assert_eq!(queue.next().await, None);
        assert!(queue.is_empty());
    }
    async fn stream_above_allowance(count: usize, bytes: usize) {
        let mut queue = ReplicaOpenQueue::default();
        let mut completed = std::collections::BTreeSet::new();
        let mut backpressured = 0;
        for n in 0..count {
            let id = n.to_string();
            while !queue.has_capacity(&id, bytes) {
                backpressured += 1;
                assert!(
                    completed.insert(queue.next().await.expect("queued preparation must drain")),
                    "a preparation published twice"
                );
            }
            queue
                .push(id, bytes, n % 2 == 0, async move { n }.boxed())
                .unwrap();
            assert!(queue.entries.len() <= MAX_ENTRIES);
            assert!(queue.bytes <= MAX_BYTES);
        }
        while let Some(n) = queue.next().await {
            assert!(completed.insert(n), "a preparation published twice");
        }
        assert!(backpressured > 0);
        assert_eq!(completed, (0..count).collect());
        assert_eq!(queue.bytes, 0);
    }
    #[tokio::test]
    async fn streams_more_sources_than_the_count_allowance_without_an_error() {
        stream_above_allowance(MAX_ENTRIES + 257, 64).await;
    }
    #[tokio::test]
    async fn streams_more_request_bytes_than_the_allowance_without_an_error() {
        stream_above_allowance(300, MAX_BYTES / 8).await;
    }
    #[tokio::test]
    async fn rejected_replacement_keeps_the_previous_preparation() {
        let mut queue = ReplicaOpenQueue::default();
        queue
            .push("same".into(), MAX_BYTES, false, async { 42 }.boxed())
            .unwrap();
        assert!(queue
            .push("same".into(), MAX_BYTES + 1, false, async { 99 }.boxed())
            .is_err());
        assert_eq!(queue.next().await, Some(42));
    }
    #[tokio::test]
    async fn enforces_bytes_and_count_for_both_queued_and_active_work() {
        let mut queue = ReplicaOpenQueue::default();
        queue
            .push(
                "large".into(),
                MAX_BYTES,
                false,
                std::future::pending::<()>().boxed(),
            )
            .unwrap();
        queue.start();
        assert!(queue
            .push("overflow".into(), 1, false, async {}.boxed())
            .is_err());
        queue.clear();
        for n in 0..MAX_ENTRIES {
            queue
                .push(n.to_string(), 1, true, std::future::pending::<()>().boxed())
                .unwrap();
        }
        assert!(queue
            .push("overflow".into(), 1, false, async {}.boxed())
            .is_err());
        queue.remove("0");
        assert!(queue
            .push("replacement".into(), 1, false, async {}.boxed())
            .is_ok());
    }
}
