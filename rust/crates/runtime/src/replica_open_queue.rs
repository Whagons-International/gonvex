//! Bounded preparation jobs owned by one socket. Dropping or clearing the queue
//! drops every SQL future; no detached task can publish into a later admission.
use futures_util::{
    future::{AbortHandle, Abortable, BoxFuture},
    stream::FuturesUnordered,
    FutureExt, StreamExt,
};
use std::collections::{BTreeMap, VecDeque};

type Completion<T> = (String, u64, bool, T);
struct Entry {
    ticket: u64,
    bytes: usize,
    abort: AbortHandle,
}
struct Waiting<T> {
    bulk: bool,
    future: BoxFuture<'static, Option<Completion<T>>>,
}
pub(crate) struct ReplicaOpenQueue<T> {
    active: FuturesUnordered<BoxFuture<'static, (bool, Option<Completion<T>>)>>,
    waiting: VecDeque<(String, Waiting<T>)>,
    entries: BTreeMap<String, Entry>,
    bytes: usize,
    ticket: u64,
    bulk_active: usize,
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
        }
    }
}
impl<T: Send + 'static> ReplicaOpenQueue<T> {
    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.waiting.is_empty()
    }
    pub fn clear(&mut self) {
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
        }
        self.waiting.retain(|(key, _)| key != id);
    }
    pub fn push(
        &mut self,
        id: String,
        bytes: usize,
        bulk: bool,
        future: BoxFuture<'static, T>,
    ) -> Result<(), &'static str> {
        self.remove(&id);
        if self.entries.len() >= 256 || bytes > 524288 - self.bytes {
            return Err("pending replica preparation allowance exceeded");
        }
        self.ticket = self.ticket.wrapping_add(1);
        let ticket = self.ticket;
        let (abort, registration) = AbortHandle::new_pair();
        self.bytes += bytes;
        self.entries.insert(
            id.clone(),
            Entry {
                ticket,
                bytes,
                abort,
            },
        );
        let key = id.clone();
        let future = Abortable::new(
            async move { (key, ticket, bulk, future.await) },
            registration,
        )
        .map(Result::ok)
        .boxed();
        self.waiting.push_back((id, Waiting { bulk, future }));
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
            self.active
                .push(async move { (bulk, job.future.await) }.boxed());
        }
    }
    pub async fn next(&mut self) -> Option<T> {
        loop {
            self.start();
            let (bulk, result) = self.active.next().await?;
            if bulk {
                self.bulk_active -= 1;
            }
            if let Some((id, ticket, _, value)) = result {
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
    #[tokio::test]
    async fn enforces_bytes_and_count_for_both_queued_and_active_work() {
        let mut queue = ReplicaOpenQueue::default();
        queue
            .push(
                "large".into(),
                524288,
                false,
                std::future::pending::<()>().boxed(),
            )
            .unwrap();
        queue.start();
        assert!(queue
            .push("overflow".into(), 1, false, async {}.boxed())
            .is_err());
        queue.clear();
        for n in 0..256 {
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
