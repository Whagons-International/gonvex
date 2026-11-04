//! Batched durability for commits.
//!
//! With `synchronous=FULL`, every COMMIT waits for a WAL sync while holding
//! the database's single writer. A ~40 ms sync then caps a tenant at ~25
//! commits per second while the CPU idles. Instead, connections run with
//! `synchronous=NORMAL` (COMMIT writes the WAL but does not sync it), and a
//! committed write transaction waits here before it is acknowledged or its
//! revision is published. One `fdatasync` of the WAL file covers every commit
//! that finished before the sync started, so concurrent commits share a sync
//! and the writer is never held across one.
//!
//! An acknowledged commit is therefore as durable as with FULL: its WAL
//! frames reached the disk before anyone was told about it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::DatabaseError;

/// WAL syncs run by every barrier in this process (observability and tests).
pub static WAL_SYNCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Debug, Default)]
struct Synced {
    /// Highest ticket covered by a finished sync.
    ticket: u64,
    /// Set when that sync failed (its commits' durability is unknown).
    error: Option<String>,
}

#[derive(Default)]
struct State {
    issued: u64,
    running: bool,
}

pub(crate) struct SyncBarrier {
    wal: PathBuf,
    state: Mutex<State>,
    synced: watch::Sender<Synced>,
}

impl SyncBarrier {
    pub(crate) fn new(database: &std::path::Path) -> Arc<Self> {
        let mut wal = database.as_os_str().to_owned();
        wal.push("-wal");
        Arc::new(Self {
            wal: PathBuf::from(wal),
            state: Mutex::default(),
            synced: watch::channel(Synced::default()).0,
        })
    }

    /// Returns once everything committed before this call is on disk.
    pub(crate) async fn wait(self: &Arc<Self>) -> Result<(), DatabaseError> {
        let (ticket, start) = {
            let mut state = self.state.lock().unwrap();
            state.issued += 1;
            let start = !state.running;
            state.running = true;
            (state.issued, start)
        };
        let mut synced = self.synced.subscribe();
        if start {
            let barrier = self.clone();
            tokio::spawn(async move { barrier.run().await });
        }
        loop {
            {
                let current = synced.borrow_and_update();
                if current.ticket >= ticket {
                    return match &current.error {
                        None => Ok(()),
                        Some(error) => Err(DatabaseError::Io(format!("WAL sync failed: {error}"))),
                    };
                }
            }
            synced
                .changed()
                .await
                .map_err(|_| DatabaseError::Io("WAL sync barrier closed".into()))?;
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            let target = self.state.lock().unwrap().issued;
            WAL_SYNCS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let wal = self.wal.clone();
            let result = tokio::task::spawn_blocking(move || sync_file(&wal))
                .await
                .map_err(|error| error.to_string())
                .and_then(|result| result.map_err(|error| error.to_string()));
            self.synced.send_replace(Synced {
                ticket: target,
                error: result.err(),
            });
            let mut state = self.state.lock().unwrap();
            if state.issued == target {
                state.running = false;
                return;
            }
        }
    }
}

fn sync_file(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(file) => file.sync_data(),
        // No WAL file means no frames are waiting to be synced.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_waiters_share_syncs_and_all_return() {
        let dir = std::env::temp_dir().join(format!("barrier-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("b.db");
        std::fs::write(format!("{}-wal", db.display()), b"frames").unwrap();
        let barrier = SyncBarrier::new(&db);
        let mut tasks = Vec::new();
        for _ in 0..200 {
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move { barrier.wait().await }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(barrier.synced.borrow().ticket, 200);
        assert!(!barrier.state.lock().unwrap().running);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
