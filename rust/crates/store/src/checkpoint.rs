//! WAL checkpoints off the writer.
//!
//! SQLite's automatic checkpoint (`wal_autocheckpoint`, 1,000 pages) runs
//! inside the COMMIT that crosses the threshold, on the database's single
//! writer: that commit copies the WAL into the database file and syncs both
//! while every other write transaction of the tenant waits behind it.
//!
//! Instead the writer gets a WAL hook (which replaces the automatic
//! checkpoint). After each commit it records the WAL size and, every
//! [`checkpoint_pages`] frames, asks one process-wide checkpoint thread to run
//! `PASSIVE` on its own connection to that file. A passive checkpoint never
//! takes the write lock, so commits continue while it copies and syncs; once
//! it has copied every frame, the next write restarts the WAL from the start.
//!
//! Fallback: when the WAL reaches [`wal_cap_pages`] (under a steady write
//! stream it does, see there) the writer checkpoints inline as SQLite would
//! have, so the WAL stays bounded and restarts.
//!
//! `GONVEX_SQLITE_CHECKPOINT=writer` restores SQLite's automatic checkpoint.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CString};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use libsqlite3_sys as ffi;
use sqlx::SqliteConnection;

use crate::DatabaseError;

/// Checkpoints run by the background thread (observability and tests).
pub static BACKGROUND_CHECKPOINTS: AtomicU64 = AtomicU64::new(0);
/// Inline checkpoint attempts by the writer because the WAL reached its cap
/// (an attempt that found a background checkpoint running is retried).
pub static INLINE_CHECKPOINTS: AtomicU64 = AtomicU64::new(0);
/// Total and longest background checkpoint time in microseconds.
pub static BACKGROUND_CHECKPOINT_US: AtomicU64 = AtomicU64::new(0);
pub static BACKGROUND_CHECKPOINT_MAX_US: AtomicU64 = AtomicU64::new(0);

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// Whether checkpoints run on the background thread (default) or inside
/// the writer's COMMIT (`GONVEX_SQLITE_CHECKPOINT=writer`).
pub fn background_enabled() -> bool {
    !std::env::var("GONVEX_SQLITE_CHECKPOINT")
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("writer")
}

/// WAL frames between background checkpoints
/// (`GONVEX_SQLITE_CHECKPOINT_PAGES`, default 1,000 like SQLite's own).
fn checkpoint_pages() -> u64 {
    env_u64("GONVEX_SQLITE_CHECKPOINT_PAGES", 1_000).max(1)
}

/// WAL size at which the writer checkpoints inline after all
/// (`GONVEX_SQLITE_WAL_CAP_PAGES`, default 16,000 frames ≈ 64 MiB).
///
/// Under a steady write stream the background checkpoint copies frames but
/// never catches up completely (commits land while it syncs), and only a
/// fully copied WAL can restart from its beginning. So the WAL does grow to
/// this cap, where the writer copies the small uncopied tail and restarts it.
/// That inline checkpoint costs two fsyncs whatever its size, so a larger cap
/// means proportionally fewer stalls (docs/performance/sqlite-writer.md: 4,000
/// frames kept p99 at 34-88 ms, 16,000 at 11 ms).
fn wal_cap_pages() -> u64 {
    env_u64("GONVEX_SQLITE_WAL_CAP_PAGES", 16_000).max(checkpoint_pages())
}

/// One database file's WAL bookkeeping, shared by its writer's hook and the
/// checkpoint thread.
pub(crate) struct WalState {
    path: CString,
    every: u64,
    cap: u64,
    /// Frames in the WAL after the last commit.
    frames: AtomicU64,
    /// Frame count at which the next background checkpoint is requested.
    next_request: AtomicU64,
    /// Frame count at which the writer next checkpoints inline.
    next_inline: AtomicU64,
    /// A request is queued and not yet served.
    queued: AtomicBool,
}

impl WalState {
    pub(crate) fn new(path: &Path) -> Arc<Self> {
        let every = checkpoint_pages();
        let cap = wal_cap_pages();
        Arc::new(Self {
            path: CString::new(path.as_os_str().as_encoded_bytes()).unwrap_or_default(),
            every,
            cap,
            frames: AtomicU64::new(0),
            next_request: AtomicU64::new(every),
            next_inline: AtomicU64::new(cap),
            queued: AtomicBool::new(false),
        })
    }

    /// Called by the writer after each commit with the WAL's frame count.
    /// Returns true when the writer must checkpoint inline (cap reached).
    fn committed(self: &Arc<Self>, frames: u64) -> bool {
        let previous = self.frames.swap(frames, Ordering::Relaxed);
        if frames < previous {
            // The WAL restarted after a complete checkpoint.
            self.next_request.store(self.every, Ordering::Relaxed);
            self.next_inline.store(self.cap, Ordering::Relaxed);
        }
        // Past the cap the writer checkpoints itself; a concurrent background
        // checkpoint would only make it fail with SQLITE_BUSY.
        if frames < self.cap && frames >= self.next_request.load(Ordering::Relaxed) {
            self.next_request
                .store(frames + self.every, Ordering::Relaxed);
            if !self.queued.swap(true, Ordering::AcqRel) {
                checkpointer().send(Message::Checkpoint(self.clone())).ok();
            }
        }
        if frames >= self.next_inline.load(Ordering::Relaxed) {
            self.next_inline
                .store(frames + self.every, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Closes the checkpoint thread's connection to this file and waits for
    /// it (call when the database closes).
    pub(crate) async fn close(self: &Arc<Self>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        if checkpointer()
            .send(Message::Close(self.path.clone(), sender))
            .is_ok()
        {
            let _ = receiver.await;
        }
    }
}

enum Message {
    Checkpoint(Arc<WalState>),
    Close(CString, tokio::sync::oneshot::Sender<()>),
}

fn checkpointer() -> &'static mpsc::Sender<Message> {
    static SENDER: OnceLock<mpsc::Sender<Message>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("gonvex-sqlite-checkpoint".into())
            .spawn(move || run(receiver))
            .expect("spawn the SQLite checkpoint thread");
        sender
    })
}

struct Connection(*mut ffi::sqlite3);

// Used only on the checkpoint thread.
unsafe impl Send for Connection {}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_close(self.0) };
    }
}

fn open(path: &CString) -> Option<Connection> {
    let mut db = std::ptr::null_mut();
    // READWRITE without CREATE: a database that was removed stays removed.
    let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_NOMUTEX;
    let rc = unsafe { ffi::sqlite3_open_v2(path.as_ptr(), &mut db, flags, std::ptr::null()) };
    if rc != ffi::SQLITE_OK {
        unsafe { ffi::sqlite3_close(db) };
        return None;
    }
    // The checkpoint copies pages straight from the WAL; it needs no cache.
    unsafe {
        ffi::sqlite3_exec(
            db,
            c"PRAGMA cache_size = -64".as_ptr(),
            None,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }
    Some(Connection(db))
}

fn run(receiver: mpsc::Receiver<Message>) {
    let mut connections: HashMap<CString, (Weak<WalState>, Connection)> = HashMap::new();
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Checkpoint(state) => {
                state.queued.store(false, Ordering::Release);
                if state.frames.load(Ordering::Relaxed) >= state.cap {
                    continue;
                }
                if !connections.contains_key(&state.path) {
                    let Some(connection) = open(&state.path) else {
                        continue;
                    };
                    connections.insert(state.path.clone(), (Arc::downgrade(&state), connection));
                }
                let (_, connection) = &connections[&state.path];
                let started = Instant::now();
                let (mut log, mut copied) = (0, 0);
                unsafe {
                    ffi::sqlite3_wal_checkpoint_v2(
                        connection.0,
                        std::ptr::null(),
                        ffi::SQLITE_CHECKPOINT_PASSIVE,
                        &mut log,
                        &mut copied,
                    )
                };
                let elapsed = started.elapsed();
                gonvex_profiler::record_duration(
                    gonvex_profiler::Stage::SqliteCheckpoint,
                    gonvex_profiler::Label::None,
                    elapsed,
                );
                gonvex_profiler::record(
                    gonvex_profiler::Stage::SqliteCheckpointPages,
                    gonvex_profiler::Label::None,
                    copied.max(0) as u64,
                );
                let micros = elapsed.as_micros() as u64;
                BACKGROUND_CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
                BACKGROUND_CHECKPOINT_US.fetch_add(micros, Ordering::Relaxed);
                BACKGROUND_CHECKPOINT_MAX_US.fetch_max(micros, Ordering::Relaxed);
            }
            Message::Close(path, done) => {
                connections.remove(&path);
                let _ = done.send(());
            }
        }
        // Databases dropped without an explicit close.
        connections.retain(|_, (state, _)| state.strong_count() > 0);
    }
}

unsafe extern "C" fn wal_hook(
    context: *mut c_void,
    db: *mut ffi::sqlite3,
    _name: *const c_char,
    frames: c_int,
) -> c_int {
    let state = &*(context as *const Arc<WalState>);
    let frames = frames.max(0) as u64;
    if state.committed(frames) {
        INLINE_CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
        let _timed = gonvex_profiler::ScopeTimer::new(
            gonvex_profiler::Stage::SqliteCheckpointInline,
            gonvex_profiler::Label::None,
        );
        let rc = ffi::sqlite3_wal_checkpoint_v2(
            db,
            std::ptr::null(),
            ffi::SQLITE_CHECKPOINT_PASSIVE,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        if rc == ffi::SQLITE_BUSY {
            // A background checkpoint is still running: retry next commit.
            state.next_inline.store(frames + 1, Ordering::Relaxed);
        }
    }
    ffi::SQLITE_OK
}

/// Owns the hook's `Arc<WalState>`; SQLite drops it when the connection
/// closes (it is the user data of a function that is never called).
unsafe extern "C" fn drop_state(data: *mut c_void) {
    drop(Box::from_raw(data as *mut Arc<WalState>));
}

unsafe extern "C" fn unused_function(
    context: *mut ffi::sqlite3_context,
    _argc: c_int,
    _argv: *mut *mut ffi::sqlite3_value,
) {
    ffi::sqlite3_result_null(context);
}

/// Installs the WAL hook on a writer connection (replacing SQLite's
/// automatic checkpoint).
pub(crate) async fn install(
    connection: &mut SqliteConnection,
    state: Arc<WalState>,
) -> Result<(), DatabaseError> {
    let mut handle = connection.lock_handle().await?;
    let db = handle.as_raw_handle().as_ptr();
    let data = Box::into_raw(Box::new(state));
    unsafe {
        let rc = ffi::sqlite3_create_function_v2(
            db,
            c"gonvex_wal_state".as_ptr(),
            0,
            ffi::SQLITE_UTF8,
            data.cast(),
            Some(unused_function),
            None,
            None,
            Some(drop_state),
        );
        if rc != ffi::SQLITE_OK {
            drop(Box::from_raw(data));
            return Err(DatabaseError::Decode(format!(
                "register WAL state: code {rc}"
            )));
        }
        ffi::sqlite3_wal_hook(db, Some(wal_hook), data.cast());
    }
    Ok(())
}
