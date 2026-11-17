//! Optional lower OS priority for SQLite reader threads
//! (`GONVEX_SQLITE_READER_NICE`, default 10; 0 = unchanged).
//!
//! sqlx runs each connection on its own worker thread and opens the
//! connection on it, so an SQLite auto-extension (called inside
//! `sqlite3_open`) runs on that thread. Read-only connections serve snapshot
//! scans, change-log reads and visibility; under CPU saturation they starve
//! the writer and the module isolate, whose work users wait on. A positive
//! nice value lets the scheduler prefer those. Writers are never changed.

use std::ffi::{c_char, c_int};
use std::sync::{Once, OnceLock};

use libsqlite3_sys as ffi;

fn nice() -> i32 {
    static NICE: OnceLock<i32> = OnceLock::new();
    *NICE.get_or_init(|| {
        std::env::var("GONVEX_SQLITE_READER_NICE")
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok())
            .unwrap_or(10)
            .clamp(0, 19)
    })
}

/// The process's base-priority tokio runtime (the network runtime), set once
/// by the binary. Writer pools open on it, so their sqlx worker threads (and
/// sqlx's pool maintenance task) never start from a niced offload thread.
static HOME: OnceLock<tokio::runtime::Handle> = OnceLock::new();

pub fn set_home_runtime(handle: tokio::runtime::Handle) {
    let _ = HOME.set(handle);
}

/// Runs `future` on the home runtime when the caller's thread is niced above
/// the process base, else in place.
pub(crate) async fn at_base_priority<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    use gonvex_profiler::thread_priority as priority;
    match HOME.get() {
        Some(home) if priority::current() > priority::base() => match home.spawn(future).await {
            Ok(value) => value,
            Err(error) => std::panic::resume_unwind(error.into_panic()),
        },
        _ => future.await,
    }
}

/// Registers the auto-extension once. It sets every connection's worker
/// thread to an absolute target: base + `nice()` for read-only connections,
/// base for writers (a writer opened from a niced thread would otherwise
/// inherit its creator's nice value; see `gonvex_profiler::thread_priority`).
pub fn install() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| unsafe {
        ffi::sqlite3_auto_extension(Some(entry_point));
    });
}

unsafe extern "C" fn entry_point(
    db: *mut ffi::sqlite3,
    _error: *mut *mut c_char,
    _api: *const ffi::sqlite3_api_routines,
) -> c_int {
    // Runs inside sqlite3_open, on the connection's own sqlx worker thread.
    let read_only = ffi::sqlite3_db_readonly(db, c"main".as_ptr()) == 1;
    let offset = if read_only { nice() } else { 0 };
    if !gonvex_profiler::thread_priority::set_current_target(offset) && !read_only {
        static WARNED: Once = Once::new();
        WARNED.call_once(|| {
            eprintln!(
                "gonvex-store: a SQLite writer connection was opened from a niced thread (nice {}) and cannot return to base priority (no CAP_SYS_NICE)",
                gonvex_profiler::thread_priority::current()
            )
        });
    }
    ffi::SQLITE_OK
}
