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

/// Registers the auto-extension once when a nice value is set.
pub fn install() {
    static INSTALLED: Once = Once::new();
    if nice() == 0 {
        return;
    }
    INSTALLED.call_once(|| unsafe {
        ffi::sqlite3_auto_extension(Some(entry_point));
    });
}

unsafe extern "C" fn entry_point(
    db: *mut ffi::sqlite3,
    _error: *mut *mut c_char,
    _api: *const ffi::sqlite3_api_routines,
) -> c_int {
    if ffi::sqlite3_db_readonly(db, c"main".as_ptr()) == 1 {
        // Per-thread on Linux: PRIO_PROCESS with a thread id.
        #[cfg(target_os = "linux")]
        {
            let thread = libc::syscall(libc::SYS_gettid) as libc::id_t;
            libc::setpriority(libc::PRIO_PROCESS, thread, nice());
        }
    }
    ffi::SQLITE_OK
}
