//! Per-statement SQLite time (perf diagnostics, `GONVEX_SQLITE_STATEMENT_PROFILE=1`).
//!
//! Registered as an SQLite auto-extension, so every connection the process
//! opens afterwards (sqlx readers and writer, direct sessions) reports each
//! finished statement through `sqlite3_trace_v2(SQLITE_TRACE_PROFILE)`: the
//! statement's SQL text (unexpanded: parameters stay `$n`/`?`), how long it
//! ran (SQLite's own clock, from first step to reset/finalize) and how many
//! VM steps it took. Off by default: the callback hashes the SQL text.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_uint, c_void, CStr};
use std::sync::{Mutex, Once, OnceLock};

use libsqlite3_sys as ffi;

#[derive(Clone, Copy, Debug, Default)]
pub struct StatementStats {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    /// `SQLITE_STMTSTATUS_VM_STEP` summed (work, independent of waiting).
    pub vm_steps: u64,
    /// Rows a full table scan stepped through (`SQLITE_STMTSTATUS_FULLSCAN_STEP`).
    pub fullscan_steps: u64,
}

fn table() -> &'static Mutex<HashMap<Box<str>, StatementStats>> {
    static TABLE: OnceLock<Mutex<HashMap<Box<str>, StatementStats>>> = OnceLock::new();
    TABLE.get_or_init(Default::default)
}

/// Whether the profile is on (environment, read once).
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("GONVEX_SQLITE_STATEMENT_PROFILE")
            .is_ok_and(|value| matches!(value.trim(), "1" | "true" | "on"))
    })
}

/// Registers the auto-extension once (no-op unless [`enabled`]). Call before
/// the first connection opens.
pub fn install() {
    static INSTALLED: Once = Once::new();
    if !enabled() {
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
    ffi::sqlite3_trace_v2(
        db,
        ffi::SQLITE_TRACE_PROFILE as c_uint,
        Some(on_trace),
        std::ptr::null_mut(),
    );
    ffi::SQLITE_OK
}

pub(crate) unsafe extern "C" fn on_trace(
    _kind: c_uint,
    _context: *mut c_void,
    statement: *mut c_void,
    elapsed: *mut c_void,
) -> c_int {
    let statement = statement as *mut ffi::sqlite3_stmt;
    let ns = *(elapsed as *const i64) as u64;
    let sql = ffi::sqlite3_sql(statement);
    if sql.is_null() {
        return 0;
    }
    let steps = ffi::sqlite3_stmt_status(statement, ffi::SQLITE_STMTSTATUS_VM_STEP, 1) as u64;
    let scans = ffi::sqlite3_stmt_status(statement, ffi::SQLITE_STMTSTATUS_FULLSCAN_STEP, 1) as u64;
    let text = CStr::from_ptr(sql).to_string_lossy();
    let text = normalize(&text);
    let mut table = table().lock().unwrap_or_else(|error| error.into_inner());
    let stats = match table.get_mut(text.as_str()) {
        Some(stats) => stats,
        None => table.entry(text.into_boxed_str()).or_default(),
    };
    stats.count += 1;
    stats.total_ns += ns;
    stats.max_ns = stats.max_ns.max(ns);
    stats.vm_steps += steps;
    stats.fullscan_steps += scans;
    0
}

/// Collapses whitespace and cuts very long texts (the key, not the report).
fn normalize(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len().min(600));
    let mut space = false;
    for character in sql.chars() {
        if character.is_whitespace() {
            space = true;
            continue;
        }
        if space && !out.is_empty() {
            out.push(' ');
        }
        space = false;
        out.push(character);
        if out.len() >= 600 {
            out.push('…');
            break;
        }
    }
    out
}

/// Every statement seen so far, most total time first.
pub fn snapshot() -> Vec<(String, StatementStats)> {
    let table = table().lock().unwrap_or_else(|error| error.into_inner());
    let mut rows = table
        .iter()
        .map(|(sql, stats)| (sql.to_string(), *stats))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| std::cmp::Reverse(row.1.total_ns));
    rows
}

/// Clears the table (benchmarks: measure one phase).
pub fn reset() {
    table()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clear();
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn normalizes_whitespace() {
        assert_eq!(
            normalize("  SELECT\n  a,\tb FROM t  "),
            "SELECT a, b FROM t"
        );
    }
}
