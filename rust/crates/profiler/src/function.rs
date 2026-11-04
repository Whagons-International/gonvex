//! Per-function counters.
//!
//! A fixed table of relaxed atomics indexed by a slot. The runtime assigns a
//! slot once per `(project, function path)` when a module is installed
//! ([`register`], which takes a lock but never runs on the call path); every
//! finished call adds to its slot through a [`Call`] guard (one clock read at
//! each end and a few relaxed atomic adds: no allocation, lock or map). The
//! last slot collects functions registered after the table filled up.
//! Names are only looked up by readers ([`name`], [`snapshot`]).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

pub const FUNCTION_SLOTS: usize = 2048;
/// "No slot": the call is not counted (function not found, profiler off).
pub const NONE: u16 = u16::MAX;
const OVERFLOW_SLOT: u16 = (FUNCTION_SLOTS - 1) as u16;

macro_rules! fields {
    ($($variant:ident => $name:literal;)*) => {
        /// One counter of a function slot.
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum FunctionField { $($variant),* }
        /// Names, indexed by `FunctionField as usize`.
        pub const FUNCTION_FIELDS: &[&str] = &[$($name),*];
    };
}

fields! {
    Invocations => "invocations";
    Errors => "errors";
    Cancelled => "cancelled";
    TotalNs => "totalNs";
    MaxNs => "maxNs";
    LastNs => "lastNs";
    LastAtNs => "lastAtNs";
}

/// `lastAtNs` is the profiler clock (`now_ns`) when the last call finished
/// (`crate::unix_ms_of` converts it).
const FIELD_COUNT: usize = FUNCTION_FIELDS.len();

#[repr(align(64))]
struct Counters([AtomicU64; FIELD_COUNT]);

#[allow(clippy::declare_interior_mutable_const)]
const COUNTERS: Counters = Counters([const { AtomicU64::new(0) }; FIELD_COUNT]);
/// 2048 × 64 B = 128 KiB of zero-initialised `.bss`; only used slots are touched.
static TABLE: [Counters; FUNCTION_SLOTS] = [COUNTERS; FUNCTION_SLOTS];
static NAMES: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// The slot of `(project, path)`, assigned on first use. Call at module
/// install, not per call. Slot numbers are stable for the life of the process.
pub fn register(project: &str, path: &str) -> u16 {
    let mut names = NAMES.lock().unwrap_or_else(|poison| poison.into_inner());
    if let Some(index) = names
        .iter()
        .position(|(known_project, known_path)| known_project == project && known_path == path)
    {
        return index as u16;
    }
    if names.len() >= OVERFLOW_SLOT as usize {
        if names.len() == OVERFLOW_SLOT as usize {
            names.push((String::new(), "(other functions)".to_owned()));
        }
        return OVERFLOW_SLOT;
    }
    names.push((project.to_owned(), path.to_owned()));
    (names.len() - 1) as u16
}

/// `(project, path)` of a slot (reader side; takes the registry lock).
pub fn name(slot: u16) -> Option<(String, String)> {
    NAMES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .get(slot as usize)
        .cloned()
}

#[inline]
fn add(slot: &Counters, field: FunctionField, value: u64) {
    if value != 0 {
        slot.0[field as usize].fetch_add(value, Relaxed);
    }
}

/// Adds one finished call to `slot`. Relaxed atomics only.
#[inline]
pub fn record(slot: u16, elapsed_ns: u64, finished_at_ns: u64, outcome: Outcome) {
    if slot == NONE || !crate::enabled() {
        return;
    }
    let counters = &TABLE[(slot as usize).min(FUNCTION_SLOTS - 1)];
    add(counters, FunctionField::Invocations, 1);
    match outcome {
        Outcome::Ok => {}
        Outcome::Error => add(counters, FunctionField::Errors, 1),
        Outcome::Cancelled => add(counters, FunctionField::Cancelled, 1),
    }
    add(counters, FunctionField::TotalNs, elapsed_ns);
    let max = &counters.0[FunctionField::MaxNs as usize];
    if elapsed_ns > max.load(Relaxed) {
        max.fetch_max(elapsed_ns, Relaxed);
    }
    counters.0[FunctionField::LastNs as usize].store(elapsed_ns, Relaxed);
    counters.0[FunctionField::LastAtNs as usize].store(finished_at_ns, Relaxed);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Error,
    /// The caller dropped the call before it finished (client went away).
    Cancelled,
}

/// Times one call into a slot. Records on [`Call::finish`], or as cancelled
/// when dropped unfinished. Inert (no clock reads) when the profiler is off.
pub struct Call {
    slot: u16,
    start: u64,
}

impl Call {
    #[inline]
    pub fn start() -> Self {
        Self {
            slot: NONE,
            start: if crate::enabled() { crate::now_ns() } else { 0 },
        }
    }

    /// Sets the slot once the function is known (after the module lookup).
    #[inline]
    pub fn set_slot(&mut self, slot: u16) {
        self.slot = slot;
    }

    pub fn slot(&self) -> u16 {
        self.slot
    }

    #[inline]
    pub fn finish(mut self, ok: bool) {
        self.record(if ok { Outcome::Ok } else { Outcome::Error });
    }

    #[inline]
    fn record(&mut self, outcome: Outcome) {
        if self.start != 0 && self.slot != NONE {
            let now = crate::now_ns();
            record(self.slot, now.saturating_sub(self.start), now, outcome);
        }
        self.start = 0;
    }
}

impl Drop for Call {
    #[inline]
    fn drop(&mut self) {
        self.record(Outcome::Cancelled);
    }
}

/// One row of the table.
#[derive(Clone, Debug)]
pub struct FunctionRow {
    pub slot: u16,
    pub project: String,
    pub path: String,
    pub values: [u64; FIELD_COUNT],
}

impl FunctionRow {
    pub fn get(&self, field: FunctionField) -> u64 {
        self.values[field as usize]
    }
}

/// Every registered slot that ran at least once, optionally only one project's.
pub fn snapshot(project: Option<&str>) -> Vec<FunctionRow> {
    let names = NAMES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    let mut out = Vec::new();
    for (slot, (name_project, path)) in names.into_iter().enumerate() {
        if project.is_some_and(|project| project != name_project) {
            continue;
        }
        let counters = &TABLE[slot];
        let mut values = [0u64; FIELD_COUNT];
        for (value, cell) in values.iter_mut().zip(&counters.0) {
            *value = cell.load(Relaxed);
        }
        if values[FunctionField::Invocations as usize] == 0 {
            continue;
        }
        out.push(FunctionRow {
            slot: slot as u16,
            project: name_project,
            path,
            values,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_stable_and_calls_are_counted() {
        crate::set_enabled(true);
        let a = register("p-fn", "tasks.save");
        let b = register("p-fn", "tasks.list");
        let other = register("p-other", "tasks.save");
        assert_ne!(a, b);
        assert_ne!(a, other);
        assert_eq!(register("p-fn", "tasks.save"), a);
        let mut call = Call::start();
        call.set_slot(a);
        call.finish(true);
        let mut call = Call::start();
        call.set_slot(a);
        call.finish(false);
        {
            let mut call = Call::start();
            call.set_slot(a);
        }
        // Not counted: no slot.
        Call::start().finish(true);
        let rows = snapshot(Some("p-fn"));
        assert!(rows.iter().all(|row| row.project == "p-fn"));
        let row = rows.iter().find(|row| row.slot == a).unwrap();
        assert_eq!(row.path, "tasks.save");
        assert_eq!(row.get(FunctionField::Invocations), 3);
        assert_eq!(row.get(FunctionField::Errors), 1);
        assert_eq!(row.get(FunctionField::Cancelled), 1);
        assert!(row.get(FunctionField::TotalNs) >= row.get(FunctionField::MaxNs));
        assert!(row.get(FunctionField::LastAtNs) > 0);
        assert!(!rows.iter().any(|row| row.slot == b));
        assert_eq!(name(a).unwrap(), ("p-fn".to_owned(), "tasks.save".to_owned()));
    }
}
