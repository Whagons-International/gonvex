//! Per-cron counters.
//!
//! A fixed table of relaxed atomics indexed by a slot. The runtime assigns a
//! slot once per cron name when a module is installed ([`register`], which
//! takes a lock but never runs on the call path); a finished scheduled run
//! adds its totals with [`record`] (no allocation, no lock, no map). Slot 0
//! collects one-shot scheduled work (`runAt`/`runAfter`/`wakeAt`); the last
//! slot collects crons registered after the table filled up.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

pub const CRON_SLOTS: usize = 64;
/// One-shot scheduled jobs (not crons).
pub const ONE_SHOT_SLOT: u16 = 0;
const OVERFLOW_SLOT: u16 = (CRON_SLOTS - 1) as u16;

macro_rules! fields {
    ($($variant:ident => $name:literal;)*) => {
        /// One counter of a cron slot.
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum CronField { $($variant),* }
        /// Names, indexed by `CronField as usize`.
        pub const CRON_FIELDS: &[&str] = &[$($name),*];
    };
}

fields! {
    Invocations => "invocations";
    Errors => "errors";
    Skipped => "skipped";
    TotalNs => "totalNs";
    MaxNs => "maxNs";
    InvokeNs => "invokeNs";
    WriterHoldNs => "writerHoldNs";
    MaxWriterHoldNs => "maxWriterHoldNs";
    HostCalls => "hostCalls";
    RowsRead => "rowsRead";
    RowsWritten => "rowsWritten";
    Revisions => "revisions";
    Fanout => "fanout";
    StartDelayNs => "startDelayNs";
    MaxStartDelayNs => "maxStartDelayNs";
}

const FIELD_COUNT: usize = CRON_FIELDS.len();

#[repr(align(64))]
struct Counters([AtomicU64; FIELD_COUNT]);

#[allow(clippy::declare_interior_mutable_const)]
const COUNTERS: Counters = Counters([const { AtomicU64::new(0) }; FIELD_COUNT]);
static TABLE: [Counters; CRON_SLOTS] = [COUNTERS; CRON_SLOTS];
static NAMES: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// The slot of `name`, assigned on first use. Call at module install, not per
/// run. Slot numbers are stable for the life of the process.
pub fn register(name: &str) -> u16 {
    let mut names = NAMES.lock().unwrap_or_else(|poison| poison.into_inner());
    if names.is_empty() {
        names.push("(one-shot)".to_owned());
    }
    if let Some(index) = names.iter().position(|known| known == name) {
        return index as u16;
    }
    if names.len() >= OVERFLOW_SLOT as usize {
        if names.len() == OVERFLOW_SLOT as usize {
            names.push("(other crons)".to_owned());
        }
        return OVERFLOW_SLOT;
    }
    names.push(name.to_owned());
    (names.len() - 1) as u16
}

/// Totals of one finished scheduled run.
#[derive(Clone, Copy, Debug, Default)]
pub struct CronSample {
    pub total_ns: u64,
    /// Module-host (V8) request time, summed over every invocation in the run.
    pub invoke_ns: u64,
    /// Tenant writer held: write transaction open → commit/rollback finished.
    pub writer_hold_ns: u64,
    pub host_calls: u64,
    /// Rows returned by `ctx.db` queries.
    pub rows_read: u64,
    /// Tracked rows changed (change-feed rows of the published revisions).
    pub rows_written: u64,
    /// Change-feed revisions published.
    pub revisions: u64,
    /// Change-feed subscribers (connections) that received those revisions.
    pub fanout: u64,
    /// Due time → run start (claim latency plus any stagger offset).
    pub start_delay_ns: u64,
    pub error: bool,
    /// The run was answered by a pre-check without entering V8.
    pub skipped: bool,
}

#[inline]
fn add(slot: &Counters, field: CronField, value: u64) {
    if value != 0 {
        slot.0[field as usize].fetch_add(value, Relaxed);
    }
}

#[inline]
fn max(slot: &Counters, field: CronField, value: u64) {
    let cell = &slot.0[field as usize];
    if value > cell.load(Relaxed) {
        cell.fetch_max(value, Relaxed);
    }
}

/// Adds one run to `slot`. Relaxed atomics only.
pub fn record(slot: u16, sample: &CronSample) {
    if !crate::enabled() {
        return;
    }
    let counters = &TABLE[(slot as usize).min(CRON_SLOTS - 1)];
    add(counters, CronField::Invocations, 1);
    add(counters, CronField::Errors, sample.error as u64);
    add(counters, CronField::Skipped, sample.skipped as u64);
    add(counters, CronField::TotalNs, sample.total_ns);
    max(counters, CronField::MaxNs, sample.total_ns);
    add(counters, CronField::InvokeNs, sample.invoke_ns);
    add(counters, CronField::WriterHoldNs, sample.writer_hold_ns);
    max(counters, CronField::MaxWriterHoldNs, sample.writer_hold_ns);
    add(counters, CronField::HostCalls, sample.host_calls);
    add(counters, CronField::RowsRead, sample.rows_read);
    add(counters, CronField::RowsWritten, sample.rows_written);
    add(counters, CronField::Revisions, sample.revisions);
    add(counters, CronField::Fanout, sample.fanout);
    add(counters, CronField::StartDelayNs, sample.start_delay_ns);
    max(counters, CronField::MaxStartDelayNs, sample.start_delay_ns);
}

/// One row of the table.
#[derive(Clone, Debug)]
pub struct CronRow {
    pub slot: u16,
    pub name: String,
    pub values: [u64; FIELD_COUNT],
}

impl CronRow {
    pub fn get(&self, field: CronField) -> u64 {
        self.values[field as usize]
    }
}

/// Every registered slot that has run at least once.
pub fn snapshot() -> Vec<CronRow> {
    let names = NAMES.lock().unwrap_or_else(|poison| poison.into_inner()).clone();
    let mut out = Vec::new();
    for (slot, counters) in TABLE.iter().enumerate() {
        let mut values = [0u64; FIELD_COUNT];
        for (value, cell) in values.iter_mut().zip(&counters.0) {
            *value = cell.load(Relaxed);
        }
        if values[CronField::Invocations as usize] == 0 {
            continue;
        }
        let name = names
            .get(slot)
            .cloned()
            .unwrap_or_else(|| if slot == 0 { "(one-shot)".to_owned() } else { format!("slot-{slot}") });
        out.push(CronRow {
            slot: slot as u16,
            name,
            values,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_stable_and_record_totals() {
        crate::set_enabled(true);
        let a = register("test.cron-a");
        let b = register("test.cron-b");
        assert_ne!(a, b);
        assert_ne!(a, ONE_SHOT_SLOT);
        assert_eq!(register("test.cron-a"), a);
        record(
            a,
            &CronSample {
                total_ns: 5,
                rows_written: 3,
                writer_hold_ns: 7,
                ..CronSample::default()
            },
        );
        record(
            a,
            &CronSample {
                total_ns: 9,
                skipped: true,
                ..CronSample::default()
            },
        );
        let row = snapshot().into_iter().find(|row| row.slot == a).unwrap();
        assert_eq!(row.name, "test.cron-a");
        assert_eq!(row.get(CronField::Invocations), 2);
        assert_eq!(row.get(CronField::Skipped), 1);
        assert_eq!(row.get(CronField::TotalNs), 14);
        assert_eq!(row.get(CronField::MaxNs), 9);
        assert_eq!(row.get(CronField::RowsWritten), 3);
        assert_eq!(row.get(CronField::MaxWriterHoldNs), 7);
    }
}
