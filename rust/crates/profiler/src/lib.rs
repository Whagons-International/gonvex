//! Always-on, allocation-free runtime profiler.
//!
//! Every recording site costs one monotonic clock read per stage boundary and
//! two or three relaxed atomic RMWs on a per-thread shard of a fixed-size,
//! statically allocated histogram. Nothing on the recording path allocates,
//! takes a lock, hashes a string or touches a map. Readers (the `/dev/profile`
//! endpoint) walk the static tables and do all aggregation.
//!
//! * [`Stage`]: a fixed list of pipeline stages (reducer, delivery, snapshot,
//!   scheduler). [`Label`]: a small fixed set of function classes, so the
//!   reducer path can be split into heartbeat / presence / other reducers.
//! * Histograms: log-linear buckets, 4 sub-buckets per power of two from
//!   1.024 µs to 68.7 s (106 buckets, ≤ 19 % relative bucket width), plus sum
//!   and max, sharded per thread (8 shards, 64-byte aligned).
//! * [`Gauge`]: current value plus a 10-second-window peak.
//! * [`Instrumented`]: a poll-timing future wrapper; polls slower than the
//!   long-poll threshold land in a lock-free ring buffer.
//! * A watchdog OS thread that sees stalls while a tokio worker is blocked.
//!
//! `GONVEX_PROFILER=0` turns every recording site into a single relaxed load.

pub mod cron;

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Switch and clock
// ---------------------------------------------------------------------------

static ENABLED: AtomicBool = AtomicBool::new(true);
static LONG_POLL_NS: AtomicU64 = AtomicU64::new(5_000_000);
static STALL_NS: AtomicU64 = AtomicU64::new(25_000_000);
static PROBE_NS: AtomicU64 = AtomicU64::new(10_000_000);
static SLOW_NS: AtomicU64 = AtomicU64::new(100_000_000);

/// Whether recording is on. One relaxed load.
#[inline(always)]
pub fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

/// Turns recording on or off (tests, benchmarks). Production uses
/// [`init_from_env`].
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Relaxed);
}

/// Profiler configuration read from the environment.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub enabled: bool,
    pub long_poll: Duration,
    pub stall: Duration,
    pub probe_interval: Duration,
    pub slow_trace: Duration,
}

/// Reads `GONVEX_PROFILER` (`0`/`false`/`off` disables),
/// `GONVEX_PROFILER_LONG_POLL_MS` (default 5), `GONVEX_PROFILER_STALL_MS`
/// (watchdog threshold, default 25) and `GONVEX_PROFILER_PROBE_MS`
/// (scheduler-lag probe interval, default 10) and `GONVEX_PROFILER_SLOW_MS`
/// (reducers at least this slow keep a per-stage trace, default 100).
pub fn init_from_env() -> Config {
    let off = std::env::var("GONVEX_PROFILER")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(false);
    let millis = |name: &str, default: f64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| *value > 0.0)
            .unwrap_or(default)
    };
    let config = Config {
        enabled: !off,
        long_poll: Duration::from_secs_f64(millis("GONVEX_PROFILER_LONG_POLL_MS", 5.0) / 1e3),
        stall: Duration::from_secs_f64(millis("GONVEX_PROFILER_STALL_MS", 25.0) / 1e3),
        probe_interval: Duration::from_secs_f64(millis("GONVEX_PROFILER_PROBE_MS", 10.0) / 1e3),
        slow_trace: Duration::from_secs_f64(millis("GONVEX_PROFILER_SLOW_MS", 100.0) / 1e3),
    };
    ENABLED.store(config.enabled, Relaxed);
    LONG_POLL_NS.store(config.long_poll.as_nanos() as u64, Relaxed);
    STALL_NS.store(config.stall.as_nanos() as u64, Relaxed);
    PROBE_NS.store(config.probe_interval.as_nanos() as u64, Relaxed);
    SLOW_NS.store(config.slow_trace.as_nanos() as u64, Relaxed);
    epoch();
    config
}

/// The current configuration.
pub fn config() -> Config {
    Config {
        enabled: enabled(),
        long_poll: Duration::from_nanos(LONG_POLL_NS.load(Relaxed)),
        stall: Duration::from_nanos(STALL_NS.load(Relaxed)),
        probe_interval: Duration::from_nanos(PROBE_NS.load(Relaxed)),
        slow_trace: Duration::from_nanos(SLOW_NS.load(Relaxed)),
    }
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Monotonic nanoseconds since the profiler epoch (first use). Never 0 after
/// the first call returns, so 0 can mean "unset".
#[inline]
pub fn now_ns() -> u64 {
    (epoch().elapsed().as_nanos() as u64).max(1)
}

/// Time since the profiler epoch, refreshed every watchdog period (10 ms).
/// 0 when the watchdog is not running.
static COARSE_NOW_NS: AtomicU64 = AtomicU64::new(0);

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Stages and labels
// ---------------------------------------------------------------------------

macro_rules! stages {
    ($($variant:ident => $name:literal, $group:literal, $doc:literal;)*) => {
        /// A fixed pipeline stage. See `docs/performance/profiler.md`.
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Stage { $($variant),* }

        /// Name, group and description of every stage, indexed by `Stage as usize`.
        pub const STAGES: &[StageInfo] = &[$(StageInfo { name: $name, group: $group, doc: $doc }),*];
    };
}

#[derive(Clone, Copy, Debug)]
pub struct StageInfo {
    pub name: &'static str,
    pub group: &'static str,
    pub doc: &'static str,
}

stages! {
    // Function path (labelled by function class).
    FnTotal => "fn.total", "reducer", "Whole Reducer/Query/Action execution in the runtime (execution.rs)";
    FnPrepare => "fn.prepare", "reducer", "Admission: module lookup, control-plane read, idempotency check, argument validation";
    InvokeConnect => "invoke.connect", "reducer", "Module-host status check, socket connect and request write";
    InvokeFirstFrame => "invoke.first_frame", "reducer", "Request written -> first frame back: module-host isolate wait + JS up to the first host call + IPC";
    InvokeJs => "invoke.js", "reducer", "JS time after the first frame (sum of gaps between host-call replies and the next frame)";
    InvokeHostCalls => "invoke.host_calls", "reducer", "Sum of host-call handling time per invocation (host-call round trips seen by the runtime)";
    InvokeTotal => "invoke.total", "reducer", "Whole module-host request (connect .. response)";
    HostCallCount => "invoke.host_call_count", "reducer", "Host calls per invocation (value is a count, not ns)";
    WriterWait => "db.writer_wait", "reducer", "Reducer write transaction open: pool admission + single-writer acquire + BEGIN + receipt claim";
    Sql => "db.sql", "reducer", "ctx.db host calls (SQL + row encode), excluding the writer wait";
    HostOther => "host.other", "reducer", "Non-database host calls (storage, scheduler, outbox)";
    FnFinish => "fn.finish", "reducer", "Reducer commit or rollback (finish_with_revision) including the four commit stages";
    CommitFinalize => "commit.finalize", "reducer", "Reference-integrity flush + change-feed finalize inside the transaction";
    CommitSql => "commit.commit", "reducer", "SQLite COMMIT";
    CommitFsync => "commit.fsync", "reducer", "Durability barrier (WAL sync) before acknowledging";
    CommitPublish => "commit.publish", "reducer", "Change-feed notification publish";
    // Store (unlabelled).
    DbAdmission => "db.admission", "store", "Process-wide pool admission semaphore wait (every transaction)";
    WriterAcquire => "db.writer_acquire", "store", "Writer connection acquire + BEGIN (single writer per tenant database)";
    ReaderBegin => "db.reader_begin", "store", "Reader connection acquire + BEGIN";
    // Delivery path.
    FeedNotifyLag => "feed.notify_lag", "delivery", "Commit publish -> change-feed reader woke up";
    FeedRead => "feed.read", "delivery", "Change-feed reader: read clock + changes and broadcast";
    DeliveryQueueWait => "delivery.queue_wait", "delivery", "Broadcast -> this connection started applying the transaction";
    DeliveryVisibility => "delivery.visibility", "delivery", "Live-query + replica visibility evaluation and frame building, per connection";
    DeliverySend => "delivery.send", "delivery", "Encoding + writing the transaction's frames, per connection";
    DeliveryTotal => "delivery.total", "delivery", "Broadcast -> all frames for the transaction written, per connection";
    DeliveryFrames => "delivery.frames", "delivery", "Frames per connection per transaction (value is a count, not ns)";
    // WebSocket transport.
    WsInboundWait => "ws.inbound_wait", "transport", "Inbound frame read by the socket reader -> taken by the connection task";
    WsEncode => "ws.encode", "transport", "JSON encode of an outbound frame";
    WsCompress => "ws.compress", "transport", "permessage-deflate compression of one outbound message";
    WsWrite => "ws.write", "transport", "Outbound socket write (includes compression and backpressure)";
    // Snapshot path.
    SnapshotPermitWait => "snapshot.permit_wait", "snapshot", "Wait for the process-wide snapshot semaphore";
    SnapshotAcl => "snapshot.acl", "snapshot", "Parent ACL index preparation";
    SnapshotBegin => "snapshot.begin", "snapshot", "Read transaction open + clock read";
    SnapshotVisibility => "snapshot.visibility", "snapshot", "Visibility resolution for the subscriber";
    SnapshotQuery => "snapshot.query", "snapshot", "Snapshot SQL + row JSON decode + row admission";
    SnapshotIntern => "snapshot.intern", "snapshot", "Row key/hash + interning into the shared row table";
    SnapshotBuild => "snapshot.build", "snapshot", "Whole snapshot build (permit wait .. interned rows)";
    SnapshotSend => "snapshot.send", "snapshot", "Encode + write the snapshot frames/pages";
    // Scheduler.
    SchedLag => "tokio.sched_lag", "scheduler", "Lateness of the scheduler-lag probe timer wake-up";
    PollWsConnection => "poll.ws_connection", "scheduler", "Single poll of a WebSocket connection task";
    PollWsReader => "poll.ws_reader", "scheduler", "Single poll of a WebSocket frame reader task";
    PollSnapshot => "poll.snapshot", "scheduler", "Single poll of a replica snapshot preparation task";
    PollFeed => "poll.feed", "scheduler", "Single poll of a change-feed reader task";
    PollOutbox => "poll.outbox", "scheduler", "Single poll of an action-outbox drain task";
    PollOther => "poll.other", "scheduler", "Single poll of another instrumented task";
}

pub const STAGE_COUNT: usize = STAGES.len();

impl Stage {
    pub fn info(self) -> &'static StageInfo {
        &STAGES[self as usize]
    }
}

/// A fixed function class. Assigned by [`Label::classify`] with a few byte
/// comparisons on the function path (no allocation, no map).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Label {
    #[default]
    None = 0,
    Heartbeat = 1,
    Presence = 2,
    Reducer = 3,
    Query = 4,
    Action = 5,
    /// Work started by the scheduler (crons, `runAt`/`wakeAt` jobs). Set by
    /// the caller, never by [`Label::classify`].
    Cron = 6,
}

pub const LABELS: [&str; 7] = [
    "-",
    "heartbeat",
    "presence",
    "reducer",
    "query",
    "action",
    "cron",
];
pub const LABEL_COUNT: usize = LABELS.len();

impl Label {
    /// `kind` is the function kind (`reducer`, `query`, `action`); reducers
    /// split into heartbeat (`presence.beat`, `*.heartbeat`), other
    /// `presence.*`, and everything else.
    #[inline]
    pub fn classify(kind: &str, path: &str) -> Self {
        match kind {
            "reducer" => {
                if path == "presence.beat" || path.ends_with("heartbeat") {
                    Self::Heartbeat
                } else if path.starts_with("presence.") {
                    Self::Presence
                } else {
                    Self::Reducer
                }
            }
            "query" => Self::Query,
            "action" => Self::Action,
            _ => Self::None,
        }
    }

    pub fn name(self) -> &'static str {
        LABELS[self as usize]
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Heartbeat,
            2 => Self::Presence,
            3 => Self::Reducer,
            4 => Self::Query,
            5 => Self::Action,
            6 => Self::Cron,
            _ => Self::None,
        }
    }
}

// ---------------------------------------------------------------------------
// Histograms
// ---------------------------------------------------------------------------

/// Sub-buckets per power of two.
const SUB_BITS: u32 = 2;
const SUB: usize = 1 << SUB_BITS;
/// Smallest resolved power: 2^10 ns = 1.024 µs. Bucket 0 holds anything below.
const MIN_POW: u32 = 10;
/// Last resolved power: values ≥ 2^36 ns (68.7 s) land in the overflow bucket.
const MAX_POW: u32 = 36;
pub const BUCKETS: usize = 1 + (MAX_POW - MIN_POW) as usize * SUB + 1;
const SHARDS: usize = 8;

#[repr(align(64))]
struct Hist {
    buckets: [AtomicU64; BUCKETS],
    sum: AtomicU64,
    max: AtomicU64,
}

#[allow(clippy::declare_interior_mutable_const)]
const HIST: Hist = Hist {
    buckets: [const { AtomicU64::new(0) }; BUCKETS],
    sum: AtomicU64::new(0),
    max: AtomicU64::new(0),
};
#[allow(clippy::declare_interior_mutable_const)]
const HIST_SHARDS: [Hist; SHARDS] = [HIST; SHARDS];
#[allow(clippy::declare_interior_mutable_const)]
const HIST_LABELS: [[Hist; SHARDS]; LABEL_COUNT] = [HIST_SHARDS; LABEL_COUNT];

/// `[stage][label][shard]`, zero-initialised in .bss: pages that are never
/// recorded into are never touched.
static HISTOGRAMS: [[[Hist; SHARDS]; LABEL_COUNT]; STAGE_COUNT] = [HIST_LABELS; STAGE_COUNT];

/// Bucket index for a value in ns.
#[inline(always)]
pub fn bucket_of(ns: u64) -> usize {
    if ns < (1 << MIN_POW) {
        return 0;
    }
    let pow = 63 - ns.leading_zeros();
    if pow >= MAX_POW {
        return BUCKETS - 1;
    }
    let sub = ((ns >> (pow - SUB_BITS)) as usize) & (SUB - 1);
    1 + (pow - MIN_POW) as usize * SUB + sub
}

/// `[lower, upper)` bounds of a bucket in ns (upper of the overflow bucket is
/// `u64::MAX`).
pub fn bucket_bounds(index: usize) -> (u64, u64) {
    if index == 0 {
        return (0, 1 << MIN_POW);
    }
    if index >= BUCKETS - 1 {
        return (1 << MAX_POW, u64::MAX);
    }
    let offset = index - 1;
    let pow = MIN_POW + (offset / SUB) as u32;
    let sub = (offset % SUB) as u64;
    let step = 1u64 << (pow - SUB_BITS);
    let lower = (1u64 << pow) + sub * step;
    (lower, lower + step)
}

thread_local! {
    static THREAD_INDEX: Cell<u32> = const { Cell::new(u32::MAX) };
}
static NEXT_THREAD: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
fn thread_index() -> usize {
    THREAD_INDEX.with(|cell| {
        let mut index = cell.get();
        if index == u32::MAX {
            index = NEXT_THREAD.fetch_add(1, Relaxed) as u32 & 0x7fff_ffff;
            cell.set(index);
        }
        index as usize
    })
}

/// Records one value (ns, or a count for count stages). Two or three relaxed
/// atomic RMWs on this thread's shard.
#[inline]
pub fn record(stage: Stage, label: Label, value: u64) {
    if !enabled() {
        return;
    }
    record_always(stage, label, value);
}

#[inline]
fn record_always(stage: Stage, label: Label, value: u64) {
    let hist = &HISTOGRAMS[stage as usize][label as usize][thread_index() % SHARDS];
    hist.buckets[bucket_of(value)].fetch_add(1, Relaxed);
    hist.sum.fetch_add(value, Relaxed);
    if value > hist.max.load(Relaxed) {
        hist.max.fetch_max(value, Relaxed);
    }
}

/// Records a [`Duration`].
#[inline]
pub fn record_duration(stage: Stage, label: Label, duration: Duration) {
    record(
        stage,
        label,
        duration.as_nanos().min(u64::MAX as u128) as u64,
    );
}

/// A stage-boundary clock: one clock read per boundary. Inert (no clock reads)
/// when the profiler is disabled.
#[derive(Clone, Copy, Debug)]
pub struct Timer {
    last: u64,
}

impl Timer {
    #[inline]
    pub fn start() -> Self {
        Self {
            last: if enabled() { now_ns() } else { 0 },
        }
    }

    /// Whether this timer is recording.
    #[inline]
    pub fn active(&self) -> bool {
        self.last != 0
    }

    /// Records the time since the previous boundary and starts the next stage.
    /// Returns the recorded ns (0 when inactive).
    #[inline]
    pub fn lap(&mut self, stage: Stage, label: Label) -> u64 {
        if self.last == 0 {
            return 0;
        }
        let now = now_ns();
        let elapsed = now.saturating_sub(self.last);
        self.last = now;
        record_always(stage, label, elapsed);
        elapsed
    }

    /// Starts the next stage without recording the previous one.
    #[inline]
    pub fn skip(&mut self) {
        if self.last != 0 {
            self.last = now_ns();
        }
    }

    /// ns since the previous boundary, without moving it.
    #[inline]
    pub fn elapsed_ns(&self) -> u64 {
        if self.last == 0 {
            0
        } else {
            now_ns().saturating_sub(self.last)
        }
    }

    /// The boundary as ns since the profiler epoch (0 when inactive).
    pub fn at_ns(&self) -> u64 {
        self.last
    }
}

/// Records the lifetime of a scope (including early returns and errors) into
/// one stage.
pub struct ScopeTimer {
    start: u64,
    stage: Stage,
    label: Label,
}

impl ScopeTimer {
    #[inline]
    pub fn new(stage: Stage, label: Label) -> Self {
        Self {
            start: if enabled() { now_ns() } else { 0 },
            stage,
            label,
        }
    }
}

impl Drop for ScopeTimer {
    #[inline]
    fn drop(&mut self) {
        if self.start != 0 {
            record_always(self.stage, self.label, now_ns().saturating_sub(self.start));
        }
    }
}

// ---------------------------------------------------------------------------
// Gauges
// ---------------------------------------------------------------------------

macro_rules! gauges {
    ($($variant:ident => $name:literal, $doc:literal;)*) => {
        /// A fixed queue/in-flight gauge.
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum GaugeId { $($variant),* }
        pub const GAUGES_INFO: &[(&str, &str)] = &[$(($name, $doc)),*];
    };
}

gauges! {
    ModuleInflight => "module_host.inflight", "Module-host requests in flight (isolate queue + executing)";
    WriterWaiting => "db.writer_waiting", "Write transactions waiting for a tenant's single writer (all tenants)";
    WriterHeld => "db.writer_held", "Write transactions currently open";
    AdmissionWaiting => "db.admission_waiting", "Transactions waiting for the process-wide pool admission semaphore";
    SnapshotWaiting => "snapshot.permit_waiting", "Snapshot builds waiting for the snapshot semaphore";
    SnapshotActive => "snapshot.active", "Snapshot builds holding a permit";
    WsSending => "ws.sending", "Connections blocked in an outbound socket write";
    OutboundFrames => "ws.outbound_frames", "Frames built for delivery but not yet written (sum over connections)";
    OutboundBatch => "ws.outbound_batch", "Largest per-connection delivery batch (peak only; current is always 0)";
}

pub const GAUGE_COUNT: usize = GAUGES_INFO.len();

/// One gauge: current value and a peak in 10-second windows.
pub struct Gauge {
    current: AtomicI64,
    /// `(window << 40) | peak`.
    peak: AtomicU64,
}

#[allow(clippy::declare_interior_mutable_const)]
const GAUGE: Gauge = Gauge {
    current: AtomicI64::new(0),
    peak: AtomicU64::new(0),
};
static GAUGES: [Gauge; GAUGE_COUNT] = [GAUGE; GAUGE_COUNT];
const PEAK_BITS: u32 = 40;
const PEAK_WINDOW_NS: u64 = 10_000_000_000;

/// The gauge for an id.
#[inline]
pub fn gauge(id: GaugeId) -> &'static Gauge {
    &GAUGES[id as usize]
}

impl Gauge {
    #[inline]
    pub fn add(&'static self, delta: i64) {
        if !enabled() {
            return;
        }
        let value = self.current.fetch_add(delta, Relaxed) + delta;
        if delta > 0 {
            self.observe(value.max(0) as u64);
        }
    }

    /// Updates the window peak with a value (no change to `current`).
    #[inline]
    pub fn observe(&self, value: u64) {
        let window = COARSE_NOW_NS.load(Relaxed) / PEAK_WINDOW_NS;
        let packed = self.peak.load(Relaxed);
        let (peak_window, peak) = (packed >> PEAK_BITS, packed & ((1 << PEAK_BITS) - 1));
        if peak_window != window || value > peak {
            let value = value.min((1 << PEAK_BITS) - 1);
            let value = if peak_window == window {
                value.max(peak)
            } else {
                value
            };
            self.peak.store((window << PEAK_BITS) | value, Relaxed);
        }
    }

    /// Increments now and decrements when the guard drops. Inert when disabled.
    #[inline]
    pub fn guard(&'static self) -> GaugeGuard {
        if enabled() {
            self.add(1);
            GaugeGuard(Some(self))
        } else {
            GaugeGuard(None)
        }
    }

    pub fn current(&self) -> i64 {
        self.current.load(Relaxed)
    }

    /// Peak in the current 10 s window, or the previous window's peak if this
    /// window has not seen a value yet.
    pub fn peak(&self) -> u64 {
        let window = COARSE_NOW_NS.load(Relaxed) / PEAK_WINDOW_NS;
        let packed = self.peak.load(Relaxed);
        let (peak_window, peak) = (packed >> PEAK_BITS, packed & ((1 << PEAK_BITS) - 1));
        if peak_window + 1 >= window {
            peak
        } else {
            0
        }
    }
}

/// Decrements its gauge on drop.
pub struct GaugeGuard(Option<&'static Gauge>);

impl Drop for GaugeGuard {
    #[inline]
    fn drop(&mut self) {
        if let Some(gauge) = self.0 {
            gauge.current.fetch_add(-1, Relaxed);
        }
    }
}

/// Frames built for a connection but not yet written: adds to the
/// `ws.outbound_frames` sum, records the batch in `ws.outbound_batch`, and
/// releases whatever was not marked sent when dropped (error paths).
pub struct Backlog {
    remaining: i64,
}

impl Backlog {
    #[inline]
    pub fn new(frames: usize) -> Self {
        if !enabled() || frames == 0 {
            return Self { remaining: 0 };
        }
        let frames = frames as i64;
        gauge(GaugeId::OutboundFrames).add(frames);
        gauge(GaugeId::OutboundBatch).observe(frames as u64);
        Self { remaining: frames }
    }

    #[inline]
    pub fn sent(&mut self) {
        if self.remaining > 0 {
            self.remaining -= 1;
            gauge(GaugeId::OutboundFrames)
                .current
                .fetch_add(-1, Relaxed);
        }
    }
}

impl Drop for Backlog {
    fn drop(&mut self) {
        if self.remaining > 0 {
            gauge(GaugeId::OutboundFrames)
                .current
                .fetch_add(-self.remaining, Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-task timestamps (commit -> feed -> connection)
// ---------------------------------------------------------------------------

const STAMPS: usize = 4096;
struct StampSlot {
    key: AtomicU64,
    at: AtomicU64,
}
#[allow(clippy::declare_interior_mutable_const)]
const STAMP: StampSlot = StampSlot {
    key: AtomicU64::new(0),
    at: AtomicU64::new(0),
};
static STAMP_TABLE: [StampSlot; STAMPS] = [STAMP; STAMPS];

/// A key for a (scope string, sequence) pair, e.g. (database URL, revision).
/// FNV-1a over the scope; no allocation.
#[inline]
pub fn stamp_key(scope: &str, sequence: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in scope.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash ^ sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1
}

/// Remembers "now" for a key in a fixed 4096-slot table (newer keys overwrite
/// older ones that hash to the same slot).
#[inline]
pub fn stamp(key: u64) {
    if !enabled() {
        return;
    }
    let slot = &STAMP_TABLE[(key as usize) % STAMPS];
    slot.key.store(0, Relaxed);
    slot.at.store(now_ns(), Relaxed);
    slot.key.store(key, Relaxed);
}

/// ns since [`stamp`] was called for this key, if it is still in the table.
#[inline]
pub fn since_stamp(key: u64) -> Option<u64> {
    if !enabled() {
        return None;
    }
    let slot = &STAMP_TABLE[(key as usize) % STAMPS];
    if slot.key.load(Relaxed) != key {
        return None;
    }
    let at = slot.at.load(Relaxed);
    if slot.key.load(Relaxed) != key || at == 0 {
        return None;
    }
    Some(now_ns().saturating_sub(at))
}

/// The epoch-relative time of a stamp, if still present.
pub fn stamp_at(key: u64) -> Option<u64> {
    let slot = &STAMP_TABLE[(key as usize) % STAMPS];
    if slot.key.load(Relaxed) != key {
        return None;
    }
    let at = slot.at.load(Relaxed);
    (slot.key.load(Relaxed) == key && at != 0).then_some(at)
}

// ---------------------------------------------------------------------------
// Long polls, active polls and the watchdog
// ---------------------------------------------------------------------------

/// An instrumented task family.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    WsConnection = 1,
    WsReader = 2,
    Snapshot = 3,
    Feed = 4,
    Outbox = 5,
    Other = 6,
    /// Watchdog record: the scheduler-lag probe stopped ticking.
    ProbeStalled = 7,
}

impl TaskKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::WsConnection => "ws_connection",
            Self::WsReader => "ws_reader",
            Self::Snapshot => "snapshot",
            Self::Feed => "feed",
            Self::Outbox => "outbox",
            Self::Other => "other",
            Self::ProbeStalled => "probe_stalled",
        }
    }

    fn stage(self) -> Stage {
        match self {
            Self::WsConnection => Stage::PollWsConnection,
            Self::WsReader => Stage::PollWsReader,
            Self::Snapshot => Stage::PollSnapshot,
            Self::Feed => Stage::PollFeed,
            Self::Outbox => Stage::PollOutbox,
            Self::Other | Self::ProbeStalled => Stage::PollOther,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::WsConnection,
            2 => Self::WsReader,
            3 => Self::Snapshot,
            4 => Self::Feed,
            5 => Self::Outbox,
            7 => Self::ProbeStalled,
            _ => Self::Other,
        }
    }
}

const RING: usize = 256;
struct RingSlot {
    seq: AtomicU64,
    at_unix_ms: AtomicU64,
    duration_ns: AtomicU64,
    /// `kind | source << 8`.
    meta: AtomicU64,
}
#[allow(clippy::declare_interior_mutable_const)]
const RING_SLOT: RingSlot = RingSlot {
    seq: AtomicU64::new(0),
    at_unix_ms: AtomicU64::new(0),
    duration_ns: AtomicU64::new(0),
    meta: AtomicU64::new(0),
};
static LONG_POLLS: [RingSlot; RING] = [RING_SLOT; RING];
static LONG_POLL_SEQ: AtomicU64 = AtomicU64::new(0);

const SOURCE_POLL: u64 = 0;
const SOURCE_WATCHDOG: u64 = 1;

fn push_long_poll(kind: TaskKind, source: u64, duration_ns: u64) {
    let seq = LONG_POLL_SEQ.fetch_add(1, Relaxed) + 1;
    let slot = &LONG_POLLS[(seq as usize) % RING];
    slot.seq.store(0, std::sync::atomic::Ordering::Release);
    slot.at_unix_ms.store(unix_ms(), Relaxed);
    slot.duration_ns.store(duration_ns, Relaxed);
    slot.meta.store(kind as u64 | source << 8, Relaxed);
    slot.seq.store(seq, std::sync::atomic::Ordering::Release);
}

/// One recorded long poll or watchdog observation.
#[derive(Clone, Debug)]
pub struct LongPoll {
    pub seq: u64,
    pub at_unix_ms: u64,
    pub duration_ns: u64,
    pub task: &'static str,
    /// `poll` (the poll finished and took this long) or `watchdog` (seen
    /// still running, or the probe stalled, by the watchdog thread).
    pub source: &'static str,
}

/// Recent long polls, newest first (at most 256).
pub fn long_polls() -> Vec<LongPoll> {
    let mut out = Vec::new();
    for slot in &LONG_POLLS {
        let seq = slot.seq.load(std::sync::atomic::Ordering::Acquire);
        if seq == 0 {
            continue;
        }
        let at_unix_ms = slot.at_unix_ms.load(Relaxed);
        let duration_ns = slot.duration_ns.load(Relaxed);
        let meta = slot.meta.load(Relaxed);
        if slot.seq.load(std::sync::atomic::Ordering::Acquire) != seq {
            continue;
        }
        out.push(LongPoll {
            seq,
            at_unix_ms,
            duration_ns,
            task: TaskKind::from_u8(meta as u8).name(),
            source: if meta >> 8 == SOURCE_WATCHDOG {
                "watchdog"
            } else {
                "poll"
            },
        });
    }
    out.sort_by(|a, b| b.seq.cmp(&a.seq));
    out
}

/// Total long polls recorded since start (the ring keeps the last 256).
pub fn long_poll_total() -> u64 {
    LONG_POLL_SEQ.load(Relaxed)
}

const ACTIVE_SLOTS: usize = 64;
/// Per-thread "task currently being polled": `kind << 56 | start_ns`.
static ACTIVE: [AtomicU64; ACTIVE_SLOTS] = [const { AtomicU64::new(0) }; ACTIVE_SLOTS];
const ACTIVE_SHIFT: u32 = 56;

/// Wraps a future so that each poll is timed: every poll is recorded in its
/// `poll.*` stage, polls over the long-poll threshold go to the ring buffer,
/// and the watchdog can see which task family a stuck worker is polling.
pub struct Instrumented<F> {
    inner: F,
    kind: TaskKind,
}

/// See [`Instrumented`].
pub fn instrument<F: Future>(kind: TaskKind, inner: F) -> Instrumented<F> {
    Instrumented { inner, kind }
}

impl<F: Future> Future for Instrumented<F> {
    type Output = F::Output;
    #[inline]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: `inner` is structurally pinned: it is never moved out of
        // `self`, and `Instrumented` has no Drop impl or `Unpin` override.
        let this = unsafe { self.get_unchecked_mut() };
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        if !enabled() {
            return inner.poll(context);
        }
        let start = now_ns();
        let slot = &ACTIVE[thread_index() % ACTIVE_SLOTS];
        let previous = slot.swap(
            (this.kind as u64) << ACTIVE_SHIFT | (start & ((1 << ACTIVE_SHIFT) - 1)),
            Relaxed,
        );
        let result = inner.poll(context);
        let elapsed = now_ns().saturating_sub(start);
        slot.store(previous, Relaxed);
        record_always(this.kind.stage(), Label::None, elapsed);
        if elapsed >= LONG_POLL_NS.load(Relaxed) {
            push_long_poll(this.kind, SOURCE_POLL, elapsed);
        }
        result
    }
}

static PROBE_TICK_NS: AtomicU64 = AtomicU64::new(0);
static STALL_NOW_NS: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_STALLS: AtomicU64 = AtomicU64::new(0);

/// Called by the scheduler-lag probe task each time it wakes; `lateness_ns`
/// is how late the timer fired.
#[inline]
pub fn probe_tick(lateness_ns: u64) {
    PROBE_TICK_NS.store(now_ns(), Relaxed);
    record(Stage::SchedLag, Label::None, lateness_ns);
}

/// Watchdog view: how long the probe has currently gone without ticking
/// (beyond its interval), and how many stalls over the threshold were seen.
pub fn watchdog_state() -> (u64, u64) {
    (STALL_NOW_NS.load(Relaxed), WATCHDOG_STALLS.load(Relaxed))
}

/// Starts the watchdog OS thread once. Every 10 ms it refreshes the coarse
/// clock, checks that the scheduler-lag probe ticked recently, and looks for
/// instrumented polls that have been running longer than the stall threshold.
pub fn start_watchdog() {
    static STARTED: AtomicBool = AtomicBool::new(false);
    if !enabled() || STARTED.swap(true, Relaxed) {
        return;
    }
    let _ = std::thread::Builder::new()
        .name("gonvex-profiler-watchdog".into())
        .spawn(|| {
            let mut reported_tick = 0u64;
            let mut reported_active = [0u64; ACTIVE_SLOTS];
            loop {
                std::thread::sleep(Duration::from_millis(10));
                let now = now_ns();
                COARSE_NOW_NS.store(now, Relaxed);
                if !enabled() {
                    continue;
                }
                let stall = STALL_NS.load(Relaxed);
                let tick = PROBE_TICK_NS.load(Relaxed);
                if tick != 0 {
                    let silent = now
                        .saturating_sub(tick)
                        .saturating_sub(PROBE_NS.load(Relaxed));
                    STALL_NOW_NS.store(silent, Relaxed);
                    if silent >= stall && reported_tick != tick {
                        reported_tick = tick;
                        WATCHDOG_STALLS.fetch_add(1, Relaxed);
                        push_long_poll(TaskKind::ProbeStalled, SOURCE_WATCHDOG, silent);
                    }
                }
                for (index, slot) in ACTIVE.iter().enumerate() {
                    let value = slot.load(Relaxed);
                    if value == 0 || reported_active[index] == value {
                        continue;
                    }
                    let start = value & ((1 << ACTIVE_SHIFT) - 1);
                    let running = now.saturating_sub(start);
                    if running >= stall {
                        reported_active[index] = value;
                        push_long_poll(
                            TaskKind::from_u8((value >> ACTIVE_SHIFT) as u8),
                            SOURCE_WATCHDOG,
                            running,
                        );
                    }
                }
            }
        });
}

// ---------------------------------------------------------------------------
// Slow reducer traces
// ---------------------------------------------------------------------------

/// Per-call stage breakdown kept for reducers slower than
/// `GONVEX_PROFILER_SLOW_MS`. Index `i` of [`TraceValues`] is `TRACE_FIELDS[i]`
/// (ns, except `invoke.host_call_count`).
pub const TRACE_FIELDS: [&str; 10] = [
    "fn.total",
    "fn.prepare",
    "invoke.connect",
    "invoke.first_frame",
    "invoke.js",
    "invoke.host_calls",
    "db.writer_wait",
    "db.sql",
    "fn.finish",
    "invoke.host_call_count",
];
pub type TraceValues = [u64; TRACE_FIELDS.len()];

const TRACE_RING: usize = 128;
struct TraceSlot {
    seq: AtomicU64,
    at_unix_ms: AtomicU64,
    label: AtomicU64,
    values: [AtomicU64; TRACE_FIELDS.len()],
}
#[allow(clippy::declare_interior_mutable_const)]
const TRACE_SLOT: TraceSlot = TraceSlot {
    seq: AtomicU64::new(0),
    at_unix_ms: AtomicU64::new(0),
    label: AtomicU64::new(0),
    values: [const { AtomicU64::new(0) }; TRACE_FIELDS.len()],
};
static TRACES: [TraceSlot; TRACE_RING] = [TRACE_SLOT; TRACE_RING];
static TRACE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Whether a call of this total duration would be kept (callers can skip
/// building the breakdown otherwise).
#[inline]
pub fn is_slow(total_ns: u64) -> bool {
    enabled() && total_ns >= SLOW_NS.load(Relaxed)
}

/// Keeps the breakdown of one slow call (`values[0]` is the total) in a
/// 128-slot lock-free ring.
pub fn record_slow_trace(label: Label, values: &TraceValues) {
    if !is_slow(values[0]) {
        return;
    }
    let seq = TRACE_SEQ.fetch_add(1, Relaxed) + 1;
    let slot = &TRACES[(seq as usize) % TRACE_RING];
    slot.seq.store(0, std::sync::atomic::Ordering::Release);
    slot.at_unix_ms.store(unix_ms(), Relaxed);
    slot.label.store(label as u64, Relaxed);
    for (target, value) in slot.values.iter().zip(values) {
        target.store(*value, Relaxed);
    }
    slot.seq.store(seq, std::sync::atomic::Ordering::Release);
}

/// One kept slow call.
#[derive(Clone, Debug)]
pub struct SlowTrace {
    pub seq: u64,
    pub at_unix_ms: u64,
    pub label: Label,
    pub values: TraceValues,
}

/// Kept slow calls, newest first, and the total ever recorded.
pub fn slow_traces() -> (Vec<SlowTrace>, u64) {
    let mut out = Vec::new();
    for slot in &TRACES {
        let seq = slot.seq.load(std::sync::atomic::Ordering::Acquire);
        if seq == 0 {
            continue;
        }
        let at_unix_ms = slot.at_unix_ms.load(Relaxed);
        let label = Label::from_u8(slot.label.load(Relaxed) as u8);
        let mut values = [0u64; TRACE_FIELDS.len()];
        for (value, source) in values.iter_mut().zip(&slot.values) {
            *value = source.load(Relaxed);
        }
        if slot.seq.load(std::sync::atomic::Ordering::Acquire) == seq {
            out.push(SlowTrace {
                seq,
                at_unix_ms,
                label,
                values,
            });
        }
    }
    out.sort_by(|a, b| b.seq.cmp(&a.seq));
    (out, TRACE_SEQ.load(Relaxed))
}

// ---------------------------------------------------------------------------
// Read side
// ---------------------------------------------------------------------------

/// Aggregated histogram of one (stage, label).
#[derive(Clone, Debug)]
pub struct StageSnapshot {
    pub stage: Stage,
    pub label: Label,
    pub count: u64,
    pub sum: u64,
    pub max: u64,
    /// Non-empty buckets as `(index, count)`.
    pub buckets: Vec<(u16, u64)>,
}

impl StageSnapshot {
    pub fn name(&self) -> &'static str {
        self.stage.info().name
    }

    /// Quantile estimate (linear interpolation inside the bucket, capped at max).
    pub fn quantile(&self, q: f64) -> u64 {
        quantile(&self.buckets, self.count, self.max, q)
    }
}

/// Quantile over sparse `(bucket, count)` pairs.
pub fn quantile(buckets: &[(u16, u64)], count: u64, max: u64, q: f64) -> u64 {
    if count == 0 {
        return 0;
    }
    let rank = ((q.clamp(0.0, 1.0) * count as f64).ceil() as u64).max(1);
    let mut seen = 0u64;
    for &(index, hits) in buckets {
        if seen + hits >= rank {
            let (lower, upper) = bucket_bounds(index as usize);
            let upper = upper.min(max.max(lower));
            let fraction = (rank - seen) as f64 / hits as f64;
            return (lower as f64 + (upper - lower) as f64 * fraction) as u64;
        }
        seen += hits;
    }
    max
}

/// Every (stage, label) with at least one record.
pub fn snapshot() -> Vec<StageSnapshot> {
    let mut out = Vec::new();
    for (stage_index, labels) in HISTOGRAMS.iter().enumerate() {
        for (label_index, shards) in labels.iter().enumerate() {
            let mut counts = [0u64; BUCKETS];
            let mut sum = 0u64;
            let mut max = 0u64;
            for shard in shards {
                for (index, bucket) in shard.buckets.iter().enumerate() {
                    counts[index] += bucket.load(Relaxed);
                }
                sum = sum.wrapping_add(shard.sum.load(Relaxed));
                max = max.max(shard.max.load(Relaxed));
            }
            let count: u64 = counts.iter().sum();
            if count == 0 {
                continue;
            }
            out.push(StageSnapshot {
                stage: stage_from_index(stage_index),
                label: Label::from_u8(label_index as u8),
                count,
                sum,
                max,
                buckets: counts
                    .iter()
                    .enumerate()
                    .filter(|(_, hits)| **hits > 0)
                    .map(|(index, hits)| (index as u16, *hits))
                    .collect(),
            });
        }
    }
    out
}

fn stage_from_index(index: usize) -> Stage {
    assert!(index < STAGE_COUNT);
    // SAFETY: `Stage` is `repr(u8)` with contiguous discriminants 0..STAGE_COUNT.
    unsafe { std::mem::transmute::<u8, Stage>(index as u8) }
}

/// One gauge reading.
#[derive(Clone, Debug)]
pub struct GaugeSnapshot {
    pub name: &'static str,
    pub doc: &'static str,
    pub current: i64,
    pub peak: u64,
}

pub fn gauges() -> Vec<GaugeSnapshot> {
    GAUGES_INFO
        .iter()
        .zip(GAUGES.iter())
        .map(|((name, doc), gauge)| GaugeSnapshot {
            name,
            doc,
            current: gauge.current(),
            peak: gauge.peak(),
        })
        .collect()
}

/// Seconds since the profiler epoch.
pub fn uptime() -> Duration {
    epoch().elapsed()
}

/// Zeroes every histogram (tests and benchmarks only; racy with writers).
pub fn reset() {
    for labels in &HISTOGRAMS {
        for shards in labels {
            for shard in shards {
                for bucket in &shard.buckets {
                    bucket.store(0, Relaxed);
                }
                shard.sum.store(0, Relaxed);
                shard.max.store(0, Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_bounded() {
        let mut previous = 0;
        for ns in (0..40).map(|shift| 1u64 << shift) {
            let index = bucket_of(ns);
            assert!(index >= previous);
            previous = index;
        }
        for ns in [
            0,
            1023,
            1024,
            1500,
            3000,
            1 << 35,
            (1 << 36) - 1,
            70_000_000_000,
        ] {
            let index = bucket_of(ns);
            let (lower, upper) = bucket_bounds(index);
            assert!(
                lower <= ns && ns < upper,
                "{ns} not in {lower}..{upper} (bucket {index})"
            );
        }
        assert_eq!(bucket_of(u64::MAX), BUCKETS - 1);
        for index in 1..BUCKETS - 1 {
            assert_eq!(bucket_bounds(index).1, bucket_bounds(index + 1).0);
            let (lower, upper) = bucket_bounds(index);
            assert_eq!(bucket_of(lower), index);
            assert_eq!(bucket_of(upper - 1), index);
        }
    }

    #[test]
    fn records_quantiles_and_labels() {
        set_enabled(true);
        for value in 1..=1000u64 {
            record(Stage::PollOther, Label::Action, value * 1000);
        }
        let stage = snapshot()
            .into_iter()
            .find(|s| s.stage == Stage::PollOther && s.label == Label::Action)
            .unwrap();
        assert_eq!(stage.count, 1000);
        assert_eq!(stage.max, 1_000_000);
        let p50 = stage.quantile(0.5) as f64;
        assert!((p50 - 500_000.0).abs() / 500_000.0 < 0.2, "{p50}");
        let p99 = stage.quantile(0.99) as f64;
        assert!((p99 - 990_000.0).abs() / 990_000.0 < 0.2, "{p99}");
        assert!(stage.quantile(1.0) <= 1_000_000);
    }

    #[test]
    fn stamps_round_trip_and_reject_other_keys() {
        set_enabled(true);
        let key = stamp_key("sqlite:///tenant.db", 42);
        stamp(key);
        assert!(since_stamp(key).is_some());
        assert!(since_stamp(stamp_key("sqlite:///tenant.db", 43)).is_none());
    }

    #[test]
    fn gauges_track_current_and_peak() {
        set_enabled(true);
        let gauge = gauge(GaugeId::WsSending);
        {
            let _a = gauge.guard();
            let _b = gauge.guard();
            assert!(gauge.current() >= 2);
            assert!(gauge.peak() >= 2);
        }
        let mut backlog = Backlog::new(3);
        backlog.sent();
        assert!(super::gauge(GaugeId::OutboundFrames).current() >= 2);
        drop(backlog);
    }

    #[test]
    fn long_polls_land_in_the_ring() {
        set_enabled(true);
        struct Slow;
        impl Future for Slow {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                std::thread::sleep(Duration::from_millis(6));
                Poll::Ready(())
            }
        }
        let before = long_poll_total();
        let waker = std::task::Waker::noop();
        let mut future = std::pin::pin!(instrument(TaskKind::Outbox, Slow));
        assert!(future
            .as_mut()
            .poll(&mut Context::from_waker(waker))
            .is_ready());
        assert!(long_poll_total() > before);
        assert!(long_polls()
            .iter()
            .any(|poll| poll.task == "outbox" && poll.duration_ns >= 5_000_000));
    }

    #[test]
    fn slow_traces_keep_only_slow_calls() {
        set_enabled(true);
        let mut values = [0u64; TRACE_FIELDS.len()];
        values[0] = 1_000;
        record_slow_trace(Label::Heartbeat, &values);
        values[0] = 500_000_000;
        values[6] = 400_000_000;
        record_slow_trace(Label::Heartbeat, &values);
        let (traces, _) = slow_traces();
        assert!(traces.iter().all(|trace| trace.values[0] >= 100_000_000));
        assert!(traces
            .iter()
            .any(|trace| trace.values[6] == 400_000_000 && trace.label == Label::Heartbeat));
    }

    #[test]
    fn classify_is_fixed_and_cheap() {
        assert_eq!(
            Label::classify("reducer", "presence.beat"),
            Label::Heartbeat
        );
        assert_eq!(
            Label::classify("reducer", "control.support.heartbeat"),
            Label::Heartbeat
        );
        assert_eq!(
            Label::classify("reducer", "presence.transition"),
            Label::Presence
        );
        assert_eq!(Label::classify("reducer", "tasks.save"), Label::Reducer);
        assert_eq!(Label::classify("query", "tasks.list"), Label::Query);
        assert_eq!(Label::from_u8(Label::Cron as u8), Label::Cron);
        assert_eq!(LABELS[Label::Cron as usize], "cron");
        assert_eq!(STAGES.len(), STAGE_COUNT);
        assert_eq!(STAGES[Stage::PollOther as usize].name, "poll.other");
    }
}
