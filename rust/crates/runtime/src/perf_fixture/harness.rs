//! A tiny benchmark harness: warmup + N timed iterations, median/p90/p99,
//! allocation counts and heap-live bytes from a counting global allocator.
//!
//! A bench binary installs the allocator itself (the library cannot):
//!
//! ```ignore
//! use gonvex_runtime::perf_fixture::harness::CountingAlloc;
//! #[global_allocator]
//! static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);
//! ```
//!
//! Counters are process-wide, so allocations made on SQLite worker threads
//! and tokio workers during an operation are included in its numbers.
//! Without the allocator installed, allocation fields are reported as null.
//!
//! Results may get extra fields after they run (`result.extra`); then
//! [`Report::finish`] prints one `BENCH_JSON {...}` line per bench (also
//! appended to `$BENCH_JSON_OUT` when set) and a readable table.
//! Environment knobs: `BENCH_FILTER` (substring of bench names to run),
//! `BENCH_ITERS` (override every bench's iteration count),
//! `BENCH_WARMUP` (override warmup counts).

use std::alloc::{GlobalAlloc, Layout};
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static PEAK_LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

thread_local! {
    /// Allocations (and reallocations) made by this thread (`raw_sites`).
    static THREAD_ALLOCATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Allocations made by the calling thread so far (0 without the counting
/// allocator).
pub fn thread_allocations() -> u64 {
    THREAD_ALLOCATIONS.with(|count| count.get())
}

#[inline]
fn count_thread_allocation() {
    // `try_with`: the allocator also runs during thread-local teardown.
    let _ = THREAD_ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
}
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// Counting wrapper around any global allocator (use mimalloc to match
/// production). Counts every allocation and reallocation, their requested
/// bytes, and the bytes currently live.
pub struct CountingAlloc<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        self.0.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        self.0.alloc_zeroed(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as i64, Relaxed);
        self.0.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        count_thread_allocation();
        ALLOCATED_BYTES.fetch_add(new_size as u64, Relaxed);
        let delta = new_size as i64 - layout.size() as i64;
        let live = LIVE_BYTES.fetch_add(delta, Relaxed) + delta;
        PEAK_LIVE_BYTES.fetch_max(live, Relaxed);
        self.0.realloc(ptr, layout, new_size)
    }
}

#[inline]
fn record_alloc(size: usize) {
    if !INSTALLED.load(Relaxed) {
        INSTALLED.store(true, Relaxed);
    }
    ALLOCATIONS.fetch_add(1, Relaxed);
    count_thread_allocation();
    ALLOCATED_BYTES.fetch_add(size as u64, Relaxed);
    let live = LIVE_BYTES.fetch_add(size as i64, Relaxed) + size as i64;
    PEAK_LIVE_BYTES.fetch_max(live, Relaxed);
}

/// Restarts peak tracking at the current heap-live bytes, which it returns.
pub fn reset_peak() -> i64 {
    let live = LIVE_BYTES.load(Relaxed);
    PEAK_LIVE_BYTES.store(live, Relaxed);
    live
}

/// Highest heap-live bytes since the last [`reset_peak`].
pub fn peak_live_bytes() -> i64 {
    PEAK_LIVE_BYTES.load(Relaxed)
}

/// Process-wide allocator counters at one instant.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllocSnapshot {
    pub allocations: u64,
    pub allocated_bytes: u64,
    /// Bytes allocated and not yet freed (heap-live).
    pub live_bytes: i64,
}

impl AllocSnapshot {
    pub fn now() -> Self {
        Self {
            allocations: ALLOCATIONS.load(Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Relaxed),
            live_bytes: LIVE_BYTES.load(Relaxed),
        }
    }
    pub fn since(self, earlier: Self) -> Self {
        Self {
            allocations: self.allocations - earlier.allocations,
            allocated_bytes: self.allocated_bytes - earlier.allocated_bytes,
            live_bytes: self.live_bytes - earlier.live_bytes,
        }
    }
}

/// True once the counting allocator has seen an allocation.
pub fn counting_installed() -> bool {
    INSTALLED.load(Relaxed)
}

/// Resident set size of this process (Linux `/proc/self/statm`), if known.
pub fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

/// Heap-live bytes (and RSS) retained by whatever `work` returns and keeps.
/// Run it with the value still alive: e.g. open subscriptions and keep them.
pub async fn retained<T, F: Future<Output = T>>(work: F) -> (T, Retained) {
    let rss_before = rss_bytes();
    let base = reset_peak();
    let before = AllocSnapshot::now();
    let value = work.await;
    let after = AllocSnapshot::now().since(before);
    let peak = peak_live_bytes() - base;
    let rss_after = rss_bytes();
    (
        value,
        Retained {
            live_bytes: counting_installed().then_some(after.live_bytes),
            peak_bytes: counting_installed().then_some(peak),
            rss_bytes: rss_before
                .zip(rss_after)
                .map(|(before, after)| after as i64 - before as i64),
        },
    )
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Retained {
    /// Heap-live bytes still held after `work` (what it retained).
    pub live_bytes: Option<i64>,
    /// Peak heap-live growth while `work` ran.
    pub peak_bytes: Option<i64>,
    pub rss_bytes: Option<i64>,
}

/// Warmup and measured iteration counts.
#[derive(Clone, Copy, Debug)]
pub struct BenchConfig {
    pub warmup: usize,
    pub iterations: usize,
}

impl BenchConfig {
    pub fn new(warmup: usize, iterations: usize) -> Self {
        let env = |name: &str| std::env::var(name).ok().and_then(|v| v.parse().ok());
        Self {
            warmup: env("BENCH_WARMUP").unwrap_or(warmup),
            iterations: env("BENCH_ITERS").unwrap_or(iterations).max(1),
        }
    }
}

/// One finished benchmark.
#[derive(Clone, Debug)]
pub struct BenchResult {
    pub name: String,
    pub iterations: usize,
    /// Divides every per-op number (e.g. subscribers per fan-out op).
    pub units_per_op: f64,
    pub unit: String,
    pub median_ns: f64,
    pub p90_ns: f64,
    pub p99_ns: f64,
    pub mean_ns: f64,
    pub min_ns: f64,
    pub max_ns: f64,
    pub allocations_per_op: Option<f64>,
    pub bytes_per_op: Option<f64>,
    /// Heap-live change across all measured iterations, per op.
    pub live_bytes_per_op: Option<f64>,
    /// Largest heap-live growth above the start of one op, over all
    /// measured iterations (process-wide, so concurrent work counts too).
    pub peak_heap_bytes: Option<i64>,
    /// Free-form numbers (rows, payload sizes, retained bytes...).
    pub extra: BTreeMap<String, Value>,
}

impl BenchResult {
    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "iterations": self.iterations,
            "unit": self.unit,
            "unitsPerOp": self.units_per_op,
            "medianNs": self.median_ns.round(),
            "p90Ns": self.p90_ns.round(),
            "p99Ns": self.p99_ns.round(),
            "meanNs": self.mean_ns.round(),
            "minNs": self.min_ns.round(),
            "maxNs": self.max_ns.round(),
            "allocationsPerOp": self.allocations_per_op.map(|v| (v * 10.0).round() / 10.0),
            "bytesPerOp": self.bytes_per_op.map(f64::round),
            "liveBytesPerOp": self.live_bytes_per_op.map(f64::round),
            "peakHeapBytes": self.peak_heap_bytes,
            "extra": self.extra,
        })
    }
}

/// Collects results and prints them.
#[derive(Default)]
pub struct Report {
    pub results: Vec<BenchResult>,
    filter: Option<String>,
}

impl Report {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            filter: std::env::var("BENCH_FILTER").ok().filter(|f| !f.is_empty()),
        }
    }

    /// Whether `name` passes `BENCH_FILTER`.
    pub fn enabled(&self, name: &str) -> bool {
        self.filter
            .as_deref()
            .is_none_or(|filter| name.contains(filter))
    }

    /// Times `op` (an async closure taking the iteration index). `setup`
    /// runs before each iteration, outside the timed window, and hands its
    /// value to `op`.
    pub async fn run<S, SF, I, O, OF>(
        &mut self,
        name: &str,
        config: BenchConfig,
        units_per_op: f64,
        unit: &str,
        mut setup: S,
        mut op: O,
    ) -> Option<&mut BenchResult>
    where
        S: FnMut(usize) -> SF,
        SF: Future<Output = I>,
        O: FnMut(usize, I) -> OF,
        OF: Future<Output = ()>,
    {
        if !self.enabled(name) {
            return None;
        }
        for i in 0..config.warmup {
            let input = setup(i).await;
            op(i, input).await;
        }
        let mut samples = Vec::with_capacity(config.iterations);
        let mut allocs = AllocSnapshot::default();
        let mut peak = 0i64;
        for i in 0..config.iterations {
            let index = config.warmup + i;
            let input = setup(index).await;
            let base = reset_peak();
            let before = AllocSnapshot::now();
            let started = Instant::now();
            op(index, input).await;
            let elapsed = started.elapsed();
            peak = peak.max(peak_live_bytes() - base);
            let delta = AllocSnapshot::now().since(before);
            allocs.allocations += delta.allocations;
            allocs.allocated_bytes += delta.allocated_bytes;
            allocs.live_bytes += delta.live_bytes;
            samples.push(elapsed);
        }
        let result = summarize(name, units_per_op, unit, &samples, allocs, peak);
        progress(&result);
        self.results.push(result);
        self.results.last_mut()
    }

    /// [`Report::run`] without per-iteration setup.
    pub async fn run_simple<O, OF>(
        &mut self,
        name: &str,
        config: BenchConfig,
        units_per_op: f64,
        unit: &str,
        mut op: O,
    ) -> Option<&mut BenchResult>
    where
        O: FnMut(usize) -> OF,
        OF: Future<Output = ()>,
    {
        self.run(
            name,
            config,
            units_per_op,
            unit,
            |_| async {},
            |i, ()| op(i),
        )
        .await
    }

    /// Synchronous variant of [`Report::run_simple`].
    pub fn run_sync<O: FnMut(usize)>(
        &mut self,
        name: &str,
        config: BenchConfig,
        units_per_op: f64,
        unit: &str,
        mut op: O,
    ) -> Option<&mut BenchResult> {
        if !self.enabled(name) {
            return None;
        }
        for i in 0..config.warmup {
            op(i);
        }
        let mut samples = Vec::with_capacity(config.iterations);
        let mut allocs = AllocSnapshot::default();
        let mut peak = 0i64;
        for i in 0..config.iterations {
            let base = reset_peak();
            let before = AllocSnapshot::now();
            let started = Instant::now();
            op(config.warmup + i);
            let elapsed = started.elapsed();
            peak = peak.max(peak_live_bytes() - base);
            let delta = AllocSnapshot::now().since(before);
            allocs.allocations += delta.allocations;
            allocs.allocated_bytes += delta.allocated_bytes;
            allocs.live_bytes += delta.live_bytes;
            samples.push(elapsed);
        }
        let result = summarize(name, units_per_op, unit, &samples, allocs, peak);
        progress(&result);
        self.results.push(result);
        self.results.last_mut()
    }

    /// Records a measurement that is not a timing loop (e.g. retained bytes).
    pub fn record(&mut self, name: &str, extra: BTreeMap<String, Value>) {
        if !self.enabled(name) {
            return;
        }
        self.results.push(BenchResult {
            name: name.to_owned(),
            iterations: 0,
            units_per_op: 1.0,
            unit: "op".to_owned(),
            median_ns: 0.0,
            p90_ns: 0.0,
            p99_ns: 0.0,
            mean_ns: 0.0,
            min_ns: 0.0,
            max_ns: 0.0,
            allocations_per_op: None,
            bytes_per_op: None,
            live_bytes_per_op: None,
            peak_heap_bytes: None,
            extra,
        });
        eprintln!("done {name}");
    }

    /// Prints one `BENCH_JSON` line per result (also appended to
    /// `$BENCH_JSON_OUT`), then the readable table.
    pub fn finish(&self) {
        let mut out = std::env::var("BENCH_JSON_OUT").ok().and_then(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()
        });
        for result in &self.results {
            let json = result.to_json();
            println!("BENCH_JSON {json}");
            if let Some(file) = out.as_mut() {
                let _ = writeln!(file, "{json}");
            }
        }
        self.print_table();
    }

    /// Human-readable summary of every result.
    pub fn print_table(&self) {
        println!();
        println!(
            "{:<58} {:>12} {:>12} {:>12} {:>11} {:>12} {:>12}",
            "bench", "median", "p90", "p99", "allocs/op", "bytes/op", "per unit"
        );
        for r in &self.results {
            if r.iterations == 0 {
                let extra = serde_json::to_string(&r.extra).unwrap_or_default();
                println!("{:<58} {extra}", r.name);
                continue;
            }
            let per_unit = if r.units_per_op > 1.0 {
                format!("{}/{}", fmt_ns(r.median_ns / r.units_per_op), r.unit)
            } else {
                String::new()
            };
            println!(
                "{:<58} {:>12} {:>12} {:>12} {:>11} {:>12} {:>12}",
                r.name,
                fmt_ns(r.median_ns),
                fmt_ns(r.p90_ns),
                fmt_ns(r.p99_ns),
                r.allocations_per_op
                    .map(|v| format!("{v:.0}"))
                    .unwrap_or_else(|| "-".into()),
                r.bytes_per_op.map(fmt_bytes).unwrap_or_else(|| "-".into()),
                per_unit,
            );
        }
    }
}

fn progress(result: &BenchResult) {
    eprintln!(
        "done {} median {} p99 {}",
        result.name,
        fmt_ns(result.median_ns),
        fmt_ns(result.p99_ns)
    );
}

fn summarize(
    name: &str,
    units_per_op: f64,
    unit: &str,
    samples: &[Duration],
    allocs: AllocSnapshot,
    peak: i64,
) -> BenchResult {
    let mut ns: Vec<f64> = samples.iter().map(|d| d.as_nanos() as f64).collect();
    ns.sort_by(f64::total_cmp);
    let pick = |q: f64| ns[((ns.len() as f64 - 1.0) * q).round() as usize];
    let n = ns.len() as f64;
    let installed = counting_installed();
    BenchResult {
        name: name.to_owned(),
        iterations: ns.len(),
        units_per_op,
        unit: unit.to_owned(),
        median_ns: pick(0.5),
        p90_ns: pick(0.9),
        p99_ns: pick(0.99),
        mean_ns: ns.iter().sum::<f64>() / n,
        min_ns: ns[0],
        max_ns: ns[ns.len() - 1],
        allocations_per_op: installed.then(|| allocs.allocations as f64 / n),
        bytes_per_op: installed.then(|| allocs.allocated_bytes as f64 / n),
        live_bytes_per_op: installed.then(|| allocs.live_bytes as f64 / n),
        peak_heap_bytes: installed.then_some(peak),
        extra: BTreeMap::new(),
    }
}

pub fn fmt_ns(ns: f64) -> String {
    if ns >= 1e9 {
        format!("{:.2}s", ns / 1e9)
    } else if ns >= 1e6 {
        format!("{:.2}ms", ns / 1e6)
    } else if ns >= 1e3 {
        format!("{:.1}us", ns / 1e3)
    } else {
        format!("{ns:.0}ns")
    }
}

pub fn fmt_bytes(bytes: f64) -> String {
    if bytes.abs() >= 1048576.0 {
        format!("{:.1}MiB", bytes / 1048576.0)
    } else if bytes.abs() >= 1024.0 {
        format!("{:.1}KiB", bytes / 1024.0)
    } else {
        format!("{bytes:.0}B")
    }
}
