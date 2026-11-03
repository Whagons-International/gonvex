//! Runtime side of `gonvex-profiler`: the scheduler-lag probe, tokio runtime
//! metrics, and the JSON dump served by `/dev/profile` and
//! `/dev/metrics?format=profile`. See `docs/performance/profiler.md`.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

static WORKERS: AtomicU64 = AtomicU64::new(0);
static ALIVE_TASKS: AtomicU64 = AtomicU64::new(0);
static GLOBAL_QUEUE: AtomicU64 = AtomicU64::new(0);
static GLOBAL_QUEUE_PEAK: AtomicU64 = AtomicU64::new(0);
static BUSY_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Worker busy ratio over the last ~1 s sample, in parts per million.
static BUSY_PPM: AtomicU64 = AtomicU64::new(0);
static PARKS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Reads the environment, starts the watchdog thread and the probe task
/// (once per process; later calls are no-ops).
pub fn start() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        let config = gonvex_profiler::init_from_env();
        if !config.enabled {
            tracing::info!("profiler disabled (GONVEX_PROFILER=0)");
            return;
        }
        gonvex_profiler::start_watchdog();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(probe(handle.clone(), config.probe_interval));
        }
    });
}

/// Sleeps `interval` in a loop and records how late each wake-up is
/// (`tokio.sched_lag`); the watchdog thread notices when it stops ticking.
/// Once a second it also samples the stable tokio runtime metrics.
async fn probe(handle: tokio::runtime::Handle, interval: Duration) {
    let metrics = handle.metrics();
    let mut sampled_at = Instant::now();
    let mut busy_before = busy_total(&metrics);
    loop {
        let before = Instant::now();
        tokio::time::sleep(interval).await;
        let late = before.elapsed().saturating_sub(interval);
        gonvex_profiler::probe_tick(late.as_nanos() as u64);
        let since = sampled_at.elapsed();
        if since >= Duration::from_secs(1) {
            let workers = metrics.num_workers();
            let busy = busy_total(&metrics);
            let delta = busy.saturating_sub(busy_before);
            let capacity = since.as_nanos() as u64 * workers.max(1) as u64;
            BUSY_PPM.store(delta.saturating_mul(1_000_000) / capacity.max(1), Relaxed);
            BUSY_NS_TOTAL.store(busy, Relaxed);
            busy_before = busy;
            sampled_at = Instant::now();
            WORKERS.store(workers as u64, Relaxed);
            ALIVE_TASKS.store(metrics.num_alive_tasks() as u64, Relaxed);
            PARKS_TOTAL.store(
                (0..workers).map(|worker| metrics.worker_park_count(worker)).sum(),
                Relaxed,
            );
        }
        // The injection queue is cheap to read: sample it every tick.
        let depth = metrics.global_queue_depth() as u64;
        GLOBAL_QUEUE.store(depth, Relaxed);
        GLOBAL_QUEUE_PEAK.fetch_max(depth, Relaxed);
    }
}

fn busy_total(metrics: &tokio::runtime::RuntimeMetrics) -> u64 {
    (0..metrics.num_workers())
        .map(|worker| metrics.worker_total_busy_duration(worker).as_nanos() as u64)
        .sum()
}

fn ms(ns: u64) -> f64 {
    (ns as f64 / 1e6 * 1000.0).round() / 1000.0
}

fn tokio_json() -> Value {
    json!({
        "workers": WORKERS.load(Relaxed),
        "aliveTasks": ALIVE_TASKS.load(Relaxed),
        "globalQueueDepth": GLOBAL_QUEUE.load(Relaxed),
        "globalQueueDepthPeak": GLOBAL_QUEUE_PEAK.load(Relaxed),
        "busyRatio": BUSY_PPM.load(Relaxed) as f64 / 1e6,
        "busyNsTotal": BUSY_NS_TOTAL.load(Relaxed),
        "parksTotal": PARKS_TOTAL.load(Relaxed),
        "unavailableWithoutTokioUnstable": [
            "worker_local_queue_depth", "poll_time_histogram", "worker_poll_count",
            "worker_steal_count", "blocking_queue_depth", "schedule_latency_histogram"
        ],
    })
}

fn gauges_json() -> Value {
    let mut gauges = serde_json::Map::new();
    for gauge in gonvex_profiler::gauges() {
        gauges.insert(
            gauge.name.to_owned(),
            json!({"current": gauge.current, "peak": gauge.peak}),
        );
    }
    gauges.insert(
        "snapshot.permits_available".to_owned(),
        json!({"current": crate::replica::snapshot_permits_available(), "peak": Value::Null}),
    );
    Value::Object(gauges)
}

fn writer_queues_json(runtime: &crate::Runtime) -> Value {
    Value::Array(
        runtime
            .inner
            .pools
            .writer_queues(10)
            .into_iter()
            .map(|queue| {
                let database = std::path::Path::new(&queue.database)
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or(queue.database);
                json!({
                    "database": database,
                    "waiting": queue.waiting,
                    "acquisitions": queue.acquisitions,
                    "waitNsTotal": queue.wait_ns,
                    "waitMaxNs": queue.wait_max_ns,
                })
            })
            .collect(),
    )
}

/// Full profile dump: per-stage histograms (with sparse buckets for deltas),
/// gauges, writer queues, tokio metrics, watchdog state and recent long polls.
pub fn profile_json(runtime: &crate::Runtime) -> Value {
    let config = gonvex_profiler::config();
    let stages = gonvex_profiler::snapshot()
        .into_iter()
        .map(|stage| {
            let info = stage.stage.info();
            json!({
                "stage": info.name,
                "group": info.group,
                "label": stage.label.name(),
                "count": stage.count,
                "sumNs": stage.sum,
                "maxNs": stage.max,
                "p50Ns": stage.quantile(0.50),
                "p90Ns": stage.quantile(0.90),
                "p99Ns": stage.quantile(0.99),
                "p999Ns": stage.quantile(0.999),
                "buckets": stage.buckets,
            })
        })
        .collect::<Vec<_>>();
    let bounds = (0..gonvex_profiler::BUCKETS)
        .map(|index| gonvex_profiler::bucket_bounds(index).0)
        .collect::<Vec<_>>();
    let (silent_ns, stalls) = gonvex_profiler::watchdog_state();
    let long_polls = gonvex_profiler::long_polls()
        .into_iter()
        .take(64)
        .map(|poll| {
            json!({
                "seq": poll.seq,
                "atUnixMs": poll.at_unix_ms,
                "durationMs": ms(poll.duration_ns),
                "task": poll.task,
                "source": poll.source,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "format": "gonvex-profile/1",
        "enabled": config.enabled,
        "generatedAtUnixMs": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_millis() as u64)
            .unwrap_or(0),
        "uptimeNs": gonvex_profiler::uptime().as_nanos() as u64,
        "config": {
            "longPollMs": config.long_poll.as_secs_f64() * 1e3,
            "stallMs": config.stall.as_secs_f64() * 1e3,
            "probeMs": config.probe_interval.as_secs_f64() * 1e3,
            "slowTraceMs": config.slow_trace.as_secs_f64() * 1e3,
        },
        "bucketLowerBoundsNs": bounds,
        "stageInfo": gonvex_profiler::STAGES.iter().map(|info| json!({
            "stage": info.name, "group": info.group, "doc": info.doc,
        })).collect::<Vec<_>>(),
        "stages": stages,
        "gauges": gauges_json(),
        "writerQueues": writer_queues_json(runtime),
        "tokio": tokio_json(),
        "watchdog": {"probeSilentMs": ms(silent_ns), "stalls": stalls},
        "longPolls": {"total": gonvex_profiler::long_poll_total(), "recent": long_polls},
        "slowTraces": slow_traces_json(),
        "crons": crons_json(),
    })
}

/// Per-cron totals since start (`gonvex_profiler::cron`), one row per cron
/// that ran; `(one-shot)` is runAt/runAfter/wakeAt work.
fn crons_json() -> Value {
    json!({
        "fields": gonvex_profiler::cron::CRON_FIELDS,
        "rows": gonvex_profiler::cron::snapshot().into_iter().map(|row| json!({
            "slot": row.slot,
            "name": row.name,
            "values": row.values,
        })).collect::<Vec<_>>(),
    })
}

/// Reducers slower than `GONVEX_PROFILER_SLOW_MS`, each with its own
/// per-stage breakdown (newest first, at most 128).
fn slow_traces_json() -> Value {
    let (traces, total) = gonvex_profiler::slow_traces();
    json!({
        "total": total,
        "fields": gonvex_profiler::TRACE_FIELDS,
        "recent": traces.into_iter().map(|trace| json!({
            "seq": trace.seq,
            "atUnixMs": trace.at_unix_ms,
            "label": trace.label.name(),
            "values": trace.values,
        })).collect::<Vec<_>>(),
    })
}

/// Compact summary embedded in `/dev/metrics` (no buckets, no ring).
pub fn summary_json() -> Value {
    let stages = gonvex_profiler::snapshot()
        .into_iter()
        .map(|stage| {
            json!({
                "stage": stage.stage.info().name,
                "label": stage.label.name(),
                "count": stage.count,
                "p50Ms": ms(stage.quantile(0.50)),
                "p99Ms": ms(stage.quantile(0.99)),
                "maxMs": ms(stage.max),
            })
        })
        .collect::<Vec<_>>();
    let (silent_ns, stalls) = gonvex_profiler::watchdog_state();
    json!({
        "enabled": gonvex_profiler::enabled(),
        "stages": stages,
        "gauges": gauges_json(),
        "tokio": tokio_json(),
        "watchdog": {"probeSilentMs": ms(silent_ns), "stalls": stalls},
        "longPolls": gonvex_profiler::long_poll_total(),
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn summary_reports_recorded_stages_and_gauges() {
        gonvex_profiler::set_enabled(true);
        gonvex_profiler::record(
            gonvex_profiler::Stage::FnTotal,
            gonvex_profiler::Label::Heartbeat,
            2_000_000,
        );
        let summary = super::summary_json();
        let stages = summary["stages"].as_array().unwrap();
        assert!(stages
            .iter()
            .any(|stage| stage["stage"] == "fn.total" && stage["label"] == "heartbeat"));
        assert!(summary["gauges"]["module_host.inflight"]["current"].is_i64());
        assert!(summary["gauges"]["snapshot.permits_available"]["current"].is_u64());
    }
}
