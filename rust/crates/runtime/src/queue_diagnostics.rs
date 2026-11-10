//! Explicit synthetic ownership probes. Not part of the production runtime.
use super::*;
use crate::perf_fixture::{
    harness::{rss_bytes, AllocSnapshot},
    Fixture, FixtureOptions, MemberKind,
};
use futures_util::FutureExt;
use serde_json::json;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
fn cpu_ns() -> u64 {
    let mut t: libc::timespec = unsafe { std::mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut t) };
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}
fn emit(v: serde_json::Value) {
    eprintln!("QUEUE_PROBE {v}");
    if let Ok(p) = std::env::var("QUEUE_PROBE_OUT") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .unwrap();
        writeln!(f, "{v}").unwrap();
        f.sync_all().unwrap();
    }
}
fn request(id: &str) -> gonvex_protocol::ReplicaOpenRequest {
    serde_json::from_value(json!({"id":id,"path":"tasks.recentWorkspaceTasks","args":{}})).unwrap()
}
fn waiting() -> i64 {
    gonvex_profiler::gauge(gonvex_profiler::GaugeId::SnapshotWaiting).current()
}
async fn fixture() -> Fixture {
    let mut o = FixtureOptions::whagons();
    o.tenants.truncate(1);
    o.cache_dir = Some(std::env::var("GONVEX_PERF_FIXTURE_DIR").unwrap().into());
    Fixture::build(o).await.unwrap()
}
#[tokio::test]
#[ignore = "explicit ownership measurement: requires an owned GONVEX_PERF_FIXTURE_DIR and resource caps"]
async fn queue_origin_probe() {
    gonvex_profiler::set_enabled(true);
    let f = fixture().await;
    let session = f.session(0, f.members_of_kind(0, MemberKind::Admin)[0]);
    let opened = f
        .runtime
        .open_replica(&session, request("warm"))
        .await
        .unwrap();
    let mut resume = request("resume");
    for m in &opened.messages {
        if let gonvex_protocol::ServerMessage::ReplicaReady { cursor, digest, .. } = m {
            resume.cursor = Some(cursor.clone());
            resume.digest = Some(digest.clone());
        }
    }
    drop(opened);
    assert!(resume.cursor.is_some());
    let admission = crate::offload::SnapshotAdmission::global();
    let mut blockers = Vec::new();
    for i in 0..admission.capacity() {
        blockers.push(
            admission
                .admit(format!("diagnostic-{i}"), "diagnostic")
                .await
                .unwrap(),
        );
    }
    let module = f
        .runtime
        .inner
        .modules
        .project(&session.identity.project_id)
        .await
        .unwrap();
    let def = module.functions["tasks.recentWorkspaceTasks"]
        .replica
        .as_ref()
        .unwrap();
    let plan = &module.visibility[&def.table];
    let args = json!({});
    let cap = f.runtime.inner.snapshot_cache.open_admission.stats().0;
    let baseline = AllocSnapshot::now();
    let cpu = cpu_ns();
    let started = Instant::now();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    let mut cold = Vec::new();
    for _ in 0..cap {
        let mut p = Box::pin(tokio::task::unconstrained(
            f.runtime.open_replica(&session, request("cold")),
        ));
        assert!(matches!(p.as_mut().poll(&mut cx), Poll::Pending));
        cold.push(p);
    }
    let cold_live = AllocSnapshot::now().since(baseline).live_bytes;
    let cold_wait = waiting();
    let mut queries = Vec::new();
    for _ in 0..cap / 2 {
        let mut p = Box::pin(tokio::task::unconstrained(
            f.runtime
                .execute_replica_collection_query(&session, def, plan, &args),
        ));
        assert!(matches!(p.as_mut().poll(&mut cx), Poll::Pending));
        queries.push(p);
    }
    let query_live = AllocSnapshot::now().since(baseline).live_bytes;
    let query_wait = waiting();
    let mut resumes = Vec::new();
    for _ in 0..cap - cap / 2 {
        let mut p = Box::pin(tokio::task::unconstrained(
            f.runtime.open_replica(&session, resume.clone()),
        ));
        assert!(matches!(p.as_mut().poll(&mut cx), Poll::Pending));
        resumes.push(p);
    }
    let all_live = AllocSnapshot::now().since(baseline).live_bytes;
    let all_wait = waiting();
    let cold_stats = f.runtime.inner.snapshot_cache.open_admission.stats();
    emit(
        json!({"probe":"origins","cold":cold.len(),"collection_queries":queries.len(),"resume_candidates":resumes.len(),"cold_active":cold_stats.1,"snapshot_waiting_after_cold":cold_wait,"after_queries":query_wait,"after_resumes":all_wait,"cold_requested_live_bytes":cold_live,"with_queries_requested_live_bytes":query_live,"with_resumes_requested_live_bytes":all_live,"cpu_ns":cpu_ns()-cpu,"wall_ns":started.elapsed().as_nanos(),"rss_bytes":rss_bytes(),"qualification":"Rust requested allocations include metadata/futures; no rows read while slots are held; not total RSS/SQLite/V8 attribution"}),
    );
    assert_eq!(all_wait, 2 * cap as i64);
    assert_eq!(cold_stats.1, cap);
    let cancel = Instant::now();
    drop((cold, queries, resumes));
    assert_eq!(waiting(), 0);
    assert_eq!(f.runtime.inner.snapshot_cache.open_admission.stats().1, 0);
    assert_eq!(admission.waiting(), (0, 0));
    drop(blockers);
    let fast = f.runtime.open_replica(&session, resume).await.unwrap();
    assert!(!fast
        .messages
        .iter()
        .any(|m| matches!(m, gonvex_protocol::ServerMessage::ReplicaSnapshot { .. })));
    emit(
        json!({"probe":"origin_cancel","cancel_recovery_ns":cancel.elapsed().as_nanos(),"remaining_waiters":waiting(),"cold_active":f.runtime.inner.snapshot_cache.open_admission.stats().1,"validated_resume_recovered":true}),
    );
    f.shutdown().await;
}
struct Held {
    _opened: crate::replica::ReplicaOpenResult,
    live: Arc<AtomicUsize>,
}
impl Drop for Held {
    fn drop(&mut self) {
        self.live.fetch_sub(1, SeqCst);
    }
}
#[tokio::test]
#[ignore = "explicit ownership measurement: requires an owned GONVEX_PERF_FIXTURE_DIR and resource caps"]
async fn queue_result_cancel_probe() {
    gonvex_profiler::set_enabled(true);
    let f = fixture().await;
    let session = f.session(0, f.members_of_kind(0, MemberKind::Admin)[0]);
    drop(
        f.runtime
            .open_replica(&session, request("warm"))
            .await
            .unwrap(),
    );
    let live = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let started_jobs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut queue = crate::replica_open_queue::ReplicaOpenQueue::default();
    let n = REPLICA_OPEN_CONCURRENCY - 1;
    let base = AllocSnapshot::now();
    let cpu = cpu_ns();
    for i in 0..n {
        let runtime = f.runtime.clone();
        let session = session.clone();
        let live = live.clone();
        let completed = completed.clone();
        let started_jobs = started_jobs.clone();
        let gate = gate.clone();
        queue
            .push(
                i.to_string(),
                2048,
                true,
                async move {
                    started_jobs.fetch_add(1, SeqCst);
                    gate.notified().await;
                    let opened = runtime
                        .open_replica(&session, request(&i.to_string()))
                        .await
                        .unwrap();
                    live.fetch_add(1, SeqCst);
                    completed.fetch_add(1, SeqCst);
                    Held {
                        _opened: opened,
                        live,
                    }
                }
                .boxed(),
            )
            .unwrap();
    }
    // Start preparations, but keep the socket from taking any completion.
    {
        let next = queue.next();
        tokio::pin!(next);
        tokio::select! {biased;_=&mut next=>panic!("completed before gate release"),_ = async {while started_jobs.load(SeqCst)<n {tokio::task::yield_now().await;}}=>{}}
    }
    gate.notify_waiters();
    tokio::time::timeout(Duration::from_secs(30), async {
        while completed.load(SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let held = AllocSnapshot::now().since(base).live_bytes;
    let before_count = live.load(SeqCst);
    let before_reading = queue.reading();
    let cancel = Instant::now();
    for i in 0..n {
        queue.remove(&i.to_string());
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let after = AllocSnapshot::now().since(base).live_bytes;
    let after_count = live.load(SeqCst);
    let after_reading = queue.reading();
    let cancel_ns = cancel.elapsed().as_nanos();
    assert!(queue.next().await.is_none());
    assert_eq!(live.load(SeqCst), 0);
    assert!(!queue.reading());
    emit(
        json!({"probe":"completed_result_cancel","actual_module_results":n,"live_results_before_close":before_count,"live_results_after_close_without_poll":after_count,"reading_before_close":before_reading,"reading_after_close_without_poll":after_reading,"requested_live_bytes_before_close":held,"requested_live_bytes_after_close":after,"requested_live_bytes_after_drain":AllocSnapshot::now().since(base).live_bytes,"close_recovery_ns":cancel_ns,"process_cpu_ns":cpu_ns()-cpu,"rss_bytes":rss_bytes(),"qualification":"Actual Whagons-shaped SQLite ReplicaOpenResult values; fake module host, not shipping132 or total retained-owner attribution; 20ms observation grace"}),
    );
    f.shutdown().await;
}
