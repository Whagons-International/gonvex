//! Same module-level probe runs unchanged against mature 2b7 and the candidate.
//! No sockets, V8, encoding or production capacity claim. Explicit unconstrained
//! first polls model independent tasks rather than one shared cooperative budget.
#![cfg(feature = "perf-fixture")]
use futures_util::task::noop_waker_ref;
use gonvex_protocol::ReplicaOpenRequest;
use gonvex_runtime::offload::SnapshotAdmission;
use gonvex_runtime::perf_fixture::harness::{rss_bytes, AllocSnapshot, CountingAlloc};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind};
use serde_json::json;
use std::{
    future::Future,
    task::{Context, Poll},
    time::Instant,
};
#[global_allocator]
static ALLOC: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);
fn number(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}
fn cpu_ns() -> u64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid owned output buffer.
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
    }
    let ns = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1000;
    ns(usage.ru_utime) + ns(usage.ru_stime)
}
#[tokio::test]
async fn module_pending_retention_before_and_after_admission() {
    let baseline = std::env::var("GONVEX_OPEN_PROBE_BASELINE").is_ok_and(|v| v == "1");
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    let session = fixture.session(0, fixture.members_of_kind(0, MemberKind::Admin)[0]);
    let admission = SnapshotAdmission::global();
    for distinct in [false, true] {
        fixture.runtime.clear_snapshot_cache();
        let mut blockers = Vec::new();
        for i in 0..admission.capacity() {
            blockers.push(
                admission
                    .admit(format!("probe-block-{i}"), "probe")
                    .await
                    .unwrap(),
            );
        }
        let before = AllocSnapshot::now();
        let rss_before = rss_bytes();
        let cpu = cpu_ns();
        let start = Instant::now();
        let mut pending = Vec::new();
        let mut refused = 0;
        let mut cx = Context::from_waker(noop_waker_ref());
        for i in 0..5000 {
            let request: ReplicaOpenRequest = serde_json::from_value(if distinct {
                json!({"id":format!("probe-{i}"),"path":"tasks.get","args":{"id":i.to_string()}})
            } else {
                json!({"id":"probe","path":"tasks.recentWorkspaceTasks","args":{}})
            })
            .unwrap();
            let mut future = Box::pin(tokio::task::unconstrained(
                fixture.runtime.open_replica(&session, request),
            ));
            match future.as_mut().poll(&mut cx) {
                Poll::Pending => pending.push(future),
                Poll::Ready(Err(gonvex_runtime::replica::ReplicaError::Overloaded(_))) => {
                    refused += 1
                }
                _ => panic!("unexpected completed open while all build slots held"),
            }
        }
        let retained = AllocSnapshot::now().since(before);
        eprintln!("MODULE_OPEN_PROBE baseline={baseline} distinct={distinct} pending={} refused={refused} allocated_live_bytes={} allocated_bytes={} cpu_ns={} wall_ns={} rss_before={rss_before:?} rss_after={:?}",pending.len(),retained.live_bytes,retained.allocated_bytes,cpu_ns()-cpu,start.elapsed().as_nanos(),rss_bytes());
        let build_waiting = number("GONVEX_SNAPSHOT_MAX_WAITING", 4096);
        let open_limit = if baseline {
            5000
        } else {
            admission.capacity() + number("GONVEX_REPLICA_OPEN_MAX_WAITING", 256)
        };
        let expected = if distinct {
            open_limit.min(build_waiting)
        } else {
            open_limit
        }
        .min(5000);
        assert_eq!(pending.len(), expected);
        assert_eq!(refused, 5000 - expected);
        drop(pending);
        assert_eq!(admission.waiting(), (0, 0));
        drop(blockers);
    }
    fixture.shutdown().await;
}
