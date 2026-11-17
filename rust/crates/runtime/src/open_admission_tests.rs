// Admission unit, profiler-race and real-runtime fixture tests.
use super::*;
#[test]
fn default_handles_share_the_process_limit() {
    let first = OpenAdmission::default();
    let second = OpenAdmission::default();
    assert!(std::sync::Arc::ptr_eq(&first.state, &second.state));
    assert_eq!(first.stats().1, 0);
    let held: Vec<_> = (0..first.stats().0)
        .map(|_| first.try_acquire().unwrap())
        .collect();
    assert!(second.try_acquire().is_err());
    drop(held);
    assert_eq!(second.stats().1, 0);
}

#[test]
fn exact_boundary_drop_and_recovery() {
    assert_eq!(
        OpenAdmission::new(usize::MAX).stats().0,
        tokio::sync::Semaphore::MAX_PERMITS
    );
    for cap in [0, 1, 2, 256] {
        let admission = OpenAdmission::new(cap);
        let mut held: Vec<_> = (0..cap).map(|_| admission.try_acquire().unwrap()).collect();
        for _ in 0..5000 {
            assert!(admission.try_acquire().is_err());
        }
        assert_eq!(admission.stats(), (cap, cap, cap, 5000));
        if cap > 0 {
            held.pop();
            let recovered = admission.try_acquire().unwrap();
            assert!(admission.try_acquire().is_err());
            drop(recovered);
        }
        drop(held);
        assert_eq!(admission.stats().1, 0);
    }
}
#[test]
fn racing_acquisitions_never_exceed_cap() {
    let was_enabled = gonvex_profiler::enabled();
    gonvex_profiler::set_enabled(true);
    let gauge = gonvex_profiler::gauge(gonvex_profiler::GaugeId::ColdOpenActive);
    assert_eq!(gauge.current(), 0);
    let admission = OpenAdmission::new(9);
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let admission = &admission;
            scope.spawn(move || {
                for _ in 0..10_000 {
                    if let Ok(permit) = admission.try_acquire() {
                        std::hint::black_box(&permit);
                        assert!(admission.stats().1 <= 9);
                        assert!(gauge.current() <= 9);
                    }
                }
            });
        }
    });
    assert_eq!(admission.stats().1, 0);
    assert!(admission.stats().2 <= 9);
    assert_eq!(gauge.current(), 0);
    gonvex_profiler::set_enabled(was_enabled);
}
#[test]
fn profiler_gauge_balances_even_if_disabled_while_owned() {
    use gonvex_profiler::{enabled, gauge, set_enabled, GaugeId};
    let was_enabled = enabled();
    set_enabled(true);
    let gauge = gauge(GaugeId::ColdOpenActive);
    let before = gauge.current();
    let admission = OpenAdmission::new(1);
    let permit = admission.try_acquire().unwrap();
    assert_eq!(gauge.current(), before + 1);
    assert!(admission.try_acquire().is_err());
    set_enabled(false);
    drop(permit);
    assert_eq!(gauge.current(), before);
    assert_eq!(admission.stats(), (1, 0, 1, 1));
    set_enabled(was_enabled);
}

#[test]
fn profiler_toggle_races_drain_without_underflow() {
    let was_enabled = gonvex_profiler::enabled();
    let gauge = gonvex_profiler::gauge(gonvex_profiler::GaugeId::ColdOpenActive);
    assert_eq!(gauge.current(), 0);
    let admission = OpenAdmission::new(7);
    gonvex_profiler::set_enabled(false);
    let disabled = admission.try_acquire().unwrap();
    gonvex_profiler::set_enabled(true);
    drop(disabled);
    assert_eq!(gauge.current(), 0);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..100_000 {
                gonvex_profiler::set_enabled(i % 2 == 0);
            }
        });
        for _ in 0..8 {
            let admission = &admission;
            scope.spawn(move || {
                for _ in 0..10_000 {
                    if let Ok(permit) = admission.try_acquire() {
                        let count = gauge.current();
                        assert!((0..=7).contains(&count));
                        drop(permit);
                    }
                }
            });
        }
    });
    assert_eq!(admission.stats().1, 0);
    assert_eq!(gauge.current(), 0);
    gonvex_profiler::set_enabled(was_enabled);
}

#[test]
fn cheap_guard_timing() {
    let was_enabled = gonvex_profiler::enabled();
    for enabled in [false, true] {
        gonvex_profiler::set_enabled(enabled);
        let admission = OpenAdmission::new(9);
        for _ in 0..100 {
            drop(admission.try_acquire().unwrap());
        }
        #[cfg(feature = "perf-fixture")]
        let allocations = crate::perf_fixture::harness::thread_allocations();
        let start = std::time::Instant::now();
        for _ in 0..1_000_000 {
            drop(std::hint::black_box(admission.try_acquire().unwrap()));
        }
        let elapsed = start.elapsed();
        #[cfg(feature = "perf-fixture")]
        assert_eq!(
            crate::perf_fixture::harness::thread_allocations(),
            allocations
        );
        eprintln!(
            "open_guard enabled={enabled} ns_per_op={}",
            elapsed.as_nanos() as f64 / 1_000_000.0
        );
        let held: Vec<_> = (0..9).map(|_| admission.try_acquire().unwrap()).collect();
        let start = std::time::Instant::now();
        for _ in 0..1_000_000 {
            assert!(std::hint::black_box(admission.try_acquire()).is_err());
        }
        eprintln!(
            "open_refusal enabled={enabled} ns_per_op={}",
            start.elapsed().as_nanos() as f64 / 1_000_000.0
        );
        drop(held);
        assert_eq!(admission.stats().1, 0);
    }
    gonvex_profiler::set_enabled(was_enabled);
}

#[cfg(all(test, feature = "perf-fixture"))]
mod runtime_tests {
    use crate::perf_fixture::{Fixture, FixtureOptions, MemberKind};
    use gonvex_protocol::{ReplicaOpenRequest, ServerMessage};
    use serde_json::json;

    fn samples(stage: gonvex_profiler::Stage) -> u64 {
        gonvex_profiler::snapshot()
            .iter()
            .filter(|s| s.stage == stage)
            .map(|s| s.count)
            .sum()
    }

    fn cpu_ns() -> u64 {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: usage is a valid output buffer owned by this thread.
        unsafe {
            libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        }
        let ns = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1000;
        ns(usage.ru_utime) + ns(usage.ru_stime)
    }

    fn request() -> ReplicaOpenRequest {
        serde_json::from_value(
            json!({"id":"guard-test","path":"tasks.recentWorkspaceTasks","args":{}}),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn real_open_five_thousand_cold_and_warm_offers_are_bounded() {
        use crate::perf_fixture::harness::{rss_bytes, AllocSnapshot};
        use std::future::Future;
        use std::task::{Context, Poll};
        let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
        let session = fixture.session(0, fixture.members_of_kind(0, MemberKind::Admin)[0]);
        let cache = &fixture.runtime.inner.snapshot_cache;
        let admission = crate::offload::SnapshotAdmission::global();
        let limit = cache.open_admission.stats().0;
        for (enabled, warm, distinct) in [
            (true, false, false),
            (true, false, true),
            (true, true, false),
            (false, false, false),
        ] {
            cache.clear();
            cache.set_enabled(enabled);
            if warm {
                fixture
                    .runtime
                    .open_replica(&session, request())
                    .await
                    .unwrap();
            }
            let mut blockers = Vec::new();
            for i in 0..admission.capacity() {
                blockers.push(
                    admission
                        .admit(format!("blocker-{i}"), "guard-test")
                        .await
                        .unwrap(),
                );
            }
            let before = AllocSnapshot::now();
            let rss_before = rss_bytes();
            let cpu_before = cpu_ns();
            let start = std::time::Instant::now();
            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
            let mut pending = Vec::new();
            let mut refused = 0;
            for i in 0..5000 {
                let r = if distinct {
                    serde_json::from_value(json!({"id":format!("detail-{i}"),"path":"tasks.get","args":{"id": i.to_string()}})).unwrap()
                } else {
                    request()
                };
                // Each socket preparation normally has its own task budget. This
                // manual first-poll probe must not share one exhausted Tokio
                // cooperative budget across all 5000 independent offers.
                let mut future = Box::pin(tokio::task::unconstrained(
                    fixture.runtime.open_replica(&session, r),
                ));
                match future.as_mut().poll(&mut cx) {
                    Poll::Pending => pending.push(future),
                    Poll::Ready(Err(crate::replica::ReplicaError::Overloaded(_))) => refused += 1,
                    _ => panic!("open unexpectedly bypassed held build slots"),
                }
            }
            let live = AllocSnapshot::now().since(before);
            eprintln!("real_open_pending enabled={enabled} warm={warm} distinct={distinct} accepted={} refused={refused} limit={limit} live_bytes={} allocated_bytes={} elapsed_ns={} cpu_ns={} rss_before={rss_before:?} rss_after={:?}", pending.len(), live.live_bytes,live.allocated_bytes,start.elapsed().as_nanos(),cpu_ns()-cpu_before,rss_bytes());
            assert_eq!(pending.len(), limit);
            assert_eq!(refused, 5000 - limit);
            assert_eq!(cache.open_admission.stats().1, limit);
            let queued = admission.waiting();
            assert!(queued.0 + queued.1 <= limit);
            drop(pending);
            assert_eq!(cache.open_admission.stats().1, 0);
            assert_eq!(admission.waiting(), (0, 0));
            drop(blockers);
        }
        fixture.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_spawned_offers_refuse_cancel_and_recover_for_small_tenant() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
        let connections = fixture.connect_all(0).await.unwrap();
        assert_eq!(connections.len(), 78);
        let small_connections = fixture.connect_all(1).await.unwrap();
        let session = fixture.session(0, fixture.members_of_kind(0, MemberKind::Admin)[0]);
        let cache = &fixture.runtime.inner.snapshot_cache;
        cache.clear(); // keep existing subscriptions; make the offered wave cold
        let limit = cache.open_admission.stats().0;
        let admission = crate::offload::SnapshotAdmission::global();
        let mut blockers = Vec::new();
        for i in 0..admission.capacity() {
            blockers.push(
                admission
                    .admit(format!("spawn-blocker-{i}"), "guard-test")
                    .await
                    .unwrap(),
            );
        }
        let was_enabled = gonvex_profiler::enabled();
        gonvex_profiler::set_enabled(true);
        let gauge = gonvex_profiler::gauge(gonvex_profiler::GaugeId::ColdOpenActive);
        let refused_before = cache.open_admission.stats().3;
        let refusal_samples = samples(gonvex_profiler::Stage::ColdOpenRefused);
        let lifetime_samples = samples(gonvex_profiler::Stage::ColdOpenLifetime);
        let barrier = Arc::new(tokio::sync::Barrier::new(5001));
        let progress = Arc::new(tokio::sync::Notify::new());
        let refused = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..5000 {
            let runtime = fixture.runtime.clone();
            let session = session.clone();
            let barrier = barrier.clone();
            let progress = progress.clone();
            let refused = refused.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let result = runtime.open_replica(&session, request()).await;
                assert!(matches!(
                    result,
                    Err(crate::replica::ReplicaError::Overloaded(_))
                ));
                refused.fetch_add(1, Ordering::Release);
                progress.notify_one();
            }));
        }
        let offer_cpu = cpu_ns();
        let offer_start = std::time::Instant::now();
        barrier.wait().await;
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let notified = progress.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if refused.load(Ordering::Acquire) == 5000 - limit {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("refusals settle without waiting for held build slots");
        eprintln!(
            "spawned_offers count=5000 accepted={limit} refused={} wall_ns={} cpu_ns={}",
            refused.load(Ordering::Acquire),
            offer_start.elapsed().as_nanos(),
            cpu_ns() - offer_cpu
        );
        assert_eq!(cache.open_admission.stats().1, limit);
        assert_eq!(
            cache.open_admission.stats().3 - refused_before,
            5000 - limit
        );
        assert_eq!(
            samples(gonvex_profiler::Stage::ColdOpenRefused) - refusal_samples,
            (5000 - limit) as u64
        );
        // The last successful atomic acquisition may be preempted before its
        // metric increment; the gauge may transiently undercount, never exceed.
        assert!((0..=limit as i64).contains(&gauge.current()));
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            if let Err(error) = task.await {
                assert!(error.is_cancelled(), "{error}");
            }
            assert!((0..=limit as i64).contains(&gauge.current()));
        }
        assert_eq!(cache.open_admission.stats().1, 0);
        assert_eq!(gauge.current(), 0);
        assert_eq!(admission.waiting(), (0, 0));
        assert_eq!(
            samples(gonvex_profiler::Stage::ColdOpenLifetime) - lifetime_samples,
            limit as u64
        );
        drop(blockers);
        let recovery_cpu = cpu_ns();
        let recovery_start = std::time::Instant::now();
        let small = fixture.session(1, fixture.members_of_kind(1, MemberKind::Admin)[0]);
        let recovered = fixture
            .runtime
            .open_replica(&small, request())
            .await
            .unwrap()
            .subscription
            .unwrap();
        let oracle = fixture
            .runtime
            .open_replica_fresh(&small, request())
            .await
            .unwrap()
            .subscription
            .unwrap();
        assert_eq!(recovered.row_count(), fixture.tenant(1).tasks.len());
        assert_eq!(recovered.digest(), oracle.digest());
        eprintln!(
            "small_tenant_recovery wall_ns={} cpu_ns={} rows={}",
            recovery_start.elapsed().as_nanos(),
            cpu_ns() - recovery_cpu,
            recovered.row_count()
        );
        assert_eq!(cache.open_admission.stats().1, 0);
        assert_eq!(gauge.current(), 0);
        gonvex_profiler::set_enabled(was_enabled);
        drop((connections, small_connections));
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn real_open_refuses_recovers_and_preserves_validated_resume() {
        let fixture = Fixture::build(FixtureOptions::small()).await.unwrap();
        let admin = fixture.members_of_kind(0, MemberKind::Admin)[0];
        let session = fixture.session(0, admin);
        let cache = &fixture.runtime.inner.snapshot_cache;
        let mut held: Vec<_> = (0..cache.open_admission.stats().0)
            .map(|_| cache.open_admission.try_acquire().unwrap())
            .collect();
        for enabled in [false, true] {
            cache.set_enabled(enabled);
            let result = fixture.runtime.open_replica(&session, request()).await;
            assert!(matches!(
                result,
                Err(crate::replica::ReplicaError::Overloaded(_))
            ));
        }
        held.pop();
        let opened = fixture
            .runtime
            .open_replica(&session, request())
            .await
            .unwrap();
        let subscription = opened.subscription.unwrap();
        assert_eq!(subscription.row_count(), fixture.tenant(0).tasks.len());
        assert_eq!(cache.open_admission.stats().1, held.len());
        // Fill the guard again. A current, runtime-validated XOR resume exits
        // before admission and before cache.entry, even when the guard is full.
        held.push(cache.open_admission.try_acquire().unwrap());
        let mut resume = request();
        resume.cursor = Some(subscription.cursor.clone());
        resume.digest = Some(subscription.digest());
        let builds = cache.stats().builds;
        let resumed = fixture
            .runtime
            .open_replica(&session, resume.clone())
            .await
            .unwrap();
        assert!(resumed
            .messages
            .iter()
            .all(|m| !matches!(m, ServerMessage::ReplicaSnapshot { .. })));
        assert_eq!(cache.stats().builds, builds);
        resume.digest = Some("xor1:tampered".into());
        assert!(matches!(
            fixture.runtime.open_replica(&session, resume).await,
            Err(crate::replica::ReplicaError::Overloaded(_))
        ));
        drop(held);
        assert_eq!(cache.open_admission.stats().1, 0);
        fixture.shutdown().await;
    }
}
