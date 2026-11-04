//! Byte-bounded change-feed backlog (`change_feed::backlog`): a receiver whose
//! transactions were evicted from memory, or whose ring position was
//! overwritten, still gets every committed transaction once, in order, with
//! the same images the log holds.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test feed_backlog`
use std::time::Duration;

use gonvex_runtime::change_feed::{self, FeedEvent, FeedReceiver, LogChange};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};
use gonvex_runtime::perf_fixture::SqlValue;

async fn write(fixture: &Fixture, count: usize, rows: usize, offset: usize) -> u64 {
    let tasks = &fixture.tenant(0).tasks;
    for i in 0..count {
        let ids: Vec<SqlValue> = (0..rows)
            .map(|r| tasks[(offset + i * rows + r) % tasks.len()].id.clone().into())
            .collect();
        let placeholders = (0..rows).map(|r| format!("${}", r + 2)).collect::<Vec<_>>().join(",");
        let mut binds = vec![SqlValue::from(format!("backlog {offset} {i}"))];
        binds.extend(ids);
        fixture
            .commit(
                0,
                &format!(r#"UPDATE "tasks" SET "description"=$1 WHERE "_id" IN ({placeholders})"#),
                binds,
            )
            .await
            .unwrap();
    }
    fixture.revision(0).await.unwrap()
}

fn key(change: &LogChange) -> (u64, i32, String, String, String) {
    (
        change.revision,
        change.ordinal,
        change.row_id.clone(),
        change.old_value.to_string(),
        change.new_value.to_string(),
    )
}

/// Drains `receiver` through revision `through`: `recv` or, with `try_first`,
/// `try_recv` while it has events (the connection loop's merge path).
async fn drain(receiver: &mut FeedReceiver, through: u64, try_first: bool) -> Vec<FeedEvent> {
    let mut events = Vec::new();
    let mut last = 0;
    while last < through {
        let event = if try_first {
            match receiver.try_recv() {
                Ok(event) => event,
                Err(_) => tokio::time::timeout(Duration::from_secs(20), receiver.recv())
                    .await
                    .expect("feed event in time")
                    .expect("no lag or close"),
            }
        } else {
            tokio::time::timeout(Duration::from_secs(20), receiver.recv())
                .await
                .expect("feed event in time")
                .expect("no lag or close")
        };
        match &event {
            FeedEvent::Transaction { revision, .. } => {
                assert!(*revision > last, "revision {revision} after {last}");
                last = *revision;
            }
            FeedEvent::Reset { reason } => panic!("unexpected reset: {reason}"),
        }
        events.push(event);
    }
    events
}

async fn assert_matches_log(fixture: &Fixture, events: &[FeedEvent], after: u64, through: u64) {
    let route = &fixture.tenant(0).route;
    let log = change_feed::read_changes(fixture.pools(), route, after, through)
        .await
        .unwrap();
    let mut revisions: Vec<u64> = log.iter().map(|change| change.revision).collect();
    revisions.dedup();
    let got: Vec<u64> = events
        .iter()
        .map(|event| match event {
            FeedEvent::Transaction { revision, .. } => *revision,
            FeedEvent::Reset { .. } => 0,
        })
        .collect();
    assert_eq!(got, revisions, "one event per committed revision, in order");
    let delivered: Vec<_> = events
        .iter()
        .flat_map(|event| match event {
            FeedEvent::Transaction { changes, .. } => changes.iter().map(key).collect::<Vec<_>>(),
            FeedEvent::Reset { .. } => Vec::new(),
        })
        .collect();
    let expected: Vec<_> = log.iter().map(key).collect();
    assert_eq!(delivered.len(), expected.len());
    assert!(delivered == expected, "delivered images equal the log");
}

fn counter(name: &str) -> u64 {
    change_feed::backlog::metrics()[name].as_u64().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evicted_and_lagged_receivers_resume_from_the_log() {
    // One byte of budget: every transaction but the newest is evicted at once.
    // A 32-signal ring overflows after 32 unread transactions.
    std::env::set_var("GONVEX_FEED_BACKLOG_BYTES", "1");
    std::env::set_var("GONVEX_FEED_RING_CAPACITY", "32");
    std::env::set_var("GONVEX_SQLITE_SYNCHRONOUS", "normal");
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let route = fixture.tenant(0).route.clone();
    let start = fixture.revision(0).await.unwrap();

    // A receiver that keeps up and two that stall until the writes are done.
    let mut fast = fixture.runtime.perf_subscribe_change_feed(&route).await;
    let mut slow = fixture.runtime.perf_subscribe_change_feed(&route).await;
    let mut slow_try = fixture.runtime.perf_subscribe_change_feed(&route).await;
    let fills_before = counter("fills");

    // Evicted but not lagged: 20 transactions fit the 32-signal ring.
    let through = write(&fixture, 20, 3, 0).await;
    let fast_events = drain(&mut fast, through, false).await;
    assert_matches_log(&fixture, &fast_events, start, through).await;
    let slow_events = drain(&mut slow, through, false).await;
    assert_matches_log(&fixture, &slow_events, start, through).await;
    assert!(counter("fills") > fills_before, "evicted events came from the log");
    assert!(counter("evicted") > 0);

    // Lagged: 100 transactions overflow the ring of both stalled receivers.
    let lag_before = counter("lagResumes");
    let second = write(&fixture, 100, 2, 60).await;
    drain(&mut fast, second, false).await;
    let lagged = drain(&mut slow, second, false).await;
    assert_matches_log(&fixture, &lagged, through, second).await;
    let lagged_try = drain(&mut slow_try, second, true).await;
    assert_matches_log(&fixture, &lagged_try, start, second).await;
    assert!(counter("lagResumes") >= lag_before + 2, "both stalled receivers lagged");

    // The store never holds more than the newest transaction (budget 1 byte).
    assert!(counter("entries") <= 1, "{}", change_feed::backlog::metrics());
}
