//! The offload pool changes where replica work runs, never its results:
//! snapshot opens through the socket queue, feed application and snapshot
//! page encoding produce the same rows, hashes and messages as the inline
//! paths. `cargo test -p gonvex-runtime --features perf-fixture --test offload_equivalence`
use gonvex_runtime::perf_fixture::stalls;
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, WHAGONS_SUBSCRIPTIONS};
use serde_json::json;

const TASKS: &str = "tasks.recentWorkspaceTasks";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn offloaded_opens_feeds_and_pages_match_inline() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let members = fixture.tenant(0).members.len();

    // Opens: the socket queue (pool) against a direct open, every member.
    for member in 0..members {
        let session = fixture.session(0, member);
        for path in WHAGONS_SUBSCRIPTIONS {
            let direct = fixture.open_replica(&session, path, path).await.unwrap();
            let direct = direct.subscription.unwrap();
            let pooled = stalls::socket_open(&fixture.runtime, &session, path)
                .await
                .unwrap();
            let pooled_subscription = pooled.subscription.unwrap();
            assert_eq!(direct.digest(), pooled_subscription.digest(), "{path} member {member}");
            assert_eq!(direct.cursor, pooled_subscription.cursor);
            assert_eq!(pooled.rows, direct.row_count());
        }
    }

    // Pages: pool encoding against inline iteration (an admin's 400 tasks
    // span several pages at the 64-row offload threshold).
    let admin = fixture.members_of_kind(0, MemberKind::Admin)[0];
    let session = fixture.session(0, admin);
    let opened = fixture.open_replica(&session, TASKS, TASKS).await.unwrap();
    let snapshot = opened
        .messages
        .into_iter()
        .find(|message| matches!(message, gonvex_protocol::ServerMessage::ReplicaSnapshot { .. }))
        .unwrap();
    let inline = stalls::snapshot_page_frames(snapshot.clone(), false).await;
    let pooled = stalls::snapshot_page_frames(snapshot, true).await;
    assert!(!inline.is_empty());
    assert_eq!(inline, pooled);

    // Feed application: every member twice, one copy inline, one pooled.
    let mut inline = fixture.connect_all(0).await.unwrap();
    let mut pooled = fixture.connect_all(0).await.unwrap();
    let tasks = fixture.tenant(0).tasks.len();
    for n in 0..6 {
        let event = if n % 2 == 0 {
            fixture
                .update_task(0, n * 37 % tasks, json!({"description": format!("offload {n}")}))
                .await
                .unwrap()
        } else {
            fixture.beat_presence(0, n % members).await.unwrap()
        };
        for (a, b) in inline.iter_mut().zip(pooled.iter_mut()) {
            let expected = fixture
                .runtime
                .apply_feed_event(&a.session, &mut a.subscriptions, event.clone())
                .await
                .unwrap();
            let actual = stalls::apply_offloaded(&fixture.runtime, b, event.clone())
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                serde_json::to_value(&actual).unwrap(),
                "event {n} member {}",
                a.member
            );
            for (path, subscription) in &a.subscriptions {
                assert_eq!(subscription.digest(), b.subscriptions[path].digest());
                assert_eq!(subscription.cursor, b.subscriptions[path].cursor);
            }
        }
    }
}
