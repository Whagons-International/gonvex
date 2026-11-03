//! Keeps the benchmark fixture working: builds a small fixture and runs every
//! helper once. `cargo test -p gonvex-runtime --features perf-fixture --test perf_fixture_smoke`
use gonvex_protocol::ServerMessage;
use gonvex_runtime::perf_fixture::{
    Encode, Fixture, FixtureOptions, MemberKind, WHAGONS_SUBSCRIPTIONS,
};
use serde_json::json;

fn ok(message: &ServerMessage) {
    assert!(
        matches!(message, ServerMessage::ReducerResult { .. }),
        "reducer failed: {message:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_builds_and_every_helper_runs() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let tenant = fixture.tenant(0);
    let tasks = tenant.tasks.len();

    let admin = fixture.members_of_kind(0, MemberKind::Admin)[0];
    let grant = fixture.members_of_kind(0, MemberKind::GrantOnly)[0];
    let admin_connection = fixture.connect(0, admin).await.unwrap();
    assert_eq!(
        admin_connection.subscriptions.len(),
        WHAGONS_SUBSCRIPTIONS.len()
    );
    assert_eq!(
        admin_connection.subscriptions["tasks.recentWorkspaceTasks"]
            .rows
            .len(),
        tasks,
        "an admin sees every task"
    );
    let grant_connection = fixture.connect(0, grant).await.unwrap();
    let granted = grant_connection.subscriptions["tasks.recentWorkspaceTasks"]
        .rows
        .len();
    assert!(
        granted > 0 && granted < tasks,
        "grant-only member sees {granted} of {tasks}"
    );

    let mut connections = fixture.connect_all(0).await.unwrap();
    let event = fixture
        .update_task(0, 3, json!({"description": "changed by the smoke test"}))
        .await
        .unwrap();
    let gonvex_runtime::change_feed::FeedEvent::Transaction { changes, .. } = &event else {
        panic!("expected a transaction");
    };
    let admitted =
        Fixture::evaluate_visibility(&connections, "tasks.recentWorkspaceTasks", &changes[0]);
    let delivery = fixture
        .deliver(&mut connections, &event, Encode::JsonDeflate)
        .await
        .unwrap();
    assert!(
        admitted >= 1 && delivery.messages >= admitted,
        "{admitted} {delivery:?}"
    );

    let presence = fixture.beat_presence(0, 1).await.unwrap();
    let delivery = fixture
        .deliver(&mut connections, &presence, Encode::Json)
        .await
        .unwrap();
    assert_eq!(delivery.connections_with_messages, connections.len());

    let session = fixture.session(0, admin);
    ok(&fixture
        .call_reducer(&session, "presence.beat", json!({}))
        .await);
    ok(&fixture
        .call_reducer(&session, "presence.transition", json!({"state": "idle"}))
        .await);
    ok(&fixture
        .call_reducer(
            &session,
            "tasks.updateDescription",
            json!({"taskId": tenant.tasks[5].id, "description": "via reducer"}),
        )
        .await);
    ok(&fixture
        .call_control_reducer(
            &session,
            "connection-1",
            "control.support.heartbeat",
            json!({"release": "perf", "environment": "test"}),
        )
        .await);
    assert!(fixture.host.host_calls() >= 6);
    fixture.shutdown().await;
}
