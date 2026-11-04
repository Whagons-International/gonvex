//! Targeted wakeups (`crate::wake`, delivery-coverage.md §9) through the real
//! connection loop: an idle connection is not woken for a commit that
//! concerns none of its subscriptions (the tenant feed skips it), yet its
//! client still receives a watermark covering that commit within the flush
//! window (one idle delay, plus the watermark-only batch's own idle delay);
//! a commit that does concern it is delivered at once; and a call whose
//! commit sends the caller no rows still completes promptly (the connection
//! leaves targeted mode while a call waits for its watermark).
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test targeted_wake`
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::accept::{AcceptedSocket, TunedListener};
use gonvex_runtime::control::ControlConnection;
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ADMIN_KEY: &str = "targeted-wake-admin-key";

async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

/// The next text frame, with a `replica.batch` expanded into its items (and
/// its watermark as a `replica.watermark` frame after them).
async fn next_frames(socket: &mut Socket) -> Vec<Value> {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .expect("frame before timeout")
            .expect("socket open")
            .expect("frame");
        let Message::Text(text) = frame else { continue };
        let frame: Value = serde_json::from_str(&text).unwrap();
        if frame["type"] != "replica.batch" {
            return vec![frame];
        }
        let mut frames: Vec<Value> = frame["items"].as_array().cloned().unwrap_or_default();
        if let Some(revision) = frame["watermark"].as_u64() {
            frames.push(json!({"type":"replica.watermark","revision":revision}));
        }
        return frames;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_connection_skips_unrelated_commits_but_gets_their_watermark_and_its_own_changes() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    for (name, value) in [
        ("GONVEX_REQUIRE_AUTH", "true"),
        ("GONVEX_ADMIN_KEY", ADMIN_KEY),
        (
            "GONVEX_DASHBOARD_SESSION_SECRET",
            "targeted-wake-control-secret-long-enough",
        ),
        ("GONVEX_AUTH_URL", "http://localhost:8080"),
    ] {
        options.config.insert(name.to_owned(), value.to_owned());
    }
    let fixture = Fixture::build(options).await.unwrap();
    let route = fixture.tenants[0].route.clone();

    // A password Member of the fixture tenant and its access token.
    let email = "targeted-wake@example.test";
    let response = fixture
        .runtime
        .router()
        .oneshot(
            Request::post("/dev/internal/e2e/members")
                .header("authorization", format!("Bearer {ADMIN_KEY}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "projectId": route.project_id, "tenantId": route.tenant_id, "email": email,
                        "name": "Targeted Wake", "password": "correct horse battery staple",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 << 10).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let login = fixture
        .runtime
        .execute_control_action(
            &ControlConnection {
                project_id: route.project_id.clone(),
                ..ControlConnection::default()
            },
            "control.auth.passwordLogin",
            &json!({"email": email, "password": "correct horse battery staple"}),
            "login-targeted-wake",
        )
        .await
        .unwrap();
    let token = login["accessToken"].as_str().unwrap().to_owned();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = fixture.runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(
            TunedListener::new(listener, Default::default()),
            router.into_make_service_with_connect_info::<AcceptedSocket>(),
        )
        .await
        .unwrap();
    });

    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
        .await
        .unwrap();
    let ready = next_frames(&mut socket).await;
    assert_eq!(ready[0]["type"], "session.ready", "{ready:?}");
    assert_eq!(ready[0]["capabilities"]["replicaBatch"], 1, "{ready:?}");
    send(
        &mut socket,
        json!({"type":"auth","id":"auth","token":token,"project":route.project_id,"tenant":route.tenant_id,
               "capabilities":{"preserveSubscriptions":1,"replicaBatchFrames":1,"replicaWatermark":1}}),
    )
    .await;
    'auth: loop {
        for frame in next_frames(&mut socket).await {
            match frame["type"].as_str() {
                Some("auth.result") => break 'auth,
                Some("auth.error") => panic!("auth failed: {frame}"),
                _ => {}
            }
        }
    }
    // One open collection whose cursor the reducer's commit passes without
    // delivering it a row.
    send(
        &mut socket,
        json!({"type":"replica.open","id":"statuses","path":"statuses.statuses","args":{}}),
    )
    .await;
    'open: loop {
        for frame in next_frames(&mut socket).await {
            match frame["type"].as_str() {
                Some("replica.ready")
                    if frame["id"] == "statuses"
                        || frame["id"].as_str().is_some_and(|id| id.starts_with('~')) =>
                {
                    break 'open
                }
                Some("replica.error") => panic!("open failed: {frame}"),
                _ => {}
            }
        }
    }

    let skipped = || gonvex_runtime::replica::fanout_coverage()["wake"]["skipped"].as_u64().unwrap();
    let mut watermark = 0u64;
    // Let the open settle into targeted mode (an idle loop iteration).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 1. A commit to a table no subscription reads: skipped, then covered by
    //    a watermark within the flush window.
    let before = skipped();
    let started = Instant::now();
    let event = fixture
        .commit(
            0,
            r#"INSERT INTO "notifications"("_id","_creationTime","userId","title","isRead") VALUES('wake-note',1,'nobody','t',0)"#,
            Vec::new(),
        )
        .await
        .unwrap();
    let unrelated = match event {
        gonvex_runtime::change_feed::FeedEvent::Transaction { revision, .. } => revision,
        gonvex_runtime::change_feed::FeedEvent::Reset { .. } => panic!("reset"),
    };
    while watermark < unrelated {
        for frame in next_frames(&mut socket).await {
            if frame["type"] == "replica.watermark" {
                watermark = watermark.max(frame["revision"].as_u64().unwrap());
            }
            assert_ne!(frame["type"], "replica.delta", "an unrelated commit sent rows: {frame}");
        }
    }
    let covered = started.elapsed();
    eprintln!("unrelated commit {unrelated}: watermark after {covered:?}, skips {}", skipped() - before);
    assert!(skipped() > before, "the unrelated commit woke the connection's feed arm");
    // One flush delay (the idle delay a watermark-only batch waited before).
    assert!(covered < Duration::from_millis(1800), "watermark for a skipped commit after {covered:?}");

    // 2. A commit to its own collection: delivered at once.
    let started = Instant::now();
    fixture
        .commit(
            0,
            r#"UPDATE "statuses" SET "name"='targeted wake' WHERE rowid IN (SELECT rowid FROM "statuses" LIMIT 1)"#,
            Vec::new(),
        )
        .await
        .unwrap();
    'delta: loop {
        for frame in next_frames(&mut socket).await {
            if frame["type"] == "replica.watermark" {
                watermark = watermark.max(frame["revision"].as_u64().unwrap());
            }
            if frame["type"] == "replica.delta" {
                break 'delta;
            }
        }
    }
    let delivered = started.elapsed();
    eprintln!("own collection change delivered after {delivered:?}");
    assert!(delivered < Duration::from_millis(900), "a relevant commit waited {delivered:?} (idle delay or flush)");

    // 3. A call whose commit sends the caller no rows: result and covering
    //    watermark promptly.
    for call in 0..4 {
        let id = format!("transition-{call}");
        let state = if call % 2 == 0 { "idle" } else { "active" };
        tokio::time::sleep(Duration::from_millis(50)).await;
        send(
            &mut socket,
            json!({"type":"reducer.call","id":id,"path":"presence.transition","args":{"state":state},
                   "scope":"tenant","idempotencyKey":id}),
        )
        .await;
        let mut committed = None;
        let mut result_at = None;
        loop {
            for frame in next_frames(&mut socket).await {
                match frame["type"].as_str() {
                    Some("reducer.result") if frame["id"] == id => {
                        committed = Some(frame["committedRevision"].as_u64().unwrap_or(0));
                        result_at = Some(Instant::now());
                    }
                    Some("reducer.error") if frame["id"] == id => panic!("call failed: {frame}"),
                    Some("replica.watermark") => {
                        watermark = watermark.max(frame["revision"].as_u64().unwrap());
                    }
                    _ => {}
                }
            }
            if committed.is_some_and(|revision| revision <= watermark) {
                break;
            }
        }
        let gap = result_at.unwrap().elapsed();
        assert!(gap < Duration::from_millis(250), "call {call}: covering watermark {gap:?} after its result");
    }
    server.abort();
    fixture.shutdown().await;
}
