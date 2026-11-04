//! A call result waiting for its commit's watermark never waits for the
//! replica batch timer (`replicaBatchFrames`): the commit of a heartbeat-like
//! reducer (`presence.transition`, which writes the caller's presence row)
//! delivers no rows to the caller's open collection, only a watermark, and
//! the call completes (result plus covering watermark) within 50 ms. Before
//! delivery v4 such a watermark-only batch waited for the 50 ms tick, or up
//! to the 1 s idle delay when a later call raised the urgent revision.
//!
//! Real WebSocket through the runtime's router; the perf fixture's fake
//! module host runs the reducer.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test call_watermark_latency`
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

const ADMIN_KEY: &str = "call-watermark-admin-key";

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
async fn a_call_whose_commit_sends_the_caller_no_rows_completes_within_50_ms() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    for (name, value) in [
        ("GONVEX_REQUIRE_AUTH", "true"),
        ("GONVEX_ADMIN_KEY", ADMIN_KEY),
        (
            "GONVEX_DASHBOARD_SESSION_SECRET",
            "call-watermark-control-secret-long-enough",
        ),
        ("GONVEX_AUTH_URL", "http://localhost:8080"),
    ] {
        options.config.insert(name.to_owned(), value.to_owned());
    }
    let fixture = Fixture::build(options).await.unwrap();
    let route = fixture.tenants[0].route.clone();

    // A password Member of the fixture tenant and its access token.
    let email = "call-watermark@example.test";
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
                        "name": "Call Watermark", "password": "correct horse battery staple",
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
            "login-call-watermark",
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

    let mut watermark = 0u64;
    let mut timings = Vec::new();
    for call in 0..8 {
        let id = format!("transition-{call}");
        let state = if call % 2 == 0 { "idle" } else { "active" };
        let started = Instant::now();
        send(
            &mut socket,
            json!({"type":"reducer.call","id":id,"path":"presence.transition","args":{"state":state},
                   "scope":"tenant","idempotencyKey":id}),
        )
        .await;
        let mut committed = None;
        loop {
            for frame in next_frames(&mut socket).await {
                match frame["type"].as_str() {
                    Some("reducer.result") if frame["id"] == id => {
                        committed = Some(frame["committedRevision"].as_u64().unwrap_or(0));
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
        let elapsed = started.elapsed();
        assert!(committed.unwrap() > 0, "the reducer committed");
        timings.push(elapsed);
    }
    eprintln!("call completion (result + covering watermark): {timings:?}");
    for (call, elapsed) in timings.iter().enumerate() {
        assert!(
            *elapsed < Duration::from_millis(50),
            "call {call} completed after {elapsed:?}: it waited for the batch timer"
        );
    }
    server.abort();
    fixture.shutdown().await;
}
