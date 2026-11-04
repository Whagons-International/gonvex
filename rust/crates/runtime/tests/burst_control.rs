//! Burst control (docs/performance/burst-control.md): a connection whose
//! replica open waits for snapshot admission still receives its deliveries.
//!
//! With the process's only snapshot slot held, a socket opens `tasks.get` for
//! a task (the open waits for admission) and saves that task. The save's
//! result and a covering watermark arrive while the open still waits (before
//! burst control the socket deferred every feed event until its opens were
//! installed). After the slot is freed the open's snapshot already contains
//! the save (the event the socket applied before the open's clock), and a
//! later save reaches the new subscription: nothing is missed.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test burst_control`
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::accept::{AcceptedSocket, TunedListener};
use gonvex_runtime::control::ControlConnection;
use gonvex_runtime::offload::SnapshotAdmission;
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, SqlValue};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ADMIN_KEY: &str = "burst-control-admin-key";

async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

/// The next text frame (raw) and its frames, a `replica.batch` expanded into
/// its items (and its watermark as a `replica.watermark` frame after them).
async fn next_frames(socket: &mut Socket, timeout: Duration) -> Option<(String, Vec<Value>)> {
    loop {
        let frame = tokio::time::timeout(timeout, socket.next())
            .await
            .ok()?
            .expect("socket open")
            .expect("frame");
        let Message::Text(text) = frame else { continue };
        let frame: Value = serde_json::from_str(&text).unwrap();
        if frame["type"] != "replica.batch" {
            return Some((text.to_string(), vec![frame]));
        }
        let mut frames: Vec<Value> = frame["items"].as_array().cloned().unwrap_or_default();
        if let Some(revision) = frame["watermark"].as_u64() {
            frames.push(json!({"type":"replica.watermark","revision":revision}));
        }
        return Some((text.to_string(), frames));
    }
}

/// Saves `task`'s description and waits (up to `timeout`) for the result and
/// a covering watermark. Returns the raw frames seen, or None on timeout.
async fn save(
    socket: &mut Socket,
    watermark: &mut u64,
    task: &str,
    description: &str,
    timeout: Duration,
) -> Option<Vec<String>> {
    let id = format!("save-{description}");
    send(
        socket,
        json!({"type":"reducer.call","id":id,"path":"tasks.updateDescription",
               "args":{"taskId":task,"description":description},"scope":"tenant","idempotencyKey":id}),
    )
    .await;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut committed = None;
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let (text, frames) = next_frames(socket, left).await?;
        for frame in &frames {
            match frame["type"].as_str() {
                Some("reducer.result") if frame["id"] == id => {
                    committed = frame["committedRevision"].as_u64();
                }
                Some("reducer.error") if frame["id"] == id => panic!("save failed: {frame}"),
                Some("replica.watermark") => {
                    *watermark = (*watermark).max(frame["revision"].as_u64().unwrap());
                }
                _ => {}
            }
        }
        seen.push(text);
        if committed.is_some_and(|revision| revision <= *watermark) {
            return Some(seen);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_socket_whose_open_waits_for_admission_still_gets_its_deliveries() {
    // One snapshot slot for the whole process (read once, at first use).
    std::env::set_var("GONVEX_REPLICA_SNAPSHOT_CONCURRENCY", "1");
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    for (name, value) in [
        ("GONVEX_REQUIRE_AUTH", "true"),
        ("GONVEX_ADMIN_KEY", ADMIN_KEY),
        (
            "GONVEX_DASHBOARD_SESSION_SECRET",
            "burst-control-control-secret-long-enough",
        ),
        ("GONVEX_AUTH_URL", "http://localhost:8080"),
    ] {
        options.config.insert(name.to_owned(), value.to_owned());
    }
    let fixture = Fixture::build(options).await.unwrap();
    let route = fixture.tenants[0].route.clone();
    let task = fixture.tenants[0].tasks[0].id.clone();

    let email = "burst-control@example.test";
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
                        "name": "Burst Control", "password": "correct horse battery staple",
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
    // The test member sees every task, like the fixture's Admins.
    let member_id = serde_json::from_slice::<Value>(&body).unwrap()["memberId"]
        .as_str()
        .unwrap()
        .to_owned();
    let admin = fixture.members_of_kind(0, MemberKind::Admin)[0];
    let admin = &fixture.tenants[0].members[admin];
    fixture
        .commit(
            0,
            "UPDATE members SET role = $1, permissions = $2 WHERE id = $3",
            vec![
                SqlValue::Text(admin.role.clone()),
                SqlValue::Text(admin.permissions.to_string()),
                SqlValue::Text(member_id),
            ],
        )
        .await
        .unwrap();
    let login = fixture
        .runtime
        .execute_control_action(
            &ControlConnection {
                project_id: route.project_id.clone(),
                ..ControlConnection::default()
            },
            "control.auth.passwordLogin",
            &json!({"email": email, "password": "correct horse battery staple"}),
            "login-burst-control",
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
    next_frames(&mut socket, Duration::from_secs(10))
        .await
        .unwrap();
    send(
        &mut socket,
        json!({"type":"auth","id":"auth","token":token,"project":route.project_id,"tenant":route.tenant_id,
               "capabilities":{"preserveSubscriptions":1,"replicaBatchFrames":1,"replicaWatermark":1}}),
    )
    .await;
    'auth: loop {
        let (_, frames) = next_frames(&mut socket, Duration::from_secs(10))
            .await
            .unwrap();
        for frame in frames {
            match frame["type"].as_str() {
                Some("auth.result") => break 'auth,
                Some("auth.error") => panic!("auth failed: {frame}"),
                _ => {}
            }
        }
    }
    // An installed subscription, so the socket has feed to deliver.
    send(
        &mut socket,
        json!({"type":"replica.open","id":"statuses","path":"statuses.statuses","args":{}}),
    )
    .await;
    let mut watermark = 0u64;
    let ready = |frames: &[Value], id: &str, watermark: &mut u64| {
        let mut done = false;
        for frame in frames {
            match frame["type"].as_str() {
                Some("replica.ready")
                    if frame["id"] == id
                        || frame["id"].as_str().is_some_and(|f| f.starts_with('~')) =>
                {
                    done = true
                }
                Some("replica.error") => panic!("open failed: {frame}"),
                Some("replica.watermark") => {
                    *watermark = (*watermark).max(frame["revision"].as_u64().unwrap())
                }
                _ => {}
            }
        }
        done
    };
    loop {
        let (_, frames) = next_frames(&mut socket, Duration::from_secs(10))
            .await
            .unwrap();
        if ready(&frames, "statuses", &mut watermark) {
            break;
        }
    }

    // Hold the only slot: the next open waits for admission.
    let held = SnapshotAdmission::global()
        .admit("burst-control-hold".into(), "burst-control-hold")
        .await
        .unwrap();
    send(
        &mut socket,
        json!({"type":"replica.open","id":"detail","path":"tasks.get","args":{"id":task}}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let during = save(
        &mut socket,
        &mut watermark,
        &task,
        "saved-while-the-open-waits",
        Duration::from_secs(2),
    )
    .await
    .expect("the save completed (result + covering watermark) while the open waited");
    assert!(
        !during.iter().any(|text| text.contains("replica.ready")),
        "the open was still waiting: {during:?}"
    );

    // Free the slot: the open's snapshot holds the first save.
    drop(held);
    let mut snapshot = Vec::new();
    loop {
        let (text, frames) = next_frames(&mut socket, Duration::from_secs(10))
            .await
            .expect("open completes once admitted");
        snapshot.push(text);
        if frames
            .iter()
            .any(|frame| frame["type"] == "replica.ready" && frame["id"] != "statuses")
        {
            break;
        }
        for frame in &frames {
            assert_ne!(frame["type"], "replica.error", "{frame}");
        }
    }
    assert!(
        snapshot
            .iter()
            .any(|text| text.contains("saved-while-the-open-waits")),
        "the snapshot contains the save applied before its clock: {snapshot:?}"
    );
    // A later save reaches the new subscription.
    let after = save(
        &mut socket,
        &mut watermark,
        &task,
        "saved-after-ready",
        Duration::from_secs(5),
    )
    .await
    .expect("the later save completes");
    assert!(
        after.iter().any(|text| text.contains("saved-after-ready")),
        "the later save is delivered to the opened collection: {after:?}"
    );
    server.abort();
    fixture.shutdown().await;
}
