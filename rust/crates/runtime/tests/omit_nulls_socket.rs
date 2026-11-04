//! `replicaOmitNulls` through the real WebSocket path (negotiation in the
//! connection loop, the open on the offload pool, snapshot pages, batch
//! frames): a socket that negotiated it receives rows without null columns
//! and, restored as the SDK does, exactly the rows (text and key order) a
//! socket without it receives, for all 10 Whagons collections.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test omit_nulls_socket`
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::accept::{AcceptedSocket, TunedListener};
use gonvex_runtime::control::ControlConnection;
use std::collections::BTreeMap;

use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, WHAGONS_SUBSCRIPTIONS};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ADMIN_KEY: &str = "omit-nulls-admin-key";

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
async fn omit_nulls_socket_rows_restore_to_the_legacy_sockets_rows() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    for (name, value) in [
        ("GONVEX_REQUIRE_AUTH", "true"),
        ("GONVEX_ADMIN_KEY", ADMIN_KEY),
        (
            "GONVEX_DASHBOARD_SESSION_SECRET",
            "omit-nulls-control-secret-long-enough-x",
        ),
        ("GONVEX_AUTH_URL", "http://localhost:8080"),
    ] {
        options.config.insert(name.to_owned(), value.to_owned());
    }
    let fixture = Fixture::build(options).await.unwrap();
    let route = fixture.tenants[0].route.clone();

    // A password Member of the fixture tenant and its access token.
    let email = "omit-nulls@example.test";
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
                        "name": "Omit Nulls", "password": "correct horse battery staple",
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
            "login-omit-nulls",
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

    // Two sockets of the same member: `replicaOmitNulls` and without it.
    let mut sockets = Vec::new();
    for omit in [true, false] {
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
            .await
            .unwrap();
        let ready = next_frames(&mut socket).await;
        assert_eq!(ready[0]["capabilities"]["replicaOmitNulls"], 1, "{ready:?}");
        let mut capabilities = json!({"preserveSubscriptions":1,"replicaBatchFrames":1,"replicaWatermark":1,
            "replicaSnapshotPages":1,"replicaNoOldValues":1});
        if omit {
            capabilities["replicaOmitNulls"] = json!(1);
        }
        send(
            &mut socket,
            json!({"type":"auth","id":"auth","token":token,"project":route.project_id,"tenant":route.tenant_id,
                   "capabilities":capabilities}),
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
        let opens: Vec<Value> = WHAGONS_SUBSCRIPTIONS
            .iter()
            .map(|path| json!({"id":path,"path":path,"args":{}}))
            .collect();
        send(&mut socket, json!({"type":"replica.openMany","opens":opens})).await;
        sockets.push(socket);
    }
    // Every frame until each collection is ready, as the SDK restores them.
    let mut opened = Vec::new();
    let mut restored = 0usize;
    for socket in &mut sockets {
        let mut client = Restorer::default();
        let mut ready = std::collections::BTreeSet::new();
        let mut rows = BTreeMap::<String, Vec<Value>>::new();
        while ready.len() < WHAGONS_SUBSCRIPTIONS.len() {
            for frame in next_frames(socket).await {
                let frame = client.restore(frame);
                match frame["type"].as_str() {
                    Some("replica.snapshot" | "replica.snapshotPage") => rows
                        .entry(frame["id"].as_str().unwrap().to_owned())
                        .or_default()
                        .extend(frame["result"].as_array().cloned().unwrap_or_default()),
                    Some("replica.ready") => {
                        ready.insert(frame["id"].as_str().unwrap().to_owned());
                    }
                    Some("replica.error") => panic!("open failed: {frame}"),
                    _ => {}
                }
            }
        }
        restored += client.restored;
        opened.push(rows);
    }
    let total: usize = opened[1].values().map(Vec::len).sum();
    eprintln!("{total} rows opened, {restored} null members restored");
    assert!(total > 0);
    assert!(restored > 0, "no row had a null column to omit");
    // Same rows, same text (key order included).
    assert_eq!(
        serde_json::to_string(&opened[0]).unwrap(),
        serde_json::to_string(&opened[1]).unwrap()
    );
    assert_eq!(sockets.len(), 2);
    server.abort();
    fixture.shutdown().await;
}

/// The SDK's `restoreReplicaFrameNulls` over expanded frames (stream tokens
/// map to subscription ids through `replica.stream` items).
#[derive(Default)]
struct Restorer {
    columns: BTreeMap<String, Vec<String>>,
    streams: BTreeMap<String, String>,
    restored: usize,
}

impl Restorer {
    fn restore(&mut self, mut frame: Value) -> Value {
        if frame["type"] == "replica.stream" {
            self.streams.insert(
                frame["stream"].as_str().unwrap().to_owned(),
                frame["id"].as_str().unwrap().to_owned(),
            );
        }
        if let Some(id) = frame["id"].as_str().and_then(|id| self.streams.get(id)).cloned() {
            frame["id"] = json!(id);
        }
        let id = frame["id"].as_str().unwrap_or_default().to_owned();
        if let Some(Value::Array(names)) = frame.as_object_mut().and_then(|f| f.remove("columns")) {
            self.columns.insert(
                id.clone(),
                names.iter().map(|n| n.as_str().unwrap().to_owned()).collect(),
            );
        }
        let field = match frame["type"].as_str() {
            Some("replica.snapshot" | "replica.snapshotPage") => "result",
            Some("replica.delta") => "upserts",
            _ => return frame,
        };
        if let (Some(columns), Some(Value::Array(rows))) = (self.columns.get(&id), frame.get_mut(field)) {
            for row in rows.iter_mut() {
                let have = row.as_object().map_or(0, |row| row.len());
                self.restored += columns.len().saturating_sub(have);
                *row = gonvex_protocol::omit_nulls::restore(row, columns);
            }
        }
        frame
    }
}
