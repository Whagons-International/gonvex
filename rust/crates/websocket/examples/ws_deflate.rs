//! Loopback benchmark using the runtime's production WebSocket transport.
use gonvex_websocket as websocket_transport;

use axum::extract::ws::Message;
use axum::{
    extract::{Query, Request, State},
    routing::get,
    Router,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
struct Fixture(Arc<HashMap<String, Vec<String>>>);

async fn socket(
    State(fixture): State<Fixture>,
    Query(query): Query<HashMap<String, String>>,
    request: Request,
) -> axum::response::Response {
    let enabled = query.get("compression").is_some_and(|v| v == "1");
    websocket_transport::upgrade(
        request,
        enabled,
        1024,
        64 << 20,
        16 << 20,
        move |mut ws| async move {
            while let Some(Ok(message)) = ws.next().await {
                match message {
                    Message::Text(kind) => {
                        if let Some(frames) = fixture.0.get(kind.as_str()) {
                            for frame in frames {
                                if ws.send(Message::Text(frame.clone().into())).await.is_err() {
                                    return;
                                }
                            }
                            if ws.send(Message::Text("done".into())).await.is_err() {
                                return;
                            }
                        } else if ws.send(Message::Text(kind)).await.is_err() {
                            return;
                        }
                    }
                    Message::Close(_) => return,
                    _ => {}
                }
            }
        },
    )
    .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let file = std::env::args().nth(1).expect("fixture JSON path");
    let fixture = Fixture(Arc::new(
        serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!("{}", listener.local_addr().unwrap());
    axum::serve(
        listener,
        Router::new().route("/ws", get(socket)).with_state(fixture),
    )
    .await
    .unwrap();
}
