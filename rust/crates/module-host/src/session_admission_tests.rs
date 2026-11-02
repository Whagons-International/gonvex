use super::*;
use base64::Engine as _;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

const FRAME_LIMIT: usize = 1024 * 1024;

async fn send(writer: &mut WriteHalf<DuplexStream>, frame: ClientFrame) {
    write_frame(writer, &serde_json::to_vec(&frame).unwrap(), FRAME_LIMIT)
        .await
        .unwrap();
}

async fn receive(reader: &mut ReadHalf<DuplexStream>) -> ServerFrame {
    serde_json::from_slice(&read_frame(reader, FRAME_LIMIT).await.unwrap()).unwrap()
}

fn invoke(
    id: u64,
    hold: bool,
    request_deadline: Option<u64>,
    context_deadline: Option<u64>,
    depth: u8,
) -> ClientFrame {
    ClientFrame::Request {
        id,
        deadline_unix_ms: request_deadline,
        payload: RequestOp::Invoke(serde_json::from_value(json!({
            "moduleId":"root-admission", "generation":1, "function":"run", "kind":"action",
            "args":json!({"hold":hold}).to_string(),
            "context":{"nestingDepth":depth,"actionTools":["hold"],"capabilities":{"actionTools":true},
                "deadlineUnixMs":context_deadline}
        })).unwrap()),
    }
}

fn returned(frame: &ServerFrame, expected_id: u64, value: &str) {
    assert!(
        matches!(frame, ServerFrame::Response { id, payload: ResponsePayload::Invoked { value: actual } }
        if *id == expected_id && actual == value),
        "unexpected invocation result: {frame:?}"
    );
}

#[test]
fn root_admission_respects_deadlines_without_leaking_capacity_or_retiring_healthy_workers() {
    gonvex_module_runtime_v8::initialize_v8_platform();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let mut missed = Vec::new();
        // Same 50/250ms queue witness as the native V8 admission regression.
        // These are failure-observation bounds, not a production timeout SLA.
        for (label, frame_ms, context_ms, depth) in [
            ("request-only", Some(50), None, 0),
            ("context-only", None, Some(50), 0),
            ("request-tighter", Some(50), Some(2000), 0),
            ("context-tighter", Some(2000), Some(50), 0),
            ("unbounded", None, None, 0),
            ("nested", None, None, 1),
        ] {
            let state = HostState::new(HostConfig {
                max_frame_bytes: FRAME_LIMIT, max_concurrent_calls: 1,
                drain_timeout: Duration::from_secs(2), shutdown_timeout: Duration::from_secs(2),
                v8: V8Config { isolate_pool_size: 1, action_execution_timeout: Duration::from_secs(2), ..Default::default() },
            });
            let (client, server) = tokio::io::duplex(FRAME_LIMIT);
            let serving = tokio::spawn(serve(server, state.clone()));
            let (mut reader, mut writer) = tokio::io::split(client);
            assert!(matches!(receive(&mut reader).await, ServerFrame::Ready { protocol: PROTOCOL_VERSION, .. }));
            let code = br#"let calls=0; export async function run(ctx,args) {
                calls++; if(args.hold) await ctx.tools.hold({}); return calls;
            }"#;
            let artifact = serde_json::from_value(json!({
                "moduleId":"root-admission", "generation":1,
                "artifact":{"language":"typescript","entrypoint":"run.js",
                    "javascript":{"path":"run.js","hash":format!("{:x}",Sha256::digest(code)),
                        "code":base64::engine::general_purpose::STANDARD.encode(code)},
                    "functions":[{"path":"run","kind":"action","export":"run","args":{"kind":"any"},"result":{"kind":"any"}}]}
            })).unwrap();
            send(&mut writer, ClientFrame::Request { id: 1, deadline_unix_ms: None, payload: RequestOp::Load(artifact) }).await;
            assert!(matches!(receive(&mut reader).await, ServerFrame::Response { id: 1, payload: ResponsePayload::Loaded { generation: 1, .. } }));
            send(&mut writer, ClientFrame::Request { id: 2, deadline_unix_ms: None,
                payload: RequestOp::Activate(ActivateRequest { module_id:"root-admission".into(),generation:1,drain_ms:None }) }).await;
            assert!(matches!(receive(&mut reader).await, ServerFrame::Response { id: 2, .. }));
            send(&mut writer, invoke(3, true, None, None, 0)).await;
            let call_id = match tokio::time::timeout(Duration::from_secs(2), receive(&mut reader)).await.unwrap() {
                ServerFrame::HostCall { id, invocation: 3, payload: HostCallFrame::ToolInvoke { tool, .. } } if tool == "hold" => id,
                other => panic!("root action did not reach its actual host call: {other:?}"),
            };
            assert_eq!(state.calls.available_permits(), 0);
            let now = unix_millis(SystemTime::now());
            send(&mut writer, invoke(4, false, frame_ms.map(|ms| now+ms), context_ms.map(|ms| now+ms), depth)).await;
            let (early, first_reply) = {
                // Preserve partial frame reads across the observation timeout.
                let reply = receive(&mut reader); tokio::pin!(reply);
                let observed = tokio::time::timeout(Duration::from_millis(250), &mut reply).await;
                let early = observed.is_ok();
                assert_eq!(state.calls.available_permits(), 0, "queued or nested completion released the original root permit");
                send(&mut writer, ClientFrame::HostResponse { id: call_id, value: json!(null) }).await;
                let frame = match observed { Ok(frame) => frame,
                    Err(_) => tokio::time::timeout(Duration::from_secs(2), &mut reply).await.unwrap() };
                (early, frame)
            };
            let second_reply = tokio::time::timeout(Duration::from_secs(2), receive(&mut reader)).await.unwrap();
            let (first, second) = match (&first_reply, &second_reply) {
                (ServerFrame::Response { id: 3, .. }, _) => (&first_reply, &second_reply),
                _ => (&second_reply, &first_reply),
            };
            returned(first, 3, "1");
            let finite = frame_ms.is_some() || context_ms.is_some();
            if finite {
                assert!(matches!(second, ServerFrame::Error { id: 4, error } if error.code == codes::BUDGET_EXCEEDED),
                    "expired root waiter ran: {second:?}");
                if !early { missed.push(label); }
            } else {
                assert_eq!(early, depth > 0, "unbounded roots must queue and nested invocations must bypass the occupied root slot");
                returned(second, 4, if depth > 0 { "1" } else { "2" });
            }
            send(&mut writer, invoke(5, false, None, None, 0)).await;
            let final_reply = tokio::time::timeout(Duration::from_secs(2), receive(&mut reader)).await.unwrap();
            returned(&final_reply, 5, if !finite && depth == 0 { "3" } else { "2" });
            let permit = state.calls.clone().try_acquire_owned().expect("root admission leaked after completion");
            assert!(state.calls.clone().try_acquire_owned().is_err(), "root admission was released twice");
            drop(permit);
            drop(writer); drop(reader);
            tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap();
            eprintln!("root admission schedule {label}: answered while original root held={early}");
        }
        assert!(missed.is_empty(), "invocation deadline did not bound module-host root admission: {missed:?}");
    });
}

fn ledger_connection() -> (Arc<Connection>, mpsc::Receiver<ServerFrame>) {
    let (outbound, receiver) = mpsc::channel(8);
    let connection = Arc::new(Connection {
        state: HostState::new(HostConfig {
            max_frame_bytes: FRAME_LIMIT,
            max_concurrent_calls: 1,
            drain_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(2),
            v8: V8Config::default(),
        }),
        outbound,
        pending: Mutex::new(HashMap::new()),
        next_host_call: AtomicU64::new(1),
        running: Mutex::new(HashMap::new()),
    });
    (connection, receiver)
}

async fn duplicate_ping(
    connection: &Arc<Connection>,
    receiver: &mut mpsc::Receiver<ServerFrame>,
) -> ServerFrame {
    dispatch_frame(
        connection,
        serde_json::to_vec(&ClientFrame::Request {
            id: 7,
            deadline_unix_ms: None,
            payload: RequestOp::Ping,
        })
        .unwrap(),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn duplicate_request_must_not_cancel_the_original_invocation() {
    let (connection, mut receiver) = ledger_connection();
    let (sender, mut cancelled) = oneshot::channel();
    connection.running.lock().unwrap().insert(7, Some(sender));
    let reply = duplicate_ping(&connection, &mut receiver).await;
    assert!(
        matches!(reply, ServerFrame::Error { id: 7, error } if error.code == codes::BAD_REQUEST)
    );
    assert!(
        matches!(
            cancelled.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "duplicate request cancelled the original invocation"
    );
    connection.cancel(7);
    assert!(cancelled.await.is_err());
}

#[tokio::test]
async fn cancelled_request_reserves_its_id_until_original_cleanup_finishes() {
    let (connection, mut receiver) = ledger_connection();
    let (sender, cancelled) = oneshot::channel();
    connection.running.lock().unwrap().insert(7, Some(sender));
    connection.cancel(7);
    assert!(cancelled.await.is_err());
    let reply = duplicate_ping(&connection, &mut receiver).await;
    assert!(
        matches!(reply, ServerFrame::Error { id: 7, error } if error.code == codes::BAD_REQUEST),
        "cancelled request ID was reused before original cleanup"
    );
    connection.finish(7);
    let reply = duplicate_ping(&connection, &mut receiver).await;
    assert!(matches!(
        reply,
        ServerFrame::Response {
            id: 7,
            payload: ResponsePayload::Pong { .. }
        }
    ));
    tokio::task::yield_now().await;
    assert!(connection.running.lock().unwrap().is_empty());
}

fn start_host_call(
    connection: Arc<Connection>,
) -> tokio::task::JoinHandle<Result<HostResponse, HostError>> {
    tokio::spawn(async move {
        connection
            .host_call(
                7,
                HostCall::ToolInvoke {
                    tool: "hold".into(),
                    args: b"{}".to_vec(),
                },
            )
            .await
    })
}

#[tokio::test]
async fn cancelling_host_work_removes_its_reply_and_late_answers_cannot_reach_new_work() {
    let (connection, mut receiver) = ledger_connection();
    let first = start_host_call(connection.clone());
    let old_id = match receiver.recv().await.unwrap() {
        ServerFrame::HostCall {
            id, invocation: 7, ..
        } => id,
        other => panic!("missing actual host call: {other:?}"),
    };
    assert_eq!(connection.pending.lock().unwrap().len(), 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(
        connection.pending.lock().unwrap().is_empty(),
        "cancelled host call leaked its pending reply"
    );
    let next = start_host_call(connection.clone());
    let new_id = match receiver.recv().await.unwrap() {
        ServerFrame::HostCall { id, .. } => id,
        other => panic!("missing replacement host call: {other:?}"),
    };
    assert_ne!(old_id, new_id);
    connection.resolve_host_call(old_id, Ok(json!("stale")));
    assert!(!next.is_finished());
    assert!(connection.pending.lock().unwrap().contains_key(&new_id));
    connection.resolve_host_call(new_id, Ok(json!(null)));
    assert_eq!(next.await.unwrap().unwrap().value, b"null");
    assert!(connection.pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn host_call_identity_exhaustion_refuses_without_wrapping_or_leaking_a_reply() {
    let (connection, mut receiver) = ledger_connection();
    connection
        .next_host_call
        .store(u64::MAX - 1, Ordering::Relaxed);
    let last = start_host_call(connection.clone());
    let last_id = match receiver.recv().await.unwrap() {
        ServerFrame::HostCall { id, .. } => id,
        other => panic!("missing last host call: {other:?}"),
    };
    assert_eq!(last_id, u64::MAX - 1);
    connection.resolve_host_call(last_id, Ok(json!(null)));
    assert_eq!(last.await.unwrap().unwrap().value, b"null");
    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        connection.host_call(
            7,
            HostCall::ToolInvoke {
                tool: "hold".into(),
                args: b"{}".to_vec(),
            },
        ),
    )
    .await;
    if outcome.is_err() {
        assert!(matches!(
            receiver.try_recv().unwrap(),
            ServerFrame::HostCall { id: u64::MAX, .. }
        ));
        assert_eq!(connection.next_host_call.load(Ordering::Relaxed), 0);
        panic!("host-call identity wrapped after exhausted allocation");
    }
    assert!(
        matches!(outcome, Ok(Err(HostError::Failed(message))) if message.contains("identity exhausted"))
    );
    assert_eq!(connection.next_host_call.load(Ordering::Relaxed), u64::MAX);
    assert!(connection.pending.lock().unwrap().is_empty());
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
