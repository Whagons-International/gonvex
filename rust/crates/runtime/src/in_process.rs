//! V8 isolates inside the runtime process.
//!
//! The separate module host answers every database call of a Reducer with a
//! JSON frame round trip over a Unix socket while the tenant writer is held.
//! Here the same `gonvex-module-host` request handlers run in this process:
//! loading, prewarm, generation activation and draining, root admission,
//! deadlines, heap limits, bounded results and error codes are the module
//! host's own code. Only the transport changes: a host call from the isolate is
//! a direct call into the invocation's `HostCallHandler`.
//!
//! Threads. Every isolate keeps its dedicated thread from the engine's pool
//! (`GONVEX_MODULE_HOST_ISOLATE_POOL_SIZE`), plus one watchdog thread that
//! calls `TerminateExecution` at the deadline. JavaScript never runs on a Tokio
//! worker. A host call travels isolate thread -> the invoking Tokio task (an
//! unbounded channel, no blocking) -> `DatabaseHostCalls` -> the SQLite
//! connection's own thread, exactly the path a socket frame took after it was
//! decoded, minus the socket, the frame encoding and the second process.
//!
//! Sandbox. The isolate still only sees the capability-checked host calls of
//! `gonvex_bootstrap.js`; it never receives a pool, transaction or handle.
//! Capabilities are checked twice, as before: by the isolate op against the
//! invocation's capabilities, and by the handler (`DatabaseHostCalls`
//! refuses writes from a Query).

use std::sync::Arc;
use std::time::Duration;

use gonvex_module_host::protocol::{
    codes, HostCallFrame, RequestOp, ResponsePayload, ShutdownRequest, WireError,
};
use gonvex_module_host::session::{handle_local, HostConfig, HostState};
use gonvex_module_runtime::{BoxFuture, HostCall, HostError, HostResponse, InvocationContext};
use gonvex_module_runtime_v8::V8Config;

use crate::config::ModuleHostConfig;
use crate::module_host::HostCallHandler;

pub(crate) fn host_state(config: &ModuleHostConfig) -> Arc<HostState> {
    let defaults = V8Config::default();
    HostState::new(HostConfig {
        max_frame_bytes: config.max_frame_bytes,
        max_concurrent_calls: config.max_concurrent_calls,
        drain_timeout: Duration::from_secs(30),
        shutdown_timeout: config.shutdown_timeout,
        v8: V8Config {
            max_heap_bytes: config.max_heap_bytes,
            execution_timeout: config.execution_timeout,
            action_execution_timeout: config.action_execution_timeout,
            isolate_pool_size: config.isolate_pool_size,
            // Same knobs as the separate module host's environment.
            on_demand_idle_ttl: std::env::var("GONVEX_MODULE_HOST_ON_DEMAND_IDLE_MS")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map_or(defaults.on_demand_idle_ttl, Duration::from_millis),
            on_demand_actions: std::env::var("GONVEX_MODULE_HOST_ON_DEMAND_ACTIONS")
                .map_or(defaults.on_demand_actions, |value| {
                    !matches!(value.trim(), "0" | "false" | "off")
                }),
            ..defaults
        },
    })
}

pub(crate) async fn request(
    state: &Arc<HostState>,
    operation: RequestOp,
    handler: Option<&mut dyn HostCallHandler>,
) -> Result<ResponsePayload, WireError> {
    let deadline_unix_ms = match &operation {
        RequestOp::Invoke(request) => request.context.deadline_unix_ms,
        _ => None,
    };
    match handler {
        Some(handler) => {
            let host = DirectHost {
                handler: tokio::sync::Mutex::new(handler),
            };
            handle_local(state, deadline_unix_ms, operation, Some(&host)).await
        }
        None => match operation {
            // The socket transport answers an invocation without a dispatcher
            // with a failed host call on first use; keep that behaviour.
            RequestOp::Invoke(_) => {
                let mut none = NoDispatcher;
                let host = DirectHost {
                    handler: tokio::sync::Mutex::new(&mut none),
                };
                handle_local(state, deadline_unix_ms, operation, Some(&host)).await
            }
            other => handle_local(state, deadline_unix_ms, other, None).await,
        },
    }
}

pub(crate) async fn shutdown(state: &Arc<HostState>, grace: Duration) {
    let _ = handle_local(
        state,
        None,
        RequestOp::Shutdown(ShutdownRequest {
            grace_ms: Some(grace.as_millis() as u64),
        }),
        None,
    )
    .await;
}

struct NoDispatcher;

#[async_trait::async_trait]
impl HostCallHandler for NoDispatcher {
    async fn handle(&mut self, _call: HostCallFrame) -> Result<serde_json::Value, String> {
        Err("this module-host request has no capability dispatcher".to_owned())
    }
}

/// The engine's `ModuleHost` for one in-process invocation. The engine issues
/// at most one serial host call at a time (network calls may overlap and go
/// through `HostCallHandler::concurrent`), so the lock is never contended by
/// database calls; it only turns `&self` into the handler's `&mut self`.
struct DirectHost<'a> {
    handler: tokio::sync::Mutex<&'a mut dyn HostCallHandler>,
}

impl gonvex_module_runtime::ModuleHost for DirectHost<'_> {
    fn call<'b>(
        &'b self,
        _context: &'b InvocationContext,
        call: HostCall,
    ) -> BoxFuture<'b, Result<HostResponse, HostError>> {
        Box::pin(async move {
            let capability = call.capability();
            let frame = HostCallFrame::from_host_call(call).map_err(HostError::Failed)?;
            let concurrent = self.handler.lock().await.concurrent(&frame);
            let outcome = match concurrent {
                Some(work) => work.await,
                None => self.handler.lock().await.handle(frame).await,
            };
            match outcome {
                Ok(value) => Ok(HostResponse {
                    value: serde_json::to_vec(&value).map_err(|err| {
                        HostError::Failed(format!("host response could not be re-encoded: {err}"))
                    })?,
                }),
                // Same text the socket transport produced, so module code and
                // error classification see identical failures.
                Err(message) => Err(HostError::Failed(format!(
                    "{} host call failed [{}]: {}",
                    capability,
                    codes::HOST_CALL_FAILED,
                    message
                ))),
            }
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Instant;

    use base64::Engine as _;
    use gonvex_module_host::protocol::{
        ActivateRequest, FunctionWire, InvokeRequest, JavaScriptWire, LoadRequest,
        ModuleArtifactWire,
    };
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::module_host::{ModuleHost, ModuleHostError};

    const CODE: &str = r#"
export const read = async (ctx, args) => (await ctx.db.query("SELECT $1 AS v", [args.v])).length;
export const fail = async (ctx) => { try { await ctx.db.query("bad", []); } catch (e) { return String(e.message); } return "no error"; };
export const spin = async () => { for (;;) {} };
export const hog = async () => { const keep = []; for (;;) keep.push(new Array(1 << 16).fill(keep.length)); };
"#;

    struct Recorder {
        calls: Vec<HostCallFrame>,
    }

    #[async_trait::async_trait]
    impl HostCallHandler for Recorder {
        async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
            let answer = match &call {
                HostCallFrame::DbQuery {
                    statement,
                    parameters,
                } if statement != "bad" => Ok(json!([{ "v": parameters[0] }])),
                _ => Err("refused by test".to_owned()),
            };
            self.calls.push(call);
            answer
        }
    }

    fn artifact() -> ModuleArtifactWire {
        let function = |name: &str| FunctionWire {
            path: format!("t.{name}"),
            kind: "reducer".into(),
            internal: false,
            delivery: None,
            handler: Some(name.into()),
            export: Some(name.into()),
            file: None,
            args: Some(json!({"kind": "any"})),
            result: Some(json!({"kind": "any"})),
            metadata: Default::default(),
        };
        ModuleArtifactWire {
            language: "typescript".into(),
            entrypoint: "t.js".into(),
            hash: String::new(),
            javascript: JavaScriptWire {
                path: "t.js".into(),
                hash: Sha256::digest(CODE.as_bytes())
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                code: base64::engine::general_purpose::STANDARD.encode(CODE),
            },
            functions: ["read", "fail", "spin", "hog"].map(function).into(),
            metadata: Default::default(),
            reducer_bundle: None,
        }
    }

    fn request(function: &str, args: Value) -> InvokeRequest {
        serde_json::from_value(json!({
            "moduleId": "t", "function": format!("t.{function}"), "kind": "reducer",
            "args": args.to_string(),
            "context": {"tenantId": "tenant", "capabilities": {"dbRead": true, "dbWrite": true}},
        }))
        .unwrap()
    }

    async fn host() -> std::sync::Arc<ModuleHost> {
        let host = ModuleHost::new(ModuleHostConfig {
            enabled: true,
            in_process: true,
            max_heap_bytes: 16 << 20,
            binary: None,
            endpoint: None,
            start_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(2),
            max_frame_bytes: 1 << 20,
            max_concurrent_calls: 4,
            isolate_pool_size: 1,
            execution_timeout: Duration::from_millis(300),
            action_execution_timeout: Duration::from_secs(5),
        });
        host.start().await.unwrap();
        host.load(LoadRequest {
            module_id: "t".into(),
            generation: Some(1),
            artifact: artifact(),
        })
        .await
        .unwrap();
        host.activate(ActivateRequest {
            module_id: "t".into(),
            generation: 1,
            drain_ms: None,
        })
        .await
        .unwrap();
        host
    }

    /// Like module-runtime-v8's `run_v8_test`: initialize V8 and run every
    /// test from one persistent parent thread, never from libtest's
    /// short-lived threads (V8 crashes when the platform's thread is gone).
    pub(crate) fn run(test: impl std::future::Future<Output = ()> + Send + 'static) {
        type Job = Box<dyn FnOnce() + Send>;
        static RUNNER: std::sync::OnceLock<std::sync::Mutex<std::sync::mpsc::Sender<Job>>> =
            std::sync::OnceLock::new();
        let runner = RUNNER.get_or_init(|| {
            let (sender, jobs) = std::sync::mpsc::channel::<Job>();
            std::thread::spawn(move || {
                gonvex_module_runtime_v8::initialize_v8_platform();
                for job in jobs {
                    job();
                }
            });
            std::sync::Mutex::new(sender)
        });
        let (done, result) = std::sync::mpsc::channel();
        runner
            .lock()
            .unwrap()
            .send(Box::new(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(1)
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(test);
                }));
                done.send(outcome).unwrap();
            }))
            .unwrap();
        if let Err(panic) = result.recv().unwrap() {
            std::panic::resume_unwind(panic);
        }
    }

    #[test]
    fn host_calls_reach_the_invocation_handler_in_order() {
        run(async {
            let host = host().await;
            let mut recorder = Recorder { calls: Vec::new() };
            let value = host
                .invoke(request("read", json!({"v": 7})), &mut recorder)
                .await
                .unwrap();
            assert_eq!(value, json!(1));
            assert!(
                matches!(&recorder.calls[..], [HostCallFrame::DbQuery { parameters, .. }] if parameters == &json!([7]))
            );
            // A failed host call reaches module code with the socket transport's text.
            let message = host
                .invoke(request("fail", json!(null)), &mut recorder)
                .await
                .unwrap();
            assert!(
                message
                    .as_str()
                    .unwrap()
                    .contains("db_read host call failed [host_call_failed]: refused by test"),
                "{message}"
            );
            host.shutdown().await;
        });
    }

    #[test]
    fn runaway_and_heap_hungry_scripts_fail_without_taking_the_runtime_down() {
        run(async {
            let host = host().await;
            let mut recorder = Recorder { calls: Vec::new() };
            let started = Instant::now();
            let error = host
                .invoke(request("spin", json!(null)), &mut recorder)
                .await
                .unwrap_err();
            assert!(started.elapsed() < Duration::from_secs(5));
            assert!(
                matches!(&error, ModuleHostError::Remote { code, .. } if code == "budget_exceeded"),
                "{error}"
            );
            let error = host
                .invoke(request("hog", json!(null)), &mut recorder)
                .await
                .unwrap_err();
            assert!(
                matches!(&error, ModuleHostError::Remote { code, message, .. } if code == "budget_exceeded" && message.contains("heap")),
                "{error}"
            );
            // The retired isolates are replaced; the next call is served.
            let value = host
                .invoke(request("read", json!({"v": 1})), &mut recorder)
                .await
                .unwrap();
            assert_eq!(value, json!(1));
            host.shutdown().await;
        });
    }

    #[test]
    fn invocations_without_a_dispatcher_and_before_start_fail_closed() {
        run(async {
            let host = ModuleHost::new(ModuleHostConfig {
                enabled: true,
                in_process: true,
                max_heap_bytes: 16 << 20,
                binary: None,
                endpoint: None,
                start_timeout: Duration::from_secs(1),
                shutdown_timeout: Duration::from_secs(1),
                max_frame_bytes: 1 << 20,
                max_concurrent_calls: 1,
                isolate_pool_size: 1,
                execution_timeout: Duration::from_secs(1),
                action_execution_timeout: Duration::from_secs(1),
            });
            assert!(matches!(host.ping().await, Err(ModuleHostError::NotReady)));
        });
    }
}
