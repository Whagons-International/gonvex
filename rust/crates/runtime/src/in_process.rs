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
