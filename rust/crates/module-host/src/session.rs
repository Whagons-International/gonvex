//! One connection between the Gonvex Rust runtime and this host.
//!
//! A connection is bidirectional and multiplexed: the runtime sends requests
//! by id, and while an invocation is running this host sends host calls back on
//! the same connection tagged with the invocation they belong to. That is what
//! lets a module read through the caller's Postgres transaction — the
//! transaction never leaves the trusted runtime, and the module reaches it only
//! through capability-checked host calls.
//!
//! Nothing here is per tenant. One process serves every project: engines are
//! per module generation, and tenancy travels on the invocation context.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use gonvex_module_runtime::{
    BoxFuture, HostCall, HostError, HostResponse, Invocation, InvocationContext, ModuleError,
    ModuleHost, MAX_INVOCATION_DEPTH,
};
use gonvex_module_runtime_v8::{V8Config, V8ModuleEngine};
use gonvex_server_host::{ModuleRegistry, RegistryError};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};

use crate::artifact;
use crate::framing::{read_frame, write_frame, FrameError};
use crate::protocol::{
    codes, kind_name, parse_kind, unix_millis, ActivateRequest, ClientFrame, DescribeRequest,
    FunctionSummary, HostCallFrame, InvokeRequest, LoadRequest, RequestOp, ResponsePayload,
    ServerFrame, ShutdownRequest, UnloadRequest, WireError, PROTOCOL_VERSION,
};

/// Extra time an invocation gets over the engine's own deadline before the
/// connection gives up on it. The isolate watchdog is the real bound; this only
/// guarantees that every request is answered even if an engine wedges.
const INVOKE_SLACK: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct HostConfig {
    pub max_frame_bytes: usize,
    pub max_concurrent_calls: usize,
    /// How long a retired generation may keep finishing calls before the reaper
    /// stops waiting for it.
    pub drain_timeout: Duration,
    /// Bound on the whole process's shutdown.
    pub shutdown_timeout: Duration,
    pub v8: V8Config,
}

pub struct HostState {
    pub config: HostConfig,
    pub modules: ModuleRegistry,
    calls: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
}

impl HostState {
    pub fn new(config: HostConfig) -> Arc<Self> {
        let permits = config.max_concurrent_calls.max(1);
        let (shutdown, _) = watch::channel(false);
        Arc::new(Self {
            config,
            modules: ModuleRegistry::new(),
            calls: Arc::new(Semaphore::new(permits)),
            shutdown,
        })
    }

    pub fn begin_shutdown(&self) {
        let _ = self.shutdown.send(true);
        // Closing the semaphore turns queued invocations into an explicit
        // "shutting down" answer instead of leaving them parked forever.
        self.calls.close();
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }
}

struct Connection {
    state: Arc<HostState>,
    outbound: mpsc::Sender<ServerFrame>,
    /// Host calls this process is waiting on, keyed by host-call id.
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<serde_json::Value, WireError>>>>,
    next_host_call: AtomicU64,
    /// Requests still executing. Each holds the sending half of a cancellation
    /// channel: dropping it stops the request, so a cancel and a teardown are
    /// the same mechanism. The entry is registered before the task is spawned,
    /// so a request that finishes immediately cannot leave one behind.
    running: Mutex<HashMap<u64, oneshot::Sender<()>>>,
}

impl Connection {
    async fn send(&self, frame: ServerFrame) {
        // A closed outbound channel means the connection is already gone; the
        // reader loop is what reports that, so nothing is logged twice here.
        let _ = self.outbound.send(frame).await;
    }

    async fn host_call(&self, invocation: u64, call: HostCall) -> Result<HostResponse, HostError> {
        let capability = call.capability();
        let frame = HostCallFrame::from_host_call(call).map_err(HostError::Failed)?;
        let id = self.next_host_call.fetch_add(1, Ordering::Relaxed);
        let (reply, response) = oneshot::channel();
        self.pending
            .lock()
            .expect("host call ledger lock")
            .insert(id, reply);
        if self
            .outbound
            .send(ServerFrame::HostCall {
                id,
                invocation,
                payload: frame,
            })
            .await
            .is_err()
        {
            self.pending
                .lock()
                .expect("host call ledger lock")
                .remove(&id);
            return Err(HostError::Failed(
                "module host connection closed before the host call was sent".to_owned(),
            ));
        }
        match response.await {
            Ok(Ok(value)) => Ok(HostResponse {
                value: serde_json::to_vec(&value).map_err(|err| {
                    HostError::Failed(format!("host response could not be re-encoded: {err}"))
                })?,
            }),
            Ok(Err(error)) => Err(HostError::Failed(format!(
                "{} host call failed [{}]: {}",
                capability, error.code, error.message
            ))),
            Err(_) => Err(HostError::Failed(
                "module host connection closed before the host call was answered".to_owned(),
            )),
        }
    }

    fn resolve_host_call(&self, id: u64, outcome: Result<serde_json::Value, WireError>) {
        let reply = self
            .pending
            .lock()
            .expect("host call ledger lock")
            .remove(&id);
        match reply {
            Some(reply) => {
                let _ = reply.send(outcome);
            }
            // An answer for a host call nobody is waiting on: the invocation
            // ended first. Dropping it is correct, and worth saying once.
            None => log(&format!("dropped host response for unknown host call {id}")),
        }
    }

    fn finish(&self, request_id: u64) {
        self.running
            .lock()
            .expect("request ledger lock")
            .remove(&request_id);
    }

    /// Dropping the cancellation sender ends the request's select, which drops
    /// the invocation future, releases its generation lease, and retires the
    /// isolate that was running it.
    fn cancel(&self, request_id: u64) {
        drop(
            self.running
                .lock()
                .expect("request ledger lock")
                .remove(&request_id),
        );
    }

    /// Ends every outstanding exchange when the connection dies, so no module
    /// call is left waiting on a host that can no longer answer.
    fn teardown(&self) {
        let pending: Vec<oneshot::Sender<Result<serde_json::Value, WireError>>> = self
            .pending
            .lock()
            .expect("host call ledger lock")
            .drain()
            .map(|(_, reply)| reply)
            .collect();
        for reply in pending {
            let _ = reply.send(Err(WireError::new(
                codes::HOST_CALL_FAILED,
                "module host connection closed",
            )));
        }
        self.running.lock().expect("request ledger lock").clear();
    }
}

/// The `ModuleHost` an invocation sees. It carries the request id so the Rust
/// runtime can bind a host call to the exact invocation — and therefore to the
/// exact transaction and identity — that asked for it.
struct ConnectionHost {
    connection: Arc<Connection>,
    invocation: u64,
}

impl ModuleHost for ConnectionHost {
    fn call<'a>(
        &'a self,
        _context: &'a InvocationContext,
        call: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async move { self.connection.host_call(self.invocation, call).await })
    }
}

pub async fn serve<S>(stream: S, state: Arc<HostState>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let limit = state.config.max_frame_bytes;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outbound, mut outbox) = mpsc::channel::<ServerFrame>(256);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outbox.recv().await {
            let encoded = match serde_json::to_vec(&frame) {
                Ok(encoded) => encoded,
                Err(err) => {
                    log(&format!("failed to encode a module host frame: {err}"));
                    continue;
                }
            };
            if let Err(err) = write_frame(&mut writer, &encoded, limit).await {
                log(&format!("module host write failed: {err}"));
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    let connection = Arc::new(Connection {
        state: Arc::clone(&state),
        outbound,
        pending: Mutex::new(HashMap::new()),
        next_host_call: AtomicU64::new(1),
        running: Mutex::new(HashMap::new()),
    });
    connection
        .send(ServerFrame::Ready {
            protocol: PROTOCOL_VERSION,
            version: env!("CARGO_PKG_VERSION").to_owned(),
        })
        .await;

    loop {
        match read_frame(&mut reader, limit).await {
            Ok(payload) => dispatch_frame(&connection, payload).await,
            Err(FrameError::Closed) => break,
            Err(err) => {
                if let FrameError::TooLarge { size, limit } = &err {
                    // Framing sync is lost, so the peer is told why before the
                    // connection ends rather than seeing a silent disconnect.
                    connection
                        .send(ServerFrame::Error {
                            id: 0,
                            error: WireError::new(
                                codes::FRAME_TOO_LARGE,
                                format!("frame of {size} bytes exceeds the {limit} byte limit"),
                            ),
                        })
                        .await;
                }
                log(&format!("module host connection ended: {err}"));
                if err.is_fatal() {
                    break;
                }
            }
        }
    }

    connection.teardown();
    drop(connection);
    let _ = writer_task.await;
}

async fn dispatch_frame(connection: &Arc<Connection>, payload: Vec<u8>) {
    let frame: ClientFrame = match serde_json::from_slice(&payload) {
        Ok(frame) => frame,
        Err(err) => {
            // The id is the only thing worth recovering from an unreadable
            // frame: without it the peer cannot tell which request failed.
            let id = serde_json::from_slice::<serde_json::Value>(&payload)
                .ok()
                .and_then(|value| value.get("id").and_then(serde_json::Value::as_u64))
                .unwrap_or(0);
            connection
                .send(ServerFrame::Error {
                    id,
                    error: WireError::new(codes::BAD_REQUEST, format!("unreadable frame: {err}")),
                })
                .await;
            return;
        }
    };

    match frame {
        ClientFrame::HostResponse { id, value } => connection.resolve_host_call(id, Ok(value)),
        ClientFrame::HostError { id, error } => connection.resolve_host_call(id, Err(error)),
        ClientFrame::Cancel { id } => connection.cancel(id),
        ClientFrame::Request {
            id,
            deadline_unix_ms,
            payload,
        } => {
            let (cancel, cancelled) = oneshot::channel::<()>();
            // Registering before the spawn is what keeps the ledger honest: a
            // request that completes instantly cannot race its own entry.
            if connection
                .running
                .lock()
                .expect("request ledger lock")
                .insert(id, cancel)
                .is_some()
            {
                connection
                    .send(ServerFrame::Error {
                        id,
                        error: WireError::new(
                            codes::BAD_REQUEST,
                            format!("request id {id} is already in flight"),
                        ),
                    })
                    .await;
                return;
            }
            let connection = Arc::clone(connection);
            tokio::spawn(async move {
                let outcome = tokio::select! {
                    biased;
                    // The sender is dropped by `cancel` or by a teardown, which
                    // is how both stop a running request.
                    _ = cancelled => Err(WireError::new(
                        codes::CANCELLED,
                        format!("request {id} was cancelled"),
                    )),
                    outcome = handle_request(&connection, id, deadline_unix_ms, payload) => outcome,
                };
                let frame = match outcome {
                    Ok(payload) => ServerFrame::Response { id, payload },
                    Err(error) => ServerFrame::Error { id, error },
                };
                connection.send(frame).await;
                connection.finish(id);
            });
        }
    }
}

async fn handle_request(
    connection: &Arc<Connection>,
    request_id: u64,
    deadline_unix_ms: Option<u64>,
    op: RequestOp,
) -> Result<ResponsePayload, WireError> {
    let state = Arc::clone(&connection.state);
    match op {
        RequestOp::Ping => Ok(ResponsePayload::Pong {
            protocol: PROTOCOL_VERSION,
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }),
        RequestOp::Load(request) => handle_load(state, request).await,
        RequestOp::Activate(request) => handle_activate(state, request),
        RequestOp::Describe(request) => handle_describe(state, request),
        RequestOp::Unload(request) => handle_unload(state, request).await,
        RequestOp::Shutdown(request) => handle_shutdown(state, request).await,
        RequestOp::Invoke(request) => {
            handle_invoke(connection, request_id, deadline_unix_ms, request).await
        }
    }
}

async fn handle_load(
    state: Arc<HostState>,
    request: LoadRequest,
) -> Result<ResponsePayload, WireError> {
    if state.is_shutting_down() {
        return Err(WireError::retryable(
            codes::SHUTTING_DOWN,
            "module host is shutting down",
        ));
    }
    let module_id = require_module_id(&request.module_id)?;
    let generation = state
        .modules
        .reserve_generation(&module_id, request.generation)
        .map_err(|err| registry_error(&module_id, err))?;

    let decoded = artifact::decode(&module_id, generation, request.artifact)?;
    let engine = V8ModuleEngine::from_artifact(decoded.artifact, state.config.v8.clone()).map_err(
        |err| {
            WireError::new(
                codes::MODULE_LOAD_FAILED,
                format!("module {module_id} generation {generation} failed to load: {err}"),
            )
        },
    )?;
    // Warming here is what makes activation safe to do atomically: a bundle
    // that throws while it evaluates fails the load instead of failing the
    // first user call after the swap.
    engine.prewarm().await.map_err(|err| {
        WireError::new(
            codes::MODULE_LOAD_FAILED,
            format!("module {module_id} generation {generation} failed to warm: {err}"),
        )
    })?;

    state
        .modules
        .stage(&module_id, generation, Arc::new(engine));
    log(&format!(
        "loaded module {module_id} generation {generation} with {} functions",
        decoded.summaries.len()
    ));
    Ok(ResponsePayload::Loaded {
        module_id,
        generation,
        functions: decoded.summaries,
    })
}

fn handle_activate(
    state: Arc<HostState>,
    request: ActivateRequest,
) -> Result<ResponsePayload, WireError> {
    let module_id = require_module_id(&request.module_id)?;
    let retired = state
        .modules
        .activate(&module_id, request.generation)
        .map_err(|err| registry_error(&module_id, err))?;
    let retired_generation = retired.as_ref().map(|retired| retired.generation());
    drop(retired);

    if retired_generation.is_some() {
        // Calls already running on the retired generation keep running on it.
        // Reaping waits for them on a blocking thread so the connection stays
        // responsive while the old generation finishes.
        let drain = request
            .drain_ms
            .map(Duration::from_millis)
            .unwrap_or(state.config.drain_timeout);
        let module = module_id.clone();
        let state = Arc::clone(&state);
        tokio::task::spawn_blocking(move || {
            let reaped = state.modules.reap(&module, Some(drain));
            for generation in reaped {
                log(&format!("reaped module {module} generation {generation}"));
            }
        });
    }
    log(&format!(
        "activated module {module_id} generation {}",
        request.generation
    ));
    Ok(ResponsePayload::Activated {
        module_id,
        generation: request.generation,
        retired: retired_generation,
    })
}

fn handle_describe(
    state: Arc<HostState>,
    request: DescribeRequest,
) -> Result<ResponsePayload, WireError> {
    let module_id = require_module_id(&request.module_id)?;
    let lease = state
        .modules
        .acquire(&module_id)
        .map_err(|err| registry_error(&module_id, err))?;
    let manifest = lease.engine().manifest();
    let functions = manifest
        .functions
        .iter()
        .map(|contract| FunctionSummary {
            path: contract.path.clone(),
            kind: kind_name(&contract.kind).to_owned(),
            internal: contract.internal,
            delivery: contract.delivery.clone(),
        })
        .collect();
    Ok(ResponsePayload::Described {
        module_id,
        generation: Some(manifest.generation),
        functions,
    })
}

async fn handle_unload(
    state: Arc<HostState>,
    request: UnloadRequest,
) -> Result<ResponsePayload, WireError> {
    let module_id = require_module_id(&request.module_id)?;
    let drain = request
        .drain_ms
        .map(Duration::from_millis)
        .unwrap_or(state.config.drain_timeout);
    let module = module_id.clone();
    let drained = tokio::task::spawn_blocking(move || state.modules.unload(&module, Some(drain)))
        .await
        .unwrap_or(false);
    Ok(ResponsePayload::Unloaded { module_id, drained })
}

async fn handle_shutdown(
    state: Arc<HostState>,
    request: ShutdownRequest,
) -> Result<ResponsePayload, WireError> {
    state.begin_shutdown();
    let grace = request
        .grace_ms
        .map(Duration::from_millis)
        .unwrap_or(state.config.shutdown_timeout);
    let draining = Arc::clone(&state);
    // The reply is the caller's proof that in-flight calls finished, so it is
    // sent after the drain rather than before it.
    let drained = tokio::task::spawn_blocking(move || draining.modules.shutdown(Some(grace)))
        .await
        .unwrap_or(false);
    Ok(ResponsePayload::ShuttingDown { drained })
}

async fn handle_invoke(
    connection: &Arc<Connection>,
    request_id: u64,
    deadline_unix_ms: Option<u64>,
    request: InvokeRequest,
) -> Result<ResponsePayload, WireError> {
    let state = Arc::clone(&connection.state);
    let module_id = require_module_id(&request.module_id)?;
    let kind = parse_kind(request.kind.trim()).ok_or_else(|| {
        WireError::new(
            codes::BAD_REQUEST,
            format!("unknown function kind {}", request.kind),
        )
    })?;
    if request.context.nesting_depth > MAX_INVOCATION_DEPTH {
        return Err(WireError::new(
            codes::BAD_REQUEST,
            "nested function invocation exceeded the maximum depth",
        ));
    }
    // Root admission protects the process from unrelated callers. Nested
    // calls already hold a root slot and use depth-specific isolate pools, so
    // taking another root permit here can deadlock the parent and child.
    let permit: Option<OwnedSemaphorePermit> = if request.context.nesting_depth == 0 {
        Some(
            Arc::clone(&state.calls)
                .acquire_owned()
                .await
                .map_err(|_| {
                    WireError::retryable(codes::SHUTTING_DOWN, "module host is shutting down")
                })?,
        )
    } else {
        None
    };

    let lease = state
        .modules
        .acquire(&module_id)
        .map_err(|err| registry_error(&module_id, err))?;
    let active = lease.generation();
    if let Some(expected) = request.generation {
        if active < expected {
            // The caller published a generation this host has not loaded. A
            // newer generation serving the call is fine; an older one is not.
            return Err(WireError::retryable(
                codes::UNKNOWN_GENERATION,
                format!(
                    "module {module_id} generation {expected} is not loaded; the host is on {active}"
                ),
            ));
        }
    }

    let context = request.context.into_context(active, deadline_unix_ms);
    let budget = remaining_budget(&context, state.config.v8.execution_timeout);
    let host = ConnectionHost {
        connection: Arc::clone(connection),
        invocation: request_id,
    };
    let invocation = Invocation {
        function: request.function.clone(),
        kind,
        args: request.args.into_bytes(),
        context,
    };

    let call = lease.engine().invoke(&host, invocation);
    let result = match tokio::time::timeout(budget + INVOKE_SLACK, call).await {
        Ok(result) => result,
        // The engine's watchdog normally wins this race; reaching here means
        // the engine itself stopped answering, so the lease is dropped and the
        // isolate retired instead of being pooled.
        Err(_) => Err(ModuleError::BudgetExceeded(format!(
            "module {module_id} function {} did not answer within {} ms",
            request.function,
            (budget + INVOKE_SLACK).as_millis()
        ))),
    };
    drop(permit);

    match result {
        Ok(result) => {
            let value = String::from_utf8(result.value).map_err(|_| {
                WireError::new(
                    codes::EXECUTION_FAILED,
                    format!("module {module_id} returned a result that is not UTF-8 JSON"),
                )
            })?;
            Ok(ResponsePayload::Invoked { value })
        }
        Err(err) => Err(module_error(&module_id, &request.function, err)),
    }
}

/// The engine shortens its own deadline from the invocation context; this is
/// the same computation, used only to bound the transport's wait.
fn remaining_budget(context: &InvocationContext, ceiling: Duration) -> Duration {
    match context.deadline {
        Some(deadline) => deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
            .min(ceiling),
        None => ceiling,
    }
}

fn require_module_id(module_id: &str) -> Result<String, WireError> {
    let trimmed = module_id.trim();
    if trimmed.is_empty() {
        return Err(WireError::new(
            codes::BAD_REQUEST,
            "a module id is required",
        ));
    }
    Ok(trimmed.to_owned())
}

fn registry_error(module_id: &str, err: RegistryError) -> WireError {
    match err {
        RegistryError::NonMonotonicGeneration(generation) => WireError::new(
            codes::GENERATION_CONFLICT,
            format!("module {module_id} generation {generation} is not newer than the active one"),
        ),
        RegistryError::UnknownGeneration(generation) => WireError::new(
            codes::UNKNOWN_GENERATION,
            format!("module {module_id} generation {generation} was never loaded"),
        ),
        RegistryError::Empty => WireError::retryable(
            codes::MODULE_NOT_LOADED,
            format!("module {module_id} has no active generation"),
        ),
        RegistryError::Invocation(err) => module_error(module_id, "", err),
    }
}

fn module_error(module_id: &str, function: &str, err: ModuleError) -> WireError {
    let where_ = if function.is_empty() {
        format!("module {module_id}")
    } else {
        format!("module {module_id} function {function}")
    };
    match err {
        ModuleError::FunctionNotFound(path) => WireError::new(
            codes::FUNCTION_NOT_FOUND,
            format!("{where_}: {path} is not registered"),
        ),
        ModuleError::WrongFunctionKind(path) => WireError::new(
            codes::WRONG_FUNCTION_KIND,
            format!("{where_}: {path} has a different kind"),
        ),
        ModuleError::InvalidArtifact(message) => {
            WireError::new(codes::INVALID_ARTIFACT, format!("{where_}: {message}"))
        }
        ModuleError::BudgetExceeded(message) => {
            WireError::new(codes::BUDGET_EXCEEDED, format!("{where_}: {message}"))
        }
        ModuleError::Unsupported(message) => {
            WireError::new(codes::UNSUPPORTED, format!("{where_}: {message}"))
        }
        ModuleError::InvalidArguments(message) => {
            WireError::new(codes::INVALID_ARGS, format!("{where_}: {message}"))
        }
        ModuleError::InvalidResult(message) => {
            WireError::new(codes::INVALID_RESULT, format!("{where_}: {message}"))
        }
        ModuleError::Execution(message) => {
            WireError::new(codes::EXECUTION_FAILED, format!("{where_}: {message}"))
        }
    }
}

pub fn log(message: &str) {
    eprintln!(
        "[gonvex-module-host] {} {message}",
        unix_millis(SystemTime::now())
    );
}
