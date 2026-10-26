//! One bounded V8 isolate, owned by one thread.
//!
//! A `JsRuntime` is neither `Send` nor `Sync`, and `ModuleEngine::invoke`
//! returns a `Send` future, so an isolate lives on its own thread behind
//! channels. The invoking task keeps the host reference and services host calls
//! for the isolate; the isolate never holds a `&dyn ModuleHost` and therefore
//! never needs the host to be `'static`.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as sync_mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use deno_core::{
    extension, op2, v8, JsRuntime, ModuleSpecifier, OpState, PollEventLoopOptions, RuntimeOptions,
};
use gonvex_module_runtime::{
    Capabilities, FunctionContract, FunctionKind, HostCall, HostError, HostResponse, Invocation,
    InvocationResult, ModuleError,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit};

use crate::dispatch::{
    decode_result, kind_name, CapabilityFlags, DispatchRequest, HostCallOutcome, HostCallRequest,
    IdentityView,
};
use crate::webcrypto::{
    op_gonvex_base64, op_gonvex_crypto_digest, op_gonvex_crypto_import, op_gonvex_crypto_key_info,
    op_gonvex_crypto_operation,
};
use crate::V8Config;

const BOOTSTRAP: &str = include_str!("gonvex_bootstrap.js");
const RUNTIME_GLOBALS: &str =
    include_str!("../../../../packages/module-sdk/runtime-contract/globals.js");
/// web-streams-polyfill 3.3.3 (MIT), the WHATWG streams the bootstrap installs.
const WEB_STREAMS: &str = include_str!("vendor/web_streams_ponyfill.js");

/// The bundled ESM artifact, shared by every isolate of one generation.
pub(crate) struct ModuleSource {
    specifier: ModuleSpecifier,
    code: String,
    #[cfg(test)]
    pub(crate) probe: Arc<WorkerTestProbe>,
}

impl ModuleSource {
    pub(crate) fn new(module_id: &str, code: String) -> Result<Self, ModuleError> {
        let slug: String = module_id
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect();
        let slug = if slug.is_empty() {
            "module".to_owned()
        } else {
            slug
        };
        let specifier = ModuleSpecifier::parse(&format!("file:///gonvex/{slug}/module.js"))
            .map_err(|err| {
                ModuleError::InvalidArtifact(format!(
                    "module id {module_id} is not a usable module specifier: {err}"
                ))
            })?;
        Ok(Self {
            specifier,
            code,
            #[cfg(test)]
            probe: Arc::default(),
        })
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct WorkerTestProbe {
    pub(crate) started: std::sync::atomic::AtomicUsize,
    pub(crate) stopped: std::sync::atomic::AtomicUsize,
    teardown: std::sync::Mutex<Option<(sync_mpsc::Sender<()>, sync_mpsc::Receiver<()>)>>,
}

#[cfg(test)]
impl WorkerTestProbe {
    pub(crate) fn hold_teardown(&self) -> (sync_mpsc::Receiver<()>, sync_mpsc::Sender<()>) {
        let (entered, receive) = sync_mpsc::channel();
        let (release, held) = sync_mpsc::channel();
        *self.teardown.lock().unwrap() = Some((entered, held));
        (receive, release)
    }
}

#[cfg(test)]
struct TestWorkerLifetime(Arc<WorkerTestProbe>);

#[cfg(test)]
impl Drop for TestWorkerLifetime {
    fn drop(&mut self) {
        if let Some((entered, release)) = self.0.teardown.lock().unwrap().take() {
            let _ = entered.send(());
            let _ = release.recv();
        }
        self.0.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

/// One host operation on its way out of the isolate, plus the channel the
/// invoking task answers on.
pub(crate) struct HostRequest {
    pub(crate) call: HostCall,
    pub(crate) reply: oneshot::Sender<Result<HostResponse, HostError>>,
}

pub(crate) struct CallSpec {
    pub(crate) contract: FunctionContract,
    pub(crate) invocation: Invocation,
    /// Already narrowed to the function kind by `dispatch::effective_capabilities`.
    pub(crate) capabilities: Capabilities,
    /// The host's wall clock for this call, surfaced to the module as `ctx.now`.
    pub(crate) now_unix_ms: u64,
    pub(crate) timeout: Option<Duration>,
    pub(crate) max_result_bytes: usize,
    pub(crate) host: mpsc::UnboundedSender<HostRequest>,
}

pub(crate) struct WorkerCall {
    pub(crate) spec: CallSpec,
    pub(crate) reply: oneshot::Sender<WorkerReply>,
}

pub(crate) struct WorkerReply {
    pub(crate) result: Result<InvocationResult, ModuleError>,
    /// False once the isolate has been terminated or left in an unknown state;
    /// the pool retires it instead of serving another call from it.
    pub(crate) healthy: bool,
}

/// A handle to one isolate thread.
pub(crate) struct IsolateWorker {
    calls: mpsc::UnboundedSender<WorkerCall>,
    control: Arc<WorkerControl>,
}

#[derive(Default)]
struct WorkerState {
    handle: Option<v8::IsolateHandle>,
    retiring_permit: Option<OwnedSemaphorePermit>,
    stopped: bool,
}

#[derive(Default)]
struct WorkerControl {
    canceled: AtomicBool,
    changed: Notify,
    state: Mutex<WorkerState>,
}

impl WorkerControl {
    fn install(&self, handle: v8::IsolateHandle) {
        let mut state = self.state.lock().expect("isolate control lock");
        if self.canceled.load(Ordering::SeqCst) {
            handle.terminate_execution();
        }
        state.handle = Some(handle);
    }

    fn cancel(&self) {
        self.canceled.store(true, Ordering::SeqCst);
        self.changed.notify_one();
        if let Some(handle) = &self.state.lock().expect("isolate control lock").handle {
            handle.terminate_execution();
        }
    }

    async fn canceled(&self) {
        if !self.canceled.load(Ordering::SeqCst) {
            self.changed.notified().await;
        }
    }

    fn stopped(&self) {
        let permit = {
            let mut state = self.state.lock().expect("isolate control lock");
            state.stopped = true;
            state.handle = None;
            state.retiring_permit.take()
        };
        drop(permit);
    }
}

struct WorkerExit(Arc<WorkerControl>);

impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.0.stopped();
    }
}

impl IsolateWorker {
    /// Starts the thread and its isolate. The returned receiver resolves once
    /// the bundle has loaded and evaluated, so a broken artifact surfaces as a
    /// load error instead of a mysterious first call.
    pub(crate) fn spawn(
        source: Arc<ModuleSource>,
        config: V8Config,
    ) -> Result<(Self, oneshot::Receiver<Result<(), ModuleError>>), ModuleError> {
        let (calls, call_receiver) = mpsc::unbounded_channel();
        let (ready, ready_receiver) = oneshot::channel();
        let control = Arc::<WorkerControl>::default();
        let worker_control = Arc::clone(&control);
        thread::Builder::new()
            .name("gonvex-module-isolate".to_owned())
            .spawn(move || worker_main(source, config, ready, call_receiver, worker_control))
            .map_err(|err| {
                ModuleError::Execution(format!("failed to start module isolate thread: {err}"))
            })?;
        Ok((Self { calls, control }, ready_receiver))
    }

    pub(crate) fn send(&self, call: WorkerCall) -> Result<(), ModuleError> {
        self.calls
            .send(call)
            .map_err(|_| ModuleError::Execution("module isolate is no longer running".to_owned()))
    }

    /// Transfer admission capacity to the worker until its runtime has been
    /// disposed. Cancellation remains nonblocking for the caller.
    pub(crate) fn retire(self, permit: OwnedSemaphorePermit) {
        let mut state = self.control.state.lock().expect("isolate control lock");
        if !state.stopped {
            assert!(state.retiring_permit.is_none(), "worker retired twice");
            state.retiring_permit = Some(permit);
        }
        // Dropping self signals cancellation after releasing the control lock.
    }
}

impl Drop for IsolateWorker {
    fn drop(&mut self) {
        self.control.cancel();
    }
}

fn worker_main(
    source: Arc<ModuleSource>,
    config: V8Config,
    ready: oneshot::Sender<Result<(), ModuleError>>,
    calls: mpsc::UnboundedReceiver<WorkerCall>,
    control: Arc<WorkerControl>,
) {
    // Declared first, so the runtime and test teardown guard drop before this
    // releases the retired worker's permit, including on early return/panic.
    let _exit = WorkerExit(Arc::clone(&control));
    #[cfg(test)]
    let _lifetime = {
        source.probe.started.fetch_add(1, Ordering::SeqCst);
        TestWorkerLifetime(Arc::clone(&source.probe))
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            let _ = ready.send(Err(ModuleError::Execution(format!(
                "failed to start module isolate runtime: {err}"
            ))));
            return;
        }
    };
    // deno_core drives `!Send` futures, so the event loop runs inside a LocalSet.
    tokio::task::LocalSet::new().block_on(
        &runtime,
        worker_loop(source, config, ready, calls, &control),
    );
}

async fn worker_loop(
    source: Arc<ModuleSource>,
    config: V8Config,
    ready: oneshot::Sender<Result<(), ModuleError>>,
    mut calls: mpsc::UnboundedReceiver<WorkerCall>,
    control: &WorkerControl,
) {
    let initialized = tokio::select! {
        biased;
        _ = control.canceled() => Err(ModuleError::Execution("module isolate startup was canceled".into())),
        result = ModuleIsolate::create(&source, config, control) => result,
    };
    let mut isolate = match initialized {
        Ok(isolate) => {
            if ready.send(Ok(())).is_err() {
                return;
            }
            isolate
        }
        Err(err) => {
            let _ = ready.send(Err(err));
            return;
        }
    };
    loop {
        let call = tokio::select! {
            biased;
            _ = control.canceled() => return,
            call = calls.recv() => match call { Some(call) => call, None => return },
        };
        let reply = tokio::select! {
            biased;
            _ = control.canceled() => return,
            reply = isolate.run(call.spec) => reply,
        };
        let healthy = reply.healthy;
        let _ = call.reply.send(reply);
        if !healthy {
            break;
        }
    }
}

/// Per-invocation state. It lives in `OpState` for exactly the length of one
/// call and is taken back out afterwards, so a recycled isolate never carries a
/// tenant's identity, host channel, or remaining budget into the next call.
#[derive(Default)]
struct InvocationSlot {
    active: Option<ActiveCall>,
}

struct ActiveCall {
    kind: FunctionKind,
    network_origins: Option<BTreeSet<String>>,
    capabilities: Capabilities,
    action_tools: BTreeSet<String>,
    host: mpsc::UnboundedSender<HostRequest>,
    violation: Option<Violation>,
}

/// A structural rule the module broke. Recorded on the Rust side because the
/// module can catch the JavaScript error the op returns, and swallowing a
/// denial must not turn into a successful invocation.
enum Violation {
    Denied(String),
    Bridge(String),
}

/// The whole host surface: one op. Every operation is named in
/// `HostCallRequest`, checked against the invocation's capabilities, counted
/// against its budget, and then forwarded to the host that owns the
/// transaction and the credentials.
///
/// Both directions are JSON text, so the op boundary carries no structure that
/// V8 and Rust have to agree on beyond this crate's own wire types.
#[op2(async)]
#[string]
async fn op_gonvex_host_call(state: Rc<RefCell<OpState>>, #[string] request: String) -> String {
    host_call(state, request).await.encode()
}

async fn host_call(state: Rc<RefCell<OpState>>, request: String) -> HostCallOutcome {
    let parsed: HostCallRequest = match serde_json::from_str(&request) {
        Ok(parsed) => parsed,
        Err(err) => return HostCallOutcome::failed(format!("unsupported host operation: {err}")),
    };
    let call = match parsed.into_host_call() {
        Ok(call) => call,
        Err(message) => return HostCallOutcome::failed(message),
    };

    let response = {
        let mut op_state = state.borrow_mut();
        let Some(active) = op_state.borrow_mut::<InvocationSlot>().active.as_mut() else {
            return HostCallOutcome::failed("no module invocation is active".to_owned());
        };
        if let Err(err) = call.check_capability(&active.capabilities) {
            let message = err.to_string();
            active
                .violation
                .get_or_insert(Violation::Denied(message.clone()));
            return HostCallOutcome::denied(message);
        }
        if let (HostCall::Fetch { request }, Some(allowed)) = (&call, &active.network_origins) {
            let origin = serde_json::from_slice::<serde_json::Value>(request)
                .ok()
                .and_then(|value| {
                    value
                        .get("url")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|raw| url::Url::parse(raw).ok())
                })
                .filter(|url| matches!(url.scheme(), "http" | "https"))
                .map(|url| url.origin().ascii_serialization());
            if !origin
                .as_ref()
                .is_some_and(|origin| allowed.contains(origin))
            {
                let message = "fetch origin is not declared for this Action".to_owned();
                active
                    .violation
                    .get_or_insert(Violation::Denied(message.clone()));
                return HostCallOutcome::denied(message);
            }
        }
        if let HostCall::ToolInvoke { tool, .. } = &call {
            if !active.action_tools.contains(tool) {
                let message = format!("Action tool {tool:?} is not declared for this invocation");
                active
                    .violation
                    .get_or_insert(Violation::Denied(message.clone()));
                return HostCallOutcome::denied(message);
            }
        }
        let (reply, response) = oneshot::channel();
        if active.host.send(HostRequest { call, reply }).is_err() {
            let message = "module host bridge closed".to_owned();
            active
                .violation
                .get_or_insert(Violation::Bridge(message.clone()));
            return HostCallOutcome::failed(message);
        }
        response
    };

    match response.await {
        // Host responses travel as bytes in the ABI; this engine speaks JSON,
        // so a non-UTF-8 response is a host/module contract mismatch.
        Ok(Ok(HostResponse { value })) => match String::from_utf8(value) {
            Ok(text) => HostCallOutcome::ok(text),
            Err(_) => HostCallOutcome::failed("host response is not valid UTF-8 JSON".to_owned()),
        },
        // A host-side failure is the host's error, not a broken module: it stays
        // catchable so a handler can fall back or report it.
        Ok(Err(err)) => HostCallOutcome::failed(err.to_string()),
        Err(_) => {
            let message = "module host bridge closed before responding".to_owned();
            let mut op_state = state.borrow_mut();
            if let Some(active) = op_state.borrow_mut::<InvocationSlot>().active.as_mut() {
                active
                    .violation
                    .get_or_insert(Violation::Bridge(message.clone()));
            }
            HostCallOutcome::failed(message)
        }
    }
}

#[op2]
#[string]
fn op_gonvex_random_bytes(
    state: &mut OpState,
    #[smi] length: u32,
) -> Result<String, deno_core::anyhow::Error> {
    if !matches!(
        state
            .borrow::<InvocationSlot>()
            .active
            .as_ref()
            .map(|call| &call.kind),
        Some(FunctionKind::Action)
    ) {
        return Err(deno_core::anyhow::anyhow!("InvalidAccessError: secure entropy is available only in Actions; use intentEntropy in Reducers"));
    }
    let mut bytes = vec![0_u8; length.min(65_536) as usize];
    rand::thread_rng().fill_bytes(&mut bytes);
    Ok(serde_json::to_string(&bytes)?)
}

#[op2]
#[string]
fn op_gonvex_sha256(#[string] input: String) -> String {
    let bytes: Vec<u8> = serde_json::from_str(&input).unwrap_or_default();
    serde_json::to_string(&Sha256::digest(bytes).as_slice()).unwrap_or_else(|_| "[]".to_owned())
}

#[op2]
#[string]
fn op_gonvex_parse_url(#[string] input: String) -> String {
    let request: serde_json::Value = serde_json::from_str(&input).unwrap_or_default();
    let raw = request
        .get("input")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let base = request
        .get("base")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| url::Url::parse(value).ok());
    let parsed = url::Url::options().base_url(base.as_ref()).parse(raw);
    match parsed {
        Ok(value) => serde_json::json!({
            "href": value.as_str(),
            "origin": value.origin().ascii_serialization(),
            "protocol": format!("{}:", value.scheme()),
            "username": value.username(),
            "password": value.password().unwrap_or_default(),
            "host": match value.port() { Some(port) => format!("{}:{port}", value.host_str().unwrap_or_default()), None => value.host_str().unwrap_or_default().to_owned() },
            "hostname": value.host_str().unwrap_or_default(),
            "port": value.port().map(|port| port.to_string()).unwrap_or_default(),
            "pathname": value.path(),
            "search": value.query().map(|query| format!("?{query}")).unwrap_or_default(),
            "hash": value.fragment().map(|fragment| format!("#{fragment}")).unwrap_or_default(),
        }).to_string(),
        Err(error) => serde_json::json!({"error": error.to_string()}).to_string(),
    }
}

#[op2(async)]
async fn op_gonvex_sleep(#[smi] delay_ms: u32) {
    tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
}

extension!(
    gonvex_host,
    ops = [
        op_gonvex_host_call,
        op_gonvex_random_bytes,
        op_gonvex_sha256,
        op_gonvex_base64,
        op_gonvex_crypto_digest,
        op_gonvex_crypto_import,
        op_gonvex_crypto_key_info,
        op_gonvex_crypto_operation,
        op_gonvex_parse_url,
        op_gonvex_sleep
    ]
);

/// Field order is drop order: the watchdog stops before the V8 handles and the
/// runtime go away, so it can never terminate an isolate that is being torn
/// down.
struct ModuleIsolate {
    watchdog: Watchdog,
    dispatch: v8::Global<v8::Function>,
    namespace: v8::Global<v8::Object>,
    runtime: JsRuntime,
    terminated: Arc<AtomicBool>,
    heap_exhausted: Arc<AtomicBool>,
    config: V8Config,
}

impl ModuleIsolate {
    async fn create(
        source: &ModuleSource,
        config: V8Config,
        control: &WorkerControl,
    ) -> Result<Self, ModuleError> {
        if config.initialization_timeout.is_zero() {
            return Err(ModuleError::BudgetExceeded(
                "module initialization deadline elapsed".into(),
            ));
        }
        let deadline = Instant::now()
            .checked_add(config.initialization_timeout)
            .ok_or_else(|| {
                ModuleError::Unsupported(
                    "module initialization timeout exceeds the monotonic clock range".into(),
                )
            })?;
        let mut runtime = JsRuntime::new(RuntimeOptions {
            extensions: vec![gonvex_host::init_ops()],
            // No module loader is installed: the artifact must be one
            // self-contained ESM bundle, so nothing the isolate runs can be
            // fetched at call time.
            create_params: Some(v8::CreateParams::default().heap_limits(0, config.max_heap_bytes)),
            ..Default::default()
        });
        runtime
            .op_state()
            .borrow_mut()
            .put(InvocationSlot::default());

        let terminated = Arc::new(AtomicBool::new(false));
        let heap_exhausted = Arc::new(AtomicBool::new(false));
        let handle = runtime.v8_isolate().thread_safe_handle();
        control.install(handle.clone());
        {
            let heap_exhausted = Arc::clone(&heap_exhausted);
            let handle = handle.clone();
            runtime.add_near_heap_limit_callback(move |current, _initial| {
                heap_exhausted.store(true, Ordering::SeqCst);
                handle.terminate_execution();
                // Raising the limit gives V8 the headroom to unwind the
                // terminated call; the isolate is retired straight after.
                current
                    .saturating_add(current / 2)
                    .max(current.saturating_add(1024 * 1024))
            });
        }

        let watchdog = Watchdog::spawn(handle, Arc::clone(&terminated))?;
        watchdog.arm(deadline);
        let initialized = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            Self::initialize(&mut runtime, source),
        )
        .await;
        watchdog.disarm();
        if heap_exhausted.load(Ordering::SeqCst) {
            return Err(ModuleError::BudgetExceeded(format!(
                "module initialization exceeded the {} byte isolate heap limit",
                config.max_heap_bytes
            )));
        }
        if initialized.is_err() || terminated.load(Ordering::SeqCst) {
            return Err(ModuleError::BudgetExceeded(format!(
                "module initialization exceeded its {} ms deadline",
                config.initialization_timeout.as_millis()
            )));
        }
        let (dispatch, namespace) = initialized.expect("initialization timeout checked")?;
        Ok(Self {
            watchdog,
            dispatch,
            namespace,
            runtime,
            terminated,
            heap_exhausted,
            config,
        })
    }

    async fn initialize(
        runtime: &mut JsRuntime,
        source: &ModuleSource,
    ) -> Result<(v8::Global<v8::Function>, v8::Global<v8::Object>), ModuleError> {
        // The bootstrap's completion value is the dispatcher, so it is reachable
        // from Rust without being installed on globalThis.
        runtime
            .execute_script("gonvex:web-streams.js", WEB_STREAMS)
            .map_err(|err| {
                ModuleError::Execution(format!("web streams bootstrap failed: {err}"))
            })?;
        let factory = RUNTIME_GLOBALS.strip_prefix("export ").ok_or_else(|| {
            ModuleError::Execution("runtime globals must export their factory".to_owned())
        })?;
        let bootstrap = format!("((createRuntimeGlobals) => {BOOTSTRAP})({factory})");
        let dispatch = runtime
            .execute_script("gonvex:bootstrap.js", bootstrap)
            .map_err(|err| ModuleError::Execution(format!("module bootstrap failed: {err}")))?;
        let dispatch = {
            let scope = &mut runtime.handle_scope();
            let value = v8::Local::new(scope, &dispatch);
            let function = v8::Local::<v8::Function>::try_from(value).map_err(|_| {
                ModuleError::Execution(
                    "module bootstrap did not evaluate to a dispatcher".to_owned(),
                )
            })?;
            v8::Global::new(scope, function)
        };

        let module = runtime
            .load_main_es_module_from_code(&source.specifier, source.code.clone())
            .await
            .map_err(|err| {
                ModuleError::InvalidArtifact(format!("failed to load JavaScript bundle: {err}"))
            })?;
        let evaluated = runtime.mod_evaluate(module);
        runtime
            .run_event_loop(PollEventLoopOptions::default())
            .await
            .map_err(|err| {
                ModuleError::InvalidArtifact(format!(
                    "JavaScript bundle failed to initialize: {err}"
                ))
            })?;
        evaluated.await.map_err(|err| {
            ModuleError::InvalidArtifact(format!("JavaScript bundle failed to evaluate: {err}"))
        })?;
        let namespace = runtime.get_module_namespace(module).map_err(|err| {
            ModuleError::InvalidArtifact(format!("JavaScript bundle exposes no exports: {err}"))
        })?;

        Ok((dispatch, namespace))
    }

    fn budget_failure(&mut self, timeout: Option<Duration>) -> Option<ModuleError> {
        self.watchdog.disarm();
        let terminated = self.terminated.swap(false, Ordering::SeqCst);
        let heap_exhausted = self.heap_exhausted.load(Ordering::SeqCst);
        if terminated {
            self.runtime.v8_isolate().cancel_terminate_execution();
        }
        if heap_exhausted {
            Some(ModuleError::BudgetExceeded(format!(
                "module exceeded the {} byte isolate heap limit",
                self.config.max_heap_bytes
            )))
        } else if terminated {
            Some(ModuleError::BudgetExceeded(format!(
                "module exceeded its {} ms execution deadline",
                timeout.unwrap_or_default().as_millis()
            )))
        } else {
            None
        }
    }

    fn early_failure(&mut self, error: ModuleError, timeout: Option<Duration>) -> WorkerReply {
        let budget = self.budget_failure(timeout);
        WorkerReply {
            healthy: budget.is_none(),
            result: Err(budget.unwrap_or(error)),
        }
    }

    async fn run(&mut self, spec: CallSpec) -> WorkerReply {
        // Export path getters run JavaScript too. They spend the same budget
        // as the handler, and every early exit must disarm before pool reuse.
        let deadline = if let Some(timeout) = spec.timeout {
            if timeout.is_zero() {
                return WorkerReply {
                    result: Err(ModuleError::BudgetExceeded(
                        "module execution deadline elapsed before handler lookup".into(),
                    )),
                    healthy: true,
                };
            }
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                return WorkerReply {
                    result: Err(ModuleError::Unsupported(
                        "module execution timeout exceeds the monotonic clock range".into(),
                    )),
                    healthy: true,
                };
            };
            self.watchdog.arm(deadline);
            Some(deadline)
        } else {
            None
        };
        let handler = match self.resolve_handler(&spec.contract) {
            Ok(handler) => handler,
            Err(err) => {
                return self.early_failure(err, spec.timeout);
            }
        };
        let args = match std::str::from_utf8(&spec.invocation.args) {
            Ok(args) => args,
            Err(_) => {
                return self.early_failure(
                    ModuleError::Execution(format!(
                        "arguments for {} are not valid UTF-8 JSON",
                        spec.invocation.function
                    )),
                    spec.timeout,
                );
            }
        };
        let request = DispatchRequest {
            invitation_inviter: spec.invocation.context.invitation_inviter.as_ref(),
            intent_entropy: spec.invocation.context.intent_entropy.as_deref(),
            function: &spec.invocation.function,
            kind: kind_name(&spec.contract.kind),
            capabilities: CapabilityFlags::from(&spec.capabilities),
            identity: IdentityView::from(&spec.invocation.context),
            invocation: &spec.invocation.context.invocation,
            environment: &spec.invocation.context.environment,
            action_tools: &spec.invocation.context.action_tools,
            now: spec.now_unix_ms,
            max_result_bytes: spec.max_result_bytes,
        };
        let request = match serde_json::to_string(&request) {
            Ok(request) => request,
            Err(err) => {
                return self.early_failure(
                    ModuleError::Execution(format!("failed to encode the invocation: {err}")),
                    spec.timeout,
                );
            }
        };

        let arguments = {
            let scope = &mut self.runtime.handle_scope();
            let request = v8::String::new(scope, &request);
            let args = v8::String::new(scope, args);
            match (request, args) {
                (Some(request), Some(args)) => {
                    let request: v8::Local<v8::Value> = request.into();
                    let args: v8::Local<v8::Value> = args.into();
                    Ok(vec![
                        handler,
                        v8::Global::new(scope, request),
                        v8::Global::new(scope, args),
                    ])
                }
                // V8 refuses strings past its own maximum length.
                _ => Err(ModuleError::BudgetExceeded(
                    "invocation arguments are too large for the isolate".to_owned(),
                )),
            }
        };

        let arguments = match arguments {
            Ok(arguments) => arguments,
            Err(error) => return self.early_failure(error, spec.timeout),
        };

        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<InvocationSlot>()
            .active =
            Some(ActiveCall {
                kind: spec.contract.kind.clone(),
                network_origins: spec.contract.metadata.get("actionCapabilities").map(
                    |capabilities| {
                        capabilities
                            .get("networkOrigins")
                            .and_then(serde_json::Value::as_array)
                            .map(|origins| {
                                origins
                                    .iter()
                                    .filter_map(serde_json::Value::as_str)
                                    .map(str::to_owned)
                                    .collect()
                            })
                            .unwrap_or_default()
                    },
                ),
                capabilities: spec.capabilities,
                action_tools: spec
                    .invocation
                    .context
                    .action_tools
                    .iter()
                    .cloned()
                    .collect(),
                host: spec.host,
                violation: None,
            });

        let call = self.runtime.call_with_args(&self.dispatch, &arguments);
        let pending = self
            .runtime
            .with_event_loop_promise(call, PollEventLoopOptions::default());
        // Terminating V8 interrupts executing JavaScript but does not wake an
        // idle event loop awaiting a host op. Bound both with the same deadline.
        let outcome = match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), pending)
                    .await
                    .ok()
            }
            None => Some(pending.await),
        };

        // Taking the slot back also drops the host channel, which tells the
        // invoking task that no further host calls are coming.
        let violation = self
            .runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<InvocationSlot>()
            .active
            .take()
            .and_then(|active| active.violation);

        let budget_error = self.budget_failure(spec.timeout).or_else(|| {
            outcome.is_none().then(|| {
                ModuleError::BudgetExceeded(format!(
                    "module exceeded its {} ms execution deadline",
                    spec.timeout.unwrap_or_default().as_millis()
                ))
            })
        });
        let mut healthy = budget_error.is_none();
        let result = if let Some(error) = budget_error {
            Err(error)
        } else if let Some(violation) = violation {
            // A denial or an exhausted budget fails the invocation even when the
            // module catches the error, so a module cannot quietly probe for
            // capabilities it was not granted.
            Err(match violation {
                Violation::Denied(message) => ModuleError::Execution(message),
                Violation::Bridge(message) => {
                    healthy = false;
                    ModuleError::Execution(message)
                }
            })
        } else {
            match outcome.expect("event-loop timeout classified above") {
                Ok(value) => self.decode(value, spec.max_result_bytes),
                Err(err) => {
                    // The dispatcher answers handler failures with an envelope,
                    // so a rejected call means the isolate itself is suspect.
                    healthy = false;
                    Err(ModuleError::Execution(format!(
                        "module dispatch failed: {err}"
                    )))
                }
            }
        };
        WorkerReply { result, healthy }
    }

    fn decode(
        &mut self,
        value: v8::Global<v8::Value>,
        max_result_bytes: usize,
    ) -> Result<InvocationResult, ModuleError> {
        let envelope = {
            let scope = &mut self.runtime.handle_scope();
            let value = v8::Local::new(scope, &value);
            if !value.is_string() {
                return Err(ModuleError::Execution(
                    "module dispatcher returned a non-string envelope".to_owned(),
                ));
            }
            value.to_rust_string_lossy(scope)
        };
        let value = decode_result(&envelope)?;
        if value.len() > max_result_bytes {
            return Err(ModuleError::BudgetExceeded(format!(
                "module result of {} bytes exceeds the {max_result_bytes} byte limit",
                value.len()
            )));
        }
        // The host owns commits, so revision and command identity stay unset here.
        Ok(InvocationResult {
            value,
            committed_revision: None,
            origin_command_id: None,
        })
    }

    /// Finds the export a contract names. A bundle may re-export per-file
    /// namespaces (`messages.list`), name an export after the full path, or
    /// export handlers flat; the manifest's `export`/`handler` metadata wins
    /// over all of it when the pipeline records one.
    fn resolve_handler(
        &mut self,
        contract: &FunctionContract,
    ) -> Result<v8::Global<v8::Value>, ModuleError> {
        let candidates = handler_candidates(contract);
        let exports = &self.namespace;
        let scope = &mut self.runtime.handle_scope();
        // A namespace lookup can throw on a cyclic import, so it runs under a
        // TryCatch: a candidate that throws is not a match, and the exception
        // must not be left pending for the call that follows.
        let scope = &mut v8::TryCatch::new(scope);
        let namespace = v8::Local::new(scope, exports);
        for candidate in &candidates {
            let mut found = lookup_property(scope, namespace, candidate);
            if found.is_none() && candidate.contains('.') {
                found = lookup_path(scope, namespace, candidate);
            }
            if scope.has_caught() {
                scope.reset();
                continue;
            }
            if let Some(value) = found {
                return Ok(v8::Global::new(scope, value));
            }
        }
        Err(ModuleError::FunctionNotFound(contract.path.clone()))
    }
}

fn handler_candidates(contract: &FunctionContract) -> Vec<String> {
    let mut candidates = Vec::new();
    for key in ["export", "handler"] {
        if let Some(serde_json::Value::String(name)) = contract.metadata.get(key) {
            let name = name.trim();
            if !name.is_empty() {
                candidates.push(name.to_owned());
            }
        }
    }
    candidates.push(contract.path.clone());
    if let Some((_, leaf)) = contract.path.rsplit_once('.') {
        candidates.push(leaf.to_owned());
    }
    // `createModule()` projects its registered functions through the module's
    // default export rather than exporting each handler as a named binding.
    // The bootstrap resolves the matching registration from this object.
    candidates.push("default".to_owned());
    candidates.dedup();
    candidates
}

fn lookup_property<'s>(
    scope: &mut v8::HandleScope<'s>,
    object: v8::Local<'s, v8::Object>,
    key: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let key = v8::String::new(scope, key)?;
    let value = object.get(scope, key.into())?;
    if value.is_undefined() {
        None
    } else {
        Some(value)
    }
}

fn lookup_path<'s>(
    scope: &mut v8::HandleScope<'s>,
    root: v8::Local<'s, v8::Object>,
    path: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let mut current: v8::Local<'s, v8::Value> = root.into();
    for segment in path.split('.') {
        let object = v8::Local::<v8::Object>::try_from(current).ok()?;
        current = lookup_property(scope, object, segment)?;
    }
    Some(current)
}

enum WatchdogSignal {
    Arm(Instant),
    /// Acknowledged, so `disarm` returns only once the watchdog has finished
    /// any termination it had already started. Without the acknowledgement a
    /// deadline that fires during teardown could terminate the next call.
    Disarm(sync_mpsc::Sender<()>),
    Shutdown,
}

/// A deadline that survives a JavaScript loop that never yields: only
/// `terminate_execution` from another thread can stop one.
struct Watchdog {
    signals: sync_mpsc::Sender<WatchdogSignal>,
}

impl Watchdog {
    fn spawn(handle: v8::IsolateHandle, terminated: Arc<AtomicBool>) -> Result<Self, ModuleError> {
        let (signals, inbox) = sync_mpsc::channel();
        thread::Builder::new()
            .name("gonvex-module-watchdog".to_owned())
            .spawn(move || watchdog_main(handle, terminated, inbox))
            .map_err(|err| {
                ModuleError::Execution(format!("failed to start module watchdog thread: {err}"))
            })?;
        Ok(Self { signals })
    }

    fn arm(&self, deadline: Instant) {
        let _ = self.signals.send(WatchdogSignal::Arm(deadline));
    }

    fn disarm(&self) {
        let (acknowledge, acknowledged) = sync_mpsc::channel();
        if self
            .signals
            .send(WatchdogSignal::Disarm(acknowledge))
            .is_ok()
        {
            // Errors here mean the watchdog thread is gone, which disarms it
            // just as effectively.
            let _ = acknowledged.recv();
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.disarm();
        let _ = self.signals.send(WatchdogSignal::Shutdown);
    }
}

fn watchdog_main(
    handle: v8::IsolateHandle,
    terminated: Arc<AtomicBool>,
    inbox: sync_mpsc::Receiver<WatchdogSignal>,
) {
    watchdog_loop(inbox, || {
        terminated.store(true, Ordering::SeqCst);
        handle.terminate_execution();
    });
}

fn watchdog_loop(inbox: sync_mpsc::Receiver<WatchdogSignal>, mut terminate: impl FnMut()) {
    let mut deadline: Option<Instant> = None;
    loop {
        let signal = match deadline {
            None => match inbox.recv() {
                Ok(signal) => Some(signal),
                Err(_) => return,
            },
            Some(at) => match inbox.recv_timeout(at.saturating_duration_since(Instant::now())) {
                Ok(signal) => Some(signal),
                Err(sync_mpsc::RecvTimeoutError::Timeout) => {
                    terminate();
                    deadline = None;
                    None
                }
                Err(sync_mpsc::RecvTimeoutError::Disconnected) => return,
            },
        };
        match signal {
            Some(WatchdogSignal::Arm(at)) => deadline = Some(at),
            Some(WatchdogSignal::Disarm(acknowledge)) => {
                deadline = None;
                let _ = acknowledge.send(());
            }
            Some(WatchdogSignal::Shutdown) => return,
            None => {}
        }
    }
}

#[cfg(test)]
mod watchdog_tests {
    use super::*;

    #[test]
    fn disarm_waits_for_an_inflight_termination_callback() {
        let (signals, inbox) = sync_mpsc::channel();
        let (entered, callback_entered) = sync_mpsc::channel();
        let (release, released) = sync_mpsc::channel();
        let worker = thread::spawn(move || {
            watchdog_loop(inbox, || {
                entered.send(()).unwrap();
                released.recv().unwrap();
            });
        });
        let watchdog = Watchdog { signals };
        watchdog.arm(Instant::now());
        callback_entered
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let (started, disarm_started) = sync_mpsc::channel();
        let (finished, disarm_finished) = sync_mpsc::channel();
        let disarmer = thread::spawn(move || {
            started.send(()).unwrap();
            watchdog.disarm();
            finished.send(()).unwrap();
        });
        disarm_started.recv_timeout(Duration::from_secs(5)).unwrap();
        // The termination callback is held by a gate, so an acknowledged
        // disarm cannot complete yet, regardless of the configured deadline.
        let early = disarm_finished.recv_timeout(Duration::from_millis(50));
        release.send(()).unwrap();
        if early.is_err() {
            disarm_finished
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        disarmer.join().unwrap();
        worker.join().unwrap();
        assert!(
            matches!(early, Err(sync_mpsc::RecvTimeoutError::Timeout)),
            "disarm returned while termination was still in flight"
        );
    }
}
