//! Bounded TypeScript/V8 module engine.
//!
//! The adapter executes one bundled JavaScript ESM artifact inside pooled
//! deno_core isolates. It implements the language-neutral `ModuleEngine`
//! contract and nothing else: the host keeps ownership of the Postgres
//! transaction, credentials, authorization, tenancy, and commit metadata, and
//! reaches the module only through `Invocation` in and `InvocationResult` out.
//!
//! What bounds a call:
//!
//! * an execution deadline enforced by a watchdog thread that terminates the
//!   isolate, so a JavaScript loop that never yields is still interruptible;
//! * a V8 heap ceiling per isolate, enforced through a near-heap-limit callback
//!   that terminates the call instead of aborting the process;
//! * a result byte budget checked in Rust;
//! * isolate recycling after `recycle_after_calls` calls, and immediately after
//!   any termination or unexplained failure.
//!
//! Modules see one host op, and the context object handed to a handler exposes
//! only the operations its function kind may reach. Tenant identity, budgets,
//! and the host channel live in `OpState` for the length of one call and are
//! taken back out afterwards, never in a JavaScript global, so isolate reuse
//! cannot carry one call's authority into the next.

mod dispatch;
mod isolate;
mod pool;

use futures_util::{stream::FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gonvex_module_runtime::{
    validate_portable_schema, validate_portable_schema_definition, BoxFuture, FunctionContract,
    Invocation, InvocationContext, InvocationResult, ModuleArtifact, ModuleEngine, ModuleError,
    ModuleHost, ModuleLanguage, ModuleManifest,
};
use tokio::sync::{mpsc, oneshot};

use crate::dispatch::effective_capabilities;
use crate::isolate::{CallSpec, HostRequest, ModuleSource, WorkerCall};
use crate::pool::IsolatePool;

/// Initialize V8 on the process's common parent thread before any isolate
/// workers are spawned. Since V8 11.6, allowing the first worker to perform
/// lazy initialization makes later generations created by sibling threads
/// abort the process instead of loading alongside the active generation.
pub fn initialize_v8_platform() {
    deno_core::JsRuntime::init_platform(None, false);
}

#[derive(Clone, Debug)]
pub struct V8Config {
    /// Ceiling on one isolate's V8 heap. Crossing it terminates the running
    /// call and retires the isolate.
    pub max_heap_bytes: usize,
    /// Upper bound on a single call. An invocation deadline shortens it; it
    /// never lengthens it.
    pub execution_timeout: Duration,
    /// Wall-clock ceiling for Actions, including external I/O and tool loops.
    pub action_execution_timeout: Duration,
    pub max_result_bytes: usize,
    /// Calls one isolate serves before it is retired. 0 disables reuse.
    pub recycle_after_calls: usize,
    /// Live isolates, and therefore the ceiling on concurrent module calls.
    pub isolate_pool_size: usize,
}

impl Default for V8Config {
    fn default() -> Self {
        Self {
            max_heap_bytes: 64 * 1024 * 1024,
            execution_timeout: Duration::from_secs(10),
            action_execution_timeout: Duration::from_secs(15 * 60),
            max_result_bytes: 8 * 1024 * 1024,
            recycle_after_calls: 10_000,
            isolate_pool_size: 1,
        }
    }
}

impl V8Config {
    pub fn timeout_for(&self, kind: &gonvex_module_runtime::FunctionKind) -> Duration {
        if matches!(kind, gonvex_module_runtime::FunctionKind::Action) {
            self.action_execution_timeout
        } else {
            self.execution_timeout
        }
    }
}

#[derive(Clone)]
pub struct V8ModuleEngine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    manifest: ModuleManifest,
    functions: HashMap<String, FunctionContract>,
    config: V8Config,
    pools: Vec<IsolatePool>,
}

impl V8ModuleEngine {
    pub fn from_artifact(artifact: ModuleArtifact, config: V8Config) -> Result<Self, ModuleError> {
        if !matches!(artifact.manifest.language, ModuleLanguage::TypeScript) {
            return Err(ModuleError::InvalidArtifact(
                "V8 adapter requires a TypeScript artifact".to_owned(),
            ));
        }
        if artifact.payload.is_empty() {
            return Err(ModuleError::InvalidArtifact(
                "empty JavaScript artifact".to_owned(),
            ));
        }
        let code = String::from_utf8(artifact.payload).map_err(|_| {
            ModuleError::InvalidArtifact("JavaScript artifact is not valid UTF-8".to_owned())
        })?;

        let mut functions = HashMap::with_capacity(artifact.manifest.functions.len());
        for contract in &artifact.manifest.functions {
            let args_schema = contract.args_schema.as_ref().ok_or_else(|| {
                ModuleError::InvalidArtifact(format!(
                    "function {} has no arguments schema",
                    contract.path
                ))
            })?;
            validate_portable_schema_definition(args_schema).map_err(|error| {
                ModuleError::InvalidArtifact(format!(
                    "function {} arguments schema is invalid: {error}",
                    contract.path
                ))
            })?;
            let result_schema = contract.result_schema.as_ref().ok_or_else(|| {
                ModuleError::InvalidArtifact(format!(
                    "function {} has no result schema",
                    contract.path
                ))
            })?;
            validate_portable_schema_definition(result_schema).map_err(|error| {
                ModuleError::InvalidArtifact(format!(
                    "function {} result schema is invalid: {error}",
                    contract.path
                ))
            })?;
            if functions
                .insert(contract.path.clone(), contract.clone())
                .is_some()
            {
                return Err(ModuleError::InvalidArtifact(format!(
                    "module declares function {} twice",
                    contract.path
                )));
            }
        }

        let source = Arc::new(ModuleSource::new(&artifact.manifest.module_id, code)?);
        // A call awaiting a host-mediated nested function must keep its V8
        // continuation alive. Separate depth pools prevent the nested call
        // from waiting on the isolate held by its parent.
        let pools = (0..=gonvex_module_runtime::MAX_INVOCATION_DEPTH)
            .map(|_| IsolatePool::new(Arc::clone(&source), config.clone()))
            .collect();
        Ok(Self {
            inner: Arc::new(EngineInner {
                manifest: artifact.manifest,
                functions,
                config,
                pools,
            }),
        })
    }

    pub fn config(&self) -> &V8Config {
        &self.inner.config
    }

    /// Starts every isolate in the pool and evaluates the bundle in each, so a
    /// broken artifact fails at load time rather than on a user's first call
    /// and an activated generation is warm before it serves traffic. Leases are
    /// held together, so this warms the pool rather than one reused isolate.
    pub async fn prewarm(&self) -> Result<(), ModuleError> {
        let mut leases = Vec::with_capacity(self.inner.config.isolate_pool_size.max(1));
        for _ in 0..self.inner.config.isolate_pool_size.max(1) {
            leases.push(self.inner.pools[0].acquire().await?);
        }
        for lease in leases {
            lease.release_unused();
        }
        Ok(())
    }
}

impl ModuleEngine for V8ModuleEngine {
    fn manifest(&self) -> &ModuleManifest {
        &self.inner.manifest
    }

    fn invoke<'a>(
        &'a self,
        host: &'a dyn ModuleHost,
        invocation: Invocation,
    ) -> BoxFuture<'a, Result<InvocationResult, ModuleError>> {
        Box::pin(async move { self.inner.execute(host, invocation).await })
    }
}

impl EngineInner {
    async fn execute(
        &self,
        host: &dyn ModuleHost,
        invocation: Invocation,
    ) -> Result<InvocationResult, ModuleError> {
        let contract = self
            .functions
            .get(&invocation.function)
            .cloned()
            .ok_or_else(|| ModuleError::FunctionNotFound(invocation.function.clone()))?;
        if contract.kind != invocation.kind {
            return Err(ModuleError::WrongFunctionKind(invocation.function.clone()));
        }
        let arguments: serde_json::Value = serde_json::from_slice(&invocation.args)
            .map_err(|error| ModuleError::InvalidArguments(format!("$: invalid JSON: {error}")))?;
        validate_portable_schema(
            contract
                .args_schema
                .as_ref()
                .expect("artifact validation requires arguments schema"),
            &arguments,
        )
        .map_err(ModuleError::InvalidArguments)?;
        let timeout = self.call_timeout(&invocation.context, &contract)?;
        let capabilities = effective_capabilities(&contract.kind, &invocation.context.capabilities);
        let context = invocation.context.clone();
        // A host-stamped `now` keeps every engine reporting the same clock for
        // one invocation; falling back to this process's clock only matters for
        // an embedder that does not set one.
        let now_unix_ms = match context.now_unix_ms {
            0 => SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis() as u64)
                .unwrap_or_default(),
            stamped => stamped,
        };

        let pool = self
            .pools
            .get(usize::from(invocation.context.nesting_depth))
            .ok_or_else(|| {
                ModuleError::Execution(
                    "nested function invocation exceeded the maximum depth".to_owned(),
                )
            })?;
        let lease = pool.acquire().await?;
        let (host_sender, mut host_calls) = mpsc::unbounded_channel::<HostRequest>();
        let (reply, mut replied) = oneshot::channel();
        lease.dispatch(WorkerCall {
            spec: CallSpec {
                contract: contract.clone(),
                invocation,
                capabilities,
                now_unix_ms,
                timeout,
                max_result_bytes: self.config.max_result_bytes,
                host: host_sender,
            },
            reply,
        })?;

        // The isolate runs on its own thread; this task holds the host reference
        // and answers the isolate's host calls until it reports a result. The
        // isolate never sees `host`, so the host never has to be `'static`.
        // Only network calls may overlap. Database and tool calls retain their
        // original ordering, while timers can forward heartbeats during a fetch.
        // These futures borrow this invocation and are cancelled when it ends.
        let mut network: FuturesUnordered<BoxFuture<'_, ()>> = FuturesUnordered::new();
        let mut bridging = true;
        let reply = loop {
            tokio::select! {
                biased;
                reply = &mut replied => break reply,
                Some(()) = network.next(), if !network.is_empty() => {},
                request = host_calls.recv(), if bridging => match request {
                    Some(request) => {
                        if matches!(request.call, gonvex_module_runtime::HostCall::Fetch { .. }) {
                            if network.len() >= 16 {
                                let _ = request.reply.send(Err(gonvex_module_runtime::HostError::Failed(
                                    "Action concurrent network request limit exceeded".into(),
                                )));
                                continue;
                            }
                            let context = &context;
                            network.push(Box::pin(async move {
                                let response = host.call(context, request.call).await;
                                let _ = request.reply.send(response);
                            }));
                        } else {
                            let response = host.call(&context, request.call).await;
                            let _ = request.reply.send(response);
                        }
                    }
                    // The isolate dropped its end of the bridge: the call is
                    // finishing and only the result is still outstanding.
                    None => bridging = false,
                },
            }
        };

        let result = match reply {
            Ok(reply) => {
                lease.finish(reply.healthy);
                reply.result
            }
            // Dropping the lease retires the isolate rather than pooling one
            // that stopped without answering.
            Err(_) => Err(ModuleError::Execution(
                "module isolate stopped before returning a result".to_owned(),
            )),
        }?;
        let value: serde_json::Value = serde_json::from_slice(&result.value)
            .map_err(|error| ModuleError::InvalidResult(format!("$: invalid JSON: {error}")))?;
        validate_portable_schema(
            contract
                .result_schema
                .as_ref()
                .expect("artifact validation requires result schema"),
            &value,
        )
        .map_err(ModuleError::InvalidResult)?;
        Ok(result)
    }

    /// The call deadline is the configured ceiling, shortened by whatever the
    /// invocation's own deadline leaves.
    fn call_timeout(
        &self,
        context: &InvocationContext,
        contract: &FunctionContract,
    ) -> Result<Option<Duration>, ModuleError> {
        let agent = matches!(contract.kind, gonvex_module_runtime::FunctionKind::Action)
            && contract
                .metadata
                .get("actionProfile")
                .and_then(serde_json::Value::as_str)
                == Some("agent");
        let ceiling = (!agent).then(|| self.config.timeout_for(&contract.kind));
        let Some(deadline) = context.deadline else {
            return Ok(ceiling);
        };
        let remaining = deadline.duration_since(SystemTime::now()).map_err(|_| {
            ModuleError::BudgetExceeded(
                "invocation deadline elapsed before the module started".to_owned(),
            )
        })?;
        Ok(Some(
            ceiling.map_or(remaining, |limit| remaining.min(limit)),
        ))
    }
}

#[cfg(test)]
mod host_call_tests {
    use super::*;
    use gonvex_module_runtime::{
        Capabilities, FunctionContract, FunctionKind, HostCall, HostError, HostResponse,
        InvocationContext, ModuleLanguage, ModuleManifest,
    };
    use serde_json::{json, Map};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn run_v8_test(test: impl std::future::Future<Output = ()> + Send + 'static) {
        // Match module-host: initialize V8 and start all isolate generations
        // from one persistent parent thread, rather than libtest's short-lived threads.
        type Job = Box<dyn FnOnce() + Send>;
        static RUNNER: std::sync::OnceLock<std::sync::mpsc::Sender<Job>> = std::sync::OnceLock::new();
        let runner = RUNNER.get_or_init(|| {
            let (sender, jobs) = std::sync::mpsc::channel::<Job>();
            std::thread::spawn(move || {
                initialize_v8_platform();
                for job in jobs { job(); }
            });
            sender
        });
        let (done, result) = std::sync::mpsc::channel();
        runner.send(Box::new(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(test);
            }));
            done.send(outcome).unwrap();
        })).unwrap();
        if let Err(panic) = result.recv().unwrap() { std::panic::resume_unwind(panic); }
    }

    struct CountingHost(AtomicUsize);
    impl ModuleHost for CountingHost {
        fn call<'a>(
            &'a self,
            _: &'a InvocationContext,
            _: HostCall,
        ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(HostResponse {
                    value: b"[]".to_vec(),
                })
            })
        }
    }

    #[test]
    fn invocation_can_complete_more_than_one_hundred_host_calls() {
        run_v8_test(async {
        let engine = V8ModuleEngine::from_artifact(
            ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "bulk-host-calls".into(),
                    generation: 1,
                    language: ModuleLanguage::TypeScript,
                    artifact_hash: "test".into(),
                    functions: vec![FunctionContract {
                        path: "run".into(),
                        kind: FunctionKind::Query,
                        internal: false,
                        delivery: None,
                        args_schema: Some(json!({"kind":"any"})),
                        result_schema: Some(json!({"kind":"any"})),
                        metadata: Map::from_iter([("export".into(), json!("run"))]),
                    }],
                    metadata: Map::new(),
                },
                payload: br#"export async function run(ctx) {
                for (let i = 0; i < 150; i++) await ctx.db.query('SELECT 1', []);
                return 150;
            }"#
                .to_vec(),
            },
            V8Config::default(),
        )
        .unwrap();
        let host = CountingHost(AtomicUsize::new(0));
        let result = engine
            .invoke(
                &host,
                Invocation {
                    function: "run".into(),
                    kind: FunctionKind::Query,
                    args: b"null".to_vec(),
                    context: InvocationContext {
                        generation: 1,
                        capabilities: Capabilities {
                            db_read: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("bulk work must not stop at a host-call count ceiling");
        assert_eq!(result.value, b"150");
        assert_eq!(host.0.load(Ordering::SeqCst), 150);
        });
    }
    struct HeartbeatHost(tokio::sync::Notify);
    impl ModuleHost for HeartbeatHost {
        fn call<'a>(
            &'a self,
            _: &'a InvocationContext,
            call: HostCall,
        ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
            Box::pin(async move {
                if matches!(call, HostCall::Fetch { .. }) {
                    tokio::time::timeout(Duration::from_secs(2), self.0.notified())
                        .await
                        .map_err(|_| HostError::Failed("heartbeat blocked behind fetch".into()))?;
                    Ok(HostResponse {
                        value: br#"{"status":200,"headers":{},"body":"ok"}"#.to_vec(),
                    })
                } else {
                    self.0.notify_one();
                    Ok(HostResponse {
                        value: b"true".to_vec(),
                    })
                }
            })
        }
    }

    #[test]
    fn timer_heartbeat_reaches_host_while_fetch_is_pending() {
        run_v8_test(async {
        let engine = V8ModuleEngine::from_artifact(
            ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "heartbeat-fetch".into(),
                    generation: 1,
                    language: ModuleLanguage::TypeScript,
                    artifact_hash: "test".into(),
                    functions: vec![FunctionContract {
                        path: "run".into(),
                        kind: FunctionKind::Action,
                        internal: false,
                        delivery: None,
                        args_schema: Some(json!({"kind":"any"})),
                        result_schema: Some(json!({"kind":"any"})),
                        metadata: Map::from_iter([("export".into(), json!("run"))]),
                    }],
                    metadata: Map::new(),
                },
                payload: br#"export async function run(ctx, args) {
                if (args?.warmup) {
                    const timer = setInterval(() => {}, 5);
                    clearInterval(timer);
                    await new Promise(resolve => setTimeout(resolve, 1));
                    return 'completed';
                }
                const heartbeat = new Promise((resolve, reject) => {
                    const timer = setInterval(() => {
                        clearInterval(timer);
                        ctx.tools.heartbeat({}).then(resolve, reject);
                    }, 20);
                });
                await ctx.fetch('https://example.com');
                await heartbeat;
                return 'completed';
            }"#
                .to_vec(),
            },
            V8Config::default(),
        )
        .unwrap();
        engine
            .invoke(
                &CountingHost(AtomicUsize::new(0)),
                Invocation {
                    function: "run".into(),
                    kind: FunctionKind::Action,
                    args: br#"{"warmup":true}"#.to_vec(),
                    context: InvocationContext {
                        generation: 1,
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let result = engine
            .invoke(
                &HeartbeatHost(tokio::sync::Notify::new()),
                Invocation {
                    function: "run".into(),
                    kind: FunctionKind::Action,
                    args: b"null".to_vec(),
                    context: InvocationContext {
                        generation: 1,
                        action_tools: vec!["heartbeat".into()],
                        capabilities: Capabilities {
                            network: true,
                            action_tools: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("timer heartbeat must not wait for a network response");
        assert_eq!(result.value, br#""completed""#);
        });
    }
    #[test]
    fn actions_can_wait_beyond_query_budget_without_extending_queries() {
        run_v8_test(async {
        for kind in [
            FunctionKind::Action,
            FunctionKind::Query,
            FunctionKind::Reducer,
        ] {
            let engine = V8ModuleEngine::from_artifact(
                ModuleArtifact {
                    manifest: ModuleManifest {
                        module_id: "kind-budget".into(),
                        generation: 1,
                        language: ModuleLanguage::TypeScript,
                        artifact_hash: "test".into(),
                        functions: vec![FunctionContract {
                            path: "run".into(),
                            kind: kind.clone(),
                            internal: false,
                            delivery: None,
                            args_schema: Some(json!({"kind":"any"})),
                            result_schema: Some(json!({"kind":"any"})),
                            metadata: Map::from_iter([("export".into(), json!("run"))]),
                        }],
                        metadata: Map::new(),
                    },
                    payload: br#"export async function run() {
                    await new Promise(resolve => setTimeout(resolve, 50));
                    return 'completed';
                }"#
                    .to_vec(),
                },
                V8Config {
                    execution_timeout: Duration::from_millis(10),
                    ..Default::default()
                },
            )
            .unwrap();
            let result = engine
                .invoke(
                    &CountingHost(AtomicUsize::new(0)),
                    Invocation {
                        function: "run".into(),
                        kind: kind.clone(),
                        args: b"null".to_vec(),
                        context: InvocationContext {
                            generation: 1,
                            deadline: Some(SystemTime::now() + Duration::from_millis(200)),
                            ..Default::default()
                        },
                    },
                )
                .await;
            if matches!(kind, FunctionKind::Action) {
                assert_eq!(
                    result
                        .expect("Action must use its own deadline, not the short query budget")
                        .value,
                    br#""completed""#
                );
            } else {
                assert!(
                    matches!(result, Err(ModuleError::BudgetExceeded(_))),
                    "Query budget must remain short: {result:?}"
                );
            }
        }
        });
    }

    #[test]
    fn agent_waits_without_wall_clock_cutoff_and_recovers_from_tool_timeout() {
        run_v8_test(async {
        struct TimeoutHost;
        impl ModuleHost for TimeoutHost {
            fn call<'a>(
                &'a self,
                _: &'a InvocationContext,
                call: HostCall,
            ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
                Box::pin(async move {
                    if matches!(call, HostCall::Fetch { .. }) {
                        std::future::pending::<()>().await;
                    }
                    Err(HostError::Failed("tool execution timed out".into()))
                })
            }
        }
        let engine = V8ModuleEngine::from_artifact(ModuleArtifact {
            manifest: ModuleManifest {
                module_id: "agent-budget".into(), generation: 1,
                language: ModuleLanguage::TypeScript, artifact_hash: "test".into(),
                functions: vec![FunctionContract {
                    path: "run".into(), kind: FunctionKind::Action, internal: false,
                    delivery: None, args_schema: Some(json!({"kind":"any"})),
                    result_schema: Some(json!({"kind":"any"})),
                    metadata: Map::from_iter([("export".into(), json!("run")), ("actionProfile".into(), json!("agent"))]),
                }], metadata: Map::new(),
            },
            payload: br#"export async function run(ctx, args) {
                if (args === 'cancel') {
                    const controller = new AbortController();
                    setTimeout(() => controller.abort(new Error('User stopped')), 20);
                    try { await ctx.fetch('https://example.com', { signal: controller.signal }); }
                    catch (error) { return String(error).includes('User stopped') ? 'recovered' : 'wrong error'; }
                }
                await new Promise(resolve => setTimeout(resolve, 50));
                try { await ctx.tools.slow({}); } catch (error) {
                    if (!String(error).includes('timed out')) throw error;
                    return 'recovered';
                }
                throw new Error('expected tool timeout');
            }"#.to_vec(),
        }, V8Config { execution_timeout: Duration::from_millis(10), action_execution_timeout: Duration::from_millis(10), ..Default::default() }).unwrap();
        for (explicit_deadline, args) in [
            (false, b"null".to_vec()),
            (true, b"null".to_vec()),
            (false, br#""cancel""#.to_vec()),
        ] {
            let result = engine
                .invoke(
                    &TimeoutHost,
                    Invocation {
                        function: "run".into(),
                        kind: FunctionKind::Action,
                        args,
                        context: InvocationContext {
                            generation: 1,
                            deadline: explicit_deadline
                                .then(|| SystemTime::now() + Duration::from_millis(10)),
                            action_tools: vec!["slow".into()],
                            capabilities: Capabilities {
                                action_tools: true,
                                network: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    },
                )
                .await;
            if explicit_deadline {
                assert!(matches!(result, Err(ModuleError::BudgetExceeded(_))));
            } else {
                assert_eq!(
                    result
                        .expect("tool timeout must be catchable by the agent after a long wait")
                        .value,
                    br#""recovered""#
                );
            }
        }
        });
    }
}
