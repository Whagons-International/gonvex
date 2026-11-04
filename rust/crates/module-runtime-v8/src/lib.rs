#![warn(clippy::redundant_clone)]
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

mod admission;
mod budget;
mod dispatch;
mod isolate;
#[cfg(test)]
mod lookup_deadline_tests;
mod pool;
#[cfg(test)]
mod runtime_contract_tests;
#[cfg(test)]
mod scheduling_tests;
#[cfg(test)]
mod split_tests;
mod webcrypto;

/// Per-thread priority (re-exported for gonvex-module-host).
pub use gonvex_profiler::thread_priority;

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

use crate::budget::CallBudget;
pub use crate::isolate::{
    heap_tally, isolate_stats, live_isolates, trim_native_heap, ISOLATE_STAT_FIELDS,
};
use crate::isolate::{CallSpec, HostRequest, ModuleSource, WorkerCall};
use crate::pool::IsolatePool;
pub use crate::admission::Lane;
use gonvex_module_runtime::effective_capabilities;

/// Initialize V8 on the process's common parent thread before any isolate
/// workers are spawned. Since V8 11.6, allowing the first worker to perform
/// lazy initialization makes later generations created by sibling threads
/// abort the process instead of loading alongside the active generation.
pub fn initialize_v8_platform() {
    // V8 freezes its flags once the platform is initialized, so operator
    // flags (for example `--max-lazy`, `--no-opt`) are applied first. The
    // deno_core defaults are applied on top and do not reset them.
    if let Some(flags) = v8_flags_from_env() {
        deno_core::v8::V8::set_flags_from_string(&flags);
    }
    deno_core::JsRuntime::init_platform(None, false);
}

/// Extra V8 flags from `GONVEX_V8_FLAGS`, space separated. Unset or blank
/// means V8's defaults.
pub fn v8_flags_from_env() -> Option<String> {
    std::env::var("GONVEX_V8_FLAGS")
        .ok()
        .map(|flags| flags.trim().to_owned())
        .filter(|flags| !flags.is_empty())
}

#[derive(Clone, Debug)]
pub struct V8Config {
    /// Ceiling on one isolate's V8 heap. Crossing it terminates the running
    /// call and retires the isolate.
    pub max_heap_bytes: usize,
    /// Ceiling on bootstrap and top-level module initialization, including
    /// asynchronous evaluation. Independent of the handler execution budget.
    pub initialization_timeout: Duration,
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
    /// Split artifacts only: how long an idle isolate of the complete bundle
    /// (which serves Actions) stays alive before it is released. Those
    /// isolates start on demand, so a quiet module keeps only its small
    /// Query/Reducer isolates resident.
    pub on_demand_idle_ttl: Duration,
    /// Split artifacts only: evaluate the complete bundle once while loading,
    /// then release that isolate, so a bundle that throws at evaluation still
    /// fails the load instead of the first Action.
    pub validate_on_demand_bundle: bool,
    /// Unsplit artifacts: run Actions in their own isolates of the complete
    /// bundle, started on demand and released after `on_demand_idle_ttl`,
    /// like a split artifact's Action isolates. Queries and Reducers keep the
    /// warm isolates, so an Action that awaits `fetch` never holds the
    /// isolate user Reducers need. Off: Actions share the warm pools.
    pub on_demand_actions: bool,
    /// Serve isolate waiters by lane (user, then outbox/system, then
    /// scheduled; see `admission`). Off: first come, first served.
    pub isolate_priority: bool,
    /// Execution ceiling for scheduled Queries and Reducers (crons and
    /// one-shot jobs at nesting depth 0), counted from isolate dispatch. A
    /// scheduled Reducer holds the tenant writer and the module isolate for
    /// its whole run, so it gets a short slice and an error that says to
    /// process a bounded batch per run. Zero: only `execution_timeout`.
    pub scheduled_execution_timeout: Duration,
    /// Run scheduled Queries and Reducers in their own isolate (started on
    /// demand, released after `on_demand_idle_ttl`), so a long cron never
    /// holds the isolate user calls wait for. Costs one more isolate heap
    /// while scheduled work runs.
    pub scheduled_isolate: bool,
}

impl Default for V8Config {
    fn default() -> Self {
        Self {
            max_heap_bytes: 64 * 1024 * 1024,
            initialization_timeout: Duration::from_secs(10),
            execution_timeout: Duration::from_secs(10),
            action_execution_timeout: Duration::from_secs(15 * 60),
            max_result_bytes: 8 * 1024 * 1024,
            recycle_after_calls: 10_000,
            isolate_pool_size: 1,
            on_demand_idle_ttl: Duration::from_secs(60),
            validate_on_demand_bundle: true,
            on_demand_actions: true,
            isolate_priority: true,
            scheduled_execution_timeout: Duration::from_secs(3),
            scheduled_isolate: false,
        }
    }
}

impl V8Config {
    /// Applies the scheduling knobs shared by the in-process and the separate
    /// module host: `GONVEX_MODULE_HOST_ISOLATE_PRIORITY` (default on),
    /// `GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS` (default 3000, 0 = off) and
    /// `GONVEX_MODULE_HOST_SCHEDULED_ISOLATE` (default off).
    pub fn with_scheduling_env(self) -> Self {
        self.with_scheduling(|name| std::env::var(name).ok())
    }

    pub fn with_scheduling(mut self, lookup: impl Fn(&str) -> Option<String>) -> Self {
        let flag = |name: &str, default: bool| {
            lookup(name)
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty())
                .map_or(default, |value| !matches!(value.as_str(), "0" | "false" | "off" | "no"))
        };
        self.isolate_priority = flag("GONVEX_MODULE_HOST_ISOLATE_PRIORITY", self.isolate_priority);
        self.scheduled_isolate = flag("GONVEX_MODULE_HOST_SCHEDULED_ISOLATE", self.scheduled_isolate);
        if let Some(ms) = lookup("GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS")
            .and_then(|value| value.trim().parse::<u64>().ok())
        {
            self.scheduled_execution_timeout = Duration::from_millis(ms);
        }
        self
    }

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

/// The Query/Reducer-only bundle of a split artifact.
#[derive(Clone, Debug)]
pub struct ReducerBundle {
    pub code: Vec<u8>,
    /// Function paths this bundle serves; each must be a Query or Reducer of
    /// the artifact. Every other function runs from the complete bundle.
    pub functions: Vec<String>,
}

struct EngineInner {
    manifest: ModuleManifest,
    functions: HashMap<String, FunctionContract>,
    config: V8Config,
    /// Complete bundle, one pool per nesting depth. In a split engine these
    /// isolates start on demand and are released after `on_demand_idle_ttl`.
    pools: Vec<IsolatePool>,
    reducer: Option<ReducerPools>,
    /// Unsplit engines with `on_demand_actions`: complete-bundle pools for
    /// Actions (one per nesting depth), started on demand and released when
    /// idle. Empty otherwise.
    actions: Vec<IsolatePool>,
    /// With `scheduled_isolate`: one pool for scheduled Queries and Reducers
    /// at depth 0, from the same bundle as the warm pools, started on demand.
    scheduled: Option<IsolatePool>,
}

struct ReducerPools {
    functions: std::collections::HashSet<String>,
    pools: Vec<IsolatePool>,
}

impl V8ModuleEngine {
    pub fn from_artifact(artifact: ModuleArtifact, config: V8Config) -> Result<Self, ModuleError> {
        Self::from_split_artifact(artifact, None, config)
    }

    /// Like `from_artifact`, plus an optional Query/Reducer-only bundle. The
    /// functions it lists run from it, resolved by exact export path; all
    /// other functions, Actions included, run from the complete bundle, whose
    /// isolates are then started on demand and released when idle.
    pub fn from_split_artifact(
        artifact: ModuleArtifact,
        reducer: Option<ReducerBundle>,
        config: V8Config,
    ) -> Result<Self, ModuleError> {
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
        let pools: Vec<IsolatePool> = (0..=gonvex_module_runtime::MAX_INVOCATION_DEPTH)
            .map(|_| IsolatePool::new(Arc::clone(&source), config.clone()))
            .collect();
        let reducer = match reducer {
            None => None,
            Some(bundle) => {
                if bundle.code.is_empty() {
                    return Err(ModuleError::InvalidArtifact(
                        "empty JavaScript reducer bundle".to_owned(),
                    ));
                }
                let code = String::from_utf8(bundle.code).map_err(|_| {
                    ModuleError::InvalidArtifact(
                        "JavaScript reducer bundle is not valid UTF-8".to_owned(),
                    )
                })?;
                let mut served = std::collections::HashSet::with_capacity(bundle.functions.len());
                for path in bundle.functions {
                    match functions.get(&path).map(|contract| &contract.kind) {
                        Some(gonvex_module_runtime::FunctionKind::Query)
                        | Some(gonvex_module_runtime::FunctionKind::Reducer) => {
                            served.insert(path);
                        }
                        Some(_) => {
                            return Err(ModuleError::InvalidArtifact(format!(
                                "reducer bundle lists {path}, which is not a Query or Reducer"
                            )))
                        }
                        None => {
                            return Err(ModuleError::InvalidArtifact(format!(
                                "reducer bundle lists unknown function {path}"
                            )))
                        }
                    }
                }
                let source = Arc::new(ModuleSource::reducer_bundle(
                    &artifact.manifest.module_id,
                    code,
                )?);
                Some(ReducerPools {
                    functions: served,
                    pools: (0..=gonvex_module_runtime::MAX_INVOCATION_DEPTH)
                        .map(|_| IsolatePool::new(Arc::clone(&source), config.clone()))
                        .collect(),
                })
            }
        };
        let actions = if reducer.is_none() && config.on_demand_actions {
            (0..=gonvex_module_runtime::MAX_INVOCATION_DEPTH)
                .map(|_| IsolatePool::new(Arc::clone(&source), config.clone()))
                .collect()
        } else {
            Vec::new()
        };
        let scheduled = config.scheduled_isolate.then(|| {
            let warm = match &reducer {
                Some(reducer) => &reducer.pools[0],
                None => &pools[0],
            };
            // One isolate: scheduled runs are one per tenant and few at once.
            IsolatePool::new(
                warm.source(),
                V8Config {
                    isolate_pool_size: 1,
                    ..config.clone()
                },
            )
        });
        let inner = Arc::new(EngineInner {
            manifest: artifact.manifest,
            functions,
            config,
            pools,
            reducer,
            actions,
            scheduled,
        });
        if inner.has_on_demand_pools() {
            spawn_on_demand_reaper(Arc::downgrade(&inner));
        }
        Ok(Self { inner })
    }

    /// Whether this engine runs a split artifact.
    pub fn is_split(&self) -> bool {
        self.inner.reducer.is_some()
    }

    pub fn config(&self) -> &V8Config {
        &self.inner.config
    }

    /// Starts every isolate in the pool and evaluates the bundle in each, so a
    /// broken artifact fails at load time rather than on a user's first call
    /// and an activated generation is warm before it serves traffic. Leases are
    /// held together, so this warms the pool rather than one reused isolate.
    ///
    /// A split engine warms its Query/Reducer pool instead, and evaluates the
    /// complete bundle once (unless `validate_on_demand_bundle` is off) before
    /// releasing that isolate again.
    pub async fn prewarm(&self) -> Result<(), ModuleError> {
        let warm = match &self.inner.reducer {
            Some(reducer) => &reducer.pools[0],
            None => &self.inner.pools[0],
        };
        let mut leases = Vec::with_capacity(self.inner.config.isolate_pool_size.max(1));
        for _ in 0..self.inner.config.isolate_pool_size.max(1) {
            leases.push(warm.acquire().await?);
        }
        for lease in leases {
            lease.release_unused();
        }
        if self.inner.reducer.is_some() && self.inner.config.validate_on_demand_bundle {
            self.inner.pools[0].acquire().await?.retire_unused();
        }
        Ok(())
    }

    /// Releases idle isolates of the on-demand (complete bundle) pools that
    /// have been idle for at least `ttl`. Returns how many were released.
    pub fn release_idle_on_demand(&self, ttl: Duration) -> usize {
        self.inner.release_idle_on_demand(ttl)
    }

    /// Idle isolates per pool family, `(warm, on demand)`, for diagnostics.
    pub fn idle_isolates(&self) -> (usize, usize) {
        let warm = match &self.inner.reducer {
            Some(reducer) => &reducer.pools,
            None => &self.inner.pools,
        };
        let scheduled = self.inner.scheduled.as_ref().map_or(0, IsolatePool::idle);
        let on_demand = match (&self.inner.reducer, self.inner.actions.is_empty()) {
            (Some(_), _) => &self.inner.pools,
            (None, false) => &self.inner.actions,
            (None, true) => return (warm.iter().map(IsolatePool::idle).sum(), scheduled),
        };
        (
            warm.iter().map(IsolatePool::idle).sum(),
            on_demand.iter().map(IsolatePool::idle).sum::<usize>() + scheduled,
        )
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

/// One thread per split engine; it ends once the engine is dropped.
fn spawn_on_demand_reaper(engine: std::sync::Weak<EngineInner>) {
    let _ = std::thread::Builder::new()
        .name("gonvex-module-reaper".to_owned())
        .spawn(move || loop {
            let ttl = match engine.upgrade() {
                Some(engine) => engine.config.on_demand_idle_ttl,
                None => return,
            };
            std::thread::sleep((ttl / 4).clamp(Duration::from_millis(50), Duration::from_secs(5)));
            match engine.upgrade() {
                Some(engine) => {
                    engine.release_idle_on_demand(ttl);
                }
                None => return,
            }
        });
}

impl EngineInner {
    fn has_on_demand_pools(&self) -> bool {
        self.reducer.is_some() || !self.actions.is_empty() || self.scheduled.is_some()
    }

    fn release_idle_on_demand(&self, ttl: Duration) -> usize {
        let on_demand = if self.reducer.is_some() {
            &self.pools
        } else {
            &self.actions
        };
        on_demand.iter().map(|pool| pool.evict_idle(ttl)).sum::<usize>()
            + self.scheduled.as_ref().map_or(0, |pool| pool.evict_idle(ttl))
    }

    /// The pool family and slot for one call.
    ///
    /// Only Actions start nested calls (tools, `ctx.functions`), and a nested
    /// call must not wait for an isolate its own ancestors hold, so a family
    /// that serves Actions is indexed by nesting depth. A Query or Reducer in
    /// a family that never serves Actions has no ancestor there, so it always
    /// uses slot 0: an Action's tool Reducers share the warm isolate with
    /// user calls instead of starting one more warm isolate per depth.
    fn pool_for(
        &self,
        function: &str,
        kind: &gonvex_module_runtime::FunctionKind,
        depth: u8,
        lane: Lane,
    ) -> Option<&IsolatePool> {
        let data = matches!(
            kind,
            gonvex_module_runtime::FunctionKind::Query
                | gonvex_module_runtime::FunctionKind::Reducer
        );
        // Scheduled data calls at the root take the scheduled isolate when
        // the warm pools would serve them.
        if let Some(scheduled) = &self.scheduled {
            let warm = match &self.reducer {
                Some(reducer) => reducer.functions.contains(function),
                None => true,
            };
            if data && depth == 0 && lane == Lane::Scheduled && warm {
                return Some(scheduled);
            }
        }
        let (pools, slot) = match &self.reducer {
            Some(reducer) if data && reducer.functions.contains(function) => (&reducer.pools, 0),
            Some(_) => (&self.pools, depth),
            None if self.actions.is_empty() => (&self.pools, depth),
            None if data => (&self.pools, 0),
            None => (&self.actions, depth),
        };
        if depth > gonvex_module_runtime::MAX_INVOCATION_DEPTH {
            return None;
        }
        pools.get(usize::from(slot))
    }

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
        let lane = Lane::of(&invocation.context);
        let slice = self.scheduled_slice(&invocation.context, &contract, lane);
        let budget = self.call_budget(&invocation.context, &contract, slice)?;
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
            .pool_for(
                &invocation.function,
                &contract.kind,
                invocation.context.nesting_depth,
                lane,
            )
            .ok_or_else(|| {
                ModuleError::Execution(
                    "nested function invocation exceeded the maximum depth".to_owned(),
                )
            })?;
        let lease = match budget.invocation_remaining()? {
            Some(remaining) => tokio::time::timeout(remaining, pool.acquire_in(lane))
                .await
                .map_err(|_| {
                    ModuleError::BudgetExceeded(
                        "invocation deadline elapsed while waiting for a module isolate".to_owned(),
                    )
                })??,
            None => pool.acquire_in(lane).await?,
        };
        host.isolate_acquired();
        let timeout = match budget.execution_timeout() {
            Ok(timeout) => timeout,
            Err(error) => {
                // A deadline may elapse just as the permit becomes available.
                // Nothing ran, so keep the healthy worker and its call budget.
                lease.release_unused();
                return Err(error);
            }
        };
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
                inline: host.inline(),
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
        let mut serial: Option<BoxFuture<'_, ()>> = None;
        let reply = loop {
            tokio::select! {
                biased;
                reply = &mut replied => break reply,
                Some(()) = network.next(), if !network.is_empty() => {},
                () = async { serial.as_mut().expect("enabled serial host future").await }, if serial.is_some() => {
                    serial = None;
                },
                request = host_calls.recv(), if bridging && serial.is_none() => match request {
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
                            let context = &context;
                            serial = Some(Box::pin(async move {
                                let response = host.call(context, request.call).await;
                                let _ = request.reply.send(response);
                            }));
                        }
                    }
                    // The isolate dropped its end of the bridge: the call is
                    // finishing and only the result is still outstanding.
                    None => bridging = false,
                },
            }
        };

        // End the borrowed host work before returning the worker or its result.
        drop(serial);
        drop(network);
        drop(host_calls);
        let sliced = slice.is_some() && timeout == slice;
        let result = match reply {
            Ok(reply) => {
                lease.finish(reply.healthy);
                match reply.result {
                    Err(ModuleError::BudgetExceeded(message))
                        if sliced && message.contains("execution deadline") =>
                    {
                        Err(ModuleError::BudgetExceeded(scheduled_slice_message(
                            &contract,
                            slice.unwrap_or_default(),
                        )))
                    }
                    other => other,
                }
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
    /// The time slice of a scheduled Query or Reducer at the root, if any.
    fn scheduled_slice(
        &self,
        context: &InvocationContext,
        contract: &FunctionContract,
        lane: Lane,
    ) -> Option<Duration> {
        let data = matches!(
            contract.kind,
            gonvex_module_runtime::FunctionKind::Query
                | gonvex_module_runtime::FunctionKind::Reducer
        );
        let slice = self.config.scheduled_execution_timeout;
        (data && lane == Lane::Scheduled && context.nesting_depth == 0 && !slice.is_zero())
            .then_some(slice)
    }

    fn call_budget(
        &self,
        context: &InvocationContext,
        contract: &FunctionContract,
        slice: Option<Duration>,
    ) -> Result<CallBudget, ModuleError> {
        let agent = matches!(contract.kind, gonvex_module_runtime::FunctionKind::Action)
            && contract
                .metadata
                .get("actionProfile")
                .and_then(serde_json::Value::as_str)
                == Some("agent");
        let ceiling = (!agent).then(|| {
            let ceiling = self.config.timeout_for(&contract.kind);
            slice.map_or(ceiling, |slice| slice.min(ceiling))
        });
        CallBudget::capture(context.deadline, ceiling)
    }
}

/// The error a scheduled Query or Reducer gets when it outruns its slice.
fn scheduled_slice_message(contract: &FunctionContract, slice: Duration) -> String {
    format!(
        "scheduled {} {} exceeded its {} ms time slice for scheduled work \
         (GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS). A scheduled run holds the tenant \
         writer and the module isolate while it runs: process a bounded batch per \
         run and schedule the rest with ctx.scheduler.runAfter",
        match contract.kind {
            gonvex_module_runtime::FunctionKind::Query => "query",
            _ => "reducer",
        },
        contract.path,
        slice.as_millis()
    )
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

    #[derive(Default)]
    struct DeadlineQueueHost {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl ModuleHost for DeadlineQueueHost {
        fn call<'a>(
            &'a self,
            _: &'a InvocationContext,
            call: HostCall,
        ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
            Box::pin(async move {
                match call {
                    HostCall::ToolInvoke { tool, .. } if tool == "hold" => {
                        self.entered.notify_one();
                        self.release.notified().await;
                        Ok(HostResponse {
                            value: b"null".to_vec(),
                        })
                    }
                    _ => Err(HostError::Failed("unexpected queue-test host call".into())),
                }
            })
        }
    }

    fn queued_invocation(hold: bool, deadline: Option<SystemTime>) -> Invocation {
        Invocation {
            function: "run".into(),
            kind: FunctionKind::Action,
            args: serde_json::to_vec(&json!({ "hold": hold })).unwrap(),
            context: InvocationContext {
                generation: 1,
                deadline,
                action_tools: vec!["hold".into()],
                capabilities: Capabilities {
                    action_tools: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        }
    }

    #[test]
    fn queued_invocation_respects_deadline_without_retiring_healthy_isolate() {
        run_v8_test(async {
            let engine = V8ModuleEngine::from_artifact(
                ModuleArtifact {
                    manifest: ModuleManifest {
                        module_id: "queue-deadline".into(),
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
                    payload: br#"let calls = 0;
                        export async function run(ctx, args) {
                            calls += 1;
                            if (args.hold) await ctx.tools.hold({});
                            return calls;
                        }"#
                    .to_vec(),
                },
                V8Config {
                    isolate_pool_size: 1,
                    action_execution_timeout: Duration::from_secs(2),
                    ..Default::default()
                },
            )
            .unwrap();
            engine.prewarm().await.unwrap();
            let host = Arc::new(DeadlineQueueHost::default());
            let first = tokio::spawn({
                let engine = engine.clone();
                let host = host.clone();
                async move { engine.invoke(&*host, queued_invocation(true, None)).await }
            });
            tokio::time::timeout(Duration::from_secs(1), host.entered.notified())
                .await
                .unwrap();
            let mut second = tokio::spawn({
                let engine = engine.clone();
                let host = host.clone();
                async move {
                    engine
                        .invoke(
                            &*host,
                            queued_invocation(
                                false,
                                Some(SystemTime::now() + Duration::from_millis(50)),
                            ),
                        )
                        .await
                }
            });
            let early = tokio::time::timeout(Duration::from_millis(250), &mut second).await;
            let answered_while_queued = early.is_ok();
            host.release.notify_one();
            first.await.unwrap().unwrap();
            let result = match early {
                Ok(result) => result.unwrap(),
                Err(_) => second.await.unwrap(),
            };
            assert!(
                matches!(result, Err(ModuleError::BudgetExceeded(_))),
                "expired queued call ran: {result:?}"
            );
            assert!(
                answered_while_queued,
                "deadline must bound the isolate queue wait"
            );
            let third = engine
                .invoke(&*host, queued_invocation(false, None))
                .await
                .unwrap();
            assert_eq!(
                third.value, b"2",
                "expired waiter must neither run nor retire the healthy isolate"
            );
        });
    }

    pub(super) fn run_v8_test(test: impl std::future::Future<Output = ()> + Send + 'static) {
        // Match module-host: initialize V8 and start all isolate generations
        // from one persistent parent thread, rather than libtest's short-lived threads.
        type Job = Box<dyn FnOnce() + Send>;
        static RUNNER: std::sync::OnceLock<std::sync::mpsc::Sender<Job>> =
            std::sync::OnceLock::new();
        let runner = RUNNER.get_or_init(|| {
            let (sender, jobs) = std::sync::mpsc::channel::<Job>();
            std::thread::spawn(move || {
                initialize_v8_platform();
                for job in jobs {
                    job();
                }
            });
            sender
        });
        let (done, result) = std::sync::mpsc::channel();
        runner
            .send(Box::new(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(test);
                }));
                done.send(outcome).unwrap();
            }))
            .unwrap();
        if let Err(panic) = result.recv().unwrap() {
            // The persistent runner may inherit another libtest thread's
            // output capture. Forward the payload on this failing test too.
            if let Some(message) = panic.downcast_ref::<String>() {
                eprintln!("V8 worker test failed: {message}");
            } else if let Some(message) = panic.downcast_ref::<&str>() {
                eprintln!("V8 worker test failed: {message}");
            }
            std::panic::resume_unwind(panic);
        }
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
    fn initialization_watchdog_does_not_shorten_later_invocations() {
        // Keep the handler wait longer than the initialization ceiling, with
        // startup slack for loaded CI hosts. The gated watchdog unit test
        // separately checks the in-flight termination/disarm race.
        run_v8_test(async {
            let engine = V8ModuleEngine::from_artifact(ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "initialization-watchdog".into(), generation: 1,
                    language: ModuleLanguage::TypeScript, artifact_hash: "test".into(),
                    functions: vec![FunctionContract {
                        path: "run".into(), kind: FunctionKind::Query, internal: false, delivery: None,
                        args_schema: Some(json!({"kind":"any"})), result_schema: Some(json!({"kind":"any"})),
                        metadata: Map::from_iter([("export".into(), json!("run"))]),
                    }], metadata: Map::new(),
                },
                payload: b"export async function run() { await new Promise(resolve => setTimeout(resolve, 1500)); return 'completed'; }".to_vec(),
            }, V8Config { initialization_timeout: Duration::from_secs(1), execution_timeout: Duration::from_secs(10), ..Default::default() }).unwrap();
            engine
                .prewarm()
                .await
                .expect("watchdog fixture initialization failed");
            for _ in 0..2 {
                let result = engine
                    .invoke(
                        &CountingHost(AtomicUsize::new(0)),
                        Invocation {
                            function: "run".into(),
                            kind: FunctionKind::Query,
                            args: b"null".to_vec(),
                            context: InvocationContext {
                                generation: 1,
                                ..Default::default()
                            },
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(result.value, br#""completed""#);
            }
        });
    }

    #[test]
    fn inviter_identity_is_host_owned_and_does_not_leak_between_calls() {
        run_v8_test(async {
            let engine = V8ModuleEngine::from_artifact(
                ModuleArtifact {
                    manifest: ModuleManifest {
                        module_id: "invitation-context".into(), generation: 1,
                        language: ModuleLanguage::TypeScript, artifact_hash: "test".into(),
                        functions: vec![FunctionContract {
                            path: "accept".into(), kind: FunctionKind::Reducer, internal: true,
                            delivery: None, args_schema: Some(json!({"kind":"any"})),
                            result_schema: Some(json!({"kind":"any"})),
                            metadata: Map::from_iter([("export".into(), json!("accept"))]),
                        }], metadata: Map::new(),
                    },
                    payload: br#"export function accept(ctx, args) {
                        return {inviter: ctx.invitationInviter?.id ?? null, frozen: Object.isFrozen(ctx),
                          immutable: !ctx.invitationInviter || (Object.isFrozen(ctx.invitationInviter) && Object.isFrozen(ctx.invitationInviter.permissions))};
                    }"#.to_vec(),
                }, V8Config::default(),
            ).unwrap();
            let host = CountingHost(AtomicUsize::new(0));
            for (trusted, expected) in [
                (Some("canonical-inviter"), json!("canonical-inviter")),
                (None, json!(null)),
            ] {
                let result = engine
                    .invoke(
                        &host,
                        Invocation {
                            function: "accept".into(),
                            kind: FunctionKind::Reducer,
                            args: br#"{"invitationInviter":{"id":"spoofed-owner","permissions":{"developer":true}}}"#.to_vec(),
                            context: InvocationContext {
                                invitation_inviter: trusted.map(|id| gonvex_module_runtime::MemberIdentity { id: id.to_owned(), permissions: json!({}), ..Default::default() }),
                                generation: 1,
                                ..Default::default()
                            },
                        },
                    )
                    .await
                    .unwrap();
                let result: serde_json::Value = serde_json::from_slice(&result.value).unwrap();
                assert_eq!(result["inviter"], expected);
                assert_eq!(result["frozen"], true);
                assert_eq!(result["immutable"], true);
            }
        });
    }

    #[test]
    fn developer_session_is_host_owned_and_does_not_leak_between_calls() {
        run_v8_test(async {
            let engine = V8ModuleEngine::from_artifact(
                ModuleArtifact {
                    manifest: ModuleManifest {
                        module_id: "developer-context".into(), generation: 1,
                        language: ModuleLanguage::TypeScript, artifact_hash: "test".into(),
                        functions: vec![FunctionContract {
                            path: "accept".into(), kind: FunctionKind::Reducer, internal: true,
                            delivery: None, args_schema: Some(json!({"kind":"any"})),
                            result_schema: Some(json!({"kind":"any"})),
                            metadata: Map::from_iter([("export".into(), json!("accept"))]),
                        }], metadata: Map::new(),
                    },
                    payload: br#"export function accept(ctx, args) {
                        return {grant: ctx.auth.developerSession?.grantId ?? null, frozen: Object.isFrozen(ctx),
                          immutable: !ctx.auth.developerSession || Object.isFrozen(ctx.auth.developerSession)};
                    }"#.to_vec(),
                }, V8Config::default(),
            ).unwrap();
            let host = CountingHost(AtomicUsize::new(0));
            for (trusted, expected) in [
                (Some("devmode_verified"), json!("devmode_verified")),
                (None, json!(null)),
            ] {
                let result = engine
                    .invoke(
                        &host,
                        Invocation {
                            function: "accept".into(),
                            kind: FunctionKind::Reducer,
                            args: br#"{"developerSession":{"grantId":"devmode_spoofed"}}"#.to_vec(),
                            context: InvocationContext {
                                identity: gonvex_module_runtime::IdentityContext { developer_session: trusted.map(|id| json!({ "grantId": id, "accountId": "account", "tenantId": "tenant", "expiresAt": 2000 })), ..Default::default() },
                                generation: 1,
                                ..Default::default()
                            },
                        },
                    )
                    .await
                    .unwrap();
                let result: serde_json::Value = serde_json::from_slice(&result.value).unwrap();
                assert_eq!(result["grant"], expected);
                assert_eq!(result["frozen"], true);
                assert_eq!(result["immutable"], true);
            }
        });
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
    /// Serves one event-stream fetch whose body arrives over three reads.
    struct StreamingHost(AtomicUsize);
    impl ModuleHost for StreamingHost {
        fn call<'a>(
            &'a self,
            _: &'a InvocationContext,
            call: HostCall,
        ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
            Box::pin(async move {
                let value: &[u8] = match call {
                    HostCall::Fetch { .. } => br#"{"status":200,"headers":{"content-type":"text/event-stream"},"stream":1}"#,
                    HostCall::FetchRead { stream: 1 } => match self.0.fetch_add(1, Ordering::SeqCst) {
                        0 => br#"{"chunk":"data: one\n\nda","done":false}"#,
                        1 => br#"{"chunk":"ta: caf\u00e9\n\n","done":false}"#,
                        _ => br#"{"done":true}"#,
                    },
                    other => return Err(HostError::Failed(format!("unexpected call {other:?}"))),
                };
                Ok(HostResponse {
                    value: value.to_vec(),
                })
            })
        }
    }

    #[test]
    fn fetch_bodies_stream_through_standard_web_streams() {
        run_v8_test(async {
            let engine = V8ModuleEngine::from_artifact(
            ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "streaming-fetch".into(),
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
                payload: br#"export async function run(ctx) {
                const response = await ctx.fetch('https://example.com/stream', { method: 'POST', body: '{}' });
                const [events, copy] = response.body.tee();
                // Shaped like the AI SDK's EventSourceParserStream: state set up in start().
                let buffer;
                const splitter = new TransformStream({
                    start() { buffer = ''; },
                    transform(chunk, controller) {
                        buffer += chunk;
                        const parts = buffer.split('\n\n');
                        buffer = parts.pop();
                        for (const part of parts) controller.enqueue(part.replace(/^data: /, ''));
                    },
                });
                const seen = [];
                const reader = events.pipeThrough(new TextDecoderStream()).pipeThrough(splitter).getReader();
                for (;;) { const { value, done } = await reader.read(); if (done) break; seen.push(value); }
                const text = await new Response(copy).text();
                let next = 0;
                const pulled = [];
                for await (const value of new ReadableStream({ pull(c) { next < 3 ? c.enqueue(next++) : c.close(); } })) pulled.push(value);
                const bytes = new TextEncoder().encode('\u00e9');
                const decoder = new TextDecoder();
                const split = decoder.decode(bytes.subarray(0, 1), { stream: true }) + decoder.decode(bytes.subarray(1));
                return { seen, text, pulled, split };
            }"#
                .to_vec(),
            },
            V8Config::default(),
        )
        .unwrap();
            let result = engine
                .invoke(
                    &StreamingHost(AtomicUsize::new(0)),
                    Invocation {
                        function: "run".into(),
                        kind: FunctionKind::Action,
                        args: b"null".to_vec(),
                        context: InvocationContext {
                            generation: 1,
                            capabilities: Capabilities {
                                network: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    },
                )
                .await
                .expect("streamed fetch body must be readable with standard streams");
            let value: serde_json::Value = serde_json::from_slice(&result.value).unwrap();
            assert_eq!(
                value,
                json!({
                    "seen": ["one", "caf\u{e9}"],
                    "text": "data: one\n\ndata: caf\u{e9}\n\n",
                    "pulled": [0, 1, 2],
                    "split": "\u{e9}",
                })
            );
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

    #[test]
    fn invocation_delegation_reaches_the_module_context() {
        run_v8_test(async {
            use gonvex_module_runtime::{
                InvocationChannel, InvocationDelegation, InvocationDelegationActor, InvocationInfo,
            };
            let engine = V8ModuleEngine::from_artifact(
                ModuleArtifact {
                    manifest: ModuleManifest {
                        module_id: "invocation-delegation".into(),
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
                const { channel, rootChannel, delegation } = ctx.invocation;
                return { channel, rootChannel, delegation };
            }"#
                    .to_vec(),
                },
                V8Config::default(),
            )
            .unwrap();
            let delegated = InvocationInfo {
                channel: InvocationChannel::Api,
                root_channel: InvocationChannel::Api,
                delegation: Some(InvocationDelegation {
                    principal: "gateway".into(),
                    actor: Some(InvocationDelegationActor {
                        kind: "api_key".into(),
                        name: "CI key".into(),
                        reference: None,
                    }),
                }),
                ..Default::default()
            };
            for (invocation, expected) in [
                (
                    delegated,
                    json!({
                        "channel": "api",
                        "rootChannel": "api",
                        "delegation": {
                            "principal": "gateway",
                            "actor": {"kind": "api_key", "name": "CI key"},
                        },
                    }),
                ),
                (
                    InvocationInfo::default(),
                    json!({"channel": "ui", "rootChannel": "ui", "delegation": null}),
                ),
            ] {
                let result = engine
                    .invoke(
                        &CountingHost(AtomicUsize::new(0)),
                        Invocation {
                            function: "run".into(),
                            kind: FunctionKind::Query,
                            args: b"null".to_vec(),
                            context: InvocationContext {
                                generation: 1,
                                invocation,
                                ..Default::default()
                            },
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&result.value).unwrap(),
                    expected
                );
            }
        });
    }
}

#[cfg(test)]
mod host_wait_tests;
