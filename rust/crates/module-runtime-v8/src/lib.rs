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
//! * a host-call budget and a result byte budget, both checked in Rust;
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gonvex_module_runtime::{
    remaining_budget, validate_portable_schema, validate_portable_schema_definition, BoxFuture,
    FunctionContract, Invocation, InvocationContext, InvocationResult, ModuleArtifact,
    ModuleEngine, ModuleError, ModuleHost, ModuleLanguage, ModuleManifest,
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
    pub max_result_bytes: usize,
    /// Budget for every host operation a call may make, not only database ones.
    pub max_host_calls: usize,
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
            max_result_bytes: 8 * 1024 * 1024,
            max_host_calls: 100,
            recycle_after_calls: 10_000,
            isolate_pool_size: 1,
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
        let timeout = self.call_timeout(&invocation.context)?;
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
                max_host_calls: self.config.max_host_calls,
                host: host_sender,
            },
            reply,
        })?;

        // The isolate runs on its own thread; this task holds the host reference
        // and answers the isolate's host calls until it reports a result. The
        // isolate never sees `host`, so the host never has to be `'static`.
        let mut bridging = true;
        let reply = loop {
            tokio::select! {
                biased;
                reply = &mut replied => break reply,
                request = host_calls.recv(), if bridging => match request {
                    Some(request) => {
                        let response = host.call(&context, request.call).await;
                        let _ = request.reply.send(response);
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
    fn call_timeout(&self, context: &InvocationContext) -> Result<Duration, ModuleError> {
        let Some(deadline) = context.deadline else {
            return Ok(self.config.execution_timeout);
        };
        if deadline <= SystemTime::now() {
            return Err(ModuleError::BudgetExceeded(
                "invocation deadline elapsed before the module started".to_owned(),
            ));
        }
        Ok(remaining_budget(context)
            .unwrap_or(self.config.execution_timeout)
            .min(self.config.execution_timeout))
    }
}
