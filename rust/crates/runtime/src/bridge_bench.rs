//! Module-level benchmark of the Reducer bridge (workstream 10e).
//!
//! Ignored by default. It runs the runtime's own `ModuleHost` client in one of
//! two transports and reports per-invoke latency, host calls per invoke, time
//! spent inside host-call handlers, the remaining bridge + JavaScript time, and
//! writer hold time (first database host call until the transaction ends):
//!
//! - `socket`: the separate `gonvex-module-host` process (`BRIDGE_BENCH_HOST`),
//!   one Unix socket connection per invoke, JSON frames (the baseline);
//! - `in-process`: isolates in this process, host calls as function calls.
//!
//! Two workloads:
//! - `synthetic`: a tiny bundle whose Reducer awaits `n` `ctx.db.query` calls.
//!   With `BRIDGE_BENCH_NULL=1` the handler answers `[]` without SQLite, which
//!   isolates the per-host-call bridge cost.
//! - `whagons`: the real Whagons bundle (`BRIDGE_BENCH_MANIFEST`, a deployed
//!   manifest with its `module.javascript`) against a copy of a lab tenant
//!   database (`BRIDGE_BENCH_DB`), through the real `DatabaseHostCalls`. Each
//!   invoke rolls back so every sample sees the same rows; commit (fsync) cost
//!   is therefore excluded from the hold time and is the same in both modes.
//!
//! `BRIDGE_BENCH_SPLIT=<artifact.json>` replaces the manifest's module with a
//! split artifact (reducer-only bundle, perf/v8-memory). In socket mode the
//! socket path lives in `TMPDIR`, so keep it short (Unix sockets allow ~107
//! bytes). Results in docs/performance/module-host-bridge.md were taken with
//! `taskset -c <cpu>` to model one vCPU.
//!
//! Example:
//!   BRIDGE_BENCH_MODE=in-process BRIDGE_BENCH_MANIFEST=... BRIDGE_BENCH_DB=... \
//!   cargo test --release -p gonvex-runtime bridge_bench -- --ignored --nocapture

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use gonvex_module_host::protocol::{
    ActivateRequest, FunctionWire, HostCallFrame, InvokeRequest, JavaScriptWire, LoadRequest,
    ModuleArtifactWire,
};
use gonvex_store::{Durability, PoolLimits, PoolRegistry};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::ModuleHostConfig;
use crate::host_calls::{DatabaseCapability, DatabaseHostCalls};
use crate::module_host::{HostCallHandler, ModuleHost};

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn in_process() -> bool {
    env("BRIDGE_BENCH_MODE").as_deref() != Some("socket")
}

fn host_config() -> ModuleHostConfig {
    ModuleHostConfig {
        enabled: true,
        in_process: in_process(),
        max_heap_bytes: 64 << 20,
        binary: env("BRIDGE_BENCH_HOST").map(Into::into),
        endpoint: None,
        start_timeout: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(5),
        max_frame_bytes: 64 << 20,
        max_concurrent_calls: 32,
        isolate_pool_size: 1,
        execution_timeout: Duration::from_secs(30),
        action_execution_timeout: Duration::from_secs(60),
    }
}

/// Answers host calls and records what the bridge cannot see: how many there
/// were and how long the handlers themselves took.
struct Probe {
    database: Option<(PoolRegistry, String, Value, String)>,
    inner: Option<DatabaseHostCalls>,
    calls: u32,
    handler: Duration,
    first: Option<Instant>,
    kinds: BTreeMap<&'static str, u32>,
    schedulable: Vec<String>,
    /// Calls answered on the isolate thread (`SqlInline`) once the
    /// transaction is open; counted and timed there.
    inline: Arc<ProbeInline>,
}

#[derive(Default)]
struct ProbeInline {
    target: std::sync::OnceLock<Arc<dyn gonvex_module_runtime::InlineHost>>,
    calls: std::sync::atomic::AtomicU32,
    queries: std::sync::atomic::AtomicU32,
    nanos: std::sync::atomic::AtomicU64,
}

impl gonvex_module_runtime::InlineHost for ProbeInline {
    fn call_inline(
        &self,
        call: gonvex_module_runtime::HostCall,
    ) -> Result<Result<gonvex_module_runtime::HostResponse, gonvex_module_runtime::HostError>, gonvex_module_runtime::HostCall>
    {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(target) = self.target.get() else {
            return Err(call);
        };
        let query = matches!(call, gonvex_module_runtime::HostCall::DbQuery { .. });
        let trace = env("BRIDGE_BENCH_TRACE").is_some().then(|| match &call {
            gonvex_module_runtime::HostCall::DbQuery { statement, .. } => statement.clone(),
            other => other.capability().to_owned(),
        });
        let started = Instant::now();
        let result = if env("BRIDGE_BENCH_INLINE_THREAD").is_some() {
            // Model "one dedicated SQLite thread per database, a job per
            // statement": the isolate thread hands the call over and blocks.
            writer_thread_call(target.clone(), call)?
        } else {
            target.call_inline(call)?
        };
        self.nanos.fetch_add(started.elapsed().as_nanos() as u64, Relaxed);
        if let Some(statement) = trace {
            let bytes = match &result {
                Ok(response) => response.value.len(),
                Err(_) => 0,
            };
            println!(
                "BRIDGE_TRACE {:>8.1}us {:>7}B {}",
                started.elapsed().as_secs_f64() * 1e6,
                bytes,
                statement.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(220).collect::<String>()
            );
        }
        self.calls.fetch_add(1, Relaxed);
        if query {
            self.queries.fetch_add(1, Relaxed);
        }
        Ok(result)
    }
}

type ThreadJob = Box<dyn FnOnce() + Send>;

fn writer_thread_call(
    target: Arc<dyn gonvex_module_runtime::InlineHost>,
    call: gonvex_module_runtime::HostCall,
) -> Result<Result<gonvex_module_runtime::HostResponse, gonvex_module_runtime::HostError>, gonvex_module_runtime::HostCall>
{
    static THREAD: std::sync::OnceLock<std::sync::Mutex<std::sync::mpsc::Sender<ThreadJob>>> =
        std::sync::OnceLock::new();
    let jobs = THREAD.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::channel::<ThreadJob>();
        std::thread::spawn(move || {
            for job in receiver {
                job();
            }
        });
        std::sync::Mutex::new(sender)
    });
    let (reply, answer) = std::sync::mpsc::sync_channel(1);
    jobs.lock()
        .unwrap()
        .send(Box::new(move || {
            let _ = reply.send(target.call_inline(call));
        }))
        .unwrap();
    answer.recv().unwrap()
}

impl Probe {
    fn new(database: Option<(PoolRegistry, String, Value, String)>) -> Self {
        Self {
            database,
            inner: None,
            calls: 0,
            handler: Duration::ZERO,
            first: None,
            kinds: BTreeMap::new(),
            schedulable: Vec::new(),
            inline: Arc::default(),
        }
    }

    /// Host calls and handler time, including those answered inline.
    fn totals(&self) -> (u32, Duration, BTreeMap<&'static str, u32>) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut kinds = self.kinds.clone();
        let queries = self.inline.queries.load(Relaxed);
        let writes = self.inline.calls.load(Relaxed) - queries;
        if queries > 0 {
            *kinds.entry("dbQuery").or_default() += queries;
        }
        if writes > 0 {
            *kinds.entry("dbWrite(inline)").or_default() += writes;
        }
        (
            self.calls + self.inline.calls.load(Relaxed),
            self.handler + Duration::from_nanos(self.inline.nanos.load(Relaxed)),
            kinds,
        )
    }

    /// Ends the transaction; returns the writer hold time.
    async fn finish(self) -> Option<Duration> {
        let first = self.first?;
        if let Some(inner) = self.inner {
            inner.finish(false).await.unwrap();
        }
        Some(first.elapsed())
    }
}

fn kind(call: &HostCallFrame) -> &'static str {
    match call {
        HostCallFrame::DbQuery { .. } => "dbQuery",
        HostCallFrame::DbInsert { .. } => "dbInsert",
        HostCallFrame::DbUpdate { .. } => "dbUpdate",
        HostCallFrame::DbDelete { .. } => "dbDelete",
        HostCallFrame::DbDeleteMany { .. } => "dbDeleteMany",
        HostCallFrame::ActionEnqueue { .. } => "actionEnqueue",
        HostCallFrame::ScheduleAfter { .. } | HostCallFrame::ScheduleAt { .. } => "schedule",
        _ => "other",
    }
}

#[async_trait::async_trait]
impl HostCallHandler for Probe {
    async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
        let started = Instant::now();
        self.calls += 1;
        *self.kinds.entry(kind(&call)).or_default() += 1;
        let result = match &self.database {
            None => Ok(match call {
                HostCallFrame::DbQuery { .. } => json!([]),
                _ => Value::Null,
            }),
            Some((pools, url, schema, command)) => {
                if self.first.is_none() {
                    self.first = Some(started);
                    let transaction = gonvex_store::begin_transaction(pools, url, false)
                        .await
                        .map_err(|error| error.to_string())?;
                    let inner = DatabaseHostCalls::new(transaction, DatabaseCapability::Reducer)
                        .with_schema(schema)
                        .with_intent(TENANT, command)
                        .with_actor("bench-account", "bench@example.test")
                        .with_schedulable_functions(self.schedulable.clone());
                    if env("BRIDGE_BENCH_NO_INLINE").is_none() {
                        if let Some(target) = inner.inline_host() {
                            let _ = self.inline.target.set(target);
                        }
                    }
                    self.inner = Some(inner);
                }
                match self.inner.as_mut() {
                    Some(inner) => inner.handle(call).await,
                    None => Err("no transaction".to_owned()),
                }
            }
        };
        if self.database.is_none() && self.first.is_none() {
            self.first = Some(started);
        }
        self.handler += started.elapsed();
        result
    }

    async fn handle_bytes(&mut self, call: HostCallFrame) -> Result<Vec<u8>, String> {
        if self.database.is_none() {
            let value = self.handle(call).await?;
            return Ok(serde_json::to_vec(&value).unwrap());
        }
        // Same accounting as `handle`, but keep the handler's own bytes.
        let started = Instant::now();
        self.calls += 1;
        *self.kinds.entry(kind(&call)).or_default() += 1;
        let (pools, url, schema, command) = self.database.clone().unwrap();
        if self.first.is_none() {
            self.first = Some(started);
            let transaction = gonvex_store::begin_transaction(&pools, &url, false)
                .await
                .map_err(|error| error.to_string())?;
            let inner = DatabaseHostCalls::new(transaction, DatabaseCapability::Reducer)
                .with_schema(&schema)
                .with_intent(TENANT, &command)
                .with_actor("bench-account", "bench@example.test")
                .with_schedulable_functions(self.schedulable.clone());
            if env("BRIDGE_BENCH_NO_INLINE").is_none() {
                if let Some(target) = inner.inline_host() {
                    let _ = self.inline.target.set(target);
                }
            }
            self.inner = Some(inner);
        }
        let result = match self.inner.as_mut() {
            Some(inner) => inner.handle_bytes(call).await,
            None => Err("no transaction".to_owned()),
        };
        self.handler += started.elapsed();
        result
    }

    fn inline_host(&self) -> Option<Arc<dyn gonvex_module_runtime::InlineHost>> {
        Some(self.inline.clone())
    }
}

const TENANT: &str = "8ec9f43e-7adb-4105-af75-1e5dc27e185a";

fn context(command: &str, member: &Value, artifact_hash: &str) -> Value {
    json!({
        "projectId": "bench",
        "tenantId": TENANT,
        "tenant": {"id": TENANT, "projectId": "bench"},
        "account": {"id": member["accountId"]},
        "member": member,
        "permissions": member["permissions"],
        "invocation": {
            "channel": "ui", "rootChannel": "ui", "rootCommandId": command,
            "commandId": command, "artifactHash": artifact_hash,
            "actorAccountId": member["accountId"], "actorMemberId": member["id"],
        },
        "capabilities": {"dbRead": true, "dbWrite": true, "actionOutbox": true,
                         "scheduler": true, "functions": true},
        "intentEntropy": hex(&Sha256::digest(command.as_bytes())),
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn percentile(values: &mut [f64], q: f64) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let index = ((q * values.len() as f64).ceil() as usize).clamp(1, values.len()) - 1;
    values[index]
}

#[derive(Default)]
struct Samples {
    total: Vec<f64>,
    handler: Vec<f64>,
    hold: Vec<f64>,
    calls: u32,
    kinds: BTreeMap<&'static str, u32>,
}

impl Samples {
    fn report(&mut self, name: &str) -> Value {
        let mean = |values: &[f64]| values.iter().sum::<f64>() / values.len().max(1) as f64;
        let bridge: Vec<f64> = self
            .total
            .iter()
            .zip(&self.handler)
            .map(|(total, handler)| total - handler)
            .collect();
        let calls = f64::from(self.calls.max(1));
        let report = json!({
            "function": name,
            "mode": if in_process() { "in-process" } else { "socket" },
            "samples": self.total.len(),
            "hostCallsPerInvoke": self.calls,
            "hostCallKinds": self.kinds,
            "invokeMs": {"p50": percentile(&mut self.total.clone(), 0.5), "p95": percentile(&mut self.total.clone(), 0.95), "p99": percentile(&mut self.total.clone(), 0.99), "mean": mean(&self.total)},
            "handlerMsMean": mean(&self.handler),
            "bridgeAndJsMsMean": mean(&bridge),
            "bridgeAndJsUsPerHostCall": mean(&bridge) * 1000.0 / calls,
            "writerHoldMs": if self.hold.is_empty() { Value::Null } else { json!({"p50": percentile(&mut self.hold.clone(), 0.5), "p95": percentile(&mut self.hold.clone(), 0.95), "mean": mean(&self.hold)}) },
        });
        println!("BRIDGE_BENCH {}", report);
        report
    }
}

fn stat_cpu(path: &str) -> (u64, u64) {
    // (utime + stime ticks, parent pid)
    let stat = std::fs::read_to_string(path).unwrap_or_default();
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let number = |index: usize| {
        fields
            .get(index)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (number(11) + number(12), number(1))
}

/// CPU of this process plus its live children (the socket mode's module host
/// process), so both transports are charged for all the work they cause.
fn cpu_time() -> Duration {
    let me = std::process::id() as u64;
    let mut ticks = stat_cpu("/proc/self/stat").0;
    for entry in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        let name = entry.file_name();
        if name.to_string_lossy().chars().all(|c| c.is_ascii_digit()) {
            let (cpu, parent) = stat_cpu(&format!("/proc/{}/stat", name.to_string_lossy()));
            if parent == me {
                ticks += cpu;
            }
        }
    }
    Duration::from_millis(ticks * 10)
}

fn child_rss_kb() -> u64 {
    let me = std::process::id() as u64;
    let mut total = 0;
    for entry in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.chars().all(|c| c.is_ascii_digit())
            && stat_cpu(&format!("/proc/{name}/stat")).1 == me
        {
            total += std::fs::read_to_string(format!("/proc/{name}/status"))
                .unwrap_or_default()
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next()?.parse::<u64>().ok())
                .unwrap_or(0);
        }
    }
    total
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

async fn start(host: &ModuleHost, artifact: ModuleArtifactWire) {
    host.start().await.unwrap();
    host.load(LoadRequest {
        module_id: "bench".into(),
        generation: Some(1),
        artifact,
    })
    .await
    .unwrap();
    host.activate(ActivateRequest {
        module_id: "bench".into(),
        generation: 1,
        drain_ms: None,
    })
    .await
    .unwrap();
}

fn invoke_request(
    function: &str,
    args: &Value,
    command: &str,
    member: &Value,
    hash: &str,
) -> InvokeRequest {
    serde_json::from_value(json!({
        "moduleId": "bench",
        "function": function,
        "kind": "reducer",
        "args": args.to_string(),
        "context": context(command, member, hash),
    }))
    .unwrap()
}

/// One Tokio worker, as the production runtime has on a 1-CPU container, on
/// the persistent V8 parent thread shared with the in-process tests.
struct BenchRuntime;

impl BenchRuntime {
    fn block_on(self, test: impl std::future::Future<Output = ()> + Send + 'static) {
        crate::in_process::tests::run(test);
    }
}

fn runtime() -> BenchRuntime {
    BenchRuntime
}

#[test]
#[ignore = "benchmark; see module docs"]
fn bridge_bench_synthetic() {
    runtime().block_on(async {
        let code = br#"
export const loop = async (ctx, args) => {
  let rows = 0;
  for (let i = 0; i < args.n; i++) rows += (await ctx.db.query("SELECT _id FROM tasks WHERE _id = $1", [args.id])).length;
  return rows;
};
"#;
        let artifact = ModuleArtifactWire {
            language: "typescript".into(),
            entrypoint: "bench.js".into(),
            hash: String::new(),
            javascript: JavaScriptWire {
                path: "bench.js".into(),
                hash: hex(&Sha256::digest(code)),
                code: base64::engine::general_purpose::STANDARD.encode(code),
            },
            functions: vec![FunctionWire {
                path: "bench.loop".into(),
                kind: "reducer".into(),
                internal: false,
                delivery: None,
                handler: Some("loop".into()),
                export: Some("loop".into()),
                file: None,
                args: Some(json!({"kind":"any"})),
                result: Some(json!({"kind":"any"})),
                metadata: Default::default(),
            }],
            metadata: Default::default(),
            reducer_bundle: None,
        };
        let host = ModuleHost::new(host_config());
        start(&host, artifact).await;
        let database = database().await;
        let task: String = match &database {
            Some((pools, url, ..)) => {
                let pool = pools.reader(url).await.unwrap();
                sqlx::query_scalar("SELECT _id FROM tasks LIMIT 1").fetch_one(&pool).await.unwrap()
            }
            None => "none".into(),
        };
        let member = json!({"id": "m", "accountId": "a", "status": "active", "permissions": {}});
        let iterations: usize = env("BRIDGE_BENCH_CALLS").and_then(|v| v.parse().ok()).unwrap_or(300);
        for n in [1usize, 30] {
            let mut samples = Samples::default();
            for i in 0..(iterations + 20) {
                let command = format!("synthetic-{n}-{i}");
                let mut probe = Probe::new(database.as_ref().map(|(p, u, s, _)| (p.clone(), u.clone(), s.clone(), command.clone())));
                let request = invoke_request("bench.loop", &json!({"n": n, "id": task}), &command, &member, "");
                let started = Instant::now();
                host.invoke(request, &mut probe).await.unwrap();
                let total = started.elapsed();
                let (calls, handler, kinds) = probe.totals();
                let hold = probe.finish().await;
                if i < 20 {
                    continue;
                }
                samples.total.push(total.as_secs_f64() * 1000.0);
                samples.handler.push(handler.as_secs_f64() * 1000.0);
                if let Some(hold) = hold {
                    samples.hold.push(hold.as_secs_f64() * 1000.0);
                }
                samples.calls = calls;
                samples.kinds = kinds;
            }
            samples.report(&format!("synthetic.query x{n}"));
        }
        host.shutdown().await;
    });
}

async fn database() -> Option<(PoolRegistry, String, Value, String)> {
    if env("BRIDGE_BENCH_NULL").is_some() {
        return None;
    }
    let source = env("BRIDGE_BENCH_DB")?;
    let directory = tempfile::tempdir().unwrap().keep();
    let copy = directory.join("tenant.db");
    std::fs::copy(&source, &copy).unwrap();
    let url = gonvex_store::database_url(&copy);
    let pools = PoolRegistry::with_durability(PoolLimits::default(), Durability::Full);
    let schema = env("BRIDGE_BENCH_MANIFEST")
        .map(|path| {
            let manifest: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            manifest.get("schema").cloned().unwrap_or(Value::Null)
        })
        .unwrap_or(Value::Null);
    Some((pools, url, schema, String::new()))
}

fn whagons_artifact(manifest: &Value) -> ModuleArtifactWire {
    let module = &manifest["module"];
    let mut functions = Vec::new();
    for (path, function) in module["functions"].as_object().unwrap() {
        let mut metadata = serde_json::Map::new();
        for name in [
            "public",
            "actionProfile",
            "offline",
            "optimistic",
            "interactive",
            "classification",
            "description",
            "agent",
            "actionCapabilities",
        ] {
            if let Some(value) = function.get(name) {
                metadata.insert(name.into(), value.clone());
            }
        }
        let text = |name: &str| {
            function
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        functions.push(FunctionWire {
            path: path.clone(),
            kind: text("kind").unwrap(),
            internal: function
                .get("internal")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            delivery: text("delivery"),
            handler: text("handler"),
            export: text("export"),
            file: text("file"),
            args: function.get("args").cloned(),
            result: function.get("result").cloned(),
            metadata,
        });
    }
    ModuleArtifactWire {
        language: module["language"].as_str().unwrap_or("typescript").into(),
        entrypoint: module["entrypoint"].as_str().unwrap_or_default().into(),
        hash: module["hash"].as_str().unwrap_or_default().into(),
        javascript: serde_json::from_value(module["javascript"].clone()).unwrap(),
        functions,
        metadata: Default::default(),
        // A split artifact (perf/v8-memory) serves Queries and Reducers from
        // the reducer-only bundle.
        reducer_bundle: module
            .get("reducerBundle")
            .filter(|_| env("BRIDGE_BENCH_NO_SPLIT").is_none())
            .map(|bundle| serde_json::from_value(bundle.clone()).unwrap()),
    }
}

#[test]
#[ignore = "benchmark; see module docs"]
fn bridge_bench_whagons() {
    runtime().block_on(async {
        let manifest_path = env("BRIDGE_BENCH_MANIFEST").expect("BRIDGE_BENCH_MANIFEST");
        let mut manifest: Value = serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        if let Some(split) = env("BRIDGE_BENCH_SPLIT") {
            let artifact: Value = serde_json::from_slice(&std::fs::read(split).unwrap()).unwrap();
            manifest["module"] = artifact["module"].clone();
        }
        let hash = manifest["module"]["hash"].as_str().unwrap_or_default().to_owned();
        let schedulable: Vec<String> = manifest["module"]["functions"]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(_, f)| matches!(f["kind"].as_str(), Some("reducer" | "action")))
            .map(|(path, _)| path.clone())
            .collect();
        let rss_before = rss_kb();
        let host = ModuleHost::new(host_config());
        let load_started = Instant::now();
        start(&host, whagons_artifact(&manifest)).await;
        let load_ms = load_started.elapsed().as_secs_f64() * 1000.0;
        drop(manifest);
        let rss_loaded = rss_kb();
        let (pools, url, schema, _) = database().await.expect("BRIDGE_BENCH_DB");
        let reader = pools.reader(&url).await.unwrap();
        let (member_id, account_id, role, permissions): (String, String, String, Option<String>) = sqlx::query_as(
            "SELECT id, account_id, role, permissions FROM members WHERE status='active' AND role='admin' ORDER BY id LIMIT 1",
        )
        .fetch_one(&reader)
        .await
        .unwrap();
        let permissions: Value = permissions
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_else(|| json!({}));
        let member = json!({"id": member_id, "accountId": account_id, "status": "active", "role": role, "permissions": permissions});
        let tasks: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT _id, workspaceId, categoryId, priorityId FROM tasks WHERE deletedAt IS NULL ORDER BY _id LIMIT 200",
        )
        .fetch_all(&reader)
        .await
        .unwrap();
        drop(reader);
        let iterations: usize = env("BRIDGE_BENCH_CALLS").and_then(|v| v.parse().ok()).unwrap_or(200);
        type Args = Box<dyn Fn(usize) -> Value + Send + Sync>;
        let workloads: Vec<(&str, Args)> = vec![
            ("tasks.update", Box::new({ let tasks = tasks.clone(); move |i| json!({"taskId": tasks[i % tasks.len()].0, "updates": {"dueDate": 1_800_000_000_000i64 + i as i64 * 86_400_000}}) })),
            ("tasks.updateDescription", Box::new({ let tasks = tasks.clone(); move |i| json!({"taskId": tasks[i % tasks.len()].0, "description": format!("bench description {i}")}) })),
            ("taskResources.createNote", Box::new({ let tasks = tasks.clone(); let member = member["id"].clone(); move |i| json!({"entity": {"_id": uuid::Uuid::new_v4().to_string(), "_creationTime": 1_793_000_000_000i64, "taskId": tasks[i % tasks.len()].0, "userId": member, "source": "web", "note": format!("bench note {i}")}}) })),
            ("tasks.create", Box::new({ let tasks = tasks.clone(); move |i| { let t = &tasks[i % tasks.len()]; json!({"entityId": uuid::Uuid::new_v4().to_string(), "name": format!("bench task {i}"), "workspaceId": t.1, "categoryId": t.2, "priorityId": t.3}) } })),
        ];
        let mut reports = Vec::new();
        for (function, args) in &workloads {
            let mut samples = Samples::default();
            let cpu_started = cpu_time();
            let mut failures = 0;
            for i in 0..(iterations + 20) {
                let command = format!("cmd-{function}-{i}");
                let mut probe = Probe::new(Some((pools.clone(), url.clone(), schema.clone(), command.clone())));
                probe.schedulable = schedulable.clone();
                let request = invoke_request(function, &args(i), &command, &member, &hash);
                let started = Instant::now();
                let result = host.invoke(request, &mut probe).await;
                let total = started.elapsed();
                let (calls, handler, kinds) = probe.totals();
                let hold = probe.finish().await;
                if let Err(error) = result {
                    failures += 1;
                    if failures <= 3 {
                        println!("BRIDGE_BENCH_ERROR {function}: {error}");
                    }
                    continue;
                }
                if i < 20 {
                    continue;
                }
                samples.total.push(total.as_secs_f64() * 1000.0);
                samples.handler.push(handler.as_secs_f64() * 1000.0);
                if let Some(hold) = hold {
                    samples.hold.push(hold.as_secs_f64() * 1000.0);
                }
                samples.calls = calls;
                samples.kinds = kinds;
            }
            let cpu = cpu_time() - cpu_started;
            if samples.total.is_empty() {
                continue;
            }
            let mut report = samples.report(function);
            report["cpuMsPerInvokeRuntimePlusHost"] = json!(cpu.as_secs_f64() * 1000.0 / (iterations + 20) as f64);
            report["failures"] = json!(failures);
            reports.push(report);
        }
        let statements = pools.writer_statement_stats(&url).await.unwrap();
        let summary = json!({
            "writerStatements": {"cached": statements.cached, "prepared": statements.prepared, "reused": statements.reused},
            "mode": if in_process() { "in-process" } else { "socket" },
            "loadMs": load_ms,
            "runtimeRssKb": {"beforeLoad": rss_before, "afterLoad": rss_loaded, "afterCalls": rss_kb()},
            "moduleHostChildRssKb": child_rss_kb(),
            "reports": reports,
        });
        println!("BRIDGE_BENCH_SUMMARY {summary}");
        if let Some(path) = env("BRIDGE_BENCH_OUT") {
            std::fs::write(path, serde_json::to_vec_pretty(&summary).unwrap()).unwrap();
        }
        host.shutdown().await;
        let _ = Arc::new(());
    });
}
