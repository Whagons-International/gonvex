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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use gonvex_module_host::protocol::{
    codes, HostCallFrame, RequestOp, ResponsePayload, ShutdownRequest, WireError,
};
use gonvex_module_host::session::{handle_local, HostConfig, HostState};
use gonvex_module_runtime::{BoxFuture, HostCall, HostError, HostResponse, InvocationContext};
use gonvex_module_runtime_v8::V8Config;

use crate::config::ModuleHostConfig;
use crate::module_host::{HostCallHandler, InvokeTimes};

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
        }
        .with_scheduling_env(),
    })
}

pub(crate) async fn request(
    state: &Arc<HostState>,
    operation: RequestOp,
    handler: Option<&mut dyn HostCallHandler>,
    label: Option<gonvex_profiler::Label>,
    connect_ns: u64,
) -> Result<ResponsePayload, WireError> {
    let deadline_unix_ms = match &operation {
        RequestOp::Invoke(request) => request.context.deadline_unix_ms,
        _ => None,
    };
    let profile = DirectProfile::new(label);
    match handler {
        Some(handler) => {
            let inline_before = handler.inline_profile();
            let host = DirectHost {
                inline: handler.inline_host(),
                handler: tokio::sync::Mutex::new(handler),
                profile,
            };
            let result = handle_local(state, deadline_unix_ms, operation, Some(&host)).await;
            let handler = host.handler.into_inner();
            let inline_after = handler.inline_profile();
            let inline = (
                inline_after.0.saturating_sub(inline_before.0),
                inline_after.1.saturating_sub(inline_before.1),
            );
            if let Some(times) = host.profile.finish(connect_ns, inline, &result) {
                if result.is_ok() {
                    handler.invoke_profiled(times);
                }
            }
            result
        }
        None => match operation {
            // The socket transport answers an invocation without a dispatcher
            // with a failed host call on first use; keep that behaviour.
            RequestOp::Invoke(_) => {
                let mut none = NoDispatcher;
                let host = DirectHost {
                    inline: None,
                    handler: tokio::sync::Mutex::new(&mut none),
                    profile,
                };
                let result = handle_local(state, deadline_unix_ms, operation, Some(&host)).await;
                host.profile.finish(connect_ns, (0, 0), &result);
                result
            }
            other => handle_local(state, deadline_unix_ms, other, None).await,
        },
    }
}

/// Timing of one in-process invocation, the in-process counterpart of the
/// socket bridge's `InvokeProfile` (module_host.rs). The engine reports when
/// the call leased an isolate (`ModuleHost::isolate_acquired`); host calls
/// are timed where the invoking task answers them (`DirectHost::call`) and,
/// for SQL answered on the isolate thread, by the handler itself
/// (`HostCallHandler::inline_profile`). Relaxed atomics on the request's own
/// stack: no allocation, no lock.
struct DirectProfile {
    label: Option<gonvex_profiler::Label>,
    /// Profiler clock at the start of the request (0: not profiling).
    start: u64,
    acquired: AtomicU64,
    first_call: AtomicU64,
    host_ns: AtomicU64,
    calls: AtomicU64,
    /// Holds `module_host.isolate_waiting` until an isolate is leased.
    waiting: AtomicBool,
}

impl DirectProfile {
    fn new(label: Option<gonvex_profiler::Label>) -> Self {
        let label = label.filter(|_| gonvex_profiler::enabled());
        let waiting = label.is_some();
        if waiting {
            gonvex_profiler::gauge(gonvex_profiler::GaugeId::IsolateWaiting).add(1);
        }
        Self {
            label,
            start: if waiting {
                gonvex_profiler::now_ns()
            } else {
                0
            },
            acquired: AtomicU64::new(0),
            first_call: AtomicU64::new(0),
            host_ns: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            waiting: AtomicBool::new(waiting),
        }
    }

    fn stop_waiting(&self) {
        if self.waiting.swap(false, Relaxed) {
            gonvex_profiler::gauge(gonvex_profiler::GaugeId::IsolateWaiting).add(-1);
        }
    }

    fn isolate_acquired(&self) {
        if self.start != 0 {
            let _ = self
                .acquired
                .compare_exchange(0, gonvex_profiler::now_ns(), Relaxed, Relaxed);
            self.stop_waiting();
        }
    }

    /// Start of one serial host call (the invoking task picked it up).
    fn call_started(&self) -> u64 {
        if self.start == 0 {
            return 0;
        }
        let now = gonvex_profiler::now_ns();
        let _ = self.first_call.compare_exchange(0, now, Relaxed, Relaxed);
        now
    }

    fn call_answered(&self, started: u64) {
        if started != 0 {
            self.host_ns
                .fetch_add(gonvex_profiler::now_ns().saturating_sub(started), Relaxed);
            self.calls.fetch_add(1, Relaxed);
        }
    }

    /// Records the invocation's stages and returns them for the handler.
    /// `inline` is `(calls, ns)` answered on the isolate thread.
    fn finish(
        &self,
        connect: u64,
        inline: (u64, u64),
        result: &Result<ResponsePayload, WireError>,
    ) -> Option<InvokeTimes> {
        use gonvex_profiler::{record, Stage};
        self.stop_waiting();
        let label = self.label?;
        let end = gonvex_profiler::now_ns();
        let acquired = self.acquired.load(Relaxed);
        let isolate_wait = if acquired != 0 {
            acquired.saturating_sub(self.start)
        } else if result
            .as_ref()
            .is_err_and(|error| error.code == codes::BUDGET_EXCEEDED)
        {
            // The deadline passed in root admission or the isolate queue:
            // the whole request was a wait (the 500-user failure mode).
            end.saturating_sub(self.start)
        } else {
            // Failed before reaching the isolate pool (unknown function,
            // invalid arguments, shutting down): not a wait.
            record(Stage::InvokeConnect, label, connect);
            return None;
        };
        record(Stage::InvokeConnect, label, connect);
        record(Stage::InvokeIsolateWait, label, isolate_wait);
        if acquired == 0 {
            return None;
        }
        let first_call = match self.first_call.load(Relaxed) {
            0 => end,
            at => at,
        };
        let first_frame = first_call.saturating_sub(self.start);
        let host_calls = self.host_ns.load(Relaxed) + inline.1;
        let host_call_count = self.calls.load(Relaxed) + inline.0;
        let js = end
            .saturating_sub(self.start)
            .saturating_sub(first_frame)
            .saturating_sub(host_calls);
        record(Stage::InvokeFirstFrame, label, first_frame);
        record(Stage::InvokeJs, label, js);
        record(Stage::InvokeHostCalls, label, host_calls);
        record(Stage::HostCallCount, label, host_call_count);
        Some(InvokeTimes {
            connect,
            first_frame,
            js,
            host_calls,
            host_call_count,
            isolate_wait,
        })
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
    /// Database calls the isolate thread answers itself, on the connection
    /// the invocation's transaction lends it (`host_calls::SqlInline`).
    inline: Option<std::sync::Arc<dyn gonvex_module_runtime::InlineHost>>,
    profile: DirectProfile,
}

impl gonvex_module_runtime::ModuleHost for DirectHost<'_> {
    fn inline(&self) -> Option<std::sync::Arc<dyn gonvex_module_runtime::InlineHost>> {
        self.inline.clone()
    }

    fn isolate_acquired(&self) {
        self.profile.isolate_acquired();
    }

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
                // Overlapping network calls are neither timed nor counted,
                // as on the socket bridge: they run alongside JS.
                Some(work) => work.await.and_then(|value| {
                    serde_json::to_vec(&value)
                        .map_err(|err| format!("host response could not be re-encoded: {err}"))
                }),
                None => {
                    let started = self.profile.call_started();
                    let outcome = self.handler.lock().await.handle_bytes(frame).await;
                    self.profile.call_answered(started);
                    outcome
                }
            };
            match outcome {
                Ok(value) => Ok(HostResponse { value }),
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

    const SQL_CODE: &str = r#"
export const crud = async (ctx, args) => {
  await ctx.db.query("INSERT INTO items (id, label, meta, done) VALUES ($1, $2, $3, $4)", [args.id, "first", {a: 1}, true]);
  const inserted = await ctx.db.insert("items", { id: args.id + "-2", label: "second", meta: {b: [1, 2.5, "x"]}, done: false });
  const rows = await ctx.db.query("SELECT id, label, meta, done, json_object('k', 1) AS expr, 'plain' AS text FROM items ORDER BY id", []);
  const updated = await ctx.db.update("items", args.id, { label: "renamed" });
  const removed = await ctx.db.delete("items", args.id + "-2");
  const none = await ctx.db.query("SELECT id FROM items WHERE id = $1", ["missing"]);
  return { inserted, rows, updated, removed, none };
};
export const recover = async (ctx) => {
  await ctx.db.query("SAVEPOINT s", []);
  await ctx.db.query("INSERT INTO items (id, label) VALUES ('x', 'one')", []);
  let failed = "";
  try { await ctx.db.query("INSERT INTO items (id, label) VALUES ('x', 'dup')", []); } catch (e) { failed = String(e.message); }
  let refused = "";
  try { await ctx.db.query("SELECT 1", []); } catch (e) { refused = String(e.message); }
  await ctx.db.query("ROLLBACK TO s", []);
  const after = await ctx.db.query("SELECT count(*) AS n FROM items", []);
  return { failed, refused, after };
};
export const swallow = async (ctx) => {
  await ctx.db.query("INSERT INTO items (id, label) VALUES ('kept?', 'no')", []);
  try { await ctx.db.query("INSERT INTO items (id, label) VALUES ('y', NULL)", []); } catch {}
  return "ok";
};
"#;

    fn sql_artifact() -> ModuleArtifactWire {
        let mut artifact = artifact();
        artifact.javascript.code = base64::engine::general_purpose::STANDARD.encode(SQL_CODE);
        artifact.javascript.hash = Sha256::digest(SQL_CODE.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        for (function, name) in artifact
            .functions
            .iter_mut()
            .zip(["crud", "recover", "swallow", "spin"])
        {
            function.path = format!("t.{name}");
            function.handler = Some(name.into());
            function.export = Some(name.into());
        }
        artifact.functions.truncate(3);
        artifact
    }

    /// Hides the handler's inline host, so every call takes the async path.
    struct AsyncOnly(crate::host_calls::DatabaseHostCalls);

    #[async_trait::async_trait]
    impl HostCallHandler for AsyncOnly {
        async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
            self.0.handle(call).await
        }
        async fn handle_bytes(&mut self, call: HostCallFrame) -> Result<Vec<u8>, String> {
            self.0.handle_bytes(call).await
        }
    }

    #[test]
    fn module_sql_runs_on_the_isolate_thread_with_the_async_path_results() {
        use crate::host_calls::{DatabaseCapability, DatabaseHostCalls};
        use gonvex_store::{begin_transaction, Durability, PoolLimits, PoolRegistry};
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let url = gonvex_store::database_url(&directory.path().join("tenant.db"));
            let pools = PoolRegistry::with_durability(PoolLimits::default(), Durability::Normal);
            let writer = pools.pool(&url).await.unwrap();
            gonvex_store::execute_script(
                &mut writer.acquire().await.unwrap(),
                "CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT NOT NULL, meta JSON, done BOOLEAN)",
            )
            .await
            .unwrap();
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
                execution_timeout: Duration::from_secs(5),
                action_execution_timeout: Duration::from_secs(5),
            });
            host.start().await.unwrap();
            host.load(LoadRequest {
                module_id: "t".into(),
                generation: Some(1),
                artifact: sql_artifact(),
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
            let calls = || async {
                let transaction = begin_transaction(&pools, &url, false).await.unwrap();
                DatabaseHostCalls::new(transaction, DatabaseCapability::Reducer)
            };

            // Same results whether the isolate answers SQL itself or every
            // call takes the async path.
            let mut inline = calls().await;
            let direct = host
                .invoke(request("crud", json!({"id": "a"})), &mut inline)
                .await
                .unwrap();
            assert!(inline.inline_calls() >= 5, "{}", inline.inline_calls());
            // Host calls answered on the isolate thread count as host calls.
            let (times, _, _) = inline.profile_times();
            assert_eq!(times.host_call_count, 6, "{times:?}");
            assert!(times.host_calls > 0 && times.js > 0, "{times:?}");
            inline.finish(false).await.unwrap();
            let mut asynchronous = AsyncOnly(calls().await);
            let routed = host
                .invoke(request("crud", json!({"id": "a"})), &mut asynchronous)
                .await
                .unwrap();
            assert_eq!(asynchronous.0.inline_calls(), 0);
            asynchronous.0.finish(false).await.unwrap();
            assert_eq!(direct, routed);
            assert_eq!(
                direct,
                json!({
                    "inserted": {"id": "a-2", "label": "second", "meta": {"b": [1, 2.5, "x"]}, "done": false},
                    "rows": [
                        {"id": "a", "label": "first", "meta": {"a": 1}, "done": true, "expr": {"k": 1}, "text": "plain"},
                        {"id": "a-2", "label": "second", "meta": {"b": [1, 2.5, "x"]}, "done": false, "expr": {"k": 1}, "text": "plain"}
                    ],
                    "updated": {"id": "a", "label": "renamed", "meta": {"a": 1}, "done": true},
                    "removed": {"deleted": 1},
                    "none": []
                })
            );

            // A failed statement aborts the transaction until ROLLBACK TO.
            let mut handler = calls().await;
            let value = host
                .invoke(request("recover", json!(null)), &mut handler)
                .await
                .unwrap();
            assert!(
                value["failed"].as_str().unwrap().contains(
                    "db_read host call failed [host_call_failed]: error returned from database: (code: 1555) UNIQUE constraint failed: items.id"
                ),
                "{value}"
            );
            assert!(
                value["refused"]
                    .as_str()
                    .unwrap()
                    .contains("current transaction is aborted"),
                "{value}"
            );
            assert_eq!(value["after"], json!([{"n": 0}]));
            assert_eq!(handler.finish_with_revision(true).await.unwrap(), None);

            // A caught failure never commits.
            let mut handler = calls().await;
            assert_eq!(
                host.invoke(request("swallow", json!(null)), &mut handler)
                    .await
                    .unwrap(),
                json!("ok")
            );
            assert!(handler
                .finish(true)
                .await
                .unwrap_err()
                .contains("current transaction is aborted"));
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM items")
                .fetch_one(&pools.reader(&url).await.unwrap())
                .await
                .unwrap();
            assert_eq!(count, 0);
            host.shutdown().await;
        });
    }

    /// Keeps the invocation timing the transport hands back.
    struct Timed {
        inner: Recorder,
        times: Option<crate::module_host::InvokeTimes>,
    }

    #[async_trait::async_trait]
    impl HostCallHandler for Timed {
        async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
            self.inner.handle(call).await
        }
        fn invoke_profiled(&mut self, times: crate::module_host::InvokeTimes) {
            self.times = Some(times);
        }
    }

    fn stage_count(stage: gonvex_profiler::Stage, label: gonvex_profiler::Label) -> u64 {
        gonvex_profiler::snapshot()
            .into_iter()
            .filter(|row| row.stage == stage && row.label == label)
            .map(|row| row.count)
            .sum()
    }

    #[test]
    fn in_process_invocations_fill_the_invoke_stages_and_isolate_wait() {
        use gonvex_profiler::{Label, Stage};
        run(async {
            gonvex_profiler::set_enabled(true);
            let starts = stage_count(Stage::V8IsolateStart, Label::None);
            let host = host().await;
            assert!(stage_count(Stage::V8IsolateStart, Label::None) > starts);
            let before: Vec<u64> = [
                Stage::InvokeConnect,
                Stage::InvokeIsolateWait,
                Stage::InvokeFirstFrame,
                Stage::InvokeJs,
                Stage::InvokeHostCalls,
                Stage::HostCallCount,
                Stage::InvokeTotal,
            ]
            .iter()
            .map(|stage| stage_count(*stage, Label::Reducer))
            .collect();
            let mut handler = Timed {
                inner: Recorder { calls: Vec::new() },
                times: None,
            };
            host.invoke(request("read", json!({"v": 7})), &mut handler)
                .await
                .unwrap();
            let times = handler
                .times
                .expect("in-process timing reaches the handler");
            assert_eq!(times.host_call_count, 1, "{times:?}");
            assert!(times.first_frame >= times.isolate_wait, "{times:?}");
            assert!(times.host_calls > 0, "{times:?}");
            for (index, stage) in [
                Stage::InvokeConnect,
                Stage::InvokeIsolateWait,
                Stage::InvokeFirstFrame,
                Stage::InvokeJs,
                Stage::InvokeHostCalls,
                Stage::HostCallCount,
                Stage::InvokeTotal,
            ]
            .iter()
            .enumerate()
            {
                assert!(
                    stage_count(*stage, Label::Reducer) > before[index],
                    "{} not recorded",
                    stage.info().name
                );
            }

            // One isolate: a call queued behind a runaway one waits for the
            // runaway's deadline (300 ms) plus a replacement isolate's start.
            let mut spinner = Recorder { calls: Vec::new() };
            let mut queued = Timed {
                inner: Recorder { calls: Vec::new() },
                times: None,
            };
            let spin = host.invoke(request("spin", json!(null)), &mut spinner);
            let read = async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                host.invoke(request("read", json!({"v": 1})), &mut queued)
                    .await
            };
            let (spun, read) = tokio::join!(spin, read);
            assert!(spun.is_err());
            read.unwrap();
            let times = queued.times.unwrap();
            assert!(times.isolate_wait >= 150_000_000, "{times:?}");
            assert!(times.first_frame >= times.isolate_wait, "{times:?}");

            // GC pauses are seen through the prologue/epilogue callbacks.
            let gcs = stage_count(Stage::V8GcMinor, Label::None)
                + stage_count(Stage::V8GcMajor, Label::None);
            let _ = host
                .invoke(
                    request("hog", json!(null)),
                    &mut Recorder { calls: Vec::new() },
                )
                .await;
            let after = stage_count(Stage::V8GcMinor, Label::None)
                + stage_count(Stage::V8GcMajor, Label::None);
            assert!(after > gcs, "no GC pause recorded ({gcs} -> {after})");
            host.shutdown().await;
            // Retired isolates tear down on their own threads.
            let deadline = Instant::now() + Duration::from_secs(5);
            while stage_count(Stage::V8IsolateStop, Label::None) == 0 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(stage_count(Stage::V8IsolateStop, Label::None) > 0);
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
