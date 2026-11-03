//! A fake module host: a Unix-socket server speaking module-host protocol v2
//! (`gonvex_module_host::protocol`), with reducers scripted in Rust instead of
//! V8. `Runtime::start()` talks to it through `module_host.endpoint`, so the
//! runtime's real client (`module_host.rs`: one connection per request, JSON
//! frames, host calls answered while the writer is held) is what gets
//! measured. Simulated JavaScript time is configurable at run time.
//!
//! The server runs on its own thread with its own current-thread tokio
//! runtime, like the separate module-host process in production, so it does
//! not compete with the runtime's workers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use gonvex_module_host::framing::{read_frame, write_frame};
use gonvex_module_host::protocol::{
    codes, ClientFrame, FunctionSummary, HostCallFrame, InvokeRequest, RequestOp, ResponsePayload,
    ServerFrame, WireError, PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use tokio::net::{UnixListener, UnixStream};

/// How simulated JavaScript time is spent: `Spin` burns CPU on the host
/// thread (like V8 executing), `Sleep` only waits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsTimeMode {
    Spin,
    Sleep,
}

/// One scripted module function. It may issue host calls (`ctx.db.*`)
/// through [`Invocation::host`] exactly as the real SDK does.
#[async_trait]
pub trait ScriptedFunction: Send + Sync {
    async fn run(&self, invocation: &mut Invocation<'_>) -> Result<Value, String>;
}

/// The running invocation as a scripted function sees it.
pub struct Invocation<'a> {
    pub function: String,
    pub args: Value,
    pub member_id: Option<String>,
    pub now_unix_ms: u64,
    request_id: u64,
    next_host_call: u64,
    stream: &'a mut UnixStream,
    max_frame_bytes: usize,
    /// Host calls issued so far by this invocation.
    pub host_calls: usize,
}

impl Invocation<'_> {
    /// Issues one host call and waits for the runtime's answer.
    pub async fn host(&mut self, payload: HostCallFrame) -> Result<Value, String> {
        self.next_host_call += 1;
        self.host_calls += 1;
        let id = self.next_host_call;
        send(
            self.stream,
            &ServerFrame::HostCall {
                id,
                invocation: self.request_id,
                payload,
            },
            self.max_frame_bytes,
        )
        .await
        .map_err(|error| error.to_string())?;
        loop {
            let frame = read_frame(self.stream, self.max_frame_bytes)
                .await
                .map_err(|error| error.to_string())?;
            match serde_json::from_slice::<ClientFrame>(&frame).map_err(|e| e.to_string())? {
                ClientFrame::HostResponse {
                    id: answered,
                    value,
                } if answered == id => return Ok(value),
                ClientFrame::HostError {
                    id: answered,
                    error,
                } if answered == id => return Err(format!("{}: {}", error.code, error.message)),
                ClientFrame::Cancel { .. } => return Err("cancelled".to_owned()),
                _ => continue,
            }
        }
    }

    /// `ctx.db.query(statement, parameters)`.
    pub async fn query(
        &mut self,
        statement: &str,
        parameters: Value,
    ) -> Result<Vec<Value>, String> {
        let value = self
            .host(HostCallFrame::DbQuery {
                statement: statement.to_owned(),
                parameters,
            })
            .await?;
        Ok(match value {
            Value::Array(rows) => rows,
            Value::Object(mut object) => match object.remove("rows") {
                Some(Value::Array(rows)) => rows,
                _ => Vec::new(),
            },
            _ => Vec::new(),
        })
    }

    /// `ctx.db.update(table, id, patch)` (the SDK leaves `key` empty: the
    /// runtime resolves the primary key).
    pub async fn update(&mut self, table: &str, id: Value, patch: Value) -> Result<Value, String> {
        self.host(HostCallFrame::DbUpdate {
            table: table.to_owned(),
            key: String::new(),
            id,
            patch,
        })
        .await
    }

    /// `ctx.db.insert(table, row)`.
    pub async fn insert(&mut self, table: &str, row: Value) -> Result<Value, String> {
        self.host(HostCallFrame::DbInsert {
            table: table.to_owned(),
            row,
            generated_id: None,
        })
        .await
    }

    /// `ctx.db.delete(table, id)`.
    pub async fn delete(&mut self, table: &str, id: Value) -> Result<Value, String> {
        self.host(HostCallFrame::DbDelete {
            table: table.to_owned(),
            key: String::new(),
            id,
        })
        .await
    }

    fn member(&self) -> Result<String, String> {
        self.member_id
            .clone()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| "an active member is required".to_owned())
    }
}

/// Simulated per-invoke JavaScript time and counters, shared with the host thread.
#[derive(Default)]
struct HostState {
    js_time: Duration,
    js_mode: Option<JsTimeMode>,
}

/// Handle to the running fake host. Dropping it stops the server thread.
pub struct FakeModuleHost {
    socket: PathBuf,
    state: Arc<Mutex<HostState>>,
    invocations: Arc<AtomicU64>,
    host_calls: Arc<AtomicU64>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FakeModuleHost {
    /// Starts the server at `socket` with `functions` (path -> script).
    pub fn start(
        socket: &Path,
        functions: BTreeMap<String, Arc<dyn ScriptedFunction>>,
    ) -> std::io::Result<Self> {
        let _ = std::fs::remove_file(socket);
        let state = Arc::new(Mutex::new(HostState::default()));
        let invocations = Arc::new(AtomicU64::new(0));
        let host_calls = Arc::new(AtomicU64::new(0));
        let (shutdown, stop) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let server = Server {
            functions: Arc::new(functions),
            state: state.clone(),
            invocations: invocations.clone(),
            host_calls: host_calls.clone(),
            generation: Arc::new(AtomicU64::new(0)),
            max_frame_bytes: 64 << 20,
        };
        let path = socket.to_path_buf();
        let thread = std::thread::Builder::new()
            .name("fake-module-host".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("fake module host runtime");
                runtime.block_on(async move {
                    let listener = match UnixListener::bind(&path) {
                        Ok(listener) => {
                            let _ = ready_tx.send(Ok(()));
                            listener
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return;
                        }
                    };
                    tokio::pin!(stop);
                    loop {
                        tokio::select! {
                            _ = &mut stop => break,
                            accepted = listener.accept() => {
                                let Ok((stream, _)) = accepted else { continue };
                                let server = server.clone();
                                tokio::spawn(async move { server.connection(stream).await });
                            }
                        }
                    }
                });
            })?;
        ready_rx
            .recv()
            .map_err(|_| std::io::Error::other("fake module host thread exited"))??;
        Ok(Self {
            socket: socket.to_path_buf(),
            state,
            invocations,
            host_calls,
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }

    /// `module_host.endpoint` value for the runtime config.
    pub fn endpoint(&self) -> String {
        format!("unix:{}", self.socket.display())
    }

    /// Simulated JavaScript time spent in every invoke (default zero).
    pub fn set_js_time(&self, time: Duration, mode: JsTimeMode) {
        let mut state = self.state.lock().unwrap();
        state.js_time = time;
        state.js_mode = Some(mode);
    }

    /// Invokes served so far.
    pub fn invocations(&self) -> u64 {
        self.invocations.load(Ordering::Relaxed)
    }

    /// Host calls issued by scripted functions so far.
    pub fn host_calls(&self) -> u64 {
        self.host_calls.load(Ordering::Relaxed)
    }
}

impl Drop for FakeModuleHost {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(&self.socket);
    }
}

#[derive(Clone)]
struct Server {
    functions: Arc<BTreeMap<String, Arc<dyn ScriptedFunction>>>,
    state: Arc<Mutex<HostState>>,
    invocations: Arc<AtomicU64>,
    host_calls: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    max_frame_bytes: usize,
}

async fn send(
    stream: &mut UnixStream,
    frame: &ServerFrame,
    max_frame_bytes: usize,
) -> Result<(), gonvex_module_host::framing::FrameError> {
    let bytes = serde_json::to_vec(frame).expect("server frames serialize");
    write_frame(stream, &bytes, max_frame_bytes).await
}

impl Server {
    async fn connection(self, mut stream: UnixStream) {
        let ready = ServerFrame::Ready {
            protocol: PROTOCOL_VERSION,
            version: "perf-fixture".to_owned(),
        };
        if send(&mut stream, &ready, self.max_frame_bytes)
            .await
            .is_err()
        {
            return;
        }
        while let Ok(frame) = read_frame(&mut stream, self.max_frame_bytes).await {
            let Ok(ClientFrame::Request { id, payload, .. }) =
                serde_json::from_slice::<ClientFrame>(&frame)
            else {
                continue;
            };
            let reply = match payload {
                RequestOp::Ping => Ok(ResponsePayload::Pong {
                    protocol: PROTOCOL_VERSION,
                    version: "perf-fixture".to_owned(),
                }),
                RequestOp::Load(load) => Ok(ResponsePayload::Loaded {
                    module_id: load.module_id,
                    generation: self.generation.fetch_add(1, Ordering::Relaxed) + 1,
                    functions: load
                        .artifact
                        .functions
                        .iter()
                        .map(|function| FunctionSummary {
                            path: function.path.clone(),
                            kind: function.kind.clone(),
                            internal: function.internal,
                            delivery: function.delivery.clone(),
                        })
                        .collect(),
                }),
                RequestOp::Activate(activate) => Ok(ResponsePayload::Activated {
                    module_id: activate.module_id,
                    generation: activate.generation,
                    retired: None,
                }),
                RequestOp::Invoke(invoke) => self.invoke(&mut stream, id, invoke).await,
                _ => Err(WireError::new(
                    codes::UNSUPPORTED,
                    "unsupported by the fake host",
                )),
            };
            let frame = match reply {
                Ok(payload) => ServerFrame::Response { id, payload },
                Err(error) => ServerFrame::Error { id, error },
            };
            if send(&mut stream, &frame, self.max_frame_bytes)
                .await
                .is_err()
            {
                return;
            }
        }
    }

    async fn invoke(
        &self,
        stream: &mut UnixStream,
        id: u64,
        request: InvokeRequest,
    ) -> Result<ResponsePayload, WireError> {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        let Some(function) = self.functions.get(&request.function).cloned() else {
            return Err(WireError::new(
                codes::FUNCTION_NOT_FOUND,
                format!("fake host has no script for {:?}", request.function),
            ));
        };
        let (js_time, js_mode) = {
            let state = self.state.lock().unwrap();
            (state.js_time, state.js_mode.unwrap_or(JsTimeMode::Spin))
        };
        if !js_time.is_zero() {
            match js_mode {
                JsTimeMode::Spin => {
                    let started = Instant::now();
                    while started.elapsed() < js_time {
                        std::hint::spin_loop();
                    }
                }
                JsTimeMode::Sleep => tokio::time::sleep(js_time).await,
            }
        }
        let args = if request.args.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&request.args)
                .map_err(|error| WireError::new(codes::INVALID_ARGS, error.to_string()))?
        };
        let mut invocation = Invocation {
            function: request.function.clone(),
            args,
            member_id: request
                .context
                .member
                .as_ref()
                .map(|member| member.id.clone()),
            now_unix_ms: match request.context.now_unix_ms {
                0 => gonvex_module_host::protocol::unix_millis(std::time::SystemTime::now()),
                now => now,
            },
            request_id: id,
            next_host_call: 0,
            stream,
            max_frame_bytes: self.max_frame_bytes,
            host_calls: 0,
        };
        let result = function.run(&mut invocation).await;
        self.host_calls
            .fetch_add(invocation.host_calls as u64, Ordering::Relaxed);
        match result {
            Ok(value) => Ok(ResponsePayload::Invoked {
                value: serde_json::to_string(&value).expect("JSON values serialize"),
            }),
            Err(message) => Err(WireError::new(codes::EXECUTION_FAILED, message)),
        }
    }
}

/// `presence.beat` / `presence.transition` as Whagons `gonvex/presence.ts`
/// runs them on the server: one `selectRows` query (SDK-compiled SQL), then
/// `ctx.db.update` (or `insert` for a member without a presence row).
pub struct Presence {
    /// `false` is `presence.beat` (state "active", returns `ctx.now`).
    pub transition: bool,
}

#[async_trait]
impl ScriptedFunction for Presence {
    async fn run(&self, call: &mut Invocation<'_>) -> Result<Value, String> {
        let member = call.member()?;
        let state = if self.transition {
            call.args
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("active")
                .trim()
                .to_lowercase()
        } else {
            "active".to_owned()
        };
        let rows = call
            .query(
                r#"SELECT "_id" FROM "userPresence" WHERE "userId" = $1 LIMIT $2"#,
                json!([member, 1]),
            )
            .await?;
        let id = rows
            .first()
            .and_then(|row| row.get("_id"))
            .cloned()
            .filter(|id| !id.is_null());
        let now = call.now_unix_ms;
        if self.transition && state == "gone" {
            if let Some(id) = id.clone() {
                call.delete("userPresence", id).await?;
            }
            return Ok(id.unwrap_or(json!("")));
        }
        match id.clone() {
            Some(id) => {
                call.update(
                    "userPresence",
                    id,
                    json!({"lastSeenAt": now, "state": state}),
                )
                .await?;
            }
            None => {
                call.insert(
                    "userPresence",
                    json!({"_id": uuid::Uuid::new_v4().to_string(), "userId": member,
                           "lastSeenAt": now, "state": state}),
                )
                .await?;
            }
        }
        Ok(if self.transition {
            id.unwrap_or(json!(""))
        } else {
            json!(now)
        })
    }
}

/// A Whagons-style task update (`tasks.updateDescription`, `tasks.update`):
/// read the task, the caller's teams and the task's status, then one
/// `ctx.db.update` with the patch and `updatedAt`. `extra_reads` adds more
/// task-sized reads to model heavier reducers.
pub struct TaskUpdate {
    pub extra_reads: usize,
}

#[async_trait]
impl ScriptedFunction for TaskUpdate {
    async fn run(&self, call: &mut Invocation<'_>) -> Result<Value, String> {
        let member = call.member()?;
        let task_id = call
            .args
            .get("taskId")
            .and_then(Value::as_str)
            .ok_or("taskId is required")?
            .to_owned();
        let mut patch = match call.args.get("updates") {
            Some(Value::Object(updates)) => updates.clone(),
            _ => serde_json::Map::new(),
        };
        if let Some(description) = call.args.get("description") {
            patch.insert("description".into(), description.clone());
        }
        let task = call
            .query(
                r#"SELECT * FROM "tasks" WHERE "_id" = $1 LIMIT $2"#,
                json!([task_id, 1]),
            )
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| format!("task {task_id} not found"))?;
        call.query(
            r#"SELECT "teamId" FROM "memberTeams" WHERE "memberId" = $1 AND "deletedAt" IS NULL"#,
            json!([member]),
        )
        .await?;
        let status = task.get("statusId").cloned().unwrap_or(Value::Null);
        call.query(
            r#"SELECT "_id", "action", "final" FROM "statuses" WHERE "_id" = $1 LIMIT $2"#,
            json!([status, 1]),
        )
        .await?;
        for _ in 0..self.extra_reads {
            call.query(
                r#"SELECT * FROM "tasks" WHERE "_id" = $1 LIMIT $2"#,
                json!([task_id, 1]),
            )
            .await?;
        }
        patch.insert("updatedAt".into(), json!(call.now_unix_ms));
        let updated = call
            .update("tasks", json!(task_id), Value::Object(patch))
            .await?;
        Ok(json!({"_id": task_id, "updatedAt": updated.get("updatedAt")}))
    }
}

/// The scripted functions of the fixture module.
pub fn whagons_functions() -> BTreeMap<String, Arc<dyn ScriptedFunction>> {
    let mut functions: BTreeMap<String, Arc<dyn ScriptedFunction>> = BTreeMap::new();
    functions.insert(
        "presence.beat".into(),
        Arc::new(Presence { transition: false }),
    );
    functions.insert(
        "presence.transition".into(),
        Arc::new(Presence { transition: true }),
    );
    functions.insert(
        "tasks.updateDescription".into(),
        Arc::new(TaskUpdate { extra_reads: 0 }),
    );
    functions.insert(
        "tasks.update".into(),
        Arc::new(TaskUpdate { extra_reads: 0 }),
    );
    functions
}
