use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use gonvex_module_host::framing::{read_frame, write_frame, FrameError};
use gonvex_module_host::protocol::{
    ActivateRequest, ClientFrame, HostCallFrame, InvokeRequest, LoadRequest, RequestOp,
    ResponsePayload, ServerFrame, WireError,
};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;

use crate::config::ModuleHostConfig;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleHostStatus {
    pub required: bool,
    pub ready: bool,
    pub active_projects: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Error)]
pub enum ModuleHostError {
    #[error("the TypeScript module host is enabled but neither GONVEX_MODULE_HOST_BINARY nor GONVEX_MODULE_HOST_ENDPOINT is configured")]
    Missing,
    #[error("failed to start TypeScript module host {binary}: {source}")]
    Start {
        binary: String,
        #[source]
        source: std::io::Error,
    },
    #[error("TypeScript module host did not report readiness within the configured timeout")]
    Timeout,
    #[error("TypeScript module host closed stdout before reporting readiness")]
    Closed,
    #[error("invalid TypeScript module host readiness message: {0}")]
    InvalidReady(String),
    #[error("TypeScript module host is not ready")]
    NotReady,
    #[error("TypeScript module host endpoint {endpoint:?} is invalid")]
    InvalidEndpoint { endpoint: String },
    #[error("failed to connect to TypeScript module host at {endpoint}: {source}")]
    Connect {
        endpoint: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("TypeScript module host returned {code}: {message}")]
    Remote { code: String, message: String },
    #[error("TypeScript module host returned an unexpected response")]
    UnexpectedResponse,
}

#[derive(Debug, Deserialize)]
struct ReadyMessage {
    ready: bool,
    protocol: u32,
    endpoint: String,
}

pub struct ModuleHost {
    config: ModuleHostConfig,
    endpoint: RwLock<Option<String>>,
    child: Mutex<Option<Child>>,
    status: RwLock<ModuleHostStatus>,
}

#[async_trait]
pub trait HostCallHandler: Send {
    async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String>;
    /// Only stateless, capability-checked work may opt out of serial dispatch.
    /// Database handlers retain the default so transaction calls stay ordered.
    fn concurrent(
        &self,
        _call: &HostCallFrame,
    ) -> Option<BoxFuture<'static, Result<Value, String>>> {
        None
    }
}

impl ModuleHost {
    pub fn new(config: ModuleHostConfig) -> Arc<Self> {
        Arc::new(Self {
            status: RwLock::new(ModuleHostStatus {
                required: config.enabled,
                ready: !config.enabled,
                active_projects: 0,
                reason: None,
            }),
            endpoint: RwLock::new(config.endpoint.clone()),
            child: Mutex::new(None),
            config,
        })
    }

    pub async fn start(&self) -> Result<(), ModuleHostError> {
        if !self.config.enabled {
            return Ok(());
        }
        if self.config.binary.is_none() {
            if self.config.endpoint.is_some() {
                let mut status = self.status.write().await;
                status.ready = true;
                status.reason = None;
                drop(status);
                return self.ping().await;
            }
            return Err(ModuleHostError::Missing);
        }

        let binary = self.config.binary.clone().expect("checked binary");
        let endpoint = self
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| temporary_endpoint(std::process::id()));
        let mut command = Command::new(&binary);
        command
            .arg("--listen")
            .arg(&endpoint)
            .arg("--max-frame-bytes")
            .arg(self.config.max_frame_bytes.to_string())
            .arg("--max-concurrent")
            .arg(self.config.max_concurrent_calls.to_string())
            .arg("--isolate-pool")
            .arg(self.config.isolate_pool_size.to_string())
            .arg("--execution-timeout-ms")
            .arg(self.config.execution_timeout.as_millis().to_string())
            .arg("--shutdown-ms")
            .arg(self.config.shutdown_timeout.as_millis().to_string())
            .arg("--exit-on-stdin-eof")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|source| ModuleHostError::Start {
            binary: binary.display().to_string(),
            source,
        })?;
        let stdout = child.stdout.take().ok_or(ModuleHostError::Closed)?;
        let mut lines = BufReader::new(stdout).lines();
        let line = timeout(self.config.start_timeout, lines.next_line())
            .await
            .map_err(|_| ModuleHostError::Timeout)?
            .map_err(|error| ModuleHostError::InvalidReady(error.to_string()))?
            .ok_or(ModuleHostError::Closed)?;
        let ready: ReadyMessage = serde_json::from_str(&line)
            .map_err(|error| ModuleHostError::InvalidReady(error.to_string()))?;
        if !ready.ready || ready.protocol != 2 || ready.endpoint.trim().is_empty() {
            return Err(ModuleHostError::InvalidReady(line));
        }
        *self.endpoint.write().await = Some(ready.endpoint);
        *self.child.lock().await = Some(child);
        let mut status = self.status.write().await;
        status.ready = true;
        status.reason = None;
        drop(status);
        self.ping().await
    }

    pub async fn status(&self) -> ModuleHostStatus {
        let mut status = self.status.read().await.clone();
        if let Some(child) = self.child.lock().await.as_mut() {
            match child.try_wait() {
                Ok(Some(exit)) => {
                    status.ready = false;
                    status.reason = Some(format!("module host exited with {exit}"));
                }
                Ok(None) => {}
                Err(error) => {
                    status.ready = false;
                    status.reason = Some(format!("module host status failed: {error}"));
                }
            }
        }
        status
    }

    pub async fn ping(&self) -> Result<(), ModuleHostError> {
        match self.request(RequestOp::Ping, None).await? {
            ResponsePayload::Pong { protocol: 2, .. } => Ok(()),
            _ => Err(ModuleHostError::UnexpectedResponse),
        }
    }

    pub async fn load(&self, request: LoadRequest) -> Result<ResponsePayload, ModuleHostError> {
        self.request(RequestOp::Load(request), None).await
    }

    pub async fn activate(
        &self,
        request: ActivateRequest,
    ) -> Result<ResponsePayload, ModuleHostError> {
        self.request(RequestOp::Activate(request), None).await
    }

    pub async fn invoke(
        &self,
        request: InvokeRequest,
        handler: &mut dyn HostCallHandler,
    ) -> Result<Value, ModuleHostError> {
        match self
            .request(RequestOp::Invoke(request), Some(handler))
            .await?
        {
            ResponsePayload::Invoked { value } => serde_json::from_str(&value)
                .map_err(|error| ModuleHostError::InvalidReady(error.to_string())),
            _ => Err(ModuleHostError::UnexpectedResponse),
        }
    }

    async fn request(
        &self,
        operation: RequestOp,
        mut handler: Option<&mut dyn HostCallHandler>,
    ) -> Result<ResponsePayload, ModuleHostError> {
        let mut stream = self.connect().await?;
        let request_timeout = if matches!(&operation, RequestOp::Invoke(_)) {
            self.config.execution_timeout
        } else {
            self.config.start_timeout
        };
        let request = ClientFrame::Request {
            id: 1,
            deadline_unix_ms: Some(unix_millis() + request_timeout.as_millis() as u64),
            payload: operation,
        };
        self.write_client_frame(&mut stream, &request).await?;
        let (reader, writer) = tokio::io::split(stream);
        dispatch_frames(reader, writer, self.config.max_frame_bytes, handler.take()).await
    }

    async fn write_client_frame(
        &self,
        stream: &mut BoxedStream,
        frame: &ClientFrame,
    ) -> Result<(), ModuleHostError> {
        let payload = serde_json::to_vec(frame)
            .map_err(|error| ModuleHostError::InvalidReady(error.to_string()))?;
        write_frame(stream, &payload, self.config.max_frame_bytes)
            .await
            .map_err(Into::into)
    }

    async fn connect(&self) -> Result<BoxedStream, ModuleHostError> {
        if !self.status().await.ready {
            return Err(ModuleHostError::NotReady);
        }
        let endpoint = self
            .endpoint
            .read()
            .await
            .clone()
            .ok_or(ModuleHostError::NotReady)?;
        let (scheme, address) = parse_endpoint(&endpoint)?;
        match scheme {
            "tcp" => TcpStream::connect(&address)
                .await
                .map(|stream| Box::new(stream) as BoxedStream)
                .map_err(|source| ModuleHostError::Connect { endpoint, source }),
            #[cfg(unix)]
            "unix" => UnixStream::connect(&address)
                .await
                .map(|stream| Box::new(stream) as BoxedStream)
                .map_err(|source| ModuleHostError::Connect { endpoint, source }),
            _ => Err(ModuleHostError::InvalidEndpoint { endpoint }),
        }
    }

    pub async fn shutdown(&self) {
        let mut child_slot = self.child.lock().await;
        let Some(child) = child_slot.as_mut() else {
            return;
        };
        let _ = child.start_kill();
        let _ = timeout(self.config.shutdown_timeout, child.wait()).await;
        self.status.write().await.ready = false;
        child_slot.take();
    }
}

trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
type BoxedStream = Box<dyn AsyncStream>;

fn parse_endpoint(endpoint: &str) -> Result<(&'static str, String), ModuleHostError> {
    let value = endpoint.trim();
    if let Some(address) = value
        .strip_prefix("tcp://")
        .or_else(|| value.strip_prefix("tcp:"))
    {
        return Ok(("tcp", address.to_owned()));
    }
    if let Some(address) = value
        .strip_prefix("unix://")
        .or_else(|| value.strip_prefix("unix:"))
    {
        #[cfg(unix)]
        return Ok(("unix", address.to_owned()));
        #[cfg(not(unix))]
        return Err(ModuleHostError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
        });
    }
    if value.starts_with('/') || value.starts_with('.') {
        #[cfg(unix)]
        return Ok(("unix", value.to_owned()));
    }
    if value.contains(':') {
        return Ok(("tcp", value.to_owned()));
    }
    Err(ModuleHostError::InvalidEndpoint {
        endpoint: endpoint.to_owned(),
    })
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn remote_error(error: WireError) -> ModuleHostError {
    ModuleHostError::Remote {
        code: error.code,
        message: error.message,
    }
}

fn temporary_endpoint(pid: u32) -> String {
    #[cfg(unix)]
    {
        let path: PathBuf = std::env::temp_dir().join(format!("gonvex-module-host-{pid}.sock"));
        format!("unix:{}", path.display())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        "tcp:127.0.0.1:0".to_owned()
    }
}

fn host_response(id: u64, result: Result<Value, String>) -> ClientFrame {
    match result {
        Ok(value) => ClientFrame::HostResponse { id, value },
        Err(message) => ClientFrame::HostError {
            id,
            error: WireError::new("host_call_failed", message),
        },
    }
}

async fn dispatch_frames<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut reader: R,
    mut writer: W,
    max_frame_bytes: usize,
    mut handler: Option<&mut dyn HostCallHandler>,
) -> Result<ResponsePayload, ModuleHostError> {
    let mut pending: FuturesUnordered<BoxFuture<'static, ClientFrame>> = FuturesUnordered::new();
    loop {
        // Preserve partial reads when a network request completes. read_exact
        // cannot be cancelled safely after it has consumed part of a frame.
        let read = read_frame(&mut reader, max_frame_bytes);
        tokio::pin!(read);
        let frame = loop {
            tokio::select! {
                frame = &mut read => break frame?,
                Some(response) = pending.next(), if !pending.is_empty() => {
                    let bytes = serde_json::to_vec(&response).map_err(|e| ModuleHostError::InvalidReady(e.to_string()))?;
                    write_frame(&mut writer, &bytes, max_frame_bytes).await?;
                }
            }
        };
        let frame: ServerFrame = serde_json::from_slice(&frame)
            .map_err(|e| ModuleHostError::InvalidReady(e.to_string()))?;
        match frame {
            ServerFrame::Ready { protocol: 2, .. } => continue,
            ServerFrame::Response { id: 1, payload } => return Ok(payload),
            ServerFrame::Error { id: 1, error } => return Err(remote_error(error)),
            ServerFrame::HostCall {
                id,
                invocation: 1,
                payload,
            } => {
                let result = match handler.as_deref_mut() {
                    Some(handler) => {
                        if let Some(work) = handler.concurrent(&payload) {
                            if pending.len() < 16 {
                                pending
                                    .push(Box::pin(async move { host_response(id, work.await) }));
                                continue;
                            }
                            Err("Action concurrent network request limit exceeded".to_owned())
                        } else {
                            handler.handle(payload).await
                        }
                    }
                    None => Err("this module-host request has no capability dispatcher".to_owned()),
                };
                let bytes = serde_json::to_vec(&host_response(id, result))
                    .map_err(|e| ModuleHostError::InvalidReady(e.to_string()))?;
                write_frame(&mut writer, &bytes, max_frame_bytes).await?;
            }
            _ => return Err(ModuleHostError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::Notify;

    struct DelayedNetwork {
        release: Arc<Notify>,
    }
    #[async_trait]
    impl HostCallHandler for DelayedNetwork {
        async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
            match call {
                HostCallFrame::ToolInvoke { .. } => Ok(json!("heartbeat")),
                HostCallFrame::Fetch { .. } => {
                    self.release.notified().await;
                    Ok(json!("network"))
                }
                _ => Err("unexpected serial call".into()),
            }
        }
        fn concurrent(
            &self,
            call: &HostCallFrame,
        ) -> Option<BoxFuture<'static, Result<Value, String>>> {
            if !matches!(call, HostCallFrame::Fetch { .. }) {
                return None;
            }
            let release = self.release.clone();
            Some(Box::pin(async move {
                release.notified().await;
                Ok(json!("network"))
            }))
        }
    }

    #[tokio::test]
    async fn heartbeat_completes_while_network_response_is_pending() {
        let (runtime, mut host) = tokio::io::duplex(8192);
        let release = Arc::new(Notify::new());
        let mut handler = DelayedNetwork {
            release: release.clone(),
        };
        let task = tokio::spawn(async move {
            let (reader, writer) = tokio::io::split(runtime);
            dispatch_frames(reader, writer, 8192, Some(&mut handler)).await
        });
        for (id, payload) in [
            (1, HostCallFrame::Fetch { request: json!({}) }),
            (
                2,
                HostCallFrame::ToolInvoke {
                    tool: "heartbeat".into(),
                    args: json!({}),
                },
            ),
        ] {
            let frame = ServerFrame::HostCall {
                id,
                invocation: 1,
                payload,
            };
            write_frame(&mut host, &serde_json::to_vec(&frame).unwrap(), 8192)
                .await
                .unwrap();
        }
        let first = timeout(
            std::time::Duration::from_secs(2),
            read_frame(&mut host, 8192),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            serde_json::from_slice::<ClientFrame>(&first).unwrap(),
            ClientFrame::HostResponse { id: 2, .. }
        ));
        // Start the next frame but leave its payload incomplete while the
        // network result arrives. The reader must retain these consumed bytes.
        use tokio::io::AsyncWriteExt;
        let done = ServerFrame::Response {
            id: 1,
            payload: ResponsePayload::Invoked { value: "{}".into() },
        };
        let done_bytes = serde_json::to_vec(&done).unwrap();
        host.write_all(&(done_bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        host.write_all(&done_bytes[..3]).await.unwrap();
        release.notify_one();
        let second = timeout(
            std::time::Duration::from_secs(2),
            read_frame(&mut host, 8192),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            serde_json::from_slice::<ClientFrame>(&second).unwrap(),
            ClientFrame::HostResponse { id: 1, .. }
        ));
        host.write_all(&done_bytes[3..]).await.unwrap();
        assert!(task.await.unwrap().is_ok());
    }
}
