use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
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
            .arg("--max-host-calls")
            .arg(self.config.max_host_calls.to_string())
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
        loop {
            let frame = read_frame(&mut stream, self.config.max_frame_bytes).await?;
            let frame: ServerFrame = serde_json::from_slice(&frame)
                .map_err(|error| ModuleHostError::InvalidReady(error.to_string()))?;
            match frame {
                ServerFrame::Ready { protocol: 2, .. } => continue,
                ServerFrame::Response { id: 1, payload } => return Ok(payload),
                ServerFrame::Error { id: 1, error } => return Err(remote_error(error)),
                ServerFrame::HostCall {
                    id,
                    invocation: 1,
                    payload,
                } => {
                    let response = match handler.as_deref_mut() {
                        Some(handler) => match handler.handle(payload).await {
                            Ok(value) => ClientFrame::HostResponse { id, value },
                            Err(message) => ClientFrame::HostError {
                                id,
                                error: WireError::new("host_call_failed", message),
                            },
                        },
                        None => ClientFrame::HostError {
                            id,
                            error: WireError::new(
                                "host_call_failed",
                                "this module-host request has no capability dispatcher",
                            ),
                        },
                    };
                    self.write_client_frame(&mut stream, &response).await?;
                }
                _ => return Err(ModuleHostError::UnexpectedResponse),
            }
        }
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
