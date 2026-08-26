//! Host-owned ephemeral TypeScript analysis workspaces.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use gonvex_postgres::TenantSession;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::{watch, Mutex, Semaphore};
use uuid::Uuid;

use crate::config::SandboxConfig;
use crate::Runtime;

#[derive(Clone)]
pub struct SandboxManager {
    inner: Arc<Inner>,
}

struct Inner {
    config: SandboxConfig,
    admission: Semaphore,
    workspaces: Mutex<BTreeMap<String, Workspace>>,
}

#[derive(Clone, Eq, PartialEq)]
struct Scope {
    project: String,
    tenant: String,
    account: String,
}

struct Workspace {
    scope: Scope,
    root: PathBuf,
    duckdb: bool,
    expires_at: DateTime<Utc>,
    executions: BTreeMap<String, Execution>,
    active: Option<String>,
    imports: Vec<Value>,
}

struct Execution {
    status: String,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    result: Value,
    error: String,
    logs: Value,
    cancel: watch::Sender<bool>,
}

impl SandboxManager {
    pub fn new(config: SandboxConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                admission: Semaphore::new(config.concurrency),
                config,
                workspaces: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub async fn shutdown(&self) {
        let roots = {
            let mut workspaces = self.inner.workspaces.lock().await;
            for workspace in workspaces.values() {
                for execution in workspace.executions.values() {
                    let _ = execution.cancel.send(true);
                }
            }
            std::mem::take(&mut *workspaces)
                .into_values()
                .map(|workspace| workspace.root)
                .collect::<Vec<_>>()
        };
        for root in roots {
            let _ = tokio::fs::remove_dir_all(root).await;
        }
    }

    async fn call(
        &self,
        scope: Scope,
        operation: &str,
        payload: Value,
        duckdb_declared: bool,
    ) -> Result<Value, String> {
        if !self.inner.config.enabled {
            return Err("sandbox is disabled by runtime policy".to_owned());
        }
        self.prune().await;
        match operation {
            "create" => self.create(scope, payload, duckdb_declared).await,
            "run" => self.run(scope, payload).await,
            "cancel" => self.cancel(scope, payload).await,
            "status" => self.status(scope, payload).await,
            "readFile" => self.read_file(scope, payload, false).await,
            "readText" => self.read_file(scope, payload, true).await,
            "writeFile" => self.write_file(scope, payload, false).await,
            "writeText" => self.write_file(scope, payload, true).await,
            "importFile" => {
                Err("sandbox importFile is dispatched by the trusted storage bridge".to_owned())
            }
            _ => Err(format!("unsupported sandbox operation {operation:?}")),
        }
    }

    async fn create(&self, scope: Scope, payload: Value, duckdb: bool) -> Result<Value, String> {
        exact_fields(&payload, &["ttlMs"])?;
        let ttl_ms = payload
            .get("ttlMs")
            .and_then(Value::as_u64)
            .unwrap_or(self.inner.config.default_ttl.as_millis() as u64);
        let ttl = Duration::from_millis(ttl_ms);
        if ttl > self.inner.config.max_ttl {
            return Err("sandbox ttl exceeds runtime policy".to_owned());
        }
        let mut workspaces = self.inner.workspaces.lock().await;
        if workspaces.len() >= self.inner.config.max_total {
            return Err("runtime sandbox limit reached".to_owned());
        }
        let owned = workspaces
            .values()
            .filter(|workspace| workspace.scope == scope)
            .count();
        if owned >= self.inner.config.max_per_account {
            return Err("sandbox limit reached for this account and tenant".to_owned());
        }
        let id = format!("sbx_{}", Uuid::new_v4());
        let root = self.inner.config.root.join(&id);
        tokio::fs::create_dir_all(root.join("files"))
            .await
            .map_err(|error| error.to_string())?;
        tokio::fs::create_dir_all(root.join("imports"))
            .await
            .map_err(|error| error.to_string())?;
        let expires_at =
            Utc::now() + chrono::Duration::from_std(ttl).map_err(|error| error.to_string())?;
        workspaces.insert(
            id.clone(),
            Workspace {
                scope,
                root,
                duckdb,
                expires_at,
                executions: BTreeMap::new(),
                active: None,
                imports: Vec::new(),
            },
        );
        Ok(serde_json::json!({
            "sandboxId":id,"expiresAt":expires_at.timestamp_millis(),"duckdb":duckdb,
        }))
    }

    async fn run(&self, scope: Scope, payload: Value) -> Result<Value, String> {
        exact_fields(&payload, &["sandboxId", "code", "timeoutMs"])?;
        let sandbox_id = required_string(&payload, "sandboxId")?.to_owned();
        let code = required_string(&payload, "code")?.to_owned();
        if code.len() > self.inner.config.max_code_bytes {
            return Err("sandbox code exceeds runtime policy".to_owned());
        }
        let timeout_ms = payload
            .get("timeoutMs")
            .and_then(Value::as_u64)
            .unwrap_or(self.inner.config.default_timeout.as_millis() as u64);
        let timeout = Duration::from_millis(timeout_ms);
        if timeout > self.inner.config.max_timeout {
            return Err("sandbox timeout exceeds runtime policy".to_owned());
        }
        let (execution_id, root, duckdb, imports, mut cancel) = {
            let mut workspaces = self.inner.workspaces.lock().await;
            let workspace = owned_workspace(&mut workspaces, &scope, &sandbox_id)?;
            if workspace.active.is_some() {
                return Err("sandbox already has an active execution".to_owned());
            }
            if workspace.executions.len() >= self.inner.config.max_executions {
                return Err("sandbox execution limit reached".to_owned());
            }
            let execution_id = format!("run_{}", Uuid::new_v4());
            let (sender, receiver) = watch::channel(false);
            workspace.executions.insert(
                execution_id.clone(),
                Execution {
                    status: "running".to_owned(),
                    started_at: Utc::now(),
                    finished_at: None,
                    result: Value::Null,
                    error: String::new(),
                    logs: Value::Array(Vec::new()),
                    cancel: sender,
                },
            );
            workspace.active = Some(execution_id.clone());
            (
                execution_id,
                workspace.root.clone(),
                workspace.duckdb,
                workspace.imports.clone(),
                receiver,
            )
        };
        let _permit = self
            .inner
            .admission
            .acquire()
            .await
            .map_err(|_| "sandbox is shutting down".to_owned())?;
        let request = serde_json::json!({
            "version":1,"root":root,"allowUnconfined":self.inner.config.allow_unconfined,
            "code":code,"duckdb":duckdb,"imports":imports,
            "maxHeapBytes":self.inner.config.max_heap_bytes,
            "maxFileBytes":self.inner.config.max_file_bytes,
            "maxWorkspaceBytes":self.inner.config.max_workspace_bytes,
            "maxOutputBytes":self.inner.config.max_output_bytes,
            "maxRows":self.inner.config.max_rows,
            "duckdbMemoryBytes":self.inner.config.duckdb_memory_bytes,
            "timeoutMs":timeout_ms,"workerUid":self.inner.config.worker_uid,
            "workerGid":self.inner.config.worker_gid,
        });
        let response = self.execute_worker(request, timeout, &mut cancel).await;
        let mut workspaces = self.inner.workspaces.lock().await;
        let workspace = owned_workspace(&mut workspaces, &scope, &sandbox_id)?;
        workspace.active = None;
        let execution = workspace
            .executions
            .get_mut(&execution_id)
            .ok_or_else(|| "sandbox execution disappeared".to_owned())?;
        execution.finished_at = Some(Utc::now());
        match response {
            Ok(value) => {
                execution.status = "completed".to_owned();
                execution.result = value.get("result").cloned().unwrap_or(Value::Null);
                execution.logs = value
                    .get("logs")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new()));
                if !value.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                    execution.status = "failed".to_owned();
                    execution.error = bounded(
                        value
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("sandbox failed"),
                        4_000,
                    );
                }
            }
            Err(error) => {
                execution.status = if error == "sandbox execution cancelled" {
                    "cancelled"
                } else {
                    "failed"
                }
                .to_owned();
                execution.error = bounded(&error, 4_000);
            }
        }
        Ok(execution_json(&sandbox_id, &execution_id, execution))
    }

    async fn execute_worker(
        &self,
        request: Value,
        timeout: Duration,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<Value, String> {
        let binary = self
            .inner
            .config
            .worker_binary
            .as_ref()
            .ok_or_else(|| "sandbox worker binary is not configured".to_owned())?;
        let mut child = Command::new(binary)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| format!("start sandbox worker: {error}"))?;
        let input = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "sandbox worker stdin is unavailable".to_owned())?;
        stdin
            .write_all(&input)
            .await
            .map_err(|error| error.to_string())?;
        drop(stdin);
        let output = child.wait_with_output();
        tokio::pin!(output);
        let bytes = tokio::select! {
            result = &mut output => result.map_err(|error| error.to_string())?.stdout,
            _ = tokio::time::sleep(timeout + Duration::from_secs(2)) => {
                return Err("sandbox execution timed out".to_owned());
            }
            changed = cancel.changed() => {
                if changed.is_ok() && *cancel.borrow() {
                    return Err("sandbox execution cancelled".to_owned());
                }
                return Err("sandbox execution cancelled".to_owned());
            }
        };
        if bytes.len() > self.inner.config.max_output_bytes {
            return Err("sandbox output exceeds runtime policy".to_owned());
        }
        serde_json::from_slice(&bytes).map_err(|error| format!("invalid sandbox response: {error}"))
    }

    async fn cancel(&self, scope: Scope, payload: Value) -> Result<Value, String> {
        exact_fields(&payload, &["sandboxId", "executionId"])?;
        let sandbox_id = required_string(&payload, "sandboxId")?;
        let execution_id = required_string(&payload, "executionId")?;
        let mut workspaces = self.inner.workspaces.lock().await;
        let workspace = owned_workspace(&mut workspaces, &scope, sandbox_id)?;
        let execution = workspace
            .executions
            .get(execution_id)
            .ok_or_else(|| "sandbox execution was not found".to_owned())?;
        if execution.status == "running" {
            let _ = execution.cancel.send(true);
        }
        Ok(execution_json(sandbox_id, execution_id, execution))
    }

    async fn status(&self, scope: Scope, payload: Value) -> Result<Value, String> {
        exact_fields(&payload, &["sandboxId", "executionId"])?;
        let sandbox_id = required_string(&payload, "sandboxId")?;
        let execution_id = required_string(&payload, "executionId")?;
        let mut workspaces = self.inner.workspaces.lock().await;
        let workspace = owned_workspace(&mut workspaces, &scope, sandbox_id)?;
        let execution = workspace
            .executions
            .get(execution_id)
            .ok_or_else(|| "sandbox execution was not found".to_owned())?;
        Ok(execution_json(sandbox_id, execution_id, execution))
    }

    async fn read_file(&self, scope: Scope, payload: Value, text: bool) -> Result<Value, String> {
        exact_fields(&payload, &["sandboxId", "path"])?;
        let sandbox_id = required_string(&payload, "sandboxId")?;
        let relative = required_string(&payload, "path")?;
        let path = {
            let mut workspaces = self.inner.workspaces.lock().await;
            let workspace = owned_workspace(&mut workspaces, &scope, sandbox_id)?;
            safe_file(&workspace.root.join("files"), relative)?
        };
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|error| error.to_string())?;
        if bytes.len() as u64 > self.inner.config.max_file_bytes {
            return Err("sandbox file exceeds runtime policy".to_owned());
        }
        if text {
            String::from_utf8(bytes)
                .map(Value::String)
                .map_err(|_| "sandbox file is not UTF-8".to_owned())
        } else {
            Ok(serde_json::json!({"contentBase64":STANDARD.encode(&bytes),"size":bytes.len()}))
        }
    }

    async fn write_file(&self, scope: Scope, payload: Value, text: bool) -> Result<Value, String> {
        let fields = if text {
            &["sandboxId", "path", "content"][..]
        } else {
            &["sandboxId", "path", "contentBase64"][..]
        };
        exact_fields(&payload, fields)?;
        let sandbox_id = required_string(&payload, "sandboxId")?;
        let relative = required_string(&payload, "path")?;
        let content = if text {
            required_string_allow_empty(&payload, "content")?
                .as_bytes()
                .to_vec()
        } else {
            STANDARD
                .decode(required_string_allow_empty(&payload, "contentBase64")?)
                .map_err(|_| "sandbox contentBase64 is invalid".to_owned())?
        };
        if content.len() as u64 > self.inner.config.max_file_bytes {
            return Err("sandbox file exceeds runtime policy".to_owned());
        }
        let (root, path) = {
            let mut workspaces = self.inner.workspaces.lock().await;
            let workspace = owned_workspace(&mut workspaces, &scope, sandbox_id)?;
            let root = workspace.root.join("files");
            let path = safe_file(&root, relative)?;
            (root, path)
        };
        let current = directory_size(&root).await?;
        let prior = tokio::fs::metadata(&path)
            .await
            .map(|value| value.len())
            .unwrap_or(0);
        if current
            .saturating_sub(prior)
            .saturating_add(content.len() as u64)
            > self.inner.config.max_workspace_bytes
        {
            return Err("sandbox workspace byte limit exceeded".to_owned());
        }
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| error.to_string())?;
        }
        tokio::fs::write(path, &content)
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({"path":relative,"size":content.len()}))
    }

    async fn import_file(
        &self,
        scope: Scope,
        sandbox_id: &str,
        _file_id: &str,
        filename: &str,
        bytes: Vec<u8>,
    ) -> Result<Value, String> {
        if filename.trim().is_empty() {
            return Err("sandbox importFile requires the original filename".to_owned());
        }
        if bytes.len() as u64 > self.inner.config.max_file_bytes {
            return Err("sandbox import exceeds runtime policy".to_owned());
        }
        let (root, alias) = {
            let mut workspaces = self.inner.workspaces.lock().await;
            let workspace = owned_workspace(&mut workspaces, &scope, sandbox_id)?;
            if !workspace.duckdb {
                return Err("sandbox importFile requires capabilities.sandbox.duckdb".to_owned());
            }
            let alias = format!("import_{}", workspace.imports.len() + 1);
            (workspace.root.clone(), alias)
        };
        let relative = format!("imports/{alias}.duckdb");
        let target = root.join(&relative);
        let filename = filename.to_owned();
        let tables = tokio::task::spawn_blocking(move || {
            crate::data_ingest::ingest(&bytes, &filename, &target)
        })
        .await
        .map_err(|error| error.to_string())??;
        let value = serde_json::json!({"alias":alias,"path":relative,"tables":tables});
        let mut workspaces = self.inner.workspaces.lock().await;
        let workspace = owned_workspace(&mut workspaces, &scope, sandbox_id)?;
        workspace.imports.push(value.clone());
        Ok(serde_json::json!({"alias":alias,"tables":tables}))
    }

    async fn prune(&self) {
        let now = Utc::now();
        let roots = {
            let mut workspaces = self.inner.workspaces.lock().await;
            let expired = workspaces
                .iter()
                .filter(|(_, workspace)| workspace.expires_at <= now && workspace.active.is_none())
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            expired
                .into_iter()
                .filter_map(|id| workspaces.remove(&id).map(|workspace| workspace.root))
                .collect::<Vec<_>>()
        };
        for root in roots {
            let _ = tokio::fs::remove_dir_all(root).await;
        }
    }
}

impl Runtime {
    pub(crate) async fn sandbox_call(
        self,
        session: TenantSession,
        operation: String,
        payload: Value,
        duckdb: bool,
    ) -> Result<Value, String> {
        let scope = Scope {
            project: session.identity.project_id.clone(),
            tenant: session.route.tenant_id.clone(),
            account: session.identity.account.id.clone(),
        };
        if operation == "importFile" {
            if !duckdb {
                return Err("sandbox importFile requires capabilities.sandbox.duckdb".to_owned());
            }
            exact_fields(&payload, &["sandboxId", "fileId", "filename"])?;
            let sandbox_id = required_string(&payload, "sandboxId")?.to_owned();
            let file_id = required_string(&payload, "fileId")?.to_owned();
            let filename = required_string(&payload, "filename")?.to_owned();
            let read = self.inner.storage.clone().read_file(
                self.clone(),
                session.clone(),
                file_id.clone(),
                usize::try_from(self.inner.config.sandbox.max_file_bytes).unwrap_or(usize::MAX),
            );
            let (bytes, _) = read.await?;
            return self
                .inner
                .sandboxes
                .import_file(scope, &sandbox_id, &file_id, &filename, bytes)
                .await;
        }
        self.inner
            .sandboxes
            .call(scope, &operation, payload, duckdb)
            .await
    }
}

fn owned_workspace<'a>(
    workspaces: &'a mut BTreeMap<String, Workspace>,
    scope: &Scope,
    id: &str,
) -> Result<&'a mut Workspace, String> {
    let workspace = workspaces
        .get_mut(id)
        .ok_or_else(|| "sandbox was not found".to_owned())?;
    if &workspace.scope != scope || workspace.expires_at <= Utc::now() {
        return Err("sandbox was not found".to_owned());
    }
    Ok(workspace)
}

fn execution_json(sandbox: &str, id: &str, execution: &Execution) -> Value {
    serde_json::json!({
        "sandboxId":sandbox,"executionId":id,"status":execution.status,
        "startedAt":execution.started_at.timestamp_millis(),
        "finishedAt":execution.finished_at.map(|value| value.timestamp_millis()),
        "result":execution.result,"error":execution.error,"logs":execution.logs,
    })
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    required_string_allow_empty(value, key).and_then(|value| {
        if value.is_empty() {
            Err(format!("{key} is required"))
        } else {
            Ok(value)
        }
    })
}

fn required_string_allow_empty<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{key} must be a string"))
}

fn exact_fields(value: &Value, fields: &[&str]) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "sandbox payload must be an object".to_owned())?;
    if let Some(field) = object.keys().find(|key| !fields.contains(&key.as_str())) {
        return Err(format!(
            "sandbox payload contains unsupported field {field:?}"
        ));
    }
    Ok(())
}

fn safe_file(root: &Path, relative: &str) -> Result<PathBuf, String> {
    let relative = Path::new(relative);
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || !relative
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err("sandbox path must be relative and cannot escape the workspace".to_owned());
    }
    Ok(root.join(relative))
}

async fn directory_size(root: &Path) -> Result<u64, String> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || directory_size_sync(&root))
        .await
        .map_err(|error| error.to_string())?
}

fn directory_size_sync(root: &Path) -> Result<u64, String> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let metadata = entry.metadata().map_err(|error| error.to_string())?;
        total = total.saturating_add(if metadata.is_dir() {
            directory_size_sync(&entry.path())?
        } else {
            metadata.len()
        });
    }
    Ok(total)
}

fn bounded(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_cannot_escape_the_workspace() {
        let root = Path::new("/tmp/sandbox/files");
        assert!(safe_file(root, "report/data.csv").is_ok());
        assert!(safe_file(root, "../secret").is_err());
        assert!(safe_file(root, "/etc/passwd").is_err());
    }
}
