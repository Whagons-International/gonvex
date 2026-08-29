//! Capability-scoped Action host operations.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use gonvex_module_host::protocol::HostCallFrame;
use gonvex_postgres::TenantSession;
use reqwest::redirect::{Attempt, Policy};
use serde::Deserialize;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::execution::{CommittedRevisionTracker, ExecutionAccess, NestedExecutionAccess};
use crate::module_host::HostCallHandler;
use crate::modules::FunctionDefinition;
use crate::modules::ModuleCallLease;
use crate::Runtime;

const MAX_FETCH_RESPONSE_BYTES: usize = 8 << 20;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolBinding {
    kind: String,
    function: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActionCapabilities {
    #[serde(default)]
    network_origins: Vec<String>,
    #[serde(default)]
    tools: BTreeMap<String, ToolBinding>,
    #[serde(default)]
    scheduler: bool,
    #[serde(default)]
    storage: bool,
    #[serde(default)]
    sandbox: Option<Value>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    functions: bool,
}

pub struct ActionHostCalls {
    runtime: Runtime,
    session: TenantSession,
    capabilities: ActionCapabilities,
    provenance: gonvex_module_runtime::InvocationProvenance,
    module: ModuleCallLease,
    committed_revisions: CommittedRevisionTracker,
}

impl ActionHostCalls {
    pub(crate) fn new(
        runtime: Runtime,
        session: TenantSession,
        definition: &FunctionDefinition,
        provenance: gonvex_module_runtime::InvocationProvenance,
        module: ModuleCallLease,
        committed_revisions: CommittedRevisionTracker,
    ) -> Result<Self, String> {
        let capabilities = if definition.action_capabilities.is_null() {
            ActionCapabilities::default()
        } else {
            serde_json::from_value(definition.action_capabilities.clone())
                .map_err(|error| format!("invalid Action capability declaration: {error}"))?
        };
        if capabilities.functions && definition.action_profile != "agent" {
            return Err(
                "interactive function invocation requires an agent Action profile".to_owned(),
            );
        }
        Ok(Self {
            runtime,
            session,
            capabilities,
            provenance,
            module,
            committed_revisions,
        })
    }

    pub fn network(&self) -> bool {
        !self.capabilities.network_origins.is_empty()
    }

    pub fn tools(&self) -> Vec<String> {
        self.capabilities.tools.keys().cloned().collect()
    }

    pub fn scheduler(&self) -> bool {
        self.capabilities.scheduler
    }

    pub fn storage(&self) -> bool {
        self.capabilities.storage
    }

    pub fn sandbox(&self) -> bool {
        self.capabilities.sandbox.is_some()
    }

    pub fn secrets(&self) -> &[String] {
        &self.capabilities.secrets
    }

    pub fn functions(&self) -> bool {
        self.capabilities.functions
    }

    async fn invoke_function(
        &self,
        path: &str,
        args: Value,
        artifact_hash: &str,
    ) -> Result<Value, String> {
        self.runtime
            .invoke_interactive_function(
                &self.session,
                &self.provenance,
                path,
                args,
                artifact_hash,
                NestedExecutionAccess {
                    module: self.module.clone(),
                    committed_revisions: self.committed_revisions.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())
    }

    async fn invoke_tool(&self, name: &str, args: Value) -> Result<Value, String> {
        if self.provenance.depth >= gonvex_module_runtime::MAX_INVOCATION_DEPTH {
            return Err("nested function invocation exceeded the maximum depth".to_owned());
        }
        let binding = self
            .capabilities
            .tools
            .get(name)
            .ok_or_else(|| format!("Action tool {name:?} is not declared"))?;
        let command_id = format!("agent-tool-{}", Uuid::new_v4());
        let mut provenance = self.provenance.clone();
        provenance.parent_command_id = Some(provenance.command_id.clone());
        provenance.command_id = command_id.clone();
        provenance.channel = gonvex_module_runtime::InvocationChannel::Agent;
        provenance.depth = provenance.depth.saturating_add(1);
        provenance.on_behalf_of_member_id = Some(self.session.member.id.clone());
        match binding.kind.as_str() {
            "query" => self
                .runtime
                .execute_tenant_query_with_access(
                    &self.session,
                    &binding.function,
                    args,
                    ExecutionAccess {
                        allow_internal: true,
                        provenance: Some(provenance),
                        module: Some(self.module.clone()),
                        committed_revisions: Some(self.committed_revisions.clone()),
                    },
                )
                .await
                .map_err(|error| error.to_string()),
            "reducer" | "internalReducer" => self
                .runtime
                .execute_tenant_reducer_with_access(
                    &self.session,
                    &command_id,
                    None,
                    &binding.function,
                    args,
                    ExecutionAccess {
                        allow_internal: binding.kind == "internalReducer",
                        provenance: Some(provenance),
                        module: Some(self.module.clone()),
                        committed_revisions: Some(self.committed_revisions.clone()),
                    },
                )
                .await
                .map(|result| result.value)
                .map_err(|error| error.to_string()),
            kind => Err(format!(
                "Action tool {name:?} has unsupported kind {kind:?}"
            )),
        }
    }

    async fn fetch(&self, request: Value) -> Result<Value, String> {
        #[derive(Deserialize)]
        struct FetchRequest {
            url: String,
            #[serde(default)]
            method: String,
            #[serde(default)]
            headers: BTreeMap<String, String>,
            #[serde(default)]
            body: Option<String>,
        }
        let request: FetchRequest = serde_json::from_value(request)
            .map_err(|error| format!("invalid fetch request: {error}"))?;
        let parsed = Url::parse(request.url.trim())
            .map_err(|_| "fetch only supports absolute http and https URLs".to_owned())?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err("fetch only supports absolute http and https URLs".to_owned());
        }
        let allowed = self
            .capabilities
            .network_origins
            .iter()
            .map(|origin| origin.trim().to_owned())
            .collect::<BTreeSet<_>>();
        let requested_origin = origin(&parsed)?;
        if !allowed.contains(&requested_origin) {
            return Err(format!(
                "fetch origin {requested_origin:?} is not declared for this Action"
            ));
        }
        let redirect_allowed = allowed.clone();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(Policy::custom(move |attempt: Attempt<'_>| {
                let permitted = origin(attempt.url())
                    .map(|origin| redirect_allowed.contains(&origin))
                    .unwrap_or(false);
                if permitted {
                    attempt.follow()
                } else {
                    attempt.error("redirect origin is not declared for this Action")
                }
            }))
            .build()
            .map_err(|error| error.to_string())?;
        let method = if request.method.trim().is_empty() {
            reqwest::Method::GET
        } else {
            reqwest::Method::from_bytes(request.method.trim().as_bytes())
                .map_err(|error| format!("invalid fetch method: {error}"))?
        };
        let mut outbound = client.request(method, parsed.clone());
        for (name, value) in request.headers {
            outbound = outbound.header(name, value);
        }
        if let Some(body) = request.body {
            outbound = outbound.body(body);
        }
        let response = outbound.send().await.map_err(|error| error.to_string())?;
        if response.content_length().unwrap_or_default() > MAX_FETCH_RESPONSE_BYTES as u64 {
            return Err(format!(
                "fetch response exceeds the {MAX_FETCH_RESPONSE_BYTES} byte limit"
            ));
        }
        let status = response.status();
        let response_url = response.url().to_string();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        let bytes = response.bytes().await.map_err(|error| error.to_string())?;
        if bytes.len() > MAX_FETCH_RESPONSE_BYTES {
            return Err(format!(
                "fetch response exceeds the {MAX_FETCH_RESPONSE_BYTES} byte limit"
            ));
        }
        Ok(serde_json::json!({
            "status": status.as_u16(),
            "statusText": status.canonical_reason().unwrap_or_default(),
            "url": response_url,
            "headers": headers,
            "body": String::from_utf8_lossy(&bytes),
        }))
    }
}

#[async_trait]
impl HostCallHandler for ActionHostCalls {
    async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
        match call {
            HostCallFrame::ToolInvoke { tool, args } => self.invoke_tool(&tool, args).await,
            HostCallFrame::FunctionInvoke { .. } if !self.capabilities.functions => {
                Err("interactive function invocation is not declared for this Action".to_owned())
            }
            HostCallFrame::FunctionInvoke {
                path,
                args,
                artifact_hash,
            } => self.invoke_function(&path, args, &artifact_hash).await,
            HostCallFrame::Fetch { request } if self.network() => self.fetch(request).await,
            HostCallFrame::Fetch { .. } => {
                Err("network access is not declared for this Action".to_owned())
            }
            HostCallFrame::ScheduleAfter { .. } | HostCallFrame::ScheduleAt { .. }
                if !self.capabilities.scheduler =>
            {
                Err("scheduler access is not declared for this Action".to_owned())
            }
            HostCallFrame::ScheduleAfter {
                delay_ms,
                function,
                args,
            } => {
                let run_at = chrono::Utc::now()
                    + chrono::Duration::milliseconds(i64::try_from(delay_ms).unwrap_or(i64::MAX));
                self.runtime
                    .enqueue_scheduled(&self.session, &function, args, run_at, &self.provenance)
                    .await
            }
            HostCallFrame::ScheduleAt {
                at_unix_ms,
                function,
                args,
            } => {
                let at = i64::try_from(at_unix_ms)
                    .ok()
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .ok_or_else(|| {
                        "scheduler atUnixMs is outside the supported range".to_owned()
                    })?;
                self.runtime
                    .enqueue_scheduled(&self.session, &function, args, at, &self.provenance)
                    .await
            }
            HostCallFrame::Storage { .. } if !self.capabilities.storage => {
                Err("storage access is not declared for this Action".to_owned())
            }
            HostCallFrame::Storage { operation, payload } => {
                let runtime = self.runtime.clone();
                let session = self.session.clone();
                let storage = runtime.inner.storage.clone();
                storage.call(runtime, session, operation, payload).await
            }
            HostCallFrame::Sandbox { .. } if self.capabilities.sandbox.is_none() => {
                Err("sandbox access is not declared for this Action".to_owned())
            }
            HostCallFrame::Sandbox { operation, .. }
                if operation == "importFile" && !self.capabilities.storage =>
            {
                Err("sandbox importFile also requires the storage capability".to_owned())
            }
            HostCallFrame::Sandbox { operation, payload } => {
                self.runtime
                    .clone()
                    .sandbox_call(
                        self.session.clone(),
                        operation,
                        payload,
                        self.capabilities
                            .sandbox
                            .as_ref()
                            .and_then(|value| value.get("duckdb"))
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    )
                    .await
            }
            _ => {
                Err("this Action capability is not implemented in the Rust runtime yet".to_owned())
            }
        }
    }
}

fn origin(url: &Url) -> Result<String, String> {
    let host = url.host_str().ok_or_else(|| "URL has no host".to_owned())?;
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Ok(format!("{}://{host}{port}", url.scheme()))
}
