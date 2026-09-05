//! The wire contract between the Gonvex Rust runtime and this module host.
//!
//! Every frame is one explicitly named shape. There is no free-form envelope
//! and no operation the host has to guess at: an unknown `type` or `op` is a
//! decode error the peer is told about by request id, not something the host
//! tries to interpret.
//!
//! Requests carry their own id and deadline. Responses and host calls carry the
//! id they belong to, which is what lets one connection multiplex many
//! concurrent invocations and, while any of them is running, host calls in the
//! opposite direction.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gonvex_module_runtime::{
    AccountIdentity, Capabilities, FunctionKind, HostCall, IdentityContext, InvocationContext,
    InvocationInfo, MemberIdentity, TenantIdentity,
};
use serde::{Deserialize, Serialize};

/// Bumped when a frame's meaning changes in a way an older peer would
/// misread. The runtime refuses a host whose protocol it does not know.
pub const PROTOCOL_VERSION: u32 = 2;

/// Precise, stable error codes. The Rust host maps them onto its own dispatch
/// errors, so a caller can tell "this function does not exist" from "the
/// module timed out" without parsing prose.
pub mod codes {
    pub const BAD_REQUEST: &str = "bad_request";
    pub const UNSUPPORTED: &str = "unsupported";
    pub const FRAME_TOO_LARGE: &str = "frame_too_large";
    pub const INVALID_ARTIFACT: &str = "invalid_artifact";
    pub const ARTIFACT_HASH_MISMATCH: &str = "artifact_hash_mismatch";
    pub const MODULE_LOAD_FAILED: &str = "module_load_failed";
    pub const GENERATION_CONFLICT: &str = "generation_conflict";
    pub const UNKNOWN_GENERATION: &str = "unknown_generation";
    pub const MODULE_NOT_LOADED: &str = "module_not_loaded";
    pub const FUNCTION_NOT_FOUND: &str = "function_not_found";
    pub const WRONG_FUNCTION_KIND: &str = "wrong_function_kind";
    pub const INVALID_ARGS: &str = "invalid_args";
    pub const INVALID_RESULT: &str = "invalid_result";
    pub const BUDGET_EXCEEDED: &str = "budget_exceeded";
    pub const EXECUTION_FAILED: &str = "execution_failed";
    pub const CANCELLED: &str = "cancelled";
    pub const SHUTTING_DOWN: &str = "shutting_down";
    pub const HOST_CALL_FAILED: &str = "host_call_failed";
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireError {
    pub code: String,
    pub message: String,
    /// True when the same request could succeed later (a drain, an overload).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retryable: bool,
}

impl WireError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
            retryable: false,
        }
    }

    pub fn retryable(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
            retryable: true,
        }
    }
}

/// Frames the Gonvex runtime sends.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
// Frames are decoded once and dispatched immediately; boxing individual wire
// variants would add protocol plumbing without reducing retained host memory.
#[allow(clippy::large_enum_variant)]
pub enum ClientFrame {
    // `rename_all` on the enum names the variants; struct-variant fields need
    // their own annotation, which is why every multi-word field carries one.
    #[serde(rename_all = "camelCase")]
    Request {
        id: u64,
        #[serde(default)]
        deadline_unix_ms: Option<u64>,
        payload: RequestOp,
    },
    /// A host call this process asked for, answered.
    HostResponse {
        id: u64,
        #[serde(default)]
        value: serde_json::Value,
    },
    HostError {
        id: u64,
        error: WireError,
    },
    /// The caller gave up on a request; the invocation is abandoned and its
    /// isolate retired rather than left running for a result nobody wants.
    Cancel {
        id: u64,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "camelCase")]
#[allow(clippy::large_enum_variant)]
pub enum RequestOp {
    Ping,
    Load(LoadRequest),
    Activate(ActivateRequest),
    Describe(DescribeRequest),
    Invoke(InvokeRequest),
    Unload(UnloadRequest),
    Shutdown(ShutdownRequest),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadRequest {
    pub module_id: String,
    /// Absent lets the host allocate the next generation, which is what the
    /// runtime that does not track host state should do.
    #[serde(default)]
    pub generation: Option<u64>,
    pub artifact: ModuleArtifactWire,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivateRequest {
    pub module_id: String,
    pub generation: u64,
    /// How long a retired generation may keep finishing its calls before the
    /// host stops waiting on the reaper.
    #[serde(default)]
    pub drain_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeRequest {
    pub module_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnloadRequest {
    pub module_id: String,
    #[serde(default)]
    pub drain_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutdownRequest {
    #[serde(default)]
    pub grace_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvokeRequest {
    pub module_id: String,
    /// The generation the caller believes is active. It is advisory: a newer
    /// generation serves the call, and an older host reports that it has not
    /// caught up instead of running the wrong code.
    #[serde(default)]
    pub generation: Option<u64>,
    pub function: String,
    pub kind: String,
    /// JSON text; the module receives exactly these bytes.
    #[serde(default)]
    pub args: String,
    pub context: InvocationContextWire,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitiesWire {
    #[serde(default)]
    pub db_read: bool,
    #[serde(default)]
    pub db_write: bool,
    #[serde(default)]
    pub action_outbox: bool,
    #[serde(default)]
    pub action_tools: bool,
    #[serde(default)]
    pub scheduler: bool,
    #[serde(default)]
    pub network: bool,
    #[serde(default)]
    pub storage: bool,
    #[serde(default)]
    pub sandbox: bool,
    #[serde(default)]
    pub secrets: bool,
    #[serde(default)]
    pub functions: bool,
}

impl From<CapabilitiesWire> for Capabilities {
    fn from(wire: CapabilitiesWire) -> Self {
        Self {
            db_read: wire.db_read,
            db_write: wire.db_write,
            action_outbox: wire.action_outbox,
            action_tools: wire.action_tools,
            scheduler: wire.scheduler,
            network: wire.network,
            storage: wire.storage,
            sandbox: wire.sandbox,
            secrets: wire.secrets,
            functions: wire.functions,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationContextWire {
    #[serde(default)]
    pub intent_entropy: Option<String>,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub tenant: Option<TenantIdentity>,
    #[serde(default)]
    pub account: Option<AccountIdentity>,
    #[serde(default)]
    pub member: Option<MemberIdentity>,
    #[serde(default)]
    pub permissions: serde_json::Value,
    #[serde(default)]
    pub invocation: InvocationInfo,
    #[serde(default)]
    pub nesting_depth: u8,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub action_tools: Vec<String>,
    #[serde(default)]
    pub capabilities: CapabilitiesWire,
    #[serde(default)]
    pub now_unix_ms: u64,
    #[serde(default)]
    pub deadline_unix_ms: Option<u64>,
}

impl InvocationContextWire {
    pub fn into_context(self, generation: u64, request_deadline: Option<u64>) -> InvocationContext {
        // The tighter of the request deadline and the context deadline wins:
        // neither side may lengthen a budget the other set.
        let deadline_unix_ms = match (self.deadline_unix_ms, request_deadline) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
        InvocationContext {
            intent_entropy: self.intent_entropy,
            project_id: self.project_id,
            tenant_id: self.tenant_id,
            operation_id: self.operation_id,
            tenant: self.tenant,
            identity: IdentityContext {
                account: self.account,
                member: self.member,
                permissions: self.permissions,
            },
            invocation: self.invocation,
            nesting_depth: self.nesting_depth,
            environment: self.environment,
            action_tools: self.action_tools,
            generation,
            capabilities: self.capabilities.into(),
            now_unix_ms: match self.now_unix_ms {
                0 => unix_millis(SystemTime::now()),
                stamped => stamped,
            },
            deadline: deadline_unix_ms.map(from_unix_millis),
        }
    }
}

pub fn unix_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

pub fn from_unix_millis(millis: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis)
}

pub fn parse_kind(kind: &str) -> Option<FunctionKind> {
    match kind {
        "query" => Some(FunctionKind::Query),
        "reducer" => Some(FunctionKind::Reducer),
        "action" => Some(FunctionKind::Action),
        _ => None,
    }
}

pub fn kind_name(kind: &FunctionKind) -> &'static str {
    match kind {
        FunctionKind::Query => "query",
        FunctionKind::Reducer => "reducer",
        FunctionKind::Action => "action",
    }
}

/// The module payload as the Gonvex runtime holds it: the manifest's declarative
/// function metadata plus the bundled JavaScript, base64 encoded with the hash
/// the build recorded. The host verifies the hash before it evaluates anything.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleArtifactWire {
    #[serde(default)]
    pub language: String,
    #[serde(default)]
    pub entrypoint: String,
    /// Hash of the whole artifact, carried for logging and cache identity.
    #[serde(default)]
    pub hash: String,
    pub javascript: JavaScriptWire,
    #[serde(default)]
    pub functions: Vec<FunctionWire>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JavaScriptWire {
    #[serde(default)]
    pub path: String,
    /// Lowercase hex SHA-256 of the decoded bundle.
    pub hash: String,
    /// Base64-encoded UTF-8 ESM bundle.
    pub code: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionWire {
    pub path: String,
    pub kind: String,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub delivery: Option<String>,
    /// The exported binding and the declared handler name. Both are recorded
    /// on the contract so the engine resolves the export the build meant.
    #[serde(default)]
    pub handler: Option<String>,
    #[serde(default)]
    pub export: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub args: Option<serde_json::Value>,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

/// Frames this process sends.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerFrame {
    /// Sent once per connection so the client can check protocol agreement
    /// before it publishes a module.
    Ready {
        protocol: u32,
        version: String,
    },
    Response {
        id: u64,
        payload: ResponsePayload,
    },
    Error {
        id: u64,
        error: WireError,
    },
    /// A host operation requested while `invocation` is still running.
    HostCall {
        id: u64,
        invocation: u64,
        payload: HostCallFrame,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "camelCase")]
pub enum ResponsePayload {
    Pong {
        protocol: u32,
        version: String,
    },
    #[serde(rename_all = "camelCase")]
    Loaded {
        module_id: String,
        generation: u64,
        functions: Vec<FunctionSummary>,
    },
    #[serde(rename_all = "camelCase")]
    Activated {
        module_id: String,
        generation: u64,
        retired: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    Described {
        module_id: String,
        generation: Option<u64>,
        functions: Vec<FunctionSummary>,
    },
    Invoked {
        /// JSON text of the handler's return value.
        value: String,
    },
    #[serde(rename_all = "camelCase")]
    Unloaded {
        module_id: String,
        drained: bool,
    },
    ShuttingDown {
        drained: bool,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionSummary {
    pub path: String,
    pub kind: String,
    pub internal: bool,
    pub delivery: Option<String>,
}

/// Host calls, named one by one. Values travel as JSON, never as SQL text a
/// module built: `dbInsert`/`dbUpdate`/`dbDelete` name a table, a key and an
/// object, and the Rust host quotes the identifiers and binds the values.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum HostCallFrame {
    DbQuery {
        statement: String,
        parameters: serde_json::Value,
    },
    DbInsert {
        table: String,
        row: serde_json::Value,
        #[serde(default, rename = "generatedId")]
        generated_id: Option<String>,
    },
    DbUpdate {
        table: String,
        key: String,
        id: serde_json::Value,
        patch: serde_json::Value,
    },
    DbDelete {
        table: String,
        key: String,
        id: serde_json::Value,
    },
    DbDeleteMany {
        table: String,
        key: String,
        ids: serde_json::Value,
    },
    ActionEnqueue {
        function: String,
        args: serde_json::Value,
    },
    ToolInvoke {
        tool: String,
        args: serde_json::Value,
    },
    FunctionInvoke {
        path: String,
        args: serde_json::Value,
        #[serde(rename = "artifactHash")]
        artifact_hash: String,
    },
    ScheduleAfter {
        #[serde(rename = "delayMs")]
        delay_ms: u64,
        function: String,
        args: serde_json::Value,
    },
    ScheduleAt {
        #[serde(rename = "atUnixMs")]
        at_unix_ms: u64,
        function: String,
        args: serde_json::Value,
    },
    Fetch {
        request: serde_json::Value,
    },
    Storage {
        operation: String,
        payload: serde_json::Value,
    },
    Sandbox {
        operation: String,
        payload: serde_json::Value,
    },
}

impl HostCallFrame {
    /// Lifts an ABI host call onto the wire. The ABI moves payloads as JSON
    /// bytes so it stays engine-neutral; the transport re-parses them once so
    /// frames stay readable JSON rather than byte arrays.
    pub fn from_host_call(call: HostCall) -> Result<Self, String> {
        let decode = |bytes: Vec<u8>, field: &str| -> Result<serde_json::Value, String> {
            if bytes.is_empty() {
                return Ok(serde_json::Value::Null);
            }
            serde_json::from_slice(&bytes)
                .map_err(|err| format!("host call {field} is not valid JSON: {err}"))
        };
        Ok(match call {
            HostCall::DbQuery {
                statement,
                parameters,
            } => Self::DbQuery {
                statement,
                parameters: decode(parameters, "parameters")?,
            },
            HostCall::DbInsert { table, row, generated_id } => Self::DbInsert {
                table,
                generated_id,
                row: decode(row, "row")?,
            },
            HostCall::DbUpdate {
                table,
                key,
                id,
                patch,
            } => Self::DbUpdate {
                table,
                key,
                id: decode(id, "id")?,
                patch: decode(patch, "patch")?,
            },
            HostCall::DbDelete { table, key, id } => Self::DbDelete {
                table,
                key,
                id: decode(id, "id")?,
            },
            HostCall::DbDeleteMany { table, key, ids } => Self::DbDeleteMany {
                table,
                key,
                ids: decode(ids, "ids")?,
            },
            HostCall::ActionEnqueue { function, args } => Self::ActionEnqueue {
                function,
                args: decode(args, "args")?,
            },
            HostCall::ToolInvoke { tool, args } => Self::ToolInvoke {
                tool,
                args: decode(args, "args")?,
            },
            HostCall::FunctionInvoke {
                path,
                args,
                artifact_hash,
            } => Self::FunctionInvoke {
                path,
                args: decode(args, "args")?,
                artifact_hash,
            },
            HostCall::ScheduleAfter {
                delay_ms,
                function,
                args,
            } => Self::ScheduleAfter {
                delay_ms,
                function,
                args: decode(args, "args")?,
            },
            HostCall::ScheduleAt {
                at_unix_ms,
                function,
                args,
            } => Self::ScheduleAt {
                at_unix_ms,
                function,
                args: decode(args, "args")?,
            },
            HostCall::Fetch { request } => Self::Fetch {
                request: decode(request, "request")?,
            },
            HostCall::Storage { operation, payload } => Self::Storage {
                operation,
                payload: decode(payload, "payload")?,
            },
            HostCall::Sandbox { operation, payload } => Self::Sandbox {
                operation,
                payload: decode(payload, "payload")?,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_allocation_survives_the_module_host_wire() {
        let frame = HostCallFrame::from_host_call(HostCall::DbInsert {
            table: "messages".to_owned(), row: br#"{"id":"explicit"}"#.to_vec(),
            generated_id: Some("intent-id".to_owned()),
        }).expect("insert encodes");
        let encoded = serde_json::to_value(frame).unwrap();
        assert_eq!(encoded["generatedId"], "intent-id");
        assert_eq!(encoded["row"]["id"], "explicit");
        assert!(encoded["row"].get("_id").is_none());
    }

    #[test]
    fn action_enqueue_host_call_is_encoded_for_the_wire() {
        let frame = HostCallFrame::from_host_call(HostCall::ActionEnqueue {
            function: "notifications.send".to_owned(),
            args: br#"{"taskId":"task-123","kind":"started"}"#.to_vec(),
        })
        .expect("action enqueue payload should be valid JSON");

        let encoded = serde_json::to_value(frame).expect("host call frame should serialize");
        assert_eq!(encoded["kind"], "actionEnqueue");
        assert_eq!(encoded["function"], "notifications.send");
        assert_eq!(encoded["args"]["taskId"], "task-123");
        assert_eq!(encoded["args"]["kind"], "started");
    }

    #[test]
    fn scheduler_host_call_is_encoded_for_the_wire() {
        let frame = HostCallFrame::from_host_call(HostCall::ScheduleAfter {
            delay_ms: 2500,
            function: "reports.generate".to_owned(),
            args: br#"{"workspaceId":"workspace-1"}"#.to_vec(),
        })
        .expect("scheduler payload should be valid JSON");

        let encoded = serde_json::to_value(frame).expect("host call frame should serialize");
        assert_eq!(encoded["kind"], "scheduleAfter");
        assert_eq!(encoded["delayMs"], 2500);
        assert_eq!(encoded["function"], "reports.generate");
        assert_eq!(encoded["args"]["workspaceId"], "workspace-1");
    }

    #[test]
    fn sandbox_host_call_is_encoded_for_the_wire() {
        let frame = HostCallFrame::from_host_call(HostCall::Sandbox {
            operation: "create".to_owned(),
            payload: br#"{"ttlMs":30000}"#.to_vec(),
        })
        .expect("sandbox payload should be valid JSON");
        let encoded = serde_json::to_value(frame).expect("host call frame should serialize");
        assert_eq!(encoded["kind"], "sandbox");
        assert_eq!(encoded["operation"], "create");
        assert_eq!(encoded["payload"]["ttlMs"], 30000);
    }

    #[test]
    fn interactive_function_host_call_preserves_path_args_and_artifact_hash() {
        let frame = HostCallFrame::from_host_call(HostCall::FunctionInvoke {
            path: "tasks.start".to_owned(),
            args: br#"{"taskId":"task-1"}"#.to_vec(),
            artifact_hash: "artifact-1".to_owned(),
        })
        .expect("function invocation payload should be valid JSON");
        let encoded = serde_json::to_value(frame).expect("host call frame should serialize");
        assert_eq!(encoded["kind"], "functionInvoke");
        assert_eq!(encoded["path"], "tasks.start");
        assert_eq!(encoded["args"]["taskId"], "task-1");
        assert_eq!(encoded["artifactHash"], "artifact-1");
    }
}
