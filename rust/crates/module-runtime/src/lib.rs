//! Language-neutral Gonvex module ABI.
//!
//! The ABI deliberately transports JSON-shaped metadata and byte payloads.
//! The host never depends on TypeScript source or a particular database driver.
//! The current application-module runtime supports TypeScript through V8 only.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Root calls use depth zero. Each host-mediated nested function call advances
/// the depth and must remain bounded so runtimes cannot recurse forever.
pub const MAX_INVOCATION_DEPTH: u8 = 8;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ModuleGeneration = u64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ModuleLanguage {
    TypeScript,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FunctionKind {
    Query,
    Reducer,
    Action,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FunctionContract {
    pub path: String,
    pub kind: FunctionKind,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub delivery: Option<String>,
    #[serde(default)]
    pub args_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub result_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModuleManifest {
    pub module_id: String,
    pub generation: ModuleGeneration,
    pub language: ModuleLanguage,
    pub artifact_hash: String,
    #[serde(default)]
    pub functions: Vec<FunctionContract>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModuleArtifact {
    pub manifest: ModuleManifest,
    /// Bundled JavaScript ESM payload.
    /// The host treats this as opaque and verifies `artifact_hash` before load.
    pub payload: Vec<u8>,
}

/// The global human identity a call runs as. Tenant-local authorization lives
/// on `MemberIdentity`; nothing here is a credential.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountIdentity {
    pub id: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
}

/// The tenant-local identity and authorization subject.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberIdentity {
    pub id: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub permissions: serde_json::Value,
}

/// The tenant a call is scoped to. It deliberately carries no database URL or
/// credential: the host resolves the connection itself.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantIdentity {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityContext {
    #[serde(default)]
    pub account: Option<AccountIdentity>,
    #[serde(default)]
    pub member: Option<MemberIdentity>,
    /// The permission fingerprint the host resolved for this call. It is a
    /// projection of the member's permissions, never a capability by itself.
    #[serde(default)]
    pub permissions: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InvocationChannel {
    #[default]
    Ui,
    Agent,
    Api,
    Scheduler,
    System,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationProvenance {
    pub channel: InvocationChannel,
    pub root_channel: InvocationChannel,
    pub actor_account_id: Option<String>,
    pub actor_member_id: Option<String>,
    pub on_behalf_of_member_id: Option<String>,
    pub root_command_id: String,
    pub command_id: String,
    pub parent_command_id: Option<String>,
    pub agent_execution_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub artifact_hash: String,
    #[serde(default)]
    pub depth: u8,
    #[serde(default)]
    pub action_stack: Vec<String>,
    /// Root in-process execution deadline. The host copies this into the
    /// invocation envelope but never exposes it through `ctx.invocation` or
    /// persists it across an outbox or scheduler boundary.
    #[serde(skip)]
    pub deadline_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationInfo {
    pub channel: InvocationChannel,
    pub root_channel: InvocationChannel,
    pub actor_account_id: Option<String>,
    pub actor_member_id: Option<String>,
    pub on_behalf_of_member_id: Option<String>,
    pub root_command_id: String,
    pub command_id: String,
    pub parent_command_id: Option<String>,
    pub agent_execution_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub artifact_hash: String,
}

impl InvocationProvenance {
    pub fn public_info(&self) -> InvocationInfo {
        InvocationInfo {
            channel: self.channel,
            root_channel: self.root_channel,
            actor_account_id: self.actor_account_id.clone(),
            actor_member_id: self.actor_member_id.clone(),
            on_behalf_of_member_id: self.on_behalf_of_member_id.clone(),
            root_command_id: self.root_command_id.clone(),
            command_id: self.command_id.clone(),
            parent_command_id: self.parent_command_id.clone(),
            agent_execution_id: self.agent_execution_id.clone(),
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            tool_call_id: self.tool_call_id.clone(),
            artifact_hash: self.artifact_hash.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationContext {
    #[serde(default)]
    pub intent_entropy: Option<String>,
    pub project_id: String,
    pub tenant_id: String,
    pub operation_id: Option<String>,
    #[serde(default)]
    pub tenant: Option<TenantIdentity>,
    pub identity: IdentityContext,
    pub invocation: InvocationInfo,
    /// Host-only scheduling metadata. This is not exposed through
    /// `ctx.invocation`, but it keeps nested calls out of the root isolate pool.
    #[serde(default)]
    pub nesting_depth: u8,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub action_tools: Vec<String>,
    pub generation: ModuleGeneration,
    #[serde(default)]
    pub capabilities: Capabilities,
    /// Wall-clock start of the call, in milliseconds since the epoch. The host
    /// decides it so every engine reports the same `now` for one invocation.
    #[serde(default)]
    pub now_unix_ms: u64,
    #[serde(skip)]
    pub deadline: Option<SystemTime>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Capabilities {
    pub db_read: bool,
    pub db_write: bool,
    pub action_outbox: bool,
    pub action_tools: bool,
    pub scheduler: bool,
    pub network: bool,
    pub storage: bool,
    pub sandbox: bool,
    pub secrets: bool,
    pub functions: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Invocation {
    pub function: String,
    pub kind: FunctionKind,
    pub args: Vec<u8>,
    pub context: InvocationContext,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationResult {
    pub value: Vec<u8>,
    #[serde(default)]
    pub committed_revision: Option<u64>,
    #[serde(default)]
    pub origin_command_id: Option<String>,
}

/// The host operations a module may ask for. The vocabulary matches the
/// `@gonvex/module-sdk` context surface one to one — `ReadDB.query` plus
/// `WriteDB.insert/update/delete` plus `ReducerActions.enqueue` — so a table
/// write never travels as interpolated SQL text and external work is recorded
/// by the host-owned transaction. Payloads are JSON bytes; the host owns the
/// transaction, the identifier quoting, and the parameter binding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum HostCall {
    DbQuery {
        statement: String,
        /// JSON array of positional values bound as `$1..$n` by the host.
        parameters: Vec<u8>,
    },
    DbInsert {
        table: String,
        /// JSON object of column name to value.
        row: Vec<u8>,
        generated_id: Option<String>,
    },
    DbUpdate {
        table: String,
        /// Key column, defaulting to `id` when empty.
        key: String,
        /// JSON-encoded key value.
        id: Vec<u8>,
        /// JSON object of column name to value.
        patch: Vec<u8>,
    },
    DbDelete {
        table: String,
        key: String,
        id: Vec<u8>,
    },
    DbDeleteMany {
        table: String,
        key: String,
        ids: Vec<u8>,
    },
    ActionEnqueue {
        function: String,
        args: Vec<u8>,
    },
    ToolInvoke {
        tool: String,
        args: Vec<u8>,
    },
    FunctionInvoke {
        path: String,
        args: Vec<u8>,
        artifact_hash: String,
    },
    ScheduleAfter {
        delay_ms: u64,
        function: String,
        args: Vec<u8>,
    },
    ScheduleAt {
        at_unix_ms: u64,
        function: String,
        args: Vec<u8>,
    },
    Fetch {
        request: Vec<u8>,
    },
    Storage {
        operation: String,
        payload: Vec<u8>,
    },
    Sandbox {
        operation: String,
        payload: Vec<u8>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostResponse {
    pub value: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum HostError {
    #[error("capability {0} is not available to this invocation")]
    CapabilityDenied(&'static str),
    #[error("host call failed: {0}")]
    Failed(String),
}

impl HostCall {
    /// The capability name this call needs, used for both the check and the
    /// denial message a module sees.
    pub fn capability(&self) -> &'static str {
        match self {
            Self::DbQuery { .. } => "db_read",
            Self::DbInsert { .. }
            | Self::DbUpdate { .. }
            | Self::DbDelete { .. }
            | Self::DbDeleteMany { .. } => "db_write",
            Self::ActionEnqueue { .. } => "action_outbox",
            Self::ToolInvoke { .. } => "action_tools",
            Self::FunctionInvoke { .. } => "functions",
            Self::ScheduleAfter { .. } | Self::ScheduleAt { .. } => "scheduler",
            Self::Fetch { .. } => "network",
            Self::Storage { .. } => "storage",
            Self::Sandbox { .. } => "sandbox",
        }
    }

    pub fn check_capability(&self, capabilities: &Capabilities) -> Result<(), HostError> {
        let name = self.capability();
        let allowed = match name {
            "db_read" => capabilities.db_read,
            "db_write" => capabilities.db_write,
            "action_outbox" => capabilities.action_outbox,
            "action_tools" => capabilities.action_tools,
            "scheduler" => capabilities.scheduler,
            "network" => capabilities.network,
            "storage" => capabilities.storage,
            "sandbox" => capabilities.sandbox,
            "functions" => capabilities.functions,
            _ => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(HostError::CapabilityDenied(name))
        }
    }
}

/// Host operations are intentionally opaque to the module engine. The host
/// implementation owns the Postgres transaction, credentials, authorization,
/// and external service clients.
pub trait ModuleHost: Send + Sync {
    fn call<'a>(
        &'a self,
        context: &'a InvocationContext,
        call: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>>;
}

#[derive(Debug, Error)]
pub enum ModuleError {
    #[error("module function {0} is not registered")]
    FunctionNotFound(String),
    #[error("module function {0} has the wrong kind")]
    WrongFunctionKind(String),
    #[error("module artifact is invalid: {0}")]
    InvalidArtifact(String),
    #[error("module execution exceeded its budget: {0}")]
    BudgetExceeded(String),
    #[error("module engine is not available: {0}")]
    Unsupported(String),
    #[error("module arguments are invalid: {0}")]
    InvalidArguments(String),
    #[error("module result is invalid: {0}")]
    InvalidResult(String),
    #[error("module execution failed: {0}")]
    Execution(String),
}

/// Validate a JSON value against the portable schema emitted by the
/// TypeScript module SDK. Keeping this validator in the language-neutral ABI
/// crate gives every execution engine exactly the same contract semantics.
pub fn validate_portable_schema(schema: &Value, value: &Value) -> Result<(), String> {
    validate_at(schema, value, "$", false)
}

/// Validate the schema itself before a module generation is activated.
pub fn validate_portable_schema_definition(schema: &Value) -> Result<(), String> {
    validate_definition_at(schema, "$", false)
}

fn validate_definition_at(
    schema: &Value,
    path: &str,
    optional_allowed: bool,
) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| format!("{path}: schema must be an object"))?;
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{path}.kind: required string"))?;
    match kind {
        "string" => {
            if let Some(format) = object.get("format") {
                match format.as_str() {
                    Some("email" | "uri" | "uuid" | "datetime") => {}
                    _ => return Err(format!("{path}.format: unsupported string format")),
                }
            }
            validate_optional_nonnegative_integer(
                object.get("minLength"),
                &format!("{path}.minLength"),
            )?;
            validate_optional_nonnegative_integer(
                object.get("maxLength"),
                &format!("{path}.maxLength"),
            )?;
        }
        "number" => {
            if let Some(integer) = object.get("integer") {
                if !integer.is_boolean() {
                    return Err(format!("{path}.integer: must be a boolean"));
                }
            }
            validate_optional_number(object.get("minimum"), &format!("{path}.minimum"))?;
            validate_optional_number(object.get("maximum"), &format!("{path}.maximum"))?;
        }
        "boolean" | "null" | "any" => {}
        "id" => {
            if object
                .get("entity")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(format!("{path}.entity: required non-empty string"));
            }
        }
        "literal" => {
            if !object.contains_key("value") {
                return Err(format!("{path}.value: required"));
            }
        }
        "array" => validate_definition_at(
            object
                .get("items")
                .ok_or_else(|| format!("{path}.items: required"))?,
            &format!("{path}.items"),
            false,
        )?,
        "object" => {
            let fields = object
                .get("fields")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("{path}.fields: required object"))?;
            if let Some(allow_unknown) = object.get("allowUnknown") {
                if !allow_unknown.is_boolean() {
                    return Err(format!("{path}.allowUnknown: must be a boolean"));
                }
            }
            for (name, field) in fields {
                validate_definition_at(field, &format!("{path}.fields.{name}"), true)?;
            }
        }
        "record" => validate_definition_at(
            object
                .get("values")
                .ok_or_else(|| format!("{path}.values: required"))?,
            &format!("{path}.values"),
            false,
        )?,
        "optional" => {
            if !optional_allowed {
                return Err(format!(
                    "{path}: optional schemas are only valid as object fields"
                ));
            }
            validate_definition_at(
                object
                    .get("value")
                    .ok_or_else(|| format!("{path}.value: required"))?,
                &format!("{path}.value"),
                false,
            )?;
        }
        other => return Err(format!("{path}.kind: unsupported schema kind {other:?}")),
    }
    Ok(())
}

fn validate_at(
    schema: &Value,
    value: &Value,
    path: &str,
    optional_allowed: bool,
) -> Result<(), String> {
    validate_definition_at(schema, path, optional_allowed)?;
    let object = schema
        .as_object()
        .expect("definition validation guarantees object");
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .expect("definition validation guarantees kind");
    match kind {
        "any" => Ok(()),
        "string" => {
            let text = value
                .as_str()
                .ok_or_else(|| expected(path, "string", value))?;
            let length = text.chars().count() as u64;
            if let Some(minimum) = object.get("minLength").and_then(Value::as_u64) {
                if length < minimum {
                    return Err(format!("{path}: string length must be at least {minimum}"));
                }
            }
            if let Some(maximum) = object.get("maxLength").and_then(Value::as_u64) {
                if length > maximum {
                    return Err(format!("{path}: string length must be at most {maximum}"));
                }
            }
            Ok(())
        }
        "number" => {
            let number = value
                .as_f64()
                .ok_or_else(|| expected(path, "number", value))?;
            if object.get("integer").and_then(Value::as_bool) == Some(true) && number.fract() != 0.0
            {
                return Err(format!("{path}: expected integer"));
            }
            if let Some(minimum) = object.get("minimum").and_then(Value::as_f64) {
                if number < minimum {
                    return Err(format!("{path}: number must be at least {minimum}"));
                }
            }
            if let Some(maximum) = object.get("maximum").and_then(Value::as_f64) {
                if number > maximum {
                    return Err(format!("{path}: number must be at most {maximum}"));
                }
            }
            Ok(())
        }
        "boolean" if value.is_boolean() => Ok(()),
        "boolean" => Err(expected(path, "boolean", value)),
        "null" if value.is_null() => Ok(()),
        "null" => Err(expected(path, "null", value)),
        "id" if value.is_string() => Ok(()),
        "id" => Err(expected(path, "entity id string", value)),
        "literal" => {
            let literal = object
                .get("value")
                .expect("definition validation guarantees literal");
            if literal == value {
                Ok(())
            } else {
                Err(format!("{path}: expected literal {literal}"))
            }
        }
        "array" => {
            let values = value
                .as_array()
                .ok_or_else(|| expected(path, "array", value))?;
            let item_schema = object
                .get("items")
                .expect("definition validation guarantees items");
            for (index, item) in values.iter().enumerate() {
                validate_at(item_schema, item, &format!("{path}[{index}]"), false)?;
            }
            Ok(())
        }
        "object" => {
            let input = value
                .as_object()
                .ok_or_else(|| expected(path, "object", value))?;
            let fields = object
                .get("fields")
                .and_then(Value::as_object)
                .expect("definition validation guarantees fields");
            for (name, field_schema) in fields {
                let field_kind = field_schema.get("kind").and_then(Value::as_str);
                match input.get(name) {
                    Some(field_value) => {
                        let effective = if field_kind == Some("optional") {
                            field_schema
                                .get("value")
                                .expect("definition validation guarantees optional value")
                        } else {
                            field_schema
                        };
                        validate_at(effective, field_value, &format!("{path}.{name}"), false)?;
                    }
                    None if field_kind == Some("optional") => {}
                    None => return Err(format!("{path}.{name}: required field is missing")),
                }
            }
            if object.get("allowUnknown").and_then(Value::as_bool) != Some(true) {
                for name in input.keys() {
                    if !fields.contains_key(name) {
                        return Err(format!("{path}.{name}: unknown field"));
                    }
                }
            }
            Ok(())
        }
        "record" => {
            let input = value
                .as_object()
                .ok_or_else(|| expected(path, "object", value))?;
            let value_schema = object
                .get("values")
                .expect("definition validation guarantees values");
            for (name, item) in input {
                validate_at(value_schema, item, &format!("{path}.{name}"), false)?;
            }
            Ok(())
        }
        "optional" => validate_at(
            object
                .get("value")
                .expect("definition validation guarantees optional value"),
            value,
            path,
            false,
        ),
        _ => Err(expected(path, kind, value)),
    }
}

fn validate_optional_nonnegative_integer(value: Option<&Value>, path: &str) -> Result<(), String> {
    if value.is_some_and(|value| value.as_u64().is_none()) {
        return Err(format!("{path}: must be a non-negative integer"));
    }
    Ok(())
}

fn validate_optional_number(value: Option<&Value>, path: &str) -> Result<(), String> {
    if value.is_some_and(|value| !value.is_number()) {
        return Err(format!("{path}: must be a number"));
    }
    Ok(())
}

fn expected(path: &str, expected: &str, value: &Value) -> String {
    format!("{path}: expected {expected}, received {}", json_type(value))
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// The only runtime contract the Rust host needs from an application module.
/// The production implementation is the bounded V8 engine.
pub trait ModuleEngine: Send + Sync {
    fn manifest(&self) -> &ModuleManifest;

    fn invoke<'a>(
        &'a self,
        host: &'a dyn ModuleHost,
        invocation: Invocation,
    ) -> BoxFuture<'a, Result<InvocationResult, ModuleError>>;
}

pub fn remaining_budget(context: &InvocationContext) -> Option<Duration> {
    context
        .deadline
        .and_then(|deadline| deadline.duration_since(SystemTime::now()).ok())
}

#[cfg(test)]
mod portable_schema_tests {
    use serde_json::json;

    use super::{validate_portable_schema, validate_portable_schema_definition};

    #[test]
    fn validates_nested_objects_and_optional_fields() {
        let schema = json!({
            "kind": "object",
            "fields": {
                "taskId": { "kind": "id", "entity": "tasks" },
                "count": { "kind": "number", "integer": true, "minimum": 1 },
                "note": { "kind": "optional", "value": { "kind": "string", "maxLength": 12 } }
            }
        });
        validate_portable_schema_definition(&schema).unwrap();
        validate_portable_schema(&schema, &json!({ "taskId": "task_1", "count": 2 })).unwrap();
        assert_eq!(
            validate_portable_schema(&schema, &json!({ "taskId": "task_1", "count": 0 }))
                .unwrap_err(),
            "$.count: number must be at least 1"
        );
        assert_eq!(
            validate_portable_schema(
                &schema,
                &json!({ "taskId": "task_1", "count": 2, "extra": true })
            )
            .unwrap_err(),
            "$.extra: unknown field"
        );
    }

    #[test]
    fn rejects_malformed_and_top_level_optional_schemas() {
        assert!(validate_portable_schema_definition(&json!({ "kind": "mystery" })).is_err());
        assert!(validate_portable_schema_definition(&json!({
            "kind": "optional",
            "value": { "kind": "string" }
        }))
        .is_err());
    }
}
