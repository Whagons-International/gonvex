//! The wire contract between the Rust host and the isolate bootstrap.
//!
//! Everything crossing into JavaScript is JSON text: the adapter never hands a
//! live host handle, a database connection, or a tenant identifier that the
//! host has not already decided to disclose. Everything coming back is parsed
//! into an explicit enum, so a module cannot widen its own surface by returning
//! an unexpected shape.

use gonvex_module_runtime::{
    AccountIdentity, Capabilities, FunctionKind, HostCall, InvocationContext, MemberIdentity,
    ModuleError, TenantIdentity,
};
use serde::{Deserialize, Serialize};

/// The capabilities a function kind may ever reach, before the host's own
/// grant is applied. This is the structural half of Query/Reducer/Action
/// separation: a query has no path to a write no matter what the host granted,
/// and an action has no direct database access at all — it goes through a
/// reducer, which is where the host owns the transaction.
fn structural_capabilities(kind: &FunctionKind) -> Capabilities {
    match kind {
        FunctionKind::Query => Capabilities {
            db_read: true,
            ..Capabilities::default()
        },
        FunctionKind::Reducer => Capabilities {
            db_read: true,
            db_write: true,
            action_outbox: true,
            scheduler: true,
            ..Capabilities::default()
        },
        // Actions run outside the transaction: they may reach the network and
        // storage, and mutate only by calling a reducer.
        FunctionKind::Action => Capabilities {
            action_tools: true,
            functions: true,
            scheduler: true,
            network: true,
            storage: true,
            sandbox: true,
            secrets: true,
            ..Capabilities::default()
        },
    }
}

/// Intersects the kind's structural reach with the grant on the invocation.
pub(crate) fn effective_capabilities(kind: &FunctionKind, granted: &Capabilities) -> Capabilities {
    let structural = structural_capabilities(kind);
    Capabilities {
        db_read: structural.db_read && granted.db_read,
        db_write: structural.db_write && granted.db_write,
        action_outbox: structural.action_outbox && granted.action_outbox,
        action_tools: structural.action_tools && granted.action_tools,
        scheduler: structural.scheduler && granted.scheduler,
        network: structural.network && granted.network,
        storage: structural.storage && granted.storage,
        sandbox: structural.sandbox && granted.sandbox,
        secrets: structural.secrets && granted.secrets,
        functions: structural.functions && granted.functions,
    }
}

pub(crate) fn kind_name(kind: &FunctionKind) -> &'static str {
    match kind {
        FunctionKind::Query => "query",
        FunctionKind::Reducer => "reducer",
        FunctionKind::Action => "action",
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CapabilityFlags {
    db_read: bool,
    db_write: bool,
    action_outbox: bool,
    action_tools: bool,
    scheduler: bool,
    network: bool,
    storage: bool,
    sandbox: bool,
    secrets: bool,
    functions: bool,
}

impl From<&Capabilities> for CapabilityFlags {
    fn from(capabilities: &Capabilities) -> Self {
        Self {
            db_read: capabilities.db_read,
            db_write: capabilities.db_write,
            action_outbox: capabilities.action_outbox,
            action_tools: capabilities.action_tools,
            scheduler: capabilities.scheduler,
            network: capabilities.network,
            storage: capabilities.storage,
            sandbox: capabilities.sandbox,
            secrets: capabilities.secrets,
            functions: capabilities.functions,
        }
    }
}

/// Identity is the only part of the invocation context the module sees, and it
/// is shaped exactly like `@gonvex/module-sdk`'s `AuthContext & TenantContext`.
/// The database URL, credentials, and the host's own routing stay behind the
/// host call boundary, so module code can never be the place tenancy is
/// enforced.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IdentityView<'a> {
    account: Option<&'a AccountIdentity>,
    member: Option<&'a MemberIdentity>,
    tenant: Option<&'a TenantIdentity>,
    permissions: &'a serde_json::Value,
}

impl<'a> From<&'a InvocationContext> for IdentityView<'a> {
    fn from(context: &'a InvocationContext) -> Self {
        Self {
            account: context.identity.account.as_ref(),
            member: context.identity.member.as_ref(),
            tenant: context.tenant.as_ref(),
            permissions: &context.identity.permissions,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DispatchRequest<'a> {
    pub(crate) function: &'a str,
    pub(crate) kind: &'static str,
    pub(crate) capabilities: CapabilityFlags,
    pub(crate) identity: IdentityView<'a>,
    pub(crate) invocation: &'a gonvex_module_runtime::InvocationInfo,
    pub(crate) environment: &'a std::collections::BTreeMap<String, String>,
    pub(crate) action_tools: &'a [String],
    /// Wall-clock milliseconds the host stamped on the invocation, surfaced as
    /// `ctx.now` so a handler never reads a clock the host does not control.
    pub(crate) now: u64,
    pub(crate) max_result_bytes: usize,
}

/// The host operations a module may ask for, named one by one so an unknown
/// `kind` is a parse error rather than a call the host has to interpret. Each
/// variant lowers into exactly one `HostCall`.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(crate) enum HostCallRequest {
    DbQuery {
        statement: String,
        #[serde(default)]
        parameters: serde_json::Value,
    },
    DbInsert {
        table: String,
        #[serde(default)]
        row: serde_json::Value,
    },
    DbUpdate {
        table: String,
        #[serde(default)]
        key: Option<String>,
        #[serde(default)]
        id: serde_json::Value,
        #[serde(default)]
        patch: serde_json::Value,
    },
    DbDelete {
        table: String,
        #[serde(default)]
        key: Option<String>,
        #[serde(default)]
        id: serde_json::Value,
    },
    ActionEnqueue {
        function: String,
        #[serde(default)]
        args: serde_json::Value,
    },
    ToolInvoke {
        tool: String,
        #[serde(default)]
        args: serde_json::Value,
    },
    FunctionInvoke {
        path: String,
        #[serde(default)]
        args: serde_json::Value,
        #[serde(rename = "artifactHash")]
        artifact_hash: String,
    },
    ScheduleAfter {
        #[serde(rename = "delayMs")]
        delay_ms: u64,
        function: String,
        #[serde(default)]
        args: serde_json::Value,
    },
    ScheduleAt {
        #[serde(rename = "atUnixMs")]
        at_unix_ms: u64,
        function: String,
        #[serde(default)]
        args: serde_json::Value,
    },
    Fetch {
        #[serde(default)]
        request: serde_json::Value,
    },
    Storage {
        operation: String,
        #[serde(default)]
        payload: serde_json::Value,
    },
    Sandbox {
        operation: String,
        #[serde(default)]
        payload: serde_json::Value,
    },
}

impl HostCallRequest {
    pub(crate) fn into_host_call(self) -> Result<HostCall, String> {
        let encode = |value: serde_json::Value| {
            serde_json::to_vec(&value)
                .map_err(|err| format!("host call payload is not encodable: {err}"))
        };
        // An empty key column means "the host's default", which keeps the
        // module from having to know the primary key of every table.
        let key_column = |key: Option<String>| key.unwrap_or_default();
        Ok(match self {
            Self::DbQuery {
                statement,
                parameters,
            } => HostCall::DbQuery {
                statement,
                parameters: encode(parameters)?,
            },
            Self::DbInsert { table, row } => HostCall::DbInsert {
                table,
                row: encode(row)?,
            },
            Self::DbUpdate {
                table,
                key,
                id,
                patch,
            } => HostCall::DbUpdate {
                table,
                key: key_column(key),
                id: encode(id)?,
                patch: encode(patch)?,
            },
            Self::DbDelete { table, key, id } => HostCall::DbDelete {
                table,
                key: key_column(key),
                id: encode(id)?,
            },
            Self::ActionEnqueue { function, args } => HostCall::ActionEnqueue {
                function,
                args: encode(args)?,
            },
            Self::ToolInvoke { tool, args } => HostCall::ToolInvoke {
                tool,
                args: encode(args)?,
            },
            Self::FunctionInvoke {
                path,
                args,
                artifact_hash,
            } => HostCall::FunctionInvoke {
                path,
                args: encode(args)?,
                artifact_hash,
            },
            Self::ScheduleAfter {
                delay_ms,
                function,
                args,
            } => HostCall::ScheduleAfter {
                delay_ms,
                function,
                args: encode(args)?,
            },
            Self::ScheduleAt {
                at_unix_ms,
                function,
                args,
            } => HostCall::ScheduleAt {
                at_unix_ms,
                function,
                args: encode(args)?,
            },
            Self::Fetch { request } => HostCall::Fetch {
                request: encode(request)?,
            },
            Self::Storage { operation, payload } => HostCall::Storage {
                operation,
                payload: encode(payload)?,
            },
            Self::Sandbox { operation, payload } => HostCall::Sandbox {
                operation,
                payload: encode(payload)?,
            },
        })
    }
}

/// What the op hands back to the bootstrap. Denial and failure are separate so
/// module code can tell "you were never allowed to do this" from "the host
/// tried and it broke".
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum HostCallOutcome {
    Ok { value: String },
    Denied { message: String },
    Failed { message: String },
}

impl HostCallOutcome {
    /// Encoding here rather than through serde_v8 keeps the op's return type a
    /// plain string, so the module boundary stays one shape in both directions.
    pub(crate) fn encode(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            r#"{"status":"failed","message":"host call outcome could not be encoded"}"#.to_owned()
        })
    }

    pub(crate) fn ok(value: String) -> Self {
        Self::Ok { value }
    }

    pub(crate) fn denied(message: String) -> Self {
        Self::Denied { message }
    }

    pub(crate) fn failed(message: String) -> Self {
        Self::Failed { message }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum DispatchErrorKind {
    /// The manifest and the bundle disagree: the export is not callable.
    Dispatch,
    /// The module's own handler threw.
    Handler,
    /// The handler returned something JSON cannot represent.
    Result,
    /// The handler returned more than the configured result budget.
    ResultSize,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum DispatchOutcome {
    Ok {
        value: String,
    },
    Error {
        kind: DispatchErrorKind,
        message: String,
        #[serde(default)]
        stack: Option<String>,
    },
}

/// Turns the dispatcher's envelope into the host's error vocabulary. The
/// envelope is JSON text rather than a thrown value so a handler failure and an
/// engine failure never look alike.
pub(crate) fn decode_result(envelope: &str) -> Result<Vec<u8>, ModuleError> {
    let outcome: DispatchOutcome = serde_json::from_str(envelope).map_err(|err| {
        ModuleError::Execution(format!(
            "module dispatcher returned an unreadable envelope: {err}"
        ))
    })?;
    match outcome {
        DispatchOutcome::Ok { value } => Ok(value.into_bytes()),
        DispatchOutcome::Error {
            kind,
            message,
            stack,
        } => Err(match kind {
            DispatchErrorKind::Dispatch => ModuleError::InvalidArtifact(message),
            DispatchErrorKind::Handler => ModuleError::Execution(stack.unwrap_or(message)),
            DispatchErrorKind::Result => ModuleError::Execution(message),
            DispatchErrorKind::ResultSize => ModuleError::BudgetExceeded(message),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_is_structurally_denied_to_queries() {
        let granted = Capabilities {
            scheduler: true,
            ..Capabilities::default()
        };
        assert!(!effective_capabilities(&FunctionKind::Query, &granted).scheduler);
        assert!(effective_capabilities(&FunctionKind::Reducer, &granted).scheduler);
        assert!(effective_capabilities(&FunctionKind::Action, &granted).scheduler);
    }

    #[test]
    fn scheduler_request_lowers_to_typed_host_call() {
        let request: HostCallRequest = serde_json::from_value(serde_json::json!({
            "kind": "scheduleAfter",
            "delayMs": 2500,
            "function": "reports.generate",
            "args": {"workspaceId": "workspace-1"}
        }))
        .expect("scheduler request should decode");
        let call = request
            .into_host_call()
            .expect("scheduler request should lower");
        assert_eq!(call.capability(), "scheduler");
        assert!(matches!(
            call,
            HostCall::ScheduleAfter { delay_ms: 2500, function, .. }
                if function == "reports.generate"
        ));
    }

    #[test]
    fn sandbox_is_structurally_available_only_to_actions() {
        let granted = Capabilities {
            sandbox: true,
            ..Capabilities::default()
        };
        assert!(!effective_capabilities(&FunctionKind::Query, &granted).sandbox);
        assert!(!effective_capabilities(&FunctionKind::Reducer, &granted).sandbox);
        assert!(effective_capabilities(&FunctionKind::Action, &granted).sandbox);
    }

    #[test]
    fn interactive_function_invocation_is_structurally_action_only() {
        let granted = Capabilities {
            functions: true,
            ..Capabilities::default()
        };
        assert!(!effective_capabilities(&FunctionKind::Query, &granted).functions);
        assert!(!effective_capabilities(&FunctionKind::Reducer, &granted).functions);
        assert!(effective_capabilities(&FunctionKind::Action, &granted).functions);

        let request: HostCallRequest = serde_json::from_value(serde_json::json!({
            "kind":"functionInvoke",
            "path":"tasks.start",
            "args":{"taskId":"task-1"},
            "artifactHash":"artifact-1"
        }))
        .expect("function invocation should decode");
        let call = request
            .into_host_call()
            .expect("function invocation should lower");
        assert_eq!(call.capability(), "functions");
        assert!(matches!(
            call,
            HostCall::FunctionInvoke { path, artifact_hash, .. }
                if path == "tasks.start" && artifact_hash == "artifact-1"
        ));
    }
}
