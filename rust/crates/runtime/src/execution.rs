//! Tenant Query and Reducer execution through the shared TypeScript host.

use std::collections::BTreeMap;
use std::time::SystemTime;

use gonvex_module_host::protocol::{
    unix_millis, CapabilitiesWire, InvocationContextWire, InvokeRequest,
};
use gonvex_module_runtime::{
    validate_portable_schema, AccountIdentity, InvocationChannel, InvocationProvenance,
    MemberIdentity, TenantIdentity,
};
use gonvex_postgres::{Account, Member, SessionIdentity, TenantRoute, TenantSession};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;

use crate::action_calls::ActionHostCalls;
use crate::host_calls::{DatabaseCapability, DatabaseHostCalls};
use crate::modules::ModuleCallLease;
use crate::Runtime;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("project {0:?} has no active TypeScript module")]
    ModuleMissing(String),
    #[error("function {0:?} is not registered")]
    FunctionMissing(String),
    #[error("function {path:?} is a {actual}, not a {expected}")]
    WrongKind {
        path: String,
        expected: &'static str,
        actual: String,
    },
    #[error("internal function {0:?} cannot be called by a client")]
    InternalFunction(String),
    #[error(transparent)]
    Database(#[from] gonvex_postgres::DatabaseError),
    #[error(transparent)]
    ModuleHost(#[from] crate::module_host::ModuleHostError),
    #[error("module database capability failed: {0}")]
    HostCall(String),
    #[error(
        "Action {path:?} requires a capability the Rust runtime has not configured: {capability}"
    )]
    CapabilityUnavailable { path: String, capability: String },
    #[error("STALE_AGENT_CATALOG: expected artifact {expected:?}, active artifact is {active:?}")]
    StaleCatalog { expected: String, active: String },
    #[error("function {0:?} is not classified as interactive")]
    NotInteractive(String),
    #[error("function {path:?} arguments do not match its schema: {message}")]
    InvalidArguments { path: String, message: String },
    #[error("function {path:?} result does not match its schema: {message}")]
    InvalidResult { path: String, message: String },
    #[error("nested function invocation exceeded the maximum depth")]
    InvocationDepth,
    #[error("recursive Action invocation detected for {0:?}")]
    RecursiveAction(String),
}

pub struct ReducerExecution {
    pub value: Value,
    pub committed_revision: Option<u64>,
}

#[derive(Default)]
pub(crate) struct ExecutionAccess {
    pub allow_internal: bool,
    pub provenance: Option<InvocationProvenance>,
    pub module: Option<ModuleCallLease>,
}

impl Runtime {
    pub async fn execute_tenant_query(
        &self,
        session: &TenantSession,
        path: &str,
        args: Value,
    ) -> Result<Value, ExecutionError> {
        self.execute_tenant_query_with_access(session, path, args, ExecutionAccess::default())
            .await
    }

    pub(crate) async fn execute_tenant_query_with_access(
        &self,
        session: &TenantSession,
        path: &str,
        args: Value,
        access: ExecutionAccess,
    ) -> Result<Value, ExecutionError> {
        let module = match access.module {
            Some(module) => module,
            None => self
                .inner
                .modules
                .project_for_call(&session.identity.project_id)
                .await
                .ok_or_else(|| {
                    ExecutionError::ModuleMissing(session.identity.project_id.clone())
                })?,
        };
        let definition = require_function(&module, path, "query", access.allow_internal)?;
        validate_portable_schema(&definition.args_schema, &args).map_err(|message| {
            ExecutionError::InvalidArguments {
                path: path.to_owned(),
                message,
            }
        })?;
        let delegated_agent_read = access
            .provenance
            .as_ref()
            .is_some_and(|provenance| provenance.channel == InvocationChannel::Agent);
        if !delegated_agent_read
            && !definition.delivery.is_empty()
            && definition.delivery != "oneShot"
        {
            return Err(ExecutionError::HostCall(format!(
                "query {path:?} is delivered as {} and cannot be called as a one-shot Query",
                definition.delivery
            )));
        }
        let plan = definition.live_query_plan.as_ref().ok_or_else(|| {
            ExecutionError::HostCall(format!(
                "one-shot query {path:?} requires a structured live query plan"
            ))
        })?;
        self.execute_structured_live_query(session, plan, &args)
            .await
            .map_err(|error| ExecutionError::HostCall(error.to_string()))
    }

    pub async fn execute_tenant_reducer(
        &self,
        session: &TenantSession,
        command_id: &str,
        idempotency_key: Option<&str>,
        path: &str,
        args: Value,
    ) -> Result<ReducerExecution, ExecutionError> {
        self.execute_tenant_reducer_with_access(
            session,
            command_id,
            idempotency_key,
            path,
            args,
            ExecutionAccess::default(),
        )
        .await
    }

    pub(crate) async fn execute_tenant_reducer_with_access(
        &self,
        session: &TenantSession,
        command_id: &str,
        idempotency_key: Option<&str>,
        path: &str,
        args: Value,
        access: ExecutionAccess,
    ) -> Result<ReducerExecution, ExecutionError> {
        let module = match access.module {
            Some(module) => module,
            None => self
                .inner
                .modules
                .project_for_call(&session.identity.project_id)
                .await
                .ok_or_else(|| {
                    ExecutionError::ModuleMissing(session.identity.project_id.clone())
                })?,
        };
        let definition = require_function(&module, path, "reducer", access.allow_internal)?;
        validate_portable_schema(&definition.args_schema, &args).map_err(|message| {
            ExecutionError::InvalidArguments {
                path: path.to_owned(),
                message,
            }
        })?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| ExecutionError::ModuleMissing(session.identity.project_id.clone()))?;
        let idempotency_key = idempotency_key.map(str::trim).filter(|key| !key.is_empty());
        if idempotency_key.is_some() {
            control.ensure_reducer_idempotency(&session.route).await?;
        }
        let mut transaction = control
            .begin_tenant_transaction(&session.route, false)
            .await?;
        transaction.set_command_id(command_id).await?;
        if let Some(key) = idempotency_key {
            let claimed = transaction
                .claim_reducer(&session.identity.account.id, key, path)
                .await?;
            if !claimed {
                transaction.rollback().await?;
                let value = control
                    .replay_reducer_result(&session.route, &session.identity.account.id, key, path)
                    .await?;
                return Ok(ReducerExecution {
                    value,
                    committed_revision: None,
                });
            }
        }
        let mut provenance = access.provenance.unwrap_or_else(|| {
            direct_provenance(
                session,
                InvocationChannel::Ui,
                command_id,
                &module.artifact_hash,
            )
        });
        install_execution_deadline(self, &mut provenance);
        transaction
            .set_invocation_provenance(
                &provenance.root_command_id,
                invocation_channel_name(provenance.channel),
                provenance.actor_account_id.as_deref(),
                provenance.actor_member_id.as_deref(),
                provenance.on_behalf_of_member_id.as_deref(),
                provenance.agent_execution_id.as_deref(),
            )
            .await?;
        let mut handler = DatabaseHostCalls::new(transaction, DatabaseCapability::Reducer)
            .with_actor(
                &session.identity.account.id,
                &session.identity.account.email,
            )
            .with_provenance(&provenance);
        let mut invocation = invocation(
            session,
            module.generation,
            path,
            "reducer",
            args,
            Some(DatabaseCapability::Reducer),
            provenance,
        );
        invocation.context.capabilities.action_outbox = true;
        let result = self
            .inner
            .module_host
            .invoke(invocation, &mut handler)
            .await;
        match result {
            Ok(value) => {
                if let Some(key) = idempotency_key {
                    handler
                        .transaction_mut()
                        .store_reducer_result(&session.identity.account.id, key, &value)
                        .await?;
                }
                handler
                    .finish(true)
                    .await
                    .map_err(ExecutionError::HostCall)?;
                let committed_revision =
                    control.command_revision(&session.route, command_id).await?;
                let runtime = self.clone();
                let session = session.clone();
                tokio::spawn(async move {
                    runtime.drain_action_outbox(session).await;
                });
                Ok(ReducerExecution {
                    value,
                    committed_revision,
                })
            }
            Err(error) => {
                let _ = handler.finish(false).await;
                Err(error.into())
            }
        }
    }

    pub async fn execute_tenant_action(
        &self,
        session: &TenantSession,
        path: &str,
        args: Value,
    ) -> Result<Value, ExecutionError> {
        self.execute_tenant_action_with_access(session, path, args, ExecutionAccess::default())
            .await
    }

    pub(crate) async fn execute_tenant_action_with_access(
        &self,
        session: &TenantSession,
        path: &str,
        args: Value,
        access: ExecutionAccess,
    ) -> Result<Value, ExecutionError> {
        let module = match access.module {
            Some(module) => module,
            None => self
                .inner
                .modules
                .project_for_call(&session.identity.project_id)
                .await
                .ok_or_else(|| {
                    ExecutionError::ModuleMissing(session.identity.project_id.clone())
                })?,
        };
        let definition = require_function(&module, path, "action", access.allow_internal)?;
        validate_portable_schema(&definition.args_schema, &args).map_err(|message| {
            ExecutionError::InvalidArguments {
                path: path.to_owned(),
                message,
            }
        })?;
        let command_id = format!("action-{}", uuid::Uuid::new_v4());
        let mut provenance = access.provenance.unwrap_or_else(|| {
            let channel = if definition.action_profile == "agent" {
                InvocationChannel::Agent
            } else {
                InvocationChannel::Ui
            };
            direct_provenance(session, channel, &command_id, &module.artifact_hash)
        });
        if definition.action_profile == "agent" && provenance.agent_execution_id.is_none() {
            if provenance.on_behalf_of_member_id.is_none() {
                provenance.on_behalf_of_member_id = Some(session.member.id.clone());
            }
            provenance.agent_execution_id =
                Some(format!("agent_{}", uuid::Uuid::new_v4().simple()));
        }
        install_execution_deadline(self, &mut provenance);
        if !provenance.action_stack.iter().any(|item| item == path) {
            provenance.action_stack.push(path.to_owned());
        }
        let mut handler = ActionHostCalls::new(
            self.clone(),
            session.clone(),
            definition,
            provenance.clone(),
            module.clone(),
        )
        .map_err(ExecutionError::HostCall)?;
        let environment = self
            .load_action_secrets(&session.identity.project_id, handler.secrets())
            .await?;
        let mut invocation = invocation(
            session,
            module.generation,
            path,
            "action",
            args,
            None,
            provenance,
        );
        invocation.context.capabilities.network = handler.network();
        invocation.context.capabilities.action_tools = !handler.tools().is_empty();
        invocation.context.capabilities.scheduler = handler.scheduler();
        invocation.context.capabilities.storage = handler.storage();
        invocation.context.capabilities.sandbox = handler.sandbox();
        invocation.context.capabilities.secrets = !environment.is_empty();
        invocation.context.capabilities.functions = handler.functions();
        invocation.context.environment = environment;
        invocation.context.action_tools = handler.tools();
        self.inner
            .module_host
            .invoke(invocation, &mut handler)
            .await
            .map_err(Into::into)
    }

    async fn load_action_secrets(
        &self,
        project_id: &str,
        names: &[String],
    ) -> Result<BTreeMap<String, String>, ExecutionError> {
        if names.is_empty() {
            return Ok(BTreeMap::new());
        }
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| ExecutionError::CapabilityUnavailable {
                path: project_id.to_owned(),
                capability: "project secrets require the Control Plane".to_owned(),
            })?;
        let mut transaction = control.begin_control_transaction(true).await?;
        let rows = sqlx::query(
            "SELECT name,value FROM gonvex_project_env WHERE project_id=$1 AND name=ANY($2)",
        )
        .bind(project_id)
        .bind(names)
        .fetch_all(&mut **transaction.transaction())
        .await
        .map_err(|error| ExecutionError::HostCall(error.to_string()))?;
        transaction.rollback().await?;
        let environment = rows
            .into_iter()
            .map(|row| (row.get::<String, _>("name"), row.get::<String, _>("value")))
            .collect::<BTreeMap<_, _>>();
        if let Some(missing) = names.iter().find(|name| !environment.contains_key(*name)) {
            return Err(ExecutionError::CapabilityUnavailable {
                path: project_id.to_owned(),
                capability: format!("unconfigured project secret {missing:?}"),
            });
        }
        Ok(environment)
    }

    pub(crate) async fn invoke_interactive_function(
        &self,
        session: &TenantSession,
        parent: &InvocationProvenance,
        path: &str,
        args: Value,
        artifact_hash: &str,
        module: ModuleCallLease,
    ) -> Result<Value, ExecutionError> {
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| ExecutionError::ModuleMissing(session.identity.project_id.clone()))?;
        // Re-resolve the authoritative tenant Member for every delegated call.
        // A stale session or directory entry therefore cannot preserve access
        // after revocation.
        let fresh = control
            .tenant_session_for_account(
                &session.identity.project_id,
                &session.route.tenant_id,
                &session.identity.account.id,
            )
            .await?;
        let definition = require_interactive_target(&module, parent, path, artifact_hash)?;
        validate_portable_schema(&definition.args_schema, &args).map_err(|message| {
            ExecutionError::InvalidArguments {
                path: path.to_owned(),
                message,
            }
        })?;
        let command_id = format!("agent-{}", uuid::Uuid::new_v4());
        let root_command_id = if parent.root_command_id.trim().is_empty() {
            parent.command_id.clone()
        } else {
            parent.root_command_id.clone()
        };
        let child = InvocationProvenance {
            channel: InvocationChannel::Agent,
            root_channel: parent.root_channel,
            actor_account_id: Some(fresh.identity.account.id.clone()),
            actor_member_id: Some(fresh.member.id.clone()),
            on_behalf_of_member_id: Some(fresh.member.id.clone()),
            root_command_id,
            command_id: command_id.clone(),
            parent_command_id: Some(parent.command_id.clone()),
            agent_execution_id: parent
                .agent_execution_id
                .clone()
                .or_else(|| Some(format!("agent_{}", uuid::Uuid::new_v4().simple()))),
            thread_id: parent.thread_id.clone(),
            turn_id: parent.turn_id.clone(),
            tool_call_id: parent.tool_call_id.clone(),
            artifact_hash: module.artifact_hash.clone(),
            depth: parent.depth.saturating_add(1),
            action_stack: parent.action_stack.clone(),
            deadline_unix_ms: parent.deadline_unix_ms,
        };
        let result = match definition.kind.as_str() {
            "query" => {
                self.execute_tenant_query_with_access(
                    &fresh,
                    path,
                    args,
                    ExecutionAccess {
                        provenance: Some(child),
                        module: Some(module.clone()),
                        ..ExecutionAccess::default()
                    },
                )
                .await
            }
            "reducer" => self
                .execute_tenant_reducer_with_access(
                    &fresh,
                    &command_id,
                    None,
                    path,
                    args,
                    ExecutionAccess {
                        provenance: Some(child),
                        module: Some(module.clone()),
                        ..ExecutionAccess::default()
                    },
                )
                .await
                .map(|result| result.value),
            "action" => {
                self.execute_tenant_action_with_access(
                    &fresh,
                    path,
                    args,
                    ExecutionAccess {
                        provenance: Some(child),
                        module: Some(module.clone()),
                        ..ExecutionAccess::default()
                    },
                )
                .await
            }
            _ => Err(ExecutionError::FunctionMissing(path.to_owned())),
        }?;
        validate_portable_schema(&definition.result_schema, &result).map_err(|message| {
            ExecutionError::InvalidResult {
                path: path.to_owned(),
                message,
            }
        })?;
        Ok(result)
    }

    pub(crate) async fn drain_action_outbox(&self, fallback_session: TenantSession) {
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return;
        };
        for _ in 0..100 {
            let claimed = match control.claim_action(&fallback_session.route).await {
                Ok(Some(claimed)) => claimed,
                Ok(None) | Err(_) => return,
            };
            let session = if claimed.actor_account_id.starts_with("_gonvex_") {
                Ok(fallback_session.clone())
            } else {
                // Durable work keeps attribution, not stale authority. Resolve
                // the tenant Member again before the Action can call a Query
                // or Reducer, including when the original actor matches the
                // session that happened to start this drain.
                control
                    .tenant_session_for_account(
                        &fallback_session.identity.project_id,
                        &fallback_session.route.tenant_id,
                        &claimed.actor_account_id,
                    )
                    .await
            };
            let result =
                match session {
                    Ok(session) => {
                        let mut provenance = serde_json::from_value::<InvocationProvenance>(
                            claimed.provenance.clone(),
                        )
                        .unwrap_or_else(|_| {
                            direct_provenance(&session, InvocationChannel::System, &claimed.id, "")
                        });
                        provenance.parent_command_id = Some(provenance.command_id.clone());
                        provenance.command_id = format!("outbox-{}", claimed.id);
                        provenance.channel = InvocationChannel::System;
                        provenance.depth = provenance.depth.saturating_add(1);
                        self.execute_tenant_action_with_access(
                            &session,
                            &claimed.path,
                            claimed.args.clone(),
                            ExecutionAccess {
                                provenance: Some(provenance),
                                ..ExecutionAccess::default()
                            },
                        )
                        .await
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                    }
                    Err(error) => Err(error.to_string()),
                };
            match result {
                Ok(()) => {
                    if control
                        .complete_action(&fallback_session.route, &claimed.id)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    let _ = control
                        .retry_action(
                            &fallback_session.route,
                            &claimed.id,
                            claimed.attempts,
                            &error,
                        )
                        .await;
                    tokio::time::sleep(std::time::Duration::from_secs(
                        claimed.attempts.clamp(1, 10) as u64,
                    ))
                    .await;
                }
            }
        }
    }

    pub(crate) async fn drain_all_action_outboxes(&self) {
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return;
        };
        let Ok(projects) = control.runtime_projects().await else {
            return;
        };
        for project in projects {
            let Some(project_id) = project.get("id").and_then(Value::as_str) else {
                continue;
            };
            let Ok(routes) = control.tenant_routes(project_id).await else {
                continue;
            };
            for route in routes {
                self.drain_action_outbox(system_tenant_session(project_id, route))
                    .await;
            }
        }
    }
}

pub(crate) fn system_tenant_session(project: &str, route: TenantRoute) -> TenantSession {
    TenantSession {
        identity: SessionIdentity {
            project_id: project.to_owned(),
            account: Account {
                id: "_gonvex_system".to_owned(),
                email: String::new(),
                email_verified: false,
                name: "Gonvex".to_owned(),
                avatar_url: String::new(),
                provider: "system".to_owned(),
            },
        },
        route,
        member: Member {
            id: "_gonvex_system".to_owned(),
            account_id: "_gonvex_system".to_owned(),
            status: "system".to_owned(),
            display_name: "Gonvex".to_owned(),
            avatar_url: String::new(),
            role: "system".to_owned(),
            permissions: serde_json::json!({}),
            membership_revision: 0,
        },
    }
}

fn require_function<'module>(
    module: &'module crate::modules::ProjectModule,
    path: &str,
    kind: &'static str,
    allow_internal: bool,
) -> Result<&'module crate::modules::FunctionDefinition, ExecutionError> {
    let function = module
        .functions
        .get(path)
        .ok_or_else(|| ExecutionError::FunctionMissing(path.to_owned()))?;
    if function.internal && !allow_internal {
        return Err(ExecutionError::InternalFunction(path.to_owned()));
    }
    if function.kind != kind {
        return Err(ExecutionError::WrongKind {
            path: path.to_owned(),
            expected: kind,
            actual: function.kind.clone(),
        });
    }
    Ok(function)
}

pub(crate) fn invocation(
    session: &TenantSession,
    generation: u64,
    function: &str,
    kind: &str,
    args: Value,
    database: Option<DatabaseCapability>,
    provenance: InvocationProvenance,
) -> InvokeRequest {
    let deadline_unix_ms = provenance.deadline_unix_ms;
    let invocation_info = provenance.public_info();
    InvokeRequest {
        module_id: session.identity.project_id.clone(),
        generation: Some(generation),
        function: function.to_owned(),
        kind: kind.to_owned(),
        args: serde_json::to_string(&args).unwrap_or_else(|_| "null".to_owned()),
        context: InvocationContextWire {
            project_id: session.identity.project_id.clone(),
            tenant_id: session.route.tenant_id.clone(),
            operation_id: None,
            tenant: Some(TenantIdentity {
                id: session.route.tenant_id.clone(),
                project_id: session.identity.project_id.clone(),
                name: None,
            }),
            account: Some(AccountIdentity {
                id: session.identity.account.id.clone(),
                email: nonempty(&session.identity.account.email),
                name: nonempty(&session.identity.account.name),
                avatar_url: nonempty(&session.identity.account.avatar_url),
            }),
            member: Some(MemberIdentity {
                id: session.member.id.clone(),
                account_id: session.member.account_id.clone(),
                status: nonempty(&session.member.status),
                role: nonempty(&session.member.role),
                display_name: nonempty(&session.member.display_name),
                permissions: session.member.permissions.clone(),
            }),
            permissions: session.member.permissions.clone(),
            invocation: invocation_info,
            environment: BTreeMap::new(),
            action_tools: Vec::new(),
            capabilities: CapabilitiesWire {
                db_read: database.is_some(),
                db_write: database == Some(DatabaseCapability::Reducer),
                ..CapabilitiesWire::default()
            },
            now_unix_ms: 0,
            deadline_unix_ms,
        },
    }
}

fn nonempty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

pub(crate) fn direct_provenance(
    session: &TenantSession,
    channel: InvocationChannel,
    command_id: &str,
    artifact_hash: &str,
) -> InvocationProvenance {
    InvocationProvenance {
        channel,
        root_channel: channel,
        actor_account_id: Some(session.identity.account.id.clone()),
        actor_member_id: Some(session.member.id.clone()),
        on_behalf_of_member_id: None,
        root_command_id: command_id.to_owned(),
        command_id: command_id.to_owned(),
        parent_command_id: None,
        agent_execution_id: None,
        thread_id: None,
        turn_id: None,
        tool_call_id: None,
        artifact_hash: artifact_hash.to_owned(),
        depth: 0,
        action_stack: Vec::new(),
        deadline_unix_ms: None,
    }
}

fn install_execution_deadline(runtime: &Runtime, provenance: &mut InvocationProvenance) {
    if provenance.deadline_unix_ms.is_none() {
        provenance.deadline_unix_ms = Some(
            unix_millis(SystemTime::now()).saturating_add(
                runtime
                    .inner
                    .config
                    .module_host
                    .execution_timeout
                    .as_millis() as u64,
            ),
        );
    }
}

fn invocation_channel_name(channel: InvocationChannel) -> &'static str {
    match channel {
        InvocationChannel::Ui => "ui",
        InvocationChannel::Agent => "agent",
        InvocationChannel::Api => "api",
        InvocationChannel::Scheduler => "scheduler",
        InvocationChannel::System => "system",
    }
}

fn require_interactive_target<'a>(
    module: &'a crate::modules::ProjectModule,
    parent: &InvocationProvenance,
    path: &str,
    artifact_hash: &str,
) -> Result<&'a crate::modules::FunctionDefinition, ExecutionError> {
    if parent.depth >= 8 {
        return Err(ExecutionError::InvocationDepth);
    }
    if artifact_hash != module.artifact_hash {
        return Err(ExecutionError::StaleCatalog {
            expected: artifact_hash.to_owned(),
            active: module.artifact_hash.clone(),
        });
    }
    let definition = module
        .functions
        .get(path)
        .ok_or_else(|| ExecutionError::FunctionMissing(path.to_owned()))?;
    if definition.internal || !definition.interactive || definition.classification != "interactive"
    {
        return Err(ExecutionError::NotInteractive(path.to_owned()));
    }
    if definition.kind == "action" && parent.action_stack.iter().any(|item| item == path) {
        return Err(ExecutionError::RecursiveAction(path.to_owned()));
    }
    Ok(definition)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::modules::{FunctionDefinition, ProjectModule};

    fn function(kind: &str, classification: &str, internal: bool) -> FunctionDefinition {
        FunctionDefinition {
            kind: kind.to_owned(),
            internal,
            delivery: "oneShot".to_owned(),
            action_profile: "standard".to_owned(),
            action_capabilities: json!({}),
            replica: None,
            dependencies: json!({}),
            live_query_plan: None,
            interactive: classification == "interactive",
            classification: classification.to_owned(),
            description: String::new(),
            tags: Vec::new(),
            confirmation: "none".to_owned(),
            args_schema: json!({"kind":"object","fields":{}}),
            result_schema: json!({"kind":"null"}),
        }
    }

    fn module(functions: BTreeMap<String, FunctionDefinition>) -> ProjectModule {
        ProjectModule {
            project_id: "project".to_owned(),
            generation: 1,
            artifact_hash: "active-hash".to_owned(),
            functions,
            manifest_functions: json!({}),
            schema: json!({}),
            visibility: BTreeMap::new(),
            invitation_acceptance_reducer: String::new(),
            migrations: Vec::new(),
            crons: Vec::new(),
        }
    }

    fn provenance() -> InvocationProvenance {
        InvocationProvenance {
            artifact_hash: "active-hash".to_owned(),
            ..InvocationProvenance::default()
        }
    }

    #[test]
    fn delegated_invocation_rejects_stale_internal_and_system_targets() {
        let module = module(BTreeMap::from([
            (
                "tasks.start".to_owned(),
                function("reducer", "interactive", false),
            ),
            (
                "tasks.secret".to_owned(),
                function("query", "internal", true),
            ),
            (
                "tasks.callback".to_owned(),
                function("action", "system", false),
            ),
        ]));
        assert!(matches!(
            require_interactive_target(&module, &provenance(), "tasks.start", "stale-hash"),
            Err(ExecutionError::StaleCatalog { .. })
        ));
        assert!(matches!(
            require_interactive_target(&module, &provenance(), "tasks.secret", "active-hash"),
            Err(ExecutionError::NotInteractive(path)) if path == "tasks.secret"
        ));
        assert!(matches!(
            require_interactive_target(&module, &provenance(), "tasks.callback", "active-hash"),
            Err(ExecutionError::NotInteractive(path)) if path == "tasks.callback"
        ));
    }

    #[test]
    fn delegated_actions_have_bounded_depth_and_recursive_loop_detection() {
        let module = module(BTreeMap::from([(
            "agent.continue".to_owned(),
            function("action", "interactive", false),
        )]));
        let mut recursive = provenance();
        recursive.action_stack.push("agent.continue".to_owned());
        assert!(matches!(
            require_interactive_target(&module, &recursive, "agent.continue", "active-hash"),
            Err(ExecutionError::RecursiveAction(path)) if path == "agent.continue"
        ));
        let mut deep = provenance();
        deep.depth = 8;
        assert!(matches!(
            require_interactive_target(&module, &deep, "agent.continue", "active-hash"),
            Err(ExecutionError::InvocationDepth)
        ));
    }
}
