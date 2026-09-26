//! Trusted service principals.
//!
//! A service principal is a backend service (for example an HTTP API gateway)
//! that authenticates with a `gvx_svc_` credential configured through
//! `GONVEX_SERVICE_PRINCIPALS`. The runtime only stores the credential's
//! SHA-256 digest. A service socket is bound to exactly one project and tenant
//! and has two narrow capabilities:
//!
//! 1. Call the tenant module functions explicitly allowlisted for the principal
//!    (`functions`). These run as a system identity (`_gonvex_service:<id>`)
//!    with the `api` invocation channel, and may be `internal` functions. This
//!    is how a gateway asks the application to verify its own credentials
//!    without the framework knowing anything about them.
//! 2. `control.servicePrincipals.delegate`: mint a short-lived, single-use,
//!    tenant-scoped grant to act as one *active* member of that tenant. The
//!    grant is redeemed on a new socket exactly like a support impersonation
//!    grant (`gvx_imp_` then rotating `gvx_dev_` reconnect credentials), so the
//!    delegated socket gets the member's normal permissions, visibility,
//!    membership revalidation and `membership-changed` resets. Delegations are
//!    recorded in `gonvex_impersonation_grants` with actor `service:<id>`,
//!    plus the optional end `actor` the service reports (for example one API
//!    key), and can be revoked with `control.servicePrincipals.revokeDelegation`.
//!    A redeemed grant yields a delegated session: its direct calls use the
//!    `api` invocation channel and modules see
//!    `ctx.invocation.delegation = {principal, actor}`.
//!
//! Every other frame on a service socket is rejected. Service sockets have no
//! change feed, replicas, live queries or Control Plane administration. These
//! restrictions apply to the service socket only, never to the member
//! sockets that redeem its delegation grants.

use chrono::Utc;
use gonvex_module_runtime::InvocationChannel;
use gonvex_postgres::{
    Account, DelegationActor, Member, SessionIdentity, TenantRoute, TenantSession,
    SERVICE_PRINCIPAL_ACTOR_PREFIX,
};
use gonvex_protocol::{ClientMessage, ExecutionScope, ServerMessage};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;

use crate::config::ServicePrincipalConfig;
use crate::execution::{direct_provenance, ExecutionAccess};
use crate::Runtime;

pub(crate) const TOKEN_PREFIX: &str = "gvx_svc_";
pub(crate) const DELEGATE_PATH: &str = "control.servicePrincipals.delegate";
pub(crate) const REVOKE_PATH: &str = "control.servicePrincipals.revokeDelegation";
const MIN_DELEGATION_SECONDS: i64 = 30;
const MAX_REASON_BYTES: usize = 200;
const MAX_ACTOR_KIND_CHARS: usize = 32;
const MAX_ACTOR_NAME_CHARS: usize = 120;
const MAX_ACTOR_REFERENCE_CHARS: usize = 200;
const AUTH_FAILURE: &str = "service credential is invalid or not permitted for this project";

/// The authenticated state of one service socket.
#[derive(Clone, Debug)]
pub(crate) struct ServiceGrant {
    pub principal_id: String,
    pub route: TenantRoute,
    pub functions: Vec<String>,
    pub max_delegation: std::time::Duration,
}

impl ServiceGrant {
    fn actor(&self) -> String {
        format!("{SERVICE_PRINCIPAL_ACTOR_PREFIX}{}", self.principal_id)
    }

    fn session(&self) -> TenantSession {
        let id = format!("_gonvex_service:{}", self.principal_id);
        TenantSession {
            identity: SessionIdentity {
                project_id: self.route.project_id.clone(),
                account: Account {
                    id: id.clone(),
                    email: String::new(),
                    email_verified: false,
                    name: self.principal_id.clone(),
                    avatar_url: String::new(),
                    provider: "service".to_owned(),
                },
            },
            route: self.route.clone(),
            member: Member {
                id: id.clone(),
                account_id: id,
                status: "system".to_owned(),
                display_name: self.principal_id.clone(),
                avatar_url: String::new(),
                role: "system".to_owned(),
                permissions: json!({}),
                membership_revision: 0,
            },
            admission_revision: 0,
            // The principal's own calls are not a delegated member session.
            delegation: None,
        }
    }

    fn allows(&self, path: &str) -> bool {
        self.functions.iter().any(|function| function == path)
    }
}

/// Finds the configured principal for a presented credential. Every
/// configured digest is compared in constant time so the lookup does not leak
/// which principal (if any) a guess is close to.
pub(crate) fn find_principal<'a>(
    principals: &'a [ServicePrincipalConfig],
    token: &str,
) -> Option<&'a ServicePrincipalConfig> {
    let token = token.trim();
    if !token.starts_with(TOKEN_PREFIX) {
        return None;
    }
    let digest = gonvex_postgres::token_hash(token);
    let mut found = None;
    for principal in principals {
        if bool::from(digest.as_bytes().ct_eq(principal.token_sha256.as_bytes())) {
            found = Some(principal);
        }
    }
    found
}

/// Validates the optional `actor` of a delegation: the end actor the service
/// acts for, such as one API key. It is stored on the grant and shown to
/// modules as `ctx.invocation.delegation.actor`; it never grants access.
fn delegation_actor(value: Option<&Value>) -> Result<Option<DelegationActor>, String> {
    let object = match value {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Object(object)) => object,
        Some(_) => return Err("invalid arguments: actor must be an object".to_owned()),
    };
    if let Some(unknown) = object
        .keys()
        .find(|key| !matches!(key.as_str(), "kind" | "name" | "reference"))
    {
        return Err(format!(
            "invalid arguments: unknown actor field {unknown:?}"
        ));
    }
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind.is_empty()
        || kind.len() > MAX_ACTOR_KIND_CHARS
        || !kind.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
    {
        return Err(format!(
            "invalid arguments: actor.kind must be 1-{MAX_ACTOR_KIND_CHARS} characters of [a-z0-9_-]"
        ));
    }
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if name.is_empty()
        || name.chars().count() > MAX_ACTOR_NAME_CHARS
        || name.chars().any(char::is_control)
    {
        return Err(format!(
            "invalid arguments: actor.name must be 1-{MAX_ACTOR_NAME_CHARS} characters without control characters"
        ));
    }
    let invalid_reference = || {
        format!(
            "invalid arguments: actor.reference must be a string of at most {MAX_ACTOR_REFERENCE_CHARS} characters without control characters"
        )
    };
    let reference = match object.get("reference") {
        None | Some(Value::Null) => None,
        Some(Value::String(reference)) => {
            let reference = reference.trim();
            if reference.chars().count() > MAX_ACTOR_REFERENCE_CHARS
                || reference.chars().any(char::is_control)
            {
                return Err(invalid_reference());
            }
            // An empty reference carries nothing; store it as absent.
            (!reference.is_empty()).then(|| reference.to_owned())
        }
        Some(_) => return Err(invalid_reference()),
    };
    Ok(Some(DelegationActor {
        kind: kind.to_owned(),
        name: name.to_owned(),
        reference,
    }))
}

/// How `/dev/manifest` treats the presented bearer credential.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ManifestAccess {
    /// Not a configured service credential; project-key and operator
    /// authorization apply unchanged.
    NotServicePrincipal,
    /// A service principal with `manifest: true` for this project. It gets
    /// the function manifest without schema or visibility plans.
    Allowed { principal: String },
    /// A service principal without manifest access to this project.
    Denied { principal: String },
}

/// Decides manifest access for a bearer credential. A recognized service
/// credential is authorized only by its own configuration, never by the
/// operator or project-key paths.
pub(crate) fn manifest_access(
    principals: &[ServicePrincipalConfig],
    token: &str,
    project: &str,
) -> ManifestAccess {
    let Some(principal) = find_principal(principals, token) else {
        return ManifestAccess::NotServicePrincipal;
    };
    let principal_id = principal.id.clone();
    if principal.manifest && principal.projects.iter().any(|allowed| allowed == project) {
        ManifestAccess::Allowed {
            principal: principal_id,
        }
    } else {
        ManifestAccess::Denied {
            principal: principal_id,
        }
    }
}

pub(crate) enum ServiceFrame {
    /// The frame is an `auth` frame and must be handled by the normal path.
    PassThrough,
    Reply(Box<ServerMessage>),
    Ignore,
    Close,
}

impl ServiceFrame {
    fn reply(message: ServerMessage) -> Self {
        Self::Reply(Box::new(message))
    }
}

impl Runtime {
    /// Authenticates a `gvx_svc_` credential for one project and tenant.
    pub(crate) async fn authenticate_service_principal(
        &self,
        id: String,
        token: &str,
        project: Option<&str>,
        tenant: Option<&str>,
    ) -> (ServerMessage, Option<ServiceGrant>) {
        let failure = |id: String| {
            (
                ServerMessage::AuthError {
                    id,
                    error: AUTH_FAILURE.to_owned(),
                },
                None,
            )
        };
        let Some(principal) = find_principal(&self.inner.config.service_principals, token) else {
            tracing::warn!(target: "gonvex_runtime::service_principal", "rejected unknown service credential");
            return failure(id);
        };
        let project = project.map(str::trim).unwrap_or_default();
        let tenant = tenant.map(str::trim).unwrap_or_default();
        if project.is_empty()
            || tenant.is_empty()
            || !principal.projects.iter().any(|allowed| allowed == project)
        {
            tracing::warn!(
                target: "gonvex_runtime::service_principal",
                principal = %principal.id,
                project,
                tenant,
                "rejected service credential outside its project allowlist"
            );
            return failure(id);
        }
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return (
                ServerMessage::AuthError {
                    id,
                    error: "auth session store is unavailable".to_owned(),
                },
                None,
            );
        };
        let route = match control.resolve_tenant(project, tenant).await {
            Ok(route) => route,
            Err(error) => {
                tracing::warn!(
                    target: "gonvex_runtime::service_principal",
                    principal = %principal.id,
                    project,
                    tenant,
                    %error,
                    "service principal tenant does not resolve"
                );
                return (
                    ServerMessage::AuthError {
                        id,
                        error: "tenant is not available".to_owned(),
                    },
                    None,
                );
            }
        };
        tracing::info!(
            target: "gonvex_runtime::service_principal",
            principal = %principal.id,
            project = %route.project_id,
            tenant = %route.tenant_id,
            "service principal authenticated"
        );
        let grant = ServiceGrant {
            principal_id: principal.id.clone(),
            route,
            functions: principal.functions.clone(),
            max_delegation: principal.max_delegation,
        };
        (
            ServerMessage::AuthResult {
                id,
                result: json!({
                    "projectId": grant.route.project_id,
                    "tenantId": grant.route.tenant_id,
                    "accountId": "",
                    "servicePrincipal": grant.principal_id,
                    "functions": grant.functions,
                    "maxDelegationSeconds": grant.max_delegation.as_secs(),
                }),
            },
            Some(grant),
        )
    }

    /// Handles one text frame received on an authenticated service socket.
    pub(crate) async fn handle_service_frame(
        &self,
        grant: &ServiceGrant,
        text: &str,
    ) -> ServiceFrame {
        let Ok(message) = serde_json::from_str::<ClientMessage>(text) else {
            return ServiceFrame::Close;
        };
        match message {
            ClientMessage::Auth { .. } => ServiceFrame::PassThrough,
            ClientMessage::ReducerCall(call) => {
                let path = call.path.clone();
                let id = call.id.clone();
                let result = if call.scope == Some(ExecutionScope::Control) {
                    self.service_control_call(grant, &path, &call.args).await
                } else {
                    self.service_tenant_call(
                        grant,
                        "reducer",
                        &id,
                        &path,
                        call.args,
                        call.idempotency_key.as_deref(),
                    )
                    .await
                };
                ServiceFrame::reply(match result {
                    Ok((result, committed_revision)) => ServerMessage::ReducerResult {
                        id: id.clone(),
                        path: Some(path),
                        result,
                        origin_command_id: id,
                        committed_revision,
                        trace: None,
                    },
                    Err(error) => ServerMessage::ReducerError {
                        id,
                        path: Some(path),
                        error,
                        trace: None,
                    },
                })
            }
            ClientMessage::ActionCall {
                id,
                path,
                args,
                scope,
                idempotency_key,
                ..
            } => {
                let result = if scope == Some(ExecutionScope::Control) {
                    Err("service principals cannot call Control Plane Actions".to_owned())
                } else {
                    self.service_tenant_call(
                        grant,
                        "action",
                        &id,
                        &path,
                        args,
                        idempotency_key.as_deref(),
                    )
                    .await
                };
                ServiceFrame::reply(match result {
                    Ok((result, committed_revision)) => ServerMessage::ActionResult {
                        id,
                        path: Some(path),
                        result,
                        committed_revision,
                        trace: None,
                    },
                    Err(error) => ServerMessage::ActionError {
                        id,
                        path: Some(path),
                        error,
                        trace: None,
                    },
                })
            }
            ClientMessage::QueryCall {
                id,
                path,
                args,
                scope,
            } => {
                let result = if scope == Some(ExecutionScope::Control) {
                    Err("service principals cannot call Control Plane Queries".to_owned())
                } else {
                    self.service_tenant_call(grant, "query", &id, &path, args, None)
                        .await
                };
                ServiceFrame::reply(match result {
                    Ok((result, _)) => ServerMessage::QueryResult {
                        id,
                        payload: std::collections::BTreeMap::from([
                            ("path".to_owned(), Value::String(path)),
                            ("result".to_owned(), result),
                        ]),
                    },
                    Err(error) => ServerMessage::QueryError {
                        id,
                        path: Some(path),
                        error,
                    },
                })
            }
            ClientMessage::QuerySubscribe { id, path, .. } => {
                ServiceFrame::reply(ServerMessage::QueryError {
                    id,
                    path: Some(path),
                    error: "service principals cannot subscribe".to_owned(),
                })
            }
            ClientMessage::ReplicaOpen(request) => {
                ServiceFrame::reply(ServerMessage::ReplicaError {
                    id: request.id,
                    path: Some(request.path),
                    error: "service principals cannot open replicas".to_owned(),
                })
            }
            ClientMessage::QuerySubscribeMany { .. }
            | ClientMessage::ReplicaOpenMany { .. }
            | ClientMessage::ReducerCallMany { .. } => {
                ServiceFrame::reply(ServerMessage::ReducerError {
                    id: String::new(),
                    path: None,
                    error: "batched frames are not available to service principals".to_owned(),
                    trace: None,
                })
            }
            _ => ServiceFrame::Ignore,
        }
    }

    async fn service_tenant_call(
        &self,
        grant: &ServiceGrant,
        kind: &str,
        id: &str,
        path: &str,
        args: Value,
        idempotency_key: Option<&str>,
    ) -> Result<(Value, Option<u64>), String> {
        if !grant.allows(path) {
            tracing::warn!(
                target: "gonvex_runtime::service_principal",
                principal = %grant.principal_id,
                tenant = %grant.route.tenant_id,
                path,
                "service principal called a function outside its allowlist"
            );
            return Err(format!(
                "function {path:?} is not allowed for this service principal"
            ));
        }
        let session = grant.session();
        let artifact_hash = self
            .inner
            .modules
            .project(&grant.route.project_id)
            .await
            .map(|module| module.artifact_hash.clone())
            .unwrap_or_default();
        let access = || ExecutionAccess {
            allow_internal: true,
            provenance: Some(direct_provenance(
                &session,
                InvocationChannel::Api,
                id,
                &artifact_hash,
            )),
            ..ExecutionAccess::default()
        };
        let result = match kind {
            "reducer" => self
                .execute_tenant_reducer_with_access(
                    &session,
                    id,
                    idempotency_key,
                    path,
                    args,
                    access(),
                )
                .await
                .map(|execution| (execution.value, execution.committed_revision)),
            "action" => self
                .execute_tenant_action_with_access(&session, path, args, access())
                .await
                .map(|execution| (execution.value, execution.committed_revision)),
            _ => self
                .execute_tenant_query_with_access(&session, path, args, access())
                .await
                .map(|value| (value, None)),
        };
        tracing::info!(
            target: "gonvex_runtime::service_principal",
            principal = %grant.principal_id,
            tenant = %grant.route.tenant_id,
            path,
            kind,
            ok = result.is_ok(),
            "service principal call"
        );
        result.map_err(|error| error.to_string())
    }

    async fn service_control_call(
        &self,
        grant: &ServiceGrant,
        path: &str,
        args: &Value,
    ) -> Result<(Value, Option<u64>), String> {
        match path {
            DELEGATE_PATH => self.delegate_service_session(grant, args).await,
            REVOKE_PATH => self.revoke_service_delegation(grant, args).await,
            _ => Err(format!(
                "Control Plane function {path:?} is not available to service principals"
            )),
        }
        .map(|value| (value, None))
    }

    /// Mints a single-use grant to act as one active member of the service
    /// socket's tenant. The member must still be admitted at redemption time.
    pub(crate) async fn delegate_service_session(
        &self,
        grant: &ServiceGrant,
        args: &Value,
    ) -> Result<Value, String> {
        let object = args
            .as_object()
            .ok_or_else(|| "invalid arguments: expected an object".to_owned())?;
        if let Some(unknown) = object.keys().find(|key| {
            !matches!(
                key.as_str(),
                "accountId" | "memberId" | "reason" | "expiresInSeconds" | "actor"
            )
        }) {
            return Err(format!("invalid arguments: unknown field {unknown:?}"));
        }
        let field = |name: &str| {
            object
                .get(name)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| format!("invalid arguments: {name} is required"))
        };
        let account_id = field("accountId")?;
        let member_id = field("memberId")?;
        let reason = field("reason")?;
        if reason.len() > MAX_REASON_BYTES {
            return Err(format!(
                "invalid arguments: reason exceeds {MAX_REASON_BYTES} bytes"
            ));
        }
        let maximum = i64::try_from(grant.max_delegation.as_secs()).unwrap_or(i64::MAX);
        let seconds = match object.get("expiresInSeconds") {
            None | Some(Value::Null) => maximum,
            Some(value) => value
                .as_i64()
                .filter(|value| (MIN_DELEGATION_SECONDS..=maximum).contains(value))
                .ok_or_else(|| {
                    format!("invalid arguments: expiresInSeconds must be between {MIN_DELEGATION_SECONDS} and {maximum}")
                })?,
        };
        let actor = delegation_actor(object.get("actor"))?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "auth session store is unavailable".to_owned())?;
        let admitted = control
            .admit_member(&grant.route.project_id, &grant.route.tenant_id, account_id)
            .await
            .ok()
            .filter(|(_, member, _)| member.id == member_id);
        if admitted.is_none() {
            tracing::warn!(
                target: "gonvex_runtime::service_principal",
                principal = %grant.principal_id,
                tenant = %grant.route.tenant_id,
                member = member_id,
                "delegation refused: target is not an active member"
            );
            return Err("target is not an active tenant member".to_owned());
        }
        let grant_id = format!("svcdel_{}", uuid::Uuid::new_v4().simple());
        let token = crate::control::service_grant_token();
        let expires = Utc::now() + chrono::Duration::seconds(seconds);
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query(
            r#"INSERT INTO gonvex_impersonation_grants
               (id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at,
                delegation_actor)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)"#,
        )
        .bind(&grant_id)
        .bind(&grant.route.project_id)
        .bind(gonvex_postgres::token_hash(&token))
        .bind(grant.actor())
        .bind(account_id)
        .bind(&grant.route.tenant_id)
        .bind(format!("{}: {reason}", grant.actor()))
        .bind(expires)
        .bind(actor.clone().map(sqlx::types::Json))
        .execute(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        tracing::info!(
            target: "gonvex_runtime::service_principal",
            principal = %grant.principal_id,
            project = %grant.route.project_id,
            tenant = %grant.route.tenant_id,
            member = member_id,
            grant = %grant_id,
            reason,
            actor_kind = actor.as_ref().map(|actor| actor.kind.as_str()),
            actor_reference = actor.as_ref().and_then(|actor| actor.reference.as_deref()),
            expires_in_seconds = seconds,
            "service principal delegated a member session"
        );
        Ok(json!({
            "id": grant_id,
            "token": token,
            "expiresAt": expires.to_rfc3339(),
            "expiresInSeconds": seconds,
        }))
    }

    pub(crate) async fn revoke_service_delegation(
        &self,
        grant: &ServiceGrant,
        args: &Value,
    ) -> Result<Value, String> {
        let id = args
            .as_object()
            .filter(|object| object.len() == 1)
            .and_then(|object| object.get("id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "invalid arguments: id is required".to_owned())?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "auth session store is unavailable".to_owned())?;
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        let affected = sqlx::query(
            r#"UPDATE gonvex_impersonation_grants SET revoked_at=now()
               WHERE project_id=$1 AND tenant_id=$2 AND id=$3 AND actor_account_id=$4
                 AND revoked_at IS NULL"#,
        )
        .bind(&grant.route.project_id)
        .bind(&grant.route.tenant_id)
        .bind(id)
        .bind(grant.actor())
        .execute(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?
        .rows_affected();
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        tracing::info!(
            target: "gonvex_runtime::service_principal",
            principal = %grant.principal_id,
            tenant = %grant.route.tenant_id,
            grant = id,
            revoked = affected > 0,
            "service principal revoked a delegation"
        );
        Ok(json!({ "updated": affected > 0 }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn principal(id: &str, token: &str) -> ServicePrincipalConfig {
        ServicePrincipalConfig {
            id: id.to_owned(),
            token_sha256: gonvex_postgres::token_hash(token),
            projects: vec!["project".to_owned()],
            functions: vec!["keys.authenticate".to_owned()],
            max_delegation: Duration::from_secs(900),
            manifest: false,
        }
    }

    fn manifest_principal(id: &str, token: &str) -> ServicePrincipalConfig {
        ServicePrincipalConfig {
            manifest: true,
            ..principal(id, token)
        }
    }

    fn grant() -> ServiceGrant {
        ServiceGrant {
            principal_id: "gateway".to_owned(),
            route: TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: "tenant".to_owned(),
                database_url: "postgres://unused".to_owned(),
            },
            functions: vec!["keys.authenticate".to_owned()],
            max_delegation: Duration::from_secs(900),
        }
    }

    #[test]
    fn credentials_match_only_their_own_digest() {
        let principals = vec![
            principal("a", "gvx_svc_alpha"),
            principal("b", "gvx_svc_beta"),
        ];
        assert_eq!(
            find_principal(&principals, "gvx_svc_beta").map(|p| p.id.as_str()),
            Some("b")
        );
        assert_eq!(
            find_principal(&principals, " gvx_svc_alpha ").map(|p| p.id.as_str()),
            Some("a")
        );
        assert!(find_principal(&principals, "gvx_svc_gamma").is_none());
        // A session token with a matching digest is still not a service token.
        let session = vec![principal("s", "gvx_session_alpha")];
        assert!(find_principal(&session, "gvx_session_alpha").is_none());
    }

    #[test]
    fn service_identity_is_distinct_from_members_and_system() {
        let session = grant().session();
        assert_eq!(session.member.id, "_gonvex_service:gateway");
        assert_eq!(session.identity.account.id, "_gonvex_service:gateway");
        assert_eq!(session.route.tenant_id, "tenant");
        assert_eq!(session.member.permissions, json!({}));
    }

    fn runtime() -> Runtime {
        let mut config = crate::config::Config::from_env().expect("default config");
        config.control_plane_database_url = None;
        config.default_database_url = None;
        config.require_auth = false;
        config.module_host.enabled = false;
        config.service_principals = vec![principal("gateway", "gvx_svc_secret")];
        Runtime::new(config)
    }

    #[tokio::test]
    async fn service_sockets_reject_everything_outside_their_capabilities() {
        let runtime = runtime();
        let grant = grant();
        let reply = |frame: ServiceFrame| match frame {
            ServiceFrame::Reply(message) => serde_json::to_value(*message).unwrap(),
            _ => panic!("expected a reply"),
        };
        let outside = reply(
            runtime
                .handle_service_frame(
                    &grant,
                    r#"{"type":"reducer.call","id":"r1","path":"tasks.create","args":{}}"#,
                )
                .await,
        );
        assert_eq!(outside["type"], "reducer.error");
        assert!(outside["error"].as_str().unwrap().contains("not allowed"));
        let control = reply(
            runtime
                .handle_service_frame(
                    &grant,
                    r#"{"type":"reducer.call","id":"r2","path":"control.tenants.create","args":{},"scope":"control"}"#,
                )
                .await,
        );
        assert!(control["error"]
            .as_str()
            .unwrap()
            .contains("not available to service principals"));
        let subscribe = reply(
            runtime
                .handle_service_frame(
                    &grant,
                    r#"{"type":"query.subscribe","id":"q1","path":"tasks.grid","args":{}}"#,
                )
                .await,
        );
        assert_eq!(subscribe["type"], "query.error");
        let replica = reply(
            runtime
                .handle_service_frame(
                    &grant,
                    r#"{"type":"replica.open","id":"p1","path":"tasks.list","args":{}}"#,
                )
                .await,
        );
        assert_eq!(replica["type"], "replica.error");
        let action = reply(
            runtime
                .handle_service_frame(
                    &grant,
                    r#"{"type":"action.call","id":"a1","path":"control.auth.passwordLogin","args":{},"scope":"control"}"#,
                )
                .await,
        );
        assert_eq!(action["type"], "action.error");
        assert!(matches!(
            runtime
                .handle_service_frame(
                    &grant,
                    r#"{"type":"auth","id":"a","token":"gvx_svc_secret"}"#
                )
                .await,
            ServiceFrame::PassThrough
        ));
        assert!(matches!(
            runtime.handle_service_frame(&grant, "not json").await,
            ServiceFrame::Close
        ));
    }

    #[tokio::test]
    async fn delegation_arguments_are_validated_before_touching_the_control_plane() {
        let runtime = runtime();
        let grant = grant();
        for args in [
            json!([]),
            json!({"accountId":"a","memberId":"m"}),
            json!({"accountId":"a","memberId":"m","reason":"x","extra":1}),
            json!({"accountId":"a","memberId":"m","reason":"x","expiresInSeconds":5}),
            json!({"accountId":"a","memberId":"m","reason":"x","expiresInSeconds":901}),
            json!({"accountId":"a","memberId":"m","reason":"x".repeat(201)}),
            json!({"accountId":"a","memberId":"m","reason":"x","actor":"api_key"}),
            json!({"accountId":"a","memberId":"m","reason":"x","actor":{"kind":"API","name":"k"}}),
            json!({"accountId":"a","memberId":"m","reason":"x","actor":{"kind":"api_key","name":"k","role":"admin"}}),
        ] {
            let error = runtime
                .delegate_service_session(&grant, &args)
                .await
                .unwrap_err();
            assert!(error.starts_with("invalid arguments"), "{args}: {error}");
        }
        for args in [
            json!({"accountId":"a","memberId":"m","reason":"api-key:k1"}),
            json!({"accountId":"a","memberId":"m","reason":"api-key:k1","actor":null}),
            json!({"accountId":"a","memberId":"m","reason":"api-key:k1",
                   "actor":{"kind":"api_key","name":"CI key","reference":"k1"}}),
        ] {
            let error = runtime
                .delegate_service_session(&grant, &args)
                .await
                .unwrap_err();
            assert_eq!(error, "auth session store is unavailable", "{args}");
        }
    }

    #[test]
    fn delegation_actors_are_validated() {
        let actor = |value: Value| delegation_actor(Some(&value));
        assert_eq!(delegation_actor(None), Ok(None));
        assert_eq!(actor(Value::Null), Ok(None));
        assert_eq!(
            actor(json!({"kind":"api_key","name":"  CI key  ","reference":" key_1 "})),
            Ok(Some(DelegationActor {
                kind: "api_key".to_owned(),
                name: "CI key".to_owned(),
                reference: Some("key_1".to_owned()),
            }))
        );
        assert_eq!(
            actor(json!({"kind":"svc-2_x","name":"Ñandú ✓ key"})),
            Ok(Some(DelegationActor {
                kind: "svc-2_x".to_owned(),
                name: "Ñandú ✓ key".to_owned(),
                reference: None,
            }))
        );
        // Empty and null references are stored as absent.
        for reference in [json!(""), json!("   "), Value::Null] {
            assert_eq!(
                actor(json!({"kind":"api_key","name":"k","reference":reference}))
                    .unwrap()
                    .unwrap()
                    .reference,
                None
            );
        }
        // Limits are counted in characters, not bytes.
        assert!(actor(
            json!({"kind":"k".repeat(32),"name":"é".repeat(120),"reference":"é".repeat(200)})
        )
        .is_ok());

        for (value, field) in [
            (json!("api_key"), "actor must be an object"),
            (json!(["api_key"]), "actor must be an object"),
            (
                json!({"kind":"api_key","name":"k","scopes":["*"]}),
                "unknown actor field \"scopes\"",
            ),
            (json!({"name":"k"}), "actor.kind"),
            (json!({"kind":"","name":"k"}), "actor.kind"),
            (json!({"kind":"k".repeat(33),"name":"k"}), "actor.kind"),
            (json!({"kind":"Api_Key","name":"k"}), "actor.kind"),
            (json!({"kind":"api key","name":"k"}), "actor.kind"),
            (json!({"kind":"api.key","name":"k"}), "actor.kind"),
            (json!({"kind":"clé","name":"k"}), "actor.kind"),
            (json!({"kind":7,"name":"k"}), "actor.kind"),
            (json!({"kind":"api_key"}), "actor.name"),
            (json!({"kind":"api_key","name":"   "}), "actor.name"),
            (
                json!({"kind":"api_key","name":"x".repeat(121)}),
                "actor.name",
            ),
            (json!({"kind":"api_key","name":"line\nbreak"}), "actor.name"),
            (json!({"kind":"api_key","name":"bell\u{7}"}), "actor.name"),
            (json!({"kind":"api_key","name":{"first":"k"}}), "actor.name"),
            (
                json!({"kind":"api_key","name":"k","reference":"r".repeat(201)}),
                "actor.reference",
            ),
            (
                json!({"kind":"api_key","name":"k","reference":"a\u{1b}[31m"}),
                "actor.reference",
            ),
            (
                json!({"kind":"api_key","name":"k","reference":42}),
                "actor.reference",
            ),
        ] {
            let error = actor(value.clone()).unwrap_err();
            assert!(
                error.starts_with("invalid arguments: ") && error.contains(field),
                "{value}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn authentication_failures_are_indistinguishable() {
        let runtime = runtime();
        let mut errors = Vec::new();
        for (token, project, tenant) in [
            ("gvx_svc_wrong", Some("project"), Some("tenant")),
            ("gvx_svc_secret", Some("other"), Some("tenant")),
            ("gvx_svc_secret", Some("project"), None),
            ("gvx_svc_secret", None, Some("tenant")),
        ] {
            let (message, grant) = runtime
                .authenticate_service_principal("a".to_owned(), token, project, tenant)
                .await;
            assert!(grant.is_none());
            errors.push(serde_json::to_value(message).unwrap());
        }
        assert!(
            errors.windows(2).all(|pair| pair[0] == pair[1]),
            "{errors:?}"
        );
    }

    #[test]
    fn manifest_access_requires_the_flag_and_a_listed_project() {
        let principals = vec![
            manifest_principal("reader", "gvx_svc_reader"),
            principal("caller", "gvx_svc_caller"),
        ];
        assert_eq!(
            manifest_access(&principals, "gvx_svc_reader", "project"),
            ManifestAccess::Allowed {
                principal: "reader".to_owned()
            }
        );
        assert_eq!(
            manifest_access(&principals, "gvx_svc_reader", "other-project"),
            ManifestAccess::Denied {
                principal: "reader".to_owned()
            }
        );
        assert_eq!(
            manifest_access(&principals, "gvx_svc_caller", "project"),
            ManifestAccess::Denied {
                principal: "caller".to_owned()
            }
        );
        // Unknown service tokens and every other credential keep the existing
        // project-key and operator authorization.
        for token in ["gvx_svc_unknown", "gvx_pat_x.y", "admin-key", ""] {
            assert_eq!(
                manifest_access(&principals, token, "project"),
                ManifestAccess::NotServicePrincipal,
                "{token}"
            );
        }
    }

    #[tokio::test]
    async fn manifest_route_gives_service_principals_only_the_function_catalog() {
        use axum::body::{to_bytes, Body};
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let mut config = crate::config::Config::from_env().expect("default config");
        config.control_plane_database_url = None;
        config.default_database_url = None;
        config.module_host.enabled = false;
        config.admin_key = Some("test-admin-key".to_owned());
        config.service_principals = vec![
            manifest_principal("reader", "gvx_svc_reader"),
            principal("caller", "gvx_svc_caller"),
        ];
        let runtime = Runtime::new(config);
        runtime
            .inner
            .modules
            .insert_for_test(crate::modules::ProjectModule {
                project_id: "project".to_owned(),
                generation: 7,
                artifact_hash: "artifact".to_owned(),
                client_contract: None,
                functions: Default::default(),
                manifest_functions: json!({"keys.authenticate": {"kind": "query"}}),
                replica_epochs: Default::default(),
                schema: json!({"tables": {"apiKeys": {}}}),
                visibility: Default::default(),
                invitation_acceptance_reducer: String::new(),
                migrations: Vec::new(),
                crons: Vec::new(),
            })
            .await;
        let get = |uri: &str, token: &str, header_project: Option<&str>| {
            let mut request = Request::get(uri).header("authorization", format!("Bearer {token}"));
            if let Some(project) = header_project {
                request = request.header("x-gonvex-project-id", project);
            }
            let router = runtime.router();
            let request = request.body(Body::empty()).unwrap();
            async move {
                let response = router.oneshot(request).await.unwrap();
                let status = response.status();
                let body: Value =
                    serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                        .unwrap_or(Value::Null);
                (status, body)
            }
        };

        let (status, body) = get("/dev/manifest?project=project", "gvx_svc_reader", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "project": "project",
                "functions": {"keys.authenticate": {"kind": "query"}},
                "module": {"hash": "artifact", "generation": 7},
            })
        );
        let (status, header_body) = get("/dev/manifest", "gvx_svc_reader", Some("project")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header_body, body);

        // Without `manifest: true`, or outside the principal's projects, a
        // service credential is refused even though it authenticates.
        let (status, _) = get("/dev/manifest?project=project", "gvx_svc_caller", None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = get("/dev/manifest?project=other", "gvx_svc_reader", None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = get("/dev/manifest?project=project", "gvx_svc_unknown", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Operators keep the full manifest.
        let (status, full) = get("/dev/manifest?project=project", "test-admin-key", None).await;
        assert_eq!(status, StatusCode::OK, "{full}");
        assert_eq!(full["schema"], json!({"tables": {"apiKeys": {}}}));
        assert_eq!(full["visibility"], json!({}));
        assert_eq!(full["functions"], body["functions"]);
    }

    /// Full delegation contract against a disposable PostgreSQL database.
    /// Set GONVEX_TEST_POSTGRES_URL to run it; it creates and drops its own
    /// schemas.
    #[tokio::test]
    async fn delegated_sessions_are_member_scoped_single_use_and_revocable() {
        use axum::body::{to_bytes, Body};
        use axum::http::{Request, StatusCode};
        use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry};
        use sqlx::postgres::PgPoolOptions;
        use tower::ServiceExt;

        let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!(
                "GONVEX_TEST_POSTGRES_URL is not set; skipping service principal contract test"
            );
            return;
        };
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let control_schema = format!("gonvex_svc_control_{nonce}");
        let tenant_schema = format!("gonvex_svc_tenant_{nonce}");
        let scoped = |schema: &str| {
            let separator = if base_url.contains('?') { '&' } else { '?' };
            format!("{base_url}{separator}options=-csearch_path%3D{schema}")
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await
            .unwrap();
        for schema in [&control_schema, &tenant_schema] {
            sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
                .execute(&admin)
                .await
                .unwrap();
        }
        let control_url = scoped(&control_schema);
        let tenant_url = scoped(&tenant_schema);
        let pools = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(&control_url, pools.clone(), Default::default())
            .await
            .unwrap();
        let route = TenantRoute {
            project_id: "project".to_owned(),
            tenant_id: "tenant".to_owned(),
            database_url: tenant_url.clone(),
        };
        control
            .clone()
            .provision_tenant_database(route, Vec::new())
            .await
            .unwrap();
        let fixture = PgPoolOptions::new()
            .max_connections(1)
            .connect(&control_url)
            .await
            .unwrap();
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_projects(id,name,database_mode,database_url,status,auth_mode)
               VALUES('project','Project','multiTenant',$1,'active','gonvex-native')"#,
        )
        .bind(&control_url)
        .execute(&fixture)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status)
               VALUES('tenant','project','tenant','Tenant',$1,'active')"#,
        )
        .bind(&tenant_url)
        .execute(&fixture)
        .await
        .unwrap();

        let mut config = crate::config::Config::from_env().expect("default config");
        config.control_plane_database_url = Some(control_url.clone());
        config.default_database_url = Some(control_url.clone());
        config.require_auth = true;
        config.module_host.enabled = false;
        config.admin_key = Some("test-admin-key".to_owned());
        config.control_secret = Some("test-control-secret-that-is-long-enough".to_owned());
        config.service_principals = vec![principal("gateway", "gvx_svc_secret")];
        let runtime = Runtime::new(config);
        runtime.start().await.unwrap();

        let mut members = Vec::new();
        for email in ["creator@example.test", "other@example.test"] {
            let response = runtime
                .router()
                .oneshot(
                    Request::post("/dev/internal/e2e/members")
                        .header("authorization", "Bearer test-admin-key")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({"projectId":"project","tenantId":"tenant","email":email,"name":email,"password":"correct horse battery staple"})
                                .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let created: Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 64 << 10).await.unwrap())
                    .unwrap();
            members.push((
                created["accountId"].as_str().unwrap().to_owned(),
                created["memberId"].as_str().unwrap().to_owned(),
            ));
        }
        let (account, member) = members[0].clone();
        let (other_account, other_member) = members[1].clone();

        let (message, grant) = runtime
            .authenticate_service_principal(
                "auth".to_owned(),
                "gvx_svc_secret",
                Some("project"),
                Some("tenant"),
            )
            .await;
        let grant = grant.unwrap_or_else(|| panic!("service auth failed: {message:?}"));
        assert_eq!(grant.route.tenant_id, "tenant");

        // The member id must belong to the account; a mismatch is refused.
        let refused = runtime
            .delegate_service_session(
                &grant,
                &json!({"accountId": other_account, "memberId": member, "reason": "api-key:k1"}),
            )
            .await
            .unwrap_err();
        assert_eq!(refused, "target is not an active tenant member");

        let delegated = runtime
            .delegate_service_session(
                &grant,
                &json!({"accountId": account, "memberId": member, "reason": "api-key:k1", "expiresInSeconds": 120,
                        "actor": {"kind": "api_key", "name": " CI key ", "reference": "key_1"}}),
            )
            .await
            .unwrap();
        let token = delegated["token"].as_str().unwrap();
        assert!(token.starts_with("gvx_imp_"));
        let stored = sqlx::query_as::<_, (String, String, String, Option<sqlx::types::Json<Value>>)>(
            "SELECT actor_account_id,reason,token_hash,delegation_actor FROM gonvex_impersonation_grants WHERE id=$1",
        )
        .bind(delegated["id"].as_str().unwrap())
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert_eq!(stored.0, "service:gateway");
        assert_eq!(stored.1, "service:gateway: api-key:k1");
        assert_ne!(stored.2, token, "only the digest is stored");
        assert_eq!(
            stored.3.map(|actor| actor.0),
            Some(json!({"kind": "api_key", "name": "CI key", "reference": "key_1"}))
        );
        let expected_delegation = gonvex_postgres::SessionDelegation {
            principal: "gateway".to_owned(),
            actor: Some(DelegationActor {
                kind: "api_key".to_owned(),
                name: "CI key".to_owned(),
                reference: Some("key_1".to_owned()),
            }),
        };

        // A delegated grant cannot be redeemed for another tenant.
        assert!(control
            .authenticate_impersonation(token, Some("project"), Some("other-tenant"), "conn-0")
            .await
            .is_err());
        let session = control
            .authenticate_impersonation(token, Some("project"), Some("tenant"), "conn-1")
            .await
            .unwrap();
        assert_eq!(session.tenant.member.id, member);
        assert_eq!(session.tenant.identity.account.id, account);
        assert_eq!(session.actor_account_id, "service:gateway");
        assert_eq!(session.tenant.delegation, Some(expected_delegation.clone()));
        // Direct calls on the redeemed session are attributed to the API
        // channel and show the delegation to modules.
        let provenance = direct_provenance(
            &session.tenant,
            InvocationChannel::Ui,
            "reducer-command",
            "artifact",
        );
        assert_eq!(provenance.channel, InvocationChannel::Api);
        assert_eq!(provenance.root_channel, InvocationChannel::Api);
        assert_eq!(
            serde_json::to_value(provenance.public_info()).unwrap()["delegation"],
            json!({"principal": "gateway",
                   "actor": {"kind": "api_key", "name": "CI key", "reference": "key_1"}})
        );
        // Single use: the grant itself cannot be redeemed twice.
        assert!(control
            .authenticate_impersonation(token, Some("project"), Some("tenant"), "conn-2")
            .await
            .is_err());
        // A reconnect with the rotating credential stays delegated.
        let session = control
            .authenticate_impersonation(
                &session.reconnect_token,
                Some("project"),
                Some("tenant"),
                "conn-1",
            )
            .await
            .unwrap();
        assert_eq!(session.tenant.delegation, Some(expected_delegation));
        control
            .validate_impersonation_session(&session.reconnect_token, "project", "tenant", "conn-1")
            .await
            .unwrap();

        // Revocation is scoped to the principal's own tenant grants and takes
        // effect at the next revalidation.
        let revoked = runtime
            .revoke_service_delegation(&grant, &json!({"id": delegated["id"]}))
            .await
            .unwrap();
        assert_eq!(revoked["updated"], true);
        assert!(control
            .validate_impersonation_session(&session.reconnect_token, "project", "tenant", "conn-1")
            .await
            .is_err());

        // Without an actor the grant stores NULL and the session is still
        // delegated, with `actor: null`.
        let unattributed = runtime
            .delegate_service_session(
                &grant,
                &json!({"accountId": other_account, "memberId": other_member, "reason": "api-key:k2"}),
            )
            .await
            .unwrap();
        let stored_actor = sqlx::query_scalar::<_, Option<sqlx::types::Json<Value>>>(
            "SELECT delegation_actor FROM gonvex_impersonation_grants WHERE id=$1",
        )
        .bind(unattributed["id"].as_str().unwrap())
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert!(stored_actor.is_none());
        let unattributed_session = control
            .authenticate_impersonation(
                unattributed["token"].as_str().unwrap(),
                Some("project"),
                Some("tenant"),
                "conn-3",
            )
            .await
            .unwrap();
        assert_eq!(
            unattributed_session.tenant.delegation,
            Some(gonvex_postgres::SessionDelegation {
                principal: "gateway".to_owned(),
                actor: None,
            })
        );

        // Support impersonation and developer mode grants name an account and
        // never produce a delegated session.
        let support_token = "gvx_imp_support_contract";
        sqlx::query(
            r#"INSERT INTO gonvex_impersonation_grants
               (id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at)
               VALUES('imp_support','project',$1,$2,$3,'tenant','support',now()+interval '5 minutes')"#,
        )
        .bind(gonvex_postgres::token_hash(support_token))
        .bind(&other_account)
        .bind(&account)
        .execute(&fixture)
        .await
        .unwrap();
        let support = control
            .authenticate_impersonation(support_token, Some("project"), Some("tenant"), "conn-4")
            .await
            .unwrap();
        assert_eq!(support.actor_account_id, other_account);
        assert_eq!(support.tenant.delegation, None);

        // A service grant whose stored actor was altered is refused and left
        // unconsumed rather than redeemed without its attribution.
        let tampered_token = "gvx_imp_tampered_contract";
        sqlx::query(
            r#"INSERT INTO gonvex_impersonation_grants
               (id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at,
                delegation_actor)
               VALUES('svcdel_tampered','project',$1,'service:gateway',$2,'tenant','service:gateway: x',
                      now()+interval '5 minutes','{"kind":"api_key"}')"#,
        )
        .bind(gonvex_postgres::token_hash(tampered_token))
        .bind(&account)
        .execute(&fixture)
        .await
        .unwrap();
        assert!(control
            .authenticate_impersonation(tampered_token, Some("project"), Some("tenant"), "conn-5")
            .await
            .is_err());
        let consumed: bool = sqlx::query_scalar(
            "SELECT used_at IS NOT NULL FROM gonvex_impersonation_grants WHERE id='svcdel_tampered'",
        )
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert!(!consumed);

        // Deactivated members cannot be delegated.
        let tenant_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&tenant_url)
            .await
            .unwrap();
        sqlx::query("UPDATE members SET status='inactive' WHERE id=$1")
            .bind(&member)
            .execute(&tenant_pool)
            .await
            .unwrap();
        let refused = runtime
            .delegate_service_session(
                &grant,
                &json!({"accountId": account, "memberId": member, "reason": "api-key:k1"}),
            )
            .await
            .unwrap_err();
        assert_eq!(refused, "target is not an active tenant member");

        // A Control Plane created before delegation actors existed gains the
        // column the next time a runtime starts against it.
        sqlx::query("ALTER TABLE gonvex_impersonation_grants DROP COLUMN delegation_actor")
            .execute(&fixture)
            .await
            .unwrap();
        ControlPlane::connect(&control_url, pools.clone(), Default::default())
            .await
            .unwrap();
        let restored: bool = sqlx::query_scalar(
            r#"SELECT EXISTS(SELECT 1 FROM information_schema.columns
               WHERE table_schema=current_schema() AND table_name='gonvex_impersonation_grants'
                 AND column_name='delegation_actor' AND data_type='jsonb' AND is_nullable='YES')"#,
        )
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert!(restored);

        tenant_pool.close().await;
        runtime.shutdown().await;
        fixture.close().await;
        pools.close().await;
        for schema in [&tenant_schema, &control_schema] {
            sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
                .execute(&admin)
                .await
                .unwrap();
        }
        admin.close().await;
    }
}
