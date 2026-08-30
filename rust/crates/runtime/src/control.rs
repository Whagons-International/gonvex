//! Trusted Control Plane Query, Reducer, and Action dispatch.
//!
//! These functions share the published invocation frames, but never enter an
//! application module. Authorization, physical database routing, and every
//! Control Plane credential remain in the Rust host.

use std::collections::{BTreeMap, BTreeSet};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use gonvex_postgres::{
    Member, SessionIdentity, TenantSession, TenantTransaction, TransactionAttribution,
};
use gonvex_protocol::ServerMessage;
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::Row;
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::external_auth::{
    verify_external_token, ExternalAuthConfiguration, ExternalAuthError, VerifiedExternalIdentity,
};
use crate::host_calls::{DatabaseCapability, DatabaseHostCalls};
use crate::Runtime;

const CONTROL_SECRET_AAD: &[u8] = b"gonvex-auth-provider";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlKind {
    Query,
    Reducer,
    Action,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Authorization {
    Public,
    Account,
    TenantAdmin,
    Developer,
    ProjectAdmin,
}

#[derive(Clone, Copy)]
struct Definition {
    kind: ControlKind,
    authorization: Authorization,
    live: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionGrant {
    pub(crate) access_token: String,
    pub(crate) access_expires_at: DateTime<Utc>,
    pub(crate) refresh_token: String,
    pub(crate) refresh_expires_at: DateTime<Utc>,
}

pub(crate) enum RefreshSessionResult {
    Refreshed(Box<(SessionGrant, AuthAccount, String)>),
    ReuseRevoked,
}

#[derive(Clone, Debug)]
pub(crate) struct AuthAccount {
    pub(crate) id: String,
    pub(crate) email: String,
    pub(crate) email_verified: bool,
    pub(crate) name: String,
    pub(crate) picture: String,
    pub(crate) provider: String,
}

#[derive(Clone, Debug, Default)]
pub struct ControlConnection {
    pub connection_id: String,
    pub project_id: String,
    pub identity: Option<SessionIdentity>,
    pub tenant: Option<TenantSession>,
    pub impersonation_id: String,
    pub auth_token: String,
}

#[derive(Clone, Debug)]
pub struct ControlSubscription {
    pub id: String,
    pub path: String,
    pub args: Value,
    pub last_result: Value,
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("unknown Control Plane function {0:?}")]
    UnknownFunction(String),
    #[error("Control Plane function {path:?} is not a {expected}")]
    WrongKind {
        path: String,
        expected: &'static str,
    },
    #[error("authentication is required")]
    AuthenticationRequired,
    #[error("tenant administrator access is required")]
    TenantAdminRequired,
    #[error("developer access is required")]
    DeveloperRequired,
    #[error("project administrator access is required")]
    ProjectAdminRequired,
    #[error("project administration is unavailable during impersonation")]
    AdministrationDuringImpersonation,
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),
    #[error("Control Plane operation is not yet implemented in Rust: {0}")]
    NotImplemented(String),
    #[error("Control Plane database invariant failed during {operation}: {detail}")]
    DatabaseInvariant {
        operation: &'static str,
        detail: String,
    },
    #[error(transparent)]
    Database(#[from] gonvex_postgres::DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    ExternalAuth(#[from] ExternalAuthError),
}

impl Runtime {
    pub async fn execute_control_query(
        &self,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
    ) -> Result<Value, ControlError> {
        let definition = require_definition(path, ControlKind::Query)?;
        self.authorize_control(connection, definition.authorization)
            .await?;
        let control = self.control_plane().await?;
        if matches!(path, "control.tenants.mine" | "users.myTenants") {
            empty_args(args)?;
            return list_account_tenants(&control, connection, path == "users.myTenants").await;
        }
        if path == "control.auth.memberProviders" {
            return member_auth_providers(&control, connection, args).await;
        }
        let mut transaction = control.begin_control_transaction(true).await?;
        let result = self
            .control_query(&control, &mut transaction, connection, path, args)
            .await?;
        transaction.commit().await?;
        if path == "control.support.sendCommand" {
            if let Some(command_id) = result.get("id").and_then(Value::as_str) {
                self.broadcast_support_command(&control, &connection.project_id, command_id)
                    .await?;
            }
        }
        Ok(result)
    }

    pub(crate) async fn broadcast_support_command(
        &self,
        control: &gonvex_postgres::ControlPlane,
        project_id: &str,
        command_id: &str,
    ) -> Result<(), ControlError> {
        control
            .notify(
                "gonvex_support_command",
                &serde_json::json!({"projectId":project_id,"commandId":command_id}).to_string(),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn deliver_support_command(
        &self,
        control: &gonvex_postgres::ControlPlane,
        project_id: &str,
        command_id: &str,
    ) -> Result<(), ControlError> {
        let mut transaction = control.begin_control_transaction(true).await?;
        let row = sqlx::query(
            r#"SELECT session.connection_id,command.id,command.kind,command.payload
               FROM gonvex_support_commands command
               JOIN gonvex_support_sessions session ON session.id=command.session_id
               WHERE command.project_id=$1 AND command.id=$2"#,
        )
        .bind(project_id)
        .bind(command_id)
        .fetch_optional(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        if let Some(row) = row {
            let _ = self
                .inner
                .runtime_events
                .send(crate::RuntimeEvent::SupportCommand {
                    project_id: project_id.to_owned(),
                    connection_id: row.get("connection_id"),
                    command: serde_json::json!({
                        "id":row.get::<String,_>("id"),
                        "kind":row.get::<String,_>("kind"),
                        "payload":row.get::<Json<Value>,_>("payload").0,
                    }),
                });
        }
        Ok(())
    }

    pub async fn execute_control_reducer(
        &self,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        self.execute_control_write(
            connection,
            path,
            args,
            idempotency_key,
            ControlKind::Reducer,
        )
        .await
    }

    pub async fn execute_control_action(
        &self,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        self.execute_control_write(connection, path, args, idempotency_key, ControlKind::Action)
            .await
    }

    async fn execute_control_write(
        &self,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
        idempotency_key: &str,
        kind: ControlKind,
    ) -> Result<Value, ControlError> {
        let definition = require_definition(path, kind)?;
        self.authorize_control(connection, definition.authorization)
            .await?;
        let idempotency_key = idempotency_key.trim();
        if idempotency_key.is_empty() {
            return Err(ControlError::InvalidArguments(
                "Control Plane write calls require an idempotency key".to_owned(),
            ));
        }
        let control = self.control_plane().await?;
        if kind == ControlKind::Action
            && matches!(
                path,
                "control.auth.passwordLogin"
                    | "control.auth.exchangeExternalToken"
                    | "control.auth.refreshSession"
            )
        {
            return self
                .execute_auth_action(&control, connection, path, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer
            && matches!(
                path,
                "control.invitations.accept" | "tenants.acceptInvitation"
            )
        {
            return self
                .accept_invitation(&control, connection, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer && path == "control.accounts.resetMemberPassword" {
            return self
                .reset_member_password(&control, connection, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer && path == "control.accounts.provisionMemberLogin" {
            return self
                .provision_member_login(&control, connection, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer
            && matches!(
                path,
                "control.developer.provisionSelf" | "control.developer.removeSelf"
            )
        {
            return self
                .change_developer_membership(&control, connection, path, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer && path == "control.support.createImpersonation" {
            return self
                .create_support_impersonation(&control, connection, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer && path == "control.tenants.create" {
            return self
                .create_tenant(&control, connection, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer && path == "control.demos.create" {
            return self
                .create_demo_account(&control, connection, args, idempotency_key)
                .await;
        }
        if kind == ControlKind::Reducer && path == "control.demos.delete" {
            return self
                .delete_demo_account(&control, connection, args, idempotency_key)
                .await;
        }
        if path == "control.developer.enter" {
            let object = exact_object(args, &["tenantId"])?;
            let tenant_id = required_string(object, "tenantId")?;
            let identity = account(connection)?;
            control
                .admit_member(&connection.project_id, tenant_id, &identity.account.id)
                .await
                .map_err(|_| {
                    ControlError::InvalidArguments(
                        "provision developer access before entering the tenant".to_owned(),
                    )
                })?;
        }
        let subject = control_subject(connection, definition.authorization);
        let mut transaction = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut transaction,
            &connection.project_id,
            &subject,
            idempotency_key,
            kind,
            path,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(result);
        }
        let result = self
            .control_write_body(&mut transaction, connection, path, args)
            .await?;
        sqlx::query(
            r#"UPDATE gonvex_control_idempotency
               SET state='completed',result=$4,error='',updated_at=now()
               WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3"#,
        )
        .bind(&connection.project_id)
        .bind(&subject)
        .bind(idempotency_key)
        .bind(Json(result.clone()))
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn execute_auth_action(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let external = if path == "control.auth.exchangeExternalToken" {
            let object = exact_object(
                args,
                &["provider", "token", "tenantId", "previousRefreshToken"],
            )?;
            let provider = required_string(object, "provider")?;
            if !matches!(provider, "firebase" | "external-oidc") {
                return Err(ControlError::InvalidArguments(
                    "provider must be firebase or external-oidc".to_owned(),
                ));
            }
            let mut transaction = control.begin_control_transaction(true).await?;
            let configuration = load_external_configuration(
                self,
                &mut transaction,
                &connection.project_id,
                provider,
            )
            .await?;
            transaction.commit().await?;
            Some(verify_external_token(&configuration, required_string(object, "token")?).await?)
        } else {
            None
        };
        let subject = auth_subject(path, args)?;
        let mut transaction = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut transaction,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Action,
            path,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(result);
        }
        let (grant, account, requested_tenant) = match path {
            "control.auth.passwordLogin" => {
                let object = exact_object(args, &["email", "password"])?;
                let mode: String = sqlx::query_scalar(
                    "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
                )
                .bind(&connection.project_id)
                .fetch_one(&mut **transaction.transaction())
                .await?;
                if !matches!(mode.as_str(), "gonvex-native" | "hybrid") {
                    return Err(ControlError::InvalidArguments(
                        "password authentication is disabled; use the project's external identity provider".to_owned(),
                    ));
                }
                let email = normalize_email(required_string(object, "email")?)?;
                let row = sqlx::query(
                    r#"SELECT account.id,password.password_hash,account.email,
                              account.name,account.avatar_url
                       FROM accounts account
                       JOIN gonvex_account_passwords password
                         ON password.project_id=account.auth_realm_id
                        AND password.account_id=account.id
                       WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2)
                         AND account.disabled_at IS NULL"#,
                )
                .bind(&connection.project_id)
                .bind(email)
                .fetch_optional(&mut **transaction.transaction())
                .await?;
                let Some(row) = row else {
                    return Err(ControlError::InvalidArguments(
                        "invalid email or password".to_owned(),
                    ));
                };
                if !verify_password(
                    required_string(object, "password")?,
                    &row.get::<String, _>("password_hash"),
                ) {
                    return Err(ControlError::InvalidArguments(
                        "invalid email or password".to_owned(),
                    ));
                }
                let account = AuthAccount {
                    id: row.get("id"),
                    email: row.get("email"),
                    email_verified: true,
                    name: row.get("name"),
                    picture: row.get("avatar_url"),
                    provider: "password".to_owned(),
                };
                let grant = issue_session(
                    &mut transaction,
                    &connection.project_id,
                    &account.id,
                    &random_id("family"),
                    Utc::now() + chrono::Duration::days(30),
                )
                .await?;
                (grant, account, String::new())
            }
            "control.auth.exchangeExternalToken" => {
                let object = exact_object(
                    args,
                    &["provider", "token", "tenantId", "previousRefreshToken"],
                )?;
                let identity = external.as_ref().expect("verified above");
                let account =
                    resolve_external_account(&mut transaction, &connection.project_id, identity)
                        .await?;
                let grant = issue_session(
                    &mut transaction,
                    &connection.project_id,
                    &account.id,
                    &random_id("family"),
                    Utc::now() + chrono::Duration::days(30),
                )
                .await?;
                if let Some(previous) = optional_string(object, "previousRefreshToken")
                    .filter(|value| !value.is_empty())
                {
                    sqlx::query(
                        r#"UPDATE gonvex_auth_refresh_tokens
                           SET revoked_at=COALESCE(revoked_at,now())
                           WHERE project_id=$1 AND account_id=$2 AND family_id=(
                             SELECT family_id FROM gonvex_auth_refresh_tokens
                             WHERE project_id=$1 AND account_id=$2 AND token_hash=$3 LIMIT 1
                           )"#,
                    )
                    .bind(&connection.project_id)
                    .bind(&account.id)
                    .bind(sha256_hex(previous.as_bytes()))
                    .execute(&mut **transaction.transaction())
                    .await?;
                }
                (
                    grant,
                    account,
                    optional_string(object, "tenantId").unwrap_or("").to_owned(),
                )
            }
            "control.auth.refreshSession" => {
                let object = exact_object(args, &["refreshToken"])?;
                match refresh_session(
                    &mut transaction,
                    &connection.project_id,
                    required_string(object, "refreshToken")?,
                )
                .await?
                {
                    RefreshSessionResult::Refreshed(session) => *session,
                    RefreshSessionResult::ReuseRevoked => {
                        let message = "refresh token reuse detected; this login was revoked";
                        sqlx::query(
                            r#"UPDATE gonvex_control_idempotency
                               SET state='failed',error=$4,updated_at=now()
                               WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3"#,
                        )
                        .bind(&connection.project_id)
                        .bind(&subject)
                        .bind(idempotency_key)
                        .bind(message)
                        .execute(&mut **transaction.transaction())
                        .await?;
                        transaction.commit().await?;
                        return Err(ControlError::InvalidArguments(message.to_owned()));
                    }
                }
            }
            _ => unreachable!(),
        };
        let result = session_result_from_directory(
            control,
            &mut transaction,
            &connection.project_id,
            &grant,
            &account,
            &requested_tenant,
        )
        .await?;
        sqlx::query(
            r#"UPDATE gonvex_control_idempotency
               SET state='completed',result=$4,error='',updated_at=now()
               WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3"#,
        )
        .bind(&connection.project_id)
        .bind(&subject)
        .bind(idempotency_key)
        .bind(Json(result.clone()))
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn reset_member_password(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let object = exact_object(args, &["memberId", "newPassword"])?;
        let member_id = required_string(object, "memberId")?;
        let new_password = required_string(object, "newPassword")?;
        if new_password.len() < 12 {
            return Err(ControlError::InvalidArguments(
                "newPassword must contain at least 12 characters".to_owned(),
            ));
        }
        let caller = tenant(connection)?;

        let mut mode_tx = control.begin_control_transaction(true).await?;
        let mode: String = sqlx::query_scalar(
            "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
        )
        .bind(&connection.project_id)
        .fetch_one(&mut **mode_tx.transaction())
        .await?;
        mode_tx.commit().await?;
        if !matches!(mode.as_str(), "gonvex-native" | "hybrid") {
            return Err(ControlError::InvalidArguments(
                "password management is owned by the configured external identity provider"
                    .to_owned(),
            ));
        }

        let route = control
            .resolve_tenant(&connection.project_id, &caller.route.tenant_id)
            .await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, true).await?;
        let target =
            sqlx::query("SELECT account_id,role FROM members WHERE id=$1 AND status='active'")
                .bind(member_id)
                .fetch_optional(&mut **tenant_tx.transaction())
                .await?
                .ok_or_else(|| {
                    ControlError::InvalidArguments("active tenant member was not found".to_owned())
                })?;
        let target_account: String = target.get("account_id");
        let target_role: String = target.get("role");
        tenant_tx.commit().await?;
        if caller.member.role == "admin" && matches!(target_role.as_str(), "owner" | "admin") {
            return Err(ControlError::TenantAdminRequired);
        }

        let subject = control_subject(connection, Authorization::TenantAdmin);
        let mut transaction = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut transaction,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            "control.accounts.resetMemberPassword",
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(result);
        }
        sqlx::query(
            r#"INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash,updated_at)
               VALUES($1,$2,$3,now()) ON CONFLICT(project_id,account_id) DO UPDATE SET
                 password_hash=EXCLUDED.password_hash,updated_at=now()"#,
        )
        .bind(&connection.project_id)
        .bind(&target_account)
        .bind(hash_password(new_password))
        .execute(&mut **transaction.transaction())
        .await?;
        for table in ["gonvex_auth_sessions", "gonvex_auth_refresh_tokens"] {
            let statement = format!(
                "UPDATE {table} SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2"
            );
            sqlx::query(&statement)
                .bind(&connection.project_id)
                .bind(&target_account)
                .execute(&mut **transaction.transaction())
                .await?;
        }
        let result = serde_json::json!({"updated":true});
        complete_control_idempotency(
            &mut transaction,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn provision_member_login(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let object = exact_object(args, &["email", "name", "password", "role", "permissions"])?;
        let email = normalize_email(required_string(object, "email")?)?;
        let name = required_string(object, "name")?;
        let password = required_string(object, "password")?;
        let role = required_string(object, "role")?;
        let permissions = object
            .get("permissions")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| {
                ControlError::InvalidArguments("permissions must be an object".to_owned())
            })?;
        if password.len() < 12 {
            return Err(ControlError::InvalidArguments(
                "password must contain at least 12 characters".to_owned(),
            ));
        }
        if !matches!(role, "owner" | "admin" | "member") {
            return Err(ControlError::InvalidArguments("role is invalid".to_owned()));
        }
        let caller = tenant(connection)?;
        if caller.member.role == "admin" && matches!(role, "owner" | "admin") {
            return Err(ControlError::TenantAdminRequired);
        }

        let mut control_tx = control.begin_control_transaction(false).await?;
        let mode: String = sqlx::query_scalar(
            "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
        )
        .bind(&connection.project_id)
        .fetch_one(&mut **control_tx.transaction())
        .await?;
        if !matches!(mode.as_str(), "gonvex-native" | "hybrid") {
            return Err(ControlError::InvalidArguments(
                "login provisioning is owned by the configured external identity provider; invite the account instead".to_owned(),
            ));
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "provision:{}:{}:{}",
                connection.project_id, caller.route.tenant_id, email
            ))
            .execute(&mut **control_tx.transaction())
            .await?;
        let existing = sqlx::query(
            r#"SELECT account.id,
                      EXISTS(SELECT 1 FROM gonvex_member_login_provisioning provisioning
                             WHERE provisioning.project_id=$1 AND provisioning.tenant_id=$3
                               AND provisioning.email=$2 AND provisioning.account_id=account.id)
                             AS resumable
               FROM accounts account
               WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2)
               ORDER BY account.id LIMIT 1"#,
        )
        .bind(&connection.project_id)
        .bind(&email)
        .bind(&caller.route.tenant_id)
        .fetch_optional(&mut **control_tx.transaction())
        .await?;
        let account_id = if let Some(row) = existing {
            if !row.get::<bool, _>("resumable") {
                return Err(ControlError::InvalidArguments(
                    "this account already exists; invite it instead of provisioning its login"
                        .to_owned(),
                ));
            }
            row.get::<String, _>("id")
        } else {
            let account_id = random_id("acct");
            sqlx::query(
                "INSERT INTO accounts(id,auth_realm_id,email,name,updated_at) VALUES($1,$2,$3,$4,now())",
            )
            .bind(&account_id)
            .bind(&connection.project_id)
            .bind(&email)
            .bind(name)
            .execute(&mut **control_tx.transaction())
            .await?;
            sqlx::query(
                r#"INSERT INTO account_identities
                   (project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
                   VALUES($1,$2,'password',$1,$3,$3,TRUE,now())"#,
            )
            .bind(&connection.project_id)
            .bind(&account_id)
            .bind(&email)
            .execute(&mut **control_tx.transaction())
            .await?;
            sqlx::query(
                "INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)",
            )
            .bind(&connection.project_id)
            .bind(&account_id)
            .bind(hash_password(password))
            .execute(&mut **control_tx.transaction())
            .await?;
            sqlx::query(
                r#"INSERT INTO gonvex_member_login_provisioning
                   (project_id,tenant_id,email,account_id,created_by)
                   VALUES($1,$2,$3,$4,$5)"#,
            )
            .bind(&connection.project_id)
            .bind(&caller.route.tenant_id)
            .bind(&email)
            .bind(&account_id)
            .bind(&caller.identity.account.id)
            .execute(&mut **control_tx.transaction())
            .await?;
            account_id
        };
        control_tx.commit().await?;

        let route = control
            .resolve_tenant(&connection.project_id, &caller.route.tenant_id)
            .await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        tenant_tx
            .set_command_id(&format!("member-provision:{account_id}"))
            .await?;
        let member_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&account_id)
        .fetch_optional(&mut **tenant_tx.transaction())
        .await?
        .unwrap_or_else(|| random_id("member"));
        let membership_revision: i64 = sqlx::query_scalar(
            r#"INSERT INTO members
               (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,'',$4,$5,now())
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 role=EXCLUDED.role,permissions=EXCLUDED.permissions,
                 membership_revision=members.membership_revision+1,updated_at=now()
               RETURNING membership_revision"#,
        )
        .bind(&member_id)
        .bind(&account_id)
        .bind(name)
        .bind(role)
        .bind(Json(permissions.clone()))
        .fetch_one(&mut **tenant_tx.transaction())
        .await?;
        tenant_tx.commit().await?;

        let subject = control_subject(connection, Authorization::TenantAdmin);
        let mut finish_tx = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            "control.accounts.provisionMemberLogin",
        )
        .await?
        {
            finish_tx.commit().await?;
            return Ok(result);
        }
        sqlx::query(
            r#"INSERT INTO account_tenant_index
               (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
               VALUES($1,$2,$3,'active',$4,now())
               ON CONFLICT(account_id,tenant_id) DO UPDATE SET member_id=EXCLUDED.member_id,
                 status='active',tenant_membership_revision=GREATEST(
                   account_tenant_index.tenant_membership_revision,
                   EXCLUDED.tenant_membership_revision),updated_at=now()"#,
        )
        .bind(&account_id)
        .bind(&caller.route.tenant_id)
        .bind(&member_id)
        .bind(membership_revision)
        .execute(&mut **finish_tx.transaction())
        .await?;
        let result = serde_json::json!({
            "updated":true,"accountId":account_id,"memberId":member_id,
        });
        complete_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        finish_tx.commit().await?;
        Ok(result)
    }

    async fn change_developer_membership(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let identity = account(connection)?;
        let object = exact_object(args, &["tenantId"])?;
        let tenant_id = required_string(object, "tenantId")?;
        let route = control
            .resolve_tenant(&connection.project_id, tenant_id)
            .await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        tenant_tx
            .set_command_id(&format!(
                "developer-membership:{}:{}",
                identity.account.id, idempotency_key
            ))
            .await?;
        let existing = sqlx::query(
            "SELECT id,membership_revision,status FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&identity.account.id)
        .fetch_optional(&mut **tenant_tx.transaction())
        .await?;
        let member_id = existing
            .as_ref()
            .map(|row| row.get::<String, _>("id"))
            .unwrap_or_else(|| random_id("member"));
        let (updated, membership_revision) = if path == "control.developer.provisionSelf" {
            let revision: i64 = sqlx::query_scalar(
                r#"INSERT INTO members
                   (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
                   VALUES($1,$2,'active',$3,$4,'admin','{"developer":true}'::jsonb,now())
                   ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                     avatar_url=EXCLUDED.avatar_url,role='admin',permissions='{"developer":true}'::jsonb,
                     membership_revision=members.membership_revision+1,updated_at=now()
                   RETURNING membership_revision"#,
            )
            .bind(&member_id)
            .bind(&identity.account.id)
            .bind(&identity.account.name)
            .bind(&identity.account.avatar_url)
            .fetch_one(&mut **tenant_tx.transaction())
            .await?;
            (true, revision)
        } else if let Some(row) = existing {
            let was_active = row.get::<String, _>("status") == "active";
            let revision: i64 = if was_active {
                sqlx::query_scalar(
                    r#"UPDATE members SET status='revoked',
                       membership_revision=membership_revision+1,updated_at=now()
                       WHERE id=$1 RETURNING membership_revision"#,
                )
                .bind(&member_id)
                .fetch_one(&mut **tenant_tx.transaction())
                .await?
            } else {
                row.get("membership_revision")
            };
            (was_active, revision)
        } else {
            (false, 0)
        };
        tenant_tx.commit().await?;

        let subject = control_subject(connection, Authorization::Developer);
        let mut finish_tx = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            path,
        )
        .await?
        {
            finish_tx.commit().await?;
            return Ok(result);
        }
        if path == "control.developer.provisionSelf" {
            sqlx::query(
                r#"INSERT INTO account_tenant_index
                   (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
                   VALUES($1,$2,$3,'active',$4,now())
                   ON CONFLICT(account_id,tenant_id) DO UPDATE SET member_id=EXCLUDED.member_id,
                     status='active',tenant_membership_revision=GREATEST(
                       account_tenant_index.tenant_membership_revision,
                       EXCLUDED.tenant_membership_revision),updated_at=now()"#,
            )
            .bind(&identity.account.id)
            .bind(tenant_id)
            .bind(&member_id)
            .bind(membership_revision)
            .execute(&mut **finish_tx.transaction())
            .await?;
        } else {
            sqlx::query(
                r#"UPDATE account_tenant_index SET status='revoked',
                   tenant_membership_revision=GREATEST(tenant_membership_revision,$3),updated_at=now()
                   WHERE account_id=$1 AND tenant_id=$2"#,
            )
            .bind(&identity.account.id)
            .bind(tenant_id)
            .bind(membership_revision)
            .execute(&mut **finish_tx.transaction())
            .await?;
        }
        let result = if path == "control.developer.provisionSelf" {
            serde_json::json!({"updated":true,"tenantId":tenant_id,"memberId":member_id})
        } else {
            serde_json::json!({"updated":updated})
        };
        complete_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        finish_tx.commit().await?;
        Ok(result)
    }

    async fn create_support_impersonation(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let actor = account(connection)?;
        let object = exact_object(args, &["accountId", "tenantId", "reason"])?;
        let target_account = required_string(object, "accountId")?;
        let tenant_id = required_string(object, "tenantId")?;
        let reason = required_string(object, "reason")?;
        control
            .admit_member(&connection.project_id, tenant_id, target_account)
            .await
            .map_err(|_| {
                ControlError::InvalidArguments(
                    "target account is not an active tenant member".to_owned(),
                )
            })?;
        let subject = control_subject(connection, Authorization::ProjectAdmin);
        let mut transaction = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut transaction,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            "control.support.createImpersonation",
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(result);
        }
        let id = random_id("imp");
        let token = secure_token("imp");
        let expires = Utc::now() + chrono::Duration::minutes(5);
        sqlx::query(
            r#"INSERT INTO gonvex_impersonation_grants
               (id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8)"#,
        )
        .bind(&id)
        .bind(&connection.project_id)
        .bind(sha256_hex(token.as_bytes()))
        .bind(&actor.account.id)
        .bind(target_account)
        .bind(tenant_id)
        .bind(reason)
        .bind(expires)
        .execute(&mut **transaction.transaction())
        .await?;
        let result = serde_json::json!({
            "id":id,"token":token,"expiresAt":timestamp(expires),
        });
        complete_control_idempotency(
            &mut transaction,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn create_tenant(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let identity = account(connection)?.clone();
        let object = exact_object(args, &["name", "domain"])?;
        let requested_name = required_string(object, "name")?.to_owned();
        let requested_domain = object
            .get("domain")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_lowercase)
            .unwrap_or_else(|| slug(&requested_name));
        if requested_domain.is_empty()
            || requested_domain.len() > 63
            || !requested_domain.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || (byte == b'-' && index > 0 && index + 1 < requested_domain.len())
            })
        {
            return Err(ControlError::InvalidArguments(
                "tenant domain must be a lowercase DNS label".to_owned(),
            ));
        }
        let base_url = self
            .inner
            .config
            .default_database_url
            .clone()
            .ok_or_else(|| {
                ControlError::InvalidArguments("DATABASE_URL is not configured".to_owned())
            })?;
        let mut reserve_tx = control.begin_control_transaction(false).await?;
        let project = sqlx::query(
            r#"SELECT COALESCE(NULLIF(database_mode,''),'single') AS database_mode,
                      EXISTS(SELECT 1 FROM gonvex_auth_providers
                             WHERE project_id=$1 AND enabled=TRUE AND signup_mode='inviteOnly')
                        AS invite_only
               FROM gonvex_runtime_projects WHERE id=$1"#,
        )
        .bind(&connection.project_id)
        .fetch_one(&mut **reserve_tx.transaction())
        .await?;
        if project.get::<String, _>("database_mode") != "multiTenant" {
            return Err(ControlError::InvalidArguments(
                "project is not configured for tenant databases".to_owned(),
            ));
        }
        if project.get::<bool, _>("invite_only") {
            return Err(ControlError::InvalidArguments(
                "this project allows tenant creation only through its Control Plane".to_owned(),
            ));
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "tenant-create:{}:{}",
                connection.project_id, idempotency_key
            ))
            .execute(&mut **reserve_tx.transaction())
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "tenant-domain:{}:{}",
                connection.project_id, requested_domain
            ))
            .execute(&mut **reserve_tx.transaction())
            .await?;
        let checkpoint = sqlx::query(
            r#"SELECT tenant_id,database_name,database_alias,name,domain,account_id
               FROM gonvex_tenant_provisioning WHERE project_id=$1 AND idempotency_key=$2"#,
        )
        .bind(&connection.project_id)
        .bind(idempotency_key)
        .fetch_optional(&mut **reserve_tx.transaction())
        .await?;
        let (tenant_id, database_name, database_alias, name, domain, owner_account) = if let Some(
            row,
        ) =
            checkpoint
        {
            (
                row.get("tenant_id"),
                row.get("database_name"),
                row.get("database_alias"),
                row.get("name"),
                row.get("domain"),
                row.get("account_id"),
            )
        } else {
            let tenant_id = random_id("tenant");
            let database_name = format!("gonvex_tenant_{}", uuid::Uuid::new_v4().simple());
            let mut database_alias = slug(&requested_name);
            if database_alias.is_empty() {
                database_alias = "workspace".to_owned();
            }
            database_alias.push('-');
            database_alias.push_str(
                &tenant_id
                    .chars()
                    .filter(|char| *char != '-')
                    .take(8)
                    .collect::<String>(),
            );
            let inserted = sqlx::query(
                    r#"INSERT INTO gonvex_tenant_provisioning
                       (project_id,idempotency_key,tenant_id,database_name,database_alias,name,domain,account_id)
                       SELECT $1,$2,$3,$4,$5,$6,$7,$8
                       WHERE NOT EXISTS (
                         SELECT 1 FROM gonvex_runtime_tenants
                         WHERE project_id=$1 AND lower(domain)=lower($7)
                           AND deleted_at IS NULL AND status NOT IN ('deleted','disabled')
                       )"#,
                )
                .bind(&connection.project_id)
                .bind(idempotency_key)
                .bind(&tenant_id)
                .bind(&database_name)
                .bind(&database_alias)
                .bind(&requested_name)
                .bind(&requested_domain)
                .bind(&identity.account.id)
                .execute(&mut **reserve_tx.transaction())
                .await?
                .rows_affected();
            if inserted == 0 {
                return Err(ControlError::InvalidArguments(
                    "tenant domain is already in use".to_owned(),
                ));
            }
            (
                tenant_id,
                database_name,
                database_alias,
                requested_name.clone(),
                requested_domain.clone(),
                identity.account.id.clone(),
            )
        };
        if owner_account != identity.account.id {
            return Err(ControlError::InvalidArguments(
                "tenant provisioning command belongs to another account".to_owned(),
            ));
        }
        reserve_tx.commit().await?;

        let database_url = control.create_database(&base_url, &database_name).await?;
        let mut directory_tx = control.begin_control_transaction(false).await?;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants
               (relationship_id,project_id,tenant_id,name,database_alias,database_name,
                database_url,status,description,provisioned,runtime_created)
               VALUES($1,$2,$1,$3,$4,$5,$6,$7,'active','Account-created tenant database.',FALSE,TRUE)
               ON CONFLICT(project_id,tenant_id) DO UPDATE SET name=EXCLUDED.name,
                 domain=EXCLUDED.domain,database_url=EXCLUDED.database_url,updated_at=now()"#,
        )
        .bind(&tenant_id)
        .bind(&connection.project_id)
        .bind(&name)
        .bind(&database_alias)
        .bind(&database_name)
        .bind(&database_url)
        .bind(&domain)
        .execute(&mut **directory_tx.transaction())
        .await?;
        directory_tx.commit().await?;
        let route = gonvex_postgres::TenantRoute {
            project_id: connection.project_id.clone(),
            tenant_id: tenant_id.clone(),
            database_url,
        };
        let module = self
            .inner
            .modules
            .project(&connection.project_id)
            .await
            .ok_or_else(|| {
                ControlError::InvalidArguments("project module is unavailable".to_owned())
            })?;
        control
            .clone()
            .provision_tenant_database(route.clone(), module.migrations.clone())
            .await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        tenant_tx
            .set_command_id(&format!("tenant-create:{tenant_id}"))
            .await?;
        let member_id =
            sqlx::query_scalar::<_, String>("SELECT id FROM members WHERE account_id=$1 LIMIT 1")
                .bind(&identity.account.id)
                .fetch_optional(&mut **tenant_tx.transaction())
                .await?
                .unwrap_or_else(|| random_id("member"));
        let revision: i64 = sqlx::query_scalar(
            r#"INSERT INTO members
               (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,$4,'owner','{}'::jsonb,now())
               ON CONFLICT(id) DO UPDATE SET status='active',role='owner',
                 membership_revision=members.membership_revision+1,updated_at=now()
               RETURNING membership_revision"#,
        )
        .bind(&member_id)
        .bind(&identity.account.id)
        .bind(&identity.account.name)
        .bind(&identity.account.avatar_url)
        .fetch_one(&mut **tenant_tx.transaction())
        .await?;
        tenant_tx.commit().await?;

        let subject = control_subject(connection, Authorization::Account);
        let mut finish_tx = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            "control.tenants.create",
        )
        .await?
        {
            finish_tx.commit().await?;
            return Ok(result);
        }
        sqlx::query(
            r#"INSERT INTO account_tenant_index
               (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
               VALUES($1,$2,$3,'active',$4,now()) ON CONFLICT(account_id,tenant_id) DO UPDATE SET
                 member_id=EXCLUDED.member_id,status='active',
                 tenant_membership_revision=GREATEST(account_tenant_index.tenant_membership_revision,
                                                     EXCLUDED.tenant_membership_revision),updated_at=now()"#,
        )
        .bind(&identity.account.id)
        .bind(&tenant_id)
        .bind(&member_id)
        .bind(revision)
        .execute(&mut **finish_tx.transaction())
        .await?;
        sqlx::query(
            "UPDATE gonvex_runtime_tenants SET provisioned=TRUE,updated_at=now() WHERE project_id=$1 AND tenant_id=$2",
        )
        .bind(&connection.project_id)
        .bind(&tenant_id)
        .execute(&mut **finish_tx.transaction())
        .await?;
        let result = serde_json::json!({
            "id":tenant_id,"name":name,"role":"owner","permissions":{},
            "domain":domain,"timezone":"UTC","description":"Account-created tenant database.",
            "profile":{},
        });
        complete_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        finish_tx.commit().await?;
        Ok(result)
    }

    async fn create_demo_account(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let object = exact_object(args, &["tenantId", "email", "name", "password", "label"])?;
        let tenant_id = required_string(object, "tenantId")?;
        let email = normalize_email(required_string(object, "email")?)?;
        let name = required_string(object, "name")?;
        let password = required_string(object, "password")?;
        let label = required_string(object, "label")?;
        if password.len() < 12 {
            return Err(ControlError::InvalidArguments(
                "password must contain at least 12 characters".to_owned(),
            ));
        }
        let route = control
            .resolve_tenant(&connection.project_id, tenant_id)
            .await?;
        let subject = control_subject(connection, Authorization::ProjectAdmin);
        let mut reserve_tx = control.begin_control_transaction(false).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "demo:{}:{}:{}",
                connection.project_id, tenant_id, email
            ))
            .execute(&mut **reserve_tx.transaction())
            .await?;
        if let Some(result) = claim_control_saga(
            &mut reserve_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            "control.demos.create",
        )
        .await?
        {
            reserve_tx.commit().await?;
            return Ok(result);
        }
        let existing = sqlx::query(
            r#"SELECT account.id,
                      EXISTS(SELECT 1 FROM gonvex_demo_accounts demo
                             WHERE demo.project_id=$1 AND demo.account_id=account.id
                               AND demo.tenant_id=$3) AS is_demo
               FROM accounts account
               WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2)
               ORDER BY account.id LIMIT 1"#,
        )
        .bind(&connection.project_id)
        .bind(&email)
        .bind(tenant_id)
        .fetch_optional(&mut **reserve_tx.transaction())
        .await?;
        let account_id = if let Some(row) = existing {
            if !row.get::<bool, _>("is_demo") {
                return Err(ControlError::InvalidArguments(
                    "email already belongs to a non-demo account".to_owned(),
                ));
            }
            row.get::<String, _>("id")
        } else {
            random_id("acct")
        };
        sqlx::query(
            r#"INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at)
               VALUES($1,$2,$3,$4,NULL,now()) ON CONFLICT(id) DO UPDATE SET
                 email=EXCLUDED.email,name=EXCLUDED.name,disabled_at=NULL,updated_at=now()"#,
        )
        .bind(&account_id)
        .bind(&connection.project_id)
        .bind(&email)
        .bind(name)
        .execute(&mut **reserve_tx.transaction())
        .await?;
        sqlx::query(
            r#"INSERT INTO account_identities
               (project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
               VALUES($1,$2,'password',$1,$3,$3,TRUE,now())
               ON CONFLICT(project_id,provider,issuer,subject) DO UPDATE SET
                 account_id=EXCLUDED.account_id,email=EXCLUDED.email,
                 verified_email=TRUE,updated_at=now()"#,
        )
        .bind(&connection.project_id)
        .bind(&account_id)
        .bind(&email)
        .execute(&mut **reserve_tx.transaction())
        .await?;
        sqlx::query(
            r#"INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash)
               VALUES($1,$2,$3) ON CONFLICT(project_id,account_id) DO UPDATE SET
                 password_hash=EXCLUDED.password_hash,updated_at=now()"#,
        )
        .bind(&connection.project_id)
        .bind(&account_id)
        .bind(hash_password(password))
        .execute(&mut **reserve_tx.transaction())
        .await?;
        sqlx::query(
            r#"INSERT INTO gonvex_demo_accounts(project_id,account_id,tenant_id,label)
               VALUES($1,$2,$3,$4) ON CONFLICT(project_id,account_id) DO UPDATE SET
                 tenant_id=EXCLUDED.tenant_id,label=EXCLUDED.label"#,
        )
        .bind(&connection.project_id)
        .bind(&account_id)
        .bind(tenant_id)
        .bind(label)
        .execute(&mut **reserve_tx.transaction())
        .await?;
        reserve_tx.commit().await?;

        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        tenant_tx
            .set_command_id(&format!("demo-create:{account_id}"))
            .await?;
        let member_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&account_id)
        .fetch_optional(&mut **tenant_tx.transaction())
        .await?
        .unwrap_or_else(|| random_id("member"));
        let membership_revision: i64 = sqlx::query_scalar(
            r#"INSERT INTO members
               (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,'','member','{}'::jsonb,now())
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 membership_revision=members.membership_revision+1,updated_at=now()
               RETURNING membership_revision"#,
        )
        .bind(&member_id)
        .bind(&account_id)
        .bind(name)
        .fetch_one(&mut **tenant_tx.transaction())
        .await?;
        tenant_tx.commit().await?;

        let mut finish_tx = control.begin_control_transaction(false).await?;
        sqlx::query(
            r#"INSERT INTO account_tenant_index
               (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
               VALUES($1,$2,$3,'active',$4,now()) ON CONFLICT(account_id,tenant_id) DO UPDATE SET
                 member_id=EXCLUDED.member_id,status='active',
                 tenant_membership_revision=GREATEST(account_tenant_index.tenant_membership_revision,
                                                     EXCLUDED.tenant_membership_revision),updated_at=now()"#,
        )
        .bind(&account_id)
        .bind(tenant_id)
        .bind(&member_id)
        .bind(membership_revision)
        .execute(&mut **finish_tx.transaction())
        .await?;
        let result = serde_json::json!({"accountId":account_id,"memberId":member_id});
        complete_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        finish_tx.commit().await?;
        Ok(result)
    }

    async fn delete_demo_account(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let object = exact_object(args, &["accountId"])?;
        let account_id = required_string(object, "accountId")?;
        let subject = control_subject(connection, Authorization::ProjectAdmin);
        let mut lookup_tx = control.begin_control_transaction(false).await?;
        if let Some(result) = claim_control_saga(
            &mut lookup_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            ControlKind::Reducer,
            "control.demos.delete",
        )
        .await?
        {
            lookup_tx.commit().await?;
            return Ok(result);
        }
        let tenant_id = sqlx::query_scalar::<_, String>(
            "SELECT tenant_id FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2",
        )
        .bind(&connection.project_id)
        .bind(account_id)
        .fetch_optional(&mut **lookup_tx.transaction())
        .await?
        .ok_or_else(|| ControlError::InvalidArguments("demo account not found".to_owned()))?;
        lookup_tx.commit().await?;

        let route = control
            .resolve_tenant(&connection.project_id, &tenant_id)
            .await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        tenant_tx
            .set_command_id(&format!("demo-delete:{account_id}"))
            .await?;
        let membership_revision = sqlx::query_scalar::<_, i64>(
            r#"UPDATE members SET status='revoked',
               membership_revision=membership_revision+1,updated_at=now()
               WHERE account_id=$1 AND status='active' RETURNING membership_revision"#,
        )
        .bind(account_id)
        .fetch_optional(&mut **tenant_tx.transaction())
        .await?
        .unwrap_or(0);
        tenant_tx.commit().await?;

        let mut finish_tx = control.begin_control_transaction(false).await?;
        sqlx::query("DELETE FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2")
            .bind(&connection.project_id)
            .bind(account_id)
            .execute(&mut **finish_tx.transaction())
            .await?;
        sqlx::query(
            "UPDATE accounts SET disabled_at=now(),updated_at=now() WHERE id=$1 AND auth_realm_id=$2",
        )
        .bind(account_id)
        .bind(&connection.project_id)
        .execute(&mut **finish_tx.transaction())
        .await?;
        sqlx::query(
            r#"UPDATE account_tenant_index SET status='revoked',
               tenant_membership_revision=GREATEST(tenant_membership_revision,$3),updated_at=now()
               WHERE account_id=$1 AND tenant_id=$2"#,
        )
        .bind(account_id)
        .bind(&tenant_id)
        .bind(membership_revision)
        .execute(&mut **finish_tx.transaction())
        .await?;
        let result = serde_json::json!({"updated":true});
        complete_control_idempotency(
            &mut finish_tx,
            &connection.project_id,
            &subject,
            idempotency_key,
            &result,
        )
        .await?;
        finish_tx.commit().await?;
        Ok(result)
    }

    async fn accept_invitation(
        &self,
        control: &gonvex_postgres::ControlPlane,
        connection: &ControlConnection,
        args: &Value,
        idempotency_key: &str,
    ) -> Result<Value, ControlError> {
        let identity = account(connection)?;
        let object = exact_object(args, &["token", "invitationToken"])?;
        let token = optional_string(object, "token")
            .or_else(|| optional_string(object, "invitationToken"))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ControlError::InvalidArguments("token is required".to_owned()))?;
        let mut claim_tx = control.begin_control_transaction(false).await?;
        let row = sqlx::query(
            r#"SELECT id,tenant_id,email,role,permissions,team_ids,
                      allowed_auth_providers,application_payload,expires_at,
                      revoked_at,accepted_at,accepted_account_id,accepted_idempotency_key,
                      handoff_state
               FROM gonvex_auth_membership_invitations
               WHERE project_id=$1 AND token_hash=$2 FOR UPDATE"#,
        )
        .bind(&connection.project_id)
        .bind(sha256_hex(token.as_bytes()))
        .fetch_optional(&mut **claim_tx.transaction())
        .await?
        .ok_or_else(invalid_invitation)?;
        let invitation_id: String = row.get("id");
        let tenant_id: String = row.get("tenant_id");
        let email: String = row.get("email");
        let role: String = row.get("role");
        let permissions = row.get::<Json<Value>, _>("permissions").0;
        let team_ids = row.get::<Json<Value>, _>("team_ids").0;
        let allowed_providers = row.get::<Json<Value>, _>("allowed_auth_providers").0;
        let payload = row.get::<Json<Value>, _>("application_payload").0;
        let expires_at: DateTime<Utc> = row.get("expires_at");
        let revoked: Option<DateTime<Utc>> = row.get("revoked_at");
        let accepted: Option<DateTime<Utc>> = row.get("accepted_at");
        let accepted_account: Option<String> = row.get("accepted_account_id");
        let accepted_key: Option<String> = row.get("accepted_idempotency_key");
        if revoked.is_some() || expires_at <= Utc::now() {
            return Err(invalid_invitation());
        }
        if accepted.is_some() {
            if accepted_account.as_deref() != Some(identity.account.id.as_str())
                || accepted_key.as_deref() != Some(idempotency_key)
            {
                return Err(invalid_invitation());
            }
            claim_tx.commit().await?;
            let session = control
                .tenant_session_for_account(
                    &connection.project_id,
                    &tenant_id,
                    &identity.account.id,
                )
                .await?;
            return Ok(serde_json::json!({
                "tenantId":tenant_id,
                "memberId":session.member.id,
            }));
        }
        if normalize_email(&email)? != normalize_email(&identity.account.email)? {
            return Err(ControlError::InvalidArguments(
                "invitation does not belong to this account".to_owned(),
            ));
        }
        let allowed = allowed_providers.as_array().ok_or_else(|| {
            ControlError::InvalidArguments("invitation provider policy is invalid".to_owned())
        })?;
        if !allowed.is_empty() {
            let linked = sqlx::query_scalar::<_, String>(
                "SELECT DISTINCT provider FROM account_identities WHERE account_id=$1",
            )
            .bind(&identity.account.id)
            .fetch_all(&mut **claim_tx.transaction())
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
            if !allowed
                .iter()
                .filter_map(Value::as_str)
                .any(|provider| linked.contains(provider))
            {
                return Err(ControlError::InvalidArguments(
                    "invitation requires a linked authentication provider".to_owned(),
                ));
            }
        }
        if !permissions.is_object() || !team_ids.is_array() {
            return Err(ControlError::InvalidArguments(
                "invitation application data is invalid".to_owned(),
            ));
        }
        let handoff_state: String = row.get("handoff_state");
        if handoff_state == "claimed"
            && (accepted_account.as_deref() != Some(identity.account.id.as_str())
                || accepted_key.as_deref() != Some(idempotency_key))
        {
            return Err(ControlError::InvalidArguments(
                "invitation acceptance is already in progress".to_owned(),
            ));
        }
        let affected = sqlx::query(
            r#"UPDATE gonvex_auth_membership_invitations
               SET handoff_state='claimed',handoff_command_id=$3,
                   accepted_account_id=$4,accepted_idempotency_key=$5,updated_at=now()
               WHERE project_id=$1 AND id=$2 AND accepted_at IS NULL AND revoked_at IS NULL
                 AND (handoff_state='pending' OR
                      (handoff_state='claimed' AND accepted_account_id=$4
                       AND accepted_idempotency_key=$5))"#,
        )
        .bind(&connection.project_id)
        .bind(&invitation_id)
        .bind(format!("invitation:{invitation_id}"))
        .bind(&identity.account.id)
        .bind(idempotency_key)
        .execute(&mut **claim_tx.transaction())
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(ControlError::InvalidArguments(
                "invitation acceptance is already in progress".to_owned(),
            ));
        }
        claim_tx.commit().await?;

        let route = control
            .resolve_tenant(&connection.project_id, &tenant_id)
            .await?;
        control.ensure_reducer_idempotency(&route).await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        let command_id = format!("invitation:{invitation_id}");
        tenant_tx.set_command_id(&command_id).await?;
        let claimed = tenant_tx
            .claim_reducer(
                &identity.account.id,
                idempotency_key,
                "$system.invitation.accept",
            )
            .await?;
        let member_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&identity.account.id)
        .fetch_optional(&mut **tenant_tx.transaction())
        .await?
        .unwrap_or_else(|| format!("member_{invitation_id}"));
        tenant_tx
            .set_invocation_provenance(TransactionAttribution {
                root_command_id: &command_id,
                root_channel: "ui",
                channel: "ui",
                actor_account_id: Some(&identity.account.id),
                actor_member_id: Some(&member_id),
                on_behalf_of_member_id: None,
                agent_execution_id: None,
            })
            .await?;
        if claimed {
            sqlx::query(
                r#"INSERT INTO members
                   (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
                   VALUES($1,$2,'active',$3,$4,$5,$6,now())
                   ON CONFLICT(id) DO UPDATE SET account_id=EXCLUDED.account_id,status='active',
                     display_name=EXCLUDED.display_name,avatar_url=EXCLUDED.avatar_url,
                     role=EXCLUDED.role,permissions=EXCLUDED.permissions,
                     membership_revision=members.membership_revision+1,updated_at=now()"#,
            )
            .bind(&member_id)
            .bind(&identity.account.id)
            .bind(&identity.account.name)
            .bind(&identity.account.avatar_url)
            .bind(&role)
            .bind(Json(permissions.clone()))
            .execute(&mut **tenant_tx.transaction())
            .await?;
            let module = self
                .inner
                .modules
                .project(&connection.project_id)
                .await
                .ok_or_else(|| {
                    ControlError::InvalidArguments("project module is unavailable".to_owned())
                })?;
            let reducer_path = module.invitation_acceptance_reducer.trim();
            let has_application_payload = team_ids
                .as_array()
                .is_some_and(|teams: &Vec<Value>| !teams.is_empty())
                || payload
                    .as_object()
                    .is_some_and(|payload: &Map<String, Value>| !payload.is_empty());
            if reducer_path.is_empty() && has_application_payload {
                return Err(ControlError::InvalidArguments(
                    "module must declare invitationAcceptance for application invitation payloads"
                        .to_owned(),
                ));
            }
            let mut reducer_result = serde_json::json!({"memberId":member_id});
            if !reducer_path.is_empty() {
                let definition = module.functions.get(reducer_path).ok_or_else(|| {
                    ControlError::InvalidArguments(
                        "invitation acceptance Reducer is invalid".to_owned(),
                    )
                })?;
                if definition.kind != "reducer" || !definition.internal {
                    return Err(ControlError::InvalidArguments(
                        "invitation acceptance Reducer is invalid".to_owned(),
                    ));
                }
                let member = Member {
                    id: member_id.clone(),
                    account_id: identity.account.id.clone(),
                    status: "active".to_owned(),
                    display_name: identity.account.name.clone(),
                    avatar_url: identity.account.avatar_url.clone(),
                    role: role.clone(),
                    permissions: permissions.clone(),
                    membership_revision: 1,
                };
                let tenant_session = TenantSession {
                    identity: identity.clone(),
                    route: route.clone(),
                    member,
                    admission_revision: 0,
                };
                let mut handler = DatabaseHostCalls::new(tenant_tx, DatabaseCapability::Reducer)
                    .with_actor(&identity.account.id, &identity.account.email);
                let mut invocation = crate::execution::invocation(
                    &tenant_session,
                    module.generation,
                    reducer_path,
                    "reducer",
                    serde_json::json!({
                        "accountId":identity.account.id,
                        "memberId":member_id,
                        "invitationId":invitation_id,
                        "teamIds":team_ids,
                        "payload":payload,
                    }),
                    Some(DatabaseCapability::Reducer),
                    crate::execution::direct_provenance(
                        &tenant_session,
                        gonvex_module_runtime::InvocationChannel::Ui,
                        idempotency_key,
                        &module.artifact_hash,
                    ),
                );
                invocation.context.capabilities.action_outbox = true;
                reducer_result = self
                    .inner
                    .module_host
                    .invoke(invocation, &mut handler)
                    .await
                    .map_err(|error| ControlError::InvalidArguments(error.to_string()))?;
                handler
                    .transaction_mut()
                    .store_reducer_result(&identity.account.id, idempotency_key, &reducer_result)
                    .await?;
                handler
                    .finish(true)
                    .await
                    .map_err(ControlError::InvalidArguments)?;
            } else {
                tenant_tx
                    .store_reducer_result(&identity.account.id, idempotency_key, &reducer_result)
                    .await?;
                tenant_tx.commit().await?;
            }
            let _ = reducer_result;
        } else {
            tenant_tx.rollback().await?;
        }

        let mut finish_tx = control.begin_control_transaction(false).await?;
        let affected = sqlx::query(
            r#"UPDATE gonvex_auth_membership_invitations
               SET accepted_at=now(),completed_at=now(),handoff_state='completed',
                   accepted_account_id=$3,accepted_idempotency_key=$4,updated_at=now()
               WHERE project_id=$1 AND id=$2 AND accepted_at IS NULL AND revoked_at IS NULL
                 AND handoff_state='claimed' AND accepted_account_id=$3
                 AND accepted_idempotency_key=$4"#,
        )
        .bind(&connection.project_id)
        .bind(&invitation_id)
        .bind(&identity.account.id)
        .bind(idempotency_key)
        .execute(&mut **finish_tx.transaction())
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(ControlError::InvalidArguments(
                "invitation was already accepted".to_owned(),
            ));
        }
        sqlx::query(
            r#"INSERT INTO account_tenant_index
               (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
               VALUES($1,$2,$3,'active',1,now())
               ON CONFLICT(account_id,tenant_id) DO UPDATE SET member_id=EXCLUDED.member_id,
                 status='active',tenant_membership_revision=GREATEST(
                   account_tenant_index.tenant_membership_revision,
                   EXCLUDED.tenant_membership_revision),updated_at=now()"#,
        )
        .bind(&identity.account.id)
        .bind(&tenant_id)
        .bind(&member_id)
        .execute(&mut **finish_tx.transaction())
        .await?;
        finish_tx.commit().await?;
        Ok(serde_json::json!({"tenantId":tenant_id,"memberId":member_id}))
    }

    pub fn control_query_is_live(path: &str) -> bool {
        definition(path)
            .is_some_and(|definition| definition.kind == ControlKind::Query && definition.live)
    }

    pub async fn open_control_query(
        &self,
        connection: &ControlConnection,
        id: String,
        path: String,
        args: Value,
    ) -> Result<(ServerMessage, ControlSubscription), ControlError> {
        if !Self::control_query_is_live(&path) {
            return Err(ControlError::InvalidArguments(format!(
                "Control Plane query {path:?} is not subscribable"
            )));
        }
        let result = self.execute_control_query(connection, &path, &args).await?;
        Ok((
            control_query_message(&id, &path, result.clone(), "initial"),
            ControlSubscription {
                id,
                path,
                args,
                last_result: result,
            },
        ))
    }

    pub async fn refresh_control_queries(
        &self,
        connection: &ControlConnection,
        subscriptions: &mut BTreeMap<String, ControlSubscription>,
        reason: &str,
    ) -> Vec<ServerMessage> {
        let mut messages = Vec::new();
        for subscription in subscriptions.values_mut() {
            match self
                .execute_control_query(connection, &subscription.path, &subscription.args)
                .await
            {
                Ok(result) if result != subscription.last_result => {
                    subscription.last_result = result.clone();
                    messages.push(control_query_message(
                        &subscription.id,
                        &subscription.path,
                        result,
                        reason,
                    ));
                }
                Ok(_) => {}
                Err(error) => messages.push(ServerMessage::QueryError {
                    id: subscription.id.clone(),
                    path: Some(subscription.path.clone()),
                    error: error.to_string(),
                }),
            }
        }
        messages
    }

    async fn authorize_control(
        &self,
        connection: &ControlConnection,
        authorization: Authorization,
    ) -> Result<(), ControlError> {
        if authorization == Authorization::Public {
            return Ok(());
        }
        let identity = connection
            .identity
            .as_ref()
            .ok_or(ControlError::AuthenticationRequired)?;
        if authorization == Authorization::Account {
            return Ok(());
        }
        if authorization == Authorization::TenantAdmin {
            let tenant = connection
                .tenant
                .as_ref()
                .ok_or(ControlError::TenantAdminRequired)?;
            let control = self.control_plane().await?;
            let (_, member, _) = control
                .admit_member(
                    &identity.project_id,
                    &tenant.route.tenant_id,
                    &identity.account.id,
                )
                .await
                .map_err(|_| ControlError::TenantAdminRequired)?;
            return if matches!(member.role.as_str(), "owner" | "admin") {
                Ok(())
            } else {
                Err(ControlError::TenantAdminRequired)
            };
        }
        if !connection.impersonation_id.is_empty() {
            return Err(ControlError::AdministrationDuringImpersonation);
        }
        let control = self.control_plane().await?;
        let mut transaction = control.begin_control_transaction(true).await?;
        let roles: &[&str] = if authorization == Authorization::ProjectAdmin {
            &["owner", "admin"]
        } else {
            &["owner", "admin", "dev"]
        };
        let allowed: bool = sqlx::query_scalar(
            r#"SELECT EXISTS(
                   SELECT 1 FROM gonvex_runtime_projects
                   WHERE id=$1 AND lower(owner_email)=lower($2) AND owner_email<>''
                   UNION ALL
                   SELECT 1 FROM gonvex_project_members
                   WHERE project_id=$1 AND lower(email)=lower($2) AND role = ANY($3)
               )"#,
        )
        .bind(&connection.project_id)
        .bind(&identity.account.email)
        .bind(roles)
        .fetch_one(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        if allowed {
            Ok(())
        } else if authorization == Authorization::ProjectAdmin {
            Err(ControlError::ProjectAdminRequired)
        } else {
            Err(ControlError::DeveloperRequired)
        }
    }

    async fn control_query(
        &self,
        _control: &gonvex_postgres::ControlPlane,
        transaction: &mut TenantTransaction,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
    ) -> Result<Value, ControlError> {
        match path {
            "control.accounts.me" => {
                empty_args(args)?;
                let account = &account(connection)?.account;
                Ok(serde_json::json!({
                    "id": account.id,
                    "email": account.email,
                    "name": account.name,
                    "avatarUrl": account.avatar_url,
                }))
            }
            "control.auth.publicSettings" => {
                empty_args(args)?;
                let mode: String = sqlx::query_scalar(
                    "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
                )
                .bind(&connection.project_id)
                .fetch_one(&mut **transaction.transaction())
                .await?;
                require_auth_mode(&mode)?;
                let rows = sqlx::query(
                    "SELECT provider FROM gonvex_auth_providers WHERE project_id=$1 AND enabled=TRUE ORDER BY provider",
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                let mut providers = Vec::new();
                if matches!(mode.as_str(), "gonvex-native" | "hybrid") {
                    providers.push(Value::String("password".to_owned()));
                }
                for row in rows {
                    let provider: String = row.get("provider");
                    let allowed = provider != "password"
                        && (mode == "hybrid"
                            || mode == provider
                            || (mode == "gonvex-native"
                                && !matches!(provider.as_str(), "firebase" | "external-oidc")));
                    if allowed {
                        providers.push(Value::String(provider));
                    }
                }
                Ok(serde_json::json!({"mode":mode,"providers":providers}))
            }
            "control.auth.realms.list" => {
                empty_args(args)?;
                let mode: String = sqlx::query_scalar(
                    "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
                )
                .bind(&connection.project_id)
                .fetch_one(&mut **transaction.transaction())
                .await?;
                let rows = sqlx::query(
                    r#"SELECT provider, enabled, signup_mode, azure_tenant_id, client_id,
                              client_secret_encrypted IS NOT NULL AS has_client_secret,
                              issuer,audience,jwks_url,firebase_project_id,firebase_tenant_id,
                              admin_credentials_encrypted IS NOT NULL AS has_admin_credentials
                       FROM gonvex_auth_providers WHERE project_id=$1 ORDER BY provider"#,
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                let mut items = vec![serde_json::json!({
                    "provider":"password",
                    "enabled":matches!(mode.as_str(),"gonvex-native"|"hybrid"),
                    "signupMode":"inviteOnly",
                    "hasClientSecret":false,
                    "hasAdminCredentials":false,
                    "authMode":mode,
                })];
                for row in rows {
                    items.push(serde_json::json!({
                        "provider":row.get::<String,_>("provider"),
                        "enabled":row.get::<bool,_>("enabled"),
                        "signupMode":row.get::<String,_>("signup_mode"),
                        "azureTenantId":row.get::<String,_>("azure_tenant_id"),
                        "clientId":row.get::<String,_>("client_id"),
                        "hasClientSecret":row.get::<bool,_>("has_client_secret"),
                        "issuer":row.get::<String,_>("issuer"),
                        "audience":row.get::<String,_>("audience"),
                        "jwksUrl":row.get::<String,_>("jwks_url"),
                        "firebaseProjectId":row.get::<String,_>("firebase_project_id"),
                        "firebaseTenantId":row.get::<String,_>("firebase_tenant_id"),
                        "hasAdminCredentials":row.get::<bool,_>("has_admin_credentials"),
                        "authMode":mode,
                    }));
                }
                Ok(Value::Array(items))
            }
            "control.tenants.mine" | "users.myTenants" => unreachable!(),
            "control.tenants.getByDomain" => {
                let object = exact_object(args, &["domain"])?;
                let domain = required_string(object, "domain")?;
                let row = sqlx::query(
                    r#"SELECT tenant_id,name,domain FROM gonvex_runtime_tenants
                       WHERE project_id=$1 AND lower(domain)=lower($2)
                         AND deleted_at IS NULL AND status <> 'deleted'"#,
                )
                .bind(&connection.project_id)
                .bind(domain)
                .fetch_optional(&mut **transaction.transaction())
                .await?
                .ok_or_else(|| ControlError::InvalidArguments("tenant not found".to_owned()))?;
                Ok(serde_json::json!({
                    "id":row.get::<String,_>("tenant_id"),
                    "name":row.get::<String,_>("name"),
                    "domain":row.get::<String,_>("domain"),
                }))
            }
            "control.invitations.lookup" | "tenants.getInvitationByToken" => {
                let object = exact_object(args, &["token", "invitationToken"])?;
                let token = optional_string(object, "token")
                    .or_else(|| optional_string(object, "invitationToken"))
                    .filter(|token| !token.is_empty())
                    .ok_or_else(|| {
                        ControlError::InvalidArguments("token is required".to_owned())
                    })?;
                let row = sqlx::query(
                    r#"SELECT invitation.tenant_id,tenant.name,invitation.role,invitation.email,
                              invitation.team_ids,invitation.allowed_auth_providers,invitation.expires_at
                       FROM gonvex_auth_membership_invitations invitation
                       JOIN gonvex_runtime_tenants tenant
                         ON tenant.project_id=invitation.project_id
                        AND tenant.tenant_id=invitation.tenant_id
                       WHERE invitation.project_id=$1 AND invitation.token_hash=$2
                         AND invitation.revoked_at IS NULL AND invitation.accepted_at IS NULL
                         AND invitation.expires_at>now()"#,
                )
                .bind(&connection.project_id)
                .bind(sha256_hex(token.as_bytes()))
                .fetch_optional(&mut **transaction.transaction())
                .await?
                .ok_or_else(|| {
                    ControlError::InvalidArguments("invitation is invalid or expired".to_owned())
                })?;
                let tenant_id: String = row.get("tenant_id");
                let email: String = row.get("email");
                let teams = row.get::<Json<Value>, _>("team_ids").0;
                let providers = row.get::<Json<Value>, _>("allowed_auth_providers").0;
                if path == "tenants.getInvitationByToken" {
                    Ok(serde_json::json!({
                        "tenantId":tenant_id,"invitationToken":token,"userEmail":email,
                        "teamIds":teams,"allowedAuthProviders":providers,
                    }))
                } else {
                    Ok(serde_json::json!({
                        "tenantId":tenant_id,"tenantName":row.get::<String,_>("name"),
                        "email":email,"role":row.get::<String,_>("role"),
                        "teamIds":teams,"allowedAuthProviders":providers,
                        "expiresAt":timestamp(row.get::<DateTime<Utc>,_>("expires_at")),
                    }))
                }
            }
            "control.invitations.list" => {
                empty_args(args)?;
                let tenant = tenant(connection)?;
                let rows = sqlx::query(
                    r#"SELECT id,email,role,permissions,team_ids,allowed_auth_providers,
                              expires_at,revoked_at,accepted_at,created_at,updated_at,handoff_state
                       FROM gonvex_auth_membership_invitations
                       WHERE project_id=$1 AND tenant_id=$2 ORDER BY created_at DESC"#,
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                Ok(Value::Array(rows.into_iter().map(|row| serde_json::json!({
                    "id":row.get::<String,_>("id"),"email":row.get::<String,_>("email"),
                    "role":row.get::<String,_>("role"),
                    "permissions":row.get::<Json<Value>,_>("permissions").0,
                    "teamIds":row.get::<Json<Value>,_>("team_ids").0,
                    "allowedAuthProviders":row.get::<Json<Value>,_>("allowed_auth_providers").0,
                    "expiresAt":timestamp(row.get::<DateTime<Utc>,_>("expires_at")),
                    "revoked":row.get::<Option<DateTime<Utc>>,_>("revoked_at").is_some(),
                    "accepted":row.get::<Option<DateTime<Utc>>,_>("accepted_at").is_some(),
                    "state":row.get::<String,_>("handoff_state"),
                    "createdAt":timestamp(row.get::<DateTime<Utc>,_>("created_at")),
                    "updatedAt":timestamp(row.get::<DateTime<Utc>,_>("updated_at")),
                })).collect()))
            }
            "control.project.developers.list" => {
                empty_args(args)?;
                let rows = sqlx::query(
                    "SELECT email,name,role FROM gonvex_project_members WHERE project_id=$1 ORDER BY lower(email)",
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                Ok(Value::Array(
                    rows.into_iter()
                        .map(|row| {
                            serde_json::json!({
                                "email":row.get::<String,_>("email"),
                                "name":row.get::<String,_>("name"),
                                "role":row.get::<String,_>("role"),
                            })
                        })
                        .collect(),
                ))
            }
            "control.developer.status" => {
                empty_args(args)?;
                let identity = account(connection)?;
                let developer: bool = sqlx::query_scalar(
                    r#"SELECT EXISTS(
                           SELECT 1 FROM gonvex_runtime_projects
                           WHERE id=$1 AND lower(owner_email)=lower($2)
                           UNION ALL
                           SELECT 1 FROM gonvex_project_members
                           WHERE project_id=$1 AND lower(email)=lower($2)
                       )"#,
                )
                .bind(&connection.project_id)
                .bind(&identity.account.email)
                .fetch_one(&mut **transaction.transaction())
                .await?;
                Ok(serde_json::json!({
                    "developer":developer,
                    "mode":!connection.impersonation_id.is_empty(),
                    "tenantId":connection.tenant.as_ref().map(|tenant|tenant.route.tenant_id.as_str()).unwrap_or(""),
                    "grantId":connection.impersonation_id,
                }))
            }
            "control.assistant.getDefaults" => {
                empty_args(args)?;
                control_setting(
                    transaction,
                    &connection.project_id,
                    "assistant.defaults",
                    "",
                )
                .await
            }
            "control.voice.getConfiguration" => {
                empty_args(args)?;
                control_settings(transaction, &connection.project_id, "voice.").await
            }
            "control.support.listSessions" => {
                empty_args(args)?;
                let rows = sqlx::query(
                    r#"SELECT id,tenant_id,account_id,release,environment,last_seen_at
                       FROM gonvex_support_sessions WHERE project_id=$1
                       ORDER BY last_seen_at DESC LIMIT 1000"#,
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                Ok(Value::Array(rows.into_iter().map(|row| serde_json::json!({
                    "id":row.get::<String,_>("id"),"tenantId":row.get::<String,_>("tenant_id"),
                    "accountId":row.get::<String,_>("account_id"),
                    "release":row.get::<String,_>("release"),
                    "environment":row.get::<String,_>("environment"),
                    "lastSeenAt":timestamp(row.get::<DateTime<Utc>,_>("last_seen_at")),
                })).collect()))
            }
            "control.support.listTenants" => {
                empty_args(args)?;
                let rows = sqlx::query(
                    r#"SELECT tenant_id,name,domain,status,timezone,seat_limit,created_at
                       FROM gonvex_runtime_tenants WHERE project_id=$1 AND deleted_at IS NULL
                       ORDER BY name,tenant_id"#,
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                Ok(Value::Array(rows.into_iter().map(|row| serde_json::json!({
                    "id":row.get::<String,_>("tenant_id"),"name":row.get::<String,_>("name"),
                    "domain":row.get::<String,_>("domain"),"status":row.get::<String,_>("status"),
                    "timezone":row.get::<String,_>("timezone"),
                    "seatLimit":row.get::<Option<i32>,_>("seat_limit"),
                    "createdAt":timestamp(row.get::<DateTime<Utc>,_>("created_at")),
                })).collect()))
            }
            "control.support.getSession" => {
                let object = exact_object(args, &["id"])?;
                let id = required_string(object, "id")?;
                let row = sqlx::query(
                    r#"SELECT id,tenant_id,account_id,connection_id,release,environment,last_seen_at,created_at
                       FROM gonvex_support_sessions WHERE project_id=$1 AND id=$2"#,
                )
                .bind(&connection.project_id)
                .bind(id)
                .fetch_optional(&mut **transaction.transaction())
                .await?
                .ok_or_else(|| ControlError::InvalidArguments("support session not found".to_owned()))?;
                Ok(serde_json::json!({
                    "id":row.get::<String,_>("id"),"tenantId":row.get::<String,_>("tenant_id"),
                    "accountId":row.get::<String,_>("account_id"),
                    "connectionId":row.get::<String,_>("connection_id"),
                    "release":row.get::<String,_>("release"),"environment":row.get::<String,_>("environment"),
                    "lastSeenAt":timestamp(row.get::<DateTime<Utc>,_>("last_seen_at")),
                    "createdAt":timestamp(row.get::<DateTime<Utc>,_>("created_at")),
                }))
            }
            "control.support.getTenant" => {
                let object = exact_object(args, &["tenantId"])?;
                let tenant_id = required_string(object, "tenantId")?;
                let row = sqlx::query(
                    r#"SELECT tenant_id,name,domain,status,timezone,description,profile,seat_limit,created_at,updated_at
                       FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL"#,
                )
                .bind(&connection.project_id)
                .bind(tenant_id)
                .fetch_optional(&mut **transaction.transaction())
                .await?
                .ok_or_else(|| ControlError::InvalidArguments("tenant not found".to_owned()))?;
                Ok(serde_json::json!({
                    "id":row.get::<String,_>("tenant_id"),"name":row.get::<String,_>("name"),
                    "domain":row.get::<String,_>("domain"),"status":row.get::<String,_>("status"),
                    "timezone":row.get::<String,_>("timezone"),"description":row.get::<String,_>("description"),
                    "profile":row.get::<Json<Value>,_>("profile").0,
                    "seatLimit":row.get::<Option<i32>,_>("seat_limit"),
                    "createdAt":timestamp(row.get::<DateTime<Utc>,_>("created_at")),
                    "updatedAt":timestamp(row.get::<DateTime<Utc>,_>("updated_at")),
                }))
            }
            "control.support.listErrors" => {
                empty_args(args)?;
                let groups = sqlx::query(
                    r#"SELECT fingerprint,project_id,title,culprit,level,status,priority,
                              assignee,first_seen,last_seen,event_count,tenants,releases,
                              environments,accounts,devices,latest_event,regression
                       FROM gonvex_error_groups WHERE project_id=$1
                       ORDER BY last_seen DESC LIMIT 500"#,
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                let releases = sqlx::query_scalar::<_, String>(
                    r#"SELECT release FROM gonvex_error_events
                       WHERE project_id=$1 AND release<>'' GROUP BY release
                       ORDER BY max(occurred_at) DESC,release DESC"#,
                )
                .bind(&connection.project_id)
                .fetch_all(&mut **transaction.transaction())
                .await?;
                Ok(serde_json::json!({
                    "groups":groups.into_iter().map(error_group_json).collect::<Vec<_>>(),
                    "releases":releases,
                }))
            }
            "control.support.getError" => {
                let object = exact_object(args, &["fingerprint"])?;
                let fingerprint = required_string(object, "fingerprint")?;
                let row = sqlx::query(
                    r#"SELECT fingerprint,project_id,title,culprit,level,status,priority,
                              assignee,first_seen,last_seen,event_count,tenants,releases,
                              environments,accounts,devices,latest_event,regression
                       FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2"#,
                )
                .bind(&connection.project_id)
                .bind(fingerprint)
                .fetch_optional(&mut **transaction.transaction())
                .await?
                .ok_or_else(|| {
                    ControlError::InvalidArguments("error group not found".to_owned())
                })?;
                Ok(error_group_json(row))
            }
            _ => Err(ControlError::UnknownFunction(path.to_owned())),
        }
    }

    async fn control_write_body(
        &self,
        transaction: &mut TenantTransaction,
        connection: &ControlConnection,
        path: &str,
        args: &Value,
    ) -> Result<Value, ControlError> {
        let account_id = connection
            .identity
            .as_ref()
            .map(|identity| identity.account.id.as_str())
            .unwrap_or("public");
        match path {
            "control.accounts.updatePassword" => {
                let object = exact_object(args, &["currentPassword", "newPassword"])?;
                let current = required_string(object, "currentPassword")?;
                let new_password = required_string(object, "newPassword")?;
                if new_password.len() < 12 {
                    return Err(ControlError::InvalidArguments(
                        "newPassword must contain at least 12 characters".to_owned(),
                    ));
                }
                let mode: String = sqlx::query_scalar(
                    "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
                )
                .bind(&connection.project_id)
                .fetch_one(&mut **transaction.transaction())
                .await?;
                if !matches!(mode.as_str(), "gonvex-native" | "hybrid") {
                    return Err(ControlError::InvalidArguments(
                        "password management is owned by the configured external identity provider"
                            .to_owned(),
                    ));
                }
                if let Some(stored) = sqlx::query_scalar::<_, String>(
                    "SELECT password_hash FROM gonvex_account_passwords WHERE project_id=$1 AND account_id=$2",
                )
                .bind(&connection.project_id)
                .bind(account_id)
                .fetch_optional(&mut **transaction.transaction())
                .await?
                {
                    if !verify_password(current, &stored) {
                        return Err(ControlError::InvalidArguments(
                            "current password is incorrect".to_owned(),
                        ));
                    }
                }
                let encoded = hash_password(new_password);
                sqlx::query(
                    r#"INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash)
                       VALUES($1,$2,$3) ON CONFLICT(project_id,account_id) DO UPDATE SET
                         password_hash=EXCLUDED.password_hash,updated_at=now()"#,
                )
                .bind(&connection.project_id)
                .bind(account_id)
                .bind(encoded)
                .execute(&mut **transaction.transaction())
                .await?;
                Ok(serde_json::json!({"updated":true}))
            }
            "control.auth.logout" => {
                let object = exact_object(args, &["refreshToken", "all"])?;
                let refresh = required_string(object, "refreshToken")?;
                let all = object.get("all").and_then(Value::as_bool).unwrap_or(false);
                if all {
                    sqlx::query(
                        "UPDATE gonvex_auth_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2",
                    )
                    .bind(&connection.project_id)
                    .bind(account_id)
                    .execute(&mut **transaction.transaction())
                    .await?;
                    sqlx::query(
                        "UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2",
                    )
                    .bind(&connection.project_id)
                    .bind(account_id)
                    .execute(&mut **transaction.transaction())
                    .await?;
                } else {
                    let family: Option<String> = sqlx::query_scalar(
                        "SELECT family_id FROM gonvex_auth_refresh_tokens WHERE project_id=$1 AND account_id=$2 AND token_hash=$3",
                    )
                    .bind(&connection.project_id)
                    .bind(account_id)
                    .bind(sha256_hex(refresh.as_bytes()))
                    .fetch_optional(&mut **transaction.transaction())
                    .await?;
                    if let Some(family) = family {
                        for table in ["gonvex_auth_sessions", "gonvex_auth_refresh_tokens"] {
                            let statement = format!(
                                "UPDATE {table} SET revoked_at=COALESCE(revoked_at,now()) WHERE family_id=$1"
                            );
                            sqlx::query(&statement)
                                .bind(&family)
                                .execute(&mut **transaction.transaction())
                                .await?;
                        }
                    }
                }
                Ok(serde_json::json!({"updated":true}))
            }
            "control.auth.realms.configure" => {
                let object = exact_object(
                    args,
                    &[
                        "provider",
                        "authMode",
                        "enabled",
                        "signupMode",
                        "azureTenantId",
                        "clientId",
                        "clientSecret",
                        "issuer",
                        "audience",
                        "jwksUrl",
                        "firebaseProjectId",
                        "firebaseTenantId",
                        "adminCredentials",
                    ],
                )?;
                let provider = required_string(object, "provider")?;
                if !matches!(
                    provider,
                    "google" | "microsoft" | "apple" | "firebase" | "external-oidc"
                ) {
                    return Err(ControlError::InvalidArguments(
                        "provider is unsupported".to_owned(),
                    ));
                }
                let signup_mode = required_string(object, "signupMode")?;
                if !matches!(signup_mode, "personal" | "inviteOnly") {
                    return Err(ControlError::InvalidArguments(
                        "signupMode must be personal or inviteOnly".to_owned(),
                    ));
                }
                let enabled = object
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| {
                        ControlError::InvalidArguments("enabled must be boolean".to_owned())
                    })?;
                let mut auth_mode = optional_string(object, "authMode")
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                if auth_mode.is_none() {
                    auth_mode = Some(
                        sqlx::query_scalar::<_, String>(
                            "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
                        )
                        .bind(&connection.project_id)
                        .fetch_one(&mut **transaction.transaction())
                        .await?,
                    );
                }
                let auth_mode = auth_mode.expect("set above");
                require_auth_mode(&auth_mode)?;
                let external = matches!(provider, "firebase" | "external-oidc");
                if enabled
                    && ((external && auth_mode != "hybrid" && auth_mode != provider)
                        || (!external && !matches!(auth_mode.as_str(), "gonvex-native" | "hybrid")))
                {
                    return Err(ControlError::InvalidArguments(format!(
                        "authMode {auth_mode:?} does not enable provider {provider:?}"
                    )));
                }
                let azure_tenant = optional_string(object, "azureTenantId").unwrap_or("");
                let client_id = optional_string(object, "clientId").unwrap_or("");
                let client_secret = optional_string(object, "clientSecret").unwrap_or("");
                let mut issuer = optional_string(object, "issuer").unwrap_or("").to_owned();
                let mut audience = optional_string(object, "audience").unwrap_or("").to_owned();
                let mut jwks_url = optional_string(object, "jwksUrl").unwrap_or("").to_owned();
                let firebase_project = optional_string(object, "firebaseProjectId").unwrap_or("");
                let firebase_tenant = optional_string(object, "firebaseTenantId").unwrap_or("");
                let admin_credentials = optional_string(object, "adminCredentials").unwrap_or("");
                if provider == "firebase" && enabled {
                    if firebase_project.is_empty() {
                        return Err(ControlError::InvalidArguments(
                            "firebaseProjectId is required when Firebase auth is enabled"
                                .to_owned(),
                        ));
                    }
                    if issuer.is_empty() {
                        issuer = format!("https://securetoken.google.com/{firebase_project}");
                    }
                    if audience.is_empty() {
                        audience = firebase_project.to_owned();
                    }
                    if jwks_url.is_empty() {
                        jwks_url = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com".to_owned();
                    }
                }
                if provider == "external-oidc"
                    && enabled
                    && (issuer.is_empty() || audience.is_empty() || jwks_url.is_empty())
                {
                    return Err(ControlError::InvalidArguments(
                        "issuer, audience, and jwksUrl are required when external OIDC is enabled"
                            .to_owned(),
                    ));
                }
                if enabled && matches!(provider, "microsoft" | "apple") && client_id.is_empty() {
                    return Err(ControlError::InvalidArguments(
                        "clientId is required when this provider is enabled".to_owned(),
                    ));
                }
                if provider == "microsoft" && enabled && azure_tenant.is_empty() {
                    return Err(ControlError::InvalidArguments(
                        "azureTenantId is required when Microsoft auth is enabled".to_owned(),
                    ));
                }
                if !jwks_url.is_empty() {
                    validate_external_url(&jwks_url)?;
                }
                if !admin_credentials.is_empty() {
                    if provider != "firebase" {
                        return Err(ControlError::InvalidArguments(
                            "adminCredentials is supported only for Firebase".to_owned(),
                        ));
                    }
                    let credential: Value =
                        serde_json::from_str(admin_credentials).map_err(|_| {
                            ControlError::InvalidArguments(
                                "Firebase adminCredentials must be valid JSON".to_owned(),
                            )
                        })?;
                    if credential.get("project_id").and_then(Value::as_str)
                        != Some(firebase_project)
                    {
                        return Err(ControlError::InvalidArguments(
                            "Firebase adminCredentials project_id does not match firebaseProjectId"
                                .to_owned(),
                        ));
                    }
                }
                let existing_secret: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM gonvex_auth_providers WHERE project_id=$1 AND provider=$2 AND client_secret_encrypted IS NOT NULL)",
                )
                .bind(&connection.project_id)
                .bind(provider)
                .fetch_one(&mut **transaction.transaction())
                .await?;
                if enabled
                    && matches!(provider, "microsoft" | "apple")
                    && client_secret.is_empty()
                    && !existing_secret
                {
                    return Err(ControlError::InvalidArguments(
                        "clientSecret is required when this provider is enabled".to_owned(),
                    ));
                }
                let encrypted_secret = (!client_secret.is_empty())
                    .then(|| encrypt_control_secret(self, client_secret))
                    .transpose()?;
                let encrypted_admin = (!admin_credentials.is_empty())
                    .then(|| encrypt_control_secret(self, admin_credentials))
                    .transpose()?;
                sqlx::query(
                    r#"INSERT INTO gonvex_auth_providers
                       (project_id,provider,enabled,signup_mode,azure_tenant_id,client_id,
                        client_secret_encrypted,issuer,audience,jwks_url,firebase_project_id,
                        firebase_tenant_id,admin_credentials_encrypted)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                       ON CONFLICT(project_id,provider) DO UPDATE SET
                         enabled=EXCLUDED.enabled,signup_mode=EXCLUDED.signup_mode,
                         azure_tenant_id=EXCLUDED.azure_tenant_id,client_id=EXCLUDED.client_id,
                         client_secret_encrypted=COALESCE(EXCLUDED.client_secret_encrypted,gonvex_auth_providers.client_secret_encrypted),
                         issuer=EXCLUDED.issuer,audience=EXCLUDED.audience,jwks_url=EXCLUDED.jwks_url,
                         firebase_project_id=EXCLUDED.firebase_project_id,
                         firebase_tenant_id=EXCLUDED.firebase_tenant_id,
                         admin_credentials_encrypted=COALESCE(EXCLUDED.admin_credentials_encrypted,gonvex_auth_providers.admin_credentials_encrypted),
                         updated_at=now()"#,
                )
                .bind(&connection.project_id)
                .bind(provider)
                .bind(enabled)
                .bind(signup_mode)
                .bind(azure_tenant)
                .bind(client_id)
                .bind(encrypted_secret)
                .bind(&issuer)
                .bind(&audience)
                .bind(&jwks_url)
                .bind(firebase_project)
                .bind(firebase_tenant)
                .bind(encrypted_admin)
                .execute(&mut **transaction.transaction())
                .await?;
                sqlx::query(
                    "UPDATE gonvex_runtime_projects SET auth_mode=$2,updated_at=now() WHERE id=$1",
                )
                .bind(&connection.project_id)
                .bind(auth_mode)
                .execute(&mut **transaction.transaction())
                .await?;
                Ok(serde_json::json!({"updated":true}))
            }
            "control.tenants.updateProfile" => {
                let object = exact_object(args, &["name", "domain", "description"])?;
                let tenant = tenant(connection)?;
                let name = required_string(object, "name")?;
                let domain = required_string(object, "domain")?.to_lowercase();
                let description = object
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                let affected = sqlx::query(
                    r#"UPDATE gonvex_runtime_tenants
                       SET name=$3,domain=$4,description=$5,updated_at=now()
                       WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL"#,
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .bind(name)
                .bind(domain)
                .bind(description)
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.tenants.updateTimezone" => {
                let object = exact_object(args, &["timezone"])?;
                let timezone = required_string(object, "timezone")?;
                if timezone.len() > 128 || !timezone.contains('/') && timezone != "UTC" {
                    return Err(ControlError::InvalidArguments(
                        "timezone must be an IANA timezone or UTC".to_owned(),
                    ));
                }
                let tenant = tenant(connection)?;
                let affected = sqlx::query(
                    "UPDATE gonvex_runtime_tenants SET timezone=$3,updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL",
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .bind(timezone)
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.tenants.delete" => {
                empty_args(args)?;
                let tenant = tenant(connection)?;
                let affected = sqlx::query(
                    "UPDATE gonvex_runtime_tenants SET status='deleted',deleted_at=COALESCE(deleted_at,now()),updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL",
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.tenants.setException" => {
                let object = exact_object(args, &["tenantId", "value"])?;
                let tenant_id = required_string(object, "tenantId")?;
                let value = object.get("value").cloned().unwrap_or(Value::Null);
                upsert_setting(
                    transaction,
                    &connection.project_id,
                    "tenant.exception",
                    tenant_id,
                    &value,
                    account_id,
                )
                .await
            }
            "control.tenants.setSeatLimit" => {
                let object = exact_object(args, &["tenantId", "seatLimit"])?;
                let tenant_id = required_string(object, "tenantId")?;
                let seat = match object.get("seatLimit") {
                    None | Some(Value::Null) => None,
                    Some(Value::Number(value)) => value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .filter(|value| *value >= 0),
                    _ => None,
                };
                if object
                    .get("seatLimit")
                    .is_some_and(|value| !value.is_null())
                    && seat.is_none()
                {
                    return Err(ControlError::InvalidArguments(
                        "seatLimit must be a nonnegative integer or null".to_owned(),
                    ));
                }
                let affected = sqlx::query(
                    "UPDATE gonvex_runtime_tenants SET seat_limit=$3,updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL",
                )
                .bind(&connection.project_id)
                .bind(tenant_id)
                .bind(seat)
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.invitations.create" => {
                let object = exact_object(
                    args,
                    &[
                        "email",
                        "role",
                        "permissions",
                        "teamIds",
                        "allowedAuthProviders",
                        "payload",
                    ],
                )?;
                let tenant = tenant(connection)?;
                let email = normalize_email(required_string(object, "email")?)?;
                let role = membership_role(required_string(object, "role")?)?;
                if tenant.member.role == "admin" && matches!(role, "owner" | "admin") {
                    return Err(ControlError::TenantAdminRequired);
                }
                let permissions = object
                    .get("permissions")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                if !permissions.is_object() {
                    return Err(ControlError::InvalidArguments(
                        "permissions must be an object".to_owned(),
                    ));
                }
                let teams = unique_string_array(object.get("teamIds"), "teamIds")?;
                let providers = invitation_providers(object.get("allowedAuthProviders"))?;
                let payload = object
                    .get("payload")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let id = random_id("invite");
                let token = secure_token("invite");
                sqlx::query(
                    r#"INSERT INTO gonvex_auth_membership_invitations
                       (project_id,tenant_id,email,role,permissions,team_ids,allowed_auth_providers,
                        application_payload,invited_by,expires_at,id,token_hash,revoked_at,accepted_at,
                        accepted_account_id,accepted_idempotency_key,handoff_state,handoff_command_id,
                        completed_at,updated_at)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,now()+interval '7 days',$10,$11,
                              NULL,NULL,NULL,NULL,'pending','',NULL,now())
                       ON CONFLICT(project_id,tenant_id,email) DO UPDATE SET
                         role=EXCLUDED.role,permissions=EXCLUDED.permissions,team_ids=EXCLUDED.team_ids,
                         allowed_auth_providers=EXCLUDED.allowed_auth_providers,
                         application_payload=EXCLUDED.application_payload,invited_by=EXCLUDED.invited_by,
                         expires_at=EXCLUDED.expires_at,id=EXCLUDED.id,token_hash=EXCLUDED.token_hash,
                         revoked_at=NULL,accepted_at=NULL,accepted_account_id=NULL,
                         accepted_idempotency_key=NULL,handoff_state='pending',handoff_command_id='',
                         completed_at=NULL,updated_at=now()"#,
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .bind(email)
                .bind(role)
                .bind(Json(permissions))
                .bind(Json(Value::Array(teams.into_iter().map(Value::String).collect())))
                .bind(Json(Value::Array(providers.into_iter().map(Value::String).collect())))
                .bind(Json(payload))
                .bind(account_id)
                .bind(&id)
                .bind(sha256_hex(token.as_bytes()))
                .execute(&mut **transaction.transaction())
                .await?;
                Ok(serde_json::json!({"id":id,"token":token}))
            }
            "control.invitations.update" => {
                let object = exact_object(
                    args,
                    &[
                        "id",
                        "role",
                        "permissions",
                        "teamIds",
                        "allowedAuthProviders",
                        "payload",
                    ],
                )?;
                let tenant = tenant(connection)?;
                let id = required_string(object, "id")?;
                let role = membership_role(required_string(object, "role")?)?;
                if tenant.member.role == "admin" && matches!(role, "owner" | "admin") {
                    return Err(ControlError::TenantAdminRequired);
                }
                let permissions = object
                    .get("permissions")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                if !permissions.is_object() {
                    return Err(ControlError::InvalidArguments(
                        "permissions must be an object".to_owned(),
                    ));
                }
                let teams = unique_string_array(object.get("teamIds"), "teamIds")?;
                let providers = invitation_providers(object.get("allowedAuthProviders"))?;
                let payload = object
                    .get("payload")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let affected = sqlx::query(
                    r#"UPDATE gonvex_auth_membership_invitations
                       SET role=$4,permissions=$5,team_ids=$6,allowed_auth_providers=$7,
                           application_payload=$8,updated_at=now()
                       WHERE project_id=$1 AND tenant_id=$2 AND id=$3
                         AND accepted_at IS NULL AND revoked_at IS NULL"#,
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .bind(id)
                .bind(role)
                .bind(Json(permissions))
                .bind(Json(Value::Array(
                    teams.into_iter().map(Value::String).collect(),
                )))
                .bind(Json(Value::Array(
                    providers.into_iter().map(Value::String).collect(),
                )))
                .bind(Json(payload))
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.invitations.revoke" => {
                let object = exact_object(args, &["id", "email"])?;
                let tenant = tenant(connection)?;
                let id = optional_string(object, "id").unwrap_or("");
                let email = optional_string(object, "email").unwrap_or("");
                if id.is_empty() && email.is_empty() {
                    return Err(ControlError::InvalidArguments(
                        "id or email is required".to_owned(),
                    ));
                }
                let affected = sqlx::query(
                    "UPDATE gonvex_auth_membership_invitations SET revoked_at=now(),updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND (id=$3 OR lower(email)=lower($4)) AND accepted_at IS NULL",
                )
                .bind(&connection.project_id)
                .bind(&tenant.route.tenant_id)
                .bind(id)
                .bind(email)
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.agentAuth.issue" => {
                let object = exact_object(args, &["permissions", "expiresInSeconds"])?;
                let permissions = unique_string_array(object.get("permissions"), "permissions")?;
                let expires = object
                    .get("expiresInSeconds")
                    .and_then(Value::as_i64)
                    .filter(|value| (1..=86400).contains(value))
                    .ok_or_else(|| {
                        ControlError::InvalidArguments(
                            "expiresInSeconds must be between 1 and 86400".to_owned(),
                        )
                    })?;
                let id = random_id("agent");
                let token = secure_token("agent_claim");
                sqlx::query(
                    "INSERT INTO gonvex_agent_claim_tokens(id,project_id,token_hash,permissions,expires_at,created_by) VALUES($1,$2,$3,$4,now()+($5 * interval '1 second'),$6)",
                )
                .bind(&id).bind(&connection.project_id).bind(sha256_hex(token.as_bytes()))
                .bind(Json(Value::Array(permissions.into_iter().map(Value::String).collect())))
                .bind(expires).bind(account_id)
                .execute(&mut **transaction.transaction()).await?;
                Ok(serde_json::json!({"id":id,"token":token}))
            }
            "control.agentAuth.claim" => {
                let object = exact_object(args, &["token"])?;
                let token = required_string(object, "token")?;
                let row = sqlx::query(
                    r#"UPDATE gonvex_agent_claim_tokens SET claimed_at=now(),claimed_account_id=$3
                       WHERE project_id=$1 AND token_hash=$2 AND claimed_at IS NULL
                         AND revoked_at IS NULL AND expires_at>now() RETURNING id,permissions"#,
                )
                .bind(&connection.project_id)
                .bind(sha256_hex(token.as_bytes()))
                .bind(account_id)
                .fetch_optional(&mut **transaction.transaction())
                .await?
                .ok_or_else(|| {
                    ControlError::InvalidArguments(
                        "agent token is invalid, expired, revoked, or already claimed".to_owned(),
                    )
                })?;
                Ok(
                    serde_json::json!({"id":row.get::<String,_>("id"),"permissions":row.get::<Json<Value>,_>("permissions").0}),
                )
            }
            "control.agentAuth.revoke" => {
                let object = exact_object(args, &["id"])?;
                let affected = sqlx::query("UPDATE gonvex_agent_claim_tokens SET revoked_at=now() WHERE project_id=$1 AND id=$2 AND revoked_at IS NULL")
                    .bind(&connection.project_id).bind(required_string(object,"id")?)
                    .execute(&mut **transaction.transaction()).await?.rows_affected();
                affected_result(affected)
            }
            "control.project.developers.invite" => {
                let object = exact_object(args, &["email", "name", "role"])?;
                let email = normalize_email(required_string(object, "email")?)?;
                let name = required_string(object, "name")?;
                let role = required_string(object, "role")?;
                if !matches!(role, "owner" | "admin" | "dev") {
                    return Err(ControlError::InvalidArguments("role is invalid".to_owned()));
                }
                sqlx::query("INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,email) DO UPDATE SET name=EXCLUDED.name,role=EXCLUDED.role")
                    .bind(&connection.project_id).bind(email).bind(name).bind(role)
                    .execute(&mut **transaction.transaction()).await?;
                Ok(serde_json::json!({"updated":true}))
            }
            "control.project.developers.remove" => {
                let object = exact_object(args, &["email"])?;
                let affected = sqlx::query(
                    "DELETE FROM gonvex_project_members WHERE project_id=$1 AND email=$2",
                )
                .bind(&connection.project_id)
                .bind(normalize_email(required_string(object, "email")?)?)
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            "control.developer.enter" => {
                let object = exact_object(args, &["tenantId"])?;
                let tenant_id = required_string(object, "tenantId")?;
                let id = random_id("devmode");
                let token = secure_token("imp");
                let expires = Utc::now() + chrono::Duration::minutes(30);
                sqlx::query("INSERT INTO gonvex_impersonation_grants(id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at) VALUES($1,$2,$3,$4,$4,$5,'developer mode',$6)")
                    .bind(&id).bind(&connection.project_id).bind(sha256_hex(token.as_bytes())).bind(account_id).bind(tenant_id).bind(expires)
                    .execute(&mut **transaction.transaction()).await?;
                Ok(serde_json::json!({"id":id,"token":token,"expiresAt":timestamp(expires)}))
            }
            "control.developer.exit" => {
                let object = exact_object(args, &["grantId"])?;
                let affected = sqlx::query("UPDATE gonvex_impersonation_grants SET revoked_at=now() WHERE project_id=$1 AND id=$2 AND actor_account_id=$3 AND revoked_at IS NULL")
                    .bind(&connection.project_id).bind(required_string(object,"grantId")?).bind(account_id)
                    .execute(&mut **transaction.transaction()).await?.rows_affected();
                affected_result(affected)
            }
            "control.assistant.setDefaults"
            | "control.voice.setRateCard"
            | "control.voice.setTenantEntitlement"
            | "control.voice.setUserOverride" => {
                let object = exact_object(args, &["scopeId", "value"])?;
                let scope = object
                    .get("scopeId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                let value = object.get("value").cloned().unwrap_or(Value::Null);
                let kind = match path {
                    "control.assistant.setDefaults" => "assistant.defaults",
                    "control.voice.setRateCard" => "voice.rateCard",
                    "control.voice.setTenantEntitlement" => "voice.tenantEntitlement",
                    _ => "voice.userOverride",
                };
                upsert_setting(
                    transaction,
                    &connection.project_id,
                    kind,
                    scope,
                    &value,
                    account_id,
                )
                .await
            }
            "control.support.heartbeat" => {
                let object = exact_object(args, &["release", "environment"])?;
                let release = object
                    .get("release")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                let environment = object
                    .get("environment")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                let session_id = format!("support_{}", connection.connection_id);
                let tenant_id = connection
                    .tenant
                    .as_ref()
                    .map(|tenant| tenant.route.tenant_id.as_str())
                    .unwrap_or("");
                sqlx::query(r#"INSERT INTO gonvex_support_sessions
                    (id,project_id,tenant_id,account_id,connection_id,release,environment,last_seen_at)
                    VALUES($1,$2,$3,$4,$5,$6,$7,now())
                    ON CONFLICT(id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,
                      account_id=EXCLUDED.account_id,connection_id=EXCLUDED.connection_id,
                      release=EXCLUDED.release,environment=EXCLUDED.environment,last_seen_at=now()"#)
                    .bind(&session_id).bind(&connection.project_id).bind(tenant_id).bind(account_id)
                    .bind(&connection.connection_id).bind(release).bind(environment)
                    .execute(&mut **transaction.transaction()).await?;
                Ok(serde_json::json!({"sessionId":session_id}))
            }
            "control.support.sendCommand" => {
                let object = exact_object(args, &["sessionId", "kind", "payload"])?;
                let session_id = required_string(object, "sessionId")?;
                let kind = required_string(object, "kind")?;
                let payload = object
                    .get("payload")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM gonvex_support_sessions WHERE project_id=$1 AND id=$2)")
                    .bind(&connection.project_id).bind(session_id).fetch_one(&mut **transaction.transaction()).await?;
                if !exists {
                    return Err(ControlError::InvalidArguments(
                        "support session not found".to_owned(),
                    ));
                }
                let id = random_id("command");
                sqlx::query("INSERT INTO gonvex_support_commands(id,project_id,session_id,kind,payload,created_by) VALUES($1,$2,$3,$4,$5,$6)")
                    .bind(&id).bind(&connection.project_id).bind(session_id).bind(kind).bind(Json(payload)).bind(account_id)
                    .execute(&mut **transaction.transaction()).await?;
                Ok(serde_json::json!({"id":id}))
            }
            "control.support.ackCommand" => {
                let object = exact_object(args, &["id"])?;
                let affected = sqlx::query(r#"UPDATE gonvex_support_commands command SET acknowledged_at=now()
                    FROM gonvex_support_sessions session WHERE command.id=$1 AND command.project_id=$2
                    AND command.session_id=session.id AND session.account_id=$3 AND command.acknowledged_at IS NULL"#)
                    .bind(required_string(object,"id")?).bind(&connection.project_id).bind(account_id)
                    .execute(&mut **transaction.transaction()).await?.rows_affected();
                affected_result(affected)
            }
            "control.support.pruneSessions" => {
                let object = exact_object(args, &["olderThanSeconds"])?;
                let seconds = object
                    .get("olderThanSeconds")
                    .and_then(Value::as_i64)
                    .filter(|value| (60..=31_536_000).contains(value))
                    .ok_or_else(|| {
                        ControlError::InvalidArguments(
                            "olderThanSeconds must be between 60 and 31536000".to_owned(),
                        )
                    })?;
                let deleted = sqlx::query("DELETE FROM gonvex_support_sessions WHERE project_id=$1 AND last_seen_at < now()-($2 * interval '1 second')")
                    .bind(&connection.project_id).bind(seconds).execute(&mut **transaction.transaction()).await?.rows_affected();
                Ok(serde_json::json!({"deleted":deleted}))
            }
            "control.demos.resetPassword" => {
                let object = exact_object(args, &["accountId", "password"])?;
                let account_id = required_string(object, "accountId")?;
                let password = required_string(object, "password")?;
                if password.len() < 12 {
                    return Err(ControlError::InvalidArguments(
                        "password must contain at least 12 characters".to_owned(),
                    ));
                }
                let affected = sqlx::query(
                    r#"UPDATE gonvex_account_passwords SET password_hash=$3,updated_at=now()
                       WHERE project_id=$1 AND account_id=$2 AND EXISTS(
                         SELECT 1 FROM gonvex_demo_accounts
                         WHERE project_id=$1 AND account_id=$2)"#,
                )
                .bind(&connection.project_id)
                .bind(account_id)
                .bind(hash_password(password))
                .execute(&mut **transaction.transaction())
                .await?
                .rows_affected();
                affected_result(affected)
            }
            _ => Err(ControlError::NotImplemented(path.to_owned())),
        }
    }

    async fn control_plane(&self) -> Result<gonvex_postgres::ControlPlane, ControlError> {
        self.inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| {
                ControlError::InvalidArguments("Control Plane is unavailable".to_owned())
            })
    }
}

async fn list_account_tenants(
    control: &gonvex_postgres::ControlPlane,
    connection: &ControlConnection,
    ids_only: bool,
) -> Result<Value, ControlError> {
    let identity = account(connection)?;
    let mut transaction = control.begin_control_transaction(true).await?;
    let rows = sqlx::query(
        r#"SELECT tenant_id,name,domain,timezone,description,profile
           FROM gonvex_runtime_tenants
           WHERE project_id=$1 AND deleted_at IS NULL
             AND status NOT IN ('deleted','disabled')
           ORDER BY lower(name),tenant_id"#,
    )
    .bind(&connection.project_id)
    .fetch_all(&mut **transaction.transaction())
    .await?;
    let candidates = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("tenant_id"),
                row.get::<String, _>("name"),
                row.get::<String, _>("domain"),
                row.get::<String, _>("timezone"),
                row.get::<String, _>("description"),
                row.get::<Json<Value>, _>("profile").0,
            )
        })
        .collect::<Vec<_>>();
    transaction.commit().await?;
    let mut items = Vec::new();
    for (id, name, domain, timezone, description, profile) in candidates {
        let Ok((_, member, _)) = control
            .admit_member(&connection.project_id, &id, &identity.account.id)
            .await
        else {
            continue;
        };
        if ids_only {
            items.push(Value::String(id));
        } else {
            items.push(serde_json::json!({
                "id":id,"name":name,"role":member.role,
                "permissions":member.permissions,"domain":domain,
                "timezone":timezone,"description":description,"profile":profile,
            }));
        }
    }
    Ok(Value::Array(items))
}

async fn member_auth_providers(
    control: &gonvex_postgres::ControlPlane,
    connection: &ControlConnection,
    args: &Value,
) -> Result<Value, ControlError> {
    let object = exact_object(args, &["memberIds"])?;
    let member_ids = unique_string_array(object.get("memberIds"), "memberIds")?;
    if member_ids.len() > 1_000 {
        return Err(ControlError::InvalidArguments(
            "memberIds cannot contain more than 1000 entries".to_owned(),
        ));
    }
    let caller = tenant(connection)?;
    let route = control
        .resolve_tenant(&connection.project_id, &caller.route.tenant_id)
        .await?;
    let mut tenant_tx = control.begin_tenant_transaction(&route, true).await?;
    let rows = sqlx::query("SELECT id,account_id FROM members WHERE id = ANY($1)")
        .bind(&member_ids)
        .fetch_all(&mut **tenant_tx.transaction())
        .await?;
    tenant_tx.commit().await?;
    let account_by_member = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("id"),
                row.get::<String, _>("account_id"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let account_ids = account_by_member.values().cloned().collect::<Vec<_>>();
    let mut control_tx = control.begin_control_transaction(true).await?;
    let identities = sqlx::query(
        r#"SELECT account_id,provider FROM account_identities
           WHERE project_id=$1 AND account_id = ANY($2)
           ORDER BY account_id,provider"#,
    )
    .bind(&connection.project_id)
    .bind(&account_ids)
    .fetch_all(&mut **control_tx.transaction())
    .await?;
    control_tx.commit().await?;
    let mut providers = BTreeMap::<String, Vec<String>>::new();
    for row in identities {
        providers
            .entry(row.get("account_id"))
            .or_default()
            .push(row.get("provider"));
    }
    Ok(Value::Array(
        member_ids
            .into_iter()
            .filter_map(|member_id| {
                let account_id = account_by_member.get(&member_id)?;
                Some(serde_json::json!({
                    "memberId":member_id,
                    "providers":providers.get(account_id).cloned().unwrap_or_default(),
                }))
            })
            .collect(),
    ))
}

async fn claim_control_idempotency(
    transaction: &mut TenantTransaction,
    project: &str,
    subject: &str,
    key: &str,
    kind: ControlKind,
    path: &str,
) -> Result<Option<Value>, ControlError> {
    let kind = match kind {
        ControlKind::Reducer => "reducer",
        ControlKind::Action => "action",
        ControlKind::Query => "query",
    };
    lock_control_idempotency(transaction, project, subject, key).await?;
    let inserted = sqlx::query(
        r#"INSERT INTO gonvex_control_idempotency
           (project_id,subject_id,idempotency_key,kind,path)
           VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING"#,
    )
    .bind(project)
    .bind(subject)
    .bind(key)
    .bind(kind)
    .bind(path)
    .execute(&mut **transaction.transaction())
    .await?
    .rows_affected();
    if inserted == 1 {
        return Ok(None);
    }
    let row = sqlx::query(
        r#"SELECT state,kind,path,result,error FROM gonvex_control_idempotency
           WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3 FOR UPDATE"#,
    )
    .bind(project)
    .bind(subject)
    .bind(key)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or_else(|| ControlError::DatabaseInvariant {
        operation: "Control Plane idempotency claim",
        detail: format!("record for {path:?} disappeared after a conflicting insert"),
    })?;
    if row.get::<String, _>("kind") != kind || row.get::<String, _>("path") != path {
        return Err(ControlError::InvalidArguments(
            "idempotency key was already used for another operation".to_owned(),
        ));
    }
    if row.get::<String, _>("state") == "pending" {
        return Err(ControlError::InvalidArguments(
            "Control Plane operation is still in progress".to_owned(),
        ));
    }
    let error: String = row.get("error");
    if !error.is_empty() {
        return Err(ControlError::InvalidArguments(error));
    }
    Ok(row
        .get::<Option<Json<Value>>, _>("result")
        .map(|result| result.0)
        .or(Some(Value::Null)))
}

/// Claims a resumable cross-database operation. A pending row represents a
/// checkpoint, not a permanent failure: each tenant-side step is idempotent
/// and a retry is allowed to continue until the Control Plane result commits.
async fn claim_control_saga(
    transaction: &mut TenantTransaction,
    project: &str,
    subject: &str,
    key: &str,
    kind: ControlKind,
    path: &str,
) -> Result<Option<Value>, ControlError> {
    let kind_name = match kind {
        ControlKind::Reducer => "reducer",
        ControlKind::Action => "action",
        ControlKind::Query => "query",
    };
    lock_control_idempotency(transaction, project, subject, key).await?;
    sqlx::query(
        r#"INSERT INTO gonvex_control_idempotency
           (project_id,subject_id,idempotency_key,kind,path)
           VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING"#,
    )
    .bind(project)
    .bind(subject)
    .bind(key)
    .bind(kind_name)
    .bind(path)
    .execute(&mut **transaction.transaction())
    .await?;
    let row = sqlx::query(
        r#"SELECT state,kind,path,result,error FROM gonvex_control_idempotency
           WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3 FOR UPDATE"#,
    )
    .bind(project)
    .bind(subject)
    .bind(key)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or_else(|| ControlError::DatabaseInvariant {
        operation: "Control Plane saga idempotency claim",
        detail: format!("record for {path:?} disappeared after insert"),
    })?;
    if row.get::<String, _>("kind") != kind_name || row.get::<String, _>("path") != path {
        return Err(ControlError::InvalidArguments(
            "idempotency key was already used for another operation".to_owned(),
        ));
    }
    if row.get::<String, _>("state") == "pending" {
        return Ok(None);
    }
    let error: String = row.get("error");
    if !error.is_empty() {
        return Err(ControlError::InvalidArguments(error));
    }
    Ok(row
        .get::<Option<Json<Value>>, _>("result")
        .map(|result| result.0)
        .or(Some(Value::Null)))
}

async fn lock_control_idempotency(
    transaction: &mut TenantTransaction,
    project: &str,
    subject: &str,
    key: &str,
) -> Result<(), ControlError> {
    let lock_key = advisory_lock_key("control-idempotency", &[project, subject, key]);
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(lock_key)
        .execute(&mut **transaction.transaction())
        .await?;
    Ok(())
}

async fn complete_control_idempotency(
    transaction: &mut TenantTransaction,
    project: &str,
    subject: &str,
    key: &str,
    result: &Value,
) -> Result<(), ControlError> {
    sqlx::query(
        r#"UPDATE gonvex_control_idempotency
           SET state='completed',result=$4,error='',updated_at=now()
           WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3"#,
    )
    .bind(project)
    .bind(subject)
    .bind(key)
    .bind(Json(result.clone()))
    .execute(&mut **transaction.transaction())
    .await?;
    Ok(())
}

async fn load_external_configuration(
    runtime: &Runtime,
    transaction: &mut TenantTransaction,
    project: &str,
    provider: &str,
) -> Result<ExternalAuthConfiguration, ControlError> {
    let mode: String = sqlx::query_scalar(
        "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1",
    )
    .bind(project)
    .fetch_one(&mut **transaction.transaction())
    .await?;
    if mode != "hybrid" && mode != provider {
        return Err(ControlError::InvalidArguments(format!(
            "project auth mode {mode:?} does not allow provider {provider:?}"
        )));
    }
    let row = sqlx::query(
        r#"SELECT provider,issuer,audience,jwks_url,firebase_project_id,
                  firebase_tenant_id,signup_mode,admin_credentials_encrypted
           FROM gonvex_auth_providers
           WHERE project_id=$1 AND provider=$2 AND enabled=TRUE"#,
    )
    .bind(project)
    .bind(provider)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or_else(|| {
        ControlError::InvalidArguments("external identity provider is disabled".to_owned())
    })?;
    let mut issuer: String = row.get("issuer");
    let mut audience: String = row.get("audience");
    let mut jwks_url: String = row.get("jwks_url");
    let firebase_project: String = row.get("firebase_project_id");
    if provider == "firebase" {
        if issuer.is_empty() {
            issuer = format!("https://securetoken.google.com/{firebase_project}");
        }
        if audience.is_empty() {
            audience = firebase_project.clone();
        }
        if jwks_url.is_empty() {
            jwks_url = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com".to_owned();
        }
    }
    if issuer.is_empty() || audience.is_empty() || jwks_url.is_empty() {
        return Err(ControlError::InvalidArguments(
            "external identity provider configuration is incomplete".to_owned(),
        ));
    }
    let encrypted_admin = row.get::<Option<Vec<u8>>, _>("admin_credentials_encrypted");
    let firebase_admin_credentials = encrypted_admin
        .as_deref()
        .map(|encrypted| decrypt_control_secret(runtime, encrypted))
        .transpose()?;
    Ok(ExternalAuthConfiguration {
        provider: row.get("provider"),
        issuer,
        audience,
        jwks_url,
        firebase_tenant_id: row.get("firebase_tenant_id"),
        signup_mode: row.get("signup_mode"),
        firebase_project_id: firebase_project,
        firebase_admin_credentials,
    })
}

pub(crate) async fn resolve_external_account(
    transaction: &mut TenantTransaction,
    project: &str,
    identity: &VerifiedExternalIdentity,
) -> Result<AuthAccount, ControlError> {
    let lock_key = advisory_lock_key(
        "external-identity",
        &[
            project,
            &identity.provider,
            &identity.issuer,
            &identity.subject,
        ],
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(lock_key)
        .execute(&mut **transaction.transaction())
        .await?;
    if identity.email_verified && !identity.email.is_empty() {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(advisory_lock_key(
                "verified-email",
                &[project, &identity.email.to_lowercase()],
            ))
            .execute(&mut **transaction.transaction())
            .await?;
    }
    if let Some(account_id) = sqlx::query_scalar::<_, String>(
        r#"SELECT account_id FROM account_identities
           WHERE project_id=$1 AND provider=$2 AND issuer=$3 AND subject=$4"#,
    )
    .bind(project)
    .bind(&identity.provider)
    .bind(&identity.issuer)
    .bind(&identity.subject)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    {
        sqlx::query(
            r#"UPDATE account_identities SET email=$5,verified_email=$6,updated_at=now()
               WHERE project_id=$1 AND provider=$2 AND issuer=$3 AND subject=$4"#,
        )
        .bind(project)
        .bind(&identity.provider)
        .bind(&identity.issuer)
        .bind(&identity.subject)
        .bind(&identity.email)
        .bind(identity.email_verified)
        .execute(&mut **transaction.transaction())
        .await?;
        sqlx::query(
            "UPDATE accounts SET name=COALESCE(NULLIF($2,''),name),avatar_url=COALESCE(NULLIF($3,''),avatar_url),updated_at=now() WHERE id=$1",
        )
        .bind(&account_id)
        .bind(&identity.name)
        .bind(&identity.picture)
        .execute(&mut **transaction.transaction())
        .await?;
        return load_auth_account(transaction, project, &account_id).await;
    }
    let mut resolution = "new";
    let account_id = if identity.email_verified && !identity.email.is_empty() {
        let matches = sqlx::query_scalar::<_, String>(
            r#"SELECT DISTINCT account_id FROM account_identities
               WHERE project_id=$1 AND verified_email=TRUE AND lower(email)=lower($2)
               ORDER BY account_id LIMIT 2"#,
        )
        .bind(project)
        .bind(&identity.email)
        .fetch_all(&mut **transaction.transaction())
        .await?;
        if matches.len() > 1 {
            return Err(ControlError::InvalidArguments(
                "verified email matches more than one account".to_owned(),
            ));
        }
        if let Some(account_id) = matches.into_iter().next() {
            resolution = "verified_email";
            account_id
        } else {
            require_external_signup(transaction, project, identity).await?;
            if identity.email.is_empty() {
                return Err(ControlError::InvalidArguments(
                    "external identity email is required for first login".to_owned(),
                ));
            }
            let account_id = random_id("acct");
            sqlx::query(
                "INSERT INTO accounts(id,auth_realm_id,email,name,avatar_url,updated_at) VALUES($1,$2,$3,$4,$5,now())",
            )
            .bind(&account_id)
            .bind(project)
            .bind(&identity.email)
            .bind(&identity.name)
            .bind(&identity.picture)
            .execute(&mut **transaction.transaction())
            .await?;
            account_id
        }
    } else {
        require_external_signup(transaction, project, identity).await?;
        let account_id = random_id("acct");
        sqlx::query(
            "INSERT INTO accounts(id,auth_realm_id,email,name,avatar_url,updated_at) VALUES($1,$2,$3,$4,$5,now())",
        )
        .bind(&account_id)
        .bind(project)
        .bind(&identity.email)
        .bind(&identity.name)
        .bind(&identity.picture)
        .execute(&mut **transaction.transaction())
        .await?;
        account_id
    };
    if identity.email.is_empty() && resolution == "new" {
        return Err(ControlError::InvalidArguments(
            "external identity email is required for first login".to_owned(),
        ));
    }
    let inserted = sqlx::query(
        r#"INSERT INTO account_identities
           (project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,now())
           ON CONFLICT(project_id,provider,issuer,subject) DO NOTHING"#,
    )
    .bind(project)
    .bind(&account_id)
    .bind(&identity.provider)
    .bind(&identity.issuer)
    .bind(&identity.subject)
    .bind(&identity.email)
    .bind(identity.email_verified)
    .execute(&mut **transaction.transaction())
    .await?
    .rows_affected();
    let resolved_id = if inserted == 1 {
        account_id
    } else {
        sqlx::query_scalar::<_, String>(
            r#"SELECT account_id FROM account_identities
               WHERE project_id=$1 AND provider=$2 AND issuer=$3 AND subject=$4"#,
        )
        .bind(project)
        .bind(&identity.provider)
        .bind(&identity.issuer)
        .bind(&identity.subject)
        .fetch_one(&mut **transaction.transaction())
        .await?
    };
    sqlx::query(
        r#"INSERT INTO gonvex_auth_identity_events
           (project_id,account_id,provider,issuer,subject,resolution)
           VALUES($1,$2,$3,$4,$5,$6)"#,
    )
    .bind(project)
    .bind(&resolved_id)
    .bind(&identity.provider)
    .bind(&identity.issuer)
    .bind(&identity.subject)
    .bind(resolution)
    .execute(&mut **transaction.transaction())
    .await?;
    load_auth_account(transaction, project, &resolved_id).await
}

pub(crate) async fn load_auth_account(
    transaction: &mut TenantTransaction,
    project: &str,
    account_id: &str,
) -> Result<AuthAccount, ControlError> {
    let row = sqlx::query(
        r#"SELECT account.id,account.email,account.name,account.avatar_url,
                  COALESCE(identity.verified_email,FALSE) AS email_verified,
                  COALESCE(identity.provider,'') AS provider
           FROM accounts account
           LEFT JOIN LATERAL (
             SELECT verified_email,provider FROM account_identities
             WHERE account_id=account.id ORDER BY updated_at DESC LIMIT 1
           ) identity ON TRUE
           WHERE account.id=$1 AND account.auth_realm_id=$2 AND account.disabled_at IS NULL"#,
    )
    .bind(account_id)
    .bind(project)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or_else(|| ControlError::InvalidArguments("account is unavailable".to_owned()))?;
    Ok(AuthAccount {
        id: row.get("id"),
        email: row.get("email"),
        email_verified: row.get("email_verified"),
        name: row.get("name"),
        picture: row.get("avatar_url"),
        provider: row.get("provider"),
    })
}

pub(crate) async fn issue_session(
    transaction: &mut TenantTransaction,
    project: &str,
    account_id: &str,
    family_id: &str,
    refresh_expires_at: DateTime<Utc>,
) -> Result<SessionGrant, ControlError> {
    let access_token = secure_token("session");
    let refresh_token = secure_token("refresh");
    let access_expires_at = Utc::now() + chrono::Duration::minutes(15);
    sqlx::query(
        "INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,family_id,expires_at) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(sha256_hex(access_token.as_bytes()))
    .bind(project)
    .bind(account_id)
    .bind(family_id)
    .bind(access_expires_at)
    .execute(&mut **transaction.transaction())
    .await?;
    sqlx::query(
        "INSERT INTO gonvex_auth_refresh_tokens(token_hash,family_id,project_id,account_id,expires_at) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(sha256_hex(refresh_token.as_bytes()))
    .bind(family_id)
    .bind(project)
    .bind(account_id)
    .bind(refresh_expires_at)
    .execute(&mut **transaction.transaction())
    .await?;
    Ok(SessionGrant {
        access_token,
        access_expires_at,
        refresh_token,
        refresh_expires_at,
    })
}

async fn require_external_signup(
    transaction: &mut TenantTransaction,
    project: &str,
    identity: &VerifiedExternalIdentity,
) -> Result<(), ControlError> {
    let signup_mode: String = sqlx::query_scalar(
        "SELECT signup_mode FROM gonvex_auth_providers WHERE project_id=$1 AND provider=$2 AND enabled=TRUE",
    )
    .bind(project)
    .bind(&identity.provider)
    .fetch_one(&mut **transaction.transaction())
    .await?;
    if signup_mode != "inviteOnly" {
        return Ok(());
    }
    if !identity.email_verified || identity.email.is_empty() {
        return Err(ControlError::InvalidArguments(
            "a verified invited email is required".to_owned(),
        ));
    }
    let invited: bool = sqlx::query_scalar(
        r#"SELECT EXISTS(
             SELECT 1 FROM gonvex_auth_membership_invitations
             WHERE project_id=$1 AND lower(email)=lower($2)
               AND revoked_at IS NULL AND accepted_at IS NULL AND expires_at>now()
           )"#,
    )
    .bind(project)
    .bind(&identity.email)
    .fetch_one(&mut **transaction.transaction())
    .await?;
    if invited {
        Ok(())
    } else {
        Err(ControlError::InvalidArguments(
            "account signup requires an active invitation".to_owned(),
        ))
    }
}

pub(crate) async fn refresh_session(
    transaction: &mut TenantTransaction,
    project: &str,
    refresh_token: &str,
) -> Result<RefreshSessionResult, ControlError> {
    let row = sqlx::query(
        r#"SELECT project_id,account_id,family_id,expires_at,used_at,revoked_at
           FROM gonvex_auth_refresh_tokens WHERE token_hash=$1 FOR UPDATE"#,
    )
    .bind(sha256_hex(refresh_token.as_bytes()))
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or_else(|| ControlError::InvalidArguments("invalid or expired refresh token".to_owned()))?;
    let stored_project: String = row.get("project_id");
    let account_id: String = row.get("account_id");
    let family_id: String = row.get("family_id");
    let expires_at: DateTime<Utc> = row.get("expires_at");
    let used_at: Option<DateTime<Utc>> = row.get("used_at");
    let revoked_at: Option<DateTime<Utc>> = row.get("revoked_at");
    if stored_project != project || revoked_at.is_some() || expires_at <= Utc::now() {
        return Err(ControlError::InvalidArguments(
            "invalid or expired refresh token".to_owned(),
        ));
    }
    if let Some(used_at) = used_at {
        if Utc::now() - used_at > chrono::Duration::seconds(5) {
            revoke_family(transaction, &family_id).await?;
            return Ok(RefreshSessionResult::ReuseRevoked);
        }
        return Err(ControlError::InvalidArguments(
            "refresh token was already rotated; use the latest session".to_owned(),
        ));
    }
    sqlx::query(
        "UPDATE gonvex_auth_refresh_tokens SET used_at=now() WHERE token_hash=$1 AND used_at IS NULL",
    )
    .bind(sha256_hex(refresh_token.as_bytes()))
    .execute(&mut **transaction.transaction())
    .await?;
    let account = load_auth_account(transaction, project, &account_id).await?;
    let grant = issue_session(transaction, project, &account_id, &family_id, expires_at).await?;
    Ok(RefreshSessionResult::Refreshed(Box::new((
        grant,
        account,
        String::new(),
    ))))
}

pub(crate) async fn revoke_family(
    transaction: &mut TenantTransaction,
    family_id: &str,
) -> Result<(), ControlError> {
    for table in ["gonvex_auth_sessions", "gonvex_auth_refresh_tokens"] {
        let statement =
            format!("UPDATE {table} SET revoked_at=COALESCE(revoked_at,now()) WHERE family_id=$1");
        sqlx::query(&statement)
            .bind(family_id)
            .execute(&mut **transaction.transaction())
            .await?;
    }
    Ok(())
}

pub(crate) async fn session_result_from_directory(
    control: &gonvex_postgres::ControlPlane,
    transaction: &mut TenantTransaction,
    project: &str,
    grant: &SessionGrant,
    account: &AuthAccount,
    requested_tenant: &str,
) -> Result<Value, ControlError> {
    let rows = sqlx::query(
        r#"SELECT tenant.tenant_id,tenant.name,tenant.domain,tenant.timezone,
                  tenant.description,tenant.profile,directory.member_id
           FROM account_tenant_index directory
           JOIN gonvex_runtime_tenants tenant ON tenant.tenant_id=directory.tenant_id
           WHERE tenant.project_id=$1 AND directory.account_id=$2
             AND directory.status='active' AND tenant.deleted_at IS NULL
             AND tenant.status NOT IN ('deleted','disabled')
           ORDER BY lower(tenant.name),tenant.tenant_id"#,
    )
    .bind(project)
    .bind(&account.id)
    .fetch_all(&mut **transaction.transaction())
    .await?;
    // The directory is only a routing projection. Every candidate is admitted
    // against the tenant-local members row before it is returned or selected.
    let candidates = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("tenant_id"),
                row.get::<String, _>("name"),
                row.get::<String, _>("domain"),
                row.get::<String, _>("timezone"),
                row.get::<String, _>("description"),
                row.get::<Json<Value>, _>("profile").0,
            )
        })
        .collect::<Vec<_>>();
    let mut tenants = Vec::new();
    for (tenant_id, name, domain, timezone, description, profile) in candidates {
        let Ok((_, member, _)) = control.admit_member(project, &tenant_id, &account.id).await
        else {
            continue;
        };
        tenants.push(serde_json::json!({
            "id":tenant_id,"name":name,"role":member.role,
            "permissions":member.permissions,"memberId":member.id,
            "domain":domain,"timezone":timezone,"description":description,"profile":profile,
        }));
    }
    let active = if let Some(tenant) = tenants.iter().find(|tenant| {
        tenant.get("id").and_then(Value::as_str) == Some(requested_tenant)
            || tenant
                .get("domain")
                .and_then(Value::as_str)
                .is_some_and(|domain| domain.eq_ignore_ascii_case(requested_tenant))
    }) {
        tenant
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned()
    } else {
        tenants
            .first()
            .and_then(|tenant| tenant.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    Ok(serde_json::json!({
        "accessToken":grant.access_token,
        "tokenType":"Bearer",
        "expiresIn":900,
        "expiresAt":grant.access_expires_at.timestamp_millis(),
        "refreshToken":grant.refresh_token,
        "refreshExpiresAt":grant.refresh_expires_at.timestamp_millis(),
        "account":{
            "id":account.id,"email":account.email,"emailVerified":account.email_verified,
            "name":account.name,"picture":account.picture,"provider":account.provider,
        },
        "tenants":tenants,
        "activeTenantId":active,
    }))
}

fn auth_subject(path: &str, args: &Value) -> Result<String, ControlError> {
    let object = args
        .as_object()
        .ok_or_else(|| ControlError::InvalidArguments("expected an object".to_owned()))?;
    let material = match path {
        "control.auth.passwordLogin" => normalize_email(required_string(object, "email")?)?,
        "control.auth.exchangeExternalToken" => format!(
            "{}\0{}",
            required_string(object, "provider")?,
            required_string(object, "token")?
        ),
        "control.auth.refreshSession" => required_string(object, "refreshToken")?.to_owned(),
        _ => "public".to_owned(),
    };
    Ok(format!("auth:{}", sha256_hex(material.as_bytes())))
}

fn verify_password(password: &str, encoded: &str) -> bool {
    let parts = encoded.split('$').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "pbkdf2_sha256" {
        return false;
    }
    let Ok(rounds) = parts[1].parse::<u32>() else {
        return false;
    };
    let Ok(salt) = URL_SAFE_NO_PAD.decode(parts[2]) else {
        return false;
    };
    let Ok(expected) = URL_SAFE_NO_PAD.decode(parts[3]) else {
        return false;
    };
    if rounds == 0 || expected.is_empty() {
        return false;
    }
    let mut actual = vec![0u8; expected.len()];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, rounds, &mut actual);
    actual.ct_eq(&expected).into()
}

pub(crate) fn hash_password(password: &str) -> String {
    const ROUNDS: u32 = 210_000;
    let mut salt = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut salt);
    let mut hash = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ROUNDS, &mut hash);
    let salt = URL_SAFE_NO_PAD.encode(salt);
    let hash = URL_SAFE_NO_PAD.encode(hash);
    format!("pbkdf2_sha256${ROUNDS}${salt}${hash}")
}

async fn upsert_setting(
    transaction: &mut TenantTransaction,
    project: &str,
    kind: &str,
    scope: &str,
    value: &Value,
    actor: &str,
) -> Result<Value, ControlError> {
    sqlx::query(
        r#"INSERT INTO gonvex_control_settings(project_id,kind,scope_id,value,updated_by)
           VALUES($1,$2,$3,$4,$5)
           ON CONFLICT(project_id,kind,scope_id) DO UPDATE SET
             value=EXCLUDED.value,updated_by=EXCLUDED.updated_by,updated_at=now()"#,
    )
    .bind(project)
    .bind(kind)
    .bind(scope)
    .bind(Json(value.clone()))
    .bind(actor)
    .execute(&mut **transaction.transaction())
    .await?;
    Ok(serde_json::json!({"updated":true}))
}

fn affected_result(affected: u64) -> Result<Value, ControlError> {
    if affected == 0 {
        Err(ControlError::InvalidArguments(
            "record not found".to_owned(),
        ))
    } else {
        Ok(serde_json::json!({"updated":true}))
    }
}

fn control_subject(connection: &ControlConnection, authorization: Authorization) -> String {
    let mut subject = connection
        .identity
        .as_ref()
        .map(|identity| identity.account.id.clone())
        .unwrap_or_else(|| "public".to_owned());
    if authorization == Authorization::TenantAdmin {
        subject.push_str("|tenant:");
        if let Some(tenant) = &connection.tenant {
            subject.push_str(&tenant.route.tenant_id);
        }
    }
    subject
}

fn normalize_email(value: &str) -> Result<String, ControlError> {
    let value = value.trim().to_lowercase();
    if value.len() > 320 || !value.contains('@') || value.starts_with('@') || value.ends_with('@') {
        Err(ControlError::InvalidArguments(
            "email is invalid".to_owned(),
        ))
    } else {
        Ok(value)
    }
}

fn membership_role(value: &str) -> Result<&str, ControlError> {
    if matches!(value, "owner" | "admin" | "member" | "viewer") {
        Ok(value)
    } else {
        Err(ControlError::InvalidArguments("role is invalid".to_owned()))
    }
}

fn unique_string_array(value: Option<&Value>, name: &str) -> Result<Vec<String>, ControlError> {
    let values = value
        .and_then(Value::as_array)
        .ok_or_else(|| ControlError::InvalidArguments(format!("{name} must be an array")))?;
    if values.len() > 500 {
        return Err(ControlError::InvalidArguments(format!(
            "{name} cannot contain more than 500 values"
        )));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        let value = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ControlError::InvalidArguments(format!("{name} must contain strings"))
            })?;
        seen.insert(value.to_owned());
    }
    Ok(seen.into_iter().collect())
}

fn invitation_providers(value: Option<&Value>) -> Result<Vec<String>, ControlError> {
    let providers = unique_string_array(value, "allowedAuthProviders")?;
    for provider in &providers {
        if !matches!(
            provider.as_str(),
            "password" | "google" | "microsoft" | "apple" | "firebase" | "external-oidc"
        ) {
            return Err(ControlError::InvalidArguments(format!(
                "authentication provider {provider:?} is unsupported"
            )));
        }
    }
    Ok(providers)
}

fn random_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn secure_token(kind: &str) -> String {
    let mut secret = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    format!("gvx_{}.{}", random_id(kind), URL_SAFE_NO_PAD.encode(secret))
}

fn control_query_message(id: &str, path: &str, result: Value, reason: &str) -> ServerMessage {
    ServerMessage::QueryResult {
        id: id.to_owned(),
        payload: BTreeMap::from([
            ("path".to_owned(), Value::String(path.to_owned())),
            ("result".to_owned(), result),
            ("reason".to_owned(), Value::String(reason.to_owned())),
        ]),
    }
}

async fn control_setting(
    transaction: &mut TenantTransaction,
    project: &str,
    kind: &str,
    scope: &str,
) -> Result<Value, ControlError> {
    let value = sqlx::query_scalar::<_, Json<Value>>(
        "SELECT value FROM gonvex_control_settings WHERE project_id=$1 AND kind=$2 AND scope_id=$3",
    )
    .bind(project)
    .bind(kind)
    .bind(scope)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .map(|value| value.0)
    .unwrap_or_else(|| Value::Object(Map::new()));
    Ok(value)
}

async fn control_settings(
    transaction: &mut TenantTransaction,
    project: &str,
    prefix: &str,
) -> Result<Value, ControlError> {
    let rows = sqlx::query(
        "SELECT kind,scope_id,value FROM gonvex_control_settings WHERE project_id=$1 AND kind LIKE $2 ORDER BY kind,scope_id",
    )
    .bind(project)
    .bind(format!("{prefix}%"))
    .fetch_all(&mut **transaction.transaction())
    .await?;
    Ok(Value::Array(
        rows.into_iter()
            .map(|row| {
                serde_json::json!({
                    "kind":row.get::<String,_>("kind"),
                    "scopeId":row.get::<String,_>("scope_id"),
                    "value":row.get::<Json<Value>,_>("value").0,
                })
            })
            .collect(),
    ))
}

fn definition(path: &str) -> Option<Definition> {
    use Authorization::*;
    use ControlKind::*;
    Some(match path {
        "control.accounts.me" => Definition {
            kind: Query,
            authorization: Account,
            live: false,
        },
        "control.accounts.updatePassword" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.accounts.resetMemberPassword" | "control.accounts.provisionMemberLogin" => {
            Definition {
                kind: Reducer,
                authorization: TenantAdmin,
                live: false,
            }
        }
        "control.auth.passwordLogin"
        | "control.auth.exchangeExternalToken"
        | "control.auth.refreshSession" => Definition {
            kind: Action,
            authorization: Public,
            live: false,
        },
        "control.auth.logout" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.auth.publicSettings" => Definition {
            kind: Query,
            authorization: Public,
            live: false,
        },
        "control.auth.realms.list" => Definition {
            kind: Query,
            authorization: ProjectAdmin,
            live: true,
        },
        "control.auth.realms.configure" => Definition {
            kind: Reducer,
            authorization: ProjectAdmin,
            live: false,
        },
        "control.auth.memberProviders" => Definition {
            kind: Query,
            authorization: TenantAdmin,
            live: false,
        },
        "control.tenants.mine" => Definition {
            kind: Query,
            authorization: Account,
            live: true,
        },
        "control.tenants.create" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.tenants.getByDomain" => Definition {
            kind: Query,
            authorization: Public,
            live: false,
        },
        "control.tenants.updateProfile"
        | "control.tenants.updateTimezone"
        | "control.tenants.delete" => Definition {
            kind: Reducer,
            authorization: TenantAdmin,
            live: false,
        },
        "control.tenants.setException" | "control.tenants.setSeatLimit" => Definition {
            kind: Reducer,
            authorization: ProjectAdmin,
            live: false,
        },
        "control.invitations.lookup" => Definition {
            kind: Query,
            authorization: Public,
            live: false,
        },
        "control.invitations.list" => Definition {
            kind: Query,
            authorization: TenantAdmin,
            live: true,
        },
        "control.invitations.create"
        | "control.invitations.update"
        | "control.invitations.revoke" => Definition {
            kind: Reducer,
            authorization: TenantAdmin,
            live: false,
        },
        "control.invitations.accept" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.agentAuth.issue" | "control.agentAuth.revoke" => Definition {
            kind: Reducer,
            authorization: ProjectAdmin,
            live: false,
        },
        "control.agentAuth.claim" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.project.developers.list" => Definition {
            kind: Query,
            authorization: ProjectAdmin,
            live: true,
        },
        "control.project.developers.invite" | "control.project.developers.remove" => Definition {
            kind: Reducer,
            authorization: ProjectAdmin,
            live: false,
        },
        "control.developer.status" => Definition {
            kind: Query,
            authorization: Account,
            live: true,
        },
        "control.developer.provisionSelf"
        | "control.developer.removeSelf"
        | "control.developer.enter" => Definition {
            kind: Reducer,
            authorization: Developer,
            live: false,
        },
        "control.developer.exit" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.assistant.getDefaults" => Definition {
            kind: Query,
            authorization: ProjectAdmin,
            live: true,
        },
        "control.assistant.setDefaults"
        | "control.voice.setRateCard"
        | "control.voice.setTenantEntitlement"
        | "control.voice.setUserOverride" => Definition {
            kind: Reducer,
            authorization: ProjectAdmin,
            live: false,
        },
        "control.voice.getConfiguration" => Definition {
            kind: Query,
            authorization: ProjectAdmin,
            live: true,
        },
        "control.support.listSessions"
        | "control.support.listTenants"
        | "control.support.listErrors" => Definition {
            kind: Query,
            authorization: ProjectAdmin,
            live: true,
        },
        "control.support.getSession" | "control.support.getError" | "control.support.getTenant" => {
            Definition {
                kind: Query,
                authorization: ProjectAdmin,
                live: false,
            }
        }
        "control.support.pruneSessions"
        | "control.support.sendCommand"
        | "control.support.createImpersonation" => Definition {
            kind: Reducer,
            authorization: ProjectAdmin,
            live: false,
        },
        "control.support.heartbeat" | "control.support.ackCommand" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        "control.demos.create" | "control.demos.resetPassword" | "control.demos.delete" => {
            Definition {
                kind: Reducer,
                authorization: ProjectAdmin,
                live: false,
            }
        }
        "users.myTenants" => Definition {
            kind: Query,
            authorization: Account,
            live: true,
        },
        "tenants.getInvitationByToken" => Definition {
            kind: Query,
            authorization: Public,
            live: false,
        },
        "tenants.acceptInvitation" => Definition {
            kind: Reducer,
            authorization: Account,
            live: false,
        },
        _ => return None,
    })
}

fn require_definition(path: &str, kind: ControlKind) -> Result<Definition, ControlError> {
    let definition =
        definition(path).ok_or_else(|| ControlError::UnknownFunction(path.to_owned()))?;
    if definition.kind != kind {
        return Err(ControlError::WrongKind {
            path: path.to_owned(),
            expected: match kind {
                ControlKind::Query => "Query",
                ControlKind::Reducer => "Reducer",
                ControlKind::Action => "Action",
            },
        });
    }
    Ok(definition)
}

fn account(connection: &ControlConnection) -> Result<&SessionIdentity, ControlError> {
    connection
        .identity
        .as_ref()
        .ok_or(ControlError::AuthenticationRequired)
}

fn tenant(connection: &ControlConnection) -> Result<&TenantSession, ControlError> {
    connection
        .tenant
        .as_ref()
        .ok_or(ControlError::TenantAdminRequired)
}

fn empty_args(args: &Value) -> Result<(), ControlError> {
    let object = args
        .as_object()
        .ok_or_else(|| ControlError::InvalidArguments("expected an object".to_owned()))?;
    if object.is_empty() {
        Ok(())
    } else {
        Err(ControlError::InvalidArguments(format!(
            "unknown field {:?}",
            object.keys().next().unwrap()
        )))
    }
}

fn exact_object<'a>(
    args: &'a Value,
    allowed: &[&str],
) -> Result<&'a Map<String, Value>, ControlError> {
    let object = args
        .as_object()
        .ok_or_else(|| ControlError::InvalidArguments("expected an object".to_owned()))?;
    let allowed = allowed.iter().copied().collect::<BTreeSet<_>>();
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(field.as_str()))
    {
        return Err(ControlError::InvalidArguments(format!(
            "unknown field {field:?}"
        )));
    }
    Ok(object)
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, ControlError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ControlError::InvalidArguments(format!("{key} is required")))
}

fn optional_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str).map(str::trim)
}

fn slug(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .fold((String::new(), false), |(mut output, separator), char| {
            if char.is_ascii_alphanumeric() {
                output.push(char);
                (output, false)
            } else if !output.is_empty() && !separator {
                output.push('-');
                (output, true)
            } else {
                (output, separator)
            }
        })
        .0
        .trim_matches('-')
        .to_owned()
}

fn require_auth_mode(mode: &str) -> Result<(), ControlError> {
    if matches!(
        mode,
        "gonvex-native" | "firebase" | "external-oidc" | "hybrid"
    ) {
        Ok(())
    } else {
        Err(ControlError::InvalidArguments(format!(
            "unsupported project auth mode {mode:?}"
        )))
    }
}

fn validate_external_url(value: &str) -> Result<(), ControlError> {
    let url = url::Url::parse(value)
        .map_err(|_| ControlError::InvalidArguments("jwksUrl is invalid".to_owned()))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
    {
        return Err(ControlError::InvalidArguments(
            "jwksUrl must be an HTTPS URL without credentials".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn encrypt_control_secret(
    runtime: &Runtime,
    plaintext: &str,
) -> Result<Vec<u8>, ControlError> {
    let secret = runtime
        .inner
        .config
        .control_secret
        .as_deref()
        .ok_or_else(|| {
            ControlError::InvalidArguments(
                "GONVEX_DASHBOARD_SESSION_SECRET is required to store provider credentials"
                    .to_owned(),
            )
        })?;
    let key =
        Sha256::digest([b"gonvex-control-secret-v1\0".as_slice(), secret.as_bytes()].concat());
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| {
        ControlError::InvalidArguments("provider credential encryption failed".to_owned())
    })?;
    let mut nonce_bytes = [0_u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let encrypted = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            aes_gcm::aead::Payload {
                msg: plaintext.as_bytes(),
                aad: CONTROL_SECRET_AAD,
            },
        )
        .map_err(|_| {
            ControlError::InvalidArguments("provider credential encryption failed".to_owned())
        })?;
    let mut result = nonce_bytes.to_vec();
    result.extend(encrypted);
    Ok(result)
}

pub(crate) fn decrypt_control_secret(
    runtime: &Runtime,
    ciphertext: &[u8],
) -> Result<String, ControlError> {
    let secret = runtime
        .inner
        .config
        .control_secret
        .as_deref()
        .ok_or_else(|| {
            ControlError::InvalidArguments(
                "GONVEX_DASHBOARD_SESSION_SECRET is required to read provider credentials"
                    .to_owned(),
            )
        })?;
    if ciphertext.len() < 13 {
        return Err(ControlError::InvalidArguments(
            "stored provider credential is invalid".to_owned(),
        ));
    }
    let key =
        Sha256::digest([b"gonvex-control-secret-v1\0".as_slice(), secret.as_bytes()].concat());
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| {
        ControlError::InvalidArguments("provider credential decryption failed".to_owned())
    })?;
    let decrypted = cipher
        .decrypt(
            Nonce::from_slice(&ciphertext[..12]),
            aes_gcm::aead::Payload {
                msg: &ciphertext[12..],
                aad: CONTROL_SECRET_AAD,
            },
        )
        .map_err(|_| {
            ControlError::InvalidArguments("provider credential decryption failed".to_owned())
        })?;
    String::from_utf8(decrypted).map_err(|_| {
        ControlError::InvalidArguments("stored provider credential is invalid".to_owned())
    })
}

fn invalid_invitation() -> ControlError {
    ControlError::InvalidArguments(
        "invitation is invalid, expired, revoked, or already accepted".to_owned(),
    )
}

fn error_group_json(row: PgRow) -> Value {
    serde_json::json!({
        "fingerprint":row.get::<String,_>("fingerprint"),
        "project":row.get::<String,_>("project_id"),
        "title":row.get::<String,_>("title"),
        "culprit":row.get::<String,_>("culprit"),
        "level":row.get::<String,_>("level"),
        "status":row.get::<String,_>("status"),
        "priority":row.get::<String,_>("priority"),
        "assignee":row.get::<String,_>("assignee"),
        "firstSeen":timestamp(row.get::<DateTime<Utc>,_>("first_seen")),
        "lastSeen":timestamp(row.get::<DateTime<Utc>,_>("last_seen")),
        "count":row.get::<i64,_>("event_count"),
        "tenants":row.get::<Json<Value>,_>("tenants").0,
        "releases":row.get::<Json<Value>,_>("releases").0,
        "environments":row.get::<Json<Value>,_>("environments").0,
        "accounts":row.get::<Json<Value>,_>("accounts").0,
        "devices":row.get::<Json<Value>,_>("devices").0,
        "latest":row.get::<Json<Value>,_>("latest_event").0,
        "regression":row.get::<bool,_>("regression"),
    })
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn advisory_lock_key(namespace: &str, components: &[&str]) -> String {
    let mut material = Vec::new();
    append_lock_component(&mut material, namespace);
    for component in components {
        append_lock_component(&mut material, component);
    }
    format!("gonvex-lock-v1:{}", sha256_hex(&material))
}

fn append_lock_component(material: &mut Vec<u8>, component: &str) {
    material.extend_from_slice(&(component.len() as u64).to_be_bytes());
    material.extend_from_slice(component.as_bytes());
}

fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_only_three_function_kinds_and_preserves_live_contracts() {
        assert_eq!(
            definition("control.accounts.me").unwrap().kind,
            ControlKind::Query
        );
        assert_eq!(
            definition("control.auth.passwordLogin").unwrap().kind,
            ControlKind::Action
        );
        assert_eq!(
            definition("control.invitations.accept").unwrap().kind,
            ControlKind::Reducer
        );
        assert!(Runtime::control_query_is_live("control.tenants.mine"));
        assert!(!Runtime::control_query_is_live("control.accounts.me"));
    }

    #[test]
    fn browser_cannot_supply_database_selectors() {
        let error = exact_object(
            &serde_json::json!({"domain":"safe.test","databaseUrl":"postgres://attacker"}),
            &["domain"],
        )
        .unwrap_err();
        assert!(error.to_string().contains("databaseUrl"));
    }

    #[test]
    fn external_identity_lock_keys_are_safe_deterministic_and_field_delimited() {
        let first = advisory_lock_key(
            "external-identity",
            &["project", "firebase", "issuer", "subject"],
        );
        let same = advisory_lock_key(
            "external-identity",
            &["project", "firebase", "issuer", "subject"],
        );
        let different_project = advisory_lock_key(
            "external-identity",
            &["other-project", "firebase", "issuer", "subject"],
        );
        let different_provider = advisory_lock_key(
            "external-identity",
            &["project", "external-oidc", "issuer", "subject"],
        );
        let different_subject = advisory_lock_key(
            "external-identity",
            &["project", "firebase", "issuer", "other-subject"],
        );
        let ambiguous_without_lengths = advisory_lock_key("external-identity", &["ab", "c"])
            != advisory_lock_key("external-identity", &["a", "bc"]);

        assert_eq!(first, same);
        assert!(different_project != first);
        assert!(different_provider != first);
        assert!(different_subject != first);
        assert!(ambiguous_without_lengths);
        assert!(first.is_ascii());
        assert!(!first.contains('\0'));
        assert!(first
            .chars()
            .all(|character| character.is_ascii_alphanumeric()
                || character == '-'
                || character == ':'));
    }
}
