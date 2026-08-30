//! Trusted operator HTTP surface used by the Gonvex dashboard and CLI.
//!
//! Application frontends use the persistent Query/Reducer/Action protocol.
//! These routes are deliberately operator-only and never expose raw database
//! routing or credentials.

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlJson;
use sqlx::Row;
use subtle::ConstantTimeEq;
use url::Url;

use crate::control::encrypt_control_secret;
use crate::Runtime;

const DASHBOARD_PASSWORD_ROUNDS: u32 = 210_000;
const DASHBOARD_SESSION_DAYS: i64 = 7;

pub fn router() -> Router<Runtime> {
    Router::new()
        .route("/dev/auth/login", post(login))
        .route("/dev/auth/me", get(me))
        .route("/dev/auth/tokens", get(list_tokens).post(create_token))
        .route("/dev/auth/tokens/{token}", delete(revoke_token))
        .route(
            "/dev/auth/accounts",
            get(list_accounts).post(create_account),
        )
        .route("/dev/auth/notifications", get(list_notifications))
        .route("/dev/auth/notifications/read", post(read_notifications))
        .route("/dev/manifest", get(manifest))
        .route("/dev/metrics", get(metrics))
        .route("/dev/metrics/stream", get(metrics_stream))
        .route("/dev/logs/stream", get(log_stream))
        .route("/dev/logs", delete(clear_logs))
        .route("/dev/cache", delete(clear_cache))
        .route("/dev/storage/files", get(storage_files))
        .route(
            "/dev/projects/{project}",
            patch(update_project).delete(delete_project),
        )
        .route(
            "/dev/projects/{project}/key/rotate",
            post(rotate_project_key),
        )
        .route(
            "/dev/projects/{project}/members",
            get(project_members).post(upsert_project_member),
        )
        .route(
            "/dev/projects/{project}/invitations",
            post(create_project_invitation),
        )
        .route(
            "/dev/projects/{project}/env",
            get(get_project_env)
                .post(set_project_env)
                .put(replace_project_env)
                .delete(delete_project_env),
        )
        .route(
            "/dev/projects/{project}/auth/providers/{provider}",
            get(get_auth_provider).put(put_auth_provider),
        )
        .route(
            "/dev/projects/{project}/auth/google",
            get(get_google_auth)
                .put(put_google_auth)
                .delete(delete_google_auth),
        )
        .route(
            "/dev/projects/{project}/auth/accounts",
            get(project_auth_accounts),
        )
        .route(
            "/dev/projects/{project}/auth/accounts/{account}",
            patch(update_project_auth_account).delete(delete_project_auth_account),
        )
        .route(
            "/dev/projects/{project}/auth/memberships",
            get(project_auth_memberships)
                .put(put_project_auth_membership)
                .delete(delete_project_auth_membership),
        )
        .route(
            "/dev/projects/{project}/auth/tenants",
            get(project_auth_tenants).post(create_project_auth_tenant),
        )
        .route("/dev/errors/status", get(error_status))
        .route("/dev/errors/groups", get(error_groups))
        .route(
            "/dev/errors/groups/{fingerprint}",
            get(error_group).patch(update_error_group),
        )
        .route(
            "/dev/errors/groups/{fingerprint}/bug-report",
            get(error_bug_report),
        )
}

#[derive(Clone, Debug)]
pub(crate) struct OperatorActor {
    pub email: String,
    pub name: String,
    pub role: String,
    credential: Credential,
    permissions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Credential {
    Session,
    NativeSession,
    PersonalAccessToken,
    AdminKey,
    Local,
}

impl OperatorActor {
    fn has(&self, permission: &str) -> bool {
        self.credential != Credential::PersonalAccessToken
            || self.permissions.iter().any(|granted| {
                granted == "*"
                    || granted == permission
                    || granted
                        .strip_suffix('*')
                        .is_some_and(|prefix| permission.starts_with(prefix))
            })
    }

    pub(crate) fn global_admin(&self) -> bool {
        self.role == "admin"
            && (matches!(self.credential, Credential::AdminKey | Credential::Local)
                || (self.credential == Credential::PersonalAccessToken
                    && self
                        .permissions
                        .iter()
                        .any(|value| value == "*" || value == "admin:projects")))
    }
}

#[derive(Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SignedSession {
    email: String,
    name: String,
    role: String,
    provider: String,
    expires_at: i64,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    access_token: String,
}

async fn login(State(runtime): State<Runtime>, Json(request): Json<LoginRequest>) -> Response {
    let email = normalize_email(&request.email);
    if email.is_empty() || request.password.is_empty() {
        return error(StatusCode::BAD_REQUEST, "email and password are required");
    }
    let actor = if runtime
        .inner
        .config
        .dashboard_account
        .as_deref()
        .is_some_and(|configured| configured == email)
        && runtime
            .inner
            .config
            .dashboard_password
            .as_deref()
            .is_some_and(|password| constant_time_eq(password, &request.password))
    {
        OperatorActor {
            email: email.clone(),
            name: display_name(&email),
            role: "admin".to_owned(),
            credential: Credential::Session,
            permissions: vec!["*".to_owned()],
        }
    } else {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "dashboard account store is unavailable",
            );
        };
        let Ok(mut transaction) = control.begin_control_transaction(true).await else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "dashboard account store is unavailable",
            );
        };
        let row = sqlx::query(
            "SELECT email,name,role,password_hash FROM gonvex_dashboard_accounts WHERE email=$1",
        )
        .bind(&email)
        .fetch_optional(&mut **transaction.transaction())
        .await;
        let Ok(Some(row)) = row else {
            return error(StatusCode::UNAUTHORIZED, "invalid email or password");
        };
        if !verify_password(&request.password, &row.get::<String, _>("password_hash")) {
            return error(StatusCode::UNAUTHORIZED, "invalid email or password");
        }
        OperatorActor {
            email: row.get("email"),
            name: row.get("name"),
            role: row.get("role"),
            credential: Credential::Session,
            permissions: vec!["*".to_owned()],
        }
    };
    let mut session = SignedSession {
        email: actor.email,
        name: actor.name,
        role: actor.role,
        provider: "gonvex".to_owned(),
        expires_at: (Utc::now() + chrono::Duration::days(DASHBOARD_SESSION_DAYS))
            .timestamp_millis(),
        access_token: String::new(),
    };
    session.access_token = match sign_session(&runtime, &session) {
        Ok(token) => token,
        Err(message) => return error(StatusCode::SERVICE_UNAVAILABLE, message),
    };
    Json(json!({"session":session})).into_response()
}

async fn me(State(runtime): State<Runtime>, headers: HeaderMap) -> Response {
    let actor = match authenticate(&runtime, &headers).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let permissions = if actor.credential == Credential::PersonalAccessToken {
        actor.permissions.clone()
    } else {
        vec!["*".to_owned()]
    };
    Json(json!({
        "account":{"email":actor.email,"name":actor.name,"role":actor.role},
        "authentication":credential_name(&actor.credential),
        "permissions":permissions,
    }))
    .into_response()
}

async fn list_accounts(State(runtime): State<Runtime>, headers: HeaderMap) -> Response {
    let actor = match authorize(&runtime, &headers, "admin:projects").await {
        Ok(actor) if actor.role == "admin" => actor,
        Ok(_) => return error(StatusCode::FORBIDDEN, "admin access is required"),
        Err(response) => return response,
    };
    let _ = actor;
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard account store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard account store is unavailable",
        );
    };
    match sqlx::query("SELECT email,name,role FROM gonvex_dashboard_accounts ORDER BY email")
        .fetch_all(&mut **transaction.transaction())
        .await
    {
        Ok(rows) => Json(json!({"accounts":rows.into_iter().map(|row| json!({
            "email":row.get::<String,_>("email"),"name":row.get::<String,_>("name"),
            "role":row.get::<String,_>("role"),
        })).collect::<Vec<_>>() }))
        .into_response(),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard account store is unavailable",
        ),
    }
}

#[derive(Deserialize)]
struct CreateAccountRequest {
    email: String,
    name: String,
    password: String,
    role: String,
}

async fn create_account(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(request): Json<CreateAccountRequest>,
) -> Response {
    let actor = match authorize(&runtime, &headers, "admin:projects").await {
        Ok(actor) if actor.role == "admin" => actor,
        Ok(_) => return error(StatusCode::FORBIDDEN, "admin access is required"),
        Err(response) => return response,
    };
    let _ = actor;
    let email = normalize_email(&request.email);
    let role = match request.role.trim() {
        "admin" => "admin",
        "" | "standard" => "standard",
        _ => return error(StatusCode::BAD_REQUEST, "role must be admin or standard"),
    };
    if email.is_empty() || request.password.len() < 12 {
        return error(
            StatusCode::BAD_REQUEST,
            "email and a password of at least 12 characters are required",
        );
    }
    let name = if request.name.trim().is_empty() {
        display_name(&email)
    } else {
        request.name.trim().to_owned()
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard account store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard account store is unavailable",
        );
    };
    let result = sqlx::query(
        r#"INSERT INTO gonvex_dashboard_accounts(email,name,role,password_hash,updated_at)
           VALUES($1,$2,$3,$4,now()) ON CONFLICT(email) DO UPDATE SET
           name=EXCLUDED.name,role=EXCLUDED.role,password_hash=EXCLUDED.password_hash,updated_at=now()"#,
    )
    .bind(&email)
    .bind(&name)
    .bind(role)
    .bind(hash_password(&request.password))
    .execute(&mut **transaction.transaction())
    .await;
    if result.is_err() || transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard account could not be stored",
        );
    }
    (
        StatusCode::CREATED,
        Json(json!({"account":{"email":email,"name":name,"role":role}})),
    )
        .into_response()
}

async fn project_members(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:members:read", false)
            .await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project member store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project member store is unavailable",
        );
    };
    let members = sqlx::query(
        "SELECT email,name,role FROM gonvex_project_members WHERE project_id=$1 ORDER BY role,email",
    )
    .bind(&project)
    .fetch_all(&mut **transaction.transaction())
    .await;
    let invitations = sqlx::query(
        r#"SELECT id,project_id,email,role,expires_at,accepted_at IS NOT NULL AS accepted
           FROM gonvex_project_invitations WHERE project_id=$1 ORDER BY created_at DESC"#,
    )
    .bind(&project)
    .fetch_all(&mut **transaction.transaction())
    .await;
    match (members, invitations) {
        (Ok(members), Ok(invitations)) => Json(json!({
            "members":members.into_iter().map(|row|json!({
                "email":row.get::<String,_>("email"),
                "name":row.get::<String,_>("name"),
                "role":row.get::<String,_>("role"),
            })).collect::<Vec<_>>(),
            "invitations":invitations.into_iter().map(|row|json!({
                "id":row.get::<String,_>("id"),
                "projectId":row.get::<String,_>("project_id"),
                "email":row.get::<String,_>("email"),
                "role":row.get::<String,_>("role"),
                "expiresAt":row.get::<DateTime<Utc>,_>("expires_at"),
                "accepted":row.get::<bool,_>("accepted"),
            })).collect::<Vec<_>>(),
        }))
        .into_response(),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project member store is unavailable",
        ),
    }
}

#[derive(Deserialize)]
struct ProjectMemberRequest {
    email: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    role: String,
}

async fn upsert_project_member(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ProjectMemberRequest>,
) -> Response {
    let actor = match authorize(&runtime, &headers, "projects:members:write").await {
        Ok(actor) if can_manage_project(&runtime, &actor, &project).await => actor,
        Ok(_) => {
            return error(
                StatusCode::FORBIDDEN,
                "project owner or admin access is required",
            )
        }
        Err(response) => return response,
    };
    let email = normalize_email(&request.email);
    let role = match request.role.trim() {
        "" | "dev" => "dev",
        "owner" => "owner",
        "admin" => "admin",
        _ => return error(StatusCode::BAD_REQUEST, "role must be owner, admin, or dev"),
    };
    if email.is_empty() {
        return error(StatusCode::BAD_REQUEST, "email is required");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project member store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project member store is unavailable",
        );
    };
    let stored = sqlx::query(
        r#"INSERT INTO gonvex_project_members(project_id,email,name,role)
           VALUES($1,$2,$3,$4)
           ON CONFLICT(project_id,email) DO UPDATE SET
             name=EXCLUDED.name,role=EXCLUDED.role"#,
    )
    .bind(&project)
    .bind(&email)
    .bind(request.name.trim())
    .bind(role)
    .execute(&mut **transaction.transaction())
    .await;
    if stored.is_err() || transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project member could not be stored",
        );
    }
    runtime.notify_control_changed(&project);
    (
        StatusCode::OK,
        Json(json!({"member":{
            "email":email,"name":request.name.trim(),"role":role,
            "updatedBy":actor.email,
        }})),
    )
        .into_response()
}

#[derive(Deserialize)]
struct ProjectInvitationRequest {
    email: String,
    #[serde(default)]
    role: String,
}

async fn create_project_invitation(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ProjectInvitationRequest>,
) -> Response {
    let actor = match authorize(&runtime, &headers, "projects:members:write").await {
        Ok(actor) if can_manage_project(&runtime, &actor, &project).await => actor,
        Ok(_) => {
            return error(
                StatusCode::FORBIDDEN,
                "project owner or admin access is required",
            )
        }
        Err(response) => return response,
    };
    let email = normalize_email(&request.email);
    let role = match request.role.trim() {
        "" | "dev" => "dev",
        "owner" => "owner",
        "admin" => "admin",
        _ => return error(StatusCode::BAD_REQUEST, "role must be owner, admin, or dev"),
    };
    if email.is_empty() {
        return error(StatusCode::BAD_REQUEST, "email is required");
    }
    let id = format!("pinv_{}", uuid::Uuid::new_v4());
    let token = format!("invite_{}", random_secret());
    let expires_at = Utc::now() + chrono::Duration::days(14);
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project invitation store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project invitation store is unavailable",
        );
    };
    let stored = sqlx::query(
        r#"INSERT INTO gonvex_project_invitations
           (id,project_id,email,role,token_hash,invited_by,expires_at,accepted_at,created_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,NULL,now())
           ON CONFLICT(project_id,email) DO UPDATE SET
             id=EXCLUDED.id,role=EXCLUDED.role,token_hash=EXCLUDED.token_hash,
             invited_by=EXCLUDED.invited_by,expires_at=EXCLUDED.expires_at,
             accepted_at=NULL,created_at=now()"#,
    )
    .bind(&id)
    .bind(&project)
    .bind(&email)
    .bind(role)
    .bind(sha256_hex(token.as_bytes()))
    .bind(&actor.email)
    .bind(expires_at)
    .execute(&mut **transaction.transaction())
    .await;
    if stored.is_err() || transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project invitation could not be stored",
        );
    }
    (
        StatusCode::CREATED,
        Json(json!({"invitation":{
            "id":id,"projectId":project,"email":email,"role":role,
            "expiresAt":expires_at,"accepted":false,"token":token,
        }})),
    )
        .into_response()
}

async fn project_auth_accounts(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:read", false).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth account store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth account store is unavailable",
        );
    };
    match sqlx::query(
        r#"SELECT account.id,account.email,account.name,account.avatar_url,
                  account.disabled_at IS NOT NULL AS disabled,account.created_at,account.updated_at,
                  COALESCE(identity.verified_email,FALSE) AS email_verified,
                  COALESCE(identity.provider,'') AS provider
           FROM accounts account LEFT JOIN LATERAL (
             SELECT provider,verified_email FROM account_identities
             WHERE account_id=account.id ORDER BY updated_at DESC LIMIT 1
           ) identity ON TRUE
           WHERE account.auth_realm_id=$1 ORDER BY account.created_at DESC"#,
    )
    .bind(&project)
    .fetch_all(&mut **transaction.transaction())
    .await
    {
        Ok(rows) => Json(json!({"accounts":rows.into_iter().map(|row|json!({
            "id":row.get::<String,_>("id"),"accountId":row.get::<String,_>("id"),
            "email":row.get::<String,_>("email"),"emailVerified":row.get::<bool,_>("email_verified"),
            "name":row.get::<String,_>("name"),"picture":row.get::<String,_>("avatar_url"),
            "provider":row.get::<String,_>("provider"),"disabled":row.get::<bool,_>("disabled"),
            "createdAt":row.get::<DateTime<Utc>,_>("created_at"),
            "lastSignedInAt":row.get::<DateTime<Utc>,_>("updated_at"),
        })).collect::<Vec<_>>() })).into_response(),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "auth account store is unavailable"),
    }
}

#[derive(Deserialize)]
struct AccountDisabledRequest {
    disabled: bool,
}

async fn update_project_auth_account(
    State(runtime): State<Runtime>,
    Path((project, account)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<AccountDisabledRequest>,
) -> Response {
    set_project_account_disabled(&runtime, &headers, &project, &account, request.disabled).await
}

async fn delete_project_auth_account(
    State(runtime): State<Runtime>,
    Path((project, account)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    set_project_account_disabled(&runtime, &headers, &project, &account, true).await
}

async fn set_project_account_disabled(
    runtime: &Runtime,
    headers: &HeaderMap,
    project: &str,
    account: &str,
    disabled: bool,
) -> Response {
    if let Err(response) =
        authorize_project_resource(runtime, headers, project, "projects:update", true).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth account store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth account store is unavailable",
        );
    };
    let updated = sqlx::query(
        r#"UPDATE accounts SET disabled_at=CASE WHEN $3 THEN COALESCE(disabled_at,now()) ELSE NULL END,
                  updated_at=now() WHERE auth_realm_id=$1 AND id=$2"#,
    )
    .bind(project)
    .bind(account)
    .bind(disabled)
    .execute(&mut **transaction.transaction())
    .await;
    let Ok(updated) = updated else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth account could not be updated",
        );
    };
    if updated.rows_affected() == 0 {
        return error(StatusCode::NOT_FOUND, "account not found");
    }
    if disabled {
        let revoked = sqlx::query(
            "UPDATE gonvex_auth_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2",
        )
        .bind(project)
        .bind(account)
        .execute(&mut **transaction.transaction())
        .await;
        if revoked.is_err() {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth account could not be updated",
            );
        }
        let refreshed = sqlx::query(
            "UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2",
        )
        .bind(project)
        .bind(account)
        .execute(&mut **transaction.transaction())
        .await;
        if refreshed.is_err() {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth account could not be updated",
            );
        }
    }
    if transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth account could not be updated",
        );
    }
    Json(json!({"ok":true,"disabled":disabled})).into_response()
}

async fn project_auth_tenants(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:read", false).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "tenant directory is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "tenant directory is unavailable",
        );
    };
    match sqlx::query(
        r#"SELECT tenant.tenant_id,tenant.name,count(directory.account_id) FILTER (WHERE directory.status='active') AS member_count
           FROM gonvex_runtime_tenants tenant
           LEFT JOIN account_tenant_index directory ON directory.tenant_id=tenant.tenant_id
           WHERE tenant.project_id=$1 AND tenant.deleted_at IS NULL
           GROUP BY tenant.tenant_id,tenant.name ORDER BY lower(tenant.name),tenant.tenant_id"#,
    )
    .bind(&project)
    .fetch_all(&mut **transaction.transaction())
    .await
    {
        Ok(rows) => Json(json!({"tenants":rows.into_iter().map(|row|json!({
            "id":row.get::<String,_>("tenant_id"),"name":row.get::<String,_>("name"),
            "memberCount":row.get::<i64,_>("member_count"),
        })).collect::<Vec<_>>() })).into_response(),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "tenant directory is unavailable"),
    }
}

#[derive(Deserialize)]
struct ProjectAuthTenantRequest {
    name: String,
    #[serde(default, rename = "ownerEmail")]
    owner_email: String,
}

async fn create_project_auth_tenant(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ProjectAuthTenantRequest>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let name = request.name.trim();
    if name.is_empty() {
        return error(StatusCode::BAD_REQUEST, "tenant name is required");
    }
    let tenant = match runtime.provision_runtime_tenant(&project, None, name).await {
        Ok(tenant) => tenant,
        Err(cause) => return error(StatusCode::UNPROCESSABLE_ENTITY, cause),
    };
    let tenant_id = tenant.get("id").and_then(Value::as_str).unwrap_or_default();
    if !request.owner_email.trim().is_empty() {
        if let Err(cause) = store_operator_membership_invitation(
            &runtime,
            &project,
            tenant_id,
            &request.owner_email,
            "owner",
            json!({}),
        )
        .await
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":cause,"tenant":tenant})),
            )
                .into_response();
        }
    }
    runtime.notify_control_changed(&project);
    (StatusCode::CREATED, Json(json!({"tenant":tenant}))).into_response()
}

#[derive(Default, Deserialize)]
struct MembershipQuery {
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    member: String,
    #[serde(default)]
    email: String,
}

#[derive(Deserialize)]
struct MembershipRequest {
    email: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    permissions: Value,
}

async fn project_auth_memberships(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Query(query): Query<MembershipQuery>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:read", false).await
    {
        return response;
    }
    let tenant = query.tenant.trim();
    if tenant.is_empty() {
        return error(StatusCode::BAD_REQUEST, "tenant is required");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project auth store is unavailable",
        );
    };
    let route = match control.resolve_tenant(&project, tenant).await {
        Ok(route) => route,
        Err(cause) => return error(StatusCode::NOT_FOUND, cause.to_string()),
    };
    let pool = match runtime.inner.pools.pool(&route.database_url).await {
        Ok(pool) => pool,
        Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
    };
    let member_rows = {
        let _admission = match runtime.inner.pools.admit().await {
            Ok(admission) => admission,
            Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
        };
        match sqlx::query(
            "SELECT id,account_id,display_name,role,permissions FROM members WHERE status='active' ORDER BY lower(display_name),id",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
        }
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project auth store is unavailable",
        );
    };
    let mut members = Vec::new();
    for row in member_rows {
        let account_id = row.get::<String, _>("account_id");
        let account =
            sqlx::query("SELECT email,name FROM accounts WHERE auth_realm_id=$1 AND id=$2")
                .bind(&project)
                .bind(&account_id)
                .fetch_optional(&mut **transaction.transaction())
                .await
                .ok()
                .flatten();
        members.push(json!({
            "memberId":row.get::<String,_>("id"),
            "email":account.as_ref().map(|account|account.get::<String,_>("email")).unwrap_or_default(),
            "name":account.as_ref().map(|account|account.get::<String,_>("name")).filter(|value|!value.is_empty()).unwrap_or_else(||row.get::<String,_>("display_name")),
            "role":row.get::<String,_>("role"),
            "permissions":row.get::<SqlJson<Value>,_>("permissions").0,
        }));
    }
    let invitation_rows = match sqlx::query(
        r#"SELECT email,role,permissions,expires_at FROM gonvex_auth_membership_invitations
           WHERE project_id=$1 AND tenant_id=$2 AND expires_at>now()
             AND revoked_at IS NULL AND accepted_at IS NULL ORDER BY lower(email)"#,
    )
    .bind(&project)
    .bind(tenant)
    .fetch_all(&mut **transaction.transaction())
    .await
    {
        Ok(rows) => rows,
        Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
    };
    let invitations = invitation_rows
        .into_iter()
        .map(|row| {
            json!({
                "email":row.get::<String,_>("email"),
                "role":row.get::<String,_>("role"),
                "permissions":row.get::<SqlJson<Value>,_>("permissions").0,
                "expiresAt":row.get::<DateTime<Utc>,_>("expires_at"),
            })
        })
        .collect::<Vec<_>>();
    if transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project auth store is unavailable",
        );
    }
    Json(json!({"members":members,"invitations":invitations})).into_response()
}

async fn put_project_auth_membership(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Query(query): Query<MembershipQuery>,
    Json(request): Json<MembershipRequest>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let tenant = query.tenant.trim();
    if tenant.is_empty() {
        return error(StatusCode::BAD_REQUEST, "tenant is required");
    }
    let role = match request.role.trim() {
        "" => "member",
        "owner" => "owner",
        "admin" => "admin",
        "member" => "member",
        _ => return error(StatusCode::BAD_REQUEST, "invalid member role"),
    };
    let permissions = if request.permissions.is_null() {
        json!({})
    } else if request.permissions.is_object() {
        request.permissions
    } else {
        return error(StatusCode::BAD_REQUEST, "permissions must be an object");
    };
    match store_operator_membership_invitation(
        &runtime,
        &project,
        tenant,
        &request.email,
        role,
        permissions,
    )
    .await
    {
        Ok(()) => {
            runtime.notify_control_changed(&project);
            Json(json!({"ok":true})).into_response()
        }
        Err(cause) => error(StatusCode::BAD_REQUEST, cause),
    }
}

async fn store_operator_membership_invitation(
    runtime: &Runtime,
    project: &str,
    tenant: &str,
    email: &str,
    role: &str,
    permissions: Value,
) -> Result<(), String> {
    let email = normalize_email(email);
    if email.is_empty() {
        return Err("email is required".to_owned());
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return Err("project auth store is unavailable".to_owned());
    };
    control
        .tenant_directory_entry(project, tenant)
        .await
        .map_err(|error| error.to_string())?;
    let id = format!("invite_{}", uuid::Uuid::new_v4());
    let token = format!("invite_{}", random_secret());
    let mut transaction = control
        .begin_control_transaction(false)
        .await
        .map_err(|error| error.to_string())?;
    sqlx::query(
        r#"INSERT INTO gonvex_auth_membership_invitations
           (project_id,tenant_id,email,role,permissions,invited_by,expires_at,id,token_hash,
            revoked_at,accepted_at,accepted_account_id,accepted_idempotency_key,handoff_state,
            handoff_command_id,completed_at,updated_at)
           VALUES($1,$2,$3,$4,$5,'project-admin',now()+interval '7 days',$6,$7,
                  NULL,NULL,NULL,NULL,'pending','',NULL,now())
           ON CONFLICT(project_id,tenant_id,email) DO UPDATE SET role=EXCLUDED.role,
             permissions=EXCLUDED.permissions,invited_by=EXCLUDED.invited_by,
             expires_at=EXCLUDED.expires_at,id=EXCLUDED.id,token_hash=EXCLUDED.token_hash,
             revoked_at=NULL,accepted_at=NULL,accepted_account_id=NULL,
             accepted_idempotency_key=NULL,handoff_state='pending',handoff_command_id='',
             completed_at=NULL,updated_at=now()"#,
    )
    .bind(project)
    .bind(tenant)
    .bind(email)
    .bind(role)
    .bind(SqlJson(permissions))
    .bind(id)
    .bind(sha256_hex(token.as_bytes()))
    .execute(&mut **transaction.transaction())
    .await
    .map_err(|error| error.to_string())?;
    transaction
        .commit()
        .await
        .map_err(|error| error.to_string())
}

async fn delete_project_auth_membership(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Query(query): Query<MembershipQuery>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let tenant = query.tenant.trim();
    if tenant.is_empty() || (query.member.trim().is_empty() && query.email.trim().is_empty()) {
        return error(
            StatusCode::BAD_REQUEST,
            "tenant and member or invitation email are required",
        );
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project auth store is unavailable",
        );
    };
    if !query.member.trim().is_empty() {
        let route = match control.resolve_tenant(&project, tenant).await {
            Ok(route) => route,
            Err(cause) => return error(StatusCode::NOT_FOUND, cause.to_string()),
        };
        let pool = match runtime.inner.pools.pool(&route.database_url).await {
            Ok(pool) => pool,
            Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
        };
        let _admission = match runtime.inner.pools.admit().await {
            Ok(admission) => admission,
            Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
        };
        match sqlx::query("UPDATE members SET status='revoked',membership_revision=membership_revision+1,updated_at=now() WHERE id=$1")
            .bind(query.member.trim()).execute(&pool).await {
            Ok(result) if result.rows_affected() > 0 => {}
            Ok(_) => return error(StatusCode::BAD_REQUEST, "member was not found"),
            Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
        }
    } else {
        let Ok(mut transaction) = control.begin_control_transaction(false).await else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "project auth store is unavailable",
            );
        };
        let result = sqlx::query("UPDATE gonvex_auth_membership_invitations SET revoked_at=now(),updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND lower(email)=lower($3) AND accepted_at IS NULL")
            .bind(&project).bind(tenant).bind(query.email.trim()).execute(&mut **transaction.transaction()).await;
        if result.is_err() || transaction.commit().await.is_err() {
            return error(StatusCode::BAD_REQUEST, "invitation could not be removed");
        }
    }
    runtime.notify_control_changed(&project);
    Json(json!({"ok":true})).into_response()
}

#[derive(Deserialize)]
struct ProjectQuery {
    #[serde(default)]
    project: String,
}

async fn storage_files(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<ProjectQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:read", false).await
    {
        return response;
    }
    if !runtime.inner.storage.configured() {
        return Json(json!({"configured":false,"files":[]})).into_response();
    }
    match runtime
        .inner
        .storage
        .list_project_files(&runtime, &project, 1_000)
        .await
    {
        Ok(files) => Json(json!({
            "configured":true,
            "bucket":runtime.inner.config.storage.bucket,
            "files":files,
        }))
        .into_response(),
        Err(message) => error(StatusCode::BAD_GATEWAY, message),
    }
}

async fn list_tokens(State(runtime): State<Runtime>, headers: HeaderMap) -> Response {
    let actor = match authorize(&runtime, &headers, "tokens:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        );
    };
    match sqlx::query(
        r#"SELECT id,name,token_prefix,permissions,created_at,expires_at,last_used_at,revoked_at
           FROM gonvex_account_access_tokens WHERE owner_email=$1 ORDER BY created_at DESC"#,
    )
    .bind(&actor.email)
    .fetch_all(&mut **transaction.transaction())
    .await
    {
        Ok(rows) => Json(json!({"tokens":rows.into_iter().map(token_json).collect::<Vec<_>>() }))
            .into_response(),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        ),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateTokenRequest {
    name: String,
    #[serde(default)]
    permissions: Vec<String>,
    #[serde(default)]
    expires_at: String,
}

async fn create_token(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(request): Json<CreateTokenRequest>,
) -> Response {
    let actor = match authorize(&runtime, &headers, "tokens:create").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let name = request.name.trim();
    if name.is_empty() || name.len() > 120 {
        return error(
            StatusCode::BAD_REQUEST,
            "token name must contain 1 to 120 characters",
        );
    }
    let permissions = match normalize_permissions(&actor, &request.permissions) {
        Ok(value) => value,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let expires_at = if request.expires_at.trim().is_empty() {
        None
    } else {
        match DateTime::parse_from_rfc3339(request.expires_at.trim()) {
            Ok(value) if value > Utc::now() => Some(value.with_timezone(&Utc)),
            _ => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "expiresAt must be a future RFC3339 timestamp",
                )
            }
        }
    };
    let id = format!("pat_{}", uuid::Uuid::new_v4());
    let access_token = format!("gvx_{id}.{}", random_secret());
    let prefix = format!("gvx_{id}");
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        );
    };
    let owner_name = if actor.name.is_empty() {
        display_name(&actor.email)
    } else {
        actor.name.clone()
    };
    if sqlx::query(
        r#"INSERT INTO gonvex_dashboard_accounts(email,name,role,password_hash)
           VALUES($1,$2,$3,'!external-provider') ON CONFLICT(email) DO NOTHING"#,
    )
    .bind(&actor.email)
    .bind(owner_name)
    .bind(&actor.role)
    .execute(&mut **transaction.transaction())
    .await
    .is_err()
        || sqlx::query(
            r#"INSERT INTO gonvex_account_access_tokens
               (id,owner_email,name,token_prefix,token_hash,permissions,expires_at)
               VALUES($1,$2,$3,$4,$5,$6,$7)"#,
        )
        .bind(&id)
        .bind(&actor.email)
        .bind(name)
        .bind(&prefix)
        .bind(sha256_hex(access_token.as_bytes()))
        .bind(SqlJson(permissions.clone()))
        .bind(expires_at)
        .execute(&mut **transaction.transaction())
        .await
        .is_err()
        || transaction.commit().await.is_err()
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token could not be stored",
        );
    }
    (
        StatusCode::CREATED,
        Json(json!({
            "token":{"id":id,"name":name,"prefix":prefix,"permissions":permissions,
                     "createdAt":Utc::now(),"expiresAt":expires_at},
            "accessToken":access_token,
        })),
    )
        .into_response()
}

async fn revoke_token(
    State(runtime): State<Runtime>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    let actor = match authorize(&runtime, &headers, "tokens:revoke").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        );
    };
    let result = sqlx::query(
        "UPDATE gonvex_account_access_tokens SET revoked_at=now() WHERE id=$1 AND owner_email=$2 AND revoked_at IS NULL",
    )
    .bind(token.trim())
    .bind(&actor.email)
    .execute(&mut **transaction.transaction())
    .await;
    match result {
        Ok(result) if result.rows_affected() == 1 && transaction.commit().await.is_ok() => {
            Json(json!({"ok":true})).into_response()
        }
        Ok(_) => error(StatusCode::NOT_FOUND, "active account token not found"),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "account token store is unavailable",
        ),
    }
}

async fn list_notifications(State(runtime): State<Runtime>, headers: HeaderMap) -> Response {
    let actor = match authenticate(&runtime, &headers).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "notifications are unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "notifications are unavailable",
        );
    };
    match sqlx::query(
        r#"SELECT id,kind,title,body,project_id,metadata,read_at,created_at
           FROM gonvex_dashboard_notifications WHERE email=$1 ORDER BY created_at DESC LIMIT 100"#,
    )
    .bind(&actor.email)
    .fetch_all(&mut **transaction.transaction())
    .await
    {
        Ok(rows) => {
            let notifications = rows
                .into_iter()
                .map(|row| {
                    json!({
                        "id":row.get::<String,_>("id"),"kind":row.get::<String,_>("kind"),
                        "title":row.get::<String,_>("title"),"body":row.get::<String,_>("body"),
                        "projectId":row.get::<String,_>("project_id"),
                        "metadata":row.get::<SqlJson<Value>,_>("metadata").0,
                        "read":row.get::<Option<DateTime<Utc>>,_>("read_at").is_some(),
                        "createdAt":row.get::<DateTime<Utc>,_>("created_at"),
                    })
                })
                .collect::<Vec<_>>();
            let unread = notifications
                .iter()
                .filter(|item| item["read"] == false)
                .count();
            Json(json!({"notifications":notifications,"unread":unread})).into_response()
        }
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "notifications are unavailable",
        ),
    }
}

#[derive(Deserialize)]
struct ReadNotificationsRequest {
    #[serde(default)]
    ids: Vec<String>,
}

async fn read_notifications(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(request): Json<ReadNotificationsRequest>,
) -> Response {
    let actor = match authenticate(&runtime, &headers).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "notifications are unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "notifications are unavailable",
        );
    };
    let result = if request.ids.is_empty() {
        sqlx::query("UPDATE gonvex_dashboard_notifications SET read_at=now() WHERE email=$1 AND read_at IS NULL")
            .bind(&actor.email)
            .execute(&mut **transaction.transaction())
            .await
    } else {
        sqlx::query("UPDATE gonvex_dashboard_notifications SET read_at=now() WHERE email=$1 AND id=ANY($2) AND read_at IS NULL")
            .bind(&actor.email)
            .bind(&request.ids)
            .execute(&mut **transaction.transaction())
            .await
    };
    if result.is_err() || transaction.commit().await.is_err() {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "notifications are unavailable",
        )
    } else {
        Json(json!({"ok":true})).into_response()
    }
}

#[derive(Deserialize)]
struct ManifestQuery {
    #[serde(default)]
    project: String,
}

async fn manifest(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<ManifestQuery>,
) -> Response {
    let project = if query.project.trim().is_empty() {
        headers
            .get("x-gonvex-project-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .trim()
    } else {
        query.project.trim()
    };
    if project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "project is required");
    }
    if !project_key_matches(&runtime, &headers, project).await {
        let actor = match authorize(&runtime, &headers, "projects:read").await {
            Ok(actor) => actor,
            Err(response) => return response,
        };
        if !can_access_project(&runtime, &actor, project).await {
            return error(StatusCode::FORBIDDEN, "project access is required");
        }
    }
    let Some(module) = runtime.inner.modules.project(project).await else {
        return error(StatusCode::NOT_FOUND, "project module is not installed");
    };
    Json(json!({
        "project":project,
        "functions":module.manifest_functions,
        "schema":module.schema,
        "visibility":module.visibility,
        "module":{"hash":module.artifact_hash,"generation":module.generation},
    }))
    .into_response()
}

#[derive(Deserialize)]
struct MetricsQuery {
    #[serde(default)]
    project: String,
}
async fn metrics(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<MetricsQuery>,
) -> Response {
    let project = if query.project.trim().is_empty() {
        headers
            .get("x-gonvex-project-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .trim()
    } else {
        query.project.trim()
    };
    if project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "project is required");
    }
    let actor = match authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_access_project(&runtime, &actor, project).await {
        return error(StatusCode::FORBIDDEN, "project access is required");
    }
    match metrics_snapshot(&runtime, project).await {
        Some(value) => Json(value).into_response(),
        None => error(StatusCode::SERVICE_UNAVAILABLE, "metrics are unavailable"),
    }
}

async fn metrics_snapshot(runtime: &Runtime, project: &str) -> Option<Value> {
    let control = runtime.inner.control_plane.read().await.clone()?;
    let mut transaction = control.begin_control_transaction(true).await.ok()?;
    let rows=sqlx::query(
        r#"SELECT kind,path,count(*)::bigint AS calls,count(*) FILTER(WHERE outcome='error')::bigint AS errors,
                  COALESCE(avg(client_duration_ms),0)::double precision AS average_duration_ms,
                  COALESCE((array_agg(client_duration_ms ORDER BY created_at DESC) FILTER(WHERE client_duration_ms IS NOT NULL))[1],0)::double precision AS last_duration_ms,
                  max(created_at) AS last_called_at
           FROM gonvex_performance_events WHERE project_id=$1 AND created_at>now()-interval '24 hours'
           GROUP BY kind,path"#,
    ).bind(project).fetch_all(&mut **transaction.transaction()).await.unwrap_or_default();
    let mut functions = serde_json::Map::new();
    if let Some(module) = runtime.inner.modules.project(project).await {
        for (path, definition) in &module.functions {
            functions.insert(path.clone(),json!({"kind":definition.kind,"calls":0,"errors":0,"averageDurationMs":0,"lastDurationMs":0,"series":[]}));
        }
    }
    for row in rows {
        functions.insert(row.get::<String,_>("path"),json!({"kind":row.get::<String,_>("kind"),"calls":row.get::<i64,_>("calls"),"errors":row.get::<i64,_>("errors"),"averageDurationMs":row.get::<f64,_>("average_duration_ms"),"lastDurationMs":row.get::<f64,_>("last_duration_ms"),"lastCalledAt":row.get::<Option<DateTime<Utc>>,_>("last_called_at"),"series":[]}));
    }
    let logs=sqlx::query("SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id FROM gonvex_performance_events WHERE project_id=$1 ORDER BY created_at DESC LIMIT 200").bind(project).fetch_all(&mut **transaction.transaction()).await.unwrap_or_default().into_iter().map(|row|json!({"time":row.get::<DateTime<Utc>,_>("created_at"),"operationId":row.get::<String,_>("event_id"),"project":project,"tenant":row.get::<String,_>("tenant_id"),"accountId":row.get::<String,_>("account_id"),"path":row.get::<String,_>("path"),"kind":row.get::<String,_>("kind"),"outcome":row.get::<String,_>("outcome"),"durationMs":row.get::<Option<f64>,_>("client_duration_ms").unwrap_or(0.0),"error":row.get::<String,_>("error"),"reason":row.get::<String,_>("reason"),"source":"client"})).collect::<Vec<_>>();
    let scheduler=sqlx::query("SELECT status,count(*)::bigint AS count FROM gonvex_scheduled_jobs WHERE project_id=$1 GROUP BY status").bind(project).fetch_all(&mut **transaction.transaction()).await.unwrap_or_default();
    let mut queued = 0_i64;
    let mut completed = 0_i64;
    for row in scheduler {
        match row.get::<String, _>("status").as_str() {
            "pending" => queued = row.get("count"),
            "completed" => completed = row.get("count"),
            _ => {}
        }
    }
    let crons = sqlx::query(
        r#"SELECT cron_name,tenant_id,function_path,min(run_at) AS next_run,count(*)::bigint AS runs
           FROM gonvex_scheduled_jobs
           WHERE project_id=$1 AND cron_name<>'' AND status='pending'
           GROUP BY cron_name,tenant_id,function_path
           ORDER BY cron_name,tenant_id,function_path"#,
    )
    .bind(project)
    .fetch_all(&mut **transaction.transaction())
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|row| {
        json!({
            "name":row.get::<String,_>("cron_name"),
            "project":project,
            "tenant":row.get::<String,_>("tenant_id"),
            "function":row.get::<String,_>("function_path"),
            "schedule":"managed by active module",
            "nextRun":row.get::<Option<DateTime<Utc>>,_>("next_run"),
            "status":"scheduled",
            "runs":row.get::<i64,_>("runs"),
            "failures":0,
        })
    })
    .collect::<Vec<_>>();
    let recent_jobs = sqlx::query(
        r#"SELECT function_path,cron_name,tenant_id,scheduled_for,completed_at,
                  GREATEST(EXTRACT(EPOCH FROM (created_at-scheduled_for))*1000,0)::double precision AS lag_ms,
                  GREATEST(EXTRACT(EPOCH FROM (completed_at-updated_at))*1000,0)::double precision AS duration_ms
           FROM gonvex_scheduled_jobs
           WHERE project_id=$1 AND status='completed'
           ORDER BY completed_at DESC NULLS LAST LIMIT 100"#,
    )
    .bind(project)
    .fetch_all(&mut **transaction.transaction())
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|row| json!({
        "time":row.get::<Option<DateTime<Utc>>,_>("completed_at"),
        "project":project,
        "tenant":row.get::<String,_>("tenant_id"),
        "function":row.get::<String,_>("function_path"),
        "cron":row.get::<String,_>("cron_name"),
        "outcome":"completed",
        "lagMs":row.get::<f64,_>("lag_ms"),
        "durationMs":row.get::<f64,_>("duration_ms"),
    }))
    .collect::<Vec<_>>();
    let websocket = runtime.inner.metrics.snapshot(project);
    let database = runtime.inner.pools.snapshot().await;
    let database_json = json!({
        "pools":database.pools,
        "openConnections":database.open_connections,
        "inUse":database.in_use,
        "idle":database.idle,
        "maxOpenConnections":database.max_open_connections,
        "waitCount":0,
        "waitDurationMs":0,
        "series":[],
    });
    let query_admission = json!({
        "enabled":database.admission_limit > 0,
        "totalPermits":database.admission_limit,
        "active":database.admission_active,
        "bootstrapPermits":0,
        "bootstrapActive":0,
        "reactive":{"active":0,"queueDepth":0,"admitted":0,"waited":0,"cancelled":0,"waitMs":0,"maxWaitMs":0,"tenantsQueued":0,"largestTenantQueue":0},
        "foreground":{"active":database.admission_active,"queueDepth":0,"admitted":0,"waited":0,"cancelled":0,"waitMs":0,"maxWaitMs":0,"tenantsQueued":0,"largestTenantQueue":0},
        "bootstrap":{"active":0,"queueDepth":0,"admitted":0,"waited":0,"cancelled":0,"waitMs":0,"maxWaitMs":0,"tenantsQueued":0,"largestTenantQueue":0},
        "reactiveDelayedByBootstrap":0,
    });
    Some(
        json!({"generatedAt":Utc::now(),"functions":functions,"cache":{"hits":0,"misses":0,"bypasses":0,"requests":0,"hitRate":0,"series":[]},"running":{"current":{},"total":0,"series":[]},"websocket":websocket,"database":database_json,"queryAdmission":query_admission,"scheduler":{"running":0,"queued":queued,"scheduled":queued,"completed":completed,"failed":0,"lagMs":0,"crons":crons,"recent":recent_jobs,"series":[]},"logs":logs}),
    )
}

async fn metrics_stream(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<MetricsQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    if project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "project is required");
    }
    let protocol = headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .split(',')
                .map(str::trim)
                .find(|value| value.starts_with("gonvex-dashboard-auth."))
        })
        .map(str::to_owned);
    let mut auth_headers = HeaderMap::new();
    if let Some(token) = protocol
        .as_deref()
        .and_then(|value| value.strip_prefix("gonvex-dashboard-auth."))
    {
        if let Ok(value) = format!("Bearer {token}").parse() {
            auth_headers.insert(axum::http::header::AUTHORIZATION, value);
        }
    }
    let actor = match authorize(&runtime, &auth_headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_access_project(&runtime, &actor, &project).await {
        return error(StatusCode::FORBIDDEN, "project access is required");
    }
    let upgrade = if let Some(protocol) = protocol {
        upgrade.protocols([protocol])
    } else {
        upgrade
    };
    upgrade.on_upgrade(move |mut socket| async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            let Some(metrics) = metrics_snapshot(&runtime, &project).await else {
                break;
            };
            let message = json!({"type":"metrics","metrics":metrics}).to_string();
            if socket.send(Message::Text(message.into())).await.is_err() {
                break;
            }
        }
    })
}

#[derive(Deserialize)]
struct LogStreamQuery {
    #[serde(default)]
    project: String,
    #[serde(default)]
    key: String,
    #[serde(default = "default_log_replay")]
    replay: String,
}

fn default_log_replay() -> String {
    "1".to_owned()
}

async fn log_stream(
    State(runtime): State<Runtime>,
    Query(query): Query<LogStreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let project = query.project.trim().to_owned();
    if project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "project id is required");
    }
    let configured_admin = runtime.inner.config.admin_key.as_deref().unwrap_or("");
    let allowed = (!configured_admin.is_empty()
        && constant_time_eq(configured_admin, query.key.trim()))
        || if let Some(control) = runtime.inner.control_plane.read().await.clone() {
            control
                .project_accepts_sync_key(&project, query.key.trim(), None)
                .await
                .unwrap_or(false)
        } else {
            false
        };
    if !allowed {
        return error(StatusCode::UNAUTHORIZED, "invalid Gonvex sync key");
    }
    let replay = !matches!(
        query.replay.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    );
    upgrade.on_upgrade(move |mut socket| async move {
        if socket
            .send(Message::Text(json!({"type":"ready"}).to_string().into()))
            .await
            .is_err()
        {
            return;
        }
        let mut last_time: Option<DateTime<Utc>> = None;
        let mut last_id = String::new();
        let mut first = true;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            interval.tick().await;
            let Some(control) = runtime.inner.control_plane.read().await.clone() else {
                break;
            };
            let Ok(mut transaction) = control.begin_control_transaction(true).await else {
                break;
            };
            let rows = if first && replay {
                sqlx::query(
                    r#"SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id
                       FROM gonvex_performance_events WHERE project_id=$1
                       ORDER BY created_at DESC,event_id DESC LIMIT 200"#,
                )
                .bind(&project)
                .fetch_all(&mut **transaction.transaction())
                .await
                .map(|mut rows| {
                    rows.reverse();
                    rows
                })
            } else if let Some(last_time) = last_time {
                sqlx::query(
                    r#"SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id
                       FROM gonvex_performance_events WHERE project_id=$1
                         AND (created_at>$2 OR (created_at=$2 AND event_id>$3))
                       ORDER BY created_at,event_id LIMIT 200"#,
                )
                .bind(&project)
                .bind(last_time)
                .bind(&last_id)
                .fetch_all(&mut **transaction.transaction())
                .await
            } else {
                sqlx::query(
                    r#"SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id
                       FROM gonvex_performance_events WHERE project_id=$1 AND FALSE"#,
                )
                .bind(&project)
                .fetch_all(&mut **transaction.transaction())
                .await
            };
            first = false;
            let Ok(rows) = rows else {
                break;
            };
            for row in rows {
                let created_at = row.get::<DateTime<Utc>, _>("created_at");
                let event_id = row.get::<String, _>("event_id");
                let log = json!({
                    "time":created_at,
                    "operationId":event_id,
                    "project":project,
                    "tenant":row.get::<String,_>("tenant_id"),
                    "accountId":row.get::<String,_>("account_id"),
                    "path":row.get::<String,_>("path"),
                    "kind":row.get::<String,_>("kind"),
                    "outcome":row.get::<String,_>("outcome"),
                    "durationMs":row.get::<Option<f64>,_>("client_duration_ms").unwrap_or(0.0),
                    "error":row.get::<String,_>("error"),
                    "reason":row.get::<String,_>("reason"),
                    "source":"client",
                });
                if socket
                    .send(Message::Text(
                        json!({"type":"log","log":log}).to_string().into(),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
                last_time = Some(created_at);
                last_id = event_id;
            }
        }
    })
}

async fn clear_logs(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<MetricsQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "logs are unavailable");
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "logs are unavailable");
    };
    let deleted = sqlx::query("DELETE FROM gonvex_performance_events WHERE project_id=$1")
        .bind(&project)
        .execute(&mut **transaction.transaction())
        .await;
    match deleted {
        Ok(deleted) if transaction.commit().await.is_ok() => {
            Json(json!({"cleared":deleted.rows_affected()})).into_response()
        }
        _ => error(StatusCode::SERVICE_UNAVAILABLE, "logs could not be cleared"),
    }
}

async fn clear_cache(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<MetricsQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    runtime.inner.live_query_cache.clear().await;
    Json(json!({"cleared":true,"project":project})).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateProjectRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    database_mode: Option<String>,
    #[serde(default)]
    error_tracking_enabled: Option<bool>,
    #[serde(default)]
    status: Option<String>,
}

async fn update_project(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateProjectRequest>,
) -> Response {
    let actor = match authorize(&runtime, &headers, "projects:update").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_manage_project(&runtime, &actor, &project).await {
        return error(
            StatusCode::FORBIDDEN,
            "project owner or admin access is required",
        );
    }
    if request.name.is_none()
        && request.database_mode.is_none()
        && request.error_tracking_enabled.is_none()
        && request.status.is_none()
    {
        return error(StatusCode::BAD_REQUEST, "no project fields provided");
    }
    if request
        .name
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.trim().len() > 120)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "project name must contain 1 to 120 characters",
        );
    }
    if request
        .database_mode
        .as_deref()
        .is_some_and(|value| !matches!(value, "single" | "multiTenant"))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "databaseMode must be single or multiTenant",
        );
    }
    if request
        .status
        .as_deref()
        .is_some_and(|value| value != "active")
    {
        return error(
            StatusCode::BAD_REQUEST,
            "status may only promote a project to active",
        );
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let row=sqlx::query(
        r#"UPDATE gonvex_runtime_projects SET
             name=COALESCE($2,name),database_mode=COALESCE($3,database_mode),
             error_tracking_enabled=COALESCE($4,error_tracking_enabled),
             status=COALESCE($5,status),updated_at=now()
           WHERE id=$1 AND status NOT IN('deleted','disabled')
           RETURNING id,name,environment,database_name,database_mode,storage_bucket,status,
                     description,provisioned,runtime_created,test_tab,error_tracking_enabled,owner_email"#,
    ).bind(&project).bind(request.name.as_deref().map(str::trim)).bind(request.database_mode).bind(request.error_tracking_enabled).bind(request.status)
      .fetch_optional(&mut **transaction.transaction()).await;
    let Ok(Some(row)) = row else {
        return error(StatusCode::NOT_FOUND, "project not found");
    };
    if transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project update could not be committed",
        );
    }
    Json(json!({"project":project_json(row)})).into_response()
}

async fn delete_project(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    let actor = match authorize(&runtime, &headers, "projects:delete").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_manage_project(&runtime, &actor, &project).await {
        return error(
            StatusCode::FORBIDDEN,
            "project owner or admin access is required",
        );
    }
    let Some(base_url) = runtime.inner.config.default_database_url.as_deref() else {
        return error(StatusCode::BAD_REQUEST, "DATABASE_URL is not configured");
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    match control.delete_runtime_project(base_url, &project).await {
        Ok(()) => Json(json!({"ok":true})).into_response(),
        Err(cause) => error(StatusCode::UNPROCESSABLE_ENTITY, cause.to_string()),
    }
}

async fn rotate_project_key(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    let actor = match authorize(&runtime, &headers, "projects:keys:write").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_manage_project(&runtime, &actor, &project).await {
        return error(
            StatusCode::FORBIDDEN,
            "project owner or admin access is required",
        );
    }
    let project_key = project_key(&project);
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    match sqlx::query(
        "UPDATE gonvex_runtime_projects SET project_key=$2,updated_at=now() WHERE id=$1",
    )
    .bind(&project)
    .bind(&project_key)
    .execute(&mut **transaction.transaction())
    .await
    {
        Ok(result) if result.rows_affected() == 1 && transaction.commit().await.is_ok() => {
            Json(json!({"projectKey":project_key})).into_response()
        }
        Ok(_) => error(StatusCode::NOT_FOUND, "project not found"),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project key rotation failed",
        ),
    }
}

async fn get_project_env(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:env:read", false).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    match sqlx::query("SELECT name,value,updated_at FROM gonvex_project_env WHERE project_id=$1 ORDER BY name").bind(&project).fetch_all(&mut **transaction.transaction()).await {
        Ok(rows)=>Json(json!({"variables":rows.into_iter().map(|row|json!({"name":row.get::<String,_>("name"),"value":row.get::<String,_>("value"),"source":"project","updatedAt":row.get::<DateTime<Utc>,_>("updated_at")})).collect::<Vec<_>>() })).into_response(),
        Err(_)=>error(StatusCode::SERVICE_UNAVAILABLE,"project environment is unavailable"),
    }
}

#[derive(Deserialize)]
struct SetEnvRequest {
    name: String,
    value: String,
}
async fn set_project_env(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SetEnvRequest>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:env:write", true).await
    {
        return response;
    }
    let name = request.name.trim();
    if !valid_env_name(name) {
        return error(StatusCode::BAD_REQUEST, "variable name is invalid");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let result=sqlx::query("INSERT INTO gonvex_project_env(project_id,name,value,updated_at) VALUES($1,$2,$3,now()) ON CONFLICT(project_id,name) DO UPDATE SET value=EXCLUDED.value,updated_at=now()")
      .bind(&project).bind(name).bind(request.value).execute(&mut **transaction.transaction()).await;
    if result.is_ok() && transaction.commit().await.is_ok() {
        Json(json!({"ok":true})).into_response()
    } else {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project environment could not be stored",
        )
    }
}

#[derive(Deserialize)]
struct ReplaceEnvRequest {
    #[serde(default)]
    content: String,
    #[serde(default)]
    variables: Vec<SetEnvRequest>,
}
async fn replace_project_env(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReplaceEnvRequest>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:env:write", true).await
    {
        return response;
    }
    let mut pairs = parse_dotenv(&request.content);
    for item in request.variables {
        if valid_env_name(item.name.trim()) {
            pairs.insert(item.name.trim().to_owned(), item.value);
        }
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    if sqlx::query("DELETE FROM gonvex_project_env WHERE project_id=$1")
        .bind(&project)
        .execute(&mut **transaction.transaction())
        .await
        .is_err()
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project environment could not be stored",
        );
    }
    for (name, value) in &pairs {
        if sqlx::query("INSERT INTO gonvex_project_env(project_id,name,value) VALUES($1,$2,$3)")
            .bind(&project)
            .bind(name)
            .bind(value)
            .execute(&mut **transaction.transaction())
            .await
            .is_err()
        {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "project environment could not be stored",
            );
        }
    }
    if transaction.commit().await.is_err() {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project environment could not be stored",
        )
    } else {
        Json(json!({"ok":true,"count":pairs.len()})).into_response()
    }
}

#[derive(Deserialize)]
struct DeleteEnvRequest {
    name: String,
}
async fn delete_project_env(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<DeleteEnvRequest>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:env:write", true).await
    {
        return response;
    }
    let name = request.name.trim();
    if !valid_env_name(name) {
        return error(StatusCode::BAD_REQUEST, "variable name is invalid");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let result = sqlx::query("DELETE FROM gonvex_project_env WHERE project_id=$1 AND name=$2")
        .bind(&project)
        .bind(name)
        .execute(&mut **transaction.transaction())
        .await;
    if result.is_ok() && transaction.commit().await.is_ok() {
        Json(json!({"ok":true})).into_response()
    } else {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project environment could not be updated",
        )
    }
}

async fn get_auth_provider(
    State(runtime): State<Runtime>,
    Path((project, provider)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if !supported_provider(&provider) {
        return error(StatusCode::BAD_REQUEST, "provider is unsupported");
    }
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:read", false).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let row=sqlx::query(
        r#"SELECT COALESCE(NULLIF(project.auth_mode,''),'gonvex-native') AS auth_mode,
                  provider.enabled,provider.signup_mode,provider.azure_tenant_id,provider.client_id,
                  provider.client_secret_encrypted IS NOT NULL AS has_client_secret,
                  provider.issuer,provider.audience,provider.jwks_url,provider.firebase_project_id,
                  provider.firebase_tenant_id,provider.admin_credentials_encrypted IS NOT NULL AS has_admin_credentials
           FROM gonvex_runtime_projects project JOIN gonvex_auth_providers provider
             ON provider.project_id=project.id AND provider.provider=$2 WHERE project.id=$1"#,
    ).bind(&project).bind(&provider).fetch_optional(&mut **transaction.transaction()).await;
    match row {
        Ok(Some(row)) => Json(provider_json(&provider, row)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "provider is not configured"),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider configuration is unavailable",
        ),
    }
}

async fn put_auth_provider(
    State(runtime): State<Runtime>,
    Path((project, provider)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    if !supported_provider(&provider) {
        return error(StatusCode::BAD_REQUEST, "provider is unsupported");
    }
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let Some(object) = payload.as_object() else {
        return error(
            StatusCode::BAD_REQUEST,
            "provider configuration must be an object",
        );
    };
    let auth_mode = object.get("authMode").and_then(Value::as_str).unwrap_or(
        if provider == "google" || provider == "microsoft" || provider == "apple" {
            "gonvex-native"
        } else {
            provider.as_str()
        },
    );
    if !matches!(
        auth_mode,
        "gonvex-native" | "firebase" | "external-oidc" | "hybrid"
    ) {
        return error(StatusCode::BAD_REQUEST, "authMode is unsupported");
    }
    let signup_mode = object
        .get("signupMode")
        .and_then(Value::as_str)
        .unwrap_or("personal");
    if !matches!(signup_mode, "personal" | "inviteOnly") {
        return error(
            StatusCode::BAD_REQUEST,
            "signupMode must be personal or inviteOnly",
        );
    }
    let enabled = object
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let issuer = text_value(object, "issuer");
    let audience = text_value(object, "audience");
    let jwks_url = text_value(object, "jwksUrl");
    let firebase_project = text_value(object, "firebaseProjectId");
    let firebase_tenant = text_value(object, "firebaseTenantId");
    let azure_tenant = text_value(object, "azureTenantId");
    let client_id = text_value(object, "clientId");
    if provider == "firebase" && firebase_project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "firebaseProjectId is required");
    }
    if provider == "external-oidc"
        && (issuer.is_empty() || audience.is_empty() || jwks_url.is_empty())
    {
        return error(
            StatusCode::BAD_REQUEST,
            "external-oidc requires issuer, audience, and jwksUrl",
        );
    }
    for value in [&issuer, &jwks_url] {
        if !value.is_empty() && !secure_external_url(value) {
            return error(
                StatusCode::BAD_REQUEST,
                "issuer and jwksUrl must use https without credentials",
            );
        }
    }
    let client_secret = object
        .get("clientSecret")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| encrypt_control_secret(&runtime, value))
        .transpose();
    let admin_credentials = object
        .get("adminCredentials")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| encrypt_control_secret(&runtime, value))
        .transpose();
    let (client_secret, admin_credentials) = match (client_secret, admin_credentials) {
        (Ok(client), Ok(admin)) => (client, admin),
        _ => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "provider secrets cannot be encrypted",
            )
        }
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let result=sqlx::query(
        r#"INSERT INTO gonvex_auth_providers
           (project_id,provider,enabled,signup_mode,azure_tenant_id,client_id,client_secret_encrypted,
            issuer,audience,jwks_url,firebase_project_id,firebase_tenant_id,admin_credentials_encrypted,updated_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now())
           ON CONFLICT(project_id,provider) DO UPDATE SET enabled=EXCLUDED.enabled,signup_mode=EXCLUDED.signup_mode,
             azure_tenant_id=EXCLUDED.azure_tenant_id,client_id=EXCLUDED.client_id,
             client_secret_encrypted=COALESCE(EXCLUDED.client_secret_encrypted,gonvex_auth_providers.client_secret_encrypted),
             issuer=EXCLUDED.issuer,audience=EXCLUDED.audience,jwks_url=EXCLUDED.jwks_url,
             firebase_project_id=EXCLUDED.firebase_project_id,firebase_tenant_id=EXCLUDED.firebase_tenant_id,
             admin_credentials_encrypted=COALESCE(EXCLUDED.admin_credentials_encrypted,gonvex_auth_providers.admin_credentials_encrypted),updated_at=now()"#,
    ).bind(&project).bind(&provider).bind(enabled).bind(signup_mode).bind(azure_tenant).bind(client_id).bind(client_secret)
      .bind(issuer).bind(audience).bind(jwks_url).bind(firebase_project).bind(firebase_tenant).bind(admin_credentials)
      .execute(&mut **transaction.transaction()).await;
    if result.is_err() {
        return error(
            StatusCode::BAD_REQUEST,
            "provider configuration could not be stored",
        );
    }
    if sqlx::query("UPDATE gonvex_runtime_projects SET auth_mode=$2,updated_at=now() WHERE id=$1")
        .bind(&project)
        .bind(auth_mode)
        .execute(&mut **transaction.transaction())
        .await
        .is_err()
        || transaction.commit().await.is_err()
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider configuration could not be committed",
        );
    }
    let _ = runtime
        .inner
        .runtime_events
        .send(crate::RuntimeEvent::ControlChanged {
            project_id: project.clone(),
        });
    Json(json!({"provider":provider,"authMode":auth_mode,"enabled":enabled,"signupMode":signup_mode,"updated":true})).into_response()
}

async fn get_google_auth(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:read", false).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let provider=sqlx::query("SELECT enabled,signup_mode,client_id,client_secret_encrypted IS NOT NULL AS has_secret FROM gonvex_auth_providers WHERE project_id=$1 AND provider='google'").bind(&project).fetch_optional(&mut **transaction.transaction()).await.ok().flatten();
    let redirects=sqlx::query_scalar::<_,String>("SELECT redirect_uri FROM gonvex_auth_redirect_uris WHERE project_id=$1 AND provider='google' ORDER BY redirect_uri").bind(&project).fetch_all(&mut **transaction.transaction()).await.unwrap_or_default();
    let enabled = provider.as_ref().is_some_and(|row| row.get("enabled"));
    let signup_mode = provider
        .as_ref()
        .map(|row| row.get::<String, _>("signup_mode"))
        .unwrap_or_else(|| "personal".to_owned());
    let configured_client = provider.as_ref().is_some_and(|row| {
        !row.get::<String, _>("client_id").is_empty() && row.get::<bool, _>("has_secret")
    });
    let env_client = runtime.inner.config.google_client_id.is_some()
        && runtime.inner.config.google_client_secret.is_some();
    let callback = runtime
        .inner
        .config
        .auth_public_url
        .as_deref()
        .map(|value| format!("{}/auth/google/callback", value.trim_end_matches('/')))
        .unwrap_or_default();
    let mut issues = Vec::new();
    if !configured_client && !env_client {
        issues.push("Google client ID and secret are not configured");
    }
    if callback.is_empty() {
        issues.push("GONVEX_AUTH_URL is required");
    }
    if redirects.is_empty() {
        issues.push("no app callback is registered");
    }
    Json(json!({"enabled":enabled,"signupMode":signup_mode,"redirectUris":redirects,"brokerCallbackUrl":callback,"ready":enabled&&issues.is_empty(),"issues":issues})).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleAuthRequest {
    redirect_uri: String,
    #[serde(default)]
    signup_mode: String,
}
async fn put_google_auth(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Json(request): Json<GoogleAuthRequest>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let redirect =
        match crate::native_auth::normalize_redirect_for_configuration(&request.redirect_uri) {
            Ok(value) => value,
            Err(message) => return error(StatusCode::BAD_REQUEST, message),
        };
    let signup = if request.signup_mode.is_empty() {
        "personal"
    } else {
        request.signup_mode.as_str()
    };
    if !matches!(signup, "personal" | "inviteOnly") {
        return error(
            StatusCode::BAD_REQUEST,
            "signupMode must be personal or inviteOnly",
        );
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let ok=sqlx::query("INSERT INTO gonvex_auth_providers(project_id,provider,enabled,signup_mode) VALUES($1,'google',TRUE,$2) ON CONFLICT(project_id,provider) DO UPDATE SET enabled=TRUE,signup_mode=EXCLUDED.signup_mode,updated_at=now()")
      .bind(&project).bind(signup).execute(&mut **transaction.transaction()).await.is_ok()
      &&sqlx::query("INSERT INTO gonvex_auth_redirect_uris(project_id,provider,redirect_uri) VALUES($1,'google',$2) ON CONFLICT DO NOTHING").bind(&project).bind(&redirect).execute(&mut **transaction.transaction()).await.is_ok()
      &&sqlx::query("UPDATE gonvex_runtime_projects SET auth_mode=CASE WHEN auth_mode IN('firebase','external-oidc') THEN 'hybrid' ELSE 'gonvex-native' END,updated_at=now() WHERE id=$1").bind(&project).execute(&mut **transaction.transaction()).await.is_ok()
      &&transaction.commit().await.is_ok();
    if ok {
        get_google_auth(State(runtime), Path(project), headers).await
    } else {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Google configuration could not be stored",
        )
    }
}

#[derive(Deserialize)]
struct GoogleDeleteQuery {
    #[serde(default)]
    redirect_uri: String,
}
async fn delete_google_auth(
    State(runtime): State<Runtime>,
    Path(project): Path<String>,
    headers: HeaderMap,
    Query(query): Query<GoogleDeleteQuery>,
) -> Response {
    if let Err(response) =
        authorize_project_resource(&runtime, &headers, &project, "projects:update", true).await
    {
        return response;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let result = if query.redirect_uri.trim().is_empty() {
        sqlx::query("UPDATE gonvex_auth_providers SET enabled=FALSE,updated_at=now() WHERE project_id=$1 AND provider='google'").bind(&project).execute(&mut **transaction.transaction()).await
    } else {
        let redirect =
            match crate::native_auth::normalize_redirect_for_configuration(&query.redirect_uri) {
                Ok(value) => value,
                Err(message) => return error(StatusCode::BAD_REQUEST, message),
            };
        sqlx::query("DELETE FROM gonvex_auth_redirect_uris WHERE project_id=$1 AND provider='google' AND redirect_uri=$2").bind(&project).bind(redirect).execute(&mut **transaction.transaction()).await
    };
    if result.is_ok() && transaction.commit().await.is_ok() {
        Json(json!({"ok":true})).into_response()
    } else {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Google configuration could not be updated",
        )
    }
}

fn provider_json(provider: &str, row: sqlx::postgres::PgRow) -> Value {
    json!({"provider":provider,"authMode":row.get::<String,_>("auth_mode"),"enabled":row.get::<bool,_>("enabled"),"signupMode":row.get::<String,_>("signup_mode"),"azureTenantId":row.get::<String,_>("azure_tenant_id"),"clientId":row.get::<String,_>("client_id"),"hasClientSecret":row.get::<bool,_>("has_client_secret"),"issuer":row.get::<String,_>("issuer"),"audience":row.get::<String,_>("audience"),"jwksUrl":row.get::<String,_>("jwks_url"),"firebaseProjectId":row.get::<String,_>("firebase_project_id"),"firebaseTenantId":row.get::<String,_>("firebase_tenant_id"),"hasAdminCredentials":row.get::<bool,_>("has_admin_credentials")})
}
fn supported_provider(value: &str) -> bool {
    matches!(
        value,
        "google" | "microsoft" | "apple" | "firebase" | "external-oidc"
    )
}
fn text_value(object: &serde_json::Map<String, Value>, key: &str) -> String {
    object
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned()
}
fn secure_external_url(value: &str) -> bool {
    Url::parse(value).ok().is_some_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
    })
}

#[derive(Deserialize)]
struct ErrorQuery {
    #[serde(default)]
    project: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    release: String,
    #[serde(default)]
    level: String,
}
async fn error_status(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<ErrorQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    let actor = match authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_access_project(&runtime, &actor, &project).await {
        return error(StatusCode::FORBIDDEN, "project access is required");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Control Plane is unavailable",
        );
    };
    let enabled = sqlx::query_scalar::<_, bool>(
        "SELECT error_tracking_enabled FROM gonvex_runtime_projects WHERE id=$1",
    )
    .bind(&project)
    .fetch_optional(&mut **transaction.transaction())
    .await
    .ok()
    .flatten()
    .unwrap_or(false);
    Json(json!({"enabled":enabled,"project":project})).into_response()
}
async fn error_groups(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<ErrorQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    let actor = match authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_access_project(&runtime, &actor, &project).await {
        return error(StatusCode::FORBIDDEN, "project access is required");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "error store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "error store is unavailable",
        );
    };
    let rows=sqlx::query("SELECT * FROM gonvex_error_groups WHERE project_id=$1 AND ($2='' OR status=$2) AND ($3='' OR level=$3) AND ($4='' OR releases ? $4) ORDER BY last_seen DESC LIMIT 500").bind(&project).bind(query.status.trim()).bind(normalize_error_level_filter(&query.level)).bind(query.release.trim()).fetch_all(&mut **transaction.transaction()).await;
    let releases=sqlx::query_scalar::<_,String>("SELECT release FROM gonvex_error_events WHERE project_id=$1 AND release<>'' GROUP BY release ORDER BY max(occurred_at) DESC,release DESC").bind(&project).fetch_all(&mut **transaction.transaction()).await.unwrap_or_default();
    match rows{Ok(rows)=>Json(json!({"groups":rows.into_iter().map(error_group_json).collect::<Vec<_>>(),"releases":releases})).into_response(),Err(_)=>error(StatusCode::SERVICE_UNAVAILABLE,"error store is unavailable")}
}
async fn error_group(
    State(runtime): State<Runtime>,
    Path(fingerprint): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ErrorQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    let actor = match authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_access_project(&runtime, &actor, &project).await {
        return error(StatusCode::FORBIDDEN, "project access is required");
    }
    match load_error_group(&runtime, &project, &fingerprint).await {
        Ok(Some(value)) => Json(value).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "error group not found"),
        Err(()) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "error store is unavailable",
        ),
    }
}
#[derive(Deserialize)]
struct ErrorGroupUpdate {
    #[serde(default)]
    status: String,
    #[serde(default)]
    priority: String,
    assignee: Option<String>,
}
async fn update_error_group(
    State(runtime): State<Runtime>,
    Path(fingerprint): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ErrorQuery>,
    Json(update): Json<ErrorGroupUpdate>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    let actor = match authorize(&runtime, &headers, "projects:update").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_manage_project(&runtime, &actor, &project).await {
        return error(
            StatusCode::FORBIDDEN,
            "project owner or admin access is required",
        );
    }
    if !update.status.is_empty()
        && !matches!(
            update.status.as_str(),
            "unresolved" | "resolved" | "ignored"
        )
    {
        return error(StatusCode::BAD_REQUEST, "status is invalid");
    }
    if !update.priority.is_empty()
        && !matches!(
            update.priority.as_str(),
            "low" | "medium" | "high" | "critical"
        )
    {
        return error(StatusCode::BAD_REQUEST, "priority is invalid");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "error store is unavailable",
        );
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "error store is unavailable",
        );
    };
    let row=sqlx::query("UPDATE gonvex_error_groups SET status=CASE WHEN $3='' THEN status ELSE $3 END,priority=CASE WHEN $4='' THEN priority ELSE $4 END,assignee=COALESCE($5,assignee) WHERE project_id=$1 AND fingerprint=$2 RETURNING *").bind(&project).bind(&fingerprint).bind(update.status).bind(update.priority).bind(update.assignee.map(|value|value.trim().to_owned())).fetch_optional(&mut **transaction.transaction()).await;
    match row {
        Ok(Some(row)) if transaction.commit().await.is_ok() => {
            Json(error_group_json(row)).into_response()
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "error group not found"),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "error store is unavailable",
        ),
    }
}
async fn error_bug_report(
    State(runtime): State<Runtime>,
    Path(fingerprint): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ErrorQuery>,
) -> Response {
    let project = project_header_or_query(&headers, &query.project);
    let actor = match authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !can_access_project(&runtime, &actor, &project).await {
        return error(StatusCode::FORBIDDEN, "project access is required");
    }
    let group = match load_error_group(&runtime, &project, &fingerprint).await {
        Ok(Some(value)) => value,
        Ok(None) => return error(StatusCode::NOT_FOUND, "error group not found"),
        Err(()) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "error store is unavailable",
            )
        }
    };
    let title = group
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Captured error");
    let count = group.get("count").and_then(Value::as_i64).unwrap_or(0);
    let latest = group.get("latest").cloned().unwrap_or(Value::Null);
    let culprit = group.get("culprit").and_then(Value::as_str).unwrap_or("");
    let stack = latest.get("stack").and_then(Value::as_str).unwrap_or("");
    let markdown=format!("## {title}\n\n**Fingerprint:** `{fingerprint}`\n**Impact:** {count} events\n**Likely source:** `{culprit}`\n\n### Error\n```\n{stack}\n```\n\n### Acceptance criteria\n- Reproduce or verify the failing path.\n- Add a regression test that fails before the fix.\n- Fix the root cause without suppressing unrelated errors.\n- Verify the fix against the affected release and tenant context.\n");
    Json(json!({"title":title,"markdown":markdown,"agentContext":{"fingerprint":fingerprint,"project":project,"tenantImpact":group.get("tenants"),"accountImpact":group.get("accounts"),"deviceImpact":group.get("devices"),"release":latest.get("release"),"culprit":culprit,"stack":stack,"breadcrumbs":latest.get("breadcrumbs"),"context":latest.get("context")}})).into_response()
}
async fn load_error_group(
    runtime: &Runtime,
    project: &str,
    fingerprint: &str,
) -> Result<Option<Value>, ()> {
    let control = runtime.inner.control_plane.read().await.clone().ok_or(())?;
    let mut transaction = control
        .begin_control_transaction(true)
        .await
        .map_err(|_| ())?;
    sqlx::query("SELECT * FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2")
        .bind(project)
        .bind(fingerprint)
        .fetch_optional(&mut **transaction.transaction())
        .await
        .map(|row| row.map(error_group_json))
        .map_err(|_| ())
}
fn error_group_json(row: sqlx::postgres::PgRow) -> Value {
    json!({"fingerprint":row.get::<String,_>("fingerprint"),"project":row.get::<String,_>("project_id"),"title":row.get::<String,_>("title"),"culprit":row.get::<String,_>("culprit"),"level":row.get::<String,_>("level"),"status":row.get::<String,_>("status"),"priority":row.get::<String,_>("priority"),"assignee":row.get::<String,_>("assignee"),"firstSeen":row.get::<DateTime<Utc>,_>("first_seen"),"lastSeen":row.get::<DateTime<Utc>,_>("last_seen"),"count":row.get::<i64,_>("event_count"),"tenants":row.get::<SqlJson<Value>,_>("tenants").0,"releases":row.get::<SqlJson<Value>,_>("releases").0,"environments":row.get::<SqlJson<Value>,_>("environments").0,"accounts":row.get::<SqlJson<Value>,_>("accounts").0,"devices":row.get::<SqlJson<Value>,_>("devices").0,"latest":row.get::<SqlJson<Value>,_>("latest_event").0,"regression":row.get::<bool,_>("regression")})
}
fn project_header_or_query(headers: &HeaderMap, query: &str) -> String {
    if query.trim().is_empty() {
        headers
            .get("x-gonvex-project-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .trim()
            .to_owned()
    } else {
        query.trim().to_owned()
    }
}
fn normalize_error_level_filter(value: &str) -> &str {
    match value.trim().to_ascii_lowercase().as_str() {
        "warn" | "warning" => "warning",
        "error" => "error",
        _ => "",
    }
}

pub(crate) async fn authorize_project_resource(
    runtime: &Runtime,
    headers: &HeaderMap,
    project: &str,
    permission: &str,
    manage: bool,
) -> Result<(), Response> {
    if project_key_matches(runtime, headers, project).await {
        return Ok(());
    }
    let actor = authorize(runtime, headers, permission).await?;
    let allowed = if manage {
        can_manage_project(runtime, &actor, project).await
    } else {
        can_access_project(runtime, &actor, project).await
    };
    if allowed {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "project access is required"))
    }
}

async fn project_key_matches(runtime: &Runtime, headers: &HeaderMap, project: &str) -> bool {
    let token = bearer(headers);
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return false;
    };
    control
        .project_accepts_sync_key(project, token, None)
        .await
        .unwrap_or(false)
}

fn project_json(row: sqlx::postgres::PgRow) -> Value {
    json!({
        "id":row.get::<String,_>("id"),"name":row.get::<String,_>("name"),"environment":row.get::<String,_>("environment"),
        "database":row.get::<String,_>("database_name"),"databaseMode":row.get::<String,_>("database_mode"),"storageBucket":row.get::<String,_>("storage_bucket"),
        "status":row.get::<String,_>("status"),"description":row.get::<String,_>("description"),"provisioned":row.get::<bool,_>("provisioned"),
        "runtimeCreated":row.get::<bool,_>("runtime_created"),"testTab":row.get::<bool,_>("test_tab"),"errorTrackingEnabled":row.get::<bool,_>("error_tracking_enabled"),"ownerEmail":row.get::<String,_>("owner_email"),
    })
}

fn valid_env_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        })
}
fn parse_dotenv(content: &str) -> std::collections::BTreeMap<String, String> {
    let mut result = std::collections::BTreeMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if valid_env_name(name) {
            let mut value = value.trim().to_owned();
            if value.len() >= 2
                && ((value.starts_with('"') && value.ends_with('"'))
                    || (value.starts_with('\'') && value.ends_with('\'')))
            {
                value = value[1..value.len() - 1].to_owned();
            }
            result.insert(name.to_owned(), value);
        }
    }
    result
}

pub(crate) async fn authenticate(
    runtime: &Runtime,
    headers: &HeaderMap,
) -> Result<OperatorActor, Response> {
    let token = bearer(headers);
    if let Some(actor) = verify_signed_session(runtime, token) {
        return Ok(actor);
    }
    if let Some(control) = runtime.inner.control_plane.read().await.clone() {
        if token.starts_with("gvx_session_") {
            if let Some(project) = runtime.inner.config.dashboard_auth_project_id.as_deref() {
                if let Ok(identity) = control.load_session_identity(token, Some(project)).await {
                    if identity.account.email_verified {
                        if let Some(actor) = actor_for_email(
                            runtime,
                            &identity.account.email,
                            Credential::NativeSession,
                        )
                        .await
                        {
                            return Ok(actor);
                        }
                    }
                }
            }
        }
        if token.starts_with("gvx_pat_") {
            if let Some(actor) = verify_personal_token(runtime, token).await {
                return Ok(actor);
            }
        }
    }
    if runtime
        .inner
        .config
        .admin_key
        .as_deref()
        .is_some_and(|key| constant_time_eq(token, key))
    {
        return Ok(OperatorActor {
            email: "admin@gonvex.local".to_owned(),
            name: "Gonvex Admin".to_owned(),
            role: "admin".to_owned(),
            credential: Credential::AdminKey,
            permissions: vec!["*".to_owned()],
        });
    }
    if dashboard_auth_optional(runtime) {
        return Ok(OperatorActor {
            email: "local@gonvex.dev".to_owned(),
            name: "Local Developer".to_owned(),
            role: "admin".to_owned(),
            credential: Credential::Local,
            permissions: vec!["*".to_owned()],
        });
    }
    Err(error(
        StatusCode::UNAUTHORIZED,
        "dashboard sign-in or personal access token is required",
    ))
}

pub(crate) async fn authorize(
    runtime: &Runtime,
    headers: &HeaderMap,
    permission: &str,
) -> Result<OperatorActor, Response> {
    let actor = authenticate(runtime, headers).await?;
    if !permission.is_empty() && !actor.has(permission) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error":"personal access token does not grant the required permission",
                "permission":permission,
            })),
        )
            .into_response());
    }
    Ok(actor)
}

pub(crate) async fn can_access_project(
    runtime: &Runtime,
    actor: &OperatorActor,
    project: &str,
) -> bool {
    if dashboard_auth_optional(runtime) || actor.global_admin() {
        return true;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return false;
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return false;
    };
    sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(
             SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND (lower(owner_email)=lower($2) OR (owner_email='' AND $3='admin'))
             UNION ALL SELECT 1 FROM gonvex_project_members WHERE project_id=$1 AND lower(email)=lower($2)
           )"#,
    )
    .bind(project)
    .bind(&actor.email)
    .bind(&actor.role)
    .fetch_one(&mut **transaction.transaction())
    .await
    .unwrap_or(false)
}

pub(crate) async fn can_manage_project(
    runtime: &Runtime,
    actor: &OperatorActor,
    project: &str,
) -> bool {
    if dashboard_auth_optional(runtime) || actor.global_admin() {
        return true;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return false;
    };
    let Ok(mut transaction) = control.begin_control_transaction(true).await else {
        return false;
    };
    sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(
             SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND lower(owner_email)=lower($2)
             UNION ALL SELECT 1 FROM gonvex_project_members
             WHERE project_id=$1 AND lower(email)=lower($2) AND role IN('owner','admin')
           )"#,
    )
    .bind(project)
    .bind(&actor.email)
    .fetch_one(&mut **transaction.transaction())
    .await
    .unwrap_or(false)
}

pub(crate) fn admin_key_matches(runtime: &Runtime, headers: &HeaderMap) -> bool {
    let token = bearer(headers);
    runtime
        .inner
        .config
        .admin_key
        .as_deref()
        .is_some_and(|key| constant_time_eq(token, key))
}

async fn actor_for_email(
    runtime: &Runtime,
    email: &str,
    credential: Credential,
) -> Option<OperatorActor> {
    let email = normalize_email(email);
    let control = runtime.inner.control_plane.read().await.clone()?;
    let mut transaction = control.begin_control_transaction(true).await.ok()?;
    if let Some(row) =
        sqlx::query("SELECT email,name,role FROM gonvex_dashboard_accounts WHERE email=$1")
            .bind(&email)
            .fetch_optional(&mut **transaction.transaction())
            .await
            .ok()?
    {
        return Some(OperatorActor {
            email: row.get("email"),
            name: row.get("name"),
            role: row.get("role"),
            credential,
            permissions: vec!["*".to_owned()],
        });
    }
    runtime
        .inner
        .config
        .dashboard_account
        .as_deref()
        .filter(|value| *value == email)
        .map(|_| OperatorActor {
            email: email.clone(),
            name: display_name(&email),
            role: "admin".to_owned(),
            credential,
            permissions: vec!["*".to_owned()],
        })
}

async fn verify_personal_token(runtime: &Runtime, token: &str) -> Option<OperatorActor> {
    let id = token.split_once('.')?.0.strip_prefix("gvx_")?;
    if !id.starts_with("pat_") {
        return None;
    }
    let control = runtime.inner.control_plane.read().await.clone()?;
    let mut transaction = control.begin_control_transaction(false).await.ok()?;
    let row = sqlx::query(
        r#"SELECT owner_email,permissions FROM gonvex_account_access_tokens
           WHERE id=$1 AND token_hash=$2 AND revoked_at IS NULL
             AND (expires_at IS NULL OR expires_at>now()) FOR UPDATE"#,
    )
    .bind(id)
    .bind(sha256_hex(token.as_bytes()))
    .fetch_optional(&mut **transaction.transaction())
    .await
    .ok()??;
    sqlx::query("UPDATE gonvex_account_access_tokens SET last_used_at=now() WHERE id=$1")
        .bind(id)
        .execute(&mut **transaction.transaction())
        .await
        .ok()?;
    let email: String = row.get("owner_email");
    let permissions = row.get::<SqlJson<Vec<String>>, _>("permissions").0;
    let actor_row =
        sqlx::query("SELECT email,name,role FROM gonvex_dashboard_accounts WHERE email=$1")
            .bind(&email)
            .fetch_optional(&mut **transaction.transaction())
            .await
            .ok()??;
    transaction.commit().await.ok()?;
    Some(OperatorActor {
        email: actor_row.get("email"),
        name: actor_row.get("name"),
        role: actor_row.get("role"),
        credential: Credential::PersonalAccessToken,
        permissions,
    })
}

fn verify_signed_session(runtime: &Runtime, token: &str) -> Option<OperatorActor> {
    let secret = dashboard_secret(runtime)?;
    let (payload, signature) = token.split_once('.')?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(payload.as_bytes());
    let expected = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    if !constant_time_eq(signature, &expected) {
        return None;
    }
    let session: SignedSession =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    if session.expires_at <= Utc::now().timestamp_millis()
        || normalize_email(&session.email).is_empty()
    {
        return None;
    }
    Some(OperatorActor {
        email: normalize_email(&session.email),
        name: session.name,
        role: normalize_role(&session.role).to_owned(),
        credential: Credential::Session,
        permissions: vec!["*".to_owned()],
    })
}

fn sign_session(runtime: &Runtime, session: &SignedSession) -> Result<String, &'static str> {
    let secret = dashboard_secret(runtime).ok_or("dashboard session secret is not configured")?;
    let mut unsigned = session.clone();
    unsigned.access_token.clear();
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&unsigned).map_err(|_| "dashboard session could not be encoded")?,
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| "dashboard session secret is invalid")?;
    mac.update(payload.as_bytes());
    Ok(format!(
        "{payload}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    ))
}

fn dashboard_secret(runtime: &Runtime) -> Option<&str> {
    runtime
        .inner
        .config
        .control_secret
        .as_deref()
        .or(runtime.inner.config.admin_key.as_deref())
}

fn dashboard_auth_optional(runtime: &Runtime) -> bool {
    !runtime.inner.config.require_auth
        && runtime.inner.config.dashboard_auth_project_id.is_none()
        && dashboard_secret(runtime).is_none()
        && runtime.inner.config.dashboard_account.is_none()
}

fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .trim()
        .strip_prefix("Bearer ")
        .unwrap_or_else(|| {
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .trim()
        })
}

fn normalize_permissions(
    actor: &OperatorActor,
    requested: &[String],
) -> Result<Vec<String>, &'static str> {
    let mut values = if requested.is_empty() {
        vec![
            "projects:read".to_owned(),
            "projects:create".to_owned(),
            "projects:keys:read".to_owned(),
        ]
    } else {
        requested
            .iter()
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty())
            .collect()
    };
    values.sort();
    values.dedup();
    const ALLOWED: &[&str] = &[
        "*",
        "projects:*",
        "projects:read",
        "projects:create",
        "projects:update",
        "projects:delete",
        "projects:keys:read",
        "projects:keys:write",
        "projects:members:read",
        "projects:members:write",
        "projects:env:read",
        "projects:env:write",
        "admin:projects",
        "tokens:*",
        "tokens:read",
        "tokens:create",
        "tokens:revoke",
    ];
    if values.is_empty()
        || values
            .iter()
            .any(|value| !ALLOWED.contains(&value.as_str()))
    {
        return Err("token permissions are invalid");
    }
    if values
        .iter()
        .any(|value| matches!(value.as_str(), "*" | "admin:projects") && !actor.global_admin())
    {
        return Err("current credential cannot grant global administration");
    }
    if actor.credential == Credential::PersonalAccessToken
        && values.iter().any(|value| !actor.has(value))
    {
        return Err("current credential cannot grant the requested permission");
    }
    Ok(values)
}

fn token_json(row: sqlx::postgres::PgRow) -> Value {
    json!({
        "id":row.get::<String,_>("id"),"name":row.get::<String,_>("name"),
        "prefix":row.get::<String,_>("token_prefix"),
        "permissions":row.get::<SqlJson<Vec<String>>,_>("permissions").0,
        "createdAt":row.get::<DateTime<Utc>,_>("created_at"),
        "expiresAt":row.get::<Option<DateTime<Utc>>,_>("expires_at"),
        "lastUsedAt":row.get::<Option<DateTime<Utc>>,_>("last_used_at"),
        "revokedAt":row.get::<Option<DateTime<Utc>>,_>("revoked_at"),
    })
}

fn hash_password(password: &str) -> String {
    let mut salt = [0_u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    let mut hash = [0_u8; 32];
    pbkdf2_hmac::<Sha256>(
        password.as_bytes(),
        &salt,
        DASHBOARD_PASSWORD_ROUNDS,
        &mut hash,
    );
    format!(
        "pbkdf2_sha256${DASHBOARD_PASSWORD_ROUNDS}${}${}",
        URL_SAFE_NO_PAD.encode(salt),
        URL_SAFE_NO_PAD.encode(hash)
    )
}

fn verify_password(password: &str, encoded: &str) -> bool {
    let parts = encoded.split('$').collect::<Vec<_>>();
    let Ok(rounds) = parts.get(1).unwrap_or(&"").parse::<u32>() else {
        return false;
    };
    if parts.len() != 4 || parts[0] != "pbkdf2_sha256" || rounds == 0 {
        return false;
    }
    let (Ok(salt), Ok(expected)) = (
        URL_SAFE_NO_PAD.decode(parts[2]),
        URL_SAFE_NO_PAD.decode(parts[3]),
    ) else {
        return false;
    };
    let mut actual = vec![0_u8; expected.len()];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, rounds, &mut actual);
    actual.len() == expected.len() && actual.ct_eq(&expected).into()
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}
fn display_name(email: &str) -> String {
    email
        .split('@')
        .next()
        .unwrap_or(email)
        .replace(['.', '_', '-'], " ")
}
fn normalize_role(value: &str) -> &str {
    if value == "admin" {
        "admin"
    } else {
        "standard"
    }
}
fn random_secret() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
fn project_key(project_id: &str) -> String {
    format!(
        "gvx_{}.{}",
        URL_SAFE_NO_PAD.encode(project_id.trim().as_bytes()),
        random_secret()
    )
}
fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn constant_time_eq(left: &str, right: &str) -> bool {
    left.len() == right.len() && left.as_bytes().ct_eq(right.as_bytes()).into()
}
fn credential_name(value: &Credential) -> &'static str {
    match value {
        Credential::Session => "session",
        Credential::NativeSession => "nativeSession",
        Credential::PersonalAccessToken => "personalAccessToken",
        Credential::AdminKey => "adminKey",
        Credential::Local => "local",
    }
}
fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error":message.into()}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hashes_match_the_existing_dashboard_contract() {
        let encoded = hash_password("correct horse battery staple");
        assert!(verify_password("correct horse battery staple", &encoded));
        assert!(!verify_password("wrong", &encoded));
    }

    #[test]
    fn pat_permission_wildcards_are_bounded() {
        let actor = OperatorActor {
            email: "a@example.test".to_owned(),
            name: "A".to_owned(),
            role: "standard".to_owned(),
            credential: Credential::PersonalAccessToken,
            permissions: vec!["projects:*".to_owned()],
        };
        assert!(actor.has("projects:read"));
        assert!(!actor.has("tokens:create"));
        assert!(!actor.global_admin());
    }

    #[test]
    fn project_keys_embed_the_project_id_for_cli_validation() {
        let id = "4985bbc5-74e7-4c82-b3aa-fbadc49c8090";
        let key = project_key(id);
        let encoded = key
            .strip_prefix("gvx_")
            .and_then(|value| value.split_once('.'))
            .map(|(project, _)| project)
            .expect("versioned project key");
        assert_eq!(URL_SAFE_NO_PAD.decode(encoded).unwrap(), id.as_bytes());
    }
}
