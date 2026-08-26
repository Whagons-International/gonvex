//! Browser-facing OAuth compatibility for Gonvex-native project auth.
//!
//! Firebase and generic external JWT projects authenticate through
//! `control.auth.exchangeExternalToken` on the persistent protocol. These
//! routes preserve the published 0.4.1 PKCE contract for projects that opt in
//! to Gonvex-native Google, Microsoft, or Apple authentication.

use std::collections::BTreeMap;

use axum::extract::{Form, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::Utc;
use gonvex_postgres::TenantTransaction;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use subtle::ConstantTimeEq;
use url::Url;

use crate::control::{
    decrypt_control_secret, issue_session, load_auth_account, refresh_session,
    resolve_external_account, session_result_from_directory, AuthAccount, ControlError,
    RefreshSessionResult, SessionGrant,
};
use crate::external_auth::{verify_external_token, ExternalAuthConfiguration};
use crate::Runtime;

const TRANSACTION_MINUTES: i64 = 10;
const CODE_MINUTES: i64 = 5;

pub fn router() -> Router<Runtime> {
    Router::new()
        .route("/auth/config", get(auth_config))
        .route("/auth/{provider}/authorize", get(authorize))
        .route(
            "/auth/{provider}/callback",
            get(callback_get).post(callback_post),
        )
        .route("/auth/token", post(token))
        .route("/auth/logout", post(logout))
}

#[derive(Deserialize)]
struct ProjectQuery {
    project: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicConfiguration {
    project: String,
    providers: Vec<String>,
    database_mode: String,
    signup_mode: String,
    access_token_lifetime_seconds: i64,
    refresh_token_lifetime_seconds: i64,
}

async fn auth_config(
    State(runtime): State<Runtime>,
    Query(query): Query<ProjectQuery>,
) -> Response {
    let project = query.project.trim();
    if project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "project is required");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth configuration is unavailable",
        );
    };
    let mut transaction = match control.begin_control_transaction(true).await {
        Ok(transaction) => transaction,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth configuration is unavailable",
            )
        }
    };
    let project_row = sqlx::query(
        "SELECT database_mode,COALESCE(NULLIF(auth_mode,''),'gonvex-native') AS auth_mode FROM gonvex_runtime_projects WHERE id=$1 AND status='active'",
    )
    .bind(project)
    .fetch_optional(&mut **transaction.transaction())
    .await;
    let Ok(Some(project_row)) = project_row else {
        return error(StatusCode::NOT_FOUND, "project is unavailable");
    };
    let auth_mode: String = project_row.get("auth_mode");
    let rows = sqlx::query(
        r#"SELECT provider,signup_mode FROM gonvex_auth_providers
           WHERE project_id=$1 AND enabled=TRUE ORDER BY provider"#,
    )
    .bind(project)
    .fetch_all(&mut **transaction.transaction())
    .await;
    let Ok(rows) = rows else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth configuration is unavailable",
        );
    };
    let native_enabled = matches!(auth_mode.as_str(), "gonvex-native" | "hybrid");
    let providers = if native_enabled {
        rows.iter()
            .filter_map(|row| {
                let provider = row.get::<String, _>("provider");
                matches!(provider.as_str(), "google" | "microsoft" | "apple").then_some(provider)
            })
            .collect()
    } else {
        Vec::new()
    };
    let signup_mode = rows
        .first()
        .map(|row| row.get("signup_mode"))
        .unwrap_or_else(|| "personal".to_owned());
    Json(PublicConfiguration {
        project: project.to_owned(),
        providers,
        database_mode: project_row.get("database_mode"),
        signup_mode,
        access_token_lifetime_seconds: 15 * 60,
        refresh_token_lifetime_seconds: 30 * 24 * 60 * 60,
    })
    .into_response()
}

#[derive(Deserialize)]
struct AuthorizeQuery {
    project: String,
    redirect_uri: String,
    state: String,
    code_challenge: String,
    code_challenge_method: String,
}

async fn authorize(
    State(runtime): State<Runtime>,
    Path(provider): Path<String>,
    Query(query): Query<AuthorizeQuery>,
) -> Response {
    if !matches!(provider.as_str(), "google" | "microsoft" | "apple") {
        return error(
            StatusCode::NOT_FOUND,
            "authentication provider is unsupported",
        );
    }
    if query.state.len() < 16
        || query.state.len() > 512
        || query.code_challenge_method != "S256"
        || !valid_pkce(&query.code_challenge)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "project, state, and PKCE S256 are required",
        );
    }
    let redirect_uri = match normalize_redirect(&query.redirect_uri) {
        Ok(value) => value,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let callback_uri = match callback_uri(&runtime, &provider) {
        Ok(value) => value,
        Err(message) => return error(StatusCode::SERVICE_UNAVAILABLE, message),
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth configuration is unavailable",
        );
    };
    let mut transaction = match control.begin_control_transaction(false).await {
        Ok(transaction) => transaction,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth configuration is unavailable",
            )
        }
    };
    let configuration = match native_provider_configuration(
        &runtime,
        &mut transaction,
        query.project.trim(),
        &provider,
        &redirect_uri,
    )
    .await
    {
        Ok(configuration) => configuration,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let state_token = secure_token("oauth");
    let nonce = secure_token("nonce");
    if sqlx::query(
        r#"INSERT INTO gonvex_auth_transactions
           (token_hash,project_id,redirect_uri,app_state,code_challenge,nonce,
            google_redirect_uri,provider,expires_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)"#,
    )
    .bind(sha256_hex(state_token.as_bytes()))
    .bind(query.project.trim())
    .bind(&redirect_uri)
    .bind(query.state.trim())
    .bind(query.code_challenge.trim())
    .bind(&nonce)
    .bind(&callback_uri)
    .bind(&provider)
    .bind(Utc::now() + chrono::Duration::minutes(TRANSACTION_MINUTES))
    .execute(&mut **transaction.transaction())
    .await
    .is_err()
        || transaction.commit().await.is_err()
    {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not start auth flow",
        );
    }
    let mut target = match Url::parse(&configuration.authorize_url) {
        Ok(value) => value,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "provider configuration is invalid",
            )
        }
    };
    {
        let mut params = target.query_pairs_mut();
        params.append_pair("client_id", &configuration.client_id);
        params.append_pair("redirect_uri", &callback_uri);
        params.append_pair("response_type", "code");
        params.append_pair("scope", configuration.scope());
        params.append_pair("state", &state_token);
        params.append_pair("nonce", &nonce);
        if provider == "microsoft" {
            params.append_pair("response_mode", "query");
        } else if provider == "apple" {
            params.append_pair("response_mode", "form_post");
        }
    }
    Redirect::temporary(target.as_str()).into_response()
}

async fn callback_get(
    State(runtime): State<Runtime>,
    Path(provider): Path<String>,
    Query(values): Query<BTreeMap<String, String>>,
) -> Response {
    finish_callback(runtime, provider, values).await
}

async fn callback_post(
    State(runtime): State<Runtime>,
    Path(provider): Path<String>,
    Form(values): Form<BTreeMap<String, String>>,
) -> Response {
    finish_callback(runtime, provider, values).await
}

async fn finish_callback(
    runtime: Runtime,
    provider: String,
    values: BTreeMap<String, String>,
) -> Response {
    let state = values.get("state").map(String::as_str).unwrap_or("").trim();
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth service is unavailable",
        );
    };
    let mut transaction = match control.begin_control_transaction(false).await {
        Ok(transaction) => transaction,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth service is unavailable",
            )
        }
    };
    let row = sqlx::query(
        r#"DELETE FROM gonvex_auth_transactions
           WHERE token_hash=$1 AND expires_at>now()
           RETURNING project_id,redirect_uri,app_state,code_challenge,nonce,
                     google_redirect_uri,provider"#,
    )
    .bind(sha256_hex(state.as_bytes()))
    .fetch_optional(&mut **transaction.transaction())
    .await;
    let Ok(Some(row)) = row else {
        return error(StatusCode::BAD_REQUEST, "invalid or expired OAuth state");
    };
    let redirect_uri: String = row.get("redirect_uri");
    let app_state: String = row.get("app_state");
    let stored_provider: String = row.get("provider");
    if stored_provider != provider {
        return redirect_error(&redirect_uri, &app_state, "provider_mismatch");
    }
    if let Some(provider_error) = values.get("error").filter(|value| !value.is_empty()) {
        let _ = transaction.commit().await;
        return redirect_error(&redirect_uri, &app_state, provider_error);
    }
    let project: String = row.get("project_id");
    if transaction.commit().await.is_err() {
        return redirect_error(&redirect_uri, &app_state, "auth_state_consumption_failed");
    }
    let mut transaction = match control.begin_control_transaction(false).await {
        Ok(transaction) => transaction,
        Err(_) => return redirect_error(&redirect_uri, &app_state, "auth_service_unavailable"),
    };
    let configuration = match native_provider_configuration(
        &runtime,
        &mut transaction,
        &project,
        &provider,
        &redirect_uri,
    )
    .await
    {
        Ok(configuration) => configuration,
        Err(_) => return redirect_error(&redirect_uri, &app_state, "provider_not_configured"),
    };
    let code = values.get("code").map(String::as_str).unwrap_or("").trim();
    let id_token = match exchange_provider_code(
        &configuration,
        code,
        &row.get::<String, _>("google_redirect_uri"),
    )
    .await
    {
        Ok(token) => token,
        Err(_) => return redirect_error(&redirect_uri, &app_state, "provider_exchange_failed"),
    };
    let external = configuration.external(&provider);
    let mut identity = match verify_external_token(&external, &id_token).await {
        Ok(identity) => identity,
        Err(_) => return redirect_error(&redirect_uri, &app_state, "invalid_provider_identity"),
    };
    let nonce: String = row.get("nonce");
    if !token_nonce_matches(&id_token, &nonce) {
        return redirect_error(&redirect_uri, &app_state, "invalid_provider_identity");
    }
    if provider == "microsoft" && !identity.email.is_empty() {
        identity.email_verified = true;
    }
    let account = match resolve_external_account(&mut transaction, &project, &identity).await {
        Ok(account) => account,
        Err(_) => return redirect_error(&redirect_uri, &app_state, "account_creation_failed"),
    };
    let auth_code = secure_token("authcode");
    let inserted = sqlx::query(
        r#"INSERT INTO gonvex_auth_codes
           (code_hash,project_id,account_id,redirect_uri,code_challenge,expires_at)
           VALUES($1,$2,$3,$4,$5,$6)"#,
    )
    .bind(sha256_hex(auth_code.as_bytes()))
    .bind(&project)
    .bind(&account.id)
    .bind(&redirect_uri)
    .bind(row.get::<String, _>("code_challenge"))
    .bind(Utc::now() + chrono::Duration::minutes(CODE_MINUTES))
    .execute(&mut **transaction.transaction())
    .await;
    if inserted.is_err() || transaction.commit().await.is_err() {
        return redirect_error(&redirect_uri, &app_state, "code_creation_failed");
    }
    redirect_values(
        &redirect_uri,
        &[("code", &auth_code), ("state", &app_state)],
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenRequest {
    grant_type: String,
    project: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    code_verifier: String,
    #[serde(default)]
    redirect_uri: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    tenant: String,
}

async fn token(State(runtime): State<Runtime>, Json(request): Json<TokenRequest>) -> Response {
    let project = request.project.trim();
    if project.is_empty() {
        return error(StatusCode::BAD_REQUEST, "project is required");
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth session service is unavailable",
        );
    };
    let mut transaction = match control.begin_control_transaction(false).await {
        Ok(transaction) => transaction,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth session service is unavailable",
            )
        }
    };
    let result = match request.grant_type.as_str() {
        "authorization_code" => exchange_code(
            &mut transaction,
            project,
            request.code.trim(),
            request.code_verifier.trim(),
            request.redirect_uri.trim(),
        )
        .await
        .map(|(grant, account)| (grant, account, request.tenant.trim().to_owned())),
        "refresh_token" => {
            match refresh_session(&mut transaction, project, request.refresh_token.trim()).await {
                Ok(RefreshSessionResult::Refreshed(session)) => Ok(*session),
                Ok(RefreshSessionResult::ReuseRevoked) => {
                    if transaction.commit().await.is_err() {
                        return error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "auth session service is unavailable",
                        );
                    }
                    return error(
                        StatusCode::UNAUTHORIZED,
                        "invalid or expired authentication grant",
                    );
                }
                Err(error) => Err(error),
            }
        }
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "grantType must be authorization_code or refresh_token",
            )
        }
    };
    let (grant, account, requested_tenant) = match result {
        Ok(result) => result,
        Err(_) => {
            return error(
                StatusCode::UNAUTHORIZED,
                "invalid or expired authentication grant",
            )
        }
    };
    let response = match session_result_from_directory(
        &control,
        &mut transaction,
        project,
        &grant,
        &account,
        &requested_tenant,
    )
    .await
    {
        Ok(response) => response,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "tenant memberships are unavailable",
            )
        }
    };
    if transaction.commit().await.is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth session service is unavailable",
        );
    }
    Json(response).into_response()
}

async fn exchange_code(
    transaction: &mut TenantTransaction,
    project: &str,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> Result<(SessionGrant, AuthAccount), ControlError> {
    if code.is_empty() || !valid_pkce(verifier) {
        return Err(ControlError::InvalidArguments(
            "invalid authorization code".to_owned(),
        ));
    }
    let redirect_uri = normalize_redirect(redirect_uri)
        .map_err(|message| ControlError::InvalidArguments(message.to_owned()))?;
    let row = sqlx::query(
        r#"SELECT project_id,account_id,redirect_uri,code_challenge
           FROM gonvex_auth_codes
           WHERE code_hash=$1 AND used_at IS NULL AND expires_at>now() FOR UPDATE"#,
    )
    .bind(sha256_hex(code.as_bytes()))
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or_else(|| ControlError::InvalidArguments("invalid authorization code".to_owned()))?;
    let challenge = pkce_challenge(verifier);
    if row.get::<String, _>("project_id") != project
        || row.get::<String, _>("redirect_uri") != redirect_uri
        || !constant_time_eq(&row.get::<String, _>("code_challenge"), &challenge)
    {
        return Err(ControlError::InvalidArguments(
            "authorization code does not match this client".to_owned(),
        ));
    }
    sqlx::query("UPDATE gonvex_auth_codes SET used_at=now() WHERE code_hash=$1")
        .bind(sha256_hex(code.as_bytes()))
        .execute(&mut **transaction.transaction())
        .await?;
    let account_id: String = row.get("account_id");
    let account = load_auth_account(transaction, project, &account_id).await?;
    let grant = issue_session(
        transaction,
        project,
        &account_id,
        &format!("family_{}", uuid::Uuid::new_v4()),
        Utc::now() + chrono::Duration::days(30),
    )
    .await?;
    Ok((grant, account))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogoutRequest {
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    all: bool,
}

async fn logout(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(request): Json<LogoutRequest>,
) -> Response {
    let access = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("")
        .trim();
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return Json(json!({"ok":true})).into_response();
    };
    let Ok(mut transaction) = control.begin_control_transaction(false).await else {
        return Json(json!({"ok":true})).into_response();
    };
    let family = sqlx::query_scalar::<_, String>(
        r#"SELECT family_id FROM gonvex_auth_sessions WHERE token_hash=$1
           UNION SELECT family_id FROM gonvex_auth_refresh_tokens WHERE token_hash=$2 LIMIT 1"#,
    )
    .bind(sha256_hex(access.as_bytes()))
    .bind(sha256_hex(request.refresh_token.as_bytes()))
    .fetch_optional(&mut **transaction.transaction())
    .await
    .ok()
    .flatten();
    if let Some(family) = family {
        if request.all {
            if let Ok(Some((project, account))) = sqlx::query_as::<_, (String, String)>(
                "SELECT project_id,account_id FROM gonvex_auth_sessions WHERE family_id=$1 LIMIT 1",
            )
            .bind(&family)
            .fetch_optional(&mut **transaction.transaction())
            .await
            {
                let _ = sqlx::query("UPDATE gonvex_auth_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2")
                    .bind(&project).bind(&account).execute(&mut **transaction.transaction()).await;
                let _ = sqlx::query("UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE project_id=$1 AND account_id=$2")
                    .bind(&project).bind(&account).execute(&mut **transaction.transaction()).await;
            }
        } else {
            let _ = crate::control::revoke_family(&mut transaction, &family).await;
        }
    }
    let _ = transaction.commit().await;
    Json(json!({"ok":true})).into_response()
}

struct NativeProviderConfiguration {
    provider: String,
    authorize_url: String,
    token_url: String,
    issuer: String,
    client_id: String,
    audience: String,
    jwks_url: String,
    client_secret: String,
}

impl NativeProviderConfiguration {
    fn scope(&self) -> &'static str {
        if self.provider == "apple" {
            "name email"
        } else {
            "openid email profile"
        }
    }

    fn external(&self, provider: &str) -> ExternalAuthConfiguration {
        ExternalAuthConfiguration {
            provider: provider.to_owned(),
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            jwks_url: self.jwks_url.clone(),
            firebase_tenant_id: String::new(),
            signup_mode: "personal".to_owned(),
            firebase_project_id: String::new(),
            firebase_admin_credentials: None,
        }
    }
}

async fn native_provider_configuration(
    runtime: &Runtime,
    transaction: &mut TenantTransaction,
    project: &str,
    provider: &str,
    redirect_uri: &str,
) -> Result<NativeProviderConfiguration, &'static str> {
    let row = sqlx::query(
        r#"SELECT p.azure_tenant_id,p.client_id,p.client_secret_encrypted,
                  p.issuer,p.audience,p.jwks_url
           FROM gonvex_auth_providers p
           JOIN gonvex_auth_redirect_uris redirect
             ON redirect.project_id=p.project_id AND redirect.provider=p.provider
           JOIN gonvex_runtime_projects project ON project.id=p.project_id
           WHERE p.project_id=$1 AND p.provider=$2 AND p.enabled=TRUE
             AND redirect.redirect_uri=$3
             AND COALESCE(NULLIF(project.auth_mode,''),'gonvex-native') IN ('gonvex-native','hybrid')"#,
    )
    .bind(project)
    .bind(provider)
    .bind(redirect_uri)
    .fetch_optional(&mut **transaction.transaction())
    .await
    .map_err(|_| "auth configuration is unavailable")?
    .ok_or("redirect URI is not registered for this project")?;
    let tenant: String = row.get("azure_tenant_id");
    let mut client_id = row.get::<String, _>("client_id");
    let encrypted = row.get::<Option<Vec<u8>>, _>("client_secret_encrypted");
    if provider == "google" && client_id.is_empty() {
        client_id = runtime
            .inner
            .config
            .google_client_id
            .clone()
            .unwrap_or_default();
    }
    let configured_secret = if let Some(encrypted) = encrypted.as_deref() {
        decrypt_control_secret(runtime, encrypted).ok()
    } else if provider == "google" {
        runtime.inner.config.google_client_secret.clone()
    } else {
        None
    };
    if client_id.is_empty() || configured_secret.is_none() {
        return Err("authentication provider is not fully configured");
    }
    let secret = configured_secret.ok_or("authentication provider credentials are unavailable")?;
    let (authorize_url, token_url, default_issuer, default_jwks) = match provider {
        "google" => (
            "https://accounts.google.com/o/oauth2/v2/auth".to_owned(),
            "https://oauth2.googleapis.com/token".to_owned(),
            "https://accounts.google.com".to_owned(),
            "https://www.googleapis.com/oauth2/v3/certs".to_owned(),
        ),
        "microsoft" if !tenant.is_empty() => (
            format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize"),
            format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"),
            format!("https://login.microsoftonline.com/{tenant}/v2.0"),
            format!("https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys"),
        ),
        "apple" => (
            "https://appleid.apple.com/auth/authorize".to_owned(),
            "https://appleid.apple.com/auth/token".to_owned(),
            "https://appleid.apple.com".to_owned(),
            "https://appleid.apple.com/auth/keys".to_owned(),
        ),
        _ => return Err("authentication provider configuration is incomplete"),
    };
    Ok(NativeProviderConfiguration {
        provider: provider.to_owned(),
        authorize_url,
        token_url,
        issuer: nonempty(row.get("issuer"), default_issuer),
        client_id: client_id.clone(),
        audience: nonempty(row.get("audience"), client_id),
        jwks_url: nonempty(row.get("jwks_url"), default_jwks),
        client_secret: secret,
    })
}

async fn exchange_provider_code(
    configuration: &NativeProviderConfiguration,
    code: &str,
    redirect_uri: &str,
) -> Result<String, ()> {
    if code.is_empty() {
        return Err(());
    }
    let response = reqwest::Client::new()
        .post(&configuration.token_url)
        .timeout(std::time::Duration::from_secs(15))
        .form(&[
            ("code", code),
            ("client_id", configuration.client_id.as_str()),
            ("client_secret", configuration.client_secret.as_str()),
            ("redirect_uri", redirect_uri),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .map_err(|_| ())?;
    if !response.status().is_success() {
        return Err(());
    }
    response
        .json::<Value>()
        .await
        .ok()
        .and_then(|value| {
            value
                .get("id_token")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|token| !token.is_empty())
        .ok_or(())
}

fn callback_uri(runtime: &Runtime, provider: &str) -> Result<String, &'static str> {
    let base = runtime
        .inner
        .config
        .auth_public_url
        .as_deref()
        .ok_or("GONVEX_AUTH_URL is required")?;
    let parsed = Url::parse(base).map_err(|_| "GONVEX_AUTH_URL is invalid")?;
    let local = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
    if parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || (!local && parsed.scheme() != "https")
        || (local && !matches!(parsed.scheme(), "http" | "https"))
    {
        return Err("GONVEX_AUTH_URL is invalid");
    }
    Ok(format!(
        "{}/auth/{provider}/callback",
        base.trim_end_matches('/')
    ))
}

fn normalize_redirect(raw: &str) -> Result<String, &'static str> {
    if raw.len() > 2_048 {
        return Err("redirect URI is too long");
    }
    let parsed = Url::parse(raw.trim()).map_err(|_| "redirect URI must be absolute")?;
    let local = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
    if parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || (!local && parsed.scheme() != "https")
        || (local && !matches!(parsed.scheme(), "http" | "https"))
    {
        return Err("redirect URI must use https without credentials or a fragment");
    }
    Ok(parsed.to_string())
}

pub(crate) fn normalize_redirect_for_configuration(raw: &str) -> Result<String, &'static str> {
    normalize_redirect(raw)
}

fn token_nonce_matches(token: &str, expected: &str) -> bool {
    let Some(payload) = token.split('.').nth(1) else {
        return false;
    };
    let Ok(decoded) = URL_SAFE_NO_PAD.decode(payload) else {
        return false;
    };
    let Ok(value) = serde_json::from_slice::<Value>(&decoded) else {
        return false;
    };
    value
        .get("nonce")
        .and_then(Value::as_str)
        .is_some_and(|actual| constant_time_eq(actual, expected))
}

fn valid_pkce(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn secure_token(kind: &str) -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("gvx_{kind}_{}", URL_SAFE_NO_PAD.encode(bytes))
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

fn nonempty(value: String, fallback: String) -> String {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn redirect_error(redirect_uri: &str, state: &str, code: &str) -> Response {
    redirect_values(redirect_uri, &[("error", code), ("state", state)])
}

fn redirect_values(redirect_uri: &str, values: &[(&str, &str)]) -> Response {
    let Ok(mut target) = Url::parse(redirect_uri) else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "invalid app redirect");
    };
    target
        .query_pairs_mut()
        .extend_pairs(values.iter().copied());
    Redirect::temporary(target.as_str()).into_response()
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error":message.into()}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirects_require_https_except_for_local_development() {
        assert!(normalize_redirect("https://app.example.test/auth/callback").is_ok());
        assert!(normalize_redirect("http://localhost:5173/auth/callback").is_ok());
        assert!(normalize_redirect("http://app.example.test/auth/callback").is_err());
        assert!(normalize_redirect("https://user@app.example.test/auth/callback").is_err());
        assert!(normalize_redirect("https://app.example.test/auth/callback#token").is_err());
    }

    #[test]
    fn pkce_and_nonce_comparisons_are_strict() {
        let verifier = "a".repeat(43);
        assert!(valid_pkce(&verifier));
        assert!(!valid_pkce("short"));
        assert_ne!(pkce_challenge(&verifier), verifier);
        assert!(constant_time_eq("same", "same"));
        assert!(!constant_time_eq("same", "other"));
    }
}
