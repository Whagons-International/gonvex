pub mod action_calls;
pub mod change_feed;
pub mod config;
pub mod control;
mod dashboard;
mod data_ingest;
pub mod error_class;
pub mod execution;
pub mod external_auth;
pub mod host_calls;
pub mod live_query;
pub mod membership_projector;
mod metrics;
pub mod module_host;
pub mod modules;
mod native_auth;
mod operations;
mod operator_data;
pub mod public_functions;
pub mod replica;
mod replica_open_queue;
pub mod sandbox;
pub mod scheduler;
mod service_principal;
mod snapshot_delivery;
pub mod storage;
pub mod telemetry;
pub mod visibility;

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use axum::extract::ws::Message;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{body, body::Body, Json, Router};
use chrono::{SecondsFormat, Utc};
use futures_util::{Stream, StreamExt};
use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry, TenantSession};
use gonvex_protocol::{
    ClientMessage, ExecutionScope, ReducerCallRequest, ReducerErrorClass, ReplicaOpenRequest,
    ServerCapabilities, ServerMessage, PROTOCOL_VERSION,
};
use gonvex_websocket::{self as websocket_transport, WebSocket};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::module_host::{ModuleHost, ModuleHostStatus};
use crate::modules::{ModuleRegistry, ModuleRegistryError};

#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    config: Config,
    module_host: Arc<ModuleHost>,
    pools: PoolRegistry,
    control_plane: RwLock<Option<ControlPlane>>,
    modules: ModuleRegistry,
    change_feeds: change_feed::ChangeFeedHub,
    telemetry: telemetry::TelemetryLimiter,
    scheduler: scheduler::Scheduler,
    runtime_events: broadcast::Sender<RuntimeEvent>,
    dashboard_changes: broadcast::Sender<()>,
    sandboxes: sandbox::SandboxManager,
    storage: storage::StorageManager,
    live_query_cache: live_query::SharedLiveQueryCache,
    membership_projector: membership_projector::MembershipProjector,
    metrics: metrics::RuntimeMetrics,
    public_limiter: Arc<public_functions::PublicLimiter>,
}

const REPLICA_OPEN_BATCH_LIMIT: usize = 256;
// Replica snapshots are independent read-only plans, but each one consumes
// tenant-pool and visibility work. Bound fan-out below both the runtime's
// normal module concurrency and the default per-database pool size.
const REPLICA_OPEN_CONCURRENCY: usize = 8;
// Tenant databases are independent shards. Provisioning them serially makes
// rolling startup time grow linearly with the tenant count and can keep the
// readiness endpoint unavailable long enough for the deployer to roll back a
// healthy candidate. Keep the work bounded, but provision independent shards
// concurrently so startup remains proportional to the slowest batch.
const STARTUP_TENANT_PROVISION_CONCURRENCY: usize = 8;

#[derive(Clone, Debug)]
enum RuntimeEvent {
    ControlChanged {
        project_id: String,
    },
    PresenceChanged {
        project_id: String,
    },
    ModuleReloaded {
        project_id: String,
    },
    SupportCommand {
        project_id: String,
        connection_id: String,
        command: serde_json::Value,
    },
}

#[derive(Debug, Error)]
pub enum RuntimeStartError {
    #[error(transparent)]
    ModuleHost(#[from] module_host::ModuleHostError),
    #[error(transparent)]
    Database(#[from] gonvex_postgres::DatabaseError),
    #[error("GONVEX_CONTROL_PLANE_DATABASE_URL or DATABASE_URL is required when authentication is enabled")]
    MissingControlPlane,
    #[error(transparent)]
    Module(#[from] ModuleRegistryError),
    #[error("invalid runtime configuration: {0}")]
    Configuration(String),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Health {
    ok: bool,
    version: String,
    time: String,
    postgres_set: bool,
    valkey_set: bool,
    rows_cache: bool,
    runtime_manifests: RuntimeManifestHealth,
    module_host: ModuleHostStatus,
    s3_set: bool,
    sandbox: SandboxHealth,
}

#[derive(Debug, Serialize)]
struct SandboxHealth {
    enabled: bool,
    ready: bool,
    error: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeManifestHealth {
    ready: bool,
    failed_projects: usize,
}

impl Runtime {
    async fn provision_runtime_tenant(
        &self,
        project_id: &str,
        tenant_id: Option<&str>,
        name: &str,
    ) -> Result<serde_json::Value, String> {
        let base_url = self
            .inner
            .config
            .default_database_url
            .as_deref()
            .ok_or_else(|| "DATABASE_URL is not configured".to_owned())?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "Control Plane is unavailable".to_owned())?;
        let module = self
            .inner
            .modules
            .project(project_id)
            .await
            .ok_or_else(|| {
                "install the TypeScript module before creating the first tenant shard".to_owned()
            })?;
        let (route, tenant) = control
            .create_runtime_tenant(base_url, project_id, tenant_id, name)
            .await
            .map_err(|error| error.to_string())?;
        control
            .clone()
            .provision_tenant_database(route, module.migrations.clone())
            .await
            .map_err(|error| error.to_string())?;
        control
            .mark_runtime_tenant_provisioned(project_id, tenant["id"].as_str().unwrap_or_default())
            .await
            .map_err(|error| error.to_string())?;
        Ok(tenant)
    }

    pub fn new(config: Config) -> Self {
        let module_host = ModuleHost::new(config.module_host.clone());
        let pools = PoolRegistry::new(PoolLimits {
            max_total_connections: config.database_max_total_connections,
            max_connections_per_database: config.database_max_connections,
            max_idle_connections_per_database: config.database_max_idle_connections,
            ..PoolLimits::default()
        });
        let change_feeds = change_feed::ChangeFeedHub::new(pools.clone());
        let (runtime_events, _) = broadcast::channel(1_024);
        let metrics = metrics::RuntimeMetrics::with_changes(runtime_events.clone());
        let sandboxes = sandbox::SandboxManager::new(config.sandbox.clone());
        let storage = storage::StorageManager::new(config.storage.clone());
        Self {
            inner: Arc::new(RuntimeInner {
                config,
                module_host,
                pools,
                control_plane: RwLock::new(None),
                modules: ModuleRegistry::new(),
                change_feeds,
                telemetry: telemetry::TelemetryLimiter::default(),
                scheduler: scheduler::Scheduler::new(),
                runtime_events,
                dashboard_changes: broadcast::channel(256).0,
                sandboxes,
                storage,
                live_query_cache: live_query::SharedLiveQueryCache::default(),
                membership_projector: membership_projector::MembershipProjector::default(),
                metrics,
                public_limiter: Arc::new(public_functions::PublicLimiter::default()),
            }),
        }
    }

    pub async fn start(&self) -> Result<(), RuntimeStartError> {
        if self.inner.config.sandbox.enabled {
            let binary = self
                .inner
                .config
                .sandbox
                .worker_binary
                .as_ref()
                .ok_or_else(|| {
                    RuntimeStartError::Configuration(
                        "GONVEX_SANDBOX_WORKER_BINARY is required when the sandbox is enabled"
                            .to_owned(),
                    )
                })?;
            tokio::fs::metadata(binary).await.map_err(|error| {
                RuntimeStartError::Configuration(format!(
                    "sandbox worker {} is unavailable: {error}",
                    binary.display()
                ))
            })?;
        }
        let storage = &self.inner.config.storage;
        let storage_partially_configured = [
            &storage.endpoint,
            &storage.bucket,
            &storage.access_key_id,
            &storage.secret_access_key,
        ]
        .iter()
        .any(|value| !value.is_empty());
        if storage_partially_configured && !storage.configured() {
            return Err(RuntimeStartError::Configuration(
                "S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID, and S3_SECRET_ACCESS_KEY must be configured together"
                    .to_owned(),
            ));
        }
        self.inner.module_host.start().await?;
        let database_url = self
            .inner
            .config
            .control_plane_database_url
            .as_deref()
            .or(self.inner.config.default_database_url.as_deref());
        if let Some(database_url) = database_url {
            let control = ControlPlane::connect(
                database_url,
                self.inner.pools.clone(),
                self.inner.config.tenant_database_urls.clone(),
            )
            .await?;
            for record in control.runtime_manifests().await? {
                let project = record.project_id.clone();
                let module = match self
                    .inner
                    .modules
                    .install(&self.inner.module_host, record)
                    .await
                {
                    Ok(module) => module,
                    Err(error) => {
                        self.inner.modules.record_failure(&project, &error).await;
                        self.inner.module_host.shutdown().await;
                        self.inner.pools.close().await;
                        return Err(error.into());
                    }
                };
                control
                    .clone()
                    .apply_control_migrations(module.migrations.clone())
                    .await?;
                let provisioned = collect_bounded_ordered(
                    control.tenant_routes(&project).await?,
                    STARTUP_TENANT_PROVISION_CONCURRENCY,
                    |route| {
                        let control = control.clone();
                        let migrations = module.migrations.clone();
                        async move { control.provision_tenant_database(route, migrations).await }
                    },
                )
                .await;
                for result in provisioned {
                    result?;
                }
            }
            *self.inner.control_plane.write().await = Some(control);
            self.start_control_event_listener().await?;
            self.inner.membership_projector.start(self.clone());
            self.inner.scheduler.start(self.clone());
        } else if self.inner.config.require_auth {
            self.inner.module_host.shutdown().await;
            return Err(RuntimeStartError::MissingControlPlane);
        }
        Ok(())
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/ws", get(websocket_upgrade))
            .route("/dev/dashboard/ws", get(dashboard::upgrade))
            .merge(native_auth::router())
            .merge(operations::router())
            .merge(operator_data::router())
            .route(
                "/storage/{*key}",
                get(storage_download)
                    .post(storage_upload)
                    .put(storage_upload)
                    .options(storage_options),
            )
            .route("/dev/sync", post(dev_sync))
            .route("/dev/projects", get(list_projects).post(create_project))
            .route("/dev/projects/{project}/key", post(project_key))
            .route("/dev/tenants", get(list_tenants).post(create_tenant_shard))
            .route(
                "/dev/tenants/{tenant}",
                axum::routing::delete(delete_tenant_shard),
            )
            .route("/dev/internal/e2e/members", post(provision_e2e_member))
            .layer(DefaultBodyLimit::max(64 << 20))
            .layer(middleware::from_fn(dev_cors))
            .layer(middleware::from_fn_with_state(
                self.clone(),
                dashboard::invalidate,
            ))
            .with_state(self.clone())
    }

    pub async fn shutdown(&self) {
        self.inner.scheduler.shutdown();
        self.inner.membership_projector.shutdown();
        self.inner.sandboxes.shutdown().await;
        self.inner.change_feeds.shutdown();
        self.inner.module_host.shutdown().await;
        self.inner.pools.close().await;
    }
}

async fn dev_cors(request: axum::http::Request<Body>, next: middleware::Next) -> Response {
    let is_dev_route = request.uri().path().starts_with("/dev/");
    if is_dev_route && request.method() == Method::OPTIONS {
        return dev_cors_headers(StatusCode::NO_CONTENT.into_response());
    }
    let response = next.run(request).await;
    if is_dev_route {
        dev_cors_headers(response)
    } else {
        response
    }
}

fn dev_cors_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, PATCH, DELETE, OPTIONS"),
    );
    headers.insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(
            "Authorization, Content-Type, X-Gonvex-Key, X-Gonvex-Project-Key, X-Gonvex-Project-Id, X-Gonvex-Tenant-Id",
        ),
    );
    headers.insert(
        axum::http::header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
    response
}

#[derive(serde::Deserialize)]
struct StorageProxyQuery {
    exp: i64,
    sig: String,
    #[serde(default)]
    upload: Option<u8>,
}

async fn storage_download(
    state: State<Runtime>,
    path: Path<String>,
    query: Query<StorageProxyQuery>,
    headers: HeaderMap,
) -> Response {
    storage_cors(storage_download_inner(state, path, query, headers).await)
}

async fn storage_download_inner(
    State(runtime): State<Runtime>,
    Path(key): Path<String>,
    Query(query): Query<StorageProxyQuery>,
    headers: HeaderMap,
) -> Response {
    if query.upload.is_some()
        || !runtime
            .inner
            .storage
            .verify_proxy(&key, query.exp, &query.sig, false)
    {
        return (
            StatusCode::FORBIDDEN,
            "invalid or expired storage signature",
        )
            .into_response();
    }
    let range = headers.get("range").and_then(|value| value.to_str().ok());
    match runtime.inner.storage.proxy_get(&key, range).await {
        Ok(upstream)
            if upstream.status().is_success()
                || upstream.status() == reqwest::StatusCode::PARTIAL_CONTENT =>
        {
            let status =
                StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let forwarded = [
                "accept-ranges",
                "content-length",
                "content-range",
                "content-type",
                "etag",
                "last-modified",
            ]
            .into_iter()
            .filter_map(|name| {
                upstream
                    .headers()
                    .get(name)
                    .cloned()
                    .map(|value| (name, value))
            })
            .collect::<Vec<_>>();
            let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
            *response.status_mut() = status;
            for (name, value) in forwarded {
                response
                    .headers_mut()
                    .insert(axum::http::HeaderName::from_static(name), value);
            }
            response.headers_mut().insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("private, max-age=300"),
            );
            response
        }
        Ok(_) => (StatusCode::NOT_FOUND, "object not found").into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}

async fn storage_upload(
    state: State<Runtime>,
    path: Path<String>,
    query: Query<StorageProxyQuery>,
    headers: HeaderMap,
    request_body: Body,
) -> Response {
    storage_cors(storage_upload_inner(state, path, query, headers, request_body).await)
}

async fn storage_upload_inner(
    State(runtime): State<Runtime>,
    Path(key): Path<String>,
    Query(query): Query<StorageProxyQuery>,
    headers: HeaderMap,
    request_body: Body,
) -> Response {
    if query.upload != Some(1)
        || !runtime
            .inner
            .storage
            .verify_proxy(&key, query.exp, &query.sig, true)
    {
        return (
            StatusCode::FORBIDDEN,
            "invalid or expired storage signature",
        )
            .into_response();
    }
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = match body::to_bytes(request_body, 128 << 20).await {
        Ok(bytes) => bytes.to_vec(),
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "upload too large").into_response(),
    };
    match runtime
        .inner
        .storage
        .proxy_put(&runtime, &key, bytes, &content_type)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "storageId": key.rsplit('/').next().unwrap_or_default()
            })),
        )
            .into_response(),
        Err(error) => (
            if error == "upload already consumed" {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_GATEWAY
            },
            Json(serde_json::json!({"error":error})),
        )
            .into_response(),
    }
}

async fn storage_options(
    State(runtime): State<Runtime>,
    Path(key): Path<String>,
    Query(query): Query<StorageProxyQuery>,
) -> Response {
    let response = if query.upload == Some(1)
        && runtime
            .inner
            .storage
            .verify_proxy(&key, query.exp, &query.sig, true)
    {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::FORBIDDEN,
            "invalid or expired storage signature",
        )
            .into_response()
    };
    storage_cors(response)
}

fn storage_cors(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, OPTIONS"),
    );
    headers.insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type"),
    );
    headers.insert(
        axum::http::header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
    response
}

async fn list_projects(State(runtime): State<Runtime>, headers: HeaderMap) -> Response {
    let actor = match operations::authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"Control Plane is unavailable"})),
        )
            .into_response();
    };
    match control.runtime_projects().await {
        Ok(projects) => {
            let mut visible = Vec::new();
            for project in projects {
                let id = project
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                if operations::can_access_project(&runtime, &actor, id).await {
                    visible.push(project);
                }
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({"projects":visible})),
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn create_project(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    let actor = match operations::authorize(&runtime, &headers, "projects:create").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(base_url) = runtime.inner.config.default_database_url.clone() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"DATABASE_URL is not configured"})),
        )
            .into_response();
    };
    let name = payload
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    let mode = payload
        .get("databaseMode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("single")
        .to_owned();
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"Control Plane is unavailable"})),
        )
            .into_response();
    };
    match control
        .create_runtime_project(&base_url, &name, &mode, &actor.email)
        .await
    {
        Ok(project) => (StatusCode::CREATED, Json(project)).into_response(),
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn project_key(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    axum::extract::Path(project): axum::extract::Path<String>,
) -> Response {
    let actor = match operations::authorize(&runtime, &headers, "projects:keys:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if !operations::can_access_project(&runtime, &actor, &project).await {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"project access is required"})),
        )
            .into_response();
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"Control Plane is unavailable"})),
        )
            .into_response();
    };
    match control.runtime_project_key(&project).await {
        Ok(project_key) => (
            StatusCode::OK,
            Json(serde_json::json!({"projectKey":project_key})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct TenantListQuery {
    project: Option<String>,
}

async fn list_tenants(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<TenantListQuery>,
) -> Response {
    let actor = match operations::authorize(&runtime, &headers, "projects:read").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if let Some(project) = query.project.as_deref() {
        if !operations::can_access_project(&runtime, &actor, project).await {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error":"project access is required"})),
            )
                .into_response();
        }
    } else if !actor.global_admin() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"project is required for non-global credentials"})),
        )
            .into_response();
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"Control Plane is unavailable"})),
        )
            .into_response();
    };
    match control.runtime_tenants(query.project.as_deref()).await {
        Ok(tenants) => {
            (StatusCode::OK, Json(serde_json::json!({"tenants":tenants}))).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn create_tenant_shard(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    let project_id = payload
        .get("projectId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    let tenant_id = payload
        .get("tenantId")
        .or_else(|| payload.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let name = payload
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    if let Err(response) = operations::authorize_project_resource(
        &runtime,
        &headers,
        project_id,
        "projects:update",
        true,
    )
    .await
    {
        return response;
    }
    let result = runtime
        .provision_runtime_tenant(project_id, tenant_id, name)
        .await;
    match result {
        Ok(tenant) => (StatusCode::CREATED, Json(tenant)).into_response(),
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn delete_tenant_shard(
    State(runtime): State<Runtime>,
    Path(tenant): Path<String>,
    headers: HeaderMap,
    Query(query): Query<TenantListQuery>,
) -> Response {
    let project = query.project.as_deref().unwrap_or("").trim();
    let actor = match operations::authorize(&runtime, &headers, "projects:update").await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    if project.is_empty() || !operations::can_manage_project(&runtime, &actor, project).await {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"project owner or admin access is required"})),
        )
            .into_response();
    }
    let Some(base_url) = runtime.inner.config.default_database_url.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"DATABASE_URL is not configured"})),
        )
            .into_response();
    };
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"Control Plane is unavailable"})),
        )
            .into_response();
    };
    match control
        .delete_runtime_tenant(base_url, project, tenant.trim())
        .await
    {
        Ok(()) => Json(serde_json::json!({"ok":true})).into_response(),
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn provision_e2e_member(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    if !operations::admin_key_matches(&runtime, &headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"runtime admin key is required"})),
        )
            .into_response();
    }
    let project_id = payload
        .get("projectId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    let tenant_id = payload
        .get("tenantId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    let email = payload
        .get("email")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim()
        .to_lowercase();
    let requested_name = payload
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    let password = payload
        .get("password")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let account_only = payload
        .get("accountOnly")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if project_id.is_empty()
        || (!account_only && tenant_id.is_empty())
        || email.is_empty()
        || email.len() > 320
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"projectId and email are required; tenantId is required unless accountOnly is true"})),
        )
            .into_response();
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"Control Plane is unavailable"})),
        )
            .into_response();
    };
    let result = async {
        let mut control_tx = control.begin_control_transaction(false).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!("e2e-account:{project_id}:{email}"))
            .execute(&mut **control_tx.transaction())
            .await?;
        let auth_mode = sqlx::query_scalar::<_, String>(
            "SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1 AND status='active'",
        )
        .bind(project_id)
        .fetch_optional(&mut **control_tx.transaction())
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound)?;
        let mut row = sqlx::query(
            "SELECT id,name,avatar_url FROM accounts WHERE auth_realm_id=$1 AND lower(email)=lower($2) AND disabled_at IS NULL",
        )
        .bind(project_id)
        .bind(&email)
        .fetch_optional(&mut **control_tx.transaction())
        .await?;
        if row.is_none() {
            if !matches!(auth_mode.as_str(), "gonvex-native" | "hybrid" | "firebase") {
                return Err(sqlx::Error::Protocol(
                    "test actor creation requires gonvex-native, hybrid, or firebase authentication"
                        .to_owned(),
                )
                .into());
            }
            let password_identity = matches!(auth_mode.as_str(), "gonvex-native" | "hybrid");
            if password_identity && password.len() < 12 {
                return Err(sqlx::Error::Protocol(
                    "a password containing at least 12 characters is required to create the test actor"
                        .to_owned(),
                )
                .into());
            }
            let account_id = format!("acct_{}", uuid::Uuid::new_v4().simple());
            let name = if requested_name.is_empty() {
                email.split('@').next().unwrap_or(&email)
            } else {
                requested_name
            };
            sqlx::query(
                "INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at) VALUES($1,$2,$3,$4,NULL,now())",
            )
            .bind(&account_id)
            .bind(project_id)
            .bind(&email)
            .bind(name)
            .execute(&mut **control_tx.transaction())
            .await?;
            if password_identity {
                sqlx::query(
                    r#"INSERT INTO account_identities
                       (project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
                       VALUES($1,$2,'password',$1,$3,$3,TRUE,now())"#,
                )
                .bind(project_id)
                .bind(&account_id)
                .bind(&email)
                .execute(&mut **control_tx.transaction())
                .await?;
                sqlx::query(
                    "INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)",
                )
                .bind(project_id)
                .bind(&account_id)
                .bind(control::hash_password(password))
                .execute(&mut **control_tx.transaction())
                .await?;
            }
            row = sqlx::query(
                "SELECT id,name,avatar_url FROM accounts WHERE id=$1 AND auth_realm_id=$2",
            )
            .bind(&account_id)
            .bind(project_id)
            .fetch_optional(&mut **control_tx.transaction())
            .await?;
        }
        let row = row.ok_or_else(|| sqlx::Error::RowNotFound)?;
        let account_id: String = sqlx::Row::get(&row, "id");
        let name: String = sqlx::Row::get(&row, "name");
        let avatar: String = sqlx::Row::get(&row, "avatar_url");
        control_tx.commit().await?;
        if account_only {
            return Ok(serde_json::json!({
                "projectId":project_id,"accountId":account_id,
            }));
        }
        let route = control.resolve_tenant(project_id, tenant_id).await?;
        let mut tenant_tx = control.begin_tenant_transaction(&route, false).await?;
        tenant_tx.set_command_id(&format!("e2e-member:{account_id}")).await?;
        let member_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&account_id)
        .fetch_optional(&mut **tenant_tx.transaction())
        .await?
        .unwrap_or_else(|| format!("member_{}", uuid::Uuid::new_v4().simple()));
        let revision = sqlx::query_scalar::<_, i64>(
            r#"INSERT INTO members
               (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,$4,'admin','{"e2e":true}'::jsonb,now())
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 avatar_url=EXCLUDED.avatar_url,role='admin',permissions='{"e2e":true}'::jsonb,
                 membership_revision=members.membership_revision+1,updated_at=now()
               RETURNING membership_revision"#,
        )
        .bind(&member_id)
        .bind(&account_id)
        .bind(name)
        .bind(avatar)
        .fetch_one(&mut **tenant_tx.transaction())
        .await?;
        tenant_tx.commit().await?;
        let mut finish = control.begin_control_transaction(false).await?;
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
        .bind(revision)
        .execute(&mut **finish.transaction())
        .await?;
        finish.commit().await?;
        Ok::<_, gonvex_postgres::DatabaseError>(serde_json::json!({
            "projectId":project_id,"tenantId":tenant_id,
            "accountId":account_id,"memberId":member_id,
        }))
    }
    .await;
    match result {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(gonvex_postgres::DatabaseError::Sql(sqlx::Error::RowNotFound)) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"test actor account not found"})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn dev_sync(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(mut manifest): Json<serde_json::Value>,
) -> Response {
    match apply_dev_sync(&runtime, &headers, &mut manifest).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err((status, error)) => (status, Json(serde_json::json!({"error":error}))).into_response(),
    }
}

async fn apply_dev_sync(
    runtime: &Runtime,
    headers: &HeaderMap,
    manifest: &mut serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let header_project = headers
        .get("x-gonvex-project-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .trim();
    let body_project = manifest
        .get("project")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    let project = if body_project.is_empty() {
        header_project.to_owned()
    } else {
        body_project.to_owned()
    };
    if project.is_empty() || !header_project.is_empty() && header_project != project {
        return Err((
            StatusCode::BAD_REQUEST,
            "manifest project does not match x-gonvex-project-id".to_owned(),
        ));
    }
    manifest["project"] = serde_json::Value::String(project.clone());
    let key = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .or_else(|| {
            headers
                .get("x-gonvex-key")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or("")
        .trim();
    let control = runtime
        .inner
        .control_plane
        .read()
        .await
        .clone()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Control Plane is unavailable".to_owned(),
            )
        })?;
    let accepted = control
        .project_accepts_sync_key(&project, key, runtime.inner.config.dev_sync_key.as_deref())
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if !accepted {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid Gonvex sync key".to_owned(),
        ));
    }
    let module = manifest
        .get("module")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                "manifest has no TypeScript module".to_owned(),
            )
        })?;
    let module_hash = module
        .get("hash")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    let record = gonvex_postgres::RuntimeManifestRecord {
        project_id: project.clone(),
        module_hash,
        manifest: manifest.clone(),
    };
    let migrations = modules::validate_manifest_record(&record)
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    control
        .clone()
        .apply_control_migrations(migrations.clone())
        .await
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    let routes = control
        .tenant_routes(&project)
        .await
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    for route in routes.iter().cloned() {
        control
            .clone()
            .provision_tenant_database(route, migrations.clone())
            .await
            .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    }
    let installed = runtime
        .inner
        .modules
        .install(&runtime.inner.module_host, record.clone())
        .await
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    runtime
        .inner
        .scheduler
        .sync_project(runtime, &project, &installed.crons, &routes)
        .await
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error))?;
    runtime.inner.live_query_cache.clear().await;
    let _ = runtime
        .inner
        .runtime_events
        .send(RuntimeEvent::ModuleReloaded {
            project_id: project.clone(),
        });
    control
        .save_runtime_manifest(&record)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(serde_json::json!({
        "ok":true,
        "project":project,
        "functionCount":installed.functions.len(),
        "schemaDefinition":manifest.get("schema").cloned().unwrap_or_else(|| serde_json::json!({})),
        "runtimeReloaded":true,
    }))
}

async fn health(State(runtime): State<Runtime>) -> Response {
    let mut module_host = runtime.inner.module_host.status().await;
    let (active_projects, failed_projects) = runtime.inner.modules.counts().await;
    module_host.active_projects = active_projects;
    let postgres_ready = runtime.inner.control_plane.read().await.is_some()
        || runtime.inner.config.control_plane_database_url.is_none();
    let sandbox_ready = !runtime.inner.config.sandbox.enabled
        || runtime
            .inner
            .config
            .sandbox
            .worker_binary
            .as_ref()
            .is_some_and(|path| path.is_file());
    let ready = module_host.ready && postgres_ready && failed_projects == 0 && sandbox_ready;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(Health {
            ok: ready,
            version: runtime.inner.config.runtime_version.clone(),
            time: Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true),
            postgres_set: runtime.inner.config.control_plane_database_url.is_some()
                || runtime.inner.config.default_database_url.is_some(),
            valkey_set: false,
            rows_cache: false,
            runtime_manifests: RuntimeManifestHealth {
                ready: failed_projects == 0,
                failed_projects,
            },
            module_host,
            s3_set: runtime.inner.storage.configured(),
            sandbox: SandboxHealth {
                enabled: runtime.inner.config.sandbox.enabled,
                ready: sandbox_ready,
                error: if sandbox_ready {
                    String::new()
                } else {
                    "sandbox worker is unavailable".to_owned()
                },
            },
        }),
    )
        .into_response()
}

async fn websocket_upgrade(
    State(runtime): State<Runtime>,
    peer: Option<axum::Extension<axum::extract::ConnectInfo<std::net::SocketAddr>>>,
    Query(params): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Response {
    let peer = peer
        .map(|peer| peer.0 .0.ip())
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let limits = &runtime.inner.config.public_functions;
    let ip = public_functions::client_ip(peer, &headers, limits);
    let public_transport = params.get("public").is_some_and(|v| v == "1");
    let Some(pending) = runtime
        .inner
        .public_limiter
        .admit_pending(ip, limits.pending_per_ip)
    else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "pending connection limit exceeded",
        )
            .into_response();
    };
    let public_connection = if public_transport {
        let Some(slot) = runtime
            .inner
            .public_limiter
            .admit(ip, limits.connections_per_ip)
        else {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "public connection limit exceeded",
            )
                .into_response();
        };
        Some(slot)
    } else {
        None
    };
    let max_message = if public_transport {
        limits.payload_bytes
    } else {
        64 << 20
    };
    let max_frame = if public_transport {
        limits.payload_bytes
    } else {
        16 << 20
    };
    websocket_transport::upgrade(
        request,
        runtime.inner.config.websocket_compression,
        runtime.inner.config.websocket_compression_threshold,
        max_message,
        max_frame,
        move |socket| {
            websocket(
                socket,
                runtime,
                ip,
                public_transport,
                public_connection,
                pending,
            )
        },
    )
    .await
}

async fn websocket(
    mut socket: WebSocket,
    runtime: Runtime,
    ip: std::net::IpAddr,
    public_transport: bool,
    _public_connection: Option<public_functions::PublicConnection>,
    pending: public_functions::PublicConnection,
) {
    let mut pending_connection = Some(pending);
    let auth_deadline =
        tokio::time::Instant::now() + runtime.inner.config.public_functions.auth_timeout;
    let mut public_scope: Option<(String, String)> = None;
    let mut public_admission: Option<Arc<public_functions::PublicAdmission>> = None;
    let mut _public_monitor: Option<public_functions::AdmissionMonitor> = None;
    let mut public_window = public_functions::Window::default();
    let mut tenant_session: Option<TenantSession> = None;
    let mut telemetry_device = None;
    let mut telemetry_batch_enabled = false;
    let mut replica_snapshot_pages_enabled = false;
    let mut replica_sum_digest_enabled = false;
    let mut replica_native_rows_enabled = false;
    let mut snapshot_delivery = snapshot_delivery::SnapshotDelivery::default();
    let mut legacy_snapshot_delivery = snapshot_delivery::LegacySnapshotDelivery::default();
    let mut service_grant: Option<service_principal::ServiceGrant> = None;
    let mut connected_client_contract: Option<u64> = None;
    let connection_id = uuid::Uuid::new_v4().to_string();
    let _connection_presence = runtime.inner.metrics.register(&connection_id);
    let mut control_connection = control::ControlConnection {
        connection_id: connection_id.clone(),
        ..control::ControlConnection::default()
    };
    let mut feed = None;
    let mut feed_scheduler = FeedFirstScheduler::default();
    let mut sent_replica_revision = 0u64;
    let mut sent_zero_bootstrap_watermark = false;
    let mut replicas = BTreeMap::new();
    let mut replica_opens = replica_open_queue::ReplicaOpenQueue::default();
    // Retain at most one decoded, transport-size-bounded frame. While it waits
    // for preparation capacity, stop reading further frames (TCP backpressure).
    // SQL preparations do not depend on socket input; auth timers remain live.
    let mut pending_replica_opens = VecDeque::<ReplicaOpenRequest>::new();
    let mut live_queries = BTreeMap::new();
    let mut control_queries = BTreeMap::new();
    let mut runtime_events = runtime.inner.runtime_events.subscribe();
    let mut auth_revalidation = tokio::time::interval(std::time::Duration::from_secs(30));
    auth_revalidation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    auth_revalidation.tick().await;
    let ready = ServerMessage::SessionReady {
        project: None,
        tenant: None,
        replica: None,
        capabilities: Some(ServerCapabilities {
            replica_snapshot_pages: Some(1),
            replica_set_digest: Some(1),
            replica_native_rows: Some(1),
            replica_resume: Some(1),
            query_resume: Some(1),
            preserve_subscriptions: Some(1),
            telemetry_batch: Some(1),
            protocol_version: Some(PROTOCOL_VERSION),
            runtime_version: Some(runtime.inner.config.runtime_version.clone()),
            replica_batch: Some(1),
            replica_integrity: Some(1),
            query_batch: Some(1),
            query_result_batch: Some(1),
            reducer_batch: Some(1),
            replica_watermark: Some(1),
            control_watermark: Some(1),
        }),
    };
    if send_connection_json(&mut snapshot_delivery, &mut socket, &ready)
        .await
        .is_err()
    {
        return;
    }

    // Discards the tenant scope after the session's own Member changed. The
    // caller then sends `membership_changed_frames`.
    macro_rules! discard_tenant_session {
        () => {{
            replicas.clear();
            replica_opens.clear();
            pending_replica_opens.clear();
            snapshot_delivery.clear();
            legacy_snapshot_delivery.clear();
            live_queries.clear();
            control_queries.clear();
            tenant_session = None;
            feed = None;
            feed_scheduler.reset();
            sent_replica_revision = 0;
            sent_zero_bootstrap_watermark = false;
            runtime.inner.metrics.authenticated(
                &connection_id,
                &control::ControlConnection {
                    connection_id: connection_id.clone(),
                    ..control::ControlConnection::default()
                },
                None,
            );
            runtime
                .inner
                .metrics
                .subscriptions(&connection_id, std::iter::empty::<&str>());
        }};
    }

    loop {
        let preparing_replicas = !replica_opens.is_empty();
        let admit_replica = pending_replica_opens.front().is_some_and(|request| {
            let bytes = replica_open_request_bytes(request);
            // Oversized individual requests must reach validation, not wait
            // forever for capacity that can never exist.
            bytes > replica_open_queue::MAX_BYTES || replica_opens.has_capacity(&request.id, bytes)
        });
        let defer_feed = preparing_replicas || !legacy_snapshot_delivery.is_empty();
        let message = tokio::select! {
            _ = std::future::ready(()), if admit_replica => {
                let request = pending_replica_opens.pop_front().unwrap();
                snapshot_delivery.remove(&request.id);
                legacy_snapshot_delivery.remove(&request.id);
                replicas.remove(&request.id);
                runtime.inner.metrics.activity(
                    &connection_id, "Replica Collection opened", Some(&request.path),
                );
                if let Some(response) = enqueue_replica_open(
                    &runtime, tenant_session.as_ref(), request,
                    replica_sum_digest_enabled, &mut replica_opens,
                ).await {
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response).await.is_err() { return; }
                }
                continue;
            }
            opened = replica_opens.next(), if preparing_replicas => {
                if let Some(outcome) = opened {
                    for response in install_replica_open_outcomes(&mut replicas, vec![outcome]) {
                        let response = if replica_snapshot_pages_enabled && !replica_native_rows_enabled {
                            let Some(response)=legacy_snapshot_delivery.admit(response) else {continue}; response
                        } else {response};
                        if send_replica_open(&mut socket, response, replica_snapshot_pages_enabled, replica_native_rows_enabled, &mut snapshot_delivery).await.is_err() { return; }
                    }
                    record_connection_subscriptions(&runtime, &connection_id, &replicas, &live_queries, &control_queries);
                }
                continue;
            }
            _ = tokio::task::yield_now(), if !legacy_snapshot_delivery.is_empty() => {
                for page in legacy_snapshot_delivery.next() {
                    if send_json(&mut socket,&page).await.is_err() {return;}
                }
                continue;
            }
            _ = tokio::time::sleep_until(auth_deadline), if pending_connection.is_some() => {
                let _ = socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1008, reason: "authentication deadline exceeded".into(),
                }))).await;
                break;
            }
            _ = async { if let Some(admission) = &public_admission { admission.changed.notified().await; } else { std::future::pending::<()>().await; } } => {
                let _ = send_connection_json(&mut snapshot_delivery, &mut socket, &ServerMessage::AuthError { code: Some("transient".into()), id: "session-expired".into(), error: "public tenant session is unavailable".into() }).await;
                break;
            }
            _ = auth_revalidation.tick(), if !control_connection.auth_token.is_empty() => {
                if !revalidate_connection(&runtime, &control_connection).await {
                    replicas.clear();
                    replica_opens.clear();
                    pending_replica_opens.clear();
                    snapshot_delivery.clear();
                    legacy_snapshot_delivery.clear();
                    live_queries.clear();
                    control_queries.clear();
                    tenant_session = None;
                    feed = None;
                    feed_scheduler.reset();
                    sent_replica_revision = 0;
                    sent_zero_bootstrap_watermark = false;
                    control_connection = control::ControlConnection {
                        connection_id: connection_id.clone(),
                        ..control::ControlConnection::default()
                    };
                    runtime.inner.metrics.authenticated(
                        &connection_id,
                        &control_connection,
                        None,
                    );
                    runtime
                        .inner
                        .metrics
                        .subscriptions(&connection_id, std::iter::empty::<&str>());
                    let response = ServerMessage::AuthError {
                        code: Some("transient".into()),
                        id: "session-expired".to_owned(),
                        error: "authentication expired or was revoked".to_owned(),
                    };
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response).await.is_err() { break; }
                }
                continue;
            }
            // Retain feed events in the bounded broadcast receiver until every
            // newly prepared snapshot is installed. Each subscription skips
            // events at/before its own snapshot cursor; receiver lag produces
            // the existing reset, never a silently missed transaction.
            transport = async {
                if defer_feed { ScheduledTransport::Socket(socket.next().await) }
                else { next_socket_or_feed(&mut socket, &mut feed, &mut feed_scheduler).await }
            }, if pending_replica_opens.is_empty() => {
                match transport {
                    ScheduledTransport::Socket(message) => {
                        let Some(message) = message else { break; };
                        message
                    }
                    ScheduledTransport::Feed(event) => match event {
                    Ok(event) => {
                        let feed_revision = match &event {
                            change_feed::FeedEvent::Transaction { revision, .. } => Some(*revision),
                            change_feed::FeedEvent::Reset { .. } => None,
                        };
                        let feed_apply_started = std::time::Instant::now();
                        tracing::debug!(
                            target: "gonvex_runtime::ws_trace",
                            connection = %connection_id,
                            tenant = tenant_session.as_ref().map(|session| session.route.tenant_id.as_str()),
                            revision = feed_revision,
                            "websocket feed apply started"
                        );
                        let reset = matches!(event, change_feed::FeedEvent::Reset { .. });
                        let transaction_watermark = replica_watermark(&event);
                        if let Some(session) = tenant_session.as_ref() {
                            if membership_affects(session, &event) {
                                discard_tenant_session!();
                                for message in membership_changed_frames(transaction_watermark) {
                                    if send_connection_json(&mut snapshot_delivery, &mut socket, &message).await.is_err() { return; }
                                }
                                continue;
                            }
                            for message in runtime
                                .apply_live_query_feed_event(session, &mut live_queries, &event)
                                .await
                            {
                                if send_connection_json(&mut snapshot_delivery, &mut socket, &message).await.is_err() { return; }
                            }
                            match runtime.apply_feed_event(session, &mut replicas, event).await {
                                Ok(messages) => {
                                    for message in messages {
                                        if send_connection_json(&mut snapshot_delivery, &mut socket, &message).await.is_err() { return; }
                                    }
                                    // This is the connection-level commit
                                    // boundary. A transaction frame updates
                                    // normalized entities first; collection
                                    // deltas that follow update membership.
                                    // Emit the watermark only after every Live
                                    // Query and Replica frame for the revision
                                    // has been written, including the case
                                    // where this connection has no visible
                                    // subscription changes.
                                    if let Some(watermark) = transaction_watermark {
                                        if send_connection_json(&mut snapshot_delivery, &mut socket, &watermark).await.is_err() { return; }
                                        if let ServerMessage::ReplicaWatermark { revision } = watermark {
                                            sent_replica_revision = sent_replica_revision.max(revision);
                                        }
                                    }
                                    if reset {
                                        replicas.clear();
                                        replica_opens.clear();
                                        pending_replica_opens.clear();
                                        snapshot_delivery.clear();
                                        legacy_snapshot_delivery.clear();
                                    }
                                }
                                Err(error) => {
                                    for subscription in replicas.values() {
                                        let response = ServerMessage::ReplicaError {
                                            id: subscription.id.clone(),
                                            path: Some(subscription.path.clone()),
                                            error: error.to_string(),
                                        };
                                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response).await.is_err() { return; }
                                    }
                                    replicas.clear();
                                    replica_opens.clear();
                                    pending_replica_opens.clear();
                                    snapshot_delivery.clear();
                                    legacy_snapshot_delivery.clear();
                                }
                            }
                        }
                        tracing::debug!(
                            target: "gonvex_runtime::ws_trace",
                            connection = %connection_id,
                            tenant = tenant_session.as_ref().map(|session| session.route.tenant_id.as_str()),
                            revision = feed_revision,
                            apply_ms = feed_apply_started.elapsed().as_secs_f64() * 1_000.0,
                            "websocket feed apply finished"
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        for subscription in replicas.values() {
                            let response = ServerMessage::ReplicaReset {
                                id: subscription.id.clone(),
                                path: Some(subscription.path.clone()),
                                reason: "change-feed-lagged".to_owned(),
                            };
                            if send_connection_json(&mut snapshot_delivery, &mut socket, &response).await.is_err() { return; }
                        }
                        replicas.clear();
                        replica_opens.clear();
                        pending_replica_opens.clear();
                        snapshot_delivery.clear();
                        legacy_snapshot_delivery.clear();
                        live_queries.clear();
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        feed = None;
                        feed_scheduler.reset();
                        sent_replica_revision = 0;
                        sent_zero_bootstrap_watermark = false;
                        continue;
                    }
                    },
                }
            }
            event = runtime_events.recv() => {
                match event {
                    Ok(RuntimeEvent::ControlChanged { project_id })
                        if project_id == control_connection.project_id =>
                    {
                        for message in runtime
                            .refresh_control_queries(
                                &control_connection,
                                &mut control_queries,
                                "control-change",
                            )
                            .await
                        {
                            if send_connection_json(&mut snapshot_delivery, &mut socket, &message).await.is_err() { return; }
                        }
                    }
                    Ok(RuntimeEvent::PresenceChanged { project_id })
                        if project_id == control_connection.project_id =>
                    {
                        for message in runtime.refresh_control_queries(
                            &control_connection, &mut control_queries, "presence-change",
                        ).await {
                            if send_connection_json(&mut snapshot_delivery, &mut socket, &message).await.is_err() { return; }
                        }
                    }
                    Ok(RuntimeEvent::ModuleReloaded { project_id })
                        if project_id == control_connection.project_id =>
                    {
                        replicas.clear();
                        replica_opens.clear();
                        pending_replica_opens.clear();
                        snapshot_delivery.clear();
                        legacy_snapshot_delivery.clear();
                        live_queries.clear();
                        if let Some(module) = runtime.inner.modules.project(&project_id).await {
                            if let Some(required) = module.client_contract {
                                if tenant_session.is_some() && connected_client_contract != Some(required) {
                                    let _ = send_connection_json(&mut snapshot_delivery, &mut socket, &ServerMessage::ClientUpdateRequired { reason: "CLIENT_UPDATE_REQUIRED".into(), contract: required }).await;
                                    break;
                                }
                            }
                        }
                        let artifact_hash = runtime
                            .inner
                            .modules
                            .project(&project_id)
                            .await
                            .map(|module| module.artifact_hash.clone());
                        let response = ServerMessage::SystemReload {
                            reason: "module generation changed".to_owned(),
                            artifact_hash,
                        };
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response).await.is_err() { return; }
                    }
                    Ok(RuntimeEvent::SupportCommand { project_id, connection_id: target, command })
                        if project_id == control_connection.project_id && target == connection_id =>
                    {
                        let id = command.get("id").and_then(serde_json::Value::as_str).unwrap_or_default().to_owned();
                        let response = ServerMessage::SupportCommand { id, result: command };
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response).await.is_err() { return; }
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
                continue;
            }
        };
        let Ok(message) = message else {
            if public_transport {
                let _ = socket
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1009,
                        reason: "public transport limit exceeded".into(),
                    })))
                    .await;
            }
            break;
        };
        if tenant_session.is_some() {
            if let Some(module) = runtime
                .inner
                .modules
                .project(&control_connection.project_id)
                .await
            {
                if let Some(required) = module.client_contract {
                    if connected_client_contract != Some(required) {
                        let _ = send_connection_json(
                            &mut snapshot_delivery,
                            &mut socket,
                            &ServerMessage::ClientUpdateRequired {
                                reason: "CLIENT_UPDATE_REQUIRED".into(),
                                contract: required,
                            },
                        )
                        .await;
                        break;
                    }
                }
            }
        }
        // A service principal socket accepts only its narrow capability set;
        // it never reaches the member, replica or Control Plane handlers below.
        if let (Some(grant), Message::Text(text)) = (service_grant.as_ref(), &message) {
            match runtime.handle_service_frame(grant, text).await {
                service_principal::ServiceFrame::PassThrough => {}
                service_principal::ServiceFrame::Reply(response) => {
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    continue;
                }
                service_principal::ServiceFrame::Ignore => continue,
                service_principal::ServiceFrame::Close => {
                    let _ = socket
                        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1003,
                            reason: "invalid Gonvex protocol frame".into(),
                        })))
                        .await;
                    break;
                }
            }
        }
        if let Message::Binary(bytes) = &message {
            let anonymous = tenant_session
                .as_ref()
                .is_some_and(TenantSession::is_anonymous);
            let unscoped = tenant_session.is_none()
                && control_connection.identity.is_none()
                && service_grant.is_none();
            if (anonymous || unscoped)
                && bytes.len() > runtime.inner.config.public_functions.payload_bytes
            {
                let _ = socket
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1009,
                        reason: "public payload too large".into(),
                    })))
                    .await;
                break;
            }
        }
        if let Message::Text(text) = &message {
            let unscoped = tenant_session.is_none()
                && control_connection.identity.is_none()
                && service_grant.is_none();
            let anonymous = tenant_session
                .as_ref()
                .is_some_and(TenantSession::is_anonymous);
            if (anonymous || unscoped)
                && text.len() > runtime.inner.config.public_functions.payload_bytes
            {
                let _ = socket
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1009,
                        reason: "public payload too large".into(),
                    })))
                    .await;
                break;
            }
            if let Ok(frame) = serde_json::from_str::<ClientMessage>(text) {
                let public_auth = matches!(&frame, ClientMessage::Auth { public: true, .. });
                if let ClientMessage::Auth { id, public, .. } = &frame {
                    if *public != public_transport {
                        let _ = send_connection_json(
                            &mut snapshot_delivery,
                            &mut socket,
                            &ServerMessage::AuthError {
                                code: Some("transient".into()),
                                id: id.clone(),
                                error:
                                    "public authentication requires the public WebSocket transport"
                                        .into(),
                            },
                        )
                        .await;
                        break;
                    }
                }
                if public_admission.as_ref().is_some_and(|a| a.revoked()) {
                    let _ = send_connection_json(
                        &mut snapshot_delivery,
                        &mut socket,
                        &ServerMessage::AuthError {
                            code: Some("transient".into()),
                            id: "session-expired".into(),
                            error: "public tenant session is unavailable".into(),
                        },
                    )
                    .await;
                    break;
                }
                if public_auth && text.len() > runtime.inner.config.public_functions.payload_bytes {
                    let _ = socket
                        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1009,
                            reason: "public payload too large".into(),
                        })))
                        .await;
                    break;
                }
                if public_transport {
                    let cost = match &frame {
                        ClientMessage::ReducerCallMany { calls } => calls.len().max(1),
                        ClientMessage::QuerySubscribeMany { subscribes } => subscribes.len().max(1),
                        ClientMessage::ReplicaOpenMany { opens } => opens.len().max(1),
                        ClientMessage::ReplicaSnapshotAck { .. } if replica_native_rows_enabled => {
                            0
                        }
                        _ => 1,
                    };
                    if !runtime.inner.public_limiter.allow(
                        ip,
                        &mut public_window,
                        cost,
                        &runtime.inner.config.public_functions,
                    ) {
                        for response in public_limit_errors(frame) {
                            if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        continue;
                    }
                }
            }
        }
        if let (Some(admission), Some(scope), Message::Text(text)) =
            (&public_admission, &public_scope, &message)
        {
            if let Ok(frame) = serde_json::from_str::<ClientMessage>(text) {
                if !matches!(&frame, ClientMessage::Auth { .. })
                    && !check_public_admission(&runtime, admission, scope).await
                {
                    for response in public_admission_errors(frame) {
                        let _ =
                            send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                                .await;
                    }
                    break;
                }
            }
        }
        match message {
            Message::Text(text) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::Auth {
                    id,
                    token: Some(token),
                    public: false,
                    project,
                    tenant,
                    ..
                }) if token.trim().starts_with(service_principal::TOKEN_PREFIX) => {
                    let service_auth = tokio::time::timeout_at(
                        if pending_connection.is_some() {
                            auth_deadline
                        } else {
                            tokio::time::Instant::now()
                                + runtime.inner.config.public_functions.auth_timeout
                        },
                        runtime.authenticate_service_principal(
                            id,
                            &token,
                            project.as_deref(),
                            tenant.as_deref(),
                        ),
                    )
                    .await;
                    let Ok((response, grant)) = service_auth else {
                        break;
                    };
                    replicas.clear();
                    replica_opens.clear();
                    pending_replica_opens.clear();
                    snapshot_delivery.clear();
                    legacy_snapshot_delivery.clear();
                    live_queries.clear();
                    control_queries.clear();
                    feed = None;
                    feed_scheduler.reset();
                    sent_replica_revision = 0;
                    sent_zero_bootstrap_watermark = false;
                    tenant_session = None;
                    connected_client_contract = None;
                    control_connection = control::ControlConnection {
                        connection_id: connection_id.clone(),
                        project_id: grant
                            .as_ref()
                            .map(|grant| grant.route.project_id.clone())
                            .unwrap_or_default(),
                        ..control::ControlConnection::default()
                    };
                    service_grant = grant;
                    if service_grant.is_some() {
                        pending_connection.take();
                    }
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::Auth {
                    id,
                    client_contract,
                    token,
                    project,
                    tenant,
                    control_only,
                    public,
                    device,
                    capabilities,
                }) => {
                    service_grant = None;
                    let tenant = control_auth_tenant(control_only, capabilities.as_ref(), tenant);
                    let auth_result = tokio::time::timeout_at(
                        if pending_connection.is_some() {
                            auth_deadline
                        } else {
                            tokio::time::Instant::now()
                                + runtime.inner.config.public_functions.auth_timeout
                        },
                        authenticate(
                            &runtime,
                            id,
                            token.as_deref(),
                            project.as_deref(),
                            tenant.as_deref(),
                            AuthMode {
                                control_only,
                                public,
                            },
                            &connection_id,
                        ),
                    )
                    .await;
                    let Ok((mut response, authenticated_control)) = auth_result else {
                        break;
                    };
                    if let Some(session) = authenticated_control
                        .tenant
                        .as_ref()
                        .filter(|s| s.is_anonymous())
                    {
                        let scope = (
                            authenticated_control.project_id.clone(),
                            session.route.tenant_id.clone(),
                        );
                        if public_scope.as_ref().is_some_and(|pinned| {
                            !public_functions::same_scope(pinned, &scope.0, &scope.1)
                        }) {
                            let _ = send_connection_json(
                                &mut snapshot_delivery,
                                &mut socket,
                                &ServerMessage::AuthError {
                                    code: Some("transient".into()),
                                    id: match &response {
                                        ServerMessage::AuthResult { id, .. } => id.clone(),
                                        _ => String::new(),
                                    },
                                    error: "public connection scope is fixed".into(),
                                },
                            )
                            .await;
                            let _ = socket
                                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                    code: 1008,
                                    reason: "public connection scope is fixed".into(),
                                })))
                                .await;
                            break;
                        }
                        if public_scope.is_none() {
                            public_scope = Some(scope.clone());
                            let admission = public_functions::PublicAdmission::new();
                            public_admission = Some(admission.clone());
                            let monitor_runtime = runtime.clone();
                            _public_monitor = Some(admission.monitor(
                                std::time::Duration::from_secs(30),
                                move || {
                                    let runtime = monitor_runtime.clone();
                                    let scope = scope.clone();
                                    async move { validate_public_scope(&runtime, &scope).await }
                                },
                            ));
                        }
                    }
                    if authenticated_control.identity.is_some()
                        || authenticated_control.tenant.is_some()
                    {
                        pending_connection.take();
                    }
                    connected_client_contract = client_contract;
                    if let Some(module) = runtime
                        .inner
                        .modules
                        .project(&authenticated_control.project_id)
                        .await
                    {
                        if let Some(required) = module.client_contract {
                            if !control_only
                                && authenticated_control.tenant.is_some()
                                && client_contract != Some(required)
                            {
                                let _ = send_connection_json(
                                    &mut snapshot_delivery,
                                    &mut socket,
                                    &ServerMessage::ClientUpdateRequired {
                                        reason: "CLIENT_UPDATE_REQUIRED".into(),
                                        contract: required,
                                    },
                                )
                                .await;
                                break;
                            }
                        }
                    }
                    if control_only {
                        suppress_control_replica(&mut response);
                    }
                    replica_snapshot_pages_enabled =
                        snapshot_pages_negotiated(capabilities.as_ref());
                    replica_native_rows_enabled =
                        snapshot_delivery::negotiated(capabilities.as_ref());
                    snapshot_delivery.set_native(replica_native_rows_enabled);
                    replica_sum_digest_enabled =
                        capabilities.as_ref().and_then(|c| c.replica_set_digest) == Some(1);
                    if !replica_sum_digest_enabled {
                        for subscription in replicas.values_mut() {
                            subscription.require_legacy_digest();
                        }
                    }
                    telemetry_device = device.clone();
                    telemetry_batch_enabled =
                        capabilities.as_ref().and_then(|c| c.telemetry_batch) == Some(1);
                    let next_tenant_session =
                        tenant_data_session(control_only, &authenticated_control);
                    let preserved = replica_opens.is_empty()
                        && pending_replica_opens.is_empty()
                        && legacy_snapshot_delivery.is_empty()
                        && (replica_native_rows_enabled || snapshot_delivery.is_empty())
                        && !control_only
                        && tenant_session.is_some()
                        && preserve_subscription_scope(
                            &control_connection,
                            &authenticated_control,
                            capabilities.as_ref(),
                        );
                    if preserved {
                        if let ServerMessage::AuthResult {
                            result: serde_json::Value::Object(result),
                            ..
                        } = &mut response
                        {
                            result.insert(
                                "subscriptionsPreserved".into(),
                                serde_json::Value::Bool(true),
                            );
                        }
                    } else {
                        replicas.clear();
                        replica_opens.clear();
                        pending_replica_opens.clear();
                        snapshot_delivery.clear();
                        legacy_snapshot_delivery.clear();
                        live_queries.clear();
                        control_queries.clear();
                        if let Some(session) =
                            next_tenant_session.as_ref().filter(|s| !s.is_anonymous())
                        {
                            feed = Some(runtime.inner.change_feeds.subscribe(&session.route).await);
                        } else {
                            feed = None;
                        }
                        feed_scheduler.reset();
                        sent_replica_revision = 0;
                        sent_zero_bootstrap_watermark = false;
                    }
                    tenant_session = next_tenant_session;
                    control_connection = authenticated_control;
                    runtime.inner.metrics.authenticated(
                        &connection_id,
                        &control_connection,
                        device.as_ref(),
                    );
                    record_connection_subscriptions(
                        &runtime,
                        &connection_id,
                        &replicas,
                        &live_queries,
                        &control_queries,
                    );
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::QueryCall {
                    id,
                    path,
                    args,
                    scope,
                }) => {
                    runtime
                        .inner
                        .metrics
                        .activity(&connection_id, "Query", Some(&path));
                    let response = if scope == Some(ExecutionScope::Control) {
                        match runtime
                            .execute_control_query(&control_connection, &path, &args)
                            .await
                        {
                            Ok(result) => query_result(id, path, result),
                            Err(error) => ServerMessage::QueryError {
                                id,
                                path: Some(path),
                                error: error.to_string(),
                            },
                        }
                    } else if let Some(session) = tenant_session.as_ref() {
                        match runtime.execute_tenant_query(session, &path, args).await {
                            Ok(result) => query_result(id, path, result),
                            Err(error) => ServerMessage::QueryError {
                                id,
                                path: Some(path),
                                error: error.to_string(),
                            },
                        }
                    } else {
                        ServerMessage::QueryError {
                            id,
                            path: Some(path),
                            error: "authenticate with an active tenant before calling a Query"
                                .to_owned(),
                        }
                    };
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::QuerySubscribe {
                    id,
                    path,
                    args,
                    scope,
                    window_revision,
                }) => {
                    runtime.inner.metrics.activity(
                        &connection_id,
                        "Live Query subscribed",
                        Some(&path),
                    );
                    let (response, subscription) = if scope == Some(ExecutionScope::Control) {
                        match runtime
                            .open_control_query(&control_connection, id.clone(), path.clone(), args)
                            .await
                        {
                            Ok((message, opened)) => {
                                control_queries.insert(opened.id.clone(), opened);
                                (message, None)
                            }
                            Err(error) => (
                                ServerMessage::QueryError {
                                    id,
                                    path: Some(path),
                                    error: error.to_string(),
                                },
                                None,
                            ),
                        }
                    } else if let Some(session) = tenant_session.as_ref() {
                        match runtime
                            .open_live_query(session, id.clone(), path.clone(), args)
                            .await
                        {
                            Ok(opened) => (
                                live_query::resume_message(
                                    opened.message,
                                    window_revision.as_deref(),
                                ),
                                Some(opened.subscription),
                            ),
                            Err(error) => (
                                ServerMessage::QueryError {
                                    id,
                                    path: Some(path),
                                    error: error.to_string(),
                                },
                                None,
                            ),
                        }
                    } else {
                        (
                            ServerMessage::QueryError {
                                id,
                                path: Some(path),
                                error: "authenticate with an active tenant before subscribing to a Live Query".to_owned(),
                            },
                            None,
                        )
                    };
                    if let Some(subscription) = subscription {
                        sent_zero_bootstrap_watermark = false;
                        live_queries.insert(subscription.id.clone(), subscription);
                    }
                    record_connection_subscriptions(
                        &runtime,
                        &connection_id,
                        &replicas,
                        &live_queries,
                        &control_queries,
                    );
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::QuerySubscribeMany { subscribes }) => {
                    if subscribes.len() > 256 {
                        let response = ServerMessage::QueryError {
                            id: String::new(),
                            path: None,
                            error: "query batch cannot contain more than 256 subscribes".to_owned(),
                        };
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    for subscribe in subscribes {
                        let (response, subscription) = if subscribe.scope
                            == Some(ExecutionScope::Control)
                        {
                            match runtime
                                .open_control_query(
                                    &control_connection,
                                    subscribe.id.clone(),
                                    subscribe.path.clone(),
                                    subscribe.args,
                                )
                                .await
                            {
                                Ok((message, opened)) => {
                                    control_queries.insert(opened.id.clone(), opened);
                                    (message, None)
                                }
                                Err(error) => (
                                    ServerMessage::QueryError {
                                        id: subscribe.id,
                                        path: Some(subscribe.path),
                                        error: error.to_string(),
                                    },
                                    None,
                                ),
                            }
                        } else if let Some(session) = tenant_session.as_ref() {
                            match runtime
                                .open_live_query(
                                    session,
                                    subscribe.id.clone(),
                                    subscribe.path.clone(),
                                    subscribe.args,
                                )
                                .await
                            {
                                Ok(opened) => (
                                    live_query::resume_message(
                                        opened.message,
                                        subscribe.window_revision.as_deref(),
                                    ),
                                    Some(opened.subscription),
                                ),
                                Err(error) => (
                                    ServerMessage::QueryError {
                                        id: subscribe.id,
                                        path: Some(subscribe.path),
                                        error: error.to_string(),
                                    },
                                    None,
                                ),
                            }
                        } else {
                            (
                                ServerMessage::QueryError {
                                    id: subscribe.id,
                                    path: Some(subscribe.path),
                                    error: "authenticate with an active tenant before subscribing to a Live Query".to_owned(),
                                },
                                None,
                            )
                        };
                        if let Some(subscription) = subscription {
                            sent_zero_bootstrap_watermark = false;
                            live_queries.insert(subscription.id.clone(), subscription);
                        }
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    record_connection_subscriptions(
                        &runtime,
                        &connection_id,
                        &replicas,
                        &live_queries,
                        &control_queries,
                    );
                }
                Ok(ClientMessage::QueryUnsubscribe { id }) => {
                    live_queries.remove(&id);
                    control_queries.remove(&id);
                    record_connection_subscriptions(
                        &runtime,
                        &connection_id,
                        &replicas,
                        &live_queries,
                        &control_queries,
                    );
                }
                Ok(ClientMessage::ReducerCall(call)) => {
                    let call_path = call.path.clone();
                    let call_id = call.id.clone();
                    let call_started = std::time::Instant::now();
                    tracing::debug!(
                        target: "gonvex_runtime::ws_trace",
                        connection = %connection_id,
                        path = %call_path,
                        id = %call_id,
                        "websocket reducer call started"
                    );
                    runtime
                        .inner
                        .metrics
                        .activity(&connection_id, "Reducer", Some(&call.path));
                    let control_write = call.scope == Some(ExecutionScope::Control);
                    let response =
                        call_reducer(&runtime, tenant_session.as_ref(), &control_connection, call)
                            .await;
                    let terminal_watermark = call_watermark_after_sent_replica_work(
                        &response,
                        !control_write && tenant_session.is_some(),
                        replicas
                            .values()
                            .map(|s| s.cursor.revision)
                            .chain(live_queries.values().map(|s| s.computed_revision))
                            .min()
                            .map(|revision| revision.max(sent_replica_revision)),
                    );
                    let control_succeeded =
                        control_write && matches!(response, ServerMessage::ReducerResult { .. });
                    let control_updates = if control_succeeded {
                        runtime
                            .refresh_control_queries(
                                &control_connection,
                                &mut control_queries,
                                "control-change",
                            )
                            .await
                    } else {
                        Vec::new()
                    };
                    for message in
                        ordered_control_completion(response, control_updates, control_succeeded)
                    {
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &message)
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    if let Some(watermark) = terminal_watermark {
                        let frames =
                            if own_membership_changed(&runtime, tenant_session.as_ref()).await {
                                discard_tenant_session!();
                                membership_changed_frames(Some(watermark))
                            } else {
                                vec![watermark]
                            };
                        if send_connection_all(&mut snapshot_delivery, &mut socket, &frames)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    if control_succeeded {
                        runtime.notify_control_changed(&control_connection.project_id);
                    }
                    tracing::debug!(
                        target: "gonvex_runtime::ws_trace",
                        connection = %connection_id,
                        path = %call_path,
                        id = %call_id,
                        call_ms = call_started.elapsed().as_secs_f64() * 1_000.0,
                        "websocket reducer call finished"
                    );
                }
                Ok(ClientMessage::ReducerCallMany { calls }) => {
                    if calls.len() > 256 {
                        let response = ReducerErrorClass::Rejected.reducer_error(
                            String::new(),
                            None,
                            "reducer batch cannot contain more than 256 calls".to_owned(),
                            None,
                        );
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    for call in calls {
                        if let (Some(admission), Some(scope)) = (&public_admission, &public_scope) {
                            if !check_public_admission(&runtime, admission, scope).await {
                                let response = ReducerErrorClass::Rejected.reducer_error(
                                    call.id,
                                    Some(call.path),
                                    "public tenant session is unavailable".into(),
                                    None,
                                );
                                if send_connection_json(
                                    &mut snapshot_delivery,
                                    &mut socket,
                                    &response,
                                )
                                .await
                                .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                        }
                        runtime
                            .inner
                            .metrics
                            .activity(&connection_id, "Reducer", Some(&call.path));
                        let control_write = call.scope == Some(ExecutionScope::Control);
                        let response = call_reducer(
                            &runtime,
                            tenant_session.as_ref(),
                            &control_connection,
                            call,
                        )
                        .await;
                        let terminal_watermark = call_watermark_after_sent_replica_work(
                            &response,
                            !control_write && tenant_session.is_some(),
                            replicas
                                .values()
                                .map(|s| s.cursor.revision)
                                .chain(live_queries.values().map(|s| s.computed_revision))
                                .min()
                                .map(|revision| revision.max(sent_replica_revision)),
                        );
                        let control_succeeded = control_write
                            && matches!(response, ServerMessage::ReducerResult { .. });
                        let control_updates = if control_succeeded {
                            runtime
                                .refresh_control_queries(
                                    &control_connection,
                                    &mut control_queries,
                                    "control-change",
                                )
                                .await
                        } else {
                            Vec::new()
                        };
                        for message in
                            ordered_control_completion(response, control_updates, control_succeeded)
                        {
                            if send_connection_json(&mut snapshot_delivery, &mut socket, &message)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        if let Some(watermark) = terminal_watermark {
                            let frames = if own_membership_changed(
                                &runtime,
                                tenant_session.as_ref(),
                            )
                            .await
                            {
                                discard_tenant_session!();
                                membership_changed_frames(Some(watermark))
                            } else {
                                vec![watermark]
                            };
                            if send_connection_all(&mut snapshot_delivery, &mut socket, &frames)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        if control_succeeded {
                            runtime.notify_control_changed(&control_connection.project_id);
                        }
                    }
                }
                Ok(ClientMessage::ActionCall {
                    id,
                    path,
                    args,
                    scope,
                    trace,
                    idempotency_key,
                }) => {
                    runtime
                        .inner
                        .metrics
                        .activity(&connection_id, "Action", Some(&path));
                    let response = if scope == Some(ExecutionScope::Control) {
                        match runtime
                            .execute_control_action(
                                &control_connection,
                                &path,
                                &args,
                                idempotency_key.as_deref().unwrap_or(""),
                            )
                            .await
                        {
                            Ok(result) => ServerMessage::ActionResult {
                                id,
                                path: Some(path),
                                result,
                                committed_revision: None,
                                trace,
                            },
                            Err(error) => ServerMessage::ActionError {
                                id,
                                path: Some(path),
                                error: error.to_string(),
                                trace,
                            },
                        }
                    } else if let Some(session) = tenant_session.as_ref() {
                        match runtime.execute_tenant_action(session, &path, args).await {
                            Ok(result) => ServerMessage::ActionResult {
                                id,
                                path: Some(path),
                                result: result.value,
                                committed_revision: result.committed_revision,
                                trace,
                            },
                            Err(error) => ServerMessage::ActionError {
                                id,
                                path: Some(path),
                                error: error.to_string(),
                                trace,
                            },
                        }
                    } else {
                        ServerMessage::ActionError {
                            id,
                            path: Some(path),
                            error: "authenticate with an active tenant before calling an Action"
                                .to_owned(),
                            trace,
                        }
                    };
                    let terminal_watermark = call_watermark_after_sent_replica_work(
                        &response,
                        scope != Some(ExecutionScope::Control) && tenant_session.is_some(),
                        replicas
                            .values()
                            .map(|s| s.cursor.revision)
                            .chain(live_queries.values().map(|s| s.computed_revision))
                            .min()
                            .map(|revision| revision.max(sent_replica_revision)),
                    );
                    let control_succeeded = scope == Some(ExecutionScope::Control)
                        && matches!(response, ServerMessage::ActionResult { .. });
                    let control_updates = if control_succeeded {
                        runtime
                            .refresh_control_queries(
                                &control_connection,
                                &mut control_queries,
                                "control-change",
                            )
                            .await
                    } else {
                        Vec::new()
                    };
                    for message in
                        ordered_control_completion(response, control_updates, control_succeeded)
                    {
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &message)
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    if let Some(watermark) = terminal_watermark {
                        let frames =
                            if own_membership_changed(&runtime, tenant_session.as_ref()).await {
                                discard_tenant_session!();
                                membership_changed_frames(Some(watermark))
                            } else {
                                vec![watermark]
                            };
                        if send_connection_all(&mut snapshot_delivery, &mut socket, &frames)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    if control_succeeded {
                        runtime.notify_control_changed(&control_connection.project_id);
                    }
                }
                Ok(ClientMessage::ReplicaOpen(request)) => {
                    sent_zero_bootstrap_watermark = false;
                    replica_opens.remove(&request.id);
                    snapshot_delivery.remove(&request.id);
                    legacy_snapshot_delivery.remove(&request.id);
                    replicas.remove(&request.id);
                    pending_replica_opens.push_back(request);
                }
                Ok(ClientMessage::ReplicaOpenMany { opens }) => {
                    if opens.len() > REPLICA_OPEN_BATCH_LIMIT {
                        let response = ServerMessage::ReplicaError {id:String::new(),path:None,error:format!("replica batch cannot contain more than {REPLICA_OPEN_BATCH_LIMIT} opens")};
                        if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    sent_zero_bootstrap_watermark = false;
                    for request in opens {
                        replica_opens.remove(&request.id);
                        snapshot_delivery.remove(&request.id);
                        legacy_snapshot_delivery.remove(&request.id);
                        replicas.remove(&request.id);
                        // Only the last open for an ID in this frame can publish.
                        pending_replica_opens.retain(|pending| pending.id != request.id);
                        pending_replica_opens.push_back(request);
                    }
                }
                Ok(ClientMessage::ReplicaSnapshotAck {
                    id,
                    snapshot_id,
                    sequence,
                }) => {
                    let outcome = if replica_native_rows_enabled && tenant_session.is_some() {
                        snapshot_delivery.acknowledge(&id, &snapshot_id, sequence)
                    } else {
                        Err("native snapshot acknowledgements were not negotiated")
                    };
                    match outcome {
                        Ok(ready) => {
                            if let Some(ready) = ready {
                                if send_connection_json(&mut snapshot_delivery, &mut socket, &ready)
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if send_snapshot_frames(&mut socket, &mut snapshot_delivery)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(error) => {
                            snapshot_delivery.remove(&id);
                            replicas.remove(&id);
                            if send_snapshot_frames(&mut socket, &mut snapshot_delivery)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            if send_json(
                                &mut socket,
                                &ServerMessage::ReplicaError {
                                    id,
                                    path: None,
                                    error: error.into(),
                                },
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                Ok(ClientMessage::ReplicaClose { id }) => {
                    replica_opens.remove(&id);
                    legacy_snapshot_delivery.remove(&id);
                    replicas.remove(&id);
                    // Confirmation precedes pumping any later binary frames.
                    if let Some(closed) = snapshot_delivery.close_subscription(&id) {
                        if send_json(&mut socket, &closed).await.is_err() {
                            return;
                        }
                    }
                    if send_snapshot_frames(&mut socket, &mut snapshot_delivery)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    record_connection_subscriptions(
                        &runtime,
                        &connection_id,
                        &replicas,
                        &live_queries,
                        &control_queries,
                    );
                }
                Ok(ClientMessage::ErrorRegister {
                    id,
                    release,
                    environment,
                }) => {
                    let response = runtime
                        .register_error_session(
                            &control_connection,
                            id,
                            release.as_deref(),
                            environment.as_deref(),
                        )
                        .await;
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::ErrorHeartbeat { id }) => {
                    let response = runtime
                        .register_error_session(&control_connection, id, None, None)
                        .await;
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::ErrorEnvelope { id, events }) => {
                    let response = runtime
                        .capture_error_envelope(&control_connection, id, events)
                        .await;
                    if send_connection_json(&mut snapshot_delivery, &mut socket, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(ClientMessage::TelemetryBatch { events }) => {
                    if telemetry_batch_enabled && events.len() <= 50 {
                        for event in events {
                            runtime
                                .capture_performance_event(
                                    &control_connection,
                                    &event.id,
                                    &event.kind,
                                    &event.path,
                                    event.reason.as_deref(),
                                    &event.outcome,
                                    event.error.as_deref(),
                                    event.client_sent_at_ms,
                                    event.client_received_at_ms,
                                    event.client_duration_ms,
                                    event.trace.as_ref(),
                                    event.device.as_ref().or(telemetry_device.as_ref()),
                                )
                                .await;
                        }
                    }
                }
                Ok(ClientMessage::TelemetryEvent {
                    id,
                    kind,
                    path,
                    reason,
                    outcome,
                    error,
                    client_sent_at_ms,
                    client_received_at_ms,
                    client_duration_ms,
                    trace,
                    device,
                }) => {
                    runtime
                        .capture_performance_event(
                            &control_connection,
                            &id,
                            &kind,
                            &path,
                            reason.as_deref(),
                            &outcome,
                            error.as_deref(),
                            client_sent_at_ms,
                            client_received_at_ms,
                            client_duration_ms,
                            trace.as_ref(),
                            device.as_ref().or(telemetry_device.as_ref()),
                        )
                        .await;
                }
                Err(_) => {
                    let _ = socket
                        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1003,
                            reason: "invalid Gonvex protocol frame".into(),
                        })))
                        .await;
                    break;
                }
            },
            Message::Ping(value) => {
                if socket.send(Message::Pong(value)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
        if replica_native_rows_enabled && tenant_session.is_some() {
            if let Some(watermark) = bootstrap_watermark_after_sent_snapshots(
                snapshot_delivery.is_empty(),
                replicas
                    .values()
                    .map(|s| s.cursor.revision)
                    .chain(live_queries.values().map(|s| s.computed_revision)),
                sent_replica_revision,
                sent_zero_bootstrap_watermark,
            ) {
                if send_connection_json(&mut snapshot_delivery, &mut socket, &watermark)
                    .await
                    .is_err()
                {
                    return;
                }
                if let ServerMessage::ReplicaWatermark { revision } = watermark {
                    sent_replica_revision = sent_replica_revision.max(revision);
                    if revision == 0 {
                        sent_zero_bootstrap_watermark = true;
                    }
                }
            }
        }
    }
}

async fn validate_public_scope(runtime: &Runtime, scope: &(String, String)) -> bool {
    match runtime.inner.control_plane.read().await.clone() {
        Some(control) => tokio::time::timeout(
            std::time::Duration::from_secs(5),
            control.resolve_public_tenant(&scope.0, &scope.1),
        )
        .await
        .is_ok_and(|result| result.is_ok()),
        None => false,
    }
}

async fn check_public_admission(
    runtime: &Runtime,
    admission: &public_functions::PublicAdmission,
    scope: &(String, String),
) -> bool {
    admission
        .check(std::time::Duration::from_secs(30), || {
            validate_public_scope(runtime, scope)
        })
        .await
}

fn public_admission_errors(frame: ClientMessage) -> Vec<ServerMessage> {
    public_limit_errors(frame)
        .into_iter()
        .map(|mut response| {
            match &mut response {
                ServerMessage::AuthError { error, .. }
                | ServerMessage::QueryError { error, .. }
                | ServerMessage::ActionError { error, .. }
                | ServerMessage::ReducerError { error, .. }
                | ServerMessage::ReplicaError { error, .. } => {
                    *error = "public tenant session is unavailable".into()
                }
                _ => {}
            }
            response
        })
        .collect()
}

async fn revalidate_connection(runtime: &Runtime, connection: &control::ControlConnection) -> bool {
    if let Some(session) = connection
        .tenant
        .as_ref()
        .filter(|session| session.is_anonymous())
    {
        return match runtime.inner.control_plane.read().await.clone() {
            Some(control) => control
                .resolve_public_tenant(&connection.project_id, &session.route.tenant_id)
                .await
                .is_ok(),
            None => false,
        };
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return false;
    };
    let project = (!connection.project_id.is_empty()).then_some(connection.project_id.as_str());
    let tenant = connection
        .tenant
        .as_ref()
        .map(|session| session.route.tenant_id.as_str());
    if !connection.impersonation_id.is_empty() {
        return control
            .validate_impersonation_session(
                &connection.auth_token,
                project.unwrap_or_default(),
                tenant.unwrap_or_default(),
                &connection.connection_id,
            )
            .await
            .is_ok();
    }
    if let Some(tenant) = tenant {
        control
            .authenticate_session(&connection.auth_token, project, Some(tenant))
            .await
            .is_ok()
    } else {
        control
            .load_session_identity(&connection.auth_token, project)
            .await
            .is_ok()
    }
}

impl Runtime {
    async fn start_control_event_listener(&self) -> Result<(), gonvex_postgres::DatabaseError> {
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return Ok(());
        };
        let mut listener = control
            .listener(&["gonvex_control_changed", "gonvex_support_command"])
            .await?;
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                let notification = match listener.recv().await {
                    Ok(notification) => notification,
                    Err(error) => {
                        tracing::error!(%error, "Control Plane event listener stopped");
                        return;
                    }
                };
                match notification.channel() {
                    "gonvex_control_changed" => {
                        let project_id = notification.payload().trim();
                        if !project_id.is_empty() {
                            let _ =
                                runtime
                                    .inner
                                    .runtime_events
                                    .send(RuntimeEvent::ControlChanged {
                                        project_id: project_id.to_owned(),
                                    });
                        }
                    }
                    "gonvex_support_command" => {
                        if let Ok(payload) =
                            serde_json::from_str::<serde_json::Value>(notification.payload())
                        {
                            let project =
                                payload.get("projectId").and_then(serde_json::Value::as_str);
                            let command =
                                payload.get("commandId").and_then(serde_json::Value::as_str);
                            if let (Some(project), Some(command)) = (project, command) {
                                if let Err(error) = runtime
                                    .deliver_support_command(&control, project, command)
                                    .await
                                {
                                    tracing::warn!(%error, "failed to deliver support command");
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }

    pub(crate) fn notify_control_changed(&self, project_id: &str) {
        if !project_id.is_empty() {
            let _ = self
                .inner
                .runtime_events
                .send(RuntimeEvent::ControlChanged {
                    project_id: project_id.to_owned(),
                });
            let runtime = self.clone();
            let project_id = project_id.to_owned();
            tokio::spawn(async move {
                if let Some(control) = runtime.inner.control_plane.read().await.clone() {
                    let _ = control.notify("gonvex_control_changed", &project_id).await;
                }
            });
        }
    }
}

#[derive(Default)]
struct FeedFirstScheduler {
    remaining_snapshot_feed_events: usize,
    socket_turn: bool,
}

impl FeedFirstScheduler {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn selected_first_feed(&mut self, additional_ready_feed_events: usize) {
        self.remaining_snapshot_feed_events = additional_ready_feed_events;
        self.socket_turn = additional_ready_feed_events == 0;
    }

    fn selected_snapshot_feed(&mut self) {
        self.remaining_snapshot_feed_events = self.remaining_snapshot_feed_events.saturating_sub(1);
        if self.remaining_snapshot_feed_events == 0 {
            self.socket_turn = true;
        }
    }

    fn selected_socket(&mut self) {
        self.socket_turn = false;
    }
}

enum ScheduledTransport<T> {
    Socket(Option<T>),
    Feed(Result<change_feed::FeedEvent, broadcast::error::RecvError>),
}

/// Select connection work in stable commit order without letting a busy tenant
/// feed monopolize the socket. The first ready feed event wins over a queued
/// socket message. We then drain only the additional events that were already
/// queued at that instant. New feed traffic does not extend that snapshot, and
/// the socket gets the next priority turn.
async fn next_socket_or_feed<S>(
    socket: &mut S,
    receiver: &mut Option<broadcast::Receiver<change_feed::FeedEvent>>,
    scheduler: &mut FeedFirstScheduler,
) -> ScheduledTransport<S::Item>
where
    S: Stream + Unpin,
{
    if scheduler.remaining_snapshot_feed_events > 0 {
        let event = next_feed_event(receiver).await;
        scheduler.selected_snapshot_feed();
        return ScheduledTransport::Feed(event);
    }

    if scheduler.socket_turn {
        tokio::select! {
            biased;
            message = socket.next() => {
                scheduler.selected_socket();
                ScheduledTransport::Socket(message)
            }
            event = next_feed_event(receiver) => {
                let additional_ready = receiver.as_ref().map_or(0, broadcast::Receiver::len);
                scheduler.selected_first_feed(additional_ready);
                ScheduledTransport::Feed(event)
            }
        }
    } else {
        tokio::select! {
            biased;
            event = next_feed_event(receiver) => {
                let additional_ready = receiver.as_ref().map_or(0, broadcast::Receiver::len);
                scheduler.selected_first_feed(additional_ready);
                ScheduledTransport::Feed(event)
            }
            message = socket.next() => {
                scheduler.selected_socket();
                ScheduledTransport::Socket(message)
            }
        }
    }
}

async fn next_feed_event(
    receiver: &mut Option<broadcast::Receiver<change_feed::FeedEvent>>,
) -> Result<change_feed::FeedEvent, broadcast::error::RecvError> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// A quiet tenant may never emit a change-feed event after authentication.
/// Close its initial cohort once all native snapshots have been durably ACKed.
/// The oldest subscribed snapshot is the only safe boundary: a newer snapshot
/// cannot certify changes still queued for an older subscription.
fn bootstrap_watermark_after_sent_snapshots(
    snapshots_complete: bool,
    revisions: impl Iterator<Item = u64>,
    sent_revision: u64,
    zero_already_sent: bool,
) -> Option<ServerMessage> {
    if !snapshots_complete {
        return None;
    }
    let revision = revisions.min()?;
    (revision > sent_revision || revision == 0 && sent_revision == 0 && !zero_already_sent)
        .then_some(ServerMessage::ReplicaWatermark { revision })
}

fn replica_watermark(event: &change_feed::FeedEvent) -> Option<ServerMessage> {
    match event {
        change_feed::FeedEvent::Transaction { revision, .. } => {
            Some(ServerMessage::ReplicaWatermark {
                revision: *revision,
            })
        }
        change_feed::FeedEvent::Reset { .. } => None,
    }
}

/// Complete a durable call when every subscribed snapshot has already been sent
/// through its revision. An idempotent retry can return an old commit without a
/// new feed event, particularly after reconnect. Waiting for that event would
/// deadlock the outbox. New commits still wait for all feed frames to be sent.
/// With no subscriptions there is no replica work to reconcile.
fn call_watermark_after_sent_replica_work(
    response: &ServerMessage,
    is_authenticated_tenant_call: bool,
    lowest_sent_revision: Option<u64>,
) -> Option<ServerMessage> {
    if !is_authenticated_tenant_call {
        return None;
    }
    let committed_revision = match response {
        ServerMessage::ReducerResult {
            committed_revision, ..
        }
        | ServerMessage::ActionResult {
            committed_revision, ..
        } => *committed_revision,
        _ => None,
    }?;
    if lowest_sent_revision.is_some_and(|sent| sent < committed_revision) {
        return None;
    }
    Some(ServerMessage::ReplicaWatermark {
        revision: committed_revision,
    })
}

/// Frames that close a tenant session whose own membership changed.
///
/// The session and every subscription are discarded, so none of the
/// transaction's Replica or Live Query frames are sent. A reducer or Action
/// on this connection may have committed that very transaction (granting the
/// caller permissions, for example) and already received its result; clients
/// complete such calls only at a replica watermark at or past the committed
/// revision. The connection holds no subscription after the auth error, so
/// the watermark trivially holds and must follow it. Without it the call never
/// completes: the next session's feed starts after this revision.
fn membership_changed_frames(transaction_watermark: Option<ServerMessage>) -> Vec<ServerMessage> {
    let mut frames = vec![ServerMessage::AuthError {
        code: Some("transient".into()),
        id: "membership-changed".to_owned(),
        error: "tenant membership changed; authenticate again".to_owned(),
    }];
    frames.extend(transaction_watermark);
    frames
}

fn membership_affects(session: &TenantSession, event: &change_feed::FeedEvent) -> bool {
    let change_feed::FeedEvent::Transaction {
        revision, changes, ..
    } = event
    else {
        return false;
    };
    if *revision <= session.admission_revision {
        return false;
    }
    changes
        .iter()
        .filter(|change| {
            change.table == "members"
                && change.changed_columns.iter().any(|column| {
                    matches!(
                        column.as_str(),
                        "account_id" | "status" | "role" | "permissions" | "membership_revision"
                    )
                })
        })
        .any(|change| {
            [&change.old_value, &change.new_value].iter().any(|row| {
                row.get("account_id").and_then(serde_json::Value::as_str)
                    == Some(session.identity.account.id.as_str())
                    || row.get("id").and_then(serde_json::Value::as_str)
                        == Some(session.member.id.as_str())
            })
        })
}

/// Whether a commit on this connection changed the session's own Member.
///
/// `call_watermark_after_sent_replica_work` completes a call immediately when
/// the connection has no subscription waiting on the change feed. If that
/// call changed the caller's own Member (granting it permissions, say), the
/// feed would close the session only after the client had already seen the
/// call complete, so the client's next call could arrive on the closed
/// session and fail as unauthenticated. Reading the Member before sending
/// that watermark closes the session first, in order. A failed read falls
/// back to the change feed, which still closes the session.
async fn own_membership_changed(runtime: &Runtime, session: Option<&TenantSession>) -> bool {
    let Some(session) = session else {
        return false;
    };
    // Service principal sessions have no Member row.
    if session.is_anonymous() || session.member.id.starts_with("_gonvex_service:") {
        return false;
    }
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return false;
    };
    match control
        .member_by_id(&session.route, &session.member.id)
        .await
    {
        Ok(current) => member_access_changed(&session.member, current.as_ref()),
        Err(error) => {
            tracing::warn!(%error, "could not re-read the session Member after a commit");
            false
        }
    }
}

/// The columns `membership_affects` treats as an access change.
fn member_access_changed(
    admitted: &gonvex_postgres::Member,
    current: Option<&gonvex_postgres::Member>,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    admitted.account_id != current.account_id
        || admitted.status != current.status
        || admitted.role != current.role
        || admitted.permissions != current.permissions
        || admitted.membership_revision != current.membership_revision
}

fn public_limit_errors(frame: ClientMessage) -> Vec<ServerMessage> {
    let error = "public call rate limit exceeded".to_owned();
    match frame {
        ClientMessage::Auth { id, .. } => vec![ServerMessage::AuthError {
            code: Some("transient".into()),
            id,
            error,
        }],
        ClientMessage::QueryCall { id, path, .. }
        | ClientMessage::QuerySubscribe { id, path, .. } => vec![ServerMessage::QueryError {
            id,
            path: Some(path),
            error,
        }],
        ClientMessage::ReducerCall(call) => {
            vec![ReducerErrorClass::Rejected.reducer_error(call.id, Some(call.path), error, None)]
        }
        ClientMessage::ReducerCallMany { calls } => calls
            .into_iter()
            .map(|call| {
                ReducerErrorClass::Rejected.reducer_error(
                    call.id,
                    Some(call.path),
                    error.clone(),
                    None,
                )
            })
            .collect(),
        ClientMessage::QuerySubscribeMany { subscribes } => subscribes
            .into_iter()
            .map(|call| ServerMessage::QueryError {
                id: call.id,
                path: Some(call.path),
                error: error.clone(),
            })
            .collect(),
        ClientMessage::ReplicaOpen(call) => vec![ServerMessage::ReplicaError {
            id: call.id,
            path: Some(call.path),
            error,
        }],
        ClientMessage::ReplicaOpenMany { opens } => opens
            .into_iter()
            .map(|call| ServerMessage::ReplicaError {
                id: call.id,
                path: Some(call.path),
                error: error.clone(),
            })
            .collect(),
        ClientMessage::ActionCall { id, path, .. } => vec![ServerMessage::ActionError {
            id,
            path: Some(path),
            error,
            trace: None,
        }],
        _ => vec![ServerMessage::AuthError {
            code: Some("transient".into()),
            id: "public-rate-limit".into(),
            error,
        }],
    }
}

fn control_auth_tenant(
    control_only: bool,
    capabilities: Option<&gonvex_protocol::ClientCapabilities>,
    tenant: Option<String>,
) -> Option<String> {
    if !control_only {
        return tenant;
    }
    if capabilities.and_then(|c| c.tenant_control) == Some(1) {
        tenant.filter(|value| !value.trim().is_empty())
    } else {
        None
    }
}

fn tenant_data_session(
    control_only: bool,
    connection: &control::ControlConnection,
) -> Option<TenantSession> {
    if control_only {
        None
    } else {
        connection.tenant.clone()
    }
}

fn suppress_control_replica(response: &mut ServerMessage) {
    if let ServerMessage::AuthResult { result, .. } = response {
        if let Some(result) = result.as_object_mut() {
            result.remove("replica");
            result.remove("localIdentity");
        }
    }
}

#[derive(Clone, Copy)]
struct AuthMode {
    control_only: bool,
    public: bool,
}

fn database_auth_error_code(error: &gonvex_postgres::DatabaseError) -> &'static str {
    use gonvex_postgres::DatabaseError;
    match error {
        DatabaseError::InvalidSession | DatabaseError::SessionProjectMismatch => {
            "credential_rejected"
        }
        DatabaseError::MemberNotFound(_) | DatabaseError::NoTenantMembership => "member_removed",
        _ => "transient",
    }
}

async fn authenticate(
    runtime: &Runtime,
    id: String,
    token: Option<&str>,
    project: Option<&str>,
    tenant: Option<&str>,
    mode: AuthMode,
    connection_id: &str,
) -> (ServerMessage, control::ControlConnection) {
    let AuthMode {
        control_only,
        public,
    } = mode;
    let control = runtime.inner.control_plane.read().await.clone();
    let Some(control) = control else {
        return (
            ServerMessage::AuthError {
                code: Some("transient".into()),
                id,
                error: "auth session store is unavailable".to_owned(),
            },
            control::ControlConnection::default(),
        );
    };
    let requested_project = project.map(str::trim).filter(|value| !value.is_empty());
    let token = token.map(str::trim).filter(|value| !value.is_empty());
    if public {
        let route = if token.is_none() && !control_only {
            if let (Some(project), Some(tenant)) =
                (requested_project, tenant.filter(|v| !v.trim().is_empty()))
            {
                control
                    .resolve_public_tenant(project, tenant.trim())
                    .await
                    .ok()
            } else {
                None
            }
        } else {
            None
        };
        let Some(route) = route else {
            return (
                ServerMessage::AuthError {
                    code: Some("transient".into()),
                    id,
                    error: "public tenant session is unavailable".to_owned(),
                },
                control::ControlConnection::default(),
            );
        };
        let artifact_hash = runtime
            .inner
            .modules
            .project(&route.project_id)
            .await
            .map(|module| module.artifact_hash.clone())
            .unwrap_or_default();
        let result = serde_json::json!({"projectId":route.project_id,"tenantId":route.tenant_id,"accountId":"","public":true,"artifactHash":artifact_hash});
        return (
            ServerMessage::AuthResult { id, result },
            control::ControlConnection {
                connection_id: connection_id.to_owned(),
                project_id: route.project_id.clone(),
                tenant: Some(TenantSession::anonymous(route)),
                ..Default::default()
            },
        );
    }
    if control_only && token.is_none() {
        let Some(project) = requested_project else {
            return (
                ServerMessage::AuthError {
                    code: Some("transient".into()),
                    id,
                    error: "project is required for public Control Plane calls".to_owned(),
                },
                control::ControlConnection::default(),
            );
        };
        return match control.project(project).await {
            Ok(_) => (
                ServerMessage::AuthResult {
                    id,
                    result: serde_json::json!({
                        "projectId": project,
                        "accountId": "",
                        "tenantId": "",
                    }),
                },
                control::ControlConnection {
                    connection_id: connection_id.to_owned(),
                    project_id: project.to_owned(),
                    ..control::ControlConnection::default()
                },
            ),
            Err(error) => (
                ServerMessage::AuthError {
                    code: Some(database_auth_error_code(&error).into()),
                    id,
                    error: error.to_string(),
                },
                control::ControlConnection::default(),
            ),
        };
    }
    let Some(token) = token else {
        return (
            ServerMessage::AuthError {
                code: Some("transient".into()),
                id,
                error: "a Gonvex app session is required".to_owned(),
            },
            control::ControlConnection::default(),
        );
    };
    if token.starts_with("gvx_imp_") || token.starts_with("gvx_dev_") {
        if control_only && tenant.is_none() {
            return (
                ServerMessage::AuthError {
                    code: Some("transient".into()),
                    id,
                    error: "developer grants require an active tenant".to_owned(),
                },
                control::ControlConnection::default(),
            );
        }
        return match control
            .authenticate_impersonation(token, requested_project, tenant, connection_id)
            .await
        {
            Ok(impersonation) => {
                let session = impersonation.tenant;
                let module = runtime
                    .inner
                    .modules
                    .project(&session.identity.project_id)
                    .await;
                let artifact_hash = module
                    .as_ref()
                    .map(|module| module.artifact_hash.clone())
                    .unwrap_or_default();
                let replica = module.filter(|_| !control_only).map(|module| {
                    module.replica_directive(
                        &session.route.tenant_id,
                        &session.route.database_url,
                        &session.identity.account.id,
                        &session.member.permissions,
                    )
                });
                let connection = control::ControlConnection {
                    connection_id: connection_id.to_owned(),
                    project_id: session.identity.project_id.clone(),
                    identity: Some(session.identity.clone()),
                    tenant: Some(session.clone()),
                    impersonation_id: impersonation.grant_id.clone(),
                    auth_token: impersonation.reconnect_token.clone(),
                };
                (
                    ServerMessage::AuthResult {
                        id,
                        result: serde_json::json!({
                            "projectId":session.identity.project_id,
                            "accountId":session.identity.account.id,
                            "tenantId":session.route.tenant_id,
                            "impersonationId":impersonation.grant_id,
                            "impersonatorId":impersonation.actor_account_id,
                            "developerSessionToken":impersonation.reconnect_token,
                            "localIdentity": {
                                "auth": { "account": {
                                    "id": session.identity.account.id,
                                    "email": session.identity.account.email,
                                    "name": session.identity.account.name,
                                    "avatarUrl": session.identity.account.avatar_url,
                                }, "developerSession": session.developer_session },
                                "tenant": { "id": session.route.tenant_id },
                                "member": {
                                    "id": session.member.id, "accountId": session.member.account_id,
                                    "status": session.member.status, "role": session.member.role,
                                    "displayName": session.member.display_name, "permissions": session.member.permissions,
                                },
                            },
                            "artifactHash":artifact_hash,
                            "replica":replica,
                        }),
                    },
                    connection,
                )
            }
            Err(error) => (
                ServerMessage::AuthError {
                    code: Some(database_auth_error_code(&error).into()),
                    id,
                    error: format!(
                        "impersonation grant is invalid, expired, revoked, or already used: {error}"
                    ),
                },
                control::ControlConnection::default(),
            ),
        };
    }
    if control_only && tenant.is_none() {
        return match control
            .load_session_identity(token, requested_project)
            .await
        {
            Ok(identity) => {
                let project_id = identity.project_id.clone();
                let account_id = identity.account.id.clone();
                let artifact_hash = runtime
                    .inner
                    .modules
                    .project(&project_id)
                    .await
                    .map(|module| module.artifact_hash.clone())
                    .unwrap_or_default();
                (
                    ServerMessage::AuthResult {
                        id,
                        result: serde_json::json!({
                            "projectId": project_id,
                            "accountId": account_id,
                            "tenantId": "",
                            "artifactHash": artifact_hash,
                        }),
                    },
                    control::ControlConnection {
                        connection_id: connection_id.to_owned(),
                        project_id,
                        identity: Some(identity),
                        tenant: None,
                        impersonation_id: String::new(),
                        auth_token: token.to_owned(),
                    },
                )
            }
            Err(error) => (
                ServerMessage::AuthError {
                    code: Some(database_auth_error_code(&error).into()),
                    id,
                    error: error.to_string(),
                },
                control::ControlConnection::default(),
            ),
        };
    }
    match control
        .authenticate_session(token, requested_project, tenant)
        .await
    {
        Ok(session) => {
            let module = runtime
                .inner
                .modules
                .project(&session.identity.project_id)
                .await;
            let artifact_hash = module
                .as_ref()
                .map(|module| module.artifact_hash.clone())
                .unwrap_or_default();
            let replica = module.filter(|_| !control_only).map(|module| {
                module.replica_directive(
                    &session.route.tenant_id,
                    &session.route.database_url,
                    &session.identity.account.id,
                    &session.member.permissions,
                )
            });
            let connection = control::ControlConnection {
                connection_id: connection_id.to_owned(),
                project_id: session.identity.project_id.clone(),
                identity: Some(session.identity.clone()),
                tenant: Some(session.clone()),
                impersonation_id: String::new(),
                auth_token: token.to_owned(),
            };
            (
                ServerMessage::AuthResult {
                    id,
                    result: serde_json::json!({
                        "projectId": session.identity.project_id,
                        "accountId": session.identity.account.id,
                        "tenantId": session.route.tenant_id,
                        "artifactHash": artifact_hash,
                        "replica": replica,
                        "localIdentity": {
                            "auth": { "account": {
                                "id": session.identity.account.id,
                                "email": session.identity.account.email,
                                "name": session.identity.account.name,
                                "avatarUrl": session.identity.account.avatar_url,
                            } },
                            "tenant": { "id": session.route.tenant_id },
                            "member": {
                                "id": session.member.id,
                                "accountId": session.member.account_id,
                                "status": session.member.status,
                                "role": session.member.role,
                                "displayName": session.member.display_name,
                                "permissions": session.member.permissions,
                            },
                        },
                    }),
                },
                connection,
            )
        }
        Err(error) => (
            ServerMessage::AuthError {
                code: Some(database_auth_error_code(&error).into()),
                id,
                error: error.to_string(),
            },
            control::ControlConnection::default(),
        ),
    }
}

fn query_result(id: String, path: String, result: serde_json::Value) -> ServerMessage {
    ServerMessage::QueryResult {
        id,
        payload: std::collections::BTreeMap::from([
            ("path".to_owned(), serde_json::Value::String(path)),
            ("result".to_owned(), result),
            (
                "reason".to_owned(),
                serde_json::Value::String("initial".to_owned()),
            ),
        ]),
    }
}

async fn call_reducer(
    runtime: &Runtime,
    session: Option<&TenantSession>,
    control_connection: &control::ControlConnection,
    call: ReducerCallRequest,
) -> ServerMessage {
    let ReducerCallRequest {
        id,
        path,
        args,
        scope,
        trace,
        idempotency_key,
        artifact_hash,
        client_contract,
        receipt_path,
        intent_entropy,
    } = call;
    if scope == Some(ExecutionScope::Control) {
        return match runtime
            .execute_control_reducer(
                control_connection,
                &path,
                &args,
                idempotency_key.as_deref().unwrap_or(""),
            )
            .await
        {
            Ok(result) => ServerMessage::ReducerResult {
                id: id.clone(),
                path: Some(path),
                result,
                origin_command_id: id,
                committed_revision: None,
                trace,
            },
            Err(error) => {
                error
                    .reducer_error_class()
                    .reducer_error(id, Some(path), error.to_string(), trace)
            }
        };
    }
    let Some(session) = session else {
        // The intent is fine; this connection just has no tenant session yet
        // (or lost it). Clients keep the intent and re-authenticate.
        return ReducerErrorClass::Unauthenticated.reducer_error(
            id,
            Some(path),
            "authenticate with an active tenant before calling a Reducer".to_owned(),
            trace,
        );
    };
    match runtime
        .execute_tenant_reducer_with_access(
            session,
            &id,
            idempotency_key.as_deref(),
            &path,
            args,
            execution::ExecutionAccess {
                expected_artifact_hash: artifact_hash,
                client_contract,
                receipt_path,
                intent_entropy,
                ..Default::default()
            },
        )
        .await
    {
        Ok(result) => ServerMessage::ReducerResult {
            id: id.clone(),
            path: Some(path),
            result: result.value,
            origin_command_id: id,
            committed_revision: result.committed_revision,
            trace,
        },
        Err(error) => {
            error
                .reducer_error_class()
                .reducer_error(id, Some(path), error.to_string(), trace)
        }
    }
}

fn ordered_control_completion(
    response: ServerMessage,
    control_updates: Vec<ServerMessage>,
    control_succeeded: bool,
) -> Vec<ServerMessage> {
    if !control_succeeded {
        return vec![response];
    }
    let id = match &response {
        ServerMessage::ReducerResult { id, .. } | ServerMessage::ActionResult { id, .. } => {
            id.clone()
        }
        _ => return vec![response],
    };
    let mut messages = Vec::with_capacity(control_updates.len() + 2);
    messages.extend(control_updates);
    messages.push(response);
    messages.push(ServerMessage::ControlWatermark { id });
    messages
}

type PreparedReplicaOpen<S> = (Vec<ServerMessage>, Option<(String, S)>);
type ReplicaOpenOutcome<S, E> = (ReplicaOpenRequest, Result<PreparedReplicaOpen<S>, E>);

async fn collect_bounded_ordered<T, U, F, Fut>(
    inputs: Vec<T>,
    concurrency: usize,
    operation: F,
) -> Vec<U>
where
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = U>,
{
    futures_util::stream::iter(inputs)
        .map(operation)
        .buffered(concurrency.max(1))
        .collect()
        .await
}

fn replica_open_request_bytes(request: &ReplicaOpenRequest) -> usize {
    serde_json::to_vec(request).map_or(usize::MAX, |value| {
        value.len().saturating_mul(2).saturating_add(1024)
    })
}

async fn enqueue_replica_open(
    runtime: &Runtime,
    session: Option<&TenantSession>,
    request: ReplicaOpenRequest,
    sum_enabled: bool,
    queue: &mut replica_open_queue::ReplicaOpenQueue<
        ReplicaOpenOutcome<replica::ReplicaSubscription, replica::ReplicaError>,
    >,
) -> Option<ServerMessage> {
    let reject = |error: &str| ServerMessage::ReplicaError {
        id: request.id.clone(),
        path: Some(request.path.clone()),
        error: error.to_owned(),
    };
    let Some(session) = session else {
        return Some(reject(
            "authenticate with an active tenant before opening a Replica Collection",
        ));
    };
    let bytes = replica_open_request_bytes(&request);
    if bytes > replica_open_queue::MAX_BYTES {
        return Some(reject("individual replica open request is too large"));
    }
    let module = runtime
        .inner
        .modules
        .project(&session.identity.project_id)
        .await;
    let bulk = module
        .as_ref()
        .and_then(|module| module.functions.get(&request.path))
        .and_then(|function| function.replica.as_ref())
        .is_none_or(|definition| definition.max_rows == 0 || definition.max_rows > 512);
    let owned_runtime = runtime.clone();
    let owned_session = session.clone();
    let owned_request = request.clone();
    use futures_util::FutureExt;
    let future = async move {
        let opened = owned_runtime
            .open_replica_negotiated(&owned_session, owned_request.clone(), sum_enabled)
            .await
            .map(|opened| {
                let subscription = opened
                    .subscription
                    .map(|subscription| (subscription.id.clone(), subscription));
                (opened.messages, subscription)
            });
        (owned_request, opened)
    }
    .boxed();
    // The socket loop reserved admission before constructing this future.
    // Queue ownership is exclusive across the await above.
    queue
        .push(request.id.clone(), bytes, bulk, future)
        .expect("replica preparation admission must have capacity");
    None
}

fn install_replica_open_outcomes<S, E>(
    replicas: &mut BTreeMap<String, S>,
    outcomes: Vec<ReplicaOpenOutcome<S, E>>,
) -> Vec<ServerMessage>
where
    E: std::fmt::Display,
{
    let mut messages = Vec::new();
    for (request, outcome) in outcomes {
        match outcome {
            Ok((opened_messages, subscription)) => {
                if let Some((id, subscription)) = subscription {
                    replicas.insert(id, subscription);
                }
                messages.extend(opened_messages);
            }
            Err(error) => messages.push(ServerMessage::ReplicaError {
                id: request.id,
                path: Some(request.path),
                error: error.to_string(),
            }),
        }
    }
    messages
}

fn preserve_subscription_scope(
    old: &control::ControlConnection,
    new: &control::ControlConnection,
    capabilities: Option<&gonvex_protocol::ClientCapabilities>,
) -> bool {
    if capabilities.is_none_or(|caps| caps.preserve_subscriptions != Some(1)) {
        return false;
    }
    let (Some(old_session), Some(new_session)) = (&old.tenant, &new.tenant) else {
        return false;
    };
    !old_session.is_anonymous()
        && old.project_id == new.project_id
        && old.identity == new.identity
        && old.impersonation_id == new.impersonation_id
        && old_session.identity == new_session.identity
        && old_session.route == new_session.route
        && old_session.member == new_session.member
        && old_session.delegation == new_session.delegation
        && old_session.developer_session == new_session.developer_session
}

fn record_connection_subscriptions(
    runtime: &Runtime,
    connection_id: &str,
    replicas: &BTreeMap<String, replica::ReplicaSubscription>,
    live_queries: &BTreeMap<String, live_query::LiveQuerySubscription>,
    control_queries: &BTreeMap<String, control::ControlSubscription>,
) {
    runtime.inner.metrics.subscriptions(
        connection_id,
        replicas
            .values()
            .map(|subscription| subscription.path.as_str())
            .chain(
                live_queries
                    .values()
                    .map(|subscription| subscription.path.as_str()),
            )
            .chain(
                control_queries
                    .values()
                    .map(|subscription| subscription.path.as_str()),
            ),
    );
}

fn snapshot_pages_negotiated(capabilities: Option<&gonvex_protocol::ClientCapabilities>) -> bool {
    capabilities.and_then(|c| c.replica_snapshot_pages) == Some(1)
}

async fn send_replica_open(
    socket: &mut WebSocket,
    message: ServerMessage,
    paged: bool,
    native: bool,
    delivery: &mut snapshot_delivery::SnapshotDelivery,
) -> Result<(), ()> {
    if native && matches!(message, ServerMessage::ReplicaSnapshot { .. }) {
        let id = if let ServerMessage::ReplicaSnapshot { id, .. } = &message {
            id.clone()
        } else {
            unreachable!()
        };
        if let Err(error) = delivery.begin(message) {
            return send_json(
                socket,
                &ServerMessage::ReplicaError {
                    id,
                    path: None,
                    error: error.into(),
                },
            )
            .await;
        }
        return send_snapshot_frames(socket, delivery).await;
    }
    if paged && matches!(message, ServerMessage::ReplicaSnapshot { .. }) {
        // Feed events stay queued while these pages are sent. The installed
        // subscription resumes from the materialized repeatable-read cursor.
        for page in replica::SnapshotPages::new(message) {
            send_json(socket, &page).await?;
        }
        Ok(())
    } else {
        send_connection_json(delivery, socket, &replica::legacy_snapshot_frame(message)).await
    }
}

async fn send_snapshot_frames(
    socket: &mut WebSocket,
    delivery: &mut snapshot_delivery::SnapshotDelivery,
) -> Result<(), ()> {
    let batch = delivery.pump();
    for start in batch.starts {
        send_json(socket, &start).await?;
    }
    for frame in batch.frames {
        socket
            .send(Message::Binary(frame.into()))
            .await
            .map_err(|_| ())?;
    }
    for error in batch.errors {
        send_json(socket, &error).await?;
    }
    Ok(())
}

async fn send_connection_json(
    delivery: &mut snapshot_delivery::SnapshotDelivery,
    socket: &mut WebSocket,
    message: &ServerMessage,
) -> Result<(), ()> {
    if matches!(
        message,
        ServerMessage::ReplicaReady { .. }
            | ServerMessage::ReplicaReadyMany { .. }
            | ServerMessage::ReplicaError { .. }
    ) {
        if let Some(message) = delivery.gate(message) {
            send_json(socket, &message).await?;
        }
        if matches!(message, ServerMessage::ReplicaError { .. }) {
            send_snapshot_frames(socket, delivery).await?;
        }
        Ok(())
    } else {
        if delivery.native() {
            if let Some(encoded) = replica::native_control_message(message).map_err(|_| ())? {
                return send_json(socket, &encoded).await;
            }
        }
        send_json(socket, message).await
    }
}

async fn send_connection_all(
    delivery: &mut snapshot_delivery::SnapshotDelivery,
    socket: &mut WebSocket,
    messages: &[ServerMessage],
) -> Result<(), ()> {
    for message in messages {
        send_connection_json(delivery, socket, message).await?;
    }
    Ok(())
}

async fn send_json(socket: &mut WebSocket, message: &ServerMessage) -> Result<(), ()> {
    let encoded = serde_json::to_string(message).map_err(|_| ())?;
    socket
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn native_snapshot_socket_backpressures_until_durable_ack() {
        use futures_util::{SinkExt, StreamExt};
        use gonvex_protocol::{ClientMessage, ReplicaCursor, ServerMessage};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        assert!(address.port() >= 5185);
        let app = axum::Router::new().route(
            "/proof",
            axum::routing::get(|upgrade: axum::extract::ws::WebSocketUpgrade| async {
                upgrade.on_upgrade(|socket| async move {
                    let mut socket = super::WebSocket::Plain(Box::new(socket));
                    let mut delivery = super::snapshot_delivery::SnapshotDelivery::default();
                    let cursor = ReplicaCursor {
                        epoch: "e".into(),
                        revision: 1,
                    };
                    let snapshot = ServerMessage::ReplicaSnapshot {
                        id: "wire".into(),
                        key: "id".into(),
                        cursor: cursor.clone(),
                        result: (0..1000)
                            .map(|id| serde_json::json!({"id":id,"body":"x".repeat(1000)}))
                            .collect(),
                        metadata: std::collections::BTreeMap::from([(
                            "pageDigest".into(),
                            serde_json::json!("sum128-v1:00000000000000000000000000000000"),
                        )]),
                    };
                    super::send_replica_open(&mut socket, snapshot, true, true, &mut delivery)
                        .await
                        .unwrap();
                    let ready = ServerMessage::ReplicaReady {
                        id: "wire".into(),
                        cursor,
                        digest: "certificate".into(),
                        metadata: Default::default(),
                    };
                    super::send_connection_json(&mut delivery, &mut socket, &ready)
                        .await
                        .unwrap();
                    while let Some(Ok(axum::extract::ws::Message::Text(text))) = socket.next().await
                    {
                        let ClientMessage::ReplicaSnapshotAck {
                            id,
                            snapshot_id,
                            sequence,
                        } = serde_json::from_str(&text).unwrap()
                        else {
                            panic!("ack");
                        };
                        if let Some(ready) =
                            delivery.acknowledge(&id, &snapshot_id, sequence).unwrap()
                        {
                            super::send_json(&mut socket, &ready).await.unwrap();
                            break;
                        }
                        super::send_snapshot_frames(&mut socket, &mut delivery)
                            .await
                            .unwrap();
                    }
                })
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/proof"))
            .await
            .unwrap();
        let mut pages = Vec::new();
        let mut start = None;
        while let Ok(Some(message)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), socket.next()).await
        {
            let message = message.unwrap();
            if message.is_text() {
                let header: serde_json::Value =
                    serde_json::from_slice(&message.into_data()).unwrap();
                assert_eq!(header["type"], "replica.snapshotStart");
                assert!(pages.is_empty());
                assert!(start.is_none());
                start = Some(header);
                continue;
            }
            assert!(message.is_binary());
            let page = serde_json::from_slice::<serde_json::Value>(&message.into_data()).unwrap();
            assert_eq!(
                Some(&page["snapshotId"]),
                start.as_ref().map(|header| &header["snapshotId"])
            );
            pages.push(page);
        }
        assert!(!pages.is_empty() && pages.len() <= 8);
        assert!(!pages.last().unwrap()["last"].as_bool().unwrap());
        let acknowledge = |page: &serde_json::Value| {
            tokio_tungstenite::tungstenite::Message::Text(serde_json::json!({"type":"replica.snapshotAck","id":"wire","snapshotId":page["snapshotId"],"sequence":page["sequence"]}).to_string().into())
        };
        socket
            .send(acknowledge(pages.last().unwrap()))
            .await
            .unwrap();
        let mut rows: usize = pages
            .iter()
            .map(|page| page["canonicalRows"].as_array().unwrap().len())
            .sum();
        loop {
            let message = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame: serde_json::Value = serde_json::from_slice(&message.into_data()).unwrap();
            if frame["type"] == "replica.ready" {
                assert_eq!(rows, 1000);
                break;
            }
            assert_eq!(frame["type"], "replica.snapshotEncodedPage");
            rows += frame["canonicalRows"].as_array().unwrap().len();
            socket.send(acknowledge(&frame)).await.unwrap();
        }
        socket.close(None).await.unwrap();
        server.abort();
    }

    #[test]
    fn snapshot_pages_negotiation_preserves_old_clients() {
        assert!(!super::snapshot_pages_negotiated(None));
        let mut capabilities = gonvex_protocol::ClientCapabilities::default();
        assert!(!super::snapshot_pages_negotiated(Some(&capabilities)));
        capabilities.replica_snapshot_pages = Some(2);
        assert!(!super::snapshot_pages_negotiated(Some(&capabilities)));
        capabilities.replica_snapshot_pages = Some(1);
        assert!(super::snapshot_pages_negotiated(Some(&capabilities)));
    }

    #[test]
    fn cached_startup_auth_error_codes() {
        use gonvex_postgres::DatabaseError;
        assert_eq!(
            super::database_auth_error_code(&DatabaseError::InvalidSession),
            "credential_rejected"
        );
        assert_eq!(
            super::database_auth_error_code(&DatabaseError::MemberNotFound("account".into())),
            "member_removed"
        );
        assert_eq!(
            super::database_auth_error_code(&DatabaseError::AdmissionTimeout),
            "transient"
        );
    }

    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use tokio::sync::{mpsc, Semaphore};
    use tower::ServiceExt;

    use super::*;
    use crate::config::ModuleHostConfig;

    #[test]
    fn control_telemetry_preserves_legacy_auth_and_removes_replica_data() {
        let tenant = Some("tenant".to_owned());
        assert_eq!(control_auth_tenant(true, None, tenant.clone()), None);
        assert_eq!(control_auth_tenant(false, None, tenant.clone()), tenant);
        let capabilities = gonvex_protocol::ClientCapabilities {
            tenant_control: Some(1),
            ..Default::default()
        };
        assert_eq!(
            control_auth_tenant(true, Some(&capabilities), tenant.clone()),
            tenant
        );
        assert_eq!(
            control_auth_tenant(true, Some(&capabilities), Some("  ".into())),
            None
        );
        let mut response = ServerMessage::AuthResult {
            id: "auth".into(),
            result: serde_json::json!({
                "tenantId":"tenant", "accountId":"account", "developerSessionToken":"rotated",
                "replica":{"visibilityScope":"scope"}, "localIdentity":{"tenant":{"id":"tenant"}},
            }),
        };
        suppress_control_replica(&mut response);
        let ServerMessage::AuthResult { result, .. } = response else {
            panic!("auth result");
        };
        assert_eq!(
            result,
            serde_json::json!({"tenantId":"tenant","accountId":"account","developerSessionToken":"rotated"})
        );
    }

    #[test]
    fn control_telemetry_tenant_control_keeps_authority_without_data() {
        let mut session = TenantSession::anonymous(gonvex_postgres::TenantRoute {
            project_id: "project".into(),
            tenant_id: "tenant".into(),
            database_url: "unused".into(),
        });
        session.identity.account.id = "account".into();
        let connection = control::ControlConnection {
            identity: Some(session.identity.clone()),
            tenant: Some(session.clone()),
            ..Default::default()
        };
        assert_eq!(
            tenant_data_session(false, &connection),
            Some(session.clone())
        );
        assert_eq!(tenant_data_session(true, &connection), None);
        assert_eq!(connection.tenant, Some(session));
        assert_eq!(connection.identity.unwrap().account.id, "account");
    }

    fn config(enabled: bool) -> Config {
        Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            control_plane_database_url: None,
            default_database_url: Some("postgres://example".to_owned()),
            tenant_database_urls: BTreeMap::new(),
            project_database_urls: BTreeMap::new(),
            require_auth: true,
            websocket_compression: false,
            websocket_compression_threshold: 1024,
            control_secret: Some("test-control-secret".to_owned()),
            auth_public_url: Some("http://localhost:8080".to_owned()),
            admin_key: Some("test-admin-key".to_owned()),
            dev_sync_key: Some("test-sync-key".to_owned()),
            dashboard_account: None,
            dashboard_password: None,
            dashboard_auth_project_id: None,
            google_client_id: None,
            google_client_secret: None,
            database_max_total_connections: 20,
            database_max_connections: 2,
            database_max_idle_connections: 1,
            module_host: ModuleHostConfig {
                enabled,
                binary: None,
                endpoint: None,
                start_timeout: Duration::from_secs(1),
                shutdown_timeout: Duration::from_secs(1),
                max_frame_bytes: 64 << 20,
                max_concurrent_calls: 32,
                isolate_pool_size: 4,
                execution_timeout: Duration::from_secs(10),
                action_execution_timeout: Duration::from_secs(900),
            },
            runtime_version: "0.4.1-test".to_owned(),
            sandbox: Default::default(),
            storage: Default::default(),
            service_principals: Vec::new(),
            public_functions: Default::default(),
        }
    }

    #[tokio::test]
    async fn public_tenant_auth_and_execution_contract() {
        use base64::Engine as _;
        use futures_util::SinkExt;
        use gonvex_postgres::{RuntimeManifestRecord, TenantRoute};
        use sha2::{Digest, Sha256};
        use sqlx::Row;

        let Some(base) = std::env::var("GONVEX_TEST_POSTGRES_URL").ok() else {
            return;
        };
        let Some(binary) = std::env::var("GONVEX_TEST_MODULE_HOST_BINARY").ok() else {
            return;
        };
        let schema = format!("public_functions_{}", uuid::Uuid::new_v4().simple());
        let admin = sqlx::PgPool::connect(&base).await.unwrap();
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{separator}options=-csearch_path%3D{schema}");
        let mut cfg = config(true);
        cfg.module_host.binary = Some(binary.into());
        cfg.module_host.execution_timeout = Duration::from_secs(45);
        cfg.public_functions.action_timeout = Duration::from_secs(2);
        cfg.storage = crate::config::StorageConfig {
            endpoint: "http://127.0.0.1:1".into(),
            region: "us-east-1".into(),
            bucket: "files".into(),
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            force_path_style: true,
            public_base_url: "https://uploads.example.test".into(),
        };
        let runtime = Runtime::new(cfg);
        let control = ControlPlane::connect(&url, runtime.inner.pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let fixture = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('project','Project','multiTenant','active')").execute(&fixture).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,domain,database_url,status) VALUES('route','project','tenant-id','Tenant','careers',$1,'active')").bind(&url).execute(&fixture).await.unwrap();
        let route = TenantRoute {
            project_id: "project".into(),
            tenant_id: "tenant-id".into(),
            database_url: url.clone(),
        };
        sqlx::query("CREATE TABLE applications(id TEXT PRIMARY KEY, consent BOOLEAN NOT NULL)")
            .execute(&fixture)
            .await
            .unwrap();
        control
            .clone()
            .provision_tenant_database(route.clone(), Vec::new())
            .await
            .unwrap();
        *runtime.inner.control_plane.write().await = Some(control);
        runtime.inner.module_host.start().await.unwrap();
        let code = r#"
const identity = ctx => ({ account: ctx.auth.account, member: ctx.member, tenant: ctx.tenant.id, invocation: ctx.invocation });
export async function preview(ctx) { return identity(ctx); }
export async function apply(ctx, args) { await ctx.db.insert("applications", { id: args.id, consent: true }); return identity(ctx); }
export async function action(ctx) { return identity(ctx); }
export async function tool(ctx) { return ctx.tools.save({}); }
export async function internal(ctx) { return identity(ctx); }
export async function upload(ctx) { return ctx.storage.generateUploadUrl({ visibility: "public", ownerId: "spoofed", size: 3, contentType: "application/pdf" }); }
export async function metadata(ctx, args) { return ctx.storage.getMetadata(args.fileId); }
export async function slow(ctx, args) { await ctx.db.query("SELECT 1 FROM pg_sleep(31)", []); return apply(ctx, args); }
export async function slowAction(ctx) { await new Promise(resolve => setTimeout(resolve, 5000)); return identity(ctx); }
"#;
        let mut functions = serde_json::json!({});
        for (path, kind, public, internal) in [
            ("preview", "query", true, false),
            ("slow", "reducer", true, false),
            ("slowAction", "action", true, false),
            ("apply", "reducer", true, false),
            ("action", "action", true, false),
            ("private", "action", false, false),
            ("internal", "reducer", false, true),
            ("tool", "action", true, false),
            ("upload", "action", true, false),
            ("metadata", "reducer", true, false),
        ] {
            functions[path] = serde_json::json!({"kind":kind,"public":public,"internal":internal,
                "interactive":false,"classification":if internal {"internal"} else {"system"},
                "handler":if path == "private" {"action"} else {path},"file":"test.js","delivery":"oneShot",
                "args":{"kind":"any"},"result":{"kind":"any"}});
        }
        functions["upload"]["actionCapabilities"] = serde_json::json!({"storage":true});
        functions["tool"]["actionProfile"] = serde_json::json!("agent");
        functions["tool"]["actionCapabilities"] =
            serde_json::json!({"tools":{"save":{"kind":"internalReducer","function":"internal"}}});
        let mut module = serde_json::json!({"generation":1,"language":"typescript","entrypoint":"test.js",
            "files":{},"visibility":{},"crons":[],"functions":functions,
            "javascript":{"path":"test.js","hash":format!("{:x}", Sha256::digest(code.as_bytes())),"code":base64::engine::general_purpose::STANDARD.encode(code)}});
        let hash = crate::modules::canonical_artifact_hash(module.as_object().unwrap()).unwrap();
        module["hash"] = serde_json::json!(hash);
        runtime
            .inner
            .modules
            .install(
                &runtime.inner.module_host,
                RuntimeManifestRecord {
                    project_id: "project".into(),
                    module_hash: hash,
                    manifest: serde_json::json!({"module":module}),
                },
            )
            .await
            .unwrap();
        for tenant in ["tenant-id", "CAREERS"] {
            let (message, connection) = authenticate(
                &runtime,
                "auth".into(),
                None,
                Some("project"),
                Some(tenant),
                AuthMode {
                    control_only: false,
                    public: true,
                },
                "connection",
            )
            .await;
            assert!(matches!(message, ServerMessage::AuthResult { .. }));
            let session = connection.tenant.unwrap();
            assert!(session.is_anonymous());
            assert!(connection.identity.is_none());
            assert_eq!(session.route.tenant_id, "tenant-id");
            let value = runtime
                .execute_tenant_query(&session, "preview", serde_json::json!({}))
                .await
                .unwrap();
            assert!(value["account"].is_null());
            assert!(value["member"].is_null());
            assert_eq!(value["tenant"], "tenant-id");
            assert!(value["invocation"]["actorAccountId"].is_null());
        }
        for (project, tenant) in [("project", "unknown"), ("other", "tenant-id")] {
            let (message, connection) = authenticate(
                &runtime,
                "auth".into(),
                None,
                Some(project),
                Some(tenant),
                AuthMode {
                    control_only: false,
                    public: true,
                },
                "connection",
            )
            .await;
            assert_eq!(
                message,
                ServerMessage::AuthError {
                    code: Some("transient".into()),
                    id: "auth".into(),
                    error: "public tenant session is unavailable".into()
                }
            );
            assert!(connection.tenant.is_none());
        }
        let session = TenantSession::anonymous(route);
        let result = runtime
            .execute_tenant_reducer(
                &session,
                "public-apply",
                None,
                "apply",
                serde_json::json!({"id":"application"}),
            )
            .await
            .unwrap();
        assert!(result.value["member"].is_null());
        assert!(result.committed_revision.is_some());
        assert!(call_watermark_after_sent_replica_work(
            &ServerMessage::ReducerResult {
                id: "r".into(),
                path: None,
                result: result.value,
                origin_command_id: "r".into(),
                committed_revision: result.committed_revision,
                trace: None
            },
            true,
            None
        )
        .is_some());
        for path in ["action", "tool"] {
            let result = runtime
                .execute_tenant_action(&session, path, serde_json::json!({}))
                .await
                .unwrap();
            assert!(result.value["member"].is_null());
            assert!(result.value["account"].is_null());
        }
        assert!(runtime
            .execute_tenant_action(&session, "private", serde_json::json!({}))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("authenticate with an active tenant"));
        assert!(runtime
            .execute_tenant_reducer(&session, "r", None, "internal", serde_json::json!({}))
            .await
            .is_err());
        assert!(runtime
            .open_live_query(
                &session,
                "l".into(),
                "preview".into(),
                serde_json::json!({})
            )
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("authenticate with an active tenant"));
        let request =
            serde_json::from_value(serde_json::json!({"id":"replica","path":"preview","args":{}}))
                .unwrap();
        assert!(runtime
            .open_replica(&session, request)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("authenticate with an active tenant"));
        let upload = runtime
            .execute_tenant_action(&session, "upload", serde_json::json!({}))
            .await
            .unwrap()
            .value;
        assert!(upload["url"]
            .as_str()
            .unwrap()
            .starts_with("https://uploads.example.test/storage/"));
        let file_id = upload["fileId"].as_str().unwrap();
        let row = sqlx::query("SELECT owner_id,visibility FROM _gonvex_files WHERE id=$1")
            .bind(file_id)
            .fetch_one(&fixture)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("owner_id"), "");
        assert_eq!(row.get::<String, _>("visibility"), "tenant");
        sqlx::query("UPDATE _gonvex_files SET status='uploaded',size_bytes=3 WHERE id=$1")
            .bind(file_id)
            .execute(&fixture)
            .await
            .unwrap();
        let metadata = runtime
            .execute_tenant_reducer(
                &session,
                "metadata",
                None,
                "metadata",
                serde_json::json!({"fileId":file_id}),
            )
            .await
            .unwrap()
            .value;
        assert_eq!(metadata["tenantId"], "tenant-id");
        assert_eq!(metadata["size"], 3);
        assert_eq!(metadata["status"], "uploaded");
        let mut member_session = session.clone();
        member_session.identity.account.id = "account".into();
        member_session.member.id = "member".into();
        member_session.member.account_id = "account".into();
        member_session.member.status = "active".into();
        let signed_in = runtime
            .execute_tenant_query(&member_session, "preview", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(signed_in["account"]["id"], "account");
        assert_eq!(signed_in["member"]["id"], "member");
        runtime
            .execute_tenant_action(&member_session, "private", serde_json::json!({}))
            .await
            .unwrap();
        // Public Actions cannot inherit the member's 15-minute budget.
        let started = std::time::Instant::now();
        assert!(runtime
            .execute_tenant_action(&session, "slowAction", serde_json::json!({}))
            .await
            .is_err());
        assert!(started.elapsed() < Duration::from_secs(4));
        assert!(runtime
            .execute_tenant_action(&member_session, "slowAction", serde_json::json!({}))
            .await
            .is_ok());

        use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
        async fn read_socket(
            socket: &mut tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        ) -> serde_json::Value {
            let frame = tokio::time::timeout(Duration::from_secs(40), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            serde_json::from_str(frame.to_text().unwrap()).unwrap()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                server_runtime
                    .router()
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let socket_url = format!("ws://{address}/ws?public=1");
        sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('other','Other','multiTenant','active')").execute(&fixture).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,domain,database_url,status) VALUES('other-route','other','tenant-id','Other','other-careers',$1,'active'),('other-tenant','project','tenant-other','Other','other',$1,'active')").bind(&url).execute(&fixture).await.unwrap();
        for (project, tenant) in [("project", "tenant-other"), ("other", "tenant-id")] {
            let (mut socket, _) = connect_async(&socket_url).await.unwrap();
            read_socket(&mut socket).await;
            socket.send(WsMessage::Text(serde_json::json!({"type":"auth","id":"first","public":true,"project":"project","tenant":"careers"}).to_string().into())).await.unwrap();
            assert_eq!(read_socket(&mut socket).await["type"], "auth.result");
            // Domain and canonical ID denote the same pinned scope.
            socket.send(WsMessage::Text(serde_json::json!({"type":"auth","id":"same","public":true,"project":"project","tenant":"tenant-id"}).to_string().into())).await.unwrap();
            assert_eq!(read_socket(&mut socket).await["type"], "auth.result");
            socket.send(WsMessage::Text(serde_json::json!({"type":"auth","id":"switch","public":true,"project":project,"tenant":tenant}).to_string().into())).await.unwrap();
            assert_eq!(
                read_socket(&mut socket).await["error"],
                "public connection scope is fixed"
            );
            assert!(matches!(socket.next().await, Some(Ok(WsMessage::Close(_)))));
        }
        let (mut socket, _) = connect_async(&socket_url).await.unwrap();
        read_socket(&mut socket).await;
        socket.send(WsMessage::Text(serde_json::json!({"type":"auth","id":"batch-auth","public":true,"project":"project","tenant":"careers"}).to_string().into())).await.unwrap();
        assert_eq!(read_socket(&mut socket).await["type"], "auth.result");
        socket.send(WsMessage::Text(serde_json::json!({"type":"reducer.callMany","calls":[{"id":"slow-first","path":"slow","args":{"id":"before-revocation"}},{"id":"after","path":"apply","args":{"id":"after-revocation"}}]}).to_string().into())).await.unwrap();
        // Wait until the first call is running before revoking its tenant.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let running: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE state='active' AND query = 'SELECT 1 FROM pg_sleep(31)')").fetch_one(&fixture).await.unwrap();
                if running { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        sqlx::query("UPDATE gonvex_runtime_tenants SET status='disabled' WHERE project_id='project' AND tenant_id='tenant-id'").execute(&fixture).await.unwrap();
        let first = read_socket(&mut socket).await;
        assert_eq!(first["type"], "reducer.result");
        assert_eq!(first["id"], "slow-first");
        assert_eq!(read_socket(&mut socket).await["type"], "replica.watermark");
        let rejected = read_socket(&mut socket).await;
        assert_eq!(rejected["id"], "after");
        assert_eq!(rejected["error"], "public tenant session is unavailable");
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM applications WHERE id='after-revocation'")
                .fetch_one(&fixture)
                .await
                .unwrap();
        assert_eq!(count, 0);
        socket.close(None).await.ok();
        server.abort();
        sqlx::query("UPDATE gonvex_runtime_tenants SET status='disabled'")
            .execute(&fixture)
            .await
            .unwrap();
        let (message, _) = authenticate(
            &runtime,
            "auth".into(),
            None,
            Some("project"),
            Some("careers"),
            AuthMode {
                control_only: false,
                public: true,
            },
            "connection",
        )
        .await;
        assert!(matches!(message, ServerMessage::AuthError { .. }));
        runtime.inner.module_host.shutdown().await;
        fixture.close().await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }

    fn upload_signature(secret: &str, key: &str, expires: i64) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(format!("gonvex-storage-upload:{secret}").as_bytes())
                .unwrap();
        mac.update(format!("{key}\n{expires}").as_bytes());
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn feed_transaction(revision: u64) -> change_feed::FeedEvent {
        change_feed::FeedEvent::Transaction {
            database_epoch: "epoch".to_owned(),
            revision,
            changes: Vec::new(),
        }
    }

    fn replica_open_request(id: &str) -> ReplicaOpenRequest {
        ReplicaOpenRequest {
            id: id.to_owned(),
            path: format!("settings.{id}"),
            args: serde_json::json!({}),
            cursor: None,
            keys: Vec::new(),
            hashes: BTreeMap::new(),
            digest: None,
            full_integrity: false,
        }
    }

    fn replica_marker(id: &str) -> ServerMessage {
        ServerMessage::ReplicaError {
            id: id.to_owned(),
            path: None,
            error: "marker".to_owned(),
        }
    }

    fn replica_message_id(message: &ServerMessage) -> &str {
        match message {
            ServerMessage::ReplicaError { id, .. } => id,
            _ => panic!("expected replica marker or error"),
        }
    }

    fn scheduled_feed_revision<T>(transport: ScheduledTransport<T>) -> u64 {
        match transport {
            ScheduledTransport::Feed(Ok(change_feed::FeedEvent::Transaction {
                revision, ..
            })) => revision,
            _ => panic!("expected committed feed transaction"),
        }
    }

    fn scheduled_socket<T>(transport: ScheduledTransport<T>) -> T {
        match transport {
            ScheduledTransport::Socket(Some(message)) => message,
            _ => panic!("expected socket message"),
        }
    }

    #[tokio::test]
    async fn replica_open_work_never_exceeds_the_database_concurrency_cap() {
        let input_count = REPLICA_OPEN_CONCURRENCY + 5;
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();

        let worker = {
            let active = active.clone();
            let peak = peak.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                collect_bounded_ordered(
                    (0..input_count).collect(),
                    REPLICA_OPEN_CONCURRENCY,
                    move |index| {
                        let active = active.clone();
                        let peak = peak.clone();
                        let gate = gate.clone();
                        let started_tx = started_tx.clone();
                        async move {
                            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            started_tx.send(index).unwrap();
                            let permit = gate.acquire_owned().await.unwrap();
                            permit.forget();
                            active.fetch_sub(1, Ordering::SeqCst);
                            index
                        }
                    },
                )
                .await
            })
        };

        for _ in 0..REPLICA_OPEN_CONCURRENCY {
            started_rx.recv().await.unwrap();
        }
        assert!(started_rx.try_recv().is_err());
        assert_eq!(active.load(Ordering::SeqCst), REPLICA_OPEN_CONCURRENCY);
        gate.add_permits(input_count);

        let output = worker.await.unwrap();
        assert_eq!(output, (0..input_count).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), REPLICA_OPEN_CONCURRENCY);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn replica_open_work_preserves_request_order_after_reverse_completion() {
        let input_count = 4;
        let gates = Arc::new(
            (0..input_count)
                .map(|_| Arc::new(Semaphore::new(0)))
                .collect::<Vec<_>>(),
        );
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let worker = {
            let gates = gates.clone();
            tokio::spawn(async move {
                collect_bounded_ordered((0..input_count).collect(), input_count, move |index| {
                    let gate = gates[index].clone();
                    let started_tx = started_tx.clone();
                    let completed_tx = completed_tx.clone();
                    async move {
                        started_tx.send(index).unwrap();
                        let permit = gate.acquire_owned().await.unwrap();
                        permit.forget();
                        completed_tx.send(index).unwrap();
                        index
                    }
                })
                .await
            })
        };

        for _ in 0..input_count {
            started_rx.recv().await.unwrap();
        }
        for index in (0..input_count).rev() {
            gates[index].add_permits(1);
            assert_eq!(completed_rx.recv().await.unwrap(), index);
        }

        assert_eq!(worker.await.unwrap(), (0..input_count).collect::<Vec<_>>());
    }

    #[test]
    fn sdk_resume_token_rotation_preserves_only_identical_authorization_scope() {
        let session = TenantSession {
            identity: gonvex_postgres::SessionIdentity {
                project_id: "project".to_owned(),
                account: gonvex_postgres::Account {
                    id: "account".to_owned(),
                    email: "account@example.test".to_owned(),
                    email_verified: true,
                    name: "Account".to_owned(),
                    avatar_url: String::new(),
                    provider: "firebase".to_owned(),
                },
            },
            member: gonvex_postgres::Member {
                id: "member".to_owned(),
                account_id: "account".to_owned(),
                status: "active".to_owned(),
                display_name: "Account".to_owned(),
                avatar_url: String::new(),
                role: "member".to_owned(),
                permissions: serde_json::json!({}),
                membership_revision: 1,
            },
            admission_revision: 5,
            ..TenantSession::anonymous(gonvex_postgres::TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: "tenant".to_owned(),
                database_url: "postgres://tenant".to_owned(),
            })
        };
        let old = control::ControlConnection {
            project_id: "project".into(),
            identity: Some(session.identity.clone()),
            tenant: Some(session),
            auth_token: "old".into(),
            ..Default::default()
        };
        let capabilities = gonvex_protocol::ClientCapabilities {
            preserve_subscriptions: Some(1),
            ..Default::default()
        };
        assert!(!preserve_subscription_scope(&old, &old, None));
        let mut refreshed = old.clone();
        refreshed.auth_token = "new".into();
        refreshed.tenant.as_mut().unwrap().admission_revision += 1;
        assert!(preserve_subscription_scope(
            &old,
            &refreshed,
            Some(&capabilities)
        ));
        refreshed.tenant.as_mut().unwrap().member.permissions = serde_json::json!({"admin":true});
        assert!(!preserve_subscription_scope(
            &old,
            &refreshed,
            Some(&capabilities)
        ));
        refreshed = old.clone();
        refreshed.tenant.as_mut().unwrap().route.tenant_id = "other".into();
        assert!(!preserve_subscription_scope(
            &old,
            &refreshed,
            Some(&capabilities)
        ));
        refreshed = old.clone();
        refreshed.identity.as_mut().unwrap().account.id = "other".into();
        assert!(!preserve_subscription_scope(
            &old,
            &refreshed,
            Some(&capabilities)
        ));
    }

    #[test]
    fn replica_open_installation_is_ordered_and_excludes_failed_subscriptions() {
        let outcomes: Vec<ReplicaOpenOutcome<String, &str>> = vec![
            (
                replica_open_request("first"),
                Ok((
                    vec![replica_marker("first")],
                    Some(("first".to_owned(), "first-subscription".to_owned())),
                )),
            ),
            (replica_open_request("failed"), Err("planned failure")),
            (
                replica_open_request("last"),
                Ok((
                    vec![replica_marker("last")],
                    Some(("last".to_owned(), "last-subscription".to_owned())),
                )),
            ),
        ];
        let mut replicas = BTreeMap::new();

        let messages = install_replica_open_outcomes(&mut replicas, outcomes);

        assert_eq!(
            messages.iter().map(replica_message_id).collect::<Vec<_>>(),
            vec!["first", "failed", "last"],
        );
        assert_eq!(
            replicas,
            BTreeMap::from([
                ("first".to_owned(), "first-subscription".to_owned()),
                ("last".to_owned(), "last-subscription".to_owned()),
            ]),
        );
        assert!(matches!(
            &messages[1],
            ServerMessage::ReplicaError { path: Some(path), error, .. }
                if path == "settings.failed" && error == "planned failure"
        ));
    }

    #[tokio::test]
    async fn committed_feed_event_runs_between_back_to_back_socket_calls() {
        let (sender, receiver) = broadcast::channel(8);
        let mut feed = Some(receiver);
        let mut socket = futures_util::stream::iter(["call-one", "call-two"]);
        let mut scheduler = FeedFirstScheduler::default();

        assert_eq!(
            scheduled_socket(next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await),
            "call-one"
        );
        sender.send(feed_transaction(41)).unwrap();
        assert_eq!(
            scheduled_feed_revision(
                next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await
            ),
            41
        );
        assert_eq!(
            scheduled_socket(next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await),
            "call-two"
        );
    }

    #[tokio::test]
    async fn feed_first_snapshot_is_bounded_under_continuous_feed_traffic() {
        let (sender, receiver) = broadcast::channel(8);
        let mut feed = Some(receiver);
        let mut socket = futures_util::stream::iter(["queued-call"]);
        let mut scheduler = FeedFirstScheduler::default();

        sender.send(feed_transaction(51)).unwrap();
        sender.send(feed_transaction(52)).unwrap();
        assert_eq!(
            scheduled_feed_revision(
                next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await
            ),
            51
        );

        // This event arrived after the scheduler captured its ready-feed
        // snapshot. It cannot extend the batch and starve the queued call.
        sender.send(feed_transaction(53)).unwrap();
        assert_eq!(
            scheduled_feed_revision(
                next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await
            ),
            52
        );
        assert_eq!(
            scheduled_socket(next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await),
            "queued-call"
        );
        assert_eq!(
            scheduled_feed_revision(
                next_socket_or_feed(&mut socket, &mut feed, &mut scheduler).await
            ),
            53
        );
    }

    #[tokio::test]
    async fn signed_storage_upload_preflight_is_cors_enabled() {
        let key = "project/tenant/file";
        let expires = Utc::now().timestamp() + 60;
        let secret = "storage-test-secret";
        let signature = upload_signature(secret, key, expires);
        let mut runtime_config = config(false);
        runtime_config.storage.secret_access_key = secret.to_owned();
        let runtime = Runtime::new(runtime_config);
        let response = runtime
            .router()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri(format!(
                        "/storage/{key}?exp={expires}&sig={signature}&upload=1"
                    ))
                    .header("origin", "http://testing.localhost:5184")
                    .header("access-control-request-method", "POST")
                    .header("access-control-request-headers", "content-type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        assert_eq!(
            response.headers()["access-control-allow-methods"],
            "GET, POST, PUT, OPTIONS"
        );
        assert_eq!(
            response.headers()["access-control-allow-headers"],
            "Content-Type"
        );
    }

    #[tokio::test]
    async fn operator_preflights_are_cors_enabled() {
        let runtime = Runtime::new(config(false));
        for (uri, requested_method) in [("/dev/auth/me", "GET"), ("/dev/projects", "POST")] {
            let response = runtime
                .router()
                .oneshot(
                    Request::builder()
                        .method("OPTIONS")
                        .uri(uri)
                        .header("origin", "https://dashboard.gonvex.test")
                        .header("access-control-request-method", requested_method)
                        .header(
                            "access-control-request-headers",
                            "authorization,content-type,x-gonvex-key,x-gonvex-project-key",
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::NO_CONTENT, "{uri}");
            assert_eq!(response.headers()["access-control-allow-origin"], "*");
            assert_eq!(
                response.headers()["access-control-allow-methods"],
                "GET, POST, PUT, PATCH, DELETE, OPTIONS"
            );
            assert_eq!(
                response.headers()["access-control-allow-headers"],
                "Authorization, Content-Type, X-Gonvex-Key, X-Gonvex-Project-Key, X-Gonvex-Project-Id, X-Gonvex-Tenant-Id"
            );
        }
    }

    #[tokio::test]
    async fn operator_cors_does_not_bypass_authorization() {
        let runtime = Runtime::new(config(false));
        for uri in ["/dev/auth/me", "/dev/projects"] {
            let response = runtime
                .router()
                .oneshot(
                    Request::get(uri)
                        .header("origin", "https://dashboard.gonvex.test")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
            assert_eq!(response.headers()["access-control-allow-origin"], "*");
        }
    }

    #[tokio::test]
    async fn health_matches_the_existing_contract() {
        let runtime = Runtime::new(config(false));
        let response = runtime
            .router()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["version"], "0.4.1-test");
        assert_eq!(body["postgresSet"], true);
        assert_eq!(body["moduleHost"]["ready"], true);
    }

    #[tokio::test]
    async fn health_reports_the_canonical_control_plane_database_before_startup() {
        let mut canonical = config(false);
        canonical.control_plane_database_url = canonical.default_database_url.take();
        let runtime = Runtime::new(canonical);
        let response = runtime
            .router()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["postgresSet"], true);
    }

    #[tokio::test]
    async fn required_module_host_fails_closed() {
        let runtime = Runtime::new(config(true));
        assert!(runtime.start().await.is_err());
        let response = runtime
            .router()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn quiet_bootstrap_closes_only_after_acks_and_through_the_oldest_snapshot() {
        assert!(
            bootstrap_watermark_after_sent_snapshots(false, [100].into_iter(), 0, false).is_none()
        );
        assert!(bootstrap_watermark_after_sent_snapshots(true, [].into_iter(), 0, false).is_none());
        assert!(matches!(
            bootstrap_watermark_after_sent_snapshots(true, [100].into_iter(), 0, false),
            Some(ServerMessage::ReplicaWatermark { revision: 100 })
        ));
        assert!(matches!(
            bootstrap_watermark_after_sent_snapshots(true, [105, 100, 103].into_iter(), 0, false),
            Some(ServerMessage::ReplicaWatermark { revision: 100 })
        ));
        assert!(
            bootstrap_watermark_after_sent_snapshots(true, [105, 100].into_iter(), 100, false)
                .is_none()
        );
        assert!(
            bootstrap_watermark_after_sent_snapshots(true, [105, 100].into_iter(), 102, false)
                .is_none()
        );
        assert!(matches!(
            bootstrap_watermark_after_sent_snapshots(true, [0].into_iter(), 0, false),
            Some(ServerMessage::ReplicaWatermark { revision: 0 })
        ));
        assert!(bootstrap_watermark_after_sent_snapshots(true, [0].into_iter(), 0, true).is_none());
    }

    #[test]
    fn committed_feed_transactions_have_a_terminal_replica_watermark() {
        let transaction = change_feed::FeedEvent::Transaction {
            database_epoch: "epoch".to_owned(),
            revision: 535,
            changes: Vec::new(),
        };
        assert!(matches!(
            replica_watermark(&transaction),
            Some(ServerMessage::ReplicaWatermark { revision: 535 })
        ));
        assert!(replica_watermark(&change_feed::FeedEvent::Reset {
            reason: "reset".to_owned(),
        })
        .is_none());
    }

    #[test]
    fn authenticated_tenant_calls_without_replica_work_get_a_terminal_watermark() {
        let action = ServerMessage::ActionResult {
            id: "action".to_owned(),
            path: Some("testing.invoke".to_owned()),
            result: serde_json::Value::Null,
            committed_revision: Some(541),
            trace: None,
        };
        let reducer = ServerMessage::ReducerResult {
            id: "command".to_owned(),
            path: Some("tasks.rename".to_owned()),
            result: serde_json::Value::Null,
            origin_command_id: "command".to_owned(),
            committed_revision: Some(542),
            trace: None,
        };
        assert!(matches!(
            call_watermark_after_sent_replica_work(&action, true, None),
            Some(ServerMessage::ReplicaWatermark { revision: 541 })
        ));
        assert!(matches!(
            call_watermark_after_sent_replica_work(&reducer, true, None),
            Some(ServerMessage::ReplicaWatermark { revision: 542 })
        ));
    }

    #[test]
    fn subscribed_connections_wait_for_the_feed_ordered_watermark() {
        let reducer = ServerMessage::ReducerResult {
            id: "command".to_owned(),
            path: Some("tasks.rename".to_owned()),
            result: serde_json::Value::Null,
            origin_command_id: "command".to_owned(),
            committed_revision: Some(542),
            trace: None,
        };
        let action = ServerMessage::ActionResult {
            id: "action".to_owned(),
            path: Some("testing.invoke".to_owned()),
            result: serde_json::Value::Null,
            committed_revision: Some(543),
            trace: None,
        };
        assert!(call_watermark_after_sent_replica_work(&reducer, true, Some(541)).is_none());
        assert!(call_watermark_after_sent_replica_work(&action, true, Some(541)).is_none());
        assert!(call_watermark_after_sent_replica_work(&action, false, None).is_none());
        // A retry after reconnect has no new feed event. Snapshots already
        // include its commit, so it must complete without blocking later edits.
        for sent in [542, 600] {
            assert!(matches!(
                call_watermark_after_sent_replica_work(&reducer, true, Some(sent)),
                Some(ServerMessage::ReplicaWatermark { revision: 542 })
            ));
        }
    }

    #[test]
    fn read_only_and_failed_calls_do_not_synthesize_watermarks() {
        let no_commit = ServerMessage::ActionResult {
            id: "action".to_owned(),
            path: Some("exports.prepare".to_owned()),
            result: serde_json::Value::Null,
            committed_revision: None,
            trace: None,
        };
        let failed = ServerMessage::ActionError {
            id: "action".to_owned(),
            path: Some("testing.invoke".to_owned()),
            error: "failed".to_owned(),
            trace: None,
        };
        assert!(call_watermark_after_sent_replica_work(&no_commit, true, None).is_none());
        assert!(call_watermark_after_sent_replica_work(&failed, true, None).is_none());
    }

    #[test]
    fn control_completion_closes_after_every_refreshed_control_query() {
        let response = ServerMessage::ReducerResult {
            id: "control-command".to_owned(),
            path: Some("control.invitations.update".to_owned()),
            result: serde_json::json!({"updated":true}),
            origin_command_id: "control-command".to_owned(),
            committed_revision: None,
            trace: None,
        };
        let updates = [
            ("tenant-profile", "control.tenants.mine"),
            ("invitations", "control.invitations.list"),
        ]
        .into_iter()
        .map(|(id, path)| ServerMessage::QueryResult {
            id: id.to_owned(),
            payload: BTreeMap::from([
                (
                    "path".to_owned(),
                    serde_json::Value::String(path.to_owned()),
                ),
                ("result".to_owned(), serde_json::json!([])),
                (
                    "reason".to_owned(),
                    serde_json::Value::String("control-change".to_owned()),
                ),
            ]),
        })
        .collect();

        let messages = ordered_control_completion(response, updates, true);
        assert!(
            matches!(messages.first(), Some(ServerMessage::QueryResult { id, .. }) if id == "tenant-profile")
        );
        assert!(
            matches!(messages.get(1), Some(ServerMessage::QueryResult { id, .. }) if id == "invitations")
        );
        assert!(matches!(
            messages.get(2),
            Some(ServerMessage::ReducerResult { .. })
        ));
        assert!(
            matches!(messages.last(), Some(ServerMessage::ControlWatermark { id }) if id == "control-command")
        );
    }

    #[test]
    fn failed_or_tenant_calls_do_not_emit_control_watermarks() {
        let response = ReducerErrorClass::Rejected.reducer_error(
            "control-command".to_owned(),
            Some("control.invitations.update".to_owned()),
            "denied".to_owned(),
            None,
        );
        let messages = ordered_control_completion(response, Vec::new(), false);
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages[0], ServerMessage::ReducerError { .. }));
    }

    #[test]
    fn membership_replay_before_the_admission_snapshot_does_not_revoke_a_new_socket() {
        let session = TenantSession {
            identity: gonvex_postgres::SessionIdentity {
                project_id: "project".to_owned(),
                account: gonvex_postgres::Account {
                    id: "account".to_owned(),
                    email: "account@example.test".to_owned(),
                    email_verified: true,
                    name: "Account".to_owned(),
                    avatar_url: String::new(),
                    provider: "firebase".to_owned(),
                },
            },
            route: gonvex_postgres::TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: "tenant".to_owned(),
                database_url: "postgres://tenant".to_owned(),
            },
            member: gonvex_postgres::Member {
                id: "member".to_owned(),
                account_id: "account".to_owned(),
                status: "active".to_owned(),
                display_name: "Account".to_owned(),
                avatar_url: String::new(),
                role: "member".to_owned(),
                permissions: serde_json::json!({}),
                membership_revision: 1,
            },
            admission_revision: 5,
            delegation: None,
            developer_session: None,
        };
        let event = |revision, changed_columns: Vec<&str>| change_feed::FeedEvent::Transaction {
            database_epoch: "epoch".to_owned(),
            revision,
            changes: vec![change_feed::LogChange {
                revision,
                ordinal: 0,
                origin_command_id: "command".to_owned(),
                table: "members".to_owned(),
                row_id: "member".to_owned(),
                operation: "UPDATE".to_owned(),
                changed_columns: changed_columns.into_iter().map(str::to_owned).collect(),
                old_value: serde_json::json!({"id":"member","account_id":"account","status":"active"}),
                new_value: serde_json::json!({"id":"member","account_id":"account","status":"revoked"}),
                provenance: change_feed::TransactionProvenance::default(),
            }],
        };

        assert!(!membership_affects(&session, &event(5, vec!["status"])));
        assert!(!membership_affects(
            &session,
            &event(6, vec!["display_name", "avatar_url", "updated_at"]),
        ));
        assert!(membership_affects(&session, &event(6, vec!["status"]),));
        assert!(membership_affects(
            &session,
            &event(6, vec!["role", "permissions"]),
        ));
    }

    #[test]
    fn member_access_change_detection_matches_the_feed_columns() {
        let admitted = gonvex_postgres::Member {
            id: "member".to_owned(),
            account_id: "account".to_owned(),
            status: "active".to_owned(),
            display_name: "Before".to_owned(),
            avatar_url: String::new(),
            role: "member".to_owned(),
            permissions: serde_json::json!({ "view-tasks": true }),
            membership_revision: 3,
        };
        // Profile edits do not revoke the session.
        let renamed = gonvex_postgres::Member {
            display_name: "After".to_owned(),
            avatar_url: "https://example.test/a.png".to_owned(),
            ..admitted.clone()
        };
        assert!(!member_access_changed(&admitted, Some(&admitted)));
        assert!(!member_access_changed(&admitted, Some(&renamed)));
        // A removed Member or any access column change does.
        assert!(member_access_changed(&admitted, None));
        for changed in [
            gonvex_postgres::Member {
                permissions: serde_json::json!({ "view-tasks": true, "create-tasks": true }),
                ..admitted.clone()
            },
            gonvex_postgres::Member {
                role: "admin".to_owned(),
                ..admitted.clone()
            },
            gonvex_postgres::Member {
                status: "suspended".to_owned(),
                ..admitted.clone()
            },
            gonvex_postgres::Member {
                account_id: "other".to_owned(),
                ..admitted.clone()
            },
            gonvex_postgres::Member {
                membership_revision: 4,
                ..admitted.clone()
            },
        ] {
            assert!(member_access_changed(&admitted, Some(&changed)));
        }
    }

    #[test]
    fn membership_change_closes_the_session_with_the_transaction_watermark() {
        // A reducer that grants the caller permissions commits revision 7 and
        // replies with committed_revision 7. The feed then drops the session;
        // the client completes the call only once it sees watermark 7, after
        // the auth error has discarded its subscriptions.
        let transaction = change_feed::FeedEvent::Transaction {
            database_epoch: "epoch".to_owned(),
            revision: 7,
            changes: Vec::new(),
        };
        let frames = membership_changed_frames(replica_watermark(&transaction));
        assert_eq!(frames.len(), 2);
        assert!(matches!(
            &frames[0],
            ServerMessage::AuthError { id, .. } if id == "membership-changed"
        ));
        assert!(matches!(
            frames[1],
            ServerMessage::ReplicaWatermark { revision: 7 }
        ));

        let reset = change_feed::FeedEvent::Reset {
            reason: "reset".to_owned(),
        };
        let frames = membership_changed_frames(replica_watermark(&reset));
        assert_eq!(frames.len(), 1);
        assert!(matches!(&frames[0], ServerMessage::AuthError { .. }));
    }
}
