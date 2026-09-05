//! Host-owned dashboard Live Queries. There is no project row, module, key or
//! tenant for this service. Every snapshot runs the existing operator handler.
use std::{collections::BTreeMap, time::Duration};

use axum::{
    body::{to_bytes, Body},
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, HeaderValue, Method, Request},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{sync::mpsc, task::JoinHandle};
use tower::ServiceExt;

use crate::{operations, operator_data, ClientMessage, Runtime};

const MAX_SUBSCRIPTIONS: usize = 64;
const MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;

pub(super) async fn invalidate(
    State(runtime): State<Runtime>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let write = request.uri().path().starts_with("/dev/")
        && !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        );
    let response = next.run(request).await;
    if write && response.status().is_success() {
        let _ = runtime.inner.dashboard_changes.send(());
    }
    response
}

pub(super) async fn upgrade(
    State(runtime): State<Runtime>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    upgrade
        .max_message_size(64 * 1024)
        .on_upgrade(move |socket| connection(socket, runtime))
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Read {
    resource: String,
    #[serde(default)]
    project: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    project_key: String,
}

fn allowed_resource(resource: &str) -> bool {
    if resource.len() > 16_384 {
        return false;
    }
    let path = resource.split('?').next().unwrap_or("");
    // Accept only relative, read-only operator resources. Encoded path segments,
    // traversal, credentials, environment values and exports are not subscribable.
    if path.contains('%') || path.contains("..") || path.contains('\\') || resource.contains('#') {
        return false;
    }
    if matches!(
        path,
        "/dev/projects"
            | "/dev/tenants"
            | "/dev/manifest"
            | "/dev/data/tables"
            | "/dev/storage/files"
            | "/dev/auth/notifications"
            | "/dev/auth/tokens"
            | "/dev/logs"
            | "/dev/errors/groups"
    ) {
        return true;
    }
    let parts: Vec<_> = path.split('/').collect();
    matches!(parts.as_slice(), ["", "dev", "projects", id, "members"] if !id.is_empty())
        || matches!(parts.as_slice(), ["", "dev", "data", "tables", table, "rows"] if !table.is_empty())
}

fn read_router(runtime: &Runtime) -> Router {
    Router::new()
        .merge(operations::router())
        .merge(operator_data::router())
        .route("/dev/projects", get(crate::list_projects))
        .route("/dev/tenants", get(crate::list_tenants))
        .with_state(runtime.clone())
}

async fn snapshot(router: &Router, read: &Read, token: &str) -> Result<Value, String> {
    let mut request = Request::builder()
        .uri(&read.resource)
        .body(Body::empty())
        .map_err(|e| e.to_string())?;
    for (name, value) in [
        ("x-gonvex-project-id", read.project.clone()),
        ("x-gonvex-tenant-id", read.tenant.clone()),
        ("x-gonvex-key", read.project_key.clone()),
        (
            "authorization",
            if token.is_empty() {
                String::new()
            } else {
                format!("Bearer {token}")
            },
        ),
    ] {
        if !value.is_empty() {
            request.headers_mut().insert(
                name,
                HeaderValue::from_str(&value).map_err(|_| "invalid header")?,
            );
        }
    }
    let response = router
        .clone()
        .oneshot(request)
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), MAX_RESULT_BYTES)
        .await
        .map_err(|_| "dashboard result exceeds limit")?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| "invalid dashboard response")?;
    if !status.is_success() {
        return Err(value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("dashboard read denied")
            .to_owned());
    }
    Ok(value)
}

async fn connection(mut socket: WebSocket, runtime: Runtime) {
    let (sender, mut results) = mpsc::channel::<(String, u64, Value)>(64);
    let mut tasks: BTreeMap<String, (u64, JoinHandle<()>)> = BTreeMap::new();
    let mut generation = 0u64;
    let mut token = String::new();
    if send(
        &mut socket,
        json!({"type":"session.ready","capabilities":{"protocolVersion":2}}),
    )
    .await
    .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            result = results.recv() => {
                let Some((id, version, value)) = result else { break; };
                // Reauth/unsubscribe can leave queued results from an obsolete read.
                if tasks.get(&id).is_some_and(|(current, _)| *current == version)
                    && send(&mut socket, value).await.is_err() { break; }
            }
            frame = socket.next() => {
                let Some(Ok(frame)) = frame else { break; };
                let Message::Text(text) = frame else {
                    if matches!(frame, Message::Close(_)) { break; }
                    continue;
                };
                let Ok(message) = serde_json::from_str::<ClientMessage>(&text) else { break; };
                match message {
                    ClientMessage::Auth { id, token: next_token, .. } => {
                        for (_, (_, task)) in std::mem::take(&mut tasks) { task.abort(); }
                        token.clear();
                        let candidate = next_token.unwrap_or_default();
                        let mut headers = HeaderMap::new();
                        if !candidate.is_empty() {
                            let Ok(value) = HeaderValue::from_str(&format!("Bearer {candidate}")) else { break; };
                            headers.insert("authorization", value);
                        }
                        let response = if operations::authorize(&runtime, &headers, "").await.is_ok() {
                            token = candidate;
                            json!({"type":"auth.result","id":id,"result":{"system":"dashboard"}})
                        } else { json!({"type":"auth.error","id":id,"error":"dashboard authentication required"}) };
                        if send(&mut socket, response).await.is_err() { break; }
                    }
                    ClientMessage::QuerySubscribe { id, path, args, .. } => {
                        if let Some((_, task)) = tasks.remove(&id) { task.abort(); }
                        let read = serde_json::from_value::<Read>(args).ok().filter(|read| allowed_resource(&read.resource));
                        if path != "dashboard.read" || read.is_none() || tasks.len() >= MAX_SUBSCRIPTIONS {
                            if send(&mut socket, json!({"type":"query.error","id":id,"error":"unsupported dashboard subscription or subscription limit reached"})).await.is_err() { break; }
                            continue;
                        }
                        generation += 1;
                        let task = tokio::spawn(watch(runtime.clone(), read.unwrap(), token.clone(), id.clone(), generation, sender.clone()));
                        tasks.insert(id, (generation, task));
                    }
                    ClientMessage::QueryUnsubscribe { id } => {
                        if let Some((_, task)) = tasks.remove(&id) { task.abort(); }
                    }
                    _ => { break; }
                }
            }
        }
    }
    for (_, (_, task)) in tasks {
        task.abort();
    }
}

async fn send(socket: &mut WebSocket, value: Value) -> Result<(), axum::Error> {
    socket.send(Message::Text(value.to_string().into())).await
}

async fn watch(
    runtime: Runtime,
    read: Read,
    token: String,
    id: String,
    generation: u64,
    sender: mpsc::Sender<(String, u64, Value)>,
) {
    let router = read_router(&runtime);
    let mut writes = runtime.inner.dashboard_changes.subscribe();
    let mut events = runtime.inner.runtime_events.subscribe();
    let mut previous = None;
    let mut feed = None;
    let telemetry =
        read.resource.starts_with("/dev/logs") || read.resource.starts_with("/dev/errors/");
    // Reconcile external changes and permission revocation, even with a missed event.
    let mut reconcile = tokio::time::interval(Duration::from_secs(if telemetry { 3 } else { 30 }));
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    reconcile.tick().await;
    loop {
        let value =
            match tokio::time::timeout(Duration::from_secs(15), snapshot(&router, &read, &token))
                .await
            {
                Ok(Ok(value)) => value,
                error => {
                    let error = match error {
                        Ok(Err(error)) => error,
                        _ => "dashboard read timed out".to_owned(),
                    };
                    let _ = sender
                        .send((
                            id.clone(),
                            generation,
                            json!({"type":"query.error","id":id,"error":error}),
                        ))
                        .await;
                    return;
                }
            };
        // Attach only after the handler has authorized this exact project/tenant.
        if feed.is_none() && read.resource.starts_with("/dev/data/") && !read.project.is_empty() {
            let configured = if read.tenant.is_empty() {
                runtime
                    .inner
                    .config
                    .project_database_urls
                    .get(&read.project)
            } else {
                runtime
                    .inner
                    .config
                    .tenant_database_urls
                    .get(&format!("{}:{}", read.project, read.tenant))
            };
            let route = if let Some(database_url) = configured {
                Some(gonvex_postgres::TenantRoute {
                    project_id: read.project.clone(),
                    tenant_id: if read.tenant.is_empty() {
                        read.project.clone()
                    } else {
                        read.tenant.clone()
                    },
                    database_url: database_url.clone(),
                })
            } else if let Some(control) = runtime.inner.control_plane.read().await.clone() {
                control
                    .resolve_tenant(
                        &read.project,
                        if read.tenant.is_empty() {
                            &read.project
                        } else {
                            &read.tenant
                        },
                    )
                    .await
                    .ok()
            } else {
                None
            };
            if let Some(route) = route {
                // Legacy databases may not have a CDC clock. Reconciliation still
                // works; do not start a failing background feed for those databases.
                if crate::change_feed::read_clock(&runtime.inner.pools, &route)
                    .await
                    .is_ok()
                {
                    feed = Some(runtime.inner.change_feeds.subscribe(&route).await);
                    // Re-read after subscribing to close the snapshot/feed race.
                    continue;
                }
            }
        }
        if previous.as_ref() != Some(&value) {
            let reason = if previous.is_none() {
                "initial"
            } else {
                "change"
            };
            if sender
                .send((
                    id.clone(),
                    generation,
                    json!({"type":"query.result","id":id,"result":value,"reason":reason}),
                ))
                .await
                .is_err()
            {
                return;
            }
            previous = Some(value);
        }
        tokio::select! {
            _ = reconcile.tick() => {},
            _ = writes.recv() => {},
            _ = events.recv() => {},
            _ = async { match &mut feed { Some(feed) => { let _ = feed.recv().await; }, None => std::future::pending::<()>().await } } => {},
            _ = sender.closed() => return,
        }
        // Coalesce bursts of committed transactions before running another snapshot.
        tokio::time::sleep(Duration::from_millis(100)).await;
        while writes.try_recv().is_ok() {}
        while events.try_recv().is_ok() {}
        if let Some(feed) = &mut feed {
            while feed.try_recv().is_ok() {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn snapshots_preserve_operator_authorization() {
        let mut config = crate::Config::from_env().unwrap();
        config.require_auth = true;
        config.control_secret = Some("dashboard-test-secret".to_owned());
        config.control_plane_database_url = None;
        config.default_database_url = None;
        let runtime = Runtime::new(config);
        for resource in [
            "/dev/projects",
            "/dev/tenants",
            "/dev/data/tables",
            "/dev/data/tables/tasks/rows",
            "/dev/auth/notifications",
            "/dev/auth/tokens",
            "/dev/projects/other/members",
        ] {
            let read = Read {
                resource: resource.to_owned(),
                project: "other".to_owned(),
                tenant: "other-tenant".to_owned(),
                project_key: String::new(),
            };
            assert!(
                snapshot(&read_router(&runtime), &read, "").await.is_err(),
                "anonymous {resource}"
            );
            assert!(
                snapshot(&read_router(&runtime), &read, "invalid-token")
                    .await
                    .is_err(),
                "invalid token {resource}"
            );
        }
        runtime.shutdown().await;
    }

    #[test]
    fn subscription_resources_are_bounded_read_only_operator_views() {
        for path in [
            "/dev/projects",
            "/dev/tenants?project=one",
            "/dev/data/tables/tasks/rows?limit=100",
            "/dev/projects/one/members",
        ] {
            assert!(allowed_resource(path), "{path}");
        }
        for path in [
            "https://example.com/dev/projects",
            "//example.com/dev/projects",
            "/dev/projects/one/key",
            "/dev/projects/one/env",
            "/dev/sync",
            "/dev/dashboard/ws",
            "/dev/data/tables/../rows",
            "/dev/data/tables/%2e%2e/rows",
            "/dev/auth/notifications/read",
        ] {
            assert!(!allowed_resource(path), "{path}");
        }
    }
}
