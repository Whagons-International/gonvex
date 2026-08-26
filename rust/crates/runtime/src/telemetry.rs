//! Native browser error telemetry over the persistent Gonvex connection.
//!
//! Attribution is always derived from the authenticated socket. Browser
//! payloads cannot select a project, tenant, account, or support session.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use gonvex_protocol::ServerMessage;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::Row;
use tokio::sync::Mutex;

use crate::control::ControlConnection;
use crate::Runtime;

const MAX_EVENTS: usize = 20;
const MAX_ENVELOPE_BYTES: usize = 256 << 10;
const MAX_EVENT_BYTES: usize = 32 << 10;

#[derive(Default)]
pub struct TelemetryLimiter {
    windows: Mutex<BTreeMap<String, RateWindow>>,
}

struct RateWindow {
    started: Instant,
    count: u32,
}

impl TelemetryLimiter {
    async fn allow(&self, key: String, limit: u32) -> bool {
        let now = Instant::now();
        let mut windows = self.windows.lock().await;
        windows.retain(|_, window| now.duration_since(window.started) < Duration::from_secs(120));
        let window = windows.entry(key).or_insert(RateWindow {
            started: now,
            count: 0,
        });
        if now.duration_since(window.started) >= Duration::from_secs(60) {
            *window = RateWindow {
                started: now,
                count: 0,
            };
        }
        if window.count >= limit {
            return false;
        }
        window.count += 1;
        true
    }
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub async fn capture_performance_event(
        &self,
        connection: &ControlConnection,
        id: &str,
        kind: &str,
        path: &str,
        reason: Option<&str>,
        outcome: &str,
        error: Option<&str>,
        client_sent_at_ms: Option<f64>,
        client_received_at_ms: f64,
        client_duration_ms: Option<f64>,
        trace: Option<&gonvex_protocol::MessageTrace>,
        device: Option<&gonvex_protocol::BrowserTelemetryInfo>,
    ) {
        if connection.project_id.is_empty()
            || !matches!(kind, "query" | "reducer" | "action")
            || !matches!(outcome, "ok" | "error")
            || id.is_empty()
            || id.len() > 200
            || path.is_empty()
            || path.len() > 500
            || !client_received_at_ms.is_finite()
            || client_sent_at_ms.is_some_and(|value| !value.is_finite())
            || client_duration_ms.is_some_and(|value| !value.is_finite() || value < 0.0)
            || !self.telemetry_allowed(connection).await
        {
            return;
        }
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return;
        };
        let tenant = connection
            .tenant
            .as_ref()
            .map(|value| value.route.tenant_id.as_str())
            .unwrap_or("");
        let account = connection
            .identity
            .as_ref()
            .map(|value| value.account.id.as_str())
            .unwrap_or("");
        let result = async {
            let mut transaction = control.begin_control_transaction(false).await?;
            sqlx::query(
                r#"INSERT INTO gonvex_performance_events
                   (project_id,event_id,tenant_id,account_id,kind,path,reason,outcome,error,
                    client_sent_at_ms,client_received_at_ms,client_duration_ms,trace,device)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                   ON CONFLICT(project_id,event_id) DO NOTHING"#,
            )
            .bind(&connection.project_id)
            .bind(id)
            .bind(tenant)
            .bind(account)
            .bind(kind)
            .bind(path)
            .bind(reason.unwrap_or(""))
            .bind(outcome)
            .bind(bounded_text(error.unwrap_or(""), 1_000))
            .bind(client_sent_at_ms)
            .bind(client_received_at_ms)
            .bind(client_duration_ms)
            .bind(trace.map(|value| Json(serde_json::to_value(value).unwrap_or(Value::Null))))
            .bind(device.map(|value| Json(serde_json::to_value(value).unwrap_or(Value::Null))))
            .execute(&mut **transaction.transaction())
            .await?;
            // Telemetry has a hard retention bound and never participates in
            // application correctness.
            sqlx::query(
                "DELETE FROM gonvex_performance_events WHERE project_id=$1 AND created_at<now()-interval '14 days'",
            )
            .bind(&connection.project_id)
            .execute(&mut **transaction.transaction())
            .await?;
            transaction.commit().await
        }
        .await;
        if let Err(error) = result {
            tracing::debug!(%error, "performance telemetry was dropped");
        }
    }

    pub async fn register_error_session(
        &self,
        connection: &ControlConnection,
        id: String,
        release: Option<&str>,
        environment: Option<&str>,
    ) -> ServerMessage {
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return error_ack(id, Some("error telemetry is unavailable"), None, None);
        };
        if connection.project_id.is_empty() {
            return error_ack(id, Some("error telemetry is unavailable"), None, None);
        }
        if !self.telemetry_allowed(connection).await {
            return error_ack(id, Some("error telemetry rate limit exceeded"), None, None);
        }
        if let Some(identity) = connection.identity.as_ref() {
            let tenant_id = connection
                .tenant
                .as_ref()
                .map(|tenant| tenant.route.tenant_id.as_str())
                .unwrap_or("");
            let result = async {
                let mut transaction = control.begin_control_transaction(false).await?;
                sqlx::query(
                    r#"INSERT INTO gonvex_support_sessions
                       (id,project_id,tenant_id,account_id,connection_id,release,environment)
                       VALUES($1,$2,$3,$4,$1,$5,$6)
                       ON CONFLICT(id) DO UPDATE SET
                         project_id=EXCLUDED.project_id,
                         tenant_id=EXCLUDED.tenant_id,
                         account_id=EXCLUDED.account_id,
                         connection_id=EXCLUDED.connection_id,
                         release=CASE WHEN EXCLUDED.release<>'' THEN EXCLUDED.release ELSE gonvex_support_sessions.release END,
                         environment=CASE WHEN EXCLUDED.environment<>'' THEN EXCLUDED.environment ELSE gonvex_support_sessions.environment END,
                         last_seen_at=now()"#,
                )
                .bind(&connection.connection_id)
                .bind(&connection.project_id)
                .bind(tenant_id)
                .bind(&identity.account.id)
                .bind(bounded_text(release.unwrap_or(""), 200))
                .bind(bounded_text(environment.unwrap_or(""), 100))
                .execute(&mut **transaction.transaction())
                .await?;
                transaction.commit().await
            }
            .await;
            if result.is_err() {
                return error_ack(id, Some("error store unavailable"), None, None);
            }
            self.notify_control_changed(&connection.project_id);
        }
        error_ack(id, None, None, None)
    }

    pub async fn capture_error_envelope(
        &self,
        connection: &ControlConnection,
        id: String,
        events: Vec<Value>,
    ) -> ServerMessage {
        let Some(control) = self.inner.control_plane.read().await.clone() else {
            return error_ack(id, Some("error telemetry is unavailable"), None, None);
        };
        if connection.project_id.is_empty() {
            return error_ack(id, Some("error telemetry is unavailable"), None, None);
        }
        if !self.telemetry_allowed(connection).await {
            return error_ack(id, Some("error telemetry rate limit exceeded"), None, None);
        }
        if events.is_empty() || events.len() > MAX_EVENTS {
            return error_ack(id, Some("invalid error envelope"), None, None);
        }
        let mut total = 0usize;
        for event in &events {
            let Ok(encoded) = serde_json::to_vec(event) else {
                return error_ack(id, Some("invalid error envelope"), None, None);
            };
            if encoded.len() > MAX_EVENT_BYTES {
                return error_ack(id, Some("error envelope is too large"), None, None);
            }
            total = total.saturating_add(encoded.len());
        }
        if total > MAX_ENVELOPE_BYTES {
            return error_ack(id, Some("error envelope is too large"), None, None);
        }

        let mut accepted = 0u64;
        let mut fingerprints = Vec::new();
        for event in events {
            let Some(event) = authoritative_event(connection, event) else {
                continue;
            };
            let fingerprint = fingerprint(&event);
            let event_id = event
                .get("eventId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| generated_event_id(&event));
            let occurred_at = event
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            match persist_event(&control, &event, &event_id, &fingerprint, occurred_at).await {
                Ok(true) => {
                    accepted += 1;
                    fingerprints.push(fingerprint);
                }
                Ok(false) => {}
                Err(_) => return error_ack(id, Some("error store unavailable"), None, None),
            }
        }
        if accepted > 0 {
            self.notify_control_changed(&connection.project_id);
        }
        error_ack(id, None, Some(accepted), Some(fingerprints))
    }

    async fn telemetry_allowed(&self, connection: &ControlConnection) -> bool {
        let (subject, limit) = match connection.identity.as_ref() {
            Some(identity) => (format!("account:{}", identity.account.id), 120),
            None => (format!("connection:{}", connection.connection_id), 20),
        };
        self.inner
            .telemetry
            .allow(format!("{}:{subject}", connection.project_id), limit)
            .await
    }
}

async fn persist_event(
    control: &gonvex_postgres::ControlPlane,
    event: &Value,
    event_id: &str,
    fingerprint: &str,
    occurred_at: DateTime<Utc>,
) -> Result<bool, gonvex_postgres::DatabaseError> {
    let project = string_field(event, "project");
    let tenant = string_field(event, "tenant");
    let release = string_field(event, "release");
    let environment = string_field(event, "environment");
    let account = event
        .get("account")
        .and_then(Value::as_object)
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let device = string_field(event, "deviceId");
    let level = match string_field(event, "level") {
        "warning" => "warning",
        _ => "error",
    };
    let title = bounded_text(string_field(event, "message"), 500);
    let culprit = bounded_text(string_field(event, "culprit"), 500);

    let mut transaction = control.begin_control_transaction(false).await?;
    let inserted = sqlx::query(
        r#"INSERT INTO gonvex_error_events
           (project_id,event_id,fingerprint,occurred_at,tenant_id,release,level,account_id,device_id,payload)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
           ON CONFLICT(project_id,event_id) DO NOTHING"#,
    )
    .bind(project)
    .bind(event_id)
    .bind(fingerprint)
    .bind(occurred_at)
    .bind(tenant)
    .bind(release)
    .bind(level)
    .bind(account)
    .bind(device)
    .bind(Json(event.clone()))
    .execute(&mut **transaction.transaction())
    .await?;
    if inserted.rows_affected() == 0 {
        transaction.rollback().await?;
        return Ok(false);
    }
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(format!("{project}:{fingerprint}"))
        .execute(&mut **transaction.transaction())
        .await?;
    let existing = sqlx::query(
        r#"SELECT tenants,releases,environments,accounts,devices
           FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2"#,
    )
    .bind(project)
    .bind(fingerprint)
    .fetch_optional(&mut **transaction.transaction())
    .await?;
    let mut tenants = existing
        .as_ref()
        .map(|row| row.get::<Json<Value>, _>("tenants").0)
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut releases = existing
        .as_ref()
        .map(|row| row.get::<Json<Value>, _>("releases").0)
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut environments = existing
        .as_ref()
        .map(|row| row.get::<Json<Value>, _>("environments").0)
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut accounts = existing
        .as_ref()
        .map(|row| row.get::<Json<Value>, _>("accounts").0)
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut devices = existing
        .as_ref()
        .map(|row| row.get::<Json<Value>, _>("devices").0)
        .unwrap_or_else(|| Value::Object(Map::new()));
    increment(&mut tenants, tenant);
    increment(&mut releases, release);
    increment(&mut environments, environment);
    increment(&mut accounts, account);
    increment(&mut devices, device);
    sqlx::query(
        r#"INSERT INTO gonvex_error_groups
           (project_id,fingerprint,title,culprit,level,first_seen,last_seen,event_count,
            tenants,releases,environments,accounts,devices,latest_event)
           VALUES($1,$2,$3,$4,$5,$6,$6,1,$7,$8,$9,$10,$11,$12)
           ON CONFLICT(project_id,fingerprint) DO UPDATE SET
             title=EXCLUDED.title,culprit=EXCLUDED.culprit,level=EXCLUDED.level,
             last_seen=EXCLUDED.last_seen,event_count=gonvex_error_groups.event_count+1,
             tenants=EXCLUDED.tenants,releases=EXCLUDED.releases,
             environments=EXCLUDED.environments,accounts=EXCLUDED.accounts,
             devices=EXCLUDED.devices,latest_event=EXCLUDED.latest_event,
             regression=(gonvex_error_groups.status='resolved')"#,
    )
    .bind(project)
    .bind(fingerprint)
    .bind(title)
    .bind(culprit)
    .bind(level)
    .bind(occurred_at)
    .bind(Json(tenants))
    .bind(Json(releases))
    .bind(Json(environments))
    .bind(Json(accounts))
    .bind(Json(devices))
    .bind(Json(event.clone()))
    .execute(&mut **transaction.transaction())
    .await?;
    transaction.commit().await?;
    Ok(true)
}

fn authoritative_event(connection: &ControlConnection, value: Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    let message = object.get("message")?.as_str()?.trim();
    if message.is_empty() {
        return None;
    }
    scrub_object(&mut object, 0);
    object.insert(
        "project".to_owned(),
        Value::String(connection.project_id.clone()),
    );
    object.insert(
        "tenant".to_owned(),
        Value::String(
            connection
                .tenant
                .as_ref()
                .map(|tenant| tenant.route.tenant_id.clone())
                .unwrap_or_default(),
        ),
    );
    object.insert(
        "sessionId".to_owned(),
        Value::String(connection.connection_id.clone()),
    );
    match connection.identity.as_ref() {
        Some(identity) => {
            object.insert(
                "account".to_owned(),
                serde_json::json!({"id": identity.account.id}),
            );
        }
        None => {
            object.remove("account");
            object.remove("context");
            object.remove("breadcrumbs");
        }
    }
    Some(Value::Object(object))
}

fn scrub_object(object: &mut Map<String, Value>, depth: usize) {
    object.retain(|key, _| !is_secret_key(key));
    for value in object.values_mut() {
        scrub_value(value, depth + 1);
    }
}

fn scrub_value(value: &mut Value, depth: usize) {
    if depth > 8 {
        *value = Value::String("[Truncated]".to_owned());
        return;
    }
    match value {
        Value::String(text) => *text = bounded_text(text, 4_000),
        Value::Array(values) => {
            values.truncate(50);
            for value in values {
                scrub_value(value, depth + 1);
            }
        }
        Value::Object(object) => {
            while object.len() > 100 {
                if let Some(key) = object.keys().next_back().cloned() {
                    object.remove(&key);
                }
            }
            scrub_object(object, depth + 1);
        }
        _ => {}
    }
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase().replace(['-', '_'], "");
    [
        "password",
        "passwd",
        "secret",
        "token",
        "authorization",
        "cookie",
        "apikey",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn fingerprint(event: &Value) -> String {
    let input = format!(
        "{}\n{}\n{}",
        string_field(event, "name"),
        string_field(event, "message"),
        string_field(event, "culprit")
    );
    hex(&Sha256::digest(input.as_bytes())[..16])
}

fn generated_event_id(event: &Value) -> String {
    let input = format!(
        "{}|{}|{}|{}",
        string_field(event, "timestamp"),
        string_field(event, "message"),
        string_field(event, "stack"),
        string_field(event, "deviceId")
    );
    format!("generated-{}", hex(&Sha256::digest(input.as_bytes())[..12]))
}

fn increment(value: &mut Value, key: &str) {
    if key.is_empty() {
        return;
    }
    let object = value.as_object_mut().expect("counter map");
    let current = object.get(key).and_then(Value::as_u64).unwrap_or(0);
    object.insert(key.to_owned(), Value::from(current.saturating_add(1)));
}

fn string_field<'a>(event: &'a Value, key: &str) -> &'a str {
    event.get(key).and_then(Value::as_str).unwrap_or("")
}

fn bounded_text(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    result
}

fn error_ack(
    id: String,
    error: Option<&str>,
    accepted: Option<u64>,
    fingerprints: Option<Vec<String>>,
) -> ServerMessage {
    let mut payload = BTreeMap::new();
    if let Some(error) = error {
        payload.insert("error".to_owned(), Value::String(error.to_owned()));
    }
    if let Some(accepted) = accepted {
        payload.insert("accepted".to_owned(), Value::from(accepted));
    }
    if let Some(fingerprints) = fingerprints {
        payload.insert(
            "fingerprints".to_owned(),
            Value::Array(fingerprints.into_iter().map(Value::String).collect()),
        );
    }
    ServerMessage::ErrorAck { id, payload }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_context_replaces_spoofed_attribution_and_scrubs_secrets() {
        let connection = ControlConnection {
            connection_id: "connection-real".to_owned(),
            project_id: "project-real".to_owned(),
            identity: Some(gonvex_postgres::SessionIdentity {
                project_id: "project-real".to_owned(),
                account: gonvex_postgres::Account {
                    id: "account-real".to_owned(),
                    email: String::new(),
                    email_verified: true,
                    name: String::new(),
                    avatar_url: String::new(),
                    provider: String::new(),
                },
            }),
            ..ControlConnection::default()
        };
        let event = authoritative_event(
            &connection,
            serde_json::json!({
                "message":"boom","project":"spoof","tenant":"spoof",
                "sessionId":"spoof","account":{"id":"spoof"},
                "context":{"password":"bad","safe":"yes"}
            }),
        )
        .expect("event");
        assert_eq!(event["project"], "project-real");
        assert_eq!(event["sessionId"], "connection-real");
        assert_eq!(event["account"]["id"], "account-real");
        assert!(event["context"].get("password").is_none());
    }

    #[tokio::test]
    async fn limiter_is_bounded_per_subject() {
        let limiter = TelemetryLimiter::default();
        assert!(limiter.allow("one".to_owned(), 1).await);
        assert!(!limiter.allow("one".to_owned(), 1).await);
        assert!(limiter.allow("two".to_owned(), 1).await);
    }
}
