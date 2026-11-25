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
                "DELETE FROM gonvex_performance_events WHERE project_id=$1 AND created_at<CAST(unixepoch('subsec')*1000 AS INTEGER)-1209600000",
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
            let release = release.map(|value| bounded_text(value, 200));
            let environment = environment.map(|value| bounded_text(value, 100));
            // Presence v2: the open socket is the session's liveness. Only a
            // new session or changed attribution needs the durable upsert and
            // the Control Plane change notification.
            if self.inner.presence.support_is_current(
                &connection.connection_id,
                &connection.project_id,
                &connection.connection_id,
                tenant_id,
                &identity.account.id,
                release.as_deref(),
                environment.as_deref(),
            ) {
                return error_ack(id, None, None, None);
            }
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
                         last_seen_at=CAST(unixepoch('subsec')*1000 AS INTEGER)"#,
                )
                .bind(&connection.connection_id)
                .bind(&connection.project_id)
                .bind(tenant_id)
                .bind(&identity.account.id)
                .bind(release.as_deref().unwrap_or(""))
                .bind(environment.as_deref().unwrap_or(""))
                .execute(&mut **transaction.transaction())
                .await?;
                transaction.commit().await
            }
            .await;
            if result.is_err() {
                return error_ack(id, Some("error store unavailable"), None, None);
            }
            // An empty value keeps the stored one (see the upsert above).
            self.inner.presence.support_register(
                &connection.connection_id,
                &connection.project_id,
                &connection.connection_id,
                tenant_id,
                &identity.account.id,
                release.as_deref().filter(|value| !value.is_empty()),
                environment.as_deref().filter(|value| !value.is_empty()),
            );
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
    control: &gonvex_store::ControlPlane,
    event: &Value,
    event_id: &str,
    fingerprint: &str,
    occurred_at: DateTime<Utc>,
) -> Result<bool, gonvex_store::DatabaseError> {
    let occurrences = Occurrences::single(event, occurred_at);
    let mut transaction = control.begin_control_transaction(false).await?;
    let applied = apply_event(
        transaction.transaction(),
        event,
        event_id,
        fingerprint,
        occurred_at,
        &occurrences,
    )
    .await?;
    if applied {
        transaction.commit().await?;
    } else {
        transaction.rollback().await?;
    }
    Ok(applied)
}

/// What one stored event stands for: one browser report, or a window of
/// coalesced runtime failures of the same group (`runtime_errors.rs`).
#[derive(Clone, Debug)]
pub(crate) struct Occurrences {
    pub count: u64,
    pub last_seen: DateTime<Utc>,
    pub tenants: BTreeMap<String, u64>,
    pub releases: BTreeMap<String, u64>,
    pub environments: BTreeMap<String, u64>,
    pub accounts: BTreeMap<String, u64>,
    pub devices: BTreeMap<String, u64>,
}

impl Occurrences {
    pub(crate) fn empty(at: DateTime<Utc>) -> Self {
        Self {
            count: 0,
            last_seen: at,
            tenants: BTreeMap::new(),
            releases: BTreeMap::new(),
            environments: BTreeMap::new(),
            accounts: BTreeMap::new(),
            devices: BTreeMap::new(),
        }
    }

    pub(crate) fn single(event: &Value, at: DateTime<Utc>) -> Self {
        let mut occurrences = Self::empty(at);
        occurrences.add(event, at);
        occurrences
    }

    /// Counts one occurrence attributed like `event`.
    pub(crate) fn add(&mut self, event: &Value, at: DateTime<Utc>) {
        self.count += 1;
        self.last_seen = self.last_seen.max(at);
        bump(&mut self.tenants, string_field(event, "tenant"));
        bump(&mut self.releases, string_field(event, "release"));
        bump(&mut self.environments, string_field(event, "environment"));
        bump(&mut self.accounts, event_account(event));
        bump(&mut self.devices, string_field(event, "deviceId"));
    }

    /// True when every occurrence carries a release older than `resolved`:
    /// stale clients that predate the fix, which must not reopen the group.
    fn all_older_than(&self, resolved: &str) -> bool {
        let versioned: u64 = self.releases.values().sum();
        !resolved.is_empty()
            && self.count > 0
            && versioned == self.count
            && self
                .releases
                .keys()
                .all(|release| crate::runtime_errors::release_is_older(release, resolved))
    }
}

fn bump(counts: &mut BTreeMap<String, u64>, key: &str) {
    if !key.is_empty() {
        *counts.entry(key.to_owned()).or_default() += 1;
    }
}

fn event_account(event: &Value) -> &str {
    event
        .get("account")
        .and_then(Value::as_object)
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// Stores `event` as the representative of `occurrences` and folds them into
/// its group, inside the caller's Control Plane transaction. Returns false
/// when the event id was already stored (nothing changed).
pub(crate) async fn apply_event(
    transaction: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
    event: &Value,
    event_id: &str,
    fingerprint: &str,
    occurred_at: DateTime<Utc>,
    occurrences: &Occurrences,
) -> Result<bool, gonvex_store::DatabaseError> {
    let project = string_field(event, "project");
    let tenant = string_field(event, "tenant");
    let release = string_field(event, "release");
    let account = event_account(event);
    let device = string_field(event, "deviceId");
    let level = match string_field(event, "level") {
        "warning" => "warning",
        _ => "error",
    };
    let title = bounded_text(string_field(event, "message"), 500);
    let culprit = bounded_text(string_field(event, "culprit"), 500);

    let inserted = sqlx::query(
        r#"INSERT INTO gonvex_error_events
           (project_id,event_id,fingerprint,occurred_at,tenant_id,release,level,account_id,device_id,payload)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
           ON CONFLICT(project_id,event_id) DO NOTHING"#,
    )
    .bind(project)
    .bind(event_id)
    .bind(fingerprint)
    .bind(occurred_at.timestamp_millis())
    .bind(tenant)
    .bind(release)
    .bind(level)
    .bind(account)
    .bind(device)
    .bind(Json(event.clone()))
    .execute(&mut **transaction)
    .await?;
    if inserted.rows_affected() == 0 {
        return Ok(false);
    }
    // The group read-modify-write below runs on the database's single writer
    // connection, so no other transaction can interleave with it.
    let existing = sqlx::query(
        r#"SELECT tenants,releases,environments,accounts,devices,status,regression,resolved_release
           FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2"#,
    )
    .bind(project)
    .bind(fingerprint)
    .fetch_optional(&mut **transaction)
    .await?;
    let counter = |name: &str| {
        existing
            .as_ref()
            .map(|row| row.get::<Json<Value>, _>(name).0)
            .unwrap_or_else(|| Value::Object(Map::new()))
    };
    let mut tenants = counter("tenants");
    let mut releases = counter("releases");
    let mut environments = counter("environments");
    let mut accounts = counter("accounts");
    let mut devices = counter("devices");
    increment_by(&mut tenants, &occurrences.tenants);
    increment_by(&mut releases, &occurrences.releases);
    increment_by(&mut environments, &occurrences.environments);
    increment_by(&mut accounts, &occurrences.accounts);
    increment_by(&mut devices, &occurrences.devices);
    let status = existing
        .as_ref()
        .map(|row| row.get::<String, _>("status"))
        .unwrap_or_else(|| "unresolved".to_owned());
    let regression = existing
        .as_ref()
        .is_some_and(|row| row.get::<bool, _>("regression"));
    let resolved_release = existing
        .as_ref()
        .map(|row| row.get::<String, _>("resolved_release"))
        .unwrap_or_default();
    // A new occurrence reopens a resolved group as a regression, unless every
    // occurrence comes from a release older than the one the fix shipped in:
    // a stale client still running the broken build is counted but cannot
    // resurrect a fixed error. Ignored groups stay ignored.
    let (status, regression, resolved_release) =
        if status == "resolved" && !occurrences.all_older_than(&resolved_release) {
            ("unresolved".to_owned(), true, String::new())
        } else {
            (status, regression, resolved_release)
        };
    sqlx::query(
        r#"INSERT INTO gonvex_error_groups
           (project_id,fingerprint,title,culprit,level,first_seen,last_seen,event_count,
            tenants,releases,environments,accounts,devices,latest_event,status,regression,resolved_release)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
           ON CONFLICT(project_id,fingerprint) DO UPDATE SET
             title=EXCLUDED.title,culprit=EXCLUDED.culprit,level=EXCLUDED.level,
             last_seen=max(gonvex_error_groups.last_seen,EXCLUDED.last_seen),
             event_count=gonvex_error_groups.event_count+EXCLUDED.event_count,
             tenants=EXCLUDED.tenants,releases=EXCLUDED.releases,
             environments=EXCLUDED.environments,accounts=EXCLUDED.accounts,
             devices=EXCLUDED.devices,latest_event=EXCLUDED.latest_event,
             status=EXCLUDED.status,regression=EXCLUDED.regression,
             resolved_release=EXCLUDED.resolved_release"#,
    )
    .bind(project)
    .bind(fingerprint)
    .bind(title)
    .bind(culprit)
    .bind(level)
    .bind(occurred_at.timestamp_millis())
    .bind(occurrences.last_seen.max(occurred_at).timestamp_millis())
    .bind(i64::try_from(occurrences.count.max(1)).unwrap_or(i64::MAX))
    .bind(Json(tenants))
    .bind(Json(releases))
    .bind(Json(environments))
    .bind(Json(accounts))
    .bind(Json(devices))
    .bind(Json(event.clone()))
    .bind(status)
    .bind(regression)
    .bind(resolved_release)
    .execute(&mut **transaction)
    .await?;
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

pub(crate) fn scrub_object(object: &mut Map<String, Value>, depth: usize) {
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

fn increment_by(value: &mut Value, counts: &BTreeMap<String, u64>) {
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    let object = value.as_object_mut().expect("counter map");
    for (key, count) in counts {
        let current = object.get(key).and_then(Value::as_u64).unwrap_or(0);
        object.insert(key.clone(), Value::from(current.saturating_add(*count)));
    }
}

pub(crate) fn string_field<'a>(event: &'a Value, key: &str) -> &'a str {
    event.get(key).and_then(Value::as_str).unwrap_or("")
}

pub(crate) fn bounded_text(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
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
            identity: Some(gonvex_store::SessionIdentity {
                project_id: "project-real".to_owned(),
                account: gonvex_store::Account {
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
