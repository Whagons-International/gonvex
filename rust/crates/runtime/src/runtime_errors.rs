//! Server-side function failures in the error inbox, and the inbox's
//! retention.
//!
//! Every Query, Reducer and Action execution ends in one of three counted
//! wrappers in `execution.rs` (WebSocket calls, HTTP/service principals, cron
//! and outbox Actions, nested agent calls all go through them). An `Err` there
//! is handed to [`Runtime::capture_runtime_error`], which renders it as an
//! error event and queues it without waiting: telemetry never delays a call.
//! One background worker drains the queue and writes everything that arrived
//! since its previous write in one Control Plane transaction, at most every
//! [`MIN_FLUSH_INTERVAL`]. Each occurrence is stored as its own event with its
//! full context; only in a storm (more than [`STORM_EVENTS_PER_FLUSH`]
//! occurrences of one group in one write) are the extra occurrences folded
//! into the last stored event, whose `count` and the group's counters stay
//! exact. A hot failing loop therefore costs a few transactions per second,
//! not one per call.
//!
//! The same module owns the error store's retention job and the
//! release-based prune used by `POST /dev/errors/prune`.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use gonvex_protocol::ReducerErrorClass;
use gonvex_store::{ControlPlane, DatabaseError, TenantSession};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::Row;
use tokio::sync::mpsc;

use crate::execution::ExecutionError;
use crate::telemetry::{apply_event, bounded_text, hex, Occurrences};
use crate::Runtime;

/// Failures waiting for the worker. A full queue drops the newest failure
/// (with a warning) instead of blocking the call that failed.
pub(crate) const QUEUE_CAPACITY: usize = 1_024;
/// Minimum time between two writes. Failures arriving meanwhile are written
/// together in the next transaction, so a storm costs at most a few
/// transactions per second however fast the calls fail.
pub(crate) const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// Occurrences of one group stored as individual events per write. Beyond
/// this (a storm), further occurrences in the same write are folded into the
/// last stored event: its `count` and the group's counters stay exact.
pub(crate) const STORM_EVENTS_PER_FLUSH: usize = 10;
/// A pending write with this many stored events is written without waiting.
pub(crate) const MAX_EVENTS_PER_FLUSH: usize = 512;
/// Execution ids kept on a coalesced event (the count is always exact).
const MAX_EXECUTION_IDS: usize = 20;
const MAX_MESSAGE_CHARS: usize = 1_000;
const MAX_STACK_CHARS: usize = 8_000;

/// Error-store retention. Each value can be overridden with the named
/// environment variable (whole days / counts); see [`RetentionPolicy::from_env`].
pub(crate) const EVENT_RETENTION_DAYS: i64 = 30; // GONVEX_ERROR_EVENT_RETENTION_DAYS
pub(crate) const EVENTS_PER_GROUP: i64 = 100; // GONVEX_ERROR_EVENTS_PER_GROUP
pub(crate) const AUTO_RESOLVE_DAYS: i64 = 14; // GONVEX_ERROR_AUTO_RESOLVE_DAYS
pub(crate) const CLOSED_GROUP_RETENTION_DAYS: i64 = 90; // GONVEX_ERROR_CLOSED_GROUP_RETENTION_DAYS
pub(crate) const RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Rows changed per retention transaction, and transactions per rule and
/// pass: one pass never holds the Control Plane writer for long, and a large
/// backlog drains over several hourly passes.
const RETENTION_BATCH: i64 = 1_000;
const RETENTION_MAX_BATCHES: usize = 50;
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

/// One failed execution, rendered on the calling task (cheap) and persisted
/// by the worker.
#[derive(Clone, Debug)]
pub(crate) struct RuntimeErrorEntry {
    pub project: String,
    pub tenant: String,
    pub account: String,
    pub account_email: String,
    pub account_name: String,
    pub member_id: String,
    pub member_name: String,
    pub kind: &'static str,
    pub path: String,
    pub execution_id: String,
    pub command_id: Option<String>,
    pub raw_error: String,
    pub class: &'static str,
    /// `error` for failures, `warning` for protocol states the SDK resolves
    /// itself (see [`capture_class`]).
    pub level: &'static str,
    pub client_release: String,
    pub client_environment: String,
    pub connection_id: Option<String>,
    pub channel: Option<&'static str>,
    /// The calling connection's device report (`BrowserTelemetryInfo`).
    pub device: Option<Value>,
    /// Argument structure (types, no values) and encoded size.
    pub args_shape: Value,
    pub args_bytes: usize,
    pub duration_ms: f64,
    pub occurred_at: DateTime<Utc>,
}

/// Where an execution came from, taken before the execution consumes its
/// access record.
#[derive(Clone, Debug, Default)]
pub(crate) struct ErrorOrigin {
    pub connection_id: Option<String>,
    pub channel: Option<&'static str>,
}

pub(crate) struct RuntimeErrorCapture {
    sender: mpsc::Sender<RuntimeErrorEntry>,
    receiver: Mutex<Option<mpsc::Receiver<RuntimeErrorEntry>>>,
    dropped: AtomicU64,
    /// Device reports of open connections, for attributing their failures.
    devices: Mutex<HashMap<String, Value>>,
}

/// Forgets a connection's device report when its socket task ends.
pub(crate) struct ConnectionDeviceGuard {
    runtime: std::sync::Weak<crate::RuntimeInner>,
    connection: String,
}

impl ConnectionDeviceGuard {
    pub(crate) fn new(runtime: &Runtime, connection: &str) -> Self {
        Self {
            runtime: Arc::downgrade(&runtime.inner),
            connection: connection.to_owned(),
        }
    }
}

impl Drop for ConnectionDeviceGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.runtime.upgrade() {
            lock(&inner.runtime_errors.devices).remove(&self.connection);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Default for RuntimeErrorCapture {
    fn default() -> Self {
        Self::with_capacity(QUEUE_CAPACITY)
    }
}

impl RuntimeErrorCapture {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
            dropped: AtomicU64::new(0),
            devices: Mutex::new(HashMap::new()),
        }
    }

    /// Never waits. Returns false when the entry was dropped.
    pub(crate) fn enqueue(&self, entry: RuntimeErrorEntry) -> bool {
        match self.sender.try_send(entry) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(entry)) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                // The first drop and every 100th after it: a storm that fills
                // the queue must not also flood the log.
                if dropped % 100 == 1 {
                    tracing::warn!(
                        project = %entry.project, kind = entry.kind, path = %entry.path,
                        dropped, "runtime error capture queue is full; dropped failure"
                    );
                }
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn take_receiver(&self) -> Option<mpsc::Receiver<RuntimeErrorEntry>> {
        lock(&self.receiver).take()
    }
}

/// The class tag and level of a failure.
///
/// Every failed execution is recorded. `rejected` (application throws,
/// argument validation, permission and data conflicts) and `transient`
/// (infrastructure) are errors. `update_required` (the client's Reducer
/// artifact or contract predates the deployed module) and `unauthenticated`
/// (the connection has no usable session) are recorded as warnings: the SDK
/// keeps the intent, installs the new build or re-authenticates, and replays
/// it, and every client with queued intents meets `update_required` on every
/// deploy. They stay visible, and filterable, without burying real failures.
/// A client that disconnects mid-call drops the execution future before it
/// returns, so a call the client abandoned never reaches capture.
///
/// Expected-version conflicts are application throws: the runtime cannot
/// tell them from other rejections, the SDK does not retry a `rejected` call,
/// and a conflict that keeps happening is worth seeing. They are errors with
/// class `rejected`.
pub(crate) fn capture_class(error: &ExecutionError) -> (&'static str, &'static str) {
    match error.reducer_error_class() {
        ReducerErrorClass::Rejected => ("rejected", "error"),
        ReducerErrorClass::Transient => ("transient", "error"),
        ReducerErrorClass::UpdateRequired => ("update_required", "warning"),
        ReducerErrorClass::Unauthenticated => ("unauthenticated", "warning"),
    }
}

const SHAPE_MAX_DEPTH: usize = 4;
const SHAPE_MAX_KEYS: usize = 50;

/// The structure of call arguments without their values: object keys (secret
/// looking keys removed by the event scrubber), array lengths with the shape
/// of the first element, and scalar types.
pub(crate) fn args_shape(value: &Value, depth: usize) -> Value {
    match value {
        Value::Null => json!("null"),
        Value::Bool(_) => json!("boolean"),
        Value::Number(_) => json!("number"),
        Value::String(text) => json!(format!("string({})", text.chars().count())),
        Value::Array(values) if depth >= SHAPE_MAX_DEPTH => {
            json!(format!("array({})", values.len()))
        }
        Value::Array(values) => json!({
            "array": values.len(),
            "items": values.first().map(|first| args_shape(first, depth + 1)).unwrap_or(Value::Null),
        }),
        Value::Object(object) if depth >= SHAPE_MAX_DEPTH => {
            json!(format!("object({})", object.len()))
        }
        Value::Object(object) => {
            let mut shape: Map<String, Value> = object
                .iter()
                .take(SHAPE_MAX_KEYS)
                .map(|(key, value)| (key.clone(), args_shape(value, depth + 1)))
                .collect();
            if object.len() > SHAPE_MAX_KEYS {
                shape.insert(
                    "…".to_owned(),
                    json!(format!("{} more keys", object.len() - SHAPE_MAX_KEYS)),
                );
            }
            Value::Object(shape)
        }
    }
}

impl Runtime {
    /// Remembers the device report of a connection for failure attribution.
    pub(crate) fn register_connection_device(
        &self,
        connection: &str,
        device: Option<&gonvex_protocol::BrowserTelemetryInfo>,
    ) {
        let mut devices = lock(&self.inner.runtime_errors.devices);
        match device.and_then(|device| serde_json::to_value(device).ok()) {
            Some(device) => {
                devices.insert(connection.to_owned(), device);
            }
            None => {
                devices.remove(connection);
            }
        }
    }
}

impl Runtime {
    /// Queues one failed execution for the error inbox. Never blocks.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn capture_runtime_error(
        &self,
        session: &TenantSession,
        kind: &'static str,
        path: &str,
        command_id: Option<&str>,
        origin: ErrorOrigin,
        args: &Value,
        error: &ExecutionError,
        duration: Duration,
    ) {
        let (class, level) = capture_class(error);
        let project = session.identity.project_id.trim();
        if project.is_empty() {
            return;
        }
        let (client_release, client_environment) = origin
            .connection_id
            .as_deref()
            .and_then(|connection| self.inner.presence.support_attribution(connection))
            .unwrap_or_default();
        let device = origin.connection_id.as_deref().and_then(|connection| {
            lock(&self.inner.runtime_errors.devices)
                .get(connection)
                .cloned()
        });
        let account = &session.identity.account;
        self.inner.runtime_errors.enqueue(RuntimeErrorEntry {
            project: project.to_owned(),
            tenant: session.route.tenant_id.clone(),
            account: account.id.clone(),
            account_email: account.email.clone(),
            account_name: account.name.clone(),
            member_id: session.member.id.clone(),
            member_name: session.member.display_name.clone(),
            kind,
            path: path.to_owned(),
            execution_id: uuid::Uuid::new_v4().to_string(),
            command_id: command_id.map(str::to_owned),
            raw_error: error.to_string(),
            class,
            level,
            client_release,
            client_environment,
            connection_id: origin.connection_id,
            channel: origin.channel,
            device,
            args_shape: args_shape(args, 0),
            args_bytes: serde_json::to_vec(args)
                .map(|bytes| bytes.len())
                .unwrap_or(0),
            duration_ms: duration.as_secs_f64() * 1_000.0,
            occurred_at: Utc::now(),
        });
    }

    /// Starts the capture worker and the hourly retention job. Idempotent.
    pub(crate) fn start_error_tasks(&self) {
        if let Some(receiver) = self.inner.runtime_errors.take_receiver() {
            tokio::spawn(capture_worker(Arc::downgrade(&self.inner), receiver));
        }
        let weak = Arc::downgrade(&self.inner);
        let policy = RetentionPolicy::from_env();
        tokio::spawn(async move {
            // First pass a few minutes after boot (not during startup work),
            // at a random offset so replicas do not run it together.
            let offset =
                Duration::from_secs(120) + Duration::from_secs(300).mul_f64(rand::random::<f64>());
            let mut interval =
                tokio::time::interval_at(tokio::time::Instant::now() + offset, RETENTION_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(inner) = weak.upgrade() else { return };
                let control = inner.control_plane.read().await.clone();
                drop(inner);
                let Some(control) = control else { continue };
                match run_error_retention(&control, Utc::now().timestamp_millis(), &policy).await {
                    Ok(report) if report.changed() => {
                        tracing::info!(?report, "error store retention pass")
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(%error, "error store retention pass failed"),
                }
            }
        });
    }

    /// Writes every queued failure now (tests). Production uses the worker.
    #[cfg(test)]
    pub(crate) async fn flush_runtime_errors_for_test(&self) -> usize {
        let mut receiver = self
            .inner
            .runtime_errors
            .take_receiver()
            .expect("capture worker is not running");
        let mut window = Coalescer::default();
        while let Ok(entry) = receiver.try_recv() {
            window.push(entry);
        }
        let events = window.len();
        flush(&self.inner, window).await;
        *lock(&self.inner.runtime_errors.receiver) = Some(receiver);
        events
    }
}

async fn capture_worker(
    runtime: std::sync::Weak<crate::RuntimeInner>,
    mut receiver: mpsc::Receiver<RuntimeErrorEntry>,
) {
    let mut last_flush: Option<tokio::time::Instant> = None;
    loop {
        // Wait for the first failure, then write as soon as the minimum
        // interval since the previous write allows, taking every failure that
        // arrived meanwhile into the same transaction.
        let Some(first) = receiver.recv().await else {
            return;
        };
        let mut window = Coalescer::default();
        window.push(first);
        let ready_at =
            last_flush.map_or_else(tokio::time::Instant::now, |at| at + MIN_FLUSH_INTERVAL);
        let mut closed = false;
        while window.len() < MAX_EVENTS_PER_FLUSH {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(ready_at) => break,
                entry = receiver.recv() => match entry {
                    Some(entry) => window.push(entry),
                    None => {
                        closed = true;
                        break;
                    }
                },
            }
        }
        while let Ok(entry) = receiver.try_recv() {
            window.push(entry);
        }
        let Some(inner) = runtime.upgrade() else {
            return;
        };
        flush(&inner, window).await;
        drop(inner);
        last_flush = Some(tokio::time::Instant::now());
        if closed {
            return;
        }
    }
}

/// One stored event and the occurrences it stands for.
#[derive(Debug)]
pub(crate) struct CoalescedGroup {
    /// The occurrence stored as the event.
    pub first: RuntimeErrorEntry,
    pub occurrences: Occurrences,
    pub execution_ids: Vec<String>,
}

/// Failures of one write, by group. Each occurrence is its own event until a
/// group passes [`STORM_EVENTS_PER_FLUSH`] in one write.
#[derive(Debug, Default)]
pub(crate) struct Coalescer {
    groups: BTreeMap<(String, String), Vec<CoalescedGroup>>,
    events: usize,
}

impl Coalescer {
    pub(crate) fn push(&mut self, entry: RuntimeErrorEntry) {
        let fingerprint = runtime_fingerprint(&entry);
        let event = attribution(&entry);
        let stored = self
            .groups
            .entry((entry.project.clone(), fingerprint))
            .or_default();
        if stored.len() < STORM_EVENTS_PER_FLUSH {
            self.events += 1;
            stored.push(CoalescedGroup {
                occurrences: Occurrences::empty(entry.occurred_at),
                execution_ids: Vec::new(),
                first: entry.clone(),
            });
        }
        let group = stored.last_mut().expect("stored event");
        group.occurrences.add(&event, entry.occurred_at);
        if group.execution_ids.len() < MAX_EXECUTION_IDS {
            group.execution_ids.push(entry.execution_id);
        }
    }

    /// Stored events pending.
    pub(crate) fn len(&self) -> usize {
        self.events
    }

    pub(crate) fn into_groups(self) -> impl Iterator<Item = (String, CoalescedGroup)> {
        self.groups
            .into_iter()
            .flat_map(|((_, fingerprint), stored)| {
                stored
                    .into_iter()
                    .map(move |group| (fingerprint.clone(), group))
            })
    }
}

async fn flush(inner: &Arc<crate::RuntimeInner>, window: Coalescer) {
    if window.len() == 0 {
        return;
    }
    let Some(control) = inner.control_plane.read().await.clone() else {
        tracing::debug!(
            groups = window.len(),
            "runtime errors dropped: no Control Plane"
        );
        return;
    };
    let module_versions = {
        let mut versions = BTreeMap::new();
        for (project, _) in window.groups.keys() {
            if !versions.contains_key(project) {
                let version = inner
                    .modules
                    .project(project)
                    .await
                    .map(|module| module_version(module.generation, &module.artifact_hash))
                    .unwrap_or_default();
                versions.insert(project.clone(), version);
            }
        }
        versions
    };
    let runtime_version = inner.config.runtime_version.clone();
    match persist_window(&control, window, &module_versions, &runtime_version).await {
        Ok(projects) => {
            let runtime = Runtime {
                inner: inner.clone(),
            };
            for project in projects {
                runtime.notify_control_changed(&project);
            }
        }
        Err(error) => tracing::warn!(%error, "runtime error capture write failed"),
    }
}

/// Writes one window in a single transaction. Returns the projects changed.
pub(crate) async fn persist_window(
    control: &ControlPlane,
    window: Coalescer,
    module_versions: &BTreeMap<String, String>,
    runtime_version: &str,
) -> Result<Vec<String>, DatabaseError> {
    let mut transaction = control.begin_control_transaction(false).await?;
    let mut projects = Vec::<String>::new();
    let mut tenant_names = BTreeMap::<(String, String), String>::new();
    for (fingerprint, group) in window.into_groups() {
        let module_version = module_versions
            .get(&group.first.project)
            .map(String::as_str)
            .unwrap_or("");
        let tenant_key = (group.first.project.clone(), group.first.tenant.clone());
        if !tenant_names.contains_key(&tenant_key) {
            let name = sqlx::query_scalar::<_, String>(
                "SELECT name FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 LIMIT 1",
            )
            .bind(&tenant_key.0)
            .bind(&tenant_key.1)
            .fetch_optional(&mut **transaction.transaction())
            .await?
            .unwrap_or_default();
            tenant_names.insert(tenant_key.clone(), name);
        }
        let event = runtime_event(
            &group,
            module_version,
            runtime_version,
            &tenant_names[&tenant_key],
        );
        let event_id = format!("runtime-{}", group.first.execution_id);
        let applied = apply_event(
            transaction.transaction(),
            &event,
            &event_id,
            &fingerprint,
            group.first.occurred_at,
            &group.occurrences,
        )
        .await?;
        if applied && !projects.contains(&group.first.project) {
            projects.push(group.first.project.clone());
        }
    }
    // Like the first browser envelope, the first server failure turns on the
    // project's Errors view.
    for project in &projects {
        sqlx::query(
            "UPDATE gonvex_runtime_projects SET error_tracking_enabled=1 WHERE id=$1 AND error_tracking_enabled=0",
        )
        .bind(project)
        .execute(&mut **transaction.transaction())
        .await?;
    }
    transaction.commit().await?;
    Ok(projects)
}

/// The fields the group counters read, for one occurrence.
fn attribution(entry: &RuntimeErrorEntry) -> Value {
    let mut event = json!({
        "tenant": entry.tenant,
        "release": entry.client_release,
        "environment": "server",
    });
    if !entry.account.is_empty() {
        event["account"] = json!({"id": entry.account});
    }
    event
}

fn module_version(generation: u64, artifact_hash: &str) -> String {
    let hash = artifact_hash.trim_start_matches("sha256:");
    format!("g{generation}:{}", &hash[..hash.len().min(12)])
}

/// The stored event for a window of one group.
pub(crate) fn runtime_event(
    group: &CoalescedGroup,
    module_version: &str,
    runtime_version: &str,
    tenant_name: &str,
) -> Value {
    let entry = &group.first;
    let message = clean_message(&entry.raw_error);
    let mut tags = Map::new();
    tags.insert("source".into(), json!("runtime"));
    tags.insert("kind".into(), json!(entry.kind));
    tags.insert("path".into(), json!(entry.path));
    tags.insert("class".into(), json!(entry.class));
    if !module_version.is_empty() {
        tags.insert("moduleVersion".into(), json!(module_version));
    }
    if !runtime_version.is_empty() {
        tags.insert("runtimeVersion".into(), json!(runtime_version));
    }
    if !entry.client_environment.is_empty() {
        tags.insert("clientEnvironment".into(), json!(entry.client_environment));
    }
    let mut context = Map::new();
    context.insert("executionId".into(), json!(entry.execution_id));
    context.insert(
        "durationMs".into(),
        json!((entry.duration_ms * 1_000.0).round() / 1_000.0),
    );
    context.insert("occurrences".into(), json!(group.occurrences.count));
    context.insert("executionIds".into(), json!(group.execution_ids));
    if let Some(command) = entry.command_id.as_deref() {
        context.insert("commandId".into(), json!(command));
    }
    if let Some(connection) = entry.connection_id.as_deref() {
        context.insert("connectionId".into(), json!(connection));
    }
    if let Some(channel) = entry.channel {
        context.insert("channel".into(), json!(channel));
    }
    if !entry.member_id.is_empty() {
        let mut member = json!({"id": entry.member_id});
        if !entry.member_name.is_empty() {
            member["name"] = json!(entry.member_name);
        }
        context.insert("member".into(), member);
    }
    if !entry.client_environment.is_empty() {
        context.insert("clientEnvironment".into(), json!(entry.client_environment));
    }
    let mut event = json!({
        "eventId": format!("runtime-{}", entry.execution_id),
        "timestamp": entry.occurred_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "level": entry.level,
        "name": format!("{}Error", entry.kind),
        "message": message,
        "culprit": format!("{} {}", entry.kind, entry.path),
        "stack": bounded_text(entry.raw_error.trim(), MAX_STACK_CHARS),
        "project": entry.project,
        "tenant": entry.tenant,
        "environment": "server",
        "release": entry.client_release,
        "count": group.occurrences.count,
        "tags": tags,
        "context": context,
    });
    if !entry.account.is_empty() {
        let mut account = json!({"id": entry.account});
        if !entry.account_email.is_empty() {
            account["email"] = json!(entry.account_email);
        }
        if !entry.account_name.is_empty() {
            account["name"] = json!(entry.account_name);
        }
        event["account"] = account;
    }
    if !tenant_name.is_empty() {
        event["tenantName"] = json!(tenant_name);
    }
    if let Some(device) = entry.device.as_ref() {
        event["device"] = device.clone();
        for (field, tag) in [
            ("platform", "platform"),
            ("deviceType", "deviceType"),
            ("browserName", "browser"),
        ] {
            if let Some(value) = device.get(field).and_then(Value::as_str) {
                event["tags"][tag] = json!(value);
            }
        }
    }
    let Value::Object(mut object) = event else {
        unreachable!()
    };
    crate::telemetry::scrub_object(&mut object, 0);
    // The argument shape is scrubbed on its own so the event's nesting does
    // not count against the scrubber's depth bound.
    let mut shape = match &entry.args_shape {
        Value::Object(shape) => shape.clone(),
        other => Map::from_iter([("value".to_owned(), other.clone())]),
    };
    crate::telemetry::scrub_object(&mut shape, 0);
    if let Some(Value::Object(context)) = object.get_mut("context") {
        context.insert(
            "args".into(),
            json!({"bytes": entry.args_bytes, "shape": shape}),
        );
    }
    Value::Object(object)
}

/// The text clients see (`client_error_text`), bounded.
pub(crate) fn clean_message(raw: &str) -> String {
    let clean = gonvex_protocol::client_error_text(raw);
    let clean = clean.trim();
    let clean = if clean.is_empty() { raw.trim() } else { clean };
    bounded_text(clean, MAX_MESSAGE_CHARS)
}

/// Groups failures per function and normalized message: ids, UUIDs, numbers
/// and hashes in the message do not split one root cause. Browser events keep
/// their own fingerprint (`telemetry::fingerprint`), unchanged.
pub(crate) fn runtime_fingerprint(entry: &RuntimeErrorEntry) -> String {
    let input = format!(
        "runtime\n{}\n{}Error\n{}\n{} {}",
        entry.level,
        entry.kind,
        normalize_message(&clean_message(&entry.raw_error)),
        entry.kind,
        entry.path
    );
    hex(&Sha256::digest(input.as_bytes())[..16])
}

/// Replaces variable tokens in an error message: UUIDs, hex hashes, numbers
/// and mixed letter/digit identifiers.
pub(crate) fn normalize_message(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut token = String::new();
    let flush_token = |token: &mut String, output: &mut String| {
        if !token.is_empty() {
            output.push_str(&normalize_token(token));
            token.clear();
        }
    };
    for character in message.chars() {
        if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
            token.push(character);
        } else {
            flush_token(&mut token, &mut output);
            output.push(character);
        }
    }
    flush_token(&mut token, &mut output);
    output
}

fn normalize_token(token: &str) -> String {
    if !token.bytes().any(|byte| byte.is_ascii_digit()) {
        return token.to_owned();
    }
    if is_uuid(token) {
        return "<uuid>".to_owned();
    }
    if token.bytes().all(|byte| byte.is_ascii_digit()) {
        return "<n>".to_owned();
    }
    if token.len() >= 8 && token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return "<hex>".to_owned();
    }
    // Generated identifiers (`k57abc9dz`, `task_01HXYZ…`, `row-42`).
    if token.len() >= 6 {
        return "<id>".to_owned();
    }
    // Short tokens such as `v2` or `e404`: keep letters, collapse digits.
    let mut output = String::new();
    let mut in_digits = false;
    for character in token.chars() {
        if character.is_ascii_digit() {
            if !in_digits {
                output.push_str("<n>");
            }
            in_digits = true;
        } else {
            output.push(character);
            in_digits = false;
        }
    }
    output
}

fn is_uuid(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

// ---- Releases ---------------------------------------------------------------

/// Numeric components of a release such as `5.4.90+288d9aa` or `v5.4.90`:
/// the part before `+` (build metadata) and `-` (pre-release label).
pub(crate) fn parse_release(release: &str) -> Option<Vec<u64>> {
    let version = release.trim();
    let version = version.split('+').next()?.split('-').next()?;
    let version = version.strip_prefix(['v', 'V']).unwrap_or(version);
    if version.is_empty() {
        return None;
    }
    version
        .split('.')
        .map(|part| part.parse::<u64>().ok())
        .collect()
}

pub(crate) fn compare_releases(left: &[u64], right: &[u64]) -> CmpOrdering {
    let length = left.len().max(right.len());
    for index in 0..length {
        let ordering = left
            .get(index)
            .copied()
            .unwrap_or(0)
            .cmp(&right.get(index).copied().unwrap_or(0));
        if ordering != CmpOrdering::Equal {
            return ordering;
        }
    }
    CmpOrdering::Equal
}

/// True only when both releases parse and `release` is older than
/// `reference`. An unparseable release is treated as newer.
pub(crate) fn release_is_older(release: &str, reference: &str) -> bool {
    match (parse_release(release), parse_release(reference)) {
        (Some(release), Some(reference)) => {
            compare_releases(&release, &reference) == CmpOrdering::Less
        }
        _ => false,
    }
}

/// The newest parseable release among `releases` (a group's counter map).
pub(crate) fn newest_release<'a>(releases: impl Iterator<Item = &'a str>) -> Option<String> {
    releases
        .filter_map(|release| parse_release(release).map(|parsed| (parsed, release)))
        .max_by(|left, right| compare_releases(&left.0, &right.0))
        .map(|(_, release)| release.to_owned())
}

// ---- Retention --------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct RetentionPolicy {
    pub event_retention_days: i64,
    pub events_per_group: i64,
    pub auto_resolve_days: i64,
    pub closed_group_retention_days: i64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            event_retention_days: EVENT_RETENTION_DAYS,
            events_per_group: EVENTS_PER_GROUP,
            auto_resolve_days: AUTO_RESOLVE_DAYS,
            closed_group_retention_days: CLOSED_GROUP_RETENTION_DAYS,
        }
    }
}

impl RetentionPolicy {
    pub(crate) fn from_env() -> Self {
        let read = |name: &str, default: i64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<i64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(default)
        };
        Self {
            event_retention_days: read("GONVEX_ERROR_EVENT_RETENTION_DAYS", EVENT_RETENTION_DAYS),
            events_per_group: read("GONVEX_ERROR_EVENTS_PER_GROUP", EVENTS_PER_GROUP),
            auto_resolve_days: read("GONVEX_ERROR_AUTO_RESOLVE_DAYS", AUTO_RESOLVE_DAYS),
            closed_group_retention_days: read(
                "GONVEX_ERROR_CLOSED_GROUP_RETENTION_DAYS",
                CLOSED_GROUP_RETENTION_DAYS,
            ),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetentionReport {
    pub expired_events: u64,
    pub capped_events: u64,
    pub auto_resolved_groups: u64,
    pub deleted_groups: u64,
    pub deleted_group_events: u64,
}

impl RetentionReport {
    fn changed(&self) -> bool {
        *self != Self::default()
    }
}

/// Runs `statement` (which must bind `$1` = a parameter and `$2` = the batch
/// size) in bounded transactions until a batch changes fewer rows.
async fn batched(
    control: &ControlPlane,
    statement: &str,
    parameter: i64,
) -> Result<u64, DatabaseError> {
    let mut total = 0u64;
    for _ in 0..RETENTION_MAX_BATCHES {
        let mut transaction = control.begin_control_transaction(false).await?;
        let changed = sqlx::query(statement)
            .bind(parameter)
            .bind(RETENTION_BATCH)
            .execute(&mut **transaction.transaction())
            .await?
            .rows_affected();
        transaction.commit().await?;
        total += changed;
        if changed < RETENTION_BATCH as u64 {
            break;
        }
    }
    Ok(total)
}

/// One retention pass over every project's error store:
/// 1. delete events older than `event_retention_days`;
/// 2. keep only the newest `events_per_group` events of each group (group
///    counters keep the full totals);
/// 3. resolve unresolved groups with no event for `auto_resolve_days` (any
///    new occurrence reopens them as a regression);
/// 4. delete resolved and ignored groups last seen more than
///    `closed_group_retention_days` ago, with their events.
pub(crate) async fn run_error_retention(
    control: &ControlPlane,
    now_ms: i64,
    policy: &RetentionPolicy,
) -> Result<RetentionReport, DatabaseError> {
    let mut report = RetentionReport {
        expired_events: batched(
            control,
            r#"DELETE FROM gonvex_error_events WHERE rowid IN (
                 SELECT rowid FROM gonvex_error_events WHERE occurred_at<$1 LIMIT $2)"#,
            now_ms - policy.event_retention_days * DAY_MS,
        )
        .await?,
        ..RetentionReport::default()
    };
    report.capped_events = batched(
        control,
        r#"DELETE FROM gonvex_error_events WHERE rowid IN (
             SELECT event_rowid FROM (
               SELECT event.rowid AS event_rowid,
                      row_number() OVER (PARTITION BY event.project_id,event.fingerprint
                                         ORDER BY event.occurred_at DESC,event.rowid DESC) AS position
               FROM gonvex_error_events AS event
               JOIN (SELECT project_id,fingerprint FROM gonvex_error_events
                     GROUP BY project_id,fingerprint HAVING count(*)>$1) AS heavy
                 ON heavy.project_id=event.project_id AND heavy.fingerprint=event.fingerprint)
             WHERE position>$1 LIMIT $2)"#,
        policy.events_per_group,
    )
    .await?;
    report.auto_resolved_groups = batched(
        control,
        r#"UPDATE gonvex_error_groups SET status='resolved',regression=0,resolved_release=''
           WHERE rowid IN (SELECT rowid FROM gonvex_error_groups
                           WHERE status='unresolved' AND last_seen<$1 LIMIT $2)"#,
        now_ms - policy.auto_resolve_days * DAY_MS,
    )
    .await?;
    let closed_before = now_ms - policy.closed_group_retention_days * DAY_MS;
    for _ in 0..RETENTION_MAX_BATCHES {
        let mut transaction = control.begin_control_transaction(false).await?;
        let groups = sqlx::query(
            r#"SELECT project_id,fingerprint FROM gonvex_error_groups
               WHERE status IN ('resolved','ignored') AND last_seen<$1 LIMIT $2"#,
        )
        .bind(closed_before)
        .bind(RETENTION_BATCH / 10)
        .fetch_all(&mut **transaction.transaction())
        .await?;
        for row in &groups {
            let project = row.get::<String, _>("project_id");
            let fingerprint = row.get::<String, _>("fingerprint");
            report.deleted_group_events += sqlx::query(
                "DELETE FROM gonvex_error_events WHERE project_id=$1 AND fingerprint=$2",
            )
            .bind(&project)
            .bind(&fingerprint)
            .execute(&mut **transaction.transaction())
            .await?
            .rows_affected();
            report.deleted_groups += sqlx::query(
                "DELETE FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2",
            )
            .bind(&project)
            .bind(&fingerprint)
            .execute(&mut **transaction.transaction())
            .await?
            .rows_affected();
        }
        transaction.commit().await?;
        if (groups.len() as i64) < RETENTION_BATCH / 10 {
            break;
        }
    }
    Ok(report)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PruneReport {
    pub deleted_events: u64,
    pub deleted_groups: u64,
    pub updated_groups: u64,
    pub releases: Vec<String>,
}

/// Deletes a project's events whose release is older than `before`, groups
/// whose every recorded release is older, and removes the old releases (and
/// their occurrence counts) from the remaining groups. Events and groups
/// without a parseable release are kept.
pub(crate) async fn prune_errors_before_release(
    control: &ControlPlane,
    project: &str,
    before: &str,
) -> Result<PruneReport, DatabaseError> {
    let mut report = PruneReport::default();
    let mut transaction = control.begin_control_transaction(false).await?;
    let releases = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT release FROM gonvex_error_events WHERE project_id=$1 AND release<>''",
    )
    .bind(project)
    .fetch_all(&mut **transaction.transaction())
    .await?;
    let old: Vec<String> = releases
        .into_iter()
        .filter(|release| release_is_older(release, before))
        .collect();
    if !old.is_empty() {
        report.deleted_events = sqlx::query(
            "DELETE FROM gonvex_error_events WHERE project_id=$1 AND release IN (SELECT value FROM json_each($2))",
        )
        .bind(project)
        .bind(Json(&old))
        .execute(&mut **transaction.transaction())
        .await?
        .rows_affected();
    }
    let groups = sqlx::query(
        "SELECT fingerprint,releases,event_count FROM gonvex_error_groups WHERE project_id=$1",
    )
    .bind(project)
    .fetch_all(&mut **transaction.transaction())
    .await?;
    for row in groups {
        let fingerprint = row.get::<String, _>("fingerprint");
        let Json(releases) = row.get::<Json<Value>, _>("releases");
        let Some(releases) = releases.as_object() else {
            continue;
        };
        let (stale, kept): (Vec<_>, Vec<_>) = releases
            .iter()
            .partition(|(release, _)| release_is_older(release, before));
        if stale.is_empty() {
            continue;
        }
        if kept.is_empty() {
            report.deleted_events += sqlx::query(
                "DELETE FROM gonvex_error_events WHERE project_id=$1 AND fingerprint=$2",
            )
            .bind(project)
            .bind(&fingerprint)
            .execute(&mut **transaction.transaction())
            .await?
            .rows_affected();
            sqlx::query("DELETE FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2")
                .bind(project)
                .bind(&fingerprint)
                .execute(&mut **transaction.transaction())
                .await?;
            report.deleted_groups += 1;
            continue;
        }
        let removed: i64 = stale
            .iter()
            .map(|(_, count)| count.as_i64().unwrap_or(0))
            .sum();
        let remaining: Map<String, Value> = kept
            .into_iter()
            .map(|(release, count)| (release.clone(), count.clone()))
            .collect();
        sqlx::query(
            "UPDATE gonvex_error_groups SET releases=$3,event_count=max(event_count-$4,0) WHERE project_id=$1 AND fingerprint=$2",
        )
        .bind(project)
        .bind(&fingerprint)
        .bind(Json(Value::Object(remaining)))
        .bind(removed)
        .execute(&mut **transaction.transaction())
        .await?;
        report.updated_groups += 1;
    }
    transaction.commit().await?;
    report.releases = old;
    Ok(report)
}

/// The triage update behind `PATCH /dev/errors/groups/:fingerprint`.
///
/// Resolving stores the release the fix shipped in: `resolved_in_release`
/// when given, otherwise the newest release recorded on the group. Later
/// occurrences from older releases are counted without reopening the group
/// (`telemetry::apply_event`). Reopening or ignoring clears it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_error_group(
    transaction: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
    project: &str,
    fingerprint: &str,
    status: &str,
    priority: &str,
    assignee: Option<String>,
    resolved_in_release: Option<&str>,
) -> Result<Option<sqlx::sqlite::SqliteRow>, DatabaseError> {
    let resolved_release = if status == "resolved" {
        match resolved_in_release {
            Some(release) => bounded_text(release.trim(), 200),
            None => {
                let releases = sqlx::query_scalar::<_, Json<Value>>(
                    "SELECT releases FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2",
                )
                .bind(project)
                .bind(fingerprint)
                .fetch_optional(&mut **transaction)
                .await?;
                releases
                    .and_then(|Json(releases)| {
                        releases
                            .as_object()
                            .and_then(|map| newest_release(map.keys().map(String::as_str)))
                    })
                    .unwrap_or_default()
            }
        }
    } else {
        String::new()
    };
    Ok(sqlx::query(
        r#"UPDATE gonvex_error_groups SET
             status=CASE WHEN $3='' THEN status ELSE $3 END,
             priority=CASE WHEN $4='' THEN priority ELSE $4 END,
             assignee=COALESCE($5,assignee),
             resolved_release=CASE WHEN $3='' THEN resolved_release WHEN $3='resolved' THEN $6 ELSE '' END,
             regression=CASE WHEN $3='resolved' THEN 0 ELSE regression END
           WHERE project_id=$1 AND fingerprint=$2 RETURNING *"#,
    )
    .bind(project)
    .bind(fingerprint)
    .bind(status)
    .bind(priority)
    .bind(assignee.map(|value| value.trim().to_owned()))
    .bind(resolved_release)
    .fetch_optional(&mut **transaction)
    .await?)
}

#[cfg(test)]
mod tests;
