//! Reducer log feed (Cloudflare package P04). Off by default.
//!
//! Shadow running replays every committed Reducer call of a tenant into a
//! second backend, in commit order. The existing tables do not hold enough to
//! do that: `_gonvex_sync_transactions` has the revision, channel and actor
//! but no path, arguments, clock or entropy; `_gonvex_reducer_idempotency`
//! holds only calls that carried a key, and only their result;
//! `gonvex_runtime_reducer_logs` is a Control Plane diagnostic log with no
//! commit order. So the feed is one more per-tenant table.
//!
//! Recording (`GONVEX_REDUCER_FEED=1`). Every Reducer that commits a write
//! transaction appends one row to `_gonvex_reducer_feed` in that same
//! transaction, just before `COMMIT`: path, arguments, the session the call
//! ran as (account, member, delegation), its full invocation provenance
//! (channel, root channel, command ancestry, artifact hash, delegation), the
//! pinned `ctx.now`, `intentEntropy`, idempotency key, receipt path and the
//! SHA-256 of its result. A trigger on `_gonvex_sync_transactions` stamps the
//! revision the commit assigns. A call that changed no tracked rows (a
//! receipt-only or outbox-only write) keeps `revision` NULL. A rolled-back
//! call leaves no row. A Reducer that never touched the database cannot
//! change state and is not recorded. The feed row and the business rows
//! commit or roll back together, so the feed has exactly the committed calls,
//! in commit order (`seq`). A failed append fails the call (fail closed): a
//! feed with holes would let a shadow drift silently.
//!
//! When recording, the runtime stamps `ctx.now` itself (the module host would
//! otherwise stamp the same clock a few microseconds later) so the recorded
//! value is the one the module saw.
//!
//! Reading: `GET /dev/reducer-feed?project&tenant&after=<seq>&limit` (admin
//! key) returns entries after a cursor, plus `gaps`: revisions committed
//! between recorded calls by something other than a Reducer (`control.*`,
//! operator edits, scheduled-job promotion). A replay cannot reproduce those.
//!
//! Replaying (`GONVEX_REDUCER_FEED_REPLAY=1`, a shadow instance only). `POST
//! /dev/reducer-feed/replay` runs entries through the ordinary Reducer path
//! with the recorded session, provenance, clock, entropy and key. A replaying
//! instance does not start the scheduler and never wakes the Action outbox:
//! Actions, crons and scheduled Reducers ran on the source, and the Reducer
//! calls they made are in the feed. `GET /dev/reducer-feed/checksums`
//! returns per-table checksums to compare the two databases.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::TryStreamExt as _;
use gonvex_module_runtime::InvocationProvenance;
use gonvex_store::{
    Account, DatabaseError, Member, SessionDelegation, SessionIdentity, TenantRoute, TenantSession,
    TenantTransaction,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::execution::{ExecutionAccess, ExecutionError};
use crate::Runtime;

/// Records committed Reducer calls when `1`/`true`/`on`.
pub const RECORD_ENV: &str = "GONVEX_REDUCER_FEED";
/// Accepts feed entries to replay when `1`/`true`/`on` (shadow instances).
pub const REPLAY_ENV: &str = "GONVEX_REDUCER_FEED_REPLAY";

const TABLE: &str = "_gonvex_reducer_feed";

/// Created lazily by the first recorded call of a database, inside that
/// call's transaction.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS _gonvex_reducer_feed (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  revision INTEGER,
  command_id TEXT NOT NULL,
  path TEXT NOT NULL,
  args JSON NOT NULL,
  session JSON NOT NULL,
  provenance JSON NOT NULL,
  now_unix_ms INTEGER NOT NULL,
  intent_entropy TEXT,
  idempotency_key TEXT,
  receipt_path TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  result_sha256 TEXT NOT NULL
);
-- The feed row is written before COMMIT, and the revision is assigned by the
-- change-feed finalize after it. One writer per database: the newest feed row
-- is this transaction's, if it has the transaction's command id.
CREATE TRIGGER IF NOT EXISTS _gonvex_reducer_feed_revision
AFTER INSERT ON _gonvex_sync_transactions
BEGIN
  UPDATE _gonvex_reducer_feed SET revision = NEW.revision
  WHERE seq = (SELECT max(seq) FROM _gonvex_reducer_feed)
    AND revision IS NULL AND command_id = NEW.origin_command_id;
END;
"#;

const INSERT: &str = r#"INSERT INTO _gonvex_reducer_feed
  (command_id, path, args, session, provenance, now_unix_ms, intent_entropy,
   idempotency_key, receipt_path, artifact_hash, result_sha256)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#;

/// Tables and columns left out of [`checksums`]. Everything else, including
/// `_gonvex_sync_changes`, receipts and `members`, must match exactly.
const EXCLUDED_TABLES: &[(&str, &str)] = &[
    (TABLE, "recorded on the source only"),
    (
        "sqlite_sequence",
        "AUTOINCREMENT counters, including the feed's own",
    ),
    (
        "sqlite_stat1",
        "planner statistics (ANALYZE, PRAGMA optimize)",
    ),
    (
        "sqlite_stat4",
        "planner statistics (ANALYZE, PRAGMA optimize)",
    ),
    (
        "_gonvex_action_outbox",
        "delivery state: the source drains Actions, a replaying shadow does not",
    ),
    (
        "_gonvex_control_plane_membership_outbox",
        "delivery state stamped with the wall clock by a members trigger",
    ),
];
const EXCLUDED_COLUMNS: &[(&str, &str)] = &[
    ("_gonvex_sync_clock", "epoch"),
    ("_gonvex_sync_changes", "created_at"),
    ("_gonvex_sync_transactions", "created_at"),
    ("_gonvex_reducer_idempotency", "created_at"),
];

const DEFAULT_PAGE: i64 = 500;
const MAX_PAGE: i64 = 5_000;

/// The two switches. Both default off; tests flip them per runtime.
#[derive(Default)]
pub(crate) struct ReducerFeed {
    record: AtomicBool,
    replay: AtomicBool,
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes"
        )
    })
}

impl ReducerFeed {
    pub(crate) fn from_env() -> Self {
        let feed = Self::default();
        feed.set(env_flag(RECORD_ENV), env_flag(REPLAY_ENV));
        feed
    }

    pub(crate) fn set(&self, record: bool, replay: bool) {
        self.record.store(record, Relaxed);
        self.replay.store(replay, Relaxed);
    }

    pub(crate) fn recording(&self) -> bool {
        self.record.load(Relaxed)
    }

    pub(crate) fn replaying(&self) -> bool {
        self.replay.load(Relaxed)
    }
}

/// The session a recorded call ran as.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FeedSession {
    pub project_id: String,
    pub tenant_id: String,
    pub account: Account,
    pub member: Member,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<SessionDelegation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub developer_session: Option<Value>,
}

/// One committed Reducer call.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FeedEntry {
    pub seq: i64,
    /// The revision the call committed; `None` when it changed no tracked rows.
    pub revision: Option<i64>,
    pub command_id: String,
    pub path: String,
    pub args: Value,
    pub session: FeedSession,
    pub provenance: InvocationProvenance,
    pub now_unix_ms: u64,
    pub intent_entropy: Option<String>,
    pub idempotency_key: Option<String>,
    pub receipt_path: String,
    pub artifact_hash: String,
    pub result_sha256: String,
}

/// What a Reducer call needs recorded, taken before it runs (the arguments
/// and provenance move into the invocation).
pub(crate) struct Capture {
    command_id: String,
    path: String,
    args: String,
    session: String,
    provenance: String,
    now_unix_ms: u64,
    intent_entropy: Option<String>,
    idempotency_key: Option<String>,
    receipt_path: String,
    artifact_hash: String,
}

impl Capture {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session: &TenantSession,
        command_id: &str,
        path: &str,
        args: &Value,
        provenance: &InvocationProvenance,
        now_unix_ms: u64,
        intent_entropy: Option<&str>,
        idempotency_key: Option<&str>,
        receipt_path: &str,
        artifact_hash: &str,
    ) -> Self {
        let feed_session = FeedSession {
            project_id: session.identity.project_id.clone(),
            tenant_id: session.route.tenant_id.clone(),
            account: session.identity.account.clone(),
            member: session.member.clone(),
            delegation: session.delegation.clone(),
            developer_session: session.developer_session.clone(),
        };
        let text =
            |value: Result<String, serde_json::Error>| value.unwrap_or_else(|_| "null".into());
        Self {
            command_id: command_id.to_owned(),
            path: path.to_owned(),
            args: text(serde_json::to_string(args)),
            session: text(serde_json::to_string(&feed_session)),
            provenance: text(serde_json::to_string(provenance)),
            now_unix_ms,
            intent_entropy: intent_entropy.map(str::to_owned),
            idempotency_key: idempotency_key.map(str::to_owned),
            receipt_path: receipt_path.to_owned(),
            artifact_hash: artifact_hash.to_owned(),
        }
    }
}

/// SHA-256 of a result's JSON text (the outcome a replay must reproduce).
pub(crate) fn result_sha256(result: &Value) -> String {
    let text = serde_json::to_string(result).unwrap_or_default();
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// Appends the call to the feed inside its own transaction, before `COMMIT`.
pub(crate) async fn append(
    transaction: &mut TenantTransaction,
    capture: &Capture,
    result: &Value,
) -> Result<(), DatabaseError> {
    let hash = result_sha256(result);
    match insert(transaction, capture, &hash).await {
        Err(error) if error.to_string().contains("no such table") => {
            // First recorded call of this database (or the first since a
            // rolled-back call created the table).
            gonvex_store::execute_script(transaction.transaction(), SCHEMA).await?;
            insert(transaction, capture, &hash).await
        }
        other => other,
    }
}

async fn insert(
    transaction: &mut TenantTransaction,
    capture: &Capture,
    hash: &str,
) -> Result<(), DatabaseError> {
    use gonvex_store::direct::Bind::{Integer, Text};
    fn optional(value: &Option<String>) -> gonvex_store::direct::Bind<'_> {
        value.as_deref().map_or(
            gonvex_store::direct::Bind::Null,
            gonvex_store::direct::Bind::Text,
        )
    }
    let session = transaction.direct().await?;
    session
        .run(|conn| {
            conn.execute(
                INSERT,
                &[
                    Text(&capture.command_id),
                    Text(&capture.path),
                    Text(&capture.args),
                    Text(&capture.session),
                    Text(&capture.provenance),
                    Integer(capture.now_unix_ms as i64),
                    optional(&capture.intent_entropy),
                    optional(&capture.idempotency_key),
                    Text(&capture.receipt_path),
                    Text(&capture.artifact_hash),
                    Text(hash),
                ],
            )
        })
        .ok_or_else(|| DatabaseError::Io("the direct database session was released".into()))??;
    Ok(())
}

pub(crate) fn router() -> Router<Runtime> {
    Router::new()
        .route("/dev/reducer-feed", get(read_feed))
        .route("/dev/reducer-feed/replay", post(replay_feed))
        .route("/dev/reducer-feed/checksums", get(read_checksums))
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error": message.into()}))).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TenantQuery {
    project: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    after: i64,
    #[serde(default)]
    limit: i64,
}

/// Admin key only: the feed carries arguments and identities.
async fn admit(
    runtime: &Runtime,
    headers: &HeaderMap,
    enabled: bool,
    project: &str,
    tenant: &str,
) -> Result<TenantRoute, Response> {
    if !enabled {
        return Err(error(StatusCode::NOT_FOUND, "the reducer feed is off"));
    }
    if !crate::operations::admin_key_matches(runtime, headers) {
        return Err(error(StatusCode::UNAUTHORIZED, "the admin key is required"));
    }
    let control = runtime
        .inner
        .control_plane
        .read()
        .await
        .clone()
        .ok_or_else(|| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Control Plane is unavailable",
            )
        })?;
    let tenant = if tenant.trim().is_empty() {
        project
    } else {
        tenant.trim()
    };
    control
        .resolve_tenant(project.trim(), tenant)
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "tenant database was not found"))
}

fn internal(cause: impl std::fmt::Display) -> Response {
    error(StatusCode::INTERNAL_SERVER_ERROR, cause.to_string())
}

async fn read_feed(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<TenantQuery>,
) -> Response {
    let recording = runtime.inner.reducer_feed.recording();
    let route = match admit(&runtime, &headers, recording, &query.project, &query.tenant).await {
        Ok(route) => route,
        Err(response) => return response,
    };
    let limit = match query.limit {
        0 => DEFAULT_PAGE,
        limit => limit.clamp(1, MAX_PAGE),
    };
    match read_page(&runtime, &route, query.after.max(0), limit).await {
        Ok(page) => Json(page).into_response(),
        Err(cause) => internal(cause),
    }
}

/// One page of the feed after `after`, read from one WAL snapshot.
pub(crate) async fn read_page(
    runtime: &Runtime,
    route: &TenantRoute,
    after: i64,
    limit: i64,
) -> Result<Value, DatabaseError> {
    let mut transaction =
        gonvex_store::begin_transaction(&runtime.inner.pools, &route.database_url, true).await?;
    let connection = &mut **transaction.transaction();
    let clock = sqlx::query("SELECT revision, epoch FROM _gonvex_sync_clock WHERE singleton = 1")
        .fetch_optional(&mut *connection)
        .await?;
    let head = clock.map_or(json!(null), |row| {
        json!({"revision": row.get::<i64, _>("revision"), "epoch": row.get::<String, _>("epoch")})
    });
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = $1)",
    )
    .bind(TABLE)
    .fetch_one(&mut *connection)
    .await?;
    if !exists {
        transaction.rollback().await?;
        return Ok(json!({"entries": [], "next": after, "more": false, "gaps": [], "head": head}));
    }
    let rows = sqlx::query(
        r#"SELECT seq, revision, command_id, path, args, session, provenance, now_unix_ms,
                  intent_entropy, idempotency_key, receipt_path, artifact_hash, result_sha256
           FROM _gonvex_reducer_feed WHERE seq > $1 ORDER BY seq LIMIT $2"#,
    )
    .bind(after)
    .bind(limit)
    .fetch_all(&mut *connection)
    .await?;
    let mut entries = Vec::with_capacity(rows.len());
    for row in &rows {
        let parse = |column: &str| -> Result<Value, DatabaseError> {
            serde_json::from_str(&row.get::<String, _>(column))
                .map_err(|cause| DatabaseError::Io(format!("feed {column}: {cause}")))
        };
        entries.push(json!({
            "seq": row.get::<i64, _>("seq"),
            "revision": row.get::<Option<i64>, _>("revision"),
            "commandId": row.get::<String, _>("command_id"),
            "path": row.get::<String, _>("path"),
            "args": parse("args")?,
            "session": parse("session")?,
            "provenance": parse("provenance")?,
            "nowUnixMs": row.get::<i64, _>("now_unix_ms"),
            "intentEntropy": row.get::<Option<String>, _>("intent_entropy"),
            "idempotencyKey": row.get::<Option<String>, _>("idempotency_key"),
            "receiptPath": row.get::<String, _>("receipt_path"),
            "artifactHash": row.get::<String, _>("artifact_hash"),
            "resultSha256": row.get::<String, _>("result_sha256"),
        }));
    }
    let next = rows.last().map_or(after, |row| row.get::<i64, _>("seq"));
    // Revisions inside the span this page covers that no recorded call made.
    let revisions: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.get::<Option<i64>, _>("revision"))
        .collect();
    let mut gaps = Vec::new();
    if let Some(&high) = revisions.last() {
        let before: Option<i64> = sqlx::query_scalar(
            "SELECT revision FROM _gonvex_reducer_feed WHERE seq <= $1 AND revision IS NOT NULL ORDER BY seq DESC LIMIT 1",
        )
        .bind(after)
        .fetch_optional(&mut *connection)
        .await?;
        let low = before.unwrap_or(revisions[0] - 1);
        let foreign = sqlx::query(
            r#"SELECT revision, origin_command_id, invocation_channel FROM _gonvex_sync_transactions
               WHERE revision > $1 AND revision <= $2 ORDER BY revision"#,
        )
        .bind(low)
        .bind(high)
        .fetch_all(&mut *connection)
        .await?;
        for row in foreign {
            let revision: i64 = row.get("revision");
            if revisions.binary_search(&revision).is_err() {
                gaps.push(json!({
                    "revision": revision,
                    "commandId": row.get::<String, _>("origin_command_id"),
                    "channel": row.get::<String, _>("invocation_channel"),
                }));
            }
        }
    }
    transaction.rollback().await?;
    Ok(json!({
        "entries": entries,
        "next": next,
        "more": rows.len() as i64 == limit,
        "gaps": gaps,
        "head": head,
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayRequest {
    project: String,
    #[serde(default)]
    tenant: String,
    entries: Vec<FeedEntry>,
}

async fn replay_feed(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Json(request): Json<ReplayRequest>,
) -> Response {
    let replaying = runtime.inner.reducer_feed.replaying();
    let route = match admit(
        &runtime,
        &headers,
        replaying,
        &request.project,
        &request.tenant,
    )
    .await
    {
        Ok(route) => route,
        Err(response) => return response,
    };
    let report = replay(&runtime, &route, &request.entries).await;
    let status = if report.failure.is_some() {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    (status, Json(report)).into_response()
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayReport {
    pub applied: usize,
    pub last_seq: Option<i64>,
    pub last_revision: Option<i64>,
    /// Entries whose replay committed a different revision number (the
    /// source had writes the feed cannot carry, see `gaps`) or committed a
    /// revision where the source did not, or the reverse.
    pub revision_mismatches: Vec<Value>,
    /// The first entry that failed or returned a different result; replay
    /// stops there.
    pub failure: Option<Value>,
}

/// Applies entries in order on this (replaying) runtime.
pub(crate) async fn replay(
    runtime: &Runtime,
    route: &TenantRoute,
    entries: &[FeedEntry],
) -> ReplayReport {
    let mut report = ReplayReport::default();
    for entry in entries {
        match replay_one(runtime, route, entry).await {
            Ok((hash, revision)) if hash == entry.result_sha256 => {
                let revision = revision.map(|revision| revision as i64);
                if revision != entry.revision {
                    report.revision_mismatches.push(json!({
                        "seq": entry.seq, "source": entry.revision, "replay": revision,
                    }));
                }
                report.applied += 1;
                report.last_seq = Some(entry.seq);
                if revision.is_some() {
                    report.last_revision = revision;
                }
            }
            Ok((hash, _)) => {
                report.failure = Some(json!({
                    "seq": entry.seq, "path": entry.path, "reason": "result differs",
                    "expected": entry.result_sha256, "actual": hash,
                }));
                break;
            }
            Err(cause) => {
                report.failure = Some(json!({
                    "seq": entry.seq, "path": entry.path, "reason": cause,
                }));
                break;
            }
        }
    }
    report
}

async fn replay_one(
    runtime: &Runtime,
    route: &TenantRoute,
    entry: &FeedEntry,
) -> Result<(String, Option<u64>), String> {
    let module = runtime
        .inner
        .modules
        .project_for_call(&route.project_id)
        .await
        .ok_or_else(|| format!("project {:?} has no active module", route.project_id))?;
    if module.artifact_hash != entry.artifact_hash {
        return Err(format!(
            "artifact differs: the call ran {:?}, this runtime has {:?}",
            entry.artifact_hash, module.artifact_hash
        ));
    }
    let session = TenantSession {
        identity: SessionIdentity {
            project_id: route.project_id.clone(),
            account: entry.session.account.clone(),
        },
        route: route.clone(),
        member: entry.session.member.clone(),
        admission_revision: 0,
        delegation: entry.session.delegation.clone(),
        developer_session: entry.session.developer_session.clone(),
    };
    let access = ExecutionAccess {
        receipt_path: Some(entry.receipt_path.clone()),
        intent_entropy: entry.intent_entropy.clone(),
        // The source admitted the call; internal paths arrive from Actions
        // and the scheduler.
        allow_internal: true,
        provenance: Some(entry.provenance.clone()),
        module: Some(module),
        now_unix_ms: Some(entry.now_unix_ms),
        ..ExecutionAccess::default()
    };
    let execution = runtime
        .execute_tenant_reducer_with_access(
            &session,
            &entry.command_id,
            entry.idempotency_key.as_deref(),
            &entry.path,
            entry.args.clone(),
            access,
        )
        .await
        .map_err(|error: ExecutionError| error.to_string())?;
    Ok((
        result_sha256(&execution.value),
        execution.committed_revision,
    ))
}

async fn read_checksums(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<TenantQuery>,
) -> Response {
    let enabled = runtime.inner.reducer_feed.recording() || runtime.inner.reducer_feed.replaying();
    let route = match admit(&runtime, &headers, enabled, &query.project, &query.tenant).await {
        Ok(route) => route,
        Err(response) => return response,
    };
    match checksums(&runtime, &route).await {
        Ok(value) => Json(value).into_response(),
        Err(cause) => internal(cause),
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Per-table row count and SHA-256 over every row in primary-key order, from
/// one WAL snapshot. Each row is its columns' SQL literals (`quote()`, which
/// keeps the storage class), so `1`, `1.0` and `'1'` differ.
pub(crate) async fn checksums(
    runtime: &Runtime,
    route: &TenantRoute,
) -> Result<Value, DatabaseError> {
    let mut transaction =
        gonvex_store::begin_transaction(&runtime.inner.pools, &route.database_url, true).await?;
    let connection = &mut **transaction.transaction();
    let tables: Vec<String> = sqlx::query_scalar(
        r#"SELECT name FROM sqlite_schema
           WHERE type = 'table' AND sql NOT LIKE 'CREATE VIRTUAL%' ORDER BY name"#,
    )
    .fetch_all(&mut *connection)
    .await?;
    let mut result = serde_json::Map::new();
    for table in tables {
        if EXCLUDED_TABLES.iter().any(|(name, _)| *name == table) {
            continue;
        }
        let columns = sqlx::query("SELECT name, pk FROM pragma_table_info($1) ORDER BY cid")
            .bind(&table)
            .fetch_all(&mut *connection)
            .await?;
        let mut keys: Vec<(i64, String)> = Vec::new();
        let mut values = Vec::new();
        for column in &columns {
            let name: String = column.get("name");
            let pk: i64 = column.get("pk");
            if pk > 0 {
                keys.push((pk, quote_identifier(&name)));
            }
            if !EXCLUDED_COLUMNS
                .iter()
                .any(|(excluded_table, excluded)| *excluded_table == table && *excluded == name)
            {
                values.push(format!("quote({})", quote_identifier(&name)));
            }
        }
        if values.is_empty() {
            continue;
        }
        keys.sort();
        // Without a declared key, the whole row orders the rows.
        let order = if keys.is_empty() {
            "1".to_owned()
        } else {
            keys.into_iter()
                .map(|(_, key)| key)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let sql = format!(
            "SELECT {} AS row FROM {} ORDER BY {order}",
            values.join(" || char(31) || "),
            quote_identifier(&table),
        );
        // Streamed: memory stays flat however large the table is.
        let mut hasher = Sha256::new();
        let mut count = 0_u64;
        let mut rows = sqlx::query_scalar::<_, String>(&sql).fetch(&mut *connection);
        while let Some(row) = rows.try_next().await? {
            hasher.update(row.as_bytes());
            hasher.update(b"\n");
            count += 1;
        }
        drop(rows);
        result.insert(
            table,
            json!({"rows": count, "sha256": format!("{:x}", hasher.finalize())}),
        );
    }
    transaction.rollback().await?;
    Ok(json!({
        "tables": result,
        "excludedTables": EXCLUDED_TABLES.iter().map(|(table, why)| json!({"table": table, "why": why})).collect::<Vec<_>>(),
        "excludedColumns": EXCLUDED_COLUMNS.iter().map(|(table, column)| format!("{table}.{column}")).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    //! The P04 `done_when`: a scripted day of Reducer calls recorded on one
    //! runtime, replayed on a fresh one through the HTTP feed, with identical
    //! per-table checksums. In-process V8, so the real module host runs.
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::{Duration, Instant};

    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use base64::Engine as _;
    use gonvex_module_runtime::{
        InvocationChannel, InvocationDelegation, InvocationDelegationActor,
    };
    use gonvex_store::{ControlPlane, DelegationActor, RuntimeManifestRecord};
    use tower::ServiceExt;

    use super::*;
    use crate::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};

    const ADMIN: &str = "reducer-feed-admin-key";
    const PROJECT: &str = "feedday";
    const TENANT: &str = "tenant";
    const DAY_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

    const TABLES: &str = r#"
CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
  priority INTEGER NOT NULL, assignee TEXT, created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL, score REAL NOT NULL, tags JSON NOT NULL DEFAULT '[]');
CREATE TABLE comments (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, author TEXT NOT NULL,
  body TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE audit (id TEXT PRIMARY KEY, actor TEXT, channel TEXT NOT NULL, root_channel TEXT NOT NULL,
  command TEXT NOT NULL, root_command TEXT NOT NULL, at INTEGER NOT NULL, delegation TEXT,
  artifact TEXT NOT NULL);
"#;

    /// A small task app. Ids come from `Math.random` (seeded by
    /// `intentEntropy`) and `generatedId`; times from `ctx.now`; identity from
    /// `ctx.member` and `ctx.invocation`. Nothing reads the wall clock.
    const MODULE: &str = r#"
const hex = (n) => { let s = ""; while (s.length < n) s += Math.floor(Math.random() * 0x100000000).toString(16).padStart(8, "0"); return s.slice(0, n); };
const audit = async (ctx, what) => {
  const inv = ctx.invocation;
  await ctx.db.insert("audit", { id: `${inv.commandId}:${what}`, actor: ctx.member?.id ?? null, channel: inv.channel,
    root_channel: inv.rootChannel, command: inv.commandId, root_command: inv.rootCommandId, at: ctx.now,
    delegation: inv.delegation ? JSON.stringify(inv.delegation) : null, artifact: inv.artifactHash });
};
export async function createTask(ctx, args) {
  const id = "t_" + hex(16);
  await ctx.db.insert("tasks", { id, title: args.title, status: "open", priority: args.priority, assignee: ctx.member.id,
    created_by: ctx.member.id, created_at: ctx.now, updated_at: ctx.now, score: Math.random(), tags: args.tags ?? [] });
  await audit(ctx, "create");
  return { id, at: ctx.now };
}
export async function updateTask(ctx, args) {
  const [row] = await ctx.db.query("SELECT status FROM tasks WHERE id = $1", [args.id]);
  if (!row) return { missing: args.id };
  await ctx.db.update("tasks", args.id, { status: args.status, updated_at: ctx.now, score: Math.random() });
  await audit(ctx, "update");
  return { id: args.id, from: row.status, to: args.status };
}
export async function comment(ctx, args) {
  const id = await ctx.db.insert("comments", { task_id: args.taskId, author: ctx.member.id, body: args.body, created_at: ctx.now },
    { generatedId: "c_" + hex(20) });
  return { id };
}
export async function deleteTask(ctx, args) {
  const comments = await ctx.db.query("SELECT id FROM comments WHERE task_id = $1 ORDER BY id", [args.id]);
  if (comments.length) await ctx.db.deleteMany("comments", comments.map((row) => row.id));
  await ctx.db.delete("tasks", args.id);
  await audit(ctx, "delete");
  return { deleted: comments.length };
}
export async function bump(ctx, args) {
  const [row] = await ctx.db.query("SELECT value FROM counters WHERE name = $1", [args.name]);
  if (row) await ctx.db.update("counters", args.name, { value: row.value + 1, updated_at: ctx.now });
  else await ctx.db.insert("counters", { name: args.name, value: 1, updated_at: ctx.now });
  return (row?.value ?? 0) + 1;
}
export async function receiptOnly(ctx, args) { return { ok: true, at: ctx.now, member: ctx.member?.id ?? null }; }
export async function noop(ctx, args) { return args; }
export async function fail(ctx, args) {
  await ctx.db.insert("counters", { name: "never-" + args.n, value: 1, updated_at: ctx.now });
  throw new Error("intentional rollback");
}
export async function nightly(ctx) {
  const done = await ctx.db.query("SELECT id FROM tasks WHERE status = 'done' ORDER BY id", []);
  for (const row of done) await ctx.db.update("tasks", row.id, { status: "archived", updated_at: ctx.now });
  await audit(ctx, "nightly");
  return done.length;
}
"#;

    fn config() -> Config {
        Config {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            control_plane_database_url: None,
            default_database_url: None,
            tenant_database_urls: BTreeMap::new(),
            project_database_urls: BTreeMap::new(),
            require_auth: true,
            websocket_compression: false,
            websocket_compression_threshold: 1024,
            control_secret: Some("reducer-feed-control-secret".to_owned()),
            auth_public_url: Some("http://localhost:8080".to_owned()),
            admin_key: Some(ADMIN.to_owned()),
            dev_sync_key: None,
            dashboard_account: None,
            dashboard_password: None,
            dashboard_auth_project_id: None,
            google_client_id: None,
            google_client_secret: None,
            database_max_total_connections: 16,
            database_max_connections: 4,
            database_max_idle_connections: 1,
            database_idle_timeout: Duration::from_secs(60),
            module_host: ModuleHostConfig {
                enabled: true,
                in_process: true,
                max_heap_bytes: 64 << 20,
                binary: None,
                endpoint: None,
                start_timeout: Duration::from_secs(10),
                shutdown_timeout: Duration::from_secs(2),
                max_frame_bytes: 1 << 20,
                max_concurrent_calls: 4,
                isolate_pool_size: 1,
                execution_timeout: Duration::from_secs(10),
                action_execution_timeout: Duration::from_secs(10),
            },
            runtime_version: "test".to_owned(),
            sandbox: SandboxConfig::default(),
            storage: StorageConfig::default(),
            service_principals: Vec::new(),
            public_functions: Default::default(),
            presence: Default::default(),
            connection: Default::default(),
        }
    }

    fn data_dir() -> tempfile::TempDir {
        match std::env::var("GONVEX_TEST_DATA_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
        {
            Some(root) => {
                std::fs::create_dir_all(&root).unwrap();
                tempfile::tempdir_in(root).unwrap()
            }
            None => tempfile::tempdir().unwrap(),
        }
    }

    fn manifest() -> (String, Value) {
        let mut functions = json!({});
        for path in [
            "createTask",
            "updateTask",
            "comment",
            "deleteTask",
            "bump",
            "receiptOnly",
            "noop",
            "fail",
            "nightly",
        ] {
            let internal = path == "nightly";
            functions[path] = json!({"kind":"reducer","public":!internal,"internal":internal,
                "interactive":false,"classification":if internal { "internal" } else { "system" },"handler":path,"file":"day.js",
                "delivery":"oneShot","args":{"kind":"any"},"result":{"kind":"any"}});
        }
        let mut module = json!({"generation":1,"language":"typescript","entrypoint":"day.js",
            "files":{},"visibility":{},"crons":[],"functions":functions,
            "javascript":{"path":"day.js","hash":format!("{:x}", Sha256::digest(MODULE.as_bytes())),
            "code":base64::engine::general_purpose::STANDARD.encode(MODULE)}});
        let hash = crate::modules::canonical_artifact_hash(module.as_object().unwrap()).unwrap();
        module["hash"] = json!(hash);
        (hash, json!({"module": module}))
    }

    struct Instance {
        runtime: Runtime,
        route: TenantRoute,
        _data: tempfile::TempDir,
    }

    /// A fresh runtime with the module deployed and the same base fixture:
    /// schema, provisioning and six members with fixed timestamps.
    async fn instance(record: bool, replay: bool) -> Instance {
        let data = data_dir();
        let control_url = gonvex_store::database_url(&data.path().join("control.db"));
        let url = gonvex_store::database_url(&data.path().join("tenant.db"));
        let runtime = Runtime::new(config());
        runtime.inner.reducer_feed.set(record, replay);
        let control =
            ControlPlane::connect(&control_url, runtime.inner.pools.clone(), BTreeMap::new())
                .await
                .unwrap();
        let control_pool = runtime.inner.pools.pool(&control_url).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES($1,'Feed day','multiTenant','active')")
            .bind(PROJECT).execute(&control_pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,domain,database_url,status,provisioned) VALUES('route',$1,$2,'Tenant',$2,$3,'active',1)")
            .bind(PROJECT).bind(TENANT).bind(&url).execute(&control_pool).await.unwrap();
        let tenant_pool = runtime.inner.pools.pool(&url).await.unwrap();
        gonvex_store::execute_script(&mut tenant_pool.acquire().await.unwrap(), TABLES)
            .await
            .unwrap();
        let route = TenantRoute {
            project_id: PROJECT.into(),
            tenant_id: TENANT.into(),
            database_url: url,
        };
        control
            .clone()
            .provision_tenant_database(route.clone(), Vec::new())
            .await
            .unwrap();
        *runtime.inner.control_plane.write().await = Some(control.clone());
        runtime.inner.module_host.start().await.unwrap();
        let (hash, manifest) = manifest();
        runtime
            .inner
            .modules
            .install(
                &runtime.inner.module_host,
                RuntimeManifestRecord {
                    project_id: PROJECT.into(),
                    module_hash: hash,
                    manifest,
                },
            )
            .await
            .unwrap();
        let mut transaction = control
            .begin_tenant_transaction(&route, false)
            .await
            .unwrap();
        for index in 0..6 {
            sqlx::query("INSERT INTO members(id,account_id,status,display_name,role,permissions,created_at,updated_at) VALUES($1,$2,'active',$3,$4,'{}',1700000000000,1700000000000)")
                .bind(format!("member-{index}")).bind(format!("account-{index}"))
                .bind(format!("Member {index}")).bind(if index == 0 { "admin" } else { "member" })
                .execute(&mut **transaction.transaction()).await.unwrap();
        }
        transaction.commit().await.unwrap();
        Instance {
            runtime,
            route,
            _data: data,
        }
    }

    fn member_session(route: &TenantRoute, index: usize) -> TenantSession {
        let mut session = TenantSession::anonymous(route.clone());
        session.identity.account = Account {
            id: format!("account-{index}"),
            email: format!("member{index}@example.test"),
            email_verified: true,
            name: format!("Member {index}"),
            avatar_url: String::new(),
            provider: "gonvex-native".into(),
        };
        session.member = Member {
            id: format!("member-{index}"),
            account_id: format!("account-{index}"),
            status: "active".into(),
            display_name: format!("Member {index}"),
            avatar_url: String::new(),
            role: if index == 0 { "admin" } else { "member" }.into(),
            permissions: json!({"tasks.write": true}),
            membership_revision: 1,
        };
        session
    }

    /// Deterministic call mix (xorshift), so a failing run reproduces.
    struct Script(u64);

    impl Script {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
        fn entropy(&mut self) -> String {
            (0..4).map(|_| format!("{:016x}", self.next())).collect()
        }
    }

    #[derive(Default, Debug)]
    struct DayStats {
        calls: u64,
        committed: u64,
        failed: u64,
        receipt_replays: u64,
        by_path: BTreeMap<String, u64>,
    }

    /// One scripted day on `runtime`: members on the UI channel, a delegated
    /// API client, Agent-channel calls, the nightly internal job on the
    /// scheduler channel, idempotent calls and their retries, receipt-only
    /// calls, no-ops and rolled-back failures.
    async fn scripted_day(
        runtime: &Runtime,
        route: &TenantRoute,
        calls: u64,
        seed: u64,
    ) -> DayStats {
        let (artifact, _) = manifest();
        let mut script = Script(seed);
        let mut stats = DayStats::default();
        let mut tasks: Vec<String> = Vec::new();
        let mut keyed: Vec<(usize, String, Value)> = Vec::new();
        for step in 0..calls {
            let member = script.below(6) as usize;
            let mut session = member_session(route, member);
            let command = format!("cmd-{seed:x}-{step:06}");
            let entropy = script.entropy();
            let mut access = ExecutionAccess {
                intent_entropy: Some(entropy),
                ..ExecutionAccess::default()
            };
            let mut key: Option<String> = None;
            let roll = script.below(100);
            let (path, args) = if step % 500 == 499 {
                // The nightly job, as the scheduler runs it.
                session = crate::execution::system_tenant_session(PROJECT, route.clone());
                let mut provenance = crate::execution::direct_provenance(
                    &session,
                    InvocationChannel::System,
                    &command,
                    &artifact,
                );
                provenance.channel = InvocationChannel::Scheduler;
                access = ExecutionAccess {
                    allow_internal: true,
                    provenance: Some(provenance),
                    ..ExecutionAccess::default()
                };
                ("nightly", json!({}))
            } else if roll < 28 || tasks.is_empty() {
                if script.below(2) == 0 {
                    key = Some(format!("key-{step}"));
                }
                (
                    "createTask",
                    json!({"title": format!("Task {step}"), "priority": script.below(5),
                        "tags": if roll.is_multiple_of(3) { json!(["a", "b"]) } else { json!([]) }}),
                )
            } else if roll < 50 {
                let id = tasks[script.below(tasks.len() as u64) as usize].clone();
                let status = ["open", "doing", "done"][script.below(3) as usize];
                ("updateTask", json!({"id": id, "status": status}))
            } else if roll < 64 {
                let id = tasks[script.below(tasks.len() as u64) as usize].clone();
                (
                    "comment",
                    json!({"taskId": id, "body": format!("note {step} ✓")}),
                )
            } else if roll < 68 {
                let id = tasks.swap_remove(script.below(tasks.len() as u64) as usize);
                ("deleteTask", json!({"id": id}))
            } else if roll < 76 {
                access.intent_entropy = None;
                let name = ["opened", "closed", "viewed"][script.below(3) as usize];
                ("bump", json!({"name": name}))
            } else if roll < 81 {
                // A service principal driving member 1 over the API channel.
                session = member_session(route, 1);
                session.delegation = Some(SessionDelegation {
                    principal: "billing-service".into(),
                    actor: Some(DelegationActor {
                        kind: "apiKey".into(),
                        name: "nightly import".into(),
                        reference: Some("key_123".into()),
                    }),
                });
                let mut provenance = crate::execution::direct_provenance(
                    &session,
                    InvocationChannel::Ui,
                    &command,
                    &artifact,
                );
                assert_eq!(provenance.channel, InvocationChannel::Api);
                provenance.delegation = Some(InvocationDelegation {
                    principal: "billing-service".into(),
                    actor: Some(InvocationDelegationActor {
                        kind: "apiKey".into(),
                        name: "nightly import".into(),
                        reference: Some("key_123".into()),
                    }),
                });
                access.provenance = Some(provenance);
                key = Some(format!("api-{step}"));
                (
                    "createTask",
                    json!({"title": format!("Imported {step}"), "priority": 1}),
                )
            } else if roll < 85 {
                // An Agent acting for the member inside a UI-rooted chain.
                let mut provenance = crate::execution::direct_provenance(
                    &session,
                    InvocationChannel::Ui,
                    &format!("root-{step}"),
                    &artifact,
                );
                provenance.channel = InvocationChannel::Agent;
                provenance.command_id = command.clone();
                provenance.parent_command_id = Some(format!("root-{step}"));
                provenance.on_behalf_of_member_id = Some(session.member.id.clone());
                provenance.agent_execution_id = Some(format!("agent_{step}"));
                provenance.depth = 1;
                access.provenance = Some(provenance);
                let id = tasks[script.below(tasks.len() as u64) as usize].clone();
                ("updateTask", json!({"id": id, "status": "doing"}))
            } else if roll < 88 {
                key = Some(format!("receipt-{step}"));
                ("receiptOnly", json!({}))
            } else if roll < 91 {
                ("noop", json!({"step": step}))
            } else if roll < 94 {
                ("fail", json!({"n": step}))
            } else if let Some((who, retry_key, retry_args)) = (!keyed.is_empty())
                .then(|| keyed[script.below(keyed.len() as u64) as usize].clone())
            {
                // A client retry: same key and arguments, new transport id.
                session = member_session(route, who);
                key = Some(retry_key);
                ("createTask", retry_args)
            } else {
                ("noop", json!({"step": step}))
            };
            stats.calls += 1;
            *stats.by_path.entry(path.to_owned()).or_default() += 1;
            let before = crate::change_feed::read_clock(&runtime.inner.pools, route)
                .await
                .unwrap()
                .revision;
            let result = runtime
                .execute_tenant_reducer_with_access(
                    &session,
                    &command,
                    key.as_deref(),
                    path,
                    args.clone(),
                    access,
                )
                .await;
            match result {
                Ok(execution) => {
                    if path == "createTask" {
                        let after = crate::change_feed::read_clock(&runtime.inner.pools, route)
                            .await
                            .unwrap()
                            .revision;
                        if after == before {
                            stats.receipt_replays += 1;
                        } else {
                            tasks.push(execution.value["id"].as_str().unwrap().to_owned());
                            if let Some(key) = key.as_ref().filter(|key| key.starts_with("key-")) {
                                keyed.push((member, key.clone(), args));
                            }
                        }
                    }
                    stats.committed += 1;
                }
                Err(error) => {
                    assert_eq!(path, "fail", "{path} failed: {error}");
                    stats.failed += 1;
                }
            }
        }
        stats
    }

    async fn http(runtime: &Runtime, request: Request<Body>) -> (StatusCode, Value) {
        let response = runtime.router().oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 256 << 20).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    fn get(uri: String) -> Request<Body> {
        Request::get(uri)
            .header("authorization", format!("Bearer {ADMIN}"))
            .body(Body::empty())
            .unwrap()
    }

    async fn checksums_of(runtime: &Runtime) -> Value {
        let (status, body) = http(
            runtime,
            get(format!(
                "/dev/reducer-feed/checksums?project={PROJECT}&tenant={TENANT}"
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["tables"].clone()
    }

    /// Streams the source feed page by page into the target's replay route.
    async fn replay_over_http(source: &Runtime, target: &Runtime) -> (u64, u64, Vec<Value>) {
        let (mut after, mut applied, mut pages, mut gaps) = (0_i64, 0_u64, 0_u64, Vec::new());
        loop {
            let (status, page) = http(
                source,
                get(format!(
                    "/dev/reducer-feed?project={PROJECT}&tenant={TENANT}&after={after}&limit=400"
                )),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{page}");
            gaps.extend(page["gaps"].as_array().unwrap().iter().cloned());
            let entries = page["entries"].as_array().unwrap();
            if entries.is_empty() {
                break;
            }
            pages += 1;
            let request = Request::post("/dev/reducer-feed/replay")
                .header("authorization", format!("Bearer {ADMIN}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"project": PROJECT, "tenant": TENANT, "entries": entries}).to_string(),
                ))
                .unwrap();
            let (status, report) = http(target, request).await;
            assert_eq!(status, StatusCode::OK, "{report}");
            assert_eq!(report["revisionMismatches"], json!([]), "{report}");
            applied += report["applied"].as_u64().unwrap();
            after = page["next"].as_i64().unwrap();
            if !page["more"].as_bool().unwrap() {
                break;
            }
        }
        (applied, pages, gaps)
    }

    fn day_calls() -> u64 {
        std::env::var("GONVEX_REDUCER_FEED_DAY_CALLS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_500)
    }

    #[test]
    fn replaying_a_recorded_day_reproduces_every_table() {
        crate::in_process::tests::run(async {
            let source = instance(true, false).await;
            let target = instance(false, true).await;
            // The feed is off by default and each route answers only when on.
            let (status, _) = http(
                &target.runtime,
                get(format!(
                    "/dev/reducer-feed?project={PROJECT}&tenant={TENANT}"
                )),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            let unauthorized = Request::get(format!(
                "/dev/reducer-feed?project={PROJECT}&tenant={TENANT}"
            ))
            .body(Body::empty())
            .unwrap();
            assert_eq!(
                http(&source.runtime, unauthorized).await.0,
                StatusCode::UNAUTHORIZED
            );
            assert_eq!(
                checksums_of(&source.runtime).await,
                checksums_of(&target.runtime).await,
                "both instances start from the same base fixture"
            );

            let calls = day_calls();
            let started = Instant::now();
            let stats = scripted_day(&source.runtime, &source.route, calls, DAY_SEED).await;
            let recorded = started.elapsed();
            assert!(stats.failed > 0 && stats.receipt_replays > 0, "{stats:?}");

            let pool = source
                .runtime
                .inner
                .pools
                .reader(&source.route.database_url)
                .await
                .unwrap();
            let (rows, revisionless): (i64, i64) = sqlx::query_as(
                "SELECT count(*), count(*) FILTER (WHERE revision IS NULL) FROM _gonvex_reducer_feed",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            // Failures roll back their feed row; retries replay a receipt and
            // commit nothing; no-ops never open a transaction.
            let noops = stats.by_path.get("noop").copied().unwrap_or(0) as i64;
            assert_eq!(
                rows,
                (stats.committed - stats.receipt_replays) as i64 - noops,
                "{stats:?}"
            );
            assert!(revisionless > 0, "receipt-only calls commit no revision");
            // Feed rows never enter the change feed.
            let leaked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM _gonvex_sync_changes WHERE table_name = '_gonvex_reducer_feed'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(leaked, 0);
            // Revisions are strictly increasing in feed order and every one
            // has its provenance row.
            let order: i64 = sqlx::query_scalar(
                r#"SELECT count(*) FROM (SELECT revision, lag(revision) OVER (ORDER BY seq) AS previous
                   FROM _gonvex_reducer_feed WHERE revision IS NOT NULL) WHERE previous >= revision"#,
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(order, 0);
            let orphaned: i64 = sqlx::query_scalar(
                r#"SELECT count(*) FROM _gonvex_reducer_feed feed
                   LEFT JOIN _gonvex_sync_transactions tx ON tx.revision = feed.revision
                   WHERE feed.revision IS NOT NULL
                     AND (tx.revision IS NULL OR tx.origin_command_id <> feed.command_id)"#,
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(orphaned, 0);

            let started = Instant::now();
            let (applied, pages, gaps) = replay_over_http(&source.runtime, &target.runtime).await;
            let replayed = started.elapsed();
            assert_eq!(applied as i64, rows);
            assert_eq!(gaps, Vec::<Value>::new(), "the day has only Reducer writes");

            let source_sums = checksums_of(&source.runtime).await;
            let target_sums = checksums_of(&target.runtime).await;
            for table in ["tasks", "comments", "counters", "audit", "members"] {
                assert!(
                    source_sums[table]["rows"].as_u64().unwrap() > 0,
                    "{table} is empty"
                );
            }
            let differing: Vec<&String> = source_sums
                .as_object()
                .unwrap()
                .keys()
                .filter(|table| source_sums[*table] != target_sums[*table])
                .collect();
            assert!(
                differing.is_empty(),
                "tables differ: {differing:?}\n{source_sums:#}\n{target_sums:#}"
            );
            assert_eq!(source_sums, target_sums);
            for (table, sums) in source_sums.as_object().unwrap() {
                eprintln!("  {table}: {} rows", sums["rows"]);
            }
            eprintln!(
                "reducer feed day: {stats:?}\n  feed rows {rows} ({revisionless} without a revision), {pages} pages, \
                 recorded in {recorded:?}, replayed in {replayed:?}, {} tables identical",
                source_sums.as_object().unwrap().len()
            );

            // The replay tool in cloudflare/tools/reducer-feed against real
            // listeners (opt in: `GONVEX_REDUCER_FEED_NODE=node`).
            if let Ok(node) = std::env::var("GONVEX_REDUCER_FEED_NODE") {
                let fresh = instance(false, true).await;
                let serve = |runtime: Runtime| async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let address = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        axum::serve(listener, runtime.router()).await.unwrap();
                    });
                    format!("http://{address}")
                };
                let source_url = serve(source.runtime.clone()).await;
                let target_url = serve(fresh.runtime.clone()).await;
                let tool = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../cloudflare/tools/reducer-feed/replay.mjs");
                let output = tokio::process::Command::new(node)
                    .arg(tool)
                    .args(["--source", &source_url, "--target", &target_url])
                    .args(["--project", PROJECT, "--tenant", TENANT, "--batch", "250"])
                    .env("GONVEX_SOURCE_ADMIN_KEY", ADMIN)
                    .env("GONVEX_TARGET_ADMIN_KEY", ADMIN)
                    .output()
                    .await
                    .unwrap();
                let stdout = String::from_utf8_lossy(&output.stdout);
                eprintln!("{stdout}{}", String::from_utf8_lossy(&output.stderr));
                assert!(output.status.success(), "replay tool failed");
                assert!(stdout.contains("\"identical\": true"), "{stdout}");
                assert_eq!(checksums_of(&fresh.runtime).await, source_sums);
            }
        });
    }

    #[test]
    fn a_replay_stops_at_the_first_divergent_result() {
        crate::in_process::tests::run(async {
            let source = instance(true, false).await;
            let target = instance(false, true).await;
            scripted_day(&source.runtime, &source.route, 40, DAY_SEED).await;
            let page = read_page(&source.runtime, &source.route, 0, 100)
                .await
                .unwrap();
            let mut entries: Vec<FeedEntry> =
                serde_json::from_value(page["entries"].clone()).unwrap();
            assert!(entries.len() > 10);
            // A different clock is a different outcome for `createTask`.
            let index = entries
                .iter()
                .position(|entry| entry.path == "createTask")
                .unwrap();
            entries[index].now_unix_ms += 1;
            let report = replay(&target.runtime, &target.route, &entries).await;
            assert_eq!(report.applied, index);
            assert_eq!(report.failure.as_ref().unwrap()["reason"], "result differs");
            // An artifact the target does not run is refused before executing.
            let mut stale = entries[index + 1].clone();
            stale.artifact_hash = "other".into();
            let report = replay(&target.runtime, &target.route, &[stale]).await;
            assert!(report.failure.unwrap()["reason"]
                .as_str()
                .unwrap()
                .contains("artifact differs"));
        });
    }

    #[test]
    fn non_reducer_writes_appear_as_gaps() {
        crate::in_process::tests::run(async {
            let source = instance(true, false).await;
            scripted_day(&source.runtime, &source.route, 5, DAY_SEED).await;
            // An operator edit: a tenant write that no Reducer made.
            let control = source
                .runtime
                .inner
                .control_plane
                .read()
                .await
                .clone()
                .unwrap();
            let mut transaction = control
                .begin_tenant_transaction(&source.route, false)
                .await
                .unwrap();
            transaction.set_command_id("operator-edit").await.unwrap();
            sqlx::query("INSERT INTO counters(name,value,updated_at) VALUES('manual',1,1)")
                .execute(&mut **transaction.transaction())
                .await
                .unwrap();
            let foreign = transaction.commit_with_revision().await.unwrap().unwrap();
            scripted_day(&source.runtime, &source.route, 5, DAY_SEED + 1).await;
            let page = read_page(&source.runtime, &source.route, 0, 100)
                .await
                .unwrap();
            assert_eq!(
                page["gaps"],
                json!([{"revision": foreign, "commandId": "operator-edit", "channel": "system"}])
            );
            assert_eq!(
                page["head"]["revision"].as_u64(),
                Some(
                    crate::change_feed::read_clock(&source.runtime.inner.pools, &source.route)
                        .await
                        .unwrap()
                        .revision
                )
            );
        });
    }

    /// `GONVEX_REDUCER_FEED_BENCH=<calls>` measures the write overhead of
    /// recording: the same Reducer mix alternates between feed off and on in
    /// blocks on one runtime, so both modes see the same database size.
    #[test]
    fn write_overhead() {
        let Some(calls) = std::env::var("GONVEX_REDUCER_FEED_BENCH")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        else {
            return;
        };
        crate::in_process::tests::run(async move {
            let source = instance(false, false).await;
            let runtime = &source.runtime;
            let session = member_session(&source.route, 1);
            let mut times: [Vec<Duration>; 2] = [Vec::new(), Vec::new()];
            let mut script = Script(42);
            let block = 100;
            // Warm-up: isolates, statement caches, the feed table itself.
            runtime.inner.reducer_feed.set(true, false);
            for step in 0..200 {
                run_bench_call(runtime, &session, &mut script, step).await;
            }
            let mut step = 200;
            while (times[0].len() + times[1].len()) < (calls as usize) * 2 {
                for mode in [0_usize, 1] {
                    runtime.inner.reducer_feed.set(mode == 1, false);
                    for _ in 0..block {
                        let started = Instant::now();
                        run_bench_call(runtime, &session, &mut script, step).await;
                        times[mode].push(started.elapsed());
                        step += 1;
                    }
                }
            }
            let stats = |samples: &mut Vec<Duration>| {
                samples.sort();
                let mean = samples.iter().sum::<Duration>() / samples.len() as u32;
                (
                    mean,
                    samples[samples.len() / 2],
                    samples[samples.len() * 99 / 100],
                )
            };
            let (off_mean, off_p50, off_p99) = stats(&mut times[0]);
            let (on_mean, on_p50, on_p99) = stats(&mut times[1]);
            let pool = runtime
                .inner
                .pools
                .reader(&source.route.database_url)
                .await
                .unwrap();
            let (rows, bytes): (i64, i64) = sqlx::query_as(
                r#"SELECT count(*), sum(length(command_id) + length(path) + length(args) + length(session)
                     + length(provenance) + 8 + coalesce(length(intent_entropy), 0)
                     + coalesce(length(idempotency_key), 0) + length(receipt_path)
                     + length(artifact_hash) + length(result_sha256) + 16)
                   FROM _gonvex_reducer_feed"#,
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            eprintln!(
                "reducer feed write overhead over {} calls per mode (block {block}):\n  \
                 off: mean {off_mean:?} p50 {off_p50:?} p99 {off_p99:?}\n  \
                 on:  mean {on_mean:?} p50 {on_p50:?} p99 {on_p99:?}\n  \
                 delta: mean {:?} ({:.1}%), p50 {:?}\n  feed: {rows} rows, {} payload bytes per row",
                times[0].len(),
                on_mean.saturating_sub(off_mean),
                (on_mean.as_secs_f64() / off_mean.as_secs_f64() - 1.0) * 100.0,
                on_p50.saturating_sub(off_p50),
                bytes / rows.max(1),
            );

            // The commit fsync dominates a call, so also time what recording
            // adds while the writer is held, without fsyncs: the feed INSERT
            // and the revision trigger fired by finalize's provenance INSERT,
            // against that provenance INSERT on a database that never had the
            // feed (trigger dropped). Each round is rolled back.
            let control = runtime.inner.control_plane.read().await.clone().unwrap();
            let provenance = crate::execution::direct_provenance(
                &session,
                InvocationChannel::Ui,
                "micro",
                "artifact",
            );
            let args = json!({"title": "Bench micro", "priority": 2});
            let entropy = "0".repeat(64);
            let iterations = 5_000_usize;
            let started = Instant::now();
            let captures: Vec<Capture> = (0..iterations)
                .map(|index| {
                    Capture::new(
                        &session,
                        &format!("micro-{index}"),
                        "createTask",
                        &args,
                        &provenance,
                        1,
                        Some(&entropy),
                        None,
                        "createTask",
                        "artifact",
                    )
                })
                .collect();
            let capture = started.elapsed() / iterations as u32;
            let mut elapsed = [Duration::ZERO; 2];
            for mode in [0_usize, 1, 0, 1, 0, 1] {
                let mut transaction = control
                    .begin_tenant_transaction(&source.route, false)
                    .await
                    .unwrap();
                if mode == 0 {
                    gonvex_store::execute_script(
                        transaction.transaction(),
                        "DROP TRIGGER _gonvex_reducer_feed_revision",
                    )
                    .await
                    .unwrap();
                }
                let started = Instant::now();
                for (index, capture) in captures.iter().enumerate() {
                    if mode == 1 {
                        append(&mut transaction, capture, &json!({"id": index}))
                            .await
                            .unwrap();
                    }
                    let command = format!("micro-{index}");
                    let direct = transaction.direct().await.unwrap();
                    direct
                        .run(|conn| {
                            use gonvex_store::direct::Bind::{Integer, Text};
                            conn.execute(
                                r#"INSERT INTO _gonvex_sync_transactions
                                   (revision, transaction_id, root_command_id, origin_command_id, invocation_channel)
                                   VALUES ($1, $1, $2, $2, 'ui')"#,
                                &[Integer(1_000_000_000 + index as i64), Text(&command)],
                            )
                        })
                        .unwrap()
                        .unwrap();
                }
                elapsed[mode] += started.elapsed();
                transaction.rollback().await.unwrap();
            }
            let per_call = |total: Duration| total / (3 * iterations) as u32;
            eprintln!(
                "  writer-held cost per Reducer (no fsync, {iterations} x 3 rounds): \
                 provenance INSERT alone {:?}, with feed INSERT + trigger {:?}, added {:?}; \
                 capture serialization (outside the writer) {capture:?}",
                per_call(elapsed[0]),
                per_call(elapsed[1]),
                per_call(elapsed[1]).saturating_sub(per_call(elapsed[0])),
            );
        });
    }

    async fn run_bench_call(
        runtime: &Runtime,
        session: &TenantSession,
        script: &mut Script,
        step: u64,
    ) {
        let (path, args) = if step.is_multiple_of(2) {
            (
                "createTask",
                json!({"title": format!("Bench {step}"), "priority": 2}),
            )
        } else {
            ("bump", json!({"name": "bench"}))
        };
        runtime
            .execute_tenant_reducer_with_access(
                session,
                &format!("bench-{step}"),
                None,
                path,
                args,
                ExecutionAccess {
                    intent_entropy: Some(script.entropy()),
                    ..ExecutionAccess::default()
                },
            )
            .await
            .unwrap();
    }
}
