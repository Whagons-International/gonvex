//! Trusted host operations for TypeScript module invocations.
//!
//! The V8 isolate can ask for database work, but it never receives a pool,
//! transaction, URL, or credential. Query calls share one read-only snapshot;
//! Reducer calls share one host-owned transaction that the caller commits only
//! after the JavaScript handler returns successfully.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gonvex_module_host::protocol::{codes, HostCallFrame};
use gonvex_module_runtime::{HostCall, HostError, HostResponse, InlineHost};
use gonvex_store::direct::{self, Bind, Conn as DirectConn, DirectSession};
use gonvex_store::rows::JsonKind;
use gonvex_store::TenantTransaction;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error_class::TransientFault;
use crate::module_host::HostCallHandler;
use crate::sql_admission::{admit_statement, AdmittedStatement};
use crate::sql_parameters::Parameter;

const DEFAULT_KEY: &str = "id";
pub(crate) const SCHEDULE_OUTBOX_PATH: &str = "_gonvex.scheduler.enqueue";
const ABORTED: &str =
    "current transaction is aborted, commands ignored until end of transaction block";
/// Returned to module code when a lazily opened Reducer transaction finds its
/// idempotency key already claimed; the caller replays the stored result.
pub(crate) const RECEIPT_REPLAY: &str =
    "idempotency key already committed; replaying the stored result";

/// Provenance recorded on a Reducer transaction when it opens.
pub(crate) struct WriteAttribution {
    pub root_command_id: String,
    pub root_channel: &'static str,
    pub channel: &'static str,
    pub actor_account_id: Option<String>,
    pub actor_member_id: Option<String>,
    pub on_behalf_of_member_id: Option<String>,
    pub agent_execution_id: Option<String>,
}

/// A Reducer write transaction opened on first use. Each tenant has one
/// SQLite writer, so a Reducer must not hold it while it queues for a V8
/// isolate; it opens the transaction at its first database host call (or
/// when its receipt is stored), when its JavaScript is already running.
pub(crate) struct PendingWrite {
    pub control: gonvex_store::ControlPlane,
    pub route: gonvex_store::TenantRoute,
    pub command_id: String,
    pub attribution: WriteAttribution,
    /// `(subject, idempotency key, receipt path)` claimed when it opens.
    pub claim: Option<(String, String, String)>,
}

// Same seed and UUID layout as module-sdk reducerRowId.
fn intent_deferred_id(
    tenant: &str,
    account: &str,
    command: &str,
    kind: &str,
    ordinal: u64,
) -> String {
    let seed = serde_json::to_vec(&serde_json::json!([
        "gonvex.reducer.ids.v1",
        tenant,
        account,
        command,
        format!("deferred:{kind}"),
        ordinal
    ]))
    .expect("intent seed serializes");
    let digest = Sha256::digest(seed);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 15) | 128;
    bytes[8] = (bytes[8] & 63) | 128;
    Uuid::from_bytes(bytes).to_string()
}

#[derive(Clone, Debug)]
struct TableKey {
    column: String,
    /// Declared SQLite type of the key column.
    data_type: Option<String>,
    database_generated: bool,
}

impl TableKey {
    /// Gonvex allocates omitted keys only for text-like key columns.
    fn text_key(&self) -> bool {
        self.data_type.as_deref().is_some_and(|declared| {
            JsonKind::from_declared(declared) == JsonKind::Text && !declared.trim().is_empty()
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseCapability {
    Query,
    Reducer,
}

pub struct DatabaseHostCalls {
    storage_context: Option<(crate::Runtime, gonvex_store::TenantSession)>,
    transaction: Option<TenantTransaction>,
    pending: Option<PendingWrite>,
    claim_conflict: bool,
    schedulable_functions: BTreeSet<String>,
    actor_account_id: String,
    actor_email: String,
    provenance: Value,
    intent_tenant: String,
    intent_command: String,
    deferred_ordinals: BTreeMap<String, u64>,
    fault: Arc<TransientFault>,
    /// Module SQL state, shared with the isolate thread (`SqlInline`).
    sql: Arc<Mutex<SqlCalls>>,
}

/// The part of a database handler that runs module SQL. It lives behind a
/// mutex shared by the async handler and [`SqlInline`], which answers the
/// same calls on the isolate's own thread once the invocation's transaction
/// has lent its connection (`DirectSession`). Every statement runs directly
/// on that connection: no sqlx worker hand-off, cached prepared statements,
/// and results written straight to the JSON the module receives.
pub(crate) struct SqlCalls {
    capability: DatabaseCapability,
    schema: Value,
    tables: BTreeMap<String, Arc<TableCatalog>>,
    fault: Arc<TransientFault>,
    /// Set by a failed SQL statement. SQLite rolls back only the failing
    /// statement; like PostgreSQL, the invocation treats the transaction as
    /// aborted until `ROLLBACK TO` a savepoint, and never commits it.
    aborted: Option<String>,
    session: Option<DirectSession>,
    /// Calls answered on the isolate thread (diagnostics and tests).
    inline_calls: u64,
}

const CLOSED: &str = "the invocation transaction is closed";

impl DatabaseHostCalls {
    fn with_parts(
        transaction: Option<TenantTransaction>,
        pending: Option<PendingWrite>,
        capability: DatabaseCapability,
    ) -> Self {
        let fault: Arc<TransientFault> = Arc::default();
        Self {
            storage_context: None,
            transaction,
            pending,
            claim_conflict: false,
            schedulable_functions: BTreeSet::new(),
            actor_account_id: String::new(),
            actor_email: String::new(),
            provenance: Value::Null,
            intent_tenant: String::new(),
            intent_command: String::new(),
            deferred_ordinals: BTreeMap::new(),
            sql: Arc::new(Mutex::new(SqlCalls {
                capability,
                schema: Value::Null,
                tables: BTreeMap::new(),
                fault: fault.clone(),
                aborted: None,
                session: None,
                inline_calls: 0,
            })),
            fault,
        }
    }

    pub fn new(transaction: TenantTransaction, capability: DatabaseCapability) -> Self {
        Self::with_parts(Some(transaction), None, capability)
    }

    /// A Reducer handler whose write transaction opens on first use.
    pub(crate) fn lazy_reducer(pending: PendingWrite) -> Self {
        Self::with_parts(None, Some(pending), DatabaseCapability::Reducer)
    }

    fn sql(&self) -> MutexGuard<'_, SqlCalls> {
        self.sql.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn capability(&self) -> DatabaseCapability {
        self.sql().capability
    }

    /// Module SQL calls answered on the isolate thread so far.
    pub(crate) fn inline_calls(&self) -> u64 {
        self.sql().inline_calls
    }

    /// Whether opening the transaction found the idempotency key claimed.
    pub(crate) fn claim_conflict(&self) -> bool {
        self.claim_conflict
    }

    /// The invocation transaction, opening a pending Reducer write first.
    pub(crate) async fn ensure_transaction(&mut self) -> Result<&mut TenantTransaction, String> {
        if self.claim_conflict {
            return Err(RECEIPT_REPLAY.to_owned());
        }
        if self.transaction.is_none() {
            if let Some(pending) = self.pending.take() {
                let fault = self.fault.clone();
                let mut transaction = pending
                    .control
                    .begin_tenant_transaction(&pending.route, false)
                    .await
                    .map_err(|error| fault.database(error))?;
                transaction
                    .set_command_id(&pending.command_id)
                    .await
                    .map_err(|error| fault.database(error))?;
                let a = &pending.attribution;
                transaction
                    .set_invocation_provenance(gonvex_store::TransactionAttribution {
                        root_command_id: &a.root_command_id,
                        root_channel: a.root_channel,
                        channel: a.channel,
                        actor_account_id: a.actor_account_id.as_deref(),
                        actor_member_id: a.actor_member_id.as_deref(),
                        on_behalf_of_member_id: a.on_behalf_of_member_id.as_deref(),
                        agent_execution_id: a.agent_execution_id.as_deref(),
                    })
                    .await
                    .map_err(|error| fault.database(error))?;
                if let Some((subject, key, path)) = &pending.claim {
                    let claimed = transaction
                        .claim_reducer(subject, key, path)
                        .await
                        .map_err(|error| fault.database(error))?;
                    if !claimed {
                        transaction
                            .rollback()
                            .await
                            .map_err(|error| fault.database(error))?;
                        self.claim_conflict = true;
                        return Err(RECEIPT_REPLAY.to_owned());
                    }
                }
                self.transaction = Some(transaction);
            }
        }
        self.transaction()
    }

    /// Opens the transaction if needed and lends its connection to module
    /// SQL (here and on the isolate thread).
    async fn ensure_session(&mut self) -> Result<(), String> {
        let active = self.sql().session.as_ref().is_some_and(DirectSession::active);
        if active {
            return Ok(());
        }
        let fault = self.fault.clone();
        let session = self
            .ensure_transaction()
            .await?
            .direct()
            .await
            .map_err(|error| fault.database(error))?;
        self.sql().session = Some(session);
        Ok(())
    }

    pub fn with_storage(
        mut self,
        runtime: crate::Runtime,
        session: gonvex_store::TenantSession,
    ) -> Self {
        self.storage_context = Some((runtime, session));
        self
    }

    /// Shared record of transient database failures seen by this invocation.
    pub fn transient_fault(&self) -> Arc<TransientFault> {
        self.fault.clone()
    }

    pub fn with_provenance(
        mut self,
        provenance: &gonvex_module_runtime::InvocationProvenance,
    ) -> Self {
        self.provenance = serde_json::to_value(provenance).unwrap_or(Value::Null);
        self
    }

    pub fn with_actor(mut self, account_id: &str, email: &str) -> Self {
        self.actor_account_id = account_id.to_owned();
        self.actor_email = email.to_owned();
        self
    }

    pub fn with_intent(mut self, tenant: &str, command: &str) -> Self {
        self.intent_tenant = tenant.to_owned();
        self.intent_command = command.to_owned();
        self
    }

    fn deferred_id(&mut self, kind: &str) -> String {
        if self.intent_command.is_empty() {
            return Uuid::new_v4().to_string();
        }
        let ordinal = self.deferred_ordinals.entry(kind.to_owned()).or_default();
        let id = intent_deferred_id(
            &self.intent_tenant,
            &self.actor_account_id,
            &self.intent_command,
            kind,
            *ordinal,
        );
        *ordinal += 1;
        id
    }

    pub fn with_schema(self, schema: &Value) -> Self {
        self.sql().schema = schema.clone();
        self
    }

    pub fn with_schedulable_functions(
        mut self,
        functions: impl IntoIterator<Item = String>,
    ) -> Self {
        self.schedulable_functions.extend(functions);
        self
    }

    pub async fn finish(self, success: bool) -> Result<(), String> {
        self.finish_with_revision(success).await.map(|_| ())
    }

    /// Commits (Reducer success) or rolls back, returning the revision the
    /// commit published, if it changed tracked rows.
    pub async fn finish_with_revision(mut self, success: bool) -> Result<Option<u64>, String> {
        if self.claim_conflict {
            return Err(RECEIPT_REPLAY.to_owned());
        }
        if self.transaction.is_none() && self.pending.take().is_some() {
            // The Reducer never touched the database: nothing to commit.
            return Ok(None);
        }
        let (capability, aborted) = {
            let mut sql = self.sql();
            sql.session = None;
            (sql.capability, sql.aborted.clone())
        };
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| "the invocation transaction is already closed".to_owned())?;
        let fault = self.fault.clone();
        if capability == DatabaseCapability::Reducer && success {
            if let Some(reason) = aborted {
                transaction.mark_failed(reason);
                transaction
                    .rollback()
                    .await
                    .map_err(|error| fault.database(error))?;
                return Err(ABORTED.to_owned());
            }
            transaction
                .commit_with_revision()
                .await
                .map_err(|error| fault.database(error))
        } else {
            transaction
                .rollback()
                .await
                .map(|_| None)
                .map_err(|error| fault.database(error))
        }
    }

    fn transaction(&mut self) -> Result<&mut TenantTransaction, String> {
        self.transaction
            .as_mut()
            .ok_or_else(|| CLOSED.to_owned())
    }

    pub fn transaction_mut(&mut self) -> &mut TenantTransaction {
        self.transaction
            .as_mut()
            .expect("invocation transaction stays open until finish")
    }

    /// Refuses further SQL after a failed statement (see `SqlCalls::aborted`).
    fn require_open(&self) -> Result<(), String> {
        self.sql().require_open()
    }

    fn require_write(&self) -> Result<(), String> {
        self.sql().require_write()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn query(&mut self, statement: &str, parameters: Value) -> Result<Value, String> {
        let bytes = self.db_bytes(HostCallFrame::DbQuery { statement: statement.to_owned(), parameters }).await?;
        decode_response(&bytes)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn insert(
        &mut self,
        table: &str,
        row: Value,
        generated_id: Option<String>,
    ) -> Result<Value, String> {
        let bytes = self
            .db_bytes(HostCallFrame::DbInsert { table: table.to_owned(), row, generated_id })
            .await?;
        decode_response(&bytes)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn update(&mut self, table: &str, key: &str, id: Value, patch: Value) -> Result<Value, String> {
        let bytes = self
            .db_bytes(HostCallFrame::DbUpdate { table: table.to_owned(), key: key.to_owned(), id, patch })
            .await?;
        decode_response(&bytes)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn delete(&mut self, table: &str, key: &str, id: Value) -> Result<Value, String> {
        let bytes = self
            .db_bytes(HostCallFrame::DbDelete { table: table.to_owned(), key: key.to_owned(), id })
            .await?;
        decode_response(&bytes)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn delete_many(&mut self, table: &str, key: &str, ids: Value) -> Result<Value, String> {
        let bytes = self
            .db_bytes(HostCallFrame::DbDeleteMany { table: table.to_owned(), key: key.to_owned(), ids })
            .await?;
        decode_response(&bytes)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn resolve_table_key(&mut self, table: &str, requested: &str) -> Result<String, String> {
        if let Some(key) = self.sql().declared_table_key(table, requested)? {
            return Ok(key);
        }
        self.ensure_session().await?;
        self.sql().resolve_table_key(table, requested)
    }

    /// A module SQL call on the async path: opens the transaction and its
    /// direct session if needed, then runs exactly what `SqlInline` runs.
    async fn db_bytes(&mut self, call: HostCallFrame) -> Result<Vec<u8>, String> {
        if matches!(
            call,
            HostCallFrame::DbInsert { .. }
                | HostCallFrame::DbUpdate { .. }
                | HostCallFrame::DbDelete { .. }
                | HostCallFrame::DbDeleteMany { .. }
        ) {
            self.require_write()?;
        }
        self.ensure_session().await?;
        self.sql().call(call)
    }
}

fn decode_response(bytes: &[u8]) -> Result<Value, String> {
    serde_json::from_slice(bytes).map_err(|error| format!("host response is not JSON: {error}"))
}

fn is_db_call(call: &HostCallFrame) -> bool {
    matches!(
        call,
        HostCallFrame::DbQuery { .. }
            | HostCallFrame::DbInsert { .. }
            | HostCallFrame::DbUpdate { .. }
            | HostCallFrame::DbDelete { .. }
            | HostCallFrame::DbDeleteMany { .. }
    )
}

#[async_trait]
impl HostCallHandler for DatabaseHostCalls {
    async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
        if is_db_call(&call) {
            let bytes = self.db_bytes(call).await?;
            return decode_response(&bytes);
        }
        self.handle_other(call).await
    }

    async fn handle_bytes(&mut self, call: HostCallFrame) -> Result<Vec<u8>, String> {
        if is_db_call(&call) {
            return self.db_bytes(call).await;
        }
        let value = self.handle_other(call).await?;
        serde_json::to_vec(&value).map_err(|error| format!("host response could not be re-encoded: {error}"))
    }

    fn inline_host(&self) -> Option<Arc<dyn InlineHost>> {
        Some(Arc::new(SqlInline {
            sql: self.sql.clone(),
        }))
    }
}

/// Answers module SQL on the isolate thread, on the connection the
/// invocation's transaction lent to it. Anything else, and every call before
/// the transaction is open, goes back through the async handler.
struct SqlInline {
    sql: Arc<Mutex<SqlCalls>>,
}

impl InlineHost for SqlInline {
    fn call_inline(&self, call: HostCall) -> Result<Result<HostResponse, HostError>, HostCall> {
        if !matches!(
            call,
            HostCall::DbQuery { .. }
                | HostCall::DbInsert { .. }
                | HostCall::DbUpdate { .. }
                | HostCall::DbDelete { .. }
                | HostCall::DbDeleteMany { .. }
        ) {
            return Err(call);
        }
        // Never wait here: the async side holds this only while it runs a
        // call itself, and then this call goes behind it.
        let Ok(mut sql) = self.sql.try_lock() else {
            return Err(call);
        };
        if !sql.session.as_ref().is_some_and(DirectSession::active) {
            return Err(call);
        }
        let capability = call.capability();
        let frame = match HostCallFrame::from_host_call(call) {
            Ok(frame) => frame,
            Err(message) => return Ok(Err(HostError::Failed(message))),
        };
        sql.inline_calls += 1;
        let writes = !matches!(frame, HostCallFrame::DbQuery { .. });
        let result = if writes { sql.require_write() } else { Ok(()) }.and_then(|()| sql.call(frame));
        Ok(match result {
            Ok(value) => Ok(HostResponse { value }),
            // Same text as the async transport (`in_process::DirectHost`).
            Err(message) => Err(HostError::Failed(format!(
                "{} host call failed [{}]: {}",
                capability,
                codes::HOST_CALL_FAILED,
                message
            ))),
        })
    }
}

impl DatabaseHostCalls {
    async fn handle_other(&mut self, call: HostCallFrame) -> Result<Value, String> {
        match call {
            HostCallFrame::Storage { operation, payload } if operation == "getMetadata" => {
                let (runtime, session) = self
                    .storage_context
                    .clone()
                    .ok_or_else(|| "storage metadata is unavailable".to_owned())?;
                let file_id = payload
                    .get("fileId")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "fileId is required".to_owned())?;
                runtime
                    .inner
                    .storage
                    .reducer_metadata(&session, self.ensure_transaction().await?, file_id)
                    .await
            }
            HostCallFrame::ActionEnqueue { function, args } => {
                self.require_write()?;
                self.require_open()?;
                let function = function.trim();
                if function.is_empty() {
                    return Err("actions.enqueue requires an Action path".to_owned());
                }
                if function == SCHEDULE_OUTBOX_PATH {
                    return Err(
                        "actions.enqueue cannot invoke a Gonvex system operation".to_owned()
                    );
                }
                let account_id = self.actor_account_id.clone();
                let email = self.actor_email.clone();
                let provenance = self.provenance.clone();
                let allocated_id = self.deferred_id("action");
                let fault = self.fault.clone();
                let id = self
                    .transaction()?
                    .enqueue_action_with_id(
                        &allocated_id,
                        function,
                        &args,
                        &account_id,
                        &email,
                        &provenance,
                    )
                    .await
                    .map_err(|error| fault.database(error))?;
                Ok(Value::String(id))
            }
            HostCallFrame::ScheduleAfter {
                delay_ms,
                function,
                args,
            } => {
                self.require_write()?;
                let delay = i64::try_from(delay_ms)
                    .map_err(|_| "scheduler delayMs is outside the supported range".to_owned())?;
                let run_at = Utc::now()
                    .checked_add_signed(chrono::Duration::milliseconds(delay))
                    .ok_or_else(|| "scheduler delayMs is outside the supported range".to_owned())?;
                self.enqueue_scheduled(&function, args, run_at, false).await
            }
            HostCallFrame::ScheduleAt {
                at_unix_ms,
                function,
                args,
                coalesce,
            } => {
                self.require_write()?;
                let run_at = i64::try_from(at_unix_ms)
                    .ok()
                    .and_then(DateTime::from_timestamp_millis)
                    .ok_or_else(|| {
                        "scheduler atUnixMs is outside the supported range".to_owned()
                    })?;
                self.enqueue_scheduled(&function, args, run_at, coalesce)
                    .await
            }
            HostCallFrame::PermissionsHas { permission, scope } => {
                self.permissions_has(&permission, &scope).await
            }
            _ => Err(match self.capability() {
                DatabaseCapability::Query => {
                    "a Query may only use its read-only database capability".to_owned()
                }
                DatabaseCapability::Reducer => {
                    "this Reducer host capability is not implemented in the Rust runtime yet"
                        .to_owned()
                }
            }),
        }
    }

    /// `ctx.permissions.has`: a bit test on the tenant's committed
    /// permission bits. When this invocation already holds its transaction,
    /// first wait until the bits cover every revision that transaction sees,
    /// so a check never answers from a state older than the Reducer's reads.
    async fn permissions_has(&mut self, permission: &str, scope: &str) -> Result<Value, String> {
        let (runtime, session) = self.storage_context.clone().ok_or_else(|| {
            "ctx.permissions is available to Reducers with a tenant session".to_owned()
        })?;
        let index = runtime.permission_index(&session).await?;
        if let Some(transaction) = self.transaction.as_mut() {
            let revision: i64 =
                sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton = 1")
                    .fetch_one(&mut **transaction.transaction())
                    .await
                    .map_err(|error| error.to_string())?;
            if !crate::permission_bits::PermissionIndex::wait_for(&index, revision.max(0) as u64)
                .await
            {
                return Err("permission bits are rebuilding; retry".to_owned());
            }
        }
        let held = index
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .has(&session.member.id, permission, scope);
        Ok(Value::Bool(held))
    }

    async fn enqueue_scheduled(
        &mut self,
        function: &str,
        args: Value,
        run_at: DateTime<Utc>,
        coalesce: bool,
    ) -> Result<Value, String> {
        self.require_open()?;
        let function = function.trim();
        if !self.schedulable_functions.contains(function) {
            return Err(format!(
                "scheduled function {function:?} is not a registered Reducer or Action"
            ));
        }
        let job_id = format!("job_{}", self.deferred_id("schedule"));
        let payload = serde_json::json!({
            "jobId": job_id,
            "function": function,
            "args": args,
            "runAtUnixMs": run_at.timestamp_millis(),
            "coalesce": coalesce,
        });
        let account_id = self.actor_account_id.clone();
        let email = self.actor_email.clone();
        let provenance = self.provenance.clone();
        let fault = self.fault.clone();
        self.ensure_transaction()
            .await?
            .enqueue_action(
                SCHEDULE_OUTBOX_PATH,
                &payload,
                &account_id,
                &email,
                &provenance,
            )
            .await
            .map_err(|error| fault.database(error))?;
        Ok(Value::String(job_id))
    }
}

/// A statement failure inside a direct session.
enum Failure {
    /// SQL failed: the transaction counts as aborted.
    Sql(sqlx::Error),
    /// Decoding a result failed; the statement itself succeeded.
    Decode(String),
}

impl From<sqlx::Error> for Failure {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

impl SqlCalls {
    fn require_open(&self) -> Result<(), String> {
        if self.aborted.is_some() {
            Err(ABORTED.to_owned())
        } else {
            Ok(())
        }
    }

    fn require_write(&self) -> Result<(), String> {
        if self.capability == DatabaseCapability::Reducer {
            Ok(())
        } else {
            Err("a Query cannot modify application tables".to_owned())
        }
    }

    fn sql_failure(&mut self, error: sqlx::Error) -> String {
        let message = self.fault.sql(error);
        if self.aborted.is_none() {
            self.aborted = Some(message.clone());
        }
        // The store refuses to commit a transaction marked failed, so a
        // caught SQL error can never become a partial commit.
        if let Some(session) = &self.session {
            session.mark_failed(message.clone());
        }
        message
    }

    /// Runs `work` on the lent connection, classifying its failure.
    fn run<R>(
        &mut self,
        work: impl FnOnce(&mut DirectConn<'_>) -> Result<R, Failure>,
    ) -> Result<R, String> {
        let session = self.session.clone().ok_or_else(|| CLOSED.to_owned())?;
        match session.run(work).ok_or_else(|| CLOSED.to_owned())? {
            Ok(value) => Ok(value),
            Err(Failure::Sql(error)) => Err(self.sql_failure(error)),
            Err(Failure::Decode(message)) => Err(message),
        }
    }

    /// One database host call, answered with the JSON bytes the module gets.
    fn call(&mut self, call: HostCallFrame) -> Result<Vec<u8>, String> {
        match call {
            HostCallFrame::DbQuery {
                statement,
                parameters,
            } => self.query(&statement, parameters),
            HostCallFrame::DbInsert {
                table,
                row,
                generated_id,
            } => self.insert(&table, row, generated_id),
            HostCallFrame::DbUpdate {
                table,
                key,
                id,
                patch,
            } => self.update(&table, &key, id, patch),
            HostCallFrame::DbDelete { table, key, id } => self.delete(&table, &key, id),
            HostCallFrame::DbDeleteMany { table, key, ids } => self.delete_many(&table, &key, ids),
            _ => Err("not a database call".to_owned()),
        }
    }

    fn query(&mut self, statement: &str, parameters: Value) -> Result<Vec<u8>, String> {
        let admitted = require_single_statement(statement)?;
        if self.capability == DatabaseCapability::Query {
            require_read_head(&admitted.first_word)?;
        }
        // Admission refuses a bare ROLLBACK, so this is ROLLBACK TO.
        let recovers = admitted.first_word == "rollback";
        if !recovers {
            self.require_open()?;
        }
        let parameters = match parameters {
            Value::Null => Vec::new(),
            Value::Array(values) => values,
            _ => return Err("query parameters must be an array".to_owned()),
        };
        // SQLite binds by value: arrays and objects arrive as JSON text that
        // module SQL reads with json_each($n) / json_extract($n, …).
        let parameters = parameters
            .iter()
            .enumerate()
            .map(|(index, value)| {
                Parameter::scalar(value)
                    .map_err(|error| format!("parameter ${}: {error}", index + 1))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // `INSERT/UPDATE/DELETE … RETURNING` columns carry no reliable declared
        // type, so decode them with the target table's pragma_table_info kinds.
        let target = match write_target(statement) {
            Some(table) if quote_identifier(&table).is_ok() => Some(self.catalog_table(&table)?),
            _ => None,
        };
        let changes_schema = matches!(admitted.first_word.as_str(), "create" | "alter" | "drop");
        let sql = statement.trim();
        let result = self.run(|conn| {
            let rows = {
                let mut statement = conn.prepare(sql)?;
                let binds = parameters.iter().map(Parameter::as_bind).collect::<Vec<_>>();
                statement.bind(&binds)?;
                write_query_rows(&mut statement, target.as_deref())?
            };
            if changes_schema {
                conn.schema_changed();
            }
            Ok(rows)
        });
        if changes_schema {
            self.tables.clear();
        }
        let rows = result?;
        if recovers {
            self.aborted = None;
            if let Some(session) = &self.session {
                session.clear_failure();
            }
        }
        Ok(rows)
    }

    fn insert(&mut self, table: &str, row: Value, generated_id: Option<String>) -> Result<Vec<u8>, String> {
        self.require_open()?;
        let row = object(row, "row")?;
        if row.is_empty() {
            return Err("an insert requires at least one column".to_owned());
        }
        let mut values: BTreeMap<String, Value> = row.into_iter().collect();
        let catalog = self.catalog_table(table)?;
        let key = catalog.key.clone();
        if !values.contains_key(&key.column) {
            if let Some(id) = generated_id.filter(|_| key.text_key()) {
                values.insert(key.column.clone(), Value::String(id));
            }
        }
        if !values.contains_key(&key.column) && !key.database_generated {
            match key.data_type.as_deref() {
                Some(_) if key.text_key() => {
                    values.insert(
                        key.column.clone(),
                        Value::String(Uuid::new_v4().to_string()),
                    );
                }
                Some(data_type) => {
                    return Err(format!(
                        "insert into {table:?} requires primary key {:?}; Gonvex can only allocate omitted text keys, but this column is {data_type}",
                        key.column,
                    ));
                }
                None => {}
            }
        }
        let quoted_table = quote_identifier(table)?;
        let columns = values
            .keys()
            .map(|column| quote_identifier(column))
            .collect::<Result<Vec<_>, _>>()?;
        let placeholders = (1..=values.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>();
        let statement = format!(
            "INSERT INTO {quoted_table} ({}) VALUES ({}) RETURNING *",
            columns.join(", "),
            placeholders.join(", ")
        );
        let parameters = values
            .iter()
            .map(|(column, value)| catalog.parameter(column, value))
            .collect::<Result<Vec<_>, _>>()?;
        self.run(|conn| returning_row(conn, &statement, &parameters, &catalog))
    }

    fn update(&mut self, table: &str, key: &str, id: Value, patch: Value) -> Result<Vec<u8>, String> {
        self.require_open()?;
        let key = self.resolve_table_key(table, key)?;
        require_row_id(&id)?;
        let values: BTreeMap<String, Value> = object(patch, "patch")?.into_iter().collect();
        if values.is_empty() {
            return Err("an update requires at least one column".to_owned());
        }
        let catalog = self.catalog_table(table)?;
        let quoted_table = quote_identifier(table)?;
        let assignments = values
            .keys()
            .enumerate()
            .map(|(index, column)| Ok(format!("{} = ${}", quote_identifier(column)?, index + 1)))
            .collect::<Result<Vec<_>, String>>()?;
        let statement = format!(
            "UPDATE {quoted_table} SET {} WHERE {key} = ${} RETURNING *",
            assignments.join(", "),
            values.len() + 1
        );
        let mut parameters = values
            .iter()
            .map(|(column, value)| catalog.parameter(column, value))
            .collect::<Result<Vec<_>, _>>()?;
        parameters.push(Parameter::scalar(&id)?);
        self.run(|conn| returning_row(conn, &statement, &parameters, &catalog))
    }

    fn delete(&mut self, table: &str, key: &str, id: Value) -> Result<Vec<u8>, String> {
        self.require_open()?;
        let key = self.resolve_table_key(table, key)?;
        let table = quote_identifier(table)?;
        require_row_id(&id)?;
        let statement = format!("DELETE FROM {table} WHERE {key} = $1");
        let parameter = Parameter::scalar(&id)?;
        let deleted = self.run(|conn| Ok(conn.execute(&statement, &[parameter.as_bind()])?))?;
        Ok(format!(r#"{{"deleted":{deleted}}}"#).into_bytes())
    }

    fn delete_many(&mut self, table: &str, key: &str, ids: Value) -> Result<Vec<u8>, String> {
        self.require_open()?;
        let ids = ids
            .as_array()
            .ok_or_else(|| "deleteMany ids must be an array".to_owned())?;
        if ids.len() > 10_000 {
            return Err("deleteMany cannot delete more than 10000 rows per call".to_owned());
        }
        if ids.is_empty() {
            return Ok(br#"{"deleted":0}"#.to_vec());
        }
        for id in ids {
            require_row_id(id)?;
        }
        let key = self.resolve_table_key(table, key)?;
        let table = quote_identifier(table)?;
        // One JSON array parameter instead of up to 10,000 placeholders
        // (SQLite's default variable limit is 32,766).
        let statement = format!(
            "DELETE FROM {table} WHERE {key} IN (SELECT value FROM json_each($1))"
        );
        let ids = Value::Array(ids.clone()).to_string();
        let deleted = self.run(|conn| Ok(conn.execute(&statement, &[Bind::Text(&ids)])?))?;
        Ok(format!(r#"{{"deleted":{deleted}}}"#).into_bytes())
    }

    /// The key a module declared for `table`, without touching the database.
    fn declared_table_key(&self, table: &str, requested: &str) -> Result<Option<String>, String> {
        let requested = requested.trim();
        let declared = declared_table_key(&self.schema, table)?.map(str::to_owned);
        if !requested.is_empty() {
            if declared
                .as_deref()
                .is_some_and(|declared| requested != declared)
            {
                return Err(format!(
                    "table {table:?} declares primary key {:?}, not {requested:?}",
                    declared.as_deref().unwrap_or_default(),
                ));
            }
            return quote_identifier(requested).map(Some);
        }
        if let Some(declared) = declared {
            return quote_identifier(&declared).map(Some);
        }
        if let Some(cached) = self.tables.get(table) {
            return quote_identifier(&cached.key.column).map(Some);
        }
        Ok(None)
    }

    fn resolve_table_key(&mut self, table: &str, requested: &str) -> Result<String, String> {
        if let Some(key) = self.declared_table_key(table, requested)? {
            return Ok(key);
        }
        let catalog = self.catalog_table(table)?;
        quote_identifier(&catalog.key.column)
    }

    /// Columns and primary key of `table`, read once per invocation
    /// transaction from `pragma_table_info`.
    fn catalog_table(&mut self, table: &str) -> Result<Arc<TableCatalog>, String> {
        if let Some(cached) = self.tables.get(table) {
            return Ok(cached.clone());
        }
        let (schema_name, table_name) = table_catalog_parts(table)?;
        self.require_open()?;
        let rows = self.run(|conn| {
            let mut statement = conn.prepare(
                r#"SELECT name, type, pk, dflt_value IS NOT NULL AS has_default
                   FROM pragma_table_info($1, COALESCE($2, 'main'))
                   ORDER BY cid"#,
            )?;
            statement.bind(&[
                Bind::Text(&table_name),
                schema_name.as_deref().map_or(Bind::Null, Bind::Text),
            ])?;
            let mut rows = Vec::new();
            while statement.step()? {
                let name = statement.str(0).map_err(Failure::Decode)?.to_owned();
                let declared = statement.str(1).map_err(Failure::Decode)?.to_owned();
                rows.push((name, declared, statement.int64(2), statement.int64(3) != 0));
            }
            Ok(rows)
        })?;
        let mut columns = BTreeMap::new();
        let mut keys = Vec::new();
        for (name, declared, position, has_default) in rows {
            columns.insert(name.clone(), JsonKind::from_declared(&declared));
            if position > 0 {
                keys.push((position, name, declared, has_default));
            }
        }
        keys.sort();
        let key = match keys.as_slice() {
            [] => TableKey {
                column: DEFAULT_KEY.to_owned(),
                data_type: None,
                database_generated: true,
            },
            [(_, column, declared, has_default)] => TableKey {
                column: column.clone(),
                data_type: Some(declared.clone()),
                // INTEGER PRIMARY KEY aliases the rowid, which SQLite assigns.
                database_generated: *has_default || declared.trim().eq_ignore_ascii_case("INTEGER"),
            },
            _ => {
                return Err(format!(
                    "table {table:?} has a composite primary key, which db.update/delete do not support"
                ));
            }
        };
        let catalog = Arc::new(TableCatalog { key, columns });
        self.tables.insert(table.to_owned(), catalog.clone());
        Ok(catalog)
    }
}

/// Declared column kinds and primary key of one application table.
#[derive(Debug)]
struct TableCatalog {
    key: TableKey,
    columns: BTreeMap<String, JsonKind>,
}

impl TableCatalog {
    fn parameter(&self, column: &str, value: &Value) -> Result<Parameter, String> {
        match self.columns.get(column) {
            Some(kind) => Parameter::column(value, *kind),
            None => Parameter::scalar(value),
        }
        .map_err(|error| format!("column {column:?}: {error}"))
    }
}

/// Runs an `INSERT/UPDATE … RETURNING *` and writes its first row (or
/// `null`) decoded by the table's declared kinds, as `rows::row_to_object`
/// does for sqlx rows.
fn returning_row(
    conn: &mut DirectConn<'_>,
    sql: &str,
    parameters: &[Parameter],
    catalog: &TableCatalog,
) -> Result<Vec<u8>, Failure> {
    let mut statement = conn.prepare(sql)?;
    let binds = parameters.iter().map(Parameter::as_bind).collect::<Vec<_>>();
    statement.bind(&binds)?;
    if !statement.step()? {
        return Ok(b"null".to_vec());
    }
    let mut out = Vec::with_capacity(256);
    out.push(b'{');
    for index in 0..statement.column_count() {
        if index > 0 {
            out.push(b',');
        }
        let name = statement.column_name(index);
        let kind = catalog
            .columns
            .get(name)
            .copied()
            .unwrap_or_else(|| sqlx_declared_kind(statement.column_decltype(index)));
        direct::write_str(&mut out, name);
        out.push(b':');
        statement
            .write_json(index, kind, &mut out)
            .map_err(Failure::Decode)?;
    }
    out.push(b'}');
    Ok(out)
}

/// The kind `JsonKind::from_declared` gives sqlx's own name for a declared
/// type (sqlx maps unknown declarations, and expressions, to `NULL`).
fn sqlx_declared_kind(declared: Option<&str>) -> JsonKind {
    let Some(declared) = declared else {
        return JsonKind::Text;
    };
    let declared = declared.to_ascii_lowercase();
    match declared.as_str() {
        "int4" | "int8" => JsonKind::Integer,
        "boolean" | "bool" => JsonKind::Boolean,
        "date" | "time" | "datetime" | "timestamp" => JsonKind::Text,
        _ if declared.contains("int") => JsonKind::Integer,
        _ if declared.contains("char") || declared.contains("clob") || declared.contains("text") => {
            JsonKind::Text
        }
        _ if declared.contains("blob") => JsonKind::Text,
        _ if declared.contains("real") || declared.contains("floa") || declared.contains("doub") => {
            JsonKind::Real
        }
        _ => JsonKind::Text,
    }
}

/// Writes every row of a module query as a JSON array of objects. Each column
/// is decoded by its declared type: the write target's catalog for
/// `RETURNING`, else the statement's exact declared result type
/// (`sqlite3_column_decltype`). Only expression columns, which have no
/// declared type, fall back to the storage class, where TEXT that is
/// unambiguously a JSON value (object, array, quoted string, `true`/`false`)
/// is decoded as JSON because SQL builds JSON there (`json_object`,
/// `json_group_array`). Columns declared TEXT are never reinterpreted.
fn write_query_rows(
    statement: &mut direct::Stmt<'_>,
    target: Option<&TableCatalog>,
) -> Result<Vec<u8>, Failure> {
    let mut out = Vec::with_capacity(256);
    out.push(b'[');
    let mut columns: Option<Vec<(Vec<u8>, Option<JsonKind>)>> = None;
    let mut first = true;
    while statement.step()? {
        let columns = columns.get_or_insert_with(|| {
            (0..statement.column_count())
                .map(|index| {
                    let name = statement.column_name(index);
                    let mut key = Vec::with_capacity(name.len() + 3);
                    direct::write_str(&mut key, name);
                    key.push(b':');
                    let kind = target
                        .and_then(|table| table.columns.get(name).copied())
                        .or_else(|| statement.column_decltype(index).map(JsonKind::from_declared));
                    (key, kind)
                })
                .collect()
        });
        if !first {
            out.push(b',');
        }
        first = false;
        out.push(b'{');
        for (index, (key, kind)) in columns.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            out.extend_from_slice(key);
            match kind {
                Some(kind) => statement
                    .write_json(index, *kind, &mut out)
                    .map_err(Failure::Decode)?,
                None => write_expression_cell(statement, index, &mut out)?,
            }
        }
        out.push(b'}');
    }
    out.push(b']');
    Ok(out)
}

fn write_expression_cell(
    statement: &direct::Stmt<'_>,
    index: usize,
    out: &mut Vec<u8>,
) -> Result<(), Failure> {
    if statement.is_text(index) {
        let text = statement.str(index).map_err(Failure::Decode)?;
        if json_shaped(text) {
            if let Ok(value) = serde_json::from_str::<Value>(text) {
                direct::write_value(out, &value);
                return Ok(());
            }
        }
        direct::write_str(out, text);
        return Ok(());
    }
    statement
        .write_json(index, JsonKind::Text, out)
        .map_err(Failure::Decode)
}

/// The table a single-table `INSERT INTO t`, `UPDATE t` or `DELETE FROM t`
/// statement writes, when its head is that simple (leading comments allowed).
fn write_target(statement: &str) -> Option<String> {
    let mut rest = statement.trim_start_matches(|ch: char| ch.is_whitespace() || ch == ';');
    loop {
        if let Some(comment) = rest.strip_prefix("--") {
            rest = comment.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(comment) = rest.strip_prefix("/*") {
            rest = comment.split_once("*/")?.1;
        } else {
            break;
        }
        rest = rest.trim_start();
    }
    let mut words = rest.split_whitespace();
    let first = words.next()?.to_ascii_lowercase();
    let mut target = match first.as_str() {
        "insert" | "delete" => {
            let into = words.next()?.to_ascii_lowercase();
            if into != if first == "insert" { "into" } else { "from" } {
                return None;
            }
            words.next()?
        }
        "update" => words.next()?,
        _ => return None,
    };
    // Strip a trailing column list or alias punctuation: `"tasks"(` → tasks.
    if let Some(index) = target.find('(') {
        target = &target[..index];
    }
    let unquoted = target
        .split('.')
        .map(|part| {
            part.strip_prefix('"')
                .and_then(|part| part.strip_suffix('"'))
                .unwrap_or(part)
        })
        .collect::<Vec<_>>()
        .join(".");
    (!unquoted.is_empty()).then_some(unquoted)
}

fn json_shaped(text: &str) -> bool {
    let trimmed = text.trim();
    matches!(trimmed.as_bytes().first(), Some(b'{' | b'[' | b'"'))
        || trimmed == "true"
        || trimmed == "false"
}

fn object(value: Value, name: &str) -> Result<Map<String, Value>, String> {
    match value {
        Value::Object(value) => Ok(value),
        Value::Null => Ok(Map::new()),
        _ => Err(format!("{name} must be an object")),
    }
}

fn require_row_id(value: &Value) -> Result<(), String> {
    if matches!(value, Value::String(_) | Value::Number(_)) {
        Ok(())
    } else {
        Err("a row id must be a string or number".to_owned())
    }
}

fn declared_table_key<'a>(schema: &'a Value, table: &str) -> Result<Option<&'a str>, String> {
    let Some(schema) = schema.as_object() else {
        return Ok(None);
    };
    let table_definition = ["tables", "tenantTables", "controlPlaneTables"]
        .iter()
        .filter_map(|scope| schema.get(*scope).and_then(Value::as_object))
        .find_map(|tables| tables.get(table).and_then(Value::as_object));
    let Some(table_definition) = table_definition else {
        return Ok(None);
    };
    let Some(columns) = table_definition.get("columns").and_then(Value::as_object) else {
        return Ok(None);
    };
    let keys = columns
        .iter()
        .filter_map(|(name, column)| {
            column
                .as_object()
                .and_then(|column| column.get("primaryKey"))
                .and_then(Value::as_bool)
                .filter(|primary| *primary)
                .map(|_| name.as_str())
        })
        .collect::<Vec<_>>();
    match keys.as_slice() {
        [] => Ok(None),
        [key] => Ok(Some(key)),
        _ => Err(format!(
            "table {table:?} declares a composite primary key, which db.update/delete do not support"
        )),
    }
}

fn table_catalog_parts(table: &str) -> Result<(Option<String>, String), String> {
    quote_identifier(table)?;
    let parts = table.trim().split('.').collect::<Vec<_>>();
    Ok(match parts.as_slice() {
        [table] => (None, (*table).to_owned()),
        [schema, table] => (Some((*schema).to_owned()), (*table).to_owned()),
        _ => return Err(format!("identifier {table:?} is not a table name")),
    })
}

fn quote_identifier(name: &str) -> Result<String, String> {
    let name = name.trim();
    let parts = name.split('.').collect::<Vec<_>>();
    if name.is_empty() || parts.len() > 2 || parts.iter().any(|part| !valid_identifier(part)) {
        return Err(format!("identifier {name:?} is not a table or column name"));
    }
    Ok(parts
        .into_iter()
        .map(|part| format!("\"{part}\""))
        .collect::<Vec<_>>()
        .join("."))
}

fn valid_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    name.chars().enumerate().all(|(index, character)| {
        character.is_ascii_alphabetic()
            || character == '_'
            || (index > 0 && (character.is_ascii_digit() || character == '$'))
    })
}

fn require_read_head(head: &str) -> Result<(), String> {
    if ["select", "with", "values", "explain"]
        .iter()
        .any(|keyword| head.eq_ignore_ascii_case(keyword))
    {
        Ok(())
    } else {
        let head = head.to_ascii_lowercase();
        Err(format!(
            "a Query may only read; {head:?} is not a read statement"
        ))
    }
}

#[cfg(test)]
fn require_read_statement(statement: &str) -> Result<(), String> {
    let admitted = require_single_statement(statement)?;
    require_read_head(&admitted.first_word)
}

fn require_single_statement(statement: &str) -> Result<AdmittedStatement, String> {
    admit_statement(statement)
}

#[cfg(test)]
mod tests {
    #[test]
    fn deferred_ids_match_the_browser_sdk_vectors() {
        assert_eq!(
            super::intent_deferred_id("tenant-1", "account-1", "command-1", "action", 0),
            "aa14126b-2f16-814a-8623-07601307140c"
        );
        assert_eq!(
            super::intent_deferred_id("tenant-1", "account-1", "command-1", "schedule", 0),
            "c9983b7c-273c-8f8e-91df-ef1b9ba899f6"
        );
        assert_ne!(
            super::intent_deferred_id("tenant-1", "account-1", "command-1", "action", 0),
            super::intent_deferred_id("tenant-1", "account-1", "command-1", "action", 1)
        );
    }
    use super::*;

    use gonvex_store::{Durability, PoolLimits, PoolRegistry};

    /// A file-backed SQLite database in a temporary directory.
    pub(super) struct TestDatabase {
        _directory: tempfile::TempDir,
        pub(super) pools: PoolRegistry,
        pub(super) url: String,
    }

    impl TestDatabase {
        pub(super) async fn new(ddl: &str) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let url = gonvex_store::database_url(&directory.path().join("tenant.db"));
            let pools = PoolRegistry::with_durability(PoolLimits::default(), Durability::Normal);
            let pool = pools.pool(&url).await.unwrap();
            gonvex_store::execute_script(&mut pool.acquire().await.unwrap(), ddl)
                .await
                .unwrap();
            Self {
                _directory: directory,
                pools,
                url,
            }
        }

        pub(super) async fn calls(&self, capability: DatabaseCapability) -> DatabaseHostCalls {
            let transaction = gonvex_store::begin_transaction(
                &self.pools,
                &self.url,
                capability == DatabaseCapability::Query,
            )
            .await
            .unwrap();
            DatabaseHostCalls::new(transaction, capability)
        }
    }

    #[test]
    fn identifiers_are_plain_and_bounded() {
        assert_eq!(
            quote_identifier("public.tasks").unwrap(),
            "\"public\".\"tasks\""
        );
        assert!(quote_identifier("tasks; DROP TABLE tasks").is_err());
        assert!(quote_identifier("a.b.c").is_err());
    }

    #[test]
    fn query_statement_rejects_writes_and_batches() {
        assert!(require_read_statement("SELECT * FROM tasks").is_ok());
        assert!(require_read_statement("sElEcT * FROM tasks").is_ok());
        assert!(require_read_statement("selective * FROM tasks").is_err());
        assert!(require_read_statement("UPDATE tasks SET title = 'x'").is_err());
        assert!(require_single_statement("SELECT ';'; SELECT 2").is_err());
        assert!(require_single_statement("SELECT ';'").is_ok());
    }

    #[tokio::test]
    async fn resolves_declared_non_id_primary_key_for_omitted_host_key() {
        let schema = serde_json::json!({
            "tables": {
                "tasks": {
                    "columns": {
                        "_id": {"type": "text", "nullable": false, "primaryKey": true},
                        "title": {"type": "text", "nullable": false, "primaryKey": false}
                    },
                    "indexes": {}
                }
            }
        });
        assert_eq!(declared_table_key(&schema, "tasks").unwrap(), Some("_id"));

        let calls = DatabaseHostCalls::with_parts(None, None, DatabaseCapability::Reducer)
            .with_schema(&schema);
        let mut calls = calls;
        assert_eq!(
            calls.resolve_table_key("tasks", "").await.unwrap(),
            "\"_id\""
        );
    }

    #[tokio::test]
    async fn rejects_explicit_key_that_conflicts_with_declared_primary_key() {
        let schema = serde_json::json!({
            "tenantTables": {
                "tasks": {
                    "columns": {
                        "_id": {"type": "text", "nullable": false, "primaryKey": true}
                    }
                }
            }
        });
        let calls = DatabaseHostCalls::with_parts(None, None, DatabaseCapability::Reducer)
            .with_schema(&schema);
        let mut calls = calls;
        assert!(calls.resolve_table_key("tasks", "id").await.is_err());
    }

    #[tokio::test]
    async fn binds_values_by_destination_column_type_and_deletes_many_rows() {
        let database = TestDatabase::new(
            r#"CREATE TABLE "tasks" ("_id" TEXT PRIMARY KEY, "title" TEXT NOT NULL, "metadata" JSON, "tags" JSON, "score" INTEGER, "done" BOOLEAN)"#,
        )
        .await;
        let mut calls = database
            .calls(DatabaseCapability::Reducer)
            .await
            .with_schema(&serde_json::json!({
                "tables": {
                    "tasks": {
                        "columns": {
                            "_id": {"type": "text", "nullable": false, "primaryKey": true},
                            "title": {"type": "text", "nullable": false, "primaryKey": false},
                            "metadata": {"type": "json", "nullable": true, "primaryKey": false},
                            "tags": {"type": "json", "nullable": true, "primaryKey": false},
                            "score": {"type": "integer", "nullable": true, "primaryKey": false},
                            "done": {"type": "boolean", "nullable": true, "primaryKey": false}
                        }
                    }
                }
            }));

        let array_rows = calls
            .query(
                "SELECT value FROM json_each($1) ORDER BY value",
                serde_json::json!([["second", "first"]]),
            )
            .await
            .unwrap();
        assert_eq!(
            array_rows,
            serde_json::json!([{"value":"first"},{"value":"second"}])
        );
        let inserted = calls
            .insert(
                "tasks",
                serde_json::json!({
                    "_id": "first",
                    "title": "first",
                    "metadata": "created",
                    "tags": ["one", "two"],
                    "score": 1,
                    "done": true
                }),
                None,
            )
            .await
            .unwrap();
        assert_eq!(inserted["metadata"], serde_json::json!("created"));
        assert_eq!(inserted["tags"], serde_json::json!(["one", "two"]));
        assert_eq!(inserted["score"], serde_json::json!(1));
        assert_eq!(inserted["done"], serde_json::json!(true));
        // Module queries decode declared JSON and BOOLEAN columns too.
        assert_eq!(
            calls
                .query(
                    "SELECT \"_id\", \"metadata\", \"tags\", \"done\" FROM \"tasks\" WHERE \"score\" IN (SELECT value FROM json_each($1))",
                    serde_json::json!([[1, 2, 3]]),
                )
                .await
                .unwrap(),
            serde_json::json!([{"_id":"first","metadata":"created","tags":["one","two"],"done":true}])
        );

        let allocated = calls
            .insert(
                "tasks",
                serde_json::json!({"title": "allocated"}),
                Some("intent-owned".to_owned()),
            )
            .await
            .unwrap();
        assert_eq!(allocated["_id"], "intent-owned");
        calls
            .delete("tasks", "", serde_json::json!("intent-owned"))
            .await
            .unwrap();
        let generated = calls
            .insert("tasks", serde_json::json!({"title": "generated"}), None)
            .await
            .unwrap();
        assert!(Uuid::parse_str(generated["_id"].as_str().unwrap()).is_ok());
        calls
            .delete("tasks", "", generated["_id"].clone())
            .await
            .unwrap();
        let explicit = calls
            .insert(
                "tasks",
                serde_json::json!({"_id": "explicit", "title": "explicit"}),
                Some("unused-allocation".to_owned()),
            )
            .await
            .unwrap();
        assert_eq!(explicit["_id"], "explicit");
        calls
            .delete("tasks", "", serde_json::json!("explicit"))
            .await
            .unwrap();

        let updated = calls
            .update(
                "tasks",
                "",
                serde_json::json!("first"),
                serde_json::json!({
                    "metadata": false,
                    "tags": ["updated"],
                    "score": 2,
                    "done": false
                }),
            )
            .await
            .unwrap();
        assert_eq!(updated["metadata"], serde_json::json!(false));
        assert_eq!(updated["tags"], serde_json::json!(["updated"]));
        assert_eq!(updated["score"], serde_json::json!(2));
        assert_eq!(updated["done"], serde_json::json!(false));

        for id in ["second", "third"] {
            calls
                .insert("tasks", serde_json::json!({"_id": id, "title": id}), None)
                .await
                .unwrap();
        }
        assert_eq!(
            calls
                .delete_many("tasks", "", serde_json::json!(["first", "third"]))
                .await
                .unwrap(),
            serde_json::json!({"deleted": 2})
        );
        assert_eq!(
            calls
                .query(
                    "SELECT \"_id\" FROM \"tasks\" ORDER BY \"_id\"",
                    serde_json::json!([]),
                )
                .await
                .unwrap(),
            serde_json::json!([{"_id": "second"}])
        );
        calls.finish(false).await.unwrap();
    }

    #[tokio::test]
    async fn sdk_write_statements_decode_returning_rows_by_declared_type() {
        let database = TestDatabase::new(
            r#"CREATE TABLE "tasks" ("_id" TEXT PRIMARY KEY, "meta" JSON, "done" BOOLEAN, "n" INTEGER)"#,
        )
        .await;
        let mut calls = database.calls(DatabaseCapability::Reducer).await;
        for statement in [
            "SAVEPOINT gonvex_data_writes",
            "RELEASE gonvex_data_writes",
            "SAVEPOINT gonvex_data_writes",
        ] {
            calls.query(statement, Value::Null).await.unwrap();
        }
        let inserted = calls
            .query(
                r#"INSERT INTO "tasks" ("_id", "meta", "done", "n") SELECT json_extract(value, '$._id'), json(json_extract(value, '$.meta')), json_extract(value, '$.done'), json_extract(value, '$.n') FROM json_each($1) RETURNING *"#,
                serde_json::json!([[{"_id": "a", "meta": {"x": [1]}, "done": true, "n": 3}]]),
            )
            .await
            .unwrap();
        assert_eq!(
            inserted,
            serde_json::json!([{"_id": "a", "meta": {"x": [1]}, "done": true, "n": 3}])
        );
        let updated = calls
            .query(
                r#"UPDATE "tasks" SET "done" = 0 FROM json_each($1) AS input WHERE "tasks"."_id" = input.value RETURNING *"#,
                serde_json::json!([["a"]]),
            )
            .await
            .unwrap();
        assert_eq!(updated[0]["done"], serde_json::json!(false));
        calls
            .query("ROLLBACK TO gonvex_data_writes", Value::Null)
            .await
            .unwrap();
        calls
            .query("RELEASE gonvex_data_writes", Value::Null)
            .await
            .unwrap();
        assert_eq!(
            calls
                .query("SELECT count(*) AS n FROM tasks", Value::Null)
                .await
                .unwrap(),
            serde_json::json!([{"n": 0}])
        );
        assert_eq!(
            write_target("/* x */ INSERT INTO \"tasks\"(a) VALUES (1)").as_deref(),
            Some("tasks")
        );
        assert_eq!(
            write_target("update main.tasks SET a=1").as_deref(),
            Some("main.tasks")
        );
        assert_eq!(write_target("SELECT 1"), None);
        let mut query = database.calls(DatabaseCapability::Query).await;
        assert!(query
            .query("SAVEPOINT gonvex_data_writes", Value::Null)
            .await
            .unwrap_err()
            .contains("may only read"));
        calls.finish(false).await.unwrap();
    }

    #[tokio::test]
    async fn primary_keys_come_from_pragma_table_info() {
        let database = TestDatabase::new(
            r#"CREATE TABLE numbered (id INTEGER PRIMARY KEY, title TEXT);
               CREATE TABLE coded (code TEXT PRIMARY KEY, title TEXT);
               CREATE TABLE pair (a TEXT, b TEXT, PRIMARY KEY (a, b));"#,
        )
        .await;
        let mut calls = database.calls(DatabaseCapability::Reducer).await;
        let numbered = calls
            .insert(
                "numbered",
                serde_json::json!({"title": "n"}),
                Some("unused".into()),
            )
            .await
            .unwrap();
        assert_eq!(numbered, serde_json::json!({"id": 1, "title": "n"}));
        assert_eq!(
            calls.resolve_table_key("coded", "").await.unwrap(),
            "\"code\""
        );
        let coded = calls
            .insert(
                "coded",
                serde_json::json!({"title": "c"}),
                Some("given".into()),
            )
            .await
            .unwrap();
        assert_eq!(coded["code"], "given");
        assert!(calls
            .resolve_table_key("pair", "")
            .await
            .unwrap_err()
            .contains("composite primary key"));
        calls.finish(false).await.unwrap();
    }
}

#[cfg(test)]
#[path = "host_calls_parameter_tests.rs"]
mod parameter_tests;
