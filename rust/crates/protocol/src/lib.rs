//! The server-side representation of the published Gonvex 0.5.0 wire contract.
//!
//! This crate deliberately contains no database or transport code. Both the
//! former Go runtime and the Rust runtime are tested against the
//! same JSON fixtures. A client-visible protocol change belongs in a new npm
//! release, not in the implementation migration.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionScope {
    Tenant,
    Control,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCursor {
    pub epoch: String,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicInvocationProvenance {
    pub root_command_id: String,
    pub channel: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_member_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of_member_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_execution_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRevision {
    pub epoch: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageTrace {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_sent_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_received_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_reducer_started_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_reducer_committed_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_completed_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_broadcast_scheduled_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_change_committed_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_subscription_started_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_subscription_sent_at_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_duration_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_perf: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserTelemetryInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub viewport_width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub viewport_height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_concurrency: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_memory: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub touch_points: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_connection_type: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_ready_many: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_watermark: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_page_patch: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_object_patch: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_order_delta: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_fanout: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_result_batch: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySubscribeRequest {
    pub id: String,
    pub path: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ExecutionScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReducerCallRequest {
    pub id: String,
    pub path: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ExecutionScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<MessageTrace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaOpenRequest {
    pub id: String,
    pub path: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ReplicaCursor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hashes: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub full_integrity: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

/// Frames sent by `@gonvex/client@0.4.1`.
///
/// `deny_unknown_fields` is intentionally omitted. A 0.4.1 server may ignore a
/// field added by a newer client, but it must never reinterpret the frame as a
/// different operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
// Frames are decoded and dispatched immediately. Boxing one variant would
// complicate every call site without reducing retained connection state.
#[allow(clippy::large_enum_variant)]
pub enum ClientMessage {
    #[serde(rename = "auth")]
    Auth {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant: Option<String>,
        #[serde(default, rename = "controlOnly", skip_serializing_if = "is_false")]
        control_only: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        device: Option<BrowserTelemetryInfo>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        capabilities: Option<ClientCapabilities>,
    },
    #[serde(rename = "query.call")]
    QueryCall {
        id: String,
        path: String,
        #[serde(default)]
        args: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<ExecutionScope>,
    },
    #[serde(rename = "query.subscribe")]
    QuerySubscribe {
        id: String,
        path: String,
        #[serde(default)]
        args: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<ExecutionScope>,
        #[serde(
            default,
            rename = "windowRevision",
            skip_serializing_if = "Option::is_none"
        )]
        window_revision: Option<String>,
    },
    #[serde(rename = "query.unsubscribe")]
    QueryUnsubscribe { id: String },
    #[serde(rename = "query.subscribeMany")]
    QuerySubscribeMany {
        subscribes: Vec<QuerySubscribeRequest>,
    },
    #[serde(rename = "replica.open")]
    ReplicaOpen(ReplicaOpenRequest),
    #[serde(rename = "replica.openMany")]
    ReplicaOpenMany { opens: Vec<ReplicaOpenRequest> },
    #[serde(rename = "replica.close")]
    ReplicaClose { id: String },
    #[serde(rename = "reducer.call")]
    ReducerCall(ReducerCallRequest),
    #[serde(rename = "reducer.callMany")]
    ReducerCallMany { calls: Vec<ReducerCallRequest> },
    #[serde(rename = "action.call")]
    ActionCall {
        id: String,
        path: String,
        #[serde(default)]
        args: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<ExecutionScope>,
        #[serde(
            default,
            rename = "idempotencyKey",
            skip_serializing_if = "Option::is_none"
        )]
        idempotency_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<MessageTrace>,
    },
    #[serde(rename = "error.register")]
    ErrorRegister {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        release: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        environment: Option<String>,
    },
    #[serde(rename = "error.envelope")]
    ErrorEnvelope { id: String, events: Vec<Value> },
    #[serde(rename = "error.heartbeat")]
    ErrorHeartbeat { id: String },
    #[serde(rename = "telemetry.event")]
    TelemetryEvent {
        id: String,
        kind: String,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        outcome: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(
            default,
            rename = "clientSentAtMs",
            skip_serializing_if = "Option::is_none"
        )]
        client_sent_at_ms: Option<f64>,
        #[serde(rename = "clientReceivedAtMs")]
        client_received_at_ms: f64,
        #[serde(
            default,
            rename = "clientDurationMs",
            skip_serializing_if = "Option::is_none"
        )]
        client_duration_ms: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<MessageTrace>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        device: Option<BrowserTelemetryInfo>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_batch: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_integrity: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_batch: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_result_batch: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reducer_batch: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_watermark: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaDirective {
    pub protocol_version: u32,
    pub scope: String,
    pub visibility_scope: String,
    pub epoch: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaChange {
    pub entity: String,
    pub id: String,
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_value: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_columns: Vec<String>,
}

/// Frames accepted by `@gonvex/client@0.4.1`.
///
/// Result and patch payloads remain `serde_json::Value`. Their schemas belong
/// to the deployed application module, not the transport protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "session.ready")]
    SessionReady {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica: Option<ReplicaDirective>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        capabilities: Option<ServerCapabilities>,
    },
    #[serde(rename = "auth.result")]
    AuthResult { id: String, result: Value },
    #[serde(rename = "auth.error")]
    AuthError { id: String, error: String },
    #[serde(rename = "query.result")]
    QueryResult {
        id: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "query.progress")]
    QueryProgress {
        id: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "query.patch")]
    QueryPatch {
        id: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "query.pagePatch")]
    QueryPagePatch {
        id: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "query.objectPatch")]
    QueryObjectPatch {
        id: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "query.batch")]
    QueryBatch { messages: Vec<ServerMessage> },
    #[serde(rename = "query.fanout")]
    QueryFanout {
        ids: Vec<String>,
        #[serde(rename = "queryType")]
        query_type: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "query.error")]
    QueryError {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        error: String,
    },
    #[serde(rename = "reducer.result")]
    ReducerResult {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        result: Value,
        #[serde(rename = "originCommandId")]
        origin_command_id: String,
        #[serde(
            default,
            rename = "committedRevision",
            skip_serializing_if = "Option::is_none"
        )]
        committed_revision: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<MessageTrace>,
    },
    #[serde(rename = "reducer.error")]
    ReducerError {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        error: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<MessageTrace>,
    },
    #[serde(rename = "action.result")]
    ActionResult {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        result: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<MessageTrace>,
    },
    #[serde(rename = "action.error")]
    ActionError {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        error: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<MessageTrace>,
    },
    #[serde(rename = "replica.transaction")]
    ReplicaTransaction {
        cursor: ReplicaCursor,
        #[serde(
            default,
            rename = "originCommandId",
            skip_serializing_if = "Option::is_none"
        )]
        origin_command_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provenance: Option<PublicInvocationProvenance>,
        changes: Vec<ReplicaChange>,
    },
    #[serde(rename = "replica.watermark")]
    ReplicaWatermark { revision: u64 },
    #[serde(rename = "replica.snapshot")]
    ReplicaSnapshot {
        id: String,
        result: Vec<Value>,
        cursor: ReplicaCursor,
        key: String,
        #[serde(flatten)]
        metadata: BTreeMap<String, Value>,
    },
    #[serde(rename = "replica.delta")]
    ReplicaDelta {
        id: String,
        cursor: ReplicaCursor,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "replica.ready")]
    ReplicaReady {
        id: String,
        cursor: ReplicaCursor,
        digest: String,
        #[serde(flatten)]
        metadata: BTreeMap<String, Value>,
    },
    #[serde(rename = "replica.readyMany")]
    ReplicaReadyMany { ready: Vec<Value> },
    #[serde(rename = "replica.needHashes")]
    ReplicaNeedHashes {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    #[serde(rename = "replica.syncing")]
    ReplicaSyncing {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        reason: String,
    },
    #[serde(rename = "replica.reset")]
    ReplicaReset {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        reason: String,
    },
    #[serde(rename = "replica.error")]
    ReplicaError {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        error: String,
    },
    #[serde(rename = "error.ack")]
    ErrorAck {
        id: String,
        #[serde(flatten)]
        payload: BTreeMap<String, Value>,
    },
    #[serde(rename = "support.command")]
    SupportCommand { id: String, result: Value },
    #[serde(rename = "system.reload")]
    SystemReload {
        reason: String,
        #[serde(
            default,
            rename = "artifactHash",
            skip_serializing_if = "Option::is_none"
        )]
        artifact_hash: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_client(source: &str) {
        let expected: Value = serde_json::from_str(source).expect("valid fixture JSON");
        let message: ClientMessage =
            serde_json::from_value(expected.clone()).expect("client frame");
        let actual = serde_json::to_value(message).expect("serialized client frame");
        assert_eq!(actual, expected);
    }

    fn round_trip_server(source: &str) {
        let expected: Value = serde_json::from_str(source).expect("valid fixture JSON");
        let message: ServerMessage =
            serde_json::from_value(expected.clone()).expect("server frame");
        let actual = serde_json::to_value(message).expect("serialized server frame");
        assert_eq!(actual, expected);
    }

    #[test]
    fn preserves_published_client_frames() {
        for frame in [
            r#"{"type":"auth","id":"auth-1","token":"gvx_session_test","project":"shop","tenant":"tenant-a","capabilities":{"replicaReadyMany":1,"queryPagePatch":1}}"#,
            r#"{"type":"query.subscribe","id":"q-1","path":"tasks.grid","args":{"filter":null},"scope":"tenant","windowRevision":"window-1"}"#,
            r#"{"type":"reducer.call","id":"r-1","path":"tasks.start","args":{"taskId":"task-1"},"scope":"tenant","idempotencyKey":"command-1"}"#,
            r#"{"type":"replica.open","id":"replica-1","path":"tasks.recent","args":{},"cursor":{"epoch":"epoch-1","revision":41},"hashes":{"task-1":"abc"},"digest":"digest-1","fullIntegrity":true}"#,
            r#"{"type":"error.envelope","id":"errors-1","events":[{"message":"safe"}]}"#,
        ] {
            round_trip_client(frame);
        }
    }

    #[test]
    fn preserves_published_server_frames_and_explicit_null() {
        for frame in [
            r#"{"type":"session.ready","project":"shop","tenant":"tenant-a","replica":{"protocolVersion":1,"scope":"shop:tenant-a","visibilityScope":"vf-1","epoch":"epoch-1"},"capabilities":{"protocolVersion":2,"runtimeVersion":"0.4.1","replicaBatch":1,"replicaIntegrity":1,"queryBatch":1,"reducerBatch":1,"replicaWatermark":1}}"#,
            r#"{"type":"query.result","id":"q-1","path":"tasks.grid","result":{"page":[]},"reason":"initial","subscriptionRevision":{"epoch":"epoch-1","sequence":42}}"#,
            r#"{"type":"reducer.result","id":"r-1","path":"tasks.start","result":null,"originCommandId":"command-1","committedRevision":42}"#,
            r#"{"type":"replica.transaction","cursor":{"epoch":"epoch-1","revision":42},"originCommandId":"command-1","changes":[{"entity":"tasks","id":"task-1","operation":"update","oldValue":{"status":"ready"},"newValue":{"status":"started"},"changedColumns":["status"]}]}"#,
            r#"{"type":"replica.ready","id":"replica-1","cursor":{"epoch":"epoch-1","revision":42},"digest":"digest-2","truncated":false}"#,
        ] {
            round_trip_server(frame);
        }
    }
}
