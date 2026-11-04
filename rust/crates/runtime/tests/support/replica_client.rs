//! A minimal model of the SDK's Replica window state, used by tests and the
//! replica-state bench to check what a client actually ends up holding.
//!
//! It applies `replica.snapshot`, `replica.delta`, `replica.ready`,
//! `replica.transaction`, `replica.needHashes` and `replica.reset` the way
//! `packages/client` does (windows keyed by subscription id; a transaction
//! delete removes the entity from every window of that entity) and verifies
//! every `replica.ready` / delta digest against its own rows, as the SDK does.
//! Only public protocol types are used, so the same file builds against any
//! runtime revision.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use gonvex_protocol::replica_digest::ReplicaSetDigest;
use gonvex_protocol::{ReplicaCursor, ReplicaOpenRequest, ServerMessage};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestKind {
    /// The JS SDK: `xor1:` aggregate.
    Xor,
    /// Negotiated `sum128-v1:` (native clients).
    Sum,
}

#[derive(Clone, Debug)]
pub struct Window {
    pub path: String,
    pub entity: String,
    pub key: String,
    pub args: Value,
    pub rows: BTreeMap<String, Value>,
    pub cursor: Option<ReplicaCursor>,
    pub truncated: bool,
    /// Set by `replica.needHashes`: the next open sends keys and hashes.
    pub needs_hashes: bool,
    /// Set by `replica.reset`: the next open is cold.
    pub needs_reopen: bool,
}

#[derive(Default)]
pub struct ClientModel {
    pub windows: BTreeMap<String, Window>,
    pub digest_kind: Option<DigestKind>,
    /// Digest mismatches observed on `replica.ready` (the SDK would reset).
    pub mismatches: Vec<String>,
    pub need_hashes: usize,
    pub resets: usize,
    /// `replicaOmitNulls`: `receive` round-trips every message through its
    /// wire text and restores omitted null columns as the SDK does
    /// (`restoreReplicaFrameNulls`) before applying it.
    pub omit_nulls: bool,
    /// Columns learned from the frames answering each open.
    pub columns: BTreeMap<String, Vec<String>>,
    /// Null members restored (omitted on the wire).
    pub restored_members: usize,
    /// Rows that arrived without some column.
    pub restored_rows: usize,
}

pub fn stable_json(value: &Value) -> String {
    let mut out = String::new();
    write_stable(&mut out, value);
    out
}

fn write_stable(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                out.push_str(&value.to_string());
            } else if let Some(value) = value.as_u64() {
                out.push_str(&value.to_string());
            } else if let Some(value) = value.as_f64() {
                out.push_str(ryu_js::Buffer::new().format(value));
            } else {
                out.push_str("null");
            }
        }
        Value::String(value) => {
            let encoded = serde_json::to_string(value).unwrap();
            out.push_str(
                &encoded
                    .replace('\u{2028}', "\\u2028")
                    .replace('\u{2029}', "\\u2029"),
            );
        }
        Value::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_stable(out, value);
            }
            out.push(']');
        }
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_stable(out, &Value::String(key.clone()));
                out.push(':');
                write_stable(out, &values[key]);
            }
            out.push('}');
        }
    }
}

pub fn row_hash(row: &Value) -> String {
    Sha256::digest(stable_json(row).as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn key_of(row: &Value, key: &str) -> String {
    match row.get(key) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        other => panic!("row without key {key}: {other:?}"),
    }
}

impl Window {
    pub fn hashes(&self) -> BTreeMap<String, String> {
        self.rows
            .iter()
            .map(|(key, row)| (key.clone(), row_hash(row)))
            .collect()
    }

    pub fn digest(&self, kind: Option<DigestKind>) -> String {
        let hashes = self.hashes();
        match kind {
            Some(DigestKind::Sum) => {
                let mut state = ReplicaSetDigest::default();
                for hash in hashes.values() {
                    state.add(hash).unwrap();
                }
                state.digest()
            }
            Some(DigestKind::Xor) => xor_digest(hashes.values()),
            None => {
                let pairs = hashes
                    .iter()
                    .map(|(key, hash)| json!([key, hash]))
                    .collect::<Vec<_>>();
                Sha256::digest(stable_json(&Value::Array(pairs)).as_bytes())
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect()
            }
        }
    }
}

pub fn xor_digest<'a>(hashes: impl Iterator<Item = &'a String>) -> String {
    let mut aggregate = [0u8; 32];
    for hash in hashes {
        for (index, byte) in aggregate.iter_mut().enumerate() {
            *byte ^= u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16).unwrap();
        }
    }
    format!(
        "xor1:{}",
        aggregate
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn digest_matches(window: &Window, kind: Option<DigestKind>, digest: &str) -> bool {
    // The SDK picks the verification from the server digest's prefix.
    let kind = if digest.starts_with("xor1:") {
        Some(DigestKind::Xor)
    } else if digest.starts_with("sum128-v1:") {
        Some(DigestKind::Sum)
    } else if kind.is_some() {
        None
    } else {
        kind
    };
    window.digest(kind) == digest
}

impl ClientModel {
    pub fn new(digest_kind: Option<DigestKind>) -> Self {
        Self {
            digest_kind,
            ..Self::default()
        }
    }

    /// The open request for subscription `id` of collection `path`: cold,
    /// a cursor + aggregate resume, or (after `replica.needHashes`) a full
    /// integrity open with keys and hashes.
    pub fn open_request(&self, id: &str, path: &str, entity_args: &Value) -> ReplicaOpenRequest {
        let empty_digest = match self.digest_kind {
            Some(DigestKind::Sum) => Some(ReplicaSetDigest::default().digest()),
            Some(DigestKind::Xor) => Some(xor_digest(std::iter::empty())),
            None => None,
        };
        let mut request = ReplicaOpenRequest {
            id: id.into(),
            path: path.into(),
            args: entity_args.clone(),
            cursor: None,
            keys: Vec::new(),
            hashes: BTreeMap::new(),
            digest: empty_digest,
            full_integrity: false,
        };
        let Some(window) = self.windows.get(id).filter(|window| !window.needs_reopen) else {
            return request;
        };
        request.cursor = window.cursor.clone();
        if self.digest_kind.is_some() {
            request.digest = Some(window.digest(self.digest_kind));
        }
        if window.needs_hashes || self.digest_kind.is_none() {
            request.keys = window.rows.keys().cloned().collect();
            request.hashes = window.hashes();
            request.full_integrity = true;
        }
        request
    }

    pub fn register(&mut self, id: &str, path: &str, entity: &str, args: Value) {
        self.windows.entry(id.to_owned()).or_insert_with(|| Window {
            path: path.into(),
            entity: entity.into(),
            key: "id".into(),
            args,
            rows: BTreeMap::new(),
            cursor: None,
            truncated: false,
            needs_hashes: false,
            needs_reopen: false,
        });
    }

    pub fn apply_all<'a>(&mut self, messages: impl IntoIterator<Item = &'a ServerMessage>) {
        for message in messages {
            if self.omit_nulls {
                self.receive(message);
            } else {
                self.apply(message);
            }
        }
    }

    /// `replicaOmitNulls`: the message as the client parses it from the wire,
    /// with omitted nulls restored, then applied.
    pub fn receive(&mut self, message: &ServerMessage) {
        let text = serde_json::to_string(message).unwrap();
        let mut message: ServerMessage = serde_json::from_str(&text).unwrap();
        let columns_key = gonvex_protocol::omit_nulls::COLUMNS;
        let learn = |columns: &mut BTreeMap<String, Vec<String>>, id: &str, value: Option<Value>| {
            if let Some(Value::Array(names)) = value {
                columns.insert(
                    id.to_owned(),
                    names.iter().map(|name| name.as_str().unwrap().to_owned()).collect(),
                );
            }
        };
        let (restored_members, restored_rows) = (&mut self.restored_members, &mut self.restored_rows);
        let mut restore = |row: &Value, columns: Option<&Vec<String>>| -> Value {
            let Some(columns) = columns else { return row.clone() };
            let restored = gonvex_protocol::omit_nulls::restore(row, columns);
            let missing = columns.len().saturating_sub(row.as_object().map_or(0, |o| o.len()));
            if missing > 0 {
                *restored_members += missing;
                *restored_rows += 1;
            }
            restored
        };
        match &mut message {
            ServerMessage::ReplicaSnapshot { id, result, metadata, .. } => {
                learn(&mut self.columns, id, metadata.remove(columns_key));
                let columns = self.columns.get(id.as_str());
                for row in result.iter_mut() {
                    *row = gonvex_protocol::JsonRow::from_value(&restore(&row.to_value(), columns));
                }
            }
            ServerMessage::ReplicaDelta { id, payload, .. } => {
                learn(&mut self.columns, id, payload.remove(columns_key));
                let columns = self.columns.get(id.as_str());
                if let Some(Value::Array(rows)) = payload.get_mut("upserts") {
                    for row in rows.iter_mut() {
                        *row = restore(row, columns);
                    }
                }
            }
            ServerMessage::ReplicaReady { id, metadata, .. } => {
                learn(&mut self.columns, id, metadata.remove(columns_key));
            }
            _ => {}
        }
        self.apply(&message);
    }

    pub fn apply(&mut self, message: &ServerMessage) {
        match message {
            ServerMessage::ReplicaSnapshot {
                id,
                result,
                cursor,
                key,
                metadata,
            } => {
                let window = self.windows.get_mut(id).expect("registered window");
                window.key = key.clone();
                window.rows = result
                    .iter()
                    .map(|row| { let row = row.to_value(); (key_of(&row, key), row) })
                    .collect();
                window.cursor = Some(cursor.clone());
                window.truncated = metadata.get("truncated") == Some(&Value::Bool(true));
                window.needs_hashes = false;
                window.needs_reopen = false;
            }
            ServerMessage::ReplicaDelta {
                id,
                cursor,
                payload,
            } => {
                let window = self.windows.get_mut(id).expect("registered window");
                for row in payload
                    .get("upserts")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    window.rows.insert(key_of(row, &window.key), row.clone());
                }
                for key in payload
                    .get("deleted")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    window.rows.remove(key.as_str().unwrap());
                }
                if let Some(truncated) = payload.get("truncated").and_then(Value::as_bool) {
                    window.truncated = truncated;
                }
                window.cursor = Some(cursor.clone());
                window.needs_hashes = false;
                window.needs_reopen = false;
                if let Some(digest) = payload.get("digest").and_then(Value::as_str) {
                    if !digest_matches(window, self.digest_kind, digest) {
                        self.mismatches.push(format!("delta {id} at {cursor:?}"));
                    }
                }
            }
            ServerMessage::ReplicaReady {
                id, cursor, digest, ..
            } => {
                let window = self.windows.get_mut(id).expect("registered window");
                window.cursor = Some(cursor.clone());
                window.needs_hashes = false;
                window.needs_reopen = false;
                if !digest_matches(window, self.digest_kind, digest) {
                    self.mismatches.push(format!("ready {id} at {cursor:?}"));
                }
            }
            ServerMessage::ReplicaNeedHashes { id, .. } => {
                self.need_hashes += 1;
                self.windows
                    .get_mut(id)
                    .expect("registered window")
                    .needs_hashes = true;
            }
            ServerMessage::ReplicaReset { id, .. } => {
                self.resets += 1;
                let window = self.windows.get_mut(id).expect("registered window");
                window.rows.clear();
                window.cursor = None;
                window.needs_reopen = true;
            }
            ServerMessage::ReplicaTransaction { changes, .. } => {
                // The SDK removes a deleted entity from every window of that
                // entity; collection deltas that follow restore memberships.
                let deleted = changes
                    .iter()
                    .filter(|change| change.operation == "delete")
                    .map(|change| (change.entity.clone(), change.id.clone()))
                    .collect::<BTreeSet<_>>();
                for window in self.windows.values_mut() {
                    window
                        .rows
                        .retain(|key, _| !deleted.contains(&(window.entity.clone(), key.clone())));
                }
            }
            _ => {}
        }
    }

    /// The connection-level `replica.watermark` after a feed event: every
    /// settled window is current through `revision` (the SDK checkpoints
    /// ready windows to the watermark).
    pub fn watermark(&mut self, revision: u64) {
        for window in self.windows.values_mut() {
            if window.needs_hashes || window.needs_reopen {
                continue;
            }
            if let Some(cursor) = &mut window.cursor {
                cursor.revision = cursor.revision.max(revision);
            }
        }
    }

    /// Windows that must be reopened (needHashes or reset), by id.
    pub fn pending_reopens(&self) -> Vec<String> {
        self.windows
            .iter()
            .filter(|(_, window)| window.needs_hashes || window.needs_reopen)
            .map(|(id, _)| id.clone())
            .collect()
    }
}
