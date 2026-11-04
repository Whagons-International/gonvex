//! Negotiated `replicaOmitNulls` (version 1): Replica Collection row images
//! on the wire may leave out top-level columns whose value is `null`. See
//! `docs/performance/omit-nulls.md` for the contract.
//!
//! Logical messages, row hashes and digests are unchanged: every hash is still
//! SHA-256 of the canonical *full* projection (nulls included). Omission is a
//! last step of encoding for one connection, and the client restores each
//! omitted column as `null` (in the collection's column order) before it
//! hashes, stores or publishes the row. A sender may omit any subset of the
//! null members (omitting none is valid), so every function here may leave a
//! row as it is when it cannot prove a member is null.
//!
//! What is omitted:
//! - `replica.snapshot` / `replica.snapshotPage` `result` rows and
//!   `replica.delta` `upserts` rows: every top-level member whose value is
//!   `null`;
//! - `replica.transaction` changes with `operation: "update"`: members of
//!   `newValue` whose value is `null` and whose column is not in
//!   `changedColumns` (the client merges `newValue` into the entity it already
//!   holds, where such a column is already `null`). A column that *became*
//!   null stays explicit. Inserts, deletes and `oldValue` are unchanged.
//!
//! The client learns a subscription's columns from `columns`, which the
//! server adds to every message that answers a `replica.open` (snapshot,
//! the first snapshot page, an open's delta and its ready). Feed messages do
//! not repeat it.

use serde_json::Value;

use crate::{JsonRow, ServerMessage};

/// Metadata / payload key carrying a subscription's projected columns.
pub const COLUMNS: &str = "columns";

/// `text` (one JSON object, as SQLite's `json_object` or serde_json writes
/// it) without its top-level members whose value is `null`. None when it has
/// none, or when `text` is not an object this scanner can walk (the caller
/// then sends it unchanged, which is always valid).
pub fn strip_null_members(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut at = skip_ws(bytes, 0);
    if bytes.get(at) != Some(&b'{') {
        return None;
    }
    at += 1;
    // Byte ranges (member start .. value end) of kept members, and whether any
    // member was dropped.
    let mut kept: Vec<(usize, usize)> = Vec::new();
    let mut dropped = false;
    at = skip_ws(bytes, at);
    if bytes.get(at) == Some(&b'}') {
        return None;
    }
    loop {
        let start = at;
        if bytes.get(at) != Some(&b'"') {
            return None;
        }
        at = skip_string(bytes, at)?;
        at = skip_ws(bytes, at);
        if bytes.get(at) != Some(&b':') {
            return None;
        }
        at = skip_ws(bytes, at + 1);
        let value = at;
        at = skip_value(bytes, at, 0)?;
        if &bytes[value..at] == b"null" {
            dropped = true;
        } else {
            kept.push((start, at));
        }
        at = skip_ws(bytes, at);
        match bytes.get(at) {
            Some(b',') => at = skip_ws(bytes, at + 1),
            Some(b'}') => {
                if skip_ws(bytes, at + 1) != bytes.len() {
                    return None;
                }
                break;
            }
            _ => return None,
        }
    }
    if !dropped {
        return None;
    }
    let mut out = String::with_capacity(kept.iter().map(|(s, e)| e - s + 1).sum::<usize>() + 2);
    out.push('{');
    for (index, (start, end)) in kept.into_iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&text[start..end]);
    }
    out.push('}');
    Some(out)
}

fn skip_ws(bytes: &[u8], mut at: usize) -> usize {
    while matches!(bytes.get(at), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        at += 1;
    }
    at
}

/// `at` is an opening quote; returns the index after the closing quote.
fn skip_string(bytes: &[u8], mut at: usize) -> Option<usize> {
    at += 1;
    loop {
        // memchr-free: rows are short and mostly ASCII.
        match bytes.get(at)? {
            b'"' => return Some(at + 1),
            b'\\' => at += 2,
            _ => at += 1,
        }
    }
}

const MAX_DEPTH: usize = 128;

fn skip_value(bytes: &[u8], at: usize, depth: usize) -> Option<usize> {
    match bytes.get(at)? {
        b'"' => skip_string(bytes, at),
        b'{' | b'[' => {
            if depth >= MAX_DEPTH {
                return None;
            }
            let close = if bytes[at] == b'{' { b'}' } else { b']' };
            let mut at = skip_ws(bytes, at + 1);
            if bytes.get(at) == Some(&close) {
                return Some(at + 1);
            }
            loop {
                if close == b'}' {
                    if bytes.get(at) != Some(&b'"') {
                        return None;
                    }
                    at = skip_ws(bytes, skip_string(bytes, at)?);
                    if bytes.get(at) != Some(&b':') {
                        return None;
                    }
                    at = skip_ws(bytes, at + 1);
                }
                at = skip_ws(bytes, skip_value(bytes, at, depth + 1)?);
                match bytes.get(at)? {
                    b',' => at = skip_ws(bytes, at + 1),
                    byte if *byte == close => return Some(at + 1),
                    _ => return None,
                }
            }
        }
        _ => {
            // Number or literal: up to the next structural byte.
            let start = at;
            let mut at = at;
            while let Some(byte) = bytes.get(at) {
                if matches!(byte, b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t') {
                    break;
                }
                at += 1;
            }
            (at > start).then_some(at)
        }
    }
}

/// A row object without its null members (in place, order kept).
pub fn strip_value(row: &mut Value) {
    if let Value::Object(object) = row {
        object.retain(|_, value| !value.is_null());
    }
}

/// A raw row without its null members (shares the text when there are none).
pub fn strip_json_row(row: &JsonRow) -> JsonRow {
    match strip_null_members(row.get()) {
        // Removing whole members of a valid object keeps it valid.
        Some(text) => JsonRow::from_json(text).unwrap_or_else(|_| row.clone()),
        None => row.clone(),
    }
}

/// Applies the omission to the rows of one outgoing message (see the
/// module documentation). Other messages are unchanged.
pub fn omit_nulls(message: &mut ServerMessage) {
    match message {
        ServerMessage::ReplicaSnapshot { result, .. }
        | ServerMessage::ReplicaSnapshotPage { result, .. } => {
            for row in result.iter_mut() {
                if let Some(text) = strip_null_members(row.get()) {
                    if let Ok(stripped) = JsonRow::from_json(text) {
                        *row = stripped;
                    }
                }
            }
        }
        ServerMessage::ReplicaDelta { payload, .. } => {
            if let Some(Value::Array(rows)) = payload.get_mut("upserts") {
                rows.iter_mut().for_each(strip_value);
            }
        }
        ServerMessage::ReplicaTransaction { changes, .. } => {
            for change in changes {
                // Unknown changed columns ([]) keep every null explicit.
                if change.operation != "update" || change.changed_columns.is_empty() {
                    continue;
                }
                if let Some(Value::Object(row)) = &mut change.new_value {
                    let changed = &change.changed_columns;
                    row.retain(|column, value| {
                        !value.is_null() || changed.iter().any(|name| name == column)
                    });
                }
            }
        }
        _ => {}
    }
}

/// Whether `message` carries rows that `omit_nulls` may change.
pub fn carries_rows(message: &ServerMessage) -> bool {
    matches!(
        message,
        ServerMessage::ReplicaSnapshot { .. }
            | ServerMessage::ReplicaSnapshotPage { .. }
            | ServerMessage::ReplicaDelta { .. }
            | ServerMessage::ReplicaTransaction { .. }
    )
}

/// Adds the subscription's `columns` to a message answering `replica.open`
/// (snapshot, delta, ready). Other messages are unchanged.
pub fn add_columns(message: &mut ServerMessage, columns: &[String]) {
    let value = || Value::Array(columns.iter().cloned().map(Value::String).collect());
    match message {
        ServerMessage::ReplicaSnapshot { metadata, .. }
        | ServerMessage::ReplicaSnapshotPage { metadata, .. }
        | ServerMessage::ReplicaReady { metadata, .. } => {
            metadata.insert(COLUMNS.to_owned(), value());
        }
        ServerMessage::ReplicaDelta { payload, .. } => {
            payload.insert(COLUMNS.to_owned(), value());
        }
        _ => {}
    }
}

/// The client's restoration (reference implementation for tests): `row`
/// with every column of `columns` it lacks set to `null`, in column order;
/// members not in `columns` follow in their order.
pub fn restore(row: &Value, columns: &[String]) -> Value {
    let Value::Object(object) = row else {
        return row.clone();
    };
    if columns.iter().all(|column| object.contains_key(column)) {
        return row.clone();
    }
    let mut out = serde_json::Map::new();
    for column in columns {
        out.insert(
            column.clone(),
            object.get(column).cloned().unwrap_or(Value::Null),
        );
    }
    for (key, value) in object {
        if !out.contains_key(key) {
            out.insert(key.clone(), value.clone());
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn columns(row: &Value) -> Vec<String> {
        row.as_object().unwrap().keys().cloned().collect()
    }

    #[test]
    fn raw_strip_matches_value_strip_and_restores_exactly() {
        for text in [
            r#"{"id":"a","x":null}"#,
            r#"{"x":null,"id":"a"}"#,
            r#"{"x":null}"#,
            r#"{"a":null,"b":null,"c":null}"#,
            r#"{"id":1,"s":"null","n":null,"t":"a\"null\\","j":{"k":null,"l":[null,1,{"m":null}]},"z":-1.5e3,"b":true,"f":false,"e":""}"#,
            r#"{ "id" : 1 , "n" : null , "m" : [ ] , "o" : { } }"#,
            "{\"id\":\"東京😀\",\"u\":\"\\u2028\",\"n\":null}",
        ] {
            let value: Value = serde_json::from_str(text).unwrap();
            let stripped = strip_null_members(text).expect("has nulls");
            let mut expected = value.clone();
            strip_value(&mut expected);
            assert_eq!(serde_json::from_str::<Value>(&stripped).unwrap(), expected, "{text}");
            // Nested nulls stay.
            assert_eq!(
                serde_json::to_string(&restore(&expected, &columns(&value))).unwrap(),
                serde_json::to_string(&value).unwrap(),
            );
        }
    }

    #[test]
    fn rows_without_nulls_or_unparseable_rows_are_left_alone() {
        for text in [
            r#"{"id":"a"}"#,
            r#"{}"#,
            r#"{"j":{"k":null}}"#,
            r#"[null]"#,
            r#"null"#,
            r#"{"id":"a","x":null"#,
            r#"{"id":"a","x":nul}x"#,
        ] {
            assert!(strip_null_members(text).is_none(), "{text}");
        }
    }

    #[test]
    fn update_changes_keep_changed_nulls_and_inserts_keep_all() {
        let mut message: ServerMessage = serde_json::from_value(json!({
            "type":"replica.transaction","cursor":{"epoch":"e","revision":2},
            "changes":[
                {"entity":"tasks","id":"a","operation":"update","newValue":{"id":"a","n":null,"cleared":null,"v":1},"changedColumns":["cleared","v"]},
                {"entity":"tasks","id":"b","operation":"insert","newValue":{"id":"b","n":null},"changedColumns":[]},
                {"entity":"tasks","id":"c","operation":"update","newValue":{"id":"c","n":null},"changedColumns":[]}
            ]
        }))
        .unwrap();
        omit_nulls(&mut message);
        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(value["changes"][0]["newValue"], json!({"id":"a","cleared":null,"v":1}));
        assert_eq!(value["changes"][1]["newValue"], json!({"id":"b","n":null}));
        assert_eq!(value["changes"][2]["newValue"], json!({"id":"c","n":null}));
    }

    #[test]
    fn snapshot_and_delta_rows_and_columns() {
        let row = json!({"id":"a","n":null,"v":2});
        let mut snapshot = ServerMessage::ReplicaSnapshot {
            id: "s".into(),
            result: vec![JsonRow::from_value(&row)],
            cursor: crate::ReplicaCursor {
                epoch: "e".into(),
                revision: 1,
            },
            key: "id".into(),
            metadata: Default::default(),
        };
        omit_nulls(&mut snapshot);
        add_columns(&mut snapshot, &columns(&row));
        assert_eq!(
            serde_json::to_string(&snapshot).unwrap(),
            r#"{"type":"replica.snapshot","id":"s","result":[{"id":"a","v":2}],"cursor":{"epoch":"e","revision":1},"key":"id","columns":["id","n","v"]}"#
        );
        let mut delta: ServerMessage = serde_json::from_value(json!({
            "type":"replica.delta","id":"s","cursor":{"epoch":"e","revision":2},
            "upserts":[row.clone()],"deleted":[]
        }))
        .unwrap();
        omit_nulls(&mut delta);
        assert_eq!(
            serde_json::to_value(&delta).unwrap()["upserts"],
            json!([{"id":"a","v":2}])
        );
    }
}
