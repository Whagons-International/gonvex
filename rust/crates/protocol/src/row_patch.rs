//! Negotiated, connection-local row compression. Decoding restores the existing
//! logical messages; dictionary versions are never durable replica cursors.
use crate::{PublicInvocationProvenance, ReplicaCursor, ServerMessage};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{self, Write};

// The 1,000-row benchmark retains ~786 KB across both wire paths. This
// opt-in safety cap leaves headroom for larger records; it is not a measured
// optimum. It bounds serialized row bytes, not allocator or JS object overhead.
pub const ROW_PATCH_BYTES: usize = 4 * 1024 * 1024;
pub const ROW_PATCH_ENTRIES: usize = 4096;

struct Entry {
    version: u64,
    bytes: usize,
    value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn signed_zero_changes_are_not_elided() {
        assert!(!wire_equal(&json!(-0.0), &json!(0.0)));
        assert!(!wire_equal(
            &json!({"nested":[-0.0]}),
            &json!({"nested":[0.0]})
        ));
        let mut encoder = RowPatchEncoder::default();
        encoder
            .prepare(&transaction(
                1,
                "a",
                json!({"id":"a","long":"x".repeat(200),"zero":-0.0}),
            ))
            .unwrap();
        let (_, patch) = encoder
            .prepare(&transaction(
                2,
                "a",
                json!({"id":"a","long":"x".repeat(200),"zero":0.0}),
            ))
            .unwrap()
            .unwrap();
        let patch: Value = serde_json::from_str(&patch.unwrap()).unwrap();
        assert_eq!(
            patch["rows"][0]["set"]["zero"].as_f64().unwrap().to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn shared_typescript_contract_stays_current() {
        let fixture: Value =
            serde_json::from_str(include_str!("../fixtures/row-patch-v1.json")).unwrap();
        let mut encoder = RowPatchEncoder::default();
        for case in fixture["cases"].as_array().unwrap() {
            let message: ServerMessage = serde_json::from_value(case["logical"].clone()).unwrap();
            let (full, patch) = encoder.prepare(&message).unwrap().unwrap();
            assert_eq!(serde_json::from_str::<Value>(&full).unwrap(), case["full"]);
            assert_eq!(
                patch
                    .map(|text| serde_json::from_str::<Value>(&text).unwrap())
                    .unwrap_or(Value::Null),
                case["patch"]
            );
        }
    }

    fn transaction(revision: u64, id: &str, value: Value) -> ServerMessage {
        serde_json::from_value(json!({"type":"replica.transaction","cursor":{"epoch":"e","revision":revision},
            "originCommandId":"command", "changes":[{"entity":"tasks","id":id,"operation":"update", "newValue":value}]})).unwrap()
    }

    fn apply(frame: &str, cache: &mut BTreeMap<u64, Value>) -> Value {
        let frame: Value = serde_json::from_str(frame).unwrap();
        let mut message = frame["message"].clone();
        for row in frame["rows"].as_array().unwrap() {
            let value = if row.get("value").is_some() {
                row["value"].clone()
            } else {
                let mut value = cache[&row["base"].as_u64().unwrap()].clone();
                for key in row["unset"].as_array().into_iter().flatten() {
                    value.as_object_mut().unwrap().remove(key.as_str().unwrap());
                }
                for (key, field) in row["set"].as_object().unwrap() {
                    value[key] = field.clone();
                }
                value
            };
            if let Some(version) = row["version"].as_u64() {
                cache.insert(version, value.clone());
            }
            let index = row["index"].as_u64().unwrap() as usize;
            if message["type"] == "replica.transaction" {
                message["changes"][index]["newValue"] = value;
            } else {
                message["upserts"].as_array_mut().unwrap().push(value);
            }
        }
        for version in frame["evicted"].as_array().unwrap() {
            cache.remove(&version.as_u64().unwrap());
        }
        message
    }

    #[test]
    fn alternatives_reconstruct_identical_rows_metadata_nulls_and_deletions() {
        let mut encoder = RowPatchEncoder::default();
        let mut full_cache = BTreeMap::new();
        let mut patch_cache = BTreeMap::new();
        for revision in 1..101 {
            let mut value = json!({"id":"a","description":"東京😀".repeat(100),"nullable":null,"nested":{"value":revision},"__proto__":"data"});
            if revision % 2 == 0 {
                value["optional"] = json!(true);
            }
            let message = transaction(revision, "a", value);
            let expected = serde_json::to_value(&message).unwrap();
            let (full, patch) = encoder.prepare(&message).unwrap().unwrap();
            assert_eq!(apply(&full, &mut full_cache), expected);
            assert_eq!(
                apply(patch.as_deref().unwrap_or(&full), &mut patch_cache),
                expected
            );
            assert_eq!(full_cache, patch_cache);
            assert_eq!(encoder.entries.len(), 1);
            if revision > 1 {
                assert!(patch.is_some());
            }
        }
        encoder.clear();
        let (full, patch) = encoder
            .prepare(&transaction(101, "a", json!({"id":"a"})))
            .unwrap()
            .unwrap();
        assert!(patch.is_none());
        assert_eq!(serde_json::from_str::<Value>(&full).unwrap()["sequence"], 1);
    }

    #[test]
    fn slots_round_trip_different_primary_keys_and_preserve_delta_metadata() {
        let mut encoder = RowPatchEncoder::default();
        let mut cache = BTreeMap::new();
        for revision in 1..4 {
            let message: ServerMessage = serde_json::from_value(json!({"type":"replica.delta","id":"sub","cursor":{"epoch":"e","revision":revision},
                "upserts":[{"customKey":revision,"description":"x".repeat(1000),"status":revision}],"deleted":["gone"],"digest":"digest","originCommandIds":["command"]})).unwrap();
            let (full, patch) = encoder.prepare(&message).unwrap().unwrap();
            assert_eq!(
                apply(patch.as_deref().unwrap_or(&full), &mut cache),
                serde_json::to_value(message).unwrap()
            );
        }
    }

    #[test]
    fn cache_limits_evict_old_versions_and_large_rows_stay_uncached() {
        let mut encoder = RowPatchEncoder::default();
        let mut cache = BTreeMap::new();
        for revision in 1..=ROW_PATCH_ENTRIES + 1 {
            let message = transaction(
                revision as u64,
                &revision.to_string(),
                json!({"id":revision}),
            );
            let (full, _) = encoder.prepare(&message).unwrap().unwrap();
            apply(&full, &mut cache);
            assert!(encoder.entries.len() <= ROW_PATCH_ENTRIES);
            assert!(encoder.retained_bytes() <= ROW_PATCH_BYTES);
        }
        assert_eq!(cache.len(), ROW_PATCH_ENTRIES);
        assert_eq!(encoder.order.len(), ROW_PATCH_ENTRIES);
        let message = transaction(
            9000,
            "large",
            json!({"id":"large","text":"x".repeat(ROW_PATCH_BYTES)}),
        );
        let retained = encoder.retained_bytes();
        let (full, patch) = encoder.prepare(&message).unwrap().unwrap();
        assert_eq!(
            apply(&full, &mut cache),
            serde_json::to_value(message).unwrap()
        );
        assert!(patch.is_none());
        assert_eq!(encoder.retained_bytes(), retained);
    }
}

#[derive(Default)]
pub struct RowPatchEncoder {
    entries: BTreeMap<String, Entry>,
    bytes: usize,
    order: BTreeMap<u64, String>,
    sequence: u64,
    version: u64,
}

#[derive(Serialize, Clone)]
struct WireRow<'a> {
    index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    set: Option<BTreeMap<&'a str, &'a Value>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unset: Vec<String>,
}

#[derive(Serialize)]
struct ChangeMetadata<'a> {
    entity: &'a str,
    id: &'a str,
    operation: &'a str,
    #[serde(rename = "newValue", skip_serializing_if = "Option::is_none")]
    new_value: Option<&'a Value>,
    #[serde(rename = "changedColumns", skip_serializing_if = "Vec::is_empty")]
    changed_columns: &'a Vec<String>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum Skeleton<'a> {
    #[serde(rename = "replica.transaction")]
    Transaction {
        cursor: &'a ReplicaCursor,
        #[serde(rename = "originCommandId", skip_serializing_if = "Option::is_none")]
        origin_command_id: Option<&'a String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        provenance: Option<&'a PublicInvocationProvenance>,
        changes: Vec<ChangeMetadata<'a>>,
    },
    #[serde(rename = "replica.delta")]
    Delta {
        id: &'a str,
        cursor: &'a ReplicaCursor,
        upserts: [u8; 0],
        #[serde(flatten)]
        payload: BTreeMap<&'a String, &'a Value>,
    },
}

#[derive(Serialize)]
struct Envelope<'a, 'b> {
    r#type: &'static str,
    sequence: u64,
    message: &'b Skeleton<'a>,
    rows: &'b [WireRow<'a>],
    evicted: &'b [u64],
}

struct ByteCount(usize);
impl Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn json_size(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    let mut writer = ByteCount(0);
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}

// serde_json's numeric equality treats -0.0 and +0.0 as equal, while
// JSON.parse and Object.is distinguish them. Preserve that wire distinction,
// including inside nested arrays and objects.
fn wire_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => {
            a == b
                && (a.as_f64() != Some(0.0)
                    || a.as_f64().unwrap().is_sign_negative()
                        == b.as_f64().unwrap().is_sign_negative())
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| wire_equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| wire_equal(a, b)))
        }
        _ => left == right,
    }
}

impl RowPatchEncoder {
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }

    /// Returns a complete-row envelope and an optional field-patch alternative.
    /// Both produce exactly the same dictionary and logical message. Transport
    /// selects using actual negotiated compressed sizes, reusing that encoding.
    pub fn prepare(
        &mut self,
        message: &ServerMessage,
    ) -> Result<Option<(String, Option<String>)>, serde_json::Error> {
        let mut values = Vec::new();
        let logical = match message {
            ServerMessage::ReplicaTransaction {
                cursor,
                changes,
                origin_command_id,
                provenance,
            } => {
                let mut metadata = Vec::with_capacity(changes.len());
                for (index, change) in changes.iter().enumerate() {
                    let object = change.new_value.as_ref().is_some_and(Value::is_object);
                    if object {
                        values.push((
                            index,
                            serde_json::to_string(&("t", &change.entity, &change.id))?,
                            change.new_value.as_ref().unwrap(),
                        ));
                    }
                    metadata.push(ChangeMetadata {
                        entity: &change.entity,
                        id: &change.id,
                        operation: &change.operation,
                        new_value: if object {
                            None
                        } else {
                            change.new_value.as_ref()
                        },
                        changed_columns: &change.changed_columns,
                    });
                }
                Skeleton::Transaction {
                    cursor,
                    changes: metadata,
                    origin_command_id: origin_command_id.as_ref(),
                    provenance: provenance.as_ref(),
                }
            }
            ServerMessage::ReplicaDelta {
                id,
                cursor,
                payload,
            } => {
                let Some(upserts) = payload.get("upserts").and_then(Value::as_array) else {
                    return Ok(None);
                };
                if !upserts.iter().all(Value::is_object) {
                    return Ok(None);
                }
                // Slot keys need no knowledge of an application's primary key.
                // A different row in the same slot is still an exact diff.
                for (index, value) in upserts.iter().enumerate() {
                    values.push((index, serde_json::to_string(&("d", id, index))?, value));
                }
                Skeleton::Delta {
                    id,
                    cursor,
                    upserts: [],
                    payload: payload
                        .iter()
                        .filter(|(key, _)| key.as_str() != "upserts")
                        .collect(),
                }
            }
            _ => return Ok(None),
        };
        if self.sequence >= 9_007_199_254_740_991
            || self.version > 9_007_199_254_740_991 - values.len() as u64
        {
            return Err(serde_json::Error::io(std::io::Error::other(
                "row patch version exhausted",
            )));
        }
        self.sequence += 1;
        let mut full_rows = Vec::new();
        let mut patch_rows = Vec::new();
        let mut evicted = Vec::new();
        let mut patched = false;
        for (index, key, value) in values {
            self.version += 1;
            let bytes = json_size(value)?;
            let version = (bytes <= ROW_PATCH_BYTES).then_some(self.version);
            let full = WireRow {
                index,
                version,
                value: Some(value),
                base: None,
                set: None,
                unset: Vec::new(),
            };
            let previous = self.entries.remove(&key);
            let mut selected = full.clone();
            if let Some(prior) = previous {
                self.bytes -= prior.bytes;
                self.order.remove(&prior.version);
                evicted.push(prior.version);
                if version.is_some() {
                    let old = prior.value.as_object().unwrap();
                    let new = value.as_object().unwrap();
                    let set = new
                        .iter()
                        .filter(|(key, value)| {
                            !old.get(*key).is_some_and(|old| wire_equal(old, value))
                        })
                        .map(|(key, value)| (key.as_str(), value))
                        .collect();
                    let unset = old
                        .keys()
                        .filter(|key| !new.contains_key(*key))
                        .cloned()
                        .collect();
                    let patch = WireRow {
                        index,
                        version,
                        value: None,
                        base: Some(prior.version),
                        set: Some(set),
                        unset,
                    };
                    // The full row's size is already counted for the cache budget.
                    // Avoid encoding it into a temporary buffer just to compare.
                    let metadata = WireRow {
                        value: None,
                        ..full.clone()
                    };
                    if json_size(&patch)? < bytes + json_size(&metadata)? + 9 {
                        selected = patch;
                        patched = true;
                    }
                }
            }
            full_rows.push(full);
            patch_rows.push(selected);
            if let Some(version) = version {
                self.bytes += bytes;
                self.order.insert(version, key.clone());
                self.entries.insert(
                    key,
                    Entry {
                        version,
                        bytes,
                        value: value.clone(),
                    },
                );
            }
            while self.bytes > ROW_PATCH_BYTES || self.entries.len() > ROW_PATCH_ENTRIES {
                let (_, key) = self.order.pop_first().unwrap();
                let entry = self.entries.remove(&key).unwrap();
                self.bytes -= entry.bytes;
                evicted.push(entry.version);
            }
        }
        let encode = |rows: &[WireRow<'_>]| {
            serde_json::to_string(&Envelope {
                r#type: "replica.rowPatch",
                sequence: self.sequence,
                message: &logical,
                rows,
                evicted: &evicted,
            })
        };
        Ok(Some((
            encode(&full_rows)?,
            patched.then(|| encode(&patch_rows)).transpose()?,
        )))
    }
}
