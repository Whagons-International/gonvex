//! JSON transaction preparation. Keep this independent of transport and routing.
use crate::ServerMessage;
use std::io::{self, Write};

/// Encode a negotiated compact transaction, reusing the resulting JSON on the
/// wire. At a compression threshold crossing, retain the legacy image: a larger
/// compressed message can be smaller than a compact message sent uncompressed.
/// A `None` threshold means the connection cannot compress. Other message
/// types return `Ok(None)`.
pub fn encode_compact_replica_transaction(
    message: &mut ServerMessage,
    compression_threshold: Option<usize>,
) -> Result<Option<String>, serde_json::Error> {
    let ServerMessage::ReplicaTransaction { changes, .. } = message else {
        return Ok(None);
    };
    // Move rather than clone row images. Preserve them only until the threshold
    // decision is made. Large compact frames never need an old-image size pass.
    let old_values: Vec<_> = changes
        .iter_mut()
        .map(|change| change.old_value.take())
        .collect();
    let compact = serde_json::to_string(message)?;
    if let Some(threshold) = compression_threshold.filter(|threshold| compact.len() < *threshold) {
        let mut count = ByteCount(compact.len());
        for old in old_values.iter().flatten() {
            count.0 += ",\"oldValue\":".len();
            serde_json::to_writer(&mut count, old)?;
            if count.0 >= threshold {
                break;
            }
        }
        if count.0 >= threshold {
            if let ServerMessage::ReplicaTransaction { changes, .. } = message {
                for (change, old) in changes.iter_mut().zip(old_values) {
                    change.old_value = old;
                }
            }
            return serde_json::to_string(message).map(Some);
        }
    }
    Ok(Some(compact))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn transaction(text: &str) -> ServerMessage {
        serde_json::from_value(json!({
            "type":"replica.transaction", "cursor":{"epoch":"epoch","revision":9},
            "originCommandId":"command", "provenance":{"rootCommandId":"command","channel":"ui"},
            "changes":[
                {"entity":"tasks","id":"a","operation":"update","oldValue":{"id":"a","text":text,"value":1},"newValue":{"id":"a","text":text,"value":null},"changedColumns":["value"]},
                {"entity":"tasks","id":"b","operation":"delete","oldValue":{"id":"b","value":2}},
                {"entity":"tasks","id":"c","operation":"insert","newValue":{"id":"c","value":3}}
            ]
        })).unwrap()
    }

    #[test]
    fn omission_preserves_nulls_deletes_and_commit_metadata() {
        let mut message = transaction("café 東京\n\"\\");
        let legacy = serde_json::to_value(&message).unwrap();
        let mut expected = legacy.clone();
        for change in expected["changes"].as_array_mut().unwrap() {
            change.as_object_mut().unwrap().remove("oldValue");
        }
        let wire = encode_compact_replica_transaction(&mut message, None)
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&wire).unwrap(), expected);
        assert!(legacy["changes"][0].get("oldValue").is_some());
        assert_eq!(
            encode_compact_replica_transaction(&mut message, None)
                .unwrap()
                .unwrap(),
            wire
        );
        assert!(encode_compact_replica_transaction(
            &mut ServerMessage::ReplicaWatermark { revision: 9 },
            Some(1024)
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn exact_utf8_and_escape_threshold_crossings_keep_the_legacy_frame() {
        for text in [
            "medium".repeat(60),
            "\"\\\n".repeat(60),
            "東京😀".repeat(60),
        ] {
            let original = transaction(&text);
            let legacy = serde_json::to_string(&original).unwrap();
            let compact = encode_compact_replica_transaction(&mut original.clone(), None)
                .unwrap()
                .unwrap();
            assert!(compact.len() < legacy.len());
            for threshold in [compact.len() + 1, legacy.len()] {
                let mut candidate = original.clone();
                assert_eq!(
                    encode_compact_replica_transaction(&mut candidate, Some(threshold))
                        .unwrap()
                        .unwrap(),
                    legacy
                );
                assert_eq!(candidate, original);
            }
            for threshold in [0, compact.len(), legacy.len() + 1] {
                assert_eq!(
                    encode_compact_replica_transaction(&mut original.clone(), Some(threshold))
                        .unwrap()
                        .unwrap(),
                    compact
                );
            }
        }
    }
}
