//! Delivery v2 oracle: for every member of a Whagons-shaped tenant, the
//! `replica.batch` frames (shared, memoized, compressed-once items) expand to
//! exactly the frames the connection's own visibility-filtered apply produced,
//! in order. No connection receives another's rows; none misses its own.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test delivery_oracle`
use std::collections::HashMap;

use flate2::{Decompress, FlushDecompress};
use gonvex_protocol::ServerMessage;
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::delivery::{perf_legacy_encode, BatchConfig, DeliveryHub, ReplicaBatcher};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};
use gonvex_websocket::OutboundBatch;
use serde_json::{json, Value};

fn inflate(body: &[u8]) -> Vec<u8> {
    let mut input = body.to_vec();
    input.extend_from_slice(&[0, 0, 255, 255]);
    let mut codec = Decompress::new_with_window_bits(false, 12);
    let mut out = Vec::with_capacity(body.len() * 8 + 1024);
    loop {
        let consumed = codec.total_in() as usize;
        if out.capacity() == out.len() {
            out.reserve(out.len());
        }
        codec
            .decompress_vec(&input[consumed..], &mut out, FlushDecompress::Sync)
            .unwrap();
        if codec.total_in() as usize == input.len() && out.len() < out.capacity() {
            return out;
        }
    }
}

/// The client rule (`expandReplicaBatch`): items in order, stream tokens
/// replaced by every bound subscription id, watermark last.
fn expand(batch: &Value, bindings: &mut HashMap<String, Vec<String>>) -> (Vec<Value>, Option<u64>) {
    assert_eq!(batch["type"], "replica.batch");
    let mut out = Vec::new();
    for item in batch["items"].as_array().unwrap() {
        match item["type"].as_str().unwrap() {
            "replica.stream" => {
                let (id, stream) = (
                    item["id"].as_str().unwrap(),
                    item["stream"].as_str().unwrap(),
                );
                for ids in bindings.values_mut() {
                    ids.retain(|bound| bound != id);
                }
                bindings
                    .entry(stream.to_owned())
                    .or_default()
                    .push(id.to_owned());
            }
            "replica.delta" | "replica.ready" => {
                let ids = bindings
                    .get(item["id"].as_str().unwrap())
                    .cloned()
                    .unwrap_or_default();
                assert!(!ids.is_empty(), "item for an unbound stream: {item}");
                for id in ids {
                    let mut item = item.clone();
                    item["id"] = json!(id);
                    out.push(item);
                }
            }
            _ => out.push(item.clone()),
        }
    }
    (out, batch["watermark"].as_u64())
}

fn revision(event: &FeedEvent) -> u64 {
    match event {
        FeedEvent::Transaction { revision, .. } => *revision,
        FeedEvent::Reset { .. } => 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_shared_frames_equal_each_connections_own_frames() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let tenant_id = fixture.tenants[0].route.tenant_id.clone();
    let tasks = fixture.tenants[0].tasks.clone();
    let members = fixture.tenants[0].members.len();
    let mut connections = fixture.connect_all(0).await.unwrap();
    for native in [false, true] {
        let hub = DeliveryHub::default();
        let mut batchers: Vec<_> = (0..members)
            .map(|_| {
                let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
                batcher.set_enabled(true);
                (batcher, OutboundBatch::default(), HashMap::new())
            })
            .collect();
        let mut shared_items = 0u64;
        for round in 0..12usize {
            // 1-3 commits per tick: edits, spot and workspace moves (rows enter
            // and leave members' visible sets) and presence beats.
            let mut events = Vec::new();
            for step in 0..(round % 3) + 1 {
                let i = round * 7 + step * 13;
                let task = i % tasks.len();
                let other = &tasks[(i * 31 + 5) % tasks.len()];
                let event = match (round + step) % 4 {
                    0 => fixture
                        .update_task(
                            0,
                            task,
                            json!({"description": format!("oracle {round} {step}")}),
                        )
                        .await
                        .unwrap(),
                    1 => fixture
                        .update_task(0, task, json!({"spotId": other.spot_id.clone()}))
                        .await
                        .unwrap(),
                    2 => fixture
                        .update_task(0, task, json!({"workspaceId": other.workspace_id.clone()}))
                        .await
                        .unwrap(),
                    _ => fixture.beat_presence(0, i % members).await.unwrap(),
                };
                events.push(event);
            }
            let mut expected: Vec<Vec<Value>> = vec![Vec::new(); members];
            // What each client receives: expanded batch items, plus frames
            // sent outside a batch (which flush the pending batch first).
            let mut received: Vec<Vec<Value>> = vec![Vec::new(); members];
            let mut unbatched = 0usize;
            for event in &events {
                for (index, connection) in connections.iter_mut().enumerate() {
                    let messages = fixture
                        .runtime
                        .apply_feed_event(
                            &connection.session,
                            &mut connection.subscriptions,
                            event.clone(),
                        )
                        .await
                        .unwrap();
                    let (batcher, batch, bindings) = &mut batchers[index];
                    for message in messages {
                        let mut legacy = message.clone();
                        // The old path: per-connection frames, compact (no
                        // old values) or native, ids are subscription ids.
                        if let ServerMessage::ReplicaTransaction { changes, .. } = &mut legacy {
                            for change in changes {
                                change.old_value = None;
                            }
                        }
                        let text = perf_legacy_encode(&mut legacy, native, None).unwrap();
                        expected[index].push(serde_json::from_str(&text).unwrap());
                        let rest = batcher
                            .push(
                                batch,
                                &hub,
                                &tenant_id,
                                native,
                                &connection.subscriptions,
                                message,
                            )
                            .unwrap();
                        if let Some(mut other) = rest {
                            // Sent the usual way (e.g. replica.needHashes):
                            // the pending batch goes first.
                            unbatched += 1;
                            if let Some(frame) = batcher.take_frame(batch, None) {
                                let value: Value = serde_json::from_slice(&frame.bytes).unwrap();
                                received[index].extend(expand(&value, bindings).0);
                            }
                            let text = perf_legacy_encode(&mut other, native, None).unwrap();
                            received[index].push(serde_json::from_str(&text).unwrap());
                        }
                    }
                    batcher.watermark(
                        batch,
                        &tenant_id,
                        &connection.subscriptions,
                        revision(event),
                    );
                }
            }
            let last = revision(events.last().unwrap());
            for (index, (batcher, batch, bindings)) in batchers.iter_mut().enumerate() {
                let mut items = std::mem::take(&mut received[index]);
                let compressed = (round + index) % 2 == 0;
                let deflate = compressed.then_some((12, 0));
                let frame = batcher.take_frame(batch, deflate).unwrap();
                assert_eq!(frame.compressed, compressed);
                let text = if compressed {
                    inflate(&frame.bytes)
                } else {
                    frame.bytes.to_vec()
                };
                let batch: Value = serde_json::from_slice(&text).unwrap();
                assert_eq!(batch["tenant"], json!(tenant_id));
                let (tail, watermark) = expand(&batch, bindings);
                items.extend(tail);
                assert_eq!(watermark, Some(last));
                assert_eq!(
                    items, expected[index],
                    "member {index} round {round} native={native}: batch differs from its own frames"
                );
                shared_items += items.len() as u64;
            }
            if unbatched > 0 {
                eprintln!("round {round}: {unbatched} frames sent outside a batch");
            }
        }
        let stats = hub.tenant(&tenant_id).stats();
        assert!(shared_items > 0);
        // Equal frames were encoded once and reused.
        assert!(stats.hits > 0, "{stats:?}");
        assert!(
            stats.encodes + stats.private_encodes < stats.hits + stats.encodes,
            "{stats:?}"
        );
    }
}
