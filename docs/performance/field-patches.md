# Field patches for replica updates

The first implementation is opt-in. It reduces repeated row transmission while preserving the existing logical transaction, collection, storage and integrity contracts. It adds no production dependencies and keeps JSON/permessage-deflate.

```ts
const client = new GonvexClient(url, {
  ...auth,
  replicaFieldPatches: true,
});
```

Omit the option or set it to `false` for the existing protocol. Both peers must advertise `replicaFieldPatches: 1`. Missing or unknown client versions use the existing full-row messages. Native canonical row delivery takes precedence. This feature does not change initial snapshots or structured live query results.

## Why the patch base lives in the connection

A durable normalized row can combine several projections at different revisions and can change through another tab. Treating that row's transaction cursor as the version of every field would be incorrect. The new [Rust encoder](../../rust/crates/protocol/src/row_patch.rs) and [browser decoder](../../packages/client/src/row-patch.ts) instead own a bounded dictionary of wire projections. They reconstruct complete logical messages before the [existing SDK dispatcher](../../packages/client/src/index.ts) applies them.

Dictionary versions identify exact transmitted values. They are distinct from database revisions and never become durable cursors or field authority. Snapshot activation, record tombstones, optimistic reconciliation, multi-tab arbitration and commit ordering remain in their existing modules. The normalized row and all storage adapters receive the same complete projections as before.

For example, a later transaction can use this envelope:

```json
{
  "type": "replica.rowPatch",
  "sequence": 3,
  "message": {
    "type": "replica.transaction",
    "cursor": { "epoch": "database-epoch", "revision": 3 },
    "changes": [{ "entity": "tasks", "id": "a", "operation": "update" }]
  },
  "rows": [{
    "index": 0, "base": 1, "version": 3,
    "set": { "status": "done", "assignee": null },
    "unset": ["temporary"]
  }],
  "evicted": [1]
}
```

`set` replaces complete top-level field values. Nested objects and arrays remain ordinary JSON values. Null is a value, while `unset` removes a field from the cached wire projection. This reconstructs the full `newValue` or collection upsert. An unset does not grant authority to erase a field supplied by another collection; the existing projection merge rules still apply. Record deletion remains the existing delete operation.

The envelope's row indexes replace object `newValue` slots in transactions or rebuild the complete ordered `upserts` array in a delta. Transaction bases are keyed by entity and record ID. Collection bases use subscription ID and upsert slot, so the transport does not need an application's primary-key schema. Replacing one row with a different row in a slot still reconstructs the exact new object. Sparse or changing batches may have less reuse.

## Ordering, recovery and memory

Every envelope has a strictly increasing connection sequence. Every retained row receives a new safe-integer version. A patch references a present exact base; duplicate and older envelopes are ignored, while a gap, malformed frame or missing base fails closed. All rows and dictionary budgets are validated before publishing the logical message or changing the dictionary. Frozen base values prevent a consumer from mutating subsequent decoding.

On failure, the SDK stops accepting the connection's queued mutations and watermarks, closes with browser-supported application code `4002`, and reconnects without advertising field patches for the rest of that client instance. Existing durable cursors drive the existing complete-row replay or snapshot repair. A successful auth starts a fresh dictionary even when subscriptions are preserved. A new socket also starts empty. Tenant-session revocation clears the server dictionary. Previous-tenant bases cannot be used after a scope switch.

The dictionary retains at most 4 MiB of serialized row values and 4,096 entries per connection. These are opt-in safety limits, not measured optimal cache sizes or heap limits. The tested 1,000-row working set retains about 786 KB across the transaction and collection paths. The larger cap permits larger records and working sets. Rust and JS object overhead, key indexes, temporary encodings and the current logical message add memory beyond the serialized-value budget. Oversized rows remain complete and uncached. Server evictions are explicit, and the browser independently checks its budget. Budget inconsistencies also fall back to the legacy connection.

Snapshots do not seed this dictionary. The first live update for a dictionary entry sends a complete value. Reconnects pay that cost again. A workload dominated by first-time updates may gain little or use more bandwidth.

## Compression and measured trade-offs

The encoder builds a full-row envelope and, when useful, a field-patch alternative. [The WebSocket transport](../../rust/crates/websocket/src/lib.rs) compares their actual encoded payload sizes under the negotiated threshold and window. It sends the selected DEFLATE result directly rather than compressing it again. Plain connections compare raw bytes. A cancellation guard prevents an alternative from being accidentally reused for an unrelated message.

This comparison can retain a compressed full envelope when a shorter raw patch would cost more on the wire. It compares the two new envelopes, not the legacy format; cold envelope metadata can still cost more than a legacy complete-row message. Comparing candidates and maintaining bases also costs CPU.

The following measurements include ten successive updates, each with a transaction, collection delta, ready and watermark. Values alternate between statuses, and database revisions advance. The first complete dictionary fill is included. Native permessage-deflate uses level 1, a 12-bit server window, a 1,024-byte threshold and no context takeover.

| Workload | Existing WS bytes | Patched WS bytes | Saving | First update, existing / patched | Rust encode + compress, existing / patched ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Small row | 14,098 | 9,694 | 31.2% | 1,416 / 1,616 | 0.015 / 0.067 |
| 16 KiB row, one changed field | 183,108 | 26,533 | 85.5% | 18,319 / 18,455 | 1.174 / 1.433 |
| 100 rows | 261,040 | 63,447 | 75.7% | 26,127 / 27,828 | 2.458 / 7.338 |
| 1,000 rows | 2,504,111 | 565,050 | 77.4% | 250,571 / 266,408 | 42.999 / 89.651 |

For the large row, the nine warm updates together used 8,078 bytes instead of 164,789, a 95.1% saving. The first update was slightly larger. The Rust CPU measurement includes cold cache allocation, field comparison, both JSON candidates and both compression candidates. Input construction, SQL/feed work and dropping old feed images are excluded. It is not a claim that backend CPU improves. The benchmark's counting allocator also adds instrumentation overhead.

Peak additional live Rust allocation in the 1,000-row encode/compress measurement increased from about 1.16 MB to 6.08 MB. That includes dictionary state and temporary candidates, not the runtime's entire heap. The browser's decoder also does more work than native `JSON.parse` alone. Its cache budget is not a claim of lower peak browser memory.

After the builds finished, a separate 100-trial large-row repeat measured these ten-update stream medians:

| Storage | Existing p50 ms | Patched p50 ms |
| --- | ---: | ---: |
| Memory | 10.42 | 6.27 |
| IndexedDB API commits | 41.47 | 38.10 |

The full runs were mixed for other workloads. Small IndexedDB updates changed from 31.11 to 34.48 ms in the final run. The 1,000-row IndexedDB comparison reversed between runs, with only three trials per format. No general large-batch or storage-latency gain is claimed. This supports opt-in evaluation for repeated updates to large projections, not enabling patches by default.

[The complete tables](field-patches-2026-10-24/summary.md), [raw results](field-patches-2026-10-24/browser.json), [the earlier run](field-patches-2026-10-24/initial-browser.json), [the large-row repeat](field-patches-2026-10-24/large-repeat.json) and [provenance](field-patches-2026-10-24/provenance.json) include the unfavorable cases. Actual TCP-counted server WebSocket bytes match the independent Rust matrix for every case.

These are single-client synthetic loopback trials on a shared workstation. Each stream waits for a benchmark-only application ACK after the previous update is applied, keeping the measured stream bounded. This ACK is not a production protocol change. Latencies are for all ten updates, not one update or saturated throughput. The browser uses real LocalReplica and native IndexedDB, with fresh databases and contexts. SQL/feed processing, permissions, SDK dispatch/digest verification, reconnect/auth handshakes, TLS and WAN are excluded. IndexedDB measures API commits in isolated contexts, not physical persistent-profile disk flush or crash durability. Some full-run cases overlapped builds or tests. Heap peaks are sampled lower bounds, and CPU/GC totals include untimed setup/cleanup. Small and large single-row cases have 100 trials; smaller samples support medians only, with raw percentiles retained for inspection.

## Validation and reproduction

The [shared contract fixture](../../rust/crates/protocol/fixtures/row-patch-v1.json) is generated by the Rust encoder and checked by both Rust and TypeScript tests. Tests cover metadata, null, removal, nested values, signed zero, prototype-looking field names, safe versions, duplicates, sequence gaps, missing bases, eviction, uncached large rows, auth renewal and tenant switches. A seeded 10,000-update test compares materialized objects and exact browser cache-byte accounting. An IndexedDB test compares durable results with the complete-message path through reload and tombstones, including overlapping projection fields. These are tests and an equivalence contract, not formal verification of the entire protocol.

The full client suite and OPFS checks, workspace type checks, package tarballs, example code generators, docs metadata, Rust protocol/WebSocket/runtime tests with PostgreSQL, strict Clippy and formatting pass. Runtime cycles remain zero relative to staging. See [benchmark commands](../../scripts/bench-websocket/README.md) to reproduce the streams and regenerate the fixture.
