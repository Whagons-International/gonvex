# Gonvex data guarantees program

## Objective and completion criteria

Establish evidence-backed confidence in critical data guarantees across the Rust
runtime and shared web/React Native SDK. Develop against staging, preserve APIs,
keep database/transport implementations replaceable, and measure changes in
resource-capped local sandboxes. Deployment is a separate authorized action.

Storage scope is native SQLite through the app SDK and both browser IndexedDB
adapters. Dedicated OPFS and SQLite WASM verification is excluded by the owner.

A guarantee is established only when its specification, assumptions, production
implementation, proof/model boundary, negative controls, adapter failure/restart
evidence and CI enforcement are recorded. Proof counts are not coverage claims.
Document unresolved gaps rather than calling a subsystem fully verified.

## Guarantee register

| ID | Intended guarantee | Current evidence and remaining work |
| --- | --- | --- |
| MUT-1 | A successful authenticated reducer corresponds to its committed tenant transaction. | Rust rollback/aborted-commit regressions and transactional receipt tests exist. Whole invocation/receipt protocol remains unproved. PostgreSQL durability, tenant routing and effects staying inside the transaction are assumptions. |
| MUT-2 | Retries of one logical intent do not repeat its transactional business effects. | Receipt claim/result and writes share a transaction. Finite protocol model plus eight passing native replay/rollback/competing-claim tests, with three failing baseline regressions, are recorded in reducer-receipts.md. No whole-source refinement proof. Namespace is tenant database + account subject + intent key. Path reuse is checked; argument equivalence is a caller obligation. Anonymous reducers/actions are excluded. Receipt retention is not promised as permanent. |
| SDK-1 | Accepted queue entries cannot be re-armed; parked entries require explicit retry. | Source-linked five-state/seven-action proof covers delivery, recovery and settlement, including accepted-state preservation. Public adapter regressions cover metadata, single/batch acceptance and missing records; SQLite checks cover adapter reopen. Deletion, callback identity, adapter atomicity and complete SDK delivery remain outside the proof. See outbox-delivery.md. |
| SDK-2 | Durable acknowledgements and local-intent retirement cannot pass an unapplied transaction. | Staging fences failed data lanes and retains capable-runtime intents through their watermark. The finite protocol model, public adapter regressions, real Chromium crash/abort/snapshot scenarios and Android/Hermes shared-handle/process-restart checks provide evidence for selected failures; see replica-durability.md, indexeddb-browser.md and sqlite-transactions.md. No complete protocol proof, iOS or native-phone evidence yet. |
| SYNC-1 | Duplicate/stale frames cannot undo newer authorized data; reconnect eventually catches up under explicit progress assumptions. | Revision primitive proofs and storage/lifecycle regressions exist. Full merge, snapshot/epoch transitions and recovery protocol remain unproved. |
| AUTH-1 | Old account/tenant/subscription operations cannot publish into a new owner. | Lifecycle regression coverage exists. Need source-linked ownership transitions and tests across every async boundary. Authorization-policy enforcement is separate from callback ownership. |
| RES-1 | Resource permits, deadlines and credits remain correctly owned through cancellation and cleanup. | Smaller Rust policy/credit proofs exist. Need composition with actual lifetime/cleanup paths and failure tests. |
| VIS-1 | Visibility feed checks are pure; ordinary data writes cause no visibility SQL or full snapshots; permission work is scoped and shared per affected member. | SDK/CLI schema guards, evaluator boundary checks, counters and a PostgreSQL fan-out fixture exist. Integrated budget measurements and a source-extracted Verus proof remain pending; see visibility-masks.md. No formal guarantee claimed. |
| UPG-1 | Supported persisted/protocol versions recover without silently changing ordering or completeness. | Adapter/revision-upgrade tests exist. Need an explicit compatibility matrix, migration contracts and older-state restart evidence. |

## First end-to-end milestone

Trace one durable local intent through SDK enqueue, server transaction/receipt,
replica persistence, watermark barrier and queue retirement. Define separate
safety and progress assumptions. Check independent schedules including lost
responses, competing delivery, storage failure, disconnect, account change and
restart. Connect protocol transitions to production operations and test fixtures.

The existing TypeScript translator proves revision expressions only. The new
delivery translator proves a closed state transition. Neither verifies arbitrary
JavaScript, storage engines, the event loop or the complete SDK. Protocol models
must state their domains/bounds and demonstrate implementation correspondence.

## Source map, inspected staging c9eed7ce

- `packages/client/src/index.ts`: local execution admission awaits durable intent
  enqueue before prediction; `drainOutbox` and `deliverOutboxBatch` retain command
  identity; `registerCall` uses the negotiated replica watermark barrier;
  `handleReplicaWatermark` advances the processed watermark after storage.
- `packages/client/src/outbox.ts` and `packages/expo-sqlite/src/outbox.ts`: queue
  storage, inflight recovery and retirement. Local execution requires durable
  admission. Ordinary non-strict queues may fall back to session-only memory.
- `rust/crates/runtime/src/execution.rs`: authenticated reducer receipt claim and
  replay; `host_calls.rs` owns success/rollback disposition.
- `rust/crates/postgres/src/lib.rs`: `_gonvex_reducer_idempotency` is keyed by
  `(subject, idempotency_key)` within the tenant database. Result and business
  writes commit together. Replay checks path, not argument equality.
- `packages/client/src/local-replica.ts`: transaction data and watermark metadata
  persist before in-memory publication on their individual success paths.

## Unresolved boundaries and next experiments

1. Extend iOS app SQLite and other browser-engine crash/restart coverage.
   Android/Hermes now covers shared-handle ownership and intent/replica process
   recovery. Chromium covers both IndexedDB adapters, including native aborts
   and preserved intent identity. Native encoded server snapshots are a separate
   protocol extension; this shared SDK does not negotiate that capability.
2. Extend receipt coverage to tenant routing and deployment restart boundaries.
   A current head can exceed the original commit revision. Native tests and the
   finite model establish selected schedules under the stated database/epoch
   assumptions, not exact historical attribution or permanent retention.
3. Extend the settlement state contract to callback identity, deletion and
   coalescing races. The seven-action proof covers state decisions on existing
   records; it does not establish every mutation's async/storage behavior.
4. Add PostgreSQL process-crash/restart evidence tied to the declared receipt
   namespace. Native competing-claim tests already cover commit and rollback.
   Use isolated fixtures without live staging traffic or production load.
5. Check server-native encoded snapshot ordering separately from the current
   SDK protocol. An installed cursor alone does not establish chunk delivery;
   current paged SDK consumers defer interleaved watermarks during staging.

Backend fixtures require PostgreSQL's documented transactional behavior and a
known durable configuration. Node SQLite tests do not establish Expo/Hermes
behavior. The selected Android/Hermes checks do not establish iOS or physical
phone filesystem behavior. Record those gaps separately.
