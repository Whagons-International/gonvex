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
| MUT-2 | Retries of one logical intent do not repeat its transactional business effects. | Receipt claim/result and writes share a transaction. Finite protocol model and required native replay/competing-claim fixtures are described in reducer-receipts.md; native results must be recorded before claiming the implementation verified. Namespace is tenant database + account subject + intent key. Path reuse is checked; argument equivalence is a caller obligation. Anonymous reducers/actions are excluded. Receipt retention is not promised as permanent. |
| SDK-1 | Delivery and recovery do not revive committed or parked queue entries. | Source-linked finite-state proof and public queue regressions in this tranche. Explicit retry, commit, rejection, deletion, adapter atomicity and complete SDK delivery remain outside the proof. See outbox-delivery.md. |
| SDK-2 | Durable acknowledgements and local-intent retirement cannot pass an unapplied transaction. | Staging now fences failed data lanes before later watermarks and retains capable-runtime patchless intents until their barrier. Public adapter schedules and a finite protocol model cover the fixed failures; see replica-durability.md. Server snapshot delivery ordering and the complete protocol remain unproved. |
| SYNC-1 | Duplicate/stale frames cannot undo newer authorized data; reconnect eventually catches up under explicit progress assumptions. | Revision primitive proofs and storage/lifecycle regressions exist. Full merge, snapshot/epoch transitions and recovery protocol remain unproved. |
| AUTH-1 | Old account/tenant/subscription operations cannot publish into a new owner. | Lifecycle regression coverage exists. Need source-linked ownership transitions and tests across every async boundary. Authorization-policy enforcement is separate from callback ownership. |
| RES-1 | Resource permits, deadlines and credits remain correctly owned through cancellation and cleanup. | Smaller Rust policy/credit proofs exist. Need composition with actual lifetime/cleanup paths and failure tests. |
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

1. Check server snapshot delivery ordering before terminal call watermarks.
   An installed cursor does not itself establish that every snapshot chunk has
   been delivered. Reproduce the pending native snapshot schedule.
2. Validate the conservative replay clock barrier against native baselines and
   fixed-source tests. A current head can exceed the original commit revision;
   prove sufficiency under the stated database/epoch assumptions rather than
   promising exact historical attribution or permanent receipt retention.
3. Specify explicit commit/reject/retry race semantics. The delivery proof does
   not establish the behavior of every mutation of an outbox record.
4. Add crash/restart and competing-claim PostgreSQL tests tied to the declared
   receipt namespace, with no live staging traffic or production load.

Backend fixtures require PostgreSQL's documented transactional behavior and a
known durable configuration. Node SQLite tests do not establish Expo/Hermes or
phone filesystem behavior. Record native-device gaps separately.
