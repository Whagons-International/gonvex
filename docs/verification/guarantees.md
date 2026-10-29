# Gonvex data guarantees program

## Objective and completion criteria

Establish evidence-backed confidence in critical data guarantees across the Rust
runtime and shared web/React Native SDK. Develop against staging, preserve APIs,
keep database/transport implementations replaceable, and measure changes in
resource-capped local sandboxes. Deployment is a separate authorized action.

A guarantee is established only when its specification, assumptions, production
implementation, proof/model boundary, negative controls, adapter failure/restart
evidence and CI enforcement are recorded. Proof counts are not coverage claims.
Document unresolved gaps rather than calling a subsystem fully verified.

## Guarantee register

| ID | Intended guarantee | Current evidence and remaining work |
| --- | --- | --- |
| MUT-1 | A successful authenticated reducer corresponds to its committed tenant transaction. | Rust rollback/aborted-commit regressions and transactional receipt tests exist. Whole invocation/receipt protocol remains unproved. PostgreSQL durability, tenant routing and effects staying inside the transaction are assumptions. |
| MUT-2 | Retries of one logical intent do not repeat its transactional business effects. | Receipt claim/result and writes share a transaction. Prove the protocol and test competing/lost-response retries. Namespace is tenant database + account subject + command key. Path reuse is checked; argument equivalence is a caller obligation. Anonymous reducers/actions are excluded. Receipt retention is not promised as permanent. |
| SDK-1 | Delivery and recovery do not revive committed or parked queue entries. | Source-linked finite-state proof and public queue regressions in this tranche. Explicit retry, commit, rejection, deletion, adapter atomicity and complete SDK delivery remain outside the proof. See outbox-delivery.md. |
| SDK-2 | Durable acknowledgements and local-intent retirement cannot pass an unapplied transaction. | Success-path and metadata-write-failure regressions exist. Failed data frame followed by watermark is a high-priority unconfirmed source candidate. No complete protocol proof yet. |
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

1. Reproduce failed `storage.applyTransaction` followed by a queued watermark.
   `enqueueReplicaFrame` currently catches an error and fulfills the frame lane
   after changing freshness. Determine whether later watermark success can
   retire work whose data was never saved. Preserve supported retry of a failed
   metadata-only watermark. Extend the real public fixture in `index.test.ts`.
2. Reconcile long-offline replay. `command_revision` reads the change feed, whose
   records are pruned after 30 days, while no receipt pruning path was found.
   Test replay when the receipt remains but its original feed revision is gone.
   An observed implementation detail is not a permanent retention guarantee.
3. Specify explicit commit/reject/retry race semantics. The delivery proof does
   not establish the behavior of every mutation of an outbox record.
4. Add crash/restart and competing-claim PostgreSQL tests tied to the declared
   receipt namespace, with no live staging traffic or production load.

Backend fixtures require PostgreSQL's documented transactional behavior and a
known durable configuration. Node SQLite tests do not establish Expo/Hermes or
phone filesystem behavior. Record native-device gaps separately.
