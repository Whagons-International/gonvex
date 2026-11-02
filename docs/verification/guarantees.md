# Gonvex data guarantees program

## Objective and completion criteria

Make Gonvex's authenticated mutation and replica pipeline preserve committed
data, intent identity and account isolation through concurrent retries, lost
responses, interrupted writes, process restarts, reconnects and supported SDK
upgrades. Establish the eight contracts below in production code, with
source-linked proofs for their critical state decisions and fault-injection
checks for their storage and asynchronous composition. Work against staging.

The goal is complete only when every contract below has its required evidence
and enforcement. A green PR, additional theorem count or passing isolated helper
does not establish the end-to-end contract.

Storage scope is native SQLite through the app SDK and both browser IndexedDB
adapters. Dedicated OPFS and SQLite WASM verification is excluded by the owner.

## Required outcomes

1. **MUT-1, commit and rollback.** A successful authenticated reducer result
   corresponds to a committed transaction in the correct tenant database.
   Business writes and its receipt commit together. Execution failure, aborted
   commit and cancellation before commit cannot leave a successful receipt or
   partial business effects.
2. **MUT-2, retry identity and effects.** Within the declared receipt namespace
   and retention contract, concurrent delivery, lost replies and restart/retry
   of the same logical intent produce at most one committed transactional
   business effect. Retries preserve the key and arguments. Receipt replay
   retains a sufficient synchronization barrier after transport-ID changes and
   feed pruning. Anonymous calls, Actions and external effects need their own
   contracts before receiving the same claim.
3. **SDK-1, intent lifecycle.** Once acceptance is known, bookkeeping cannot
   make an intent sendable again. Parked work requires explicit retry.
   Recovery and supported migrations preserve IDs, keys, arguments, owner and
   causal order. Late callbacks, discard and coalescing cannot overwrite or
   delete a different logical intent.
4. **SDK-2, durable acknowledgement.** Rows, memberships, tombstones and the
   checkpoint describe one committed post-image. The SDK cannot publish success,
   advance an acknowledgement or retire an intent past unapplied data.
   Failed writes and interrupted snapshot promotion retain a recoverable prior
   checkpoint. Restart cannot expose a checkpoint ahead of its persisted data.
5. **SYNC-1, ordered recovery.** Duplicate and stale frames cannot overwrite
   newer authorized values or resurrect deleted rows. Snapshot/delta overlap,
   reconnect and epoch changes preserve this invariant. Catch-up progress must
   have explicit transport, retention and scheduling assumptions, with recovery
   schedules that demonstrate progress under those assumptions.
6. **AUTH-1, ownership.** Account, tenant and subscription changes fence stale
   asynchronous work before it can publish into a new owner. This includes
   storage completion, reducer settlement, snapshot promotion, queued replay
   and token refresh. Backend receipt lookup and transaction execution must use
   the authenticated tenant/subject namespace. Callback fencing alone is not
   evidence of backend authorization.
7. **RES-1, resource ownership.** Cancellation, timeout, disconnect and cleanup
   cannot leak or double-release invocation permits, snapshot credits or queue
   ownership. Admission cannot exceed the configured capacity. Verify both the
   accounting decisions and their actual lifetime/cleanup paths.
8. **UPG-1, compatibility.** An explicit supported persisted/protocol version
   matrix defines what can reopen or migrate. Supported upgrades preserve
   intent identity, ordering and replica completeness. Unsupported or corrupt
   state fails explicitly rather than silently discarding accepted work or
   manufacturing a later checkpoint.

## Evidence required to finish

For every outcome, record its precise preconditions, production entry points,
state transitions, remaining trusted components and failure behavior. Trace a
durable local intent through the real SDK, Rust module host, PostgreSQL receipt,
replica persistence and final intent retirement. Composed execution must cover
lost replies, competing attempts, rollback, write failure, disconnect, account
change, process restart and supported upgrade boundaries.

Use machine-checked proofs of actual production decisions where the verifier
supports them. Use protocol/state models for interleavings and document their
finite bounds and correspondence to source. Do not substitute a copied model
for a source proof or a helper proof for the whole protocol. Deliberately broken
transitions must produce the intended counterexamples; tool errors and skipped
tests do not count as negative controls.

Run real PostgreSQL/backend tests, both browser IndexedDB adapters in real
browsers, and the app SDK against native SQLite. Record the tested platform and
version matrix. Missing evidence for a supported platform remains an open item;
Android emulator evidence alone cannot close iOS or physical-device claims.
State the filesystem, database, retention and scheduling assumptions explicitly.
Do not claim machine power-loss durability from process-restart tests.

Require the applicable checks in CI and bind evidence to the actual source.
Fix every reproduced violation and retain its regression. Finish only after an
independent requirement-by-requirement audit finds no missing evidence, known
violation or unimplemented part of these eight contracts. Unsupported cases
must be explicit product-contract exclusions, not quietly treated as verified.

## CI enforcement inspection

The runtime workflow's final `CI` job runs even after failures and requires
runtime tests, policy proofs and native-storage checks to succeed. The receipt
initialization step explicitly selects its five named native cases, requires
PostgreSQL and the module host, and rejects ignored or missing execution. Both
configured Mergify queues require `CI` before merge.

Staging branch protection was verified on 2026-11-02. It requires the aggregate
`CI` check from the GitHub Actions app and applies to administrators. No
reviewer-approval gate was added. The required aggregate checks runtime tests,
policy proofs and native-storage verification. The configured Mergify staging
queue tests a batch against the latest base before merging. GitHub's strict
up-to-date-branch setting is disabled because that queue tests a separate batch
PR rather than rewriting each original branch. Required CI remains enforced.
The configuration and API readback trust GitHub and Mergify to apply these
policies. Published head `afc8112c` passed CI, including the Android historical SQLite
schedules. Queue admission, successful batch CI and the remaining contract
audit are still required; later PR heads require their own green checks. Local checks alone do not close those items.

The query callback checkpoint adds public-listener ownership checks, a durable
published cache, ordered publication and independent revision reset for each
owner. Its 27 SDK schedules, nine compiled controls and 70 actual Chromium
storage schedules cover selected SDK-2, SYNC-1 and AUTH-1 boundaries. The
[query publication audit](ownership.md#query-and-replica-callback-publication)
describes the failing baselines, source correspondence and native transaction
abort. Platform and published-head CI requirements remain open.

## Execution order

PR creation, queue monitoring and merging are delivery work at the end of the
substantive goal work. They are not milestones for these guarantees and must
not replace implementation, proofs or fault-injection experiments. Keep the
working results and evidence locally while closing the remaining contracts.
Deployment requires separate authorization. Preserve public APIs and replaceable
storage/transport interfaces; a language rewrite is not a prerequisite.

The module-upgrade coordinator now also combines durable received acceptance
with a native IndexedDB counter failure. It witnesses a rejected replica data
write, unchanged raw records, no acknowledgement or retirement attempt, and a
retained accepted intent. Restoring only the injected counter record permits
backend replacement and fresh-browser receipt recovery without another ordinary
delivery of that intent. A compiled reducer-result guard omission fails at the
early-acknowledgement assertion. These schedules strengthen SDK-1, SDK-2 and
UPG-1 for this failure boundary; they do not close the remaining platform or
asynchronous protocol audit. See [the composition register](composed-sdk.md).

A separate four-adapter baseline exposed a storage completion from an old
tenant advancing the replacement tenant's processed watermark. The replacement
reducer incorrectly succeeded before its durable cursor reached the commit.
`persistReplicaWatermark` now rechecks captured scope, socket and receipt
activation after storage completes. Eight schedules cover tenant replacement
and return to the same scope. A second four-adapter baseline found an old ready
write restoring its epoch onto a reused subscription and stalling the new
tenant's accepted reducer. That publication now checks its captured subscription
and owner identities too. Seven source-linked publication laws, six solver
controls, a readiness AST bridge and three compiled runtime controls cover these
decisions and their placement.
See [ownership after storage completion](opening-watermarks.md#ownership-after-storage-completion).

## Guarantee register

| ID | Intended guarantee | Current evidence and remaining work |
| --- | --- | --- |
| MUT-1 | A successful authenticated reducer corresponds to its committed tenant transaction. | Rust rollback/aborted-commit regressions and transactional receipt tests exist. Whole invocation/receipt protocol remains unproved. PostgreSQL durability, tenant routing and effects staying inside the transaction are assumptions. |
| MUT-2 | Retries of one logical intent do not repeat its transactional business effects. | Receipt claim/result and writes share a transaction. Finite protocol model plus thirteen passing native receipt tests, with three failing baseline regressions, are recorded in reducer-receipts.md. Two authenticated SDKs with independent journals also witness claim overlap, owner commit and cancellation/rollback takeover, then durable retirement without extra ordinary calls. Five initialization regressions preserve routed legacy replay, refuse ambiguous history as infrastructure failure, repair a missing index after reset and witness cold callers waiting before initialization. Actual task cancellation during that wait returns admission, permits another initializer to complete and preserves exact capacity. Two history controls failed before the fix. No whole-source refinement proof. Namespace is tenant database + account subject + intent key. Path reuse is checked; argument equivalence is a caller obligation. Anonymous reducers/actions are excluded. Receipt retention is not promised as permanent. |
| SDK-1 | Accepted queue entries cannot be re-armed; parked entries require explicit retry. | The five-state/seven-action proof preserves acceptance. Four failing recovery controls drove a wire-result callback that persists committed plus an unresolved replica marker before data completion. The real SQLite failure trace now uses exactly one reducer call and read-only receipt reconciliation; a separately compiled callback-omission mutant fails the resend assertion. Dependency barriers, local predictions and owner changes have regressions. Four additional journal-rollback controls drove a read-only probe before redelivery of previously sent intents on capable runtimes; a real SQLite journal abort and probe-omission mutant now pass their expected checks. Actual SDK SIGKILL before and after the local acceptance commit now passes with fresh-process no-resend recovery and a missing-data negative control. Seven source-derived settlement laws cover the pending-replica marker. Older-runtime combinations and full asynchronous refinement remain open. See composed-sdk.md and outbox-delivery.md. |
| SDK-2 | Durable acknowledgements and local-intent retirement cannot pass an unapplied transaction. | Staging fences failed data lanes and retains capable-runtime intents through their watermark. The finite protocol model, public adapter regressions, real Chromium crash/abort/snapshot scenarios and Android/Hermes shared-handle/process-restart checks provide evidence for selected failures; see replica-durability.md, indexeddb-browser.md and sqlite-transactions.md. No complete protocol proof, iOS or native-phone evidence yet. |
| SYNC-1 | Duplicate/stale frames cannot undo newer authorized data; reconnect eventually catches up under explicit progress assumptions. | Revision primitive proofs and storage/lifecycle regressions exist. Full merge, snapshot/epoch transitions and recovery protocol remain unproved. An actual browser upgrade stall exposed snapshot preparations starving an inline receipt lookup. A real PostgreSQL pool-ownership witness fails on the previous source and passes with independently scheduled, queue-owned preparation tasks. Clear/remove/drop cleanup without completion polling and all eight queue tests pass; the rebuilt SDK coordinator reaches all eleven intended controls. A fresh native runtime run also passes all 18 snapshot-delivery tests; Eleven source-linked deferred-watermark laws now cover the owner/socket and readiness blocker decisions, with nine broken guards rejected and a structural check for readiness clearing on scope reset. Blocker capture and asynchronous persistence remain separate; these selected schedules do not establish the whole protocol. See opening-watermarks.md. |
| AUTH-1 | Old account/tenant/subscription operations cannot publish into a new owner. | Retry, discard, restore and accepted-settlement deletion races are reproduced/fixed locally. Delayed failures cannot close or reauthenticate a new owner in the exercised schedules. The actual ownership predicate is source-linked; activation identities use fresh Symbols with checked allocation sites. Retirement effects have four source-linked laws. Runtime schedules include valid-owner controls, and a finite async model rejects missing await guards and journal rerouting. Local admission and return-to-owner peer refresh have additional fixes and controls. Real PostgreSQL routing and authenticated WebSocket account A->B->A now pass with SQLite, delayed acceptance, same-key distinct-owner journals and a compiled stale-completion guard control. B cannot read A receipt, and original-owner reconciliation does not resend. A fixed-account tenant A->B->A schedule also passes with separate PostgreSQL databases, private row and receipt isolation, delayed acceptance and its own source mutant. Four adapter baselines expose a cached readiness certificate crossing database epochs; clearing it on scope reset restores accepted-intent retirement on return. The intent snapshot observer also now captures activation and checks close after its journal read. Account-return and post-close publication have failing baselines, five positive schedules, a closed AST bridge and compiled guard controls. Broader permission and publication paths remain open. Duplicate delivery from independent SDK journals now passes both commit and rollback/takeover schedules; shared live-journal coordination remains outside that fixture. See ownership.md. |
| RES-1 | Resource permits, deadlines and credits remain correctly owned through cancellation and cleanup. | Source-linked Rust budget/credit proofs and all 36 real V8 library tests pass. The finite retirement model exhausts capacities 1 and 2 and rejects three broken policies. Two native early-release mutants fail while teardown is held. All 18 native snapshot-delivery tests pass on the current runtime fixture. Five additional module-host defects have failing baselines and fixes: root waits ignored invocation deadlines, duplicate requests cancelled original work, cancellation allowed premature request-ID reuse, cancelled host calls retained replies, and reply IDs wrapped. All 15 module-host tests pass, including five selected lifetime regressions. The actual deadline minimum and checked reply-ID successor pass source-linked Verus checks; eleven broken numeric policies and ten structural bridge controls are rejected. Fresh module, SDK and PostgreSQL crash runs use the rebuilt host. A separate task-abort baseline exposed leaked sandbox workspace ownership before admission. A captured execution guard fixes that path. Three further failing baselines exposed explicit cancel waiting behind admission and both cancel and timeout waiting behind a blocked stdin write. Admission now observes cancellation, and the worker I/O watchdog covers request writing and response waiting. Eight controller tests, all eleven ordinary sandbox tests and the actual identity/unknown-exit policy proofs pass. Seven broken policies and fifteen structural cleanup controls are rejected. Four actual exit-held native baselines started a replacement child while old mappings remained live at concurrency one. Pairing the child with its owned permit through wait fixes those schedules; all four now retain capacity and permit healthy replacement only after native exit. An unconfirmed wait closes admission and wakes queued callers with an error. The native coordinator is mandatory in Linux CI. This source/lifetime argument trusts Rust/Tokio ownership and Linux process semantics; it is not a machine-wide or arbitrary-kernel-failure proof. Complete deadline, snapshot-credit and cleanup composition remain. See resource-ownership.md. |
| VIS-1 | Visibility feed checks are pure; ordinary data writes cause no visibility SQL or full snapshots; permission work is scoped and shared per affected member. | SDK/CLI schema guards, evaluator boundary checks, counters and a PostgreSQL fan-out fixture exist. Integrated budget measurements and a source-extracted Verus proof remain pending; see visibility-masks.md. No formal guarantee claimed. |
| UPG-1 | Supported persisted/protocol versions recover without silently changing ordering or completeness. | Adapter/revision-upgrade tests exist. Committed-to-pending migration and silent corrupt/unowned journal recovery are fixed locally with positive and failing-baseline controls. A source-derived compatibility register and real Chromium staged-upgrade crash/ownerless-rollback/corrupt-journal checks exist. Recovered coalescing identity and false durable-transition success are also fixed with failing controls and a source-linked admission proof. A real Chromium SDK/backend rollback-to-contract-rename composition now verifies the zero-effect/zero-receipt boundary in PostgreSQL, preserves parked intent identity through browser contract 1->2, explicitly retries in the original receipt namespace, checks durable data and refuses downgrade before connecting. Real Chromium scoped schema-1 outbox upgrade/restart now preserves all five delivery states, owner separation and durable identity with a changed-accepted-key control in CI. Historical replica schemas 3 and 4 now survive native abort and browser kill during index upgrade, with complete owner state and indexed predicates after restart. Five future-format baselines drove a bounded native-open fix across replicas and journals; five compiled controls reproduce the unsupported reads. The renamed-handler schedule now drops the actual committed reply, kills Chromium and requires receipt-only recovery with the original identity. A separate real module-contract composition now uses production sync to rename a tenant column and replace handlers, fences a live old client, survives Chromium SIGKILL, recovers the committed intent by its original receipt and sends the queued edit exactly once. A PostgreSQL operation ledger verifies insert then update; a compiled migration control must fail actual backend namespace rejection. Native-driver older-state restart evidence remains open. |

## Upgrade and corruption recovery checks

The SQLite historical/restart schedule now passes against the actual Node
SQLite driver and public SDK adapters. It kills two owned processes during
uncommitted native SQL, then checks raw rollback, both owner namespaces,
tombstones, complete membership, indexed predicates, original intent receipt
paths and all five delivery states. The public legacy-migration wrapper removes
its source only after durable import. Fresh reopen and deliberate re-import
preserve identity and allocation order. A compiled re-arming policy fails the
accepted-state oracle. A separate fresh-process corrupt-JSON schedule refuses
load, update, restore and new admission while retaining both accepted rows.
The same schedules are wired into the mandatory Expo/Hermes fixture, whose fresh
Android execution remains pending. Node execution does not close that platform
gap or provide iOS/physical-device evidence. See
[the native fixture](../../verification/native-sqlite/README.md).

`migrateClientData` contained an acceptance transition outside the original
seven-action proof: it reset committed records to pending. Upgrade recovery now
uses `deliveryState(..., "recover")`, which preserves acceptance. Five-state,
two-version tests and staged browser-upgrade recovery tests pass locally.

Generic KV loading previously ignored malformed rows, and IndexedDB v1-to-v2
migration deleted records without owner scope. Recovery now validates durable
identity/state, preserves those rows and reports `OutboxRecoveryError`. A failed
owner-resolution upgrade aborts its IndexedDB transaction. This error propagates
through ordinary memory-fallback catches, so an unresolved journal cannot
silently become an empty memory queue. Six recovery schedules verify that old
pending/committed records and malformed KV bytes remain available. The expanded
upgrade/recovery group passes 65 tests. Reverting only the migration change
produces three expected failures; reverting KV/IndexedDB recovery changes
produces six. Test sources remain unchanged during both baseline runs. Real Chromium process-crash upgrade recovery, legacy upgrade rollback and
IndexedDB-backed corrupt-journal refusal also pass. Real historical replica schema-1 and schema-2 Chromium upgrades now preserve complete state after abort, mid-upgrade browser crash and retry. The schema-1 primary-key change failed on the previous source and passes with staged store recreation inside one version transaction. Ambiguous ownerless checkpoints refuse; a compiled guard-removal control demonstrates overwrite. Native SQLite upgrade and
corrupt-byte restart schedules remain required. A real browser counter-exhaustion regression also exposed an unsafe normalized-engine sequence increment. Both adapters now reject the exercised invalid/exhausted counters, permit the last safe increment, and have eight source-linked counter laws with six rejected mutations each. Shared LocalReplica startup now rejects invalid numeric positions before exposing rows or a cursor. Real browser baselines fail for both adapters, and raw state survives refusal and same-replica recovery after fault removal. Settled activation handles now retire with an identity guard; forced reload and old-owner/new-owner overlap controls pass. Local activation also rejects unsafe identities, captures identity before invalidation listeners and retires superseded cache handles before callbacks. Baselines reproduce both wrong-owner publication and an incorrect return-to-owner result. Eight binary64 allocation laws and compiled owner controls cover the fixes; both adapters now reject noncanonical counter encodings and counters behind durable entity, window or reset positions. Real browser baselines show reused position 1 hiding a newly committed peer update; native refusals preserve raw state, and compiled integrity omissions reproduce it. Eight additional source-linked integrity laws, eleven rejected policies and 22 checked transaction entry points cover this boundary. Native index and transaction semantics remain trusted. See upgrade-compatibility.md.


## First end-to-end milestone

A real authenticated SDK/WebSocket/runtime/V8/PostgreSQL/Node SQLite trace now
passes ordinary commit, lost reply, SQLite write failure and fresh SDK/database
handle recovery. Its journal and replica use independent files, and a separate
connection checks committed row, membership and cursor at retirement. That trace
found and drove fixes for an opening-watermark race, truncated SQLite window
signatures and accepted-intent replay after replica failure. See [composed-sdk.md](composed-sdk.md) for its exact boundary and
controls. It does not establish the complete failure matrix or native app-driver
behavior.

The trace now includes true SDK process crashes at both sides of local acceptance
COMMIT, a real Chromium rollback-to-browser-contract-rename schedule, an
authenticated account A->B->A switch and competing delivery from two independent
SDK journals. PostgreSQL witnesses owner commit and cancellation/rollback
takeover before both SDKs reconcile durable data. A fixed-account tenant A->B->A
schedule now covers separate PostgreSQL databases and independent epochs, and
exposes a readiness-certificate lifecycle bug fixed at scope reset. Extend these
selected schedules to broader permission transitions and the remaining supported
upgrade combinations. Keep safety and progress
assumptions separate and connect each protocol transition to production code.

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
  admission. The direct IndexedDB and Expo SQLite queues report failed durable operations. Generic non-strict custom stores may still fall back to session-only memory and require a separate contract.
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
4. Extend the passing PostgreSQL process-crash checks to SDK transport and
   automatic pool reconnection. The isolated native coordinator now verifies
   committed lost-reply and uncommitted boundaries with retained PGDATA and
   explicit runtime pool reset. A supplementary authenticated SDK/runtime/PostgreSQL/SQLite schedule now kills PostgreSQL after a real committed reply is dropped, retains the runtime and its pools, then reconciles the original durable intent through automatic SQLx pool recovery without another ordinary call. It checks matching independent SQL witnesses before and after SIGKILL. Uncommitted crash during an SDK business call and automatic reconnection of the same SDK object remain.
5. Check server-native encoded snapshot ordering separately from the current
   SDK protocol. An installed cursor alone does not establish chunk delivery;
   current paged SDK consumers defer interleaved watermarks during staging.

Backend fixtures require PostgreSQL's documented transactional behavior and a
known durable configuration. Node SQLite tests do not establish Expo/Hermes
behavior. The selected Android/Hermes checks do not establish iOS or physical
phone filesystem behavior. Record those gaps separately.
