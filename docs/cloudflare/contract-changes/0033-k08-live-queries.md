# 0033: K08 Queries and Live Queries: findings and proposals

Date: 2026-11-11. Status: **§1–§6 accepted in 0032** (lead); §7 fixed in this PR. Proposed by K08 (#356).
Affects: K05, K09, K10, the tenant object, the conformance fixture.

K08's engine (`cloudflare/kernel/query/`) sends the Rust runtime's frames. Its parity test replays Rust's own run of
the new scenario `live-query-task-grid` (Whagons' real `tasks.grid` plan) and matches every step's normalized frames
and every `windowRevision`, so the result bytes are identical too. These findings need a ruling or another
package.

## 1. "Page and object patches" stay deferred

The package title lists page and object patches. Rust never sends `query.patch`, `query.pagePatch` or
`query.objectPatch` (CONTRACT A.3, A.6 item 5; 0012 R19 defers them). Every live update from Rust is a full
`query.result` window, so the kernel sends the same. **Proposal:** confirm that K08 is done without them. They
stay a stage-3, capability-flagged change.

## 2. Rust coalesces reruns by timing; the kernel answers every commit

Rust reruns a live query in the socket task, at the clock revision current *when the rerun runs*
(`execute_live_query_plan` reads `_gonvex_sync_clock`), and skips any event at or below the computed revision
(`apply_live_query_feed_event`). When several commits land before a socket task catches up, one rerun covers
them and the intermediate windows are never sent. How many are skipped depends on timing. The kernel applies each
commit in order inside the tenant object, so it sends every intermediate window that changed.

- Both converge on the same final window, and every frame is a true state at its `subscriptionRevision`.
- The first version of the scenario raced four writes against reruns. Rust sent one `g-status` frame where the kernel
  sent three, and two Rust backends agreed only by luck. The scenario now waits for quiet after each write
  (harness determinism rule: never compare an order the backend may choose freely).

**Proposal:** accept as Rust-equivalent. If fan-out load needs it (R05/X01), delivery can coalesce the same
way later, by applying only the newest revision of a backlog. That is an optimization, not contract.

## 3. Per-socket frames in a delivery (K05)

Live query frames are per subscription. `DeliveryBatch` (`kernel/session/placement/gateway.ts`) carries only
`all` and `byClass`. **Proposal (additive, versioned):** `bySocket?: Record<connectionId, string[]>`, delivered
before `all`/`byClass` for that socket, because Rust sends live query frames before the commit's replica frames
and watermark (`lib.rs` 2203-2210). K08 encodes the result once per execution. Only the `{"type":…,"id":…`
prefix differs per subscription, so a later version can send shared tails plus ids.

## 4. Live subscriptions across a tenant-object eviction (K05, K10)

Subscriptions live in tenant-object memory: plan, arguments, member resolution and last result. Nothing ephemeral
goes to SQLite. Gateways hold the sockets (0016 G0-1). If the tenant object is evicted while a gateway socket stays
open, the socket's live queries silently stop updating. Rust has no such state, because socket and subscriptions
share one task. **Proposal:**

- The gateway keeps each socket's open `query.subscribe` frames (small, already decoded) in memory.
- The tenant object reports an instance id on `deliver` and `attachGateway`.
- On a new instance id, the gateway resends those subscribes with the `windowRevision` it last sent. The client
  then gets `query.progress` if nothing changed and the new window otherwise, exactly as a Rust reconnect does.

K10 needs the same for replica cursors. The alternative, keeping the tenant object awake while live subscriptions
exist, is an R02 cost decision.

## 5. Relevance: same frames, different mechanism (K09)

Rust skips reruns a member cannot observe (v2 `mask_change_relevant`, v1 dependency columns). K08 reruns a
subscription when a commit touches a table its plan or masks read, and narrows that with K09's optional
`changeConcerns` for v2 masks. Skipping more or fewer reruns cannot change a frame, because an unchanged result
sends nothing. The cost is measured on the parity replay: 4 statements and about 117 rows read per execution.
K09 (#355) implements the port as written (`createQueryVisibility(registry)` in
`kernel/visibility/query-port.ts`). K08 uses it through `kernelVisibility(registry)`, with one `VisibilityRegistry`
per tenant object. K08's earlier stand-in is deleted, so the tree has one visibility implementation. The parity
test gives the same frames and hashes on it.

## 6. More than 100 bound parameters

Rust binds one parameter per `in` value and per `oneOf` value. Two Whagons one-shot plans
(`files.taskAttachmentNotes`, `reports.taskUsersForReport`) bind 153 with 150 ids. K08 rewrites each such list as
`IN (SELECT value FROM json_each($k))`. `json_each` yields the storage class Rust binds, and its `value` has no
affinity, like a bound parameter, so the comparison is unchanged. Tests check this against SQLite. A statement
still over 100 reads every parameter from one JSON array (`json_extract`). No plan produces one. **Proposal:**
accept as an adapt.

## 7. Conformance fixture

The scenario adds `fixtures/app/migrations/0002_task_grid.sql` (task-grid tables with Whagons' columns and types)
and `fixtures/app/gonvex/tasks.ts`, whose Reducers are named `putRow`/`removeRow`. A Reducer named `remove`
collides with `board.remove` under the fixture's `export *` and is silently dropped from the bundle. The fixture
had one such collision already: `savepoints` was exported by both `host.ts` and `storage.ts`. Per the lead's
review, this PR renames the storage-layer one to `storage.sqlSavepoints` and updates `storage-sql`.

## Follow-ups blocked by the credentials outage (2026-11-11)

- Run the task grid against the largest staging tenant copied into `gonvex-cf-dev-*`, with Whagons' full `tasks`
  mask Record rows read, CPU per rerun and statement size against the 100 KB limit. Without
  visibility the plans compile to 0.7–15 KB.
- Platform check of `json_each`, `printf('%!.17g')` and the 60-column `json_object` splice under DO SQLite. All
  pass in local workerd.
