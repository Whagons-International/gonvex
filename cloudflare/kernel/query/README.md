# Kernel Queries and Live Queries (K08)

`query.call`, `query.subscribe`, `query.subscribeMany` and `query.unsubscribe` with tenant scope, and the
structured plans (`liveQueryPlan`) every Whagons Query is: 10 live plans and 35 one-shot plans (0021 §2). The
reference is `rust/crates/runtime/src/live_query.rs` (with `live_query_projection.rs`), `execution.rs` 130-330
and `lib.rs` 2990-3259 at `0bf3ad13`. The plan is metadata only: the kernel compiles it to SQL and injects the
table's visibility predicate, so a module cannot bypass row authorization.

```ts
const queries = new QueryEngine({
  visibility: kernelVisibility(registry), // K09's port; ONE VisibilityRegistry per tenant object, shared
  module: (projectId) => module,     // moduleFromManifest(projectId, manifest): functions and visibility plans
  run: snapshotRunner(tenantStorage),// one K01 Query invocation per snapshot; the tenant object serializes them
  validateArgs,                      // K04's portable-schema check (kernel/host/src/schema.ts validateValue)
  receipt, replica, handler,         // optional ports: K07 receipt Query, K10 Replica Collection Query, K04 handler
});
await queries.dispatch(socketView, frame);            // TenantPort: frames for query.* (undefined otherwise)
const frames = await queries.live.applyFeed(event);   // after each commit, BEFORE replica frames and the watermark
queries.live.clear(connectionId);                     // close, re-auth without preservation, membership change, reload, lag
```

| File | What | Rust |
| --- | --- | --- |
| `plan.ts` | `LiveQueryPlan`, `validatePlan` and its texts, identifiers, relation and projection tables | `LiveQueryPlan::validate`, `validate_expression`, `quote` |
| `compile.ts` | Plan to SQL: visibility guard, the live membership check, `where` (incl. `inRelation` with the related table's mask), filters, search (columns, projection, documents, `$and`/`$or`), index sort, window, exact count, row JSON by declared type | `execute_in_transaction` and helpers, `rows::json_object_expr` |
| `parameters.ts` | The 100-parameter limit: long `IN` lists become one `json_each` parameter; a last-resort `json_extract` form | (Rust binds one per value) |
| `execute.ts` | One snapshot: clock, member resolution, related plans, window, count, `shape_result`, the 32 MiB cap | `execute_live_query_plan` |
| `live.ts` | `LiveQueries`: subscribe, resume (`query.progress`), reruns per commit, shared executions, socket lifecycle | `open_live_query`, `apply_live_query_feed_event`, `resume_message` |
| `call.ts` | `query.call`: Rust's check order and texts; plan execution; ports for the other branches | `execute_tenant_query` |
| `frames.ts`, `json.ts` | Frames in serde byte order; serde_json's bytes (insertion order, ryu floats, escapes); Rust `{:?}` | `query_message`, `query_result`, serde_json |
| `visibility.ts` | **The port K09 implements** (`resolve`, `resolveRelated`, `compilePredicate`, `dependencyTables`, `changeConcerns`) and `kernelVisibility(registry)`, K09's implementation (`kernel/visibility/query-port.ts`) | `visibility.rs` |
| `feed.ts` | The feed event (`FeedEvent`) and a reader of one revision from `_gonvex_sync_changes` | `change_feed::FeedEvent` |
| `module.ts` | The query view of a `/dev/sync` manifest | `modules.rs` |

## Behavior

- **Frames.** `query.result` is `{type, id, path, reason, result, subscriptionRevision:{epoch, sequence},
  windowRevision}` with the payload keys in `BTreeMap` order; a one-shot result is `{type, id, path,
  reason:"initial", result}`; `query.progress` is `{type, id, path, throughRevision}`. Results are written as
  serde_json writes them (Rust parses SQLite's JSON and re-serializes it with `preserve_order`): object order is
  the plan's column order, REAL values keep `.0` and use ryu's shortest form, strings use serde's escapes. So
  `windowRevision`, the SHA-256 of the result, equals Rust's (the parity test checks every one).
- **Live updates.** After a commit, a subscription whose tables the commit touched reruns its whole plan in one
  snapshot and sends `query.result` with reason `change` (`reconnect` after a feed reset) only if the result or
  epoch changed; an untouched subscription is certified at the new revision without a rerun. Rust never sends
  `query.patch`, `query.pagePatch` or `query.objectPatch` (0012 R19), so the kernel does not either: "page and
  object patches" in the package title are deferred with them, and the patch-shaped frames clients receive are
  full windows, as from Rust.
- **Resume.** A subscribe carrying the `windowRevision` of the fresh result gets `query.progress` at the new
  revision instead of the rows.
- **Errors.** Every text is Rust's, including the `invalid structured query:` and `module database capability
  failed:` wrappings, Rust's `{:?}` quoting of paths and names, and the filter, window and argument errors. A
  failed re-subscribe keeps the subscription it named; re-subscribing an id replaces it.
- **Sharing (design rule).** Subscriptions of one account with the same path and arguments run once per commit
  and their frames share one encoding of the result after their own `id`.
- **Relevance.** A rerun is skipped when the commit touches none of the tables the plan reads: the plan table,
  `inRelation` tables, the index projection and search documents with their dependencies, and every table
  the visibility plans read (`dependencyTables`). K09's optional `changeConcerns` narrows it further for v2
  masks, as Rust's `mask_change_relevant` does. Relevance changes cost, never frames: an unchanged result sends
  nothing.
- **100 bound parameters.** Rust binds one parameter per `in` value and per `oneOf` value. Over 100, each list
  `IN ($a, $b, …)` becomes `IN (SELECT value FROM json_each($k))`: `json_each` yields the storage class Rust
  binds (TEXT, INTEGER, REAL, NULL, booleans as 0/1) and its `value` has no affinity, like a parameter, so the
  comparison is the same. v2 masks already bind a set as one JSON array (`visibility.rs` `inSet`).

## Integration (owned elsewhere)

| Need | Owner | Hook |
| --- | --- | --- |
| Tenant frames reach the engine | the tenant object (K05 `TenantPort`) | `QueryEngine.dispatch(socket, frame)` |
| Commits reach live queries, before replica frames and the watermark (`lib.rs` 2203-2210) | delivery (K10) | `live.applyFeed(event)` per committed revision, in order |
| Per-socket frames in a delivery | K05 `DeliveryBatch` | `bySocket`, ordered before replica frames (0033 §3, accepted in 0032) |
| Socket close, re-auth without `subscriptionsPreserved`, membership change, `system.reload`, feed lag | K05 / K10 | `live.clear(connectionId)`, `live.rebind(connectionId, session)` |
| Bootstrap and call watermarks | K10 | `live.computedRevisions`, `live.epochs`, `live.takeOpeningCohort` |
| Visibility | K09 | `kernelVisibility(registry)`: one `VisibilityRegistry` per tenant object |
| `__gonvex.reducerReceipt`, Replica Collection Queries, public Query handlers | K07, K10, K04 | `CallPorts.receipt`, `.replica`, `.handler` |

## Tests and numbers

`pnpm test` runs them in workerd:

- `test/task-grid-parity.test.ts` (**done_when**): replays Rust's run of the conformance scenario
  `live-query-task-grid` (`test/golden/rust-task-grid.json`, written by `conformance/src/live-query-golden.mjs`):
  54 writes as their change rows, 32 client frames at the revisions Rust answered them. Every step's frames,
  normalized by the harness's own normalizer, equal Rust's (117 query frames, 85 of them from the server), and
  all 67 `windowRevision`s equal Rust's, so the result bytes are identical. A mutation of the NULL ordering or of the float formatting fails it.
- `test/engine.test.ts`: serde bytes (ryu vectors, order, escapes), the 100-parameter packing against SQLite,
  subscribe/resume/rerun/unsubscribe/reset frames byte for byte, refusals and `query.call`'s branches.

The scenario runs Whagons' real `tasks.grid` plan (whagons5-client `2457bb65f`, verbatim) over Whagons' legacy
masks for `tasks`, `memberTeams` and `taskWorkspaceContexts`, reduced to the sets the fixture has tables for.
The 45 plans of the Whagons manifest of 2026-11-10 all validate and compile (checked once, not committed: the
manifest is app code); the two `in` plans bind 153 parameters in Rust and 4 here.

Cost in the parity replay (K09's visibility, legacy masks): 4 statements per execution (clock, member, window, exact
count) and ≈ 117 rows read per execution of the task grid on the fixture (178 executions for 194 reruns:
20,865 rows read). Writes: none (a Query never commits). The engine is not in the Worker bundle yet (no
tenant object imports it), so bundle bytes are unchanged.
