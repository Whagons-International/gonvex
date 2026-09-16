# Durable Sync Collections

The public guide lives in
[`apps/docs/content/docs/durable-sync.mdx`](../apps/docs/content/docs/durable-sync.mdx).
This file records the lower-level delivery model for runtime contributors.

Gonvex syncs are explicit, authorized, single-table collections that render
from IndexedDB and resume from a durable Postgres cursor. They complement live
queries; they do not replace computed queries, search, aggregates, or arbitrary
joins.

## Function declaration

```go
app.Sync(
    "tasks.recent",
    RecentTasks,
    gonvex.SyncTable("tasks").
        Key("_id").
        Columns("_id", "tenantId", "workspaceId", "title", "updatedAt", "deletedAt").
        EqualArg("tenantId").
        EqualArg("workspaceId").
        ExcludeWhenSet("deletedAt").
        VisibilityDependsOn("userTeams", "workspaces").
        OrderBy("updatedAt", "desc").
        Progressive().
        Budget(100, 4194304),
)
```

The handler returns the initial authorized rows. The declaration tells the
runtime which projected columns may enter the durable log and how later row
changes enter or leave the collection.

Use `Eager` when the complete collection fits its row and byte budgets. Use
`Progressive` for a bounded ordered window. Progressive streams reconcile the
current top-N keys after relevant transactions, so inserts, deletes, and
reorders correctly cross the window boundary while only changed rows travel to
the browser.

## Commit and resume model

1. Per-table row triggers stage projected old/new row values.
2. A deferred constraint trigger assigns one monotonically increasing revision
   to every row change in the transaction.
3. `NOTIFY` is only a wake-up hint. The durable `_gonvex_sync_changes` table is
   the source of truth.
4. The browser persists normalized collections plus `{epoch, revision}`.
5. Reload hashes the actual persisted rows and sends the cursor plus a
   collection digest. If retained, the runtime returns only later deltas;
   otherwise it sends a fresh authorized snapshot.
6. Every `sync.ready` carries the runtime's digest. The client recomputes the
   digest from its materialized rows and only then marks the collection
   up-to-date. A mismatch is repaired or reset; it is never accepted as ready.

Small collections (up to 256 rows) include their row keys and hashes in the
first resume request. Large collections normally send only the fixed-size
digest. If that digest differs, the runtime requests keys and hashes once and
sends only the differing rows. The unchanged path therefore stays constant
size regardless of collection size, while corruption is still repairable by a
delta rather than a full download.

The runtime prunes the log no more than hourly. Default retention is seven
days. The Go declaration API includes `RetainFor`, but the npm CLI does not yet
emit custom retention into the uploaded manifest, so applications currently use
the default.

## Auth and visibility

IndexedDB collections are scoped to the server-issued cache scope. The client
also remembers the last scope by project, tenant, JWT issuer, and JWT subject,
which allows a warm snapshot to render while server auth is revalidated.
Server auth, definition, or visibility changes reset the collection.

Every table declared through `Reads(...)` is automatically an effective sync
dependency. `VisibilityDependsOn(...)` remains available for dependencies that
are not already declared as reads and explicitly observes the whole table.
`Reads("tasks").Columns("spotId", "workspaceId", "deletedAt")` narrows update
invalidation to those columns; filter and order columns are included too.
Dependency columns are retained in the durable log so offline replay uses the
same rule. Inserts, deletes, broad events, and old logs missing these columns
still reconcile conservatively. A relevant dependency change triggers an
authoritative handler reconciliation, so computed membership (for example a
task appearing in an approver workspace) cannot be maintained by replaying the
physical source row alone.

Only declared columns are captured. Soft-delete markers must be declared with
`ExcludeWhenSet`, matching the snapshot handler.

## Client storage

`GonvexClientOptions.sync` configures the normalized Dexie store:

```ts
new ConvexReactClient(url, {
  sync: {
    databaseName: "my-app-sync",
    maxBytes: 150 * 1024 * 1024,
  },
});
```

Each collection enforces its own server-declared row/byte budget. The global
budget evicts least-recently-used collections. Deltas update only affected
IndexedDB rows. Collection metadata and rows load in one IndexedDB transaction,
and writes are serialized across unsubscribe/resubscribe incarnations so an old
async write cannot overwrite a newer cursor.

`sync.ready` means all of the following are true: the listener was installed
before the runtime sampled the cursor, all durable changes through that cursor
were processed, handler-derived membership was reconciled when required, and
the browser verified the resulting digest. A disconnect immediately revokes
that state and emits `sync.syncing`; reconnect replays from the durable cursor.

React exposes `useSync` and `useSyncSelector`. Prefer selectors for consumers
that only need a derived subset so unrelated row changes do not rerender them.

## Deliberate v1 boundaries

- One source table per sync.
- Equality arguments and null/non-null exclusion predicates.
- Handler-defined authorization and snapshot shaping.
- Server queries remain the path for search, scroll past a progressive window,
  aggregates, joins, and cache misses.
- Offline writes and mutation rebasing are not enabled yet. Mutation IDs are
  carried through the log so an optimistic queue can be added without changing
  the wire identity model.
