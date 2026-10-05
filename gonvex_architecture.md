# Gonvex Architecture

Gonvex is a transactional Postgres-backed realtime Local Replica system with
an indexed server-query escape hatch for datasets that are too large to
replicate. This document is the architecture contract, not a future wishlist.

## Constitution

1. Postgres commits are the only realtime truth.
2. Reducers are the only normal durable write path.
3. Actions cannot access application database handles; they call Reducers.
4. One Reducer represents one atomic business intent.
5. Declared writes and manual invalidation do not exist.
6. Bounded data uses Replica Collections.
7. Unbounded data uses indexed Live Queries.
8. Live Queries require a structured, inspectable query plan.
9. One server transaction is applied atomically to the Local Replica before UI notification.
10. The Local Replica is the only replicated server-state store exposed to UI bindings.
11. Interactive Reducers require an optimistic contract or an explicit reviewed exception.
12. Capacity limits protect execution but never determine freshness.

## Executable functions

Application code has exactly three executable function kinds.

### Query

A Query is read-only and one-shot. Gonvex executes it in a repeatable-read,
read-only Postgres transaction. It is called with `useQuery` or `client.query`.

### Reducer

A Reducer is the only public durable state transition. Gonvex begins one
Postgres transaction and exposes only the bounded `ctx.db` capability to application code. Raw pools
are hidden so an operation cannot accidentally commit outside the business
transaction. Failure rolls the complete intent back.

```ts
export const renameTask = reducer({
  args: schema.object({ taskId: schema.id("tasks"), title: schema.string() }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [{ operation: "patch", entity: "tasks", id: ["taskId"], fields: { title: { $arg: "title" } } }],
  },
  run: ({ db }, { taskId, title }) => db.update("tasks", taskId, { title }),
});
```

`Writes(...)`, manual notifications, and refetch instructions are not part of
the API. `_gonvex_sync_changes` records what actually committed.

### Action

An Action performs external or long-running work such as HTTP, email, AI,
push, and files. It starts with no ambient capabilities and never receives a
database handle. Network origins, secret names, storage, scheduling, and named
Query/Reducer tools must be declared in its signed module contract. Durable
state re-enters through an exact Reducer tool binding:

```ts
await ctx.tools.markDelivered(args);
```

There is no generic `runQuery(path)`, `runReducer(path)`, `ctx.db`, or complete
environment object in an Action.

Business rows and external-work requests should be committed together through
a durable outbox. The Action worker processes the outbox idempotently.

## Public tenant functions

Tenant calls normally require an active Member. Explicit `public: true` Query,
Action, and server-only Reducer declarations also admit anonymous browsers on
one active tenant route. They receive `ctx.auth.account === null`,
`ctx.member === null`, and the canonical `ctx.tenant.id`. Public Queries run
an application-owned read-only handler, which must enforce token and data
visibility policy. Public reducers require `interactive: false` and have no
local execution, offline outbox, or anonymous scheduling. Live Queries and
Replica Collections remain member-only. See [public-functions.md](docs/public-functions.md)
for protocol shapes, storage rules, and configurable abuse limits.

## Authoritative change feed

Every application table has a row-level trigger that appends to
`_gonvex_sync_changes`. Each committed transaction has one monotonically
increasing revision and ordered row changes:

```text
revision / ordinal / origin command / table / row id
operation / old value / new value / changed columns
```

Postgres sends a small `gonvex_change_feed` notification only to announce that
a revision exists. Gonvex reads the durable revision before routing it. The
former `gonvex_table_change` statement triggers are removed during migration
and have no installer or runtime fallback.

## Delivery modes

Delivery is separate from executable function kind.

### Replica Collection

A Replica Collection is a bounded, entity-shaped set with explicit row and
byte budgets. A complete collection supports exact offline evaluation; a
truncated collection reports `partial` rather than pretending to represent the
database.

```ts
export const recentTasks = replicaCollection({
  args: schema.object({ workspaceId: schema.id("workspaces") }),
  result: schema.array(taskSchema),
  replica: {
    table: "tasks",
    key: "id",
    columns: ["id", "title", "status", "updated_at"],
    orderBy: "updated_at",
    orderDirection: "desc",
    maxRows: 10_000,
    maxBytes: 50 * 1024 * 1024,
  },
});
```

### Live Query

A Live Query maintains an exact server-computed result window for a large
dataset. It is not arbitrary reactive SQL. Registration requires
`gonvex.LivePlan(...)`, which declares searchable/filterable/sortable fields,
window budgets, and offline capability.

```ts
export const taskGrid = liveQuery({
  args: taskGridArgs,
  result: taskGridResult,
  liveQueryPlan: {
    table: "tasks",
    key: "id",
    columns: ["id", "workspace_id", "title", "description", "deadline"],
    where: { operator: "eq", column: "workspace_id", value: { argument: "workspaceId" } },
    search: { argument: "search", columns: ["title", "description"] },
    sort: {
      columnArgument: "sort",
      directionArgument: "direction",
      defaultColumn: "deadline",
      defaultDirection: "asc",
      allowedColumns: ["deadline", "created_at"],
    },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 150 },
  },
});
```

The plan compiles to parameterized PostgreSQL. Identifiers are allowlisted by
the plan. Portable operators—equality, inequality, ranges, membership,
contains, case-insensitive contains, boolean composition, sort, and window—can
also run against cached replica rows. A plan can explicitly mark a capability
online-only.

Canonical groups share computation only when query arguments and visibility
fingerprints are equivalent. A title-only change can patch an entity directly.
A change to filter/order/window semantics reruns one canonical SQL query,
diffs IDs, and fans out the small delta.

## Revision-only correctness

A Live Query group has two freshness values:

```text
requiredRevision
computedRevision
```

It is authoritative exactly when:

```text
computedRevision >= requiredRevision
```

If revision 993 arrives during the execution for 992, completing 992 cannot
mark the group current. Retained snapshots and grace timers only control
memory; they carry no correctness meaning. Unknown-dependency and broad
invalidation fallbacks do not exist.

## Local Replica

The Local Replica contains normalized authoritative entities, optimistic
overlays, Live Query ID memberships, completeness/freshness, pending commands,
and the server revision. Live Queries store ordering and membership IDs—not
duplicate entities.

The server emits one `replica.transaction` per visible Postgres transaction:

```text
cursor: database epoch + revision
originCommandId
ordered entity changes
query membership changes
```

The client serializes transaction application. Persistence completes before
one in-memory state swap and one subscriber notification. Back-to-back frames
cannot interleave.

- Web uses transactional IndexedDB storage.
- Expo/mobile uses `@gonvex/expo-sqlite`, with normalized entity/query tables
  and the cursor updated inside one SQLite transaction.

React bindings integrate the replica as an external store. Signals are
selectors over the replica, never another server-state owner.

## Optimistic commands

Optimistic state is an overlay, not fabricated authoritative state. A command
is identified by `originCommandId`. Reducer success includes its committed
revision; the overlay remains until that revision is applied locally. This
gives a continuous transition from optimistic to authoritative state without a
blank or rollback frame.

Every public Reducer must provide optimistic metadata. A genuinely unsuitable
operation must use `OnlineOnlyNonOptimistic("reviewed reason")`, making the
exception visible in generated manifests and the dashboard.

## Visibility

Routing evaluates old and new visibility. A row moving out of a caller's
scope becomes a delete; a row moving in becomes an upsert. Live Query sharing
includes the visibility fingerprint. Equal SQL arguments alone never prove
equal results.

Each delivered table has one structured rule exported by the TypeScript
module. Gonvex compiles that same rule into snapshot/Live Query SQL and into
the in-memory old/new change router:

```ts
export const taskVisibility = visibility({
  table: "tasks",
  key: "id",
  sets: {
    assigned: {
      table: "taskUsers",
      select: "taskId",
      joins: [],
      where: [{ table: "taskUsers", column: "memberId", context: "member.id" }],
    },
  },
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "tasks.viewAll" },
      { operator: "inSet", column: "id", set: "assigned" },
    ],
  },
});
```

## Offline contract

Hooks expose:

```text
source:       server | cache
completeness: complete | partial
freshness:    current | verifying | offline
```

Portable plans run against cached entities while offline. Partial caches must
be labeled as partial. Server-only operators require a connection.

## Operational protection

Initial Queries, snapshots, and Live Query recomputations share one bounded
admission controller. Admission controls concurrency and latency; it never
changes which revision is required or whether a result is authoritative.
