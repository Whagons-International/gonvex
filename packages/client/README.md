# @gonvex/client

Browser client for Gonvex Queries, Reducers, Actions, Live Queries, the
persistent Local Replica, and telemetry.

Most React apps should use `@gonvex/react`, which wraps this package with hooks.
Use `@gonvex/client` directly when you want lower-level control.

## Control Plane

Host-owned account, tenant-directory, invitation, agent-auth, voice, and support
functions use the same persistent connection as tenant functions:

```ts
import { control } from "@gonvex/client";

const account = await client.query(control.accounts.me, {});
const tenants = await client.query(control.tenants.mine, {});
await client.reducer(control.tenants.updateTimezone, { timezone: "America/Los_Angeles" });

const stop = client.watchControlQuery(control.support.sessions, {}, (result) => {
  renderSessions(result);
});
```

The references include argument/result schemas and their authorization class.
They never accept database URLs. Tenant-admin references always operate on the
active, authoritatively admitted tenant.

Control Plane live Queries resubscribe on reconnect and refresh after an
authorized Control Plane Reducer. Use `watchControlQuery` instead of refetching
after a write.

`authenticate` installs a new authentication scope and resolves only after the
runtime accepts it. It exists for provider-owned transitions such as developer
mode; React applications should use `GonvexAuthProvider`. The runtime rotates
single-use developer credentials on every connection, and the client retains
their successor only in memory.

## Install

```bash
npm install @gonvex/client
```

## Usage

```ts
import { GonvexClient } from "@gonvex/client";
import { api } from "./gonvex/_generated/api";

const client = new GonvexClient("ws://localhost:8080/ws", {
  project: "my-project",
  tenant: "demo",
});

client.connect();

const unsubscribe = client.subscribeLiveQuery(
  api.tasks.list,
  { status: "open" },
  (message) => {
    if (message.type === "query.result") {
      console.log(message.result);
    }
  },
);

await client.reducer(
  { kind: "reducer", path: "tasks.create" },
  { title: "Ship Gonvex" },
);

unsubscribe();
client.close();
```

## Local Replica

The Local Replica is the only client-side server-state store. It owns normalized
entities, live-query/replica windows, revision metadata, and optimistic command
patches. One-shot Query results are transient and are never persisted.

Inject a durable adapter when desired; omit it for memory-only operation:

```ts
const client = new GonvexClient(url, {
  localReplica: { storage: indexedDbReplicaStorage },
});
```

All window updates are applied atomically before listeners are notified. Live
Query windows and Replica Collections reference the same normalized entities,
so every view of an entity converges together.

## Replica Collections

Replica Collections materialize bounded, authorized entity sets in normalized
IndexedDB storage and resume them from a durable Postgres revision:

```ts
const watch = client.watchReplica<Task>(
  api.tasks.recent,
  { workspaceId: "workspace-a" },
);

const stop = watch.onUpdate(() => {
  const state = watch.localReplicaState();
  render(state?.rows ?? []);
  console.log(state?.completeness, state?.truncated, state?.computedRevision);
  console.log(watch.status()); // { isLoading, isUpToDate }
});
```

Configure Local Replica persistence when constructing the client:

```ts
const client = new GonvexClient(url, {
  localReplica: { storage: indexedDbReplicaStorage },
});
```

## Optimistic Reducers

Every public interactive Reducer declares its optimistic transaction. The
authoritative transaction and optimistic patches are reconciled in LocalReplica.

Generated references carry this metadata, so a normal Reducer call is enough:

```ts
await client.reducer(api.tasks.update, {
  taskId,
  updates: { priority_id: priorityId },
});
```

The client persists the pending command, applies it through LocalReplica, and
notifies watchers immediately. Reducer success includes an
`originCommandId` and committed revision. The overlay is removed only after the
corresponding authoritative transaction has been applied locally, preventing an
empty or stale frame between optimistic and committed state.

Authoritative entity state and optimistic entities are materialized by one
LocalReplica graph. Durable pending state lives in the command outbox and is
re-applied after reload. Outbox
rows are isolated by project, tenant, and authenticated identity; an account
switch removes the previous identity's overlay and can never replay its writes
under the new session. Unscoped rows from the pre-isolation schema are removed
during migration because their owner cannot be proven. If an opaque credential
does not expose a stable identity (and no `identity` hint is supplied), its
outbox is deliberately session-only rather than risking cross-user replay.

## Offline Live Queries

Generated Live Query references include the same structured plan Gonvex
compiles to PostgreSQL. While offline, the client executes that plan over the
normalized cached corpus:

```ts
const result = client.offlineLiveQuery(api.tasks.grid, args);
// { rows, completeness: "complete" | "partial", supported }
```

`supported: false` means the plan is explicitly server-only. A partial result
must be labeled as cached data rather than presented as the full database.

## Lightweight Error Tracking

Capture global browser failures and failed Gonvex operations with the same
client. Reports are batched, retried locally, scrubbed, persisted by the runtime,
and grouped in the Gonvex dashboard:

```ts
const client = new GonvexClient(url, {
  project: "my-project",
  tenant: "acme",
  errorReporting: {
    release: "2.4.0+abc123",
    environment: "production",
  },
});
```

Error registration and envelopes use native persistent-protocol frames. No
browser HTTP ingestion endpoint is needed.

Applications can report an explicit event through the public API:

```ts
await client.reportError("envelope", {
  events: [{
    message: "Task preview failed",
    level: "error",
    context: { component: "TaskPreview" },
  }],
});
```

`new GonvexErrorReporter({ client })` adds global browser error capture and
re-registers its telemetry session after reconnect.

## Connection reliability

The client reconnects automatically after an unexpected socket close (exponential
backoff from ~250ms to 5s). On reconnect it re-authenticates, then resubscribes
active live queries and pending one-shot queries. Explicit `close()` disables
reconnect.

```ts
client.connectionState();
// {
//   isWebSocketConnected, hasEverConnected, connectionCount, connectionRetries,
//   hasInflightRequests, inflightReducers, inflightActions, inflightOneShotQueries
// }

const stop = client.subscribeToConnectionState((state) => {
  // drive banners / health UI
});
```

### Timeouts (defaults)

| Operation | Default |
| --- | --- |
| One-shot `query()` | 20s |
| `reducer()` | 20s |
| `action()` | 60s |

Override per client (`timeouts` option) or per call (`{ timeoutMs }`). Use `0` to disable.

### Typed errors

Rejected operations throw `GonvexClientError` with `code`:

- `server`: runtime executed the function and returned an error
- `timeout`: no response within the timeout
- `disconnected`: socket dropped while the operation was pending
- `closed`: client was explicitly closed
- `auth`: authentication rejected

### Reducer / Action disconnect policy

Actions and Reducers without `{ offline: "queue" }` fail closed after a
disconnect. They reject with `code: "disconnected"` (or `timeout` / `closed`).
Optimistic Reducers are persisted before transport even in fail-closed mode,
so a process reload cannot expose an older cached row while an accepted write
is still waiting for its authoritative subscription update.

Pass `{ offline: "queue" }` to a Reducer to durably accept a transport failure
and replay the same idempotency key after reconnect, whether or not that
Reducer also declares optimistic UI metadata. Actions are never queued.
Deterministic server errors are never queued and always roll an optimistic
entity overlay back when one exists.

Live Queries persist their last verified window in the Local Replica and
resubscribe after reconnect. Call `client.retryLiveQuery(ref, args)` to force a
re-request after a server error. `useQueryResult` is for one-shot Queries.

## Exports

The package exports:

- `GonvexClient`
- `GonvexClientError`, `ConnectionState`, timeout defaults
- normalized Local Replica entities and Live Query windows
- durable optimistic Reducer overlays reconciled by command ID and revision
- opt-in command outbox replay with stable idempotency keys
- `subscribeReplica`, `watchReplica`, and persistent Replica storage
- `watchControlQuery` for authorized Control Plane live Queries
- browser capability and telemetry helpers
- `reportError`, `GonvexErrorReporter`, and automatic operation error reporting

## Related Packages

- `@gonvex/react` - React hooks over this client
- `@gonvex/protocol` - protocol message and JSON types
- `@gonvex/cli` - development CLI

## Documentation

Full docs live at https://desarso.github.io/gonvex/
