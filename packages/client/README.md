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

For a separate auth/control connection, pass `role: "control"`:

```ts
const controlClient = new GonvexClient(socketUrl, {
  project: projectId,
  tenant: selectedTenantId,
  role: "control",
});
```

This disables generated local execution, Replica Collections, replica storage,
and the offline outbox. The selected tenant still scopes Control Plane calls.
Deploy a runtime supporting the `tenantControl` auth capability before enabling
this mode for tenant-scoped calls. Older runtimes treat control-only sessions as
account-scoped and reject developer-grant authentication.

Control Plane live Queries resubscribe on reconnect and refresh after an
authorized Control Plane Reducer. Use `watchControlQuery` instead of refetching
after a write.

Projects may use Gonvex-native auth, Firebase, external OIDC, or an explicit
hybrid mode. `createFirebaseAuthAdapter` defines the small client boundary used
by `@gonvex/react`; it does not import or bundle Firebase. The public
`control.auth.exchangeExternalToken` Action accepts a Firebase ID token, never
a Firebase custom token, and returns a canonical Gonvex Account session before
tenant selection.

`authenticate` installs a new authentication scope and resolves only after the
runtime accepts it. It exists for provider-owned transitions such as developer
mode; React applications should use `GonvexAuthProvider`. The runtime rotates
single-use developer credentials on every connection. The client uses the
successor for its own reconnects and publishes it through
`onDeveloperCredential` so the React provider can resume developer mode after a
reload of the same tab.

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

## Performance telemetry

`telemetry: true` reports operation timing. Device information is captured once
per client and sent with authentication. Runtimes advertising `telemetryBatch: 1`
receive up to 50 events per frame, flushed every five seconds while events are
pending, on page hide, when the document becomes hidden, and before auth changes
or close. Older runtimes receive individual events with cached device information.
Buffered performance events are dropped on transport loss rather than replayed
under a different session. This does not affect the durable reducer outbox.

Live Query patches still update state and notify `onTelemetry` listeners. Only
initial/full server results and errors are reported over the network, avoiding
an extra telemetry frame for each normalized patch.

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

### Queued intent lifecycle

Runtimes from 0.5.2-staging.15 classify every `reducer.error` as `rejected`,
`transient`, `update_required` or `unauthenticated` (also exposed as
`GonvexClientError.errorClass` / `retryable`). The outbox acts on it:

| Class | Outbox behaviour |
| --- | --- |
| `transient` (and timeouts) | Retry with exponential backoff. After `outbox.retry.maxAttempts` (default 10, backoff capped at `maxBackoffMs`, default 60s) the intent is parked as `failed`: kept durably with its prediction, and no longer blocking later intents. |
| `unauthenticated` | Keep the intent, re-authenticate, retry. Does not spend the retry budget. |
| `update_required` | Keep the intent pending and call `onUpdateRequired`. |
| `rejected` | Roll the prediction back, rebase later local intents, fire `onReducerRejection`, and keep the intent as a `rejected` record until the app discards or retries it. |

Older runtimes send no class: their errors are treated as rejections, except
the legacy stale-artifact and "authenticate with an active tenant" messages.

```ts
const intents = await client.listIntents();       // or subscribeIntents()/intentsSnapshot()
client.entityIntentStatus("tasks", taskId);       // "syncing" | "failed" | "rejected" | undefined
await client.retryIntent(intent.id);              // same idempotency key, fresh budget
await client.discardIntent(intent.id);            // pending/failed/rejected only
await client.listOutboxScopes();                  // includes identities that never returned
await client.purgeForeignOutboxScopes();          // never automatic
```

`@gonvex/react` exposes the same list through `useOutboxIntents()` and a
per-row `useEntityIntentStatus(entity, id)`.

### Local execution fallbacks and residency

A locally executing Reducer whose body reads rows the Local Replica does not
hold is queued with no local result (`{ status: "queued", reducerId }`).
`client.onLocalExecutionFallback(listener)` (or the `onLocalExecutionFallback`
option, or `useLocalExecutionFallback` in `@gonvex/react`) reports every such
fallback with the Reducer path, intent id, phase (`call` or `replay`), reason,
and missing table. Generated clients also carry each Reducer's codegen-derived
`localDependencies`; with `localDependencyResidency: "bounded"` (the default)
the client keeps dependency tables whose generated collection declares
`maxRows <= 1000` subscribed for the session, so those Reducers run locally
from their first call. See the Local Reducer Execution docs page.

### Resetting the local cache

`client.resetLocalReplica({ keepOutbox = true })` implements a "Clear cache"
setting without the app touching replica storage. It deletes the active
identity's persisted replica rows (IndexedDB, Expo SQLite, or the configured
storage), keeps the saved offline session, closes and re-opens every active
Replica Collection and Live Query without cursors, and re-applies the
predictions of all live intents so they reappear on top of the fresh
snapshots. Pending, failed and rejected intents are kept unless
`keepOutbox: false` is passed explicitly (inflight intents are never dropped).

It is **online only**: offline (or before the socket is authenticated) it
rejects with `GonvexClientError` `code: "disconnected"` and changes nothing,
so the user is never left with an empty cache that cannot be refilled. React
apps can use `useResetLocalReplica()` for `{ reset, isResetting, error }`.

A parked intent lets later intents proceed; the server validates each of them
on its own, so an intent that depended on the parked one is rejected rather
than applied out of order. Retrying a parked intent after later intents
committed re-applies it on top of them.

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

Full docs live at https://whagons-international.github.io/gonvex/

[Open online anonymous tenant connections with `public: { tenant }`](../../docs/public-functions.md).

### Cached startup before server auth

`client.restoreSession(auth)` installs account and tenant credentials, starts
connecting in the background, and resolves `true` when their persisted replica
is readable. It resolves `false` when no matching session exists or the scope
changes during hydration. Storage failures reject the promise. The read-only
replica path works without `localRuntime`.

`client.localSessionReady` observes the current local restore without starting
or waiting for network authentication. Subscribe with
`client.onLocalSessionChange(listener)` and read `client.localSessionState()`:
`"unavailable"`, `"restored"`, or `"authenticated"`. A restored session is local
readability, not server authorization. Server reads and durable reducer replay
still wait for accepted auth. `authenticate(auth)` still waits for the server,
including for developer and impersonation transitions.

Always install the current account and tenant, and clear `token`, `identity`,
`fetchToken`, and `tenant` on sign-out. JWT claims conflicting with an identity
hint cannot restore either account's cache. Opaque tokens need the account
identity hint supplied by your auth provider. Definitive auth rejection and
sign-out forget the saved directive and immediately hide the old replica;
durable queued intents remain isolated for a future authorized login. Token-fetch
failures, socket closure, and transient or uncoded server errors preserve cached
access. Only `details?.definitive` in `onAuthError((message, details) => ...)`
authorizes clearing an application session.

### Causal offline delivery

Local reducers persist point reads, table scans and written row IDs with each
intent. The outbox sends up to eight independent intents at once, using
`reducer.callMany` when the server advertises `reducerBatch: 1`. Dependent
intents wait for the earlier call to settle. A backed-off call blocks its
causal dependents; unrelated saves can proceed. Old executor results and
uncovered local reads retain conservative barriers.

Browser upgrade stores cache journal entries between peer invalidations and
lock ownership changes. A full reload at lock acquisition also covers delayed
broadcasts from older tabs. Contract checks are cached only during short intent
locks. Delivery waits never hold the upgrade fence. SQLite and browser stores
write batch admission, completion and deletion in one transaction per phase
for the items that settle together.

Pure overwrite reducers can explicitly opt into replacing a never-sent intent:

```ts
const setTitle = reducer({
  coalesce: { mode: "latest", key: ["taskId"] },
  // args, result and run ...
});
```

The key lists scalar argument fields identifying the overwritten value. Include
all fields needed to distinguish separate values. This declaration promises
that dropping intermediate calls has no observable side effects. Do not use it
for increments, sparse patches updating different fields, audit events,
notifications or workflow transitions. Deferred work disables coalescing.
Undeclared reducers, previously sent intents and intents with downstream
causal dependents are never replaced. Replacement is an atomic durable write;
stores without that operation keep every intent.

## Opt-in membership storage

The legacy IndexedDB adapter remains unchanged. To try the engine's membership-row format in a fresh, isolated replica namespace:

```ts
import { GonvexClient, indexedDBEngineReplica } from '@gonvex/client';

const replicaStorage = indexedDBEngineReplica('my-app-fresh-engine-cohort');
const client = new GonvexClient(runtimeUrl, {
  ...existingOptions,
  localReplica: {
    ...existingOptions.localReplica,
    storage: replicaStorage,
  },
});
```

This creates `my-app-fresh-engine-cohort:engine-members-v1`. It never opens or upgrades the legacy database with the same base name. Engine participants use a separate peer channel and keep existing scope/authority, offline outbox, reducer coverage and resume rules. This option changes replica persistence only; it does not start a worker or move subscriptions off the main thread.

Use a fresh cohort namespace. This release supplies no automatic legacy import or lossless backend export. Keep the engine database when disabling the option; do not treat changing storage factories as a migration of populated data. Existing durable outbox storage and its coordination remain unchanged. Runtime and application contract changes are unnecessary.
