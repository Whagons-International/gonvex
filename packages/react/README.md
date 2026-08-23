# @gonvex/react

React bindings for Gonvex.

This package provides the provider and hooks used by generated Gonvex bindings:
`useQuery`, `useQueryResult`, `useLiveQuery`, `useLiveQueryState`, `useReducer`, `useAction`, `useEntity`,
`useReplicaCollection`, `useReplicaCollectionState`, `useReplicaEntities`,
`useRetainedLiveQuery`, and auth-aware providers.

## Install

```bash
npm install @gonvex/react @gonvex/client
```

## Usage

```tsx
import { GonvexClient } from "@gonvex/client";
import { GonvexProvider, useReducer, useQuery } from "@gonvex/react";
import { api } from "./gonvex/_generated/api";

const client = new GonvexClient("ws://localhost:8080/ws", {
  project: "my-project",
});

export function AppRoot() {
  return (
    <GonvexProvider client={client}>
      <Tasks />
    </GonvexProvider>
  );
}

function Tasks() {
  const tasks = useQuery(api.tasks.list, { status: "open" });
  const createTask = useReducer(api.tasks.create);

  return (
    <button onClick={() => void createTask({ title: "New task" })}>
      {tasks?.length ?? 0} open tasks
    </button>
  );
}
```

`useQuery` executes a read-only Query once. Persistent, continuously verified
windows use `useLiveQueryState`, which returns normalized rows plus `source`,
`completeness`, and `freshness`.

### `useQueryResult`

Use when a one-shot Query needs explicit loading, error, timeout, and retry
state. A retry can retain its last successful value while verifying again:

```tsx
const { data, status, error, isStale, retry } = useQueryResult(api.tasks.list, { status: "open" });

if (status === "loading" && !data) return <Spinner />;
if (status === "error") {
  return <button onClick={retry}>Retry: {error?.message}</button>;
}
// status success | timeout, data may still be last-good during a retry
```

Statuses: `skip` | `loading` | `success` | `error` | `timeout`.
Soft timeout default is 15s (subscription stays alive; does not reject).

### Connection state

```tsx
const { isWebSocketConnected, hasEverConnected, connectionRetries } = useGonvexConnectionState();
```

This reflects the real WebSocket lifecycle (not a stub). Reducers/Actions
reject with `GonvexClientError` on timeout or disconnect and never hang forever.

## Replica Collections

`useReplicaCollection` reads a bounded entity collection from the client's normalized
IndexedDB store, then updates it as the server resumes or snapshots the durable
Postgres cursor:

```tsx
const tasks = useReplicaCollection<Task>(api.tasks.recent, { workspaceId });
```

Read completeness and truncation from the protocol instead of guessing from a
row count:

```tsx
const state = useReplicaCollectionState<Task>(api.tasks.recent, { workspaceId });
// state: { rows, source, completeness, freshness, truncated, computedRevision }
```

Virtualized grids retain query membership as ordered IDs and resolve all rows
with one Replica subscription:

```tsx
const window = useRetainedLiveQuery<Task>(api.tasks.grid, args);
const rows = useReplicaEntities<Task>("tasks", window.ids);
```

Use `useReplicaSelector` when a component needs only derived state:

```tsx
const openCount = useReplicaSelector<Task, number>(
  api.tasks.recent,
  { workspaceId },
  (tasks) => tasks.filter((task) => task.status === "open").length,
);
```

Both hooks return `undefined` before any local/server snapshot is available and
accept `"skip"` as the args value. Selectors use `Object.is` by default and
accept a custom equality function as the fourth argument.

## Native Google auth

Enable Google for the project and generate a configured auth module:

```bash
npx gonvex auth add google --origin http://localhost:5173
```

```tsx
import { GonvexAuthProvider, GoogleSignInButton, useGonvexAuth } from "./gonvex/auth";

function Root() {
  return (
    <GonvexAuthProvider client={client}>
      <Account />
    </GonvexAuthProvider>
  );
}

function Account() {
  const { account, activeTenant } = useGonvexAuth();
  return <>{account?.email} · {activeTenant?.name}<GoogleSignInButton /></>;
}
```

The provider performs Authorization Code + PKCE against the Gonvex runtime and
attaches the resulting project-scoped session to the realtime client. Access tokens
are short-lived and refresh tokens rotate across tabs. Multi-tenant memberships are
verified by the runtime and switched with `setActiveTenant`. The provider does not
load a Google browser SDK.

## Related Packages

- `@gonvex/client` - browser WebSocket client
- `@gonvex/protocol` - shared protocol types
- `@gonvex/cli` - generated bindings and runtime sync

## Documentation

Full docs live at https://desarso.github.io/gonvex/
