# Gonvex

Gonvex is an open source Convex-style backend for teams that want the same fast
app-building loop with Go, Postgres, TypeScript, React, and realtime data.

You write backend functions next to your app, Gonvex generates frontend API
references, and your React UI calls queries and mutations over a realtime
runtime.

```tsx
import { api } from "./gonvex/_generated/api";
import { useMutation, useQuery } from "./gonvex/_generated/react";

export function Tasks() {
  const tasks = useQuery(api.tasks.list, { status: "open" });
  const createTask = useMutation(api.tasks.create);

  return <TaskList tasks={tasks ?? []} onCreate={createTask} />;
}
```

Gonvex is for developers who like Convex's product shape but want infrastructure they can inspect, extend, and self-host.

> **Status: beta.** The runtime, self-hosted stack, generic Go function
> execution, native Google auth, multi-tenant routing, realtime subscriptions,
> durable sync collections, scheduling, and dashboard are implemented. Migration
> rollouts, fleet backup/restore, deployment automation, and a public hosted
> service are still stabilizing before 1.0.

## Why Gonvex

- **Go backend functions**: define queries, mutations, actions, HTTP handlers, schema, and LiveGrid-style data views in Go.
- **TypeScript client bindings**: generate stable API references, schema metadata, and React hook exports from your backend.
- **Realtime by default**: subscribe to query results over WebSockets and refresh UI when data changes.
- **Durable sync collections**: render authorized table collections from IndexedDB and resume from a Postgres change cursor.
- **Postgres underneath**: keep your data in a database you already know how to run, back up, inspect, and tune.
- **Projects, tenants, and auth**: route isolated tenant databases, verify memberships, and add native Google OAuth without a per-app identity SDK.
- **Background work**: register interval or cron jobs and schedule mutations/actions after a transaction commits.
- **Operational tooling**: inspect functions, data, files, errors, metrics, connections, and scheduler health in the dashboard.
- **Self-hostable runtime**: run Gonvex with Postgres, Valkey/Redis, and optional S3-compatible object storage.
- **Open source core**: the runtime, CLI, client packages, dashboard, docs, and starter templates live in this repo.

## Quickstart

First, start the local reference runtime in a separate checkout:

```bash
git clone https://github.com/Whagons-International/gonvex.git
cd gonvex
make stack
```

Then create a new app:

```bash
npm create gonvex@latest my-app
cd my-app
npm install
npm run dev
```

Or start Gonvex in an existing app:

```bash
npm install --save-dev @gonvex/cli
npm install @gonvex/client @gonvex/react
npx gonvex init
npx gonvex dev -- vite
```

A Gonvex app keeps backend code beside the frontend:

```txt
my-app/
  gonvex/
    schema.go
    tasks.go
    _generated/
      api.ts
      client.ts
      react.ts
  src/
  gonvex.json
  package.json
```

## Define Your Backend

Schema and functions are written in Go:

```go
package backend

import "github.com/gonvex/gonvex/pkg/gonvex"

func Schema(s *gonvex.Schema) {
  s.Table("tasks", func(t *gonvex.Table) {
    t.ID("id")
    t.String("title")
    t.String("status")
    t.Time("created_at")
    t.Index("by_status", "status")
  })
}

type ListTasksArgs struct {
  Status string `json:"status,omitempty"`
}

func Register(app *gonvex.App) {
  app.Query(
    "tasks.list",
    ListTasks,
    gonvex.Reads("tasks").Filters("status").OrdersBy("created_at"),
  )
  app.Mutation("tasks.create", CreateTask, gonvex.Writes("tasks"))
}

func ListTasks(ctx *gonvex.QueryCtx, args ListTasksArgs) ([]Task, error) {
  // Query Postgres through Gonvex APIs.
}
```

During development, `gonvex dev` watches the `gonvex/` folder, regenerates
TypeScript bindings, uploads the Go source bundle and manifest, applies safe
schema changes, and optionally runs your app dev server.

## Realtime Queries and Durable Sync

Use a live query for computed results, joins, search, aggregates, and dynamic
windows. Gonvex coalesces matching subscriptions, uses declared read/write
dependencies to avoid unrelated reruns, suppresses unchanged results, and can
send keyed-list patches.

Use a sync collection when the browser should keep a bounded, authorized
single-table collection locally:

```go
app.Sync(
  "tasks.recent",
  RecentTasks,
  gonvex.SyncTable("tasks").
    Columns("id", "title", "status", "updated_at", "deleted_at").
    ExcludeWhenSet("deleted_at").
    OrderBy("updated_at", "desc").
    Progressive().
    Budget(500, 8388608),
)
```

```tsx
import { useSync, useSyncSelector } from "./gonvex/_generated/react";

const tasks = useSync<Task>(api.tasks.recent, {});
const openCount = useSyncSelector<Task, number>(
  api.tasks.recent,
  {},
  (rows) => rows.filter((task) => task.status === "open").length,
);
```

Sync collections render from a scope-isolated IndexedDB store, then resume with
delta-only delivery when the retained Postgres cursor is still valid. Writes
still go through mutations. Mutations with generated optimistic metadata are
durably overlaid onto every matching sync or query projection and reconcile on
the authoritative mutation id; callers can opt into disconnect replay with
`{ offline: "queue" }`.

## Native Google Login

Create, provision, and wire a production-origin Google app through Gonvex itself:

```bash
npm create gonvex@latest my-app -- \
  --runtime-url https://gonvex.example.com \
  --google-auth \
  --origin https://my-app.example.com
```

Or add Google to an existing single-database or multi-tenant project:

```bash
npx gonvex auth add google --origin http://localhost:5173
npx gonvex auth doctor
```

Invite-only creation accepts `--owner <verified-google-email>` so the first
scope/workspace is usable immediately. Registered preview callbacks can be retired
with `gonvex auth remove google --origin <url>`.

This registers the app callback with the runtime and generates `gonvex/auth.tsx`
with `GonvexAuthProvider`, `GoogleSignInButton`, and `useGonvexAuth`. Apps do not
install Firebase or create their own Google Cloud project. The Gonvex installation
owns one centrally configured Google OAuth client, validates Google identity
server-side, and issues project-scoped accounts and rotating sessions using
Authorization Code + PKCE. Enabling the provider also makes the runtime enforce
authentication for that project; the sign-in screen is not the security boundary.

## Self-Hosting

Gonvex is designed to be self-hosted. A full deployment has:

```txt
Gonvex runtime       Executes functions, serves HTTP/WebSocket traffic, routes projects and tenants
Postgres            Stores app data and Gonvex control-plane data
Valkey or Redis     Required for cache, realtime coordination, and ephemeral app state
Object storage      Optional S3-compatible storage for apps that use file APIs
Dashboard           Optional web UI for inspecting projects, tables, functions, logs, and metrics
```

For local self-hosting, run the full stack with Docker:

```bash
git clone https://github.com/Whagons-International/gonvex.git
cd gonvex
cp .env.example .env
make stack
```

This starts:

```txt
Runtime:   http://localhost:8080
Dashboard: http://localhost:3000
Postgres:  localhost:5432
Valkey:    localhost:6380
S3 API:    http://localhost:9000
MinIO UI:  http://localhost:9001
```

For production self-hosting, put the runtime behind TLS, provide managed Postgres and Valkey/Redis, configure backups, set allowed origins, and use S3-compatible storage only if your app needs files. Production deployment automation is still early, so treat the Docker stack as the best current reference implementation rather than a finished operations guide.

### Telegram performance alerts

Set `GONVEX_TELEGRAM_BOT_TOKEN` and `GONVEX_TELEGRAM_CHAT_ID` on the runtime service. Both are required. Coolify's own notification settings and settings on a retired runtime application are not inherited by the active runtime container. The production Compose template forwards these optional settings only to the runtime.

- `GONVEX_ALERT_OPERATION_DURATION` defaults to `5s`. Completed server queries, mutations, actions and sync operations at or above this duration alert, whether successful or failed. These alerts work with telemetry persistence disabled.
- Client round trips and timeouts use the same threshold when the client has `telemetry: true`. Timeout reports require the updated client SDK and a working connection to deliver them. Offline clients and requests that never finish cannot be detected by a completion-based alert alone.
- `GONVEX_ALERT_TTLU` continues to cover live query invalidation propagation separately, defaulting to `5s`. A subscription's age is not treated as a slow request.
- `GONVEX_ALERT_COOLDOWN` defaults to `15m`. Slow-operation alerts are deduplicated per project, tenant, function and operation kind, including duplicate server/client reports. CPU and TTLU cooldowns remain separate. The cooldown map holds at most 1024 active keys; additional keys are suppressed until entries expire. Delivery uses the existing bounded background queue.

An 18-second workspace query and a reported 20-second timeout qualify at the default threshold. A successful 1.73-second acknowledgment does not. Set an individual threshold to `0` to disable it. Alerts identify the environment, tenant, function, duration and outcome, without request arguments, raw error messages or user identities. After merging, deploy the runtime and configure its destination before expecting notifications; client timeout coverage additionally requires shipping the updated SDK in the consuming app.

`VALKEY_URL` (or the legacy alias `REDIS_URL`) is mandatory. The runtime pings
it during startup and exits with an actionable error if it is unset, invalid,
or unreachable; there is no in-memory fallback.

## Current Scope

Gonvex currently includes:

- generic uploaded Go function execution for queries, mutations, actions, HTTP
  handlers, internal mutations, LiveGrid functions, and sync collections
- safe Postgres schema sync with project and tenant scopes
- generated TypeScript API and schema references
- React hooks for queries, mutations, actions, auth, connection state, and sync
- reconnecting WebSocket client with typed failures and operation timeouts
- dependency-aware realtime invalidation, shared subscription runners, result
  revisions, unchanged suppression, and adaptive list patches
- transparent, scope-isolated browser query cache
- durable delta sync backed by Postgres and IndexedDB
- multi-project and database-per-tenant routing
- native Google OAuth with PKCE, memberships, invitations, roles, and live
  session revocation
- recurring and one-shot scheduled work
- optional S3-compatible files, browser error reporting, and uploaded data-file
  analysis
- dashboard views for projects, tenants, data, functions, files, logs, errors,
  metrics, realtime connections, and scheduler health

Still in progress before a stable production release:

- migration previews, explicit destructive changes, rollback, and staged tenant
  rollout controls
- automated tenant database backup/restore, moves, and fleet operations
- deployment records, zero-downtime upgrade automation, and rollback tooling
- enterprise identity providers, organization policy, SSO, and audit export
- a generally available hosted control plane

## Documentation

- Docs: https://desarso.github.io/gonvex/
- Quickstart: https://desarso.github.io/gonvex/docs/quickstart/
- Installation: https://desarso.github.io/gonvex/docs/installation/
- Deployment model: https://desarso.github.io/gonvex/docs/deployment/
- Current limits: https://desarso.github.io/gonvex/docs/current-limits/
- Durable sync: https://desarso.github.io/gonvex/docs/durable-sync/
- Scheduling: https://desarso.github.io/gonvex/docs/scheduling/

Run the docs locally:

```bash
pnpm install
pnpm dev:docs
```

## Repository Layout

```txt
apps/dashboard/          Dashboard and local integration harness
apps/docs/               Documentation site
apps/query-cache-test/    Browser cache integration harness
packages/client/         Browser WebSocket client
packages/react/          React provider and hooks
packages/protocol/       Shared TypeScript protocol types
packages/gonvex/         CLI package
packages/create-gonvex/  App initializer
pkg/gonvex/              Public Go function SDK
pkg/manifest/            Runtime manifest model
templates/vite-react/    Default starter template
cmd/gonvex/              Go manifest/code-generation CLI
cmd/gonvex-load/         Persistent WebSocket and mutation load runner
server/                  Go runtime server
infra/                   Local infrastructure helpers
releases/                Versioned release notes
```

## Contributing

Install dependencies:

```bash
pnpm install
```

Start the local development stack:

```bash
make dev
```

Run the full Docker stack:

```bash
make stack
```

### Releasing npm packages

The package release runs through Make. Without `VERSION`, it selects the next
patch after both the newest `v*` Git tag and the highest checked-in publishable
package version. This keeps a stale tag from producing an already-published or
lower package version.

```bash
make version
make release-test
make release-dry-run
make release-prod
```

Set `VERSION=x.y.z` on the Make command only when choosing an explicit higher
version. The release script rejects a version that does not advance every
package and the latest release tag.

Useful checks:

```bash
pnpm typecheck
pnpm test
pnpm test:go
pnpm build
```

Useful development commands:

```bash
make services      # start local Postgres and Valkey
make runtime       # run the Go runtime with Air
make dashboard     # run the dashboard app
make packages      # watch/build npm packages
make docs          # run docs at http://localhost:3001
```

## License

Gonvex is open source under the Apache License 2.0.
