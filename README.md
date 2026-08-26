# Gonvex

Gonvex is an open source realtime backend for teams that want a fast
TypeScript, Postgres, React, and realtime data workflow.

You write backend functions next to your app, Gonvex generates frontend API
references, and your React UI calls Queries and Reducers through a persistent
Local Replica.

```tsx
import { api } from "./gonvex/_generated/api";
import { useReducer, useReplicaCollection } from "./gonvex/_generated/react";

export function Tasks() {
  const tasks = useReplicaCollection(api.tasks.list, {});
  const createTask = useReducer(api.tasks.create);

  return <TaskList tasks={tasks ?? []} onCreate={createTask} />;
}
```

Gonvex keeps application state in Postgres while delivering an instant,
persistent Local Replica to web and mobile clients.

> **Status: beta.** The Rust runtime, self-hosted stack, TypeScript module execution,
> modular authentication, multi-tenant routing, Live Queries,
> Replica Collections, scheduling, and dashboard are implemented. Migration
> rollouts, fleet backup/restore, deployment automation, and a public hosted
> service are still stabilizing before 1.0.

## Why Gonvex

- **Three executable kinds**: define read-only Queries, transactional Reducers, and external-work Actions in TypeScript.
- **TypeScript client bindings**: generate stable API references, schema metadata, and React hook exports from your backend.
- **Committed realtime truth**: route exact Postgres transaction changes through one durable change feed.
- **Local Replica**: render normalized entities from IndexedDB or SQLite and apply each server transaction atomically.
- **Live Queries**: keep exact indexed server windows over datasets too large to replicate.
- **Postgres underneath**: keep your data in a database you already know how to run, back up, inspect, and tune.
- **Projects, tenants, and auth**: route isolated tenant databases, verify Members, and select Gonvex-native, Firebase, external OIDC, or hybrid project authentication.
- **Agent API catalogs**: emit a deterministic, signed inventory of interactive functions and invoke them through the acting Member's normal authorization path.
- **Background work**: commit Action outbox rows with business data and process external work after commit.
- **Operational tooling**: inspect functions, data, files, errors, metrics, connections, and scheduler health in the dashboard.
- **Self-hostable runtime**: run Gonvex with Postgres, Valkey/Redis, and optional S3-compatible object storage.
- **Open source core**: the runtime, CLI, client packages, dashboard, docs, and starter templates live in this repo.

## Gonvex v2 model

The public execution model has three executable kinds: `Query`, `Reducer`,
and `Action`. Reads are delivered through Replica Collections and Live Queries.
Applications use Reducers for durable
writes, Actions for external work, Replica Collections for bounded local data,
and Live Queries for indexed server-computed windows.

The identity model is a Control Plane plus isolated tenant databases. The
Control Plane owns Accounts, auth identities, the tenant directory and routing,
and the `account_tenant_index` projection. A tenant database owns Members and
tenant roles, teams, and permissions.

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
    index.ts
    tasks.ts
    _generated/
      api.ts
      client.ts
      react.ts
  migrations/
    tenant/
      001_initial.sql
  src/
  gonvex.json
  package.json
```

## Define your backend

Application functions are TypeScript. Versioned SQL migrations own the tenant
schema:

```ts
import { reducer, schema } from "@gonvex/module-sdk";

export const create = reducer({
  args: schema.object({ id: schema.id("tasks"), title: schema.string() }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [{ operation: "upsert", entity: "tasks", id: ["id"], value: { id: { $arg: "id" }, title: { $arg: "title" } } }],
  },
  run: ({ db }, task) => db.insert("tasks", task),
});
```

During development, `gonvex dev` watches the `gonvex/` folder, regenerates
client bindings, bundles the TypeScript module for the bounded V8 host, applies
versioned SQL migrations, and optionally runs your app dev server.

## Agent function catalogs

Interactive functions carry literal metadata in their normal declaration:

```ts
export const start = reducer({
  interactive: true,
  description: "Start a task and assign the acting member.",
  agent: { tags: ["tasks", "workflow"], confirmation: "none" },
  args: schema.object({ taskId: schema.id("tasks") }),
  result: schema.object({ taskId: schema.id("tasks") }),
  offline: { mode: "onlineOnly", reason: "requires current workflow state" },
  nonOptimisticReason: "workflow state is server-authoritative",
  run: async (ctx, args) => { /* normal authorization and business logic */ },
});
```

Generate grep-friendly NDJSON and readable TypeScript signatures from the
compiled, hashed artifact:

```bash
gonvex functions emit --format ndjson --output agent-api.ndjson
gonvex functions emit --format typescript --output agent-api.d.ts
gonvex functions check
```

An Agent Action may declare `capabilities: { functions: true }` and call
`ctx.functions.invoke({ path, args, artifactHash })`. The Rust host accepts only
functions classified as interactive, validates their schemas and active artifact
hash, rechecks tenant membership, and runs the same Query, Reducer, or Action
used by the UI. The catalog describes the callable API. It does not grant access.

## Live Queries and Replica Collections

Use a Live Query for indexed search, filters, sorting, and dynamic windows over
unbounded data. Every Live Query has a structured plan, so Gonvex derives its
dependencies and never needs declared writes or broad invalidation.

```ts
import { liveQuery } from "@gonvex/module-sdk";

export const grid = liveQuery({
  liveQueryPlan: {
    table: "tasks",
    key: "id",
    columns: ["id", "workspaceId", "title", "status", "createdAt"],
    where: { operator: "eq", column: "workspaceId", value: { argument: "workspaceId" } },
    search: { argument: "search", columns: ["title"] },
    sort: {
      columnArgument: "sort",
      directionArgument: "direction",
      defaultColumn: "createdAt",
      defaultDirection: "desc",
      allowedColumns: ["createdAt", "title"],
    },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 250 },
  },
});
```

Use a Replica Collection when the browser should keep a bounded, authorized
single-table collection locally:

```ts
import { replicaCollection } from "@gonvex/module-sdk";

export const recent = replicaCollection({
  replica: {
    table: "tasks",
    key: "id",
    columns: ["id", "workspaceId", "title", "status", "updatedAt", "deletedAt"],
    excludeWhenSet: ["deletedAt"],
    orderBy: "updatedAt",
    orderDirection: "desc",
    mode: "progressive",
    maxRows: 500,
    maxBytes: 8_388_608,
  },
});
```

Both delivery modes require one explicit, centralized visibility plan for the
source table. Gonvex injects it into snapshot and Live Query SQL and evaluates
the same plan over committed old/new rows before routing Replica changes:

```ts
import { visibility } from "@gonvex/module-sdk";

export const taskVisibility = visibility({
  table: "tasks",
  key: "id",
  sets: {
    workspaces: {
      table: "workspaceMembers",
      select: "workspaceId",
      joins: [],
      where: [{ table: "workspaceMembers", column: "memberId", context: "member.id" }],
    },
  },
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "tasks.viewAll" },
      { operator: "inSet", column: "workspaceId", set: "workspaces" },
    ],
  },
});
```

Use `{ operator: "public" }` only for intentionally public tables such as
status definitions. A module update that exposes Live or Replica delivery
without a visibility plan is rejected.

```tsx
import { useReplicaCollection, useReplicaSelector } from "./gonvex/_generated/react";

const tasks = useReplicaCollection<Task>(api.tasks.recent, {});
const openCount = useReplicaSelector<Task, number>(
  api.tasks.recent,
  {},
  (rows) => rows.filter((task) => task.status === "open").length,
);
```

Replica Collections render from a scope-isolated Local Replica, then resume
from the retained Postgres revision. Writes go through Reducers. Optimistic
transactions are overlaid on normalized entities and reconcile through the
origin command ID plus committed revision; callers can opt into offline replay with
`{ offline: "queue" }`.

## Authentication

Authentication is project-scoped. Gonvex-native auth can be enabled on its own,
or a project can exchange Firebase or external OIDC identity tokens for a
canonical Gonvex session. Tenant admission always checks the tenant database's
active `members.account_id` row.

Native Google OAuth remains available:

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

Firebase projects keep provider-specific login UX in Firebase and install the
optional client adapter instead of using Gonvex's provider buttons. Gonvex
verifies each Firebase ID token, resolves the project Account identity, issues a
rotating Gonvex session, and preserves tenant-local Member authorization.

## Self-hosting

Gonvex is designed to be self-hosted. A full deployment has:

```txt
Gonvex Rust runtime  Executes functions, serves HTTP/WebSocket traffic, routes projects and tenants
Postgres            Stores app data and Gonvex control-plane data
Valkey or Redis     Optional cache for dashboard data-explorer reads
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

For production self-hosting, put the runtime behind TLS, provide managed Postgres, configure backups and allowed origins, and add Valkey or S3-compatible storage only when you need dashboard row caching or file APIs. Production deployment automation is still early, so treat the Docker stack as the best current reference implementation rather than a finished operations guide.

`VALKEY_URL` is optional. It accelerates dashboard data-explorer reads and has
no role in reducer, change-feed, visibility, Live Query, or Local Replica correctness.

## Current scope

Gonvex currently includes:

- application execution through exactly Queries, Reducers, and Actions, with
  structured Live Queries and Replica Collections as Query delivery modes
- safe Postgres schema sync with project and tenant scopes
- generated TypeScript API and schema references
- React hooks for Queries, Reducers, Actions, auth, connection state, Live Queries, and Replica Collections
- reconnecting WebSocket client with typed failures and operation timeouts
- exact committed-change routing, canonical Live Query groups, revision-only
  freshness, unchanged suppression, and adaptive window diffs
- transparent, scope-isolated persistent Local Replica
- a normalized Local Replica backed by Postgres revisions and IndexedDB or SQLite
- multi-project and database-per-tenant routing
- pluggable Gonvex-native, Firebase, external OIDC, and hybrid authentication,
  with memberships, invitations, roles, and live session revocation
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
- organization policy, SSO administration, and audit export
- a generally available hosted control plane

## Documentation

- Docs: https://desarso.github.io/gonvex/
- Quickstart: https://desarso.github.io/gonvex/docs/quickstart/
- Installation: https://desarso.github.io/gonvex/docs/installation/
- Deployment model: https://desarso.github.io/gonvex/docs/deployment/
- Current limits: https://desarso.github.io/gonvex/docs/current-limits/
- Replica Collections and Local Replica: https://desarso.github.io/gonvex/docs/replica-collections/
- Scheduling: https://desarso.github.io/gonvex/docs/scheduling/
- Agent function catalog and attribution: [docs/agent-function-catalog-and-attribution.md](docs/agent-function-catalog-and-attribution.md)

Run the docs locally:

```bash
pnpm install
pnpm dev:docs
```

## Repository layout

```txt
apps/dashboard/          Dashboard and local integration harness
apps/docs/               Documentation site
apps/local-replica-test/  Local Replica integration harness
packages/client/         Browser WebSocket client
packages/react/          React provider and hooks
packages/protocol/       Shared TypeScript protocol types
packages/gonvex/         CLI package
packages/create-gonvex/  App initializer
packages/module-sdk/     TypeScript Query/Reducer/Action module SDK
pkg/manifest/            Runtime manifest model
templates/vite-react/    Default starter template
cmd/gonvex/              Migration-only command-line utilities
cmd/gonvex-load/         Persistent WebSocket and Reducer load runner
rust/crates/runtime/     Rust HTTP, WebSocket, Control Plane, tenant, and execution runtime
rust/crates/module-host/ Rust process supervising bounded V8 module generations
rust/crates/postgres/    Rust Postgres routing, transactions, and provisioning
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
make runtime       # run the local runtime
make dashboard     # run the dashboard app
make packages      # watch/build npm packages
make docs          # run docs at http://localhost:3001
```

## License

Gonvex is open source under the Apache License 2.0.
