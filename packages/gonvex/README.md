# @gonvex/cli

The Gonvex CLI for app-local development with the Gonvex runtime.

Gonvex gives you a Convex-style workflow with TypeScript backend functions,
generated TypeScript bindings, React hooks, realtime subscriptions,
and a local runtime backed by Postgres.

## Install

```bash
npm install -D @gonvex/cli
```

Most apps run it through a package script:

```json
{
  "scripts": {
    "dev": "gonvex dev -- vite",
    "gonvex:dev": "gonvex dev"
  }
}
```

## Commands

Authenticate with an account session or an existing personal access token:

```bash
npx gonvex login --runtime-url https://gonvex.example.com --email you@example.com
npx gonvex login --runtime-url https://gonvex.example.com --token gvx_pat_...
```

Create a scoped account token and provision a project without leaving the terminal:

```bash
npx gonvex token create "Developer CLI"
npx gonvex project create my-app
```

The default token permissions are `projects:read`, `projects:create`, and
`projects:keys:read`. Use repeated `--permission` flags or `--permission 'projects:*'`
to choose broader owner-scoped access. Dashboard administrators can combine
`projects:*` with `admin:projects` for runtime-wide project automation, or create the
same admin key from their profile in the dashboard. Account credentials are stored
per runtime in a user config file with mode `0600`; project credentials stay in the
app's `.env.local`.

Initialize Gonvex files in an existing app:

```bash
npx gonvex init
```

Watch `gonvex/`, regenerate bindings, sync the runtime, and optionally run your
frontend dev server:

```bash
npx gonvex dev -- vite
```

The manifest supports exactly Queries, Reducers, and Actions as executable
application kinds. Structured Live Queries and bounded Replica Collections are
Query delivery modes. Live Query dependencies are derived from their plans;
declared write sets do not exist. TypeScript modules are bundled by the CLI into
one self-contained, platform-neutral ESM
module before upload. The default entrypoint search starts at `gonvex/index.ts`;
configure a different project-relative path with `module.entrypoint` in
`gonvex.json`:

```json
{
  "language": "typescript",
  "module": { "entrypoint": "gonvex/index.ts" }
}
```

Build output is written to `gonvex/_build/module.js` and is not watched as source.
Unresolved packages and Node built-in imports fail the build instead of becoming
runtime imports. Registered `app.Cron`, `app.CronExpr`,
`app.TenantCron`, and `app.TenantCronExpr` schedules are loaded from that
compiled app.

Run a one-shot sync for CI or Docker builds:

```bash
npx gonvex dev --once
```

By default, `gonvex dev` streams only runtime warnings and errors to the
terminal. To tail every Query, Reducer, and Action:

```bash
npx gonvex dev --verbose-logs -- vite
```

Manage project environment variables:

```bash
npx gonvex env list
npx gonvex env get NAME
npx gonvex env set NAME value
npx gonvex env push .env.production
npx gonvex env remove NAME
```

Enable native authentication without Firebase or a browser provider SDK:

```bash
npx gonvex auth add google --origin http://localhost:5173
npx gonvex auth status
npx gonvex auth doctor
npx gonvex auth accounts
```

The command registers the exact callback with the runtime and writes
`gonvex/auth.tsx`, which exports a configured provider and hook. The hook exposes
password login plus explicit Google, Microsoft, and Apple selection. A Gonvex
operator stores provider credentials in the Control Plane. Client code receives
only public provider settings and `hasClientSecret` flags.

Configure Firebase as the project's identity provider with a project key:

```bash
npx gonvex auth configure firebase \
  --firebase-project-id whagons-prod \
  --mode firebase \
  --signup-mode inviteOnly
npx gonvex auth status firebase
```

Add `--firebase-tenant-id` for Firebase Authentication multi-tenancy. An
optional `--admin-credentials-file` uploads Firebase Admin credentials through
the trusted project endpoint. The runtime encrypts them and never returns or
prints their contents. Generic OIDC uses `auth configure external-oidc` with
`--issuer`, `--audience`, and `--jwks-url`.

For a new app, provision and wire everything at once:

```bash
npm create gonvex@latest my-app -- --runtime-url https://gonvex.example.com --google-auth --origin https://my-app.example.com
```

Both single-database and multi-tenant projects are supported. Use
`--signup-mode personal|inviteOnly`, `gonvex auth tenants`, and
`gonvex auth memberships` for workspace onboarding. An invite-only app created in
one command also takes `--owner <verified-google-email>` to bootstrap its first
scope/workspace invitation. Retire a callback with
`gonvex auth remove google --origin <url>` without disabling the provider.

`env push` resolves the file from the selected project root and atomically
replaces that project's server-side environment-variable set. Pass a dedicated
deployment env file; the CLI refuses to upload `GONVEX_PROJECT_KEY` and related
CLI credentials.

Environment commands require a runtime built from Gonvex v0.1.9 or newer. The
CLI sends the selected project key in both supported authentication headers, and
the runtime scopes that key to the exact project in the request. If environment
commands return `dashboard sign-in is required` while `gonvex dev --once` works,
upgrade and recreate the runtime; updating this npm package alone does not update
the deployed Gonvex runtime.

## Runtime Settings

The CLI reads `.env.local` and `.env`:

```txt
GONVEX_PROJECT_ID=my-project
GONVEX_RUNTIME_URL=http://localhost:8080
GONVEX_PROJECT_KEY=gvx_...
```

For Vite/browser clients, also expose:

```txt
VITE_GONVEX_PROJECT_ID=my-project
VITE_GONVEX_URL=http://localhost:8080
VITE_GONVEX_WS_URL=ws://localhost:8080/ws
```

The CLI does not start the runtime. For local development, start the repository
reference stack with `make stack`; for a remote runtime, set these values to its
public HTTPS/WSS origin.

## Related Packages

- `@gonvex/client` - browser WebSocket client
- `@gonvex/react` - React provider and hooks
- `@gonvex/protocol` - shared TypeScript protocol types
- `create-gonvex` - project initializer

## Documentation

Full docs live at https://desarso.github.io/gonvex/
