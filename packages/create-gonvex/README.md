# create-gonvex

Project initializer for Gonvex apps.

Use it to scaffold a new Gonvex app with a frontend template, app-local
`gonvex/` functions, generated bindings, and local runtime configuration.

## Usage

```bash
npm create gonvex@latest my-app
cd my-app
npm install
npm run dev
```

The generated app expects a running Gonvex runtime. `npm run dev` watches and
syncs the backend and starts Vite; it does not start Postgres, Valkey, or the
Rust runtime. New local projects receive
`VALKEY_URL=redis://127.0.0.1:6380/0`; Gonvex requires that service to be
reachable when the runtime starts.

With pnpm:

```bash
pnpm create gonvex my-app
```

Choose the Vite React template explicitly:

```bash
npm create gonvex@latest my-app -- --template vite-react
```

Provision the project on an existing runtime:

```bash
npm create gonvex@latest my-app -- \
  --runtime-url https://gonvex.example.com \
  --database-mode multiTenant
```

Add native Google login in the same operation:

```bash
npm create gonvex@latest my-app -- \
  --runtime-url https://gonvex.example.com \
  --google-auth \
  --origin https://app.example.com \
  --signup-mode inviteOnly \
  --owner owner@example.com
```

`--google-auth` implies project provisioning. Production origins require HTTPS,
and invite-only creation requires the verified owner email used to bootstrap
the first workspace invitation.

## What It Creates

```txt
my-app/
  gonvex/
    index.ts
    messages.ts
    _generated/
  src/
  gonvex.json
  .env.local
  package.json
```

The generated app uses `@gonvex/cli`, `@gonvex/client`, and `@gonvex/react`.

## Related Packages

- `@gonvex/cli` - development CLI
- `@gonvex/client` - browser WebSocket client
- `@gonvex/react` - React hooks
- `@gonvex/protocol` - shared protocol types

## Documentation

Full docs live at https://desarso.github.io/gonvex/
