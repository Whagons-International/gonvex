# Router: `/dev/*` (K19)

The router Worker's half of the deploy and operator routes. The control-object half, the release builder and
the full description are in `cloudflare/control/operator/README.md`; decisions in
`docs/cloudflare/contract-changes/0036-k19-deploy-and-operator-routes.md`.

| Path | What |
| --- | --- |
| `routes.ts` | `handleDevRequest`: `/healthz`, the `/dev` CORS layer (OPTIONS 204, headers on every response), `/dev/sync` (stream the body into R2, the control object's `devSync`, then deploy the Worker version), forwarding of every other `/dev/*` request and operator socket to the control object, and the release builder's blob route `/_gonvex/k19/blobs/<key>` |
| `rollout.ts` | The rollout queue consumer: one tenant per message, `applyRelease` once then poll, report to the control object |
| `local/` | **The local kernel**: `worker.ts` (router, queue consumer, control object, tenant object), `tenant.ts` (`LocalKernelTenant`: K01 + K02 + K03 + K04 behind `OperatorTenantPort`), `wrangler.jsonc`, `start.mjs` (kernel + builder), `e2e.mjs` and `dashboard.mjs` (done_when) |
| `test/` | workerd tests against the local kernel (`vitest run --root router/dev`) |

## Local kernel

```sh
cd cloudflare
node router/dev/local/start.mjs --port 8787        # prints the URL and the admin key; state in ~/.cache/gonvex-k19-local
curl -X POST localhost:8787/dev/projects -H "authorization: Bearer <admin key>" \
     -H 'content-type: application/json' -d '{"name":"app","databaseMode":"multiTenant"}'
(cd <app> && gonvex dev --once --runtime-url http://127.0.0.1:8787 --project-id <id> --key <projectKey>)
```

`wrangler dev` reloads on any file written under `router/dev/local/`. The builder stages release modules under
the state directory and only rewrites `local/.generated/releases.js`, the one module the kernel imports for
them, when a version goes live. Keep other writes out of that directory while the kernel runs.

## done_when

```sh
node router/dev/local/e2e.mjs --app <whagons5-client checkout> --work ~/work/k19-e2e/run --keep
node router/dev/local/dashboard.mjs --report ~/work/k19-e2e/run/report.json --playwright <dir containing node_modules/playwright>
```

`e2e.mjs` creates a project, runs the repository's CLI (`gonvex dev --once`), provisions a tenant from the
active release, syncs again through the queued rollout, waits for the new Worker version and checks that the
tenant object installs the Whagons release, then reads the operator routes the dashboard uses.
`dashboard.mjs` builds `apps/dashboard`, serves it with its own `server.mjs` against the kernel, signs in in
headless Chromium (`--disable-gpu`) and visits projects, data, logs, errors and settings, recording every
`/dev` response and screenshot.
