# Ingress Worker (K18)

One Worker for every inbound webhook and for the public task share pages (CONTRACT "Ingress Worker", IB4,
decisions 0002, 0043 and 0045). It holds no tenant data and no provider key. A per-(project, selector) rate
limit runs first; the control object resolves the tenant (cached by exact selector, misses budgeted); one
ingress object per tenant applies the tenant's rate limit and catches replays; and the Action runs through
K17's tenant service path (`serviceCall`), where it verifies the provider's signature on the exact bytes the
provider sent.

| Route | Action | Arguments |
|---|---|---|
| `POST /webhooks/telnyx?tenant=<tenant>[&project=<project>]` | `calling.ingestWebhook` | `rawBody`, `signature` ← `telnyx-signature-ed25519`, `timestamp` ← `telnyx-timestamp` |
| `POST /webhooks/telnyx/relay?tenant=…` | `relay.ingestTelnyxWebhook` | the same |
| `POST /webhooks/github?tenant=…` | `github.ingestWebhook` | `rawBody`, `signature` ← `x-hub-signature-256`, `event` ← `x-github-event` |
| `GET /share/task?token=&tenant=` | `taskPublicShares.getPublicPreviewByToken` | `token` (server-rendered page with Open Graph tags) |
| `GET /share/task-image?token=&tenant=` | the same | `token` (1200x630 SVG card) |

`tenant` is the tenant id or its domain. Webhooks answer as the adapter they replace (whagons-api-gateway
`src/webhooks.rs`) does:
- 200: handled, or a replay of a delivery already handled;
- 400: malformed body (not UTF-8, not JSON, or a Telnyx body without `data.id`), or a missing tenant;
- 401: unsigned, a bad signature, or a Telnyx timestamp more than 300 s off;
- 404: unknown tenant or project;
- 405: not POST;
- 413: over 1 MiB (5 MiB for GitHub);
- 429: over the (project, selector) rate, the project's lookup budget or the tenant's rate;
- 503: anything else (the provider retries).

The body is `{"ok":<status is 200>}`.

| File | What |
|---|---|
| `routes.ts` | `handleIngressRequest(request, env)`: the routes above; `null` for any other path |
| `providers.ts` | The provider table, header and event checks, the Telnyx timestamp window, Action error classes |
| `body.ts` | Bounded raw-body reads and strict UTF-8 that round-trips every byte, a leading BOM included |
| `directory.ts` | Tenant resolution: the ingress authenticates as a `gvx_svc_` service principal through K06's `authenticate`; caches keyed by the exact selector, capped; a per-project lookup budget |
| `object.ts` | `IngressObject`, one per (project, tenant): token-bucket rate limit, in-memory replay ledger, the Action call |
| `invoke.ts` | The call through K17's tenant `serviceCall(grant, call)`, and its result |
| `limits.ts` | Token buckets keyed by name and key, capped at 10,000 keys |
| `share.ts` | Share page and image rendering, escaping, links, Open Graph tags |
| `worker.ts`, `wrangler.jsonc` | The deployable Ingress Worker |
| `test/` | workerd tests: K06's control object, the ingress object, Actions on K04's engine |
| `platform/` | A platform run on the development account (`run.mjs`, results in `results.json`) |
| `whagons/prepare.mjs` | Builds the real Whagons Actions into `.build/` (git-ignored) for `test/whagons.test.ts` |

## Configuration

- **Secrets.** `GONVEX_INGRESS_SERVICE_TOKEN`, a `gvx_svc_` credential. Its SHA-256 goes into the kernel's
  `GONVEX_SERVICE_PRINCIPALS` with
  `functions: ["calling.ingestWebhook","relay.ingestTelnyxWebhook","github.ingestWebhook","taskPublicShares.getPublicPreviewByToken"]`
  and `"channel": "system"` (0045 §3: the channel comes from configuration, never from the request).
- **Projects.** `GONVEX_INGRESS_PROJECTS` lists the projects served, comma separated; the first is the default.
- **Rate limits.** `GONVEX_INGRESS_TENANT_RATE` (default `50/100`) and `GONVEX_INGRESS_SHARE_RATE` (default
  `20/40`), each `<per second>/<burst>`, applied per (project, selector) before the lookup and per tenant after
  it. `GONVEX_INGRESS_LOOKUP_RATE` (default `20/40`): control lookups on a cache miss, per project and isolate.
- **Share pages.** `GONVEX_SHARE_SITE_NAME`, `GONVEX_SHARE_APP_SCHEME`, `GONVEX_SHARE_WEB_URL` (the web app
  origin; `{tenant}` is replaced) and `GONVEX_SHARE_IMAGE_PROXY_URL` (an imgproxy base for PNG cards).
- **Bindings.** `CONTROL` (the kernel's control object), `TENANT` (the kernel's tenant objects, for `serviceCall`)
  and `INGRESS` (this Worker's objects).

## Commands (run in `cloudflare/`)

```sh
vitest run --root router/ingress            # also part of `pnpm test`
tsc -p router/ingress/tsconfig.json --noEmit # also part of `pnpm typecheck`

# The real Whagons Actions behind the ingress (local only; the app is private):
node spikes/s2/scripts/build-artifact.mjs <whagons5-client checkout>
node router/ingress/whagons/prepare.mjs spikes/s2/.artifact
vitest run --root router/ingress            # test/whagons.test.ts now runs instead of skipping

# Platform run (deploys gonvex-cf-dev-k18-ingress, runs, deletes it):
whagons-dev credentials exec full-cloudflare-access -- node router/ingress/platform/run.mjs
```
