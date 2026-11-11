# 0040: Ingress Worker (K18): adapters found, routes, tenant resolution, limits, replay, share pages

Date: 2026-11-11. Status: **proposed** (K18), for the lead. Numbered 0040 because 0039 is reserved for K12 (0037).
Affects: K06, K12, K17, M01, V01, V06, `CONTRACT.md` D.1 "Webhooks and HTTP actions" and IB4. Code:
`cloudflare/router/ingress/`.

## 1. The adapters K18 replaces (0002 asked for the list)

| Webhook | Today | Action |
|---|---|---|
| Telnyx, calling | whagons-api-gateway `src/webhooks.rs` (`01e8193`, 2026-09-28), `POST /webhooks/telnyx?tenant=`. It replaced `/api/telnyx/events` of `main`'s Go backend | `calling.ingestWebhook {rawBody, timestamp, signature}` |
| GitHub | the same gateway, `POST /webhooks/github?tenant=`. The route exists but was never provisioned. It replaced `/api/github/webhook` | `github.ingestWebhook {event, rawBody, signature}` |
| Telnyx, Relay | **no adapter anywhere** | `relay.ingestTelnyxWebhook {rawBody, signature, timestamp}` |

These are the only Whagons Actions that take a provider body: whagons5-client `origin/staging` `a6de676951`,
all 138 Actions checked. The gateway forwards the body unmodified on its service socket and holds no key.
Its tests (`tests/webhooks.rs`) are the reference for status codes and the invocation.

## 2. Routes and answers

The table is in `router/ingress/README.md`. The gateway's paths and `?tenant=`/`?project=` are kept, so moving
a provider is a host change only. Relay gets `/webhooks/telnyx/relay`. Status codes and the `{"ok":bool}` body
are the gateway's.

One ordering differs. The ingress checks the signature headers before it reads the body, so an unsigned
oversized request gets 401 where the gateway answers 413. An unsigned request's body is never read.

## 3. Channel `system`, not `api` (needs a ruling)

Rust runs service-principal calls on channel and root channel `api` (`service_principal.rs:521`). Every Relay
recording Reducer refuses a root channel of `api` (`relay.ts:136,285,487`), so `relay.ingestTelnyxWebhook`
**cannot work through a Rust service socket**. It fails at `recordWebhookEvent` with "Telnyx events can only
be recorded by the verified webhook Action". The guard exists to keep the External API (root `api`) out.

The ingress is the host, not an API client, so its calls use `channel: "system"` and `rootChannel: "system"`.
Everything else follows Rust:
- the member is `_gonvex_service:<id>` with role `system`;
- the allowlist is checked;
- `commandId` is fresh (`ingress_<uuid>`).

`calling.ingestWebhook` and `github.ingestWebhook` do not read the channel. **Lead:** confirm `system` for the
ingress principal only. Other service principals keep `api`.

## 4. The port K12/K17 implement: `serviceAction`, inline

`invoke.ts` defines `serviceAction({v, principal, path, args, channel, commandId}) → {ok, value} | {ok:false,
error, transient?}`. It is bound as `INGRESS_ACTIONS`. Three requirements:

- **Inline.** All four functions are `profile: "agent"`. K12's `ActionWorker.call` turns an agent Action from a
  non-anonymous session into a Workflow run with a pending id (about 2 s later start, 0037). Providers wait
  on the HTTP answer (GitHub gives up after 10 s), and Rust runs service calls inline
  (`execute_tenant_action_with_access`). So `serviceAction` must run inline and answer the result.
- **Error text unchanged.** The ingress maps by substring:
  - 401: "Invalid Telnyx webhook signature", "Telnyx webhook signature is invalid", "Telnyx webhook timestamp
    is outside the replay window", "GitHub webhook signature is invalid";
  - 413: "GitHub webhook payload is too large";
  - 404: "tenant is not available";
  - 503: anything else.
- **The allowlist is checked again** on the tenant side, as `handle_service_call` does.

The test Worker's `TestActions` is a reference implementation on K04's engine.

## 5. Tenant resolution

The ingress is a service principal, like the gateway (`runtime/pool.rs` `service`). It calls K06's
`ControlObject.authenticate` with its `gvx_svc_` token, the project and the `tenant` selector:

- the selector may be the tenant id or its domain (`resolve_tenant`);
- a disabled or deleted tenant answers "tenant is not available": **404**;
- the grant carries the canonical id and the allowlist;
- a refused credential is a deploy error: **503** and an error log, never 404.

K06 needs no change. Each isolate remembers answers for 30 s (the gateway caches unknown tenants for 30 s and
never revalidates its service sockets), so a tenant that is turned off stops receiving deliveries within 30 s.

## 6. Size limits and replay protection

| Check | Rule |
|---|---|
| Body | 1 MiB (the gateway's default, and `calling`'s own cap); 5 MiB for GitHub (`MAX_WEBHOOK_BODY`). Read as a stream and cut at the limit; a larger `Content-Length` is refused unread |
| Bytes | Strict UTF-8 that keeps a leading BOM (the default `TextDecoder` drops it), so `TextEncoder` in the Action gives back the signed bytes. Not UTF-8: 400 |
| Telnyx timestamp | An integer within ±300 s. Both Actions refuse a timestamp more than 300 s old. `calling` also refuses one more than 30 s ahead and `relay` one more than 300 s ahead, so the edge refuses only what every Action refuses |
| Replay | Key: provider, signed headers (Telnyx timestamp and signature; GitHub `X-GitHub-Delivery`, event and signature) and the **SHA-256 of the body**. Ed25519 is deterministic, so without the body a captured pair could be "acknowledged" with a different body. Rules below |
| Rate | Per tenant: 50/s with a burst of 100 (the gateway's `tenant_limiter`); share pages 20/40. Per-object counters (0012 R11) |

How the replay ledger works:
- Only a delivery the Action **accepted** is remembered: a Telnyx one until its window closes, a GitHub one for
  1 h. GitHub signs no timestamp.
- A replay answers 200 without running.
- Concurrent duplicates wait for the first delivery's answer.
- A failed delivery is not remembered, so the provider's retry runs.

The ledger lives in memory in the per-tenant `IngressObject`. Nothing ephemeral is written to SQLite (DOD), so
an evicted object forgets it, at most 10,000 entries per tenant. The Actions are idempotent per provider event
(`relayWebhookEvents` dedupes on the provider id; `calling` applies an event twice safely), which bounds a replay
after an eviction.

**Signature verification** stays in the Action. The keys belong to the app:
- calling's key is per tenant in `TELNYX_TENANTS_JSON`;
- Relay's key is `TELNYX_PUBLIC_KEY`;
- GitHub's secret is sealed in the tenant database.

The gateway holds none either. IB4's "verifying signatures on the raw body" is met as follows: the ingress
guarantees the raw body reaches the verifier byte for byte, refuses unsigned and out-of-window deliveries at
the edge, and maps a failed verification to 401. **Lead:** confirm, or rule that the ingress should verify
Telnyx itself. That would need K12's `projectSecrets` port and would duplicate the app's key layout.

## 7. Share pages

**Found:**
- Convex served `/share/task` (server-rendered, Open Graph tags) and `/share/task-image` (an SVG card) from
  `convex/http.ts`. The Go module kept them (`gonvex/task_public_shares.go`).
- Since the TypeScript v2 migration (whagons5-client `e8168dc04d`) nothing serves them. `/share/task` is only a
  web-app route that sets its meta tags from JavaScript after `taskPublicShares.getPublicPreviewByToken`, so
  link-preview crawlers, which run no JavaScript, see none. `/share/task-image` does not exist, and the internal
  docs still list it.
- The bridge README's "share pages stay on the runtime" is stale.

**Built:** both routes, server-rendered, from the same Action, called in the tenant named by `tenant` (the web
app's links carry the tenant's domain).
- **Page.** Title, description, `og:*`, `twitter:*` and `al:*` tags, facts, "Open in app"
  (`<scheme>://task-share/<token>?tenantId=<tenant>`, as the web app builds it), "Open on web" (the web app's
  own `/share/task` on `GONVEX_SHARE_WEB_URL`), and the mobile auto-open script.
- **Image.** The 1200x630 SVG card. With `GONVEX_SHARE_IMAGE_PROXY_URL`, `og:image` is a PNG through imgproxy,
  as Convex did. Without it, the SVG; many crawlers do not accept SVG.
- **Hardening.** Every preview field is escaped. The page has a strict CSP with a per-response nonce, `no-store`,
  `Referrer-Policy: no-referrer` (the token is in the URL) and `X-Robots-Tag: noindex`. The image is cached for
  300 s.
- **Errors.** An unknown token, tenant or selector, or no token, gets a 404 page. Over the rate limit: 429.
  An Action failure: 503.

**Routing (M01).** Serve the ingress on the share host and keep the web app's `/share/task` on tenant web hosts.
"Open on web" points there. Routing both to the ingress would loop. iOS and Android open the app only when
the **same host** serves `/.well-known/apple-app-site-association` and `assetlinks.json`. The web build generates
those files (`scripts/generate-task-share-associations.mjs`; Convex served them too), so M01 must route them on
the share host. Links without `tenant` (Convex-era) cannot be resolved, because tokens are per tenant database:
they get 404.

## 8. Conformance

P02 runs scenarios against backends that serve routes. Rust has none (0002), and the kernel Worker does not
compose the ingress until K12/K17 provide `serviceAction`, so there is **no P02 scenario yet**. The comparison 0002
asks for is in the workerd suite instead:

- **Adapter cases.** `ingress.test.ts` replays the gateway's `tests/webhooks.rs` cases and asserts the same
  invocation (path, args, principal) and status codes.
- **Real Whagons Actions.** `whagons.test.ts` runs the real Actions from the complete Whagons bundle on K04's
  engine behind the ingress. Built locally from a private checkout; skipped in CI.
- **Platform.** `platform/run.mjs` ran the composition on the development account.

**Follow-up:** a P02 `ingress` scenario once the kernel router composes the ingress.

## 9. Numbers

- **Ingress Worker bundle:** 33.5 KiB, 10.2 KiB gzip (`wrangler deploy --dry-run -c router/ingress/wrangler.jsonc`).
- **Rows:** the ingress writes none. A tenant resolution is one control-object statement with no write. That
  statement reads **every tenant row of the project** (24 rows for 23 tenants in the test): K06's
  `resolveTenant`, copied from Rust, matches `tenant_id = $2 OR lower(domain) = lower($2)`, which no index
  serves. With the 30 s cache that is one scan per tenant per isolate per 30 s. **Follow-up for K06:** two
  indexed lookups (id, then an index on `lower(domain)`) would make it O(1). The same statement serves every
  socket `auth`.
- **Platform** (2026-11-11, test composition, deleted afterwards; all from arch3 over the internet):

  | Measure | Result |
  |---|---|
  | Upload | 347.6 KiB, 79.5 KiB gzip (with K06 and K04) |
  | First delivery (control, ingress and Action objects cold) | 577 ms |
  | 30 warm Telnyx deliveries | p50 120 ms, p95 207 ms |
  | Share page | 89 ms |
  | GitHub 5 MiB delivery | 730 ms |

  Checks: 31 of 31 bodies reached the Action byte for byte and verified. Wrong signature 401; stale 401;
  unknown or disabled tenant 404; replay ran once; 5 MiB 200 and 5 MiB + 1 413; share page and image 200.
  Results: `router/ingress/platform/results.json`.

## 10. At cutover (nothing changes in the app repository now)

- **Provider URLs.**
  - Telnyx: the Voice API application's webhook URL and each tenant's `webhookUrl` in `TELNYX_TENANTS_JSON` →
    `https://<ingress host>/webhooks/telnyx?tenant=<tenant>`.
  - Relay → `/webhooks/telnyx/relay?tenant=`.
  - GitHub: each tenant's hook → `/webhooks/github?tenant=`.
- **Credentials.** Add the ingress principal to `GONVEX_SERVICE_PRINCIPALS`.
- **Security review (V06).** The share route is anonymous and runs an Action per request: per-tenant 20/s, no
  cache.
