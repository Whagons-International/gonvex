# 0043: Ingress Worker (K18): adapters found, routes, tenant resolution, limits, replay, share pages

Date: 2026-11-11, revised 2026-11-12 after the lead's review of #368. Status: **ruled in 0045** (§3, §4, §6
decided there). Numbered 0043 by 0045. Affects: K06, K07, K12, K17, M01, V01, V06, `CONTRACT.md` D.1 "Webhooks
and HTTP actions" and IB4 (amended in 0045). Code: `cloudflare/router/ingress/`, plus the configured channel in
K06 (`control/src/auth.ts`, `types.ts`), K17 (`kernel/principals/grant.ts`, `tenant.ts`) and K07's channel option
(`kernel/reducer/path.ts`).

## 1. The adapters K18 replaces (0002 asked for the list)

| Webhook | Today | Action |
|---|---|---|
| Telnyx, calling | whagons-api-gateway `src/webhooks.rs` (`01e8193`, 2026-09-28), `POST /webhooks/telnyx?tenant=`. It replaced `/api/telnyx/events` of `main`'s Go backend | `calling.ingestWebhook {rawBody, timestamp, signature}` |
| GitHub | the same gateway, `POST /webhooks/github?tenant=`. The route exists but was never provisioned. It replaced `/api/github/webhook` | `github.ingestWebhook {event, rawBody, signature}` |
| Telnyx, Relay | **no adapter anywhere** | `relay.ingestTelnyxWebhook {rawBody, signature, timestamp}` |

These are the only Whagons Actions that take a provider body: whagons5-client `origin/staging` `a6de676951`,
all 138 Actions checked. The gateway forwards the body unmodified on its service socket and holds no key.
Its tests (`tests/webhooks.rs`) are the reference for status codes and the invocation.

## 2. Routes, answers, and the order of checks

The table is in `router/ingress/README.md`. The gateway's paths and `?tenant=`/`?project=` are kept, so moving
a provider is a host change only. Relay gets `/webhooks/telnyx/relay`. Status codes and the `{"ok":bool}` body
are the gateway's.

Order of checks, gateway (`webhooks.rs` `receive`/`ingest`, with axum's body limit first) against ingress:

| # | Gateway | Ingress |
|---|---|---|
| 1 | 413 body over the limit (extractor) | 405 not POST |
| 2 | 405 not POST | 400 no tenant |
| 3 | 400 no tenant | 404 unknown project (503 if no project is configured) |
| 4 | 404 unknown project | 401 a signature header missing |
| 5 | 401 a signature header missing | **413 body over the limit** |
| 6 | 400 not UTF-8, 400 no Telnyx event id | 400 not UTF-8, 400 no Telnyx event id |
| 7 | | **401 Telnyx timestamp more than 300 s off** (new) |
| 8 | 429 over the (project, selector) rate | 429 over the (project, selector) rate |
| 9 | the call: 404 unknown tenant, 401 bad signature, 503 other | **429 over the project's lookup budget** (new, only on a cache miss), then 404 unknown tenant |
| 10 | | 429 over the tenant's rate across isolates (per-tenant object), then the call: 401, 413, 404, 503 |

Every difference a caller can see:

- **413 comes after 405, 400, 404 and 401.** The gateway's extractor refuses an oversized body before the handler
  runs; the ingress checks method, tenant, project and signature headers first, so an unsigned request's body is
  never read. Oversized and also malformed in one of those ways: the ingress answers 405/400/404/401 where the
  gateway answers 413.
- **A new edge 401 for a stale or future Telnyx timestamp**, before any tenant lookup. The gateway forwards it and
  the Action answers 401, so the answer differs only when the tenant is also unknown: 401 here, 404 there.
- **429 before 404**, as the gateway: the (project, selector) bucket is drawn before the lookup, so a flood to an
  unknown selector gets 429 once its burst is spent. In the first revision the lookup came first (404 before
  429); fixed. The bucket is per isolate, as the gateway's is per process.
- **A new 429 on the lookup budget.** A selector the isolate has not resolved in the last 30 s costs one control
  lookup, from a per-project budget (default 20/s, burst 40). Past it the ingress answers 429 without a lookup,
  where the gateway would look up and answer 404 (or deliver). The provider retries a 429.
- **A second 429 per canonical tenant**, across isolates (the ingress object), which the gateway does not have.

## 3. Channel `system`, not `api` (ruled in 0045 §3)

Rust runs service-principal calls on channel and root channel `api` (`service_principal.rs:521`). Every Relay
recording Reducer refuses a root channel of `api` (`relay.ts:136,285,487`), so `relay.ingestTelnyxWebhook`
**cannot work through a Rust service socket**. It fails at `recordWebhookEvent` with "Telnyx events can only
be recorded by the verified webhook Action". The guard exists to keep the External API (root `api`) out.

**Ruling (0045):** the tenant side (K17) sets channel `system` from the principal's configuration, never from the
request. Implemented:
- K06: a principal entry takes an optional `"channel": "api" | "system"` (default `api`; any other value refuses
  the configuration). Its grant carries `channel: "system"` only when configured so, so `auth.result` and every
  existing grant are unchanged.
- K17: `serviceTenantCall` runs the call with `channel: grant.channel ?? "api"`; K07's `ExecutionAccess.channel`
  accepts `"system"`.
- The ingress sends no channel. Its principal entry sets `"channel": "system"`.

Everything else follows Rust: the member is `_gonvex_service:<id>` with role `system`, the allowlist is checked,
and the frame id is a fresh command id (`ingress_<uuid>`). `calling.ingestWebhook` and `github.ingestWebhook`
do not read the channel.

## 4. The call: K17's tenant service path (ruled in 0045 §4)

**Ruling (0045):** K17's tenant service path is the only service-call implementation. The ingress calls the
tenant object's `serviceCall(grant, {kind: "action", id, path, args})` (`kernel/principals/tenant.ts`), bound as
`TENANT`, with the grant K06 returned, and reads K17's `{ok, result}` / `{ok: false, error}`. The first
revision's own `serviceAction` port and `INGRESS_ACTIONS` binding are gone.

- **The allowlist is checked by the ingress** before the call, as K17's gateway object does: the tenant object
  trusts the grant it is handed.
- **Inline.** All four functions are `profile: "agent"`. Providers wait on the HTTP answer (GitHub gives up after
  10 s), and Rust runs service calls inline (`execute_tenant_action_with_access`). Actions reach K17's path once
  the assembly composes K12 into the tenant object (0045 "Assembly"), with K12's inline option. Until then
  `serviceTenantCall` answers its "not implemented" text for an Action, which the ingress answers as 503.
- **Error text unchanged.** The ingress maps by substring:
  - 401: "Invalid Telnyx webhook signature", "Telnyx webhook signature is invalid", "Telnyx webhook timestamp
    is outside the replay window", "GitHub webhook signature is invalid";
  - 413: "GitHub webhook payload is too large";
  - 404: "tenant is not available";
  - 503: anything else.

The test Worker's `TestTenant` stands in for the composed tenant object: K06's stub tenant plus `serviceCall`
running Actions on K04's engine, taking the channel from the grant as K17 does.

## 5. Tenant resolution

The ingress is a service principal, like the gateway (`runtime/pool.rs` `service`). It calls K06's
`ControlObject.authenticate` with its `gvx_svc_` token, the project and the `tenant` selector:

- the selector may be the tenant id or its domain (`resolve_tenant`);
- a disabled or deleted tenant answers "tenant is not available": **404**;
- the grant carries the canonical route, the allowlist and the configured channel;
- a refused credential is a deploy error: **503** and an error log, never 404.

**Cache (fixed after review).** Each isolate remembers answers for 30 s (the gateway caches unknown tenants for
30 s and never revalidates its service sockets), so a tenant that is turned off stops receiving deliveries
within 30 s. The key is the **exact** selector, as the gateway's is. The first revision lowercased it, but K06
matches `tenant_id` case-sensitively (and the domain case-insensitively): an unauthenticated
`?tenant=TENANT_ALPHA` cached a 404 that blocked deliveries to `tenant_alpha` for 30 s. Each spelling is now
its own entry; a test covers the case.

**Bounded misses (fixed after review).** Random selectors from anyone each cost a scan of the project's tenants in
the control object, which also serves sign-ins (§10). Three bounds now apply before or around the lookup:
- the (project, selector) token bucket (§2, row 8), drawn before the lookup;
- a negative cache, capped at 10,000 entries per isolate (the gateway's `MAX_UNKNOWN_TENANTS`): expired entries
  go first, then the oldest. The positive cache has its own cap, so misses cannot evict known tenants;
- a per-project lookup budget per isolate (`GONVEX_INGRESS_LOOKUP_RATE`, default 20/s with a burst of 40), spent
  only on cache misses. Past it, 429 without a lookup.

## 6. Size limits and replay protection

| Check | Rule |
|---|---|
| Body | 1 MiB (the gateway's default, and `calling`'s own cap); 5 MiB for GitHub (`MAX_WEBHOOK_BODY`). Read as a stream and cut at the limit; a larger `Content-Length` is refused unread |
| Bytes | Strict UTF-8 that keeps a leading BOM (the default `TextDecoder` drops it), so `TextEncoder` in the Action gives back the signed bytes. Not UTF-8: 400 |
| Telnyx timestamp | An integer within ±300 s. Both Actions refuse a timestamp more than 300 s old. `calling` also refuses one more than 30 s ahead and `relay` one more than 300 s ahead, so the edge refuses only what every Action refuses |
| Replay | Key: provider, signed headers (Telnyx timestamp and signature; GitHub `X-GitHub-Delivery`, event and signature) and the **SHA-256 of the body**. Ed25519 is deterministic, so without the body a captured pair could be "acknowledged" with a different body. Rules below |
| Rate | Per (project, selector) before the lookup, per isolate: 50/s with a burst of 100 (the gateway's `tenant_limiter`); share pages 20/40. The same limits again per canonical tenant across isolates, as per-object counters (0012 R11). Tenant lookups on a miss: 20/40 per project per isolate (§5) |

How the replay ledger works:
- Only a delivery the Action **accepted** is remembered: a Telnyx one until its window closes, a GitHub one for
  1 h. GitHub signs no timestamp.
- A replay answers 200 without running.
- Concurrent duplicates wait for the first delivery's answer.
- A failed delivery is not remembered, so the provider's retry runs.

The ledger lives in memory in the per-tenant `IngressObject`. Nothing ephemeral is written to SQLite (DOD), so
an evicted object forgets it, at most 10,000 entries per tenant: **about 1.9 MB** when full (measured: 10,000
entries of a 64-character key and `{state, untilMs}`, 1,855,816 bytes of V8 heap, about 186 bytes each). The Actions are idempotent per provider event
(`relayWebhookEvents` dedupes on the provider id; `calling` applies an event twice safely), which bounds a replay
after an eviction.

**Signature verification** stays in the Action (accepted in 0045 §6, which amends IB4: "signature verification
happens in the invoked Action or the Ingress Worker, never in an external adapter"). The keys belong to the app:
- calling's key is per tenant in `TELNYX_TENANTS_JSON`;
- Relay's key is `TELNYX_PUBLIC_KEY`;
- GitHub's secret is sealed in the tenant database.

The gateway holds none either. The ingress guarantees the raw body reaches the verifier byte for byte, refuses
unsigned and out-of-window deliveries at the edge, and maps a failed verification to 401.

**Memory per isolate at the caps** (same measurement): negative cache 1.9 MB, known-tenant cache 3.9 MB,
selector buckets 2.0 MB; at most about 7.7 MB, reached only under a flood of 10,000 distinct selectors.

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
compose the ingress until the assembly (0045), so there is **no P02 scenario yet**. **Ruling (0045):** the
Rust-referenced webhook scenarios are deferred to V01. The comparison 0002 asks for is in the workerd suite:

- **Adapter cases.** `ingress.test.ts` replays the gateway's `tests/webhooks.rs` cases and asserts the same
  invocation (path, args, principal) and status codes.
- **Real Whagons Actions.** `whagons.test.ts` runs the real Actions from the complete Whagons bundle on K04's
  engine behind the ingress. Built locally from a private checkout; skipped in CI.
- **Platform.** `platform/run.mjs` ran the composition on the development account.

## 9. Numbers

- **Ingress Worker bundle:** 35.5 KiB, 10.7 KiB gzip (`wrangler deploy --dry-run -c router/ingress/wrangler.jsonc`);
  33.5 / 10.2 KiB before the review fixes (limiter module, bounded caches, K17's call shape).
- **Rows:** the ingress writes none. A tenant resolution is one control-object statement with no write, which
  reads **every tenant row of the project** plus one (24 rows for 23 tenants in the test): K06's
  `resolveTenant`, copied from Rust, matches `tenant_id = $2 OR lower(domain) = lower($2)`, which no index
  serves. A resolution happens at most once per selector per isolate per 30 s, and misses are budgeted (§5).
  **Follow-up for K06:** a `lower(domain)` index (0045). The same statement serves every socket `auth`.
- **Memory:** §6.
- **Platform** (development account, test composition, deleted after each run; from arch3 over the internet):

  | Measure | First revision (2026-11-11) | Revised, run A | Revised, run B |
  |---|---|---|---|
  | Upload | 347.6 KiB, 79.5 KiB gzip | 350.1 KiB, 80.1 KiB gzip | same |
  | First delivery (objects cold) | 577 ms | 711 ms | 424 ms |
  | 30 warm Telnyx deliveries | p50 120 ms, p95 207 ms | p50 198 ms, p95 262 ms | p50 93 ms, p95 141 ms |
  | Share page | 89 ms | 239 ms | 87 ms |
  | GitHub 5 MiB delivery | 730 ms | 1,153 ms | 907 ms |

  The revised path adds one Durable Object hop (ingress object → tenant object, the real topology; the first
  revision's port ran in the same isolate). Runs A and B, 26 s apart, differ more than the revisions do, so the
  spread is the network, not the hop. Each run: 19 of 19 checks, 31 of 31 bodies reached the Action byte for
  byte and verified; wrong signature 401; stale 401; unknown or disabled tenant 404; replay ran once; 5 MiB 200
  and 5 MiB + 1 413; share page and image 200. Results (run B): `router/ingress/platform/results.json`.

## 10. At cutover (nothing changes in the app repository now)

- **Provider URLs.**
  - Telnyx: the Voice API application's webhook URL and each tenant's `webhookUrl` in `TELNYX_TENANTS_JSON` →
    `https://<ingress host>/webhooks/telnyx?tenant=<tenant>`.
  - Relay → `/webhooks/telnyx/relay?tenant=`.
  - GitHub: each tenant's hook → `/webhooks/github?tenant=`.
- **Credentials.** Add the ingress principal to `GONVEX_SERVICE_PRINCIPALS` with `"channel": "system"` and the
  four functions.

## 11. Follow-ups (from the review)

- K06: a `lower(domain)` index for `resolveTenant` (§9).
- M01: the share host serves the `.well-known` association files (§7).
- V06: a review of the anonymous share route. It runs an Action per request, bounded by the selector and tenant
  limits (20/s, burst 40) and the lookup budget, with no cache.
- The replay-ledger memory figure: measured, §6.
