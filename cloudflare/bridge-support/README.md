# Bridge support (K21)

Released Whagons mobile apps from 5.1.31 use the Gonvex 0.1.x SDK. They never talk to the runtime. They talk
to the Go compatibility bridge in whagons5-client `compatibility/legacy-convex-v5.1.29-bridge/`, which
translates for them and holds one upstream socket per phone and tenant. This directory holds what the kernel
needs for that bridge, the one app-side change it needs, and the checks for both. Findings and rulings to
request are in `docs/cloudflare/contract-changes/0040-k21-bridge-support.md`.

```text
phone (0.1.x SDK) ──/ws──▶ Go bridge ──/ws (Rust-era protocol)──▶ kernel router ─▶ gateway ─▶ control / tenant objects
                            │
                            └──gvx_svc_ service socket──▶ legacyBridge.createBoardMessage (replaces its Postgres INSERT)
```

| Path | What |
| --- | --- |
| `../conformance/scenarios/bridge-upstream.mjs` | The bridge's upstream frames, step by step with source line references, compared Rust against kernel |
| `patches/whagons5-client-bridge-service-principal.patch` | The app-side change: bridge writer, Whagons internal Reducer, tests, docs (source only; see "Apply") |
| `patches/COMMIT_MSG.txt` | Commit message for the patch |
| `verify/service-principal.mjs`, `verify/app/` | Proves the replacement against a runtime with service principals; runs the patched bridge's writer |

## What the bridge needs from the kernel

| Upstream use | Contract | Owner | State on `cloudflare/integration` |
| --- | --- | --- | --- |
| `/ws?project=&tenant=`, `session.ready` first, `auth` with `clientContract: 3` and no capabilities | A.1 | K05 | passes |
| Public `auth` (`controlOnly`), `control.auth.publicSettings` and `control.tenants.mine` with `scope:"control"` | C.1 | K06 | implemented in `control/`, not yet behind K05 |
| `control.auth.exchangeExternalToken` (Firebase) and `control.auth.refreshSession` as `action.call` | C.3 | K06 | as above |
| Unsolicited `auth.error` `session-expired` / `membership-changed` | C.3.8 | K05, K06 | covered by `session-handshake` |
| `replica.open` / `replica.close`, legacy `replica.snapshot`, `replica.delta`, `replica.reset`, `replica.needHashes` | A.2-A.4 | K10 | not yet |
| `query.subscribe` / `query.unsubscribe` (Live Queries) | A.2 | K08 + K10 wiring | engine merged, not wired |
| `reducer.call` with `idempotencyKey`; `action.call` | A.2, B | K07, K12 | Reducers pass |
| `error.register`, `error.envelope` answered with `error.ack` | A.2 | K15 | no ack yet |
| WebSocket protocol ping and pong (25 s keepalive) | A.1.8 | platform | passes locally |
| `gvx_svc_` socket, internal allowlisted Reducer, keyed replay | A.2 service row | K17 | not yet (`verify/` is its acceptance) |
| `GET/PUT/OPTIONS /storage/{*key}` on the same host as the bridge's `LEGACY_BRIDGE_GONVEX_HTTP_URL` | D.1 | K13 | open PR |
| A route for released apps' `/ws` to the bridge | — | M01, M07 | cutover |

## Run

```sh
# The bridge's upstream protocol: Rust against Rust (passes), then Rust against the kernel
cd cloudflare/conformance
node src/cli.mjs --scenario bridge-upstream
node src/cli.mjs --backend rust=rust --backend cf=cloudflare-reducer --scenario bridge-upstream

# The service-principal replacement against the Rust reference (11 checks). With --bridge, it also runs
# the patched bridge's own writer (TestServicePrincipalE2E) against that runtime.
node cloudflare/bridge-support/verify/service-principal.mjs --runtime <gonvex-runtime> \
  --bridge <whagons5-client with the patch>/compatibility/legacy-convex-v5.1.29-bridge
```

On `cloudflare/integration` at c8210864, the `cloudflare-reducer` backend cannot provision any tenant. The
fixture's `tasks` table makes the transform refuse its capture trigger (0040 §1). Until that is fixed, the
kernel run needs a local copy of the fixture without `tasks`.

## Apply the patch (Gabriel, in whagons5-client)

The patch is made against `origin/staging` a6de676951. It holds source only. Generated output must be
regenerated, never hand-merged.

```sh
git switch -c bridge-service-principal origin/staging
git apply --index <gonvex>/cloudflare/bridge-support/patches/whagons5-client-bridge-service-principal.patch
pnpm gonvex:types                 # regenerates gonvex/_generated and agent-api.* for legacyBridge.createBoardMessage
git add -A && git commit -F <gonvex>/cloudflare/bridge-support/patches/COMMIT_MSG.txt
# checks that ran green on the prepared branch:
pnpm gonvex:v2:typecheck && pnpm gonvex:functions:check && pnpm gonvex:agent-metadata:check
pnpm exec vitest run --config vitest.gonvex.config.ts --configLoader runner gonvex/module/legacyBridge.test.ts
(cd compatibility/legacy-convex-v5.1.29-bridge && go vet ./... && go test -race -skip '^TestBridgeE2E' ./...)
```

Deploy:

1. Create a credential: `token="gvx_svc_$(openssl rand -hex 32)"`, then `printf %s "$token" | sha256sum`.
2. Add this entry to the runtime's `GONVEX_SERVICE_PRINCIPALS`:
   `{"id":"whagons-legacy-bridge","tokenSha256":"<hex>","projects":["<project>"],"functions":["legacyBridge.createBoardMessage"]}`.
   On the kernel, K17 decides where this configuration lives.
3. Set `LEGACY_BRIDGE_SERVICE_TOKEN=$token` on the bridge. Remove `LEGACY_BRIDGE_DATABASE_URL` and
   `LEGACY_BRIDGE_TENANT_DATABASE`; while `LEGACY_BRIDGE_DATABASE_URL` is set, the bridge only logs that it is
   ignored.

The patch lands only at cutover, applied by Gabriel (brief rule 5); nothing from it goes to whagons5-client
before then. Gabriel decides at cutover between the patch and dropping the fallback (0040 §2, ruled in 0045); the
Postgres path already does not work against a SQLite runtime.

### What the patch changes

- `service_principal_writes.go`: `writeLegacyBoardMessage` keeps the old validations and calls
  `legacyBridge.createBoardMessage` on a short-lived service socket bound to the runtime tenant (tenant aliases
  apply). The message id is `legacy-board-message-<uuid>` and the idempotency key is
  `legacy-board-message:<intent>`, both from the phone's write intent. A phone retry or a bridge restart after
  commit therefore creates one post. Without a token, the write fails closed.
- `bridge.go`, `main.go`, `go.mod`: the tenant database, its config and pgx are removed; the
  `LEGACY_BRIDGE_SERVICE_TOKEN` config is added, and a non-`gvx_svc_` value is refused at startup.
- `gonvex/legacyBridge.ts`: the internal Reducer. It refuses every caller except
  `_gonvex_service:whagons-legacy-bridge` on the `api` channel. In its transaction it re-checks that the author
  is an active Member, that the board exists, and that the author may publish to it (tenant admin, board
  creator, or board admin directly or through a team). It applies the released post limits and stamps
  `_creationTime` with the runtime clock.
- Tests: the frames against a fake runtime (auth, path, key, entity, tenant alias, replay, allowlist and
  credential refusals, validation before dialing), an env-gated live test, and module tests for the Reducer.
- Docs: the bridge README ("Service principal"), `.env.example` and `scripts/run-local.sh`.
