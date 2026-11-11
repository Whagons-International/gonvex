# 0040: Bridge support findings (K21)

Date: 2026-11-11. Status: **proposed** by K21. Items 1, 3 and 5 need a lead ruling. Item 4 needs Gabriel.
Affects: K06 (assembly), K08, K10, K15, K17, M01, M07, V02; the app repository's bridge at cutover.

Released Whagons mobile apps from 5.1.31 use the Gonvex 0.1.x SDK. They talk only to the Go bridge in
whagons5-client `compatibility/legacy-convex-v5.1.29-bridge/` (read at `origin/staging` a6de676951). The bridge
holds one upstream socket per phone and tenant and speaks the Rust-era protocol to the runtime. Everything the
kernel must do for these phones is what that upstream client sends. Evidence is in `cloudflare/bridge-support/`.

## 1. The bridge's upstream protocol, run against the kernel

The conformance scenario `bridge-upstream` replays the upstream client's frames, with line references to the
bridge source, on the fixture app. Results:

- Rust against Rust: passes 3 of 3 runs.
- Rust against the kernel: `cloudflare-reducer` backend, which has K05 sockets, K07 Reducers and the K05 control
  stand-in. Step results:

| Step (bridge source) | Kernel | Owner and planned fix |
|---|---|---|
| Dial with `?project=&tenant=`, `session.ready` first | **same as Rust** | — |
| Public Control Plane auth (`controlOnly`, `clientContract: 3`, no capabilities) | **same** | — |
| Sign-in grant fields the bridge reads (`accessToken`, `refreshToken`, `expiresAt`, `accountId`) | **same** (through the stand-in) | K06 has the real call. The bridge's own call, `exchangeExternalToken` with Firebase, is covered by `control-external-auth`. |
| Tenant auth on the same socket | **same** | — |
| Keyed `reducer.call` and its exact replay: one row | **same** | — |
| WebSocket protocol ping gets a pong (the bridge drops an upstream after 2.5 silent 25 s windows) | **same** (workerd answers pings) | Confirm on the platform in V02 |
| `control.auth.publicSettings` (`query.call`, `scope:"control"`) | `unknown Control Plane function` | K06 implements it in `cloudflare/control/`, but it is not yet behind K05's `ControlPort`: kernel assembly |
| `control.tenants.mine` (`query.subscribe`, `scope:"control"`) | same error | same |
| `control.auth.refreshSession` (`action.call`), re-auth in place, reopen under the old id | same error | same. Re-auth and reopen are K05 and K10. |
| `replica.open` with no paging capability gives one legacy `replica.snapshot`; then `replica.delta` with `upserts`/`deleted` | `replica.error` "not implemented by the kernel tenant object yet" | **K10**. The bridge reads only `replica.snapshot` (never `snapshotPage`, since it negotiates nothing), `replica.delta`, `replica.reset`/`needHashes` (reopen) and `replica.error`. |
| Live Query through `query.subscribe` follows a write | `query.error` "not implemented…" | K08's engine is merged, but it is not wired to the tenant object: K10 or assembly |
| `error.register` and `error.envelope` answered with `error.ack` | no reply (forwarded, unanswered) | **K15.** The bridge waits for the ack with its 30 s operation timeout. Without the ack, every released app's `techSupport.recordHeartbeat` and `recordClientError` call fails after 30 s. The heartbeat is also how the bridge learns the app version for forced updates (README "Forced updates"). |

`committedRevision` shows `r1` on the kernel against Rust's `r2`. That is only rank numbering: Rust's session had
already compared a snapshot cursor. It goes away with K10.

**Pre-existing break found on the base:** the `cloudflare-reducer` backend cannot provision any tenant on
`cloudflare/integration` (c8210864). K08 added the fixture table `tasks`, and the transform refuses its update
capture: `_gonvex_sync_tasks_update: expression-depth: expression depth about 111`. `receipt-lost-reply` fails the
same way. The kernel run above used a local, uncommitted copy of the fixture without `tasks`. **Proposed:** A01 or
K08 owns the fix. This note does not change the fixture.

## 2. The bridge's direct database access is already dead, and its replacement

The bridge's only database use is one `INSERT INTO "boardMessages"` (its "SELECT" is the connection ping). It is
used when a released app posts to a board whose Boards powerup is disabled. The reference runtime has no
Postgres: tenants are SQLite files, and `config.rs` refuses `postgres://` URLs. Against staging today the
fallback ends one of three ways:

- it is unconfigured, so the post fails;
- it writes to a leftover Postgres database the runtime no longer reads, so the post is silently lost;
- it cannot reach a database, so the bridge refuses to start.

Release CI never sets `LEGACY_BRIDGE_DATABASE_URL`, so CI never exercised the path.

**Replacement (prepared as an app patch, `cloudflare/bridge-support/patches/`):**

- Whagons gains the internal Reducer `legacyBridge.createBoardMessage`. Only the principal
  `whagons-legacy-bridge` on the `api` channel may call it. It re-checks the author and their right to publish in
  its own transaction, and writes no notifications, like the insert it replaces.
- The bridge calls it on a short-lived `gvx_svc_` socket bound to the tenant. The message id and the
  idempotency key derive from the phone's write intent.
- `LEGACY_BRIDGE_SERVICE_TOKEN` replaces `LEGACY_BRIDGE_DATABASE_URL` and `LEGACY_BRIDGE_TENANT_DATABASE`. The
  pgx dependency is removed.
- This works against the Rust reference now (`verify/service-principal.mjs`, 11 of 11 checks, including the
  patched bridge's own writer), so it can land before cutover.

**What K17 must provide for K21** (from `docs/service-principals.md` and `service_principal.rs`; nothing beyond
Rust):

1. `auth` with a `gvx_svc_` token binds the socket to `project` and `tenant`. An unknown credential gets an
   `auth.error`.
2. `reducer.call` may target an **internal** allowlisted function, and accepts `idempotencyKey`. A repeated key
   replays the stored result.
3. The call runs as `_gonvex_service:<id>`, with `ctx.member.id` set to that value and `ctx.invocation.channel`
   set to `"api"`.
4. A function outside the allowlist is refused with Rust's text. Subscriptions and replicas are refused.

K17's acceptance can reuse the frame-level half of `verify/service-principal.mjs`.

**Alternative for Gabriel:** drop the fallback. The path already works nowhere. A post to a disabled-powerup
board then fails with `Boards powerup is disabled for this tenant`, as it does on an unconfigured bridge today.

## 3. HTTP the bridge uses

- `/storage/` is reverse-proxied to `LEGACY_BRIDGE_GONVEX_HTTP_URL`. URLs in results whose host equals that host
  and whose path starts with `/storage/` are rewritten to the bridge's public host. Rust's `/storage/{*key}`
  routes are **adapt** in D.1 (K13), with the same path and query.
- **Requirement:** K13's signed URLs must carry the same host the bridge is configured with, the router's public
  host. If they point at another host, such as presigned R2, the bridge leaves them unrewritten. Phones then
  fetch that host directly, which works only if it is reachable and signed for direct use.
- The bridge never calls `/dev/*` or the operator routes.

## 4. Routing at cutover (M01, M07; Gabriel)

Production sends `gonvex.whagons.com/ws` to the bridge with a Traefik rule. The web app and current SDK use the
same host. When that host moves to Cloudflare, the router Worker must send the released apps' sockets to the
bridge origin. The bridge's README names the discriminator: the 0.1.x SDK always sends
`?project=3db6f499-…` (the mobile alias) or the web project id.

**Proposed for M01/M07:** a router rule that proxies a `/ws` upgrade to the bridge when its `project` parameter
is a configured mobile alias. Otherwise, move the native clients to another host. Either way the bridge's
upstream (`LEGACY_BRIDGE_GONVEX_WS_URL`) must be the kernel's `/ws`, never the public host the rule captures.
Otherwise the bridge's sockets loop back into itself.

## 5. The Convex-SDK path (apps 5.1.30 and below) can be dropped from the kernel's concerns

- The support policy refuses 5.1.29 and 5.1.30 with `UPDATE_REQUIRED` (`forcedUpdate.minimumVersion: 5.1.31`;
  retired 2026-09-30).
- Those sessions use the same upstream client as the 0.1.x path, so they add **no kernel requirement**. K21 did no
  work for them.
- Release CI still asserts the refusal with each retired SDK (`verify-retired-release.mjs`).
- **Proposed:** the kernel and K21 drop the path. Keeping `/api/{v}/sync` in the bridge after cutover costs
  nothing and keeps the refusal screen. Removing it, along with `convex.whagons.com`, means those phones see a
  connection error instead of the update message. That is Gabriel's call, and it is an app-side change either way.

## Numbers

No kernel code changed. Bundle bytes, rows read and rows written are unchanged. `bridge-upstream` runs in about
8 s against Rust.
