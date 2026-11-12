# 0040: Bridge support findings (K21)

Date: 2026-11-11. Status: proposed by K21; **ruled in 0045**. K21 merges as part 1, and the package stays open
until assembly. `depends_on` gains K10, K15 and K17, and the final `done_when` run is V02, through the real bridge.
Items 2 (patch or fallback), 4 and 5 go to Gabriel at cutover. Item 3 is accepted.
Affects: K06 (assembly), K08, K10, K15, K17, M01, M07, V02; the app repository's bridge at cutover.

Released Whagons mobile apps from 5.1.31 use the Gonvex 0.1.x SDK. They talk only to the Go bridge in
whagons5-client `compatibility/legacy-convex-v5.1.29-bridge/` (read at `origin/staging` a6de676951). The bridge
holds one upstream socket per phone and tenant and speaks the Rust-era protocol to the runtime. Everything the
kernel must do for these phones is what that upstream client sends. Evidence is in `cloudflare/bridge-support/`.

## 1. The bridge's upstream protocol, run against the kernel

The conformance scenario `bridge-upstream` replays the upstream client's frames, with line references to the
bridge source, on the fixture app. All runs below are from committed files on `cloudflare/integration` 39066008
(K17), whose A01 `capture.ts` change fixes the fixture depth break.

- Rust against Rust: passes 3 of 3 runs.
- Rust against `cloudflare-principals`: K05 sockets in front of K06's real control object and K07's tenant object
  (the K17 harness).
- Rust against `cloudflare-reducer`: K05 sockets, the K05 control stand-in and K07 Reducers.

| Step (bridge source) | `cloudflare-principals` | `cloudflare-reducer` | Owner and planned fix |
|---|---|---|---|
| Dial with `?project=&tenant=`, `session.ready` first | **same as Rust** | **same** | — |
| Public Control Plane auth (`controlOnly`, `clientContract: 3`, no capabilities) | **same** | **same** | — |
| `control.auth.publicSettings` (`query.call`, `scope:"control"`) | **same** | `unknown Control Plane function` | K06 (already in K17's harness) |
| Sign-in grant fields the bridge reads (`accessToken`, `refreshToken`, `expiresAt`, `accountId`) | **same** | **same** (stand-in) | The bridge's own call, `exchangeExternalToken` with Firebase, is covered by `control-external-auth` |
| Tenant auth on the same socket | **same** | **same** | — |
| `control.tenants.mine` (`query.subscribe`, `scope:"control"`) | **same** | `unknown Control Plane function` | K06 |
| `replica.open` with no paging capability gives one legacy `replica.snapshot`; then `replica.delta` with `upserts`/`deleted` | `replica.error` "not implemented by the kernel tenant object yet" | same | **K10**. The bridge reads only `replica.snapshot` (never `snapshotPage`, since it negotiates nothing), `replica.delta`, `replica.reset`/`needHashes` (reopen) and `replica.error`. |
| Keyed `reducer.call` and its exact replay: one row | **same** (the reply; the delta is K10) | **same** | — |
| Live Query through `query.subscribe` follows a write | `query.error` "not implemented…" | same | K08's engine is merged, but it is not wired to the tenant object: K10 or assembly |
| `control.auth.refreshSession` (`action.call`), re-auth in place | **same** | `unknown Control Plane function` | K06 |
| Reopen the collection under the old id after re-auth | `replica.error` | — | K10 |
| `error.register` and `error.envelope` answered with `error.ack` | no reply (forwarded, unanswered) | same | **K15.** The bridge waits for the ack with its 30 s operation timeout. Without the ack, every released app's `techSupport.recordHeartbeat` and `recordClientError` call fails after 30 s. The heartbeat is also how the bridge learns the app version for forced updates (README "Forced updates"). |
| WebSocket protocol ping gets a pong (the bridge drops an upstream after 2.5 silent 25 s windows) | **same** (workerd answers pings) | **same** | Confirm on the platform in V02 |

`committedRevision` shows `r1` on the kernel against Rust's `r2`. That is only rank numbering: Rust's session had
already compared a snapshot cursor. It goes away with K10.

What is left is K10 (replicas and the Live Query wiring) and K15 (`error.ack`).

### The bridge's own live tests (`TestBridgeE2E*`)

`verify/bridge-e2e.mjs` starts a backend, deploys fixture generation 1, creates a tenant and a Member, and runs
`go test -run '^TestBridgeE2E' -v` in the bridge with `BRIDGE_E2E_REQUIRED=1`. The bridge signs in only by
exchanging a Firebase ID token. No Firebase project or token exists here, so the run used a JWT-shaped placeholder.
Result, identical on `rust` and `cloudflare-principals`:

```text
--- FAIL: TestBridgeE2ELegacyProtocolAgainstLiveGonvex
    bridge_e2e_test.go:238: message type = AuthError, want Transition; message={..."error":"exchange Firebase token:
    invalid arguments: project auth mode \"gonvex-native\" does not allow provider \"firebase\"","type":"AuthError"}
--- FAIL: TestBridgeE2EDisconnectDuringAuthResubscribeKeepsBridgeAlive   (same error, bridge_e2e_test.go:406)
--- FAIL: TestBridgeE2EReleasedFindingRetry
    finding_retry_e2e_test.go:15: isolated finding and destination team fixtures are required
```

They cannot pass here, on either backend, for three reasons:

1. They need a Firebase-mode project and a real Firebase ID token for a Member: the Firebase Auth Emulator, with a
   runtime that accepts it.
2. They are Whagons tests: they read `tasks:list` (a seeded task with a numeric `pgId`) and
   `settings:listNotifications` (a seeded notification). The finding test needs `BRIDGE_E2E_FINDING_ID` and
   `BRIDGE_E2E_TEAM_ID`. Running them on the kernel needs the Whagons bundle deployed through K19, the A01-transformed
   Whagons schema, and seeded data.
3. Past sign-in they open Replica Collections, which is K10.

They are the V02 run (0045).

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
- It is verified against the Rust reference (`verify/service-principal.mjs`, 11 of 11 checks, including the
  patched bridge's own writer). Like every app-repo change from this work, it lands only at cutover, applied by
  Gabriel (brief rule 5).

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
