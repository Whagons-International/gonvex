# Option A: the tenant object holds the client's tenant socket

Status: design and feasibility only (2026-11-20). Nothing here is implemented. It is written so Gabriel can choose
between option A, option D (the frontend sends fewer waves) and option E (re-baseline the rule) from
[X17 §4](../verification/X17-durable-object-latency/README.md#options-that-would-remove-the-floor-decision-for-gabriel).
Base: `cloudflare/integration` at `7c5dd4b6` (#545).

## Summary

| Question | Answer |
|---|---|
| Expected cold-load saving | **≈250–500 ms per cold load, central estimate ≈350 ms**, about the same on -12, -11 and -04 (they share the same step shape). This is less than X17's "−400 to −700 ms": about half of today's gap is on the *control* socket (connect, token exchange, control-only auths, `tenants.mine`), which option A does not touch. Realtime create saves ≈40–80 ms more (one dispatch and one delivery leg on each of A's and B's sockets). |
| Does it close the cells? | Against the **b29 interim** cold-load medians (integration `ca4a2b89`, raw files only, report not out), what is left over Rust_RTT + 300 ms is -12 +323 ms and -11 +181 ms; -04 already passes. A would very likely close -11 and probably close -12, without margin on -12. It does not close -11 cold create (dominated by the external LLM call). |
| Possible without a frontend change? | **Yes, for web clients on a tenant subdomain**, which includes every measured cell. The socket URL carries no tenant (`wss://<runtime>/ws?project=whagons-5`), but the browser's `Origin` header does (`https://<tenant>.<base>`; `http://devcopy-stg1117-12.localhost:<port>` in the harness). The router can route the application socket by `Origin` and confirm it at `auth`. Hand-off after `auth` and socket transfer are not possible on the platform. Native apps, the landlord domain and `/share/task` keep today's path. |
| Total effort | **Full A: ≈8–10 engineer-weeks. Hybrid "A-lite": ≈4–5 weeks** (milestones below, each behind a flag). Both include review cycles at the current pace. |
| Top 3 risks | (1) **Every app publication drops every tenant-held socket.** Tenant objects live in the runtime Worker, which adopts app code with the `immediate` strategy; sockets would reconnect and resume instead of getting Rust's in-place `system.reload`. (2) **One object's outbound frame ceiling** (S3: ≈10–14k frames/s, then the platform closes all sockets with 1013) and its 128 MB isolate, now shared with the tenant's own CPU-bound opens and commits. (3) **The auth/revocation model is re-hosted**: leases, the attach check, revalidation, Control watchers and Action reply routing move to a new holder kind, and the new holder takes unauthenticated sockets. |
| A cheaper hybrid? | **Yes: A-lite.** Only the application socket moves, only for `Origin`-routed web clients and tenants under a socket cap. Tenants over the cap fall back to gateways at connect; there is no live sharding. Publication drops are accepted (with a jittered close and the SDK's resume). It captures essentially all of A's latency gain for the measured cells at about half the cost. D (days, but changes the frozen frontend) would save ≈150–300 ms; E costs nothing. |

Recommendation, for the decision: if exact Rust parity on cold load is not a hard product requirement, **E (or D
when the frontend unfreezes) is far cheaper** than A for a remaining gap of ≈180–320 ms. If A stays on the table,
start with **M0 (3–4 days, a separate dev Worker, no frontend change)**. M0 measures A's actual saving and the
publication behavior before the 4–5 weeks of A-lite.

## 1. Which steps move in-process

### 1.1 The cold load is two sockets, not one

The staging frontend (whagons5-client `678bdc80`, read-only) creates two `GonvexClient`s on the same URL
(`src/providers/GonvexClientProvider.tsx`):

- **The application socket** (`gonvex`). It is opened at page load, before sign-in, and sends a control-only
  anonymous `auth` and `control.auth.publicSettings`. After sign-in it re-authenticates to the tenant
  (`auth` with `token` and `tenant`) and carries every tenant frame of the load. It also carries a few
  `scope:"control"` frames (`control.developer.status`).
- **The control socket** (`controlClient`, `role:'control'`). It is created after the Firebase sign-in and carries
  `control.auth.exchangeExternalToken`, the control-only token auths and `control.tenants.mine`.

Option A moves the application socket. The control socket's steps stay Control steps whichever object holds the
socket: Control is one global object, not the tenant.

### 1.2 Where the gap is (one cache-hit -12 cold load)

CF dev x18h (`bcb23689`, the b28 release), probe `/home/gabriel/work/gonvex-x17/ev/cf-12-x18h-gw2.json`, spans
`logs/cf-12-x18h-gw2.jsonl` and `logs/cf-12-x18h-cold2.jsonl`; Rust_0 `ev/rust-12-x17ref-prof.json` (the b28 budget
sheet's reference). ms from the sign-in click.

| Segment | Socket | Server path today | CF | Rust_0 | Δ | Under A |
|---|---|---|---|---|---|---|
| Firebase sign-in, then the control socket's connect | control | router → controller → limiter → gateway | 767 | 451 | +316 | unchanged (#538 already cuts ≈100) |
| control-only `auth` + `exchangeExternalToken` | control | gateway → Control (Firebase `accounts:lookup`, tenant admits, session write) | 266 | 22 | +244 | unchanged (#537, #530) |
| exchange answered → application socket's tenant `auth` answered | both | 2 control token auths, `tenants.mine`, the app socket's tenant auth (Control `sessionAuthenticate` 57 + epoch 12) | 229 | 25 | +204 | only the app socket's auth gains ≈30–40 ms (#539, #540 and #542 cut the rest) |
| tenant `auth` answered → cohort `openMany` sent | app | 6–7 dependent tenant steps (below) + `developer.status` (Control) | 1085 | 905 | +180 | **the main gain** |
| cohort sent → rows | app | cohort opens, `listWindow`, `files.getFileUrls`, the `recentWorkspaceTasks` open | 1014 | 1101 | −87 | small gain (delivery transit), uncertain |
| **Total** | | | **3361** | **2504** | **+857** | |

The same split holds in the other cache-hit -12 profiles (x18j-gd2, x18k ab2): from the sign-in click to the
application socket's tenant `auth` answer takes 1262–1654 ms on CF against 499–865 ms on Rust. All of that but ≈70 ms
of the app socket's own auth is Control-path work, and it was ≈+750–1,150 ms of each b28-era load. #537–#540 and
#542 have cut ≈250–400 ms of it since.

### 1.3 The tenant steps, measured

Each tenant step today is client → gateway → tenant dispatch RPC → tenant → gateway delivery RPC → client. Under A it
is client → tenant → client: one frame to a fresh object costs 12–14 ms (X17 `step.mjs` "direct"), against 47–51 ms
for the delivery-shaped step ("deliver"). That is a floor of ≈35 ms per step, before the dev overheads around it
(gateway turn waits, X02 deferrals, delivery transit).

Answer latency per dependent step on the application socket, ms (CF cache-hit -12 profiles x18h-gw2 / x18j-gd2 /
x18k-s2; Rust_0 x17ref-prof):

| Step (in order) | Kind | CF | Rust_0 | Under A |
|---|---|---|---|---|
| tenant `auth` | Control session + epoch, tenant admit | 69–72 | 6 | tenant → Control (2 DO legs) stays, admit in-process: −30 to −40 |
| `openMany` (2) | tenant | 54 / 88 / 76 | 25 | in-process |
| `openMany` (4) | tenant | 38 / 72 / 140 | 15 | in-process |
| `subscribeMany` `control.developer.status` | **Control** | 105 / 87 / 193 | 10 | tenant → Control: unchanged |
| `reducer.callMany` `presence.transition` | tenant, durable commit | 115 / 199 / 238 | 13 | in-process; the confirmed-write wait (≈30–50) stays |
| `openMany` (4) | tenant | 54 / 66 / 70 | 16 | in-process |
| `openMany` (1) | tenant | 47 / 57 / 70 | ≈5 | in-process |
| `presence.subscribe` | tenant | 71 / 75 / 66 | 31 | in-process |
| cohort `openMany` (48–55) | tenant, CPU-bound | 265 / 288 / 333 | 62 (25-open) | delivery transit removed; tenant CPU unchanged |
| `query.subscribe` `listWindow` | tenant | 312 / 568 / 696 | 329 | in-process; waits behind the cohort's CPU either way |
| `recentWorkspaceTasks` `openMany` | tenant | 137–405 | 8 | in-process |

Per profile, the removable part of the six pre-cohort tenant steps is the CF answer minus (Rust_0 answer + a 13 ms
direct frame), less ≈30 ms of durability on the reducer: **≈165 / 345 / 445 ms**. With the tenant auth (≈30–40) and
an uncertain 0–150 ms in the cohort phase (gateway receive transit was 360–650 ms in cache-miss loads, but X17
found the cached cohort at par: 1.3 s vs 1.2 s first open → last ready), that gives the **≈250–500 ms, central ≈350
ms** of the summary.

Per tenant: every profile of -12, -11 and -04 has 7–9 pre-cohort steps on the application socket, so the estimate
is the same order for all three. The -11 and -04 profiles available (x17x, x18a) predate #532 and show larger
per-step overheads (presence reducer 275–407 ms). Part of that is tenant CPU contention, which A does not remove, so
they are not used for the estimate.

### 1.4 What it would close

| Cell | b28 gap over the rule | b29 interim (CF / Rust_RTT median) | b29 left over the rule | A (central ≈350) |
|---|---|---|---|---|
| load -12 cold | +967 | 3496 / 2873 | **+323** | probably closes, no margin |
| load -11 cold | +1022 | 3226 / 2745 | **+181** | closes |
| load -04 cold | +846 | 3612 / 3703 | passes (−391) | n/a |
| create (all) | +34 to +959 | not yet | | −40 to −80 per create; -11 cold create stays dominated by the LLM call |

b29 interim: `/home/gabriel/work/gonvex-c3-walkthrough-20261117/batches/b29/ev/wsload-b29-W{04,11,12}-*.json`
(x18m = integration `ca4a2b89`, which carries #537–#540, #542 and #543), n = 3 cold per backend, the first CF load a
snapshot-cache miss. The batch was still running at the time of writing. Use the b29 report when it is out.

## 2. Routing the application socket without a frontend change

### 2.1 What the staging frontend sends at connect

- URL: `gonvexWebSocketUrl(VITE_GONVEX_URL, 'whagons-5')` → `wss://<runtime>/ws?project=whagons-5`, fixed at module
  load for both clients (`src/utils/gonvexWebSocketUrl.ts`). **No tenant in the URL.** The public-form client adds
  `public=1`.
- Headers: the browser's own: `Origin: https://<tenant>.<base>` (the app runs on the tenant's subdomain:
  `src/hooks/useTenant.ts` `getTenantFromHostname`), `Sec-WebSocket-Extensions` (deflate), cookies for the runtime's
  domain only (a different site from the app, so no usable routing cookie).
- First frames: the application socket's control-only anonymous `auth` + `publicSettings` at page load; after
  sign-in, `auth` with `token`, `tenant` (the subdomain selector) and `capabilities`.

### 2.2 The options

| Option | Possible? | Why |
|---|---|---|
| The socket URL carries the tenant or domain | **No** (frontend change) | The URL is built once from `VITE_GONVEX_URL` and the project. Adding `tenant=` means a new frontend build, and `@gonvex/client` reconnects to `this.url` unchanged. |
| **`Origin` header as the routing hint** | **Yes** | Every browser sends it on the handshake, and it names the tenant subdomain on every measured page. It is a placement hint, never authority: the `auth` frame still decides the tenant, and a mismatch is handled (§2.3). Not available for native apps (no browser `Origin`), the landlord domain or `/share/task?tenant=`; those keep the gateway path. |
| The gateway hands the socket off after `auth` | **No** | The platform cannot move an accepted WebSocket to another object. The gateway can only proxy it (today's hop) or close it. A close-and-reconnect costs a full connect (≈300 ms), more than it saves on a cold load. The client reconnects to the same URL, so the router would also need a sticky hint per address. |
| A hibernatable-WebSocket transfer | **No** | `acceptWebSocket` binds the socket to the accepting object, and hibernation preserves it only within that object ([S3](../spikes/S3-sockets.md), "Hibernation, state rebuilt").
There is no transfer API. |
| A second socket only for the tenant stream | **Already the shape** | The frontend has two sockets. Routing the application socket to the tenant (by `Origin`) while the control socket stays on gateways *is* this split, with no client change. A dedicated third socket would need an SDK and frontend change. |

### 2.3 Routing rules (router Worker)

1. `?public=1`, no `Origin`, an `Origin` that is not a tenant subdomain of a known base, or the flag off → today's
   path (controller → gateway).
2. Otherwise resolve `(project, selector)` → `tenantId` from a colo Cache API entry (as X17i). On a miss, ask
   Control (`sessionPublicTenant`-shaped, cached 60 s, negative entries 10 s). Unknown selector → gateway. The router
   never creates a tenant object for an unresolved name.
3. The tenant's routing record says local sockets are allowed and under the cap (§3.3) → `TENANT.get(id).fetch`
   with the same upgrade headers the gateway gets today (connection id, address, slots, public flag). The K16 address
   limiter reserves the pending slot first, as today. Over the cap → gateway.
4. At `auth`:
   - the session's tenant is the host tenant → served in-process;
   - control-only `auth` → served (Control frames are forwarded as a gateway forwards them);
   - another tenant (`Origin` and `auth` disagree, or a later re-auth to another tenant) → full A: served as a
     gateway serves a foreign tenant (RPC dispatch and delivery). A-lite: the socket is closed with 1012 and a
     per-address routing hint (60 s) sends the reconnect to a gateway. In this frontend that only happens on
     `/share/task`, which already routes to the gateway.

## 3. What moves into the tenant object, and what stays

The session layer is already placement-neutral: `SocketHost` "hosts sessions on hibernatable WebSockets inside one
Durable Object" and `Session` is "placement-neutral" (`kernel/session/README.md`), and the delivery package already
has a `LocalOutlet` for sockets in the tenant object (used in tests). What is gateway-specific is
`GatewayObjectBase` (`kernel/session/placement/gateway.ts`): the tenant index, the replica receivers
(`deliverReplica*`, recovery and bootstrap barriers), `presence`, `controlChanged`, capacity and drain, admission
and CallOrigin. M1 extracts that holder logic so a tenant object can embed it.

### 3.1 Responsibilities

| Concern | Today | Under A (tenant-held application socket) |
|---|---|---|
| Upgrade, pending-auth deadline, size limits, close codes, liveness auto-response | gateway (`SocketHost`) | tenant object, same `SocketHost` and alarm multiplexed with the tenant's alarm (scheduler, presence grace, keepalive) |
| `auth`: token → identity | gateway → Control (`sessionIdentity` / `sessionAuthenticate`), identity lease (#539) | tenant → Control, with the same lease store held by the tenant holder |
| `auth`: tenant admission | Control → tenant `admit`, or the leased-session path → tenant `admit` (two legs) | in-process `admit` on the tenant's own `members` row |
| Session state (`SessionRecord`, recovery references) | gateway attachments | tenant attachments, same format (≤16 KiB) |
| Revalidation (30 s round), public admission recheck (K16) | gateway alarm → Control `sessionRevalidate` | tenant alarm → Control, one batched call per round |
| Tenant frames (`replica.*`, `query.*`, `reducer.*`, `action.*`, `presence.*`) | gateway → tenant `dispatch` RPC | in-process `dispatch` |
| `scope:"control"` frames | gateway → Control (`sessionDispatch`), control-read cache (#542) | tenant → Control, same cache class per holder |
| Replica outlet | `GatewayOutlet`: one ordered RPC channel per gateway, credits, stall ticks | composite outlet: **one local holder per socket** (in-process, ordered), `GatewayOutlet` for gateway-held sockets of the same tenant |
| Catch-up, deferral, `change-feed-lagged` | per holder (gateway) | per local socket. This is closer to Rust's per-socket `defer_feed`: one slow client no longer defers its gateway neighbors. |
| Instance-change recovery (`replica.reset` after a tenant restart) | the gateway notices the new instance | not needed for local sockets: they close with the instance and resume by cursor and digest on reconnect (Rust's restart semantics). Still needed for gateway-held sockets. |
| Presence fan-out | tenant → one RPC per gateway with subscribers | local subscribers in-process; gateways as today. The rebuild after a wake reads local attachments plus the gateways. |
| Control notices (`controlChanged`, `presenceChanged`, `moduleReloaded`) | Control → watching gateways (`watch(project,"GATEWAY",id)`) | Control → tenant holders too: `watch(project,"TENANT",id)`. The binding argument is already generic. |
| Action replies (`settleActionReply`, `actionFinished`) | ActionHost → gateway named by the reply's `host` | the reply's `host` names a holder address `{kind, name}`. Today it is a bare gateway name. |
| Capacity (sockets, frames/s, bytes/s), controller observation | gateway → controller cell | tenant cap and pressure in the tenant (§3.3). No controller lease: the tenant is not a pooled holder. |
| Scale-in drain (BUG-9) | controller drains gateways in cohorts | not applicable to tenant-held sockets. A *shed* path moves sockets off an overloaded tenant (jittered 1012 + routing record → gateway). |
| K16 limiter audit (`liveConnections`) | asks gateways | asks the holder named in the slot: gateway or tenant |
| Tenant delete / transfer / operator reset | tenant + gateway sockets reset | the tenant also closes its own sockets (1011/1012) |
| Multi-tenant users switching tenants | re-`auth` on the same socket, any tenant | navigation to another subdomain (new page, new `Origin`, new socket); in-place re-auth to another tenant as §2.3 |
| Control socket | gateway | **unchanged** (gateway) |

### 3.2 Unchanged

The controller, limiter, gateway pool and drain stay for control sockets, public sockets, native clients and
overflow. Control and its exchange, admission cache and notices stay. The reducer, query and replica engines stay;
they already run in the tenant.

### 3.3 Capacity in one object

A tenant holder takes a socket at connect only if all of these hold. Over any of them, the router sends the socket to
a gateway.

- local sockets < `TENANT_LOCAL_SOCKETS` (start at 125, the gateway's `MAX_SOCKETS`; M0/M5 measure it);
- pending (unauthenticated) local sockets < 16;
- the tenant's last-minute pressure is under the gateway's `pressured()` thresholds (4,000 frames/s,
  4 MiB/s) *and* its own invocation queue age is under a bound (e.g. 250 ms p95), because opens and commits now share
  the thread with socket I/O.

## 4. Limits and risks of one single-threaded object

### 4.1 Sockets

- **-12's peak socket count: not known from this repository's evidence.** The dev copies hold only the test members,
  and this work did not read staging (out of bounds for this task). M0's first input is the peak concurrent
  application sockets per tenant from Rust staging's connection metrics, read-only, with Gabriel's approval.
- What one object can hold (S3, measured): **1,000 sockets ≈ 8.5 MB** (≈6.7 KB each). Memory is not the socket
  limit.
- What it can push: **≈10 fan-outs/s to 1,000 sockets (≈10–14k frames/s)**. Beyond that the queue grows without any
  signal, and at 60/s the platform closed all 1,000 sockets with `1013`. 8 gateways × 125 carried 30/s. A tenant
  holder must keep `sockets × commits/s` well under that ceiling, e.g. ≤4k frames/s: 125 sockets at 30 commits/s,
  or 400 sockets at 10 commits/s. Hence the cap and the gateway overflow.

### 4.2 CPU per frame

- What moves in with the socket: client-frame handling ≈2–3 ms CPU per frame (cold2: 35 socket events, 114 ms),
  delivery receive 0–65 ms per batch (median ≈0–1). The gateway total was ≈0.2–0.7 s per cold load. The tenant's own
  share of a -12 cold load is ≈0.6–1.2 s (cohort opens ≈7 ms each). So **a cold load costs the tenant ≈20–50 % more
  CPU**, on the same thread that serves every other client of the tenant.
- Steady state: a 2 KB fan-out costs ≈3.8 µs per send (S3), ≈9.6 µs with deflate (compression is billed to the
  object). 125 sockets × 30 commits/s ≈ 36 ms/s with deflate: affordable.
- The real exposure is head-of-line blocking. A 400 ms cohort open delays every local socket's frames in that
  window. Pings are not delayed: the platform answers them by auto-response. Today the same open delays that tenant's
  RPCs, so the effect is similar, except that gateway-side work (auth, Control frames, batching) no longer overlaps
  with it.

### 4.3 Hibernation and code updates (the structural issue)

- Tenant objects live in the **runtime Worker, which adopts app code with the `immediate` strategy**. This is
  deliberate: "the runtime exports no client socket owner"
  ([development-runtime-isolation.md](../development-runtime-isolation.md)), so publications never drop sockets.
- Platform rule: "A forced code update disconnects WebSockets attached to the running instance. This happens when a
  `deferred` code update's `max_delay` is reached before hibernation, or when the code update strategy is set to
  `immediate`." A subscribed tenant never hibernates: the BUG-2/BUG-9 keepalive holds it awake while any subscription
  exists. So **every publication closes every tenant-held socket of every tenant (1006)**. S3 measured 1,000 sockets
  back in 4.1–6.0 s. Rust keeps the sockets and sends `system.reload`.
- Mitigations, cheapest first:
  1. Accept it, but make it orderly. Before activation, Control asks each tenant to close local sockets with 1012 at a
     jitter of ≤2 s. Clients resume by cursor and digest (warm-load cost ≈1.4 s), and a client whose contract changed
     gets `client.updateRequired` at its next `auth`.
  2. The Facet layout of [S5](../spikes/S5-worker-loader.md): sockets in a kernel-Worker tenant object, app code in a
     per-generation facet. It keeps sockets across generations, but costs a 0.4–1.1 s generation load on every wake,
     its switch protocol failed intermittently, and Dynamic Workers are beta. Not recommended now.
- Tenant resets (memory, CPU limit, `fixtureRestart`) close the local sockets too: Rust's process-restart semantics,
  at tenant granularity.

### 4.4 Memory (128 MB isolate, 32 MiB caps)

- Each local socket adds its `Session` and delivery state (≈7 KB of socket plus its replica tracking, ≈80 replicas
  on -12) to the tenant's isolate. The isolate already holds subscriptions, SQLite page cache, the snapshot cache
  (#528 budget 24 MB) and initial admissions (32 MiB isolate budget, `initial-admission.ts`).
- The risk is outbound frames in flight: a -12 cohort is ≈3.9 MB compressed (6.8 MB decompressed) per cold load, and
  `WebSocket.send` gives no back-pressure (S3). Today those bytes are bounded by RPC credits and sit on gateways.
  With local holders, ten concurrent cold loads (or a reconnect storm after a publication) put tens of MB in one
  isolate. Mitigation: a per-socket and isolate-wide byte budget on the local outlet, reusing the engine's credits.
  Pages wait for room exactly as they wait for RPC credits today.
- A frame stays under the platform's 32 MiB message cap. Delivery batches no longer cross the 32 MiB RPC argument cap
  (local), but frames must.

### 4.5 When a tenant outgrows one object

A tenant's data is one SQLite object; writes cannot be sharded. Sockets can:

1. **Overflow to gateways (the design).** Sockets beyond the cap, or arriving under pressure, take today's path and
   pay the hop. A mixed outlet already serves both. Shedding moves sockets back out in jittered cohorts.
2. Per-tenant delivery shards fed by the change log (a holder per N sockets). This is today's gateway design under
   another name, so it gains nothing over (1).
3. A replica at the holder (option B) to answer opens without the tenant: weeks, with ACL and consistency risk.

So A is a fast path for tenants that fit. Large tenants keep correctness and today's latency.

## 5. Invariants to preserve

| Invariant | Source | How A keeps it |
|---|---|---|
| The tenant is decided by the `auth` frame, never by placement | 0004, K05 | `Origin` only selects the holder. Every `auth` runs `authenticate` unchanged, and a foreign tenant is served or re-routed (§2.3), never answered from the host tenant's data. |
| Exchange admission cache keyed `(member_id, revision, updated_at)`, 1 h TTL, active tenant live | #530 | Control-side; unchanged. |
| Identity leases ≤10 s, only `gvx_session_*`, never past `expires_at`, never impersonation/developer/delegated/anonymous; revocation pushed by `controlChanged`; a Control restart (`runtimeInstance`) drops them; the holder that forwarded a Control write drops its own at once | #539 | The tenant holder owns an `IdentityLeases` store (one per holder object, as now), registers with `watch(project,"TENANT",id)` **before** it leases, and implements `controlChanged` (drop leases and control-read cache, then refresh). The TTL backstop is unchanged. |
| A tenant auth right after sign-in reuses the sign-in's live admission (≤5 s, row-bound); after a tenant refusal, Control admits `liveOnly` | #540 | Control-side; unchanged. The leased path's `tenant.admit` becomes an in-process read: same row, same turn, at least as strict. |
| The tenant checks the session's Member at attach (`observe` → `currentMember`); a stale admission fails closed with `auth.error` on the commit channel | #543 | Unchanged code path (`dispatchFrame` → `delivery.observe`). The per-socket channel keeps "the hand-off cannot overtake the commit's frames" (0048 §1a), now in-process. |
| Membership changes drop that socket's subscriptions and send `auth.error membership-changed` + watermark | K10 | Same engine; the local holder applies it in order. |
| Revalidation every 30 s, the pending-auth 10 s deadline, K16 address limits and the 30 s public admission recheck | K05, K16 | Same `SocketHost` alarm. Router slot reservation as today; release on close. The limiter audit learns tenant holders. |
| Watermarks monotonic per socket; the call watermark floor (`subscriptionFloor`); cursors, digests and resets | K07, K10 | Same engine. Local sockets die with the instance, so no cross-instance replay is needed for them. |
| Presence leave with a 10 s grace on close and on every `auth` | K14 | Same `TenantPresence`; local leave is a call. |
| No subrequest-depth ratchet | X02 | The tenant holder's outgoing calls (Control, other tenants' holders, gateways) go through the same CallOrigin gate. In-process steps make no call. |
| Replies wait for durability (output gate) | platform | Already true today (the RPC reply is gated); unchanged for the client. |
| Action replies carry generation authority; "durable ownership retained" is UNKNOWN, not obsolete | K22, `settleActionReply` | Holder addresses gain a kind; a tenant holder answers with the same rules. |

## 6. Incremental plan

Each milestone is behind a flag (`GONVEX_TENANT_SOCKETS`: `off` | `header` (test-only routing header) | `origin`) and
can merge with the flag off.

| # | Milestone | Ships | Effort | A-lite? |
|---|---|---|---|---|
| M0 | **Spike and go/no-go.** A separate dev Worker (not the measurement lane's) with a minimal tenant holder for the application socket, routed by a test-only header from the probe. Measure -12 cold (cache-hit) A vs baseline, n ≥ 5, SJC. Measure a publication with 100 local sockets (close and resume times), and the frames/s ceiling with deflate. Read -12/-11/-04 peak sockets from Rust staging metrics (read-only, approved). | numbers + decision | 3–4 days | yes |
| M1 | **Extract the holder.** Move `GatewayObjectBase`'s holder logic (tenant index, replica/presence receivers, recovery barriers, Control notices, CallOrigin) into a `SocketHolder` that a gateway and a tenant can embed. Holder addresses `{kind, name}` in outlets, presence, Action replies and the K16 audit. No behavior change. | refactor, conformance unchanged | 1–1.5 wk | yes |
| M2 | **Tenant holder.** `AssemblyTenant` accepts upgrades (flag `header`). Composite outlet with per-socket local holders and a byte budget. In-process own-tenant dispatch, Control and foreign tenants via ports. `watch(…,"TENANT",…)`, leases and control-read cache per holder. Conformance suites (`session-*`, replica, presence) against the tenant placement. | dev-only | 2–2.5 wk | yes (foreign tenant → 1012 + hint) |
| M3 | **Router.** `Origin` → selector → tenant id (colo cache + Control on miss), caps and pressure, gateway fallback, per-project/tenant enablement. | flag `origin` on dev | 1 wk | yes |
| M4 | **Lifecycle.** Orderly publication close (Control → tenants, jittered 1012), tenant reset/delete/transfer closes, shedding to gateways, metrics. | dev soak | 1–1.5 wk | partly (no shedding) |
| M5 | **Measure.** A b-batch (lane lock, SJC) of cold/warm load and create vs Rust_RTT; a reconnect storm after a publication; the 1013 ceiling at the cap. | evidence + decision to enable | 1 wk | yes |
| M6 | **Full A only.** Foreign-tenant sessions on a tenant holder (gateway parity), live shedding and controller integration, native/landlord routing (needs a client hint, so a frontend/SDK change), Facet evaluation for socket-preserving publication. | | 2–3 wk | no |

Totals: **A-lite ≈4–5 weeks** (M0–M5 without shedding or foreign-tenant parity); **full A ≈8–10 weeks**.

### Main risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Publication drops all tenant-held sockets (§4.3) | every client of every tenant reconnects and resumes on each app publication; a 4–6 s blip per 1,000 sockets; not Rust's in-place reload | orderly jittered close + resume (M4). Measure in M0; if unacceptable, stop at M0 (or the S5 Facet route, much later). |
| One object's frame ceiling and thread (§4.1, §4.2) | a busy tenant's commits slow every local socket; at the ceiling the platform closes all sockets (1013) | cap + pressure at connect, gateway overflow, shedding; deflate cost counted in the budget |
| Memory from in-flight frames (§4.4) | an isolate reset closes every local socket and loses subscriptions | per-socket and isolate byte budget on the local outlet; reconnect jitter |
| Auth model re-hosted (§5) | a lease, notice or attach check missing on the new holder = a revocation gap | M1 extracts code instead of copying it; the #539/#540/#543 regression specs run against both holder kinds; Control watcher registered before the first lease |
| Unauthenticated sockets reach tenant objects | anyone can wake a tenant and occupy its thread with `Origin: <victim>` | only resolved selectors; ≤16 pending per tenant; K16 address limits before the upgrade; the 10 s pending deadline; overflow to gateways |
| X02 depth with a new caller kind | "Subrequest depth limit exceeded" closes (dev 2026-11-17) | CallOrigin in the holder (M1); a depth regression test with tenant ↔ gateway ↔ Control traffic |
| The saving is smaller than estimated | weeks for less than the remaining gap | M0 is the go/no-go gate: require ≥250 ms on -12 cache-hit cold loads |
| Rust semantics drift for mixed placements | a tenant serves some sockets locally and some through gateways | one engine, one outlet abstraction; conformance runs on both placements |

## 7. The cheaper hybrid, and the alternatives

**A-lite** is A cut to what the measured cells need:
- only the application socket of web clients on a tenant subdomain (`Origin`); the control socket, native, landlord,
  public and share-route clients stay on gateways;
- a fixed cap with fallback at connect (no live shedding or controller integration);
- foreign-tenant `auth` re-routed with 1012 + hint instead of gateway parity;
- publication drops accepted and made orderly.

It removes the same per-step legs as full A for every measured cell (≈250–500 ms), at ≈4–5 weeks instead of ≈8–10.
Its costs are the publication reconnects and a second holder kind to maintain.

Nothing cheaper removes the legs: the tenant steps are client-sequenced (no pipelining helps; X17 cut 1), the gateway
cannot hand a socket over, and an inline `openMany` reply would save only the "deliver" vs "hop" difference (≈10 ms
per step). X17's upper-bound A/B with every X02 deferral removed did not make the -12 cold load faster.

| Option | Saving (cold load) | Cost | Main risk |
|---|---|---|---|
| A (full) | ≈250–500 ms; create −40–80 | ≈8–10 wk | publication drops; one object per tenant; auth re-hosting |
| **A-lite** | ≈250–500 ms on the measured cells | ≈4–5 wk | as A, minus live shedding |
| D: fewer frontend waves (send the first-screen opens and `presence.subscribe` in 1–2 frames, earlier) | ≈150–300 ms (3–6 steps × 45–55 ms) | days, but changes the frozen frontend `678bdc80` | changes the measured client |
| E: re-baseline (Rust_RTT + 300 ms + the measured per-step floor) | closes by definition | none | product decision |
