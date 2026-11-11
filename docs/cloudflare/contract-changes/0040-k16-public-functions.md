# 0040: K16 public functions: per-address budgets in a limiter object, and what is not yet testable

Date: 2026-11-11. Status: **proposed** (K16), for the lead. Affects: 0012 R11, CONTRACT.md A.1.1 (upgrade
rejections), A.2 (public rate limits), C.5 (limits table), K05 (session files edited additively), K12, K13, P02.

## 1. Per-address budgets: one Durable Object per address bucket (supersedes 0012 R11 for them)

0012 R11 ruled "per-object counters in the object that owns the socket". That cannot hold the per-address budgets:
the router picks a gateway from a hash of the connection id (0016 G0-1), so one client's sockets sit on several
gateways, and the 429s must be decided in the router before any gateway holds the socket (CONTRACT.md A.1.1 says
"adapt: counters per IP across objects need a limiter object or the Workers rate-limit binding"). The package brief
asks for limits that hold across isolates, through the Rate Limiting binding or a Durable Object counter.

**Choice: a Durable Object counter.** `PublicLimiterObject`, bound as `PUBLIC_LIMITER`, one object per address
bucket (`addr:<bucket>`). It holds the bucket's pending slots, public slots and 60 s call window.

| | Rate Limiting binding | Limiter object (chosen) |
|---|---|---|
| Pending and public **socket** caps | Cannot express them: they are concurrency counts with a release on close or auth, not rates | Slots per connection id, acquired at the upgrade and released by the socket owner |
| Exact counts | Counts per Cloudflare location, eventually consistent, so it trips "around" the limit | Single-threaded per bucket: trips at exactly 100, 20, 120 |
| Rust's window | Periods of 10 s or 60 s only, sliding | Rust's fixed window, both budgets charged independently, a batch costing its length |
| Cost | Free, no added latency | One RPC per upgrade, per public frame and per release (README "Costs") |

The per-connection window (30 calls) stays in the object that owns the socket, as R11 says, in the socket's
attachment.

Properties the lead should know:

- **Memory only.** Nothing is written to SQLite (design rule). An evicted limiter restarts at zero, which can only
  admit, never refuse wrongly. Slots are keyed by connection id, so late releases are no-ops.
- **Leaked slots.** A gateway reset skips its close handlers. When a bucket is full, the limiter asks the gateways
  that hold slots older than 10 s which connections are still open (`GatewayObject.liveConnections`) and drops
  the rest. It does this at most once per 10 s per bucket, and only on the refusal path.
- **Fail open.** If the limiter cannot be reached, the router admits the upgrade and the frame passes the address
  budget (logged). The connection budget, the payload cap and the 10 s auth deadline still apply. The alternative
  is to refuse every public page while one object is unreachable.
- **No bucket cap.** Rust caps each map at 16,384 buckets as a process memory bound; one object per bucket has no
  counterpart and needs none.
- **A deployment-wide budget** (R11's second sentence) is still out of scope. Each address has one budget across
  the whole deployment, which is stricter than Rust's per-process budgets with several runtime instances.

Measured on the platform (a `gonvex-cf-dev-preview-*` deploy, deleted afterwards;
`cloudflare/kernel/public/probe/results/limits-cf.json`): every default limit trips at its count from one address.
That covers the 31st call per connection, 120 calls per address across four connections, the 21st public socket
and the 101st pending socket on both transports.

**Asked of the lead:** confirm the object counter, and amend R11 to "per-address budgets: one limiter object per
address bucket; per-connection budget: the socket owner".

## 2. Client address

`CF-Connecting-IP` is the address. The edge sets it and a client cannot forge it. The trusted-proxy variables are
not read (C.5 "replace"). A missing, repeated or invalid value falls back to the shared bucket `0.0.0.0`, as
Rust's unknown peer does. Buckets are as in Rust: IPv4 per address, IPv4-mapped IPv6 as its IPv4 address, IPv6 per
/64. The conformance Rust backend now trusts `CF-Connecting-IP` from 127.0.0.1
(`GONVEX_TRUSTED_PROXY_CIDRS=127.0.0.1/32`, `GONVEX_CLIENT_IP_HEADER=cf-connecting-ip`), so a scenario can give each
step its own address on both backends. Sockets that send no header keep using the peer address, as before.

## 3. Admission and close codes

- Where Rust ends the socket with no close frame (a failed admission re-check, a revoked admission, the monitor),
  the kernel sends the same frames and then closes **1000**, as K05 does for its other no-frame exits (0020 §4).
- The monitor runs on the gateway's single alarm and re-checks a socket at most 30 s after its last check (Rust's
  timer can leave it about 60 s). It makes one Control Plane read per pinned scope per round, not one per socket.
  Only revocation can be seen sooner.

## 4. Public Action timeout

`publicActionDeadline` (kernel/public/config.ts) returns the earlier of the Action's deadline and now +
`GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS`, for agent Actions too (execution.rs:904-915). K12's `ActionRuntime`
(PR #362) already applies the same rule through its `publicActionTimeoutMs` option. **Follow-up for K12/K19:** pass
`publicLimits(env).actionTimeoutMs` as that option, so the variable reaches it.

## 5. Whagons' public calls are not testable on `cloudflare/integration` yet

All seven calls named in the K16 brief are Actions with `profile: "agent"`:

- `publicForms.getPublicFormByToken`, `publicForms.submitPublicResponse` and `spaceForms.publicUploadUrl` are
  `public: true` (whagons5-client `origin/staging` a6de676951). Their tools are internal Reducers.
- `taskPublicShares.getPublicPreviewByToken`, `workspaceViews.getPublicPreviewByToken`, `documents.getPublicByUuid`
  and `nfc.resolveTagByUuid` are **not** public. A signed-in Member calls them; an anonymous socket gets
  `module database capability failed: authenticate with an active tenant before calling a non-public action`, as in Rust.

The kernel cannot run any of them end to end yet:

- the Action runtime is K12 (#362, changes requested);
- routing `action.call` to that runtime is listed under "Not here" in K12 (K05/K07);
- `ctx.storage.generateUploadUrl` for `publicUploadUrl` is K13 (#354).

The tenant object answers every `action.call` with "not implemented" today. K16 provides what these calls need
from the public transport: anonymous auth, pinning, admission before each call, budgets, payload caps and the
deadline helper. It is tested on the socket path and, for anonymous calls, on K07's real Reducer path. **Proposed:**
the seven calls become a V01 check once K12 and K13 are merged. The check runs each one anonymously (and the four
token Actions as a Member) against Rust and the kernel on a staging tenant copy.

## 6. Found in passing (not K16)

The `cloudflare-reducer` conformance backend cannot provision the fixture on `cloudflare/integration` as it stands.
The fixture's `tasks` table (added by K08) fails K02's change-feed install:
`schema transform: migration table:tasks: _gonvex_sync_tasks_update: expression-depth: expression depth about 111`.
So every scenario on that backend fails at setup. Reproduced on the base commit `c8210864` with `receipt-lost-reply`.
K16's anonymous-call check therefore runs as a workerd spec on K07's test Worker
(`kernel/public/test/reducer/anonymous.spec.ts`), which uses K07's own fixture manifest. Owner: K02/K08 (the
transform's depth limit, or the fixture).
