# 0042: K16 public functions: per-address budgets in a limiter object

Date: 2026-11-11 (revised after 0045). Status: **ruled in 0045** (proposed as 0040, renumbered there). Affects:
0012 R11, CONTRACT.md A.1.1 (upgrade rejections), A.2 (public rate limits), C.5 (limits table), K05 (session files
edited at the hook points), K12, P02.

## 1. Per-address budgets: one Durable Object per address bucket (0012 R11 amended in 0045)

0012 R11 ruled "per-object counters in the object that owns the socket". That cannot hold the per-address budgets:
the router picks a gateway from a hash of the connection id (0016 G0-1), so one client's sockets sit on several
gateways, and the 429s must be decided in the router before any gateway holds the socket (CONTRACT.md A.1.1 says
"adapt: counters per IP across objects need a limiter object or the Workers rate-limit binding").

**Choice: a Durable Object counter.** `PublicLimiterObject`, bound as `PUBLIC_LIMITER`, one object per address
bucket (`addr:<bucket>`). It holds the bucket's pending slots, public slots and 60 s call window.

| | Rate Limiting binding | Limiter object (chosen) |
|---|---|---|
| Pending and public **socket** caps | Cannot express them: they are concurrency counts with a release on close or auth, not rates | Slots per connection id, taken at the upgrade and released by the socket owner |
| Exact counts | Counts per Cloudflare location, eventually consistent, so it trips "around" the limit | Single-threaded per bucket: trips at exactly 100, 20, 120 |
| Rust's window | Periods of 10 s or 60 s only, sliding | Rust's fixed window, both budgets charged independently, a batch costing its length |
| Cost | Free, no added latency | One RPC per upgrade, per public frame and per release |

The per-connection window (30 calls) stays in the object that owns the socket, in the socket's attachment.

Behaviour, as ruled in 0045:

- **Slots are persisted.** One KV entry per held connection in the limiter's own storage
  (`slot:<connectionId>` → gateway, admission time, kinds held), written when a slot is taken or freed and read back
  when the object wakes. An evicted limiter therefore keeps the 20-socket and 100-pending caps while public sockets
  hibernate. Writes per socket: one at the upgrade, one when the first auth frees the pending slot (a delete for a
  member socket, which then holds nothing), one delete at a public socket's close. A bucket with no socket stores
  nothing.
- **The call window stays in memory.** It is 60 s of ephemeral state (design rule: nothing ephemeral in SQLite). An
  object only goes idle between bursts, so an eviction can only restart a window nobody is using.
- **Leaked slots.** A gateway reset skips its close handlers. When a bucket is full, the limiter asks the gateways
  that hold slots older than 10 s which connections are still open (`GatewayObject.liveConnections`) and drops the
  rest, at most once per 10 s per kind. While any slot is stored, an hourly alarm runs the same audit over every
  slot, so a leak in a bucket that never fills does not stay in storage.
- **Failure modes.** If the limiter cannot be reached, the router **refuses the upgrade** with HTTP 503
  `connection limiter is unavailable` (`admit`), so an outage cannot lift the socket caps. Call charging (`allow`)
  stays **fail-open**: the connection budget, the payload cap and the 10 s auth deadline still apply. A failed
  `release` is logged and left to the audit.
- **No bucket cap.** Rust caps each map at 16,384 buckets as a process memory bound. One object per bucket has no
  counterpart and needs none.
- A deployment-wide budget is still out of scope. Each address has one budget across the whole deployment, which is
  stricter than Rust's per-process budgets with several runtime instances.

Measured on the platform (a `gonvex-cf-dev-*` preview, deleted afterwards;
`cloudflare/kernel/public/probe/results/limits-cf.json`, re-run with persisted slots): every default limit trips
at its count from one address. That covers the 31st call per connection, 120 calls per address across four
connections, the 21st public socket and the 101st pending socket on both transports.

## 2. Client address

`CF-Connecting-IP` is the address. The edge sets it and a client cannot forge it. The trusted-proxy variables are
not read (C.5 "replace"). A missing, repeated or invalid value falls back to the shared bucket `0.0.0.0`, as Rust's
unknown peer does. Buckets are as in Rust: IPv4 per address, IPv4-mapped IPv6 as its IPv4 address, IPv6 per /64. A
dual-stack client that connects over both families has two addresses and two budgets, as in Rust.

The conformance Rust backend trusts `CF-Connecting-IP` from 127.0.0.1 (`GONVEX_TRUSTED_PROXY_CIDRS=127.0.0.1/32`,
`GONVEX_CLIENT_IP_HEADER=cf-connecting-ip`), so a scenario can give each step its own address on both backends.
Sockets that send no header keep using the peer address, as before.

## 3. Admission and close codes

- Where Rust ends the socket with no close frame (a failed admission re-check, a revoked admission, the monitor),
  the kernel sends the same frames and then closes **1000**, as K05 does for its other no-frame exits (0020 §4).
- The monitor runs on the gateway's single alarm and re-checks a socket at most 30 s after its last check (Rust's
  timer can leave it about 60 s). It makes one Control Plane read per pinned scope per round, not one per socket.
  Only revocation can be seen sooner.

## 4. Public Action timeout: one rule

K12's `ActionRuntime` applies the anonymous deadline (`actions/src/runtime.ts`: the earlier of the Action's deadline
and now + `publicActionTimeoutMs`, agent Actions included, as `execution.rs:904-915`). K16 has no second copy of
the rule. `GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS` is parsed once, by `publicLimits(env)`, and the Action Worker's
`runtimeFor` passes `publicLimits(env).actionTimeoutMs` as that option.

## 5. `done_when` (amended in 0045)

The transport limits trip at their counts, and one anonymous call per kind passes. K16 shows this through
`cloudflare/kernel/public/harness/`, a test-only composition: K07's tenant object, with tenant-scope `query.call`
answered by K08's `callQuery` and `action.call` by K12's `ActionRuntime`. The real composition is the assembly
package's (0045). Conformance `public-calls` passes Rust vs `cloudflare-public`:

- the public Queries `actions.boardCount` and `host.querySurface`, with the anonymous ctx surface identical to Rust's;
- the public Action `actions.publicEcho`, with no account in its context;
- refusals of a non-public Reducer, alone and in a batch, and of a non-public Action, with Rust's texts.

The fixture has no public Reducer. Public Reducers need `interactive: false` and a fixture change; the Reducer path
is covered by its refusals here and by K07.

The seven Whagons calls named in the K16 brief are a V01 item, once K12 is routed into the tenant object:
`publicForms.getPublicFormByToken`, `publicForms.submitPublicResponse`, `spaceForms.publicUploadUrl` (public agent
Actions), and the token Actions `taskPublicShares.getPublicPreviewByToken`, `workspaceViews.getPublicPreviewByToken`,
`documents.getPublicByUuid` and `nfc.resolveTagByUuid`. The token Actions are not public: a Member calls them, and an
anonymous socket gets `module database capability failed: authenticate with an active tenant before calling a
non-public action`, as in Rust.
