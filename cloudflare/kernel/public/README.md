# Public functions (K16)

Anonymous (signed-out) access to a tenant's `public: true` functions over `/ws?public=1`: the per-address and
per-connection budgets, the payload cap, the public admission re-check and the public Action timeout. The reference
is `rust/crates/runtime/src/public_functions.rs` and the public paths of `lib.rs` (`websocket_upgrade`
1576-1648, the frame checks 2524-2722, `check_public_admission` 3871-3934, `public_limit_errors` 4360-4430), and
`docs/public-functions.md`, at `0bf3ad13`. Decision: `docs/cloudflare/contract-changes/0040-k16-public-functions.md`.

```text
client ──/ws?public=1──▶ router (kernel/session/router.ts)
                           │  address = CF-Connecting-IP bucket          ──RPC admit──▶ PublicLimiterObject "addr:<bucket>"
                           │  429 pending / public connection limit         (pending + public slots, the address call window)
                           ▼
                        gateway ─ Session (kernel/session/session.ts)
                           │  each public frame: address window (RPC allow) + connection window (attachment)
                           │  tenant-code frames: admission re-read (control resolvePublicTenant)
                           │  first auth: release pending slot; close: release every held slot   ──RPC release──▶ limiter
                           └  alarm: admission monitor, one Control Plane read per pinned scope
```

| File | What | Rust |
| --- | --- | --- |
| `config.ts` | `GONVEX_PUBLIC_*` and `GONVEX_PENDING_CONNECTIONS_PER_IP` (positive integers, Rust's defaults); `publicActionDeadline` | `PublicLimits`, `config.rs:482-516`, `execution.rs:904-915` |
| `address.ts` | The client address from `CF-Connecting-IP`; buckets (IPv4, IPv4-mapped → IPv4, IPv6 /64); one shared bucket when the header is missing or unusable | `client_ip`, `ip_bucket` |
| `window.ts` | The fixed 60 s call window | `Window` |
| `limiter.ts` | `PublicLimiterObject`, one per address bucket: pending and public slots, the address call window; the audit of leaked slots | `PublicLimiter` |
| `guard.ts` | `PublicGuard` (what the session layer uses), `admitUpgrade` (the router's 429), `validatePublicScope` | `validate_public_scope` |
| `frames.ts` | Frame cost, the frames that run tenant code, the correlated errors of a refused frame | `public_limit_errors`, `public_admission_errors`, `public_frame_runs_tenant_code` |
| `test/` | `units.spec.ts` (Rust's unit vectors), `public.spec.ts` (sockets in workerd with small limits, `test/wrangler.jsonc`), `reducer/anonymous.spec.ts` (anonymous calls on K07's real Reducer path) | `public_functions.rs` tests |
| `probe/limits.mjs` | Platform probe of the default limits against a deployed preview; `probe/results/limits-cf.json` | |

The session layer files (K05's) changed additively: `router.ts` reserves slots, `session.ts` runs the frame checks,
`host.ts` releases slots and runs the monitor, `placement/gateway.ts` passes the address and answers
`liveConnections`.

## Behaviour

| Rule | Kernel |
| --- | --- |
| Pending sockets per address | 100, both transports, reserved at the upgrade (HTTP 429 `pending connection limit exceeded`, `text/plain`); freed by the first auth that sets an identity or a tenant, or by the close |
| Public sockets per address | 20, reserved at the upgrade (429 `public connection limit exceeded`); a refused public slot takes no pending slot; held until the close, through re-auths |
| Calls per connection | 30 per fixed 60 s window from the socket's open, kept in the socket's attachment |
| Calls per address | 120 per fixed 60 s window from the address's first call, in the address's limiter object |
| Cost | 1 per frame (auth included), a batch its length (at least 1), a `replica.snapshotAck` 0 once native rows were negotiated. Both windows are charged independently when they have room; a call passes only when both had room |
| Refused call | Its correlated error with `public call rate limit exceeded` (auth, query, reducer `rejected`, replica, action; one per batch item); any other frame `auth.error {id:"public-rate-limit"}`. The socket stays open |
| Payload | `GONVEX_PUBLIC_MAX_PAYLOAD_BYTES` (65,536 UTF-8 bytes): over it on the public transport closes 1002 (0022 §4.3); on an unscoped or anonymous socket 1009 `public payload too large` |
| Admission | Pinned at the first public auth, with the check that auth made. Re-read before every frame that runs tenant code and before each call of a `reducer.callMany`; other frames accept a check up to 30 s old. A failed check revokes the socket for good: the frame's correlated errors (`public tenant session is unavailable`), then close 1000. A batch's refused calls are followed by `auth.error session-expired` and close 1000 |
| Monitor | The gateway's alarm re-checks every admitted public socket whose last check is 30 s old, one Control Plane read per pinned scope per round; a failure sends `auth.error {id:"session-expired"}` and closes 1000 |
| Public Action | `publicActionDeadline`: the earlier of the Action's deadline and now + `GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS` (60), also for agent Actions. K12's `ActionRuntime` takes it as `publicActionTimeoutMs` (`publicLimits(env).actionTimeoutMs`) |

## Differences from Rust (0040)

1. **Address budgets are one Durable Object per address bucket** (`addr:<bucket>`), not process-local maps: they hold
   across isolates and gateways. Counters live in memory; an evicted limiter restarts at zero (it can only admit
   more, never refuse a client wrongly). Rust's 16,384-bucket cap per map is a process memory bound and has no
   counterpart.
2. **Client address** is `CF-Connecting-IP`, which the platform sets; `GONVEX_TRUSTED_PROXY_CIDRS`,
   `GONVEX_PUBLIC_TRUSTED_PROXY_IPS` and `GONVEX_CLIENT_IP_HEADER` are not read.
3. **Leaked slots** (a gateway reset that skips its close handlers) are reclaimed when a bucket is full: the limiter
   asks the gateways holding slots older than 10 s which connections are still open, at most once per 10 s.
4. **Where Rust ends the socket without a close frame**, the kernel closes 1000 after the same frames (as K05 does).
5. **The monitor** re-checks a socket at most 30 s after its last check (Rust: a 30 s timer from the auth, so up to
   about 60 s). Only revocation can be seen earlier.
6. **Limiter unreachable**: the address budget and the upgrade reservation fail open (logged); the connection
   budget, the payload cap and the auth deadline still apply.

## Tests and conformance

```sh
pnpm vitest run --root kernel/public     # in cloudflare/: 25 tests in workerd
pnpm vitest run --config kernel/public/vitest.reducer.config.mjs   # anonymous calls on K07's Reducer path
cd conformance && node src/cli.mjs --backend rust=rust --backend cf=cloudflare --scenario public-limits
node src/cli.mjs --backend rust=rust --backend cfr=cloudflare-reducer --scenario public-limits --scenario public-calls
```

- `public-limits`: public auth and scope; 30 calls per connection then every frame kind's error; 120 per address
  over four sockets; 20 public sockets (429 at the 21st, admitted after a close); 100 pending sockets over both
  transports (429 at the 101st, admitted after an auth). Each step has its own address; the Rust backend trusts
  `CF-Connecting-IP` from 127.0.0.1.
- `public-calls` (`publicCalls` backends: Rust, `cloudflare-reducer`): an anonymous socket is refused a non-public
  Reducer, alone and in a batch.
- `session-frames` (K05) already covers the transport mismatch, the public 64 KiB cap and unscoped sockets.
- `cloudflare-reducer` cannot provision the fixture on `cloudflare/integration` today (0040 §6), so the anonymous
  calls are also asserted in `test/reducer/anonymous.spec.ts` with Rust's texts.

**On the platform** (`gonvex-cf-dev-preview-cf-k16-public`, deleted after the run; `probe/results/limits-cf.json`),
from one IPv4 address at the default limits: the 31st call on a connection is refused; four connections are
admitted exactly 30 + 30 + 30 + 29 calls (the refused 31st still counts toward the address, as in Rust); the
21st public socket and the 101st pending socket (on either transport) get 429 with Rust's text; a closed socket's
slot is free at once. A dual-stack client that connects over IPv4 and IPv6 has two addresses and two budgets. A
socket that never authenticates is closed by the 10 s auth deadline, which frees its slots, as in Rust.

## Costs

- **Upgrade:** one limiter RPC (`admit`) on every socket, both transports (Rust checks the pending budget on every
  upgrade too). One more (`release`) at the first successful auth and one at the close of a public socket. Member
  sockets hold nothing after auth, so their close costs nothing.
- **Public frame:** one limiter RPC (`allow`). A tenant-code frame also re-reads admission from the control object,
  as Rust re-reads the Control Plane.
- **Monitor:** while an admitted public socket is open, its gateway's alarm fires every 30 s, waking it from
  hibernation, with one Control Plane read per pinned scope (Rust runs one monitor task per socket).
- **Bundle:** the kernel Worker grows from 131.51 KiB (32.81 KiB gzip) to 152.43 KiB (38.45 KiB gzip),
  `wrangler deploy --dry-run`.
- **Storage:** none. The limiter writes nothing; rows read and written by the functions touched are unchanged.
- **Memory:** per address object, two maps of live slots and one window; per socket record, about 120 bytes of
  attachment (address, slots, window, admission).

## Not here

| Item | Owner |
| --- | --- |
| Routing `action.call` to the Action runtime; applying `publicActionDeadline` there | K12 (with K07) |
| Anonymous uploads (`generateUploadUrl` forces `visibility:"tenant"`, owner `""`) | K13 |
| Public Query handlers over the socket (`query.call` in the tenant object) | K08 wiring |
