# Session layer (K05)

The `/ws` path of the kernel: the upgrade, `session.ready`, `auth` in every branch, re-auth in place,
capability negotiation, `clientContract` / `client.updateRequired`, close codes and size limits, public
sockets, liveness, and the gateway placement that holds the sockets. The reference is the Rust socket loop
(`rust/crates/runtime/src/lib.rs` 1576-3860 and `authenticate`, 4475-4780) at `0bf3ad13`.

```text
client ──/ws──▶ router Worker ──▶ gateway object (gw-N) ──RPC──▶ control object (K06; stub/control.ts today)
                 router.ts         placement/gateway.ts   ──RPC──▶ tenant object   (stub/tenant.ts today)
                                   host.ts + session.ts   ◀─RPC─── tenant: one delivery per gateway per commit
```

| File | What |
| --- | --- |
| `router.ts` | `GET /ws` in the Worker: handshake checks, `?public=1`, the extension-header grammar (400), then a gateway. |
| `placement/gateway.ts` | **The only placement-specific module.** Shard choice, `GatewayObject`, tenant registration and `TenantGateways` (the tenant side: one `deliver` RPC per gateway per commit). If gateway placement is overruled, only this file changes. |
| `host.ts` | `SocketHost`: hibernatable sockets in any object, one attachment per socket, the single alarm, batched revalidation, the `{"type":"ping"}` auto-response. |
| `session.ts` | `Session`: one socket's state machine, placement-neutral. Size checks, the decoder, close codes, `auth`, preservation, update gating, deadline, revalidation, membership changes, liveness. |
| `decode.ts` | The client frame decoder with serde's exact accept/close boundary (1003). |
| `capabilities.ts`, `directive.ts`, `protocol.ts` | `session.ready`, negotiation, the replica directive hashes, server frames in serde key order. |
| `ports.ts`, `bindings.ts`, `rpc.ts` | What the session layer needs from K06, the module registry (K19) and the tenant object, and where each binding points today. |
| `stub/` | Stand-ins for the control and tenant objects, and the `/_stub/session/*` dev routes (only with `GONVEX_STUB_ADMIN_KEY`). |
| `bench/hop.mjs` | One gateway hop, client-observed and in-object. |
| `test/` | workerd tests: decoder vectors measured against Rust, placement, alarms, hibernation, limits. |

## Behaviour

- **Placement (0016 G0-1).** The router cannot know the tenant (0004), so it hands each upgrade to
  `gw-<fnv1a(connection id) mod GONVEX_GATEWAY_SHARDS>`. Size the shard count as expected concurrent sockets
  / 125 (S3: 8 × 125 sockets carried 30 commits/s). When a gateway gets its first socket of a tenant it calls
  the tenant's `attachGateway`; the tenant keeps the set in KV storage (`gateway:<name>`: one write per new
  gateway, never per socket or frame). A commit goes to each registered gateway once (`TenantGateways.deliver`),
  with frames for all sockets and frames per delivery class; a gateway that reports no socket left is dropped.
  RPC payloads are versioned (`v`) and additive for the deploy skew of S3 §7.
- **State across hibernation (G0-9).** Each socket's `SessionRecord` lives in its attachment (≤ 16 KiB) and is
  rewritten on auth, revalidation and liveness, never per delivered frame. Gateways rebuild their index from
  attachments on every wake; tenants keep the registration, so delivery survives a gateway eviction.
- **Timers (G0-9).** One alarm per gateway: the 10 s pending-auth deadline, the 30 s revalidation round (one
  batched control call for every due socket) and liveness.
- **Liveness (0016 G0-6).** `{"type":"ping"}` is answered `{"type":"pong"}` by the runtime's auto-response
  without waking the gateway. A tenant socket with no outbound frame for 20 s gets a watermark at the revision
  it already has (`replica.watermark`, or an empty `replica.batch` when it negotiated batch frames), so the
  edge's TCP stack can notice a dead peer. It certifies nothing the socket was not already sent.
- **Close codes (A.1.9).** 1003, 1008 and 1009 with Rust's reasons; 1002 for an oversize public-transport frame
  (what Rust's client actually sees, 0020). Where Rust ends the task without a close frame, the kernel sends
  1000 (after `client.updateRequired`, a public/transport mismatch) or 1011 (an auth past its deadline, a
  failed port call).
- **Limits.** 64 KiB (UTF-8 bytes) on unscoped and anonymous sockets and on the public transport; the
  platform's 32 MiB on everything else (0018). The kernel never sends a frame of 32 MiB − 1 KiB or more; a
  frame that large closes the socket 1011 rather than being dropped silently. Workers RPC also caps a
  serialized argument at 32 MiB, so a delivery batch must stay under it (K10 splits).

## Owned elsewhere (hooks are in place)

| Item | Package | Today |
| --- | --- | --- |
| Sessions, tokens, tenant resolution, admission, revalidation, `control.*` frames | K06 | `stub/control.ts` (Rust's results and error texts for the paths here) |
| Module generation: artifact hash, client contract, replica epoch, the reload fan-out | K19 (with K02) | `stub/control.ts` `deploy`, fan-out to every gateway shard |
| Tenant frames (`query.*`, `reducer.*`, `action.*`, `replica.*`) with a tenant session | K07, K08, K10 | `stub/tenant.ts`: a fixed "not implemented" error |
| Presence frames | K14 | `kernel/presence/`: per-socket state in the session record, frames to the tenant port, deliveries through `GatewayObject.presence`, leave on auth / discard / close |
| `error.*`, `telemetry.*` | K15 | forwarded to the tenant port, no reply |
| Per-address 429 limits, the 30 s public admission recheck, public rate limits | K16 | `kernel/public/` (address limiter object, `PublicGuard`; hooks in `router.ts`, `session.ts`, `host.ts`) |
| `gvx_svc_` service principals | K17 | `kernel/principals`: `Session.serviceAuth` / `serviceFrame`, `SessionRecord.service`; refused when the control port has no `authenticateService` (the stand-in) |

## Run

```sh
pnpm test                                        # in cloudflare/: workerd tests, including these
cd conformance && node src/cli.mjs --backend rust=rust --backend cf=cloudflare \
  --scenario session-handshake --scenario session-frames
node kernel/session/bench/hop.mjs http://127.0.0.1:8787 <stub-admin-key>   # against `pnpm dev --var GONVEX_STUB_ADMIN_KEY:<key>`
```
