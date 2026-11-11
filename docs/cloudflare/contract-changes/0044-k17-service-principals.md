# 0044: K17 service principals: adaptations and three fixes outside the package

Date: 2026-11-11. Status: **proposed** (K17). Affects: K05, K06, K07, K04, A01, K19, K08 (fixture), M08, V01.

K17 wires `gvx_svc_` service sockets into the kernel (`cloudflare/kernel/principals/`) on top of K06's existing
service-principal code (credential parsing and admission, `control.servicePrincipals.delegate` /
`revokeDelegation`, grant redemption). No contract change is needed. Conformance (`service-principals`, Rust vs
kernel) and the gateway's consumer contracts (`verify/gateway-contract.mjs`, real Whagons release) pass.

## Adaptations (no observable change)

| # | Rust | Kernel | Why |
|---|---|---|---|
| 1 | `service_grant` is task-local in the socket loop | `SessionRecord.service` in the socket attachment | Survives hibernation (G0-9). Size grows with the allowlist: about 50 bytes per path; the 16 KiB attachment holds about 300 paths. The gateway has one |
| 2 | One process checks the grant and runs the call | The gateway object checks the allowlist; the tenant object runs the call as the grant's system identity and trusts the grant it is handed | Same trust boundary as `SocketView`: tenant objects are reachable only through bindings. A refused path never reaches the tenant |
| 3 | `ExecutionAccess { allow_internal, provenance: direct_provenance(.., Api, ..) }` | `ReducerPath.execute(session, request, { allowInternal: true, channel: "api" })` | Additive option on K07's path; everything else (receipts, capture provenance, commit, texts) is the member path |
| 4 | Service sockets are not revalidated (C.3.8 item 11) | Same: the record has no auth token, so no revalidation round and no liveness watermark | |

## Fixes outside `kernel/principals/` (for the lead to accept or move)

1. **A01 T5: a wide table's update capture exceeds SQLite's expression depth.** `captureValueSql` chains one `OR`
   and one `||` per column, so a table of about 48 columns or more fails the transform (`expression depth about
   111` for 52 columns). The conformance fixture's `tasks` table (52 columns, added by K08) therefore fails tenant
   activation on every kernel backend that deploys the CLI-built fixture and installs capture in the object (the
   K17 harness, and K07's `cloudflare-reducer`, which uses the same `HarnessTenant.activate`). Whagons' widest table
   is 43 columns today (S1). Fix in `kernel/schema-transform/capture.ts`: when, and only when, the rewritten
   trigger is over the limit, the two chains are nested as balanced trees (same value, depth log₂ n). Every
   statement within the limit is rewritten byte for byte as before; a workerd test covers 40 and 60 columns.
2. **K19 `/dev/manifest`**: the service branch compared digests with `===` and did not trim the token after
   `Bearer ` (`Bearer  gvx_svc_…` fell through to operator auth: 401 where Rust answers 200). It now decides
   through `manifestAccess` (K06 `findPrincipal`: trimmed, constant-time over every principal).
3. **K04 outbox provenance** dropped `delegation`, so Actions and scheduled jobs queued by a delegated call lost
   it (Rust persists `InvocationProvenance.delegation`). Added when present. The 0032 follow-up (full provenance:
   `artifactHash`, `durableHops`) still stands; without it K11 cannot parse the row at all.

## Findings for other packages

- **K08/K12 composition.** One-shot Queries and Actions are not in the tenant object yet, so an allowlisted service
  `query.call` / `action.call` answers its "not implemented" text on the kernel. `kernel/principals/tenant.ts`
  is where the composing package routes them (K08's `callQuery` needs an `allowInternal` option, as K07's path got).
  The gateway calls neither on its service socket; its member sockets need K08, K10 and K12 like any client.
- **First K05 ↔ K06 assembly.** K05's sockets were built against a control stand-in whose port differs from K06's
  RPC surface. The K17 harness (`kernel/principals/harness/control.ts`) maps K05's `ControlPort` onto K06's
  `authenticate`, `query`, `write`, `revalidate` and `serviceCall`, and K06's `TenantPort` onto K07's tenant object.
  It is test assembly (live Control subscriptions are not refreshed there), but the mapping is what the kernel's
  composition needs; whoever composes it can start from it.
- **M08 (gateway re-point).** The gateway connects unchanged: it reads `auth.result.result.tenantId`, calls the
  internal key check without an idempotency key, delegates with an actor, redeems with `clientContract`, and
  detects dead sockets with its own WebSocket pings, which the edge answers. `GONVEX_SERVICE_PRINCIPALS` must be a
  secret binding of the control object; it is read when the object starts, so rotating a credential needs the
  object to restart (a deploy), as Rust needs a restart.

## Verification

- `service-principals` (11 steps): Rust vs Rust passes, Rust vs `cloudflare-principals` passes.
- `verify/gateway-contract.mjs` against whagons5-client `a6de676951` (`origin/staging`): the committed release
  synced to Rust and to K19's local kernel; `/dev/manifest` as the gateway principal: 1,997 functions, the same
  catalog hash and module hash on both; `checkConsumerContract` gives 0 errors for `whagons-api-gateway` (40
  functions) and `whagons-cli` on both, with the same 5 warnings (`customFields.listTaskValues` row fields, from
  the app's own contract); a principal without `manifest:true` gets the same 403 on both.
