# Service principals (K17)

A service principal is a trusted backend service (the Whagons API gateway) that authenticates with a `gvx_svc_`
credential, configured by its SHA-256 digest in `GONVEX_SERVICE_PRINCIPALS`, bound to one project and tenant per
socket and to an exact function allowlist. It can also mint short-lived, single-use member grants
(`control.servicePrincipals.delegate`) that a new socket redeems as that member, with delegated invocation
provenance. Reference: `rust/crates/runtime/src/service_principal.rs`, `docs/service-principals.md`, CONTRACT A.1.4,
A.2 (notes), C.1 rows 61-62, C.3.1, C.3.8 items 1 and 11, D.1 `/dev/manifest`, at `0bf3ad13`.

```text
gateway ──/ws──▶ gateway object (K05 Session) ── service socket? ──▶ frames.ts (allowlist, refusals)
                     │ auth gvx_svc_                                   ├─ control scope ─RPC─▶ control object (K06 service.ts)
                     └──RPC──▶ control object (K06 auth.ts)            └─ tenant scope ──RPC─▶ tenant object (K07) → tenant.ts
delegated socket ──auth gvx_imp_──▶ K06 grant redemption ──▶ an ordinary member session (delegation set, channel api)
GET /dev/manifest (Bearer gvx_svc_) ──▶ manifest.ts manifestAccess ──▶ K19's route (and the harness)
```

| File | What | Rust |
| --- | --- | --- |
| `grant.ts` | `ServiceGrant` (kept in the socket's hibernation record), the system identity `_gonvex_service:<id>`, the `auth.result` body | `ServiceGrant`, `ServiceGrant::session`, `authenticate_service_principal` result |
| `frames.ts` | The service socket's frame boundary: allowlist, control routing, refusals, ignores, frames in serde byte order | `handle_service_frame`, `service_control_call` |
| `tenant.ts` | Allowlisted calls in the tenant object: K07's Reducer path with `allow_internal` and the `api` channel | `service_tenant_call` |
| `manifest.ts` | `/dev/manifest` access for a bearer credential (`manifest:true` and a listed project, no fallback) | `manifest_access`, `operations::manifest` |
| `harness/` | K05's sockets in front of K06's control object and K07's tenant object, for the specs and the conformance backend `cloudflare-principals` | |
| `verify/gateway-contract.mjs` | The gateway's consumer contracts against a real Whagons release on Rust and on K19's local kernel | |

Owned elsewhere and reused unchanged: credential parsing and admission, delegation and revocation, grant
redemption and revalidation (K06 `control/src/auth.ts`, `service.ts`, `directory.ts`), the Reducer path (K07),
the session layer (K05). K17 adds the hooks: `Session.serviceAuth` / `serviceFrame` and `SessionRecord.service`
(K05), the optional `ControlPort.authenticateService` / `serviceCall` and `TenantPort.serviceCall`,
`ReducerTenantObject.serviceCall` and `ReducerPath.execute(…, { allowInternal, channel })` (K07), `delegation` in
outbox provenance (K04), and K19's `/dev/manifest` deciding through `manifestAccess`.

## Behavior

- **Admission.** `auth` with a token that starts with `gvx_svc_` (after trimming) and `public:false` takes this
  branch. The digest must match a configured principal (constant-time over every principal), the project must be
  in its `projects`, project and tenant must be non-blank: otherwise `auth.error {code:"transient"}` with one text
  for every case, `service credential is invalid or not permitted for this project`. A tenant that does not
  resolve gives `tenant is not available`. Whatever the outcome, the socket drops its previous session (presence,
  subscriptions, client contract). With a grant the 10 s auth deadline clears; a refused socket stays pending.
- **Binding.** The socket is bound to one tenant until its next `auth`. It has no tenant session, no change feed,
  no revalidation and no liveness watermark. A later `auth` with another `gvx_svc_` credential or tenant rebinds
  it; any other `auth` ends the grant first, whatever its outcome.
- **Allowlist.** `reducer.call`, `action.call` and `query.call` with tenant scope must name an exact allowlisted
  path, or get `function "<p>" is not allowed for this service principal` without reaching the tenant. Allowlisted
  calls run as `_gonvex_service:<id>` (role and status `system`, no permissions, no delegation), may target
  `internal` functions, and run on channel `api`. Their receipts are keyed by that identity. Service `reducer.error`
  frames carry no `class` or `retryable`; replies never carry a trace or a watermark.
- **Refused and ignored frames.** `query.subscribe` → `service principals cannot subscribe`; `replica.open` →
  `service principals cannot open replicas`; batches → `reducer.error {id:"", class:"rejected"}`; control Queries
  and Actions → `service principals cannot call Control Plane Queries` / `Actions`; any other control Reducer →
  `Control Plane function "<p>" is not available to service principals`. Every other frame is ignored. A frame
  that does not decode closes the socket 1003.
- **Delegation.** `control.servicePrincipals.delegate` and `revokeDelegation` with control scope go to K06
  (`service.ts`): argument checks, the target must be an active Member of the socket's tenant, a `svcdel_` grant
  with a single-use `gvx_imp_` token, actor `service:<id>`, reason `service:<id>: <reason>`, the optional actor.
  No idempotency (a retry mints a second grant). A principal revokes only its own grants in its tenant.
- **Delegated sessions.** Redemption is K06's grant path (`gvx_imp_` once, then a rotating `gvx_dev_`): an
  ordinary member socket with 30 s revalidation and `session-expired` on revocation. Its direct calls run on
  channel `api` with `ctx.invocation.delegation = {principal, actor}` (K07 `invocationInfoOf`), and outbox and
  scheduled rows it writes keep the delegation (K04 `database-host.ts`; K11 parses it).
- **Manifest.** `GET /dev/manifest` with a recognized `gvx_svc_` bearer answers only from its configuration: 200
  with `{project, functions, module:{hash, generation}}` when `manifest:true` and the project is listed, else 403
  `service principal is not allowed to read this project's manifest`, never falling back to project-key or
  operator authorization.

## Differences from Rust

None observable. One-shot Queries (K08) and Actions (K12) are not composed into the tenant object yet, so an
allowlisted service `query.call` or `action.call` answers the tenant object's "not implemented" text there, as a
member's call does today; `tenant.ts` is where the composing package routes them. The gateway uses neither on its
service socket.

## Numbers

Per allowlisted call: one gateway → tenant RPC, then K07's path unchanged (fixture, workerd):
`principals.verifyKey` (internal, read-only) 4 statements, 37 rows read, 0 written; `principals.record` (one
insert, keyed) 19 statements, 305 rows read (56 catalog), 16 written (6 capture). A delegation is one tenant
admission RPC from the control object and one control row written. The service grant adds about 200 bytes to the
socket attachment (one path per allowlisted function).

## Tests and conformance

```sh
pnpm vitest run --root kernel/principals        # in cloudflare/: workerd specs over the harness
cd conformance && node src/cli.mjs --backend rust=rust --backend cf=cloudflare-principals --scenario service-principals
node kernel/principals/verify/gateway-contract.mjs --app <whagons5-client checkout> --ref origin/staging \
  --out kernel/principals/verify/results/gateway-contract.json
```

- `test/frames.spec.ts`: the boundary's bytes, the allowlist refused before the tenant is asked, ignores, the
  manifest decision and bearer parsing.
- `test/service.spec.ts`: admission and the hibernation record, allowlisted Reducers on the `api` channel with
  receipts and capture provenance under the principal, the delegated session's provenance in the outbox row, the
  grant row.
- `service-principals` (conformance): token admission, tenant binding (socket and grant), the allowlist and the
  narrow frame set, delegation argument checks, redemption (single use, wrong tenant, reconnect credential),
  delegated provenance, revocation ending the member session, re-auth, the 1003 close, `/dev/manifest`.
- `verify/gateway-contract.mjs`: deploys whagons5-client's committed release to Rust and to K19's local kernel
  through `/dev/sync`, reads `/dev/manifest` as the gateway principal and runs the app's own
  `checkConsumerContract` on `whagons-api-gateway.json` and `whagons-cli.json`. Result in `verify/results/`.
