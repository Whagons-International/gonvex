# Control object (package K06)

One SQLite-backed Durable Object holds the Control Plane: the 36-table schema of
`rust/crates/store/src/control_schema.sql`, imported unchanged (it is the same file, loaded as a Text
module). It serves every `control.*` function of `docs/cloudflare/CONTRACT.md` C.1, socket
authentication (C.3.8), the native OAuth routes (C.3.5), service principals, tenant routing and the
membership projection.

Reference: `rust/crates/runtime/src/control.rs`, `external_auth.rs`, `native_auth.rs`,
`membership_projector.rs`, `service_principal.rs` and the `ControlPlane` admission functions in
`rust/crates/store/src/lib.rs`, at `0bf3ad13`.

## Layout

| Path | What |
| --- | --- |
| `src/control-object.ts` | `ControlObject`: the RPC surface below, the alarm (receipt expiry), `/auth/*` |
| `src/functions.ts` | Control function dispatch: authorization, idempotency, queries, writes, sagas, auth Actions |
| `src/definitions.ts` | The 60-name registry and argument rules (`exact_object`, `required_string`, …) |
| `src/directory.ts` | Tenant routing and admission: `resolve_tenant`, `admit_member`, `authenticate_session`, grants |
| `src/sessions.ts` | Session issue and refresh rotation, `session_result_from_directory`, external account resolution |
| `src/auth.ts` | The `auth` frame branches (lib.rs `authenticate`), service-principal auth, replica directives |
| `src/external-auth.ts` | Firebase and OIDC ID tokens: JWKS, RS256 (WebCrypto), `accounts:lookup`, emulator decoder |
| `src/native-auth.ts` | `/auth/config`, `/auth/{provider}/authorize`, `/auth/{provider}/callback`, `/auth/token`, `/auth/logout` |
| `src/service.ts` | `control.servicePrincipals.delegate` / `revokeDelegation` |
| `src/password.ts`, `src/pbkdf2.ts`, `src/pbkdf2.wasm`, `wasm/pbkdf2/` | PBKDF2-HMAC-SHA256 at 210,000 rounds (decision 0007, see below) |
| `src/secrets.ts` | Provider secrets: AES-256-GCM with the reference's byte layout |
| `src/tenant-port.ts` | **The interface a tenant object implements for the control object** (K01/K02/K04) |
| `src/errors.ts` | Error texts, classes and auth codes, byte for byte |
| `test-support/` | A stub tenant object, a minimal control socket owner and a router, for tests and conformance |
| `test/` | workerd tests (`pnpm vitest run --root control` from `cloudflare/`) |

## RPC surface

Every method returns plain JSON. Control call failures come back as `Outcome` values (`{ ok:false,
error, errorClass }`), because a thrown error loses its class across RPC.

| Method | Caller | Reference |
| --- | --- | --- |
| `authenticate(request)` → `{ message, connection, service? }` | socket owner (K05) | lib.rs `authenticate`, service_principal.rs |
| `query(connection, path, args)` | socket owner | `execute_control_query` |
| `write(connection, path, args, key, kind)` | socket owner | `execute_control_write` |
| `checkSubscribe(connection, path)` | socket owner | `open_control_query` checks |
| `serviceCall(grant, path, args)` | socket owner (service socket) | `service_control_call` |
| `revalidate(connection)`, `validSessionHashes`, `validateImpersonation`, `activeMemberAccounts`, `memberById` | socket owner, every 30 s | C.3.8 item 10 |
| `projectMemberships(projectId, tenantId, rows)` | tenant object after a commit | membership_projector.rs |
| `connectionState(connectionId, state)` | socket owner | process presence and metrics |
| `watch` / `unwatch(projectId, binding, id)` and `controlChanged` on the watcher | socket owner | `RuntimeEvent::ControlChanged` |
| `fetch(request)` for `/auth/*` | router | native_auth.rs |

The socket owner keeps the per-socket state the reference keeps per connection: the live Control
subscriptions and their last result, the support-heartbeat attribution answered from memory, the
10 s auth deadline and the 30 s revalidation timer. `test-support/control-socket.ts` is a complete,
small owner that does exactly that; K05's gateway takes it over.

## The tenant port

The control object never opens a tenant database. Each reference code path that opens a tenant
transaction maps to one `TenantPort` call (`src/tenant-port.ts`): `admit`, `activeMemberAccounts`,
`memberById`, `memberAccounts`, `activeMember`, `activeMemberCount`, `writeMembership` (the seven
`members` writes of provisioning, developer membership, tenant creation, demos and seeding),
`acceptInvitation` (the whole tenant transaction of `control.invitations.accept`, module Reducer
included) and `provision`. A tenant object must also push its `_gonvex_control_plane_membership_outbox`
rows to `projectMemberships` after a commit, delete what was acknowledged, and retry from its alarm.
`test-support/stub-tenant.ts` implements all of it with the reference's SQL and triggers; it runs no
module code, so an invitation that names an acceptance Reducer fails closed there.

## Passwords (decision 0007)

Workers refuse PBKDF2 above 100,000 iterations in WebCrypto and, **on the deployed platform, in
`node:crypto` too** (`NotSupportedError: Pbkdf2 failed: iteration counts above 100000 are not
supported`). Local workerd does not enforce the cap, so only a platform run shows it. The derivation runs
in a 12.7 KB WebAssembly module built from the reference's own crates (`pbkdf2` 0.12.2 + `sha2` 0.10.9,
`wasm/pbkdf2/build.sh`), with no imports, and falls back to `src/pbkdf2.ts` for inputs larger than its
buffer. Measured on the dev account with `wrangler tail`: **about 125 ms CPU per 210k verification**
(123–176 ms over five calls; pure JS 253–417 ms). Hashes the Rust runtime stored verify unchanged
(`test/control.test.ts` uses one).

## Deviations from the reference

Each is either in the contract already or listed in `docs/cloudflare/contract-changes/0025-k06-control-object.md`.

- Control idempotency rows expire after 24 h; an expired key runs the call again (0012 R9).
- `tenants.mine`, the default tenant at sign-in and `support.listTenants` answer from
  `account_tenant_index` and re-admit through at most 32 concurrent tenant RPCs (0012 R15).
- `gonvex_auth_sessions.last_seen_at` is not written (C.3.8 item 10).
- `ControlChanged` reaches each watcher once per successful write. Rust also echoes it through its
  database listener a moment later (README of the conformance harness, "Reference behaviours").
- JWKS fetches cannot resolve-and-pin a public IP; HTTPS, no credentials, no `localhost`, no redirects
  and a 5 s timeout are kept (C.3.4).
- The Firebase emulator decoder is gated by configuration (`GONVEX_ENV=test`,
  `GONVEX_TEST_AUTH_EMULATOR=true`, a loopback `FIREBASE_AUTH_EMULATOR_HOST`) instead of a debug build.

## Commands (from `cloudflare/`)

```sh
pnpm vitest run --root control          # workerd tests
./node_modules/.bin/tsc -p control/tsconfig.json --noEmit
(cd control && ../node_modules/.bin/wrangler dev)   # local control worker on :8787
control/wasm/pbkdf2/build.sh            # rebuild src/pbkdf2.wasm (Rust, wasm32-wasip1)
```

Conformance (Rust against the control object): `cloudflare/conformance/README.md`, "Control plane".
