# Whagons Control Plane v2 handoff

This is the migration contract for replacing Whagons' internal browser HTTP
calls. Import `control` from the generated `gonvex/_generated/api` module. The
generated value re-exports the same typed references from `@gonvex/client`.
Use the existing `GonvexClient`. Do not add another client, transport, cache,
or database selector.

## Generated Control Plane references

Every reference has `scope: "control"`, `delivery: "oneShot"`, an argument
schema, a result schema, and one host-enforced authorization class. The checked
in schema source is `packages/client/src/control.ts`.

| Reference | Kind | Authorization | Arguments | Result |
| --- | --- | --- | --- | --- |
| `control.accounts.me` | Query | account | `{}` | `{ id, email, name, avatarUrl }` |
| `control.accounts.updatePassword` | Reducer | account | `{ currentPassword, newPassword }` | `{ updated }` |
| `control.accounts.provisionMemberLogin` | Reducer | tenantAdmin | `{ email, name, password, role, permissions }` | `{ updated, accountId, memberId }` |
| `control.auth.passwordLogin` | Action | public | `{ email, password }` | session grant |
| `control.auth.refreshSession` | Action | public | `{ refreshToken }` | rotated session grant |
| `control.auth.logout` | Reducer | account | `{ refreshToken, all }` | `{ updated }` |
| `control.auth.publicSettings` | Query | public | `{}` | `{ providers: string[] }` |
| `control.auth.realms.list` | Query | projectAdmin | `{}` | `{ provider, enabled, signupMode }[]` |
| `control.auth.realms.configure` | Reducer | projectAdmin | `{ provider, enabled, signupMode }` | `{ updated }` |
| `control.tenants.mine` | Query | account | `{}` | tenant directory rows without database URLs |
| `control.tenants.create` | Reducer | account | `{ name }` | `{ id, name, role, permissions }` |
| `control.tenants.getByDomain` | Query | public | `{ domain }` | `{ id, name, domain }` |
| `control.tenants.updateProfile` | Reducer | tenantAdmin | `{ name, domain, description }` | `{ updated }` |
| `control.tenants.updateTimezone` | Reducer | tenantAdmin | `{ timezone }` | `{ updated }` |
| `control.tenants.delete` | Reducer | tenant owner | `{}` | `{ updated }` |
| `control.tenants.setException` | Reducer | projectAdmin | `{ tenantId, value }` | `{ updated }` |
| `control.tenants.setSeatLimit` | Reducer | projectAdmin | `{ tenantId, seatLimit }` | `{ updated }` |
| `control.invitations.lookup` | Query | public | `{ token }` | `{ tenantName, role, expiresAt }` |
| `control.invitations.create` | Reducer | tenantAdmin | `{ email, role, permissions }` | `{ id, token }` |
| `control.invitations.accept` | Reducer | account | `{ token }` | `{ tenantId, memberId }` |
| `control.invitations.revoke` | Reducer | tenantAdmin | `{ id, email }` | `{ updated }` |
| `control.agentAuth.issue` | Reducer | projectAdmin | `{ permissions, expiresInSeconds }` | `{ id, token }` |
| `control.agentAuth.claim` | Reducer | account | `{ token }` | `{ id, permissions }` |
| `control.agentAuth.revoke` | Reducer | projectAdmin | `{ id }` | `{ updated }` |
| `control.project.developers.list` | Query | projectAdmin | `{}` | `{ email, name, role }[]` |
| `control.project.developers.invite` | Reducer | projectAdmin | `{ email, name, role }` | `{ updated }` |
| `control.project.developers.remove` | Reducer | projectAdmin | `{ email }` | `{ updated }` |
| `control.assistant.getDefaults` | Query | projectAdmin | `{}` | configured JSON value |
| `control.assistant.setDefaults` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.voice.getConfiguration` | Query | projectAdmin | `{}` | `{ kind, scopeId, value }[]` |
| `control.voice.setRateCard` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.voice.setTenantEntitlement` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.voice.setUserOverride` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.support.listSessions` | Query | projectAdmin | `{}` | `{ id, tenantId, accountId, release, environment, lastSeenAt }[]` |
| `control.support.listTenants` | Query | projectAdmin | `{}` | `{ id, name, domain, status, timezone, seatLimit, createdAt }[]` |
| `control.support.listErrors` | Query | projectAdmin | `{}` | `{ groups: ErrorGroup[], releases: string[] }` |
| `control.support.heartbeat` | Reducer | account | `{ release, environment }` | `{ sessionId }` |
| `control.support.sendCommand` | Reducer | projectAdmin | `{ sessionId, kind, payload }` | `{ id }` |
| `control.support.ackCommand` | Reducer | account | `{ id }` | `{ updated }` |
| `control.support.createImpersonation` | Reducer | projectAdmin | `{ accountId, tenantId, reason }` | `{ id, token, expiresAt }` |
| `control.demos.create` | Reducer | projectAdmin | `{ tenantId, email, name, password, label }` | `{ accountId, memberId }` |
| `control.demos.resetPassword` | Reducer | projectAdmin | `{ accountId, password }` | `{ updated }` |
| `control.demos.delete` | Reducer | projectAdmin | `{ accountId }` | `{ updated }` |

`ErrorGroup` contains `fingerprint`, `project`, `title`, `level`, optional
`culprit`, `status`, `priority`, optional `assignee`, `firstSeen`, `lastSeen`,
`count`, per-tenant/release/environment/account/device count maps, `regression`,
and the latest redacted event. The exported TypeScript types and machine-readable
schemas live in `packages/client/src/control.ts`.

The host binds tenant-admin references to the active tenant. Only
project-admin references accept a target `tenantId`. No reference accepts a
database name, URL, connection string, role assertion, account override, or
session attribution override.

## Client and React APIs

Use these public APIs instead of `client.localReplica` access or a grid row
cache:

```ts
client.replicaCollectionState(reference, args)
client.retainedLiveQuery(signature)
client.replicaEntities(table, ids)
client.onSupportCommand(handler)
```

```ts
useReplicaCollectionState(reference, args)
useRetainedLiveQuery(referenceOrSignature, args)
useReplicaEntities(table, ids)
```

`useReplicaCollectionState` returns normalized rows plus `source`,
`completeness`, `freshness`, `truncated`, and `computedRevision`. A collection
is partial while its snapshot is verifying and remains partial when the server
reports truncation. The metadata persists with the Local Replica.

`useRetainedLiveQuery` owns only ordered IDs and pagination metadata.
`useReplicaEntities` resolves an ID batch from the normalized entity store with
one subscription. A transaction updates all affected entities and memberships
before React receives one notification.

## Whagons replacements

Replace internal auth and directory fetches with `control.accounts.*`,
`control.auth.*`, and `control.tenants.*`. Keep OAuth authorize, callback, and
authorization-code exchange as HTTP because OAuth requires those public
endpoints.

Replace invitation HTTP with `control.invitations.*`. Membership management,
roles, teams, assignments, and other tenant business state stay in Whagons
tenant Reducers.

Replace support/admin HTTP with `control.support.*`, `control.project.*`,
`control.assistant.*`, `control.voice.*`, and `control.demos.*`. Subscribe to
remote commands with `client.onSupportCommand` and acknowledge each command
through `control.support.ackCommand`.

Remove task-count completeness guesses. Render the value returned by
`useReplicaCollectionState`. Remove the virtual-grid entity `rowCache`. Persist
and retain the Live Query window, then resolve `window.ids` through
`useReplicaEntities`.

Do not copy Gonvex entities into React state, issue a manual refetch after a
Reducer, open a second WebSocket, call internal auth HTTP, or read Local Replica
storage directly.

## Runtime and migration requirements

The runtime applies the Control Plane schema additions during normal Control
Plane migration/startup. Deploy the updated runtime before using the new
references. Existing identity-v2 rules remain unchanged:

1. `account.id` is global.
2. `member.id` is tenant-local and is preserved when an existing member is
   activated.
3. The tenant `members` row is the admission authority.
4. `account_tenant_index` is an asynchronous directory projection only.

The update adds Control Plane tables for passwords, idempotency claims, tenant
provisioning checkpoints, agent claims, support sessions and commands,
impersonation grants, project settings, and demo accounts. It also adds tenant
profile/timezone/deletion fields and invitation token lifecycle fields. No
Whagons tenant business table is added to the Control Plane.

The production image must include both `gonvex-runtime` and
`gonvex-module-host`. The Go runtime currently owns HTTP, authentication,
Postgres, the change feed, visibility, and the Replica protocol. The Rust/V8
host owns TypeScript module execution and generation lifecycle. This change
does not claim that the complete Gonvex runtime has moved to Rust.

Control Plane calls terminate in the host before tenant module dispatch, so
they never cross the Rust/V8 module ABI. The shared browser protocol gained the
`control` execution scope. The Rust module protocol did not gain database or
Control Plane capabilities, which preserves tenant-module isolation.

Internal test automation uses the authenticated CLI surface:

```text
gonvex internal provision-tenant
gonvex internal resolve-identity
gonvex internal e2e-setup
```

All commands require `--runtime`, `--project`, and `--admin-key`. They are not
browser APIs.

## Package versions

The publish-ready package versions are:

```text
@gonvex/protocol 0.2.0
@gonvex/client   0.2.0
@gonvex/react    0.2.0
@gonvex/cli      0.2.0
@gonvex/module-sdk 0.2.0
```

They have not been published. Use workspace packages or a release candidate
until explicit publish approval is given.

## Verification

The release gate passed with these commands:

```text
go test ./...
go vet ./...
go test -race ./server/internal/server -count=1
GONVEX_TEST_POSTGRES_URL=... go test ./server/internal/server -count=1
pnpm test
pnpm typecheck
cargo test --workspace --locked -q
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The final focused package run passed 129 client tests and 22 React tests. npm
pack checks produced 0.2.0 tarballs with 6 protocol files, 36 client files, 9
React files, 9 module SDK files, and 47 CLI files. The packed client and React
manifests reference the matching 0.2.0 Gonvex dependencies instead of
`workspace:*`.

## Remaining cutover conditions

There is no Gonvex-side blocker to removing Whagons' internal browser HTTP,
task-count completeness heuristic, or virtual-grid entity cache. Whagons still
has to replace its call sites and implement tenant business operations as
TypeScript Reducers. The 0.2.0 packages must also be published or consumed from
the workspace before a production build can import them.

Do not remove Whagons' existing isolated code-execution sandbox during this
cutover. Gonvex's host-owned agent sandbox and opt-in DuckDB capability are
available, but replacing the old sandbox is a separate behavioral-parity gate.
OAuth endpoints and customer-facing external REST are public protocols, not an
internal function transport, and remain HTTP.
