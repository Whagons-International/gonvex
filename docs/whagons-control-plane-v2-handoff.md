# Whagons Control Plane v2 handoff

This is the migration contract for `@gonvex/*` version `0.5.1`. Whagons should
use one `GonvexClient` for Control Plane calls, tenant calls, live Control Plane
queries, and the Local Replica stream. OAuth callbacks and public customer APIs
remain HTTP because their protocols require HTTP.

Import `control` from `@gonvex/client` or the generated Gonvex API. Every
reference below has `scope: "control"`, an argument schema, a result schema,
and a host-enforced authorization class. Browser arguments cannot select a
database, account, role, tenant membership, developer status, or telemetry
attribution.

## Authentication and tenant directory

| Reference | Kind and delivery | Authorization | Arguments | Result |
| --- | --- | --- | --- | --- |
| `control.accounts.me` | Query | account | `{}` | `{ id, email, name, avatarUrl }` |
| `control.accounts.updatePassword` | Reducer | account | `{ currentPassword, newPassword }` | `{ updated }` |
| `control.accounts.resetMemberPassword` | Reducer | tenantAdmin | `{ memberId, newPassword }` | `{ updated }` |
| `control.accounts.provisionMemberLogin` | Reducer | tenantAdmin | `{ email, name, password, role, permissions }` | `{ updated, accountId, memberId }` |
| `control.auth.passwordLogin` | Action | public | `{ email, password }` | session grant |
| `control.auth.exchangeExternalToken` | Action | public | `{ provider, token, tenantId?, previousRefreshToken? }` | session grant |
| `control.auth.refreshSession` | Action | public | `{ refreshToken }` | rotated session grant |
| `control.auth.logout` | Reducer | account | `{ refreshToken, all }` | `{ updated }` |
| `control.auth.publicSettings` | Query | public | `{}` | `{ mode, providers: string[] }` |
| `control.auth.realms.list` | live Query | projectAdmin | `{}` | realm rows |
| `control.auth.realms.configure` | Reducer | projectAdmin | `{ provider, authMode?, enabled, signupMode, azureTenantId?, clientId?, clientSecret?, issuer?, audience?, jwksUrl?, firebaseProjectId?, firebaseTenantId?, adminCredentials? }` | `{ updated }` |
| `control.auth.memberProviders` | Query | tenantAdmin | `{ memberIds: string[] }` | `{ memberId, providers: string[] }[]` |
| `control.tenants.mine` | live Query | account | `{}` | tenant directory rows |
| `control.tenants.create` | Reducer | account | `{ name }` | tenant row |
| `control.tenants.getByDomain` | Query | public | `{ domain }` | `{ id, name, domain }` |
| `control.tenants.updateProfile` | Reducer | tenantAdmin | `{ name, domain, description }` | `{ updated }` |
| `control.tenants.updateTimezone` | Reducer | tenantAdmin | `{ timezone }` | `{ updated }` |
| `control.tenants.delete` | Reducer | tenantAdmin | `{}` | `{ updated }` |
| `control.tenants.setException` | Reducer | projectAdmin | `{ tenantId, value }` | `{ updated }` |
| `control.tenants.setSeatLimit` | Reducer | projectAdmin | `{ tenantId, seatLimit }` | `{ updated }` |

`GonvexAuthProvider` owns developer mode. Applications call
`enterDeveloperMode(tenantId)` and `exitDeveloperMode()` and read the safe
`developerMode` state. The one-time activation credential and each rotating
reconnect credential remain inside the provider/client process and are never
written to storage or a URL. Account refresh continues underneath developer
mode without replacing it. Expiry, revocation, reload, or an authentication
error restores the normal account session and its original tenant.

A session grant contains access and refresh credentials, the global account,
the active tenant ID, and tenant rows with `id`, `name`, `role`, `permissions`,
`domain`, `timezone`, `description`, and `profile`.

`resetMemberPassword` accepts only a tenant-local member ID. The host reloads
that member from the active tenant database, resolves its global account ID,
updates the password, revokes every access and refresh session for that
account, then closes its live connections. A browser-supplied `accountId` fails
argument decoding.

Microsoft realm rows store `azureTenantId`, `clientId`, and an encrypted client
secret. Realm queries return `hasClientSecret`, never the secret. Apple uses
`clientId` and an encrypted Apple client-secret JWT. The runtime supports
Google, Microsoft, and Apple authorize/callback flows. Apple callbacks accept
the provider's required `form_post` response.

## Firebase and pluggable project auth

Project `authMode` is one of `gonvex-native`, `firebase`, `external-oidc`, or
`hybrid`. Gonvex-native remains available, but Whagons should set `firebase`.
The host provider interface verifies an external credential and returns a
trusted identity. Shared host code then resolves the Account, issues a
`gvx_session_*` session, lists tenants, and enforces tenant-local Member
admission. Tenant modules never receive Firebase configuration, Control Plane
credentials, or a way to select another database.

Configure Whagons with a project key:

```bash
gonvex auth configure firebase \
  --firebase-project-id whagons-prod \
  --mode firebase \
  --signup-mode inviteOnly
gonvex auth status firebase
```

Optional flags are `--firebase-tenant-id`, `--issuer`, `--audience`,
`--jwks-url`, and `--admin-credentials-file`. The defaults use Firebase's
Secure Token issuer, Firebase project ID audience, and Google signing-key URL.
The runtime encrypts Admin credentials. Status and realm queries return only
`hasAdminCredentials`.

The browser adapter uses the existing Firebase SDK:

```ts
import { onIdTokenChanged, signOut } from "firebase/auth";
import { createFirebaseAuthAdapter } from "@gonvex/react";

const externalAuth = createFirebaseAuthAdapter({
  getIdToken: (forceRefresh) => auth.currentUser?.getIdToken(forceRefresh) ?? Promise.resolve(null),
  onIdTokenChanged(listener) {
    return onIdTokenChanged(auth, (user) => listener(user ? { uid: user.uid } : null));
  },
  signOut: () => signOut(auth),
});
```

Pass `externalAuth` to `GonvexAuthProvider`. Firebase owns the Google,
Microsoft, Apple, password, and linked-provider UI. Gonvex stores no Firebase
password. The provider keeps the Firebase ID token in memory and persists only
the canonical Gonvex session. Token rotation calls
`control.auth.exchangeExternalToken`, retires the previous Gonvex refresh
family, and keeps the active tenant when that Member remains active.

Gonvex access tokens last 15 minutes. Canonical refresh families last 30 days,
but the Firebase adapter re-verifies the Firebase ID token for rotation and
forced reconnect refresh. A Firebase sign-out removes the canonical session.

The host keys Firebase identities by project, provider `firebase`, issuer, and
Firebase UID. Linked Firebase sign-in providers therefore resolve to one
Account. A verified email may link one existing Account when the match is
unique; unverified email never merges identities. The host records each
resolution decision in `gonvex_auth_identity_events`.

Firebase invitation acceptance uses the already authenticated Account and the
same host-to-module invitation handoff described below. It does not create a
Gonvex password. Native password operations return a provider-owned error when
the project mode disables Gonvex-native auth.

## Developer and support operations

| Reference | Kind and delivery | Authorization | Arguments | Result |
| --- | --- | --- | --- | --- |
| `control.developer.status` | live Query | account | `{}` | `{ developer, mode, tenantId, grantId }` |
| `control.developer.provisionSelf` | Reducer | developer | `{ tenantId }` | `{ updated, tenantId, memberId }` |
| `control.developer.removeSelf` | Reducer | developer | `{ tenantId }` | `{ updated }` |
| `control.developer.enter` | Reducer | developer | `{ tenantId }` | `{ id, token, expiresAt }` |
| `control.developer.exit` | Reducer | account | `{ grantId }` | `{ updated }` |
| `control.project.developers.list` | live Query | projectAdmin | `{}` | `{ email, name, role }[]` |
| `control.project.developers.invite` | Reducer | projectAdmin | `{ email, name, role }` | `{ updated }` |
| `control.project.developers.remove` | Reducer | projectAdmin | `{ email }` | `{ updated }` |
| `control.agentAuth.issue` | Reducer | projectAdmin | `{ permissions, expiresInSeconds }` | `{ id, token }` |
| `control.agentAuth.claim` | Reducer | account | `{ token }` | `{ id, permissions }` |
| `control.agentAuth.revoke` | Reducer | projectAdmin | `{ id }` | `{ updated }` |
| `control.assistant.getDefaults` | live Query | projectAdmin | `{}` | assistant defaults |
| `control.assistant.setDefaults` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.voice.getConfiguration` | live Query | projectAdmin | `{}` | voice settings |
| `control.voice.setRateCard` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.voice.setTenantEntitlement` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.voice.setUserOverride` | Reducer | projectAdmin | `{ scopeId, value }` | `{ updated }` |
| `control.support.listSessions` | live Query | projectAdmin | `{}` | support session summaries |
| `control.support.getSession` | Query | projectAdmin | `{ id }` | support session detail |
| `control.support.listErrors` | live Query | projectAdmin | `{}` | `{ groups, releases }` |
| `control.support.getError` | Query | projectAdmin | `{ fingerprint }` | error detail |
| `control.support.listTenants` | live Query | projectAdmin | `{}` | tenant summaries |
| `control.support.getTenant` | Query | projectAdmin | `{ tenantId }` | tenant detail |
| `control.support.pruneSessions` | Reducer | projectAdmin | `{ olderThanSeconds }` | `{ deleted }` |
| `control.support.heartbeat` | Reducer | account | `{ release, environment }` | `{ sessionId }` |
| `control.support.sendCommand` | Reducer | projectAdmin | `{ sessionId, kind, payload }` | `{ id }` |
| `control.support.ackCommand` | Reducer | account | `{ id }` | `{ updated }` |
| `control.support.createImpersonation` | Reducer | projectAdmin | `{ accountId, tenantId, reason }` | `{ id, token, expiresAt }` |
| `control.demos.create` | Reducer | projectAdmin | `{ tenantId, email, name, password, label }` | `{ accountId, memberId }` |
| `control.demos.resetPassword` | Reducer | projectAdmin | `{ accountId, password }` | `{ updated }` |
| `control.demos.delete` | Reducer | projectAdmin | `{ accountId }` | `{ updated }` |

The `developer` authorization class is separate from `projectAdmin`. Project
owners, project admins, and registered developers can use only the developer
self-service calls. A `dev` project member cannot read support data, realm
secrets, voice configuration, or project settings. Developer mode uses an
audited, expiring, single-use grant.

These existing configuration queries are now live, so their matching Reducers
do not need a manual refetch:

- `control.auth.realms.list`
- `control.project.developers.list`
- `control.assistant.getDefaults`
- `control.voice.getConfiguration`
- `control.support.listSessions`
- `control.support.listTenants`
- `control.support.listErrors`

## Invitations

| Reference | Kind and delivery | Authorization | Arguments | Result |
| --- | --- | --- | --- | --- |
| `control.invitations.list` | live Query | tenantAdmin | `{}` | invitation rows |
| `control.invitations.lookup` | Query | public | `{ token }` | public invitation detail |
| `control.invitations.create` | Reducer | tenantAdmin | `{ email, role, permissions, teamIds, allowedAuthProviders, payload }` | `{ id, token }` |
| `control.invitations.update` | Reducer | tenantAdmin | `{ id, role, permissions, teamIds, allowedAuthProviders, payload }` | `{ updated }` |
| `control.invitations.revoke` | Reducer | tenantAdmin | `{ id, email }` | `{ updated }` |
| `control.invitations.accept` | Reducer | account | `{ token }` | `{ tenantId, memberId }` |

The public lookup result contains `tenantId`, `tenantName`, `email`, `role`,
`teamIds`, `allowedAuthProviders`, and `expiresAt`. The list adds permissions,
revoked/accepted state, handoff state, and timestamps. It never returns the
token.

Acceptance validates token hash, expiry, revocation, email ownership, replay
state, and linked-provider policy. The host claims the invitation in the
Control Plane. It then opens one tenant transaction, creates or activates the
canonical member, and calls the module's declared internal Reducer in that same
transaction. The Reducer applies team assignments and application payload.
After the tenant commit, the host marks the Control Plane invitation complete
and schedules the tenant-directory projection. A retry resumes at the recorded
step and cannot duplicate the member or team rows.

The Whagons TypeScript module must declare one internal Reducer:

```ts
import { internalReducer, invitationAcceptance, schema } from "@gonvex/module-sdk";

export const applyInvitation = internalReducer({
  args: schema.object({
    accountId: schema.string(),
    memberId: schema.string(),
    invitationId: schema.string(),
    teamIds: schema.array(schema.string()),
    payload: schema.any(),
  }),
  result: schema.any(),
  async run(ctx, args) {
    // Idempotently write memberTeams and other Whagons invitation state.
  },
});

export const invitationLifecycle = invitationAcceptance("invitations.applyInvitation");
```

Module artifact generation 7 signs this declaration into the canonical
artifact hash. Gonvex rejects a declaration that does not target an internal
Reducer. Tenant modules still receive no Control Plane credentials or API.

## v5.1.39 mobile bridge

The compatibility references live under `control.legacy`, but their wire paths
remain unchanged. The bridge must call them with `scope: "control"`.

| SDK reference | Wire path | Contract |
| --- | --- | --- |
| `control.legacy.users.myTenants` | `users.myTenants` | live Query returning `string[]` |
| `control.legacy.tenants.getInvitationByToken` | `tenants.getInvitationByToken` | accepts `token` or `invitationToken`; returns `tenantId`, `invitationToken`, `userEmail`, `teamIds`, `allowedAuthProviders` |
| `control.legacy.tenants.acceptInvitation` | `tenants.acceptInvitation` | accepts `token` or `invitationToken`; returns `{ tenantId, memberId }` |

These are Control Plane compatibility references, not tenant application
functions. They remain inside the trusted host.

## React, telemetry, and Replica APIs

`GonvexAuthProvider` now exposes:

```ts
signInWithPassword(email, password)
signInWithProvider("google" | "microsoft" | "apple")
signIn(provider?)
signOut({ allDevices? })
setActiveTenant(tenantId)
refreshMemberships()
createTenant(name)
inviteMember(tenantId, email, options)
acceptInvitation(token)
revokeInvitation(tenantId, email)
```

Password login uses the same `installSession` path as OAuth. It persists and
rotates the refresh token, installs account and active-tenant scope into the
client, watches `control.tenants.mine`, and clears the same state on logout.

New public hooks and client methods:

```ts
useControlQuery(reference, args)
useCurrentTenantProfile()
useInvitationList()

client.reportError(type, payload)
new GonvexErrorReporter({ client })

useReplicaCollectionState(reference, args)
useRetainedLiveQuery(referenceOrSignature, args)
useReplicaEntities(table, ids)
```

The error reporter registers again after reconnect and sends native
`error.register`, `error.envelope`, and `error.heartbeat` frames. The server
limits batch count and bytes, rate-limits each connection identity, strips
pre-auth context, and overwrites project, tenant, account, and session fields
with connection-owned values.

Replica collection state returns `rows`, `source`, `completeness`, `freshness`,
`truncated`, and `computedRevision`. Retained Live Query windows persist IDs,
ordering, and pagination metadata. `useReplicaEntities` resolves the whole ID
batch with one Local Replica subscription. Entity values are not copied into
window state.

## CLI-only E2E setup

All commands require `--runtime`, `--project`, and `--admin-key`:

```text
gonvex internal e2e-base --tenant-name E2E
gonvex internal e2e-shard --tenant-name E2E --shard worker-1 --email actor@example.test
gonvex internal clone-test-actor --tenant-id <id> --email actor@example.test
gonvex internal resolve-identity --email actor@example.test
```

`e2e-base` and `e2e-shard` derive a stable UUIDv6 tenant ID. Repeating a command
resumes the same tenant. With `--email`, the CLI resolves the existing account
inside the host and idempotently creates the tenant-local test member. The
internal member endpoint accepts only the runtime admin key and is not part of
the browser protocol.

Firebase E2E uses the production boundary. Start a Firebase Auth emulator or a
dedicated Firebase test project, sign in the test user through the Firebase
SDK, and pass its Firebase ID token through the adapter. The remaining path is:

```text
Firebase ID token
control.auth.exchangeExternalToken
control.tenants.mine
tenant selection and Member admission
TypeScript Reducer
change feed
Local Replica
```

Do not pass a Firebase custom token to Gonvex. Exchange it with Firebase first.
The CLI commands above may create the tenant shard and clone an already
resolved test Account, but they do not mint browser credentials or expose a
tenant database URL.

## Configuration and migration

Deploy the updated runtime before updating Whagons packages. Startup installs
the new Control Plane columns and tables. No Whagons application table moves
into the Control Plane.

Required configuration:

- Set `GONVEX_DASHBOARD_SESSION_SECRET` to a stable random secret before saving
  Microsoft, Apple, or Firebase Admin credentials.
- Run `gonvex auth configure firebase --firebase-project-id <id> --mode firebase`
  for Whagons. This endpoint requires the exact project key and works before the
  first project-admin Account exists.
- Register callback origins only for projects that use Gonvex-native Google,
  Microsoft, or Apple login. Firebase projects keep provider login and callback
  handling in Firebase.
- Supply Microsoft's Azure tenant ID, client ID, and client secret through
  `control.auth.realms.configure`.
- Supply Apple's service ID as `clientId` and a current signed Apple client
  secret as `clientSecret`.
- Set `GONVEX_ADMIN_KEY` only where the internal E2E CLI is needed.
- Add the internal invitation Reducer declaration before creating invitations
  with teams or application payload.

For an existing Firebase user without a Gonvex Account, the first ID-token
exchange creates the project Account and its Firebase identity mapping. No SQL
insert is required. If the user already has a migrated Gonvex Account, include
the reviewed Firebase UID mapping in the identity-v2 migration plan so the
first exchange resolves that Account. Use provider `firebase`, the accepted
issuer, and Firebase UID as the identity key. Never use a linked-provider
subject or an unverified email. The concurrent first-login lock prevents
duplicate Accounts during normal exchange.

The reference runtime image builds and installs `gonvex-module-host` and sets
`GONVEX_MODULE_HOST_BINARY`. Local `pnpm dev:runtime` builds the workers and
starts the Rust server. `/healthz` reports module-host readiness and
fails once an active TypeScript project requires a missing host. A clean
multi-tenant setup creates the project, configures Firebase with its project
key, installs the TypeScript artifact, then creates the first tenant through a
Control Plane Reducer. No manual Control Plane SQL is part of this sequence.

Visibility plans may self-join one physical table with literal `alias`,
`leftAlias`, and `selectFrom` fields. Gonvex rejects an unaliased repeated
table. It emits safe SQL aliases and records the physical table once as a
dependency, so changes on either logical side rebuild the context.

Tenant admission still loads `members(account_id, status)` from the selected
tenant database. `account_tenant_index` remains a resumable directory
projection and cannot grant access. Invitation acceptance and member changes
project only after the tenant transaction commits.

## Agent function catalog and invocation

Whagons should remove `assistantApprovedActions.ts`. The compiled Gonvex module
is the only function inventory. Add literal metadata to each function that an
agent may call:

```ts
export const start = reducer({
  interactive: true,
  description: "Start a task and assign the acting member.",
  agent: {
    tags: ["tasks", "workflow"],
    confirmation: "none",
  },
  args: schema.object({
    taskId: schema.id("tasks"),
    expectedVersion: schema.number(),
  }),
  result: schema.object({
    taskId: schema.id("tasks"),
    version: schema.number(),
  }),
  async run(ctx, args) {
    // Existing business implementation and authorization.
  },
});
```

Public Queries and Reducers retain their current interactive default. Public
Actions must set `interactive: true` before they enter the agent catalog.
Internal and system functions never enter it. Gonvex rejects computed metadata
because the artifact builder must extract exact literal values.

Generate and verify both catalog formats from the compiled artifact:

```bash
gonvex functions emit --format ndjson --output agent-api.ndjson
gonvex functions emit --format typescript --output agent-api.d.ts
gonvex functions check
```

NDJSON has one complete, grep-friendly JSON object per line. The TypeScript
file renders portable schemas, including `Id<"tasks">`, optional values,
records, literals, null, and `JsonValue` for `schema.any()`. Both files contain
the signed module artifact hash.

The agent-profile Action requests the host capability once:

```ts
export const runAgent = action({
  profile: "agent",
  capabilities: { functions: true },
  async run(ctx, input) {
    return ctx.functions.invoke({
      path: input.path,
      args: input.args,
      artifactHash: input.artifactHash,
    });
  },
});
```

The host validates the path, schema, classification, active artifact hash,
tenant, active Member, recursion depth, deadline, and normal target
authorization. It then calls the same Query, Reducer, or Action implementation
used by the UI. A hash mismatch returns the stable `STALE_AGENT_CATALOG` error.
Regenerate the catalog and retry the model turn.

`ctx.invocation` contains host-owned channel, actor, root command, current
command, parent command, and agent execution attribution. Reducer transaction
metadata keeps that root attribution across durable Actions and scheduled
work. Clients cannot supply trusted actor IDs or channels. Local Replica
messages receive only the sanitized provenance needed for audit display.

Whagons integration is mechanical:

1. Upgrade every Gonvex package to `0.5.1`.
2. Add literal metadata to functions that the agent may call.
3. Regenerate bindings and the two catalog files.
4. Give the agent ordinary grep and file-read access to the generated catalog.
5. Replace the handwritten allowlist with `ctx.functions.invoke`.
6. Handle `STALE_AGENT_CATALOG` by loading the new catalog.
7. Keep Markdown skills limited to product and workflow knowledge.

Skills do not define function names, parameters, or permission. The generated
catalog defines the executable API, and the target function remains responsible
for its normal authorization checks.

## Package release

The compatible release version is `0.5.1`, published to npm on August 26,
2026, for:

```text
@gonvex/protocol
@gonvex/client
@gonvex/expo-sqlite
@gonvex/react
@gonvex/module-sdk
@gonvex/cli
create-gonvex
```

The packed npm manifests contain exact `0.5.1` Gonvex dependencies and no
`workspace:*` entries. The packages were published in this dependency order:

```bash
pnpm --dir packages/protocol publish --access public --no-git-checks
pnpm --dir packages/client publish --access public --no-git-checks
pnpm --dir packages/expo-sqlite publish --access public --no-git-checks
pnpm --dir packages/react publish --access public --no-git-checks
pnpm --dir packages/module-sdk publish --access public --no-git-checks
pnpm --dir packages/gonvex publish --access public --no-git-checks
pnpm --dir packages/create-gonvex publish --access public --no-git-checks
```

## Validation

The final tree passed these checks on August 26, 2026:

```text
pnpm -r typecheck
pnpm -r test
pnpm build

GONVEX_TEST_POSTGRES_URL=... cargo test --manifest-path rust/Cargo.toml \
  --workspace --all-targets --locked -- --test-threads=1
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets \
  --locked -- -D warnings

node --test scripts/production-compose.test.mjs scripts/runtime-dockerfile.test.mjs \
  scripts/deploy-coolify.test.mjs scripts/release-cli.test.mjs
```

The TypeScript package run passed 19 module SDK tests, 135 client tests, one
Expo SQLite test, 29 React tests, 28 CLI tests, and 38 dashboard tests. The
complete Rust workspace, real PostgreSQL tests, and Clippy with warnings denied
passed. The deployment and release script suite also passed.

Each of the seven npm packages produced a real tarball with `pnpm pack`.
Inspection of the packed `package.json` files confirmed version `0.5.1`, exact
`0.5.1` Gonvex dependencies, and no `workspace:*` entry. The CLI tarball
contains the function catalog generator and both starter catalog files. The
client tarball contains the external-auth adapter declarations and JavaScript
implementation.

## Cutover status

Gonvex no longer needs a second browser transport or state store for these
flows. Whagons can remove its internal browser HTTP calls after it replaces the
call sites, declares the invitation Reducer, and consumes version `0.5.1`.
OAuth callbacks and external customer REST remain HTTP.

The reference runtime is a Rust HTTP/WebSocket server with separate Rust/V8
module and sandbox workers. Rust owns Control Plane auth, PostgreSQL routing,
transactions, visibility, Live Queries, Replica delivery, scheduling, storage,
and telemetry. Control Plane calls terminate in the trusted Rust host. Tenant
TypeScript modules receive capability-scoped operations and never receive
Control Plane or tenant database credentials.

Version `0.5.0` includes the explicit-null artifact hashing fix first shipped in
`0.4.1`. The TypeScript CLI and Rust runtime sign the same contract without
rewriting the query.

Version `0.5.1` preserves explicit `interactive: false` declarations. The Rust
runtime also accepts signed 0.5.0 artifacts that encode the same intent as
`classification: "system"` without an interactive field.
