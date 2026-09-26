# Service principals

A service principal is a trusted backend service, such as an HTTP API
gateway, that needs to run application functions for a tenant without a
browser session. The runtime has no HTTP route for application functions and
never will; a service principal uses the ordinary WebSocket protocol.

## Configuration

`GONVEX_SERVICE_PRINCIPALS` is a JSON array. The runtime stores only the
SHA-256 digest of each credential. The Whagons External API gateway, for
example, runs with:

```json
[
  {
    "id": "whagons-api-gateway",
    "tokenSha256": "<hex>",
    "projects": ["<project>"],
    "functions": ["apiKeys.authenticate"],
    "maxDelegationSeconds": 900,
    "manifest": true
  }
]
```

- `id`: `[A-Za-z0-9_-]{1,64}`, unique. It appears in logs and grant rows.
- `tokenSha256`: digest of a credential that starts with `gvx_svc_`. Generate a
  credential with at least 256 bits of randomness, for example
  `echo "gvx_svc_$(openssl rand -hex 32)"`, and digest it with
  `printf %s "$TOKEN" | sha256sum`.
- `projects`: projects the credential may open. Required.
- `functions`: exact tenant module paths the principal may call directly. They
  may be `internal`. `control.*` paths and wildcards are rejected at startup.
- `maxDelegationSeconds`: upper bound for delegated member grants, 60 to 3600.
  Defaults to 900.
- `manifest`: `true` lets the credential read `/dev/manifest` for its
  `projects`. Defaults to `false`.

An invalid value or an unknown field stops the runtime at startup. Rotate a
credential by adding a second principal with the new digest, moving the
service to it, and removing the old entry.

## Protocol

1. Open `/ws` and send
   `{"type":"auth","id":"a1","token":"gvx_svc_...","project":"<p>","tenant":"<t>"}`.
   The socket is bound to that project and tenant. Every failure (unknown
   credential, project outside the allowlist, missing tenant) returns the same
   `auth.error`.
2. On that socket, `reducer.call`, `action.call` and `query.call` may target
   only the allowlisted `functions`. They run as the system identity
   `_gonvex_service:<id>` on the `api` invocation channel. Subscriptions,
   replicas, batches and every other Control Plane function are refused.
3. `reducer.call` with `"scope":"control"` and path
   `control.servicePrincipals.delegate`, args
   `{"accountId","memberId","reason","expiresInSeconds"?,"actor"?}`, returns
   `{"id","token","expiresAt","expiresInSeconds"}`. The target must be an active
   member of the socket's tenant and `memberId` must be that account's member.
   The token is a single-use `gvx_imp_` grant stored in
   `gonvex_impersonation_grants` with actor `service:<id>` and reason
   `service:<id>: <reason>`.
4. Redeem the token on a new socket with a normal `auth` frame (`token`,
   `project`, `tenant`, `clientContract`). The socket is a regular tenant
   session for that member: permissions, visibility, 30-second revalidation,
   `session-expired` when the grant expires or is revoked, and
   `membership-changed` when the member changes. `auth.result` carries a
   rotating `developerSessionToken` for reconnects until the grant expires,
   and `impersonatorId` is `service:<id>`.
5. `control.servicePrincipals.revokeDelegation` with `{"id"}` revokes a grant
   the principal created in its own tenant.

Delegation has no Control Plane idempotency record, because the result is a
secret; a retried call mints a second short-lived grant.

## Delegation actor

`actor` names who the service acts for when that is not the member itself,
such as one API key:

```json
{"kind": "api_key", "name": "CI deploy key", "reference": "key_01J..."}
```

- `kind`: 1 to 32 characters of `[a-z0-9_-]`.
- `name`: 1 to 120 characters after trimming, without control characters.
- `reference`: optional, at most 200 characters after trimming, without
  control characters. An empty reference is stored as absent.

A non-object `actor`, any other field inside it, or an invalid value fails
the call with `invalid arguments`, before any grant is written. The grant
keeps the actor in the nullable JSONB column
`gonvex_impersonation_grants.delegation_actor`. A Control Plane created
earlier gains that column the next time a runtime starts.

Runtimes without this feature reject `actor` as an unknown argument. Send it
only to runtimes that support it.

The actor is attribution. It does not change access, which still comes from
the member's own permissions.

## Delegated sessions

A socket that redeems a service principal grant is a delegated session. It
is still an ordinary member socket. `replica.open`, `query.subscribe`,
`query.call`, `reducer.call` and `action.call` behave as they do for the
member's own client. The limits in step 2 apply to the service socket only.

Modules see the delegation in `ctx.invocation`:

- Direct `reducer.call` and `action.call` run with `channel` and
  `rootChannel` set to `api` instead of `ui`.
- Calls made through `ctx.functions.invoke` or Action tools run on channel
  `agent` and keep root channel `api`.
- Outbox Actions and scheduled jobs queued by a delegated call keep root
  channel `api` and the delegation. Their own channel is `system` or
  `scheduler`, as for any durable work.
- `ctx.invocation.delegation` names the principal and the actor.

```ts
ctx.invocation.delegation;
// {
//   principal: "whagons-api-gateway",
//   actor: { kind: "api_key", name: "CI deploy key", reference: "key_01J..." },
// }
```

`actor` is `null` when the grant has none, and `reference` is left out when
absent. `ctx.invocation.delegation` is `null` for every other session,
including developer mode, support impersonation and the service socket's own
calls. The TypeScript type is `InvocationDelegation` from
`@gonvex/module-sdk`.

`query.call`, `query.subscribe` and `replica.open` execute structured plans
without a module handler, so they have no `ctx.invocation` to label.

## Function manifest

A principal with `"manifest": true` can read the function catalog of its
projects:

```http
GET /dev/manifest?project=<project id>
Authorization: Bearer gvx_svc_...
```

The `x-gonvex-project-id` header works in place of the query parameter. The
response has `project`, `functions` and `module` (`hash` and `generation`).
It leaves out `schema` and `visibility`, which project keys and operator
tokens still receive. A service credential without `manifest: true`, or one
asking for a project outside its `projects`, gets `403`. The runtime does not
fall back to project-key or operator authorization for a known service
credential.

## Audit

Logs use the `gonvex_runtime::service_principal` target and include the
principal, project, tenant, member, grant id, reason, and the actor's kind
and reference. Refused manifest reads are logged as warnings. Credentials and
tokens are never logged. Grant rows keep the full delegation history,
including the actor.
