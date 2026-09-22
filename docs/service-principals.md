# Service principals

A service principal is a trusted backend service, such as an HTTP API
gateway, that needs to run application functions for a tenant without a
browser session. The runtime has no HTTP route for application functions and
never will; a service principal uses the ordinary WebSocket protocol.

## Configuration

`GONVEX_SERVICE_PRINCIPALS` is a JSON array. The runtime stores only the
SHA-256 digest of each credential:

```json
[
  {
    "id": "api-gateway",
    "tokenSha256": "<sha256 hex of gvx_svc_...>",
    "projects": ["<project id>"],
    "functions": ["apiKeys.authenticate"],
    "maxDelegationSeconds": 900
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

An invalid value stops the runtime at startup. Rotate a credential by adding a
second principal with the new digest, moving the service to it, and removing
the old entry.

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
   `{"accountId","memberId","reason","expiresInSeconds"?}`, returns
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
   rotating `developerSessionToken` for reconnects until the grant expires.
5. `control.servicePrincipals.revokeDelegation` with `{"id"}` revokes a grant
   the principal created in its own tenant.

Delegation has no Control Plane idempotency record, because the result is a
secret; a retried call mints a second short-lived grant.

## Audit

Logs use the `gonvex_runtime::service_principal` target and include the
principal, project, tenant, member, grant id and reason. Credentials and
tokens are never logged. Grant rows keep the full delegation history.
