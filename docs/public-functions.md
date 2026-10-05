# Public tenant functions

Public functions let signed-out browsers call a small, explicit set of tenant functions. They do not grant a visitor an account, membership, role, or permissions. Service principals remain the server-to-server mechanism described in [service-principals.md](service-principals.md).

## Declaration and authorization

```ts
import { action, query, reducer, schema } from "@gonvex/module-sdk";

export const openJobs = query({
  public: true,
  args: schema.object({}),
  result: schema.array(schema.record(schema.any())),
  run: async ctx => ctx.db.query(
    'SELECT id, title, description FROM jobs WHERE status = $1 LIMIT 100',
    ["open"],
  ),
});

export const apply = reducer({
  public: true,
  interactive: false,
  args: schema.object({ jobId: schema.string(), consent: schema.boolean() }),
  result: schema.null(),
  run: async (ctx, args) => {
    // Validate that this job accepts applications and that consent is true.
    // Write the application in the tenant transaction after validation.
    return null;
  },
});

export const uploadApplicationFile = action({
  public: true,
  capabilities: { storage: true },
  args: schema.object({}),
  result: schema.any(),
  run: async ctx => ctx.storage.generateUploadUrl({
    contentType: "application/pdf",
    visibility: "tenant",
  }),
});
```

The signed manifest records `public: true`. Missing or false flags keep existing authentication requirements. SDK declarations, CLI artifact construction, and Rust module loading reject public internal functions, public Live Queries and Replica Collections, and public reducers with local execution, optimistic metadata, or allowed offline execution. Public reducers must explicitly set `interactive: false`; their default offline policy is `forbidden`.

Public one-shot Queries execute their JavaScript handler in a read-only tenant transaction, including when a member calls them. They may omit a structured query plan. They do not use the member-only structured-query visibility engine. The handler must validate bearer tokens, restrict columns, bound reads, and enforce its application's public policy. Marking an existing token-gated handler public makes its checks reachable; it does not implement those checks. Use parameterized SQL and argument schemas. A public declaration is deliberate authority to access that tenant's data through the handler.

Actions retain their declared capability restrictions. Exact internal Query and Reducer tool bindings can run from a public Action and inherit the same anonymous context. A tool binding cannot use an anonymous session to call a non-public business function. Host-authorized internal tool calls are the only exception to the public manifest gate.

Signed-in members may call public functions with their normal account, member, and permissions. Non-public tenant calls retain their existing behavior.

## Connection and protocol

```ts
const client = new GonvexClient("wss://runtime.example/ws", {
  project: "whagons",
  public: { tenant: "careers" }, // Canonical tenant ID or routing domain.
});

await client.query(api.recruiting.openJobs, {});
await client.action(api.recruiting.uploadApplicationFile, {});
await client.reducer(api.recruiting.apply, { jobId, consent: true });
```

Use this client with the regular `GonvexProvider`, `useQuery`, `useQueryResult`, `useAction`, and `useReducer`. An authentication provider is unnecessary. Generated function references keep their current call shape.

Public mode is fixed for the lifetime of the client. Create another client to change project or tenant, or to sign in. Credentials and refresh callbacks cannot be combined with public mode. The client uses online server calls and does not restore persisted replicas, local executors, or reducer outboxes. It repeats public authentication after reconnecting. Live Query and Replica subscriptions remain unavailable.

The actual protocol uses `auth`, not `authenticate`:

```json
{"type":"auth","id":"auth-1","project":"whagons","tenant":"careers","public":true,"controlOnly":false}
```

`token` must be absent or empty, `project` and `tenant` are required, and `controlOnly` must be absent or false. Optional `clientContract`, `device`, and `capabilities` fields retain their existing meaning. A successful response canonicalizes the tenant ID:

```json
{"type":"auth.result","id":"auth-1","result":{"projectId":"whagons","tenantId":"tenant-id","accountId":"","public":true,"artifactHash":"..."}}
```

There is no replica directive or local identity. Invalid, unknown, cross-project, deleted, disabled, and inactive tenant routes return the same public-admission error:

```json
{"type":"auth.error","id":"auth-1","error":"public tenant session is unavailable"}
```

The runtime checks an active Control Plane tenant directory row, even for configured tenant database URLs. Routing domains are case insensitive. A matching tenant ID takes precedence over a matching domain. A session stays scoped to one canonical tenant database; caller arguments cannot change it. The runtime rechecks public admission every 30 seconds and closes authorization when the tenant is disabled.

Query, Action, and Reducer call frames are unchanged. Non-public calls return an error containing `authenticate with an active tenant`. Anonymous sessions cannot subscribe to Live Queries or open Replica Collections, including batched requests. They have no change-feed subscription. Committed calls still receive the terminal replica watermark needed to resolve normal client promises.

## Context and files

For anonymous execution:

```ts
ctx.auth.account === null;
ctx.member === null;
ctx.tenant?.id; // Canonical tenant ID.
```

There are no actor account or member IDs in invocation attribution. The host supplies an empty permission object. Module SDK identity types already model account and member as nullable.

Only an explicitly public Action with `capabilities: { storage: true }` can expose an upload URL to an anonymous browser. Anonymous uploads always have `visibility: "tenant"` and an empty `ownerId`, overriding requested owner and visibility values. They are tenant files, not publicly readable objects. `GONVEX_PUBLIC_URL` must be set so the URL uses the runtime proxy; direct S3 upload URLs cannot enforce this path's byte ceiling. The existing proxy rejects bodies larger than 128 MiB. Applications should check a smaller PDF size limit and content type themselves.

Server-only Reducers expose optional `ctx.storage`, with only `getMetadata(fileId)`. This reads authoritative metadata in the reducer's own transaction and finalizes a pending upload through object-store HEAD when needed. It is absent during local reducer execution. An application reducer can require this capability and verify `tenantId`, `status === "uploaded"`, `contentType`, `size`, and ownership before linking a file. The metadata lookup binds both file ID and session tenant. It never trusts client-supplied metadata. Storage tables must already exist, normally created by the upload Action.

Anonymous calls have no durable reducer replay receipts, background Action outbox, or scheduler capabilities in v1. Their idempotency keys do not create shared anonymous receipts. Applications needing deduplication should implement it in the public business transaction, scoped to a validated application token or request ID. Disconnects can leave a write's outcome unknown.

## Abuse limits

Budgets use 60-second fixed windows, count auth attempts and each call in a batch, and apply across projects and tenants on each runtime process. Opening another connection does not reset the IP budget. A public connection occupies its IP slot until the socket closes, even if it later reauthenticates. Payload size is the UTF-8 JSON frame size.

| Environment variable | Default |
| --- | --- |
| `GONVEX_PUBLIC_CALLS_PER_CONNECTION` | 30 calls per minute |
| `GONVEX_PUBLIC_CALLS_PER_IP` | 120 calls per minute |
| `GONVEX_PUBLIC_MAX_PAYLOAD_BYTES` | 65536 bytes |
| `GONVEX_PUBLIC_CONNECTIONS_PER_IP` | 20 connections |
| `GONVEX_PUBLIC_TRUSTED_PROXY_IPS` | Empty, socket peer IP only |

Rate limits return correlated call errors containing `public call rate limit exceeded`. Connection exhaustion returns `auth.error` with `public connection limit exceeded` and closes the socket. Oversized anonymous or unscoped frames close with WebSocket code 1009. Authenticated member frames retain their existing transport limits.

Behind a proxy, configure its exact IP in the comma-separated trusted proxy list. The proxy must replace `X-Real-IP` with the real client address. Untrusted peers cannot override their IP with forwarding headers. Custom Axum servers must use `into_make_service_with_connect_info::<SocketAddr>()`; without connection info, requests share one conservative unknown-peer budget.

These budgets are process local. Multiple runtime instances need an edge rate limit for a deployment-wide budget. Function limits do not replace application controls for repeated file uploads, bot submissions, consent, token expiry, or retention.

## Release and Whagons adoption

No package version bump or publication is part of this change. The release CLI assigns one coordinated version to protocol, client, React, module SDK, local runtime, CLI, Expo SQLite, and create-gonvex, and generates release notes. There is no changeset requirement.

Deploy the new Rust runtime and module-host binaries, then release the coordinated npm packages through the repository's normal staging or stable release workflow. An old runtime rejects public auth safely. Whagons needs the new SDK and CLI to rebuild its signed module with explicit public flags, and the new client packages to open public tenant connections. Keep existing token validation, add public upload and submission handlers, and deploy that module. The existing `files.generateUploadUrl` still requires an active member and should remain unchanged for signed-in uploads.
