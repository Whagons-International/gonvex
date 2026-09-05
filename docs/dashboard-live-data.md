# Dashboard live data

The dashboard uses a host-owned system service at `/dev/dashboard/ws`. It is not a row in `gonvex_runtime_projects`, has no project key, and does not need provisioning, module deployment, a tenant, or a separate database. It cannot appear in project discovery, account memberships, or user project billing.

`@gonvex/client` subscribes to the `dashboard.read` control query on this connection. Its arguments identify an allowlisted operator resource, project, tenant, and an optional legacy development key. Authentication uses the SDK's auth frame, not the URL. Every snapshot invokes the existing read-only operator handler, including its account, project, token-permission, and tenant routing checks. Reauthentication cancels existing reads; subscription generations discard queued responses from earlier reads.

Projects, notifications, manifests, tenants, table lists, row pages, files, account token metadata, and project members use this service. Administrative writes and secret/environment management retain their existing online HTTP APIs. Health charts, logs, schedules, and connection metrics share the existing metrics stream, which now survives route changes and reconnects after disconnection.

## Cache and updates

- Identical reads share a subscription and synchronously replay the latest result on navigation.
- Unused reads expire after 30 seconds. The cache evicts unused entries when opening a 33rd entry. The server limits a connection to 64 subscriptions, a snapshot to 8 MiB, and a read to 15 seconds.
- Results live in memory. Browsed customer rows and credentials are not written to browser persistence. Logout, account changes, and app unmount clear connections and caches. Changing project or auth session remounts page state.
- Successful operator writes invalidate dashboard subscriptions after completion. Control-plane events and module reloads also refresh them. Data reads attach to Gonvex's durable database change feed after authorization when that database has a sync clock. Bursts coalesce for 100 ms, and unchanged results are not sent again.
- A 30-second reconciliation catches missed events, external changes without a feed, and permission revocation. Telemetry resources use a shorter reconciliation interval. Existing metrics retain their own stream cadence.
- Older runtimes can fall back to HTTP after 500 ms without a system response. A system authorization error clears the cached result and does not trigger HTTP fallback. Older runtimes do not provide the new live-data behavior.

This cache accelerates navigation within an open dashboard. A full browser reload starts a new memory cache. It does not enable offline administrative writes.

## Table-list latency

The previous table-list handler fetched each table's columns and then ran an exact `COUNT(*)`, sequentially. One large table could delay the whole data browser.

The handler now makes one PostgreSQL catalog query for table names, columns, and estimated row counts. The response marks counts with `rowCountEstimated: true`, and the sidebar and schema views show `≈`. The selected row page still returns its exact filtered total. Ordinary PostgreSQL statistics refreshes update the sidebar estimates; an unanalyzed table can have an estimate of zero.

## Deployment and verification

Build and deploy both the runtime and dashboard from the same checkout. The dashboard Docker build includes the workspace SDK, including the fix that recognizes signed-out control subscriptions as control-plane work. No system project migration, new secret, or environment variable is required. The dashboard Node server and Vite proxy both support WebSocket upgrades. External reverse proxies must forward upgrades for `/dev/dashboard/ws` and `/dev/metrics/stream`.

Checks cover cache sharing and expiry, session cleanup, signed-in and signed-out SDK connections, streamed updates, auth rejection without HTTP fallback, the production WebSocket proxy, resource allowlisting, and reuse of operator authorization.

An isolated local PostgreSQL verification received the first SDK row snapshot in 15 ms and reflected an operator row edit in 152 ms. Browser verification also showed the edited cell without reloading. These are local measurements, not production latency guarantees.
