# Runtime dependencies and backend choices

Gonvex runs transactional TypeScript functions and delivers committed changes to
local replicas. Its core guarantees concern transaction outcomes, authorization,
ordered change history and recovery. Analysis tools are optional extensions.

## Current deployment

The Rust server requires PostgreSQL for application and control-plane data.
SQLx provides database access. Tokio and Axum provide async HTTP/WebSocket
services. A separate native V8 process executes application JavaScript.

Redis and Valkey are not used by the Rust runtime. Reference Compose files and
new project configuration therefore do not start or require them. Existing
deployments may still have an unused service or volume; this source change does
not delete either. Historical release notes describe older implementations.
The existing health response retains its cache fields, which report disabled.

S3-compatible storage is needed only for file APIs. The reference full stack
includes MinIO. Browser replicas use IndexedDB; the Expo adapter uses SQLite.
Neither local storage adapter replaces the authoritative server database.

DuckDB and the spreadsheet reader are excluded from default Rust builds.
The `duckdb` feature enables the sandbox's analysis binding and file imports.
Enable it on both `gonvex-runtime` and `gonvex-sandbox-worker`, or build the
Docker image with `GONVEX_DUCKDB=true`. Ordinary sandbox TypeScript and workspace
files do not require it. See [build commands](../rust/README.md#optional-duckdb-analysis).

## Why PostgreSQL remains the reference backend

Current code uses PostgreSQL triggers to capture row changes, a durable
transaction change log, transactional revision updates, JSONB, typed parameters,
advisory locks and row locks for worker coordination. The server also exposes SQL,
so backend portability includes application query semantics and migrations.

`LISTEN/NOTIFY` wakes listeners after commit. The notification is not the durable
change record. Listeners read the log, recover after reconnect and periodically
check it even without notifications. Removing a notification service therefore
requires a replacement wakeup strategy, not a replacement for durable history.

SQLite supports row triggers and transactions. It could implement equivalent
change-capture and log guarantees, but it is not compatible with the current
PostgreSQL SQL, types, locking or driver. Ordinary SQLite serializes writers per
database, so a server implementation also needs an explicit ownership and
concurrency model. A local PostgreSQL instance remains necessary for backend
tests. SDK SQLite tests validate the local adapter only.

The current recommendation is to retain PostgreSQL and define explicit contracts
for transaction completion, committed change capture, replay and worker claims.
Do not flatten those guarantees into a generic SQL interface. A future SQLite
backend should first pass the same contract suite, including rollback, concurrent
writes, reconnect recovery, visibility and idempotency.

## Durable Objects as a possible deployment adapter

Cloudflare Durable Objects combine private transactional SQLite storage with
single-threaded compute and WebSocket support. Tenant or room ownership makes
them worth investigating for connection coordination or a separate embedded
backend. This is an architectural option, not a supported deployment today.

The existing native Tokio server, V8 processes, local sockets and filesystem
isolation cannot run unchanged in the Workers JavaScript/Wasm environment.
An adapter would need new execution, storage, scheduling and restart boundaries.
Object storage limits and per-object throughput matter; splitting one tenant
across objects also gives up a single local transaction across those objects.
Cross-object atomicity cannot be assumed.

Cloudflare Containers are a separate option for hosting a native runtime under
Durable Object control. They avoid a full Wasm port, but still need PostgreSQL
connectivity and validation of process isolation, restart and sandbox permissions.
No Cloudflare deployment or compatibility test is part of this change.

Keep portable policy code separate from those services. Evaluate a narrowly
scoped Durable Object prototype against the existing replica protocol and
benchmark it before committing to a second server backend.

## References

- [PostgreSQL notification transaction behavior](https://www.postgresql.org/docs/current/sql-notify.html)
- [SQLite triggers](https://www.sqlite.org/lang_createtrigger.html) and [isolation](https://www.sqlite.org/isolation.html)
- [Durable Objects architecture](https://developers.cloudflare.com/durable-objects/concepts/what-are-durable-objects/)
- [Durable Object SQLite transactions](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Rust on Workers](https://developers.cloudflare.com/workers/languages/rust/)
- [Cloudflare Containers](https://developers.cloudflare.com/containers/)
