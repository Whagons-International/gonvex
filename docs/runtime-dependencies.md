# Runtime dependencies and backend choices

Gonvex runs transactional TypeScript functions and delivers committed changes to
local replicas. Its core guarantees concern transaction outcomes, authorization,
ordered change history and recovery. Analysis tools are optional extensions.

## Current deployment

The Rust server stores application and Control Plane data in SQLite files that
it owns, under `GONVEX_DATA_DIR` (see [SQLite storage](sqlite-storage.md)). No
database server is required. SQLx provides database access. Tokio and Axum provide async HTTP/WebSocket
services. A separate native V8 process executes application JavaScript.

Redis and Valkey are not used by the Rust runtime. Reference Compose files and
new project configuration therefore do not start or require them. Existing
deployments may still have an unused service or volume; this source change does
not delete either. Historical release notes describe older implementations.
The existing health response retains its cache fields, which report disabled.

S3-compatible storage is needed only for file APIs. The reference full stack
includes MinIO. Browser replicas use IndexedDB; the Expo adapter uses SQLite.
Neither local storage adapter replaces the authoritative server database files.

DuckDB and the spreadsheet reader are excluded from default Rust builds.
The `duckdb` feature enables the sandbox's analysis binding and file imports.
Enable it on both `gonvex-runtime` and `gonvex-sandbox-worker`, or build the
Docker image with `GONVEX_DUCKDB=true`. Ordinary sandbox TypeScript and workspace
files do not require it. See [build commands](../rust/README.md#optional-duckdb-analysis).

## Why SQLite is the server backend

Gonvex moved from PostgreSQL to SQLite on 2026-11-02. On the same two cores a
point read, a 100-row grid read and a reducer write cost several times less CPU
in process than through a PostgreSQL connection, opening a database costs about
1 ms instead of 12.5 ms, and the connection-pool failure modes (exhaustion,
churn, `too many connections`) disappear. [SQLite storage](sqlite-storage.md)
has the measurements and the full contract.

The guarantees are unchanged in meaning. Row triggers append to the durable
`_gonvex_sync_changes` log; each commit stamps one revision inside its own
transaction, and only then does the runtime publish an in-process wake-up (this
replaces `LISTEN/NOTIFY`). The notification is not the durable change record:
listeners read the log, recover after reconnect and periodically check it.

The costs are explicit. Each database file has one writer connection, so a
tenant's writes are serialized in process (different tenants write in
parallel). One runtime process owns a data directory and holds a lock file;
horizontal replicas over shared files and overlapping rolling deploys are not
supported. Backups are file snapshots (online backup API); continuous
replication is a follow-up. Application SQL follows the dialect rules in the
storage contract.

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
Durable Object control. They avoid a full Wasm port, but still need a persistent
volume for the data directory and validation of process isolation, restart and sandbox permissions.
No Cloudflare deployment or compatibility test is part of this change.

Keep portable policy code separate from those services. Evaluate a narrowly
scoped Durable Object prototype against the existing replica protocol and
benchmark it before committing to a second server backend.

## References

- [SQLite write-ahead logging](https://www.sqlite.org/wal.html)
- [SQLite triggers](https://www.sqlite.org/lang_createtrigger.html) and [isolation](https://www.sqlite.org/isolation.html)
- [Durable Objects architecture](https://developers.cloudflare.com/durable-objects/concepts/what-are-durable-objects/)
- [Durable Object SQLite transactions](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Rust on Workers](https://developers.cloudflare.com/workers/languages/rust/)
- [Cloudflare Containers](https://developers.cloudflare.com/containers/)
