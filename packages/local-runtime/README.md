# Local Reducer execution

The generated Gonvex client executes an interactive Reducer's existing TypeScript
body in a browser worker. Applications define argument/result schemas and `run`;
they do not maintain a separate optimistic handler or effects list.

The CLI compiles a local SQL schema from tenant migrations and bundles the public
Reducer exports. Unused Action declarations are removed from that bundle. The
worker uses PGlite as disposable execution memory, seeded from the SDK Local
Replica. It does not persist another database or become a second source of server
state. It captures typed database writes and SQL write CTEs as one transaction.
Both seed rows and writes are rolled back after execution.

The client persists the intent, arguments, command ID, artifact hash and local
transaction in its existing durable outbox before publishing the predicted rows.
A successful local call returns the Reducer's result without waiting for a server.
On reconnect the SDK sends the same intent and idempotency key. The Rust runtime
executes the Reducer in an authoritative PostgreSQL transaction, checking the
artifact and current permissions. A retry of an already committed command returns
its stored result and commit barrier. It does not execute the write twice.

A server rejection removes the command's entire prediction and recomputes later
pending intents against the remaining base. The SDK publishes that replacement
atomically and emits `onReducerRejection`. Transport failures retain the intent.
The queue and cached session are partitioned by project, tenant and account.

Generated replica subscriptions reuse the declared visibility plans and only
columns already exposed by application reads. The runtime refuses to interpret
an incomplete collection or an omitted column as an empty/NULL value. A known
primary-key row can be read from a partial large collection. If required input is
unavailable, the intent remains queued without a guessed result or transaction.
First-time authentication and data never downloaded cannot be fabricated offline.

Database inserts use the same intent-owned ID allocator in the server module and
the local module. Explicit IDs remain supported. Action and scheduler calls are
recorded locally but only the authoritative server commits and executes their
outbox work. The worker denies ambient network access after loading its WASM.
Server timestamps, constraints, hidden data and concurrent writes remain
server-authoritative and reconcile through the change feed.

The default host targets browser workers. Other hosts can implement the exported
`LocalExecutor` interface; this package does not claim a native PGlite host.
The IndexedDB and Expo SQLite adapters both persist SDK session metadata.

## Verification

```sh
pnpm --filter @gonvex/local-runtime test
pnpm --filter @gonvex/client test
pnpm --filter @gonvex/module-sdk test
pnpm --filter @gonvex/cli test
```

Host tests cover atomic writes, SQL CTE capture, incomplete reads, ordered replay,
stable IDs, validation and scope isolation. Client tests cover durable admission,
restart, server rejection, dependent edits, concurrent admission, lost responses,
account switching and argument capture. CLI tests cover generated execution and
ensure unexposed columns and unused external Actions do not enter local delivery.
