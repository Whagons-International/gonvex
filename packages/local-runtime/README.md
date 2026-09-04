# Local reducer execution

Experimental execution host for the existing TypeScript reducer body. This
package is private and is not connected to `GonvexClient` or a released SDK.
It does not yet make applications offline-capable.

The host runs `ReducerDefinition.handler` against an in-memory PostgreSQL
workspace using PGlite. It receives a snapshot from the Local Replica and
captures `db.insert`, `db.update`, `db.delete`, and `db.deleteMany` as one
transaction. No application-authored optimistic effects are consumed. Successful
execution returns the result and captured writes; failed execution publishes
nothing. The temporary database rolls back both input rows and staged writes
after every execution. It is not a durable store or another application cache.

`replay` executes serialized intent envelopes in order against an authoritative
base. Later reducers read the effects of earlier successful reducers. A rejected
reducer contributes no changes. Incomplete cached dependencies abort replay
without classifying pending intents as rejected.

The host uses PostgreSQL query plans to identify read dependencies, including
joins and CTEs. A complete filtered collection is not proof that an entire table
is complete; the SDK integration must establish the scope of that completeness
before constructing a snapshot. Generated schema is required even for empty
tables. Raw SQL writes through `db.query` are rejected.

Action and scheduler calls produce deferred descriptors. This host does not
send external requests or execute those descriptors. Server acceptance must own
their eventual execution. The host itself is not a security sandbox; the future
worker integration must restrict ambient capabilities as the server host does.

## Remaining integration

Before enabling this for applications:

1. Generate the browser-safe reducer entrypoint and complete local SQL schema
   from the same application sources as the server artifact. The current CLI
   deliberately emits empty schema bindings for these TypeScript projects.
2. Run the host in the SDK worker and initialize it before interactive writes.
   The current implementation seeds a disposable database from a supplied
   snapshot for each execution; large-snapshot latency has not been optimized.
3. Persist each intent's arguments, command ID, artifact version, execution time,
   and identity scope in the existing SDK outbox before exposing its transaction.
   Do not add an application queue or a second server-state store.
4. Connect local results and ordered replay to the Local Replica's transaction
   overlays. Publish the entire rebased set atomically. Retain pending intents on
   transport failure and report authoritative rejections to the caller/UI.
5. Establish matching ID allocation and version/conflict semantics in the server
   host. `deterministicId` currently proves local replay stability only; the Rust
   server has not adopted this seed contract. Never synchronize locally created
   IDs to the existing server while assuming they match.
6. Verify durable restart, real reconnect, server rejection, concurrent edits,
   tenant switching, and ambiguous network outcomes end to end. Current tests
   exercise executor behavior and serialization, not network synchronization.
7. Make this the default reducer path, remove application-authored optimistic
   declarations and blanket `onlineOnly` policies, and migrate application
   consumers after the end-to-end tests pass. Do not remove those safeguards
   while the execution/synchronization path remains incomplete.

Native hosting, local schema constraints/defaults, argument/result validation,
and server artifact upgrades also need parity before a general SDK release.

## Verification

```sh
pnpm --filter @gonvex/local-runtime test
pnpm --filter @gonvex/local-runtime build
```

Tests cover multi-table capture, failure rollback, deterministic replay,
re-execution against changed data, ordered dependent replay, serialized envelopes,
scope/artifact isolation, internal reducer exclusion, incomplete reads, SQL write
rejection, and input snapshot isolation.
