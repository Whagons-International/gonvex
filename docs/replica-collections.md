# Replica Collections and Local Replica

The public guide lives in
[`apps/docs/content/docs/replica-collections.mdx`](../apps/docs/content/docs/replica-collections.mdx).
This file records the contributor-level contract.

Replica Collections are authorized, bounded entity sets. They complement
structured Live Queries, which own exact server-computed membership and
ordering for datasets too large to replicate.

Each source table has one required TypeScript `visibility(...)` declaration.
The runtime resolves a revisioned tenant-local context, compiles the rule into
SQLite SQL for snapshots and Live Queries, and evaluates the same rule over
the change feed's old/new rows. Dependency tables are derived from the plan;
role, team, workspace, or audience changes invalidate the context and regroup
Live Queries before results can fan out. `{ operator: "public" }` is the only
way to declare that every tenant entrant may receive a table.

## Commit and resume model

1. Row triggers stage full old/new values in `_gonvex_sync_changes`.
2. One deferred commit trigger gives every row in the transaction one revision.
3. `NOTIFY` is a wake-up hint; the committed log is the only source of truth.
4. Gonvex routes one `replica.transaction` containing all visible row changes.
5. The client persists entities, memberships, and cursor atomically, then
   notifies subscribers once.
6. Reload resumes from `{epoch, revision}` or receives a fresh authorized
   snapshot when retention or visibility requires it.

There is no declared write set, table-change callback, or broad invalidation
path. A transaction carries its `originCommandId`, allowing the Local Replica
to replace an optimistic overlay only after the authoritative revision arrives.

This is the v2 delivery contract: public reads are Queries delivered as Live
Queries or Replica Collections, and public writes are Reducers. The former
declared `Writes` metadata, manual invalidation API, and generic reactive-query
API are not part of v2. This document describes the implemented sync path and
the Control Plane account plus tenant-local Member identity model.

## One normalized store

The Local Replica owns authoritative entities, optimistic overlays, ordered
Live Query ID memberships, completeness, freshness, pending commands, and the
applied cursor. IndexedDB persists web state; `@gonvex/expo-sqlite` applies the
same transaction to normalized SQLite tables.

React bindings use `useSyncExternalStore`. `useEntity` reads one normalized
entity, `useReplicaCollection` reads a bounded collection, and
`useLiveQueryState` exposes rows plus `source`, `completeness`, and `freshness`.

## Deliberate boundaries

- Replica Collections have one source table and explicit row/byte budgets.
- Equality arguments and null/non-null exclusions are portable collection rules.
- Search, arbitrary sorting, joins, aggregates, and unbounded pagination stay
  in indexed structured Live Queries.
- A partial cache is always labeled partial offline.
- SQLite-specific Live Query expressions are explicitly online-only.
