# 0020: K04 module host: the storage interface it needs from K01, and three findings

Date: 2026-11-11. Status: **proposed by K04**, for the lead. Affects: K01, K04, K05, K19, the K04 entry in
`work-packages.json`.

## 1. Storage interface between K04 and K01 (proposed)

K01 is being built in parallel. K04 codes against this interface (`cloudflare/kernel/host/src/storage.ts`)
and ships a stand-in, `DurableObjectInvocationStorage`, built from S2's `engines.js`. K01 replaces the
stand-in; K04's code does not change.

```ts
interface TenantStorage {
  invocation<T>(mode: "query" | "reducer",
                body: (tx: InvocationTransaction) => Promise<T>,
                shouldCommit: (result: T) => boolean): Promise<InvocationOutcome<T>>;
}
interface InvocationTransaction {
  exec(sql: string, binds: readonly SqlValue[]): ExecResult;   // throws SqlError (Rust text + code)
  savepoint(name: string): Promise<void>;
  release(name: string): Promise<void>;
  rollbackTo(name: string): Promise<void>;
}
interface InvocationOutcome<T> { result: T; committed: boolean; commitError?: SqlError; commitCheckRowsRead: number }
```

| Duty | Owner | Rule |
|---|---|---|
| One storage transaction per invocation; a Query never commits | K01 | G0-3 |
| `SAVEPOINT` / `RELEASE` / `ROLLBACK TO` as nested storage transactions; open savepoints settle at the end | K01 | 0006, G0-3 |
| `$N` → `?N` in `exec` | K01 | 0011.1 |
| A Query statement that writes fails with `error returned from database: (code: 8) attempt to write a readonly database` | K01 | 0011.2 |
| Deferred-FK check before commit, reported as `commitError` with Rust's text | K01 | G0-5 |
| Admission, typing, catalogs, the aborted-transaction rule, refusing the commit after a caught SQL error | K04 | B.2a, B.7 |
| `SqlError.message` is sqlx's text: `error returned from database: (code: N) <sqlite message>` | K01 | B.5 |

The stand-in takes two shortcuts that K01 must not: its FK check scans the whole
`pragma_foreign_key_check` (S2: 55 ms per 1M child rows), and it leaves the 100-parameter error to the
platform.

Any kernel code that changes the schema outside the host (K02 migrations, the A01 transform) calls
`TenantModuleHost.schemaChanged()` after it commits, so the catalog cache is dropped. A new generation
drops it too.

## 2. Finding: no Whagons Query runs module JavaScript

All 859 Whagons Queries are plans that the runtime executes itself:

- 474 replica collections;
- 35 one-shot plans and 10 live plans with a `liveQueryPlan`;
- 340 generated `__local.*` collections.

None of them has a handler. Checked on the whagons5-client `2457bb65f` artifact. Rust runs them in
`RT/execution.rs:300-330` and never calls the module host for them. K04's `done_when` asks for "a real
Query from the app bundle" to run through the host. Literally, that cannot happen. Dispatching such an
export gives Rust's own outcome, `invalid_artifact: module export for <path> is not a callable query
handler`, which is what K04 does.

The Query path through the host is covered by the 10 Query vectors and the host tests. The plan executors
belong to K08 (structured plans) and K10 (replica snapshots).
**Lead:** reword K04's `done_when` to "a real Reducer runs from the app bundle; the Query dispatch path
passes the vectors", and move "a real Whagons Query" to K08's.

## 3. Finding: the module globals are lexical bindings, not `globalThis` patches

B.3 allows either form. K04 binds them lexically, as generated local bundles do. The kernel build
prepends one import to the app bundle (`src/bundle.ts`), and that import binds `Date`, `Math`,
`crypto`, `console`, `globalThis`, `self` and the globals Rust does not install (`fetch`, `WebSocket`,
`Request`, `caches`, `performance`, …) inside the bundle only. The invocation behind them comes from
`AsyncLocalStorage` (`nodejs_als`).

Two consequences:

- The kernel's own clock and crypto stay native.
- A Query or Reducer cannot reach `fetch` even as `globalThis.fetch`. The S2 spike patched `globalThis`,
  which left workerd's `fetch` reachable from module code.

**K19 builds the Worker version with this prelude.**

## 4. Finding: an export that does not resolve fails its call, not the load

Rust checks schemas and duplicates when it loads a generation. If an export has no handler, Rust fails
that call at dispatch, not the load (`V8/lib.rs:261-296`, `BS:766-772`). K04 does the same. Without
this, the Whagons artifact would not load at all, because of the 859 plan Queries above.
