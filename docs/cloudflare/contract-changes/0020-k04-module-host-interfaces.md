# 0020: K04 module host: the storage interface it needs from K01, three findings and the R25 proposal

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

## 5. 0012 R25 / 0016 G0-11: a runaway Reducer (measured; proposal)

Measured on the platform. Details and raw numbers are in `cloudflare/kernel/host/probes/r25/FINDINGS.md`;
the summary is in `cloudflare/kernel/host/README.md`.

A CPU overrun resets the tenant object. Committed data is intact and the runaway's writes roll back.

- Every call to the tenant is blocked for the whole overrun: about 34–36 s at the default limit.
- From about 7 s, new calls are shed and tenant-held sockets are closed with 1013.
- Gateway-held sockets were never dropped.
- The platform clock does not advance during synchronous work. A wall-clock budget cannot fire inside a
  Reducer that never awaits a real timer.

K04 implements a host-call counter (`maxHostCalls`, default 100,000) and a per-object command quarantine.
Both answer with Rust's `module exceeded its 10000 ms execution deadline` (budget_exceeded, transient).
The counter is a deviation in mechanism, not in text: it fires on call count, not on elapsed time.

**Proposed rulings:**

| # | Proposal | Why |
|---|---|---|
| a | Callers (K05 gateway, K07, the Action Worker for tool calls) map `Durable Object exceeded its CPU time limit and was reset.` to Rust's deadline error for the command that caused it, then record the command id on a fresh stub (`quarantine.add`). They map `… reset because its code was updated.` and `… is overloaded. Requests queued for too long.` to transient for every other call. | Without the mark, every client retry of a transient error resets the tenant again (measured: 2 retries → 2 resets; with the mark, 1). |
| b | Set `limits.cpu_ms` for the tenant-object script to the lowest value that the heaviest legitimate event allows, ideally under about 4 s. | That bounds the outage below the 7 s shed point. **Conflict:** S1's migration 0003 used 45 s of CPU in one transaction. K02 must chunk migrations under the limit, or migrations must run in a different script. |
| c | Accept that a pure `for(;;){}` is not stopped per call in stage 1. Keep sockets in gateways (G0-1), which makes the blast radius "in-flight calls of one tenant for up to the CPU limit". | No per-call CPU guard works with one transaction per Reducer. |
| d | S5 also answers this: can module code in a Worker Loader isolate (with `limits.cpuMs`) make host calls into a tenant that holds its transaction? | The probe measured that such a runaway is stopped with no reset at about +1–2 ms p50, but 0 host calls completed while the tenant held its input gate. |
| e | The conformance step `reducer-errors` / "transient: invocation deadline" (`errors.spin`, a pure loop) cannot match Rust on the kernel until (b): the error arrives after the CPU limit instead of 10 s, and the harness waits 15 s. | Platform-forced, like 0012 R21 and R26. |
