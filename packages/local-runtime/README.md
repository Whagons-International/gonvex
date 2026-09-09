# Local Reducer execution

The generated client executes the same TypeScript Reducer body used by the Rust
server. Structured reads and writes use the normalized Local Replica in browsers
and native JavaScript, and compile to parameterized PostgreSQL operations on the
server. Production execution has no PGlite, WASM startup, scratch database, or
WebView. The CLI uses PGlite only during schema compilation, and the test suite uses it
as a PostgreSQL reference. Neither use is part of application execution.

Use the module SDK structured data operations inside an interactive Reducer.
Arbitrary SQL belongs in server Queries or explicitly server-only work. One
Reducer captures one atomic transaction, including relationship rows, logs,
notifications, and deferred Action requests. The server checks permissions and
constraints and executes deferred work only after its authoritative commit.

The client first predicts against resident rows. It publishes a successful
prediction immediately, then durably stores the intent before network delivery.
A coordinated admission checks the shared journal and base version and re-executes
when necessary. Storage failure removes the prediction. A successful call resolves
after durable admission, without waiting for the server. Never treat a visible
prediction as confirmation of durable admission or server acceptance.

A missing row or projected field retries the body through a transaction-consistent
storage read view. IndexedDB and Expo SQLite use primary/secondary indexes and
bounded pages. Missing coverage is never interpreted as an empty collection.
An intent needing unavailable data waits for the server without a guessed result.
The SDK supplies stable intent-owned IDs and captures Action/scheduler requests.

The confirmed replica and ordered pending intents produce one frontend entity
view. Rejection removes the rejected prediction and rebases later intents. Server
commits and local predictions notify the same entity subscriptions. The durable
journal is scoped by project, tenant, and account; replay also checks the artifact.

## Storage and multiple tabs

IndexedDB owns persistent rows; the Local Replica keeps a bounded resident working
set. Active rows and windows are retained while observed. Cold rows stay on disk
and can be read by Reducers without loading the entire collection into RAM.

Browser tabs share the journal and confirmed storage. Web Locks serialize intent
admission and outbox delivery. BroadcastChannel signals changes without copying
whole databases. Field revisions, tombstones, and epochs reject stale projections.
An online tab can deliver an intent created in another offline tab. Server receipts
remain the final protection against duplicate effects after a lost response.

Expo SQLite implements the same read-view contract with indexed scalar lookups,
bounded pages, projected writes, and transactional row authority. The generated
native runtime executes JavaScript directly. Native database files remain scoped
and shared only through the SDK adapter.

## Verification

The tests cover PostgreSQL/portable conformance, deterministic IDs, atomic rollback,
read-own-writes, incomplete data, ordered replay, persistence failures, rejection,
concurrent tabs, scope changes, residency budgets, and real SQLite transactions.
CLI tests bundle and execute generated Reducers in an offline browser and verify
that execution does not start a worker or load a SQL engine.
