# 0050: Final rulings for K10, K15, K16 and K20

Date: 2026-11-12. Status: decided (lead), except §2a, which goes to Gabriel.

## 1. K10, replica delivery (note 0046): approved

- `connectionReset` is now optional on `TenantRpc` and a no-op on K07's object and K05's stub.
- `affected_rows` reads through one `TransactionReads` per transaction, so commit reads stay flat as
  subscribers grow: 1 / 10 / 100 subscribers = 4 statements and 108 rows each, against 202 statements and
  5,355 rows before at 100.
- 0048 (a) is partly done: own-membership hand-off rides the ordered `ReplicaDelivery` channel, and K08's
  frames join it in K22.
- 0048 (c) goes to K22: live-query resubscribe and the `operatorDataChanged` binding (the hook is in place).

## 2. K20, sandbox (note 0047): approved

`ctx.sandbox` runs Rust's `gonvex-sandbox-worker` unchanged, built with DuckDB, in a per-tenant Container with
no internet. The Dynamic Worker design is kept as the L09 candidate in `cloudflare/spikes/l09-sandbox/`.

- **a. Staging runs no DuckDB** (for Gabriel). The staging image is built without DuckDB (no `GONVEX_DUCKDB`), so
  every Whagons sandbox `create` there answers "DuckDB analysis is unavailable in this build". The kernel's
  reference is the runtime **built with DuckDB**, as specified. The staging build flag is a staging defect,
  outside this branch.
- **b. Intended behavior IB6: a writable sandbox workspace.** Rust creates the chroot workspace as root and runs
  the worker as uid 65534, so DuckDB cannot open `./analysis.duckdb`. That is 15 of the 19 failed staging
  `analyzeInSandbox` calls. The kernel's manager chowns the workspace to the worker's uid. Rust-vs-kernel
  comparison excludes the failure path.
- **L09 data for Gabriel:** across the 15 staging tenants, 24 model-written statements (10 distinct) ran. None
  uses DuckDB-only syntax that SQLite refuses. The output-format differences are listed in the spike report.

## 3. K15 and K16

- **K15 (note 0041):** approved. The lead combined its session hooks with K16's.
- **K16 (note 0042):** merged.
