# 0028: Rulings from the third wave-1 review pass (S5, K03, K02)

Date: 2026-11-11. Status: decided (lead).
Inputs: #347 (S5), #348 (K03) and its note 0026, #349 (K02) and its note 0027.

## 1. S5: Worker Loader

- **0016 G0-2 stands for stage 1:** the module bundle is part of the deployed Worker version.
- Plain Worker Loader calls into a tenant that holds its storage transaction **deadlock**: the second call
  never arrives and the object resets after about 30 s. This answers 0022 §3 (R25d): no.
- A **Facet per generation** keeps every socket open through a generation switch, with outcomes and rows
  read identical to the baked build. It is recorded as a **stage-3 candidate** for in-place `system.reload`.
  It is not adopted now, for three reasons:
  - it adds 0.4–1.1 s of cold generation load per object wake, which works against R02/R03;
  - Dynamic Workers are in open beta;
  - `facets.abort` failed intermittently (2 resets and 1 stuck facet in about 100 switches).
- Follow-ups, blocked only by the credentials outage on 2026-11-11:
  - run S5's remaining platform probes (`runaway`, `gateway-sockets`, `roundtrip`, `split-deploy`, `txcalls`);
  - delete `gonvex-cf-dev-s5`, `-s5-baked`, `-s5-gw` and `-s5-tn`.

## 2. K03: change capture (note 0026)

| # | Finding | Ruling |
|---|---|---|
| a | 16 of 114 change rows differ from Rust's only in how a REAL is written (`5` vs `5.0`, `1e+300` vs `1.0e+300`). The JSON values are equal. | **Compare change-row images by JSON value.** The parity test keeps its named rule and its count check. M02 and the P04 replay checksum compare `_gonvex_sync_changes` images by parsed value. No client sees a difference, because the client parses the JSON. |
| b | Rust fails every write that reaches a captured `WITHOUT ROWID` entity table (`no such column: NEW.rowid`) | **Keep** (code is the reference). `keepRowidReference` stays, with the same message. Whagons is not affected. |
| c | Rust's 7-term `UNION ALL` in `application_tables` is refused by the platform | Accepted: K03 uses an equivalent `NOT EXISTS` over `sqlite_schema` that also excludes `_cf_*`. |
| d | Capture reports rows only for captured entity tables. K01's row-level FK check would miss derived, metadata, record-storage and `_gonvex_*` tables. | **K07 implements K01's mixed mode before G1:** a row-level check for reported tables and a table-level check for the rest. Until then the table-level check (0022 §2c) stays in force. |
| e | The tests use a stand-in for K01's session because K01 was not merged | K01 is now merged. **K07 wires `invokeCaptured` to K01's real `StorageSession`** and runs `capture-parity` on it. |
| f | Rows written per change (≈3 in the trigger, ≈4 in finalize, 2 per revision) | Recorded as **R05's baseline**. |

## 3. K02: migrations and provisioning (note 0027)

| # | Finding | Ruling |
|---|---|---|
| 1 | The transform needs about 10 s of CPU and more than 128 MB of heap over the whole history. No tenant object or Worker can run it. | **Plans are built once per release, outside every object.** K19 runs the build in a **Container** (the K20 container infrastructure) at `/dev/sync`, keyed by release hash, and stores the plan next to the bundle in R2 (`R2PlanStore`). The CLI is unchanged. A01's owner may later cut the transform's memory below 128 MB so the build can move into a Workflow step. That is an optimization, not a requirement. |
| 2 | Rust's whole-database `pragma_foreign_key_check` after an FK-rebuild migration runs SQLite out of memory | Accepted. Check **one table at a time** across events, with the gate closed. When several tables violate, the reported table may differ from Rust's; the error text is Rust's. |
| 3 | A migration spans several storage transactions. Atomicity comes from a point-in-time bookmark restored on failure. | Accepted. **K04 and K07 must refuse to start an application write while `gateClosed()`** (Rust's schema lock), answering with Rust's transient error class. The bookmark restore must be verified on the platform (local workerd has no point-in-time recovery). |
| 4 | Staged `_gonvex_record_pending` rows between chunks would trip their deferred guard FK and reset the object | Accepted: the mid-migration stash table `_gonvex_st_migrate_record_stash`. It is added to the A01 `addedObjects()` exclusion list. |
| 5 | `-- gonvex:no-transaction` outside `transactionSync` | Accepted as proposed (Rust's documented semantics; `VACUUM` refused). |
| 6 | The declared-reference engine (`_gonvex_reference_requests`) | Accepted: a no-op when nothing is declared, and a loud failure if a catalog is declared, until K07 ports the engine. Whagons declares none. |
| 7 | Rust re-runs tenant identity on every provision, so a Rust tenant has its membership-outbox triggers only after the second provision | **Keep**: every release re-runs the prelude. A new tenant object equals a Rust tenant provisioned twice. Bug-compatible on purpose; a fix waits for stage 3. |
| 8 | `PRAGMA foreign_keys` is a no-op inside a transaction | Accepted as proposed. |

Platform numbers for K02 (fresh provision, interrupted provision, rollout, bookmark restore) are a
**follow-up blocked by the credentials outage**. The merge is on local evidence, as the coordinator
directed. The per-event budget (stop starting chunks after 1.5 s; slowest local chunk 1.36 s) must be
confirmed on the platform against `cpu_ms = 5000`.
