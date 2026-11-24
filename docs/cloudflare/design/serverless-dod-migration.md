# Serverless Data-Oriented Design (DOD) Architecture & Staging-to-Serverless Migration Plan

Status: **Active Blueprint** (Phase 1 hot-path optimizations shipped in PR #574; Phase 2/3 foundations in `cloudflare/kernel/{codec,edge-auth,tx-log}` and `cloudflare/scripts/check-dod.mjs`).

---

## 1. Executive Summary & Architectural Goal

Whagons' long-term target is a **100% serverless Cloudflare Workers + Durable Objects + R2 + D1** backend that matches or beats `whagons-lite`'s Data-Oriented Design (DOD) speed on large tenants (`hotel-100k`: ~100k tasks, sub-500 ms cold boot, sub-300 ms warm boot, sub-50 ms edits, flat SQL cost per commit regardless of subscriber count), while allowing **zero-downtime, tenant-by-tenant migration** of all staging (and later production) tenants with instant single-key rollback.

### Non-Negotiable Empirical Constraints
1. **Keep Gateway DOs as WebSocket owners** (`docs/cloudflare/design/option-a-tenant-socket.md` proved moving client WebSockets directly onto Tenant DOs regresses cohort p95 by +1.8 s to +5.8 s and blows single-threaded Tenant DO CPU 4.5x due to per-socket framing/deflate).
2. **Respect Cloudflare platform limits** (`cloudflare/AGENTS.md`): 100 KB/SQL, 100 bound params, 100 columns, 2 MB/row, 128 MB isolate memory, 30 s CPU/event, 10 GB/DO.
3. **Zero big-bang flag days**: Legacy JSON protocol clients and new `WLP1` binary clients must coexist on the same deployment via capability negotiation.

---

## 2. Target Serverless DOD Architecture (3-Tier Topology)

```
Client (Browser / Mobile / Offline Store)
  │
  ├── HTTPS GET /sync/checkpoint (Cold boot) ──► Edge Worker ──► Ed25519 Verify (0 DO hops)
  │                                                                └──► R2 Immutable WLP1 Base Snapshot + Tail
  │
  ├── WSS /ws (Live sync + Mutations) ─────────► Edge Router ──► Ed25519 Verify (0 DO hops)
  │                                                                └──► Gateway DO (WebSocket owner, ~125 sockets/holder)
  │                                                                       ▲
  │                                                                       │ 1 RPC / commit / gateway (shared WLP1 bytes)
  │                                                                       ▼
  └── Mutations / Tail Catch-up ───────────────► Tenant DO (Single-Writer SQLite + Triggerless Tx Log + Hoisted O(tables) Fan-Out)
                                                                          │
                                                                          └──► Async R2 Base Checkpoint Compactor (alarm)
```

### Key Structural Shifts

| Dimension | Current Cloudflare Kernel (`cloudflare/integration`) | Target Serverless DOD (`kernel/{codec,edge-auth,tx-log}`) | Expected Gain |
|---|---|---|---|
| **Edge Auth & Routing** | Control DO RPC + KV read/write per connect (`SessionControlObject`) | Stateless Ed25519 binary token (`wlt1.`, `kernel/edge-auth`) verified in Edge Worker + D1/KV read-through for tenant routing + `RevocationSet` fence | **0 DO hops** on auth/connect; eliminates Control DO singleton bottleneck |
| **Change Capture** | Per-table `INSERT`/`UPDATE`/`DELETE` SQLite triggers serializing full `json_object(...)` old & new rows into `_gonvex_sync_changes` | Triggerless single-writer `Tx` (`kernel/tx-log`) writing compact 5-int `(seq, tbl, rid, op, scope_hint)` rows in `<=100`-bind batches (`WITHOUT ROWID`) | **-50% to -70% `rowsWritten`**, zero trigger bytecode & JSON serialization on writes |
| **Commit Fan-Out** | Per-connection `applyTransaction()` + JSON frame serialization | Hoisted O(tables) batch read (`buildBatchFanout` in `kernel/tx-log/fanout.ts`): reads SQLite **once per touched table**, encodes **once per visibility class** directly into `WLP1` `PackedPage` bytes | **Flat SQL (`O(tables)`)** from 1 to 1,000+ members; zero per-member SQL |
| **Wire Format** | Row-oriented JSON objects + per-row `stableJson` + SHA-256 | `WLP1` binary columnar `PackedPage` (`kernel/codec`, 20 adaptive encodings, direct `SqlStorageCursor.raw()` streaming) | **4x–6x smaller raw wire bytes** (~40–76 B/row vs ~250–400 B/row JSON), zero per-row JS object allocation |
| **Cold Boot (100k rows)** | Tenant DO scans SQLite on every cold client connect (competing with writers on the single-threaded DO) | Immutable per-epoch `WLP1` base checkpoint pages cached in R2 + Cloudflare CDN; Tenant DO only serves tiny `seq > checkpointSeq` binary tail | **Offloads 95%+ of cold-boot reads & CPU** off Tenant DO |
| **DOD Regression Guard** | Manual code review only | Mechanical static gate (`scripts/check-dod.mjs` + `scripts/dod.baseline.json` ratchet) enforcing `sql-in-loop`, `concat-key`, and hot-zone allocation/iterator budgets | Prevents accidental N+1 SQL or per-row allocations from sneaking back in |

---

## 3. Phased Implementation Roadmap

### Phase 1 — Immediate Hot-Path Fixes (Shipped: PR #574)
Contract-preserving kernel fixes with zero schema or client changes:
- Eliminated per-commit spurious membership alarm storm and coalesced duplicate `scheduler.notify()` calls (`assembly/tenant.ts`).
- Reused `SnapshotCache` on warm/resumable opens (`aggregate`, `wholeSet`, `reconcile`: `0` table rows read on hit) and dropped redundant `readClock()` (`delivery/subscription.ts`).
- Scoped `shared` commit frame text pools per gateway host (`delivery/engine.ts`).
- Fixed O(N²) array spread in capture row-ID collection (`capture/finalize.ts`).
- Made compiled visibility SQL aliases deterministic per predicate for SQLite plan caching (`visibility/sql.ts`).
- Parallelized direct `/ws` upgrade limiter + controller admission (`session/placement/gateway.ts`).
- Packed small recovery entries into batched KV chunks (`<= 2` chunks for 128 replicas) and skipped redundant `kv.get()` on fresh generation keys (`session/recovery-store.ts`).
- Gated hot-path presence JSON logging behind `spanTimingOn()` (`presence/tenant.ts`, `assembly/tenant.ts`).
- Cached kernel SQL tokenization and replaced `Object.fromEntries` in `storage/session.ts`.

### Phase 2 — Serverless Foundations & Dual-Mode Protocol (This PR + Next Follow-ups)
1. **Shipped in this PR (`cf/dod-serverless-foundations`)**:
   - `cloudflare/kernel/codec/`: Full `WLP1` binary columnar `PackedPage` encoder/decoder + direct DO `SqlStorageCursor.raw()` streaming (`>4x` smaller wire size vs JSON on 1,000 tasks).
   - `cloudflare/kernel/edge-auth/`: Stateless Web Crypto Ed25519 binary session token (`wlt1.`) mint/verify + `RevocationSet` (0 DO hops, 0 JSON parsing).
   - `cloudflare/kernel/tx-log/`: Triggerless `Tx` writer + payload-free `_gonvex_tx_changes` + hoisted O(tables) `buildBatchFanout` directly into shared `WLP1` pages.
   - `cloudflare/scripts/check-dod.mjs`: Zero-dependency static DOD ratchet gate (`pnpm --dir cloudflare check:dod`).
2. **Next Follow-up PRs (Incremental Wiring)**:
   - **PR 2b (Capability Negotiation & Binary Wire Bridge)**: Advertise `replicaPackedPages: 1` and `edgeSessionToken: 1` in `kernel/session/capabilities.ts`. Clients that do not negotiate them stay on the existing JSON path seamlessly; upgraded clients receive `WLP1` binary WS frames (`F_PAGE` / `F_DELETE` / `F_COMMIT`).
   - **PR 2c (Edge Auth Rollout in Ingress/Router)**: Mint `wlt1.` tokens on login/handoff in `SessionControlObject`, publish public keys (`kid -> raw32`) to Router & Gateway Workers via env/KV, verify statelessly at edge, and push revocations via Gateway broadcast.
   - **PR 2d (Dual-Write → Triggerless Cutover)**: Add per-tenant feature flag `tx_log_mode = 'triggers' | 'dual' | 'triggerless'`. Verify parity between `_gonvex_sync_changes` and `_gonvex_tx_changes` in staging, then drop SQLite triggers per tenant.

### Phase 3 — R2 Checkpoints & Serverless Cold-Boot Offload
- Tenant DO periodic alarm compacts replicated tables into immutable `WLP1` base checkpoint objects in R2 (`checkpoints/<tenantId>/<epoch>/<seq>/<tbl>.wlp1`).
- Edge Worker serves `GET /sync/checkpoint` directly from R2 (with immutable `Cache-Control` + Ed25519 visibility-class check) without waking the Tenant DO.
- Client applies R2 base pages + opens WebSocket with `cursor = checkpointSeq` to fetch only the small delta tail from Tenant DO.

---

## 4. Easy Staging-to-Serverless Tenant Migration Plan

Moving all staging tenants to the Cloudflare serverless backend requires **zero manual DB surgery** and **under 30 seconds of write-pause per tenant** (with instant rollback if any check fails).

### 4.1 Routing Control Plane & Instant Rollback Pointer

Every tenant has a single authoritative routing record in the edge routing table (Control Plane / D1 + edge cache with 5 s TTL):

```json
{
  "tenantId": "stg-acme-01",
  "backend": "legacy",            // "legacy" | "cf-serverless"
  "cfNamespace": "gonvex-cf-stg-runtime",
  "cfTenantName": "stg-acme-01",
  "wireMode": "dual",             // "json" | "dual" | "wlp1"
  "txLogMode": "triggers",        // "triggers" | "dual" | "triggerless"
  "previousBackend": "legacy",    // instant rollback target
  "cutoverAt": null
}
```

**Rollback guarantee:** Changing `backend` back from `"cf-serverless"` to `"legacy"` (or flipping `wireMode` from `"wlp1"` to `"json"`) immediately routes new connections back to the previous target and closes active sockets with WebSocket close code `1012` (Service Restart), triggering automatic client reconnect.

### 4.2 Automated Per-Tenant Migration Pipeline (Reusing P03 Transfer)

We reuse the already-tested, CPU-budgeted P03 transfer transport (`cloudflare/kernel/assembly/tenant-transfer.ts`, verified at 115k rows inside DO CPU limits in `tenant-transfer-budget.spec.ts`):

1. **Stage 0 — Pre-flight (Online, 0 downtime)**:
   - Provision fresh Tenant DO (`cfTenantName`) in the staging Cloudflare Worker namespace.
   - Ensure target release bundle is baked and published in `env.RELEASES` (R2).
   - Call `POST /__tenant-transfer/t/<tenantId>/wipe` (verifies fresh empty DO).
2. **Stage 1 — Bulk Seed (Online, 0 downtime)**:
   - Export staging tenant SQLite snapshot (read-only copy/checkpoint).
   - Stream schema DDL via `POST /__tenant-transfer/t/<tenantId>/schema`.
   - Stream rows in 2–4 MiB batches via `POST /__tenant-transfer/t/<tenantId>/insert` (each request `< 80 ms` CPU).
3. **Stage 2 — Quiesce & Final Delta Catch-up (Brief write freeze: ~2–10 s)**:
   - Set legacy tenant write-gate to `draining` (in-flight mutations finish; new writes return retryable `transient` for a few seconds).
   - Flush final WAL delta rows since Stage 1 snapshot into the Cloudflare Tenant DO.
   - Run resumable integrity checks:
     - `POST /__tenant-transfer/t/<tenantId>/check` (`PRAGMA quick_check`, FK violations = 0, FTS integrity = ok).
     - `POST /__tenant-transfer/t/<tenantId>/verify` (exact per-table row count + SHA-256 checksum match against source).
   - Finalize transfer: `POST /__tenant-transfer/t/<tenantId>/complete` and activate baked release.
4. **Stage 3 — Shadow Smoke & Atomic Routing Flip**:
   - Run automated read-only smoke probe against the Cloudflare Tenant DO (verify `admit`, `replica.open`, `WLP1` page decode, row counts).
   - Flip routing record `backend = "cf-serverless"` and issue `1012` reconnect to legacy sockets.
   - Legacy DB stays read-only & warm for 24–72 hours as the cold backup / rollback safety net.
5. **Stage 4 — Progressive Feature Enablement (Per Tenant, Zero Re-Copy)**:
   - Once stable on `cf-serverless` (`wireMode: "dual"`, `txLogMode: "triggers"`):
     1. Enable `edgeSessionToken: 1` (stateless Ed25519 edge auth).
     2. Enable `replicaPackedPages: 1` (`WLP1` binary wire for upgraded clients).
     3. Enable `txLogMode: "dual"` → verify parity → switch to `txLogMode: "triggerless"` (drops SQLite triggers).
     4. Enable R2 base checkpoint compactor alarm.

### 4.3 4-Ring Staging Rollout Schedule

| Ring | Scope | Entry Criteria | Promotion Gate (24 h soak) |
|---|---|---|---|
| **Ring 0** | 2–3 synthetic / seeded QA tenants (including `hotel-100k` load fixture) | `pnpm typecheck`, `pnpm test`, `pnpm check:dod` green | Cold boot p95 < 600 ms, warm boot p95 < 300 ms, edit p95 < 80 ms, 0 checksum mismatches, 0 DO OOM/CPU resets |
| **Ring 1** | Internal Whagons dogfood staging tenants (~5–10 tenants) | Ring 0 green + rollback drill verified (`cf-serverless` → `legacy` → `cf-serverless`) | Error rate <= baseline, reconnect rate <= baseline, `rowsRead` & `rowsWritten` within DOD budgets |
| **Ring 2** | 25% → 50% → 100% of remaining staging tenants (automated batch script, 5 tenants in parallel) | Ring 1 green for 48 h | Zero stuck transfers, zero data-parity alerts, billing/DO duration within forecast |
| **Ring 3** | Production canary & wave cutover (post-staging sign-off) | 100% staging running cleanly on serverless for 1 week | Same SLO gates + customer-facing maintenance window notice (if needed for >500k-row tenants) |

### 4.4 Migration CLI Orchestration Workflow

A single idempotent orchestrator command drives staging waves safely:

```sh
# Dry-run inventory & size check for staging tenants
node scripts/migrate-staging.mjs plan --ring 1

# Migrate a batch (max 5 concurrent), auto-pausing on any verify/check mismatch
node scripts/migrate-staging.mjs run --ring 1 --concurrency 5

# Instant rollback of one tenant or an entire ring
node scripts/migrate-staging.mjs rollback --tenant stg-acme-01
```

Each tenant transition records an auditable state machine (`pending -> seeding -> verifying -> cutover -> soaking -> complete | rolled_back`), making retries and wave resumes completely safe and idempotent.

---

## 5. Performance & Resource Budgets (CI Enforced)

1. **Static DOD Gate (`pnpm --dir cloudflare check:dod`)**:
   - Hot zones (`kernel/codec/`, `kernel/edge-auth/`, `kernel/tx-log/`): **0 violations** allowed.
   - Legacy zones (`kernel/delivery/`, `kernel/capture/`, repo-wide): **ratchet-only** baseline (`scripts/dod.baseline.json`); counts may only decrease.
2. **Per-Operation SQL Budgets (asserted via `SqlStorageCursor.rowsRead` / `rowsWritten` in tests)**:
   - Edge session auth verify: **0 SQL rows, 0 DO RPCs**.
   - Warm/resumable `SnapshotCache` replica open: **0 table scan rows**.
   - Hoisted commit fan-out (`buildBatchFanout`): **`O(touched tables)` SQL statements**, invariant from 1 to 1,000+ members.
   - `WLP1` 1,000-task snapshot wire size: **`< 25%` of JSON wire size** (`>4x` compression before gzip).
3. **Worker Bundle Budget (`pnpm --dir cloudflare bundle`)**:
   - Track raw & gzip bytes on every PR; keep runtime Worker upload well under Cloudflare's 10 MiB compressed limit (currently ~216 KiB gzip).
