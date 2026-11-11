# Kernel Reducer path (K07)

`reducer.call`, `reducer.callMany` and the receipt query `__gonvex.reducerReceipt` in the tenant object, end to end:
idempotency keys and receipts (with expiry, 0009), committed revision and watermark, the deferred-FK check in mixed
mode, declared reference integrity, permission bits, the outbox, error classes. The reference is
`rust/crates/runtime/src/execution.rs` (`execute_tenant_reducer_with_access_counted`, `replay_receipt`,
`reducer_receipt_status`), `lib.rs` (`call_reducer`, the watermark rule), `error_class.rs`, `permission_bits.rs`,
`rust/crates/store/src/lib.rs` (receipts, commit order) and `reference_integrity.rs`, at `0bf3ad13`.

```text
gateway (K05) ──RPC dispatch──▶ ReducerTenantObject (object.ts)
                                 └ KernelTenant (tenant.ts): K01 storage · K02 migrator · K03 capture · K04 host
                                    └ ReducerPath (path.ts) ─ ReducerStorage (storage.ts) ─ receipts.ts · permissions.ts · reference/
```

| File | What | Rust |
| --- | --- | --- |
| `path.ts` | `ReducerPath`: the checks in Rust's order, the receipt claim and replay, the call through K04, committed revision, the terminal watermark and the own-membership hand-off; the receipt query | `execution.rs` 143-245, 405-820; `lib.rs` 3271-3466, 4195-4345, 5007-5080 |
| `storage.ts` | `ReducerStorage`: K01's `TenantStorage` with the steps Rust runs around the module in its transaction (store the receipt, reference flush, K03 finalize, then K01's FK check and commit) | `store/src/lib.rs` 1600-1834 |
| `receipts.ts` | `_gonvex_reducer_idempotency` byte for byte (legacy rename, ambiguity), read, store, and 0009 expiry in pages | `store/src/lib.rs` 1106-1199 |
| `permissions.ts` | `PermissionBits`: K04's `PermissionIndex`, from the manifest's `permissionGrants`, committed state only | `permission_bits.rs` |
| `reference/` | Declared reference integrity, ported whole (ledger, `flush`, `declare`, coverage, JSON path) | `reference_integrity.rs`, `jsonpath.rs` |
| `errors.ts`, `frames.ts` | Rust's texts and classes; `reducer.result` / `query.result` in serde key order | `execution.rs` 32-76, `error_class.rs`, `protocol` |
| `module.ts` | The active module from the `/dev/sync` manifest; the K04 artifact over the Worker version's bundle | `modules.rs` |
| `tenant.ts`, `object.ts` | The tenant object composed from K01–K04 and K07; K05's `TenantRpc` | |
| `harness/` | A Worker around it for workerd tests and the conformance backend `cloudflare-reducer` | |

## Behavior

- **Order of checks** (as Rust): module → function (missing, internal, kind) → public access → idempotency key (not for
  anonymous sessions, trimmed) and the receipt table → client contract → artifact hash (`accepts_client_artifact`) →
  argument schema → provenance (`ui`, or `api` for a delegated session; deadline now + 10 s) → entropy → the migration
  gate (0028 §3) → receipt → module → stored result → reference flush → finalize → FK check → commit.
- **Receipts.** A committed key replays its stored result with the clock's current revision as `committedRevision`; a
  key used for another path is `IdempotencyPathMismatch` (rejected); a failed call keeps no receipt, so its replay runs
  and fails again. The receipt row (subject = account id) commits in the Reducer's own transaction, with its result, or
  not at all. The Reducer path is serialized per object, so a replay that arrives while the first call is still
  committing waits for it (`receipt-lost-reply`).
- **Expiry (0009, IB3).** With a numeric `offlineMaxAgeMs`, results older than the window + 24 h are cleared on the
  tenant alarm, in pages of 500 queued behind Reducers; the row stays as a tombstone (`result IS NULL`, which a committed
  Rust receipt never has, so the table keeps Rust's exact schema). A key that hits a tombstone gets
  `reducer.error{class:"rejected", error:"Reducer receipt expired"}`, and so does the receipt query. Tombstones go 365
  days after their result expired. A null window, or no client contract, keeps receipts as Rust does.
- **Committed revision and watermark.** `committedRevision` is the revision K03's finalize assigned (absent when the
  call changed no captured row). The call completes with `replica.watermark` at once when the socket has no
  subscription, or every subscription was already sent through that revision (`call_watermark_after_sent_replica_work`);
  the socket's floor comes from `subscriptionFloor`, which K10 provides. A found receipt query completes the same way.
- **Own membership.** When that watermark would go out and the commit changed the caller's own Member row
  (`member_access_changed`), the object instead delivers `membershipChanged` to the tenant's gateways after the reply:
  the session sends `auth.error membership-changed`, then the watermark, and drops its tenant session (Rust's order).
- **Deferred foreign keys, mixed mode (0028 §2d).** K01 tracks statements, but tables whose written rows capture
  reports (captured entity tables with rowids) stay row-level; derived, metadata, record-storage and `_gonvex_*` tables
  are checked per table. A violation fails the call with Rust's commit text and the object keeps serving.
- **Permission bits.** Loaded from the projection on first use and brought to the committed clock before each Reducer's
  transaction opens (the grant table's change rows since the index's revision; a reload when they were pruned or the
  epoch changed). A Reducer never sees its own uncommitted grants, as in Rust.
- **Outbox.** `actions.enqueue` and Reducer `scheduler.*` rows (K04) commit with the Reducer, with Rust's deterministic
  intent ids; a committed call that wrote them is reported to the object (`onCommit`, Rust's `outbox_written`), where
  K11's drain hooks its wake.
- **Alarm.** When the module declares an offline window, the object arms its one alarm for the hourly receipt expiry,
  only ever moving it earlier; the handler runs K02's migration work and the expiry (20 pages per event).
- **Errors.** Module failures keep K04's `TypeScript module host returned <code>: …` text and class; refusals before the
  module use Rust's `ExecutionError` texts; commit refusals read `module database capability failed: …`.

## Differences from Rust

1. **Replay before the module runs.** Rust claims the receipt at the module's first database call, so a module that
   fails before touching the database (only possible when the same key comes back with different arguments) reports
   its own error; the kernel replays the stored result. Every other case is identical.
2. **Tombstones** (0009): Rust never expires receipts.
3. **Migration gate**: Rust's writers wait on the schema lock; the kernel refuses with a transient error while a
   migration unit is partly applied (0028 §3), `module database capability failed: tenant schema migration in progress; retry`.

## Tests and conformance

```sh
pnpm vitest run --root kernel/reducer          # in cloudflare/: workerd, through the K05 socket path
cd conformance && node src/cli.mjs --backend rust=rust --backend cf=cloudflare-reducer \
  --scenario receipt-lost-reply --scenario receipt-expiry
```

- `test/receipts.spec.ts`: lost reply, duplicate send, the receipt query, rejection, receipts per account, expiry and
  the unlimited window, the upgrade (generation 2, stale artifact, generation-1 receipts).
- `test/path.spec.ts`: error classes, `callMany`, row ids equal to the client's `reducerRowId`, outbox ids, the deferred
  FK, permission bits, own membership, rows read and written.
- `reference/test/`: the Rust unit and integration tests of reference integrity, on Durable Object SQLite.
- `kernel/capture/test/capture-parity.test.ts` runs on K01's real `StorageSession` in mixed mode (0028 §2e).
- The conformance backend `cloudflare-reducer` (`conformance/src/backends/cloudflare-reducer.mjs`) builds the
  fixture generations with the CLI, puts their bundles into the harness Worker (`harness/generate-bundles.mjs`) and runs
  `wrangler dev`. It restarts workerd for the crash scenario.
