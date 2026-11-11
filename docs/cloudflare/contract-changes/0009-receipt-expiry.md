# 0009: Reducer receipt expiry (IB3)

Date: 2026-11-10. Status: decided (lead). Affects: K07, P02 receipt scenarios.

## Facts at `0bf3ad13`

- Receipts live in `_gonvex_reducer_idempotency(subject, idempotency_key, path, result, created_at)`
  (`rust/crates/store/src/lib.rs:1140-1168`). Nothing deletes from it.
- The offline window is the app's `client-contract.json` `offlineMaxAgeMs`: a positive integer or `null`
  (unlimited). There is no SDK default (`packages/client/src/client-upgrades.ts:6`,
  `rust/crates/runtime/src/modules.rs:1252-1276`).
- The window only stops the client from **creating** new offline intents
  (`packages/client/src/index.ts:1652`, `1674`). Intents already in the outbox never expire and are sent
  whenever the client reconnects (`packages/client/src/outbox.ts:232`, `614`). The intent's creation time does
  not cross the wire.

So deleting a receipt outright lets a late duplicate of an already committed intent run a second time.

## Rule

1. When `offlineMaxAgeMs` is a number, a receipt's **result** is deleted once
   `created_at < now - (offlineMaxAgeMs + 24 h)`. The row stays as a tombstone (`subject`, `idempotency_key`,
   `path`, `created_at`, `result = NULL`, `expired = 1`).
2. Tombstones are deleted after 365 days.
3. A `reducer.call` whose key hits a tombstone is not executed. It gets
   `reducer.error{class:"rejected", retryable:false, error:"Reducer receipt expired"}`. The receipt lookup
   query (`__gonvex.reducerReceipt`) returns the same outcome.
4. When `offlineMaxAgeMs` is `null` or the app has no client contract, receipts are kept as in Rust.
5. Pruning runs on the tenant alarm, in pages, never inside a Reducer transaction.

Conformance: P02's receipt scenarios gain an "expired receipt" case asserting rule 3, and they keep the Rust
behavior for every case inside the window.
