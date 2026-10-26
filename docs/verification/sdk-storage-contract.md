# SDK replica storage contract

Keep application and replica semantics behind `LocalReplicaStorage`. The public
TypeScript interface stays in `packages/client/src/local-replica.ts`. A new
backend should implement that interface and run the shared factory-based suite
in `packages/client/test/replica-storage-contract.ts` before replacing an adapter.
The suite is test support, excluded from package builds and public exports.

## Executable checks

The same cases run against memory storage, legacy IndexedDB, normalized-engine
IndexedDB and SQLite. They check:

- Identical row and window keys remain isolated by scope, including scoped clear.
- Successful transaction application exposes its row, membership and cursor
  post-image together. Replaying it preserves the same logical state.
- A deleted row stays absent when an older insert is replayed, while the cursor
  and empty membership retain the newer revision.
- Mutating input or loaded values cannot change stored state through aliases.
- A new persistent adapter instance can load the complete committed state.
- An injected failure after an earlier write batch succeeds rolls back the
  transaction's rows, windows and cursor.

The snapshot argument supplied to `applyTransaction` is the resolved post-image
that `LocalReplica` provides. Memory storage persists that post-image; database
adapters apply the transaction themselves. The contract does not require every
adapter to interpret an obsolete post-image as an independent command.
Internal commit sequences and optional adapter receipts are excluded from
logical equality. Their stronger receipt/peer-catch-up laws need separate checks.

There are four common cases for memory and six for each database adapter,
22 cases total. The failure fixture spans SQLite's 100-row write batch and both
IndexedDB adapters' smaller batches. It throws on the second row-write operation,
after the first succeeds inside the still-open transaction. The suite verifies
that rejecting the commit restores the prior complete state.

## Scope and integration

IndexedDB tests use Dexie over `fake-indexeddb`. SQLite uses the production
adapter over Node's in-memory SQLite database, with a transaction-capable driver
implementing the Expo database interface. Reopening means a new adapter instance
on the same backing database. These checks do not establish OS crash durability,
real-browser quota/eviction behavior or an Expo device driver's transaction
implementation. Memory has no reopen or durable rollback profile.

The existing client integration test in `index.test.ts`, "settles a tenant
Reducer only after its collection membership delta is durable", holds persistence
behind a controlled promise. It checks that completion stays blocked until that
promise is released. Snapshot activation and rollback have additional existing
coverage in `snapshot-pages.test.ts`. These tests remain necessary alongside the
adapter contract. Native snapshot ACK credit accounting is a separate Rust
protocol; this suite does not claim to prove that protocol or add a new TS ACK.

Verus verifies supported Rust implementations. This TypeScript suite is sampled
executable conformance testing, with no claim of a universal TypeScript proof.
IndexedDB remains the web storage implementation. The contract also gives any
future adapter a concrete compatibility target without changing public APIs.

## Reproduce

After `pnpm install --frozen-lockfile` and `pnpm build:packages`:

```sh
pnpm --filter @gonvex/client exec vitest run src/replica-storage-contract.test.ts
pnpm --filter @gonvex/expo-sqlite exec vitest run src/replica-storage-contract.test.ts
```

Existing `pnpm -r test` CI discovers both files. To add another adapter, supply a
factory with `storage` and `close`. Add `reopen` and a partial-write failure hook
only when those behaviors exist in that adapter's supported profile. Do not
replace the shared cases with adapter-specific expected results.
