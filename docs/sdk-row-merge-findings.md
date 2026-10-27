# SDK replica row findings

Base: staging `79668483b46fd8901aa0beacaf9f6e837d2f92e6`.
Both findings were reproduced against the unchanged production code before
applying the fixes. Three direct row tests and six adapter contract cases failed
as intended, covering both IndexedDB adapters and Expo SQLite. These are native
regressions, not formal proofs of the row merger or storage engines.

## SDK-ROW-LIFETIME-FENCE

A row at revision 1 contains an old projected field. Revision 2 deletes the row.
A narrow revision-3 projection recreates it without that field. The merger
discarded the deletion timestamp on recreation, so a delayed revision-1 or
revision-2 projection could fill the missing field with data from the old row
lifetime. Reopening the adapter did not prevent this because the deletion
history was already absent from the persisted authority.

The merger now retains a valid deletion timestamp on a live row as a lower bound
for admissible projections. It still accepts new fields at the recreation
revision and resets authority on an epoch change. The separate stored row's
`deleted` flag continues to indicate whether it is a tombstone. Invalid legacy
deletion metadata is cleared during valid recovery as before; retaining NaN
would poison the newest-revision calculation.

Regressions: `replica-record-lifetime.test.ts` checks delayed projections,
deletion ties, new-lifetime fields, epochs and legacy NaN recovery. The shared
`replica-storage-contract.ts` runs the persisted recreation/reopen/replay case
against both IndexedDB adapters and SQLite.

## SDK-ROW-JSON-PROPERTY

An incoming JSON row can have an own property named `__proto__`. Assigning that
key to a plain object invokes its inherited setter. The row lost the property
when serialized, its prototype could change, and its authority dictionary did
not retain the column's revision. A later stale update could therefore bypass
the intended field ordering. This changes the row object; no mutation of the
global Object prototype or deployed occurrence is claimed.

The merger defines that special key as an own, enumerable, writable data
property for both row values and authority revisions. Ordinary field writes
keep their assignment path. Regressions check JSON round-trip, unchanged row
prototype, copy-on-write preservation, stale field/deletion rejection and
adapter reopen through both IndexedDB implementations and SQLite.

## Boundaries and related inspection

The fixes change shared per-record merging used by durable adapters. They do not
prove complete client publication or repair fields that were already discarded
from older stored data. Storage correctness is checked with fake-indexeddb and
Node SQLite, without a native-device or crash-durability claim.

Independent inspection also identified candidate snapshot lifecycle and Expo
hydration/epoch issues. Those need their own reproduction and fixes. In
particular, adapter-only row tests do not establish that every SDK path publishes
the adapter's resolved result into its in-memory replica.
