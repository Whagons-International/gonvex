# SDK storage publication findings

A database could reject an older projection while the active replica published
that projection in memory. A reload then showed a different value. The affected
operations were transaction application, complete window replacement and window
deltas. React Native uses the same replica with the Expo SQLite adapter.

The fix returns authoritative row and window post-images after the atomic storage
commit, then applies those corrections before publication. It preserves existing
field authority and pruning decisions. See [the storage contract](verification/sdk-storage-contract.md).

## Reproduced cases

- A row at revision 10 rejected a revision-9 field or delete. The active replica
  could still publish the old field or remove the row, including when the stored
  value's modification sequence already preceded the checkpoint.
- A rejected older window could restore rows without restoring its membership
  or certificate. Accepted deletes could also leave RAM and persisted membership
  different when another window retained a newer certificate or an explicit
  membership override ran last.
- A speculative removed ID outside prior window membership could disappear from
  RAM while remaining on disk. Both accepted and rejected replacements need a
  correction for that key.
- A sequence-gap read with a full residency budget could omit a deleted prior
  resident key that a peer had recreated. Required read interest now retains
  attempted and prior resident keys.
- A proposed receipt filter could hide an accepted membership removal when a
  sibling delete was rejected. Regressions cover both delete orders and compare
  published, stored, reopened and observed window IDs.

Against staging `08fd3359`, the expanded pre-fix suite recorded 11 client and
13 SQLite assertion failures. Six failures used the public client wire path.
The first proposed fix then failed two mixed-delete cases and two SQLite accepted
outside-membership cases added during review. Those failures were retained as
negative evidence before the corresponding corrections.

## Limits

These are local reproductions. They do not establish production occurrence,
phone-driver transaction isolation, browser quota or crash durability. Tests use
fake-indexeddb and real in-memory Node SQLite. SQLite gains no durable sequence
or peer synchronization. The TypeScript revision-policy proof remains separate;
these storage and publication paths are covered by tests and source review.

The work stays behind `LocalReplicaStorage`, so another storage backend can run
the same contracts. Native snapshot ACKs, metadata-only watermarks, optimistic
command settlement and independent-writer epoch races remain separate work.

Review also reproduced a supported-input epoch edge in all three adapters: a
cursor-bearing live window could switch datasets while retaining the old global
checkpoint. Consecutive live windows now start the new epoch at revision zero,
so they do not acknowledge their individual revisions. SQLite rejects an old
snapshot epoch as the reset checkpoint for a new-epoch window. The production
live-query dispatcher currently omits a cursor, so this fixture does not establish
that wire path's production reachability.

## Exploratory publication cost

Measured candidate `7d8a71d5` against control `e6ad8d33`, which includes PR222 hydration and staging `08fd3359`. Six serial jobs retained eight alternating pairs and 20 calls per sample for every adapter, size and operation. Each job used one CPU, a 512 MiB hard cap and zero swap. An initial all-in-one run hit its240-second cap and is excluded.

The table shows 1,000-row fixtures. Before/after are medians of sample mean time per call. Paired ratios summarize each adjacent A/B pair separately. Timings varied enough that these summaries sometimes disagree. These measurements do not establish performance parity.

| Adapter | Operation | Before ms | After ms | Ratio of medians | Median paired ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| legacy-idb | accepted-transaction | 28.395 | 31.657 | 1.115 | 0.860 |
| legacy-idb | stale-projection | 24.690 | 26.602 | 1.077 | 1.034 |
| legacy-idb | watermark | 0.815 | 0.793 | 0.974 | 0.961 |
| engine-idb | accepted-transaction | 26.579 | 24.012 | 0.903 | 0.903 |
| engine-idb | stale-projection | 32.284 | 39.314 | 1.218 | 1.151 |
| engine-idb | watermark | 1.103 | 1.002 | 0.908 | 0.956 |
| node-sqlite | accepted-transaction | 0.303 | 0.313 | 1.032 | 1.092 |
| node-sqlite | stale-projection | 0.309 | 0.330 | 1.069 | 1.107 |
| node-sqlite | watermark | 0.127 | 0.132 | 1.043 | 1.056 |

The Node fixture uses fake-indexeddb and in-memory SQLite with instrumented method calls. Setup and committed-value checks run outside each timer, but influence caches. Both builds share a process with no explicit warmup. Every call reaches its intended storage mutator. Source/dist/dependency hashes remain unchanged across all six completed reports. These are fixture API calls, not physical disk I/O or phone costs. SQLite stores row authority in its shared metadata table, so table counters alone cannot distinguish those keys.

SQLite control publishes the old status during the stale-projection cell, while both IndexedDB controls publish the correct status in that mixed-field workload. Therefore that cell compares correction cost against an incorrect SQLite hot result, not equivalent behavior. Separate negative regressions cover unchanged rejected writes in IndexedDB. Some cells are slower; a controlled native-backend profile is needed before any general speed claim.
