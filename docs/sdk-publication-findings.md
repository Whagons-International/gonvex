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
