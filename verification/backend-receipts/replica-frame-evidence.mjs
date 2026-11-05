// The wire can hold a shared batch envelope rather than standalone frames.
// Require actual row data at the committed revision; a watermark alone is not
// evidence that the replica persistence barrier is being exercised.
export function hasHeldReplicaData(frames, revision, rowId) {
  return frames.some(frame => {
    if (frame?.type === 'replica.batch') {
      return Array.isArray(frame.items) && hasHeldReplicaData(frame.items, revision, rowId);
    }
    if (!['replica.transaction', 'replica.delta', 'replica.snapshotPage'].includes(frame?.type)
        || !(frame.cursor?.revision >= revision)) return false;
    const rows = frame.type === 'replica.snapshotPage' ? frame.result
      : frame.changes?.map(change => change.newValue);
    return Array.isArray(rows) && rows.some(row => row?.id === rowId);
  });
}
