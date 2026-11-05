// Synthetic fixture only. Deterministic keys make interrupted setup resumable.
export async function seedFixture(storage, scope, fixtureHash) {
  const empty = { entities: {}, liveQueries: {} };
  const markerCoverage = { _memoryFixture: { key: '_id', complete: true } };
  const marker = await storage.withReadView(scope, markerCoverage, view => view.select({ table: '_memoryFixture', where: { column: '_id', op: 'eq', value: 'fixture' }, limit: 1 }));
  if (marker.rows[0]?.hash === fixtureHash) return { reused: true };
  // Long, distinct bodies make accidental payload retention visible. Write in
  // bounded batches; no array containing 10,000 task bodies is ever constructed.
  const prose = 'Verify equipment, record observations, and document the inspection. '.repeat(250);
  for (let start = 0; start < 10_000; start += 200) {
    const changes = [];
    for (let index = start; index < start + 200; index++) {
      const id = `task-${String(index).padStart(5, '0')}`;
      changes.push({ entity: 'tasks', id, operation: 'insert', newValue: {
        _id: id, name: `Inspection ${index}`, workspaceId: Math.floor(index / 2000), rank: index,
        description: `Distinct reference ${index}. ${index < 2000 ? prose : 'Verify the equipment and record the outcome.'}`, deletedAt: null,
      } });
    }
    await storage.applyTransaction({ cursor: { epoch: 'fixture', revision: start + 1 }, changes }, empty, scope);
  }
  await storage.applyTransaction({ cursor: { epoch: 'fixture', revision: 10_001 }, changes: [{ entity: '_memoryFixture', id: 'fixture', operation: 'insert', newValue: { _id: 'fixture', hash: fixtureHash } }] }, empty, scope);
  return { reused: false };
}
