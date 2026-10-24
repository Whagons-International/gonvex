// Benchmark candidate, deliberately outside the supported SDK protocol.
// A missing base fails closed; the caller must request a complete row/snapshot.
export function applyPatch(base, patch) {
  if (!Number.isSafeInteger(patch.revision) || !Number.isSafeInteger(patch.baseRevision)
    || patch.revision <= patch.baseRevision) throw new Error('invalid-version');
  if (base && patch.revision <= base.revision) return base;
  if (!base || base.revision !== patch.baseRevision) throw new Error('missing-base');
  const value = { ...base.value };
  for (const key of patch.unset) delete value[key];
  for (const [key, field] of Object.entries(patch.set)) Object.defineProperty(value, key, { value: field, enumerable: true, writable: true, configurable: true });
  return { revision: patch.revision, value };
}
