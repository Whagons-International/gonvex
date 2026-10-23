export function setEngineProperty(target: any, key: string, value: unknown) {
  if (key === "__proto__") Object.defineProperty(target, key, { value, writable: true, enumerable: true, configurable: true });
  else target[key] = value;
}

export type EnginePatch = { path: string[]; value?: unknown; remove?: true };

/** Worker snapshots are immutable. Equal references skip entire row subtrees. */
export function diffEngineValue(previous: unknown, next: unknown, limit = 128): EnginePatch[] | undefined {
  const patches: EnginePatch[] = [];
  const visit = (before: any, after: any, path: string[]) => {
    if (Object.is(before, after) || patches.length > limit) return;
    if (!before || !after || typeof before !== "object" || typeof after !== "object"
      || Array.isArray(before) !== Array.isArray(after) || (Array.isArray(after) && before.length !== after.length)) {
      patches.push({ path, value: after }); return;
    }
    for (const key of Object.keys(before)) if (!Object.hasOwn(after, key)) patches.push({ path: [...path, key], remove: true });
    for (const key of Object.keys(after)) {
      if (!Object.hasOwn(before, key)) patches.push({ path: [...path, key], value: after[key] });
      else visit(before[key], after[key], [...path, key]);
    }
  };
  visit(previous, next, []);
  return patches.length > limit ? undefined : patches;
}

/** Clone each changed container once; unaffected rows and nested values stay shared. */
export function applyEnginePatches(previous: unknown, patches: readonly EnginePatch[]): unknown {
  let result: any = previous;
  const copies = new WeakMap<object, any>();
  const owned = new Set<object>();
  const copy = (value: any) => {
    if (owned.has(value)) return value;
    let next = copies.get(value);
    if (!next) { next = Array.isArray(value) ? value.slice() : { ...value }; copies.set(value, next); owned.add(next); }
    return next;
  };
  for (const patch of patches) {
    if (!patch.path.length) { result = patch.value; continue; }
    if (!result || typeof result !== "object") throw new Error("Engine patch requires a resnapshot");
    result = copy(result);
    let parent = result;
    for (const key of patch.path.slice(0, -1)) {
      if (!Object.hasOwn(parent, key) || !parent[key] || typeof parent[key] !== "object") throw new Error("Engine patch requires a resnapshot");
      setEngineProperty(parent, key, copy(parent[key])); parent = parent[key];
    }
    const key = patch.path[patch.path.length - 1];
    if (patch.remove) delete parent[key]; else setEngineProperty(parent, key, patch.value);
  }
  for (const value of owned) Object.freeze(value);
  return result;
}
