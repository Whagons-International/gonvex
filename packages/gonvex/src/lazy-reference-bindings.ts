/** Runtime emitted into generated API bindings. Unused metadata stays unallocated. */
export const lazyReferenceRuntime = `function lazyReferences<T extends Record<string, () => unknown>>(factories: T): { readonly [Key in keyof T]: ReturnType<T[Key]> } {
  const references = {} as { readonly [Key in keyof T]: ReturnType<T[Key]> };
  for (const key of Object.keys(factories)) {
    const factory = factories[key]!;
    let initialized = false;
    let value: unknown;
    Object.defineProperty(references, key, { enumerable: true, get() {
      if (!initialized) { value = factory(); initialized = true; }
      return value;
    } });
  }
  return references;
}`;

export function renderLazyReferences(value: Record<string, unknown>, depth: number,
  isReference: (value: unknown) => boolean, renderReference: (value: unknown, depth: number) => string): string {
  if (isReference(value)) return renderReference(value, depth);
  const indent = '  '.repeat(depth), childIndent = '  '.repeat(depth + 1);
  const entries = Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  return ['/* @__PURE__ */ lazyReferences({', ...entries.map(([key, child]) =>
    `${childIndent}${JSON.stringify(key)}: () => (${renderLazyReferences(child as Record<string, unknown>, depth + 1, isReference, renderReference)}),`), `${indent}})`].join('\n');
}
