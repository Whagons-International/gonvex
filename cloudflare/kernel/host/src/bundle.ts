// The app-bundle prelude: Query/Reducer bundles are ES modules evaluated as
// part of the Worker version (0016 G0-2). Prepending one import gives the
// bundle lexical `Date`, `Math`, `crypto`, `console`, `globalThis`, `self` and the
// absent globals from `module-globals.ts`, without touching the kernel's
// own globals (CONTRACT.md B.3, Cloudflare paragraph).
//
// Plain TypeScript with no imports, so Node build scripts can load it directly.

export const moduleGlobalBindings = [
  "Date", "Math", "crypto", "console", "globalThis", "self", "fetch", "WebSocket", "Request", "caches", "structuredClone",
  "queueMicrotask", "performance", "Blob", "FormData", "EventTarget", "navigator", "scheduler",
] as const;

/** Returns the bundle source with the prelude. `specifier` resolves to `module-globals`. */
export function wrapBundle(source: string, specifier: string): string {
  const declared = topLevelConflicts(source);
  if (declared.length) throw new Error(`the module bundle declares ${declared.join(", ")} at top level; the kernel prelude binds these names`);
  return `import { ${moduleGlobalBindings.join(", ")} } from ${JSON.stringify(specifier)};\n${source}`;
}

/** Cheap guard: a top-level `const|let|var|function|class <name>` for a prelude name would be a SyntaxError. */
function topLevelConflicts(source: string): string[] {
  const names = new Set<string>(moduleGlobalBindings);
  const found = new Set<string>();
  const pattern = /^(?:export\s+)?(?:const|let|var|function\*?|class|async\s+function)\s+([A-Za-z_$][\w$]*)/gm;
  for (const match of source.matchAll(pattern)) if (match[1] !== undefined && names.has(match[1])) found.add(match[1]);
  return [...found];
}
