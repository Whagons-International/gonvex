/**
 * Backend registry. `--backend <label>=<kind>` picks a kind from here, or a
 * path to a module whose default export is a Backend subclass.
 */
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { RustBackend } from './rust.mjs';

export const BACKENDS = {
  rust: RustBackend,
  // cloudflare: added by the kernel packages (local workerd, then the dev account).
};

export async function backendClass(kind) {
  if (BACKENDS[kind]) return BACKENDS[kind];
  if (/[./]/.test(kind)) {
    const module = await import(pathToFileURL(resolve(kind)).href);
    if (typeof module.default !== 'function') throw new Error(`${kind}: default export must be a Backend subclass`);
    return module.default;
  }
  throw new Error(`unknown backend kind "${kind}" (known: ${Object.keys(BACKENDS).join(', ')}, or a path to a module)`);
}
