/**
 * Backend registry. `--backend <label>=<kind>` picks a kind from here, or a
 * path to a module whose default export is a Backend subclass.
 */
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { RustBackend } from './rust.mjs';
import { CloudflareControlBackend } from './cloudflare-control.mjs';

export const BACKENDS = {
  rust: RustBackend,
  // K06: the control object in local workerd (control-plane scenarios only).
  'cloudflare-control': CloudflareControlBackend,
  // cloudflare: the full kernel (local workerd, then the dev account), added by the kernel packages.
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
