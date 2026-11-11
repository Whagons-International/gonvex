/**
 * Backend registry. `--backend <label>=<kind>` picks a kind from here, or a
 * path to a module whose default export is a Backend subclass.
 */
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { RustBackend } from './rust.mjs';
import { CloudflareControlBackend } from './cloudflare-control.mjs';
import { CloudflareBackend } from './cloudflare.mjs';
import { CloudflareReducerBackend } from './cloudflare-reducer.mjs';
import { CloudflareKernelBackend } from './cloudflare-kernel.mjs';
import { CloudflarePrincipalsBackend } from './cloudflare-principals.mjs';
import { CloudflareDeliveryBackend } from './cloudflare-delivery.mjs';

export const BACKENDS = {
  rust: RustBackend,
  // K06: the control object in local workerd (control-plane scenarios only).
  'cloudflare-control': CloudflareControlBackend,
  // The kernel on local workerd (K05: session layer; later packages fill in the rest).
  cloudflare: CloudflareBackend,
  // K07: the kernel with the Reducer path (receipt scenarios), fixture bundles built into the Worker.
  'cloudflare-reducer': CloudflareReducerBackend,
  // K19: the local kernel with the deploy and operator routes (operator-routes scenario).
  'cloudflare-kernel': CloudflareKernelBackend,
  // K17: K05 sockets in front of K06's control object and K07's tenant object (service-principals scenario).
  'cloudflare-principals': CloudflarePrincipalsBackend,
  // K10: the K07 tenant object plus replica delivery (replica scenarios, G1).
  'cloudflare-delivery': CloudflareDeliveryBackend,
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
