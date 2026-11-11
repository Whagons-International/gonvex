import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

/** cloudflare/conformance */
export const CONFORMANCE = resolve(fileURLToPath(new URL('..', import.meta.url)));
/** Repository root. */
export const REPO = resolve(CONFORMANCE, '../..');
