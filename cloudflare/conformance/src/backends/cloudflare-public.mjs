/**
 * K16's harness on local workerd (`cloudflare/kernel/public/harness`): the
 * `cloudflare-reducer` backend (K05 sockets, K07's tenant object, the fixture
 * bundles of the run) with the address limiter, and with tenant-scope
 * `query.call` (K08's `callQuery`) and `action.call` (K12's `ActionRuntime`)
 * composed into the tenant object. It runs `public-calls` and `public-limits`.
 * The tenant-object composition proper is the assembly package's (0045).
 */
import { join } from 'node:path';
import { CloudflareReducerBackend } from './cloudflare-reducer.mjs';
import { REPO } from '../paths.mjs';

export class CloudflarePublicBackend extends CloudflareReducerBackend {
  get capabilities() { return { restart: true, crash: true, publicCalls: true }; }
  get harnessConfig() { return join(REPO, 'cloudflare/kernel/public/harness/wrangler.jsonc'); }
}
