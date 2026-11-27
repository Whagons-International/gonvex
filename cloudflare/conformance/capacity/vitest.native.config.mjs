import { defineConfig } from 'vitest/config';
import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { fileURLToPath } from 'node:url';
export default defineConfig({plugins:[cloudflareTest({wrangler:{configPath:fileURLToPath(new URL('../../kernel/gateway/test/wrangler.jsonc',import.meta.url))}})],
 test:{include:['conformance/capacity/*.native.spec.ts']}});
