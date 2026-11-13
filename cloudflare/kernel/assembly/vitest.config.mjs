import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import { wrapBundle } from '../host/src/bundle.ts';
const globals = fileURLToPath(new URL('../host/src/module-globals.ts', import.meta.url));
export default defineConfig({
 plugins:[{name:'module-prelude',enforce:'pre',transform(code,id){return id.split('?')[0].endsWith('.module.js')?{code:wrapBundle(code,globals),map:null}:null;}},cloudflareTest({wrangler:{configPath:'./test/wrangler.jsonc'}})],
 resolve:{alias:{'gonvex:releases':fileURLToPath(new URL('./releases.ts',import.meta.url))}},
 test:{include:['test/**/*.spec.ts'],testTimeout:30000},
});
