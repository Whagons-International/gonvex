import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
const source = path => fileURLToPath(new URL(path, import.meta.url));
export default defineConfig({
  resolve: { alias: {
    '@gonvex/client': source('../../../packages/client/src/index.ts'),
    '@gonvex/protocol': source('../../../packages/protocol/src/index.ts'),
    '@gonvex/module-sdk': source('../../../packages/module-sdk/src/index.ts'),
    '@gonvex/local-runtime/read-view': source('../../../packages/local-runtime/src/read-view.ts'),
    'capacity-app-refs': process.env.GONVEX_CAPACITY_APP_REFS ?? source('./app-refs.fixture.mjs'),
  } },
  test: { environment: 'node', include: ['conformance/capacity/*.test.{mjs,ts}'], maxWorkers: 1 },
});
