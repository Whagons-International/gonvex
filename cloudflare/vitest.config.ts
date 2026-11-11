import { fileURLToPath } from "node:url";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// Tests run inside workerd with the bindings from wrangler.jsonc. No account
// or network access is needed. GONVEX_STUB_ADMIN_KEY enables the K05 stub
// routes (kernel/session/stub/routes.ts) for the session tests.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./wrangler.jsonc" }, miniflare: { bindings: { GONVEX_STUB_ADMIN_KEY: "test-key" } } })],
  resolve: {
    // The kernel storage tests (K01) run the SDK's own SQL suites against
    // Durable Object SQLite, from the SDK's sources.
    alias: { "@gonvex/module-sdk": fileURLToPath(new URL("../packages/module-sdk/src/index.ts", import.meta.url)) },
  },
  // scripts/*.test.mjs are Node tests (node --test), not workerd tests.
  // kernel/host (K04) runs with its own config: `vitest run --root kernel/host`.
  test: { include: ["test/**/*.test.ts", "kernel/**/*.test.ts"], exclude: ["kernel/host/**", "**/node_modules/**"] },
});
