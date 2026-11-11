import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// Tests run inside workerd with the bindings from wrangler.jsonc. No account
// or network access is needed. GONVEX_STUB_ADMIN_KEY enables the K05 stub
// routes (kernel/session/stub/routes.ts) for the session tests.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./wrangler.jsonc" }, miniflare: { bindings: { GONVEX_STUB_ADMIN_KEY: "test-key" } } })],
  // scripts/*.test.mjs are Node tests (node --test), not workerd tests.
  test: { include: ["test/**/*.test.ts", "kernel/**/*.test.ts"] },
});
