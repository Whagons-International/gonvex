import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// Tests run inside workerd with the bindings from wrangler.jsonc. No account
// or network access is needed.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./wrangler.jsonc" } })],
  // scripts/*.test.mjs are Node tests (node --test), not workerd tests.
  test: { include: ["test/**/*.test.ts", "kernel/**/*.test.ts"] },
});
