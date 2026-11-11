import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// Kernel files (K13) tests: inside workerd, with a SQLite-backed tenant object
// and a local R2 bucket. `vitest run --root kernel/files`.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" } })],
  test: { include: ["test/**/*.test.ts"] },
});
