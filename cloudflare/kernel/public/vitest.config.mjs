import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// K16 tests run the kernel's own Worker (src/index.ts) inside workerd with
// small limits (test/wrangler.jsonc), so every limit trips at its configured
// count quickly. `*.spec.ts`, so the root config (`*.test.ts`) leaves them to this one;
// `test/reducer/` runs on K07's Worker (vitest.reducer.config.mjs).
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" }, miniflare: { bindings: { GONVEX_STUB_ADMIN_KEY: "k16-test-key" } } })],
  test: { include: ["test/*.spec.ts"] },
});
