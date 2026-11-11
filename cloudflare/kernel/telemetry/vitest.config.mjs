import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// K15 telemetry tests: inside workerd, with the control object and its
// operator routes (K06 + K19) from test/wrangler.jsonc. `*.spec.ts`, so the
// root config (`*.test.ts`) does not pick them up without this Worker.
export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./test/wrangler.jsonc" },
      miniflare: { bindings: { GONVEX_ADMIN_KEY: "k15-admin-key" } },
    }),
  ],
  test: { include: ["test/**/*.spec.ts"], testTimeout: 60_000 },
});
