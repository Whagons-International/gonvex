// Contention benchmark for the outlet's cross-tenant slot handoff (not part of `pnpm test`). One scenario per run:
//   BENCH=<tenants>x<holders> npx vitest run --root kernel/gateway --config vitest.bench.config.mjs
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" }, miniflare: { bindings: { BENCH: process.env.BENCH ?? "4x3" } } })],
  test: { testTimeout: 120_000, include: ["test/*.bench.ts"] },
});
