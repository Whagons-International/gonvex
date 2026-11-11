import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { wrapBundle } from "../host/src/bundle.ts";

const moduleGlobals = fileURLToPath(new URL("../host/src/module-globals.ts", import.meta.url));
const moduleSdk = fileURLToPath(new URL("../../../packages/module-sdk/src/index.ts", import.meta.url));
/** The P02 conformance fixture app: module code, so it gets the kernel prelude (K04 `src/bundle.ts`). */
const conformanceFixture = "/cloudflare/conformance/fixtures/";

function modulePrelude() {
  return {
    name: "gonvex-module-prelude",
    enforce: "pre",
    transform(code, id) {
      const path = id.split("?")[0] ?? id;
      if (!path.includes(conformanceFixture) && !path.endsWith(".module.js")) return null;
      return { code: wrapBundle(code, moduleGlobals), map: null };
    },
  };
}

// Service principal (K17) specs: inside workerd, through the K05 socket path,
// with K06's control object and K07's tenant object (test/wrangler.jsonc).
// `*.spec.ts`, so the root config (`*.test.ts`) does not pick them up.
export default defineConfig({
  plugins: [modulePrelude(), cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" }, miniflare: { bindings: { GONVEX_ADMIN_KEY: "k17-test-admin" } } })],
  resolve: {
    alias: {
      "@gonvex/module-sdk": moduleSdk,
      "@conformance-fixture": fileURLToPath(new URL("../../conformance/fixtures/app/gonvex", import.meta.url)),
    },
  },
  test: { include: ["test/**/*.spec.ts"], testTimeout: 60_000 },
});
