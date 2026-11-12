import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { wrapBundle } from "../host/src/bundle.ts";

// K16's anonymous calls on the kernel's real paths: the K16 harness tenant
// (harness/tenant.ts: K07's tenant object, K08's callQuery, K12's
// ActionRuntime) behind K05 sockets and the address limiter, hosting K07's
// test fixture plus the conformance app's `actions` module.
// `vitest run --config kernel/public/vitest.reducer.config.mjs` from cloudflare/.
const moduleGlobals = fileURLToPath(new URL("../host/src/module-globals.ts", import.meta.url));
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

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  plugins: [modulePrelude(), cloudflareTest({ wrangler: { configPath: "./test/reducer/wrangler.jsonc" }, miniflare: { bindings: { GONVEX_STUB_ADMIN_KEY: "k07-test-key" } } })],
  resolve: {
    alias: {
      "@gonvex/module-sdk": fileURLToPath(new URL("../../../packages/module-sdk/src/index.ts", import.meta.url)),
      "@conformance-fixture": fileURLToPath(new URL("../../conformance/fixtures/app/gonvex", import.meta.url)),
      "@conformance-fixture-v2": fileURLToPath(new URL("../../conformance/fixtures/app-v2/gonvex", import.meta.url)),
    },
  },
  test: { include: ["test/reducer/*.spec.ts"] },
});
