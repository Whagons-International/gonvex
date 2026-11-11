import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vitest/config";
import { wrapBundle } from "../kernel/host/src/bundle.ts";

const moduleGlobals = fileURLToPath(new URL("../kernel/host/src/module-globals.ts", import.meta.url));
const moduleSdk = fileURLToPath(new URL("../../packages/module-sdk/src/index.ts", import.meta.url));
/** The P02 conformance fixture app: its `actions` functions run through the K12 runtime here. */
const conformanceFixture = "/cloudflare/conformance/fixtures/app/gonvex/";

/** Module code gets the kernel's app-bundle prelude (K04 `src/bundle.ts`), as the Worker build gives it. */
function modulePrelude(): Plugin {
  return {
    name: "gonvex-module-prelude",
    enforce: "pre",
    transform(code, id) {
      const path = id.split("?")[0] ?? id;
      if (!path.endsWith(".module.js") && !path.includes(conformanceFixture)) return null;
      return { code: wrapBundle(code, moduleGlobals), map: null };
    },
  };
}

// Action runtime (K12) tests: inside workerd, with a SQLite-backed tenant object and a real Workflow.
export default defineConfig({
  plugins: [modulePrelude(), cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" } })],
  resolve: {
    alias: {
      "@gonvex/module-sdk": moduleSdk,
      "@conformance-fixture": fileURLToPath(new URL("../conformance/fixtures/app/gonvex", import.meta.url)),
      "@conformance-scenarios": fileURLToPath(new URL("../conformance/scenarios", import.meta.url)),
    },
  },
  test: { include: ["test/**/*.test.ts"], testTimeout: 60_000 },
});
