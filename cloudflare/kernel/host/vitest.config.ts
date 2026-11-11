import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vitest/config";
import { wrapBundle } from "./src/bundle.ts";

const moduleGlobals = fileURLToPath(new URL("./src/module-globals.ts", import.meta.url));
const moduleSdk = fileURLToPath(new URL("../../../packages/module-sdk/src/index.ts", import.meta.url));
/** The P02 conformance fixture app, run through the K04 host by test/conformance-parity.test.ts. */
const conformanceFixture = "/cloudflare/conformance/fixtures/app/gonvex/";

/**
 * Test modules (`*.module.js`) and the published runtime-contract runner are
 * module bundles: give them the same prelude the kernel build gives the app
 * bundle (src/bundle.ts), so they see the module globals, not workerd's.
 */
function modulePrelude(): Plugin {
  return {
    name: "gonvex-module-prelude",
    enforce: "pre",
    transform(code, id) {
      const path = id.split("?")[0] ?? id;
      if (!path.endsWith(".module.js") && !path.endsWith("runtime-contract/runner.js") && !path.includes(conformanceFixture)) return null;
      return { code: wrapBundle(code, moduleGlobals), map: null };
    },
  };
}

// Kernel module host (K04) tests: inside workerd, with SQLite-backed objects.
export default defineConfig({
  plugins: [modulePrelude(), cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" } })],
  // The fixture app imports the SDK by name; CI installs only this workspace, so use its source.
  resolve: {
    alias: {
      "@gonvex/module-sdk": moduleSdk,
      "@conformance-fixture": fileURLToPath(new URL("../../conformance/fixtures/app/gonvex", import.meta.url)),
    },
  },
  test: { include: ["test/**/*.test.ts"] },
});
