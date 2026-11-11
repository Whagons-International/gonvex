import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig, type Plugin } from "vitest/config";
import { fileURLToPath } from "node:url";
import { wrapBundle } from "../kernel/host/src/bundle.ts";

const moduleGlobals = fileURLToPath(new URL("../kernel/host/src/module-globals.ts", import.meta.url));

/** Test Action modules (`*.module.js`) get the kernel's app-bundle prelude, as K12's tests do. */
function modulePrelude(): Plugin {
  return {
    name: "gonvex-module-prelude",
    enforce: "pre",
    transform(code, id) {
      const path = id.split("?")[0] ?? id;
      return path.endsWith(".module.js") ? { code: wrapBundle(code, moduleGlobals), map: null } : null;
    },
  };
}

// Sandbox (K20) tests: inside workerd, with the three objects and a Worker Loader.
export default defineConfig({
  plugins: [modulePrelude(), cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" } })],
  test: { include: ["test/**/*.test.ts"] },
});
