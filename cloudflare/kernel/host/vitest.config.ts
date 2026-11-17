import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { createRequire } from "node:module";
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

/**
 * `gonvex-test:namespaced-app` is the BUG-1 fixture app built the way the
 * release builder ships an Action bundle: one ES module bundled from the app
 * entry (`export * as ns from "./file"`, as Whagons does), with the prelude
 * from src/bundle.ts. The test then runs its Actions through the real exports.
 */
function namespacedAppBundle(): Plugin {
  const id = "gonvex-test:namespaced-app";
  const resolved = `\0${id}.js`;
  return {
    name: "gonvex-namespaced-app-bundle",
    enforce: "pre",
    resolveId: (source) => (source === id ? resolved : null),
    async load(source) {
      if (source !== resolved) return null;
      const { build } = createRequire(import.meta.resolve("@cloudflare/vitest-plugin"))("esbuild") as typeof import("esbuild");
      const result = await build({
        entryPoints: [fileURLToPath(new URL("./test/fixtures/namespaced-app/index.ts", import.meta.url))],
        bundle: true, write: false, format: "esm", platform: "neutral", target: "esnext",
        alias: { "@gonvex/module-sdk": moduleSdk },
      });
      return { code: wrapBundle(result.outputFiles[0]!.text, moduleGlobals), map: null };
    },
  };
}

// Kernel module host (K04) tests: inside workerd, with SQLite-backed objects.
export default defineConfig({
  plugins: [modulePrelude(), namespacedAppBundle(), cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" } })],
  // The fixture app imports the SDK by name; CI installs only this workspace, so use its source.
  resolve: {
    alias: {
      "@gonvex/module-sdk": moduleSdk,
      "@conformance-fixture": fileURLToPath(new URL("../../conformance/fixtures/app/gonvex", import.meta.url)),
    },
  },
  test: { include: ["test/**/*.test.ts"] },
});
