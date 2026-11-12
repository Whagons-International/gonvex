import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig, type Plugin } from "vitest/config";
import { fileURLToPath } from "node:url";
import { wrapBundle } from "../kernel/host/src/bundle.ts";
import { MANAGER_PORT } from "./test/global-setup.mjs";

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

// Sandbox (K20) tests: the service, the Container backend and object in workerd,
// against the manager test/global-setup.ts runs (Rust binaries, the image, or stubs).
const managerUrl = process.env.K20_SANDBOX_URL ?? `http://127.0.0.1:${MANAGER_PORT}`;
export default defineConfig({
  plugins: [
    modulePrelude(),
    cloudflareTest({ wrangler: { configPath: "./test/wrangler.jsonc" }, miniflare: { bindings: { SANDBOX_CONTAINER_URL: managerUrl } } }),
  ],
  test: { include: ["test/**/*.test.ts"], globalSetup: ["./test/global-setup.mjs"], testTimeout: 60_000 },
});
