import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig, type Plugin } from "vitest/config";
import { wrapBundle } from "../../kernel/host/src/bundle.ts";

// K18 tests run inside workerd: the ingress routes, K06's control object for
// tenant resolution, the ingress object, and a test Action port that runs
// module Actions on K04's engine (test/worker.ts).

export const INGRESS_TOKEN = "gvx_svc_k18_ingress_test_credential";
/** A principal configured without `channel` (Rust's `api`). */
export const PLAIN_TOKEN = "gvx_svc_k18_plain_test_credential";

const here = (path: string) => fileURLToPath(new URL(path, import.meta.url));
const moduleGlobals = here("../../kernel/host/src/module-globals.ts");
const whagonsApp = here("./.build/whagons.module.js");
const whagonsFunctions = here("./.build/whagons.functions.json");
const built = existsSync(whagonsApp) && existsSync(whagonsFunctions);

/** Module bundles get the prelude the kernel build gives an app bundle (K04 `wrapBundle`). */
function modulePrelude(): Plugin {
  return {
    name: "gonvex-module-prelude",
    enforce: "pre",
    transform(code, id) {
      const path = id.split("?")[0] ?? id;
      if (!path.endsWith(".module.js")) return null;
      return { code: wrapBundle(code, moduleGlobals), map: null };
    },
  };
}

export default defineConfig({
  plugins: [
    modulePrelude(),
    cloudflareTest({
      wrangler: { configPath: "./test/wrangler.jsonc" },
      miniflare: {
        bindings: {
          GONVEX_ENV: "test",
          GONVEX_INGRESS_SERVICE_TOKEN: INGRESS_TOKEN,
          GONVEX_SERVICE_PRINCIPALS: JSON.stringify([
            {
              id: "ingress",
              tokenSha256: createHash("sha256").update(INGRESS_TOKEN).digest("hex"),
              projects: ["proj-fixture", "proj-whagons"],
              functions: ["calling.ingestWebhook", "relay.ingestTelnyxWebhook", "github.ingestWebhook", "taskPublicShares.getPublicPreviewByToken"],
              maxDelegationSeconds: 600,
              // 0045 §3: the channel comes from the principal's configuration, never the request.
              channel: "system",
            },
            {
              id: "plain",
              tokenSha256: createHash("sha256").update(PLAIN_TOKEN).digest("hex"),
              projects: ["proj-fixture"],
              functions: ["calling.ingestWebhook"],
            },
          ]),
          WHAGONS_BUILT: built ? "1" : "",
        },
      },
    }),
  ],
  resolve: {
    alias: {
      "gonvex:whagons-app": built ? whagonsApp : here("./test/fixtures/absent.js"),
      "gonvex:whagons-functions": built ? whagonsFunctions : here("./test/fixtures/absent.json"),
    },
  },
  test: { include: ["test/**/*.test.ts"], testTimeout: 60_000 },
});
