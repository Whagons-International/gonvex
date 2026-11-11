import { fileURLToPath } from "node:url";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// K19 tests run inside workerd against the local kernel (local/wrangler.jsonc):
// the router, the control object with the operator surface, the local tenant
// object, R2 and the rollout queue. The release builder runs in-process in the
// control object (`setReleaseBuilderForTest`); the router's deploy call goes
// to a fake builder URL answered by `outboundService`.
export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./local/wrangler.jsonc" },
      miniflare: {
        bindings: {
          GONVEX_ADMIN_KEY: "k19-admin-key",
          GONVEX_DASHBOARD_SESSION_SECRET: "k19-session-secret",
          DASHBOARD_AUTH_ACCOUNT: "Operator@Example.test",
          DASHBOARD_AUTH_PASSWORD: "configured-password-123",
          GONVEX_DEV_SYNC_KEY: "k19-fallback-sync-key",
          GONVEX_RELEASE_BUILDER_URL: "http://builder.k19.test",
          GONVEX_RELEASE_BUILDER_TOKEN: "k19-builder-token",
          GONVEX_SYNC_DEADLINE_MS: "60000",
        },
        outboundService: async (request: Request) => {
          const url = new URL(request.url);
          if (url.host === "builder.k19.test" && url.pathname === "/deploy") return Response.json({ ok: true });
          return new Response("unexpected outbound fetch", { status: 599 });
        },
      },
    }),
  ],
  resolve: {
    alias: {
      "gonvex:releases": fileURLToPath(new URL("./test/releases.fixture.js", import.meta.url)),
      "gonvex:module-globals": fileURLToPath(new URL("../../kernel/host/src/module-globals.ts", import.meta.url)),
    },
  },
  test: { include: ["test/**/*.test.ts"], testTimeout: 60_000 },
});
