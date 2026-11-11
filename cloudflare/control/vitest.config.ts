import { createHash } from "node:crypto";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// Tests run inside workerd with the control object, the stub tenant object and
// the minimal control socket owner from wrangler.jsonc. No account or network.
export const SERVICE_TOKEN = "gvx_svc_test_gateway_credential";
export const SERVICE_PROJECT = "project-service-principal";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          GONVEX_ADMIN_KEY: "test-admin-key",
          GONVEX_DASHBOARD_SESSION_SECRET: "conformance-control-secret",
          GONVEX_AUTH_URL: "https://auth.gonvex.test",
          GONVEX_GOOGLE_CLIENT_ID: "test-google-client",
          GONVEX_GOOGLE_CLIENT_SECRET: "test-google-secret",
          GONVEX_TEST_REVALIDATION_MS: "300",
          GONVEX_TEST_AUTH_DEADLINE_MS: "2500",
          GONVEX_SERVICE_PRINCIPALS: JSON.stringify([
            {
              id: "gateway",
              tokenSha256: createHash("sha256").update(SERVICE_TOKEN).digest("hex"),
              projects: [SERVICE_PROJECT],
              functions: ["keys.check"],
              maxDelegationSeconds: 600,
            },
          ]),
        },
      },
    }),
  ],
  test: { include: ["test/**/*.test.ts"] },
});
