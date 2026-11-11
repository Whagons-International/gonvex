/**
 * The local kernel (K19): the router's `/dev/*` and `/auth/*`, the rollout
 * queue consumer, the control object with the operator surface and the local
 * tenant object. `gonvex dev --once` deploys to it and the dashboard runs
 * against it (`local/README.md`, `local/start.mjs`).
 *
 * Local and test only. Deployed kernels compose the same pieces in the real
 * router Worker (`cloudflare/src/index.ts`) once the tenant object exists.
 */
import { OperatorControlObject } from "../../../control/operator/object.ts";
import type { RolloutMessage } from "../../../control/operator/release.ts";
import type { OperatorTenantPort } from "../../../control/operator/tenant/port.ts";
import { handleRolloutBatch } from "../rollout.ts";
import { CONTROL_NAME, handleDevRequest, type DevRouterEnv } from "../routes.ts";
import { LocalKernelTenant } from "./tenant.ts";
// @ts-expect-error resolved by the wrangler alias to the generated releases module.
import * as generated from "gonvex:releases";

const releasesVersion = (generated as { version?: string }).version;

export { LocalKernelTenant, OperatorControlObject };

export interface LocalKernelEnv extends DevRouterEnv {
  TENANT: DurableObjectNamespace<LocalKernelTenant>;
  GONVEX_ADMIN_KEY?: string;
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    const dev = await handleDevRequest(request, env, ctx);
    if (dev) return dev;
    if (url.pathname.startsWith("/auth/")) return env.CONTROL.getByName(CONTROL_NAME).fetch(request);
    // Local verification: which release this Worker version serves to a tenant (admin key only).
    if (url.pathname === "/_gonvex/k19/served" && env.GONVEX_ADMIN_KEY && request.headers.get("authorization") === `Bearer ${env.GONVEX_ADMIN_KEY}`) {
      const tenant = env.TENANT.getByName(url.searchParams.get("tenant") ?? "") as unknown as OperatorTenantPort;
      const project = url.searchParams.get("project") ?? "";
      return Response.json({ served: await tenant.operatorServedModule(project), status: await tenant.operatorMigrationStatus(), ledger: await tenant.operatorLedger() });
    }
    // Which Worker version is live (local tools wait for it after a sync: the swap reloads workerd).
    if (url.pathname === "/_gonvex/k19/version") return Response.json({ version: releasesVersion ?? null });
    if (url.pathname === "/") return Response.json({ ok: true, service: "gonvex-cloudflare-local-kernel" });
    return new Response(null, { status: 404 });
  },
  async queue(batch, env): Promise<void> {
    await handleRolloutBatch(batch as MessageBatch<RolloutMessage>, env as never);
  },
} satisfies ExportedHandler<LocalKernelEnv>;
