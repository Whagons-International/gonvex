// K22: the serving kernel. Fixture routes and the K05 stand-ins are test-only.
import { handleTenantTransfer, type TransferEnv } from "../kernel/assembly/tenant-transfer";
import { fixtureControl } from "../kernel/assembly/fixture-control";
import type { RolloutMessage } from "../control/operator/release";
import { handleWebSocketUpgrade } from "../kernel/session/router";
import { handleDevRequest } from "../router/dev/routes";
import { handleRolloutBatch } from "../router/dev/rollout";
import { handleStorageRequest, STORAGE_PREFIX } from "../router/storage/index";
import { filesConfigFromEnv } from "../kernel/files/config";
import type { FilesTenantRpc } from "../kernel/files/rpc";
import { handleIngressRequest } from "../router/ingress/routes";
import { resident, residentHandler } from "../kernel/assembly/residency";
import { handleBackupRequest, runBackup, type BackupEnv } from "../kernel/assembly/backups";
import { SessionControlObject as Control } from "../kernel/assembly/control";
import { AssemblyGateway as Gateway } from "../kernel/assembly/gateway";
import { GatewayControllerObject as GatewayController } from "../kernel/gateway/controller";
import { PublicLimiterObject as PublicLimiter } from "../kernel/public";
import { IngressObject as Ingress } from "../router/ingress/object";

// Data residency (kernel/assembly/residency.ts): every export receives an env whose Durable Object namespaces are
// restricted to GONVEX_JURISDICTION when the stack has one.
export const SessionControlObject = resident(Control);
export type SessionControlObject = Control;
export const AssemblyGateway = resident(Gateway);
export type AssemblyGateway = Gateway;
export const GatewayControllerObject = resident(GatewayController);
export type GatewayControllerObject = GatewayController;
export const PublicLimiterObject = resident(PublicLimiter);
export type PublicLimiterObject = PublicLimiter;
export const IngressObject = resident(Ingress);
export type IngressObject = Ingress;

interface ServingEnv extends Env, TransferEnv { GONVEX_ASSEMBLY_FIXTURE_KEY?: string; GONVEX_STACK_NAME?: string; GONVEX_MAINTENANCE?: string }

export default residentHandler({
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/") return Response.json({ ok: true, service: "gonvex-cloudflare" });
    const backups = await handleBackupRequest(request, env as unknown as BackupEnv, env.GONVEX_STACK_NAME ?? null);
    if (backups) return backups;
    // Maintenance (e.g. while a stack's objects are copied elsewhere, or an app release migrates its tenants): client
    // traffic (sockets, ingress, /auth, storage) gets 503. Deploying the flag resets the gateways, so no client socket
    // stays open either. Operator routes still answer, each behind its own credential (admin key, project sync key,
    // builder token): the backup routes above, /dev/* (incl. /dev/sync) and the release builder's /_gonvex/k19/*.
    if (env.GONVEX_MAINTENANCE === "1") {
      if (url.pathname.startsWith("/dev/") || url.pathname.startsWith("/_gonvex/k19/")) {
        const operator = await handleDevRequest(request, env, ctx);
        if (operator) return operator;
      }
      return Response.json({ error: "maintenance", retryable: true }, { status: 503, headers: { "retry-after": "300", "cache-control": "no-store" } });
    }
    if (url.pathname === "/ws") return handleWebSocketUpgrade(request, env, ctx);
    const transfer = await handleTenantTransfer(request, env);
    if (transfer) return transfer;
    const fixture = await fixtureControl(request, env);
    if (fixture) return fixture;
    const dev = await handleDevRequest(request, env, ctx);
    if (dev) return dev;
    if (url.pathname.startsWith("/auth/")) return env.CONTROL.getByName("control").fetch(request);
    if (url.pathname.startsWith(STORAGE_PREFIX)) return handleStorageRequest(request, {
      bucket: env.FILES, config: filesConfigFromEnv(env as unknown as Record<string, unknown>), tenant: (id) => env.TENANT.getByName(id) as unknown as FilesTenantRpc,
    });
    const ingress = await handleIngressRequest(request, env);
    return ingress ?? Response.json({ error: "not found" }, { status: 404 });
  },
  queue: (batch, env) => handleRolloutBatch(batch as MessageBatch<RolloutMessage>, env),
  // Nightly backups (deploy.mjs `backups`): Control and every tenant into BACKUPS.
  scheduled: (_controller, env, ctx) => { ctx.waitUntil(runBackup(env as unknown as BackupEnv, { stack: env.GONVEX_STACK_NAME ?? null }).then(() => undefined)); },
} satisfies ExportedHandler<ServingEnv>);
