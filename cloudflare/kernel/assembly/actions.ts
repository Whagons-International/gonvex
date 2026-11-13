// The Action script carries its own code and CPU limit; it talks to the assembled tenant.
import { ModuleRegistry } from "../host/src/index";
import { moduleArtifact, projectModuleFromManifest } from "../reducer/module";
import { configureActions, type ActionEnv } from "../../actions/src/worker";
import type { ControlPort } from "../../actions/src/ports";
import { ActionFiles, type FilesTenantRpc } from "../files/rpc";
import { filesConfigFromEnv } from "../files/config";
import type { SandboxPort } from "../../actions/src/ports";
import type { ModuleExports } from "../host/src/index";
// @ts-expect-error K19 generates the Action-only entry of the same release version.
import * as generated from "gonvex:action-releases";

interface Env extends ActionEnv {
  CONTROL: DurableObjectNamespace;
  FILES: R2Bucket;
  SANDBOX: SandboxPort;
  [key: string]: unknown;
}
const releases = (generated as { releases: Record<string, { manifest: Record<string, unknown>; exports: ModuleExports }> }).releases;
const registries = new Map<string, ModuleRegistry>();
configureActions<Env>({
  registry: (_env, projectId) => {
    let registry = registries.get(projectId);
    if (!registry) {
      const release = releases[projectId];
      if (!release) throw new Error(`project ${JSON.stringify(projectId)} has no active TypeScript module`);
      const module = projectModuleFromManifest(projectId, release.manifest);
      registry = new ModuleRegistry(projectId);
      registry.install(moduleArtifact(module, module.generation, release.exports));
      registries.set(projectId, registry);
    }
    return registry;
  },
  control: (env) => env.CONTROL.getByName("control") as unknown as ControlPort,
  storage: (env, tenantId) => {
    const files = new ActionFiles({ tenant: env.TENANT.getByName(tenantId) as unknown as FilesTenantRpc, bucket: env.FILES, config: filesConfigFromEnv(env) });
    return { call: (operation, payload, session) => files.call({ projectId: session.projectId, tenantId: session.tenantId, accountId: session.account.id, memberId: session.member.id, system: session.member.status === "system" }, operation, payload) };
  },
  sandbox: (env) => env.SANDBOX,
});
export { ActionWorker, ActionWorkflow } from "../../actions/src/worker";
export default { fetch: () => Response.json({ ok: true, service: "gonvex-actions" }) };
