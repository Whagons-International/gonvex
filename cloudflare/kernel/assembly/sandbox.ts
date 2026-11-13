// K20 serving implementation: unchanged Rust/DuckDB per-tenant Container, no L09 imports.
import { WorkerEntrypoint } from "cloudflare:workers";
import { sandboxFromEnv, type SandboxEnv } from "../../sandbox/src/index";
import { ActionFiles, type FilesTenantRpc } from "../files/rpc";
import { filesConfigFromEnv } from "../files/config";
import { filesSession } from "../../sandbox/src/service";
import type { ActionSession } from "../../actions/src/session";
interface Env extends SandboxEnv {
  TENANT: DurableObjectNamespace;
  FILES: R2Bucket;
  [key: string]: unknown;
}
export { SandboxContainer, SandboxRegistry } from "../../sandbox/src/index";
export class SandboxWorker extends WorkerEntrypoint<Env> {
  async call(operation: string, payload: unknown, session: ActionSession, duckdb: boolean) {
    const files = new ActionFiles({ tenant: this.env.TENANT.getByName(session.tenantId) as unknown as FilesTenantRpc, bucket: this.env.FILES, config: filesConfigFromEnv(this.env) });
    return sandboxFromEnv(this.env, { readFile: (caller, id, limit) => files.readFile(caller, id, limit) }).call(operation, payload, session, duckdb);
  }
}
export default { fetch: () => Response.json({ ok: true, service: "gonvex-sandbox" }) };
