// Local-only Worker for long Action runs (see wrangler.jsonc and README.md).
// It reuses the test tenant object and Workflow, with workerd's real fetch, so
// an Action's requests leave the isolate as they would on the platform.
import { configureActions, type ActionEnv } from "../src/worker.js";
import { instanceId } from "../src/tenant.js";
import { FakeScheduler, FakeStorage, registry, testControl } from "../test/worker.js";

export { ActionWorkflow } from "../src/worker.js";
export { TestTenant } from "../test/worker.js";

configureActions<ActionEnv>({
  registry,
  control: () => testControl,
  scheduler: () => new FakeScheduler(),
  storage: () => new FakeStorage(),
});

const json = (value: unknown, status = 200) => Response.json(value, { status });

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    const tenantId = url.searchParams.get("tenant") ?? "dev-tenant";
    const tenant = env.TENANT.getByName(tenantId);
    try {
      if (request.method === "POST" && url.pathname === "/enqueue") {
        const body = (await request.json()) as { path: string; args: unknown; lease?: { leaseMs: number; renewMs: number } };
        await tenant.configure(tenantId, body.lease ?? {});
        const id = await tenant.enqueue(body.path, body.args);
        return json({ id, instance: instanceId({ id, n: 1 }) });
      }
      if (request.method === "GET" && url.pathname === "/run") {
        const id = url.searchParams.get("id")!;
        return json({ row: await tenant.inspect(id), runs: await tenant.runRows(), outbox: await tenant.outboxRows(), stats: await tenant.outboxStats() });
      }
      if (request.method === "GET" && url.pathname === "/workflow") {
        const instance = await env.ACTION_WORKFLOW.get(url.searchParams.get("instance")!);
        return json(await instance.status());
      }
      return json({ error: "not found" }, 404);
    } catch (error) {
      return json({ error: String((error as Error)?.stack ?? error) }, 500);
    }
  },
} satisfies ExportedHandler<Env>;
