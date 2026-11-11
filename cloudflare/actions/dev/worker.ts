// Local-only Worker for long Action runs (see wrangler.jsonc and README.md).
// It reuses the test tenant object (K11's scheduler with K12's WorkflowHandoff)
// and the Workflow, with workerd's real fetch, so an Action's requests leave
// the isolate as they would on the platform. A queued Action goes the way every
// outbox Action goes: K11 claims it on the alarm and hands it to the Workflow.
import { configureActions, type ActionEnv } from "../src/worker.js";
import { handoffInstanceId } from "../src/handoff.js";
import { FakeStorage, registry, testControl } from "../test/worker.js";

export { ActionWorkflow } from "../src/worker.js";
export { TestTenant } from "../test/worker.js";

configureActions<ActionEnv>({
  registry,
  control: () => testControl,
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
        const body = (await request.json()) as { path: string; args: unknown };
        await tenant.configure(tenantId, {});
        const id = await tenant.enqueue(body.path, body.args);
        return json({ id, instance: handoffInstanceId({ id, claim: { source: "outbox", id, attempt: 1, leaseMs: 300_000 } }) });
      }
      if (request.method === "GET" && url.pathname === "/run") {
        const id = url.searchParams.get("id")!;
        return json({ id, outbox: await tenant.outboxRows(), dead: await tenant.deadRows(), events: await tenant.schedulerEvents() });
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
