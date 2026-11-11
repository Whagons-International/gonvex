// S4 runner Worker. One script holds the tenant object, the three runners
// and a small HTTP API the scenario driver uses.
//   Runner A: Queue consumer           (`queue` below)
//   Runner B: Workflow step            (`ActionWorkflow`)
//   Runner C: Durable Object alarm     (`RunnerObject`)
import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import { runAction, type RunOutcome } from "./action";
import { tenantStub, type Env, type Job, type Runner } from "./env";

export { TenantObject } from "./tenant";
export { RunnerObject } from "./runner-object";

export class ActionWorkflow extends WorkflowEntrypoint<Env, Job> {
  override async run(event: Readonly<WorkflowEvent<Job>>, step: WorkflowStep) {
    const owner = `workflow:${event.instanceId}`;
    const outcome = (await step.do(
      "run action",
      { retries: { limit: 10, delay: "5 seconds", backoff: "exponential" }, timeout: "6 hours" },
      async () => {
        const outcome = await runAction(this.env, event.payload, owner);
        // Throwing hands the retry to the Workflow engine. The same instance
        // retries, so the same owner takes the lease over at once.
        if (outcome.kind === "failed") throw new Error(outcome.error);
        if (outcome.kind === "skipped" && outcome.reason === "leased") {
          throw new Error(`leased by another owner for ${outcome.retryAfterMs} ms`);
        }
        return JSON.parse(JSON.stringify(outcome));
      },
    )) as RunOutcome;
    if (outcome.kind !== "done") return { outcome: outcome.kind };
    const applied = await step.do("apply result", async () =>
      tenantStub(this.env, event.payload.tenant).complete(event.payload.id, outcome.attempt, outcome.result),
    );
    return { outcome: outcome.kind, attempt: outcome.attempt, applied };
  }
}

const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value, null, 1), { status, headers: { "content-type": "application/json" } });

export default {
  async queue(batch: MessageBatch<Job>, env: Env) {
    for (const message of batch.messages) {
      const outcome = await runAction(env, message.body, `queue:${message.id}`);
      if (outcome.kind === "done") {
        await tenantStub(env, message.body.tenant).complete(message.body.id, outcome.attempt, outcome.result);
        message.ack();
      } else if (outcome.kind === "skipped" && outcome.reason === "leased") {
        message.retry({ delaySeconds: Math.min(Math.ceil((outcome.retryAfterMs ?? 30_000) / 1000) + 1, 43_200) });
      } else if (outcome.kind === "failed") {
        message.retry({ delaySeconds: Math.min(2 ** outcome.attempt, 60) });
      } else {
        message.ack();
      }
    }
  },

  async fetch(request: Request, env: Env): Promise<Response> {
    if (env.S4_TOKEN && request.headers.get("authorization") !== `Bearer ${env.S4_TOKEN}`) {
      return json({ error: "unauthorized" }, 401);
    }
    const url = new URL(request.url);
    const tenant = url.searchParams.get("tenant") ?? "tenant-s4";
    const parts = url.pathname.split("/").filter(Boolean);
    const stub = tenantStub(env, tenant);
    try {
      if (request.method === "POST" && url.pathname === "/actions") {
        const body = (await request.json()) as { runner: Runner; spec: unknown; skipHandoff?: boolean };
        return json(await stub.enqueue(tenant, body.runner, "agents:run", body.spec, body.skipHandoff));
      }
      if (parts[0] === "actions" && parts[1]) {
        const id = parts[1];
        if (request.method === "GET" && parts.length === 2) return json(await stub.inspect(id));
        if (request.method === "POST" && parts[2] === "complete") {
          const body = (await request.json()) as { attempt: number; result: unknown };
          return json(await stub.complete(id, body.attempt, body.result));
        }
        if (request.method === "POST" && parts[2] === "redispatch") return json(await stub.redispatch(id));
        if (request.method === "POST" && parts[2] === "cancel") return json(await stub.cancel(id));
        if (parts[2] === "runner") {
          const runner = env.RUNNER.get(env.RUNNER.idFromName(`${tenant}/${id}`));
          if (request.method === "POST" && parts[3] === "kill") {
            await runner.kill().catch(() => undefined);
            return json({ killed: true });
          }
          return json(await runner.state());
        }
      }
      if (request.method === "POST" && url.pathname === "/tenant/crash") {
        await stub.crash().catch(() => undefined);
        return json({ crashed: true });
      }
      if (parts[0] === "workflow" && parts[1]) {
        const instance = await env.ACTION_WF.get(parts[1]);
        if (request.method === "POST" && parts[2] === "terminate") {
          await instance.terminate();
          return json({ terminated: true });
        }
        return json(await instance.status());
      }
      return json({ error: "not found" }, 404);
    } catch (error) {
      return json({ error: String(error) }, 500);
    }
  },
} satisfies ExportedHandler<Env, Job>;
