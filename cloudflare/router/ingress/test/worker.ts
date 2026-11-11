/**
 * Test Worker for the Ingress Worker (K18). Never deployed as such; the name
 * keeps the gonvex-cf-dev- prefix anyway.
 *
 * - The router: `handleIngressRequest`.
 * - K06's control object (tenant resolution through `authenticate`), with K06's
 *   stub tenant object behind it, and one test-only method to set a tenant's
 *   domain and status.
 * - `IngressObject`.
 * - `TestActions`: the `serviceAction` port K12/K17 will implement, here a
 *   K04 `ModuleEngine` over either the fixture module (project
 *   `proj-fixture`) or the real Whagons bundle (project `proj-whagons`, only
 *   when `whagons/prepare.mjs` built it). Tool calls are answered by stubs.
 *   Every call is recorded in `Recorder`.
 */
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { ControlObject, type ControlEnv } from "../../../control/src/control-object.ts";
import { StubTenantObject } from "../../../control/test-support/stub-tenant.ts";
import { ModuleEngine, type FunctionContract, type ModuleHost } from "../../../kernel/host/src/index.js";
import type { HostPayload } from "../../../kernel/host/src/context.js";
import { IngressObject } from "../object.ts";
import { handleIngressRequest, type IngressEnv } from "../routes.ts";
import { serviceMemberId, type ServiceActionReport, type ServiceActionRequest } from "../invoke.ts";
import * as fixture from "./fixtures/webhooks.module.js";
// @ts-expect-error resolved by the vitest alias (the Whagons bundle, or an empty stand-in).
import * as whagonsApp from "gonvex:whagons-app";
// @ts-expect-error resolved by the vitest alias.
import whagonsFunctions from "gonvex:whagons-functions";

export { IngressObject, StubTenantObject };

export class TestControlObject extends ControlObject {
  testTenant(projectId: string, tenantId: string, patch: { domain?: string | null; status?: string }): void {
    if (patch.domain !== undefined) this.sql.run("UPDATE gonvex_runtime_tenants SET domain=$3 WHERE project_id=$1 AND tenant_id=$2", projectId, tenantId, patch.domain);
    if (patch.status !== undefined) this.sql.run("UPDATE gonvex_runtime_tenants SET status=$3 WHERE project_id=$1 AND tenant_id=$2", projectId, tenantId, patch.status);
  }
}

export interface RecordedCall {
  request: ServiceActionRequest;
  identity: { member: string; role: string; tenant: string };
  tools: { tool: string; args: unknown }[];
  report: ServiceActionReport;
}

/** Calls and per-project secrets, shared by the test and the Worker. */
export class Recorder extends DurableObject {
  private calls: RecordedCall[] = [];
  private secrets: Record<string, Record<string, string>> = {};
  private seal: Record<string, unknown> = {};

  push(call: RecordedCall): void { this.calls.push(call); }
  list(): RecordedCall[] { return this.calls; }
  clear(): void { this.calls = []; }
  setSecrets(project: string, values: Record<string, string>): void { this.secrets[project] = values; }
  secretsOf(project: string): Record<string, string> { return this.secrets[project] ?? {}; }
  setValue(name: string, value: unknown): void { this.seal[name] = value; }
  value(name: string): unknown { return this.seal[name]; }
}

interface TestEnv extends IngressEnv, ControlEnv {
  RECORDER: DurableObjectNamespace<Recorder>;
}

const any = { kind: "any" };
const strings = (...names: string[]) => ({ kind: "object", fields: Object.fromEntries(names.map((name) => [name, { kind: "string" }])) });

/** The fixture's contracts: the Whagons argument schemas and declared capabilities. */
const fixtureContracts: FunctionContract[] = [
  { path: "calling.ingestWebhook", kind: "action", export: "calling_ingestWebhook", argsSchema: strings("rawBody", "timestamp", "signature"), resultSchema: any, metadata: { actionProfile: "agent", actionCapabilities: { secrets: ["TELNYX_PUBLIC_KEY"] } } },
  { path: "relay.ingestTelnyxWebhook", kind: "action", export: "relay_ingestTelnyxWebhook", argsSchema: strings("rawBody", "signature", "timestamp"), resultSchema: any, metadata: { actionProfile: "agent", actionCapabilities: { secrets: ["TELNYX_PUBLIC_KEY"] } } },
  { path: "github.ingestWebhook", kind: "action", export: "github_ingestWebhook", argsSchema: strings("event", "rawBody", "signature"), resultSchema: any, metadata: { actionProfile: "agent", actionCapabilities: { secrets: ["GITHUB_WEBHOOK_SECRET"] } } },
  { path: "taskPublicShares.getPublicPreviewByToken", kind: "action", export: "taskPublicShares_getPublicPreviewByToken", argsSchema: strings("token"), resultSchema: any, metadata: { actionProfile: "agent", actionCapabilities: { tools: { resolvePublicPreview: { kind: "reducer", function: "taskPublicShares.resolvePublicPreview" } } } } },
];

/** The Whagons contracts (`functions.json` of the S2 artifact) and their exports (one namespace per file). */
function whagonsEngine(): ModuleEngine | null {
  const functions = whagonsFunctions as Record<string, { kind: "action"; export: string; file: string; internal?: boolean; args: unknown; result: unknown; actionCapabilities?: unknown }>;
  if (!functions || !Object.keys(functions).length) return null;
  const app = whagonsApp as Record<string, Record<string, unknown>>;
  const contracts: FunctionContract[] = [];
  const exports: Record<string, unknown> = {};
  for (const [path, entry] of Object.entries(functions)) {
    contracts.push({ path, kind: entry.kind, internal: entry.internal ?? false, argsSchema: entry.args, resultSchema: entry.result, metadata: { actionProfile: "agent", actionCapabilities: entry.actionCapabilities } });
    exports[path] = app[path.split(".")[0]!]?.[entry.export];
  }
  return new ModuleEngine(contracts, exports);
}

let engines: { fixture: ModuleEngine; whagons: ModuleEngine | null } | undefined;

function engineFor(project: string): ModuleEngine | null {
  engines ??= { fixture: new ModuleEngine(fixtureContracts, fixture as Record<string, unknown>), whagons: whagonsEngine() };
  if (project === "proj-fixture") return engines.fixture;
  if (project === "proj-whagons") return engines.whagons;
  return null;
}

export const SHARE_PREVIEW = {
  token: "share-ok",
  taskId: "task_1",
  taskNumber: 42,
  title: "Fix the <lobby> door & \"lock\"",
  description: "The lock sticks when it's cold. <script>alert(1)</script>",
  workspaceName: "Front desk",
  statusName: "Open",
  priorityName: "High",
  spotName: "Lobby",
  creatorName: "Ana",
  assigneeCount: 2,
  dueDate: Date.UTC(2026, 10, 20),
  createdAt: Date.UTC(2026, 10, 1),
};

/** Tool answers: the internal Reducers the Actions call, as recorded stand-ins. */
async function tool(env: TestEnv, request: ServiceActionRequest, name: string, args: unknown): Promise<unknown> {
  const input = (args ?? {}) as Record<string, unknown>;
  switch (name) {
    case "resolvePublicPreview":
      return input.token === "share-ok" ? { ...SHARE_PREVIEW, tenantId: request.principal.tenantId } : null;
    case "control": // calling.control
      if (input.op === "forwarding-event") return { handled: false };
      return { id: "call_1" };
    case "recordWebhookEvent": // relay.recordWebhookEvent
      return { webhookEventId: "telnyx-event", callId: null, duplicate: false };
    case "webhookConnection": // github.webhookConnection
      return env.RECORDER.getByName(request.principal.projectId).value("githubConnection");
    case "webhookTargets":
      return { numbers: [] };
    case "webhookApply":
      return { linked: 0, refreshed: 0 };
    default:
      throw new Error(`no stub for tool ${name}`);
  }
}

export class TestActions extends WorkerEntrypoint<TestEnv> {
  async serviceAction(request: ServiceActionRequest): Promise<ServiceActionReport> {
    const recorder = this.env.RECORDER.getByName(request.principal.projectId);
    const tools: RecordedCall["tools"] = [];
    const member = serviceMemberId(request.principal.id);
    const identity = { member, role: "system", tenant: request.principal.tenantId };
    let report: ServiceActionReport;
    try {
      // K17's check, repeated on the tenant side.
      if (!request.principal.functions.includes(request.path)) throw new Error(`function ${JSON.stringify(request.path)} is not allowed for this service principal`);
      const engine = engineFor(request.principal.projectId);
      if (!engine) throw new Error(`project ${request.principal.projectId} has no module`);
      const contract = engine.functions.get(request.path);
      const declared = ((contract?.metadata?.["actionCapabilities"] ?? {}) as { secrets?: string[]; tools?: Record<string, unknown> });
      const secrets = await recorder.secretsOf(request.principal.projectId);
      const environment = Object.fromEntries((declared.secrets ?? []).filter((name) => name in secrets).map((name) => [name, secrets[name]!]));
      const host: ModuleHost = {
        call: async (payload: HostPayload) => {
          if (payload.kind !== "toolInvoke") throw new Error(`the test host has no ${payload.kind}`);
          const name = String(payload["tool"]);
          tools.push({ tool: name, args: payload["args"] });
          return tool(this.env, request, name, payload["args"]);
        },
      };
      const result = await engine.invoke(host, {
        function: request.path,
        kind: "action",
        args: request.args,
        context: {
          nowUnixMs: 0,
          identity: {
            account: { id: member, email: null, name: null, avatarUrl: null },
            member: { id: member, accountId: member, status: "active", role: "system", displayName: null, permissions: {} },
            tenant: { id: request.principal.tenantId, projectId: request.principal.projectId, name: null },
          },
          invocation: { channel: request.channel, rootChannel: request.channel, commandId: request.commandId, rootCommandId: request.commandId, actorAccountId: member, actorMemberId: member },
          capabilities: { actionTools: true, secrets: true, network: false },
          actionTools: Object.keys(declared.tools ?? {}),
          environment,
        },
      });
      report = { ok: true, value: JSON.parse(result.value) };
    } catch (error) {
      report = { ok: false, error: String((error as Error)?.message ?? error) };
    }
    await recorder.push({ request, identity, tools, report });
    return report;
  }
}

export default {
  async fetch(request: Request, env: TestEnv): Promise<Response> {
    return (await handleIngressRequest(request, env)) ?? new Response(null, { status: 404 });
  },
} satisfies ExportedHandler<TestEnv>;
