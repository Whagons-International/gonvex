// Test Worker for the Action runtime (K12): a SQLite-backed tenant object
// composed as the kernel's tenant object will be (K19 wires K07's object):
//
// - K11's real `Scheduler` on a real `TenantAlarm` (the object's alarm), with
//   K12's `WorkflowHandoff` as its `actions` port: outbox Actions are claimed by
//   K11 and run in K12's Workflow;
// - K12's `ActionCalls` for direct agent calls, its sweep registered on the same alarm;
// - K04's module host on K01 for tools, `ctx.functions` and the queuing Reducer.
//
// Stand-ins, each named: the Reducer path (K07): `enqueue` runs a real Reducer
// through K04 that calls `ctx.actions.enqueue`, then `scheduler.notify()` as K07
// will; control (K06): sessions and secrets from a table; storage (K13): a
// recording fake; the network: the conformance upstream answered in-process.
import { DurableObject } from "cloudflare:workers";
import { upstream, UPSTREAM_ORIGIN } from "@conformance-scenarios/actions-upstream.mjs";
import { TenantStorage } from "../../kernel/storage/index";
import { ModuleRegistry, TenantModuleHost } from "../../kernel/host/src/index.js";
import { SYNC_INFRASTRUCTURE_SQL } from "../../kernel/capture/index.ts";
import { Scheduler, storageAlarm, TenantAlarm, type ActionHandoff, type ActiveModule, type DurableRun as K11Run, type ReducerRunner, type SchedulerEvent } from "../../kernel/scheduler/index.ts";
import { ActionCalls, type CallFinished, type CallJob, type Dispatcher } from "../src/calls.js";
import { WorkflowHandoff, type HandoffJob } from "../src/handoff.js";
import type { ClaimRequest, CompleteRequest, ControlPort, RenewRequest, ScheduleRequest, SchedulerPort, StoragePort, TenantCallRequest } from "../src/ports.js";
import type { ActionSession } from "../src/session.js";
import { TenantActions, WorkflowDispatcher } from "../src/tenant.js";
import type { HandoffClaim, HandoffRunOutcome } from "../src/handoff.js";
import { configureActions, type ActionEnv } from "../src/worker.js";
import { BOARD_SQL, fixtureArtifact, PROJECT } from "./fixture.js";

export { ActionWorkflow } from "../src/worker.js";

/** Members the control stand-in admits. */
export const MEMBERS: Record<string, ActionSession> = {
  "acct-alice": {
    projectId: PROJECT, tenantId: "", account: { id: "acct-alice", email: "alice@example.com", name: "Alice" },
    member: { id: "mem-alice", accountId: "acct-alice", status: "active", role: "member", displayName: "Alice", permissions: {} },
  },
};

export function member(tenantId: string, accountId = "acct-alice"): ActionSession {
  const session = MEMBERS[accountId];
  if (!session) throw new Error("invalid session");
  return { ...session, tenantId };
}

export const testControl: ControlPort = {
  async tenantSessionForAccount(_project, tenantId, accountId) { return member(tenantId, accountId); },
  async projectSecrets(_project, names) { return Object.fromEntries(names.filter((name) => name === "K12_SECRET").map((name) => [name, "s3cr3t"])); },
};

/** Recording K11 port: wakes coalesce per (function, args), as `wakeAt` does. */
export class FakeScheduler implements SchedulerPort {
  readonly jobs: Array<{ id: string; function: string; args: unknown; runAtUnixMs: number; coalesce: boolean; provenance: Record<string, unknown> }> = [];
  async enqueue(request: Parameters<SchedulerPort["enqueue"]>[0]): Promise<string> {
    if (request.coalesce) {
      const key = JSON.stringify([request.function, request.args]);
      const existing = this.jobs.find((job) => job.coalesce && JSON.stringify([job.function, job.args]) === key);
      if (existing) { existing.runAtUnixMs = Math.min(existing.runAtUnixMs, request.runAtUnixMs); return existing.id; }
    }
    this.jobs.push({ id: request.id, function: request.function, args: request.args, runAtUnixMs: request.runAtUnixMs, coalesce: request.coalesce, provenance: request.provenance });
    return request.id;
  }
}

/** Recording K13 port: a private file of someone else answers Rust's "file not found" (IB1). */
export class FakeStorage implements StoragePort {
  readonly calls: Array<{ operation: string; payload: unknown; account: string }> = [];
  async call(operation: string, payload: unknown, session: ActionSession): Promise<unknown> {
    this.calls.push({ operation, payload, account: session.account.id });
    const fileId = (payload as { fileId?: string } | null)?.fileId;
    if (operation === "getMetadata" && fileId === "private-file") throw new Error("file not found");
    if (operation === "getMetadata") return { id: fileId, ownerId: session.account.id, visibility: "tenant", status: "uploaded" };
    if (operation === "store") return { id: "stored-1", sizeBytes: 5, contentType: (payload as { contentType?: string }).contentType };
    throw new Error(`unknown storage operation ${JSON.stringify(operation)}`);
  }
}

/** The upstream origin answered in-process; anything else fails like an unreachable host. */
export async function testFetcher(request: Request): Promise<Response> {
  if (new URL(request.url).origin === UPSTREAM_ORIGIN) return upstream(request);
  throw new TypeError("Network connection lost.");
}

let sharedRegistry: ModuleRegistry | undefined;
export function registry(): ModuleRegistry {
  if (!sharedRegistry) {
    sharedRegistry = new ModuleRegistry(PROJECT);
    sharedRegistry.install(fixtureArtifact());
  }
  return sharedRegistry;
}

configureActions<ActionEnv>({
  registry,
  control: () => testControl,
  storage: () => new FakeStorage(),
  fetcher: () => testFetcher,
});

interface TestConfig {
  /** Direct calls (ActionCalls). */
  leaseMs: number;
  renewMs: number;
  maxAttempts: number;
  dispatch: "workflow" | "record" | "fail";
  /** K11's outbox. */
  outboxLeaseMs: number;
  outboxMaxAttempts: number;
  handoff: "workflow" | "record" | "fail";
}
const DEFAULT_CONFIG: TestConfig = { leaseMs: 300_000, renewMs: 30_000, maxAttempts: 8, dispatch: "workflow", outboxLeaseMs: 300_000, outboxMaxAttempts: 8, handoff: "workflow" };

export class TestTenant extends DurableObject<Env> {
  calls!: ActionCalls;
  scheduler!: Scheduler;
  alarmClock!: TenantAlarm;
  actions!: TenantActions;
  host!: TenantModuleHost;
  /** Direct-call hand-offs (dispatch "record") and K11 hand-offs (handoff "record"). */
  readonly recorded: CallJob[] = [];
  readonly handedOff: K11Run[] = [];
  readonly finished: CallFinished[] = [];
  readonly events: SchedulerEvent[] = [];
  readonly instance = crypto.randomUUID();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(BOARD_SQL);
    // The runtime tables every tenant has (K03), `_gonvex_action_outbox` among them.
    ctx.storage.transactionSync(() => { ctx.storage.sql.exec(SYNC_INFRASTRUCTURE_SQL); });
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS _k12_test (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
    this.setup();
  }

  private config(): TestConfig {
    const row = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM _k12_test WHERE k = 'config'").toArray()[0];
    return row ? { ...DEFAULT_CONFIG, ...(JSON.parse(row.v) as Partial<TestConfig>) } : DEFAULT_CONFIG;
  }

  private tenantId(): string {
    return this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM _k12_test WHERE k = 'tenant'").toArray()[0]?.v ?? "unnamed";
  }

  private setup(): void {
    const config = this.config();
    const tenantId = this.tenantId();
    const callWorkflow = new WorkflowDispatcher(this.env.ACTION_WORKFLOW as unknown as Workflow<CallJob>);
    const dispatcher: Dispatcher = {
      dispatch: async (job) => {
        if (config.dispatch === "fail") throw new Error("hand-off lost");
        if (config.dispatch === "record") { this.recorded.push(job); return; }
        await callWorkflow.dispatch(job);
      },
    };
    const workflowHandoff = new WorkflowHandoff(this.env.ACTION_WORKFLOW as unknown as Workflow<HandoffJob>, PROJECT, tenantId);
    const handoff: ActionHandoff = {
      startAction: async (run) => {
        if (config.handoff === "fail") throw new Error("hand-off lost");
        if (config.handoff === "record") { this.handedOff.push(run); return { accepted: true }; }
        return workflowHandoff.startAction(run as never);
      },
    };
    this.host = new TenantModuleHost({ storage: new TenantStorage(this.ctx.storage), registry: registry() });
    const host = this.host;
    const generation = () => registry().activeGeneration!;
    const module: ActiveModule = {
      kindOf: (path) => generation().engine.functions.get(path)?.kind,
      crons: () => [],
      artifactHash: () => generation().artifact.artifactHash,
    };
    const reducers: ReducerRunner = {
      async runReducer(run) {
        const report = await host.run({ function: run.function, kind: "reducer", args: run.args, identity: { tenant: { id: tenantId, projectId: PROJECT } }, invocation: run.provenance as unknown as Record<string, unknown>, lane: "scheduled" });
        return report.ok ? { ok: true } : { ok: false, error: report.error?.message ?? "failed" };
      },
    };
    this.alarmClock = new TenantAlarm(storageAlarm(this.ctx.storage), { onError: () => {} });
    this.scheduler = new Scheduler({
      storage: this.ctx.storage, project: PROJECT, tenant: tenantId, module, reducers, actions: handoff,
      outboxLeaseMs: config.outboxLeaseMs, maxAttempts: config.outboxMaxAttempts, log: (event) => this.events.push(event),
    });
    this.scheduler.attach(this.alarmClock);
    this.calls = new ActionCalls({
      storage: this.ctx.storage, projectId: PROJECT, tenantId, dispatcher,
      wake: async () => { await this.alarmClock.arm(); },
      onFinished: (finished) => { this.finished.push(finished); },
      leaseMs: config.leaseMs, renewMs: config.renewMs, maxAttempts: config.maxAttempts,
    });
    this.alarmClock.register(this.calls.alarmSource());
    this.actions = new TenantActions(this.calls, this.host, this.scheduler);
  }

  /** Names the tenant and sets timings for a test. */
  configure(tenantId: string, config: Partial<TestConfig> = {}): void {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO _k12_test (k, v) VALUES ('tenant', ?), ('config', ?)", tenantId, JSON.stringify(config));
    this.setup();
  }

  // ---- the RPCs the Action side calls (TenantActions) ----
  call(request: TenantCallRequest) { return this.actions.call(request); }
  claimCall(request: ClaimRequest) { return this.actions.claimCall(request); }
  renewCall(request: RenewRequest) { return this.actions.renewCall(request); }
  completeCall(request: CompleteRequest) { return this.actions.completeCall(request); }
  beginCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown }) { return this.actions.beginCall(request); }
  renewRun(request: Parameters<TenantActions["renewRun"]>[0]) { return this.actions.renewRun(request); }
  completeRun(request: Parameters<TenantActions["completeRun"]>[0]) { return this.actions.completeRun(request); }
  schedule(request: ScheduleRequest) { return this.actions.schedule(request); }

  /** K07 stand-in: a Reducer queues the Action in its transaction; K11 is woken after the commit. */
  async enqueue(path: string, args: unknown, options: { accountId?: string; row?: string; skipWake?: boolean } = {}): Promise<string> {
    const session = member(this.tenantId(), options.accountId ?? "acct-alice");
    const report = await this.host.run({
      function: "queue.enqueue", kind: "reducer", args: { path, args, row: options.row ?? null },
      identity: { account: { id: session.account.id, email: session.account.email ?? null }, member: { id: session.member.id }, tenant: { id: this.tenantId(), projectId: PROJECT } },
      invocation: { channel: "ui", rootChannel: "ui", commandId: `enqueue-${crypto.randomUUID()}`, rootCommandId: "root-command", artifactHash: "a" },
    });
    if (!report.ok) throw new Error(report.error?.message ?? "enqueue failed");
    if (!options.skipWake) await this.scheduler.notify();
    return (JSON.parse(report.value!) as { id: string }).id;
  }

  /** Fires the alarm now (what the platform does at the alarm time). */
  async fire() { return this.alarmClock.fire(); }
  inspectCall(id: string) { return this.calls.inspect(id); }
  cancelCall(id: string) { return this.calls.cancel(id); }
  listDead() { return this.scheduler.listDead(); }
  requeueDead(id: string) { return this.scheduler.requeueDead(id); }
  recordedCalls() { return this.recorded; }
  handedOffRuns() { return this.handedOff; }
  finishedCalls() { return this.finished; }
  schedulerEvents() { return this.events.map((event) => ({ ...event })); }
  callStats() { return { ...this.calls.stats }; }
  board() { return this.ctx.storage.sql.exec("SELECT id, title FROM board ORDER BY id").toArray(); }
  outboxRows() { return this.ctx.storage.sql.exec("SELECT id, action_path, status, attempts, last_error FROM _gonvex_action_outbox ORDER BY created_at").toArray(); }
  deadRows() { return this.ctx.storage.sql.exec("SELECT id, action_path, attempts, last_error FROM _gonvex_action_dead ORDER BY id").toArray(); }
  jobRows() { return this.ctx.storage.sql.exec("SELECT id, function_path, status FROM gonvex_scheduled_jobs ORDER BY id").toArray(); }
  runRows() { return this.ctx.storage.sql.exec("SELECT id, state, status, attempts, dispatch_count, last_error FROM _gonvex_st_action_runs ORDER BY id").toArray(); }
  instanceId() { return this.instance; }
  crash(): void { this.ctx.abort("k12 tenant crash test"); }

  override async alarm(): Promise<void> {
    await this.alarmClock.fire();
  }
}

export default {
  fetch(): Response {
    return new Response("k12 actions test worker");
  },
} satisfies ExportedHandler<Env>;
