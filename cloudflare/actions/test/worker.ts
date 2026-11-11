// Test Worker for the Action runtime (K12): a SQLite-backed tenant object that
// hosts the outbox lease protocol and the module host (K04 on K01), and the
// Action Workflow configured with the test artifact. Stand-ins, each named:
//
// - the Reducer path (K07): `enqueue` runs a real Reducer through K04 that
//   calls `ctx.actions.enqueue`, then calls `afterCommit` as K07 will;
// - the alarm (K11): `alarm()` runs only the outbox sweep;
// - control (K06): `tenantSessionForAccount` and `projectSecrets` from a table;
// - scheduler (K11) and storage (K13): recording fakes behind their ports;
// - the network: the conformance upstream answered in-process.
import { DurableObject } from "cloudflare:workers";
import { upstream, UPSTREAM_ORIGIN } from "@conformance-scenarios/actions-upstream.mjs";
import { TenantStorage } from "../../kernel/storage/index";
import { ModuleRegistry, TenantModuleHost } from "../../kernel/host/src/index.js";
import { ActionOutbox, type ActionJob, type CallFinished, type Dispatcher } from "../src/outbox.js";
import type { ClaimRequest, CompleteRequest, ControlPort, FailRequest, RenewRequest, SchedulerPort, StoragePort, TenantCallRequest } from "../src/ports.js";
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
  scheduler: () => new FakeScheduler(),
  storage: () => new FakeStorage(),
  fetcher: () => testFetcher,
});

interface TestConfig { leaseMs: number; renewMs: number; maxAttempts: number; dispatch: "workflow" | "record" | "fail" }
const DEFAULT_CONFIG: TestConfig = { leaseMs: 300_000, renewMs: 30_000, maxAttempts: 8, dispatch: "workflow" };

export class TestTenant extends DurableObject<Env> {
  outbox!: ActionOutbox;
  actions!: TenantActions;
  host!: TenantModuleHost;
  readonly recorded: ActionJob[] = [];
  readonly finished: CallFinished[] = [];
  /** K11 stand-in for handed-off runs: live claims, and the outcomes reported. */
  readonly claims = new Map<string, boolean>();
  readonly reported: Array<{ claim: HandoffClaim; outcome: HandoffRunOutcome }> = [];
  readonly instance = crypto.randomUUID();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(BOARD_SQL);
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
    const workflow = new WorkflowDispatcher(this.env.ACTION_WORKFLOW);
    const dispatcher: Dispatcher = {
      dispatch: async (job) => {
        if (config.dispatch === "fail") throw new Error("hand-off lost");
        if (config.dispatch === "record") { this.recorded.push(job); return; }
        await workflow.dispatch(job);
      },
    };
    this.outbox = new ActionOutbox({
      storage: this.ctx.storage,
      projectId: PROJECT,
      tenantId: this.tenantId(),
      dispatcher,
      wake: async (at) => {
        const current = await this.ctx.storage.getAlarm();
        if (current === null || current > at) await this.ctx.storage.setAlarm(at);
      },
      onFinished: (finished) => { this.finished.push(finished); },
      leaseMs: config.leaseMs,
      renewMs: config.renewMs,
      maxAttempts: config.maxAttempts,
    });
    this.host = new TenantModuleHost({ storage: new TenantStorage(this.ctx.storage), registry: registry() });
    this.actions = new TenantActions(this.outbox, this.host);
  }

  /** Names the tenant and sets lease timings for a test. */
  configure(tenantId: string, config: Partial<TestConfig> = {}): void {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO _k12_test (k, v) VALUES ('tenant', ?), ('config', ?)", tenantId, JSON.stringify(config));
    this.setup();
  }

  // ---- TenantActionPort (the RPCs the Action side calls) ----
  call(request: TenantCallRequest) { return this.actions.call(request); }
  claim(request: ClaimRequest) { return this.actions.claim(request); }
  renew(request: RenewRequest) { return this.actions.renew(request); }
  fail(request: FailRequest) { return this.actions.fail(request); }
  complete(request: CompleteRequest) { return this.actions.complete(request); }
  beginCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown }) { return this.actions.beginCall(request); }

  // ---- K11 stand-in: Scheduler.renew / Scheduler.complete for handed-off runs ----
  private claimKey(claim: HandoffClaim) { return claim.source === "outbox" ? `${claim.id}#${claim.attempt}` : `${claim.id}#${claim.token}`; }
  grantClaim(claim: HandoffClaim) { this.claims.set(this.claimKey(claim), true); }
  revokeClaim(claim: HandoffClaim) { this.claims.set(this.claimKey(claim), false); }
  async renewRun(request: { v: 1; claim: HandoffClaim }) { return this.claims.get(this.claimKey(request.claim)) === true; }
  async completeRun(request: { v: 1; claim: HandoffClaim; outcome: HandoffRunOutcome }) {
    if (this.claims.get(this.claimKey(request.claim)) !== true) return false;
    this.claims.set(this.claimKey(request.claim), false);
    this.reported.push({ claim: request.claim, outcome: request.outcome });
    return true;
  }
  reportedRuns() { return this.reported; }

  /** K07 stand-in: a Reducer queues the Action in its transaction; the hand-off follows the commit. */
  async enqueue(path: string, args: unknown, options: { accountId?: string; row?: string; skipHandoff?: boolean } = {}): Promise<string> {
    const session = member(this.tenantId(), options.accountId ?? "acct-alice");
    const report = await this.host.run({
      function: "queue.enqueue", kind: "reducer", args: { path, args, row: options.row ?? null },
      identity: { account: { id: session.account.id, email: session.account.email ?? null }, member: { id: session.member.id }, tenant: { id: this.tenantId(), projectId: PROJECT } },
      invocation: { channel: "ui", rootChannel: "ui", commandId: `enqueue-${crypto.randomUUID()}`, rootCommandId: "root-command" },
    });
    if (!report.ok) throw new Error(report.error?.message ?? "enqueue failed");
    if (!options.skipHandoff) await this.outbox.afterCommit();
    return (JSON.parse(report.value!) as { id: string }).id;
  }

  sweep() { return this.outbox.sweep(); }
  inspect(id: string) { return this.outbox.inspect(id); }
  cancel(id: string) { return this.outbox.cancel(id); }
  listDead() { return this.outbox.listDead(); }
  requeue(id: string) { return this.outbox.requeue(id); }
  recordedJobs() { return this.recorded; }
  finishedCalls() { return this.finished; }
  outboxStats() { return { ...this.outbox.stats }; }
  board() { return this.ctx.storage.sql.exec("SELECT id, title FROM board ORDER BY id").toArray(); }
  outboxRows() { return this.ctx.storage.sql.exec("SELECT id, action_path, status, attempts, locked_at, last_error FROM _gonvex_action_outbox ORDER BY created_at").toArray(); }
  runRows() { return this.ctx.storage.sql.exec("SELECT id, origin, state, attempts, dispatch_count, last_error FROM _gonvex_st_action_runs ORDER BY id").toArray(); }
  /** Moves `locked_at` back, as if the last persisted renewal was `ms` ago (lease-expiry tests without waiting 5 min). */
  ageLease(id: string, ms: number) { this.ctx.storage.sql.exec("UPDATE _gonvex_action_outbox SET locked_at = locked_at - ? WHERE id = ?", ms, id); this.outbox.forgetMemory(); }
  instanceId() { return this.instance; }
  crash(): void { this.ctx.abort("k12 tenant crash test"); }

  override async alarm(): Promise<void> {
    await this.outbox.sweep();
  }
}

export default {
  fetch(): Response {
    return new Response("k12 actions test worker");
  },
} satisfies ExportedHandler<Env>;
