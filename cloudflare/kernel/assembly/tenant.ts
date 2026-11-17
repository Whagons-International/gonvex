/// <reference path="../host/src/shims.d.ts" />
import type { TenantCall } from "../host/src/invoke";
import type { ModuleExports } from "../host/src/index";
import { validateValue } from "../host/src/index";
import { ReplicaTenantObject } from "../delivery/object";
import { R2PlanStore, type PlanStore } from "../migrate/plan";
import { QueryEngine, moduleFromManifest, snapshotRunner, kernelVisibility, type Manifest, type LiveQueries, type QueryModule } from "../query/index";
import { MODULE_KEY, type StoredModule } from "../reducer/tenant";
import { RECEIPT_PRUNE_KEY } from "../reducer/tenant";
import { RECEIPT_PRUNE_INTERVAL_MS } from "../reducer/receipts";
import { pruneChangeLog, PRUNE_INTERVAL_MS } from "../capture/index";
import type { CommittedCall } from "../reducer/path";
import { RECEIPT_QUERY } from "../reducer/receipts";
import { identityOf, invocationInfoOf } from "../reducer/path";
import type { ClientFrame } from "../session/decode";
import type { SocketView, TenantSession } from "../session/ports";
import { rpc, type GatewayRpc } from "../session/rpc";
import { TenantPresence } from "../presence/tenant";
import { presenceConfig, type ConnectionReport } from "../presence/hub";
import { TenantFiles } from "../files/tenant-files";
import { filesRpc, type FilesTenantRpc } from "../files/rpc";
import { filesConfigFromEnv } from "../files/config";
import { reducerFiles } from "../files/reducer";
import { TenantOperator } from "../../control/operator/tenant/operator";
import type { OperatorDataSink, RowsQuery, ServedModule } from "../../control/operator/tenant/port";
import { TenantMemberships } from "./memberships";
import type { AssemblyGatewayEnv } from "./gateway";
import type { BakedRelease } from "./releases";
// @ts-expect-error K19 build alias; the bundle ships in this Worker version.
import * as generated from "gonvex:releases";

import { Scheduler, TenantAlarm, storageAlarm, storageDueWhen, parseCronSpecs, type ActiveModule, type DurableRun } from "../scheduler/index";
import { ActionCalls, type CallJob, type CallFinished } from "../../actions/src/calls";
import { WorkflowDispatcher } from "../../actions/src/tenant";
import { WorkflowHandoff, type HandoffJob } from "../../actions/src/handoff";
import type { TenantActionPort, TenantCallRequest, ClaimRequest, RenewRequest, CompleteRequest, ScheduleRequest } from "../../actions/src/ports";
import type { ActionSession } from "../../actions/src/session";
import { identityOf as actionIdentity, isAnonymous, systemSession } from "../../actions/src/session";
import type { DirectCall } from "../../actions/src/worker";
import type { ActionReport } from "../../actions/src/runtime";
import { directProvenance, publicInfo, persisted } from "../../actions/src/provenance";
import { actionSession } from "./action-session";
import { frames, echoTrace } from "../session/protocol";
import type { MessageTrace } from "../session/decode";
import { serviceSession, type ServiceGrant } from "../principals/grant";
import type { ServiceTenantCall, ServiceTenantOutcome } from "../principals/frames";
import { executeQuery } from "../query/call";
import { replicaModule } from "../delivery/definition";
import { collectionQuery } from "./replica-query";
import { flushReferences } from "../reducer/reference/integrity";
import { overrunMarkerKey } from "./deadline";
import { writeAdmissionError } from "../reducer/admission";
import { DeliveryAdmissionError } from "../delivery/authority";
import { retryable } from "../reducer/errors";

const releases = (generated as { releases: Record<string, BakedRelease> }).releases;
const PROJECT_KEY = "assembly/project";
export interface AssemblyTenantEnv extends AssemblyGatewayEnv {
  RELEASES: R2Bucket;
  FILES: R2Bucket;
  ACTIONS: { call(input: DirectCall): Promise<ActionReport | { pending: true; id: string }> };
  ACTION_WORKFLOW: Workflow<CallJob | HandoffJob>;
  [key: string]: unknown;
}

export class AssemblyTenant extends ReplicaTenantObject<AssemblyTenantEnv> {
  readonly queries: QueryEngine;
  private scheduler: Scheduler | undefined;
  private calls: ActionCalls | undefined;
  private alarms: TenantAlarm | undefined;
  readonly files: TenantFiles;
  private readonly filePort: FilesTenantRpc;
  private readonly presence: TenantPresence;
  private readonly operator: TenantOperator;
  private readonly memberships: TenantMemberships;
  private readonly openedAt = Date.now();
  private membershipRetry: number | null = null;
  private receiptRetry: number | null = null;
  private logRetry: number | null = null;
  private queryModule: { hash: string; module: QueryModule } | undefined;
  private fixtureRecoveryFailure=false;

  protected bundle(hash: string): ModuleExports | undefined { return Object.values(releases).find((release) => release.hash === hash)?.exports; }
  protected plans(): PlanStore { return new R2PlanStore(this.env.RELEASES); }
  protected override liveQueries(): LiveQueries | undefined { return this.queries?.live; }
  protected override operatorSink(): OperatorDataSink { return rpc(this.env.CONTROL, "control"); }
  protected override composition() {
    return {
      tenantOnly: true,
      beforeInvoke: async (call: TenantCall) => {
        if (call.kind !== "reducer") return;
        this.ctx.storage.kv.put(overrunMarkerKey(this.kernel.module?.artifactHash, call.invocation.commandId, call.function), { commandId: call.invocation.commandId, path: call.function, artifact: this.kernel.module?.artifactHash,
          revision: this.ctx.storage.sql.exec<{ revision: number }>("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1").toArray()[0]?.revision ?? 0 });
        // The causal marker must survive the platform resetting a pure loop.
        // This happens before the application storage transaction opens.
        await this.ctx.storage.sync();
      },
      afterInvoke: (call: TenantCall) => { if (call.kind === "reducer") this.ctx.storage.kv.delete(overrunMarkerKey(this.kernel.module?.artifactHash, call.invocation.commandId, call.function)); },
      files: () => reducerFiles({ bucket: this.env.FILES, configured: this.files.configured }),
      restoreManifest: (stored: StoredModule): StoredModule => {
        const release = releases[stored.projectId];
        return release ? { projectId: stored.projectId, manifest: release.manifest } : stored;
      },
    };
  }

  constructor(ctx: DurableObjectState, env: AssemblyTenantEnv) {
    super(ctx, env);
    this.queries = new QueryEngine({
      module: () => this.activeQueryModule(),
      visibility: kernelVisibility(this.delivery.registry),
      run: (body) => this.kernel.reducers.serial(() => snapshotRunner(this.kernel.storage)(body)),
      replica: (session, _path, fn, args) => this.kernel.reducers.serial(() => {
        const stored = this.ctx.storage.kv.get<StoredModule>(MODULE_KEY)!;
        return collectionQuery(this.kernel.storage, this.delivery.registry, replicaModule(stored.projectId, stored.manifest), session, fn, args);
      }),
      validateArgs: (schema, args) => { try { validateValue(schema, args); return null; } catch (error) { return (error as Error).message; } },
      handler: async (session, path, args) => {
        const report = await this.kernel.reducers.serial(() => this.kernel.invoke({ function: path, kind: "query", args, identity: identityOf(session), invocation: invocationInfoOf(session, crypto.randomUUID(), this.kernel.module!.artifactHash), anonymous: session.anonymous }));
        return report.ok ? { ok: true, text: report.value ?? "null" } : { ok: false, error: report.error?.message ?? "query failed" };
      },
    });
    this.files = new TenantFiles({ sql: ctx.storage.sql, transactionSync: (f) => ctx.storage.transactionSync(f), bucket: env.FILES, config: filesConfigFromEnv(env) });
    this.files.ensureSchema();
    this.filePort = filesRpc(this.files);
    this.presence = new TenantPresence({
      tenantId: ctx.id.name ?? "", config: presenceConfig(env), gateway: (name) => rpc<GatewayRpc>(env.GATEWAY, name),
      registeredGateways: () => this.registeredGateways(), waitUntil: (p) => ctx.waitUntil(p),
      armAlarm: (at) => { ctx.storage.kv.put("assembly/presence-alarm", at); ctx.waitUntil(this.armEarlier(at)); },
      lostPending: async () => ctx.storage.kv.get<number>("assembly/presence-alarm") !== undefined,
    });
    this.operator = new TenantOperator({ storage: ctx.storage, tenantStorage: this.kernel.storage, capture: this.kernel.capture.state, wake: this.kernel.capture.wake, migrator: this.kernel.migrator });
    this.memberships = new TenantMemberships(this.kernel, ctx.storage);
    this.kernel.capture.wake.subscribe(() => {
      this.membershipRetry = Date.now();
      ctx.waitUntil(this.setupScheduled().then(() => this.scheduler?.notify()));
    });
    ctx.blockConcurrencyWhile(async () => {
      const project = ctx.storage.kv.get<string>(PROJECT_KEY);
      const release = project && releases[project];
      if (release) { await super.activate(project, release.manifest, null); await this.setupScheduled(); }
    });
  }

  private async armEarlier(at: number): Promise<void> {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null || at < existing) await this.ctx.storage.setAlarm(at);
  }
  private activeQueryModule(): QueryModule | null {
    const active = this.kernel.module;
    if (!active) return null;
    if (this.queryModule?.hash !== active.artifactHash) {
      const stored = this.ctx.storage.kv.get<StoredModule>(MODULE_KEY)!;
      this.queryModule = { hash: active.artifactHash, module: moduleFromManifest(stored.projectId, stored.manifest as Manifest, active.artifactHash) };
    }
    return this.queryModule.module;
  }
  override async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    // Every real caller supplies its current server-owned scope, including a
    // writer with no subscriptions. Its no-op commit watermark is scoped too.
    try {
      if (socket.tenant && !socket.tenant.anonymous && !this.delivery.observe(socket, false)) throw new Error("stale tenant subscription scope");
    } catch (error) {
      if (!(error instanceof DeliveryAdmissionError) || (socket as SocketView & {queryRecovery?:boolean}).queryRecovery) throw error;
      const refused = (id:string,path:string,trace?:MessageTrace) => frame.type.startsWith("reducer.") ? frames.reducerError(id,path,error.message,"transient",trace)
        : frame.type.startsWith("query.") ? frames.queryError(id,path,error.message) : frame.type.startsWith("action.") ? frames.actionError(id,path,error.message,trace) : frames.replicaError(id,path,error.message);
      if (frame.type === "reducer.call" || frame.type === "query.call" || frame.type === "query.subscribe" || frame.type === "action.call" || frame.type === "replica.open") return [refused(frame.id,frame.path,"trace" in frame?frame.trace:undefined)];
      if (frame.type === "reducer.callMany") return frame.calls.map(call=>refused(call.id,call.path,call.trace));
      if (frame.type === "replica.openMany") return frame.opens.map(open=>refused(open.id,open.path));
      throw error;
    }
    if (!(socket as SocketView & { queryRecovery?: boolean }).queryRecovery && !["query.subscribe", "replica.open", "replica.openMany", "replica.close"].includes(frame.type)) this.delivery.externalEvent(socket.host);
    if(this.env.GONVEX_ASSEMBLY_FIXTURE_KEY&&this.fixtureRecoveryFailure&&(socket as SocketView & {queryRecovery?:boolean}).queryRecovery
      &&frame.type==="replica.openMany"&&!frame.opens.length){
      this.fixtureRecoveryFailure=false;throw new Error("assembly fixture: scope registration fault");
    }
    if (frame.type === "query.call" && frame.path === RECEIPT_QUERY) return super.dispatch(socket, frame);
    if (frame.type === "query.unsubscribe") {
      this.delivery.queryUnsubscribe(socket, frame.id);
      return [];
    }
    if (frame.type === "query.subscribe") return this.delivery.querySubscribe({ ...socket, queryId: frame.id } as SocketView, (db) => {
      // Install and enqueue the initial result before a later reducer can commit.
      // Recovery uses this identical auth/visibility path inside the holder barrier.
      if ((socket as SocketView & { queryRecovery?: boolean }).queryRecovery === true && socket.tenant) {
        const session = socket.tenant;
        const row = db.run("SELECT account_id,status,role,permissions,membership_revision FROM members WHERE id=$1", [session.member.id]).rows[0];
        if (!row || row[0] !== session.identity.account.id || row[1] !== "active" || row[2] !== session.member.role
          || JSON.stringify(JSON.parse(String(row[3]))) !== JSON.stringify(session.member.permissions) || Number(row[4]) !== session.member.membershipRevision) throw new Error("delivery recovery Member authorization changed");
      }
      return this.queries.live.subscribeSnapshot(db, socket.connectionId, socket.tenant, frame);
    }, (socket as SocketView & { queryRecovery?: boolean }).queryRecovery === true);
    const query = await this.queries.dispatch(socket, frame);
    if (query) return query;
    if (frame.type === "presence.set" || frame.type === "presence.subscribe" || frame.type === "presence.unsubscribe") return this.presence.frame(socket, frame);
    if (frame.type === "action.call" && socket.tenant) {
      const session = actionSession(socket.tenant);
      session.reply = { host: socket.host, connectionId: socket.connectionId, epoch: socket.subscriptionEpoch ?? 0, id: frame.id, path: frame.path, ...(socket.actionGeneration === undefined ? {} : { generation: socket.actionGeneration }), ...(frame.trace ? { traceJson: JSON.stringify(echoTrace(frame.trace)) } : {}) };
      const result = await this.env.ACTIONS.call({ session, path: frame.path, args: frame.args });
      if ("pending" in result) return [];
      return [this.actionFrame(frame.id, frame.path, result, frame.trace)];
    }
    return super.dispatch(socket, frame);
  }
  async presenceLeave(id: string, host: string, previous?: ConnectionReport | null): Promise<void> { await this.presence.leave(id, host, previous ?? null); }
  override async connectionReset(id: string, epoch: number): Promise<void> { await super.connectionReset(id, epoch); this.queries.live.clear(id); }
  override async alarm(): Promise<void> {
    await this.setupScheduled();
    if (this.alarms) await this.alarms.fire();
    else if (this.kernel.migrator.wantsAlarm()) await this.kernel.migrator.alarm();
  }

  private actionFrame(id: string, path: string, result: ActionReport, trace?: MessageTrace): string {
    if (!result.ok) return frames.actionError(id, path, result.error, trace);
    return `{"type":"action.result","id":${JSON.stringify(id)},"path":${JSON.stringify(path)},"result":${result.value}`
      + (result.committedRevision === undefined ? "" : `,"committedRevision":${result.committedRevision}`) + (trace ? `,"trace":${JSON.stringify(echoTrace(trace))}` : "") + "}";
  }
  private async setupScheduled(): Promise<void> {
    const module = this.kernel.module;
    if (!module || this.scheduler) return;
    const tenantId = this.ctx.id.name ?? "";
    const stored = () => this.ctx.storage.kv.get<StoredModule>(MODULE_KEY)!;
    const active: ActiveModule = {
      kindOf: (path) => this.kernel.module?.functions.get(path)?.kind as ReturnType<ActiveModule["kindOf"]>,
      artifactHash: () => this.kernel.module!.artifactHash,
      crons: () => parseCronSpecs((stored().manifest.module as Record<string, unknown>).crons),
    };
    this.alarms = new TenantAlarm(storageAlarm(this.ctx.storage));
    this.scheduler = new Scheduler({ storage: this.ctx.storage, project: module.projectId, tenant: tenantId, module: active,
      reducers: { runReducer: (run) => this.runScheduledReducer(run) },
      actions: new WorkflowHandoff(this.env.ACTION_WORKFLOW as Workflow<HandoffJob>, module.projectId, tenantId),
      dueWhen: { evaluate: (sql, now) => this.kernel.reducers.serial(() => storageDueWhen(this.kernel.storage).evaluate(sql, now)) },
    });
    this.scheduler.attach(this.alarms);
    this.calls = new ActionCalls({ storage: this.ctx.storage, projectId: module.projectId, tenantId,
      dispatcher: new WorkflowDispatcher(this.env.ACTION_WORKFLOW as Workflow<CallJob>),
      wake: () => this.alarms!.arm().then(() => undefined), onFinished: (finished) => this.finished(finished),
    });
    this.alarms.register(this.calls.alarmSource());
    this.alarms.register({ name: "migrations", nextAt: (now) => this.kernel.migrator.wantsAlarm() ? now : null,
      run: async () => { await this.kernel.reducers.serial(() => this.kernel.migrator.alarm()); return 1; } });
    this.alarms.register({ name: "receipts", nextAt: () => this.kernel.module?.offlineMaxAgeMs == null ? null : this.receiptRetry ?? (this.ctx.storage.kv.get<number>(RECEIPT_PRUNE_KEY) ?? this.openedAt) + RECEIPT_PRUNE_INTERVAL_MS,
      run: async (now) => { const result = await this.kernel.pruneReceipts(now, { force: true, maxPages: 20 }); this.receiptRetry = result && !result.done ? now + 1000 : null; return result?.pages ?? 0; } });
    this.alarms.register({ name: "change-log", nextAt: () => this.logRetry ?? (this.ctx.storage.kv.get<number>("assembly/log-pruned") ?? this.openedAt) + PRUNE_INTERVAL_MS,
      run: async (now) => { const result = await this.kernel.reducers.serial(() => pruneChangeLog(this.ctx.storage, { maxPages: 20 })); this.logRetry = result.done ? null : now + 1000; if (result.done) this.ctx.storage.kv.put("assembly/log-pruned", now); return result.pages; } });
    this.alarms.register({ name: "presence", nextAt: () => this.presence.nextAlarm() ?? this.ctx.storage.kv.get<number>("assembly/presence-alarm") ?? null,
      run: async () => { this.ctx.storage.kv.delete("assembly/presence-alarm"); await this.presence.alarm(); return 1; } });
    this.alarms.register({ name: "memberships", nextAt: (now) => this.membershipRetry ?? (this.hasMemberships() ? now : null),
      run: async (now) => { const n = await this.projectMemberships(); this.membershipRetry = this.hasMemberships() ? now + 1000 : null; return n; } });

    await this.scheduler.notify();
  }
  private hasMemberships(): boolean {
    try { return this.ctx.storage.sql.exec("SELECT 1 FROM _gonvex_control_plane_membership_outbox LIMIT 1").toArray().length > 0; }
    catch { return false; } // A legacy schema may not declare the projection outbox.
  }
  private async projectMemberships(): Promise<number> {
    if (!this.hasMemberships()) return 0;
    const rows = this.ctx.storage.sql.exec<{ account_id: string; member_id: string; status: string; membership_revision: number }>("SELECT account_id,member_id,status,membership_revision FROM _gonvex_control_plane_membership_outbox ORDER BY membership_revision,account_id LIMIT 1000").toArray();
    const batch = rows.map((row) => ({ accountId: row.account_id, memberId: row.member_id, status: row.status, membershipRevision: row.membership_revision }));
    await rpc<{ projectMemberships(project: string, tenant: string, rows: typeof batch): Promise<number> }>(this.env.CONTROL, "control").projectMemberships(this.kernel.module!.projectId, this.ctx.id.name ?? "", batch);
    this.ctx.storage.sql.exec(`DELETE FROM _gonvex_control_plane_membership_outbox WHERE EXISTS (SELECT 1 FROM json_each(?) j WHERE json_extract(j.value,'$.accountId')=account_id AND json_extract(j.value,'$.membershipRevision')>=membership_revision)`, JSON.stringify(batch));
    return rows.length;
  }
  private async runScheduledReducer(run: DurableRun) {
    const session = run.actor.kind === "system" ? systemSession(this.kernel.module!.projectId, this.ctx.id.name ?? "")
      : await rpc<{ tenantSessionForAccount(p: string, t: string, a: string): Promise<ActionSession> }>(this.env.CONTROL, "control").tenantSessionForAccount(this.kernel.module!.projectId, this.ctx.id.name ?? "", run.actor.accountId);
    const report = await this.callWithLane({ v: 1, function: run.function, kind: "reducer", args: run.args, session,
      provenance: run.provenance as unknown as TenantCallRequest["provenance"], allowInternal: true }, "scheduled");
    return report.ok ? { ok: true as const } : { ok: false as const, error: report.error };
  }
  private async finished(finished: CallFinished): Promise<void> {
    const reply = finished.reply;
    if (!reply) return;
    let text = finished.ok ? `{"type":"action.result","id":${JSON.stringify(reply.id)},"path":${JSON.stringify(reply.path)},"result":${finished.value ?? "null"}${finished.committedRevision === undefined ? "" : `,"committedRevision":${finished.committedRevision}`}}`
      : frames.actionError(reply.id, reply.path, finished.error ?? "action failed");
    if (reply.traceJson) text = text.slice(0, -1) + `,"trace":${reply.traceJson}}`;
    const receipt = await rpc<{ settleActionReply(target: typeof reply, text: string): Promise<{ v: 1; status: "enqueued" | "obsolete" }> }>(this.env.GATEWAY, reply.host)
      .settleActionReply(reply, text);
    if (receipt?.v !== 1 || (receipt.status !== "enqueued" && receipt.status !== "obsolete")) throw new Error("Action reply settlement unsupported");
  }
  async call(request: TenantCallRequest) {
    return this.callWithLane(request, "interactive");
  }
  private async callWithLane(request: TenantCallRequest, lane: "interactive" | "scheduled") {
    if (request.kind === "query") {
      try {
        const tenant = this.sessionFromAction(request.session);
        const value = await executeQuery(this.queries.ports, tenant, request.function, request.args, { allowInternal: request.allowInternal, delegatedAgent: request.provenance.channel === "agent" });
        return { ok: true as const, value };
      } catch (error) { return { ok: false as const, error: (error as Error).message, transient: false }; }
    }
    const outcome = await this.kernel.reducers.serial(async () => {
      const admissionError = writeAdmissionError(() => this.kernel.migrator.gateClosed());
      if (admissionError) return { ok: false as const, error: admissionError.message, transient: retryable(admissionError.class) };
      this.kernel.storage.arm({ provenance: publicInfo(request.provenance), afterSuccess: (db) => { flushReferences(db); } });
      try {
        const report = await this.kernel.invoke({ function: request.function, kind: "reducer", args: request.args, identity: actionIdentity(request.session), anonymous: isAnonymous(request.session), invocation: publicInfo(request.provenance), hostProvenance: persisted(request.provenance), lane });
        const commit = this.kernel.storage.takeReport();
        return report.ok ? { ok: true as const, value: report.value ?? "null", committedRevision: commit?.notice?.revision } : { ok: false as const, error: report.error?.message ?? "call failed", transient: report.error?.retryable ?? false };
      } finally { this.kernel.storage.disarm(); }
    });
    if (outcome.ok) await this.kernel.reducers.serial(async () => { await this.scheduler?.notify(); });
    return outcome;
  }
  private sessionFromAction(s: ActionSession): TenantSession {
    return {
      identity: { projectId: s.projectId, account: { id: s.account.id, email: s.account.email ?? "", name: s.account.name ?? "", avatarUrl: s.account.avatarUrl ?? "", emailVerified: false, provider: "" } },
      route: { projectId: s.projectId, tenantId: s.tenantId, databaseUrl: "" },
      member: { id: s.member.id, accountId: s.member.accountId ?? s.account.id, status: s.member.status ?? "", role: s.member.role ?? "", displayName: s.member.displayName ?? "", avatarUrl: "", permissions: (s.member.permissions ?? {}) as TenantSession["member"]["permissions"], membershipRevision: 0 },
      anonymous: s.account.id === "", admissionRevision: 0, delegation: (s.delegation ?? null) as TenantSession["delegation"], developerSession: (s.developerSession ?? null) as TenantSession["developerSession"],
    };
  }
  async claimCall(request: ClaimRequest) { await this.setupScheduled(); return this.calls!.claim(request); }
  async renewCall(request: RenewRequest) { await this.setupScheduled(); return this.calls!.renew(request); }
  async completeCall(request: CompleteRequest) { await this.setupScheduled(); return this.calls!.complete(request); }
  async beginCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown }) { await this.setupScheduled(); return this.calls!.begin(request); }
  async renewRun(request: { v: 1; claim: Parameters<Scheduler["renew"]>[0] }) { await this.setupScheduled(); return this.scheduler!.renew(request.claim); }
  async completeRun(request: { v: 1; claim: Parameters<Scheduler["complete"]>[0]; outcome: Parameters<Scheduler["complete"]>[1] }) { await this.setupScheduled(); return this.scheduler!.complete(request.claim, request.outcome); }
  async schedule(request: ScheduleRequest) { await this.setupScheduled(); return this.scheduler!.enqueue({ function: request.function, args: request.args, runAtUnixMs: request.runAtUnixMs, provenance: request.provenance, coalesce: request.coalesce, jobId: request.id }); }
  protected override committed(call: CommittedCall): void {
    super.committed(call);
    this.ctx.waitUntil(this.setupScheduled().then(() => this.scheduler?.notify()));
  }
  override async serviceCall(grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome> {
    if (call.kind === "reducer") return super.serviceCall(grant, call);
    const tenant = serviceSession(grant);
    if (call.kind === "query") {
      try { return { ok: true, result: await executeQuery(this.queries.ports, tenant, call.path, call.args, { allowInternal: true }) }; }
      catch (error) { return { ok: false, error: (error as Error).message }; }
    }
    const session = actionSession(tenant);
    if (call.reply) session.reply = call.reply;
    const result = await this.env.ACTIONS.call({ session, path: call.path, args: call.args, allowInternal: true, inline: !call.reply,
      provenance: directProvenance(session, grant.channel ?? "api", call.id, this.kernel.module!.artifactHash) });
    if ("pending" in result) {
      if (!call.reply) throw new Error("awaited service Action returned a pending execution");
      return { ok: true, result: "null", pending: true };
    }
    return result.ok ? { ok: true, result: result.value, ...(result.committedRevision === undefined ? {} : { committedRevision: result.committedRevision }) } : { ok: false, error: result.error };
  }

  // K06 admission returns the same Member plus its snapshot revision for both consumers.
  override async admit(accountId: string) {
    const admitted = await super.admit(accountId);
    if (!admitted) return null;
    const { admissionRevision, ...member } = admitted;
    return { ...admitted, member, revision: admissionRevision };
  }
  async acceptInvitation(acceptance: Parameters<TenantMemberships["acceptInvitation"]>[0]) {
    return this.memberships.acceptInvitation(acceptance, this.kernel.module!.projectId, this.ctx.id.name ?? "");
  }
  activeMemberAccounts(ids: string[]) { return this.memberships.activeMemberAccounts(ids); }
  memberById(id: string) { return this.memberships.memberById(id); }
  memberAccounts(ids: string[]) { return this.memberships.memberAccounts(ids); }
  activeMember(id: string) { return this.memberships.activeMember(id); }
  activeMemberCount() { return this.memberships.activeMemberCount(); }
  writeMembership(write: Parameters<TenantMemberships["writeMembership"]>[0]) { return this.memberships.writeMembership(write); }

  async provision(projectId: string, _tenantId: string): Promise<void> {
    this.ctx.storage.kv.put(PROJECT_KEY, projectId);
    const release = releases[projectId];
    if (!release) throw new Error(`project ${JSON.stringify(projectId)} has no active TypeScript module`);
    const planId = ((release.manifest.module as Record<string, unknown>).release as { planId: string }).planId;
    await this.activate(projectId, release.manifest, planId);
  }
  async operatorApplyRelease(planId: string, projectId?: string) {
    if (projectId) this.ctx.storage.kv.put(PROJECT_KEY, projectId);
    const status = await this.kernel.reducers.serial(() => this.operator.applyRelease(planId));
    await this.activateProvisioned(status);
    return status;
  }
  private async activateProvisioned(status: Awaited<ReturnType<typeof this.kernel.migrator.status>>): Promise<void> {
    if (status.state !== "done" || this.kernel.module) return;
    const project = this.ctx.storage.kv.get<string>(PROJECT_KEY);
    const release = project && releases[project];
    if (release) { await super.activate(project, release.manifest, null); await this.setupScheduled(); }
  }
  async operatorMigrationStatus() { const status = await this.kernel.migrator.status(); await this.activateProvisioned(status); return status; }
  async operatorLedger() { return this.kernel.migrator.ledger(); }
  async operatorServedModule(projectId: string): Promise<ServedModule | null> {
    const module = this.kernel.module;
    return module?.projectId === projectId ? { projectId, hash: module.artifactHash, generation: module.generation, functions: module.functions.size, loadMs: 0, error: null } : null;
  }
  async operatorModuleReloaded(event: { projectId: string; hash: string; generation: number; clientContract: number | null }): Promise<void> {
    const info = { artifactHash: event.hash, clientContract: event.clientContract };
    await Promise.all((await this.registeredGateways()).map((name) => rpc<GatewayRpc>(this.env.GATEWAY, name).moduleReloaded(event.projectId, info)));
  }
  async operatorDeleteAll() { await this.ctx.storage.deleteAlarm(); await this.ctx.storage.deleteAll(); this.ctx.abort("tenant deleted"); }
  async operatorListTables() { return this.operator.listTables(); }
  async operatorReadRows(table: string, query: RowsQuery) { return this.operator.readRows(table, query); }
  operatorInsertRow(table: string, payload: Record<string, unknown>) { return this.kernel.reducers.serial(() => this.operator.insertRow(table, payload)); }
  operatorUpdateRow(table: string, id: string, payload: Record<string, unknown>) { return this.kernel.reducers.serial(() => this.operator.updateRow(table, id, payload)); }
  operatorDeleteRow(table: string, id: string) { return this.kernel.reducers.serial(() => this.operator.deleteRow(table, id)); }
  operatorReplaceReferences(replacements: Record<string, string>, dryRun: boolean) { return this.kernel.reducers.serial(() => this.operator.replaceReferences(replacements, dryRun)); }
  async operatorActiveMembers() { return this.operator.activeMembers(); }
  operatorRevokeMember(id: string) { return this.kernel.reducers.serial(() => this.operator.revokeMember(id)); }

  filesCall(...args: Parameters<FilesTenantRpc["filesCall"]>) { return this.filePort.filesCall(...args); }
  filesPrepareStore(...args: Parameters<FilesTenantRpc["filesPrepareStore"]>) { return this.filePort.filesPrepareStore(...args); }
  filesRecordStored(...args: Parameters<FilesTenantRpc["filesRecordStored"]>) { return this.filePort.filesRecordStored(...args); }
  filesReadTarget(...args: Parameters<FilesTenantRpc["filesReadTarget"]>) { return this.filePort.filesReadTarget(...args); }
  filesClaimUpload(...args: Parameters<FilesTenantRpc["filesClaimUpload"]>) { return this.filePort.filesClaimUpload(...args); }
  filesReleaseUpload(...args: Parameters<FilesTenantRpc["filesReleaseUpload"]>) { return this.filePort.filesReleaseUpload(...args); }
  filesDownloadTarget(...args: Parameters<FilesTenantRpc["filesDownloadTarget"]>) { return this.filePort.filesDownloadTarget(...args); }
  async recordOverrun(commandId: string, path: string): Promise<"quarantined" | "committed" | "unrelated"> {
    const key = overrunMarkerKey(this.kernel.module?.artifactHash, commandId, path);
    const marker = this.ctx.storage.kv.get<{ commandId: string; path: string; artifact: string; revision: number }>(key);
    if (!marker || marker.commandId !== commandId || marker.path !== path || marker.artifact !== this.kernel.module?.artifactHash) return "unrelated";
    const committed = this.ctx.storage.sql.exec("SELECT 1 FROM _gonvex_sync_transactions WHERE revision>? AND origin_command_id=? LIMIT 1", marker.revision, commandId).toArray().length > 0;
    this.ctx.storage.kv.delete(key);
    if (committed) return "committed";
    this.kernel.recordOverrun(commandId, path);
    return "quarantined";
  }
  async fixtureFailRecoveryOnce():Promise<void>{
    if(!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY)throw new Error("assembly fixture control disabled");
    this.fixtureRecoveryFailure=true;
  }
  async fixtureDeliveryState(){
    if(!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY)throw new Error('assembly fixture control disabled');
    return {...this.delivery.stats,pendingInitials:this.delivery.pendingInitialCount()};
  }
  async fixtureEpoch(epoch: string): Promise<void> {
    await this.kernel.reducers.serial(async () => { this.ctx.storage.sql.exec("UPDATE _gonvex_sync_clock SET epoch=? WHERE singleton=1", epoch); });
  }
  async fixtureRestart(): Promise<void> { this.ctx.abort("assembly fixture: tenant restart"); }
  async fixtureChangeLog(offset = 0) {
    return this.kernel.reducers.serial(async () => ({
      clock: this.ctx.storage.sql.exec("SELECT epoch,revision,retained_revision FROM _gonvex_sync_clock WHERE singleton=1").toArray()[0],
      changes: this.ctx.storage.sql.exec("SELECT * FROM _gonvex_sync_changes ORDER BY revision,ordinal LIMIT 1000 OFFSET ?", offset).toArray(),
      transactions: this.ctx.storage.sql.exec("SELECT * FROM _gonvex_sync_transactions ORDER BY revision LIMIT 1000 OFFSET ?", offset).toArray(),
    }));
  }

}
