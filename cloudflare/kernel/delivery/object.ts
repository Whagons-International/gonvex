/// <reference path="../host/src/shims.d.ts" />
// The tenant object with replica delivery: K07's `ReducerTenantObject` plus
// the engine of this package. Commits reach the engine through K03's
// `ChangeWake`, replica frames go to the gateways holding the sockets
// (`GatewayOutlet`), and K07's call watermark takes its floor from here.
//
// A Worker version subclasses it to provide its bundles and plan store, as
// for `ReducerTenantObject`.

import { MEMBER_ACCESS_SQL, type MemberRow } from "../session/member-access";
import type { ClientFrame } from "../session/decode";
import type { SocketView } from "../session/ports";
import { rpc } from "../session/rpc";
import { LiveQueries, snapshotRunner } from "../query/live.ts";
import { MODULE_KEY, type StoredModule } from "../reducer/tenant.ts";
import { ReducerTenantObject, type ReducerTenantEnv } from "../reducer/object.ts";
import { replicaModule, type ReplicaModule } from "./definition.ts";
import { ReplicaDeliveryEngine } from "./engine.ts";
import { openReplica, resolveCollection, sqlOf } from "./subscription.ts";
import { readClock } from "./log.ts";
import { pageStatement, scanPlan, scanWindow, STATEMENT_ROWS } from "./scan.ts";
import { GatewayOutlet, type ReplicaHolderRpc } from "./outlet.ts";
import { keepAliveCheck, SubscriberKeepAlive } from "./keepalive.ts";

export abstract class ReplicaTenantObject<E extends ReducerTenantEnv = ReducerTenantEnv> extends ReducerTenantObject<E> {
  readonly delivery: ReplicaDeliveryEngine;
  /** BUG-2/BUG-9: subscriptions live in memory; a subscribed tenant does not hibernate while any subscription exists. */
  readonly keepAlive: SubscriberKeepAlive;
  protected liveQueries(): LiveQueries | undefined { return undefined; }
  /** X13g: the tenant gained its first live subscriber (true) or lost its last one (false). */
  protected subscribersChanged(_holding: boolean): void {}

  private cachedModule: { key: string; module: ReplicaModule } | undefined;
  private dataChangedScheduled = false;

  /**
   * Where the operator dashboard learns of commits (K19 0036 §8, 0037): the operator control object's
   * `operatorDataChanged`. The Worker version that binds it overrides this; without it nothing is sent.
   */
  protected operatorSink(): { operatorDataChanged(projectId: string, tenantId: string): Promise<void> } | undefined {
    return undefined;
  }

  constructor(ctx: DurableObjectState, env: E) {
    super(ctx, env);
    const tenantId = ctx.id.name ?? ctx.id.toString();
    const owner = this;
    this.keepAlive = new SubscriberKeepAlive(() => this.delivery.holdsSubscriptions(),
      keepAliveCheck((env as { GONVEX_TENANT_KEEPALIVE_MS?: string }).GONVEX_TENANT_KEEPALIVE_MS), undefined,
      (holding) => this.subscribersChanged(holding));
    this.delivery = new ReplicaDeliveryEngine({
      tenantId,
      serial: (fn) => this.kernel.reducers.serial(fn),
      read: snapshotRunner(this.kernel.storage),
      module: () => this.replicaModule(),
      // 04-hang: keyed by this object's id, so this instance's outlet returns the isolate's delivery slots and credits
      // a reset predecessor in the same isolate still held (its RPCs never settle there).
      outlet: new GatewayOutlet(tenantId, (name) => rpc<ReplicaHolderRpc>(env.GATEWAY, name), () => this.gateways.list(), ctx.id.toString()),
      delivered: () => this.dataChanged(tenantId),
      get live() { return owner.liveQueries(); },
      activity: () => owner.keepAlive.touch(),
      // #554 (default off): the backlog budget in work units; a gateway that advertised `acceptsBusy` holds the rest.
      backlogUnits: Number((env as { GONVEX_TENANT_BACKLOG_UNITS?: string }).GONVEX_TENANT_BACKLOG_UNITS ?? 0) || 0,
      // X17m: the attach-time member check (one indexed read per attach).
      currentMember: (memberId) => {
        const row = ctx.storage.sql.exec<MemberRow & Record<string, SqlStorageValue>>(MEMBER_ACCESS_SQL, memberId).toArray()[0];
        const clock = ctx.storage.sql.exec<{ revision: number }>("SELECT revision FROM _gonvex_sync_clock WHERE singleton = 1").toArray()[0];
        return { row, revision: Number(clock?.revision ?? 0) };
      },
    });
    this.kernel.capture.wake.subscribe((wake) => { this.delivery.wake(wake.revision); this.dataChanged(tenantId); });
    // Commits after this point are owed to the sockets; a tenant not provisioned yet has no clock.
    try {
      const row = ctx.storage.sql.exec<{ epoch: string; revision: number }>("SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton = 1").toArray()[0];
      this.delivery.start(row ? { epoch: String(row.epoch), revision: Number(row.revision) } : undefined);
    } catch {
      this.delivery.start(undefined);
    }
  }

  /** One `operatorDataChanged` per 100 ms of delivered commits (as K19's local tenant object coalesces them). */
  private dataChanged(tenantId: string): void {
    const sink = this.operatorSink();
    const projectId = this.kernel.module?.projectId;
    if (!sink || !projectId || this.dataChangedScheduled) return;
    this.dataChangedScheduled = true;
    this.ctx.waitUntil((async () => {
      await new Promise((resolve) => setTimeout(resolve, 100));
      this.dataChangedScheduled = false;
      await sink.operatorDataChanged(projectId, tenantId).catch((error) => console.error("delivery: operatorDataChanged failed", error));
    })());
  }

  /**
   * X17 dev profiling (only while `GONVEX_SPAN_TIMING=1`): runs every replica function of the active module for one
   * active member, `rounds` times, read-only, in the invocation queue, with the engine's own registry. `phase`:
   * "none" (the loop only), "resolve" (clock, visibility resolution, scan plan) or "open" (the whole open, frames
   * discarded, nothing installed). The caller reads each call's CPU from Workers Logs.
   */
  async devOpenCost(phase: "none" | "resolve" | "open" | "light" | "scan" | "explain", rounds = 1, only?: string[]): Promise<{ paths: number; frames: number; errors: number; failed?: string; explain?: unknown[] }> {
    if ((this.env as { GONVEX_SPAN_TIMING?: string }).GONVEX_SPAN_TIMING !== "1") throw new Error("dev profiling is disabled");
    const module = this.replicaModule();
    const active = this.kernel.module;
    if (!module || !active) throw new Error("no active module");
    const row = this.ctx.storage.sql.exec<{ account_id: string }>("SELECT account_id FROM members WHERE status='active' ORDER BY (role='owner') DESC, (role='admin') DESC LIMIT 1").toArray()[0];
    if (!row) throw new Error("no active member");
    const admitted = await this.admit(String(row.account_id));
    if (!admitted) throw new Error("member not admitted");
    const { admissionRevision, ...member } = admitted;
    const session = {
      identity: { projectId: active.projectId, account: { id: String(row.account_id), email: "", emailVerified: true, name: "", avatarUrl: "", provider: "" } },
      route: { projectId: active.projectId, tenantId: this.ctx.id.name ?? "", databaseUrl: "" },
      member, admissionRevision, delegation: null, developerSession: null, anonymous: false,
    } as unknown as Parameters<typeof resolveCollection>[1]["session"];
    const paths = [...module.functions].filter(([path, fn]) => fn.kind === "query" && fn.delivery === "replica" && fn.replica && (!only || only.includes(path))).map(([path]) => path);
    let failed: string | undefined;
    const explain: unknown[] = [];
    const read = snapshotRunner(this.kernel.storage);
    let frames = 0, errors = 0;
    await this.kernel.reducers.serial(async () => {
      for (let round = 0; round < rounds; round++) for (const path of paths) {
        if (phase === "none") continue;
        const fn = module.functions.get(path)!;
        const definition = fn.replica!;
        const plan = module.visibility.get(definition.table);
        if (!plan) { errors++; continue; }
        const context = { session, module, sumNegotiated: true, paged: true, scope: "dev", visibilityScope: "dev", registry: this.delivery.registry, read,
          emit: (out: string[]) => { frames += out.length; if (out.some((f) => f.includes('"replica.error"'))) errors++; }, drain: async () => {} };
        if (phase === "explain") {
          await read((db) => {
            const clock = readClock(sqlOf(db));
            const resolved = resolveCollection(db, context, definition, plan, clock.revision);
            const scan = scanPlan(definition, plan, resolved, {}, db.catalogOf(definition.table).columns);
            const statement = pageStatement(scan, undefined, STATEMENT_ROWS);
            const qp = db.run(`EXPLAIN QUERY PLAN ${statement.sql}`, statement.parameters as never).rows.map((r) => r[r.length - 1]);
            explain.push({ path, sql: statement.sql.slice(0, 1500), plan: qp });
          });
          continue;
        }
        if (phase === "light" || phase === "scan") {
          try {
            const scan = await read((db) => {
              const clock = readClock(sqlOf(db));
              const resolved = resolveCollection(db, context, definition, plan, clock.revision);
              return scanPlan(definition, plan, resolved, {}, db.catalogOf(definition.table).columns);
            });
            await scanWindow(read, definition, scan, () => { frames++; }, { chunkBytes: 2 << 20, light: phase === "light" });
          } catch (error) { errors++; failed ??= `${path}: ${String((error as Error)?.message ?? error).slice(0, 200)}`; }
          continue;
        }
        if (phase === "resolve") {
          try {
            await read((db) => {
              const clock = readClock(sqlOf(db));
              const resolved = resolveCollection(db, context, definition, plan, clock.revision);
              return scanPlan(definition, plan, resolved, {}, db.catalogOf(definition.table).columns);
            });
          } catch (error) { errors++; failed ??= `${path}: ${String((error as Error)?.message ?? error).slice(0, 200)}`; }
        } else {
          try { await openReplica({ id: `dev-${path}`, path, args: {}, keys: [], hashes: {}, fullIntegrity: false } as never, context as never); }
          catch (error) { errors++; failed ??= `${path}: ${String((error as Error)?.message ?? error).slice(0, 200)}`; }
        }
      }
    });
    return { paths: paths.length, frames, errors, ...(failed ? { failed } : {}), ...(explain.length ? { explain } : {}) };
  }

  /** The replica view of the active generation, rebuilt when the generation changes. */
  private replicaModule(): ReplicaModule | undefined {
    const active = this.kernel.module;
    if (!active) return undefined;
    const key = `${active.projectId}\u0000${active.artifactHash}`;
    if (this.cachedModule?.key !== key) {
      const stored = this.ctx.storage.kv.get<StoredModule>(MODULE_KEY);
      if (!stored) return undefined;
      this.cachedModule = { key, module: replicaModule(stored.projectId, stored.manifest) };
    }
    return this.cachedModule.module;
  }

  override async activate(projectId: string, manifest: Record<string, unknown>, planId: string | null): Promise<{ ok: true }> {
    const result = await super.activate(projectId, manifest, planId);
    // A migration may change tables visibility reads (capture classes, projections).
    this.delivery.registry.invalidateSchema();
    this.cachedModule = undefined;
    return result;
  }

  /** 0048 §1a: one tenant → holder channel for commits, so the hand-off cannot overtake the commit's frames. */
  protected override ownMembershipChanged(socket: SocketView, revision: number): void {
    const memberId = socket.tenant?.member.id;
    if (memberId) this.delivery.membershipChanged(socket.host, memberId, revision);
  }

  protected override subscriptionFloor(socket: SocketView): number | undefined {
    return this.delivery.subscriptionFloor(socket);
  }

  override async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    const replica = this.delivery.dispatch(socket, frame);
    if (replica) return replica;
    return super.dispatch(socket, frame);
  }

  /** K05 `TenantPort.connectionReset`: the socket's subscriptions were dropped. */
  override async connectionReset(connectionId: string, epoch: number): Promise<void> {
    this.delivery.reset(connectionId, epoch);
  }

  /** Delivery counters (tests and the PR's numbers). */
  async deliveryStats(): Promise<unknown> {
    await this.delivery.settled();
    return { ...this.delivery.stats };
  }
}
