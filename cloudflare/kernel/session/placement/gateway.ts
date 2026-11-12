/**
 * Gateway placement (0004 option 2, decided in 0016 G0-1). This is the only
 * module that knows where sockets live. If the placement is overruled, this
 * file is what changes: `Session` and `SocketHost` run in any object.
 *
 *  - The router sends each upgrade to a gateway chosen without the tenant
 *    (the tenant arrives later, in `auth`): a shard of the connection id.
 *  - A gateway holds its sockets with the hibernation API (`SocketHost`).
 *  - When a gateway gets its first socket of a tenant it registers with that
 *    tenant's object (`attachGateway`), which keeps the set durably (KV,
 *    one write per new gateway, not per socket).
 *  - Each commit goes from the tenant to each registered gateway once
 *    (`deliver`); the gateway fans it out to its sockets of that tenant,
 *    encoded once per delivery class (S3 §3: 8 × 125 sockets hold 30 commits/s).
 *
 * RPC between tenants and gateways is versioned (`v`) and additive, so one
 * deploy's skew (10-40 s, S3 §7) never breaks a call: unknown fields are
 * ignored and every method tolerates older callers.
 */
import { ASSIGNMENT_HEADER, MAX_SOCKETS, controller, withDeadline, parseAssignment, type AutoscaleEnv, type CapacitySample, type DrainCommand, type DrainResult, pressured, hashConnection } from "../../gateway/placement";
import { GatewayMetrics } from "../../gateway/metrics";
import { DurableObject } from "cloudflare:workers";
import type { ModuleInfo, Ports, TenantRoute, TenantSession } from "../ports";
import { SocketHost } from "../host";
import { deliveryClass } from "../session";
import { type ConnectionReport, type PresenceEnv, presenceConfig } from "../../presence/hub";
import { type TelemetryEnv, telemetryHostFromEnv } from "../../telemetry/env";
import { PRESENCE_PROTOCOL_VERSION, type PresenceDelivery, type PresenceReceipt } from "../../presence/tenant";
import { REPLICA_DELIVERY_VERSION, type ReplicaDelivery, type ReplicaReceipt } from "../../delivery/protocol";
import { UNKNOWN_ADDRESS } from "../../public/address";
import type { PublicGuard } from "../../public/guard";
import { publicLimits } from "../../public/config";
import { limiterFor } from "../../public/limiter";
import type { SlotKind } from "../../public/limiter";

/** S3: about 125 sockets per gateway carried 30 commits/s to 1,000 sockets. */
export const SOCKETS_PER_GATEWAY = 125;
export const DEFAULT_GATEWAY_SHARDS = 8;
export const PROTOCOL_VERSION = 1;

export interface GatewayEnv extends PresenceEnv, TelemetryEnv, AutoscaleEnv {
  GATEWAY?: DurableObjectNamespace;
  PUBLIC_LIMITER?: DurableObjectNamespace<any>;
  GONVEX_GATEWAY_SHARDS?: string;
  GONVEX_RUNTIME_VERSION?: string;
  GONVEX_PRESENCE?: string;
}

/**
 * Number of gateway shards. Size it as expected concurrent sockets / 125.
 * Explicit legacy/static adapter only. With GATEWAY_CONTROLLER bound, X01 owns admission.
 */
export function gatewayShards(env: GatewayEnv): number {
  const n = Number(env.GONVEX_GATEWAY_SHARDS ?? DEFAULT_GATEWAY_SHARDS);
  return Number.isInteger(n) && n >= 1 && n <= 4096 ? n : DEFAULT_GATEWAY_SHARDS;
}

/** Gateway object name for a new connection: a stable shard of its id (FNV-1a). */
export function gatewayFor(env: GatewayEnv, connectionId: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < connectionId.length; i++) {
    h ^= connectionId.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return `gw-${h % gatewayShards(env)}`;
}

/** One commit as a tenant hands it to one gateway. */
export interface DeliveryBatch {
  v: number;
  tenantId: string;
  revision: number;
  /** Frames for every socket of the tenant on this gateway. */
  all?: string[];
  /** Frames per delivery class, encoded once by the tenant (design rule: encode once per class). */
  byClass?: Record<string, string[]>;
  /** Members whose own row changed in this commit (C.4 item 5a): their sockets discard the tenant scope. */
  membershipChanged?: string[];
}

export interface DeliveryReceipt { v: number; sockets: number }

/** The tenant-object side of a gateway link (the tenant object implements it; `stub/tenant.ts` until then). */
export interface TenantLink {
  attachGateway(gateway: string): Promise<void>;
  detachGateway(gateway: string): Promise<void>;
}

export interface GatewayBindings {
  ports(env: unknown): Ports;
  tenantLink(env: unknown, route: TenantRoute): TenantLink;
  /** K16: public-function limits and the address budgets (absent: default payload cap only). */
  publicGuard?(env: unknown): PublicGuard;
}

/** Headers the router hands the gateway with an upgrade (K05, K16). */
export const UPGRADE_HEADERS = {
  connectionId: "x-gonvex-connection-id",
  publicTransport: "x-gonvex-public",
  /** The client address bucket (`CF-Connecting-IP`, kernel/public/address.ts). */
  address: "x-gonvex-client-address",
  /** Slots the router reserved in the address limiter, comma-separated (`pending`, `public`). */
  slots: "x-gonvex-limiter-slots",
} as const;

/**
 * Gateway object. Sockets of any tenant; the class is exported by the Worker
 * entry and bound as `GATEWAY`.
 */
export abstract class GatewayObjectBase<E extends GatewayEnv> extends DurableObject<E> {
  protected readonly host: SocketHost;
  readonly gatewayName: string;
  /** tenantId -> sockets of that tenant here. */
  private readonly byTenant = new Map<string, Set<WebSocket>>();
  private readonly metrics = new GatewayMetrics();
  private placementEpoch = 0;
  private placementCell: number | undefined;
  private reportInFlight = false;
  private reportAgain = false;
  private draining: (DrainCommand & { nextAt: number }) | undefined;
  private drainOffset = 0;
  private drainTickRunning = false;
  private drainResult: DrainResult = { closed: 0, blocked: {}, remaining: 0 };
  private admissionTail: Promise<unknown> = Promise.resolve();
  private admissionWaiting = 0;
  private readonly links = new Map<string, Promise<void>>();

  protected abstract bindings(): GatewayBindings;

  constructor(ctx: DurableObjectState, env: E) {
    super(ctx, env);
    this.gatewayName = ctx.id.name ?? ctx.id.toString();
    const presence = presenceConfig(env);
    const publicGuard = this.bindings().publicGuard?.(env);
    this.host = new SocketHost({
      ...(publicGuard ? { publicGuard } : {}),
      presence,
      sent: (text) => this.metrics.sent(text),
      placementWake: () => this.draining?.nextAt ?? null,
      telemetry: telemetryHostFromEnv(env, presence.enabled),
      ctx,
      ports: this.bindings().ports(env),
      name: this.gatewayName,
      ready: { runtimeVersion: env.GONVEX_RUNTIME_VERSION || "development", presence: presence.enabled },
      tenantChanged: (ws, previous, next) => this.tenantChanged(ws, previous, next),
    });
    // Restored sockets: rebuild the tenant index only (the tenant already has this gateway registered).
    for (const [ws, session] of this.host.sessions) {
      const tenant = session.record.tenant;
      if (tenant && !tenant.anonymous) this.index(tenant.route.tenantId).add(ws);
    }
  }

  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("expected a WebSocket upgrade", { status: 400 });
    const headers = request.headers;
    if (this.env.GATEWAY_CONTROLLER) {
      const assignment = parseAssignment(headers.get(ASSIGNMENT_HEADER));
      if (!assignment || assignment.gateway !== this.gatewayName || assignment.connectionId !== headers.get(UPGRADE_HEADERS.connectionId)) return new Response("gateway assignment required", { status: 503 });
      // No socket migration: an admitted socket owns this holder until normal disconnect.
      // Serialize claim + accept, so parallel upgrades cannot race the socket ceiling.
      if (this.admissionWaiting >= 16) return new Response("gateway admission busy", { status: 503 });
      this.admissionWaiting++;
      const accepted = this.admissionTail.then(async () => {
        await this.loadPlacement();
        if (assignment.epoch < this.placementEpoch || this.host.sessions.size >= MAX_SOCKETS) return new Response("gateway full", { status: 503 });
        if (!await withDeadline(controller(this.env, assignment.cell).claim(assignment))) return new Response("gateway assignment expired", { status: 503 });
        if (assignment.epoch !== this.placementEpoch) {
          if (this.host.sessions.size) return new Response("gateway epoch busy", { status: 503 });
          this.draining = undefined;
          await this.ctx.storage.delete("autoscale-drain");
          this.placementEpoch = assignment.epoch;
          this.placementCell = assignment.cell;
          await this.ctx.storage.put("autoscale-placement", { epoch: this.placementEpoch, cell: this.placementCell });
        }
        const response = this.accept(headers);
        this.reportCapacity();
        return response;
      }).finally(() => { this.admissionWaiting--; });
      this.admissionTail = accepted.catch(() => undefined);
      return accepted;
    }
    return this.accept(headers);
  }

  private accept(headers: Headers): Response {
    const connectionId = headers.get(UPGRADE_HEADERS.connectionId) ?? crypto.randomUUID();
    const slots = (headers.get(UPGRADE_HEADERS.slots) ?? "").split(",").filter((k): k is SlotKind => k === "pending" || k === "public");
    return this.host.accept(headers.get(UPGRADE_HEADERS.publicTransport) === "1", connectionId, headers.get(UPGRADE_HEADERS.address) ?? UNKNOWN_ADDRESS, slots);
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.host.message(ws, message);
    this.reportCapacity();
  }

  override async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    await this.host.closed(ws, code, reason);
    this.reportCapacity(this.host.sessions.size === 0);
  }

  override async webSocketError(ws: WebSocket): Promise<void> {
    await this.host.closed(ws);
    this.reportCapacity(this.host.sessions.size === 0);
  }

  override async alarm(): Promise<void> {
    await this.loadPlacement();
    await this.host.alarm();
    await this.drainTick();
    this.host.reschedule();
    this.reportCapacity();
  }

  // ---- RPC from tenant objects and the deploy path ---------------------------------

  /** One commit for this gateway's sockets of `batch.tenantId`. */
  async deliver(batch: DeliveryBatch): Promise<DeliveryReceipt> {
    const sockets = this.byTenant.get(batch.tenantId);
    if (!sockets?.size) return { v: PROTOCOL_VERSION, sockets: 0 };
    const changed = new Set(batch.membershipChanged ?? []);
    const all = batch.all ?? [];
    const work: Array<Promise<void>> = [];
    for (const ws of sockets) {
      const session = this.host.sessions.get(ws);
      const tenant = session?.record.tenant;
      if (!session || !tenant) continue;
      if (changed.has(tenant.member.id)) { work.push(session.membershipChanged(batch.revision)); continue; }
      const own = batch.byClass?.[deliveryClass(tenant)];
      work.push(session.deliver(own ? [...all, ...own] : all, batch.revision));
    }
    await Promise.all(work);
    this.host.reschedule();
    return { v: PROTOCOL_VERSION, sockets: sockets.size };
  }

  /** A deploy changed the project's module: `system.reload` or `client.updateRequired` (A.1.5 c). */
  async moduleReloaded(projectId: string, module: ModuleInfo | null): Promise<number> {
    const n = await this.host.moduleReloaded(projectId, module);
    this.host.reschedule();
    return n;
  }

  /**
   * K14: one ordered presence delivery from a tenant object: subscribe
   * snapshots for named sockets, then one patch (encoded once by the tenant)
   * or a resync for every subscribed socket of that tenant here.
   */
  async presence(delivery: PresenceDelivery): Promise<PresenceReceipt> {
    const sockets = this.byTenant.get(delivery.tenantId);
    let subscribers = 0;
    if (sockets?.size) {
      const work: Array<Promise<void>> = [];
      for (const ws of sockets) {
        const session = this.host.sessions.get(ws);
        if (!session || session.record.tenant?.route.projectId !== delivery.projectId) continue;
        if (session.record.presence?.subscription != null) subscribers++;
        work.push(session.presenceDelivery(delivery));
      }
      // Enqueued in delivery order on each socket's own queue; not awaited, so one socket busy with a
      // slow call never holds the tenant's channel (Rust drains each connection's slot on its own).
      this.ctx.waitUntil(Promise.all(work));
    }
    return { v: PRESENCE_PROTOCOL_VERSION, subscribers };
  }

  /** K14: this gateway's sockets of `tenantId` with presence, from their records (the tenant rebuilds and sweeps from it). */
  async presenceConnections(tenantId: string): Promise<ConnectionReport[]> {
    const out: ConnectionReport[] = [];
    for (const ws of this.byTenant.get(tenantId) ?? []) {
      const report = this.host.sessions.get(ws)?.presence.report();
      if (report) out.push(report);
    }
    return out;
  }

  /**
   * K10: one replica delivery from a tenant object (`kernel/delivery`): each
   * socket's work is applied at once, in the order the tenant sent it (the
   * tenant sends one payload at a time per gateway). Not queued behind frames
   * the socket is still handling, so a call's reply never waits for, nor
   * overtakes, the commit frames the tenant already delivered.
   */
  async deliverReplica(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    const started = performance.now();
    const sockets = this.byTenant.get(tenantId);
    const seen = new Set<string>();
    if (payload.recovery) {
      const sessions = [...(sockets ?? [])].map(ws => this.host.sessions.get(ws))
        .filter((session): session is NonNullable<typeof session> => Boolean(session && !session.record.tenant?.anonymous && session.record.tenant?.route.tenantId === tenantId));
      const receivers = sessions.map(session => session as unknown as {
        recoverReplica?: (tenantId: string, payload: ReplicaDelivery) => Promise<void>;
      });
      // Skew is fail-closed: an old session must not be mistaken for an applied whole-holder barrier.
      if (receivers.some(session => typeof session.recoverReplica !== "function")) return { v: REPLICA_DELIVERY_VERSION, sockets: sessions.length };
      await Promise.all(receivers.map(session => session.recoverReplica!(tenantId, payload)));
      this.host.reschedule();
      this.metrics.delivery(performance.now() - started, payload.pressure?.queueAgeMs, payload.pressure?.rpcMs);
      this.reportCapacity();
      return { v: REPLICA_DELIVERY_VERSION, sockets: sessions.length, recoveryAck: 1 };
    }
    const membership = payload.membership;
    const work: Array<Promise<void>> = [];
    for (const ws of sockets ?? []) {
      const session = this.host.sessions.get(ws);
      const tenant = session?.record.tenant;
      if (!session || !tenant || tenant.anonymous) continue;
      const id = session.record.connectionId;
      seen.add(id);
      if (membership && membership.revision > tenant.admissionRevision
        && (membership.members.includes(tenant.member.id) || membership.accounts.includes(tenant.identity.account.id))) {
        work.push(session.membershipChanged(membership.revision));
        continue;
      }
      session.replicaDelivery(payload, payload.sockets[id]);
    }
    if (work.length) this.ctx.waitUntil(Promise.all(work).then(() => this.host.reschedule()));
    this.host.reschedule();
    this.metrics.delivery(performance.now() - started, payload.pressure?.queueAgeMs, payload.pressure?.rpcMs);
    this.reportCapacity();
    const missing = Object.keys(payload.sockets).filter((id) => !seen.has(id));
    return { v: REPLICA_DELIVERY_VERSION, sockets: seen.size, ...(missing.length ? { missing } : {}) };
  }

  /** K16: which of these connections are open here (the address limiter's audit of leaked slots). */
  async liveConnections(connectionIds: string[]): Promise<string[]> {
    return this.host.liveConnections(connectionIds);
  }

  /** Liveness of the RPC path itself; used by the hop benchmark. */
  async ping(): Promise<number> { return Date.now(); }

  private async loadPlacement(): Promise<void> {
    if (this.placementEpoch) return;
    this.draining = await this.ctx.storage.get<DrainCommand & { nextAt: number }>("autoscale-drain");
    this.drainResult = await this.ctx.storage.get<DrainResult>("autoscale-drain-result") ?? this.drainResult;
    const stored = await this.ctx.storage.get<{ epoch: number; cell: number }>("autoscale-placement");
    if (stored) { this.placementEpoch = stored.epoch; this.placementCell = stored.cell; }
  }

  /** Authoritative open-socket count on wake; no controller estimate can retire a live holder. */
  async capacity(epoch: number): Promise<CapacitySample> {
    await this.loadPlacement();
    if (this.placementEpoch > epoch || (this.placementEpoch < epoch && this.host.sessions.size)) throw new Error("gateway epoch mismatch");
    // A new/reused empty holder is probed before its first admission; no state is changed by a probe.
    return this.metrics.snapshot(epoch, this.host.sessions.size);
  }

  /** Idempotent command. Session eligibility is supplied by K22, never duplicated here. */
  async drain(command: DrainCommand): Promise<DrainResult> {
    await this.loadPlacement();
    if (command.v !== 1 || command.epoch !== this.placementEpoch) return this.drainResult;
    if (!this.draining) {
      this.draining = { ...command, nextAt: Math.max(Date.now(), command.notBefore) + (hashConnection(`${this.gatewayName}:${this.placementEpoch}`) % 1000) };
      await this.ctx.storage.put("autoscale-drain", this.draining);
    }
    await this.drainTick();
    this.host.reschedule();
    return this.drainResult;
  }

  async drainStatus(): Promise<DrainResult> { await this.loadPlacement(); return this.drainResult; }

  private async drainTick(): Promise<void> {
    const drain = this.draining;
    if (!drain || Date.now() < drain.nextAt || this.drainTickRunning) return;
    this.drainTickRunning = true;
    try {
    const blocked: Record<string, number> = {};
    let closed = 0;
    try {
      if (this.placementCell === undefined || !await withDeadline(controller(this.env, this.placementCell).drainReady(this.gatewayName, this.placementEpoch, drain.replacement))) throw new Error("controller unavailable");
      // Recheck replacement each cohort. Auth/limiter-wide health remains a V03 measured gate.
      const replacement = this.env.GATEWAY && (this.env.GATEWAY as DurableObjectNamespace).getByName(drain.replacement) as unknown as { capacity(epoch: number): Promise<CapacitySample> };
      if (!replacement) throw new Error("replacement unavailable");
      if (pressured(await withDeadline(replacement.capacity(drain.replacementEpoch)))) throw new Error("replacement pressured");
      const sessions = [...this.host.sessions.values()];
      const candidates = Array.from({ length: Math.min(2, sessions.length) }, (_, i) => sessions[(this.drainOffset + i) % sessions.length]!);
      this.drainOffset = (this.drainOffset + candidates.length) % Math.max(1, sessions.length);
      const items = candidates.map(s => s.revalidationItem()).filter(i => i !== null);
      const controlPort = this.bindings().ports(this.env).control;
      const valid = await withDeadline(controlPort.revalidate(items));
      if (valid.length !== items.length || valid.some(v => !v)) throw new Error("auth not healthy");
      for (const session of candidates) {
        const scope = session.publicScope();
        if (scope && !(await withDeadline(controlPort.resolvePublicTenant(scope[0], scope[1]))).ok) throw new Error("public auth not healthy");
      }
      const limits = publicLimits(this.env as unknown as import("../../public/config").PublicLimitsEnv);
      for (const address of new Set(candidates.map(s => s.record.address ?? UNKNOWN_ADDRESS))) {
        if (!this.env.PUBLIC_LIMITER) continue;
        const counts = await withDeadline(limiterFor(this.env.PUBLIC_LIMITER, address).counts());
        if (counts.pending + candidates.length > limits.pendingPerAddress || counts.public > limits.connectionsPerAddress) throw new Error("limiter not healthy");
      }
      for (const session of candidates) {
        if (closed >= 2) break;
        const eligible = session as unknown as { gatewayDrain?(): { closed: boolean; blocked?: string } };
        const result = eligible.gatewayDrain?.() ?? { closed: false, blocked: "session-hook-unavailable" };
        if (result.closed) closed++;
        else { const reason = result.blocked ?? "inflight"; blocked[reason] = (blocked[reason] ?? 0) + 1; }
      }
    } catch { blocked["replacement-or-admission-unavailable"] = this.host.sessions.size; }
    this.drainResult = { closed, blocked, remaining: this.host.sessions.size };
    await this.ctx.storage.put("autoscale-drain-result", this.drainResult);
    if (!this.host.sessions.size) {
      this.draining = undefined;
      await this.ctx.storage.delete("autoscale-drain");
      this.reportCapacity(true);
    } else {
      // No tight retry for a long Action: defer a full 30s when no eligible socket was found.
      drain.nextAt = Date.now() + (closed ? 5_000 : 30_000) + (hashConnection(`${this.gatewayName}:${this.placementEpoch}`) % 1000);
      await this.ctx.storage.put("autoscale-drain", drain);
    }
    } finally { this.drainTickRunning = false; }
  }

  private reportCapacity(force = false): void {
    if (!this.env.GATEWAY_CONTROLLER) return;
    if (this.reportInFlight) { this.reportAgain ||= force; return; }
    if (!force && !this.metrics.due()) return;
    this.reportInFlight = true;
    this.ctx.waitUntil((async () => {
      await this.loadPlacement();
      if (this.placementCell === undefined) return;
      const sample = this.metrics.snapshot(this.placementEpoch, this.host.sessions.size);
      this.metrics.rotate();
      try { await controller(this.env, this.placementCell).observe(this.gatewayName, sample); }
      catch { /* Existing sessions continue. The next active event reports; no idle retry timer. */ }
    })().finally(() => {
      this.reportInFlight = false;
      if (this.reportAgain) { this.reportAgain = false; this.reportCapacity(true); }
    }));
  }

  // ---- tenant registration ------------------------------------------------------------

  private index(tenantId: string): Set<WebSocket> {
    let set = this.byTenant.get(tenantId);
    if (!set) this.byTenant.set(tenantId, set = new Set());
    return set;
  }

  private tenantChanged(ws: WebSocket, previous: TenantSession | null, next: TenantSession | null): void {
    if (previous && !previous.anonymous) {
      const set = this.byTenant.get(previous.route.tenantId);
      if (set?.delete(ws) && set.size === 0) {
        this.byTenant.delete(previous.route.tenantId);
        this.link(previous.route, "detach");
      }
    }
    if (next && !next.anonymous) {
      const set = this.index(next.route.tenantId);
      const first = set.size === 0;
      set.add(ws);
      if (first) this.link(next.route, "attach");
    }
  }

  /** Attach/detach calls for one tenant run in order. */
  private link(route: TenantRoute, action: "attach" | "detach"): void {
    const key = route.tenantId;
    const previous = this.links.get(key) ?? Promise.resolve();
    const run = async () => {
      const link = this.bindings().tenantLink(this.env, route);
      // A detach that raced a new socket of the same tenant is skipped.
      if (action === "detach" && this.byTenant.get(key)?.size) return;
      if (action === "attach") await link.attachGateway(this.gatewayName);
      else await link.detachGateway(this.gatewayName);
    };
    const next = previous.then(run, run).catch((error) => console.error(`gateway ${this.gatewayName}: ${action} ${key} failed`, error));
    this.links.set(key, next);
    this.ctx.waitUntil(next.finally(() => { if (this.links.get(key) === next) this.links.delete(key); }));
  }
}

/**
 * The tenant object's side: the set of gateways holding its sockets, and the
 * fan-out of one commit to each of them once. Embedded by the tenant object.
 */
export class TenantGateways {
  private names: Set<string> | undefined;
  constructor(
    private readonly storage: DurableObjectStorage,
    private readonly gateway: (name: string) => { deliver(batch: DeliveryBatch): Promise<DeliveryReceipt> },
  ) {}

  private async load(): Promise<Set<string>> {
    if (!this.names) {
      const stored = await this.storage.list<number>({ prefix: "gateway:" });
      this.names = new Set([...stored.keys()].map(k => k.slice("gateway:".length)));
    }
    return this.names;
  }

  async attach(name: string): Promise<void> {
    const names = await this.load();
    if (names.has(name)) return;
    names.add(name);
    await this.storage.put(`gateway:${name}`, 1);
  }

  async detach(name: string): Promise<void> {
    const names = await this.load();
    if (!names.delete(name)) return;
    await this.storage.delete(`gateway:${name}`);
  }

  async list(): Promise<string[]> { return [...await this.load()]; }

  /** Sends one commit to every registered gateway, once each, in parallel. Gateways with no socket left are dropped. */
  async deliver(batch: Omit<DeliveryBatch, "v">): Promise<{ gateways: number; sockets: number }> {
    const names = [...await this.load()];
    const full: DeliveryBatch = { v: PROTOCOL_VERSION, ...batch };
    const receipts = await Promise.allSettled(names.map(name => this.gateway(name).deliver(full)));
    let sockets = 0;
    await Promise.all(receipts.map(async (r, i) => {
      if (r.status === "fulfilled") {
        sockets += r.value.sockets;
        if (r.value.sockets === 0) await this.detach(names[i]!);
      } else {
        console.error(`tenant: delivery to ${names[i]} failed`, r.reason);
      }
    }));
    return { gateways: names.length, sockets };
  }
}
