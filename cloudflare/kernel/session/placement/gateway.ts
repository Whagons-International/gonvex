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
import { DurableObject } from "cloudflare:workers";
import type { ModuleInfo, Ports, TenantRoute, TenantSession } from "../ports";
import { SocketHost } from "../host";
import { deliveryClass } from "../session";
import { type ConnectionReport, type PresenceEnv, presenceConfig } from "../../presence/hub";
import { PRESENCE_PROTOCOL_VERSION, type PresenceDelivery, type PresenceReceipt } from "../../presence/tenant";
import { REPLICA_DELIVERY_VERSION, type ReplicaDelivery, type ReplicaReceipt } from "../../delivery/protocol";

/** S3: about 125 sockets per gateway carried 30 commits/s to 1,000 sockets. */
export const SOCKETS_PER_GATEWAY = 125;
export const DEFAULT_GATEWAY_SHARDS = 8;
export const PROTOCOL_VERSION = 1;

export interface GatewayEnv extends PresenceEnv {
  GONVEX_GATEWAY_SHARDS?: string;
  GONVEX_RUNTIME_VERSION?: string;
  GONVEX_PRESENCE?: string;
}

/**
 * Number of gateway shards. Size it as expected concurrent sockets / 125.
 * Stage 1 uses a fixed count (0004); X01 later grows it from frames per second.
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
}

/**
 * Gateway object. Sockets of any tenant; the class is exported by the Worker
 * entry and bound as `GATEWAY`.
 */
export abstract class GatewayObjectBase<E extends GatewayEnv> extends DurableObject<E> {
  protected readonly host: SocketHost;
  readonly gatewayName: string;
  /** tenantId -> sockets of that tenant here. */
  private readonly byTenant = new Map<string, Set<WebSocket>>();
  private readonly links = new Map<string, Promise<void>>();

  protected abstract bindings(): GatewayBindings;

  constructor(ctx: DurableObjectState, env: E) {
    super(ctx, env);
    this.gatewayName = ctx.id.name ?? ctx.id.toString();
    const presence = presenceConfig(env);
    this.host = new SocketHost({
      presence,
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
    const connectionId = request.headers.get("x-gonvex-connection-id") ?? crypto.randomUUID();
    return this.host.accept(request.headers.get("x-gonvex-public") === "1", connectionId);
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.host.message(ws, message);
  }

  override async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    await this.host.closed(ws, code, reason);
  }

  override async webSocketError(ws: WebSocket): Promise<void> {
    await this.host.closed(ws);
  }

  override async alarm(): Promise<void> {
    await this.host.alarm();
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
    const sockets = this.byTenant.get(tenantId);
    const seen = new Set<string>();
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
    const missing = Object.keys(payload.sockets).filter((id) => !seen.has(id));
    return { v: REPLICA_DELIVERY_VERSION, sockets: seen.size, ...(missing.length ? { missing } : {}) };
  }

  /** Liveness of the RPC path itself; used by the hop benchmark. */
  async ping(): Promise<number> { return Date.now(); }

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
    this.ctx.waitUntil(next);
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
