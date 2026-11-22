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
import { setDeliveryTiming } from "../../delivery/batcher";
import { ASSIGNMENT_HEADER, DIRECT_HEADER, MAX_SOCKETS, controller, isDirectRefusal, withDeadline, parseAssignment, parseDirect, type DirectAdmission, type AutoscaleEnv, type CapacitySample, type DrainCommand, type DrainResult, pressured, hashConnection } from "../../gateway/placement";
import { GatewayMetrics } from "../../gateway/metrics";
import { DurableObject } from "cloudflare:workers";
import type { ModuleInfo, Ports, TenantRoute, TenantSession } from "../ports";
import { SocketHost } from "../host";
import { deliveryClass } from "../session";
import { type ConnectionReport, type PresenceEnv, presenceConfig } from "../../presence/hub";
import { type TelemetryEnv, telemetryHostFromEnv } from "../../telemetry/env";
import { PRESENCE_PROTOCOL_VERSION, type PresenceDelivery, type PresenceReceipt } from "../../presence/tenant";
import { validRecoveryInitialDelivery, initialDeliveryOnly, initialFailureOnly, validInitialCompletions, REPLICA_DELIVERY_VERSION, type InitialCompletionStatus, type ReplicaDelivery, type ReplicaReceipt, type SocketWork } from "../../delivery/protocol";
import type { ReplicaBatchResult } from "../../delivery/outlet";
import { UNKNOWN_ADDRESS } from "../../public/address";
import { type PublicGuard, admitUpgrade, publicGuard, LIMITER_UNAVAILABLE } from "../../public/guard";
import { publicLimits } from "../../public/config";
import { limiterFor } from "../../public/limiter";
import type { SlotKind } from "../../public/limiter";
import { GatewayRoutingIdentity } from "./routing-identity";
import { CallOrigin, gatedEnv, trackEntries, type OriginStats } from "./origin";
import { setSpanTiming, span, spanTimingFromEnv } from "../../delivery/spans";

/**
 * Helpers only the gateway itself calls. Every other method is an inbound request
 * (X02 entry tracking); an unlisted helper only makes the gate more conservative.
 */
const INTERNAL_METHODS: ReadonlySet<string> = new Set([
  "accept", "applyReplica", "applyReplicaNow", "applyRecovery", "receiveReplica", "replicaGate", "loadPlacement", "drainTick", "reportCapacity", "index", "tenantChanged", "link",
  "bindings", "alarmEntry", "refreshControlWatches",
]);
const tracked = new WeakSet<object>();

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
  projectAuthenticated?(env: unknown, projectId: string, holderId: string): Promise<void>;
  /**
   * X15: `projectAuthenticated` and the module read after it in ONE Control call (the holder is registered
   * first, in the same Control turn as the read). Absent: the two separate calls.
   */
  projectAuthenticatedModule?(env: unknown, projectId: string, holderId: string): Promise<ModuleInfo | null>;
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

/** Content-free, finite diagnostics. Never echo the assignment or its identifiers. */
export const ADMISSION_REASON_HEADER = "x-gonvex-admission-reason";
type AdmissionReason = "parse-invalid" | "holder-identity" | "connection-mismatch" | "identity-unavailable"
  | "admission-busy" | "capacity" | "claim-refused" | "epoch-busy"
  /** X17g: a direct upgrade this holder does not take; the router falls back to the controller's `acquire`. */
  | "direct-invalid" | "direct-identity" | "direct-unavailable" | "direct-busy" | "direct-refused";
/** X17g: the router retries a refusal with this prefix through the controller (it is not the client's answer). */
export const DIRECT_REFUSAL_PREFIX = "direct-";
/** X17g: a direct admission's whole budget at the holder, under the router's 2 s hop deadline (#533 review). */
export const DIRECT_BUDGET_MS = 1_500;
function refusedAdmission(reason: AdmissionReason, body = "gateway assignment required"): Response {
  return new Response(body, { status: 503, headers: { [ADMISSION_REASON_HEADER]: reason } });
}

/**
 * Gateway object. Sockets of any tenant; the class is exported by the Worker
 * entry and bound as `GATEWAY`.
 */
export abstract class GatewayObjectBase<E extends GatewayEnv> extends DurableObject<E> {
  protected readonly host: SocketHost;
  private readonly routingIdentity: GatewayRoutingIdentity | undefined;
  get gatewayName(): string { return this.routingIdentity?.name ?? this.ctx.id.name ?? this.ctx.id.toString(); }
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
  /** X02: every outgoing call starts from a fresh event (see `origin.ts`). */
  private readonly origin: CallOrigin;
  private alarmBody: Promise<void> | undefined;

  protected abstract bindings(): GatewayBindings;

  constructor(ctx: DurableObjectState, rawEnv: E) {
    let wake = () => {};
    const origin = new CallOrigin(() => wake());
    // A0 SPIKE: a holder embedded in a tenant object (kernel/assembly/a0-local-holder.ts) is not gated.
    const ungated = (new.target as unknown as { ungated?: boolean }).ungated === true;
    // Every binding this object (and its sessions, ports and subclasses) calls through `this.env` is gated.
    super(ctx, ungated ? rawEnv : gatedEnv(rawEnv, origin));
    this.origin = origin;
    const env = this.env;
    // X13e: time-only per-batch send log on dev (GONVEX_DELIVERY_TIMING=1); slow batches are always logged.
    // X13i: GONVEX_SPAN_TIMING=1 adds the per-call spans (kernel/delivery/spans.ts) and every batch's send line.
    if (spanTimingFromEnv(rawEnv)) setSpanTiming(true);
    setDeliveryTiming((rawEnv as { GONVEX_DELIVERY_TIMING?: string }).GONVEX_DELIVERY_TIMING === "1" || spanTimingFromEnv(rawEnv));
    const proto = new.target.prototype as object;
    if (!ungated && !tracked.has(proto)) {
      tracked.add(proto);
      trackEntries(proto, DurableObject.prototype, INTERNAL_METHODS, (self) => (self as GatewayObjectBase<E>).origin,
        (name, original) => name === "alarm"
          ? function (this: object) { return (this as GatewayObjectBase<E>).alarmEntry(original as () => Promise<void>); }
          : undefined);
    }
    this.routingIdentity = env.GATEWAY_CONTROLLER ? new GatewayRoutingIdentity(ctx, env.GATEWAY) : undefined;
    const presence = presenceConfig(env);
    const publicGuard = this.bindings().publicGuard?.(env);
    const gateway = this;
    this.host = new SocketHost({
      ...(publicGuard ? { publicGuard } : {}),
      presence,
      sent: (text) => this.metrics.sent(text),
      placementWake: () => this.draining?.nextAt ?? null,
      telemetry: telemetryHostFromEnv(env, presence.enabled),
      ctx,
      ports: this.bindings().ports(env),
      // A fresh ID-only holder learns its name only after the controller claim,
      // before creating a Session. Restored sessions use the validated durable name.
      get name() { return gateway.gatewayName; },
      ready: { runtimeVersion: env.GONVEX_RUNTIME_VERSION || "development", presence: presence.enabled },
      tenantChanged: (ws, previous, next) => this.tenantChanged(ws, previous, next),
      projectAuthenticated: (project) => this.bindings().projectAuthenticated?.(env, project, ctx.id.toString()) ?? Promise.resolve(),
      ...(this.bindings().projectAuthenticatedModule
        ? { projectAuthenticatedModule: (project: string) => this.bindings().projectAuthenticatedModule!(env, project, ctx.id.toString()) } : {}),
    });
    // Restored sockets: rebuild the tenant index only (the tenant already has this gateway registered).
    for (const [ws, session] of this.host.sessions) {
      const tenant = session.record.tenant;
      if (tenant && !tenant.anonymous) this.index(tenant.route.tenantId).add(ws);
    }
    wake = () => this.host.wakeNow();
    // Calls made while constructing (before any request was seen) wait for an alarm.
    if (this.origin.stats().waiting) wake();
  }

  /**
   * X02: the alarm is a fresh event, so it releases waiting calls first. An alarm armed
   * only for that re-arms the host's own alarm and returns. If a call has to wait while
   * the alarm body runs, the handler returns and the body continues: the platform runs
   * no new alarm while a handler is running, and the waiting call needs one.
   */
  private async alarmEntry(original: () => Promise<void>): Promise<void> {
    const entry = this.origin.enter("fresh");
    const stall = this.origin.stalled();
    try {
      if (this.host.wakeOnly()) return;
      if (this.alarmBody) {
        // The platform consumed this alarm, possibly with a deadline the host still has cached:
        // once the running body ends, forget the cache and arm the next host need again.
        this.ctx.waitUntil(this.alarmBody.then(() => this.host.rearm(), () => this.host.rearm()));
        return;
      }
      const body = original.call(this);
      this.alarmBody = body;
      const settled = body.finally(() => { if (this.alarmBody === body) this.alarmBody = undefined; });
      const yielded = await Promise.race([settled.then(() => false), stall.promise.then(() => true)]);
      if (yielded) this.ctx.waitUntil(settled.catch((error) => console.error("gateway: alarm failed after yielding to a waiting call", error)));
    } finally {
      stall.cancel();
      this.origin.exit(entry);
    }
  }

  /** X02 test seam: how many outgoing calls launched at once or waited for a fresh event. */
  originStats(): OriginStats { return this.origin.stats(); }

  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("expected a WebSocket upgrade", { status: 400 });
    const headers = request.headers;
    if (this.env.GATEWAY_CONTROLLER) {
      if (headers.has(DIRECT_HEADER)) {
        const direct = parseDirect(headers.get(DIRECT_HEADER));
        return direct ? this.admitDirect(direct, headers) : refusedAdmission("direct-invalid");
      }
      const assignment = parseAssignment(headers.get(ASSIGNMENT_HEADER));
      if (!assignment) return refusedAdmission("parse-invalid");
      if (!this.routingIdentity!.matches(assignment.gateway)) return refusedAdmission("holder-identity");
      if (assignment.connectionId !== headers.get(UPGRADE_HEADERS.connectionId)) return refusedAdmission("connection-mismatch");
      // A pre-upgrade legacy holder with neither a stored nor platform name
      // cannot relabel already-restored sessions from an incoming assignment.
      if (!this.routingIdentity!.known && this.host.sessions.size) return refusedAdmission("identity-unavailable");
      // No socket migration: an admitted socket owns this holder until normal disconnect.
      // Serialize claim + accept, so parallel upgrades cannot race the socket ceiling.
      if (this.admissionWaiting >= 16) return refusedAdmission("admission-busy", "gateway admission busy");
      this.admissionWaiting++;
      const queued = Date.now();
      const accepted = this.admissionTail.then(async () => {
        const started = Date.now();
        await this.loadPlacement();
        if (assignment.epoch < this.placementEpoch || this.host.sessions.size >= MAX_SOCKETS) return refusedAdmission("capacity", "gateway full");
        const claiming = Date.now();
        if (!await withDeadline(controller(this.env, assignment.cell).claim(assignment))) return refusedAdmission("claim-refused", "gateway assignment expired");
        const claimMs = Date.now() - claiming;
        queueMicrotask(() => span("gateway.admit", { waitMs: started - queued, placementMs: claiming - started, claimMs, totalMs: Date.now() - queued, sockets: this.host.sessions.size }));
        if (assignment.epoch !== this.placementEpoch) {
          if (this.host.sessions.size) return refusedAdmission("epoch-busy", "gateway epoch busy");
          this.draining = undefined;
          await this.ctx.storage.delete("autoscale-drain");
          this.placementEpoch = assignment.epoch;
          this.placementCell = assignment.cell;
          await this.ctx.storage.put("autoscale-placement", { epoch: this.placementEpoch, cell: this.placementCell });
        }
        this.routingIdentity!.claimed(assignment.gateway);
        const response = this.accept(headers);
        this.reportCapacity();
        return response;
      }).finally(() => { this.admissionWaiting--; });
      this.admissionTail = accepted.catch(() => undefined);
      return accepted;
    }
    return this.accept(headers);
  }

  /**
   * X17g: an upgrade the router sent straight here (`DIRECT_HEADER`): this holder already serves this cell at this
   * epoch, so one controller call (`admitDirect`: acquire and claim at once) admits it, while the address limiter
   * reserves its slots first (the router did neither). Anything this holder does not take is refused with a
   * `direct-*` reason and the router falls back to `acquire`. A limiter refusal is the client's answer (429/503, as
   * the router sends it) and no lease is taken for it.
   */
  private admitDirect(direct: DirectAdmission, headers: Headers): Promise<Response> | Response {
    const identity = this.routingIdentity!;
    if (!identity.known || !identity.matches(direct.gateway)) return refusedAdmission("direct-identity");
    const connectionId = headers.get(UPGRADE_HEADERS.connectionId);
    if (!connectionId || direct.connectionId !== connectionId) return refusedAdmission("direct-invalid");
    if (this.admissionWaiting >= 16) return refusedAdmission("direct-busy", "gateway admission busy");
    this.admissionWaiting++;
    const queued = Date.now();
    const accepted = this.admissionTail.then(async () => {
      const started = Date.now();
      await this.loadPlacement();
      if (direct.epoch !== this.placementEpoch || direct.cell !== this.placementCell || this.draining
        || this.host.sessions.size >= MAX_SOCKETS) return refusedAdmission("direct-unavailable", "gateway full");
      const address = headers.get(UPGRADE_HEADERS.address) ?? UNKNOWN_ADDRESS;
      const publicTransport = headers.get(UPGRADE_HEADERS.publicTransport) === "1";
      const kinds: SlotKind[] = this.env.PUBLIC_LIMITER ? (publicTransport ? ["pending", "public"] : ["pending"]) : [];
      // The limiter first, then the lease only for an admitted upgrade (#533 review): a refused upgrade must hold no
      // claimed lease (`release` keeps claimed leases until they expire). Both within the router's deadline: a budget
      // spent waiting here refuses (`direct-busy`) instead of admitting after the router gave up.
      const budget = DIRECT_BUDGET_MS - (Date.now() - queued);
      if (budget < 200) return refusedAdmission("direct-busy", "gateway admission busy");
      const deadline = Date.now() + budget;
      // X17g follow-up (#533 re-review): releases never wait on this holder's admission queue; idempotent, bounded.
      const release = () => { if (kinds.length) this.ctx.waitUntil(withDeadline(publicGuard(this.env).release(address, connectionId, kinds)).catch(() => undefined)); };
      const limited = await withDeadline(admitUpgrade(this.env, { address, connectionId, gateway: direct.gateway, publicTransport, expiresAt: deadline }), budget)
        .catch(() => {
          // The limiter may still admit after this deadline: free what it may have reserved.
          release();
          return new Response(LIMITER_UNAVAILABLE, { status: 503, headers: { "content-type": "text/plain; charset=utf-8", [ADMISSION_REASON_HEADER]: "limiter-unavailable" } });
        });
      if (limited) return limited;
      const left = deadline - Date.now();
      let timedOut = false;
      const answer = left < 100 ? null : await withDeadline(controller(this.env, direct.cell).admitDirect({ ...direct, wantReason: true }, this.host.sessions.size), left).catch((error: unknown) => {
        timedOut = String((error as Error)?.message ?? error) === "gateway RPC deadline";
        console.error("gateway: direct admission failed", { cause: String((error as Error)?.message ?? error).slice(0, 120) });
        return null;
      });
      // #538 re-review: the controller names why it refused (an older controller answers null).
      const granted = answer && !isDirectRefusal(answer) ? answer : null;
      if (!granted) {
        span("gateway.admitDirect", { refused: isDirectRefusal(answer) ? answer.refused : left < 100 ? "budget" : timedOut ? "deadline" : "unknown", totalMs: Date.now() - queued });
        release();
        // A grant the controller writes after this deadline would stay a claimed lease for LEASE_MS while the router
        // falls back to `acquire` (#533 re-review): cancel it. The controller serializes calls, so the cancel runs
        // after a late grant.
        if (timedOut) this.ctx.waitUntil(withDeadline(controller(this.env, direct.cell).cancelDirect(direct)).catch(() => undefined));
        return refusedAdmission("direct-refused", "gateway assignment expired");
      }
      queueMicrotask(() => span("gateway.admitDirect", { waitMs: started - queued, admitMs: Date.now() - started, totalMs: Date.now() - queued, sockets: this.host.sessions.size }));
      identity.claimed(direct.gateway);
      try {
        const response = this.host.accept(publicTransport, connectionId, address, kinds);
        this.reportCapacity();
        return response;
      } catch (error) { release(); throw error; }
    }).finally(() => { this.admissionWaiting--; });
    this.admissionTail = accepted.catch(() => undefined);
    return accepted;
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
  /** Additive marker-only method: old receivers fail before ordinary apply. */
  async deliverInitialFailures(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    if (!initialFailureOnly(payload)) throw new Error("invalid initial-failure-only delivery");
    const pending = new Set(Object.keys(payload.initialFailure!.requests));
    const work: Array<Promise<void>> = [];
    for (const ws of this.byTenant.get(tenantId) ?? []) {
      const session = this.host.sessions.get(ws);
      if (!session || session.closed || session.record.tenant?.route.tenantId !== tenantId) continue;
      const refs = payload.initialFailure!.requests[session.record.connectionId];
      if (!refs) continue;
      pending.delete(session.record.connectionId);
      work.push(session.failReplicaInitials(tenantId, refs));
    }
    await Promise.all(work);
    this.host.reschedule();
    return { v: REPLICA_DELIVERY_VERSION, sockets: work.length, initialFailureAck: 1,
      ...(pending.size ? { missing: [...pending] } : {}) };
  }

  /**
   * X14: the tenant's queued payloads for this holder in one ordered RPC. Each is applied in order through the
   * receiver its single RPC would have reached (initial failures, initial work, ordinary work), so every check and
   * effect is that RPC's; one failing item is answered with its error and does not stop the ones after it, as
   * separate RPCs did not. The output gate then holds ONE reply for the whole batch instead of one per page.
   */
  async deliverReplicaBatch(tenantId: string, payloads: ReplicaDelivery[]): Promise<ReplicaBatchResult[]> {
    if (!Array.isArray(payloads)) throw new Error("invalid replica batch");
    const results: ReplicaBatchResult[] = [];
    const failed = (error: unknown): ReplicaBatchResult => {
      const failure = error as (Error & { code?: unknown }) | undefined;
      return { error: String(failure?.message ?? error), ...(failure?.name ? { name: failure.name } : {}),
        ...(typeof failure?.code === "string" ? { code: failure.code } : {}) };
    };
    // X15: consecutive items that apply without I/O run in one synchronous turn, so the recovery writes their
    // completions need are coalesced into one per socket (`SocketHost.batchPersists`), issued in that same turn
    // as their frames. An item that has to wait (initial failures, recovery, a bootstrap, the dev fixture gate)
    // ends the run: the run's write is issued before that wait, and the item continues alone.
    let next = 0;
    while (next < payloads.length) {
      let waiting: Promise<ReplicaReceipt> | undefined;
      this.host.batchPersists(() => {
        while (next < payloads.length && !waiting) {
          const payload = payloads[next++]!;
          try {
            const receipt = this.receiveReplica(tenantId, payload);
            if (receipt instanceof Promise) waiting = receipt;
            else results.push({ receipt });
          } catch (error) {
            results.push(failed(error));
          }
        }
      });
      if (waiting) {
        try { results.push({ receipt: await waiting }); } catch (error) { results.push(failed(error)); }
      }
    }
    return results;
  }

  /** The receiver the item's own single RPC would reach; a receipt when it applied without waiting. */
  private receiveReplica(tenantId: string, payload: ReplicaDelivery): ReplicaReceipt | Promise<ReplicaReceipt> {
    if (payload.initialFailure) return this.deliverInitialFailures(tenantId, payload);
    if (Object.values(payload.sockets).some(work => work.initialScope || work.initialCompletions?.length)) {
      if (!initialDeliveryOnly(payload)) throw new Error("invalid initial-only delivery");
    }
    const gate = this.replicaGate(tenantId, payload);
    return gate ? gate.then(() => this.applyReplicaNow(tenantId, payload)) : this.applyReplicaNow(tenantId, payload);
  }

  /** Dev fixture seam: a promise holds this delivery until it settles (the base never holds one). */
  protected replicaGate(_tenantId: string, _payload: ReplicaDelivery): Promise<void> | undefined {
    return undefined;
  }

  async deliverReplicaInitial(tenantId:string,payload:ReplicaDelivery):Promise<ReplicaReceipt> {
    if(!initialDeliveryOnly(payload))throw new Error("invalid initial-only delivery");
    const gate = this.replicaGate(tenantId, payload);
    if (gate) await gate;
    return this.applyReplicaNow(tenantId,payload);
  }
  async deliverReplica(tenantId:string,payload:ReplicaDelivery):Promise<ReplicaReceipt> {
    if(Object.values(payload.sockets).some(work=>work.initialScope||work.initialCompletions?.length))throw new Error("initial work requires the initial-only receiver");
    const gate = this.replicaGate(tenantId, payload);
    if (gate) await gate;
    return this.applyReplicaNow(tenantId,payload);
  }
  /** Applies one delivery. Synchronous unless it has to wait (recovery, a bootstrap); then a promise. */
  private applyReplicaNow(tenantId: string, payload: ReplicaDelivery): ReplicaReceipt | Promise<ReplicaReceipt> {
    const sentAt = payload.pressure?.sentAt;
    if (typeof sentAt === "number") span("gateway.receive", { transitMs: Date.now() - sentAt, queueAgeMs: payload.pressure?.queueAgeMs ?? 0 });
    if (payload.initialFailure) throw new Error("initial failures require the marker-only receiver");
    if (!validRecoveryInitialDelivery(payload)) throw new Error("invalid recovery initial offer");
    if (Object.values(payload.sockets).some(work=>!validInitialCompletions(work))) throw new Error("invalid initial completion metadata");
    const initialCompletions: Record<string, readonly InitialCompletionStatus[]> = Object.create(null);
    const started = performance.now();
    const sockets = this.byTenant.get(tenantId);
    const seen = new Set<string>();
    if (payload.recovery) return this.applyRecovery(tenantId, payload, sockets, started);
    const membership = payload.membership;
    const work: Array<Promise<void>> = [];
    const scopeRequired: Record<string, number> = {};
    const scopeAck: Record<string, number> = {};
    const bootstraps: Array<Promise<void>> = [];
    const bootstrapScopes=new Map<string,{session:NonNullable<ReturnType<SocketHost["sessions"]["get"]>>;epoch:number|undefined}>();
    const wholeHolderChallenge = !Object.keys(payload.sockets).length && !payload.shared.length
      && payload.watermark === undefined && !payload.membership;
    for (const ws of sockets ?? []) {
      const session = this.host.sessions.get(ws);
      const tenant = session?.record.tenant;
      if (!session || !tenant || tenant.anonymous) continue;
      const id = session.record.connectionId;
      seen.add(id);
      if (membership && membership.revision > tenant.admissionRevision
        && (membership.members.includes(tenant.member.id) || membership.accounts.includes(tenant.identity.account.id))) {
        work.push(session.membershipChanged(membership.watermark ?? membership.revision));
        continue;
      }
      const own = payload.sockets[id];
      if (!own) {
        if (payload.scopeFence === 1 && (wholeHolderChallenge || payload.watermark !== undefined)) {
          // A global watermark needs authority even for the same instance.
          // An unaddressed socket applies no instance, data or watermark.
          scopeRequired[id] = session.record.subscriptionEpoch ?? 0;
          continue;
        }
        // Targeted work for another socket grants no authority over this one.
        if (payload.watermark === undefined) continue;
      }
      if (payload.scopeFence === 1 && own && !own.ops.length && own.watermark === undefined && !own.opened && !own.removed
        && !payload.shared.length && payload.watermark === undefined && !payload.membership && !own.initialScope && !own.initialCompletions?.length) {
        const epoch = (own as SocketWork & { subscriptionEpoch?: number }).subscriptionEpoch;
        bootstrapScopes.set(id,{session,epoch});
        bootstraps.push(session.bootstrapReplica(tenantId, payload, own).then((facts) => {
          if (epoch !== undefined) scopeAck[id] = epoch;
          if(facts?.length)initialCompletions[id]=facts;
        }, (error) => {
          if ((error as Error)?.message !== "delivery recovery scope changed") throw error;
          // A scope switch is a fresh negative challenge, never an ack or an
          // automatic replay of this fence's epoch/data.
          if (!session.closed && session.record.tenant?.route.tenantId === tenantId) scopeRequired[id] = session.record.subscriptionEpoch ?? 0;
          else seen.delete(id);
        }));
        continue;
      }
      const completed = session.replicaDelivery(payload, own);
      if (own?.initialCompletions?.length) initialCompletions[id] = completed;
    }
    const finish = (): ReplicaReceipt => {
    for(const [id,{session,epoch}] of bootstrapScopes){
      if(session.closed||session.record.tenant?.route.tenantId!==tenantId||(session.record.subscriptionEpoch??0)!==epoch){
        delete scopeAck[id];delete initialCompletions[id];
        if(!session.closed&&session.record.tenant?.route.tenantId===tenantId)scopeRequired[id]=session.record.subscriptionEpoch??0;
        else seen.delete(id);
      }
    }
    if (work.length) this.ctx.waitUntil(Promise.all(work).then(() => this.host.reschedule()));
    this.host.reschedule();
    this.metrics.delivery(performance.now() - started, payload.pressure?.queueAgeMs, payload.pressure?.rpcMs);
    this.reportCapacity();
    const missing = Object.keys(payload.sockets).filter((id) => !seen.has(id));
    for (const id of missing) {
      const own = payload.sockets[id]!;
      if (!own.initialCompletions?.length) continue;
      const physical = [...this.host.sessions.values()].find(session=>session.record.connectionId===id);
      if (physical) {
        // Actual current physical scope superseded the old tenant/epoch.
        initialCompletions[id] = own.initialCompletions.map(c=>({generation:c.request.generation,status:"obsolete"}));
      } else {
        if (this.host.ownsConnection(id)) throw new Error("initial completion holder unavailable with retained owner");
        initialCompletions[id] = own.initialCompletions.map(c=>({generation:c.request.generation,status:"obsolete"}));
      }
    }
    return { v: REPLICA_DELIVERY_VERSION, sockets: seen.size, ...(missing.length ? { missing } : {}),
      ...(Object.keys(scopeRequired).length ? { scopeRequired } : {}), ...(Object.keys(scopeAck).length ? { scopeAck } : {}),
      ...(Object.keys(initialCompletions).length ? { initialCompletions: Object.fromEntries(Object.entries(initialCompletions)) } : {}) };
    };
    // Only a bootstrap waits; everything else was applied in this turn.
    return bootstraps.length ? Promise.all(bootstraps).then(finish) : finish();
  }

  private async applyRecovery(tenantId: string, payload: ReplicaDelivery, sockets: ReadonlySet<WebSocket> | undefined, started: number): Promise<ReplicaReceipt> {
    const initialCompletions: Record<string, readonly InitialCompletionStatus[]> = Object.create(null);
    const sessions = [...(sockets ?? [])].map(ws => this.host.sessions.get(ws))
      .filter((session): session is NonNullable<typeof session> => Boolean(session && !session.record.tenant?.anonymous && session.record.tenant?.route.tenantId === tenantId));
    const scopes=sessions.map(session=>session.record.subscriptionEpoch??0);
    const receivers = sessions.map(session => session as unknown as {
      recoverReplica?: (tenantId: string, payload: ReplicaDelivery) => Promise<readonly InitialCompletionStatus[]|void>;
    });
    // Skew is fail-closed: an old session must not be mistaken for an applied whole-holder barrier.
    if (receivers.some(session => typeof session.recoverReplica !== "function")) return { v: REPLICA_DELIVERY_VERSION, sockets: sessions.length };
    const facts=await Promise.all(receivers.map(session => session.recoverReplica!(tenantId, payload)));
    if(sessions.some((session,i)=>session.closed||session.record.tenant?.route.tenantId!==tenantId||(session.record.subscriptionEpoch??0)!==scopes[i]))throw new Error("delivery recovery scope changed");
    for(let i=0;i<sessions.length;i++)if(facts[i]?.length)initialCompletions[sessions[i]!.record.connectionId]=facts[i]!;
    this.host.reschedule();
    this.metrics.delivery(performance.now() - started, payload.pressure?.queueAgeMs, payload.pressure?.rpcMs);
    this.reportCapacity();
    return { v: REPLICA_DELIVERY_VERSION, sockets: sessions.length, recoveryAck: 1,
      ...(Object.keys(initialCompletions).length?{initialCompletions:Object.fromEntries(Object.entries(initialCompletions))}:{}) };
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
    } else if (this.draining.replacement !== command.replacement || this.draining.replacementEpoch !== command.replacementEpoch) {
      // The controller re-pointed this drain because its old replacement drained or retired (BUG-9 review).
      // Updated in place, so a tick in flight holding the same object persists the new target too.
      this.draining.replacement = command.replacement;
      this.draining.replacementEpoch = command.replacementEpoch;
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
    // Review r4609090617: the awaits above may have ended or replaced this drain. Only the drain this tick
    // started with is advanced or ended; a newer one (or none) is never overwritten with this stale copy.
    if (this.draining !== drain) return;
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
