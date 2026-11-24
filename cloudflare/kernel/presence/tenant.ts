/**
 * Presence in the tenant object (one object = one `(project, tenant)` scope,
 * contract E.e.2). The tenant object embeds one `TenantPresence` and routes to
 * it:
 *
 *  - `presence.set` / `presence.subscribe` / `presence.unsubscribe` frames from
 *    its `dispatch` (the gateway forwards them with the socket's view);
 *  - `leave(connectionId)` when a socket closes or its session changes
 *    (Rust's `ConnectionGuard` drop and `reset`: removal with grace);
 *  - `alarm()` from the object's alarm handler, and `nextAlarm()` when it
 *    multiplexes its single alarm (E.timers).
 *
 * Publication follows Rust's scheduler: leading edge, then at most one patch
 * per 250 ms window per scope, offline after the 10 s grace. While the object
 * is awake a `setTimeout` runs the window. Every pending grace end is also on
 * the object's alarm, read from the hub's Members (`nextAlarm()`), so the
 * offline is published at the grace end even when no timer runs (505), and a
 * reset object still publishes it. Nothing is written to SQLite.
 *
 * Delivery: each publication is encoded once and sent once per gateway that
 * holds a subscriber (`PresenceGatewayLink.presence`). Every gateway has one
 * ordered channel with at most one RPC in flight. Publications that queue
 * behind it are merged (last state per Member wins) and, past
 * `maxPendingBatches`, replaced by a snapshot resync, which is Rust's
 * per-connection slot moved to the gateway hop. A subscribe's snapshot travels
 * on the same channel, so no patch can overtake it.
 *
 * Memory is lost when the object is evicted or reset, as Rust loses it on a
 * restart. The gateways still hold the sockets and each socket's presence in
 * its attachment, so the first presence event after a wake rebuilds the hub
 * from them (one RPC per registered gateway) and resynchronizes every
 * subscriber with a snapshot.
 */
import {
  type ConnectionReport, type PresenceConfig, type PresenceMember, type Publication, PresenceHub, encodeSnapshot,
  mergePublications, parseState, scopeKey,
} from "./hub";
import { presenceEligible, type PresenceSocketView } from "./socket";
import { spanTimingOn } from "../delivery/spans";

export const PRESENCE_PROTOCOL_VERSION = 1;

/** Publication sweep: how often (at most) the hub asks gateways which of its connections still exist. */
export const SWEEP_INTERVAL_MS = 60_000;

/** One ordered delivery from a tenant to a gateway. Additive and versioned (deploy skew, G0-1). */
export interface PresenceDelivery {
  v: number;
  tenantId: string;
  projectId: string;
  /** Subscribe answers, in order, before the patch: each goes to one connection if it still wants `id`. */
  snapshots?: Array<{ connectionId: string; id: string; frame: string }>;
  /** One patch for every subscribed socket of the tenant on the gateway (encoded once). */
  patch?: string;
  /** Instead of a patch: every subscribed socket gets a snapshot of these Members under its own subscription id. */
  resync?: { serverTime: number; members: PresenceMember[] };
}

export interface PresenceReceipt { v: number; subscribers: number }

/** What the tenant needs from a gateway (the gateway object implements it). */
export interface PresenceGatewayLink {
  presence(delivery: PresenceDelivery): Promise<PresenceReceipt>;
  /** Every socket of `tenantId` on this gateway with a presence state or subscription. */
  presenceConnections(tenantId: string): Promise<ConnectionReport[]>;
}

export interface TenantPresenceOptions {
  tenantId: string;
  config: PresenceConfig;
  gateway(name: string): PresenceGatewayLink;
  /** Gateways registered with this tenant (rebuild after a wake). */
  registeredGateways(): Promise<string[]>;
  /** Arms the object's alarm no later than `at` (the object multiplexes it). */
  armAlarm?(at: number): void;
  /**
   * After a memory loss: whether presence work may have been pending (a grace
   * end on the alarm). Only then are subscribers resynchronized; an idle
   * eviction left nothing pending and needs no snapshot. Absent: never.
   */
  lostPending?(): Promise<boolean>;
  /** Keeps background deliveries alive past the request (`ctx.waitUntil`). */
  waitUntil?(promise: Promise<unknown>): void;
  /** Timer seam (tests drop timers to prove the alarm alone publishes grace ends). Default `setTimeout`. */
  setTimer?(fn: () => void, ms: number): unknown;
  clearTimer?(handle: unknown): void;
  now?: () => number;
  wall?: () => number;
}

interface Channel {
  /** Publications per scope, in order, not yet sent. */
  pending: Map<string, Publication[]>;
  /** Scopes whose subscribers on this gateway need a snapshot (overflow or a failed delivery). */
  resync: Set<string>;
  snapshots: Map<string, Array<{ connectionId: string; id: string; frame: string }>>;
  inFlight: Promise<void> | null;
  /** After a failed delivery: no new attempt before this time (a retry timer is armed). */
  retryAt: number | null;
}

/** Delay before a gateway whose delivery failed is tried again. */
export const RETRY_MS = 1_000;

/** A window timer this far past its due time is treated as lost and set again (505). */
export const TIMER_STALE_MS = 1_000;

export class TenantPresence {
  readonly hub: PresenceHub;
  private readonly channels = new Map<string, Channel>();
  private loaded: Promise<void> | null = null;
  private timer: unknown = null;
  private timerAt: number | null = null;
  /** The grace end last handed to `armAlarm` (the object's alarm is at or before it). */
  private alarmAt: number | null = null;
  private lastSweep = 0;
  private readonly now: () => number;
  private readonly wall: () => number;
  /** Counters for tests and the PR's numbers. */
  readonly stats = { publications: 0, deliveries: 0, merged: 0, resyncs: 0, rebuilds: 0, sweeps: 0, swept: 0 };

  constructor(private readonly options: TenantPresenceOptions) {
    this.hub = new PresenceHub(options.config);
    this.now = options.now ?? Date.now;
    this.wall = options.wall ?? Date.now;
  }

  get enabled(): boolean { return this.options.config.enabled; }

  /**
   * Diagnostics for the 505 presence investigation: one `presence-diag` line per scheduling step, with
   * times (ms) and ids only (connection, Member, gateway). No names, tokens or row data.
   */
  private diag(event: string, fields: Record<string, unknown>): void {
    if (!spanTimingOn()) return;
    try { console.log(JSON.stringify({ diag: "presence", event, tenant: this.options.tenantId, now: this.now(), ...fields })); } catch { /* never let logging fail presence */ }
  }

  private memberOf(connection: string): { key: string; member: string } | null {
    const r = this.hub.registered(connection);
    const key = this.hub.scopeOf(connection);
    return r?.member && key ? { key, member: r.member } : null;
  }

  // ---- entry points --------------------------------------------------------------------

  /** `presence.*` frames of one socket. Returns the frames to send at once (only `presence.error`). */
  async frame(socket: PresenceSocketView, frame: { type: "presence.set"; state: string } | { type: "presence.subscribe"; id: string } | { type: "presence.unsubscribe" }): Promise<string[]> {
    if (frame.type === "presence.subscribe" && (!this.enabled || !presenceEligible(socket.tenant))) {
      return [JSON.stringify({ type: "presence.error", id: frame.id, error: this.enabled ? "presence requires an authenticated tenant member session" : "presence v2 is disabled" })];
    }
    if (!this.enabled) return [];
    const connection = socket.connectionId;
    await this.load(connection, false, socket.host, socket.presencePrevious ?? null);
    const now = this.now();
    this.diag("frame", { type: frame.type, connection, gateway: socket.host, state: frame.type === "presence.set" ? parseState(frame.state) : undefined });
    switch (frame.type) {
      case "presence.set": {
        const state = parseState(frame.state);
        const tenant = socket.tenant;
        if (state && presenceEligible(tenant)) {
          this.hub.setState(connection, socket.host, scopeKey(tenant!.route.projectId, tenant!.route.tenantId), tenant!.member.id, state, socket.legacy === true, now);
        }
        break;
      }
      case "presence.subscribe": {
        const tenant = socket.tenant!;
        const key = scopeKey(tenant.route.projectId, tenant.route.tenantId);
        const snapshot = this.hub.subscribe(connection, socket.host, key, frame.id, now, this.wall());
        this.enqueueSnapshot(socket.host, key, { connectionId: connection, id: frame.id, frame: snapshot });
        break;
      }
      case "presence.unsubscribe":
        this.hub.unsubscribe(connection);
        break;
    }
    this.afterChange();
    return [];
  }

  /** An old client's keepalive Reducer (legacy beat): active unless already present. */
  async legacyBeat(socket: PresenceSocketView): Promise<void> {
    const tenant = socket.tenant;
    if (!this.enabled || !presenceEligible(tenant)) return;
    await this.load(socket.connectionId, false, socket.host, socket.presencePrevious ?? null);
    this.hub.legacyBeat(socket.connectionId, socket.host, scopeKey(tenant!.route.projectId, tenant!.route.tenantId), tenant!.member.id, this.now());
    this.afterChange();
  }

  /** The socket closed, or its session changed: removal with grace. */
  async leave(connectionId: string, host = "", previous: ConnectionReport | null = null): Promise<void> {
    if (!this.enabled) return;
    await this.load(connectionId, false, host, previous);
    const who = this.memberOf(connectionId);
    this.hub.disconnect(connectionId, this.now());
    this.diag("leave", { connection: connectionId, gateway: host, registered: who !== null, member: who?.member, after: who ? this.hub.memberDiag(who.key, who.member) : null });
    this.afterChange();
  }

  /** The object's alarm fired (grace ends; at-least-once, idempotent). */
  async alarm(): Promise<void> {
    this.diag("alarm", { alarmAt: this.alarmAt, timerAt: this.timerAt, loaded: this.loaded !== null });
    if (this.alarmAt !== null && this.alarmAt <= this.now()) this.alarmAt = null;
    if (!this.enabled) return;
    await this.load(undefined, true);
    this.afterChange(true);
  }

  /**
   * The earliest grace end the object's alarm must cover (null: none), read from the hub's Members,
   * not from what was last armed: it stays right when a window was pending at the leave, when the
   * alarm ran presence just before a grace end, or when no timer ran at all (505).
   */
  nextAlarm(): number | null { return this.enabled ? this.hub.nextGraceEnd() : null; }

  /** Online Members of this tenant's scope (control's `activeSessions` and tests). */
  async published(projectId: string): Promise<PresenceMember[]> {
    await this.load();
    return this.hub.published(scopeKey(projectId, this.options.tenantId));
  }

  /** Waits until every queued delivery has been sent (tests and alarms). */
  async flushed(): Promise<void> {
    for (;;) {
      const inFlight = [...this.channels.values()].map(c => c.inFlight).filter((p): p is Promise<void> => p !== null);
      if (!inFlight.length) return;
      await Promise.allSettled(inFlight);
    }
  }

  // ---- scheduling ------------------------------------------------------------------------

  /** Publishes what is due (leading edge), then schedules the next due time. */
  private afterChange(fromAlarm = false): void {
    const now = this.now();
    const { publications, next } = this.hub.runDue(now, this.wall());
    for (const publication of publications) {
      this.diag("publish", { changes: publication.changes.map(c => `${c.memberId}:${c.state}`), gateways: publication.gateways });
      this.enqueuePublication(publication);
    }
    this.schedule(next, now);
    if (fromAlarm || now - this.lastSweep >= SWEEP_INTERVAL_MS) this.sweep();
  }

  private schedule(next: number | null, now: number): void {
    if (next !== null || this.timerAt !== null) this.diag("schedule", { next, timerAt: this.timerAt, alarmAt: this.alarmAt });
    if (next === null) {
      this.clearTimer();
    } else if (this.timerAt === null || next < this.timerAt || this.timerAt + TIMER_STALE_MS < now) {
      // A timer long past its due time never ran: set a new one instead of waiting on it.
      this.clearTimer();
      this.timerAt = next;
      const fire = () => {
        this.diag("timer", { due: next });
        this.timer = null;
        this.timerAt = null;
        this.afterChange();
      };
      this.timer = this.options.setTimer ? this.options.setTimer(fire, Math.max(0, next - now)) : setTimeout(fire, Math.max(0, next - now));
    }
    this.armGraceEnd(now);
  }

  private clearTimer(): void {
    if (this.timer !== null) {
      if (this.options.clearTimer) this.options.clearTimer(this.timer); else clearTimeout(this.timer as ReturnType<typeof setTimeout>);
    }
    this.timer = null;
    this.timerAt = null;
  }

  /**
   * Every pending grace end goes on the alarm, whatever the next due time is (a 250 ms window may come
   * first) and however close it is, so the offline never depends on a timer alone (505). Rust's
   * publisher is a loop that cannot be lost; the alarm is this object's equivalent. Armed only when
   * it moves earlier or the last one passed: the object multiplexes its alarm and only moves it earlier.
   */
  private armGraceEnd(now: number): void {
    const grace = this.nextAlarm();
    if (grace === null) return;
    if (this.alarmAt === null || grace < this.alarmAt || this.alarmAt <= now) {
      this.alarmAt = grace;
      this.diag("armAlarm", { at: grace });
      this.options.armAlarm?.(grace);
    }
  }

  // ---- delivery ----------------------------------------------------------------------------

  private channel(gateway: string): Channel {
    let channel = this.channels.get(gateway);
    if (!channel) this.channels.set(gateway, channel = { pending: new Map(), resync: new Set(), snapshots: new Map(), inFlight: null, retryAt: null });
    return channel;
  }

  private enqueueSnapshot(gateway: string, key: string, snapshot: { connectionId: string; id: string; frame: string }): void {
    const channel = this.channel(gateway);
    const list = channel.snapshots.get(key) ?? [];
    list.push(snapshot);
    channel.snapshots.set(key, list);
    this.pump(gateway);
  }

  private enqueuePublication(publication: Publication): void {
    this.stats.publications++;
    for (const gateway of publication.gateways) {
      const channel = this.channel(gateway);
      if (channel.resync.has(publication.scope)) { this.pump(gateway); continue; }
      const list = channel.pending.get(publication.scope) ?? [];
      if (list.length >= this.options.config.maxPendingBatches) {
        channel.pending.delete(publication.scope);
        channel.resync.add(publication.scope);
      } else {
        list.push(publication);
        channel.pending.set(publication.scope, list);
      }
      this.pump(gateway);
    }
  }

  /** Sends what the channel holds, one RPC in flight per gateway. */
  private pump(gateway: string): void {
    const channel = this.channel(gateway);
    if (channel.inFlight || (channel.retryAt !== null && this.now() < channel.retryAt)) return;
    channel.retryAt = null;
    const keys = new Set([...channel.snapshots.keys(), ...channel.pending.keys(), ...channel.resync]);
    if (!keys.size) return;
    const deliveries: PresenceDelivery[] = [];
    for (const key of keys) {
      const [projectId, tenantId] = key.split("\u0000") as [string, string];
      const delivery: PresenceDelivery = { v: PRESENCE_PROTOCOL_VERSION, tenantId, projectId };
      const snapshots = channel.snapshots.get(key);
      if (snapshots?.length) delivery.snapshots = snapshots;
      if (channel.resync.has(key)) {
        delivery.resync = { serverTime: this.wall(), members: this.hub.published(key) };
        this.stats.resyncs++;
      } else {
        const batches = channel.pending.get(key) ?? [];
        if (batches.length > 1) this.stats.merged += batches.length - 1;
        const patch = mergePublications(batches);
        if (patch !== null) delivery.patch = patch;
      }
      deliveries.push(delivery);
    }
    channel.snapshots.clear();
    channel.pending.clear();
    channel.resync.clear();
    const link = this.options.gateway(gateway);
    let failed = false;
    const run = (async () => {
      for (const delivery of deliveries) {
        try {
          this.stats.deliveries++;
          const started = this.now();
          const receipt = await link.presence(delivery);
          this.diag("delivered", { gateway, ms: this.now() - started, subscribers: receipt?.subscribers, patch: delivery.patch !== undefined, snapshots: delivery.snapshots?.length ?? 0, resync: delivery.resync !== undefined });
        } catch (error) {
          // The gateway missed this delivery: its subscribers get a snapshot on the retry.
          console.error(`presence: delivery to ${gateway} failed`, error);
          this.diag("deliveryFailed", { gateway });
          channel.resync.add(scopeKey(delivery.projectId, delivery.tenantId));
          failed = true;
        }
      }
    })().finally(() => {
      channel.inFlight = null;
      if (failed) {
        // Never retry at once: a gateway that keeps failing would spin without yielding.
        channel.retryAt = this.now() + RETRY_MS;
        setTimeout(() => this.pump(gateway), RETRY_MS);
      } else if (channel.pending.size || channel.snapshots.size || channel.resync.size) this.pump(gateway);
      else if (!this.hub.scopesOn(gateway).length) this.channels.delete(gateway);
    });
    channel.inFlight = run;
    this.options.waitUntil?.(run);
  }

  // ---- memory loss and dead gateways --------------------------------------------------------

  /**
   * First use after a wake: rebuild from the gateways' socket attachments,
   * then resynchronize subscribers. `trigger` is the connection whose event
   * caused the wake: its gateway's record already shows the event (or no
   * longer holds a closed socket), so it is restored from `previous`, its
   * state before the event, and the event then applies as usual.
   */
  private load(trigger?: string, fromAlarm = false, host = "", previous: ConnectionReport | null = null): Promise<void> {
    if (!this.loaded) {
      this.loaded = this.rebuild(trigger, fromAlarm, host, previous).catch((error) => {
        console.error("presence: rebuild failed", error);
        this.loaded = null;
      });
    }
    return this.loaded;
  }

  private async rebuild(trigger: string | undefined, fromAlarm: boolean, host: string, previous: ConnectionReport | null): Promise<void> {
    this.lastSweep = this.now();
    const gateways = await this.options.registeredGateways();
    let restored = 0;
    if (previous && previous.connectionId === trigger && host) {
      this.hub.restore(previous, host, scopeKey(previous.projectId, this.options.tenantId), this.now(), this.wall());
      restored++;
    }
    if (!gateways.length) return;
    const reports = await Promise.allSettled(gateways.map(name => this.options.gateway(name).presenceConnections(this.options.tenantId)));
    const now = this.now(), wall = this.wall();
    reports.forEach((r, i) => {
      if (r.status !== "fulfilled") return console.error(`presence: ${gateways[i]} did not report its connections`, r.reason);
      for (const report of r.value) {
        if (report.connectionId === trigger) continue;
        this.hub.restore(report, gateways[i]!, scopeKey(report.projectId, this.options.tenantId), now, wall);
        restored++;
      }
    });
    this.diag("rebuild", { trigger, fromAlarm, gateways: gateways.length, restored });
    if (!restored) return;
    this.stats.rebuilds++;
    // A memory loss with a grace pending (the alarm that covers it fired, or is still set): subscribers
    // may hold a Member this object no longer knows, so each gets one snapshot. After an idle eviction
    // nothing was pending and the rebuilt state is what subscribers already have.
    if (!fromAlarm && !(await this.options.lostPending?.().catch(() => true))) return;
    for (const gateway of this.hub.gateways()) {
      for (const key of this.hub.scopesOn(gateway)) this.channel(gateway).resync.add(key);
      this.pump(gateway);
    }
  }

  /**
   * Gateways that restarted (a deploy) lose sockets without close events.
   * At most once a minute, while presence is in use, ask each gateway that
   * holds registered connections which still exist; the others leave with grace.
   */
  private sweep(): void {
    const started = this.now();
    this.lastSweep = started;
    const gateways = [...this.hub.gateways()];
    if (!gateways.length) return;
    this.stats.sweeps++;
    const run = Promise.allSettled(gateways.map(async (gateway) => {
      const live = new Set((await this.options.gateway(gateway).presenceConnections(this.options.tenantId)).map(r => r.connectionId));
      return { gateway, live };
    })).then((results) => {
      const now = this.now();
      let removed = 0;
      for (const r of results) {
        if (r.status !== "fulfilled") continue;
        for (const connection of this.hub.connectionsOn(r.value.gateway, started)) {
          if (!r.value.live.has(connection)) { this.hub.disconnect(connection, now); removed++; }
        }
      }
      this.diag("sweep", { gateways: gateways.length, removed });
      if (removed) {
        this.stats.swept += removed;
        const { publications, next } = this.hub.runDue(now, this.wall());
        for (const publication of publications) this.enqueuePublication(publication);
        this.schedule(next, now);
      }
    });
    this.options.waitUntil?.(run);
  }
}

/** A resync snapshot for one socket (the gateway encodes it with the socket's own subscription id). */
export function resyncFrame(id: string, resync: NonNullable<PresenceDelivery["resync"]>): string {
  return encodeSnapshot(id, resync.serverTime, resync.members);
}
