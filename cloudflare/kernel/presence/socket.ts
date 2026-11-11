/**
 * Presence on the socket side: the part of Rust's socket loop that touches
 * presence (`lib.rs` 3712-3772, 4807-4995), placement-neutral. The session
 * layer (K05) owns one `SocketPresence` per socket and calls it for:
 *
 *  - `presence.set` / `presence.subscribe` / `presence.unsubscribe`;
 *  - every `auth`, a discarded tenant scope and the socket's close
 *    (`presence_hub.reset` and the `ConnectionGuard` drop: leave with grace);
 *  - `reducer.call` (and each call of `reducer.callMany`): the in-memory
 *    answers of `call_reducer_with_presence`: a repeated
 *    `control.support.heartbeat`, the legacy beat paths, and the legacy
 *    transition paths applied to the feed after a successful call;
 *  - deliveries from the tenant object (`PresenceDelivery`).
 *
 * Per-socket state lives in the socket's `SessionRecord` (its attachment),
 * written only when it changes: a transition, a subscribe, a first support
 * heartbeat. It is what the tenant object rebuilds its hub from after a wake.
 */
import type { Json } from "../session/decode";
import type { ReducerCallRequest } from "../session/decode";
import type { Negotiated } from "../session/capabilities";
import type { Connection, SocketView, TenantRoute, TenantSession } from "../session/ports";
import { type ConnectionReport, type PresenceConfig, parseState, rustTrim } from "./hub";

export const SUPPORT_HEARTBEAT = "control.support.heartbeat";
export const PRESENCE_ERRORS = {
  ineligible: "presence requires an authenticated tenant member session",
  disabled: "presence v2 is disabled",
} as const;

/** This socket's presence, as the tenant object last heard it. Survives hibernation. */
export interface PresenceRecord {
  /** Own reported state; null after `offline` or before any report. */
  state: "active" | "idle" | null;
  /** The subscription the client asked for (null: none). */
  subscription: string | null;
  /** The subscription's snapshot was delivered; patches flow to this socket. */
  live: boolean;
  /** When `state` last changed (Unix ms), reported to a tenant that rebuilds. */
  since?: number;
}

/** The support session this connection registered (`SupportLive`); repeats with the same attribution need no write. */
export interface SupportRecord {
  sessionId: string;
  project: string;
  tenant: string;
  account: string;
  release: string;
  environment: string;
}

/** What `TenantPresence` sees of a socket (a subset of `SocketView`). */
export interface PresenceSocketView {
  connectionId: string;
  host: string;
  tenant: TenantSession | null;
  legacy?: boolean;
  /**
   * The socket's presence before this event, as `report()` gives it. A tenant
   * object that lost its memory restores the connection from it before
   * applying the event (the gateway's own record already shows the event).
   */
  presencePrevious?: ConnectionReport | null;
}

/** `presence_member_scope`: anonymous, delegated and service-principal sessions have no presence. */
export function presenceEligible(tenant: TenantSession | null | undefined): boolean {
  return !!tenant && !tenant.anonymous && tenant.delegation === null && tenant.member.id !== "" && !tenant.member.id.startsWith("_gonvex_service:");
}

/** The fields of `SessionRecord` presence reads and writes. */
export interface PresenceRecordHost {
  connectionId: string;
  connection: Connection;
  tenant: TenantSession | null;
  negotiated: Negotiated;
  presence?: PresenceRecord | null;
  support?: SupportRecord | null;
  /** Last replica watermark sent since auth (Rust `sent_replica_revision`; 0: none). */
  watermark?: number;
}

/** The session's side of the contract: its record, its view and its output. */
export interface PresenceSession {
  readonly record: PresenceRecordHost;
  view(): SocketView;
  send(text: string): void;
  /** Sends one feed item: inside a `replica.batch` envelope when the socket negotiated batch frames. */
  sendItem(text: string): void;
  persist(): void;
}

/** Ports presence uses. `presenceLeave` / `presenceBeat` are optional so older tenant objects still work. */
export interface PresencePorts {
  tenants: {
    dispatch(socket: SocketView, frame: { type: "presence.set"; state: string } | { type: "presence.subscribe"; id: string } | { type: "presence.unsubscribe" }): Promise<string[]>;
    presenceLeave?(route: TenantRoute, connectionId: string, host: string, previous?: ConnectionReport | null): Promise<void>;
  };
  control: {
    /** K06 `connectionState`: live support sessions (`support_live_ids`) and the close-time last-seen write. */
    connectionState?(connectionId: string, state: { projectId: string; tenantId: string; authenticated: boolean; supportSessionId?: string } | null): Promise<void>;
  };
}

export interface ShortCircuit {
  /** Frames to send in order instead of calling the Reducer. */
  frames: string[];
  /** Work after the frames (a beat that registers the caller). */
  after?(): Promise<void>;
}

const reducerResult = (call: ReducerCallRequest, result: Json, committedRevision: number | undefined, echo: (t: NonNullable<ReducerCallRequest["trace"]>) => Json) =>
  JSON.stringify({
    type: "reducer.result", id: call.id, path: call.path, result, originCommandId: call.id,
    ...(committedRevision === undefined ? {} : { committedRevision }),
    ...(call.trace ? { trace: echo(call.trace) } : {}),
  });

/** `support_heartbeat_fields`: `release` and `environment`, trimmed, "" unless strings. */
function heartbeatFields(args: Json): [string, string] {
  const field = (name: string) => {
    const value = args !== null && typeof args === "object" && !Array.isArray(args) ? (args as Record<string, Json>)[name] : undefined;
    return typeof value === "string" ? rustTrim(value) : "";
  };
  return [field("release"), field("environment")];
}

export class SocketPresence {
  /**
   * This socket's presence RPCs, in order. A leave (on auth, discard or close)
   * is not awaited by its caller, so the next `presence.set` waits for it here:
   * otherwise the tenant could apply the set first and the late leave would
   * remove the re-announced state.
   */
  private chain: Promise<void> = Promise.resolve();

  constructor(
    private readonly session: PresenceSession,
    private readonly ports: PresencePorts,
    private readonly config: PresenceConfig,
    private readonly echoTrace: (trace: NonNullable<ReducerCallRequest["trace"]>) => Json,
  ) {}

  private get record(): PresenceRecordHost { return this.session.record; }

  private presence(): PresenceRecord {
    return this.record.presence ??= { state: null, subscription: null, live: false };
  }

  // ---- frames -------------------------------------------------------------------------------

  async set(state: string): Promise<void> {
    if (!this.config.enabled || !presenceEligible(this.record.tenant)) return;
    const parsed = parseState(state);
    if (parsed === null) return;
    const next = parsed === "offline" ? null : parsed;
    const p = this.presence();
    // A repeat changes nothing observable in the hub (Rust re-marks the Member, publishes no change).
    if (p.state === next) return;
    const previous = this.report();
    p.state = next;
    p.since = Date.now();
    this.session.persist();
    await this.forward({ type: "presence.set", state: parsed }, previous);
  }

  async subscribe(id: string): Promise<void> {
    if (!this.config.enabled) return this.session.send(JSON.stringify({ type: "presence.error", id, error: PRESENCE_ERRORS.disabled }));
    if (!presenceEligible(this.record.tenant)) return this.session.send(JSON.stringify({ type: "presence.error", id, error: PRESENCE_ERRORS.ineligible }));
    const previous = this.report();
    const p = this.presence();
    p.subscription = id;
    p.live = false;
    this.session.persist();
    // The snapshot comes back on the tenant's ordered presence channel, ahead of any later patch.
    await this.forward({ type: "presence.subscribe", id }, previous);
  }

  async unsubscribe(): Promise<void> {
    const p = this.record.presence;
    if (!this.config.enabled || !p || p.subscription === null) return;
    const previous = this.report();
    p.subscription = null;
    p.live = false;
    this.session.persist();
    if (presenceEligible(this.record.tenant)) await this.forward({ type: "presence.unsubscribe" }, previous);
  }

  private async forward(frame: { type: "presence.set"; state: string } | { type: "presence.subscribe"; id: string } | { type: "presence.unsubscribe" }, previous: ConnectionReport | null): Promise<void> {
    await this.chain;
    const view = { ...this.session.view(), presencePrevious: previous };
    for (const text of await this.ports.tenants.dispatch(view, frame)) this.session.send(text);
  }

  // ---- session changes ------------------------------------------------------------------------

  /**
   * `presence_hub.reset` (every `auth`, a discarded scope) and the guard drop
   * (socket closed): the tenant removes the connection with grace. The caller
   * persists the record. `tenant` is the session the presence belonged to.
   */
  leave(tenant: TenantSession | null): Promise<void> {
    const p = this.record.presence;
    const previous = this.report(tenant);
    this.record.presence = null;
    if (!p || (p.state === null && p.subscription === null) || !presenceEligible(tenant) || !this.ports.tenants.presenceLeave) return this.chain;
    const leave = this.ports.tenants.presenceLeave.bind(this.ports.tenants);
    const host = this.session.view().host;
    this.chain = this.chain
      .then(() => leave(tenant!.route, this.record.connectionId, host, previous))
      .catch((error) => console.error("presence: leave failed", error));
    return this.chain;
  }

  /** The socket is gone: presence leaves, and a registered support session is last seen now (K06 flush). */
  async closed(tenant: TenantSession | null): Promise<void> {
    const work: Array<Promise<void>> = [this.leave(tenant)];
    if (this.record.support && this.ports.control.connectionState) {
      work.push(this.ports.control.connectionState(this.record.connectionId, null).catch((error) => console.error("presence: support close failed", error)));
    }
    await Promise.all(work);
  }

  // ---- deliveries from the tenant --------------------------------------------------------------

  /** One snapshot addressed to this socket: sent only if the socket still wants that subscription. */
  snapshot(id: string, frame: string): void {
    const p = this.record.presence;
    if (!p || p.subscription !== id) return;
    if (!p.live) { p.live = true; this.session.persist(); }
    this.session.send(frame);
  }

  /** A patch for the tenant's subscribed sockets. */
  patch(encoded: string): void {
    if (this.record.presence?.live) this.session.sendItem(encoded);
  }

  /** A resync: a snapshot under this socket's own subscription id. */
  resync(encode: (id: string) => string): void {
    const p = this.record.presence;
    if (!p || p.subscription === null) return;
    if (!p.live) { p.live = true; this.session.persist(); }
    this.session.sendItem(encode(p.subscription));
  }

  /** What the tenant rebuilds from (null: nothing to report). */
  report(tenant: TenantSession | null = this.record.tenant): ConnectionReport | null {
    const p = this.record.presence;
    if (!p || (p.state === null && p.subscription === null) || !presenceEligible(tenant)) return null;
    return {
      connectionId: this.record.connectionId, projectId: tenant!.route.projectId, memberId: tenant!.member.id, state: p.state, subscriptionId: p.subscription,
      ...(p.since === undefined ? {} : { since: p.since }),
    };
  }

  // ---- reducer calls (`call_reducer_with_presence`) --------------------------------------------

  /** Answers in memory when Rust does; null: call the Reducer. */
  shortCircuit(call: ReducerCallRequest): ShortCircuit | null {
    if (!this.config.enabled) return null;
    const control = call.scope === "control";
    if (control && call.path === SUPPORT_HEARTBEAT) {
      const attribution = this.supportAttribution(call);
      const live = this.record.support;
      if (attribution && live && live.sessionId === attribution.sessionId && live.project === attribution.project && live.tenant === attribution.tenant
        && live.account === attribution.account && live.release === attribution.release && live.environment === attribution.environment) {
        // `reducer.result` then `control.watermark` (ordered_control_completion), no write, no refresh, no notify.
        return { frames: [reducerResult(call, { sessionId: live.sessionId }, undefined, this.echoTrace), JSON.stringify({ type: "control.watermark", id: call.id })] };
      }
      return null;
    }
    const watermark = this.record.watermark ?? 0;
    if (!control && watermark > 0 && this.config.legacyBeatPaths.includes(call.path) && presenceEligible(this.record.tenant)) {
      // `result` = server time, `committedRevision` = the last watermark sent, then that watermark
      // (`call_watermark_after_sent_replica_work`: nothing the socket holds is behind it).
      const present = this.record.presence?.state != null;
      const frames = [reducerResult(call, Date.now(), watermark, this.echoTrace), JSON.stringify({ type: "replica.watermark", revision: watermark })];
      return present ? { frames } : { frames, after: () => this.set("active") };
    }
    return null;
  }

  /** After a call the session sent on: register a support session, or apply a legacy transition. */
  async afterCall(call: ReducerCallRequest, replies: readonly string[]): Promise<void> {
    if (!this.config.enabled) return;
    const succeeded = () => replies.some(text => {
      if (!text.startsWith('{"type":"reducer.result"')) return false;
      try { return (JSON.parse(text) as { id?: string }).id === call.id; } catch { return false; }
    });
    const control = call.scope === "control";
    if (control && call.path === SUPPORT_HEARTBEAT) {
      const attribution = this.supportAttribution(call);
      if (!attribution || !succeeded()) return;
      this.record.support = attribution;
      this.session.persist();
      await this.ports.control.connectionState?.(this.record.connectionId, {
        projectId: this.record.connection.projectId, tenantId: attribution.tenant, authenticated: true, supportSessionId: attribution.sessionId,
      }).catch((error) => console.error("presence: support registration not reported", error));
      return;
    }
    if (!control && !this.record.negotiated.presence && this.config.legacyTransitionPaths.includes(call.path)) {
      const args = call.args;
      const state = args !== null && typeof args === "object" && !Array.isArray(args) ? (args as Record<string, Json>).state : undefined;
      if (typeof state === "string" && parseState(state) !== null && succeeded()) await this.set(state);
    }
  }

  private supportAttribution(call: ReducerCallRequest): SupportRecord | null {
    const connection = this.record.connection;
    if (!connection.identity) return null;
    const [release, environment] = heartbeatFields(call.args);
    return {
      sessionId: `support_${this.record.connectionId}`, project: connection.projectId, tenant: connection.tenant?.route.tenantId ?? "",
      account: connection.identity.account.id, release, environment,
    };
  }
}
