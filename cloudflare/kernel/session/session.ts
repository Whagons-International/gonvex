/**
 * One socket's session, placement-neutral (it runs the same in a gateway
 * object or a tenant object; `placement/` decides where sockets live).
 *
 * It reproduces the Rust socket loop (`rust/crates/runtime/src/lib.rs`
 * 1682-3860) for everything that is not a frame handler of another package:
 * `session.ready`, the frame boundary (size limits, the strict decoder, close
 * codes), `auth` in every branch, re-auth in place with subscription
 * preservation, client-contract gating, the pending-auth deadline,
 * revalidation, membership changes and liveness (0016 G0-6).
 *
 * State that must survive hibernation is the `SessionRecord`, kept in the
 * socket attachment (≤ 16 KiB, G0-9). It changes on auth and on the rare
 * timer-driven events, never per delivered frame.
 */
import { type Negotiated, NOTHING_NEGOTIATED, negotiate, sessionReadyFrame, type ReadyOptions } from "./capabilities";
import { decodeClientFrame, type AuthFrame, type ClientCapabilities, type ClientFrame, type Json } from "./decode";
import { replicaDirective, type ReplicaDirective } from "./directive";
import { AUTH_ERRORS, CLOSE, type CloseFrame, LIMITS, UNAUTHENTICATED, echoTrace, exceedsUtf8, frames } from "./protocol";
import {
  type Connection, type Failure, type ModuleInfo, NO_CONNECTION, type Ports, type RevalidationItem,
  type SocketView, type TenantSession, authErrorCode,
} from "./ports";
import { DEFAULT_PRESENCE_CONFIG, type PresenceConfig } from "../presence/hub";
import { type PresenceRecord, SocketPresence, type SupportRecord } from "../presence/socket";
import { type PresenceDelivery, resyncFrame } from "../presence/tenant";
import { handleServiceFrame, type ServiceCalls } from "../principals/frames";
import { serviceAuthResult, type ServiceGrant } from "../principals/grant";
import { SocketDelivery, type TrackedReplicas } from "../delivery/socket";
import { recoveryOf, type RecoverySocketView, type ScopedSocketWork } from "../delivery/recovery";
import type { ReplicaDelivery, SocketWork } from "../delivery/protocol";
import { ADMISSION_MAX_AGE_MS } from "../public/config";
import { UNKNOWN_ADDRESS } from "../public/address";
import { ADMISSION_ERROR, frameCost, publicAdmissionErrors, publicLimitErrors, runsTenantCode } from "../public/frames";
import { type PublicGuard, validatePublicScope } from "../public/guard";
import type { SlotKind } from "../public/limiter";
import { allowCalls, type CallWindow, newWindow } from "../public/window";
import { type ErrorSessionRecord, SocketTelemetry, type TelemetryFrame, type TelemetryHostState, telemetryHostState } from "../telemetry/socket";

export const SERVICE_TOKEN_PREFIX = "gvx_svc_";

/** Survives hibernation (socket attachment). */
export interface SessionRecord {
  v: 1;
  connectionId: string;
  openedAt: number;
  publicTransport: boolean;
  /** Close 1008 at this time unless an identity, tenant or service grant is established (null once cleared). */
  pendingUntil: number | null;
  connection: Connection;
  /** `tenant_session`: the data session, null for control-only sockets. */
  tenant: TenantSession | null;
  /** `connected_client_contract`, as decimal text (u64). */
  clientContract: string | null;
  negotiated: Negotiated;
  /** Public sockets: the (project, tenant) pinned by the first public auth. */
  publicScope: [string, string] | null;
  /** Next revalidation of `connection.authToken` (null: none). */
  revalidateAt: number | null;
  /** Highest revision delivered to this socket (or covered by its admission). */
  sentRevision: number;
  /** Last liveness frame sent (G0-6), for sockets restored after hibernation. */
  livenessAt: number;
  /** K14: this socket's presence state and subscription (what the tenant rebuilds from). */
  presence?: PresenceRecord | null;
  /** K14: the support session registered by this connection (repeat heartbeats answered from memory). */
  support?: SupportRecord | null;
  /** K14: last replica watermark sent since auth (Rust `sent_replica_revision`; 0 or absent: none). */
  watermark?: number;
  /** K17: set while the socket is a service-principal socket (`service_grant`); it then has no tenant session. */
  service?: ServiceGrant | null;
  /** K10: `scope` and `visibilityScope` of the replica directive sent at auth (snapshot metadata). */
  replica?: { scope: string; visibilityScope: string } | null;
  /** K10: bumped whenever the socket's subscriptions are dropped; the tenant discards older state. */
  subscriptionEpoch?: number;
  /** K10: this socket's Replica Collections (id → path) and the tenant instance serving them. */
  replicas?: TrackedReplicas;
  /** K22: successful live subscriptions, retained by the socket owner for tenant restarts. */
  liveQueries?: Record<string, Extract<ClientFrame, { type: "query.subscribe" }>>;
  /** Initial replies use ordered delivery and may arrive after dispatch returned. */
  pendingLiveQueries?: Record<string, Array<Extract<ClientFrame, { type: "query.subscribe" }>>>;
  replicaInstance?: string;
  pendingActions?: Record<string, string>;
  controlQueries?: Record<string, { request: Extract<ClientFrame, { type: "query.subscribe" }>; last: string }>;

  /** K16: the client address bucket (`CF-Connecting-IP`), the key of every per-address budget. */
  address?: string;
  /** K16: slots this socket holds in its address limiter (released on first auth and on close). */
  slots?: SlotKind[];
  /** K16: the per-connection call window of the public transport (starts when the socket opens). */
  publicWindow?: CallWindow;
  /** K16: public admission, created by the first public auth (`PublicAdmission`); a revoked one stays revoked. */
  admission?: { checkedAt: number; revoked: boolean } | null;
  /** K15: the error support session registered by this connection (repeat registers answered from memory). */
  errorSession?: ErrorSessionRecord | null;
  /** K15: `telemetry_device`, the last `auth` frame's `device` (at most 2 KiB of JSON; larger is not kept). */
  telemetryDevice?: Json | null;
}

/** Where frames go. The host maps it onto a hibernatable WebSocket. */
export interface SocketSink {
  send(text: string): void;
  close(frame: CloseFrame): void;
}

export interface SessionHooks {
  /** Semantic recovery metadata changed; generic attachment writes never dirty it. */
  recoveryChanged?(): void;
  /** The tenant data session changed (feed subscription in Rust): placement registers the socket with the tenant. */
  tenantChanged?(previous: TenantSession | null, next: TenantSession | null): void;
  /** Subscriptions, opens and live queries were dropped (not preserved): owners reset their per-socket state. */
  subscriptionsReset?(): void;
  /** The record changed in a way that must survive hibernation. */
  persist?(): void;
  /** Keeps work started outside a frame (presence leave on close) alive (`ctx.waitUntil`). */
  waitUntil?(promise: Promise<unknown>): void;
  /** K16: the socket authenticated for the first time; its pending slot is freed (`pending_connection.take()`). */
  pendingCleared?(): void;
}

export interface SessionOptions {
  ports: Ports;
  ready: ReadyOptions;
  sink: SocketSink;
  hooks?: SessionHooks;
  /** Host id (gateway name) reported to ports. */
  host: string;
  now?: () => number;
  /** Module info cache shared by the host's sessions, invalidated on reload. */
  modules?: ModuleCache;
  /** Presence v2 settings (K14); defaults follow `ready.presence`. */
  presence?: PresenceConfig;
  /** K10: timers of the batch tick (defaults to `setTimeout`; the holder is awake while a batch is open). */
  timers?: SessionTimers;
  /** K16: public-function limits and budgets. Without it only the default payload cap applies. */
  publicGuard?: PublicGuard;
  /** K15: telemetry state shared by the host's sockets (rate-limit windows); default: one per session. */
  telemetry?: TelemetryHostState;
}

export interface SessionTimers {
  later(ms: number, fn: () => void): () => void;
}

/** Per-host cache of each project's active module (one RPC per project, refreshed on reload). */
export class ModuleCache {
  private entries = new Map<string, Promise<ModuleInfo | null>>();
  constructor(private ports: Ports) {}
  get(projectId: string): Promise<ModuleInfo | null> {
    if (!projectId) return Promise.resolve(null);
    let entry = this.entries.get(projectId);
    if (!entry) {
      entry = this.ports.modules.module(projectId).catch((error) => { this.entries.delete(projectId); throw error; });
      this.entries.set(projectId, entry);
    }
    return entry;
  }
  set(projectId: string, info: ModuleInfo | null): void { this.entries.set(projectId, Promise.resolve(info)); }
}

class AuthTimeout extends Error {}

export class Session {
  readonly record: SessionRecord;
  private readonly ports: Ports;
  private readonly sink: SocketSink;
  private readonly hooks: SessionHooks;
  private readonly ready: ReadyOptions;
  private readonly host: string;
  private readonly now: () => number;
  private readonly modules: ModuleCache;
  /** Frames are handled one at a time, in order, as the Rust loop does. */
  private queue: Promise<void> = Promise.resolve();
  private readonly actionWaiters = new Map<string, { result: Promise<string | null>; resolve(text: string | null): void; delivered: Promise<void>; done(): void }>();
  private queueDepth = 0;
  private queueActive = false;
  closed = false;
  /** Last outbound frame time (memory only; `record.livenessAt` covers hibernation). */
  lastOutboundAt: number;
  /** Presence v2 on this socket (K14). */
  readonly presence: SocketPresence;
  /** K10: replica delivery on this socket (`replica.batch`, watermarks, instance resets). */
  readonly delivery: SocketDelivery;
  private readonly timers: SessionTimers;
  private readonly publicGuard: PublicGuard | undefined;
  /** `GONVEX_PUBLIC_MAX_PAYLOAD_BYTES`. */
  private readonly payloadBytes: number;
  /** Error and performance telemetry on this socket (K15). */
  readonly telemetry: SocketTelemetry;

  private constructor(record: SessionRecord, options: SessionOptions) {
    this.record = record;
    this.ports = options.ports;
    this.sink = options.sink;
    this.hooks = options.hooks ?? {};
    this.ready = options.ready;
    this.host = options.host;
    this.now = options.now ?? Date.now;
    this.modules = options.modules ?? new ModuleCache(options.ports);
    this.lastOutboundAt = record.livenessAt;
    this.timers = options.timers ?? { later: (ms, fn) => { const t = setTimeout(fn, ms); return () => clearTimeout(t); } };
    this.delivery = new SocketDelivery({
      send: (text) => this.send(text),
      sendRaw: (text) => this.sendRaw(text),
      end: () => this.close(CLOSE.failed),
      envelope: () => {
        const tenant = this.record.tenant;
        return tenant ? { tenant: tenant.route.tenantId, class: deliveryClass(tenant) } : undefined;
      },
      now: () => this.now(),
      later: (ms, fn) => this.timers.later(ms, fn),
      batchFrames: () => this.record.negotiated.batchFrames,
      watermark: () => this.record.watermark ?? 0,
      setWatermark: (revision) => {
        this.record.watermark = revision;
        if (revision > this.record.sentRevision) this.record.sentRevision = revision;
      },
      tracked: () => (this.record.replicas ??= {}),
      instance: () => this.record.replicaInstance ?? "",
      setInstance: (instance) => { this.record.replicaInstance = instance; },
      trackedChanged: () => { this.hooks.recoveryChanged?.(); this.hooks.persist?.(); },
    });
    this.delivery.batcher.enabled = record.negotiated.batchFrames;
    this.publicGuard = options.publicGuard;
    this.payloadBytes = options.publicGuard?.limits.payloadBytes ?? LIMITS.publicPayloadBytes;
    const presence = options.presence ?? { ...DEFAULT_PRESENCE_CONFIG, enabled: options.ready.presence };
    this.presence = new SocketPresence({
      record: this.record,
      view: () => this.view(),
      send: (text) => this.send(text),
      sendItem: (text) => this.sendItem(text),
      persist: () => this.hooks.persist?.(),
    }, this.ports, presence, echoTrace);
    this.telemetry = new SocketTelemetry(
      { record: this.record, persist: () => this.hooks.persist?.() },
      this.ports.telemetry,
      options.telemetry ?? telemetryHostState({ presenceEnabled: presence.enabled, now: this.now }),
    );
  }

  /** A new socket: sends `session.ready` before anything else (A.1.2 step 1). */
  static open(options: SessionOptions & { connectionId: string; publicTransport: boolean; address?: string; slots?: SlotKind[] }): Session {
    const now = (options.now ?? Date.now)();
    const session = new Session({
      v: 1,
      connectionId: options.connectionId,
      openedAt: now,
      publicTransport: options.publicTransport,
      pendingUntil: now + LIMITS.authTimeoutMs,
      connection: NO_CONNECTION,
      tenant: null,
      clientContract: null,
      negotiated: NOTHING_NEGOTIATED,
      publicScope: null,
      revalidateAt: null,
      sentRevision: 0,
      livenessAt: now,
      address: options.address ?? UNKNOWN_ADDRESS,
      slots: options.slots ?? [],
      ...(options.publicTransport ? { publicWindow: newWindow(now) } : {}),
    }, options);
    session.send(sessionReadyFrame(options.ready));
    return session;
  }

  /** A socket restored after hibernation. */
  static restore(record: SessionRecord, options: SessionOptions): Session {
    return new Session(record, options);
  }

  /** The socket view handed to ports. */
  view(): SocketView {
    return {
      connectionId: this.record.connectionId,
      connection: this.record.connection,
      tenant: this.record.tenant,
      negotiated: this.record.negotiated,
      publicTransport: this.record.publicTransport,
      host: this.host,
      replica: this.record.replica ?? null,
      subscriptionEpoch: this.record.subscriptionEpoch ?? 0,
    };
  }

  // ---- inbound --------------------------------------------------------------

  /** Enqueues one inbound message; resolves when it has been handled. */
  receive(message: string | ArrayBuffer): Promise<void> {
    const run = () => (typeof message === "string" ? this.onText(message) : this.onBinary(message));
    return this.serial(run);
  }

  private async onBinary(bytes: ArrayBuffer): Promise<void> {
    if (this.closed) return;
    if (this.record.publicTransport && bytes.byteLength > this.payloadBytes) return this.close(CLOSE.publicTransportLimit);
    if (await this.updateRequiredBeforeFrame()) return;
    if (this.unscopedOrAnonymous() && bytes.byteLength > this.payloadBytes) return this.close(CLOSE.publicPayloadTooLarge);
    // Binary frames have no handler (lib.rs:3855).
  }

  private async onText(text: string): Promise<void> {
    if (this.closed) return;
    // The public transport's read limit fails before the handler sees the frame.
    if (this.record.publicTransport && exceedsUtf8(text, this.payloadBytes)) return this.close(CLOSE.publicTransportLimit);
    if (await this.updateRequiredBeforeFrame()) return;
    if (this.unscopedOrAnonymous() && exceedsUtf8(text, this.payloadBytes)) return this.close(CLOSE.publicPayloadTooLarge);
    const decoded = decodeClientFrame(text);
    if (!decoded.ok) return this.close(CLOSE.invalidFrame);
    const frame = decoded.frame;
    // K17: a service socket accepts only its narrow set; `auth` takes the normal path (lib.rs:2570-2592).
    if (this.record.service && frame.type !== "auth") return this.serviceFrame(this.record.service, frame);
    if (frame.type === "auth" && frame.public !== this.record.publicTransport) {
      this.send(frames.authError(frame.id, AUTH_ERRORS.publicTransport, "transient"));
      return this.close(CLOSE.ended);
    }
    // K16, in Rust's order (lib.rs:2650-2722): a revoked admission ends the socket; the public transport's
    // call budgets; then the admission re-check (fresh before tenant code, at most 30 s old otherwise).
    if (this.record.admission?.revoked) return this.admissionRevoked();
    if (this.record.publicTransport && !await this.allowPublicCall(frameCost(frame, this.record.negotiated.nativeRows))) {
      for (const text of publicLimitErrors(frame)) this.send(text);
      return;
    }
    if (frame.type !== "auth" && this.record.admission && !await this.admitted(runsTenantCode(frame))) {
      for (const text of publicAdmissionErrors(frame)) this.send(text);
      // Rust breaks the loop: the socket ends without a close frame (kernel: 1000, as every such exit).
      return this.close(CLOSE.ended);
    }
    await this.dispatch(frame);
  }

  // ---- public functions (K16) ------------------------------------------------------

  /**
   * `PublicLimiter::allow`: the address window (in the address's limiter
   * object) and this connection's window are each charged when they have
   * room, independently; the call passes only when both had room.
   */
  private async allowPublicCall(cost: number): Promise<boolean> {
    const guard = this.publicGuard;
    if (!guard) return true;
    const address = await guard.allowAddress(this.record.address ?? UNKNOWN_ADDRESS, cost);
    const window = this.record.publicWindow ??= newWindow(this.record.openedAt);
    const connection = allowCalls(window, cost, guard.limits.connectionCalls, this.now());
    this.hooks.persist?.();
    return address && connection;
  }

  /**
   * `PublicAdmission::check` (public_functions.rs:253-273): re-reads the
   * pinned scope's admission when `fresh` or when the last check is 30 s old.
   * A failed check revokes the socket's admission for good.
   */
  private async admitted(fresh: boolean): Promise<boolean> {
    const admission = this.record.admission;
    const scope = this.record.publicScope;
    if (!admission || !scope) return true;
    if (admission.revoked) return false;
    if (fresh || this.now() - admission.checkedAt >= ADMISSION_MAX_AGE_MS) {
      if (!await validatePublicScope(this.ports.control, scope)) {
        admission.revoked = true;
        this.hooks.persist?.();
        return false;
      }
      admission.checkedAt = this.now();
      this.hooks.persist?.();
    }
    return true;
  }

  /** The revoked-admission exit (lib.rs:2046-2049, 2650-2662): `auth.error session-expired`, then the socket ends. */
  private admissionRevoked(): void {
    this.send(frames.authError("session-expired", ADMISSION_ERROR, "transient"));
    this.close(CLOSE.ended);
  }

  /** When the admission monitor next needs to re-check this socket (null: no public admission to watch). */
  admissionDueAt(): number | null {
    const admission = this.record.admission;
    if (this.closed || !admission || admission.revoked || !this.record.publicScope) return null;
    return admission.checkedAt + ADMISSION_MAX_AGE_MS;
  }

  /** The pinned public scope, for the host to batch monitor checks per scope. */
  publicScope(): readonly [string, string] | null { return this.record.publicScope; }

  /**
   * The monitor's result for this socket (`AdmissionMonitor`, every 30 s in
   * Rust; here the host's alarm, batched per scope). A frame that re-checked
   * in the meantime wins; a failed check revokes and ends the socket.
   */
  admissionMonitored(valid: boolean, now: number): Promise<void> {
    return this.serial(() => {
      const admission = this.record.admission;
      if (this.closed || !admission || admission.revoked || now - admission.checkedAt < ADMISSION_MAX_AGE_MS) return;
      if (valid) {
        admission.checkedAt = now;
        this.hooks.persist?.();
        return;
      }
      admission.revoked = true;
      this.hooks.persist?.();
      this.admissionRevoked();
    });
  }

  private unscopedOrAnonymous(): boolean {
    const unscoped = this.record.tenant === null && this.record.connection.identity === null && !this.record.service;
    return unscoped || this.record.tenant?.anonymous === true;
  }

  /** A.1.5 (b): before every frame while a tenant session exists. */
  private async updateRequiredBeforeFrame(): Promise<boolean> {
    if (!this.record.tenant) return false;
    const module = await this.modules.get(this.record.connection.projectId);
    if (module?.clientContract == null || this.record.clientContract === String(module.clientContract)) return false;
    this.send(frames.clientUpdateRequired(module.clientContract));
    this.close(CLOSE.ended);
    return true;
  }

  // ---- dispatch ----------------------------------------------------------------

  private async dispatch(frame: ClientFrame): Promise<void> {
    const tenant = this.record.tenant;
    const control = async (f: ClientFrame) => {
      const replies = await this.forward(this.ports.control.dispatch(this.view(), f));
      if (f.type === "query.subscribe") {
        const reply = replies.map((text) => JSON.parse(text)).find((value) => value.type === "query.result");
        if (reply) { (this.record.controlQueries ??= {})[f.id] = { request: f, last: JSON.stringify(reply.result) }; this.hooks.recoveryChanged?.(); }
      }
      if (f.type === "query.unsubscribe") { delete this.record.controlQueries?.[f.id]; this.hooks.recoveryChanged?.(); }
      this.hooks.persist?.();
    };
    const toTenant = async (f: ClientFrame) => { await this.forward(this.ports.tenants.dispatch(this.view(), f)); };
    switch (frame.type) {
      case "auth":
        if (frame.token !== undefined && !frame.public && frame.token.trim().startsWith(SERVICE_TOKEN_PREFIX)) return this.serviceAuth(frame);
        // Any other auth ends a service grant first, whatever its outcome (lib.rs:2797).
        this.record.service = null;
        return this.auth(frame);
      case "query.call":
        if (frame.scope === "control") return control(frame);
        if (tenant) {
          const replies = await this.forward(this.ports.tenants.dispatch(this.view(), frame));
          this.awaitWatermark(replies);
          return;
        }
        return this.send(frames.queryError(frame.id, frame.path, UNAUTHENTICATED.query));
      case "query.subscribe":
        if (frame.scope === "control") return control(frame);
        if (tenant) {
          ((this.record.pendingLiveQueries ??= {})[frame.id] ??= []).push(frame);
          this.hooks.recoveryChanged?.();
          this.hooks.persist?.();
          await this.forward(this.ports.tenants.dispatch(this.view(), frame));
          return;
        }
        return this.send(frames.queryError(frame.id, frame.path, UNAUTHENTICATED.subscribe));
      case "query.subscribeMany":
        if (frame.subscribes.length > LIMITS.subscribeMany) return this.send(frames.queryError("", undefined, `query batch cannot contain more than ${LIMITS.subscribeMany} subscribes`));
        for (const subscribe of frame.subscribes) {
          const one: ClientFrame = { type: "query.subscribe", ...subscribe };
          if (subscribe.scope === "control") await control(one);
          else if (tenant) await this.dispatch(one);
          else this.send(frames.queryError(subscribe.id, subscribe.path, UNAUTHENTICATED.subscribe));
        }
        return;
      case "query.unsubscribe":
        delete this.record.liveQueries?.[frame.id];
        delete this.record.pendingLiveQueries?.[frame.id];
        this.hooks.recoveryChanged?.();
        this.hooks.persist?.();
        if (tenant) await toTenant(frame);
        await control(frame);
        return;
      case "reducer.call":
        return this.reducerCall(frame);
      case "reducer.callMany":
        if (frame.calls.length > LIMITS.callMany) return this.send(frames.reducerError("", undefined, `reducer batch cannot contain more than ${LIMITS.callMany} calls`, "rejected"));
        for (const call of frame.calls) {
          // K16: public admission is re-read before each call of a batch (lib.rs:3378-3399).
          if (this.record.admission && !await this.admitted(true)) {
            this.send(frames.reducerError(call.id, call.path, ADMISSION_ERROR, "rejected"));
            continue;
          }
          await this.reducerCall({ type: "reducer.call", ...call });
        }
        // The failed check notified the connection loop, which then ends the socket.
        if (this.record.admission?.revoked && !this.closed) this.admissionRevoked();
        return;
      case "action.call":
        if (frame.scope === "control") return control(frame);
        if (tenant) {
          this.prepareAction(frame.id, frame.path);
          const replies = await this.forward(this.ports.tenants.dispatch(this.view(), frame));
          await this.finishAction(frame.id, replies.length > 0);
          return;
        }
        return this.send(frames.actionError(frame.id, frame.path, UNAUTHENTICATED.action, frame.trace));
      case "replica.open":
        if (tenant) {
          this.delivery.opening(frame.id, frame.path);
          return toTenant(frame);
        }
        return this.send(frames.replicaError(frame.id, frame.path, UNAUTHENTICATED.replica));
      case "replica.openMany":
        if (frame.opens.length > LIMITS.openMany) return this.send(frames.replicaError("", undefined, `replica batch cannot contain more than ${LIMITS.openMany} opens`));
        if (tenant) {
          for (const open of frame.opens) this.delivery.opening(open.id, open.path);
          return toTenant(frame);
        }
        for (const open of frame.opens) this.send(frames.replicaError(open.id, open.path, UNAUTHENTICATED.replica));
        return;
      case "replica.snapshotAck":
        if (tenant && this.record.negotiated.nativeRows) return toTenant(frame);
        return this.send(frames.replicaError(frame.id, undefined, "native snapshot acknowledgements were not negotiated"));
      case "replica.close":
        this.delivery.forget(frame.id);
        if (tenant) return toTenant(frame);
        return;
      // K14: presence state is kept per socket and forwarded to the tenant object (kernel/presence).
      case "presence.set":
        return this.guard(this.presence.set(frame.state));
      case "presence.unsubscribe":
        return this.guard(this.presence.unsubscribe());
      case "presence.subscribe":
        return this.guard(this.presence.subscribe(frame.id));
      case "error.register":
      case "error.heartbeat":
      case "error.envelope":
      case "telemetry.event":
      case "telemetry.batch":
        // K15: handled next to the socket, with or without a tenant (the project decides); the store is the control object's.
        for (const text of await this.telemetry.handle(frame as TelemetryFrame)) this.send(text);
        return;
    }
  }

  private async forward(reply: Promise<string[]>): Promise<string[]> {
    let out: string[];
    try {
      out = await reply;
    } catch (error) {
      console.error("session: port call failed", error);
      this.close(CLOSE.failed);
      return [];
    }
    for (const text of out) this.send(text);
    return out;
  }

  /** A port call made by a presence handler: a failure closes the socket, as `forward` does. */
  private async guard(work: Promise<void>): Promise<void> {
    try {
      await work;
    } catch (error) {
      console.error("session: port call failed", error);
      this.close(CLOSE.failed);
    }
  }

  /** One Reducer call, with presence's in-memory answers first (`call_reducer_with_presence`, K14). */
  private async reducerCall(call: Extract<ClientFrame, { type: "reducer.call" }>): Promise<void> {
    const tenant = this.record.tenant;
    if (call.scope !== "control" && !tenant) {
      return this.send(frames.reducerError(call.id, call.path, UNAUTHENTICATED.reducer, "unauthenticated", call.trace));
    }
    const short = this.presence.shortCircuit(call);
    if (short) {
      for (const text of short.frames) this.send(text);
      if (short.after) await this.guard(short.after());
      return;
    }
    const replies = await this.forward(call.scope === "control" ? this.ports.control.dispatch(this.view(), call) : this.ports.tenants.dispatch(this.view(), call));
    if (call.scope !== "control") this.awaitWatermark(replies);
    if (!this.closed) await this.guard(this.presence.afterCall(call, replies));
  }

  /** A feed item: inside a `replica.batch` envelope when the socket negotiated batch frames (A.4 row 6). */
  private sendItem(text: string): void {
    const tenant = this.record.tenant;
    if (!this.record.negotiated.batchFrames || !tenant) return this.send(text);
    // Presence rides in the open batch: sent at once unless it already holds row changes (delivery.rs push_item).
    this.delivery.batcher.presence(text);
  }

  /**
   * A durable call result whose watermark did not come with it waits for the
   * feed's: the batch that carries it is sent at once (lib.rs `batcher.urgent`).
   */
  private awaitWatermark(replies: readonly string[]): void {
    if (!this.record.negotiated.batchFrames || replies.some((text) => text.startsWith('{"type":"replica.watermark"'))) return;
    for (const text of replies) {
      let revision: number | undefined;
      const match = /"committedRevision":(\d+)/.exec(text);
      if (match && (text.startsWith('{"type":"reducer.result"') || text.startsWith('{"type":"action.result"'))) revision = Number(match[1]);
      else if (text.startsWith('{"type":"query.result"') && text.includes('"found":true')) {
        try {
          const result = (JSON.parse(text) as { path?: string; result?: { found?: boolean; cursor?: { revision?: number } } });
          if (result.path === "__gonvex.reducerReceipt" && result.result?.found === true) revision = result.result.cursor?.revision;
        } catch { /* not a receipt */ }
      }
      if (revision !== undefined && revision > (this.record.watermark ?? 0)) this.delivery.batcher.urgent(revision);
    }
  }

  /** K10: one replica delivery from the tenant object. Applied at once, ahead of frames still being handled. */
  replicaDelivery(payload: ReplicaDelivery, work: SocketWork | undefined): void {
    if (this.closed || !this.record.tenant || this.record.tenant.anonymous) return;
    const epoch = (work as ScopedSocketWork | undefined)?.subscriptionEpoch;
    if (work && epoch === undefined) {
      // Older senders cannot certify the auth scope of their queued work.
      this.close({ code: 1012, reason: "delivery scope requires reconnect" });
      return;
    }
    if (work && epoch !== (this.record.subscriptionEpoch ?? 0)) return;
    if (!work) {
      // An unscoped holder-wide watermark is never an auth-scope certificate.
      if (this.record.replicaInstance && this.record.replicaInstance !== payload.instance) this.close({ code: 1012, reason: "delivery scope requires reconnect" });
      return;
    }
    const previous = this.record.replicaInstance ?? "";
    // A tenant reply may already be queued when unsubscribe reaches this holder.
    // Keep the local semantic request fence ahead of those late LQ frames.
    const ops = work.ops.filter((op) => {
      if (op[0] !== "f" || !/^\{"type":"query\.(result|progress|error)"/.test(op[1])) return true;
      const id = /^\{"type":"query\.(?:result|progress|error)","id":("(?:[^"\\]|\\.)*")/.exec(op[1])?.[1];
      if (!id) return true;
      const key = JSON.parse(id) as string;
      return !!this.record.liveQueries?.[key] || !!this.record.pendingLiveQueries?.[key];
    });
    this.delivery.apply(payload, { ...work, ops });
    if (previous && previous !== payload.instance && Object.keys(this.record.liveQueries ?? {}).length) {
      const epoch = this.record.subscriptionEpoch ?? 0;
      const recover = async () => {
        if (this.closed || epoch !== (this.record.subscriptionEpoch ?? 0)) return;
        for (const request of Object.values(this.record.liveQueries ?? {})) {
          const replies = await this.ports.tenants.dispatch({ ...this.view(), queryRecovery: true } as RecoverySocketView, request);
          if (this.closed || epoch !== (this.record.subscriptionEpoch ?? 0)) return;
          await this.forward(Promise.resolve(replies.filter((text) => !text.startsWith('{"type":"query.progress"')).map((text) => text.replace('"reason":"initial"', '"reason":"reconnect"'))));
        }
      };
      const next = this.serial(recover).catch((error) => console.error("session: live-query resubscribe failed", error));
      this.hooks.waitUntil?.(next);
    }
    this.record.livenessAt = this.lastOutboundAt;
  }

  /** 0056: resolves only after reset frames and real live-query rebuilds completed on this socket. */
  recoverReplica(tenantId: string, payload: ReplicaDelivery): Promise<void> {
    const epoch = this.record.subscriptionEpoch ?? 0;
    return this.serial(async () => {
      if (this.closed || epoch !== (this.record.subscriptionEpoch ?? 0) || this.record.tenant?.route.tenantId !== tenantId) throw new Error("delivery recovery scope changed");
      const recovery = recoveryOf(payload);
      if (!recovery) throw new Error("recovery marker is required");
      await this.verifyRecoveryAuth(tenantId, epoch);
      this.delivery.recover(payload.instance, recovery.reason);
      await this.rebuildLiveQueries(tenantId, epoch);
      await this.verifyRecoveryAuth(tenantId, epoch);
      this.hooks.persist?.();
    });
  }

  /** 0061: a returned epoch can authorize only this EMPTY bootstrap, never retained data. */
  bootstrapReplica(tenantId: string, payload: ReplicaDelivery, work: SocketWork): Promise<void> {
    const epoch = (work as ScopedSocketWork).subscriptionEpoch;
    if (payload.scopeFence !== 1 || epoch === undefined || work.ops.length || work.watermark !== undefined || work.opened || work.removed
      || payload.shared.length || payload.watermark !== undefined || payload.membership) return Promise.reject(new Error("delivery bootstrap must contain only empty scoped work"));
    return this.serial(async () => {
      this.requireRecoveryScope(tenantId, epoch);
      await this.verifyRecoveryAuth(tenantId, epoch);
      this.delivery.recover(payload.instance, "change-feed-reset");
      await this.rebuildLiveQueries(tenantId, epoch);
      await this.verifyRecoveryAuth(tenantId, epoch);
      this.requireRecoveryScope(tenantId, epoch);
      this.hooks.persist?.();
    });
  }

  private requireRecoveryScope(tenantId: string, epoch: number): void {
    if (this.closed || epoch !== (this.record.subscriptionEpoch ?? 0) || this.record.tenant?.route.tenantId !== tenantId) throw new Error("delivery recovery scope changed");
  }

  private async verifyRecoveryAuth(tenantId: string, epoch: number): Promise<void> {
    this.requireRecoveryScope(tenantId, epoch);
    const item = this.revalidationItem();
    if (!item || !(await this.ports.control.revalidate([item]))[0]) {
      this.close({ code: 1012, reason: "session recovery requires reconnect" });
      throw new Error("delivery recovery authorization changed");
    }
    this.requireRecoveryScope(tenantId, epoch);
  }

  private async rebuildLiveQueries(tenantId: string, epoch: number): Promise<void> {
      const requests = [
        ...Object.entries(this.record.liveQueries ?? {}).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([, request]) => ({ request, pending: false })),
        ...Object.entries(this.record.pendingLiveQueries ?? {}).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).flatMap(([, pending]) => pending.map((request) => ({ request, pending: true }))),
      ];
      for (const { request, pending } of requests) {
        this.requireRecoveryScope(tenantId, epoch);
        const replies = await this.ports.tenants.dispatch({ ...this.view(), queryRecovery: true } as RecoverySocketView, request);
        this.requireRecoveryScope(tenantId, epoch);
        for (const text of replies) {
          if (text.startsWith('{"type":"query.progress"')) { this.trackQueryReply(text, pending); continue; }
          this.send(text.replace('"reason":"initial"', '"reason":"reconnect"'), pending);
          if (text.startsWith('{"type":"query.error"')) throw new Error("live query recovery was refused");
        }
      }
  }

  /** K06 ControlChanged; refreshes stay behind the socket's auth/call queue. */
  controlChanged(projectId: string, reason: "control-change" | "presence-change" = "control-change"): Promise<void> {
    const run = async () => {
      if (this.closed || this.record.connection.projectId !== projectId) return;
      for (const subscription of Object.values(this.record.controlQueries ?? {}).sort((a, b) => a.request.id < b.request.id ? -1 : a.request.id > b.request.id ? 1 : 0)) {
        const request = subscription.request;
        if (reason === "presence-change" && request.path !== "control.support.listTenants") continue;
        const replies = await this.ports.control.dispatch(this.view(), { ...request, type: "query.call" });
        for (const text of replies) {
          const frame = JSON.parse(text);
          if (frame.type === "query.result") {
            const value = JSON.stringify(frame.result);
            if (value === subscription.last) continue;
            subscription.last = value;
            frame.reason = reason;
          }
          this.send(JSON.stringify(frame));
        }
      }
      this.hooks.persist?.();
    };
    return this.serial(run);
  }

  private prepareAction(id: string, path: string): void {
    let resolve!: (text: string | null) => void, done!: () => void;
    const result = new Promise<string | null>((r) => { resolve = r; });
    const delivered = new Promise<void>((r) => { done = r; });
    this.actionWaiters.set(id, { result, resolve, delivered, done });
    (this.record.pendingActions ??= {})[id] = path;
    this.hooks.recoveryChanged?.();
    this.hooks.persist?.();
  }
  private async finishAction(id: string, immediate: boolean): Promise<void> {
    const waiter = this.actionWaiters.get(id);
    try {
      if (!immediate && !this.closed && waiter) {
        const text = await waiter.result;
        if (text !== null && !this.closed) { this.send(text); this.awaitWatermark([text]); }
      }
    } finally {
      this.actionWaiters.delete(id);
      delete this.record.pendingActions?.[id];
      this.hooks.recoveryChanged?.();
      this.hooks.persist?.();
      waiter?.done();
    }
  }
  /** Complete the existing serial event; after hibernation, restore delivery from the attachment. */
  async actionFinished(id: string, path: string, epoch: number, text: string): Promise<void> {
    if (this.closed || epoch !== (this.record.subscriptionEpoch ?? 0) || this.record.pendingActions?.[id] !== path) return;
    const waiter = this.actionWaiters.get(id);
    if (waiter) { waiter.resolve(text); await waiter.delivered; return; }
    await this.serial(() => {
      if (this.closed || epoch !== (this.record.subscriptionEpoch ?? 0) || this.record.pendingActions?.[id] !== path) return;
      this.send(text); this.awaitWatermark([text]);
      delete this.record.pendingActions[id]; this.hooks.recoveryChanged?.(); this.hooks.persist?.();
    });
  }

  /** The socket's subscriptions are gone (re-auth without preservation, discard, reload, close): tell the tenant. */
  private resetSubscriptions(previous: TenantSession | null): void {
    this.record.subscriptionEpoch = (this.record.subscriptionEpoch ?? 0) + 1;
    this.delivery.forget();
    this.record.liveQueries = {};
    this.record.pendingLiveQueries = {};
    this.record.pendingActions = {};
    this.record.controlQueries = {};
    this.record.replicaInstance = "";
    this.hooks.recoveryChanged?.();
    if (!previous || previous.anonymous || !this.ports.tenants.connectionReset) return;
    const work = this.ports.tenants.connectionReset(previous.route, this.record.connectionId, this.record.subscriptionEpoch)
      .catch((error) => console.error("session: tenant reset notice failed", error));
    this.hooks.waitUntil?.(work);
  }

  // ---- auth ----------------------------------------------------------------------

  /**
   * `gvx_svc_` tokens (K17, lib.rs:2730-2790). The socket drops whatever
   * session it had; with a grant it is bound to one project and tenant, its
   * pending deadline clears and it has no tenant session or change feed.
   * Every refusal is `auth.error` code `transient`; a refused socket stays
   * pending. A control port without `authenticateService` refuses every
   * service credential.
   */
  private async serviceAuth(frame: AuthFrame): Promise<void> {
    const port = this.ports.control;
    let grant: ServiceGrant | null = null;
    let error: string = AUTH_ERRORS.serviceInvalid;
    if (port.authenticateService) {
      const deadline = this.record.pendingUntil ?? this.now() + LIMITS.authTimeoutMs;
      try {
        const outcome = await withDeadline(port.authenticateService(frame.token!, frame.project, frame.tenant), deadline - this.now());
        if (outcome.ok) grant = outcome.grant;
        else error = outcome.error;
      } catch (failure) {
        if (!(failure instanceof AuthTimeout)) console.error("session: service auth failed", failure);
        return this.close(CLOSE.failed);
      }
      if (this.closed) return;
    }
    this.leavePresence();
    this.discardScope();
    this.record.clientContract = null;
    this.record.connection = grant ? { ...NO_CONNECTION, projectId: grant.route.projectId } : NO_CONNECTION;
    this.record.service = grant;
    if (grant && this.record.pendingUntil !== null) {
      this.record.pendingUntil = null;
      // K16: a service grant takes the pending slot too (`pending_connection.take()`, lib.rs:2765-2767).
      this.hooks.pendingCleared?.();
    }
    this.hooks.persist?.();
    this.send(grant ? frames.authResult(frame.id, serviceAuthResult(grant) as Json) : frames.authError(frame.id, error, "transient"));
    this.reportConnection();
  }

  /** One frame on a service socket (`handle_service_frame`); a failed port call closes the socket, as `forward` does. */
  private async serviceFrame(grant: ServiceGrant, frame: ClientFrame): Promise<void> {
    const action = frame.type === "action.call" && frame.scope !== "control" ? frame : null;
    if (action) this.prepareAction(action.id, action.path);
    let out: string[] | null;
    try {
      out = await handleServiceFrame(this.serviceCalls(), grant, frame);
    } catch (error) {
      console.error("session: service call failed", error);
      if (action) await this.finishAction(action.id, true);
      return this.close(CLOSE.failed);
    }
    for (const text of out ?? []) this.send(text);
    if (action) await this.finishAction(action.id, (out?.length ?? 0) > 0);
  }

  private serviceCalls(): ServiceCalls {
    const { control, tenants } = this.ports;
    return {
      control: (grant, path, args) => control.serviceCall
        ? control.serviceCall(grant, path, args)
        : Promise.resolve({ ok: false, error: AUTH_ERRORS.storeUnavailable }),
      tenant: (grant, call) => tenants.serviceCall
        ? tenants.serviceCall(grant, call.kind === "action" ? { ...call, reply: { host: this.host, connectionId: this.record.connectionId, epoch: this.record.subscriptionEpoch ?? 0, id: call.id, path: call.path } } : call)
        : Promise.resolve({ ok: false, error: `project ${JSON.stringify(grant.route.projectId)} has no active TypeScript module` }),
    };
  }

  private async auth(frame: AuthFrame): Promise<void> {
    const record = this.record;
    const tenantRequest = controlAuthTenant(frame.controlOnly, frame.capabilities, frame.tenant);
    const deadline = record.pendingUntil ?? this.now() + LIMITS.authTimeoutMs;
    let outcome: { response: AuthResponse; connection: Connection };
    try {
      outcome = await withDeadline(this.authenticate(frame, tenantRequest), deadline - this.now());
    } catch (error) {
      if (!(error instanceof AuthTimeout)) console.error("session: auth failed", error);
      // lib.rs:2809: the loop breaks and the socket closes without a frame.
      return this.close(CLOSE.failed);
    }
    if (this.closed) return;
    const { response, connection } = outcome;
    // lib.rs:2826: every auth drops the connection's presence (with grace); the client re-announces.
    this.leavePresence();

    const anonymous = connection.tenant?.anonymous ? connection.tenant : null;
    if (anonymous) {
      const scope: [string, string] = [connection.projectId, anonymous.route.tenantId];
      if (record.publicScope && (record.publicScope[0] !== scope[0] || record.publicScope[1] !== scope[1])) {
        this.send(frames.authError(response.ok ? response.id : "", AUTH_ERRORS.publicScopeFixed, "transient"));
        return this.close(CLOSE.publicScopeFixed);
      }
      if (!record.publicScope) {
        record.publicScope = scope;
        // K16: the admission this auth just read starts the 30 s cache (`PublicAdmission::new`).
        record.admission = { checkedAt: this.now(), revoked: false };
      }
    }
    if ((connection.identity || connection.tenant) && record.pendingUntil !== null) {
      record.pendingUntil = null;
      this.hooks.pendingCleared?.();
    }
    record.clientContract = frame.clientContract === undefined ? null : frame.clientContract.toString();

    const module = await this.modules.get(connection.projectId);
    if (module?.clientContract != null && !frame.controlOnly && connection.tenant && record.clientContract !== String(module.clientContract)) {
      this.send(frames.clientUpdateRequired(module.clientContract));
      return this.close(CLOSE.ended);
    }
    if (frame.controlOnly && response.ok) {
      delete response.result.replica;
      delete response.result.localIdentity;
    }
    record.negotiated = negotiate(frame.capabilities, this.ready.presence);
    this.delivery.authenticated(record.negotiated.batchFrames);
    // lib.rs:2932 `telemetry_device = device.clone()` (K15), kept only while it fits the attachment comfortably.
    record.telemetryDevice = frame.device !== undefined && JSON.stringify(frame.device).length <= 2048 ? (frame.device as unknown as Json) : null;
    const next = frame.controlOnly ? null : connection.tenant;
    const preserved = !frame.controlOnly && record.tenant !== null && preserveScope(record.connection, connection, frame.capabilities);
    if (preserved) {
      if (response.ok) response.result.subscriptionsPreserved = true;
    } else {
      this.hooks.subscriptionsReset?.();
      this.resetSubscriptions(record.tenant);
    }
    const directive = response.ok ? response.result.replica : undefined;
    record.replica = directive && typeof directive === "object" ? { scope: String((directive as ReplicaDirective).scope), visibilityScope: String((directive as ReplicaDirective).visibilityScope) } : null;
    const previous = record.tenant;
    record.tenant = next;
    record.connection = connection;
    record.revalidateAt = connection.authToken ? this.now() + LIMITS.revalidateMs : null;
    if (next && !preserved) record.sentRevision = next.admissionRevision;
    if (!preserved) record.watermark = 0;
    if (!sameRoute(previous, next) || (previous?.anonymous ?? false) !== (next?.anonymous ?? false)) this.hooks.tenantChanged?.(previous, next);
    this.hooks.persist?.();
    this.send(response.ok ? frames.authResult(response.id, response.result as Json) : frames.authError(response.id, response.error, response.code));
    this.reportConnection();
  }

  /** `authenticate` (lib.rs:4475-4780). */
  private async authenticate(frame: AuthFrame, tenant: string | undefined): Promise<{ response: AuthResponse; connection: Connection }> {
    const { id, controlOnly } = frame;
    const control = this.ports.control;
    const project = nonBlank(frame.project);
    const token = nonBlank(frame.token);
    const fail = (error: string, code: string = "transient") => ({ response: { ok: false as const, id, error, code }, connection: NO_CONNECTION });
    const failed = (failure: Failure, prefix = "") => fail(prefix + failure.message, authErrorCode(failure.kind));
    const artifactHash = async (projectId: string) => (await this.modules.get(projectId))?.artifactHash ?? "";

    if (frame.public) {
      const requestedTenant = tenant?.trim();
      const route = !token && !controlOnly && project && requestedTenant ? await control.resolvePublicTenant(project, requestedTenant) : undefined;
      if (!route?.ok) return fail(AUTH_ERRORS.publicUnavailable);
      const r = route.value;
      const result = { projectId: r.projectId, tenantId: r.tenantId, accountId: "", public: true, artifactHash: await artifactHash(r.projectId) };
      return { response: { ok: true, id, result }, connection: { ...NO_CONNECTION, projectId: r.projectId, tenant: anonymousSession(r) } };
    }
    if (controlOnly && !token) {
      if (!project) return fail(AUTH_ERRORS.projectRequired);
      const found = await control.project(project);
      if (!found.ok) return failed(found);
      return { response: { ok: true, id, result: { projectId: project, accountId: "", tenantId: "" } }, connection: { ...NO_CONNECTION, projectId: project } };
    }
    if (!token) return fail(AUTH_ERRORS.sessionRequired);
    if (token.startsWith("gvx_imp_") || token.startsWith("gvx_dev_")) {
      if (controlOnly && tenant === undefined) return fail(AUTH_ERRORS.developerNeedsTenant);
      const grant = await control.authenticateImpersonation(token, project, tenant, this.record.connectionId);
      if (!grant.ok) return failed(grant, "impersonation grant is invalid, expired, revoked, or already used: ");
      const { session, grantId, actorAccountId, reconnectToken } = grant.value;
      const replica = controlOnly ? null : await this.directive(session);
      const result: Record<string, unknown> = {
        projectId: session.identity.projectId, accountId: session.identity.account.id, tenantId: session.route.tenantId,
        impersonationId: grantId, impersonatorId: actorAccountId, developerSessionToken: reconnectToken,
        localIdentity: localIdentity(session, true), artifactHash: await artifactHash(session.identity.projectId), replica,
      };
      return {
        response: { ok: true, id, result },
        connection: { projectId: session.identity.projectId, identity: session.identity, tenant: session, impersonationId: grantId, authToken: reconnectToken },
      };
    }
    if (controlOnly && tenant === undefined) {
      const identity = await control.loadSessionIdentity(token, project);
      if (!identity.ok) return failed(identity);
      const projectId = identity.value.projectId;
      const result = { projectId, accountId: identity.value.account.id, tenantId: "", artifactHash: await artifactHash(projectId) };
      return { response: { ok: true, id, result }, connection: { projectId, identity: identity.value, tenant: null, impersonationId: "", authToken: token } };
    }
    const authenticated = await control.authenticateSession(token, project, tenant);
    if (!authenticated.ok) return failed(authenticated);
    const session = authenticated.value;
    const result: Record<string, unknown> = {
      projectId: session.identity.projectId, accountId: session.identity.account.id, tenantId: session.route.tenantId,
      artifactHash: await artifactHash(session.identity.projectId),
      replica: controlOnly ? null : await this.directive(session),
      localIdentity: localIdentity(session, false),
    };
    return {
      response: { ok: true, id, result },
      connection: { projectId: session.identity.projectId, identity: session.identity, tenant: session, impersonationId: "", authToken: token },
    };
  }

  /** `module.replica_directive(...)`, or null when the project has no active module (`module.filter`). */
  private async directive(session: TenantSession) {
    const module = await this.modules.get(session.identity.projectId);
    if (!module) return null;
    const epoch = await this.ports.modules.epoch(session.identity.projectId, session.route.databaseUrl);
    return replicaDirective({
      projectId: session.identity.projectId, tenantId: session.route.tenantId, accountId: session.identity.account.id,
      permissions: session.member.permissions, epoch,
    });
  }

  /** Drops the tenant scope and everything subscribed under it. */
  private discardScope(): void {
    const previous = this.record.tenant;
    this.leavePresence();
    this.record.watermark = 0;
    this.hooks.subscriptionsReset?.();
    this.resetSubscriptions(previous);
    this.record.tenant = null;
    this.record.revalidateAt = null;
    if (previous) this.hooks.tenantChanged?.(previous, null);
    this.reportConnection();
  }

  private reportConnection(closed = false): void {
    if (!this.ports.control.connectionState) return;
    // tenantControl can retain a Control connection's tenant while tenant-data
    // dispatch intentionally has no tenant session (Rust's count uses the former).
    const tenant = this.record.connection.tenant;
    const state = closed ? null : { projectId: this.record.connection.projectId, tenantId: tenant?.anonymous ? "" : tenant?.route.tenantId ?? "",
      authenticated: !!this.record.connection.identity || (!!tenant && !tenant.anonymous), ...(this.record.support ? { supportSessionId: this.record.support.sessionId } : {}) };
    const work = this.ports.control.connectionState(this.record.connectionId, state).catch((error) => console.error("session: connection report failed", error));
    this.hooks.waitUntil?.(work);
  }

  // ---- events from the host --------------------------------------------------------

  /** `presence_hub.reset`: the tenant drops this connection's presence with grace (K14). */
  private leavePresence(): void {
    const work = this.presence.leave(this.record.tenant);
    this.hooks.waitUntil?.(work);
  }

  /** Runs `fn` after the frames already queued, in the socket's order. */
  private serial(fn: () => void | Promise<void>): Promise<void> {
    this.queueDepth++;
    const run = async () => {
      this.queueActive = true;
      try { await fn(); } finally { this.queueActive = false; this.queueDepth--; }
    };
    const next = this.queue.then(run, run);
    this.queue = next.catch(() => {});
    return next;
  }

  /** X01/0054: authoritative quiescent drain check; never interrupts or replays a call. */
  gatewayDrain(): { closed: boolean; blocked?: "inflight" | "direct-action" | "queued" } {
    if (this.closed) return { closed: true };
    if (this.queueActive) return { closed: false, blocked: "inflight" };
    if (this.queueDepth) return { closed: false, blocked: "queued" };
    if (Object.keys(this.record.pendingActions ?? {}).length) return { closed: false, blocked: "direct-action" };
    // This synchronous turn is between serialized events; every accepted feed item precedes the close.
    this.delivery.batcher.flush();
    this.close({ code: 1012, reason: "gateway capacity rebalance" });
    return { closed: true };
  }

  /** The pending-auth deadline passed (A.1.8). */
  deadline(now: number): Promise<void> {
    return this.serial(() => {
      if (!this.closed && this.record.pendingUntil !== null && now >= this.record.pendingUntil) this.close(CLOSE.authDeadline);
    });
  }

  /** The item this socket contributes to a revalidation round, if any. */
  revalidationItem(): RevalidationItem | null {
    const c = this.record.connection;
    if (this.closed || !c.authToken) return null;
    return {
      connectionId: this.record.connectionId, projectId: c.projectId, token: c.authToken, impersonationId: c.impersonationId,
      accountId: c.identity?.account.id ?? "",
      ...(this.record.tenant ? { tenantId: this.record.tenant.route.tenantId, memberId: this.record.tenant.member.id } : {}),
    };
  }

  /** Result of a revalidation round for this socket (lib.rs:2044-2083). */
  revalidated(valid: boolean, now: number): Promise<void> {
    return this.serial(() => {
      if (this.closed || !this.record.connection.authToken) return;
      if (valid) {
        this.record.revalidateAt = now + LIMITS.revalidateMs;
        this.hooks.persist?.();
        return;
      }
      this.discardScope();
      this.record.connection = NO_CONNECTION;
      this.hooks.persist?.();
      this.send(frames.authError("session-expired", AUTH_ERRORS.expired, "transient"));
    });
  }

  /** The session's own Member changed in a commit (C.4 item 5a): discard, then the commit's watermark. */
  membershipChanged(revision: number): Promise<void> {
    return this.serial(() => {
      if (this.closed || !this.record.tenant) return;
      this.discardScope();
      this.hooks.persist?.();
      this.send(frames.authError("membership-changed", AUTH_ERRORS.membershipChanged, "transient"));
      this.send(frames.replicaWatermark(revision));
    });
  }

  /** `RuntimeEvent::ModuleReloaded` for this socket's project (A.1.5 c). */
  moduleReloaded(module: ModuleInfo | null): Promise<void> {
    return this.serial(() => {
      if (this.closed) return;
      this.hooks.subscriptionsReset?.();
      this.resetSubscriptions(this.record.tenant);
      if (module?.clientContract != null && this.record.tenant && this.record.clientContract !== String(module.clientContract)) {
        this.send(frames.clientUpdateRequired(module.clientContract));
        return this.close(CLOSE.ended);
      }
      this.send(frames.systemReload(module?.artifactHash));
    });
  }

  /** Frames from the tenant for this socket (delivery); `revision` covers them. */
  deliver(texts: readonly string[], revision: number): Promise<void> {
    return this.serial(() => {
      if (this.closed) return;
      for (const text of texts) this.send(text);
      if (revision > this.record.sentRevision) this.record.sentRevision = revision;
      if (revision > (this.record.watermark ?? 0)) this.record.watermark = revision;
    });
  }

  /**
   * G0-6 liveness: a tenant socket with no outbound frame for 20 s gets a
   * watermark at the revision it already has, so the edge's TCP stack can
   * notice a dead peer. It certifies nothing new. Batch-frame sockets get it
   * in the `replica.batch` envelope they receive every watermark in (A.0).
   */
  liveness(now: number): Promise<void> {
    return this.serial(() => {
      const tenant = this.record.tenant;
      if (this.closed || !tenant || tenant.anonymous || now - this.lastOutboundAt < LIMITS.livenessMs) return;
      const revision = this.record.sentRevision;
      this.send(this.record.negotiated.batchFrames
        ? JSON.stringify({ type: "replica.batch", tenant: tenant.route.tenantId, class: deliveryClass(tenant), from: revision, items: [], watermark: revision })
        : frames.replicaWatermark(revision));
      this.record.livenessAt = now;
      this.hooks.persist?.();
    });
  }

  /** One ordered presence delivery from the tenant object (K14), in the socket's order. */
  presenceDelivery(delivery: PresenceDelivery): Promise<void> {
    return this.serial(() => {
      if (this.closed) return;
      for (const s of delivery.snapshots ?? []) if (s.connectionId === this.record.connectionId) this.presence.snapshot(s.id, s.frame);
      if (delivery.resync) this.presence.resync(id => resyncFrame(id, delivery.resync!));
      else if (delivery.patch !== undefined) this.presence.patch(delivery.patch);
    });
  }

  /** Next time this session needs the host's alarm. */
  nextWake(): number | null {
    if (this.closed) return null;
    const times = [this.record.pendingUntil, this.record.revalidateAt, this.admissionDueAt()];
    if (this.record.tenant && !this.record.tenant.anonymous) times.push(this.lastOutboundAt + LIMITS.livenessMs);
    const due = times.filter((t): t is number => t !== null);
    return due.length ? Math.min(...due) : null;
  }

  // ---- outbound ------------------------------------------------------------------

  /** Sends a frame after any open `replica.batch` (A.4 row 1: a batch is flushed before any other frame). */
  private send(text: string, consumePending = true): void {
    // Retain only the result hash for restart recovery, without parsing/copying row payloads per socket.
    this.trackQueryReply(text, consumePending);
    if (this.closed) return;
    if (this.delivery?.batcher.hasOpen) this.delivery.batcher.flush();
    this.sendRaw(text);
  }

  private trackQueryReply(text: string, consumePending = true): void {
    if (/^\{"type":"query\.(result|progress|error)"/.test(text)) {
      const encodedId = /^\{"type":"query\.(?:result|progress|error)","id":("(?:[^"\\]|\\.)*")/.exec(text)?.[1];
      const id: string | undefined = encodedId ? JSON.parse(encodedId) : undefined;
      const window = /"windowRevision":"([a-f0-9]+)"}$/.exec(text.slice(-100))?.[1];
      if (id !== undefined) {
        const queue = this.record.pendingLiveQueries?.[id];
        const pending = consumePending ? queue?.shift() : undefined;
        const success = !text.startsWith('{"type":"query.error"');
        if (pending && success) (this.record.liveQueries ??= {})[id] = { ...pending };
        if (!queue?.length) delete this.record.pendingLiveQueries?.[id];
        const request = this.record.liveQueries?.[id];
        if (request && window && success) request.windowRevision = window;
        if (pending) { this.hooks.recoveryChanged?.(); this.hooks.persist?.(); }
      }
    }
  }

  private sendRaw(text: string): void {
    if (this.closed) return;
    // 0018: the platform drops outbound frames this large without an error.
    if (exceedsUtf8(text, LIMITS.maxOutboundBytes - 1)) {
      console.error(`session: refusing to send a ${text.length}-character frame over the platform's outbound limit`);
      return this.close({ code: 1011, reason: "frame exceeds the outbound limit" });
    }
    this.lastOutboundAt = this.now();
    this.sink.send(text);
  }

  close(frame: CloseFrame): void {
    for (const waiter of this.actionWaiters.values()) waiter.resolve(null);
    if (this.closed) return;
    if (this.delivery.batcher.hasOpen) this.delivery.batcher.flush();
    this.resetSubscriptions(this.record.tenant);
    this.closed = true;
    this.reportConnection(true);
    const previous = this.record.tenant;
    this.sink.close(frame);
    if (previous) this.hooks.tenantChanged?.(previous, null);
    this.hooks.waitUntil?.(this.presence.closed(previous));
  }

  /** The peer closed or the socket failed. Resolves once presence has been told (K14). */
  closedByPeer(): Promise<void> {
    for (const waiter of this.actionWaiters.values()) waiter.resolve(null);
    if (this.closed) return Promise.resolve();
    this.resetSubscriptions(this.record.tenant);
    this.closed = true;
    this.reportConnection(true);
    if (this.record.tenant) this.hooks.tenantChanged?.(this.record.tenant, null);
    return this.presence.closed(this.record.tenant);
  }
}

type AuthResponse = { ok: true; id: string; result: Record<string, unknown> } | { ok: false; id: string; error: string; code: string };

function nonBlank(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

/** `control_auth_tenant` (lib.rs:4422). */
export function controlAuthTenant(controlOnly: boolean, capabilities: ClientCapabilities | undefined, tenant: string | undefined): string | undefined {
  if (!controlOnly) return tenant;
  if (capabilities?.tenantControl !== 1) return undefined;
  return tenant !== undefined && tenant.trim() !== "" ? tenant : undefined;
}

/** `preserve_subscription_scope` (lib.rs:5223). */
export function preserveScope(old: Connection, next: Connection, capabilities: ClientCapabilities | undefined): boolean {
  if (capabilities?.preserveSubscriptions !== 1) return false;
  const a = old.tenant, b = next.tenant;
  if (!a || !b || a.anonymous) return false;
  return old.projectId === next.projectId
    && same(old.identity, next.identity)
    && old.impersonationId === next.impersonationId
    && same(a.identity, b.identity)
    && sameRoute(a, b)
    && same(a.member, b.member)
    && same(a.delegation, b.delegation)
    && same(a.developerSession, b.developerSession);
}

function sameRoute(a: TenantSession | null, b: TenantSession | null): boolean {
  if (!a || !b) return a === b;
  return a.route.projectId === b.route.projectId && a.route.tenantId === b.route.tenantId && a.route.databaseUrl === b.route.databaseUrl;
}

/** Structural equality (Rust's `PartialEq` on the session structs). */
function same(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ka = Object.keys(a), kb = Object.keys(b);
  if (ka.length !== kb.length) return false;
  return ka.every(k => same((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
}

function anonymousSession(route: { projectId: string; tenantId: string; databaseUrl: string }): TenantSession {
  return {
    identity: { projectId: route.projectId, account: { id: "", email: "", emailVerified: false, name: "", avatarUrl: "", provider: "" } },
    route,
    member: { id: "", accountId: "", status: "", displayName: "", avatarUrl: "", role: "", permissions: null, membershipRevision: 0 },
    admissionRevision: 0,
    delegation: null,
    developerSession: null,
    anonymous: true,
  };
}

/** `auth.result.result.localIdentity` (lib.rs:4618-4636, 4741-4761). */
function localIdentity(session: TenantSession, developer: boolean) {
  const a = session.identity.account, m = session.member;
  const account = { id: a.id, email: a.email, name: a.name, avatarUrl: a.avatarUrl };
  return {
    auth: developer ? { account, developerSession: session.developerSession } : { account },
    tenant: { id: session.route.tenantId },
    member: { id: m.id, accountId: m.accountId, status: m.status, role: m.role, displayName: m.displayName, permissions: m.permissions },
  };
}

/**
 * The socket's delivery class: sockets of one tenant with the same permissions
 * receive the same encoded frames (design rule: encode once per class). K09
 * refines classes from visibility plans; the wire `class` is informational
 * (A.4 row 1), 16 lowercase hex.
 */
export function deliveryClass(tenant: TenantSession): string {
  let h = 0xcbf29ce484222325n;
  for (const ch of `${tenant.route.tenantId}\u0000${JSON.stringify(tenant.member.permissions)}`) {
    h ^= BigInt(ch.codePointAt(0)!);
    h = (h * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return h.toString(16).padStart(16, "0");
}

function withDeadline<T>(promise: Promise<T>, ms: number): Promise<T> {
  if (ms <= 0) return Promise.reject(new AuthTimeout());
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => { timer = setTimeout(() => reject(new AuthTimeout()), ms); }),
  ]).finally(() => { if (timer !== undefined) clearTimeout(timer); });
}
