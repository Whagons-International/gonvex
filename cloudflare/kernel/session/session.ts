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
import { replicaDirective } from "./directive";
import { AUTH_ERRORS, CLOSE, type CloseFrame, LIMITS, UNAUTHENTICATED, exceedsUtf8, frames } from "./protocol";
import {
  type Connection, type Failure, type ModuleInfo, NO_CONNECTION, type Ports, type RevalidationItem,
  type SocketView, type TenantSession, authErrorCode,
} from "./ports";

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
}

/** Where frames go. The host maps it onto a hibernatable WebSocket. */
export interface SocketSink {
  send(text: string): void;
  close(frame: CloseFrame): void;
}

export interface SessionHooks {
  /** The tenant data session changed (feed subscription in Rust): placement registers the socket with the tenant. */
  tenantChanged?(previous: TenantSession | null, next: TenantSession | null): void;
  /** Subscriptions, opens and live queries were dropped (not preserved): owners reset their per-socket state. */
  subscriptionsReset?(): void;
  /** The record changed in a way that must survive hibernation. */
  persist?(): void;
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
  closed = false;
  /** Last outbound frame time (memory only; `record.livenessAt` covers hibernation). */
  lastOutboundAt: number;

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
  }

  /** A new socket: sends `session.ready` before anything else (A.1.2 step 1). */
  static open(options: SessionOptions & { connectionId: string; publicTransport: boolean }): Session {
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
    };
  }

  // ---- inbound --------------------------------------------------------------

  /** Enqueues one inbound message; resolves when it has been handled. */
  receive(message: string | ArrayBuffer): Promise<void> {
    const run = () => (typeof message === "string" ? this.onText(message) : this.onBinary(message));
    const next = this.queue.then(run, run);
    this.queue = next.catch(() => {});
    return next;
  }

  private async onBinary(bytes: ArrayBuffer): Promise<void> {
    if (this.closed) return;
    if (this.record.publicTransport && bytes.byteLength > LIMITS.publicPayloadBytes) return this.close(CLOSE.publicTransportLimit);
    if (await this.updateRequiredBeforeFrame()) return;
    if (this.unscopedOrAnonymous() && bytes.byteLength > LIMITS.publicPayloadBytes) return this.close(CLOSE.publicPayloadTooLarge);
    // Binary frames have no handler (lib.rs:3855).
  }

  private async onText(text: string): Promise<void> {
    if (this.closed) return;
    // The public transport's read limit fails before the handler sees the frame.
    if (this.record.publicTransport && exceedsUtf8(text, LIMITS.publicPayloadBytes)) return this.close(CLOSE.publicTransportLimit);
    if (await this.updateRequiredBeforeFrame()) return;
    if (this.unscopedOrAnonymous() && exceedsUtf8(text, LIMITS.publicPayloadBytes)) return this.close(CLOSE.publicPayloadTooLarge);
    const decoded = decodeClientFrame(text);
    if (!decoded.ok) return this.close(CLOSE.invalidFrame);
    const frame = decoded.frame;
    if (frame.type === "auth" && frame.public !== this.record.publicTransport) {
      this.send(frames.authError(frame.id, AUTH_ERRORS.publicTransport, "transient"));
      return this.close(CLOSE.ended);
    }
    await this.dispatch(frame);
  }

  private unscopedOrAnonymous(): boolean {
    const unscoped = this.record.tenant === null && this.record.connection.identity === null;
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
    const control = (f: ClientFrame) => this.forward(this.ports.control.dispatch(this.view(), f));
    const toTenant = (f: ClientFrame) => this.forward(this.ports.tenants.dispatch(this.view(), f));
    switch (frame.type) {
      case "auth":
        return frame.token !== undefined && !frame.public && frame.token.trim().startsWith(SERVICE_TOKEN_PREFIX)
          ? this.serviceAuth(frame)
          : this.auth(frame);
      case "query.call":
        if (frame.scope === "control") return control(frame);
        if (tenant) return toTenant(frame);
        return this.send(frames.queryError(frame.id, frame.path, UNAUTHENTICATED.query));
      case "query.subscribe":
        if (frame.scope === "control") return control(frame);
        if (tenant) return toTenant(frame);
        return this.send(frames.queryError(frame.id, frame.path, UNAUTHENTICATED.subscribe));
      case "query.subscribeMany":
        if (frame.subscribes.length > LIMITS.subscribeMany) return this.send(frames.queryError("", undefined, `query batch cannot contain more than ${LIMITS.subscribeMany} subscribes`));
        for (const subscribe of frame.subscribes) {
          const one: ClientFrame = { type: "query.subscribe", ...subscribe };
          if (subscribe.scope === "control") await control(one);
          else if (tenant) await toTenant(one);
          else this.send(frames.queryError(subscribe.id, subscribe.path, UNAUTHENTICATED.subscribe));
        }
        return;
      case "query.unsubscribe":
        await control(frame);
        if (tenant) await toTenant(frame);
        return;
      case "reducer.call":
        if (frame.scope === "control") return control(frame);
        if (tenant) return toTenant(frame);
        return this.send(frames.reducerError(frame.id, frame.path, UNAUTHENTICATED.reducer, "unauthenticated", frame.trace));
      case "reducer.callMany":
        if (frame.calls.length > LIMITS.callMany) return this.send(frames.reducerError("", undefined, `reducer batch cannot contain more than ${LIMITS.callMany} calls`, "rejected"));
        for (const call of frame.calls) {
          const one: ClientFrame = { type: "reducer.call", ...call };
          if (call.scope === "control") await control(one);
          else if (tenant) await toTenant(one);
          else this.send(frames.reducerError(call.id, call.path, UNAUTHENTICATED.reducer, "unauthenticated", call.trace));
        }
        return;
      case "action.call":
        if (frame.scope === "control") return control(frame);
        if (tenant) return toTenant(frame);
        return this.send(frames.actionError(frame.id, frame.path, UNAUTHENTICATED.action, frame.trace));
      case "replica.open":
        if (tenant) return toTenant(frame);
        return this.send(frames.replicaError(frame.id, frame.path, UNAUTHENTICATED.replica));
      case "replica.openMany":
        if (frame.opens.length > LIMITS.openMany) return this.send(frames.replicaError("", undefined, `replica batch cannot contain more than ${LIMITS.openMany} opens`));
        if (tenant) return toTenant(frame);
        for (const open of frame.opens) this.send(frames.replicaError(open.id, open.path, UNAUTHENTICATED.replica));
        return;
      case "replica.snapshotAck":
        if (tenant && this.record.negotiated.nativeRows) return toTenant(frame);
        return this.send(frames.replicaError(frame.id, undefined, "native snapshot acknowledgements were not negotiated"));
      case "replica.close":
        if (tenant) return toTenant(frame);
        return;
      case "presence.set":
      case "presence.unsubscribe":
        if (tenant && !tenant.anonymous && this.ready.presence) return toTenant(frame);
        return;
      case "presence.subscribe":
        if (tenant && !tenant.anonymous && this.ready.presence) return toTenant(frame);
        return this.send(frames.presenceError(frame.id, this.ready.presence ? UNAUTHENTICATED.presence : "presence v2 is disabled"));
      case "error.register":
      case "error.heartbeat":
      case "error.envelope":
      case "telemetry.event":
      case "telemetry.batch":
        // K15 owns error telemetry and performance events; they reach it through the tenant port.
        if (tenant) return toTenant(frame);
        return;
    }
  }

  private async forward(reply: Promise<string[]>): Promise<void> {
    let out: string[];
    try {
      out = await reply;
    } catch (error) {
      console.error("session: port call failed", error);
      return this.close(CLOSE.failed);
    }
    for (const text of out) this.send(text);
  }

  // ---- auth ----------------------------------------------------------------------

  /** `gvx_svc_` tokens (K17). Until K17 lands every service credential is refused, with Rust's reset. */
  private async serviceAuth(frame: AuthFrame): Promise<void> {
    this.discardScope();
    this.record.clientContract = null;
    this.record.connection = NO_CONNECTION;
    this.hooks.persist?.();
    this.send(frames.authError(frame.id, AUTH_ERRORS.serviceInvalid, "transient"));
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

    const anonymous = connection.tenant?.anonymous ? connection.tenant : null;
    if (anonymous) {
      const scope: [string, string] = [connection.projectId, anonymous.route.tenantId];
      if (record.publicScope && (record.publicScope[0] !== scope[0] || record.publicScope[1] !== scope[1])) {
        this.send(frames.authError(response.ok ? response.id : "", AUTH_ERRORS.publicScopeFixed, "transient"));
        return this.close(CLOSE.publicScopeFixed);
      }
      record.publicScope ??= scope;
    }
    if (connection.identity || connection.tenant) record.pendingUntil = null;
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
    const next = frame.controlOnly ? null : connection.tenant;
    const preserved = !frame.controlOnly && record.tenant !== null && preserveScope(record.connection, connection, frame.capabilities);
    if (preserved) {
      if (response.ok) response.result.subscriptionsPreserved = true;
    } else {
      this.hooks.subscriptionsReset?.();
    }
    const previous = record.tenant;
    record.tenant = next;
    record.connection = connection;
    record.revalidateAt = connection.authToken ? this.now() + LIMITS.revalidateMs : null;
    if (next && !preserved) record.sentRevision = next.admissionRevision;
    if (!sameRoute(previous, next) || (previous?.anonymous ?? false) !== (next?.anonymous ?? false)) this.hooks.tenantChanged?.(previous, next);
    this.hooks.persist?.();
    this.send(response.ok ? frames.authResult(response.id, response.result as Json) : frames.authError(response.id, response.error, response.code));
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
    this.hooks.subscriptionsReset?.();
    this.record.tenant = null;
    this.record.revalidateAt = null;
    if (previous) this.hooks.tenantChanged?.(previous, null);
  }

  // ---- events from the host --------------------------------------------------------

  /** Runs `fn` after the frames already queued, in the socket's order. */
  private serial(fn: () => void | Promise<void>): Promise<void> {
    const next = this.queue.then(fn, fn);
    this.queue = next.catch(() => {});
    return next;
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

  /** Next time this session needs the host's alarm. */
  nextWake(): number | null {
    if (this.closed) return null;
    const times = [this.record.pendingUntil, this.record.revalidateAt];
    if (this.record.tenant && !this.record.tenant.anonymous) times.push(this.lastOutboundAt + LIMITS.livenessMs);
    const due = times.filter((t): t is number => t !== null);
    return due.length ? Math.min(...due) : null;
  }

  // ---- outbound ------------------------------------------------------------------

  private send(text: string): void {
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
    if (this.closed) return;
    this.closed = true;
    const previous = this.record.tenant;
    this.sink.close(frame);
    if (previous) this.hooks.tenantChanged?.(previous, null);
  }

  /** The peer closed or the socket failed. */
  closedByPeer(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.record.tenant) this.hooks.tenantChanged?.(this.record.tenant, null);
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
