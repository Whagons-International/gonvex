/**
 * Hosts sessions on hibernatable WebSockets inside one Durable Object. The
 * object that embeds it (a gateway by default, see `placement/`) forwards its
 * `fetch` upgrade, `webSocketMessage`, `webSocketClose`, `webSocketError` and
 * `alarm` handlers here.
 *
 * - One `SessionRecord` per socket in its attachment, rewritten only when the
 *   record changes (auth, revalidation, liveness), never per delivered frame.
 * - Every timer is the object's single alarm (G0-9): the pending-auth
 *   deadline, the 30 s revalidation round (one batched control call per
 *   round) and the 20 s liveness watermark (G0-6).
 * - `{"type":"ping"}` is answered `{"type":"pong"}` by the runtime's
 *   auto-response, without waking the object (G0-6).
 * - K16: the public admission monitor runs on the same alarm, one Control
 *   Plane read per pinned scope per round; slots a socket holds in its
 *   address limiter are freed on first auth (pending) and on close.
 */
import type { ReadyOptions } from "./capabilities";
import { CLOSE } from "./protocol";
import { ModuleCache, Session, type SessionHooks, type SessionRecord } from "./session";
import type { ModuleInfo, Ports, TenantSession } from "./ports";
import type { PresenceConfig } from "../presence/hub";
import { UNKNOWN_ADDRESS } from "../public/address";
import { type PublicGuard, validatePublicScope } from "../public/guard";
import type { SlotKind } from "../public/limiter";
import { type TelemetryHostState, telemetryHostState } from "../telemetry/socket";
import { HolderRecoveryStore, recoveryScope, attachmentRecord, type AttachmentRecord, type RecoveryReference } from "./recovery-store";

/** Liveness and revalidation due within this window run in the same alarm (deadlines never run early). */
const ALARM_SLACK_MS = 2_000;

export interface HostOptions {
  ctx: DurableObjectState;
  ports: Ports;
  ready: ReadyOptions;
  /** This object's name, reported to ports and tenants. */
  name: string;
  /** The placement's reaction to a socket entering or leaving a tenant. */
  tenantChanged?(socket: WebSocket, previous: TenantSession | null, next: TenantSession | null): void;
  /** Presence v2 settings (K14); default: on when `ready.presence`. */
  presence?: PresenceConfig;
  /** K16: public-function limits and the address budgets. */
  publicGuard?: PublicGuard;
  /** K15: telemetry state shared by this object's sockets (rate limits per owning object, 0012 R11). */
  telemetry?: TelemetryHostState;
  /** X01: aggregate actual outbound work at the common sink, including live-query/replica batches. */
  sent?(text: string): void;
  /** Placement shares the host alarm; null when no reconnect work is due. */
  placementWake?(): number | null;
}

interface Attachment { session: AttachmentRecord; recovery?: RecoveryReference }

export class SocketHost {
  ownsConnection(connectionId: string): boolean { return this.recovery.ownsConnection(connectionId); }
  readonly sessions = new Map<WebSocket, Session>();
  readonly modules: ModuleCache;
  /** K15: one set of telemetry rate-limit windows for every socket of this object. */
  readonly telemetry: TelemetryHostState;
  private scheduled: number | null | undefined;
  private readonly recovery: HolderRecoveryStore;
  private readonly recoveryDirty = new WeakSet<WebSocket>();
  private readonly recoveryRefs = new WeakMap<WebSocket, RecoveryReference>();
  private readonly publishing = new WeakSet<WebSocket>();
  private readonly socketOwners = new WeakMap<WebSocket, string>();
  private recoveryCleanupAt: number | null = null;

  constructor(private readonly options: HostOptions) {
    this.recovery = new HolderRecoveryStore(options.ctx.storage);
    this.modules = new ModuleCache(options.ports);
    this.telemetry = options.telemetry ?? telemetryHostState({ presenceEnabled: options.presence?.enabled ?? options.ready.presence });
    options.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'));
    // Rebuild the in-memory index from attachments on every wake (S3 §8: eager rebuild costs nothing measurable).
    const active = new Set<string>();
    for (const ws of options.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment() as Attachment | null;
      if (attachment?.session) {
        try {
          this.sessions.set(ws, this.restore(ws, attachment.session, attachment.recovery));
          if (attachment.recovery) active.add(attachment.recovery.generation);
        }
        catch (error) {
          console.error("session host: recovery metadata unavailable", error);
          try { ws.close(1012, "session recovery requires reconnect"); } catch { /* already closed */ }
          this.abandonRecovery(ws);
          this.releaseAttachmentSlots(attachment.session);
        }
      }
    }
    this.recoveryCleanupAt = this.recovery.sweep(active);
    this.reschedule();
  }

  /**
   * Accepts an upgrade. `publicTransport` comes from the router (`?public=1`);
   * `address` and `slots` from the router's reservation in the address limiter (K16).
   */
  accept(publicTransport: boolean, connectionId: string, address: string = UNKNOWN_ADDRESS, slots: SlotKind[] = []): Response {
    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    const socketOwner = crypto.randomUUID();
    this.options.ctx.acceptWebSocket(server, [`recovery:${socketOwner}`]);
    this.socketOwners.set(server, socketOwner);
    const session = Session.open({
      ...this.sessionOptions(server),
      connectionId,
      publicTransport,
      address,
      slots,
    });
    this.sessions.set(server, session);
    this.persist(server, session);
    this.reschedule();
    return new Response(null, { status: 101, webSocket: client });
  }

  async message(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const session = this.sessions.get(ws);
    if (!session) return ws.close(CLOSE.failed.code, CLOSE.failed.reason);
    await session.receive(message);
    this.reschedule();
  }

  async closed(ws: WebSocket, code?: number, reason?: string): Promise<void> {
    // The hibernation API leaves the close handshake to the object: answer the client's close frame, as Rust does,
    // so the client sees its socket closed (and reconnects) at once rather than when the TCP connection times out.
    if (code !== undefined) {
      try { ws.close(code === 1005 || code === 1006 || code === 1015 ? 1000 : code, reason ?? ""); } catch { /* already closed */ }
    }
    let session = this.sessions.get(ws);
    if (!session) {
      // A close that wakes a hibernated object: the closing socket is not among `getWebSockets()`
      // when the constructor rebuilds the index (measured in workerd), but its attachment is readable.
      const attachment = ws.deserializeAttachment() as Attachment | null;
      if (attachment?.session) {
        try { session = this.restore(ws, attachment.session, attachment.recovery); }
        catch { this.releaseAttachmentSlots(attachment.session); }
      }
    }
    this.sessions.delete(ws);
    if (session) this.releaseSlots(ws, session, session.record.slots ?? []);
    await session?.closedByPeer();
    this.abandonRecovery(ws);
  }

  /** The alarm: deadlines, liveness and one batched revalidation round. */
  async alarm(): Promise<void> {
    this.scheduled = null;
    const now = Date.now();
    const active = new Set<string>();
    for (const ws of this.sessions.keys()) { const ref = this.recoveryRefs.get(ws); if (ref) active.add(ref.generation); }
    this.recoveryCleanupAt = this.recovery.sweep(active, now);
    const work: Array<Promise<void>> = [];
    const revalidate: Session[] = [];
    const admissions = new Map<string, { scope: readonly [string, string]; sessions: Session[] }>();
    for (const session of this.sessions.values()) {
      const r = session.record;
      if (r.pendingUntil !== null && now >= r.pendingUntil) work.push(session.deadline(now));
      work.push(session.liveness(now + ALARM_SLACK_MS));
      if (r.revalidateAt !== null && now + ALARM_SLACK_MS >= r.revalidateAt) revalidate.push(session);
      const due = session.admissionDueAt(), scope = session.publicScope();
      if (due !== null && scope && now >= due) {
        const key = `${scope[0]}\u0000${scope[1]}`;
        let group = admissions.get(key);
        if (!group) admissions.set(key, group = { scope, sessions: [] });
        group.sessions.push(session);
      }
    }
    // K16: the admission monitor, one Control Plane read per pinned scope (not per socket).
    for (const { scope, sessions } of admissions.values()) {
      work.push(validatePublicScope(this.options.ports.control, scope).then(valid => Promise.all(sessions.map(s => s.admissionMonitored(valid, now)))).then(() => {}));
    }
    await Promise.all(work);
    const items = revalidate.map(s => s.revalidationItem());
    const due = revalidate.filter((_, i) => items[i] !== null);
    if (due.length) {
      let valid: boolean[];
      try {
        valid = await this.options.ports.control.revalidate(items.filter(i => i !== null));
      } catch (error) {
        // Rust treats a failed round as "not revoked" and tries again next interval.
        console.error("session host: revalidation round failed", error);
        valid = due.map(() => true);
      }
      await Promise.all(due.map((s, i) => s.revalidated(valid[i] ?? true, now)));
    }
    this.reschedule();
  }

  /** A deploy changed a project's module (A.1.5 c). */
  async moduleReloaded(projectId: string, module: ModuleInfo | null): Promise<number> {
    this.modules.set(projectId, module);
    const affected = [...this.sessions.values()].filter(s => !s.closed && s.record.connection.projectId === projectId);
    await Promise.all(affected.map(s => s.moduleReloaded(module)));
    return affected.length;
  }

  /** Arms the single alarm for the earliest session need. */
  reschedule(): void {
    const placement = this.options.placementWake?.() ?? null;
    let next: number | null = this.recoveryCleanupAt;
    if (placement !== null) next = Math.min(next ?? Infinity, placement);

    for (const session of this.sessions.values()) {
      const at = session.nextWake();
      if (at !== null && (next === null || at < next)) next = at;
    }
    if (next === null || next === this.scheduled) return;
    // Only move the alarm earlier; a later alarm re-arms itself when it fires.
    if (this.scheduled != null && this.scheduled <= next) return;
    this.scheduled = next;
    void this.options.ctx.storage.setAlarm(Math.max(next, Date.now() + 10));
  }

  private sessionOptions(ws: WebSocket) {
    const hooks: SessionHooks = {
      recoveryChanged: () => this.recoveryDirty.add(ws),
      persist: () => this.persist(ws, this.sessions.get(ws)),
      tenantChanged: (previous, next) => this.options.tenantChanged?.(ws, previous, next),
      waitUntil: (promise) => this.options.ctx.waitUntil(promise),
      pendingCleared: () => {
        const session = this.sessions.get(ws);
        if (session) this.releaseSlots(ws, session, ["pending"]);
      },
    };
    return {
      ports: this.options.ports,
      ready: this.options.ready,
      host: this.options.name,
      modules: this.modules,
      hooks,
      ...(this.options.presence ? { presence: this.options.presence } : {}),
      ...(this.options.publicGuard ? { publicGuard: this.options.publicGuard } : {}),
      telemetry: this.telemetry,
      sink: {
        send: (text: string) => { ws.send(text); this.options.sent?.(text); },
        close: (frame: { code: number; reason: string }) => {
          const session = this.sessions.get(ws);
          if (session) this.releaseSlots(ws, session, session.record.slots ?? []);
          try { ws.close(frame.code, frame.reason); } catch (error) { console.error("session host: close failed", error); }
          this.sessions.delete(ws);
          this.abandonRecovery(ws);
        },
      },
    };
  }

  /**
   * K16: frees `kinds` of the slots `session` holds in its address limiter,
   * and records that in the attachment at once, so a close that restores the
   * record afterwards does not release them a second time.
   */
  private releaseSlots(ws: WebSocket, session: Session, kinds: readonly SlotKind[]): void {
    const held = session.record.slots ?? [];
    const freed = held.filter(k => kinds.includes(k));
    if (!freed.length) return;
    session.record.slots = held.filter(k => !freed.includes(k));
    this.persist(ws, session);
    const guard = this.options.publicGuard;
    if (guard) this.options.ctx.waitUntil(guard.release(session.record.address ?? UNKNOWN_ADDRESS, session.record.connectionId, freed));
  }

  /** A corrupt private identity cannot resume, but its trusted attachment's
   * primitive reservation locator can still release this closing socket. */
  private releaseAttachmentSlots(record: AttachmentRecord): void {
    const held = record.slots ?? [];
    if (held.length && this.options.publicGuard) this.options.ctx.waitUntil(this.options.publicGuard.release(record.address ?? UNKNOWN_ADDRESS, record.connectionId, held));
  }

  /** K16: which of `connectionIds` are open on this object (the address limiter's audit). */
  liveConnections(connectionIds: readonly string[]): string[] {
    const open = new Set<string>();
    for (const session of this.sessions.values()) if (!session.closed) open.add(session.record.connectionId);
    return connectionIds.filter(id => open.has(id));
  }

  private restore(ws: WebSocket, record: AttachmentRecord, ref?: RecoveryReference): Session {
    if (ref) {
      const tagged = this.options.ctx.getTags(ws).find((tag) => tag.startsWith("recovery:"))?.slice("recovery:".length);
      if (tagged !== ref.socketOwner) throw new Error("holder recovery socket ownership mismatch");
      record = this.recovery.restore(record, ref);
      this.recoveryRefs.set(ws, ref);
      this.socketOwners.set(ws, ref.socketOwner);
    } else {
      if (!record.connection || !("tenant" in record) || record.privateIdentity) throw new Error("holder recovery identity unavailable");
      this.recoveryDirty.add(ws); // Old attachments migrate on their next semantic/attachment change.
    }
    return Session.restore(record as SessionRecord, this.sessionOptions(ws));
  }

  private abandonRecovery(ws: WebSocket): void {
    const owner = this.socketOwners.get(ws) ?? this.options.ctx.getTags(ws).find((tag) => tag.startsWith("recovery:"))?.slice("recovery:".length);
    if (owner) this.recovery.abandon(owner);
    this.socketOwners.delete(ws);
    this.recoveryRefs.delete(ws);
    this.recoveryDirty.delete(ws);
  }

  private persist(ws: WebSocket, session: Session | undefined): void {
    if (!session || session.closed || this.publishing.has(ws)) return;
    this.publishing.add(ws);
    const previous = this.recoveryRefs.get(ws);
    let next = previous;
    try {
      if (!previous || this.recoveryDirty.has(ws) || previous.scope !== recoveryScope(session.record)) next = this.recovery.write(session.record, previous, this.socketOwners.get(ws));
      ws.serializeAttachment({ session: attachmentRecord(session.record), recovery: next! } satisfies Attachment);
      this.recoveryRefs.set(ws, next!);
      this.socketOwners.set(ws, next!.socketOwner);
      if (next !== previous) this.recovery.commit(next!);
      this.recoveryDirty.delete(ws);
    } catch (error) {
      // Do not leave the socket running under a stale current-looking attachment.
      if (next && next !== previous) { try { this.recovery.discard(next); } catch { /* wake sweep reclaims an abandoned generation */ } }
      console.error("session host: attachment/recovery publish failed", error);
      session.close({ code: 1012, reason: "session recovery requires reconnect" });
      return;
    } finally {
      this.publishing.delete(ws);
    }
    if (previous && previous !== next) { try { this.recovery.discard(previous); } catch { /* safe orphan; never invalidate the published generation */ } }
  }
}
