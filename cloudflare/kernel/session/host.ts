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
 */
import type { ReadyOptions } from "./capabilities";
import { CLOSE } from "./protocol";
import { ModuleCache, Session, type SessionHooks, type SessionRecord } from "./session";
import type { ModuleInfo, Ports, TenantSession } from "./ports";
import type { PresenceConfig } from "../presence/hub";

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
}

interface Attachment { session: SessionRecord }

export class SocketHost {
  readonly sessions = new Map<WebSocket, Session>();
  readonly modules: ModuleCache;
  private scheduled: number | null | undefined;

  constructor(private readonly options: HostOptions) {
    this.modules = new ModuleCache(options.ports);
    options.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'));
    // Rebuild the in-memory index from attachments on every wake (S3 §8: eager rebuild costs nothing measurable).
    for (const ws of options.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment() as Attachment | null;
      if (attachment?.session) this.sessions.set(ws, this.restore(ws, attachment.session));
    }
  }

  /** Accepts an upgrade. `publicTransport` comes from the router (`?public=1`). */
  accept(publicTransport: boolean, connectionId: string): Response {
    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.options.ctx.acceptWebSocket(server);
    const session = Session.open({
      ...this.sessionOptions(server),
      connectionId,
      publicTransport,
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

  async closed(ws: WebSocket): Promise<void> {
    let session = this.sessions.get(ws);
    if (!session) {
      // A close that wakes a hibernated object: the closing socket is not among `getWebSockets()`
      // when the constructor rebuilds the index (measured in workerd), but its attachment is readable.
      const attachment = ws.deserializeAttachment() as Attachment | null;
      if (attachment?.session) session = this.restore(ws, attachment.session);
    }
    this.sessions.delete(ws);
    await session?.closedByPeer();
  }

  /** The alarm: deadlines, liveness and one batched revalidation round. */
  async alarm(): Promise<void> {
    this.scheduled = null;
    const now = Date.now();
    const work: Array<Promise<void>> = [];
    const revalidate: Session[] = [];
    for (const session of this.sessions.values()) {
      const r = session.record;
      if (r.pendingUntil !== null && now >= r.pendingUntil) work.push(session.deadline(now));
      work.push(session.liveness(now + ALARM_SLACK_MS));
      if (r.revalidateAt !== null && now + ALARM_SLACK_MS >= r.revalidateAt) revalidate.push(session);
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
    let next: number | null = null;
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
      persist: () => this.persist(ws, this.sessions.get(ws)),
      tenantChanged: (previous, next) => this.options.tenantChanged?.(ws, previous, next),
      waitUntil: (promise) => this.options.ctx.waitUntil(promise),
    };
    return {
      ports: this.options.ports,
      ready: this.options.ready,
      host: this.options.name,
      modules: this.modules,
      hooks,
      ...(this.options.presence ? { presence: this.options.presence } : {}),
      sink: {
        send: (text: string) => ws.send(text),
        close: (frame: { code: number; reason: string }) => {
          try { ws.close(frame.code, frame.reason); } catch (error) { console.error("session host: close failed", error); }
          this.sessions.delete(ws);
        },
      },
    };
  }

  private restore(ws: WebSocket, record: SessionRecord): Session {
    return Session.restore(record, this.sessionOptions(ws));
  }

  private persist(ws: WebSocket, session: Session | undefined): void {
    if (!session || session.closed) return;
    try {
      ws.serializeAttachment({ session: session.record } satisfies Attachment);
    } catch (error) {
      // Over the 16 KiB attachment limit (S3 §8). The socket keeps working while
      // the object is awake; after hibernation it resumes from the last record that fit.
      console.error("session host: attachment not saved", error);
    }
  }
}
