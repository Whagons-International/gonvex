// S3 socket spike: one Durable Object holding ~1,000 hibernatable WebSockets,
// fan-out encoded once per visibility class, liveness through
// setWebSocketAutoResponse, and an optional tier of gateway objects.
//
// This is measurement code, not kernel code. Instrumentation that a kernel
// must not do (a wake counter written to SQLite, memory ballast) is marked.

import { DurableObject } from "cloudflare:workers";

export interface Env {
  TENANT: DurableObjectNamespace<TenantDO>;
  GATEWAY: DurableObjectNamespace<GatewayDO>;
  ADMIN_KEY: string;
  BUILD: string;
}

/** Per-socket state that survives hibernation (serializeAttachment, max 16 KiB). */
interface Attachment {
  /** member id */
  m: string;
  /** visibility class, 0..CLASSES-1; -1 = writer/driver socket that gets no fan-out */
  c: number;
  /** cursor: last change sequence the socket had when it attached (resume point) */
  cur: number;
  /** 1 when the client sends the liveness ping frame (auto-response) */
  live: 0 | 1;
  /** connect time (ms) */
  at: number;
}

const PING = '{"type":"ping"}';
const PONG = '{"type":"pong"}';
const CLASSES = 8;
const WORDS =
  "task asset site inspection overdue assigned priority status category workspace team template note comment approval checklist due pending done blocked review urgent low medium high floor building room equipment".split(
    " ",
  );

/** Deterministic pseudo-random generator so every class of one change shares the same row. */
function rng(seed: number) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return (s >>> 0) / 4294967296;
  };
}

/** A ~2 KB change (one replica.transaction with a few rows), projected per visibility class. */
function encodeChange(seq: number, w: number, size: number, classes: number): string[] {
  const r = rng(seq * 2654435761);
  const hex = (n: number) => Array.from({ length: n }, () => Math.floor(r() * 16).toString(16)).join("");
  const text = (n: number) => Array.from({ length: n }, () => WORDS[Math.floor(r() * WORDS.length)]).join(" ");
  const rows: Record<string, unknown>[] = [];
  // Build rows until the class-0 projection is about `size` bytes.
  let approx = 120;
  while (approx < size) {
    const row = {
      id: `${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}`,
      workspaceId: Math.floor(r() * 40),
      statusId: Math.floor(r() * 12),
      priority: Math.floor(r() * 4),
      name: text(4),
      description: text(14),
      assignees: [Math.floor(r() * 1000), Math.floor(r() * 1000)],
      updatedAt: 1762790000000 + seq,
      internalNote: text(6),
      cost: Math.round(r() * 100000) / 100,
    };
    approx += JSON.stringify(row).length + 1;
    rows.push(row);
  }
  const frames: string[] = [];
  for (let c = 0; c < classes; c++) {
    // A visibility class sees a projection: lower classes lose internal fields,
    // and each class sees a different subset of rows (by workspace).
    const visible = rows
      .filter((row, i) => c === 0 || (i + c) % CLASSES !== 0)
      .map((row) => (c % 2 === 0 ? row : { ...row, internalNote: undefined, cost: undefined }));
    frames.push(
      `{"type":"replica.transaction","s":${seq},"w":${w},"c":${c},"revision":${seq},"collection":"tasks","rows":${JSON.stringify(visible)}}`,
    );
  }
  return frames;
}

type DeliverMode = "tags" | "cache" | "attach";

/** Socket delivery, identical whether it runs in the tenant object or in a gateway object. */
class SocketHub {
  private byClass: Map<number, Set<WebSocket>> | null = null;
  readonly lastMessage = new Map<WebSocket, number>();
  rebuildCount = 0;

  constructor(private readonly ctx: DurableObjectState) {}

  /** Rebuild the in-memory index from the attachments (after a wake or eviction). */
  rebuild(): number {
    const map = new Map<number, Set<WebSocket>>();
    let n = 0;
    for (const ws of this.ctx.getWebSockets()) {
      const a = ws.deserializeAttachment() as Attachment | null;
      if (!a) continue;
      let set = map.get(a.c);
      if (!set) map.set(a.c, (set = new Set()));
      set.add(ws);
      n++;
    }
    this.byClass = map;
    this.rebuildCount++;
    return n;
  }

  private index() {
    if (!this.byClass) this.rebuild();
    return this.byClass!;
  }

  accept(request: Request, head: number): Response {
    const url = new URL(request.url);
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair) as [WebSocket, WebSocket];
    const a: Attachment = {
      m: url.searchParams.get("m") ?? crypto.randomUUID(),
      c: Number(url.searchParams.get("c") ?? 0),
      cur: Number(url.searchParams.get("cur") ?? head),
      live: url.searchParams.get("live") === "1" ? 1 : 0,
      at: Date.now(),
    };
    this.ctx.acceptWebSocket(server, [`c${a.c}`, `m:${a.m}`]);
    server.serializeAttachment(a);
    if (this.byClass) {
      let set = this.byClass.get(a.c);
      if (!set) this.byClass.set(a.c, (set = new Set()));
      set.add(server);
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  forget(ws: WebSocket) {
    this.lastMessage.delete(ws);
    if (!this.byClass) return;
    for (const set of this.byClass.values()) set.delete(ws);
  }

  deliver(frames: string[], mode: DeliverMode = "cache"): number {
    let sent = 0;
    if (mode === "tags") {
      for (let c = 0; c < frames.length; c++) {
        for (const ws of this.ctx.getWebSockets(`c${c}`)) {
          try {
            ws.send(frames[c]);
            sent++;
          } catch {}
        }
      }
    } else if (mode === "attach") {
      for (const ws of this.ctx.getWebSockets()) {
        const a = ws.deserializeAttachment() as Attachment | null;
        if (!a || a.c < 0 || a.c >= frames.length) continue;
        try {
          ws.send(frames[a.c]);
          sent++;
        } catch {}
      }
    } else {
      const index = this.index();
      for (let c = 0; c < frames.length; c++) {
        const set = index.get(c);
        if (!set) continue;
        for (const ws of set) {
          try {
            ws.send(frames[c]);
            sent++;
          } catch {}
        }
      }
    }
    return sent;
  }

  counts() {
    const out: Record<string, number> = {};
    for (const [c, set] of this.index()) out[c] = set.size;
    return out;
  }

  /** Liveness sweep: close live sockets whose last ping (auto-response) or message is older than timeoutMs. */
  sweep(timeoutMs: number) {
    const now = Date.now();
    let checked = 0;
    let closed = 0;
    let withTs = 0;
    let oldestAge = 0;
    for (const ws of this.ctx.getWebSockets()) {
      const a = ws.deserializeAttachment() as Attachment | null;
      if (!a || !a.live) continue;
      checked++;
      const auto = this.ctx.getWebSocketAutoResponseTimestamp(ws)?.getTime() ?? 0;
      if (auto) withTs++;
      const last = Math.max(auto, this.lastMessage.get(ws) ?? 0, a.at);
      const age = now - last;
      if (age > oldestAge) oldestAge = age;
      if (age > timeoutMs) {
        try {
          ws.close(4000, "presence liveness timeout");
        } catch {}
        this.forget(ws);
        closed++;
      }
    }
    return { checked, closed, withTs, oldestAge, now };
  }

  autoTimestamps() {
    const now = Date.now();
    const ages: number[] = [];
    let missing = 0;
    for (const ws of this.ctx.getWebSockets()) {
      const t = this.ctx.getWebSocketAutoResponseTimestamp(ws);
      if (t) ages.push(now - t.getTime());
      else missing++;
    }
    ages.sort((x, y) => x - y);
    return {
      count: ages.length,
      missing,
      minAge: ages[0] ?? null,
      medianAge: ages[Math.floor(ages.length / 2)] ?? null,
      maxAge: ages[ages.length - 1] ?? null,
    };
  }
}

// --- spike-only instrumentation ------------------------------------------------
const ballast: unknown[] = [];
let ballastMB = 0;
function addBallast(mb: number, kind: string) {
  for (let i = 0; i < mb; i++) {
    if (kind === "heap") ballast.push(Array.from({ length: 131072 }, (_, j) => j + 0.5));
    else ballast.push(new Uint8Array(1 << 20).fill(i & 255));
    ballastMB++;
  }
  return ballastMB;
}
// -------------------------------------------------------------------------------

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json" } });
}

abstract class SocketObject extends DurableObject<Env> {
  protected hub: SocketHub;
  protected readonly inst = crypto.randomUUID().slice(0, 8);
  protected readonly bootAt = Date.now();
  protected wakes: number;
  protected rebuiltSockets = -1;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.hub = new SocketHub(ctx);
    const sql = ctx.storage.sql;
    sql.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)`);
    // Spike instrumentation: count constructor runs (wakes). A kernel must not write on wake.
    sql.exec(`INSERT INTO meta (k, v) VALUES ('wakes', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + 1`);
    this.wakes = Number(sql.exec(`SELECT v FROM meta WHERE k = 'wakes'`).one().v);
    // Liveness pings are answered by the runtime without waking the object and without billing.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair(PING, PONG));
    if (this.meta("rebuild") !== "lazy") this.rebuiltSockets = this.hub.rebuild();
  }

  protected meta(k: string): string | null {
    const row = this.ctx.storage.sql.exec(`SELECT v FROM meta WHERE k = ?`, k).toArray()[0];
    return row ? String(row.v) : null;
  }
  protected setMeta(k: string, v: string) {
    this.ctx.storage.sql.exec(`INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v`, k, v);
  }

  protected head(): number {
    return 0;
  }

  /** Spike only: the colo running this object (one subrequest per call). */
  async colo(): Promise<string | null> {
    const trace = await (await fetch("https://www.cloudflare.com/cdn-cgi/trace")).text();
    return /colo=(\w+)/.exec(trace)?.[1] ?? null;
  }

  protected info() {
    return {
      inst: this.inst,
      build: this.env.BUILD,
      wakes: this.wakes,
      bootAt: this.bootAt,
      now: Date.now(),
      sockets: this.ctx.getWebSockets().length,
      rebuiltSockets: this.rebuiltSockets,
      rebuildCount: this.hub.rebuildCount,
      ballastMB,
    };
  }

  /** Admin operations shared by both placements. Returns null when not handled. */
  protected async adminCommon(op: string, url: URL): Promise<Response | null> {
    const q = (k: string, d: string) => url.searchParams.get(k) ?? d;
    switch (op) {
      case "stats":
        return json({ ...this.info(), classes: this.hub.counts(), auto: this.hub.autoTimestamps() });
      case "fanout": {
        // n fan-outs of one change each, encoded once per class, in one event.
        const n = Number(q("n", "1"));
        const size = Number(q("size", "2048"));
        const classes = Number(q("classes", String(CLASSES)));
        const mode = q("mode", "cache") as DeliverMode;
        const encodeOnly = q("encodeOnly", "0") === "1";
        let sent = 0;
        let bytes = 0;
        for (let i = 0; i < n; i++) {
          const frames = encodeChange(i + 1, Number(q("w", "0")), size, classes);
          bytes = frames.reduce((s, f) => s + f.length, 0) / frames.length;
          if (!encodeOnly) sent += this.hub.deliver(frames, mode);
        }
        return json({ n, sent, avgFrameBytes: Math.round(bytes), mode, inst: this.inst });
      }
      case "touch": {
        // Rewrite every attachment (e.g. to persist cursors). Measures serializeAttachment cost.
        let n = 0;
        const reps = Number(q("reps", "1"));
        for (let r = 0; r < reps; r++)
          for (const ws of this.ctx.getWebSockets()) {
            const a = ws.deserializeAttachment() as Attachment;
            a.cur++;
            ws.serializeAttachment(a);
            n++;
          }
        return json({ touched: n });
      }
      case "rebuild": {
        const reps = Number(q("reps", "1"));
        let n = 0;
        for (let r = 0; r < reps; r++) n = this.hub.rebuild();
        return json({ rebuilt: n, reps });
      }
      case "ballast":
        return json({ ballastMB: addBallast(Number(q("mb", "1")), q("kind", "ab")), inst: this.inst });
      case "auto":
        return json(this.hub.autoTimestamps());
      case "pad": {
        // Grow every attachment by `bytes` of padding: finds the attachment size limit and
        // lets a wake rebuild large attachments.
        const bytes = Number(q("bytes", "0"));
        let ok = 0;
        let err: string | null = null;
        for (const ws of this.ctx.getWebSockets()) {
          const a = ws.deserializeAttachment() as Attachment & { pad?: string };
          a.pad = "x".repeat(bytes);
          try {
            ws.serializeAttachment(a);
            ok++;
          } catch (e) {
            err = String(e);
            break;
          }
        }
        return json({ ok, err, bytes });
      }
      case "sweep":
        return json(this.hub.sweep(Number(q("timeout", "60000"))));
      case "sendbig": {
        const bytes = Number(q("bytes", "1048576"));
        const target = this.ctx.getWebSockets(`m:${q("m", "big")}`)[0];
        if (!target) return json({ error: "no socket" }, 404);
        try {
          if (q("kind", "text") === "binary") target.send(new Uint8Array(bytes).fill(97));
          else target.send("a".repeat(bytes));
          return json({ sent: bytes });
        } catch (e) {
          return json({ error: String(e) }, 500);
        }
      }
      case "set":
        this.setMeta(q("name", ""), q("value", ""));
        return json({ ok: true });
      case "get":
        return json({ value: this.meta(q("name", "")) });
      case "where":
        return json({ colo: await this.colo(), inst: this.inst });
      case "closeall": {
        let n = 0;
        for (const ws of this.ctx.getWebSockets()) {
          try {
            ws.close(1000, "spike reset");
          } catch {}
          n++;
        }
        this.hub.rebuild();
        return json({ closed: n });
      }
    }
    return null;
  }

  override async webSocketClose(ws: WebSocket, code: number, reason: string) {
    this.hub.forget(ws);
  }
  override async webSocketError(ws: WebSocket) {
    this.hub.forget(ws);
  }
}

export class TenantDO extends SocketObject {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const sql = ctx.storage.sql;
    sql.exec(`CREATE TABLE IF NOT EXISTS changes (seq INTEGER PRIMARY KEY, at INTEGER NOT NULL, body TEXT NOT NULL)`);
    sql.exec(`CREATE TABLE IF NOT EXISTS gateways (name TEXT PRIMARY KEY)`);
  }

  protected override head(): number {
    return Number(this.ctx.storage.sql.exec(`SELECT COALESCE(MAX(seq), 0) AS h FROM changes`).one().h);
  }

  private gateways(): string[] {
    return this.ctx.storage.sql.exec(`SELECT name FROM gateways`).toArray().map((r) => String(r.name));
  }

  /** RPC from a gateway object on its first socket. */
  async registerGateway(name: string) {
    this.ctx.storage.sql.exec(`INSERT OR IGNORE INTO gateways (name) VALUES (?)`, name);
    return { ok: true };
  }

  /** One commit: append to the change log, encode once per class, deliver locally and once per gateway. */
  private async commit(w: number, size: number, mode: DeliverMode) {
    const sql = this.ctx.storage.sql;
    const seq = this.head() + 1;
    const frames = encodeChange(seq, w, size, CLASSES);
    sql.exec(`INSERT INTO changes (seq, at, body) VALUES (?, ?, ?)`, seq, Date.now(), frames[0]);
    const sent = this.hub.deliver(frames, mode);
    const gws = this.gateways();
    let hop: number[] = [];
    let gwSent = 0;
    if (gws.length) {
      const t0 = Date.now();
      const results = await Promise.all(
        gws.map(async (name) => {
          const r = await this.env.GATEWAY.getByName(name).deliver(frames, t0);
          return { ...r, ms: Date.now() - t0 };
        }),
      );
      hop = results.map((r) => r.ms);
      gwSent = results.reduce((s, r) => s + r.sent, 0);
    }
    return { seq, sent, gwSent, hop, bytes: frames[0].length };
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.headers.get("Upgrade") === "websocket") return this.hub.accept(request, this.head());
    const op = url.pathname.split("/").pop() ?? "";
    const common = await this.adminCommon(op, url);
    if (common) return common;
    switch (op) {
      case "liveness": {
        const on = url.searchParams.get("on") === "1";
        this.setMeta("liveness", on ? `${url.searchParams.get("interval") ?? "20000"}:${url.searchParams.get("timeout") ?? "60000"}` : "");
        if (on) await this.ctx.storage.setAlarm(Date.now() + Number(url.searchParams.get("interval") ?? "20000"));
        else await this.ctx.storage.deleteAlarm();
        return json({ ok: true });
      }
      case "hop": {
        // RPC round trip tenant -> gateway with an empty delivery, per gateway.
        // names=a,b,... also creates gateways from here (placed near this object).
        const names = url.searchParams.get("names")?.split(",") ?? this.gateways();
        const reps = Number(url.searchParams.get("reps") ?? "20");
        const out = [];
        for (const name of names) {
          const stub = this.env.GATEWAY.getByName(name);
          const ms: number[] = [];
          for (let i = 0; i < reps; i++) {
            const t = Date.now();
            await stub.deliver([], t);
            ms.push(Date.now() - t);
          }
          ms.sort((a, b) => a - b);
          out.push({ name, colo: await stub.colo(), min: ms[0], p50: ms[Math.floor(ms.length / 2)], max: ms[ms.length - 1] });
        }
        return json({ tenantColo: await this.colo(), hops: out });
      }
      case "commit":
        return json(await this.commit(Number(url.searchParams.get("w") ?? "0"), Number(url.searchParams.get("size") ?? "2048"), "cache"));
      case "reset":
        this.ctx.storage.sql.exec(`DELETE FROM changes`);
        this.ctx.storage.sql.exec(`DELETE FROM gateways`);
        return json({ ok: true });
    }
    return json({ error: `unknown op ${op}` }, 404);
  }

  override async alarm() {
    const cfg = this.meta("liveness");
    if (!cfg) return;
    const [interval, timeout] = cfg.split(":").map(Number);
    const r = this.hub.sweep(timeout);
    this.setMeta("lastSweep", JSON.stringify(r));
    // Re-arm only while live sockets remain; an empty tenant sleeps for free.
    if (r.checked - r.closed > 0) await this.ctx.storage.setAlarm(Date.now() + interval);
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    const now = Date.now();
    this.hub.lastMessage.set(ws, now);
    if (typeof message !== "string") {
      ws.send(JSON.stringify({ type: "big", len: message.byteLength, kind: "binary" }));
      return;
    }
    if (message.length > 4096) {
      ws.send(JSON.stringify({ type: "big", len: message.length, kind: "text" }));
      return;
    }
    const msg = JSON.parse(message) as { op: string; w?: number; size?: number; mode?: DeliverMode; id?: number };
    if (msg.op === "echo") {
      ws.send(JSON.stringify({ type: "echo", id: msg.id, w: msg.w, ...this.info() }));
    } else if (msg.op === "write") {
      const r = await this.commit(msg.w ?? 0, msg.size ?? 2048, msg.mode ?? "cache");
      ws.send(JSON.stringify({ type: "ack", w: msg.w, ...r }));
    }
  }
}

export class GatewayDO extends SocketObject {
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.headers.get("Upgrade") === "websocket") {
      const name = request.headers.get("x-gateway-name")!;
      const tenant = request.headers.get("x-tenant")!;
      if (this.meta("registered") !== name) {
        await this.env.TENANT.getByName(tenant).registerGateway(name);
        this.setMeta("registered", name);
      }
      return this.hub.accept(request, 0);
    }
    const op = url.pathname.split("/").pop() ?? "";
    return (await this.adminCommon(op, url)) ?? json({ error: `unknown op ${op}` }, 404);
  }

  /** RPC from the tenant object: one call per commit per gateway, same bytes for every socket of a class. */
  async deliver(frames: string[], sentAt: number) {
    return { sent: this.hub.deliver(frames, "cache"), at: Date.now(), sentAt };
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    this.hub.lastMessage.set(ws, Date.now());
    if (typeof message === "string" && message.includes('"echo"')) {
      const msg = JSON.parse(message);
      ws.send(JSON.stringify({ type: "echo", id: msg.id, w: msg.w, ...this.info() }));
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.searchParams.get("k") !== env.ADMIN_KEY) return new Response("forbidden", { status: 403 });
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts[0] === "ws") {
      const tenant = url.searchParams.get("t") ?? "t1";
      const gw = url.searchParams.get("gw");
      if (gw !== null) {
        const name = `${tenant}/gw${gw}`;
        const headers = new Headers(request.headers);
        headers.set("x-gateway-name", name);
        headers.set("x-tenant", tenant);
        return env.GATEWAY.getByName(name).fetch(new Request(request, { headers }));
      }
      return env.TENANT.getByName(tenant).fetch(request);
    }
    if (parts[0] === "admin" && parts[1] === "t") return env.TENANT.getByName(parts[2]).fetch(request);
    if (parts[0] === "admin" && parts[1] === "g") return env.GATEWAY.getByName(`${parts[2]}/gw${parts[3]}`).fetch(request);
    if (parts[0] === "bytes") {
      // Bandwidth probe from the edge to the load host.
      const total = Number(url.searchParams.get("n") ?? "10000000");
      const chunk = new Uint8Array(65536);
      crypto.getRandomValues(chunk);
      let left = total;
      const body = new ReadableStream({
        pull(controller) {
          if (left <= 0) return controller.close();
          const n = Math.min(left, chunk.length);
          controller.enqueue(chunk.subarray(0, n));
          left -= n;
        },
      });
      return new Response(body, { headers: { "content-type": "application/octet-stream" } });
    }
    return new Response("not found", { status: 404 });
  },
};
