import { DurableObject } from "cloudflare:workers";

export interface GreetResult {
  id: number;
  name: string;
  rowsRead: number;
  rowsWritten: number;
}

/**
 * Hello-world Durable Object (P01). Proves the platform pieces the tenant
 * object will use: SQLite storage, a write through it, and a hibernatable
 * WebSocket. One instance per name, like one tenant object per tenant.
 */
export class HelloObject extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    // Synchronous and idempotent; runs before any event is delivered.
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS greetings (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at INTEGER NOT NULL)",
    );
  }

  /** RPC: write one row and report the storage cost of doing so. */
  greet(name: string): GreetResult {
    const cursor = this.sql.exec<{ id: number }>(
      "INSERT INTO greetings (name, created_at) VALUES (?, ?) RETURNING id",
      name,
      Date.now(),
    );
    const { id } = cursor.one();
    return { id, name, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
  }

  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("expected a WebSocket upgrade", { status: 426 });
    }
    const { 0: client, 1: server } = new WebSocketPair();
    // Hibernation API: the object can be evicted while the socket stays open.
    this.ctx.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  override webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    ws.send(message);
  }

  override webSocketClose(ws: WebSocket, code: number, reason: string): void {
    ws.close(code, reason);
  }
}
