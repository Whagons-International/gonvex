/**
 * `GET /ws` in the router Worker (CONTRACT.md A.1.1). The Worker cannot know
 * the tenant (it arrives in the first `auth` frame, 0004), so it only checks
 * the handshake and hands the upgrade to a gateway (`placement/gateway.ts`).
 *
 * - `?public=1` selects the public transport; any other value does not.
 * - An invalid `Sec-WebSocket-Extensions` field is HTTP 400, as in Rust
 *   (`ws/negotiation.rs`). Valid offers pass through: workerd negotiates
 *   permessage-deflate itself (A.1.6 adapt, S3 §4).
 * - Per-address pending and public connection limits (HTTP 429) belong to
 *   K16 and plug in here.
 */
import { gatewayFor, type GatewayEnv } from "./placement/gateway";

export interface WebSocketRouteEnv extends GatewayEnv {
  GATEWAY: DurableObjectNamespace;
}

export async function handleWebSocketUpgrade(request: Request, env: WebSocketRouteEnv): Promise<Response> {
  if (request.method !== "GET") return new Response(null, { status: 405, headers: { allow: "GET" } });
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("Connection header did not include 'upgrade'", { status: 400 });
  const extensions = request.headers.get("sec-websocket-extensions");
  if (extensions !== null && !validExtensionsField(extensions)) return new Response(null, { status: 400 });
  const url = new URL(request.url);
  const connectionId = crypto.randomUUID();
  const headers = new Headers(request.headers);
  headers.set("x-gonvex-connection-id", connectionId);
  headers.set("x-gonvex-public", url.searchParams.get("public") === "1" ? "1" : "0");
  const gateway = env.GATEWAY.getByName(gatewayFor(env, connectionId));
  return gateway.fetch(new Request(request.url, { method: "GET", headers }));
}

const TOKEN = /^[A-Za-z0-9!#$%&'*+\-.^_`|~]+/;

/**
 * RFC 6455 extension list grammar as `ws/negotiation.rs` parses it: an invalid
 * field rejects the handshake; unknown extensions and bad parameter values do
 * not (they only decline that offer). A field with no extension at all is
 * invalid. `Headers.get` joins repeated fields with ", ", which the list
 * grammar accepts the same way.
 */
export function validExtensionsField(field: string): boolean {
  let rest = field;
  let members = 0;
  const ows = () => { rest = rest.replace(/^[ \t]*/, ""); };
  const take = (c: string) => { if (rest.startsWith(c)) { rest = rest.slice(1); return true; } return false; };
  const token = (): boolean => { const m = TOKEN.exec(rest); if (!m) return false; rest = rest.slice(m[0].length); return true; };
  const value = (): boolean => {
    if (!take('"')) return token();
    for (;;) {
      if (rest.length === 0) return false;
      const c = rest[0]!;
      rest = rest.slice(1);
      if (c === '"') return true;
      if (c === "\\") { if (rest.length === 0) return false; rest = rest.slice(1); }
    }
  };
  for (;;) {
    ows();
    if (take(",")) continue;
    if (rest.length === 0) break;
    if (!token()) return false;
    members++;
    for (;;) {
      ows();
      if (!take(";")) break;
      ows();
      if (!token()) return false;
      ows();
      if (take("=")) { ows(); if (!value()) return false; }
    }
    ows();
    if (rest.length === 0) break;
    if (!take(",")) return false;
  }
  return members > 0;
}
