// Gateway script for the split-deploy probe: holds the sockets (hibernation API) and calls the
// tenant object in another script by RPC. It is never redeployed during the probe.
import { DurableObject } from "cloudflare:workers";

export class Gateway extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.inst = crypto.randomUUID().slice(0, 8); this.lastBuild = null; }
  async fetch(request) {
    const url = new URL(request.url);
    if (request.headers.get("upgrade") === "websocket") {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    return Response.json({ inst: this.inst, sockets: this.ctx.getWebSockets().length, lastBuild: this.lastBuild });
  }
  async webSocketMessage(socket, message) {
    if (message !== "invoke") return;
    const started = Date.now();
    try {
      const tenant = this.env.TENANT.get(this.env.TENANT.idFromName("split-tenant"));
      const result = await tenant.run(1);
      if (this.lastBuild !== null && result.build !== this.lastBuild) {
        // The generation changed: system.reload to every socket on this gateway, as the kernel would.
        const frame = JSON.stringify({ type: "system.reload", reason: "module generation changed", artifactHash: result.build });
        for (const peer of this.ctx.getWebSockets()) { try { peer.send(frame); } catch { /* closing */ } }
      }
      this.lastBuild = result.build;
      socket.send(JSON.stringify({ type: "result", ok: true, build: result.build, inst: result.inst, ms: Date.now() - started }));
    } catch (error) {
      socket.send(JSON.stringify({ type: "result", ok: false, error: String(error?.message ?? error).slice(0, 200), retryable: error?.retryable ?? null, overloaded: error?.overloaded ?? null }));
    }
  }
  webSocketClose(socket, code, reason) { try { socket.close(code, reason); } catch { /* closed */ } }
}
export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (!env.ADMIN_KEY || url.searchParams.get("k") !== env.ADMIN_KEY) return new Response("forbidden", { status: 403 });
    return env.GATEWAY.get(env.GATEWAY.idFromName(url.searchParams.get("g") ?? "g0")).fetch(request);
  },
};
