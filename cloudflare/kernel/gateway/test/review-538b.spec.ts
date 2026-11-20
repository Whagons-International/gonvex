// Re-review #538 at b21ca1a2: gaps in the per-client shared holder.
import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DIRECT_HEADER, hashConnection, type GatewayAssignment } from "../placement";
import { handleWebSocketUpgrade, resetWarmCell, WarmCell } from "../../session/router";
import type { GatewayControllerObject } from "../controller";
import type { GatewayObject } from "../../session";

const bindings = env as unknown as Omit<Env, "GATEWAY" | "GATEWAY_CONTROLLER"> & {
  GATEWAY: DurableObjectNamespace<GatewayObject>; GATEWAY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject>;
};
const sockets: WebSocket[] = [];
beforeEach(() => resetWarmCell());
afterEach(() => { vi.useRealTimers(); });
function accepted(r: Response) { expect(r.status).toBe(101); r.webSocket!.accept(); sockets.push(r.webSocket!); }
async function closeAll() { for (const ws of sockets.splice(0)) ws.close(1000); await new Promise((r) => setTimeout(r, 300)); }
function counting() {
  const calls = { acquire: 0, fetches: [] as Array<{ name: string; direct: string | null }> };
  const wrapped = { ...bindings,
    GATEWAY_CONTROLLER: { getByName(name: string) { const a = bindings.GATEWAY_CONTROLLER.getByName(name);
      return { acquire: (id: string) => { calls.acquire++; return a.acquire(id); }, release: (x: GatewayAssignment) => a.release(x) }; } },
    GATEWAY: { getByName(name: string) { return { fetch: (r: Request) => { calls.fetches.push({ name, direct: r.headers.get(DIRECT_HEADER) }); return bindings.GATEWAY.getByName(name).fetch(r); } }; } },
  } as unknown as typeof bindings;
  return { calls, wrapped };
}
function context() { const work: Promise<unknown>[] = []; return { ctx: { waitUntil: (p: Promise<unknown>) => { work.push(p); } }, settle: async () => { while (work.length) await Promise.allSettled(work.splice(0)); } }; }
const cache = () => (globalThis as unknown as { caches: { default: Cache } }).caches.default;
const key = (host: string, ip: string) => new Request(`https://gonvex-warm-holder.invalid/${encodeURIComponent(host)}/${hashConnection(ip).toString(36)}`);

describe("re-review #538 (b21ca1a2)", () => {
  it("a reload's first socket does not pay a direct hop on a hint older than the socket-2 window (x18k s2: 17 s old, direct-refused 160 ms)", async () => {
    const { calls, wrapped } = counting(); const { ctx, settle } = context();
    const up = () => new Request("https://r538b.test/ws", { headers: { upgrade: "websocket", "cf-connecting-ip": "192.0.2.150" } });
    accepted(await handleWebSocketUpgrade(up(), wrapped, ctx)); await settle();
    // The previous page load's entry, 17 s old (x18k s1 -> s2), whose holder no longer takes it (here: a changed epoch).
    const old = JSON.parse(await (await cache().match(key("r538b.test", "192.0.2.150")))!.text());
    await cache().put(key("r538b.test", "192.0.2.150"), new Response(JSON.stringify({ ...old, epoch: old.epoch + 1 }), { headers: { "cache-control": "max-age=120" } }));
    vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(old.at + 17_000); // the reload comes 17 s later
    resetWarmCell(new WarmCell(), { keepShared: true }); // the reload's socket 1 reaches another isolate
    const before = calls.fetches.length;
    accepted(await handleWebSocketUpgrade(up(), wrapped, ctx)); await settle();
    // Socket 2 follows socket 1 by ~0.9 s (x18k s1 and s2); a 17 s old hint is a reload, the case most likely stale.
    // Today: a direct attempt (refused), then acquire: two gateway hops for socket 1.
    expect(calls.fetches.slice(before).map((f) => f.direct !== null)).toEqual([false]);
    await closeAll();
  });

  it("clients without a usable address (the shared 0.0.0.0 bucket) do not share one colo-wide holder", async () => {
    const { calls, wrapped } = counting(); const { ctx, settle } = context();
    const up = () => new Request("https://r538c.test/ws", { headers: { upgrade: "websocket" } }); // no CF-Connecting-IP
    accepted(await handleWebSocketUpgrade(up(), wrapped, ctx)); await settle();
    resetWarmCell(new WarmCell(), { keepShared: true }); // an unrelated client on another isolate
    accepted(await handleWebSocketUpgrade(up(), wrapped, ctx)); await settle();
    expect(calls.fetches[1]!.direct).toBeNull();
    await closeAll();
  });
});
