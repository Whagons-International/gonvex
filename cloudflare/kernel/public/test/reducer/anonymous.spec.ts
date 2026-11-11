/**
 * Anonymous sessions on the kernel's real Reducer path (K07's tenant object
 * behind K05's gateway): Rust's texts as the conformance scenario
 * `public-calls` records them on the Rust backend. The `cloudflare-reducer`
 * conformance backend cannot provision the fixture since K08 added `tasks`
 * (pre-existing, 0040 §6), so this spec stands in for it.
 */
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { seed } from "../../../reducer/test/client.ts";
import { fixtureManifest } from "../../../reducer/test/fixture.ts";

type Frame = Record<string, any>;

async function anonymous(project: string, tenant: string): Promise<{ send(f: unknown): void; next(id: string): Promise<Frame>; ws: WebSocket; closed: () => boolean }> {
  const response = await SELF.fetch("http://k07.test/ws?public=1", { headers: { upgrade: "websocket", "cf-connecting-ip": "192.0.2.77" } });
  const ws = response.webSocket!;
  ws.accept();
  const frames: Frame[] = [];
  let closed = false;
  ws.addEventListener("message", (e) => frames.push(JSON.parse(String(e.data))));
  ws.addEventListener("close", () => { closed = true; });
  const next = async (id: string) => {
    for (let i = 0; i < 200; i++) {
      const at = frames.findIndex(f => f.id === id || (id === "session.ready" && f.type === id));
      if (at >= 0) return frames.splice(at, 1)[0]!;
      await new Promise(r => setTimeout(r, 25));
    }
    throw new Error(`timed out waiting for ${id}`);
  };
  await next("session.ready");
  ws.send(JSON.stringify({ type: "auth", id: "anon", project, tenant, public: true, clientContract: 1 }));
  expect((await next("anon")).type).toBe("auth.result");
  return { send: (f) => ws.send(JSON.stringify(f)), next, ws, closed: () => closed };
}

const refusal = "module database capability failed: authenticate with an active tenant before calling a non-public reducer";

describe("anonymous sessions on the Reducer path", () => {
  it("are refused non-public Reducers with Rust's text, alone and in a batch, and the socket stays open", async () => {
    const { projectId, tenant } = await seed("k16-anon", fixtureManifest());
    const s = await anonymous(projectId, tenant);
    s.send({ type: "reducer.call", id: "np", path: "board.add", args: { id: "x", title: "x" }, idempotencyKey: "k" });
    expect(await s.next("np")).toEqual({ type: "reducer.error", id: "np", path: "board.add", error: refusal, class: "rejected", retryable: false });
    s.send({ type: "reducer.callMany", calls: [{ id: "b1", path: "board.add", args: {} }, { id: "b2", path: "no.such", args: {} }] });
    expect(await s.next("b1")).toEqual({ type: "reducer.error", id: "b1", path: "board.add", error: refusal, class: "rejected", retryable: false });
    expect(await s.next("b2")).toEqual({ type: "reducer.error", id: "b2", path: "no.such", error: 'function "no.such" is not registered', class: "rejected", retryable: false });
    expect(s.closed()).toBe(false);
    s.ws.close();
  });
});
