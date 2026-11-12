/**
 * Anonymous sessions on the kernel's real call paths: the K16 harness tenant
 * (K07's tenant object, K08's `callQuery`, K12's `ActionRuntime`) behind K05
 * sockets and the address limiter. Expected values are what the Rust backend
 * records for the conformance scenario `public-calls`.
 */
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { seed } from "../../../reducer/test/client.ts";
import { fixtureManifest } from "./fixture.ts";

type Frame = Record<string, any>;

let addresses = 0;

async function anonymous(project: string, tenant: string): Promise<{ send(f: unknown): void; next(id: string): Promise<Frame>; ws: WebSocket; closed: () => boolean }> {
  const response = await SELF.fetch("http://k07.test/ws?public=1", { headers: { upgrade: "websocket", "cf-connecting-ip": `192.0.2.${++addresses}` } });
  const ws = response.webSocket!;
  ws.accept();
  const frames: Frame[] = [];
  let closed = false;
  ws.addEventListener("message", (e) => frames.push(JSON.parse(String(e.data))));
  ws.addEventListener("close", () => { closed = true; });
  const next = async (id: string) => {
    for (let i = 0; i < 400; i++) {
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

const hostCall = (text: string) => `module database capability failed: ${text}`;

describe("anonymous sessions", () => {
  it("call public Queries and public Actions", async () => {
    const { projectId, tenant } = await seed("k16-pass", fixtureManifest());
    const s = await anonymous(projectId, tenant);
    s.send({ type: "query.call", id: "count", path: "actions.boardCount", args: {} });
    expect(await s.next("count")).toEqual({ type: "query.result", id: "count", path: "actions.boardCount", reason: "initial", result: { n: 0 } });
    s.send({ type: "query.call", id: "surface", path: "host.querySurface", args: {} });
    const surface = await s.next("surface");
    // Exactly what the Rust backend answers an anonymous socket (conformance `public-calls`).
    expect(surface).toEqual({
      type: "query.result", id: "surface", path: "host.querySurface", reason: "initial",
      result: {
        keys: ["auth", "db", "function", "invocation", "kind", "member", "now", "permissions", "tenant"], db: ["query"],
        typeofFetch: "undefined", typeofPerformance: "undefined", clockPinned: true,
        random: "Error: Math.random requires intentEntropy in Queries and Reducers",
        entropy: "InvalidAccessError: secure entropy is available only in Actions; use intentEntropy in Reducers",
        write: 'GonvexHostError: host call failed: db_read host call failed [host_call_failed]: a Query may only read; "delete" is not a read statement',
        permissions: "GonvexHostError: host call failed: db_read host call failed [host_call_failed]: ctx.permissions is available to Reducers with a tenant session",
        afterWrite: "no error",
      },
    });
    s.send({ type: "action.call", id: "echo", path: "actions.publicEcho", args: { text: "hi" } });
    expect(await s.next("echo")).toEqual({ type: "action.result", id: "echo", path: "actions.publicEcho", result: { text: "hi", account: null } });
    expect(s.closed()).toBe(false);
    s.ws.close();
  });

  it("are refused non-public functions of every kind with Rust's text", async () => {
    const { projectId, tenant } = await seed("k16-anon", fixtureManifest());
    const s = await anonymous(projectId, tenant);
    s.send({ type: "reducer.call", id: "np", path: "board.add", args: { id: "x", title: "x" } });
    expect(await s.next("np")).toEqual({ type: "reducer.error", id: "np", path: "board.add", error: hostCall("authenticate with an active tenant before calling a non-public reducer"), class: "rejected", retryable: false });
    s.send({ type: "reducer.callMany", calls: [{ id: "b1", path: "board.add", args: {} }, { id: "b2", path: "no.such", args: {} }] });
    expect(await s.next("b1")).toEqual({ type: "reducer.error", id: "b1", path: "board.add", error: hostCall("authenticate with an active tenant before calling a non-public reducer"), class: "rejected", retryable: false });
    expect(await s.next("b2")).toEqual({ type: "reducer.error", id: "b2", path: "no.such", error: 'function "no.such" is not registered', class: "rejected", retryable: false });
    s.send({ type: "action.call", id: "na", path: "actions.fails", args: {} });
    expect(await s.next("na")).toEqual({ type: "action.error", id: "na", path: "actions.fails", error: hostCall("authenticate with an active tenant before calling a non-public action") });
    expect(s.closed()).toBe(false);
    s.ws.close();
  });
});
