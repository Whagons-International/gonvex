import { env, runInDurableObject } from "cloudflare:test";
import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { HelloObject } from "../src/index";

const BASE = "https://gonvex.test";

function worker(path: string, init?: RequestInit): Promise<Response> {
  return exports.default.fetch(new Request(`${BASE}${path}`, init));
}

interface GreetBody {
  tenant: string;
  id: number;
  name: string;
  rowsRead: number;
  rowsWritten: number;
}

describe("worker", () => {
  it("answers the root health route", async () => {
    const response = await worker("/");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true, service: "gonvex-cloudflare" });
  });

  it("rejects unknown routes and malformed tenant names", async () => {
    expect((await worker("/nope")).status).toBe(404);
    expect((await worker("/t/Bad.Name/greet", { method: "POST" })).status).toBe(404);
  });
});

describe("HelloObject SQLite", () => {
  it("writes exactly one row per greet and reads it back from SQLite", async () => {
    const first = await worker("/t/acme/greet?name=ada", { method: "POST" });
    expect(first.status).toBe(200);
    const body = (await first.json()) as GreetBody;
    expect(body).toMatchObject({ tenant: "acme", id: 1, name: "ada" });
    expect(body.rowsWritten).toBe(1);
    expect(body.rowsRead).toBe(1);

    const second = (await (await worker("/t/acme/greet?name=grace", { method: "POST" })).json()) as GreetBody;
    expect(second.id).toBe(2);

    const stub = env.HELLO.getByName("acme");
    const rows = await runInDurableObject(stub, (_instance: HelloObject, state) =>
      state.storage.sql.exec<{ id: number; name: string }>("SELECT id, name FROM greetings ORDER BY id").toArray(),
    );
    expect(rows).toEqual([
      { id: 1, name: "ada" },
      { id: 2, name: "grace" },
    ]);
  });

  it("keeps each named object's database separate", async () => {
    await worker("/t/one/greet?name=a", { method: "POST" });
    const other = (await (await worker("/t/two/greet?name=b", { method: "POST" })).json()) as GreetBody;
    expect(other.id).toBe(1);
  });

  it("is SQLite-backed", async () => {
    const stub = env.HELLO.getByName("sqlite-check");
    const size = await runInDurableObject(stub, (_instance: HelloObject, state) => state.storage.sql.databaseSize);
    expect(size).toBeGreaterThan(0);
  });
});

describe("HelloObject WebSocket", () => {
  it("echoes text and binary frames", async () => {
    const response = await worker("/t/acme/ws", { headers: { Upgrade: "websocket" } });
    expect(response.status).toBe(101);
    const socket = response.webSocket;
    if (!socket) throw new Error("expected a WebSocket on the 101 response");
    socket.accept();

    const received: unknown[] = [];
    const done = new Promise<void>((resolve) => {
      socket.addEventListener("message", (event) => {
        received.push(event.data);
        if (received.length === 2) resolve();
      });
    });
    socket.send("hello");
    socket.send(new Uint8Array([1, 2, 3]));
    await done;
    socket.close(1000, "done");

    expect(received[0]).toBe("hello");
    // At this compatibility date client-side binary frames arrive as Blob.
    const binary = received[1];
    const bytes = binary instanceof Blob ? await binary.arrayBuffer() : (binary as ArrayBuffer);
    expect(new Uint8Array(bytes)).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("refuses non-upgrade requests on the socket route", async () => {
    expect((await worker("/t/acme/ws")).status).toBe(426);
  });
});
