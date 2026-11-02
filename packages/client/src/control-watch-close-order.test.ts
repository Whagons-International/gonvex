import { afterEach, expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";
import { appendFileSync } from "node:fs";
import { GonvexClient } from "./index.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup(); vi.unstubAllGlobals(); });
const reference = { kind: "query", scope: "control", delivery: "live", path: "control.private" } as const;
const privateResult = { private: "Alice control data" };
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

it.each(["before-sdk", "after-sdk", "after-sdk-reconnect", "unchanged"] as const)(
  "scope clear survives a native WebSocket close observer running %s", async order => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve, reject) => { server.once("listening", resolve); server.once("error", reject); });
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("loopback port missing");
    const trace: string[] = [], peers: WebSocket[] = [];
    const sdkClosed = deferred(), resumeAuth = deferred();
    let client!: GonvexClient, connection = 0, closeCode: number | undefined;
    const record = (value: unknown) => {
      if (process.env.GONVEX_CONTROL_CLOSE_OBSERVATIONS) appendFileSync(process.env.GONVEX_CONTROL_CLOSE_OBSERVATIONS, JSON.stringify(value) + "\n");
    };
    server.on("connection", peer => {
      const number = connection++;
      peers.push(peer);
      peer.on("message", async data => {
        const frame = JSON.parse(String(data));
        if (frame.type === "auth") {
          if (number > 0) await resumeAuth.promise;
          if (peer.readyState === WebSocket.OPEN) peer.send(JSON.stringify({ type: "auth.result", id: frame.id, result: {} }));
        } else if (frame.type === "query.subscribe") {
          peer.send(JSON.stringify({ type: "query.result", id: frame.id,
            result: number === 0 ? privateResult : { private: "Bob control data" } }));
        }
      });
    });
    // This wrapper observes actual native close events. It does not synthesize
    // a close, edit SDK fields, or invoke an SDK event handler directly.
    class ObservedSocket extends WebSocket {
      constructor(url: string) {
        super(url);
        const externalClose = (event: WebSocket.CloseEvent) => {
          trace.push("external-close"); closeCode = event.code;
          if (order !== "unchanged") client.setAuth({ token: "bob-session", identity: { sub: "bob", iss: "issuer" } });
        };
        if (order.startsWith("after-sdk")) queueMicrotask(() => super.addEventListener("close", externalClose));
        else super.addEventListener("close", externalClose);
      }
      addEventListener(type: any, listener: any, options?: any) {
        if (type !== "close") return super.addEventListener(type, listener, options);
        return super.addEventListener(type, event => {
          trace.push("sdk-close");
          try { listener.call(this, event); } finally { sdkClosed.resolve(); }
        }, options);
      }
    }
    vi.stubGlobal("WebSocket", ObservedSocket);
    client = new GonvexClient(`ws://127.0.0.1:${address.port}`, { role: "control", project: "project", tenant: "tenant",
      token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false });
    const watch = client.watchControlQuery(reference, {});
    const observed: unknown[] = [];
    let rendered: unknown, armed = false, forced = false;
    const stopReconnect = watch.onUpdate(() => {
      if (armed && !forced && order === "after-sdk-reconnect" && watch.getSnapshot().result === undefined) {
        forced = true; trace.push("public-connect"); client.connect();
      }
    });
    const stop = watch.onUpdate(() => { rendered = watch.getSnapshot().result; if (armed && rendered === undefined) trace.push("clear-notification"); observed.push(rendered); });
    cleanups.push(async () => {
      resumeAuth.resolve(); stopReconnect(); stop(); client.close();
      for (const peer of peers) peer.terminate();
      await new Promise<void>(resolve => server.close(() => resolve()));
    });
    await vi.waitFor(() => expect(rendered).toEqual(privateResult));
    await new Promise<void>(resolve => setImmediate(resolve)); observed.length = 0;
    const snapshot = watch.getSnapshot();
    armed = true;
    peers[0]!.close(1000, "fixture remote disconnect");
    await sdkClosed.promise;
    await new Promise<void>(resolve => setImmediate(resolve));
    record({ phase: "after-close", order, trace: [...trace], closeCode, forced,
      rendered: rendered ?? null, snapshot: watch.getSnapshot().result ?? null,
      observed: [...observed].map(value => value ?? null), freshAuthHeld: true });
    if (order.startsWith("after-sdk")) expect(trace.indexOf("sdk-close")).toBeLessThan(trace.indexOf("external-close"));
    else expect(trace.indexOf("external-close")).toBeLessThan(trace.indexOf("sdk-close"));
    if (order === "unchanged") {
      expect(watch.getSnapshot()).toBe(snapshot);
      expect(rendered).toEqual(privateResult);
    } else {
      expect(watch.getSnapshot().result).toBeUndefined();
      await vi.waitFor(() => expect(rendered, "native close suppressed the new owner's scope-clear notification").toBeUndefined());
      expect(observed).toContain(undefined);
      resumeAuth.resolve();
      await vi.waitFor(() => expect(rendered, "fresh owner failed to progress after native reconnect").toEqual({ private: "Bob control data" }));
    }
    record({ phase: "recovered", order, rendered, trace: [...trace] });
  },
);
