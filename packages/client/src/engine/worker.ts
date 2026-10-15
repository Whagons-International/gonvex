import { serveEngineHost, type StaticEngineBinding } from "./host.js";
import type { BrowserUpgradeOptions } from "../browser-upgrades.js";
import type { EngineEndpoint } from "./rpc.js";

/** Call from a generated worker module with statically imported reducer bindings. */
export function serveEngineWorker(options: { localRuntime?: StaticEngineBinding; migrations?: BrowserUpgradeOptions["migrations"] } = {}) {
  const transport = globalThis.WebSocket;
  // Transport construction stays in the host closure. Reducers have no access
  // to captured factories; generated portable bindings validate their graph.
  function denied(): never { throw new Error("External I/O is unavailable in a local Reducer; enqueue an Action"); }
  Object.assign(denied, { OPEN: transport.OPEN, CONNECTING: transport.CONNECTING, CLOSING: transport.CLOSING, CLOSED: transport.CLOSED });
  for (const key of ["fetch", "WebSocket", "XMLHttpRequest"]) {
    Object.defineProperty(globalThis, key, { value: denied, writable: false, configurable: false });
  }
  serveEngineHost(globalThis as unknown as EngineEndpoint, options.localRuntime, transport, options.migrations);
}
