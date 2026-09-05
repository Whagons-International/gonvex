import { LocalReducerRuntime, type LocalRuntimeOptions } from "./index.js";

/** Generated worker entrypoint calls this exactly once. */
export function serveLocalReducerWorker(options: LocalRuntimeOptions): void {
  const host = new LocalReducerRuntime(options);
  const endpoint = globalThis as unknown as {
    postMessage(message: unknown): void;
    addEventListener(type: "message", callback: (event: MessageEvent) => void): void;
  };
  const respond = async (id: number, run: () => Promise<unknown>) => {
    try { endpoint.postMessage({ id, result: await run() }); }
    catch (error) { endpoint.postMessage({ id, error: { name: error instanceof Error ? error.name : "Error", message: error instanceof Error ? error.message : String(error) } }); }
  };
  void respond(0, async () => {
    await host.initializeReady();
    // WASM is loaded before closing ambient network access. Reducers enqueue
    // external work through ctx.actions; they cannot send it during prediction.
    const denied = () => { throw new Error("External I/O is unavailable in a local Reducer; enqueue an Action"); };
    Object.defineProperty(globalThis, "fetch", { value: denied, writable: false, configurable: false });
    Object.defineProperty(globalThis, "WebSocket", { value: denied, writable: false, configurable: false });
    Object.defineProperty(globalThis, "XMLHttpRequest", { value: denied, writable: false, configurable: false });
  });
  endpoint.addEventListener("message", ({ data }) => {
    if (!data || !Number.isSafeInteger(data.id) || !Array.isArray(data.args)) return;
    void respond(data.id, async () => {
      if (data.method === "execute") return host.execute(data.args[0], data.args[1], data.args[2], data.args[3]);
      if (data.method === "replay") return host.replay(data.args[0], data.args[1]);
      throw new Error("Unknown local reducer operation");
    });
  });
}
