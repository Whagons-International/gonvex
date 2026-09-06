import { LocalReducerRuntime, type LocalRuntimeOptions } from "./index.js";
import { createScratchFilesystem } from "./scratch-filesystem.js";

/** Generated worker entrypoint calls this exactly once. */
export function serveLocalReducerWorker(options: LocalRuntimeOptions): void {
  const host = Promise.all([loadEmptyDatabase(), createScratchFilesystem()]).then(async ([databaseTemplate, filesystem]) => {
    const runtime = new LocalReducerRuntime({ ...options, databaseTemplate, filesystem, ownsSnapshots: true });
    try { await runtime.initializeReady(); return runtime; }
    catch (error) {
      if (!filesystem) throw error;
      await runtime.close().catch(() => undefined);
      return new LocalReducerRuntime({ ...options, databaseTemplate, ownsSnapshots: true });
    }
  });
  const endpoint = globalThis as unknown as {
    postMessage(message: unknown): void;
    addEventListener(type: "message", callback: (event: MessageEvent) => void): void;
  };
  const respond = async (id: number, run: () => Promise<unknown>) => {
    try { endpoint.postMessage({ id, result: await run() }); }
    catch (error) { endpoint.postMessage({ id, error: { name: error instanceof Error ? error.name : "Error", message: error instanceof Error ? error.message : String(error) } }); }
  };
  void respond(0, async () => {
    await (await host).initializeReady();
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
      if (data.method === "execute") return (await host).execute(data.args[0], data.args[1], data.args[2], data.args[3]);
      if (data.method === "replay") return (await host).replay(data.args[0], data.args[1]);
      throw new Error("Unknown local reducer operation");
    });
  });
}

async function loadEmptyDatabase(): Promise<Blob | undefined> {
  try {
    const response = await fetch(new URL("./empty-database.b64", import.meta.url));
    if (!response.ok) return undefined;
    const encoded = (await response.text()).trim();
    const decoded = atob(encoded);
    const bytes = new Uint8Array(decoded.length);
    // Uint8Array.from(string) first materializes a character iterable. A direct
    // fill avoids millions of temporary elements while decoding this asset.
    for (let index = 0; index < decoded.length; index++) bytes[index] = decoded.charCodeAt(index);
    return new Blob([bytes]);
  } catch {
    // Older offline caches may not yet contain the template. Initdb remains
    // available so introducing the startup optimization cannot prevent editing.
    return undefined;
  }
}
