import type { JsonValue } from "@gonvex/module-sdk";
import type { LocalExecution, LocalIntent, LocalReplay, LocalSnapshot, LocalTransactionResult } from "./index.js";

export type LocalExecutor = {
  ready: Promise<void>;
  execute(path: string, args: JsonValue, snapshot: LocalSnapshot, execution: LocalExecution): Promise<LocalTransactionResult>;
  replay(snapshot: LocalSnapshot, intents: readonly LocalIntent[]): Promise<LocalReplay>;
  close(): void;
};
export type LocalRuntimeBinding = { artifactHash: string; tables: readonly string[]; create(): LocalExecutor };

/** Worker lifetime and RPC are SDK-owned; applications only import generated bindings. */
export function createLocalReducerWorker(worker: Worker): LocalExecutor {
  let sequence = 0;
  let closed = false;
  let failure: Error | undefined;
  const pending = new Map<number, { resolve(value: any): void; reject(error: Error): void }>();
  const ready = new Promise<void>((resolve, reject) => { pending.set(0, { resolve, reject }); });
  // An eager worker can fail before the first reducer is called.
  void ready.catch(() => undefined);
  const fail = (error: Error) => {
    failure = error;
    for (const callback of pending.values()) callback.reject(error);
    pending.clear();
  };
  worker.addEventListener("error", (event) => fail(new Error(event.message || "Local reducer worker failed")));
  worker.addEventListener("messageerror", () => fail(new Error("Local reducer worker response could not be decoded")));
  worker.addEventListener("message", (event: MessageEvent) => {
    const message = event.data;
    const callback = pending.get(message?.id);
    if (!callback) return;
    pending.delete(message.id);
    if (message.error) callback.reject(Object.assign(new Error(message.error.message), { name: message.error.name }));
    else callback.resolve(message.result);
  });
  const call = async (method: string, args: unknown[]) => {
    await ready;
    if (failure) throw failure;
    if (closed) throw new Error("Local reducer worker is closed");
    const id = ++sequence;
    return new Promise<any>((resolve, reject) => {
      pending.set(id, { resolve, reject });
      try { worker.postMessage({ id, method, args }); }
      catch (error) { pending.delete(id); reject(error); }
    });
  };
  return {
    ready,
    execute: (...args) => call("execute", args),
    replay: (...args) => call("replay", args),
    close: () => { closed = true; worker.terminate(); fail(new Error("Local reducer worker closed")); },
  };
}
