import { assert, encode } from "./codec.js";
import type {
  ReplicaSchema,
  IncomingRow,
  RowChange,
  MetadataChange,
  Row,
  Json,
} from "./replica.js";
import type { Page, PagePlan, PageBudget } from "./query.js";

/** One bounded FIFO RPC lane. A lost response never means a write rolled back.
 * This low-level prototype refuses a second OPFS owner; production routing and
 * owner fencing must reuse the SDK broker before becoming a client default. */
export async function openOPFSReplicaWorker(input: {
  scope: string;
  schema: ReplicaSchema;
  worker?: Worker;
  deadlineMs?: number;
}) {
  const worker =
    input.worker ??
    new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
  const pending = new Map<
    number,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
      bytes: number;
    }
  >();
  let next = 0,
    queuedBytes = 0,
    closed = false;
  const fail = (message: string) => {
    closed = true;
    for (const request of pending.values()) {
      clearTimeout(request.timer);
      request.reject(new Error(message));
    }
    pending.clear();
    queuedBytes = 0;
    worker.terminate();
  };
  worker.onmessage = ({ data }) => {
    const request = pending.get(data.id);
    if (!request) return;
    clearTimeout(request.timer);
    pending.delete(data.id);
    queuedBytes -= request.bytes;
    if (data.error) request.reject(new Error(data.error));
    else request.resolve(data.result);
  };
  worker.onerror = () =>
    fail(
      "Replica worker failed; durable write outcome may be unknown. Reopen and inspect its receipt.",
    );
  const call = <T>(op: string, args: unknown = {}): Promise<T> => {
    assert(!closed, "Replica worker closed");
    const bytes = encode(JSON.stringify(args)).length;
    assert(
      bytes <= 262144 && pending.size < 16 && queuedBytes + bytes <= 2097152,
      "Replica RPC queue budget",
    );
    const id = ++next;
    queuedBytes += bytes;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(
        () =>
          fail(
            "Replica RPC deadline; durable write outcome may be unknown. Reopen and inspect its receipt.",
          ),
        input.deadlineMs ?? 30000,
      );
      pending.set(id, {
        resolve: (value) => resolve(value as T),
        reject,
        timer,
        bytes,
      });
      try {
        worker.postMessage({ id, op, args });
      } catch (error) {
        pending.delete(id);
        queuedBytes -= bytes;
        clearTimeout(timer);
        reject(error);
      }
    });
  };
  try {
    await call("open", { scope: input.scope, schema: input.schema });
  } catch (error) {
    fail("Replica open failed");
    throw error;
  }
  return {
    entity: (table: string, id: string) =>
      call<Row | undefined>("entity", { table, id }),
    metadata: (key: string) => call<Json | undefined>("metadata", { key }),
    page: (plan: PagePlan, budget?: PageBudget) =>
      call<Page>("page", { plan, budget }),
    apply: (
      changes: readonly RowChange[],
      metadata: readonly MetadataChange[] = [],
    ) => call<number>("apply", { changes, metadata }),
    acceptIntent: (id: string, payload: Json, changes: readonly RowChange[]) =>
      call<{ sequence: number; duplicate: boolean }>("acceptIntent", {
        id,
        payload,
        changes,
      }),
    beginSnapshot: (sortBytes?: number) =>
      call<void>("beginSnapshot", { sortBytes }),
    appendSnapshot: (rows: readonly IncomingRow[]) =>
      call<void>("appendSnapshot", { rows }),
    commitSnapshot: () => call<number>("commitSnapshot"),
    abortSnapshot: () => call<void>("abortSnapshot"),
    verify: () =>
      call<{ entries: number; sequence: number; bytes: number }>("verify"),
    close: async () => {
      if (closed) return;
      try {
        await call("close");
      } finally {
        fail("Replica closed");
      }
    },
  };
}
