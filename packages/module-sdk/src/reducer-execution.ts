import type { JsonObject, ReducerContext } from "./index.js";

/** Shared by the browser host and the server module: no optimistic handler. */
export async function reducerRowId(context: ReducerContext, table: string, ordinal: number): Promise<string> {
  const seed = JSON.stringify([
    "gonvex.reducer.ids.v1", context.tenant?.id ?? "", context.auth.account?.id ?? "",
    context.invocation.commandId, table, ordinal,
  ]);
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(seed)));
  bytes[6] = (bytes[6]! & 15) | 128;
  bytes[8] = (bytes[8]! & 63) | 128;
  const hex = Array.from(bytes.subarray(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Each insert without an application id receives a stable, intent-owned id.
 * Explicit ids remain supported. Per-table counters prevent unrelated audit
 * inserts from changing the ids of the user's business rows during replay.
 */
export function reducerExecutionContext(context: ReducerContext): ReducerContext {
  if (!context.invocation?.commandId) return context;
  const counters = new Map<string, number>();
  return {
    ...context,
    db: {
      ...context.db,
      insert: async <T>(table: string, row: JsonObject): Promise<T> => {
        const ordinal = counters.get(table) ?? 0;
        counters.set(table, ordinal + 1);
        return context.db.insert<T>(table, row, { generatedId: await reducerRowId(context, table, ordinal) });
      },
    },
  };
}

/** Generate stable IDs for nested JSON entities inside one intent. Not a secret token. */
export async function reducerIdGenerator(context: ReducerContext, namespace: string): Promise<() => string> {
  if (!context.invocation?.commandId) throw new Error("Intent IDs require a reducer commandId");
  const prefix = (await reducerRowId(context, `json:${namespace}`, 0)).slice(0, 28);
  let ordinal = 0;
  return () => {
    if (ordinal > 0xffffffff) throw new Error("Reducer ID allocation exhausted");
    return prefix + (ordinal++).toString(16).padStart(8, "0");
  };
}

/** A secret token, stable for the same intent and purpose. Separate from public row IDs. */
export async function reducerToken(context: ReducerContext, purpose: string): Promise<string> {
  if (!context.intentEntropy) return crypto.randomUUID();
  if (!/^[0-9a-f]{64}$/.test(context.intentEntropy)) throw new Error("Invalid reducer entropy");
  const seed = JSON.stringify(["gonvex.reducer.tokens.v1", context.intentEntropy, context.tenant?.id ?? "", purpose]);
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(seed)));
  return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}
