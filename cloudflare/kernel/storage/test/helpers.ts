// Test helpers: run code inside a real SQLite-backed Durable Object in
// workerd and give it the storage layer. The P01 HelloObject is the host
// object until the tenant object exists; only its storage is used.
import { env, runInDurableObject } from "cloudflare:test";
import { TenantStorage, type InvocationKind, type InvocationResult, type StorageSession } from "../index";

const tenants = new WeakMap<DurableObjectStorage, TenantStorage>();

export function tenantOf(storage: DurableObjectStorage): TenantStorage {
  let tenant = tenants.get(storage);
  if (!tenant) {
    tenant = new TenantStorage(storage);
    tenants.set(storage, tenant);
  }
  return tenant;
}

let counter = 0;
export const uniqueName = (prefix = "k01") => `${prefix}-${Date.now()}-${++counter}-${Math.random().toString(36).slice(2)}`;

/** A fresh Durable Object; `run` executes inside it with its storage layer. */
export function objectFor(name = uniqueName()) {
  const stub = env.HELLO.getByName(name);
  return {
    name,
    stub,
    run<T>(callback: (tenant: TenantStorage, state: DurableObjectState) => Promise<T> | T): Promise<T> {
      return runInDurableObject(stub, async (_instance, state) => callback(tenantOf(state.storage), state));
    },
    /** Raw SQL outside any invocation (fixtures, dumps). */
    sql<T extends Record<string, SqlStorageValue> = Record<string, SqlStorageValue>>(query: string, ...binds: SqlStorageValue[]): Promise<T[]> {
      return runInDurableObject(stub, (_instance, state) => state.storage.sql.exec<T>(query, ...binds).toArray());
    },
    invoke<T>(kind: InvocationKind, body: (session: StorageSession) => Promise<T>): Promise<InvocationResult<T>> {
      return runInDurableObject(stub, (_instance, state) => tenantOf(state.storage).invoke(kind, body));
    },
  };
}

/** The error message of a failed invocation, or "ok". */
export function outcome(result: InvocationResult<unknown>): string {
  return result.ok ? "ok" : String((result.error as Error).message);
}

/** Captures a rejection's message. */
export async function rejection(promise: Promise<unknown>): Promise<string> {
  try {
    await promise;
  } catch (error) {
    return String((error as Error).message);
  }
  throw new Error("expected a rejection");
}
