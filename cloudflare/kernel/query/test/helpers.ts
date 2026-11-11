// Test helpers: a SQLite-backed Durable Object per test, a tenant session for
// a Member, and the engine wired as the tenant object will wire it.
import { env, runInDurableObject } from "cloudflare:test";
import { validateValue } from "../../host/src/schema";
import type { TenantSession } from "../../session/ports";
import { TenantStorage } from "../../storage";
import { VisibilityRegistry } from "../../visibility";
import { QueryEngine, kernelVisibility, moduleFromManifest, snapshotRunner, type Manifest, type QueryModule, type QueryVisibility } from "../index";

let objects = 0;

export async function withObject<T>(fn: (state: DurableObjectState) => T | Promise<T>): Promise<T> {
  objects += 1;
  const stub = env.HELLO.getByName(`k08-test-${objects}-${Math.random()}`);
  return runInDurableObject(stub, (_instance, state) => fn(state));
}

export function session(member: { id: string; accountId: string; role?: string; permissions?: unknown }, tenantId = "<tenant:a>", projectId = "<project>"): TenantSession {
  return {
    identity: { projectId, account: { id: member.accountId, email: `${member.accountId}@test.invalid`, emailVerified: true, name: member.accountId, avatarUrl: "", provider: "password" } },
    route: { projectId, tenantId, databaseUrl: "" },
    member: { id: member.id, accountId: member.accountId, status: "active", displayName: member.id, avatarUrl: "", role: member.role ?? "admin", permissions: (member.permissions ?? {}) as never, membershipRevision: 1 },
    admissionRevision: 0,
    delegation: null,
    developerSession: null,
    anonymous: false,
  };
}

/** K04's portable-schema check, as the tenant object passes it to `query.call`. */
export function validateArgs(schema: unknown, args: unknown): string | null {
  try {
    validateValue(schema, args);
    return null;
  } catch (error) {
    return (error as Error).message;
  }
}

/** The clock, the members table and a module, ready for the engine. */
export function provision(state: DurableObjectState, options: { epoch?: string; revision?: number; members?: Array<Record<string, string>>; migration?: string }): void {
  const sql = state.storage.sql;
  sql.exec("CREATE TABLE _gonvex_sync_clock (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), epoch TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0, retained_revision INTEGER NOT NULL DEFAULT 0)");
  sql.exec("INSERT INTO _gonvex_sync_clock (singleton, epoch, revision) VALUES (1, ?, ?)", options.epoch ?? "epoch-1", options.revision ?? 0);
  sql.exec("CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT NOT NULL, status TEXT NOT NULL, role TEXT NOT NULL, permissions TEXT NOT NULL)");
  for (const m of options.members ?? []) sql.exec("INSERT INTO members (id, account_id, status, role, permissions) VALUES (?, ?, ?, ?, ?)", m.id!, m.account_id!, m.status!, m.role!, m.permissions!);
  if (options.migration) sql.exec(options.migration);
}

export function setRevision(state: DurableObjectState, revision: number): void {
  state.storage.sql.exec("UPDATE _gonvex_sync_clock SET revision = ? WHERE singleton = 1", revision);
}

/** The engine as the tenant object wires it: K09's visibility over one registry for this object. */
export function engine(state: DurableObjectState, module: QueryModule, visibility: QueryVisibility = kernelVisibility(new VisibilityRegistry())): { engine: QueryEngine; storage: TenantStorage } {
  const storage = new TenantStorage(state.storage);
  return { storage, engine: new QueryEngine({ visibility, module: () => module, run: snapshotRunner(storage), validateArgs }) };
}

export function module(manifest: Manifest): QueryModule {
  return moduleFromManifest("<project>", manifest, "artifact-1");
}

const bind = (value: unknown): SqlStorageValue => {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number" || typeof value === "string") return value;
  return JSON.stringify(value);
};

/** Applies one captured change to its table by key (insert/update write the new image). */
export function applyChange(sql: SqlStorage, keys: Readonly<Record<string, string>>, change: { table: string; rowId: string; operation: string; newValue: Record<string, unknown> | null }): void {
  const key = keys[change.table];
  if (!key) return;
  if (change.operation === "delete") {
    sql.exec(`DELETE FROM "${change.table}" WHERE "${key}" = ?`, change.rowId);
    return;
  }
  const columns = Object.keys(change.newValue ?? {});
  sql.exec(
    `INSERT OR REPLACE INTO "${change.table}" (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`,
    ...columns.map((c) => bind(change.newValue![c])),
  );
}
