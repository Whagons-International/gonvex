// Test Worker: one SQLite-backed tenant object that hosts the fixture module.
import { DurableObject } from "cloudflare:workers";
import * as fixture from "./fixtures/host.module.js";
import {
  CatalogCache, DurableObjectInvocationStorage, ModuleRegistry, TenantModuleHost,
  type EngineConfig, type FunctionContract, type ModuleArtifact, type PermissionIndex,
} from "../src/index.js";

const any = { kind: "any" };

/** Fixture function table: path -> [kind, export]. */
const table: [string, "query" | "reducer" | "action"][] = [
  ["notes.list", "query"], ["notes.clock", "query"], ["notes.writeInQuery", "query"], ["notes.cteDelete", "query"],
  ["notes.big", "query"], ["notes.badResult", "query"], ["notes.queryPermission", "query"],
  ["notes.random", "reducer"], ["notes.add", "reducer"], ["notes.update", "reducer"], ["notes.remove", "reducer"],
  ["notes.removeMany", "reducer"], ["notes.counter", "reducer"], ["notes.composite", "reducer"], ["notes.enqueue", "reducer"],
  ["notes.scheduleUnknown", "reducer"], ["notes.caughtSqlError", "reducer"], ["notes.abortedThenQuery", "reducer"],
  ["notes.savepointRecover", "reducer"], ["notes.throwAfterWrite", "reducer"], ["notes.orphanChild", "reducer"],
  ["notes.ddl", "reducer"], ["notes.insertInto", "reducer"], ["notes.recordAdd", "reducer"], ["notes.slow", "reducer"],
  ["notes.spin", "reducer"], ["notes.permission", "reducer"], ["notes.metadata", "reducer"],
  ["mail.send", "action"], ["net.swallowDenied", "action"],
];

export function fixtureFunctions(): FunctionContract[] {
  return table.map(([path, kind]) => ({
    path, kind, export: path.replace(".", "_"),
    argsSchema: path === "notes.add" ? { kind: "object", fields: { id: { kind: "string" }, body: { kind: "string" }, done: { kind: "optional", value: { kind: "boolean" } }, meta: { kind: "optional", value: any } } } : any,
    resultSchema: path === "notes.badResult" ? { kind: "string" } : any,
    ...(path === "net.swallowDenied" ? { metadata: { actionCapabilities: { networkOrigins: ["https://allowed.invalid"] } } } : {}),
    // A non-interactive Reducer gets ctx.storage (RT/execution.rs:587-588).
    ...(path === "notes.metadata" ? { metadata: { interactive: false } } : {}),
  }));
}

export function fixtureArtifact(generation: number, artifactHash = `fixture-${generation}`): ModuleArtifact {
  return {
    moduleId: "fixture", generation, artifactHash, clientContract: 1,
    functions: fixtureFunctions(),
    exports: { ...fixture, "mail_send": () => null },
    schema: { tables: { notes: { columns: { _id: { primaryKey: true } } } } },
  };
}

export const SCHEMA = [
  `CREATE TABLE notes (_id TEXT PRIMARY KEY, body TEXT NOT NULL, done BOOLEAN NOT NULL DEFAULT 0, meta JSON, n INTEGER)`,
  `CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)`,
  `CREATE TABLE pairs (a TEXT, b TEXT, PRIMARY KEY (a, b))`,
  `CREATE TABLE children (_id TEXT PRIMARY KEY, parent TEXT REFERENCES notes(_id) DEFERRABLE INITIALLY DEFERRED)`,
  `CREATE TABLE _gonvex_record_views (name TEXT PRIMARY KEY, storage_table TEXT NOT NULL UNIQUE, key_column TEXT NOT NULL, schema TEXT NOT NULL CHECK (json_valid(schema))) STRICT`,
  `CREATE TABLE items (_id TEXT PRIMARY KEY, label TEXT, created INTEGER)`,
  `INSERT INTO _gonvex_record_views VALUES ('items', 'items', '_id', '{"key":"_id","columns":{"_id":{"type":"TEXT","default":"''item-'' || lower(hex(randomblob(4)))"},"label":{"type":"TEXT"},"created":{"type":"INTEGER","default":"7"}}}')`,
  `CREATE TABLE _gonvex_action_outbox (id TEXT PRIMARY KEY, action_path TEXT NOT NULL, args JSON NOT NULL, actor_user_id TEXT, actor_email TEXT,
    provenance JSON NOT NULL DEFAULT '{}', status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)), locked_at INTEGER, last_error TEXT,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)))`,
];

export interface SetupOptions {
  engine?: Partial<EngineConfig>;
  cache?: boolean;
  permissions?: Record<string, boolean | "rebuilding">;
  generation?: number;
}

export class TestTenant extends DurableObject {
  registry!: ModuleRegistry;
  host!: TenantModuleHost;
  /** Constructor runs, to prove the object was not reset. */
  static constructed = 0;
  readonly instance: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.instance = ++TestTenant.constructed;
    this.setup();
  }

  setup(options: SetupOptions = {}): void {
    if (!this.ctx.storage.sql.exec("SELECT 1 FROM sqlite_schema WHERE name = 'notes'").toArray().length) {
      for (const statement of SCHEMA) this.ctx.storage.sql.exec(statement);
    }
    this.registry = new ModuleRegistry("fixture", options.engine ?? {});
    const bits = options.permissions;
    const permissions: PermissionIndex | undefined = bits === undefined ? undefined : { has: (_member, permission, scope) => bits[`${permission}@${scope}`] ?? false };
    this.host = new TenantModuleHost({
      storage: new DurableObjectInvocationStorage(this.ctx.storage),
      registry: this.registry,
      cache: new CatalogCache(options.cache ?? true),
      ...(permissions ? { permissions } : {}),
    });
    this.registry.install(fixtureArtifact(options.generation ?? 1));
  }
}

export default {
  fetch(): Response {
    return new Response("k04 host test worker");
  },
};
