import { validateValue } from "./validation.js";
import { PGlite, type Transaction } from "@electric-sql/pglite";
import { reducerRowId } from "@gonvex/module-sdk";
import type { JsonObject, JsonValue, ReducerContext, ReducerDefinition } from "@gonvex/module-sdk";
import type { LocalSchema } from "./schema.js";
export type { LocalSchema, LocalTableSchema, LocalColumn } from "./schema.js";

/** Schema emitted by code generation, never inferred from possibly empty rows. */
export type LocalColumnType = "text" | "boolean" | "bigint" | "integer" | "double precision" | "jsonb" | "uuid";
export type LocalTables = Readonly<Record<string, Readonly<Record<string, LocalColumnType>>>>;
export type LocalSnapshot = {
  scope: string;
  tables: Record<string, { complete: boolean; rows: JsonObject[]; columns?: readonly string[] }>;
};
export type LocalExecution = {
  scope: string;
  commandId: string;
  intentEntropy?: string;
  now: number;
  artifactHash: string;
  identity: Pick<ReducerContext, "auth" | "tenant" | "member">;
};
export type LocalPatch =
  | { entity: string; rowId: string; op: "patch" | "insert"; fields: JsonObject }
  | { entity: string; rowId: string; op: "delete" };
export type DeferredWork =
  | { id: string; kind: "action"; path: string; args: JsonValue }
  | { id: string; kind: "schedule"; path: string; args: JsonValue; at: number };
export type LocalTransactionResult = {
  result: JsonValue;
  patches: LocalPatch[];
  deferred: DeferredWork[];
  readTables: string[];
};
export type LocalRuntimeOptions = {
  /** Prebundled engines let native hosts start without any network access. */
  engine?: { pgliteWasmModule: WebAssembly.Module; initdbWasmModule: WebAssembly.Module; fsBundle: Blob };
  tables?: LocalTables;
  schema?: LocalSchema;
  reducers: Readonly<Record<string, ReducerDefinition<any, any>>>;
  artifactHash: string;
};
/** Persist these envelopes in the SDK outbox, never application state. */
export type LocalIntent = { path: string; args: JsonValue; execution: LocalExecution };
export type LocalReplay = {
  transactions: { commandId: string; transaction: LocalTransactionResult }[];
  rejected: { commandId: string; error: Error }[];
};

export class IncompleteReplicaError extends Error {
  constructor(readonly table: string) {
    super(`Local replica for ${table} is incomplete; this reducer cannot decide from missing rows.`);
    this.name = "IncompleteReplicaError";
  }
}

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;
const columnTypes = new Set<LocalColumnType>(["text", "boolean", "bigint", "integer", "double precision", "jsonb", "uuid"]);
const clone = <T>(value: T): T => structuredClone(value);
const parseInteger = (value: string) => {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error("Local reducer read an integer outside the JSON safe range");
  return parsed;
};
const queryOptions = { parsers: { 20: parseInteger } };

/**
 * Executes an existing module reducer against an isolated PostgreSQL workspace.
 * This database is disposable execution memory, never a second durable cache.
 * The caller supplies one immutable Local Replica snapshot. Only a successful
 * execution returns a transaction for the SDK's existing outbox/overlay path.
 */
export class LocalReducerRuntime {
  private readonly schema: LocalSchema;
  private readonly database: PGlite;
  private readonly ready: Promise<void>;
  private tail: Promise<unknown> = Promise.resolve();
  private closed = false;

  constructor(private readonly options: LocalRuntimeOptions) {
    this.database = new PGlite(options.engine);
    this.schema = clone(options.schema ?? Object.fromEntries(Object.entries(options.tables ?? {}).map(([table, columns]) => [table, {
      key: "_id", columns: Object.fromEntries(Object.entries(columns).map(([name, type]) => [name, { type, nullable: name !== "_id" }])),
    }])));
    this.ready = this.initialize();
  }

  async initializeReady(): Promise<void> { await this.ready; }

  execute(path: string, args: JsonValue, snapshot: LocalSnapshot, execution: LocalExecution): Promise<LocalTransactionResult> {
    // Capture at admission: the caller may receive a new server transaction or
    // switch tenants while another reducer awaits its database turn.
    const input = clone({ args, snapshot, execution });
    const job = this.tail.then(async () => {
      if (this.closed) throw new Error("Local reducer runtime is closed");
      await this.ready;
      return this.run(path, input.args, input.snapshot, input.execution);
    });
    this.tail = job.catch(() => undefined);
    return job;
  }

  /**
   * Rebuild pending transactions in intent order after hydration or a changed
   * authoritative base. Failed intent contributes no rows to later execution.
   * The SDK publishes the returned overlays together, after replay completes.
   */
  async replay(snapshot: LocalSnapshot, intents: readonly LocalIntent[]): Promise<LocalReplay> {
    const working = clone(snapshot);
    const queue = clone(intents);
    const ids = new Set<string>();
    // These are journal integrity errors, not business rejections. Do not
    // silently drop an intent belonging to another artifact or identity.
    for (const intent of queue) {
      if (intent.execution.scope !== working.scope) throw new Error("Local reducer scope mismatch");
      if (intent.execution.artifactHash !== this.options.artifactHash) throw new Error("Local reducer artifact mismatch");
      if (ids.has(intent.execution.commandId)) throw new Error("Duplicate local command id");
      ids.add(intent.execution.commandId);
    }
    const transactions: LocalReplay["transactions"] = [];
    const rejected: LocalReplay["rejected"] = [];
    for (const intent of queue) {
      let transaction: LocalTransactionResult;
      try {
        transaction = await this.execute(intent.path, intent.args, working, intent.execution);
      } catch (error) {
        // A missing cache dependency must keep the journal pending. It is not
        // evidence that the server will reject the business operation.
        if (error instanceof IncompleteReplicaError) throw error;
        rejected.push({ commandId: intent.execution.commandId, error: error instanceof Error ? error : new Error(String(error)) });
        continue;
      }
      // Invariant failures in the transaction output abort the whole replay;
      // they must never be downgraded to a user/business rejection.
      for (const patch of transaction.patches) {
        const table = working.tables[patch.entity] ??= { complete: false, rows: [] };
        const key = this.schema[patch.entity]?.key ?? "_id";
        const index = table.rows.findIndex((row) => (row[key] ?? row._id) === patch.rowId);
        if (patch.op === "delete") {
          if (index >= 0) table.rows.splice(index, 1);
        } else if (patch.op === "insert") {
          if (index >= 0) throw new Error("Local replay insert refers to an existing row");
          table.rows.push(clone(patch.fields));
        } else {
          if (index < 0) throw new Error("Local replay patch refers to a missing row");
          table.rows[index] = { ...table.rows[index], ...clone(patch.fields) };
        }
      }
      transactions.push({ commandId: intent.execution.commandId, transaction });
    }
    return { transactions, rejected };
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.tail;
    await this.ready.catch(() => undefined);
    await this.database.close();
  }

  private async initialize(): Promise<void> {
    for (const [table, definition] of Object.entries(this.schema)) {
      const { columns, key } = definition;
      if (!table || !columns[key]) throw new Error(`Local table ${table} requires its primary key column`);
      const definitions = Object.entries(columns).map(([column, spec]) => {
        // Types/defaults come only from the generated, build-time schema.
        return `${quote(column)} ${spec.type}${column === key ? " PRIMARY KEY" : ""}${spec.default ? ` DEFAULT ${spec.default}` : ""}`;
      });
      await this.database.exec(`CREATE TABLE ${quote(table)} (${definitions.join(", ")})`);
    }
  }

  private async run(path: string, args: JsonValue, snapshot: LocalSnapshot, execution: LocalExecution): Promise<LocalTransactionResult> {
    if (!execution.scope || execution.scope !== snapshot.scope) throw new Error("Local reducer scope mismatch");
    if (!execution.commandId || !Number.isSafeInteger(execution.now)) throw new Error("Invalid local execution envelope");
    if (execution.artifactHash !== this.options.artifactHash) throw new Error("Local reducer artifact mismatch");
    const definition = this.options.reducers[path];
    if (!definition || definition.kind !== "reducer" || definition.internal || !definition.handler) {
      throw new Error(`No public local reducer registered for ${path}`);
    }
    validateValue(definition.options.args, args);
    const patches: LocalPatch[] = [];
    const deferred: DeferredWork[] = [];
    const reads = new Set<string>();
    let ordinal = 0;
    const deferredOrdinals = new Map<string, number>();
    const nextId = (kind: string) => deterministicId(execution.scope, execution.commandId, kind, ordinal++);
    const requireTable = (table: string) => {
      const definition = this.schema[table];
      if (!definition) throw new Error(`Unknown local table ${table}`);
      return definition.columns;
    };
    const requireColumns = (table: string) => {
      const columns = snapshot.tables[table]?.columns;
      if (columns && Object.keys(requireTable(table)).some(column => !columns.includes(column))) throw new IncompleteReplicaError(table);
    };
    const requireComplete = (table: string) => {
      requireTable(table);
      reads.add(table);
      requireColumns(table);
      if (!snapshot.tables[table]?.complete) throw new IncompleteReplicaError(table);
    };

    return this.database.transaction(async (tx) => {
      // Both the input rows and reducer writes are rolled back on success. The
      // only published output is the returned transaction, applied by the SDK.
      const seeded = new Set<string>();
      const seed = async (table: string) => {
        requireTable(table);
        if (seeded.has(table)) return;
        seeded.add(table);
        for (const row of snapshot.tables[table]?.rows ?? []) await this.insertRow(tx, table, row, true);
      };
      const readRow = async (table: string, id: string) => {
        requireTable(table);
        reads.add(table);
        requireColumns(table);
        await seed(table);
        const result = await tx.query<JsonObject>(`SELECT * FROM ${quote(table)} WHERE ${quote(this.schema[table]!.key)} = $1`, [id], queryOptions);
        if (!result.rows.length && !snapshot.tables[table]?.complete) throw new IncompleteReplicaError(table);
        return result.rows[0];
      };
      const query = async <T>(statement: string, parameters: readonly JsonValue[] = []): Promise<readonly T[]> => {
        // PostgreSQL parses the actual statement and reports every scanned
        // relation, including joins/subqueries. Never infer completeness using
        // a regex that could silently miss a nested relation.
        const explain = await tx.query<Record<string, any>>(`EXPLAIN (FORMAT JSON) ${statement}`, [...parameters], queryOptions);
        const tables = new Set<string>();
        const written = new Set<string>();
        const inspect = (value: unknown): void => {
          if (Array.isArray(value)) { value.forEach(inspect); return; }
          if (!value || typeof value !== "object") return;
          const node = value as Record<string, unknown>;
          if (node["Node Type"] === "ModifyTable" && typeof node["Relation Name"] === "string") written.add(node["Relation Name"]);
          if (typeof node["Relation Name"] === "string") { tables.add(node["Relation Name"]); }
          Object.values(node).forEach(inspect);
        };
        inspect(explain.rows);
        for (const table of tables) {
          // Only a proven primary-key lookup can use a partial collection.
          // PostgreSQL still discovers all relations above; joins, counts,
          // subqueries and range reads must have complete input coverage.
          const key = this.schema[table]?.key;
          const exact = /^SELECT (?:\*|"[A-Za-z_][A-Za-z0-9_]*"(?:,\s*"[A-Za-z_][A-Za-z0-9_]*")*) FROM "([A-Za-z_][A-Za-z0-9_]*)" WHERE "([A-Za-z_][A-Za-z0-9_]*)" = \$(\d+)(?: AND "deletedAt" IS NULL)?(?: LIMIT 1)?\s*;?$/i.exec(statement.trim());
          const id = exact ? parameters[Number(exact[3]) - 1] : undefined;
          if (tables.size === 1 && exact?.[1] === table && exact[2] === key && snapshot.tables[table]?.rows.some(row => row[key!] === id)) {
            requireColumns(table); reads.add(table);
          } else requireComplete(table);
          await seed(table);
        }
        const before = new Map<string, Map<string, JsonObject>>();
        for (const table of written) {
          const rows = (await tx.query<JsonObject>(`SELECT * FROM ${quote(table)}`, [], queryOptions)).rows;
          before.set(table, new Map(rows.map(row => [String(row[this.schema[table]!.key]), row])));
        }
        const result = await tx.query<T>(statement, [...parameters], queryOptions);
        // Reducer SQL (including data-modifying CTEs) is part of the same
        // transaction. Capture its row delta just like typed database writes.
        for (const table of written) {
          const prior = before.get(table)!;
          const key = this.schema[table]!.key;
          for (const row of (await tx.query<JsonObject>(`SELECT * FROM ${quote(table)}`, [], queryOptions)).rows) {
            const id = String(row[key]);
            const old = prior.get(id); prior.delete(id);
            if (!old) patches.push({ entity: table, rowId: id, op: "insert", fields: clone(row) });
            else {
              const fields = Object.fromEntries(Object.entries(row).filter(([column, value]) => JSON.stringify(value) !== JSON.stringify(old[column])));
              if (Object.keys(fields).length) patches.push({ entity: table, rowId: id, op: "patch", fields });
            }
          }
          for (const id of prior.keys()) patches.push({ entity: table, rowId: id, op: "delete" });
        }
        return result.rows;
      };
      const context: ReducerContext = {
        intentEntropy: execution.intentEntropy,
        ...clone(execution.identity), now: execution.now,
        invocation: {
          channel: "ui", rootChannel: "ui", commandId: execution.commandId,
          rootCommandId: execution.commandId, artifactHash: execution.artifactHash,
          actorAccountId: execution.identity.auth.account?.id ?? null,
          actorMemberId: execution.identity.member?.id ?? null, onBehalfOfMemberId: null,
        },
        db: {
          query,
          insert: async <T>(table: string, row: JsonObject, allocation?: { generatedId: string }) => {
            const columns = requireTable(table);
            await seed(table);
            const value = clone(row);
            const key = this.schema[table]!.key;
            if (value[key] == null) value[key] = allocation?.generatedId ?? await nextId(`insert:${table}`);
            if (typeof value[key] !== "string" || !value[key]) throw new Error("Insert requires a non-empty string id");
            if (columns._creationTime && value._creationTime == null) value._creationTime = execution.now;
            const inserted = await this.insertRow(tx, table, value);
            patches.push({ entity: table, rowId: value[key] as string, op: "insert", fields: clone(inserted) });
            return clone(inserted) as T;
          },
          update: async <T>(table: string, id: string, patch: JsonObject) => {
            const columns = requireTable(table);
            const key = this.schema[table]!.key;
            if (Object.hasOwn(patch, key)) throw new Error("Reducer update cannot change a row id");
            const before = await readRow(table, id);
            if (!before) throw new Error(`Row ${table}/${id} not found`);
            const keys = Object.keys(patch);
            keys.forEach((key) => { if (!columns[key]) throw new Error(`Unknown column ${table}.${key}`); });
            for (const name of keys) {
              if (!columns[name]!.nullable && patch[name] == null) throw new Error(`Required field ${table}.${name} is missing`);
            }
            if (!keys.length) return clone(before) as T;
            const updated = await tx.query<JsonObject>(
              `UPDATE ${quote(table)} SET ${keys.map((key, index) => `${quote(key)} = $${index + 1}`).join(", ")} WHERE ${quote(key)} = $${keys.length + 1} RETURNING *`,
              [...keys.map((key) => patch[key]), id], queryOptions,
            );
            patches.push({ entity: table, rowId: id, op: "patch", fields: clone(patch) });
            return clone(updated.rows[0]) as T;
          },
          delete: async (table, id) => {
            const before = await readRow(table, id);
            if (!before) return;
            await tx.query(`DELETE FROM ${quote(table)} WHERE ${quote(this.schema[table]!.key)} = $1`, [id]);
            patches.push({ entity: table, rowId: id, op: "delete" });
          },
          deleteMany: async (table, ids) => {
            let deleted = 0;
            for (const id of new Set(ids)) {
              if (typeof id !== "string") throw new Error("Delete requires a string id");
              const before = await readRow(table, id);
              if (before) { await context.db.delete(table, id); deleted++; }
            }
            return { deleted };
          },
        },
        actions: { enqueue: async (path, args) => {
          const ordinal = deferredOrdinals.get("action") ?? 0;
          deferredOrdinals.set("action", ordinal + 1);
          const id = await reducerRowId(context, "deferred:action", ordinal);
          deferred.push({ id, kind: "action", path, args: clone(args) });
          return id;
        } },
        scheduler: {
          runAfter: async (delay, path, args = {}) => context.scheduler.runAt(execution.now + delay, path, args),
          runAt: async (at, path, args = {}) => {
            if (!Number.isSafeInteger(at)) throw new Error("Invalid schedule time");
            const ordinal = deferredOrdinals.get("schedule") ?? 0;
            deferredOrdinals.set("schedule", ordinal + 1);
            const id = `job_${await reducerRowId(context, "deferred:schedule", ordinal)}`;
            deferred.push({ id, kind: "schedule", path, args: clone(args), at });
            return id;
          },
        },
      };
      const result = clone(await definition.handler!(context, args));
      validateValue(definition.options.result, result);
      await tx.rollback();
      return { result, patches, deferred, readTables: [...reads].sort() };
    });
  }

  private async insertRow(tx: Transaction, table: string, row: JsonObject, seed = false): Promise<JsonObject> {
    const { columns, key } = this.schema[table]!;
    if (seed) {
      row = Object.fromEntries(Object.entries(row).filter(([name]) => !!columns[name]));
    }
    const keys = Object.keys(row);
    keys.forEach((key) => { if (!columns[key]) throw new Error(`Unknown column ${table}.${key}`); });
    if (!keys.length) throw new Error("Cannot insert an empty local row");
    const result = await tx.query<JsonObject>(
      `INSERT INTO ${quote(table)} (${keys.map(quote).join(", ")}) VALUES (${keys.map((_, index) => `$${index + 1}`).join(", ")}) RETURNING *`,
      keys.map((key) => row[key]), queryOptions,
    );
    const value = result.rows[0]!;
    if (!seed) for (const [name, column] of Object.entries(columns)) {
      if (!column.nullable && value[name] == null) throw new Error(`Required field ${table}.${name} is missing`);
    }
    return JSON.parse(JSON.stringify(value)) as JsonObject;
  }
}

/** Stable per-intent IDs. The server host must use the identical seed contract. */
export async function deterministicId(scope: string, commandId: string, kind: string, ordinal: number): Promise<string> {
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify([scope, commandId, kind, ordinal]))));
  bytes[6] = (bytes[6]! & 0x0f) | 0x80;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes.subarray(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
