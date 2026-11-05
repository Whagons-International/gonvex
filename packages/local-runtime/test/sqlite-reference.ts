import { validateValue } from "../src/validation.js";
import { DatabaseSync } from "node:sqlite";
import { reducerRowId } from "@gonvex/module-sdk";
import type { JsonObject, JsonValue, ReducerContext, ReducerDefinition } from "@gonvex/module-sdk";
import type { LocalSchema } from "../src/schema.js";
export type { LocalSchema, LocalTableSchema, LocalColumn } from "../src/schema.js";

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
  /** Worker RPC already owns its structured-cloned messages. Native/direct callers keep the default copy. */
  ownsSnapshots?: boolean;
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

/** Bind a JSON parameter the way the runtime does: booleans as 0/1, arrays and
 * objects as JSON text, everything else unchanged. */
const bindValue = (value: JsonValue | undefined): null | number | string => {
  if (value === undefined || value === null) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "object") return JSON.stringify(value);
  return value;
};
/** Decode one column by its declared type (docs/sqlite-storage.md). */
const decodeValue = (declared: string | null, value: unknown): JsonValue => {
  if (value === null || value === undefined) return null;
  const type = (declared ?? "").toUpperCase();
  if (type.includes("JSON") && typeof value === "string") return JSON.parse(value) as JsonValue;
  if (type.includes("BOOL") && typeof value === "number") return value !== 0;
  return value as JsonValue;
};

type Rows<T> = { rows: T[] };
/** The reference's transaction handle: `$n` numbered parameters, rows decoded
 * by declared type, exactly like the runtime's tenant connection. */
export class SqliteTransaction {
  constructor(private readonly db: DatabaseSync) {}
  query<T = JsonObject>(statement: string, parameters: readonly JsonValue[] = []): Promise<Rows<T>> {
    try {
      const prepared = this.db.prepare(statement);
      prepared.setAllowUnknownNamedParameters(true);
      const named = Object.fromEntries(parameters.map((value, index) => [`$${index + 1}`, bindValue(value)]));
      const columns = prepared.columns();
      if (!columns.length) { prepared.run(named); return Promise.resolve({ rows: [] }); }
      const rows = prepared.all(named).map((row) => Object.fromEntries(columns.map(({ name, type }) => [name, decodeValue(type, (row as Record<string, unknown>)[name])])));
      return Promise.resolve({ rows: rows as T[] });
    } catch (error) { return Promise.reject(error); }
  }
  /** Tables a statement opens for reading and for writing, from its bytecode.
   * This covers joins, subqueries, CTEs and `DELETE` without a WHERE. */
  tables(statement: string): { read: Set<string>; written: Set<string> } {
    const pages = new Map(this.db.prepare("SELECT rootpage, tbl_name FROM sqlite_schema WHERE rootpage > 0").all().map((row) => [Number(row.rootpage), String(row.tbl_name)]));
    const read = new Set<string>(), written = new Set<string>();
    for (const op of this.db.prepare(`EXPLAIN ${statement}`).all() as { opcode: string; p1: number; p2: number; p3: number }[]) {
      const table = op.opcode === "Clear" ? pages.get(op.p1) : (op.opcode === "OpenRead" || op.opcode === "OpenWrite") && op.p3 === 0 ? pages.get(op.p2) : undefined;
      if (!table) continue;
      read.add(table);
      if (op.opcode !== "OpenRead") written.add(table);
    }
    return { read, written };
  }
}

/** One in-memory SQLite workspace, rolled back after every execution. */
export class SqliteWorkspace {
  readonly db = new DatabaseSync(":memory:");
  constructor() {
    // The runtime registers the same function (SQLite's REGEXP operator).
    this.db.function("regexp", { deterministic: true }, (pattern, value) => value == null ? null : new RegExp(String(pattern)).test(String(value)) ? 1 : 0);
  }
  exec(sql: string): void { this.db.exec(sql); }
  async transaction<T>(run: (tx: SqliteTransaction & { rollback(): Promise<void> }) => Promise<T>): Promise<T> {
    this.db.exec("BEGIN");
    let open = true;
    const tx = Object.assign(new SqliteTransaction(this.db), {
      rollback: async () => { if (open) { open = false; this.db.exec("ROLLBACK"); } },
    });
    try { return await run(tx); }
    finally { if (open) { open = false; this.db.exec("ROLLBACK"); } }
  }
  close(): void { this.db.close(); }
}
type Transaction = SqliteTransaction;

/**
 * Executes an existing module reducer against an isolated SQLite workspace,
 * the same engine and SQL dialect the runtime uses for tenant databases.
 * This database is disposable execution memory, never a second durable cache.
 * The caller supplies one immutable Local Replica snapshot. Only a successful
 * execution returns a transaction for the SDK's existing outbox/overlay path.
 */
export class LocalReducerRuntime {
  private readonly schema: LocalSchema;
  private readonly database: SqliteWorkspace;
  private readonly ready: Promise<void>;
  private tail: Promise<unknown> = Promise.resolve();
  private closed = false;

  constructor(private readonly options: LocalRuntimeOptions) {
    this.database = new SqliteWorkspace();
    this.schema = clone(options.schema ?? Object.fromEntries(Object.entries(options.tables ?? {}).map(([table, columns]) => [table, {
      key: "_id", columns: Object.fromEntries(Object.entries(columns).map(([name, type]) => [name, { type, nullable: name !== "_id" }])),
    }])));
    this.ready = this.initialize();
  }

  async initializeReady(): Promise<void> { await this.ready; }

  execute(path: string, args: JsonValue, snapshot: LocalSnapshot, execution: LocalExecution): Promise<LocalTransactionResult> {
    // Capture at admission: the caller may receive a new server transaction or
    // switch tenants while another reducer awaits its database turn.
    const input = this.options.ownsSnapshots
      ? { args: clone(args), snapshot, execution: clone(execution) }
      : clone({ args, snapshot, execution });
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
    this.database.close();
  }

  private async initialize(): Promise<void> {
    for (const [table, definition] of Object.entries(this.schema)) {
      const { columns, key } = definition;
      if (!table || !columns[key]) throw new Error(`Local table ${table} requires its primary key column`);
      const definitions = Object.entries(columns).map(([column, spec]) => {
        // Types/defaults come only from the generated, build-time schema.
        return `${quote(column)} ${spec.type}${column === key ? " PRIMARY KEY" : ""}${spec.default ? ` DEFAULT ${spec.default}` : ""}`;
      });
      this.database.exec(`CREATE TABLE ${quote(table)} (${definitions.join(", ")})`);
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
      const seeded = new Map<string, Promise<void>>();
      const rowSeeds = new Map<string, Map<string, Promise<void>>>();
      const seedRow = async (table: string, id: string) => {
        requireTable(table);
        if (!snapshot.tables[table]) throw new IncompleteReplicaError(table);
        if (seeded.has(table)) return seeded.get(table);
        const loads = rowSeeds.get(table) ?? new Map<string, Promise<void>>();
        rowSeeds.set(table, loads);
        if (!loads.has(id)) {
          const row = snapshot.tables[table]?.rows.find(row => String(row[this.schema[table]!.key]) === id);
          loads.set(id, row ? this.seedRows(tx, table, [row]) : Promise.resolve());
        }
        await loads.get(id);
      };
      const seed = async (table: string) => {
        requireTable(table);
        if (!seeded.has(table)) {
          const loads = rowSeeds.get(table);
          const load = (async () => {
            await Promise.all(loads?.values() ?? []);
            // Previously loaded rows may already have been updated or deleted
            // by this reducer. A later scan must not restore their input values.
            const remaining = (snapshot.tables[table]?.rows ?? []).filter(row => !loads?.has(String(row[this.schema[table]!.key])));
            await this.seedRows(tx, table, remaining);
          })();
          seeded.set(table, load);
        }
        await seeded.get(table);
      };
      const readRow = async (table: string, id: string) => {
        requireTable(table);
        reads.add(table);
        requireColumns(table);
        await seedRow(table, id);
        const result = await tx.query<JsonObject>(`SELECT * FROM ${quote(table)} WHERE ${quote(this.schema[table]!.key)} = $1`, [id]);
        if (!result.rows.length && !snapshot.tables[table]?.complete) throw new IncompleteReplicaError(table);
        return result.rows[0];
      };
      // Savepoints (applyDataWrites) roll back their own patches too.
      // Seeding inside a savepoint is rolled back with it, so forget it too.
      const savepoints = new Map<string, { patches: number; seeded: Set<string>; rows: Map<string, Set<string>> }>();
      const query = async <T>(statement: string, parameters: readonly JsonValue[] = []): Promise<readonly T[]> => {
        const savepoint = /^\s*(SAVEPOINT|ROLLBACK\s+TO|RELEASE)\s+(\w+)\s*;?\s*$/i.exec(statement);
        if (savepoint) {
          const [, command, name] = savepoint;
          const mark = savepoints.get(name!);
          if (/^SAVEPOINT$/i.test(command!)) savepoints.set(name!, { patches: patches.length, seeded: new Set(seeded.keys()), rows: new Map([...rowSeeds].map(([table, rows]) => [table, new Set(rows.keys())])) });
          else if (/^ROLLBACK/i.test(command!) && mark) {
            patches.length = mark.patches;
            for (const table of [...seeded.keys()]) if (!mark.seeded.has(table)) seeded.delete(table);
            for (const [table, rows] of rowSeeds) for (const id of [...rows.keys()]) if (!mark.rows.get(table)?.has(id)) rows.delete(id);
          } else if (!/^ROLLBACK/i.test(command!)) savepoints.delete(name!);
          await tx.query(statement);
          return [];
        }
        // SQLite compiles the actual statement; its bytecode opens every
        // table it reads or writes, including joins/subqueries. Never infer
        // completeness using a regex that could silently miss a relation.
        const { read: tables, written } = tx.tables(statement);
        for (const table of tables) {
          // Only a proven primary-key lookup can use a partial collection.
          // SQLite still discovers all relations above; joins, counts,
          // subqueries and range reads must have complete input coverage.
          const key = this.schema[table]?.key;
          const exact = /^SELECT (?:\*|"[A-Za-z_][A-Za-z0-9_]*"(?:,\s*"[A-Za-z_][A-Za-z0-9_]*")*) FROM "([A-Za-z_][A-Za-z0-9_]*)" WHERE "([A-Za-z_][A-Za-z0-9_]*)" = \$(\d+)(?: AND "deletedAt" IS NULL)?(?: LIMIT 1)?\s*;?$/i.exec(statement.trim());
          const id = exact ? parameters[Number(exact[3]) - 1] : undefined;
          if (tables.size === 1 && exact?.[1] === table && exact[2] === key && snapshot.tables[table]?.rows.some(row => row[key!] === id)) {
            requireColumns(table); reads.add(table);
          } else requireComplete(table);
          if (tables.size === 1 && exact?.[1] === table && exact[2] === key && (typeof id === "string" || typeof id === "number")) await seedRow(table, String(id));
          else await seed(table);
        }
        const before = new Map<string, Map<string, JsonObject>>();
        for (const table of written) {
          const rows = (await tx.query<JsonObject>(`SELECT * FROM ${quote(table)}`, [])).rows;
          before.set(table, new Map(rows.map(row => [String(row[this.schema[table]!.key]), row])));
        }
        const result = await tx.query<T>(statement, [...parameters]);
        // Reducer SQL is part of the same transaction. Capture its row delta just like typed database writes.
        for (const table of written) {
          const prior = before.get(table)!;
          const key = this.schema[table]!.key;
          for (const row of (await tx.query<JsonObject>(`SELECT * FROM ${quote(table)}`, [])).rows) {
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
          delegation: null,
        },
        db: {
          query,
          insert: async <T>(table: string, row: JsonObject, allocation?: { generatedId?: string }) => {
            const columns = requireTable(table);
            const value = clone(row);
            const key = this.schema[table]!.key;
            if (value[key] == null) value[key] = allocation?.generatedId ?? await nextId(`insert:${table}`);
            if (typeof value[key] !== "string" || !value[key]) throw new Error("Insert requires a non-empty string id");
            if (columns._creationTime && value._creationTime == null) value._creationTime = execution.now;
            await seedRow(table, value[key] as string);
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
              [...keys.map((key) => patch[key]), id],
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
          wakeAt: async (at, path, args = {}) => context.scheduler.runAt(at, path, args),
        },
      };
      const result = clone(await definition.handler!(context, args));
      validateValue(definition.options.result, result);
      await tx.rollback();
      return { result, patches, deferred, readTables: [...reads].sort() };
    });
  }

  private async seedRows(tx: Transaction, table: string, rows: readonly JsonObject[]): Promise<void> {
    const columns = this.schema[table]!.columns;
    // Bound both parameter count and transient SQL size. Missing columns keep
    // SQL DEFAULT semantics; JSON values remain bound parameters.
    for (let offset = 0; offset < rows.length;) {
      const first = rows[offset]!;
      const keys = Object.keys(first).filter(key => !!columns[key]);
      if (!keys.length) throw new Error("Cannot insert an empty local row");
      const maxRows = Math.max(1, Math.min(128, Math.floor(16000 / keys.length)));
      const params: JsonValue[] = [];
      const values: string[] = [];
      while (offset < rows.length && values.length < maxRows) {
        const row = rows[offset]!;
        const rowKeys = Object.keys(row).filter(key => !!columns[key]);
        if (rowKeys.length !== keys.length || rowKeys.some(key => !keys.includes(key))) break;
        values.push(`(${keys.map(key => { params.push(row[key]!); return `$${params.length}`; }).join(", ")})`);
        offset++;
      }
      // Seeding is rolled back after prediction and needs no RETURNING rows.
      await tx.query(`INSERT INTO ${quote(table)} (${keys.map(quote).join(", ")}) VALUES ${values.join(", ")}`, params);
    }
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
      keys.map((key) => row[key]),
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
