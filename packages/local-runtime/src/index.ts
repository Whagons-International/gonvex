import { PGlite, type Transaction } from "@electric-sql/pglite";
import type { JsonObject, JsonValue, ReducerContext, ReducerDefinition } from "@gonvex/module-sdk";

/** Schema emitted by code generation, never inferred from possibly empty rows. */
export type LocalColumnType = "text" | "boolean" | "bigint" | "integer" | "double precision" | "jsonb" | "uuid";
export type LocalTables = Readonly<Record<string, Readonly<Record<string, LocalColumnType>>>>;
export type LocalSnapshot = {
  scope: string;
  tables: Record<string, { complete: boolean; rows: JsonObject[] }>;
};
export type LocalExecution = {
  scope: string;
  commandId: string;
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
  tables: LocalTables;
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
  private readonly database = new PGlite();
  private readonly ready: Promise<void>;
  private tail: Promise<unknown> = Promise.resolve();
  private closed = false;

  constructor(private readonly options: LocalRuntimeOptions) {
    this.ready = this.initialize();
  }

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
        const index = table.rows.findIndex((row) => row._id === patch.rowId);
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
    for (const [table, columns] of Object.entries(this.options.tables)) {
      if (!table || !columns._id) throw new Error(`Local table ${table} requires an _id column`);
      const definitions = Object.entries(columns).map(([column, type]) => {
        if (!columnTypes.has(type)) throw new Error(`Unsupported local column type: ${type}`);
        return `${quote(column)} ${type}${column === "_id" ? " PRIMARY KEY" : ""}`;
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
    const patches: LocalPatch[] = [];
    const deferred: DeferredWork[] = [];
    const reads = new Set<string>();
    let ordinal = 0;
    const nextId = (kind: string) => deterministicId(execution.scope, execution.commandId, kind, ordinal++);
    const requireTable = (table: string) => {
      const columns = this.options.tables[table];
      if (!columns) throw new Error(`Unknown local table ${table}`);
      return columns;
    };
    const requireComplete = (table: string) => {
      requireTable(table);
      reads.add(table);
      if (!snapshot.tables[table]?.complete) throw new IncompleteReplicaError(table);
    };

    return this.database.transaction(async (tx) => {
      // Both the input rows and reducer writes are rolled back on success. The
      // only published output is the returned transaction, applied by the SDK.
      for (const [table, slice] of Object.entries(snapshot.tables)) {
        requireTable(table);
        for (const row of slice.rows) await this.insertRow(tx, table, row);
      }
      const readRow = async (table: string, id: string) => {
        requireTable(table);
        reads.add(table);
        const result = await tx.query<JsonObject>(`SELECT * FROM ${quote(table)} WHERE "_id" = $1`, [id], queryOptions);
        if (!result.rows.length && !snapshot.tables[table]?.complete) throw new IncompleteReplicaError(table);
        return result.rows[0];
      };
      const query = async <T>(statement: string, parameters: readonly JsonValue[] = []): Promise<readonly T[]> => {
        // PostgreSQL parses the actual statement and reports every scanned
        // relation, including joins/subqueries. Never infer completeness using
        // a regex that could silently miss a nested relation.
        const explain = await tx.query<Record<string, any>>(`EXPLAIN (FORMAT JSON) ${statement}`, [...parameters], queryOptions);
        const inspect = (value: unknown): void => {
          if (Array.isArray(value)) { value.forEach(inspect); return; }
          if (!value || typeof value !== "object") return;
          const node = value as Record<string, unknown>;
          if (node["Node Type"] === "ModifyTable" || typeof node.Operation === "string") {
            throw new Error("Reducer db.query is read-only; use db.insert/update/delete for durable writes");
          }
          if (typeof node["Relation Name"] === "string") requireComplete(node["Relation Name"]);
          Object.values(node).forEach(inspect);
        };
        inspect(explain.rows);
        return (await tx.query<T>(statement, [...parameters], queryOptions)).rows;
      };
      const context: ReducerContext = {
        ...clone(execution.identity), now: execution.now,
        invocation: {
          channel: "ui", rootChannel: "ui", commandId: execution.commandId,
          rootCommandId: execution.commandId, artifactHash: execution.artifactHash,
          actorAccountId: execution.identity.auth.account?.id ?? null,
          actorMemberId: execution.identity.member?.id ?? null, onBehalfOfMemberId: null,
        },
        db: {
          query,
          insert: async <T>(table: string, row: JsonObject) => {
            const columns = requireTable(table);
            const value = clone(row);
            if (value._id == null) value._id = await nextId(`insert:${table}`);
            if (typeof value._id !== "string" || !value._id) throw new Error("Insert requires a non-empty string id");
            if (columns._creationTime && value._creationTime == null) value._creationTime = execution.now;
            const inserted = await this.insertRow(tx, table, value);
            patches.push({ entity: table, rowId: value._id, op: "insert", fields: clone(inserted) });
            return clone(inserted) as T;
          },
          update: async <T>(table: string, id: string, patch: JsonObject) => {
            const columns = requireTable(table);
            if (Object.hasOwn(patch, "_id")) throw new Error("Reducer update cannot change a row id");
            const before = await readRow(table, id);
            if (!before) throw new Error(`Row ${table}/${id} not found`);
            const keys = Object.keys(patch);
            keys.forEach((key) => { if (!columns[key]) throw new Error(`Unknown column ${table}.${key}`); });
            if (!keys.length) return clone(before) as T;
            const updated = await tx.query<JsonObject>(
              `UPDATE ${quote(table)} SET ${keys.map((key, index) => `${quote(key)} = $${index + 1}`).join(", ")} WHERE "_id" = $${keys.length + 1} RETURNING *`,
              [...keys.map((key) => patch[key]), id], queryOptions,
            );
            patches.push({ entity: table, rowId: id, op: "patch", fields: clone(patch) });
            return clone(updated.rows[0]) as T;
          },
          delete: async (table, id) => {
            const before = await readRow(table, id);
            if (!before) return;
            await tx.query(`DELETE FROM ${quote(table)} WHERE "_id" = $1`, [id]);
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
          const id = await nextId("action");
          deferred.push({ id, kind: "action", path, args: clone(args) });
          return id;
        } },
        scheduler: {
          runAfter: async (delay, path, args = {}) => context.scheduler.runAt(execution.now + delay, path, args),
          runAt: async (at, path, args = {}) => {
            if (!Number.isSafeInteger(at)) throw new Error("Invalid schedule time");
            const id = await nextId("schedule");
            deferred.push({ id, kind: "schedule", path, args: clone(args), at });
            return id;
          },
        },
      };
      const result = clone(await definition.handler!(context, args));
      await tx.rollback();
      return { result, patches, deferred, readTables: [...reads].sort() };
    });
  }

  private async insertRow(tx: Transaction, table: string, row: JsonObject): Promise<JsonObject> {
    const keys = Object.keys(row);
    const columns = this.options.tables[table]!;
    keys.forEach((key) => { if (!columns[key]) throw new Error(`Unknown column ${table}.${key}`); });
    if (!keys.length) throw new Error("Cannot insert an empty local row");
    const result = await tx.query<JsonObject>(
      `INSERT INTO ${quote(table)} (${keys.map(quote).join(", ")}) VALUES (${keys.map((_, index) => `$${index + 1}`).join(", ")}) RETURNING *`,
      keys.map((key) => row[key]), queryOptions,
    );
    return result.rows[0]!;
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
