import { executionReadColumns } from './read-view.js';
import {
  compileDataRead,
  orderedDataRead,
  matchesDataPredicate,
  reducerRowId,
} from "@gonvex/module-sdk";
import type {
  DataRead,
  JsonObject,
  JsonValue,
  ReducerContext,
  ReducerDefinition,
} from "@gonvex/module-sdk";
import type {
  LocalExecution,
  LocalPatch,
  LocalTransactionResult,
} from "./index.js";
import type { LocalColumn, LocalSchema } from "./schema.js";
import { validateValue } from "./validation.js";

/** A transaction-consistent SDK read view. Implementations may use RAM or disk.
 * Rows are detached values; complete describes this read, not the whole database.
 */
export type ReducerReadView = {
  readonly keepAliveFor?: <T>(promise: Promise<T>) => Promise<T>;
  select(
    read: DataRead,
    /** Base rows fully replaced or removed by the SDK's pending journal. */
    excludedRowIds?: readonly string[],
  ): Promise<{ rows: readonly JsonObject[]; complete: boolean }>;
};
export class MissingReducerDataError extends Error {
  constructor(readonly read: DataRead) {
    super(
      `Local replica for ${read.table} is incomplete; this reducer cannot decide from missing rows.`,
    );
    this.name = "IncompleteReplicaError";
  }
}
export class UnsupportedLocalOperationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedLocalOperationError";
  }
}
export type PortableReducerOptions = {
  schema: LocalSchema;
  reducers: Readonly<Record<string, ReducerDefinition<any, any> | (() => Promise<ReducerDefinition<any, any>>)>>;
  artifactHash: string;
};

const copy = <T>(value: T): T => structuredClone(value);
function defaultValue(column: LocalColumn, now: number): JsonValue {
  const expression = column.default?.trim();
  if (!expression || /^NULL(?:::.*)?$/i.test(expression)) return null;
  if (
    column.type === "timestamp with time zone" &&
    /^(?:clock_timestamp|now)\(\)$/i.test(expression)
  )
    return new Date(now).toISOString();
  if (/^(true|false)$/i.test(expression))
    return expression.toLowerCase() === "true";
  if (/^-?\d+(?:\.\d+)?$/.test(expression)) return Number(expression);
  const literal = /^'((?:[^']|'')*)'(?:::(?:[a-z ]+)(?:\[\])?)?$/i.exec(
    expression,
  );
  if (literal) {
    const text = literal[1]!.replaceAll("''", "'");
    if (column.type === "jsonb" || column.type === "json")
      return JSON.parse(text) as JsonValue;
    if (column.type === "boolean") return text === "true";
    if (
      [
        "integer",
        "bigint",
        "smallint",
        "numeric",
        "double precision",
        "real",
      ].includes(column.type)
    )
      return Number(text);
    return text;
  }
  throw new UnsupportedLocalOperationError(
    `Default ${expression} must be supplied explicitly by the shared reducer`,
  );
}

function columnValue(
  table: string,
  name: string,
  column: LocalColumn,
  value: JsonValue,
): JsonValue {
  if (value == null) {
    if (!column.nullable)
      throw new Error(`Required field ${table}.${name} is missing`);
    return null;
  }
  const type = column.type;
  if (["integer", "bigint", "smallint"].includes(type)) {
    const number =
      typeof value === "string" && /^[-+]?\d+$/.test(value.trim())
        ? Number(value)
        : value;
    if (typeof number !== "number" || !Number.isSafeInteger(number))
      throw new Error(`Invalid integer for ${table}.${name}`);
    if (type === "integer" && (number < -2147483648 || number > 2147483647))
      throw new Error(`Integer out of range for ${table}.${name}`);
    if (type === "smallint" && (number < -32768 || number > 32767))
      throw new Error(`Integer out of range for ${table}.${name}`);
    return number;
  }
  if (["double precision", "real", "numeric"].includes(type)) {
    const number =
      typeof value === "string" && value.trim() ? Number(value) : value;
    if (typeof number !== "number" || !Number.isFinite(number))
      throw new Error(`Invalid number for ${table}.${name}`);
    return number;
  }
  if (type === "boolean") {
    if (typeof value !== "boolean")
      throw new Error(`Invalid boolean for ${table}.${name}`);
    return value;
  }
  if (type === "uuid") {
    if (
      typeof value !== "string" ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        value,
      )
    )
      throw new Error(`Invalid UUID for ${table}.${name}`);
    return value.toLowerCase();
  }
  if (type === "text" || type.startsWith("character varying")) {
    if (typeof value !== "string")
      throw new Error(`Invalid text for ${table}.${name}`);
    return value;
  }
  if (type === "jsonb" || type === "json") return copy(value);
  if (type === "timestamp with time zone") {
    if (typeof value !== "string" || !Number.isFinite(Date.parse(value)))
      throw new Error(`Invalid timestamp for ${table}.${name}`);
    return new Date(value).toISOString();
  }
  throw new UnsupportedLocalOperationError(
    `Column type ${type} requires a portable adapter`,
  );
}

export function orderDataRows(rows: JsonObject[], read: DataRead): void {
  if (!read.orderBy?.length) return;
  rows.sort((a, b) => {
    for (const order of read.orderBy!) {
      const sortValue = (row: JsonObject) => {
        const value = row[order.column];
        if (order.transform !== "numericSuffix" || value == null) return value;
        if (typeof value !== "string")
          throw new Error("Numeric suffix requires text");
        const suffix = value.match(/[0-9]+$/)?.[0];
        return suffix === undefined ? null : BigInt(suffix);
      };
      const left = sortValue(a),
        right = sortValue(b);
      if (left === right) continue;
      const direction = order.direction === "desc" ? -1 : 1;
      const nullFirst =
        (order.nulls ?? (direction === 1 ? "last" : "first")) === "first";
      if (left == null) return nullFirst ? -1 : 1;
      if (right == null) return nullFirst ? 1 : -1;
      if (typeof left !== typeof right || typeof left === "object")
        throw new Error(`Incompatible ordering for ${order.column}`);
      return (left < right ? -1 : 1) * direction;
    }
    return 0;
  });
}

/** Shared reducer execution with no SQL engine and no collection snapshot copies. */
export class PortableReducerRuntime {
  private readonly definitions = new Map<string, Promise<ReducerDefinition<any, any>>>();
  constructor(private readonly options: PortableReducerOptions) {}

  private definition(path: string): Promise<ReducerDefinition<any, any>> {
    const entry = this.options.reducers[path];
    if (!entry) return Promise.reject(new Error(`No public local reducer registered for ${path}`));
    if (typeof entry !== 'function') return Promise.resolve(entry);
    let pending = this.definitions.get(path);
    if (!pending) {
      pending = Promise.resolve().then(entry).catch(error => { this.definitions.delete(path); throw error; });
      this.definitions.set(path, pending);
    }
    return pending;
  }

  async prepare(path: string): Promise<void> { await this.definition(path); }

  async execute(
    path: string,
    args: JsonValue,
    source: ReducerReadView,
    execution: LocalExecution,
  ): Promise<LocalTransactionResult> {
    if (execution.artifactHash !== this.options.artifactHash)
      throw new Error("Local reducer artifact mismatch");
    const definition = await this.definition(path);
    if (
      !definition ||
      definition.kind !== "reducer" ||
      definition.internal ||
      !definition.handler
    )
      throw new Error(`No public local reducer registered for ${path}`);
    validateValue(definition.options.args, args);
    const changes = new Map<string, Map<string, JsonObject | null>>();
    const patches: LocalPatch[] = [];
    const deferred: LocalTransactionResult["deferred"] = [];
    const reads = new Set<string>();
    const primaryReads = new Map<
      string,
      Map<string, Promise<JsonObject | undefined>>
    >();
    const tableSchema = (table: string) => {
      const schema = this.options.schema[table];
      if (!schema) throw new Error(`Unknown local table ${table}`);
      return schema;
    };
    const changed = (table: string) => {
      let rows = changes.get(table);
      if (!rows) {
        rows = new Map();
        changes.set(table, rows);
      }
      return rows;
    };
    const select = async <T>(read: DataRead): Promise<readonly T[]> => {
      read = orderedDataRead({
        ...read,
        key: read.key ?? tableSchema(read.table).key,
      });
      compileDataRead(read);
      const schema = tableSchema(read.table);
      const columns = new Set(read.columns ?? []);
      const collect = (predicate: NonNullable<DataRead["where"]>): void => {
        if ("and" in predicate) predicate.and.forEach(collect);
        else if ("or" in predicate) predicate.or.forEach(collect);
        else columns.add(predicate.column);
      };
      if (read.where) collect(read.where);
      for (const order of read.orderBy ?? []) columns.add(order.column);
      for (const column of columns)
        if (!schema.columns[column])
          throw new Error(`Unknown column ${read.table}.${column}`);
      reads.add(read.table);
      if (read.limit === 0) return [];
      const staged = changes.get(read.table);
      // Fetch enough ordered candidates to replace rows removed by this transaction.
      const result = await source.select({
        ...read,
        columns: executionReadColumns(read, schema.key),
        limit:
          read.limit === undefined
            ? undefined
            : read.limit + (staged?.size ?? 0),
      });
      if (!result.complete) throw new MissingReducerDataError(read);
      const rows = new Map(
        result.rows.map((row) => [String(row[schema.key]), row]),
      );
      for (const [id, row] of staged ?? []) {
        rows.delete(id);
        if (row && (!read.where || matchesDataPredicate(row, read.where)))
          rows.set(id, row);
      }
      const ordered = [...rows.values()];
      orderDataRows(ordered, read);
      const window =
        read.limit === undefined ? ordered : ordered.slice(0, read.limit);
      return window.map((row) =>
        copy(
          read.columns
            ? Object.fromEntries(
                read.columns.map((column) => [column, row[column] ?? null]),
              )
            : row,
        ),
      ) as T[];
    };
    const get = async (
      table: string,
      id: string,
    ): Promise<JsonObject | undefined> => {
      const schema = tableSchema(table);
      const staged = changes.get(table);
      if (staged?.has(id)) return copy(staged.get(id) ?? undefined);
      let tableReads = primaryReads.get(table);
      if (!tableReads) {
        tableReads = new Map();
        primaryReads.set(table, tableReads);
      }
      let pending = tableReads.get(id);
      if (!pending) {
        pending = select<JsonObject>({
          table,
          key: schema.key,
          where: { column: schema.key, op: "eq", value: id },
          limit: 1,
        }).then((rows) => rows[0]);
        tableReads.set(id, pending);
      }
      return copy(await pending);
    };
    const context: ReducerContext = {
      ...copy(execution.identity),
      now: execution.now,
      intentEntropy: execution.intentEntropy,
      invocation: {
        channel: "ui",
        rootChannel: "ui",
        commandId: execution.commandId,
        rootCommandId: execution.commandId,
        artifactHash: execution.artifactHash,
        actorAccountId: execution.identity.auth.account?.id ?? null,
        actorMemberId: execution.identity.member?.id ?? null,
        onBehalfOfMemberId: null,
      },
      db: {
        keepAliveFor: source.keepAliveFor,
        tables: Object.keys(this.options.schema),
        select,
        lock: async () => {
          /* The SDK serializes local intent execution. */
        },
        atomic: async (run) => {
          const prior = new Map(
            [...changes].map(([table, rows]) => [table, new Map(rows)]),
          );
          const length = patches.length;
          try {
            return await run();
          } catch (error) {
            changes.clear();
            for (const [table, rows] of prior) changes.set(table, rows);
            patches.length = length;
            throw error;
          }
        },
        query: async () => {
          throw new UnsupportedLocalOperationError(
            "Raw SQL is not available in portable reducers; use structured database reads",
          );
        },
        insert: async <T>(
          table: string,
          input: JsonObject,
          allocation?: { generatedId?: string },
        ): Promise<T> => {
          const schema = tableSchema(table);
          const row = copy(input);
          const allocated =
            allocation?.generatedId !== undefined &&
            (row[schema.key] == null ||
              row[schema.key] === allocation.generatedId);
          if (row[schema.key] == null)
            row[schema.key] =
              allocation?.generatedId ??
              (await reducerRowId(context, table, 0));
          if (schema.columns._creationTime && row._creationTime == null)
            row._creationTime = execution.now;
          for (const name of Object.keys(row))
            if (!schema.columns[name])
              throw new Error(`Unknown column ${table}.${name}`);
          for (const [name, column] of Object.entries(schema.columns))
            row[name] = columnValue(
              table,
              name,
              column,
              row[name] === undefined
                ? defaultValue(column, execution.now)
                : row[name]!,
            );
          const id = String(row[schema.key]);
          // An intent-owned ID is new by construction. Requiring a complete
          // history/log collection to prove its absence defeats bounded offline
          // storage. Authoritative replay still enforces uniqueness.
          if (allocated ? changed(table).has(id) : await get(table, id))
            throw new Error(`Row ${table}/${id} already exists`);
          changed(table).set(id, row);
          patches.push({
            entity: table,
            rowId: id,
            op: "insert",
            fields: copy(row),
          });
          return copy(row) as T;
        },
        update: async <T>(
          table: string,
          id: string,
          input: JsonObject,
        ): Promise<T> => {
          const schema = tableSchema(table);
          if (Object.hasOwn(input, schema.key))
            throw new Error("Reducer update cannot change a row id");
          const before = await get(table, id);
          if (!before) throw new Error(`Row ${table}/${id} not found`);
          const fields: JsonObject = {};
          for (const [name, value] of Object.entries(input)) {
            if (value === undefined) continue;
            const column = schema.columns[name];
            if (!column) throw new Error(`Unknown column ${table}.${name}`);
            fields[name] = columnValue(table, name, column, value);
          }
          const row = { ...before, ...fields };
          if (Object.keys(fields).length) {
            changed(table).set(id, row);
            patches.push({ entity: table, rowId: id, op: "patch", fields });
          }
          return copy(row) as T;
        },
        delete: async (table, id) => {
          if ((await select({table, columns: [tableSchema(table).key], where: {column: tableSchema(table).key, op: "eq", value: id}, limit: 1})).length) {
            changed(table).set(id, null);
            patches.push({ entity: table, rowId: id, op: "delete" });
          }
        },
        deleteMany: async (table, ids) => {
          let deleted = 0;
          for (const id of new Set(ids)) {
            if (typeof id !== "string")
              throw new Error("Delete requires a string id");
            if ((await select({table, columns: [tableSchema(table).key], where: {column: tableSchema(table).key, op: "eq", value: id}, limit: 1})).length) {
              await context.db.delete(table, id);
              deleted++;
            }
          }
          return { deleted };
        },
      },
      actions: {
        enqueue: async (path, args) => {
          const id = await reducerRowId(
            context,
            "deferred:action",
            deferred.filter((d) => d.kind === "action").length,
          );
          deferred.push({ id, kind: "action", path, args: copy(args) });
          return id;
        },
      },
      scheduler: {
        runAfter: async (delay, path, args = {}) =>
          context.scheduler.runAt(execution.now + delay, path, args),
        runAt: async (at, path, args = {}) => {
          if (!Number.isSafeInteger(at))
            throw new Error("Invalid schedule time");
          const id = `job_${await reducerRowId(context, "deferred:schedule", deferred.filter((d) => d.kind === "schedule").length)}`;
          deferred.push({ id, kind: "schedule", path, args: copy(args), at });
          return id;
        },
      },
    };
    const result = copy(await definition.handler(context, copy(args)));
    validateValue(definition.options.result, result);
    return { result, patches, deferred, readTables: [...reads].sort() };
  }
}
