import { reducerRowId } from "./reducer-execution.js";
import type {
  JsonObject,
  JsonValue,
  ReducerContext,
  WriteDB,
} from "./index.js";
import { compileDataRead, selectRows, type DataRead } from "./data.js";

const quote = (name: string) => {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
    throw new Error(`Invalid data identifier: ${name}`);
  return `"${name}"`;
};
const allocations = new WeakMap<WriteDB, Map<string, number>>();
/** `statement` ends in `RETURNING 1`; one tiny row per affected row. */
async function affectedRows(
  db: WriteDB,
  statement: string,
  parameters: readonly JsonValue[],
): Promise<number> {
  return (await db.query(statement, parameters)).length;
}
/**
 * The value of `column` in a JSON object `source`, converted to what the
 * column stores. JSON columns keep the JSON text of the value (`->`), with a
 * JSON null stored as SQL NULL; every other column takes the SQL value
 * (`->>`: text, integer, real, NULL, and 0/1 for booleans). The declared
 * type comes from pragma_table_info, the same source of truth the runtime
 * uses to decode rows, so callers never pass column types.
 */
const jsonColumn = (table: string, column: string, source: string): string => {
  const path = `'$.${column}'`;
  return `CASE WHEN (SELECT upper(type) FROM pragma_table_info('${table}') WHERE name = '${column}') LIKE '%JSON%' THEN nullif(${source} -> ${path}, 'null') ELSE ${source} ->> ${path} END`;
};
const groups = <T>(values: readonly T[], columns: (value: T) => string[]) => {
  const result = new Map<string, T[]>();
  for (const value of values) {
    const signature = JSON.stringify(columns(value).sort());
    const group = result.get(signature) ?? [];
    group.push(value);
    result.set(signature, group);
  }
  return result;
};

/** One shared bulk operation. SQLite receives bounded JSON row batches;
 * local execution stages the same rows in the current intent transaction.
 */
export async function insertDataRows(
  ctx: ReducerContext,
  table: string,
  input: readonly JsonObject[],
  key = "_id",
): Promise<JsonObject[]> {
  quote(table);
  quote(key);
  const counters = allocations.get(ctx.db) ?? new Map<string, number>();
  allocations.set(ctx.db, counters);
  let ordinal = counters.get(table) ?? 0;
  counters.set(table, ordinal + input.length);
  const prepared = await Promise.all(
    input.map(async (value) => {
      const row = { ...value };
      const generatedId =
        row[key] == null
          ? await reducerRowId(ctx, `batch:${table}`, ordinal++)
          : undefined;
      if (generatedId) row[key] = generatedId;
      Object.keys(row).forEach(quote);
      return { row, generatedId };
    }),
  );
  if (ctx.db.select) {
    const result: JsonObject[] = [];
    for (const { row, generatedId } of prepared)
      result.push(await ctx.db.insert<JsonObject>(table, row, { generatedId }));
    return result;
  }
  const result: JsonObject[] = [];
  for (const [signature, group] of groups(prepared, (entry) =>
    Object.keys(entry.row),
  )) {
    const names = JSON.parse(signature) as string[];
    const columns = names.map(quote).join(", ");
    const values = names
      .map((name) => jsonColumn(table, name, "source.value"))
      .join(", ");
    for (let offset = 0; offset < group.length; offset += 500) {
      const rows = group.slice(offset, offset + 500).map((entry) => entry.row);
      // ORDER BY keeps input order for rowids.
      result.push(
        ...(await ctx.db.query<JsonObject>(
          `INSERT INTO ${quote(table)} (${columns}) SELECT ${values} FROM json_each($1) AS source ORDER BY source.key RETURNING *`,
          [JSON.stringify(rows)],
        )),
      );
    }
  }
  const byId = new Map(result.map((row) => [String(row[key]), row]));
  return prepared.map(({ row }) => byId.get(String(row[key]))!).filter(Boolean);
}

export type DataRowUpdate = { id: string; fields: JsonObject };
export async function updateDataRows(
  db: WriteDB,
  table: string,
  updates: readonly DataRowUpdate[],
  key = "_id",
): Promise<void> {
  quote(table);
  quote(key);
  const ids = new Set<string>();
  for (const { id, fields } of updates) {
    if (!id || ids.has(id))
      throw new Error("Batch update requires distinct nonempty row IDs");
    if (key in fields)
      throw new Error("Batch update cannot change the primary key");
    Object.keys(fields).forEach(quote);
    ids.add(id);
  }
  if (db.select) {
    for (const update of updates) {
      const rows = await selectRows(db, {
        table,
        key,
        columns: [key],
        where: { column: key, op: "eq", value: update.id },
        limit: 1,
      });
      if (rows.length) await db.update(table, update.id, update.fields);
    }
    return;
  }
  for (const [signature, values] of groups(updates, (update) =>
    Object.keys(update.fields),
  )) {
    const columns = JSON.parse(signature) as string[];
    if (!columns.length) continue;
    const assignments = columns
      .map((name) => `${quote(name)} = ${jsonColumn(table, name, "source.value")}`)
      .join(", ");
    for (let offset = 0; offset < values.length; offset += 500) {
      const rows = values
        .slice(offset, offset + 500)
        .map((update) => ({ ...update.fields, [key]: update.id }));
      await db.query(
        `UPDATE ${quote(table)} SET ${assignments} FROM json_each($1) AS source WHERE ${quote(table)}.${quote(key)} = source.value ->> '$.${key}'`,
        [JSON.stringify(rows)],
      );
    }
  }
}

export async function deleteDataRows(
  db: WriteDB,
  table: string,
  ids: readonly string[],
  key = "_id",
): Promise<number> {
  quote(table);
  quote(key);
  const unique = [...new Set(ids)];
  if (!unique.length) return 0;
  if (unique.some((id) => typeof id !== "string" || !id))
    throw new Error("Delete requires nonempty row IDs");
  if (db.select) return (await db.deleteMany(table, unique)).deleted;
  let deleted = 0;
  for (let offset = 0; offset < unique.length; offset += 500)
    deleted += (
      await db.query(
        `DELETE FROM ${quote(table)} WHERE ${quote(key)} IN (SELECT value FROM json_each($1)) RETURNING 1`,
        [JSON.stringify(unique.slice(offset, offset + 500))],
      )
    ).length;
  return deleted;
}

export async function deleteDataWhere(
  db: WriteDB,
  read: DataRead,
  key = "_id",
): Promise<number> {
  const selected = { ...read, key, columns: [key] };
  if (db.select)
    return deleteDataRows(
      db,
      read.table,
      (await selectRows(db, selected)).map((row) => String(row[key])),
      key,
    );
  const sql = compileDataRead(selected);
  return affectedRows(
    db,
    `DELETE FROM ${quote(read.table)} WHERE ${quote(key)} IN (${sql.statement}) RETURNING 1`,
    sql.parameters,
  );
}

export async function updateDataWhere(
  db: WriteDB,
  read: DataRead,
  fields: JsonObject,
  key = "_id",
): Promise<number> {
  const names = Object.keys(fields);
  names.forEach(quote);
  if (key in fields) throw new Error("Update cannot change the primary key");
  if (!names.length) return 0;
  if (db.select) {
    const rows = await selectRows(db, { ...read, key, columns: [key] });
    await updateDataRows(
      db,
      read.table,
      rows.map((row) => ({ id: String(row[key]), fields })),
      key,
    );
    return rows.length;
  }
  const sql = compileDataRead({ ...read, key, columns: [key] });
  const parameter = `$${sql.parameters.length + 1}`;
  return affectedRows(
    db,
    `UPDATE ${quote(read.table)} SET ${names.map((name) => `${quote(name)} = ${jsonColumn(read.table, name, parameter)}`).join(", ")} WHERE ${quote(key)} IN (${sql.statement}) RETURNING 1`,
    [...sql.parameters, JSON.stringify(fields)],
  );
}

export type DataWrite =
  | { kind: "insert"; table: string; rows: readonly JsonObject[]; key?: string }
  | {
      kind: "update";
      table: string;
      rows: readonly DataRowUpdate[];
      key?: string;
    }
  | {
      kind: "updateWhere";
      table: string;
      read: Omit<DataRead, "table">;
      fields: JsonObject;
      key?: string;
    }
  | {
      kind: "deleteWhere";
      table: string;
      read: Omit<DataRead, "table">;
      key?: string;
    }
  | { kind: "delete"; table: string; ids: readonly string[]; key?: string };

/** A prepared multi-table fanout. All IDs and calculations
 * come from the shared reducer; no SQL business logic is hidden in this adapter.
 */
export async function applyDataWrites(
  ctx: ReducerContext,
  operations: readonly DataWrite[],
): Promise<void> {
  const seen = new Set<string>();
  const predicateTables = new Set(
    operations
      .filter(
        (operation) =>
          operation.kind === "updateWhere" || operation.kind === "deleteWhere",
      )
      .map((operation) => operation.table),
  );
  for (const table of predicateTables)
    if (operations.filter((operation) => operation.table === table).length > 1)
      throw new Error(
        "Predicate writes require one operation per table in a data batch",
      );
  for (const operation of operations) {
    if (operation.kind === "updateWhere" || operation.kind === "deleteWhere")
      continue;
    const ids =
      operation.kind === "delete"
        ? operation.ids
        : operation.kind === "update"
          ? operation.rows.map((row) => row.id)
          : operation.rows.map((row) => row[operation.key ?? "_id"]);
    for (const id of ids) {
      if (id == null) continue;
      const target = JSON.stringify([operation.table, id]);
      if (seen.has(target))
        throw new Error("A data batch cannot write the same row twice");
      seen.add(target);
    }
  }
  if (ctx.db.atomic) {
    const db = { ...ctx.db, atomic: undefined };
    allocations.set(db, allocations.get(ctx.db) ?? new Map<string, number>());
    allocations.set(ctx.db, allocations.get(db)!);
    return ctx.db.atomic(() => applyDataWrites({ ...ctx, db }, operations));
  }
  // SQLite has no data-modifying CTEs, so each operation is its own
  // statement in the Reducer's transaction, in order. A savepoint keeps the
  // batch all-or-nothing even when the Reducer catches the error and commits.
  const db = ctx.db;
  if (db.select) return writeOperations(ctx, operations);
  await db.query("SAVEPOINT gonvex_data_writes");
  try {
    await writeOperations(ctx, operations);
  } catch (error) {
    await db.query("ROLLBACK TO gonvex_data_writes");
    await db.query("RELEASE gonvex_data_writes");
    throw error;
  }
  await db.query("RELEASE gonvex_data_writes");
}

async function writeOperations(
  ctx: ReducerContext,
  operations: readonly DataWrite[],
): Promise<void> {
  const db = ctx.db;
  for (const operation of operations) {
    if (operation.kind === "insert")
      await insertDataRows(ctx, operation.table, operation.rows, operation.key);
    else if (operation.kind === "update")
      await updateDataRows(db, operation.table, operation.rows, operation.key);
    else if (operation.kind === "updateWhere")
      await updateDataWhere(
        db,
        { ...operation.read, table: operation.table },
        operation.fields,
        operation.key,
      );
    else if (operation.kind === "deleteWhere")
      await deleteDataWhere(
        db,
        { ...operation.read, table: operation.table },
        operation.key,
      );
    else
      await deleteDataRows(db, operation.table, operation.ids, operation.key);
  }
}
