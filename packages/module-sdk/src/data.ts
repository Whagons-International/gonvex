import type { JsonObject, JsonValue, ReadDB, WriteDB } from "./index.js";

/** Portable reducer reads. SQL remains available to server queries. */
export type DataScalar = string | number | boolean | null;
type DataColumn = {
  column: string;
  transform?: "lower" | "trim" | "lowerTrim";
};
export type DataPredicate =
  | (DataColumn & {
      op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte";
      value: DataScalar;
    })
  | (DataColumn & { op: "in"; values: readonly DataScalar[] })
  | (DataColumn & { op: "isNull" | "notNull" })
  | (DataColumn & { op: "matches"; pattern: string })
  | (DataColumn & { op: "arrayContains"; value: DataScalar })
  | { and: readonly DataPredicate[] }
  | { or: readonly DataPredicate[] };
export type DataRead = {
  table: string;
  /** Primary key used as the final stable ordering term. Defaults to _id. */
  key?: string;
  columns?: readonly string[];
  where?: DataPredicate;
  orderBy?: readonly {
    column: string;
    direction?: "asc" | "desc";
    nulls?: "first" | "last";
    transform?: "numericSuffix";
  }[];
  limit?: number;
  /** Accepted for compatibility. SQLite runs every Reducer transaction on the
   * tenant's single writer connection, so rows read in a Reducer cannot change
   * before commit and no row lock is emitted. */
  lock?: "update";
};
export type DataReader = {
  select<T = JsonObject>(read: DataRead): Promise<readonly T[]>;
};

export function orderedDataRead(read: DataRead): DataRead {
  const key = read.key ?? "_id";
  const orderBy = read.orderBy ?? [];
  return orderBy.some((order) => order.column === key)
    ? read
    : { ...read, orderBy: [...orderBy, { column: key }] };
}

const identifier = (value: string): string => {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
    throw new Error(`Invalid data identifier: ${value}`);
  return `"${value}"`;
};
export const dataValue = (value: unknown): DataScalar => {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  )
    return value;
  throw new Error(
    "Data comparisons require a finite number, string, boolean or null",
  );
};
export const dataValues = (value: unknown): DataScalar[] => {
  if (!Array.isArray(value))
    throw new Error("Data membership requires an array");
  return value.map(dataValue);
};

/** Trailing ASCII digits as a number, NULL when the text has none. SQLite has
 * no regular-expression extraction, so strip the digits with rtrim and take
 * the remainder. */
const numericSuffix = (column: string): string =>
  `CAST(NULLIF(substr(${column}, length(rtrim(${column}, '0123456789')) + 1), '') AS REAL)`;

/** The SQL adapter only emits identifiers and bound parameters, never caller SQL. */
export function compileDataRead(
  read: DataRead,
  options: { ordered?: boolean } = {},
): { statement: string; parameters: JsonValue[] } {
  if (options.ordered !== false) read = orderedDataRead(read);
  const parameters: JsonValue[] = [];
  const bind = (value: unknown) => {
    parameters.push(dataValue(value));
    return `$${parameters.length}`;
  };
  const predicate = (value: DataPredicate): string => {
    if ("and" in value)
      return value.and.length
        ? `(${value.and.map(predicate).join(" AND ")})`
        : "1";
    if ("or" in value)
      return value.or.length
        ? `(${value.or.map(predicate).join(" OR ")})`
        : "0";
    let column = identifier(value.column);
    if (value.transform) {
      if (value.transform === "lower") column = `lower(${column})`;
      else if (value.transform === "trim") column = `trim(${column})`;
      else if (value.transform === "lowerTrim")
        column = `lower(trim(${column}))`;
      else throw new Error("Invalid column transform");
    }
    if (value.op === "isNull") return `${column} IS NULL`;
    if (value.op === "notNull") return `${column} IS NOT NULL`;
    if (value.op === "matches") {
      validateDataPattern(value.pattern);
      // SQLite's REGEXP operator calls regexp(pattern, value), which the
      // runtime registers on every tenant connection.
      return `${column} REGEXP ${bind(value.pattern)}`;
    }
    if (value.op === "arrayContains") {
      const element = dataValue(value.value);
      // Match JSON element types exactly, like JSON containment: the string
      // "1" never matches the number 1 and true never matches 1.
      let match: string;
      if (element === null) match = "element.type = 'null'";
      else if (typeof element === "boolean")
        match = `element.type = '${element ? "true" : "false"}'`;
      else if (typeof element === "number")
        match = `element.type IN ('integer', 'real') AND element.value = ${bind(element)}`;
      else match = `element.type = 'text' AND element.value = ${bind(element)}`;
      return `(CASE WHEN json_type(${column}) = 'array' THEN EXISTS (SELECT 1 FROM json_each(${column}) AS element WHERE ${match}) ELSE 0 END)`;
    }
    if (value.op === "in") {
      if (!value.values.length) return "0";
      // One JSON array parameter keeps the statement shape independent of
      // the list length. Booleans compare as the 0/1 SQLite stores.
      parameters.push(JSON.stringify(dataValues(value.values)));
      return `${column} IN (SELECT value FROM json_each($${parameters.length}))`;
    }
    const operator = {
      eq: "=",
      ne: "<>",
      lt: "<",
      lte: "<=",
      gt: ">",
      gte: ">=",
    }[value.op];
    if (!operator) throw new Error("Unknown data comparison");
    return `${column} ${operator} ${bind((value as { value: DataScalar }).value)}`;
  };
  if (read.columns?.length === 0)
    throw new Error("Data projection must contain at least one column");
  let statement = `SELECT ${read.columns ? read.columns.map(identifier).join(", ") : "*"} FROM ${identifier(read.table)}`;
  if (read.where) statement += ` WHERE ${predicate(read.where)}`;
  if (read.orderBy?.length)
    statement += ` ORDER BY ${read.orderBy
      .map((order) => {
        if (
          order.direction !== undefined &&
          order.direction !== "asc" &&
          order.direction !== "desc"
        )
          throw new Error("Invalid data sort direction");
        if (
          order.nulls !== undefined &&
          order.nulls !== "first" &&
          order.nulls !== "last"
        )
          throw new Error("Invalid data null ordering");
        const direction = order.direction ?? "asc";
        if (
          order.transform !== undefined &&
          order.transform !== "numericSuffix"
        )
          throw new Error("Invalid sort transform");
        const column =
          order.transform === "numericSuffix"
            ? numericSuffix(identifier(order.column))
            : identifier(order.column);
        return `${column} ${direction.toUpperCase()} NULLS ${(order.nulls ?? (direction === "asc" ? "last" : "first")).toUpperCase()}`;
      })
      .join(", ")}`;
  if (read.limit !== undefined) {
    if (!Number.isSafeInteger(read.limit) || read.limit < 0)
      throw new Error("Data read limit must be a non-negative safe integer");
    parameters.push(read.limit);
    statement += ` LIMIT $${parameters.length}`;
  }
  if (read.lock !== undefined) {
    if (read.lock !== "update") throw new Error("Invalid data lock");
    // Single writer per tenant: nothing to lock (see DataRead.lock).
  }
  return { statement, parameters };
}

/** Select uses the local structured adapter when present, otherwise server SQL. */
export async function selectRows<T = JsonObject>(
  db: ReadDB,
  read: DataRead,
): Promise<T[]> {
  read = orderedDataRead(read);
  // Validate before either implementation so invalid input has the same behavior.
  const sql = compileDataRead(read);
  if (db.select) return Array.from(await db.select<T>(read));
  return Array.from(await db.query<T>(sql.statement, sql.parameters));
}

export async function selectFirst<T = JsonObject>(
  db: ReadDB,
  read: DataRead,
): Promise<T | undefined> {
  return (
    await selectRows<T>(db, { ...read, limit: read.limit === 0 ? 0 : 1 })
  )[0];
}

/** Independent reads. Local reads use the same transaction view; no network or
 * asynchronous work can escape that view. On the server each read is one
 * statement against the tenant's in-process SQLite file, so rows come back
 * through the runtime's declared-type decoder (JSON columns as JSON, BOOLEAN
 * columns as booleans) instead of being re-encoded as JSON inside SQL. */
export async function selectDataBatch(
  db: ReadDB,
  reads: readonly DataRead[],
): Promise<JsonObject[][]> {
  if (db.select)
    return Promise.all(reads.map((read) => selectRows<JsonObject>(db, read)));
  const result: JsonObject[][] = [];
  // Compile every read first so invalid input fails before any SQL runs.
  const compiled = reads.map((read) => compileDataRead(read));
  for (const sql of compiled)
    result.push(
      Array.from(await db.query<JsonObject>(sql.statement, sql.parameters)),
    );
  return result;
}

export async function getRow<T = JsonObject>(
  db: ReadDB,
  table: string,
  key: string,
  id: DataScalar,
): Promise<T | undefined> {
  return (
    await selectRows<T>(db, {
      table,
      key,
      where: { column: key, op: "eq", value: id },
      limit: 1,
    })
  )[0];
}

export async function existingDataTables(
  db: ReadDB,
  tables: readonly string[],
): Promise<ReadonlySet<string>> {
  tables.forEach(identifier);
  if (db.tables)
    return new Set(tables.filter((table) => db.tables!.includes(table)));
  const rows = await db.query<{ name: string }>(
    "SELECT name FROM sqlite_schema WHERE type IN ('table', 'view') AND name IN (SELECT value FROM json_each($1))",
    [JSON.stringify(tables)],
  );
  return new Set(rows.map((row) => row.name));
}

/** Batch indexed existence probes without transferring matching rows. Useful
 * when a reducer must check references for many selected configuration rows. */
export async function existsDataRows(
  db: ReadDB,
  reads: readonly Pick<DataRead, "table" | "key" | "where">[],
): Promise<boolean[]> {
  if (db.select)
    return Promise.all(
      reads.map(
        async (read) =>
          (
            await selectRows(db, {
              ...read,
              columns: [read.key ?? "_id"],
              limit: 1,
            })
          ).length > 0,
      ),
    );
  const results: boolean[] = [];
  for (let offset = 0; offset < reads.length; offset += 256) {
    const parameters: JsonValue[] = [];
    const expressions = reads.slice(offset, offset + 256).map((read, index) => {
      const sql = compileDataRead(
        { ...read, columns: [read.key ?? "_id"] },
        { ordered: false },
      );
      const base = parameters.length;
      const statement = sql.statement.replace(
        /\$(\d+)/g,
        (_, number) => `$${Number(number) + base}`,
      );
      parameters.push(...sql.parameters);
      return `EXISTS (${statement}) AS found_${index}`;
    });
    const [row] = await db.query<Record<string, boolean | number>>(
      `SELECT ${expressions.join(", ")}`,
      parameters,
    );
    // EXISTS is an untyped expression column: SQLite returns 0 or 1.
    const found = (value: unknown): boolean | undefined =>
      value === true || value === 1 ? true : value === false || value === 0 ? false : undefined;
    if (
      !row ||
      expressions.some((_, index) => found(row[`found_${index}`]) === undefined)
    )
      throw new Error("Database returned an invalid existence result");
    results.push(...expressions.map((_, index) => found(row[`found_${index}`])!));
  }
  return results;
}

export type DataMetric =
  | { op: "count"; where?: DataPredicate }
  | { op: "sum"; column: string; where?: DataPredicate };

/** Aggregates share their predicates and arithmetic across both adapters. */
export async function summarizeRows(
  db: ReadDB,
  read: Omit<DataRead, "columns" | "orderBy" | "limit" | "lock">,
  metrics: Readonly<Record<string, DataMetric>>,
): Promise<Record<string, number>> {
  const names = Object.keys(metrics);
  if (!names.length) return {};
  const columns = new Set<string>([read.key ?? "_id"]);
  const collect = (predicate: DataPredicate): void => {
    if ("and" in predicate) predicate.and.forEach(collect);
    else if ("or" in predicate) predicate.or.forEach(collect);
    else columns.add(predicate.column);
  };
  const expressions = names.map((name) => {
    identifier(name);
    const metric = metrics[name]!;
    if (metric.op === "sum") {
      identifier(metric.column);
      columns.add(metric.column);
    } else if (metric.op !== "count")
      throw new Error("Unsupported data metric");
    if (metric.where) collect(metric.where);
    return { name, metric };
  });
  if (db.select) {
    const rows = await selectRows<JsonObject>(db, {
      ...read,
      columns: [...columns],
    });
    return Object.fromEntries(
      expressions.map(({ name, metric }) => [
        name,
        rows.reduce((total, row) => {
          if (metric.where && !matchesDataPredicate(row, metric.where))
            return total;
          return (
            total +
            (metric.op === "count" ? 1 : Number(row[metric.column] ?? 0))
          );
        }, 0),
      ]),
    );
  }
  const base = compileDataRead(
    { ...read, columns: [...columns] },
    { ordered: false },
  );
  const parameters = [...base.parameters];
  const sql = expressions.map(({ name, metric }) => {
    let filter = "";
    if (metric.where) {
      const compiled = compileDataRead(
        { table: read.table, key: read.key, where: metric.where },
        { ordered: false },
      );
      const offset = parameters.length;
      // Everything after the first WHERE; predicates may contain nested
      // subqueries with their own WHERE (arrayContains).
      const where = compiled.statement.slice(
        compiled.statement.indexOf(" WHERE ") + " WHERE ".length,
      );
      filter =
        " FILTER (WHERE " +
        where
          .replace(/\$(\d+)/g, (_, n) => `$${Number(n) + offset}`) +
        ")";
      parameters.push(...compiled.parameters);
    }
    return `COALESCE(${metric.op === "count" ? "count(*)" : `sum(${identifier(metric.column)})`}${filter},0) AS ${identifier(name)}`;
  });
  const [row] = await db.query<JsonObject>(
    `SELECT ${sql.join(", ")} FROM (${base.statement}) AS source`,
    parameters,
  );
  return Object.fromEntries(
    names.map((name) => [name, Number(row?.[name] ?? 0)]),
  );
}

/** Serialize a named business resource for the authoritative transaction.
 * On the server this is a no-op: every Reducer transaction for a tenant runs
 * on that tenant's single SQLite writer connection, one at a time, so a
 * Reducer already holds every resource until it commits. Adapters that run
 * concurrent transactions provide `db.lock`. */
export async function lockData(db: WriteDB, key: string): Promise<void> {
  if (!key || typeof key !== "string")
    throw new Error("Data lock requires a nonempty key");
  if (db.lock) await db.lock(key);
}

function validateDataPattern(pattern: string): void {
  // Deliberately exclude engine-specific regex extensions. Application
  // patterns use common anchors, literals, groups and character classes.
  if (
    typeof pattern !== "string" ||
    pattern.includes("(?") ||
    pattern.includes("[[:") ||
    /\\(?:[1-9dDsSwWbBpP]|[uUxXcC])/.test(pattern)
  )
    throw new Error("Pattern is not portable");
  new RegExp(pattern);
}

/** SQL WHERE semantics: absent/null operands never pass ordinary comparisons. */
export function matchesDataPredicate(
  row: JsonObject,
  predicate: DataPredicate,
): boolean {
  if ("and" in predicate)
    return predicate.and.every((part) => matchesDataPredicate(row, part));
  if ("or" in predicate)
    return predicate.or.some((part) => matchesDataPredicate(row, part));
  let left = row[predicate.column];
  if (left != null && predicate.transform) {
    if (typeof left !== "string")
      throw new Error("Text transform requires a string");
    // SQLite trim without a character argument strips ASCII spaces only.
    if (predicate.transform === "trim" || predicate.transform === "lowerTrim")
      left = left.replace(/^ +| +$/g, "");
    if (predicate.transform === "lower" || predicate.transform === "lowerTrim")
      left = left.toLowerCase();
  }
  if (predicate.op === "isNull") return left == null;
  if (predicate.op === "notNull") return left != null;
  if (left == null) return false;
  if (predicate.op === "arrayContains")
    return (
      Array.isArray(left) && left.some((value) => value === predicate.value)
    );
  if (predicate.op === "matches") {
    validateDataPattern(predicate.pattern);
    return typeof left === "string" && new RegExp(predicate.pattern).test(left);
  }
  if (predicate.op === "in")
    return predicate.values.some((value) => value === left);
  const right = (predicate as { value: DataScalar }).value;
  if (right === null) return false;
  // Type conversion belongs to the generated column adapter, never JS coercion.
  if (typeof left !== typeof right)
    throw new Error(`Incompatible comparison for ${predicate.column}`);
  switch (predicate.op) {
    case "eq":
      return left === right;
    case "ne":
      return left !== right;
    case "lt":
      return (left as Exclude<DataScalar, null>) < right;
    case "lte":
      return (left as Exclude<DataScalar, null>) <= right;
    case "gt":
      return (left as Exclude<DataScalar, null>) > right;
    case "gte":
      return (left as Exclude<DataScalar, null>) >= right;
    default:
      throw new Error("Unknown data comparison");
  }
}
