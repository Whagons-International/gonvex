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
  /** Locks authoritative rows until commit; the local intent lane is serialized. */
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
        : "TRUE";
    if ("or" in value)
      return value.or.length
        ? `(${value.or.map(predicate).join(" OR ")})`
        : "FALSE";
    let column = identifier(value.column);
    if (value.transform) {
      if (value.transform === "lower") column = `lower(${column})`;
      else if (value.transform === "trim") column = `btrim(${column})`;
      else if (value.transform === "lowerTrim")
        column = `lower(btrim(${column}))`;
      else throw new Error("Invalid column transform");
    }
    if (value.op === "isNull") return `${column} IS NULL`;
    if (value.op === "notNull") return `${column} IS NOT NULL`;
    if (value.op === "matches") {
      validateDataPattern(value.pattern);
      return `${column} ~ ${bind(value.pattern)}`;
    }
    if (value.op === "arrayContains") {
      parameters.push(JSON.stringify([dataValue(value.value)]));
      return `(jsonb_typeof(${column}) = 'array' AND ${column} @> $${parameters.length}::text::jsonb)`;
    }
    if (value.op === "in") {
      if (!value.values.length) return "FALSE";
      parameters.push(dataValues(value.values));
      return `${column} = ANY($${parameters.length})`;
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
            ? `substring(${identifier(order.column)} from '[0-9]+$')::numeric`
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
    statement += " FOR UPDATE";
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

/** Independent reads share one server host call. Local reads use the same
 * transaction view; no network or asynchronous work can escape that view. */
export async function selectDataBatch(
  db: ReadDB,
  reads: readonly DataRead[],
): Promise<JsonObject[][]> {
  if (db.select)
    return Promise.all(reads.map((read) => selectRows<JsonObject>(db, read)));
  const result: JsonObject[][] = [];
  for (let offset = 0; offset < reads.length; offset += 32) {
    const parameters: JsonValue[] = [];
    const expressions = reads.slice(offset, offset + 32).map((read, index) => {
      const compiled = compileDataRead(read);
      const base = parameters.length;
      const statement = compiled.statement.replace(
        /\$(\d+)/g,
        (_, number) => `$${Number(number) + base}`,
      );
      parameters.push(...compiled.parameters);
      return `(SELECT COALESCE(jsonb_agg(to_jsonb(source)), '[]'::jsonb) FROM (${statement}) AS source) AS read_${index}`;
    });
    const [row] = await db.query<Record<string, JsonObject[]>>(
      `SELECT ${expressions.join(", ")}`,
      parameters,
    );
    if (
      !row ||
      expressions.some((_, index) => !Array.isArray(row[`read_${index}`]))
    )
      throw new Error("Database returned an invalid batch read result");
    result.push(...expressions.map((_, index) => row[`read_${index}`]!));
  }
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
    "SELECT name FROM unnest($1::text[]) AS name WHERE to_regclass(quote_ident(name)) IS NOT NULL",
    [[...tables]],
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
    const [row] = await db.query<Record<string, boolean>>(
      `SELECT ${expressions.join(", ")}`,
      parameters,
    );
    if (
      !row ||
      expressions.some((_, index) => typeof row[`found_${index}`] !== "boolean")
    )
      throw new Error("Database returned an invalid existence result");
    results.push(...expressions.map((_, index) => row[`found_${index}`]!));
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
      filter =
        " FILTER (WHERE " +
        compiled.statement
          .split(" WHERE ")[1]!
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

/** Serialize a named business resource for the authoritative transaction. */
export async function lockData(db: WriteDB, key: string): Promise<void> {
  if (!key || typeof key !== "string")
    throw new Error("Data lock requires a nonempty key");
  if (db.lock) await db.lock(key);
  else
    await db.query(
      "SELECT $1::text FROM pg_advisory_xact_lock(hashtextextended($1, 0))",
      [key],
    );
}

function validateDataPattern(pattern: string): void {
  // Deliberately exclude PostgreSQL/JS-specific regex extensions. Application
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
    // PostgreSQL btrim without a character argument strips ASCII spaces only.
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
