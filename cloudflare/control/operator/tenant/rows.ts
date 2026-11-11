/**
 * Row JSON and column helpers ported from rust/crates/store/src/rows.rs
 * (`JsonKind`, `table_columns`, `json_object_expr`) and the filter builder of
 * rust/crates/runtime/src/operator_data.rs.
 */

export type JsonKind = "json" | "boolean" | "integer" | "real" | "text";

/** `JsonKind::from_declared`. */
export function kindOf(declared: string): JsonKind {
  const upper = declared.toUpperCase();
  if (upper.includes("JSON")) return "json";
  if (upper.includes("BOOL")) return "boolean";
  if (upper.includes("INT")) return "integer";
  if (["REAL", "DOUB", "FLOA", "NUMERIC", "DECIMAL"].some((part) => upper.includes(part))) return "real";
  return "text";
}

export interface ColumnInfo {
  name: string;
  declared: string;
  notNull: boolean;
  primaryKey: boolean;
}

/** `table_columns`: a record view's declared columns, else `pragma_table_info`, in declaration order. */
export function tableColumns(sql: SqlStorage, table: string): ColumnInfo[] {
  const records = sql.exec<{ found: number }>("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_views' AND type='table') AS found").one().found === 1;
  if (records) {
    const row = sql.exec<{ schema: string }>("SELECT schema FROM _gonvex_record_views WHERE name=?1", table).toArray()[0];
    if (row) {
      const schema = JSON.parse(row.schema) as { key?: unknown; columns?: Record<string, { type?: unknown; nullable?: unknown }> };
      if (typeof schema.key !== "string") throw new Error("record schema has no key");
      if (!schema.columns || typeof schema.columns !== "object") throw new Error("record schema has no columns");
      return Object.entries(schema.columns).map(([name, column]) => {
        if (typeof column?.type !== "string") throw new Error("record column has no type");
        return { name, declared: column.type, notNull: column.nullable === false, primaryKey: name === schema.key };
      });
    }
  }
  return sql
    .exec<{ name: string; type: string; notnull: number; pk: number }>('SELECT name, type, "notnull", pk FROM pragma_table_info(?1) ORDER BY cid', table)
    .toArray()
    .map((row) => ({ name: row.name, declared: String(row.type ?? ""), notNull: row.notnull !== 0, primaryKey: row.pk !== 0 }));
}

export const quoteIdentifier = (name: string): string => `"${name.replaceAll('"', '""')}"`;

/** operator_data.rs `valid_ident`. */
export const validIdent = (value: string): boolean => /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);

/** operator_data.rs `quote_ident`. */
export function quoteIdent(value: string): string {
  if (!validIdent(value)) throw new Error("invalid identifier");
  return quoteIdentifier(value);
}

/** operator_data.rs `internal_table`. */
export const internalTable = (name: string): boolean => name === "members" || name === "telemetry_events" || name.startsWith("gonvex_") || name.startsWith("_gonvex_");

const finiteReal = (q: string) => `typeof(${q}) = 'real' AND abs(${q}) <= 1.7976931348623157e308`;
const exactReal = (q: string) => `json(printf('%!.17g', ${q}))`;

/** rows.rs `json_value_expr`. */
export function jsonValueExpr(qualified: string, kind: JsonKind): string {
  switch (kind) {
    case "json":
      return `CASE WHEN ${qualified} IS NULL THEN NULL WHEN json_valid(${qualified}) THEN json(${qualified}) ELSE ${qualified} END`;
    case "boolean":
      return `CASE WHEN ${qualified} IS NULL THEN NULL WHEN ${qualified} THEN json('true') ELSE json('false') END`;
    case "integer":
    case "real":
      return `CASE WHEN ${finiteReal(qualified)} THEN ${exactReal(qualified)} ELSE ${qualified} END`;
    case "text":
      return `CASE WHEN typeof(${qualified}) = 'blob' THEN lower(hex(${qualified})) WHEN ${finiteReal(qualified)} THEN ${exactReal(qualified)} ELSE ${qualified} END`;
  }
}

/** rows.rs `json_object_expr`: one `json_object` per 60 columns, spliced. */
export function jsonObjectExpr(alias: string, columns: ReadonlyArray<readonly [string, JsonKind]>): string {
  const pairs = columns.map(([name, kind]) => {
    const qualified = alias === "" ? quoteIdentifier(name) : `${alias}.${quoteIdentifier(name)}`;
    return `'${name.replaceAll("'", "''")}', ${jsonValueExpr(qualified, kind)}`;
  });
  const chunks: string[] = [];
  for (let i = 0; i < pairs.length; i += 60) chunks.push(`json_object(${pairs.slice(i, i + 60).join(", ")})`);
  if (chunks.length === 0) return "json_object()";
  if (chunks.length === 1) return chunks[0]!;
  return `json('{' || ${chunks.map((chunk) => `substr(${chunk}, 2, length(${chunk}) - 2)`).join(" || ',' || ")} || '}')`;
}

/** operator_data.rs `text_expr`. */
export function textExpr(quoted: string, kind: JsonKind): string {
  return kind === "boolean" ? `(CASE WHEN ${quoted} IS NULL THEN NULL WHEN ${quoted} THEN 'true' ELSE 'false' END)` : `CAST(${quoted} AS TEXT)`;
}

/** operator_data.rs `json_input_expr`: the value of `name` in the JSON object bound as ?1. */
export function jsonInputExpr(name: string, kind: JsonKind): string {
  const path = `'$.${name}'`;
  switch (kind) {
    case "json":
      return `CASE json_type(?1,${path}) WHEN 'null' THEN NULL ELSE ?1 -> ${path} END`;
    case "boolean":
      return (
        `CASE json_type(?1,${path}) WHEN 'true' THEN 1 WHEN 'false' THEN 0 ` +
        `WHEN 'text' THEN CASE lower(json_extract(?1,${path})) ` +
        `WHEN 'true' THEN 1 WHEN 't' THEN 1 WHEN 'false' THEN 0 WHEN 'f' THEN 0 ` +
        `ELSE json_extract(?1,${path}) END ` +
        `ELSE json_extract(?1,${path}) END`
      );
    default:
      return `json_extract(?1,${path})`;
  }
}

export interface RowsFilter {
  column: string;
  operator: string;
  value: string;
  valueTo: string;
}

/** Parses the `filters` query value (serde `Vec<RowsFilter>`). */
export function parseFilters(raw: string): RowsFilter[] {
  if (raw.trim() === "") return [];
  const value = JSON.parse(raw) as unknown;
  if (!Array.isArray(value)) throw new Error("invalid type: expected a sequence");
  return value.map((item, index) => {
    if (!item || typeof item !== "object") throw new Error(`invalid type: expected struct RowsFilter at index ${index}`);
    const filter = item as Record<string, unknown>;
    if (typeof filter.column !== "string") throw new Error(`missing field \`column\` at index ${index}`);
    const text = (key: string) => {
      const v = filter[key];
      if (v === undefined || v === null) return "";
      if (typeof v !== "string") throw new Error(`invalid type: expected a string for \`${key}\``);
      return v;
    };
    return { column: filter.column, operator: text("operator"), value: text("value"), valueTo: text("valueTo") };
  });
}

/**
 * operator_data.rs `append_where`: appends the WHERE clause and returns its
 * bindings, numbered from `next`. Throws a message on an invalid filter.
 */
export function appendWhere(all: ReadonlyMap<string, JsonKind>, search: string, filters: readonly RowsFilter[], next: number): { sql: string; binds: unknown[] } {
  const binds: unknown[] = [];
  const bind = (value: unknown) => {
    binds.push(value);
    return `?${next + binds.length - 1}`;
  };
  let sql = "";
  let hasClause = false;
  const prefix = () => {
    sql += hasClause ? " AND " : " WHERE ";
    hasClause = true;
  };
  const LIKE = ",'') LIKE ";
  const ESCAPE = " ESCAPE '\\'";
  // BTreeMap iteration: columns in byte order.
  const ordered = [...all.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  if (search.trim() !== "" && ordered.length > 0) {
    prefix();
    sql += "(";
    // Rust binds the same pattern once per column; one numbered parameter is the
    // same value and keeps wide tables under the platform's 100-parameter limit.
    const pattern = bind(`%${search.trim()}%`);
    ordered.forEach(([column, kind], index) => {
      if (index > 0) sql += " OR ";
      sql += `COALESCE(${textExpr(quoteIdent(column), kind)}${LIKE}${pattern}${ESCAPE}`;
    });
    sql += ")";
  }
  for (const filter of filters) {
    const column = filter.column.trim();
    if (column === "") continue;
    const kind = all.get(column);
    if (kind === undefined || !validIdent(column)) throw new Error("invalid filter column");
    prefix();
    const quoted = quoteIdent(column);
    const text = textExpr(quoted, kind);
    switch (filter.operator) {
      case "empty":
        sql += `(${quoted} IS NULL OR ${text}='')`;
        break;
      case "notEmpty":
        sql += `(${quoted} IS NOT NULL AND ${text}<>'')`;
        break;
      case "equals":
        sql += `${text}=${bind(filter.value)}`;
        break;
      case "notEquals":
        sql += `(${quoted} IS NULL OR ${text}<>${bind(filter.value)})`;
        break;
      case "startsWith":
        sql += `COALESCE(${text}${LIKE}${bind(`${filter.value}%`)}${ESCAPE}`;
        break;
      case "endsWith":
        sql += `COALESCE(${text}${LIKE}${bind(`%${filter.value}`)}${ESCAPE}`;
        break;
      case "notContains":
        sql += `COALESCE(${text},'') NOT LIKE ${bind(`%${filter.value}%`)}${ESCAPE}`;
        break;
      case "lessThan":
      case "lessThanOrEqual":
      case "greaterThan":
      case "greaterThanOrEqual": {
        const op = filter.operator === "lessThan" ? "<" : filter.operator === "lessThanOrEqual" ? "<=" : filter.operator === "greaterThan" ? ">" : ">=";
        sql += `${text} ${op} ${bind(filter.value)}`;
        break;
      }
      case "inRange":
        sql += `(${quoted} IS NOT NULL`;
        if (filter.value.trim() !== "") sql += ` AND ${text} >= ${bind(filter.value)}`;
        if (filter.valueTo.trim() !== "") sql += ` AND ${text} <= ${bind(filter.valueTo)}`;
        sql += ")";
        break;
      case "oneOf": {
        let values: unknown;
        try {
          values = JSON.parse(filter.value);
        } catch {
          throw new Error("invalid oneOf filter");
        }
        if (!Array.isArray(values) || values.some((v) => typeof v !== "string")) throw new Error("invalid oneOf filter");
        if (values.length === 0) sql += "1";
        else sql += `COALESCE(${text},'') IN (${values.map((v) => bind(v)).join(",")})`;
        break;
      }
      default:
        sql += `COALESCE(${text}${LIKE}${bind(`%${filter.value}%`)}${ESCAPE}`;
    }
  }
  return { sql, binds };
}

/** replace_json_references: every JSON string equal to a source id. */
export function replaceJsonReferences(value: unknown, replacements: ReadonlyMap<string, string>): unknown {
  if (typeof value === "string") return replacements.get(value) ?? value;
  if (Array.isArray(value)) return value.map((item) => replaceJsonReferences(item, replacements));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, replaceJsonReferences(item, replacements)]));
  }
  return value;
}
