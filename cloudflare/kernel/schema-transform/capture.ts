// T5: change capture without custom SQL functions or hooks (0010, 0016 G0-4).
//
// The Rust runtime registers two functions (rust/crates/store/src/capture.rs):
//  - gonvex_record_value(record, key): the public JSON image of a logical
//    record, read through its view (record_views::image);
//  - gonvex_capture(table, rowid, op): [old, new, changed] for the row change
//    the pre-update hook saw.
// Both become plain SQL here: json_object row images typed exactly as
// cell_json types them (JSON text parsed, BOOLEAN as true/false, BLOB as hex,
// non-finite REAL as null), in column order (serde_json preserve_order).

import { closingParen, isNameToken, quoteIdent, quoteString, significant } from "./lexer.ts";

export type JsonKind = "json" | "boolean" | "integer" | "real" | "text";

/** rust/crates/store/src/rows.rs JsonKind::from_declared. */
export function jsonKind(declared: string): JsonKind {
  const d = declared.toUpperCase();
  if (d.includes("JSON")) return "json";
  if (d.includes("BOOL")) return "boolean";
  if (d.includes("INT")) return "integer";
  if (d.includes("REAL") || d.includes("DOUB") || d.includes("FLOA") || d.includes("NUMERIC") || d.includes("DECIMAL")) return "real";
  return "text";
}

/** A logical record view: one `_gonvex_record_views` row. */
export interface RecordView {
  name: string;
  storageTable: string;
  keyColumn: string;
  /** Parsed `schema` JSON: `{key, columns: {name: {type}}}` in column order. */
  schema: { key: string; columns: Record<string, { type: string }> };
}

/** A captured table's columns in declaration order (pragma_table_info). */
export interface TableShape {
  name: string;
  columns: Array<{ name: string; declared: string; pk: number }>;
}

const MAX_FUNCTION_ARGS = 127;

/**
 * The JSON value cell_json gives one SQLite value of `kind`, as a SQL
 * expression over `value`. `record` images read JSON columns through `json()`
 * exactly as record_views::image selects them (malformed JSON is an error);
 * captured `row` cells fall back to the text, as cell_json does.
 */
export function jsonValue(value: string, kind: JsonKind, mode: "record" | "row"): string {
  const blob = `WHEN 'blob' THEN lower(hex(${value}))`;
  const real = `WHEN 'real' THEN CASE WHEN abs(${value}) <= 1.7976931348623157e308 THEN ${value} END`;
  switch (kind) {
    case "json":
      if (mode === "record") return `json(${value})`;
      return `CASE typeof(${value}) WHEN 'text' THEN CASE WHEN json_valid(${value}) THEN json(${value}) ELSE ${value} END ${blob} ${real} ELSE ${value} END`;
    case "boolean":
      return `CASE typeof(${value}) WHEN 'integer' THEN json(iif(${value} <> 0, 'true', 'false')) WHEN 'real' THEN json(iif(${value} <> 0, 'true', 'false')) ${blob} ELSE ${value} END`;
    default:
      return `CASE typeof(${value}) ${blob} ${real} ELSE ${value} END`;
  }
}

/** json_object over (name, expression) pairs; past 63 pairs, json_insert adds the rest in order. */
export function jsonObject(pairs: Array<[string, string]>): string {
  const per = Math.floor(MAX_FUNCTION_ARGS / 2);
  const head = pairs.slice(0, per);
  let sql = `json_object(${head.map(([k, v]) => `${quoteString(k)}, ${v}`).join(", ")})`;
  for (let k = per; k < pairs.length; k += per - 1) {
    const chunk = pairs.slice(k, k + per - 1);
    sql = `json_insert(${sql}, ${chunk.map(([name, v]) => `${quoteString(`$.${JSON.stringify(name)}`)}, ${v}`).join(", ")})`;
  }
  return sql;
}

function checkKeyOrder(names: string[], what: string): void {
  // JSON.parse moves array-index keys first; such a column would change order.
  for (const n of names) if (/^(0|[1-9]\d*)$/.test(n) && Number(n) < 2 ** 32 - 1) throw new Error(`${what}: column ${n} is an array-index name; JSON key order would not survive`);
}

/** Parses a `_gonvex_record_views` row. */
export function recordView(row: { name: string; storage_table: string; key_column: string; schema: string }): RecordView {
  const schema = JSON.parse(row.schema) as RecordView["schema"];
  checkKeyOrder(Object.keys(schema.columns), row.name);
  return { name: row.name, storageTable: row.storage_table, keyColumn: row.key_column, schema };
}

/** The image of one record row selected from its view, as a json_object expression over columns of `alias`. */
export function recordImage(record: RecordView, alias = ""): string {
  const prefix = alias ? `${alias}.` : "";
  const pairs = Object.entries(record.schema.columns).map(([name, def]): [string, string] => [name, jsonValue(`${prefix}${quoteIdent(name)}`, jsonKind(def.type), "record")]);
  return jsonObject(pairs);
}

/**
 * Plain-SQL `gonvex_record_value(record, key)`. In a trigger it raises the
 * same errors as the Rust function for a non-text key and for duplicate keys;
 * at top level (the flush) RAISE is not available, and the key comes from
 * `_gonvex_record_pending.row_id`, which is TEXT NOT NULL.
 */
export function recordValueSql(record: RecordView, key: string, inTrigger: boolean): string {
  const view = quoteIdent(record.name);
  const where = `${quoteIdent(record.schema.key)} = ${key}`;
  if (!inTrigger) return `(SELECT ${recordImage(record)} FROM ${view} WHERE ${where})`;
  return (
    `(SELECT CASE WHEN typeof(${key}) <> 'text' THEN RAISE(ABORT, 'record snapshots require public text identities') ` +
    `WHEN count(*) = 0 THEN NULL WHEN count(*) > 1 THEN RAISE(ABORT, 'a logical record view returned duplicate keys') ` +
    `ELSE ${recordImage(record)} END FROM ${view} WHERE ${where})`
  );
}

export interface CaptureCatalog {
  records: ReadonlyMap<string, RecordView>;
  tables: ReadonlyMap<string, TableShape>;
}

export class CaptureError extends Error {}

/** Replaces every gonvex_record_value / gonvex_capture call in a trigger statement. */
export function rewriteCaptureCalls(sql: string, catalog: CaptureCatalog, inTrigger: boolean): string {
  const toks = significant(sql);
  let out = "";
  let last = 0;
  for (let i = 0; i < toks.length; i += 1) {
    const t = toks[i]!;
    const name = t.upper;
    if ((name !== "GONVEX_RECORD_VALUE" && name !== "GONVEX_CAPTURE") || toks[i + 1]?.type !== "(") continue;
    const open = i + 1;
    const close = closingParen(toks, open);
    const args: string[] = [];
    let from = open + 1;
    let depth = 0;
    for (let j = open + 1; j < close; j += 1) {
      const u = toks[j]!;
      if (u.type === "(") depth += 1;
      else if (u.type === ")") depth -= 1;
      else if (u.type === "," && depth === 0) {
        args.push(sql.slice(toks[from]!.start, toks[j - 1]!.end));
        from = j + 1;
      }
    }
    args.push(sql.slice(toks[from]!.start, toks[close - 1]!.end));
    const literal = /^'((?:[^']|'')*)'$/.exec(args[0]!.trim());
    if (!literal) throw new CaptureError(`${name.toLowerCase()}() with a non-literal first argument`);
    const target = literal[1]!.replace(/''/g, "'");
    let replacement: string;
    if (name === "GONVEX_RECORD_VALUE") {
      if (args.length !== 2) throw new CaptureError("gonvex_record_value() takes two arguments");
      const record = catalog.records.get(target);
      if (!record) throw new CaptureError(`gonvex_record_value(): record view ${target} is not in the catalog`);
      replacement = recordValueSql(record, args[1]!.trim(), inTrigger);
    } else {
      if (args.length !== 3) throw new CaptureError("gonvex_capture() takes three arguments");
      const shape = catalog.tables.get(target);
      if (!shape) throw new CaptureError(`gonvex_capture(): table ${target} is not in the catalog`);
      const op = args[2]!.trim();
      if (op !== "0" && op !== "1" && op !== "2") throw new CaptureError(`gonvex_capture(): operation ${op} is not a literal 0, 1 or 2`);
      const row = args[1]!.trim();
      const expected = op === "2" ? "OLD.rowid" : "NEW.rowid";
      if (row.toLowerCase() !== expected.toLowerCase()) throw new CaptureError(`gonvex_capture(): expected ${expected}, found ${row}`);
      replacement = captureValueSql(shape, Number(op) as 0 | 1 | 2);
    }
    out += sql.slice(last, t.start) + replacement;
    last = toks[close]!.end;
    i = close;
  }
  return out + sql.slice(last);
}

/** Row image of NEW or OLD for a captured table. */
export function rowImage(shape: TableShape, row: "NEW" | "OLD"): string {
  checkKeyOrder(shape.columns.map((c) => c.name), shape.name);
  return jsonObject(shape.columns.map((c): [string, string] => [c.name, jsonValue(`${row}.${quoteIdent(c.name)}`, jsonKind(c.declared), "row")]));
}

/**
 * Plain-SQL `gonvex_capture(table, rowid, op)`: `[old, new, changed]`, or NULL
 * for an update that changed no column. A column changed when its storage
 * class or its bytes differ (Rust compares Cell values).
 */
export function captureValueSql(shape: TableShape, op: 0 | 1 | 2): string {
  if (op === 0) return `json_array(NULL, ${rowImage(shape, "NEW")}, json('[]'))`;
  if (op === 2) return `json_array(${rowImage(shape, "OLD")}, NULL, json('[]'))`;
  const changed = (c: string) => `(OLD.${quoteIdent(c)} IS NOT NEW.${quoteIdent(c)} COLLATE BINARY OR typeof(OLD.${quoteIdent(c)}) IS NOT typeof(NEW.${quoteIdent(c)}))`;
  const any = shape.columns.map((c) => changed(c.name)).join(" OR ");
  const list = shape.columns.map((c) => `iif(${changed(c.name)}, ${quoteString(`,${JSON.stringify(c.name)}`)}, '')`).join(" || ");
  return `CASE WHEN ${any} THEN json_array(${rowImage(shape, "OLD")}, ${rowImage(shape, "NEW")}, json('[' || substr(${list}, 2) || ']')) END`;
}

/** Whether a statement calls one of the runtime's custom functions. */
export function callsCaptureFunctions(sql: string): boolean {
  const toks = significant(sql);
  return toks.some((t, i) => (t.upper === "GONVEX_RECORD_VALUE" || t.upper === "GONVEX_CAPTURE") && toks[i + 1]?.type === "(" && !isNameToken(toks[i - 1] ?? undefined));
}

/**
 * The commit-time flush (record_views FLUSH_SQL) without the custom function:
 * one INSERT per record name present in `_gonvex_record_pending`. Running them
 * in ascending `table_name` order (BINARY) reproduces Rust's event order, which
 * follows the pending table's primary key (table_name, row_id).
 */
export function recordFlushSql(record: RecordView): string {
  const name = quoteString(record.name);
  return `INSERT INTO _gonvex_sync_changes(table_name,row_id,operation,new_value)
WITH resolved AS MATERIALIZED (
 SELECT table_name,row_id,old_value,${recordValueSql(record, "p.row_id", false)} new_value FROM _gonvex_record_pending p WHERE p.table_name=${name} ORDER BY p.row_id
)
SELECT table_name,row_id,CASE WHEN old_value IS NULL THEN 'insert' WHEN new_value IS NULL THEN 'delete' ELSE 'update' END,
 json_array(json(old_value),json(new_value),json(CASE WHEN old_value IS NULL OR new_value IS NULL THEN '[]' ELSE
 (SELECT json_group_array(k.key) FROM json_each(new_value) k
  WHERE json_type(old_value,'$.'||json_quote(k.key)) IS NOT json_type(new_value,'$.'||json_quote(k.key))
     OR json_extract(old_value,'$.'||json_quote(k.key)) IS NOT json_extract(new_value,'$.'||json_quote(k.key))) END))
FROM resolved WHERE old_value IS NOT new_value`;
}

/** Record names with pending images, in flush order. */
export const RECORD_PENDING_NAMES_SQL = "SELECT DISTINCT table_name FROM _gonvex_record_pending ORDER BY table_name COLLATE BINARY";
export const RECORD_PENDING_CLEANUP_SQL = "DELETE FROM _gonvex_record_pending";

