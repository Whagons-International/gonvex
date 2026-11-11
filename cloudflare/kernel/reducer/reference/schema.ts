// Catalog helpers reference_integrity.rs borrows from provision.rs
// (`application_tables`, `column_exists`, `table_exists`), rows.rs
// (`json_object_expr`, `table_columns`) and feed.rs
// (`any_column_changed_expr`), plus its staging triggers.

import { objects, quoteIdentifier, scalar, type CaptureSql } from "../../capture/sql.ts";
import { get, isObject, parseJson } from "./json.ts";

/** rows.rs `ColumnInfo`. */
export interface ColumnInfo {
  readonly name: string;
  readonly declared: string;
  readonly notNull: boolean;
  readonly primaryKey: boolean;
}

/** rows.rs `JsonKind`. */
export type JsonKind = "json" | "boolean" | "integer" | "real" | "text";

/** Rust's `to_ascii_uppercase`. */
export const asciiUpper = (text: string): string => text.replace(/[a-z]+/g, (letters) => letters.toUpperCase());

/** rows.rs `JsonKind::from_declared`. */
export function kindOf(declared: string): JsonKind {
  const d = asciiUpper(declared);
  if (d.includes("JSON")) return "json";
  if (d.includes("BOOL")) return "boolean";
  if (d.includes("INT")) return "integer";
  if (d.includes("REAL") || d.includes("DOUB") || d.includes("FLOA") || d.includes("NUMERIC") || d.includes("DECIMAL")) return "real";
  return "text";
}

/** rows.rs `DatabaseError::Decode` Display. */
export class DecodeError extends Error {
  override readonly name = "DecodeError";
  constructor(detail: string) {
    super(`value decode failed: ${detail}`);
  }
}

const truthy = (value: unknown) => Number(value ?? 0) !== 0;

/** provision.rs `table_exists`. */
export function tableExists(sql: CaptureSql, name: string): boolean {
  return truthy(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)", [name])));
}

/** provision.rs `column_exists`. */
export function columnExists(sql: CaptureSql, table: string, column: string): boolean {
  return truthy(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name=?2)", [table, column])));
}

/** reference_integrity.rs `primary_key`. */
export function primaryKey(sql: CaptureSql, table: string): string[] {
  return sql.run("SELECT name FROM pragma_table_info(?1) WHERE pk>0 ORDER BY pk", [table]).rows.map((row) => String(row[0]));
}

/**
 * reference_integrity.rs `unique_column`: the single-column primary key or a
 * non-partial single-column unique index.
 */
export function uniqueColumn(sql: CaptureSql, table: string, column: string): boolean {
  const key = primaryKey(sql, table);
  if (key.length === 1 && key[0] === column) return true;
  return truthy(
    scalar(
      sql.run(
        `SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1) list
           WHERE list."unique"=1 AND list.partial=0
             AND (SELECT count(*) FROM pragma_index_info(list.name))=1
             AND (SELECT name FROM pragma_index_info(list.name))=?2)`,
        [table, column],
      ),
    ),
  );
}

function recordViewsExist(sql: CaptureSql): boolean {
  return truthy(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_views' AND type='table')")));
}

/** A logical record's columns from its `_gonvex_record_views.schema` (rows.rs `table_columns`). */
function recordColumns(schemaText: string): ColumnInfo[] {
  let schema;
  try {
    schema = parseJson(schemaText);
  } catch (error) {
    throw new DecodeError(error instanceof Error ? error.message : String(error));
  }
  const key = get(schema, "key");
  if (typeof key !== "string") throw new DecodeError("record schema has no key");
  const columns = get(schema, "columns");
  if (!isObject(columns)) throw new DecodeError("record schema has no columns");
  return [...columns].map(([name, column]) => {
    const declared = get(column, "type");
    if (typeof declared !== "string") throw new DecodeError("record column has no type");
    return { name, declared, notNull: get(column, "nullable") === false, primaryKey: name === key };
  });
}

/** rows.rs `table_columns`: columns of `table` in declaration order (a logical record's from its schema). */
export function tableColumns(sql: CaptureSql, table: string): ColumnInfo[] {
  if (recordViewsExist(sql)) {
    const schema = scalar(sql.run("SELECT schema FROM _gonvex_record_views WHERE name=?1", [table]));
    if (schema !== undefined && schema !== null) return recordColumns(String(schema));
  }
  return sql
    .run('SELECT name, type, "notnull", pk FROM pragma_table_info(?1) ORDER BY cid', [table])
    .rows.map((row) => ({ name: String(row[0]), declared: String(row[1] ?? ""), notNull: truthy(row[2]), primaryKey: truthy(row[3]) }));
}

/** `table_columns` for several tables in two statements (check_coverage). */
export function tablesColumns(sql: CaptureSql, tables: readonly string[]): Map<string, ColumnInfo[]> {
  const out = new Map<string, ColumnInfo[]>(tables.map((table) => [table, []]));
  if (tables.length === 0) return out;
  const list = JSON.stringify(tables);
  const rows = objects(
    sql.run(
      `SELECT m.name AS tbl, p.name AS name, p.type AS declared, p."notnull" AS nn, p.pk AS pk
       FROM sqlite_schema AS m JOIN pragma_table_info(m.name) AS p
       WHERE m.type='table' AND m.name IN (SELECT value FROM json_each(?1))
       ORDER BY m.name, p.cid`,
      [list],
    ),
  );
  for (const row of rows) {
    out.get(String(row.tbl))?.push({ name: String(row.name), declared: String(row.declared ?? ""), notNull: truthy(row.nn), primaryKey: truthy(row.pk) });
  }
  if (recordViewsExist(sql)) {
    for (const row of sql.run("SELECT name, schema FROM _gonvex_record_views WHERE name IN (SELECT value FROM json_each(?1))", [list]).rows) {
      if (row[1] !== null && row[1] !== undefined) out.set(String(row[0]), recordColumns(String(row[1])));
    }
  }
  return out;
}

/**
 * provision.rs `application_tables`, over `sqlite_schema` like K03's
 * install.ts: `pragma_table_list` is too expensive on the platform, and Rust's
 * 7-term UNION ALL exceeds its 5-term compound SELECT limit. The platform's
 * `_cf_*` tables and local workerd's `__miniflare_*` are excluded too.
 * Without `_gonvex_table_options` (Rust requires it) no table is excluded by
 * capture class.
 */
export function applicationTables(sql: CaptureSql): string[] {
  const options = tableExists(sql, "_gonvex_table_options")
    ? "AND m.name NOT IN (SELECT name FROM _gonvex_table_options WHERE name IS NOT NULL AND capture NOT IN ('entity','owned'))"
    : "";
  return sql
    .run(
      `WITH virtual(name) AS MATERIALIZED (
         SELECT name FROM sqlite_schema WHERE type='table' AND sql LIKE 'CREATE VIRTUAL TABLE%')
       SELECT m.name FROM sqlite_schema m
       WHERE m.type='table'
         AND m.name NOT LIKE '\\_gonvex%' ESCAPE '\\'
         AND m.name NOT LIKE 'gonvex\\_%' ESCAPE '\\'
         AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
         AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\'
         AND m.name NOT LIKE '\\_\\_miniflare\\_%' ESCAPE '\\'
         AND NOT EXISTS (SELECT 1 FROM virtual v WHERE m.name IN (v.name, v.name||'_data', v.name||'_idx', v.name||'_content', v.name||'_docsize', v.name||'_config'))
         ${options}
       ORDER BY m.name`,
    )
    .rows.map((row) => String(row[0]));
}

function finiteReal(qualified: string): string {
  return `typeof(${qualified}) = 'real' AND abs(${qualified}) <= 1.7976931348623157e308`;
}

function exactReal(qualified: string): string {
  return `json(printf('%!.17g', ${qualified}))`;
}

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

/** rows.rs `json_object_expr`: one object per 60 columns, spliced (127-argument limit). */
export function jsonObjectExpr(alias: string, columns: readonly (readonly [string, JsonKind])[]): string {
  const PAIRS_PER_CALL = 60;
  const pairs = columns.map(([name, kind]) => {
    const qualified = alias === "" ? quoteIdentifier(name) : `${alias}.${quoteIdentifier(name)}`;
    return `'${name.replace(/'/g, "''")}', ${jsonValueExpr(qualified, kind)}`;
  });
  const chunks: string[] = [];
  for (let index = 0; index < pairs.length; index += PAIRS_PER_CALL) chunks.push(`json_object(${pairs.slice(index, index + PAIRS_PER_CALL).join(", ")})`);
  if (chunks.length === 0) return "json_object()";
  if (chunks.length === 1) return chunks[0]!;
  const members = chunks.map((chunk) => `substr(${chunk}, 2, length(${chunk}) - 2)`).join(" || ',' || ");
  return `json('{' || ${members} || '}')`;
}

/** feed.rs `any_column_changed_expr`. */
export function anyColumnChangedExpr(columns: readonly ColumnInfo[]): string {
  const COLUMNS_PER_GROUP = 100;
  if (columns.length === 0) return "0";
  const groups: string[] = [];
  for (let index = 0; index < columns.length; index += COLUMNS_PER_GROUP) {
    const terms = columns.slice(index, index + COLUMNS_PER_GROUP).map((column) => {
      const name = quoteIdentifier(column.name);
      return `OLD.${name} IS NOT NEW.${name}`;
    });
    groups.push(`(${terms.join(" OR ")})`);
  }
  return groups.join(" OR ");
}

export function stagingTriggerNames(table: string): [string, string, string] {
  return [`_gonvex_reference_${table}_insert`, `_gonvex_reference_${table}_update`, `_gonvex_reference_${table}_delete`];
}

/**
 * The three `CREATE TRIGGER` statements staging `table`'s row images, as
 * SQLite stores them in `sqlite_schema.sql` (reference_integrity.rs
 * `staging_triggers`, byte for byte).
 */
export function stagingTriggers(table: string, columns: readonly ColumnInfo[]): [string, string, string] {
  const quoted = quoteIdentifier(table);
  const literal = table.replace(/'/g, "''");
  const kinds = columns.map((column) => [column.name, kindOf(column.declared)] as const);
  const newImage = jsonObjectExpr("NEW", kinds);
  const oldImage = jsonObjectExpr("OLD", kinds);
  const anyChange = anyColumnChangedExpr(columns);
  const [insert, update, remove] = stagingTriggerNames(table).map(quoteIdentifier) as [string, string, string];
  return [
    `CREATE TRIGGER ${insert} AFTER INSERT ON ${quoted} BEGIN\n  INSERT INTO _gonvex_reference_pending(table_name,old_doc,new_doc) VALUES('${literal}',NULL,${newImage});\nEND`,
    `CREATE TRIGGER ${update} AFTER UPDATE ON ${quoted} WHEN ${anyChange} BEGIN\n  INSERT INTO _gonvex_reference_pending(table_name,old_doc,new_doc) VALUES('${literal}',${oldImage},${newImage});\nEND`,
    `CREATE TRIGGER ${remove} AFTER DELETE ON ${quoted} BEGIN\n  INSERT INTO _gonvex_reference_pending(table_name,old_doc,new_doc) VALUES('${literal}',${oldImage},NULL);\nEND`,
  ];
}

export function dropStagingTriggers(sql: CaptureSql, table: string): void {
  for (const name of stagingTriggerNames(table)) sql.run(`DROP TRIGGER IF EXISTS ${quoteIdentifier(name)}`);
}

/** (Re)installs staging triggers whose definition no longer matches the table's columns; unchanged triggers cause no DDL. */
export function installStagingTriggers(sql: CaptureSql, table: string): void {
  const statements = stagingTriggers(table, tableColumns(sql, table));
  const names = stagingTriggerNames(table);
  const installed = new Map(
    sql
      .run("SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=?1 AND name IN (?2,?3,?4)", [table, ...names])
      .rows.map((row) => [String(row[0]), row[1]]),
  );
  const current = names.every((name, index) => installed.get(name) === statements[index]);
  if (!current) {
    dropStagingTriggers(sql, table);
    for (const statement of statements) sql.run(statement);
  }
}
