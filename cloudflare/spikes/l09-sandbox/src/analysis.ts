// The `duckdb` binding of one sandbox, on its own Durable Object's SQLite
// (L09 candidate, note 0047 §4). It holds only module tables: registered rows and imported
// files. Its own bookkeeping is in key-value storage, which SQL cannot reach.
//
// It is a separate object from the workspace so that a long statement blocks
// only this object: the workspace keeps answering, its run deadline fires,
// and a statement that runs past the platform's CPU limit resets only this
// object (the open statement rolls back, committed tables stay).
import { answer, type Answer } from "../../../sandbox/src/answer";
import { DurableObject } from "cloudflare:workers";
import type { ImportedTable } from "../../../sandbox/src/backend";
import { ingestFile } from "./ingest/index";
import { MAX_TABLE_COLUMNS } from "./ingest/table";
import { prepareStatement, prepareStatements, quoteIdentifier, specialStatement } from "./sql";

const IMPORTS = "imports";
const WORKSPACE_FULL = "sandbox workspace byte limit exceeded";

export interface AnalysisStats {
  statements: number;
  rowsRead: number;
  rowsWritten: number;
  databaseSize: number;
}

export interface AnalysisCall {
  operation: "query" | "register";
  payload: Record<string, unknown>;
  maxRows: number;
  /** What the database may grow to: the workspace limit less the files. */
  budgetBytes: number;
}

type Cell = string | number | null;

/** `duck_to_json`, for the values SQLite can hold. */
function toJson(value: unknown): unknown {
  if (value instanceof ArrayBuffer) return `<${value.byteLength} bytes>`;
  if (ArrayBuffer.isView(value)) return `<${value.byteLength} bytes>`;
  return value ?? null;
}

/** `json_to_duck`: null, booleans, numbers and strings bind as themselves; anything else as JSON text. */
function toSql(value: unknown): Cell {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number" || typeof value === "string") return value;
  return JSON.stringify(value);
}

/** Rust's `identifier` (sandbox-worker main.rs:531-543). */
function identifier(value: string): string {
  if (value.length <= 63 && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) return value;
  throw new Error(`invalid DuckDB identifier ${JSON.stringify(value)}`);
}

type Kind = "BOOLEAN" | "BIGINT" | "DOUBLE" | "VARCHAR";

/** `infer_type` (main.rs:643-668). */
function inferType(rows: readonly Record<string, unknown>[], column: string): Kind {
  let kind: Kind | "NULL" = "NULL";
  for (const row of rows) {
    const value = row[column];
    if (value === null || value === undefined) continue;
    const current: Kind = typeof value === "boolean" ? "BOOLEAN"
      : typeof value === "number" ? (Number.isInteger(value) && Math.abs(value) < 9.223372036854776e18 ? "BIGINT" : "DOUBLE")
      : "VARCHAR";
    if (kind === "NULL") kind = current;
    else if (kind === current) continue;
    else if ((kind === "BIGINT" && current === "DOUBLE") || (kind === "DOUBLE" && current === "BIGINT")) kind = "DOUBLE";
    else kind = "VARCHAR";
  }
  return kind === "NULL" ? "VARCHAR" : kind;
}

/** `register_value` (main.rs:671-698). */
function registerValue(value: unknown, kind: Kind): Cell {
  if (value === null || value === undefined) return null;
  switch (kind) {
    case "BOOLEAN":
      if (typeof value !== "boolean") throw new Error("boolean column contains another type");
      return value ? 1 : 0;
    case "BIGINT":
      if (typeof value !== "number" || !Number.isInteger(value)) throw new Error("integer column contains another type");
      return value;
    case "DOUBLE":
      if (typeof value !== "number") throw new Error("number column contains another type");
      return value;
    default:
      return typeof value === "string" ? value : JSON.stringify(value);
  }
}

const numeric = (value: unknown): value is number => typeof value === "number";
const show = (value: unknown): string | null => (value === null || value === undefined ? null : typeof value === "number" ? String(value) : String(toJson(value)));

export class SandboxAnalysis extends DurableObject {
  /** Cursors of the current call, for the rows read and written it reports. */
  private cursors: SqlStorageCursor<Record<string, SqlStorageValue>>[] = [];

  /** `ctx.storage.sql`, counting every statement's rows. */
  private get sql(): SqlStorage {
    const storage = this.ctx.storage.sql;
    const cursors = this.cursors;
    return {
      exec: (query: string, ...bindings: unknown[]) => {
        const cursor = storage.exec(query, ...bindings);
        cursors.push(cursor);
        return cursor;
      },
      get databaseSize() {
        return storage.databaseSize;
      },
    } as SqlStorage;
  }

  /** Rows read and written since the last call to this, and the database size. */
  private settle(): AnalysisStats {
    const stats = { statements: this.cursors.length, rowsRead: 0, rowsWritten: 0, databaseSize: this.ctx.storage.sql.databaseSize };
    for (const cursor of this.cursors) {
      stats.rowsRead += cursor.rowsRead;
      stats.rowsWritten += cursor.rowsWritten;
    }
    this.cursors = [];
    return stats;
  }

  private async imports(): Promise<Map<string, Set<string>>> {
    const stored = (await this.ctx.storage.get<Record<string, string[]>>(IMPORTS)) ?? {};
    return new Map(Object.entries(stored).map(([alias, tables]) => [alias.toLowerCase(), new Set(tables.map((t) => t.toLowerCase()))]));
  }

  async ingest(request: { alias: string; filename: string; body: ReadableStream<Uint8Array>; maxRows: number }): Promise<Answer<{ tables: ImportedTable[]; databaseSize: number; stats: AnalysisStats }>> {
    return answer(() => this.ingestNow(request));
  }

  private async ingestNow(request: { alias: string; filename: string; body: ReadableStream<Uint8Array>; maxRows: number }): Promise<{ tables: ImportedTable[]; databaseSize: number; stats: AnalysisStats }> {
    this.cursors = [];
    let tables: ImportedTable[];
    try {
      tables = await ingestFile(this.sql, request.alias, request.filename, request.body);
    } catch (error) {
      // Rust builds the import beside its target and renames it only on success.
      const prefix = `${request.alias}.`;
      const created = this.sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND substr(name, 1, ?) = ?", prefix.length, prefix).toArray();
      for (const { name } of created) this.sql.exec(`DROP TABLE IF EXISTS ${quoteIdentifier(name)}`);
      throw error;
    }
    const stored = (await this.ctx.storage.get<Record<string, string[]>>(IMPORTS)) ?? {};
    stored[request.alias] = tables.map((table) => table.tableName);
    await this.ctx.storage.put(IMPORTS, stored);
    const stats = this.settle();
    return { tables, databaseSize: stats.databaseSize, stats };
  }

  /** The answer travels as JSON text. */
  async call(request: AnalysisCall): Promise<Answer<{ json: string; databaseSize: number; stats: AnalysisStats }>> {
    return answer(() => this.callNow(request));
  }

  private async callNow(request: AnalysisCall): Promise<{ json: string; databaseSize: number; stats: AnalysisStats }> {
    this.cursors = [];
    try {
      const value = request.operation === "query" ? await this.query(request) : this.register(request);
      const stats = this.settle();
      return { json: JSON.stringify(value), databaseSize: stats.databaseSize, stats };
    } finally {
      this.cursors = [];
    }
  }

  async destroy(): Promise<void> {
    await this.ctx.storage.deleteAll();
  }

  // ---- query ---------------------------------------------------------------------------

  /** `WorkerHost::query` (main.rs:386-426): at most `maxRows` rows, then `truncated`. */
  private async query(request: AnalysisCall): Promise<unknown> {
    const statement = request.payload["statement"];
    if (typeof statement !== "string" || statement === "") throw new Error("statement must be a non-empty string");
    const raw = request.payload["parameters"];
    const parameters = Array.isArray(raw) ? raw.map(toSql) : [];
    const imports = await this.imports();
    const special = specialStatement(statement);
    if (special?.kind === "showTables") {
      const names = this.sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY name").toArray();
      return this.answer(["name"], names.map((row) => [row.name]), request.maxRows);
    }
    if (special) {
      const source = prepareStatement(special.source, imports);
      return special.kind === "summarize" ? this.summarize(source, parameters, request.maxRows) : this.describe(source, parameters, request.maxRows);
    }
    // duckdb-rs `prepare` runs every statement before the last without
    // parameters, then prepares the last one (inner_connection.rs:98-118).
    const statements = prepareStatements(statement, imports);
    const last = statements.pop() ?? "SELECT NULL WHERE 0";
    return this.ctx.storage.transactionSync(() => {
      for (const earlier of statements) this.sql.exec(earlier).toArray();
      const cursor = this.sql.exec(last, ...parameters);
      const columns = cursor.columnNames;
      const rows: unknown[][] = [];
      let truncated = false;
      for (const row of cursor.raw()) {
        if (rows.length >= request.maxRows) {
          truncated = true;
          break;
        }
        rows.push(row);
      }
      if (this.sql.databaseSize > request.budgetBytes) throw new Error(WORKSPACE_FULL);
      return this.answer(columns, rows, request.maxRows, truncated);
    });
  }

  private answer(columns: string[], rows: unknown[][], maxRows: number, truncated = false): unknown {
    const kept = rows.slice(0, maxRows);
    return {
      columns,
      rowCount: kept.length,
      rows: kept.map((row) => {
        const record: Record<string, unknown> = {};
        columns.forEach((name, index) => { record[name] = toJson(row[index]); });
        return record;
      }),
      truncated: truncated || rows.length > maxRows,
    };
  }

  /** Column names and SQLite storage classes of a SELECT. */
  private profile(source: string, parameters: Cell[]): { columns: string[]; types: string[] } {
    const cursor = this.sql.exec(`SELECT * FROM (${source}) LIMIT 1000`, ...parameters);
    const columns = cursor.columnNames;
    const seen = columns.map(() => new Set<string>());
    for (const row of cursor.raw()) {
      row.forEach((value, index) => {
        if (value === null) return;
        seen[index]!.add(typeof value === "number" ? (Number.isInteger(value) ? "integer" : "real") : typeof value === "string" ? "text" : "blob");
      });
    }
    const types = seen.map((kinds) => {
      if (kinds.has("text")) return "VARCHAR";
      if (kinds.has("blob")) return "BLOB";
      if (kinds.has("real")) return "DOUBLE";
      if (kinds.has("integer")) return "BIGINT";
      return "VARCHAR";
    });
    return { columns, types };
  }

  /** DuckDB's `DESCRIBE`: one row per column. */
  private describe(source: string, parameters: Cell[], maxRows: number): unknown {
    const { columns, types } = this.profile(source, parameters);
    const names = ["column_name", "column_type", "null", "key", "default", "extra"];
    return this.answer(names, columns.map((name, i) => [name, types[i], "YES", null, null, null]), maxRows);
  }

  /**
   * DuckDB's `SUMMARIZE` (column_name, column_type, min, max, approx_unique,
   * avg, std, q25, q50, q75, count, null_percentage), computed exactly.
   */
  private summarize(source: string, parameters: Cell[], maxRows: number): unknown {
    const { columns, types } = this.profile(source, parameters);
    const names = ["column_name", "column_type", "min", "max", "approx_unique", "avg", "std", "q25", "q50", "q75", "count", "null_percentage"];
    const rows: unknown[][] = [];
    // Columns by position (names can repeat): the first member of the UNION names them column1..N.
    const base = `(SELECT * FROM (VALUES(${columns.map(() => "NULL").join(", ")})) WHERE 0 UNION ALL SELECT * FROM (${source})) AS __t`;
    for (let i = 0; i < columns.length && rows.length < maxRows; i++) {
      const c = `column${i + 1}`;
      const stats = this.sql.exec(
        `SELECT min(${c}) AS lo, max(${c}) AS hi, count(DISTINCT ${c}) AS uniq, count(${c}) AS present, count(*) AS total,
                avg(CASE WHEN typeof(${c}) IN ('integer','real') THEN ${c} END) AS mean,
                sum(typeof(${c}) IN ('integer','real')) AS numbers
         FROM ${base}`,
        ...parameters,
      ).one();
      const numbersOnly = numeric(stats["numbers"]) && stats["numbers"] === stats["present"] && (stats["numbers"] as number) > 0;
      const quantile = (q: number): string | null => {
        if (!numbersOnly) return null;
        const n = stats["present"] as number;
        const offset = Math.floor(q * (n - 1));
        const row = this.sql.exec(`SELECT ${c} AS v FROM ${base} WHERE ${c} IS NOT NULL ORDER BY ${c} LIMIT 1 OFFSET ${offset}`, ...parameters).toArray()[0];
        return show(row?.["v"]);
      };
      const present = stats["present"] as number;
      const total = stats["total"] as number;
      let std: string | null = null;
      if (numbersOnly && present > 1) {
        // Two passes, as numerically stable as DuckDB's stddev_samp.
        const mean = stats["mean"] as number;
        const squares = this.sql.exec(`SELECT sum((${c} - (${String(mean)})) * (${c} - (${String(mean)}))) AS ss FROM ${base} WHERE ${c} IS NOT NULL`, ...parameters).one()["ss"] as number;
        std = String(Math.sqrt(squares / (present - 1)));
      }
      rows.push([
        columns[i],
        types[i],
        show(stats["lo"]),
        show(stats["hi"]),
        stats["uniq"],
        numbersOnly ? show(stats["mean"]) : null,
        std,
        quantile(0.25),
        quantile(0.5),
        quantile(0.75),
        total,
        total === 0 ? "0.00" : ((100 * (total - present)) / total).toFixed(2),
      ]);
    }
    return this.answer(names, rows, maxRows);
  }

  // ---- register ------------------------------------------------------------------------

  /** `WorkerHost::register` (main.rs:429-499). */
  private register(request: AnalysisCall): unknown {
    const rawName = request.payload["name"];
    if (typeof rawName !== "string" || rawName === "") throw new Error("name must be a non-empty string");
    const rows = request.payload["rows"];
    if (!Array.isArray(rows)) throw new Error("rows must be an array");
    const name = identifier(rawName);
    if (rows.length > 100_000) throw new Error("DuckDB register is limited to 100000 rows");
    const objects = rows.map((row) => {
      if (row === null || typeof row !== "object" || Array.isArray(row)) throw new Error("every registered row must be an object");
      return row as Record<string, unknown>;
    });
    const columns = [...new Set(objects.flatMap((row) => Object.keys(row)))].sort();
    if (columns.length === 0) throw new Error("DuckDB register requires at least one column");
    for (const column of columns) identifier(column);
    if (columns.length > MAX_TABLE_COLUMNS) throw new Error(`DuckDB register supports at most ${MAX_TABLE_COLUMNS} columns on this runtime`);
    const kinds = columns.map((column) => inferType(objects, column));
    const table = quoteIdentifier(name);
    // Rust drops and creates the table before its insert transaction.
    this.sql.exec(`DROP TABLE IF EXISTS ${table}`);
    this.sql.exec(`CREATE TABLE ${table} (${columns.map((column, i) => `${quoteIdentifier(column)} ${kinds[i]}`).join(", ")})`);
    const select = columns.map((_, i) => `value->>${i}`).join(", ");
    this.ctx.storage.transactionSync(() => {
      let batch: string[] = [];
      let bytes = 0;
      const flush = () => {
        if (batch.length === 0) return;
        this.sql.exec(`INSERT INTO ${table} SELECT ${select} FROM json_each(?)`, `[${batch.join(",")}]`);
        batch = [];
        bytes = 0;
      };
      for (const row of objects) {
        const json = JSON.stringify(columns.map((column, i) => registerValue(row[column], kinds[i]!)));
        batch.push(json);
        bytes += json.length + 1;
        if (bytes >= 512 * 1024) flush();
      }
      flush();
      if (this.sql.databaseSize > request.budgetBytes) throw new Error(WORKSPACE_FULL);
    });
    return { table: name, rows: rows.length, columns };
  }
}

