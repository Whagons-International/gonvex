// Rows of text into one SQLite table, the way DuckDB's `read_csv_auto(header=true)`
// reads them for Rust's `data_ingest` (RT/data_ingest.rs): the first row names
// the columns, a sample decides each column's type, empty fields are NULL.
//
// Types: BOOLEAN (true/false), BIGINT, DOUBLE, else VARCHAR. SQLite's column
// affinity converts the text of a BIGINT or DOUBLE column, and keeps a value
// that does not fit as text instead of failing the import. DATE and TIMESTAMP
// columns stay ISO text: Rust's DuckDB answered them as `Date32(19723)`
// (note 0047 §4, the L09 candidate). BOOLEAN values are stored and answered as 1/0.
import type { ImportedTable } from "../../../../sandbox/src/backend";
import { quoteIdentifier } from "../sql";

/** Durable Object SQLite's limit on columns per table (Rust allowed 200 per sheet). */
export const MAX_TABLE_COLUMNS = 100;
/** Rows sampled for type detection (DuckDB used 100,000; this bounds memory). */
const SAMPLE_ROWS = 20_000;
/** JSON bytes per insert statement's single bound value (the platform allows 2 MB). */
const BATCH_BYTES = 512 * 1024;

type Kind = "BOOLEAN" | "BIGINT" | "DOUBLE" | "VARCHAR";

const INTEGER = /^[+-]?\d+$/;
const DECIMAL = /^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/;
const BOOLEAN = /^(?:true|false)$/i;

function kindOf(value: string): Kind {
  if (BOOLEAN.test(value)) return "BOOLEAN";
  if (INTEGER.test(value)) {
    const n = Number(value);
    return Math.abs(n) <= 9.223372036854776e18 ? "BIGINT" : "DOUBLE";
  }
  if (DECIMAL.test(value)) return "DOUBLE";
  return "VARCHAR";
}

function widen(current: Kind | null, next: Kind): Kind {
  if (current === null || current === next) return next;
  if ((current === "BIGINT" && next === "DOUBLE") || (current === "DOUBLE" && next === "BIGINT")) return "DOUBLE";
  return "VARCHAR";
}

/** DuckDB's names for a header row: `column{i}` for an empty name, `_1`, `_2` for repeats. */
export function headerNames(header: readonly (string | null)[]): string[] {
  const width = String(Math.max(0, header.length - 1)).length;
  const seen = new Map<string, number>();
  const used = new Set<string>();
  return header.map((raw, index) => {
    let name = raw === null || raw === "" ? `column${String(index).padStart(width, "0")}` : raw;
    const key = name.toLowerCase();
    if (used.has(key)) {
      let n = seen.get(key) ?? 0;
      let candidate: string;
      do { n++; candidate = `${name}_${n}`; } while (used.has(candidate.toLowerCase()));
      seen.set(key, n);
      name = candidate;
    }
    used.add(name.toLowerCase());
    return name;
  });
}

export class TableSink {
  private columns: string[] | null = null;
  private kinds: Kind[] = [];
  private sample: (string | null)[][] = [];
  private created = false;
  private batch: string[] = [];
  private batchBytes = 0;
  private rows = 0;

  constructor(
    private readonly sql: SqlStorage,
    /** The stored table name (`import_1.data`). */
    private readonly table: string,
    /** The name the result reports (`data`). */
    private readonly reported: string,
    /** Rust's error for a table that is too wide, worded for the source. */
    private readonly tooWide: (columns: number) => string,
  ) {}

  push(row: (string | null)[]): void {
    if (this.columns === null) {
      if (row.length > MAX_TABLE_COLUMNS) throw new Error(this.tooWide(row.length));
      this.columns = headerNames(row);
      return;
    }
    if (!this.created) {
      this.sample.push(row);
      if (this.sample.length >= SAMPLE_ROWS) this.create();
      return;
    }
    this.add(row);
  }

  private create(): void {
    const columns = this.columns ?? [];
    const kinds: (Kind | null)[] = columns.map(() => null);
    for (const row of this.sample) {
      for (let c = 0; c < columns.length; c++) {
        const value = row[c];
        if (value !== null && value !== undefined && value !== "") kinds[c] = widen(kinds[c] ?? null, kindOf(value));
      }
    }
    this.kinds = kinds.map((kind) => kind ?? "VARCHAR");
    const definition = columns.map((name, c) => `${quoteIdentifier(name)} ${this.kinds[c]}`).join(", ");
    this.sql.exec(`CREATE TABLE ${quoteIdentifier(this.table)} (${definition || '"column0" VARCHAR'})`);
    this.created = true;
    const sample = this.sample;
    this.sample = [];
    for (const row of sample) this.add(row);
  }

  private add(row: (string | null)[]): void {
    const width = this.columns?.length ?? 0;
    const values: (string | number | null)[] = new Array(width);
    for (let c = 0; c < width; c++) {
      const value = row[c];
      if (value === null || value === undefined || value === "") values[c] = null;
      else if (this.kinds[c] === "BOOLEAN" && BOOLEAN.test(value)) values[c] = value.toLowerCase() === "true" ? 1 : 0;
      else values[c] = value;
    }
    const json = JSON.stringify(values);
    this.batch.push(json);
    this.batchBytes += json.length + 1;
    this.rows++;
    if (this.batchBytes >= BATCH_BYTES) this.flush();
  }

  private flush(): void {
    if (this.batch.length === 0) return;
    const width = this.columns?.length ?? 0;
    const select = Array.from({ length: Math.max(width, 1) }, (_, c) => `value->>${c}`).join(", ");
    this.sql.exec(`INSERT INTO ${quoteIdentifier(this.table)} SELECT ${select} FROM json_each(?)`, `[${this.batch.join(",")}]`);
    this.batch = [];
    this.batchBytes = 0;
  }

  /** The table as `DESCRIBE` and `count(*)` report it. Null when there was no header row. */
  finish(): ImportedTable | null {
    if (this.columns === null) return null;
    if (!this.created) this.create();
    this.flush();
    return { tableName: this.reported, rowCount: this.rows, columns: [...this.columns] };
  }
}
