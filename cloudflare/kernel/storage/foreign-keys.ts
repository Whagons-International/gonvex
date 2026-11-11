// The deferred-foreign-key check before commit (contract-changes 0016 G0-5).
//
// SQLite checks a DEFERRABLE INITIALLY DEFERRED foreign key only at COMMIT.
// A Durable Object commits its implicit transaction after the event, where a
// violation fails the request and resets the object (S2). So the storage
// layer checks, inside the invocation's transaction, every deferred foreign
// key that the invocation's writes could have broken, and fails the call with
// Rust's commit error instead.
//
// Only what was written is checked:
//   - a table that got an INSERT or UPDATE is checked as a child of its
//     deferred foreign keys;
//   - a table that got an UPDATE or DELETE is checked as the parent of every
//     deferred foreign key that references it;
//   - writes reach further than the statement's own table through triggers
//     and ON DELETE/UPDATE actions, so the written set is closed over the
//     schema's triggers (their body statements) and FK actions.
// With row ids for a table (change capture, K03, or the writer), the check
// reads only those rows. Without them it uses pragma_foreign_key_check on the
// table, restricted to the deferred foreign keys, which Durable Object SQL
// does not bill as rows read.
//
// Immediate foreign keys need no check: SQLite fails the statement itself.

import { mainVerbIndex, writtenTable, type WriteOperation } from "./admission";
import { headWord, isSymbol, isWord, tokenize, closeParen, type Token } from "./lexer";

export interface ForeignKey {
  /** pragma_foreign_key_list / pragma_foreign_key_check `id` / `fkid`. */
  readonly id: number;
  readonly child: string;
  readonly parent: string;
  readonly from: readonly string[];
  /** Parent columns; empty means the parent's primary key. */
  readonly to: readonly string[];
  readonly onUpdate: string;
  readonly onDelete: string;
  readonly deferred: boolean;
}

export interface SchemaReader {
  rows(sql: string, ...binds: (string | number | null)[]): Record<string, unknown>[];
}

const ACTIONS_THAT_WRITE = new Set(["CASCADE", "SET NULL", "SET DEFAULT"]);

/**
 * Declaration-order deferrability of each foreign key clause in a CREATE
 * TABLE statement. SQLite numbers foreign keys in reverse declaration order.
 */
export function deferredClauses(createSql: string): boolean[] {
  let tokens: Token[];
  try {
    tokens = tokenize(createSql);
  } catch {
    return [];
  }
  const out: boolean[] = [];
  for (let index = 0; index < tokens.length; index++) {
    if (!isWord(tokens[index], "references")) continue;
    let cursor = index + 2; // REFERENCES name
    if (isSymbol(tokens[cursor], "(")) cursor = closeParen(tokens, cursor) + 1;
    let deferred = false;
    // The clause runs until the next constraint or column: ON/MATCH/[NOT] DEFERRABLE/INITIALLY words.
    while (cursor > 0 && cursor < tokens.length) {
      const word = headWord(tokens, cursor);
      if (word === "on") { cursor += isWord(tokens[cursor + 2], "set") || isWord(tokens[cursor + 2], "no") ? 4 : 3; continue; }
      if (word === "match") { cursor += 2; continue; }
      if (word === "not" && isWord(tokens[cursor + 1], "deferrable")) {
        cursor += 2;
        if (isWord(tokens[cursor], "initially")) cursor += 2;
        deferred = false;
        continue;
      }
      if (word === "deferrable") {
        cursor++;
        deferred = isWord(tokens[cursor], "initially") && isWord(tokens[cursor + 1], "deferred");
        if (isWord(tokens[cursor], "initially")) cursor += 2;
        continue;
      }
      break;
    }
    out.push(deferred);
  }
  return out;
}

interface TriggerWrites {
  readonly events: ReadonlySet<WriteOperation>;
  readonly writes: readonly { table: string; operations: ReadonlySet<WriteOperation> }[];
}

/** The event and the body's write statements of one CREATE TRIGGER. */
export function triggerWrites(createSql: string): TriggerWrites | undefined {
  let tokens: Token[];
  try {
    tokens = tokenize(createSql);
  } catch {
    return undefined;
  }
  const begin = tokens.findIndex((found) => isWord(found, "begin"));
  const on = tokens.findIndex((found) => isWord(found, "on"));
  if (begin < 0 || on < 0) return undefined;
  const events = new Set<WriteOperation>();
  for (const found of tokens.slice(0, on)) {
    if (isWord(found, "insert")) events.add("insert");
    else if (isWord(found, "update")) events.add("update");
    else if (isWord(found, "delete")) events.add("delete");
  }
  const writes: { table: string; operations: ReadonlySet<WriteOperation> }[] = [];
  let statement: Token[] = [];
  let depth = 0;
  for (const found of tokens.slice(begin + 1)) {
    if (isSymbol(found, "(")) depth++;
    else if (isSymbol(found, ")")) depth--;
    if (depth === 0 && isSymbol(found, ";")) {
      const written = writtenTable(statement);
      if (written) writes.push(written);
      statement = [];
      continue;
    }
    statement.push(found);
  }
  return { events, writes };
}

/** Foreign keys, deferrability and trigger write paths, read once per schema generation. */
export class ForeignKeyModel {
  /** Deferred foreign keys by child table (lowercase). */
  readonly byChild = new Map<string, ForeignKey[]>();
  /** Deferred foreign keys by parent table (lowercase). */
  readonly byParent = new Map<string, ForeignKey[]>();
  /** Every foreign key with a writing ON action, by parent table (lowercase). */
  private readonly actionsByParent = new Map<string, ForeignKey[]>();
  /** Raw trigger SQL by table (lowercase), parsed on first use. */
  private readonly triggerSql = new Map<string, string[]>();
  private readonly triggers = new Map<string, TriggerWrites[]>();
  readonly deferredCount: number;

  constructor(reader: SchemaReader) {
    // The platform's own tables (`_cf_KV`, `_cf_METADATA`) refuse pragma reads with SQLITE_AUTH
    // ("not authorized"); they exist as soon as the object uses `storage.kv` and have no foreign keys.
    const rows = reader.rows(
      `SELECT m.name AS child, m.sql AS sql, f.id AS id, f.seq AS seq, f."table" AS parent, f."from" AS "from", f."to" AS "to", f.on_update AS on_update, f.on_delete AS on_delete
       FROM sqlite_schema AS m JOIN pragma_foreign_key_list(m.name) AS f
       WHERE m.type = 'table' AND substr(m.name, 1, 4) <> '_cf_' ORDER BY m.name, f.id, f.seq`,
    );
    const grouped = new Map<string, { sql: string; keys: Map<number, Record<string, unknown>[]> }>();
    for (const row of rows) {
      const child = String(row.child);
      const entry = grouped.get(child) ?? { sql: String(row.sql ?? ""), keys: new Map() };
      grouped.set(child, entry);
      const id = Number(row.id);
      const parts = entry.keys.get(id) ?? [];
      parts.push(row);
      entry.keys.set(id, parts);
    }
    let deferredCount = 0;
    for (const [child, { sql, keys }] of grouped) {
      const clauses = deferredClauses(sql);
      // Unknown deferrability (a clause count mismatch): check it, to be safe.
      const known = clauses.length === keys.size;
      for (const [id, parts] of keys) {
        const first = parts[0]!;
        const key: ForeignKey = {
          id, child, parent: String(first.parent),
          from: parts.map((part) => String(part.from)),
          to: parts.every((part) => part.to !== null && part.to !== undefined) ? parts.map((part) => String(part.to)) : [],
          onUpdate: String(first.on_update ?? "NO ACTION").toUpperCase(),
          onDelete: String(first.on_delete ?? "NO ACTION").toUpperCase(),
          deferred: known ? clauses[clauses.length - 1 - id]! : true,
        };
        if (ACTIONS_THAT_WRITE.has(key.onDelete) || ACTIONS_THAT_WRITE.has(key.onUpdate)) push(this.actionsByParent, key.parent, key);
        if (!key.deferred) continue;
        deferredCount++;
        push(this.byChild, child, key);
        push(this.byParent, key.parent, key);
      }
    }
    this.deferredCount = deferredCount;
    for (const row of reader.rows("SELECT tbl_name, sql FROM sqlite_schema WHERE type = 'trigger' AND sql IS NOT NULL")) {
      push(this.triggerSql, String(row.tbl_name), String(row.sql));
    }
  }

  private readonly columnTypes = new Map<string, Map<string, { type: string; pk: number }>>();
  private readonly targetedSql = new Map<ForeignKey, string | null>();

  private columnsOf(table: string, reader: CheckReader): Map<string, { type: string; pk: number }> {
    const key = table.toLowerCase();
    let columns = this.columnTypes.get(key);
    if (!columns) {
      columns = new Map();
      for (const row of reader.rows("SELECT name, type, pk FROM pragma_table_info(?1)", table).rows) {
        columns.set(String(row.name).toLowerCase(), { type: String(row.type ?? ""), pk: Number(row.pk) });
      }
      this.columnTypes.set(key, columns);
    }
    return columns;
  }

  /**
   * SQL that checks `key` for the child rows whose rowids are in `?1` (a
   * JSON array), or undefined when a row-level check would not compare like
   * SQLite: SQLite applies the parent column's affinity to the child value,
   * which a plain `=` only matches when both columns share an affinity.
   */
  targetedChildCheck(key: ForeignKey, reader: CheckReader): string | undefined {
    const cached = this.targetedSql.get(key);
    if (cached !== undefined) return cached ?? undefined;
    const parentColumns = this.columnsOf(key.parent, reader);
    const childColumns = this.columnsOf(key.child, reader);
    let to = key.to;
    if (!to.length) {
      to = [...parentColumns].filter(([, column]) => column.pk > 0).sort((a, b) => a[1].pk - b[1].pk).map(([name]) => name);
    }
    let sql: string | null = null;
    const comparable = to.length === key.from.length && key.from.every((column, index) => {
      const child = childColumns.get(column.toLowerCase());
      const parent = parentColumns.get(to[index]!.toLowerCase());
      return child !== undefined && parent !== undefined && affinity(child.type) === affinity(parent.type);
    });
    if (comparable) {
      const notNull = key.from.map((column) => `c.${quote(column)} IS NOT NULL`).join(" AND ");
      const match = key.from.map((column, index) => `p.${quote(to[index]!)} = c.${quote(column)}`).join(" AND ");
      sql = `SELECT ${sqlString(key.child)} AS "table", c.rowid AS rowid, ${sqlString(key.parent)} AS parent, ${key.id} AS fkid ` +
        `FROM ${quote(key.child)} AS c WHERE c.rowid IN (SELECT value FROM json_each(?1)) AND ${notNull} ` +
        `AND NOT EXISTS (SELECT 1 FROM ${quote(key.parent)} AS p WHERE ${match}) LIMIT 1`;
    }
    this.targetedSql.set(key, sql);
    return sql ?? undefined;
  }

  private triggersOn(table: string): TriggerWrites[] {
    const key = table.toLowerCase();
    let parsed = this.triggers.get(key);
    if (!parsed) {
      parsed = (this.triggerSql.get(key) ?? []).map(triggerWrites).filter((found): found is TriggerWrites => found !== undefined);
      this.triggers.set(key, parsed);
    }
    return parsed;
  }

  /**
   * Every table and operation a write of `operations` on `table` can reach
   * through triggers and foreign key actions, the table itself included.
   */
  closure(table: string, operations: ReadonlySet<WriteOperation>, into: Map<string, Set<WriteOperation>>): void {
    const queue: [string, WriteOperation][] = [...operations].map((operation) => [table, operation]);
    while (queue.length) {
      const [current, operation] = queue.pop()!;
      const key = current.toLowerCase();
      const seen = into.get(key) ?? new Set<WriteOperation>();
      if (seen.has(operation)) continue;
      seen.add(operation);
      into.set(key, seen);
      for (const trigger of this.triggersOn(current)) {
        if (!trigger.events.has(operation)) continue;
        for (const write of trigger.writes) for (const next of write.operations) queue.push([write.table, next]);
      }
      for (const key of this.actionsByParent.get(current.toLowerCase()) ?? []) {
        const action = operation === "delete" ? key.onDelete : operation === "update" ? key.onUpdate : undefined;
        if (action === "CASCADE") queue.push([key.child, operation]);
        else if (action === "SET NULL" || action === "SET DEFAULT") queue.push([key.child, "update"]);
      }
    }
  }
}

function push<K, V>(map: Map<K, V[]>, key: K, value: V): void {
  const lowered = (typeof key === "string" ? key.toLowerCase() : key) as K;
  const list = map.get(lowered);
  if (list) list.push(value);
  else map.set(lowered, [value]);
}

/**
 * What one invocation wrote. Statement-level writes are closed over the
 * schema's triggers and FK actions and checked per table. Row-level reports
 * (change capture, K03, or a writer that knows its row ids) name the exact
 * child rows, and a table only ever reported that way is checked by row id.
 */
export class WrittenSet {
  /** Lowercase table -> operations. */
  readonly tables = new Map<string, Set<WriteOperation>>();
  /** Lowercase table -> inserted or updated rowids, or null when only a table-level check is sound. */
  readonly rowids = new Map<string, Set<number> | null>();
  /** A write whose table is unknown: check every deferred foreign key. */
  everything = false;

  private add(table: string, operation: WriteOperation): void {
    const set = this.tables.get(table) ?? new Set<WriteOperation>();
    set.add(operation);
    this.tables.set(table, set);
  }

  /**
   * A module or kernel statement that wrote (rowsWritten > 0 or a write head).
   * `rowReported` (mixed mode, 0028 §2d) names the tables whose written rows
   * change capture reports before the check: those stay row-level.
   */
  noteStatement(tokens: readonly Token[], model: ForeignKeyModel | undefined, rowReported?: (table: string) => boolean): void {
    const written = writtenTable(tokens);
    if (!written) {
      this.everything = true;
      return;
    }
    this.noteTable(written.table, written.operations, model, rowReported);
  }

  /** A write to `table` whose rows are not known, closed over triggers and FK actions. */
  noteTable(table: string, operations: ReadonlySet<WriteOperation>, model: ForeignKeyModel | undefined, rowReported?: (table: string) => boolean): void {
    const bare = (table.includes(".") ? table.slice(table.indexOf(".") + 1) : table).toLowerCase();
    const reached = new Map<string, Set<WriteOperation>>();
    if (model) model.closure(bare, operations, reached);
    else reached.set(bare, new Set(operations));
    for (const [name, set] of reached) {
      for (const operation of set) this.add(name, operation);
      // Mixed mode: capture reports this table's rows (noteRows); keep it row-level.
      if (rowReported?.(name)) {
        if (!this.rowids.has(name)) this.rowids.set(name, new Set());
      } else this.rowids.set(name, null);
    }
  }

  /** Rows written to `table`, as change capture reports them (no closure: capture sees trigger writes too). */
  noteRows(table: string, operation: WriteOperation, rowids: readonly number[]): void {
    const key = table.toLowerCase();
    this.add(key, operation);
    if (operation === "delete") {
      if (!this.rowids.has(key)) this.rowids.set(key, new Set());
      return;
    }
    const known = this.rowids.get(key);
    if (known === null) return;
    const set = known ?? new Set<number>();
    for (const rowid of rowids) set.add(rowid);
    this.rowids.set(key, set);
  }
}

export interface ForeignKeyCheckResult {
  readonly violation: { readonly table: string; readonly rowid: number | null; readonly parent: string; readonly fkid: number } | undefined;
  readonly statements: number;
  readonly rowsRead: number;
  readonly ms: number;
  readonly tablesChecked: number;
  readonly targeted: number;
}

export interface CheckReader {
  rows(sql: string, ...binds: (string | number | null)[]): { rows: Record<string, unknown>[]; rowsRead: number };
}

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

/** Runs the deferred foreign key check for one invocation's writes. */
export function checkDeferredForeignKeys(model: ForeignKeyModel, written: WrittenSet, reader: CheckReader, now: () => number): ForeignKeyCheckResult {
  const started = now();
  let statements = 0, rowsRead = 0, tablesChecked = 0, targeted = 0;
  const run = (sql: string, ...binds: (string | number | null)[]) => {
    statements++;
    const result = reader.rows(sql, ...binds);
    rowsRead += result.rowsRead;
    return result.rows[0];
  };
  const finish = (row: Record<string, unknown> | undefined): ForeignKeyCheckResult => ({
    violation: row ? { table: String(row.table), rowid: row.rowid === null || row.rowid === undefined ? null : Number(row.rowid), parent: String(row.parent), fkid: Number(row.fkid) } : undefined,
    statements, rowsRead, ms: now() - started, tablesChecked, targeted,
  });
  if (model.deferredCount === 0) return finish(undefined);
  // child table -> deferred fk ids to check against all of its rows.
  const tableLevel = new Map<string, Set<number>>();
  const add = (child: string, id: number) => {
    const set = tableLevel.get(child) ?? new Set<number>();
    set.add(id);
    tableLevel.set(child, set);
  };
  if (written.everything) {
    for (const [child, keys] of model.byChild) for (const key of keys) add(child, key.id);
  } else {
    for (const [table, operations] of written.tables) {
      if (operations.has("insert") || operations.has("update")) {
        const rowids = written.rowids.get(table);
        for (const key of model.byChild.get(table) ?? []) {
          const sql = rowids ? model.targetedChildCheck(key, reader) : undefined;
          if (sql === undefined) { add(table, key.id); continue; }
          if (!rowids!.size) continue;
          targeted++;
          const row = run(sql, JSON.stringify([...rowids!]));
          if (row) return finish(row);
        }
      }
      if (operations.has("update") || operations.has("delete")) {
        for (const key of model.byParent.get(table) ?? []) add(key.child.toLowerCase(), key.id);
      }
    }
  }
  for (const [child, ids] of tableLevel) {
    tablesChecked++;
    const row = run(
      'SELECT "table" AS "table", rowid AS rowid, parent AS parent, fkid AS fkid FROM pragma_foreign_key_check(?1) WHERE fkid IN (SELECT value FROM json_each(?2)) LIMIT 1',
      child, JSON.stringify([...ids]),
    );
    if (row) return finish(row);
  }
  return finish(undefined);
}

/** SQLite column affinity of a declared type (datatype3 §3.1). */
export function affinity(declared: string): "integer" | "text" | "blob" | "real" | "numeric" {
  const upper = declared.toUpperCase();
  if (upper.includes("INT")) return "integer";
  if (upper.includes("CHAR") || upper.includes("CLOB") || upper.includes("TEXT")) return "text";
  if (upper.includes("BLOB") || upper.trim() === "") return "blob";
  if (upper.includes("REAL") || upper.includes("FLOA") || upper.includes("DOUB")) return "real";
  return "numeric";
}

const sqlString = (text: string) => `'${text.replaceAll("'", "''")}'`;
