// X17p (#555 R1): which captured tables a write can resolve a conflict on with REPLACE, so storage logs the
// change-log marker (`../capture/replace-marker.ts`) for them. Read once per schema generation.
//
// A write reaches: the table it names; every write of a trigger it fires (by event); and, through foreign key
// actions, each child table a parent delete or update rewrites (CASCADE deletes or updates the child, SET NULL
// and SET DEFAULT update it), whose own triggers fire in turn. A table is replaced on when a reached write is
// `REPLACE` / `INSERT OR REPLACE` / `UPDATE OR REPLACE`, or is an INSERT or UPDATE of a table whose DDL declares
// `ON CONFLICT REPLACE` (found on the lexer's tokens: comments inside the clause do not hide it).

import { writtenTable, type WriteOperation, type WrittenTable } from "./admission";
import { triggerWrites, type TriggerWrites } from "./foreign-keys";
import { isWord, tokenize, type Token } from "./lexer";

type SchemaRows = (sql: string) => Record<string, unknown>[];

interface ChildAction {
  readonly child: string;
  readonly onDelete: string;
  readonly onUpdate: string;
}

const lower = (name: string) => (name.includes(".") ? name.slice(name.indexOf(".") + 1) : name).toLowerCase();

/** Whether `ddl` holds an `ON CONFLICT REPLACE` clause, read on tokens. */
export function declaresReplace(ddl: string): boolean {
  let tokens: Token[];
  try {
    tokens = tokenize(ddl);
  } catch {
    // Unreadable DDL: assume it may replace (a marker only makes readers rebuild).
    return true;
  }
  for (let index = 0; index + 2 < tokens.length; index++) {
    if (isWord(tokens[index], "on") && isWord(tokens[index + 1], "conflict") && isWord(tokens[index + 2], "replace")) return true;
  }
  return false;
}

export class ReplaceModel {
  /** Captured tables (lowercase -> name): an entity capture insert trigger writing the change log. */
  private readonly captured = new Map<string, string>();
  /** Tables declared `ON CONFLICT REPLACE` (lowercase). */
  private readonly declared = new Set<string>();
  private readonly triggerSql = new Map<string, string[]>();
  private readonly triggers = new Map<string, TriggerWrites[]>();
  /** Foreign keys with a writing action, by parent table (lowercase). */
  private readonly actions = new Map<string, ChildAction[]>();

  constructor(rows: SchemaRows) {
    for (const row of rows("SELECT type, name, tbl_name, coalesce(sql, '') AS sql FROM sqlite_schema WHERE type IN ('table', 'trigger') AND substr(name, 1, 4) <> '_cf_'")) {
      const name = String(row.name), sql = String(row.sql);
      if (row.type === "table") {
        if (declaresReplace(sql)) this.declared.add(name.toLowerCase());
        continue;
      }
      const table = String(row.tbl_name);
      const capture = /^_gonvex_sync_(.*)_insert$/.exec(name);
      if (capture && capture[1]!.toLowerCase() === table.toLowerCase() && sql.includes("_gonvex_sync_changes")) this.captured.set(table.toLowerCase(), table);
      const list = this.triggerSql.get(table.toLowerCase()) ?? [];
      list.push(sql);
      this.triggerSql.set(table.toLowerCase(), list);
    }
    // The platform's own tables refuse pragma reads (see ForeignKeyModel).
    for (const row of rows(
      `SELECT m.name AS child, f."table" AS parent, f.on_update AS on_update, f.on_delete AS on_delete
       FROM sqlite_schema AS m JOIN pragma_foreign_key_list(m.name) AS f
       WHERE m.type = 'table' AND substr(m.name, 1, 4) <> '_cf_'`,
    )) {
      const action = { child: String(row.child), onDelete: String(row.on_delete ?? "NO ACTION").toUpperCase(), onUpdate: String(row.on_update ?? "NO ACTION").toUpperCase() };
      const parent = String(row.parent).toLowerCase();
      const list = this.actions.get(parent) ?? [];
      list.push(action);
      this.actions.set(parent, list);
    }
  }

  /** Captured tables a statement can resolve a REPLACE conflict on, directly or through what it fires. */
  targets(tokens: readonly Token[]): string[] {
    if (!this.captured.size) return [];
    const written = writtenTable(tokens);
    if (!written) return [];
    const found = new Set<string>();
    const seen = new Set<string>();
    const queue: WrittenTable[] = [written];
    while (queue.length) {
      const write = queue.pop()!;
      const bare = lower(write.table);
      const replaces = write.replace === true || (this.declared.has(bare) && (write.operations.has("insert") || write.operations.has("update")));
      const captured = this.captured.get(bare);
      if (replaces && captured !== undefined) found.add(captured);
      for (const operation of write.operations) {
        const key = `${bare}\u0000${operation}${replaces ? "\u0000r" : ""}`;
        if (seen.has(key)) continue;
        seen.add(key);
        for (const trigger of this.triggersOn(bare)) if (trigger.events.has(operation)) queue.push(...trigger.writes);
        for (const next of this.childWrites(bare, operation)) queue.push(next);
      }
    }
    return [...found].sort();
  }

  /** The child writes foreign key actions make for a parent `operation` (a REPLACE's delete included). */
  private childWrites(parent: string, operation: WriteOperation): WrittenTable[] {
    if (operation === "insert") return [];
    const out: WrittenTable[] = [];
    for (const action of this.actions.get(parent) ?? []) {
      const rule = operation === "delete" ? action.onDelete : action.onUpdate;
      if (rule === "CASCADE") out.push({ table: action.child, operations: new Set<WriteOperation>([operation]) });
      else if (rule === "SET NULL" || rule === "SET DEFAULT") out.push({ table: action.child, operations: new Set<WriteOperation>(["update"]) });
    }
    return out;
  }

  private triggersOn(table: string): TriggerWrites[] {
    let parsed = this.triggers.get(table);
    if (!parsed) {
      parsed = (this.triggerSql.get(table) ?? []).map(triggerWrites).filter((found): found is TriggerWrites => found !== undefined);
      this.triggers.set(table, parsed);
    }
    return parsed;
  }
}
