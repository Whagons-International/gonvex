// X17p (#555 R1): which captured tables a write can resolve a conflict on with REPLACE, so storage logs the
// change-log marker (`../capture/replace-marker.ts`) for them. Read once per schema generation.

import { writtenTable, type WrittenTable } from "./admission";
import { triggerWrites, type TriggerWrites } from "./foreign-keys";
import type { Token } from "./lexer";

type SchemaRows = (sql: string) => Record<string, unknown>[];

/** A column or table constraint `ON CONFLICT REPLACE` (any INSERT or UPDATE of the table may replace). */
const DDL_REPLACE = /\bON\s+CONFLICT\s+REPLACE\b/i;

export class ReplaceModel {
  /** Captured tables (lowercase -> name): an entity capture insert trigger writing the change log. */
  private readonly captured = new Map<string, string>();
  /** Tables declared `ON CONFLICT REPLACE` (lowercase). */
  private readonly declared = new Set<string>();
  private readonly triggerSql = new Map<string, string[]>();
  private readonly triggers = new Map<string, TriggerWrites[]>();

  constructor(rows: SchemaRows) {
    for (const row of rows("SELECT type, name, tbl_name, coalesce(sql, '') AS sql FROM sqlite_schema WHERE type IN ('table', 'trigger') AND substr(name, 1, 4) <> '_cf_'")) {
      const name = String(row.name), sql = String(row.sql);
      if (row.type === "table") {
        if (DDL_REPLACE.test(sql)) this.declared.add(name.toLowerCase());
        continue;
      }
      const table = String(row.tbl_name);
      const capture = /^_gonvex_sync_(.*)_insert$/.exec(name);
      if (capture && capture[1]!.toLowerCase() === table.toLowerCase() && sql.includes("_gonvex_sync_changes")) this.captured.set(table.toLowerCase(), table);
      const list = this.triggerSql.get(table.toLowerCase()) ?? [];
      list.push(sql);
      this.triggerSql.set(table.toLowerCase(), list);
    }
  }

  /** Whether any captured table exists (no change feed: never a marker). */
  get any(): boolean {
    return this.captured.size > 0;
  }

  /** Captured tables a statement can resolve a REPLACE conflict on, directly or through triggers it fires. */
  targets(tokens: readonly Token[]): string[] {
    if (!this.captured.size) return [];
    const written = writtenTable(tokens);
    if (!written) return [];
    const found = new Set<string>();
    const seen = new Set<string>();
    const queue: WrittenTable[] = [written];
    while (queue.length) {
      const write = queue.pop()!;
      const bare = (write.table.includes(".") ? write.table.slice(write.table.indexOf(".") + 1) : write.table).toLowerCase();
      const replaces = write.replace === true || (this.declared.has(bare) && (write.operations.has("insert") || write.operations.has("update")));
      const captured = this.captured.get(bare);
      if (replaces && captured !== undefined) found.add(captured);
      for (const operation of write.operations) {
        const key = `${bare}\u0000${operation}`;
        if (seen.has(key)) continue;
        seen.add(key);
        for (const trigger of this.triggersOn(bare)) if (trigger.events.has(operation)) queue.push(...trigger.writes);
      }
    }
    return [...found].sort();
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
