// X17p (#555, lead decision round 3): when storage logs the REPLACE marker (`../capture/replace-marker.ts`).
// Conservative by construction: no model of where a REPLACE reaches.
//
// - Schema hazard H, read once per catalog generation: a change feed exists (an entity capture trigger), and a
//   table declares `ON CONFLICT REPLACE` (column or table constraint) or a trigger body uses REPLACE conflict
//   resolution (`REPLACE INTO`, `OR REPLACE`). Found on the lexer's tokens: comments do not hide them, quoted
//   identifiers and strings do not fake them; unreadable SQL counts as a hazard.
// - Statement hazard S: the statement's own conflict resolution is REPLACE (`writtenTable(...).replace`).
// - Marker: S, or H and the statement writes a table.

import { writtenTable } from "./admission";
import { isWord, tokenize, type Token } from "./lexer";

type SchemaRows = (sql: string) => Record<string, unknown>[];

/** Whether `ddl` holds an `ON CONFLICT REPLACE` clause, read on tokens (unreadable: yes). */
export function declaresReplace(ddl: string): boolean {
  return hasSequence(ddl, (tokens, index) => isWord(tokens[index], "on") && isWord(tokens[index + 1], "conflict") && isWord(tokens[index + 2], "replace"));
}

/** Whether a trigger body uses REPLACE conflict resolution (`REPLACE INTO`, `OR REPLACE`), read on tokens (unreadable: yes). */
export function triggerReplaces(sql: string): boolean {
  return hasSequence(sql, (tokens, index) =>
    (isWord(tokens[index], "or") && isWord(tokens[index + 1], "replace")) || (isWord(tokens[index], "replace") && isWord(tokens[index + 1], "into")));
}

function hasSequence(sql: string, at: (tokens: Token[], index: number) => boolean): boolean {
  let tokens: Token[];
  try {
    tokens = tokenize(sql);
  } catch {
    return true;
  }
  for (let index = 0; index < tokens.length; index++) if (at(tokens, index)) return true;
  return false;
}

export class ReplaceModel {
  /** Schema hazard H (false without a change feed: nothing to mark). */
  readonly hazard: boolean;

  constructor(rows: SchemaRows) {
    let feed = false, hazard = false;
    for (const row of rows("SELECT type, name, coalesce(sql, '') AS sql FROM sqlite_schema WHERE type IN ('table', 'trigger') AND substr(name, 1, 4) <> '_cf_'")) {
      const name = String(row.name), sql = String(row.sql);
      if (row.type === "table") {
        if (!hazard && declaresReplace(sql)) hazard = true;
        continue;
      }
      if (/^_gonvex_sync_.*_insert$/.test(name) && sql.includes("_gonvex_sync_changes")) {
        feed = true;
        continue;
      }
      if (!hazard && triggerReplaces(sql)) hazard = true;
    }
    this.feed = feed;
    this.hazard = feed && hazard;
  }

  private readonly feed: boolean;

  /** Whether a write statement must log the marker. */
  marks(tokens: readonly Token[]): boolean {
    if (!this.feed) return false;
    const written = writtenTable(tokens);
    if (!written) return false;
    return this.hazard || written.replace === true;
  }
}
