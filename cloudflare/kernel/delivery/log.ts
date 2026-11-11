// Committed transactions from the tenant's change log, with provenance
// (change_feed.rs `CHANGE_COLUMNS`, `log_change_from_row`; raw_log.rs
// `read_changes`, `collapse`). Read in pages; a transaction is never split.

import type { JsonValue } from "../visibility/json.ts";
import type { Provenance } from "./frames.ts";

export interface LogChange {
  readonly revision: number;
  readonly ordinal: number;
  readonly originCommandId: string;
  readonly table: string;
  readonly rowId: string;
  readonly operation: string;
  readonly changedColumns: readonly string[];
  readonly oldValue: JsonValue;
  readonly newValue: JsonValue;
  readonly provenance: Provenance;
}

export interface FeedTransaction {
  readonly kind: "transaction";
  readonly epoch: string;
  readonly revision: number;
  readonly changes: readonly LogChange[];
}

export interface FeedReset {
  readonly kind: "reset";
  readonly reason: string;
}

export type FeedEvent = FeedTransaction | FeedReset;

export interface Clock {
  readonly epoch: string;
  readonly revision: number;
  readonly retained: number;
}

/** Synchronous SQL with positional `?` parameters (Durable Object `sql.exec`). */
export interface LogSql {
  exec(sql: string, ...bindings: unknown[]): { toArray(): Record<string, SqlStorageValue>[] };
}

export function readClock(sql: LogSql): Clock {
  const row = sql.exec("SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = 1").toArray()[0];
  if (!row) throw new Error("no rows returned by a query that expected to return at least one row");
  return { epoch: String(row.epoch), revision: Math.max(0, Number(row.revision)), retained: Math.max(0, Number(row.retained_revision)) };
}

const COLUMNS = `SELECT changes.revision AS revision, changes.ordinal AS ordinal, COALESCE(changes.command_id, '') AS command_id,
  changes.table_name AS table_name, changes.row_id AS row_id, changes.operation AS operation,
  COALESCE(changes.changed_columns, '[]') AS changed_columns, changes.old_value AS old_value, changes.new_value AS new_value,
  COALESCE(tx.root_command_id, COALESCE(changes.command_id, '')) AS root_command_id,
  COALESCE(tx.root_invocation_channel, tx.invocation_channel, 'system') AS root_invocation_channel,
  COALESCE(tx.invocation_channel, 'system') AS invocation_channel,
  tx.actor_account_id AS actor_account_id, tx.actor_member_id AS actor_member_id,
  tx.on_behalf_of_member_id AS on_behalf_of_member_id, tx.agent_execution_id AS agent_execution_id
FROM _gonvex_sync_changes changes LEFT JOIN _gonvex_sync_transactions tx ON tx.revision = changes.revision`;

/** Change rows per statement. */
export const LOG_PAGE_ROWS = 1000;

const json = (cell: SqlStorageValue | undefined): JsonValue => (cell === null || cell === undefined ? null : (JSON.parse(String(cell)) as JsonValue));
const optional = (cell: SqlStorageValue | undefined): string | null => (cell === null || cell === undefined ? null : String(cell));

function change(row: Record<string, SqlStorageValue>): LogChange {
  const columns = json(row.changed_columns);
  return {
    revision: Number(row.revision),
    ordinal: Number(row.ordinal ?? 0),
    originCommandId: String(row.command_id ?? ""),
    table: String(row.table_name),
    rowId: String(row.row_id),
    operation: String(row.operation),
    changedColumns: Array.isArray(columns) ? columns.filter((c): c is string => typeof c === "string") : [],
    oldValue: json(row.old_value),
    newValue: json(row.new_value),
    provenance: {
      rootCommandId: String(row.root_command_id ?? ""),
      rootChannel: String(row.root_invocation_channel ?? "system"),
      channel: String(row.invocation_channel ?? "system"),
      actorAccountId: optional(row.actor_account_id),
      actorMemberId: optional(row.actor_member_id),
      onBehalfOfMemberId: optional(row.on_behalf_of_member_id),
      agentExecutionId: optional(row.agent_execution_id),
    },
  };
}

/**
 * The change rows of `(after, through]` in revision and ordinal order, in
 * pages; `tables` restricts them (resume). `visit` gets each row once.
 */
export function readChangeRows(sql: LogSql, after: number, through: number, tables: readonly string[] | undefined, visit: (change: LogChange) => void): number {
  if (after >= through) return 0;
  let cursor: [number, number] = [after, Number.MAX_SAFE_INTEGER];
  let rows = 0;
  const tableFilter = tables ? ` AND changes.table_name IN (${tables.map(() => "?").join(", ")})` : "";
  for (;;) {
    const page = sql.exec(
      `${COLUMNS} WHERE changes.revision IS NOT NULL AND changes.revision <= ?${tableFilter}
         AND (changes.revision > ? OR (changes.revision = ? AND changes.ordinal > ?))
       ORDER BY changes.revision, changes.ordinal LIMIT ${LOG_PAGE_ROWS}`,
      through, ...(tables ?? []), cursor[0], cursor[0], cursor[1] === Number.MAX_SAFE_INTEGER ? Number.MAX_SAFE_INTEGER : cursor[1],
    ).toArray();
    for (const row of page) {
      const c = change(row);
      visit(c);
      cursor = [c.revision, c.ordinal];
      rows++;
    }
    if (page.length < LOG_PAGE_ROWS) return rows;
  }
}

/** The committed transactions of `(after, through]` in order (every revision with change rows). */
export function readTransactions(sql: LogSql, epoch: string, after: number, through: number): FeedTransaction[] {
  const out: Array<{ kind: "transaction"; epoch: string; revision: number; changes: LogChange[] }> = [];
  readChangeRows(sql, after, through, undefined, (c) => {
    let last = out[out.length - 1];
    if (!last || last.revision !== c.revision) out.push((last = { kind: "transaction", epoch, revision: c.revision, changes: [] }));
    last.changes.push(c);
  });
  return out;
}

/** One row's net change: its first before-image and last after-image, and the union of changed columns. */
export interface Collapsed {
  readonly table: string;
  readonly rowId: string;
  readonly old: JsonValue;
  readonly new: JsonValue;
  readonly changedColumns: readonly string[];
}

const byte = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** `collapse_changes` / `raw_log::collapse`: rows by (table, row id) in byte order. */
export class CollapsedChanges {
  private readonly rows: Collapsed[] = [];
  private readonly index = new Map<string, Collapsed>();

  constructor(changes: readonly LogChange[]) {
    const groups = new Map<string, LogChange[]>();
    for (const c of changes) {
      const key = `${c.table}\u0000${c.rowId}`;
      let list = groups.get(key);
      if (!list) groups.set(key, (list = []));
      list.push(c);
    }
    const keys = [...groups.keys()].sort((a, b) => {
      const [ta, ra] = a.split("\u0000") as [string, string];
      const [tb, rb] = b.split("\u0000") as [string, string];
      return byte(ta, tb) || byte(ra, rb);
    });
    for (const key of keys) {
      const list = groups.get(key)!;
      const columns = [...new Set(list.flatMap((c) => c.changedColumns))].sort(byte);
      const row: Collapsed = { table: list[0]!.table, rowId: list[0]!.rowId, old: list[0]!.oldValue, new: list[list.length - 1]!.newValue, changedColumns: columns };
      this.rows.push(row);
      this.index.set(key, row);
    }
  }

  get(table: string, rowId: string): Collapsed | undefined {
    return this.index.get(`${table}\u0000${rowId}`);
  }

  has(table: string, rowId: string): boolean {
    return this.index.has(`${table}\u0000${rowId}`);
  }

  /** The rows of one table, by row id. */
  table(table: string): Collapsed[] {
    return this.rows.filter((row) => row.table === table);
  }

  get all(): readonly Collapsed[] {
    return this.rows;
  }
}
