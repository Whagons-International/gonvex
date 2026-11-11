// Parent ACL indexes shared by every connection of the tenant object
// (visibility.rs `ParentAclRegistry`, `load_index`, `project_changes`,
// `run_applier`).
//
// Each index has one writer, this registry, which applies committed
// transactions in revision order. Rust loads an index after subscribing its
// applier to the feed; in the tenant object a load is one synchronous read
// of the clock and the rows, so no commit can fall between them. A holder
// gets an index that reflects exactly the snapshot it is reading: `ensure`
// brings every index it returns to the snapshot's clock, from the
// transactions a caller already pushed (`apply`) or from the change log. An
// epoch change, a log pruned past the index, or a changed projection poisons
// the index and reloads it.

import { ParentAclIndex } from "./acl-index.ts";
import { readChanges, readClock, type FeedTransaction, type LogChange, type TenantClock } from "./feed.ts";
import { get, isObject, scalar, type JsonValue } from "./json.ts";
import { debugString } from "./json.ts";
import { indexColumns, quote, requiredIndexes, rowIdColumn, VisibilityError, type VisibilityPlan } from "./plan.ts";
import type { ParentAclSource, VisibilitySql } from "./principal.ts";

/** Declared column kinds (K01 `JsonKind`). */
export type ColumnKind = "text" | "integer" | "real" | "boolean" | "json";

/** What an index projects. A module change that alters it replaces the index. */
export interface IndexShape {
  readonly table: string;
  readonly key: string;
  readonly idColumn: string;
  readonly columns: readonly string[];
}

function sameShape(left: IndexShape, right: IndexShape): boolean {
  return left.table === right.table && left.key === right.key && left.idColumn === right.idColumn && left.columns.join("\u0000") === right.columns.join("\u0000");
}

/** Optional catalog access: K01's `StorageSession.catalogOf` gives declared kinds, so JSON and BOOLEAN columns stay typed. */
export interface CatalogSql extends VisibilitySql {
  catalogOf?(table: string): { readonly columns: ReadonlyMap<string, string> };
}

/** rows.rs `json_value_expr`: a column as a JSON value, typed by its declared kind. */
export function jsonValueExpr(qualified: string, kind: ColumnKind): string {
  const finiteReal = `typeof(${qualified}) = 'real' AND abs(${qualified}) <= 1.7976931348623157e308`;
  const exactReal = `json(printf('%!.17g', ${qualified}))`;
  switch (kind) {
    case "json":
      return `CASE WHEN ${qualified} IS NULL THEN NULL WHEN json_valid(${qualified}) THEN json(${qualified}) ELSE ${qualified} END`;
    case "boolean":
      return `CASE WHEN ${qualified} IS NULL THEN NULL WHEN ${qualified} THEN json('true') ELSE json('false') END`;
    case "integer":
    case "real":
      return `CASE WHEN ${finiteReal} THEN ${exactReal} ELSE ${qualified} END`;
    case "text":
      return `CASE WHEN typeof(${qualified}) = 'blob' THEN lower(hex(${qualified})) WHEN ${finiteReal} THEN ${exactReal} ELSE ${qualified} END`;
  }
}

function sqlTextLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function columnKind(kind: string | undefined): ColumnKind {
  return kind === "json" || kind === "boolean" || kind === "integer" || kind === "real" ? kind : "text";
}

/** `load_index`: the projected rows of `shape` and the clock they were read at. */
export function loadIndex(sql: CatalogSql, shape: IndexShape): ParentAclIndex {
  const kinds = sql.catalogOf?.(shape.table).columns;
  const projection = shape.columns.map((column) => `${sqlTextLiteral(column)}, ${jsonValueExpr(`r.${quote(column)}`, columnKind(kinds?.get(column)))}`).join(", ");
  const statement = `SELECT CAST(r.${quote(shape.idColumn)} AS TEXT), CAST(r.${quote(shape.key)} AS TEXT), json_object(${projection}) FROM ${quote(shape.table)} AS r`;
  const clock = readClock(sql);
  const rows: Array<[string, string, JsonValue]> = [];
  for (const row of sql.run(statement).rows) {
    const id = row[0];
    const key = row[1];
    if (id === null || id === undefined || key === null || key === undefined) continue;
    let value: JsonValue;
    try {
      value = JSON.parse(String(row[2])) as JsonValue;
    } catch (error) {
      throw VisibilityError.invalid(`parent ACL row: ${(error as Error).message}`);
    }
    rows.push([String(id), String(key), value]);
  }
  return ParentAclIndex.maintained(clock.epoch, clock.revision, rows);
}

/** `project_changes`: one transaction's rows of `shape.table`, projected to the index columns. */
export function projectChanges(shape: IndexShape, changes: readonly LogChange[]): Array<[string, (readonly [string, JsonValue]) | undefined]> {
  const output: Array<[string, (readonly [string, JsonValue]) | undefined]> = [];
  for (const change of changes) {
    if (change.table !== shape.table) continue;
    const row = change.newValue;
    let projected: (readonly [string, JsonValue]) | undefined;
    if (isObject(row)) {
      const key = get(row, shape.key);
      if (key !== undefined && key !== null) {
        const value: Record<string, JsonValue> = {};
        for (const column of shape.columns) value[column] = get(row, column) ?? null;
        projected = [scalar(key), value];
      }
    }
    output.push([change.rowId, projected]);
  }
  return output;
}

/** Indexes a caller prepared before reading its snapshot. Inside the snapshot only lookups happen. */
export class PreparedParentAcl implements ParentAclSource {
  private readonly indexes = new Map<string, ParentAclIndex>();

  set(table: string, key: string, index: ParentAclIndex): void {
    this.indexes.set(`${table}\u0000${key}`, index);
  }

  get(table: string, key: string): ParentAclIndex {
    const index = this.indexes.get(`${table}\u0000${key}`);
    if (!index) throw VisibilityError.invalid(`parent ACL index ${table}.${key} was not prepared before the snapshot`);
    return index;
  }
}

interface Entry {
  readonly shape: IndexShape;
  readonly index: ParentAclIndex;
}

export interface RegistryStats {
  indexes: number;
  rows: number;
  loads: number;
  /** Rows read by loads and catch-up reads. */
  rowsRead: number;
  /** Transactions applied (pushed or caught up). */
  applied: number;
}

export class ParentAclRegistry {
  private readonly entries = new Map<string, Entry>();
  readonly counters = { loads: 0, rowsRead: 0, applied: 0 };

  /** Prepares every index the given plans bind, at the snapshot `sql` reads. */
  ensure(sql: CatalogSql, plans: ReadonlyMap<string, VisibilityPlan>, roots: readonly VisibilityPlan[]): PreparedParentAcl {
    const needed = new Map<string, [string, string]>();
    for (const plan of roots) for (const pair of requiredIndexes(plans, plan)) needed.set(pair.join("\u0000"), pair);
    return this.ensureNeeded(sql, plans, [...needed.values()]);
  }

  ensureNeeded(sql: CatalogSql, plans: ReadonlyMap<string, VisibilityPlan>, needed: ReadonlyArray<readonly [string, string]>): PreparedParentAcl {
    const prepared = new PreparedParentAcl();
    if (!needed.length) return prepared;
    const clock = readClock(sql);
    for (const [table, key] of needed) {
      const shape: IndexShape = { table, key, idColumn: rowIdColumn(plans, table), columns: indexColumns(plans, table, key) };
      prepared.set(table, key, this.ensureIndex(sql, shape, clock));
    }
    return prepared;
  }

  private ensureIndex(sql: CatalogSql, shape: IndexShape, clock: TenantClock): ParentAclIndex {
    const name = `${shape.table}\u0000${shape.key}`;
    const entry = this.entries.get(name);
    if (entry && sameShape(entry.shape, shape) && !entry.index.poisoned && entry.index.epoch === clock.epoch) {
      if (entry.index.revision >= clock.revision) return entry.index;
      // Changes after `retained` are all in the log.
      if (entry.index.revision >= clock.retained) {
        this.catchUp(sql, entry, clock);
        if (!entry.index.poisoned) return entry.index;
      }
    }
    entry?.index.poison();
    const before = { rowsRead: 0 };
    const index = loadIndex(countingSql(sql, before), shape);
    this.counters.loads += 1;
    this.counters.rowsRead += before.rowsRead;
    if (index.revision !== clock.revision) {
      index.poison();
      throw VisibilityError.invalid(`parent ACL index ${debugString(shape.table)} loaded at revision ${index.revision}, snapshot is at ${clock.revision}`);
    }
    this.entries.set(name, { shape, index });
    return index;
  }

  private catchUp(sql: CatalogSql, entry: Entry, clock: TenantClock): void {
    const counted = { rowsRead: 0 };
    for (const transaction of readChanges(countingSql(sql, counted), clock.epoch, [entry.shape.table], entry.index.revision, clock.revision)) {
      entry.index.applyRows(transaction.revision, projectChanges(entry.shape, transaction.changes));
      this.counters.applied += 1;
    }
    this.counters.rowsRead += counted.rowsRead;
    // Revisions without rows of this table complete the index too.
    entry.index.applyRows(clock.revision, []);
  }

  /**
   * Applies one committed transaction to every index, in revision order (the
   * push path: delivery hands over what it read). A transaction at or below
   * an index's revision is skipped; a gap or another epoch poisons it.
   */
  apply(transaction: FeedTransaction, previousRevision?: number): void {
    for (const entry of this.entries.values()) {
      const index = entry.index;
      if (index.poisoned || transaction.revision <= index.revision) continue;
      if (index.epoch !== transaction.epoch || (previousRevision !== undefined && index.revision < previousRevision)) {
        index.poison();
        continue;
      }
      index.applyRows(transaction.revision, projectChanges(entry.shape, transaction.changes));
      this.counters.applied += 1;
    }
  }

  /** Feed reset or lost completeness: every holder rebuilds. */
  poisonAll(): void {
    for (const entry of this.entries.values()) entry.index.poison();
    this.entries.clear();
  }

  stats(): RegistryStats {
    let rows = 0;
    for (const entry of this.entries.values()) rows += entry.index.size;
    return { indexes: this.entries.size, rows, ...this.counters };
  }
}

function countingSql(sql: CatalogSql, into: { rowsRead: number }): CatalogSql {
  return {
    run(text, parameters) {
      const result = sql.run(text, parameters);
      into.rowsRead += result.rowsRead;
      return result;
    },
    ...(sql.catalogOf ? { catalogOf: (table: string) => sql.catalogOf!(table) } : {}),
  };
}
