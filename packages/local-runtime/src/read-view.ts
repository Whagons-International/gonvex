import { matchesDataPredicate, orderedDataRead } from "@gonvex/module-sdk";
import type { DataRead, DataPredicate, JsonObject } from "@gonvex/module-sdk";
import type { LocalPatch } from "./index.js";
import { orderDataRows, type ReducerReadView } from "./portable.js";

export type ReadCoverage = Readonly<
  Record<
    string,
    { key: string; complete: boolean; completeWhere?: readonly DataPredicate[]; columns?: readonly string[] }
  >
>;

/** Index ID-list membership for this read only, without retaining row data. */
function readPredicate(predicate: DataPredicate): (row: JsonObject) => boolean {
  if ('and' in predicate) {
    const parts = predicate.and.map(readPredicate);
    return row => parts.every(part => part(row));
  }
  if ('or' in predicate) {
    const parts = predicate.or.map(readPredicate);
    return row => parts.some(part => part(row));
  }
  if (predicate.op === 'in' && !predicate.transform) {
    const values = new Set<unknown>(predicate.values);
    return row => {
      const value = row[predicate.column];
      return value != null && value === value && values.has(value);
    };
  }
  return row => matchesDataPredicate(row, predicate);
}

/** Conservative implication: one complete slice must cover the whole read. */
export function predicateImplies(read: DataPredicate | undefined, covered: DataPredicate): boolean {
  if (!read) return false;
  if ('and' in covered) return covered.and.every(term => predicateImplies(read, term));
  if ('or' in read) return read.or.every(term => predicateImplies(term, covered));
  if ('or' in covered) return covered.or.some(term => predicateImplies(read, term));
  if ('and' in read) return read.and.some(term => predicateImplies(term, covered));
  return read.column === covered.column && read.op === covered.op
    && read.transform === covered.transform
    && JSON.stringify(read) === JSON.stringify(covered);
}

export function readIsCovered(known: ReadCoverage[string] | undefined, read: DataRead): boolean {
  return known?.complete === true || known?.completeWhere?.some(where => predicateImplies(read.where, where)) === true;
}

/** A primary-key list bounds the result even when the surrounding collection is partial. */
export function primaryReadKeys(read: DataRead, key: string): string[] | undefined {
  const where = read.where;
  if (where && 'and' in where) {
    for (const term of where.and) {
      const keys = primaryReadKeys({ ...read, where: term }, key);
      if (keys !== undefined) return keys;
    }
    return undefined;
  }
  if (!where || !('column' in where) || where.column !== key || where.transform) return undefined;
  if (where.op === 'eq' && typeof where.value === 'string') return [where.value];
  if (where.op === 'in' && where.values.every(value => typeof value === 'string')) return [...new Set(where.values as string[])];
  return undefined;
}

/** Preserve keys and predicates for overlays without fetching unrelated fields. */
export function executionReadColumns(read: DataRead, key: string): string[] | undefined {
  if (!read.columns) return undefined;
  const columns = new Set([...read.columns, key]);
  const collect = (where: DataPredicate): void => {
    if ('and' in where) where.and.forEach(collect);
    else if ('or' in where) where.or.forEach(collect);
    else columns.add(where.column);
  };
  if (read.where) collect(read.where);
  for (const order of read.orderBy ?? []) columns.add(order.column);
  return [...columns];
}

export function projectDataRows(
  rows: JsonObject[],
  read: DataRead,
): JsonObject[] {
  orderDataRows(rows, read);
  const window = read.limit === undefined ? rows : rows.slice(0, read.limit);
  return window.map((row) =>
    structuredClone(
      read.columns
        ? Object.fromEntries(
            read.columns.map((column) => [column, row[column] ?? null]),
          )
        : row,
    ),
  );
}

/** A captured immutable RAM view. Only matching rows are copied. */
export function memoryReadView(
  tables: ReadonlyMap<string, ReadonlyMap<string, JsonObject>>,
  coverage: ReadCoverage,
): ReducerReadView {
  return {
    async select(read, excludedRowIds = []) {
      read = orderedDataRead({
        ...read,
        key: read.key ?? coverage[read.table]?.key,
      });
      const known = coverage[read.table];
      const matches = read.where ? readPredicate(read.where) : undefined;
      const excluded = new Set(excludedRowIds);
      const table = tables.get(read.table);
      const keys = primaryReadKeys(read, known?.key ?? '_id');
      const candidates = keys === undefined ? (table?.values() ?? [])
        : keys.flatMap(id => table?.has(id) ? [table.get(id)!] : []);
      const rows: JsonObject[] = [];
      const required = new Set(read.columns ?? known?.columns ?? []);
      const collect = (predicate: NonNullable<DataRead["where"]>): void => {
        if ("and" in predicate) predicate.and.forEach(collect);
        else if ("or" in predicate) predicate.or.forEach(collect);
        else required.add(predicate.column);
      };
      if (read.where) collect(read.where);
      for (const order of read.orderBy ?? []) required.add(order.column);
      const requiredColumns = [...required];
      let completeFields = true;
      for (const row of candidates) {
        if (excluded.has(String(row[known?.key ?? '_id']))) continue;
        if (requiredColumns.some((column) => !Object.hasOwn(row, column)))
          completeFields = false;
        if (!matches || matches(row)) {
          rows.push(row);
          if (
            !read.orderBy?.length &&
            read.limit !== undefined &&
            rows.length >= read.limit
          )
            break;
        }
      }
      return {
        rows: projectDataRows(rows, read),
        complete:
          read.limit === 0 ||
          (completeFields &&
            (readIsCovered(known, read) ||
              (keys !== undefined && keys.every(id => excluded.has(id) || table?.has(id) === true)))),
      };
    },
  };
}

/** Predictions overlay the confirmed replica. No second entity cache is created. */
export function overlayReadView(
  base: ReducerReadView,
  coverage: ReadCoverage,
  patches: readonly LocalPatch[],
): ReducerReadView {
  const byTable = new Map<string, Map<string, LocalPatch[]>>();
  for (const patch of patches) {
    let table = byTable.get(patch.entity);
    if (!table) {
      table = new Map();
      byTable.set(patch.entity, table);
    }
    const chain = table.get(patch.rowId);
    if (chain) chain.push(patch);
    else table.set(patch.rowId, [patch]);
  }
  return {
    keepAliveFor: base.keepAliveFor,
    async select(read, excludedRowIds = []) {
      read = orderedDataRead({
        ...read,
        key: read.key ?? coverage[read.table]?.key,
      });
      const changes = byTable.get(read.table);
      if (!changes?.size) return base.select(read, excludedRowIds);
      const key = coverage[read.table]?.key ?? "_id";
      const keys = primaryReadKeys(read, key);
      const exact = read.where && 'column' in read.where && read.where.column === key
        && !read.where.transform && read.where.op === 'eq' && typeof read.where.value === 'string'
        ? read.where.value : undefined;
      const excluded = new Set(excludedRowIds);
      const relevant = new Map<string, LocalPatch[]>();
      // A primary-key predicate bounds the candidates even within an AND.
      // Do not fetch or apply unrelated pending rows for a small bulk read.
      for (const id of keys ?? changes.keys()) {
        const chain = changes.get(id);
        if (chain && !excluded.has(id)) relevant.set(id, chain);
      }
      if (!relevant.size) return base.select(read, excludedRowIds);
      // A full replacement or deletion makes the old row irrelevant, including
      // fields absent from a narrow server projection. Exclude it before base
      // coverage checks; missing fields on every other row remain an error.
      const superseded = [...relevant].filter(([, chain]) => chain.some(patch => patch.op !== 'patch')).map(([id]) => id);
      const result = await base.select({
        ...read,
        columns: executionReadColumns(read, key),
        limit:
          read.limit === undefined ? undefined : read.limit + relevant.size,
      }, [...excludedRowIds, ...superseded]);
      const rows = new Map(result.rows.map((row) => [String(row[key]), row]));
      const missing = [...relevant].filter(([id, chain]) => !rows.has(id) && chain[0]?.op === 'patch').map(([id]) => id);
      const originals = new Map<string, JsonObject>();
      if (missing.length) {
        const fetched = await base.select({table:read.table,key,where:{column:key,op:'in',values:missing}});
        for (const row of fetched.rows) originals.set(String(row[key]), row);
      }
      const resolved = new Set<string>();
      for (const [id, chain] of relevant) {
        let row = rows.get(id);
        if (!row && chain[0]?.op === "patch") row = originals.get(id);
        for (const patch of chain) {
          if (patch.op === "delete") {
            row = undefined;
            resolved.add(id);
          } else if (patch.op === "patch") {
            if (row) {
              row = { ...row, ...patch.fields };
              resolved.add(id);
            }
          } else {
            row = patch.fields;
            resolved.add(id);
          }
        }
        rows.delete(id);
        if (row && (!read.where || matchesDataPredicate(row, read.where)))
          rows.set(id, row);
      }
      return {
        rows: projectDataRows([...rows.values()], read),
        complete:
          result.complete || (exact !== undefined && resolved.has(exact)),
      };
    },
  };
}
