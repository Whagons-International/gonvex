import { type Collection, type Table, type IndexableType } from 'dexie';
import { compileDataRead, orderedDataRead, matchesDataPredicate } from '@gonvex/module-sdk';
import type { DataPredicate, DataRead, DataScalar, JsonObject } from '@gonvex/module-sdk';
import type { ReducerReadView } from '@gonvex/local-runtime/portable';

export type EntityRecord = { scope: string; entity: string; id: string; value: string; lookupKeys?: IndexableType[]; authority?: import('./replica-record.js').ReplicaRecordVersion; sequence?:number; deleted?:boolean };
import { primaryReadKeys, readIsCovered, type ReadCoverage } from '@gonvex/local-runtime/read-view';
export type { ReadCoverage } from '@gonvex/local-runtime/read-view';
export const encodeReadKey = (value: DataScalar): [number, string | number] => {
  if (value === null) return [0, 0];
  if (typeof value === 'boolean') return [1, Number(value)];
  if (typeof value === 'number') return [2, value];
  return [3, value];
};

/** Secondary keys live on disk alongside the record, not in another row cache.
 * Large text/JSON stays unindexed; a residual predicate can still examine it.
 */
export function entityRecord(scope: string, entity: string, id: string, row: JsonObject): EntityRecord {
  const lookupKeys: IndexableType[] = [];
  // Avoid temporary entry, flatMap and spread arrays for every column of each
  // persisted row. Keep the same on-disk index encoding and residual reads.
  for (const column of Object.keys(row)) {
    const value = row[column];
    let type: number;
    let key: string | number;
    // SQL equality never selects NULL; isNull reads already use a residual scan.
    // Indexing every absent optional field only duplicates disk/native allocations.
    if (typeof value === 'boolean') { type = 1; key = Number(value); }
    else if (typeof value === 'number' && Number.isFinite(value)) { type = 2; key = value; }
    else if (typeof value === 'string' && value.length <= 256) { type = 3; key = value; }
    else continue;
    lookupKeys.push([scope, entity, column, type, key]);
  }
  return { scope, entity, id, value: JSON.stringify(row), lookupKeys };
}

type IndexedEquality = {column: string; op: 'eq'; value: DataScalar} | {column: string; op: 'in'; values: readonly DataScalar[]};
function equality(predicate: DataPredicate | undefined): IndexedEquality | undefined {
  if (!predicate) return;
  if ('and' in predicate) return predicate.and.map(equality).find(Boolean);
  if ('or' in predicate) return;
  if(predicate.transform) return;
  if (predicate.op === 'eq' && (typeof predicate.value !== 'string' || predicate.value.length <= 256)) return { column: predicate.column, op: 'eq', value: predicate.value };
  if (predicate.op === 'in' && predicate.values.every(value => typeof value !== 'string' || value.length <= 256)) return predicate;
}

function compare(read: DataRead, a: JsonObject, b: JsonObject): number {
  for (const order of read.orderBy ?? []) {
    const sortValue = (row: JsonObject) => {
      const value = row[order.column];
      if (order.transform !== 'numericSuffix' || value == null) return value;
      if (typeof value !== 'string') throw new Error('Numeric suffix requires text');
      const suffix = value.match(/[0-9]+$/)?.[0];
      return suffix === undefined ? null : BigInt(suffix);
    };
    const left = sortValue(a), right = sortValue(b);
    if (left === right) continue;
    const direction = order.direction === 'desc' ? -1 : 1;
    const nullFirst = (order.nulls ?? (direction === 1 ? 'last' : 'first')) === 'first';
    if (left == null) return nullFirst ? -1 : 1;
    if (right == null) return nullFirst ? 1 : -1;
    if (typeof left !== typeof right || typeof left === 'object') throw new Error(`Incompatible ordering for ${order.column}`);
    return (left < right ? -1 : 1) * direction;
  }
  return 0;
}

/** Must be used inside the storage adapter's transaction. Cursor iteration keeps
 * unrelated records off the JS heap; bounded reads retain at most limit rows.
 */
export function indexedDBReadView(entities: Table<EntityRecord, [string, string, string]>, scope: string, coverage: ReadCoverage): ReducerReadView {
  return {
    async select(read, excludedRowIds = []) {
      read=orderedDataRead({...read,key:read.key ?? coverage[read.table]?.key});
      compileDataRead(read);
      const known = coverage[read.table];
      const excluded = new Set(excludedRowIds);
      const key = known?.key ?? '_id';
      const restriction = equality(read.where);
      let candidates: Collection<EntityRecord, [string, string, string]>;
      const primaryKeys = primaryReadKeys(read, key);
      const exactKey = restriction?.op === 'eq' && restriction.column === key && typeof restriction.value === 'string';
      if (exactKey) {
        candidates = entities.where('[scope+entity+id]').equals([scope, read.table, String(restriction.value)]);
      } else if (primaryKeys) {
        candidates = entities.where('[scope+entity+id]').anyOf(primaryKeys.map(id => [scope, read.table, id]));
      } else if (restriction) {
        const values = restriction.op === 'eq' ? [restriction.value] : restriction.values;
        const keys = values.filter(value => value !== null).map(value => [scope, read.table, restriction.column, ...encodeReadKey(value)]);
        candidates = entities.where('lookupKeys').anyOf(keys).distinct();
      } else candidates = entities.where('[scope+entity]').equals([scope, read.table]);
      const result: JsonObject[] = [];
      const foundPrimary = new Set<string>();
      let fieldsComplete=true;
      const required=new Set(read.columns ?? known?.columns ?? []);
      const requirePredicate=(predicate:DataPredicate):void=>{
        if('and' in predicate) predicate.and.forEach(requirePredicate);
        else if('or' in predicate) predicate.or.forEach(requirePredicate);
        else required.add(predicate.column);
      };
      if(read.where) requirePredicate(read.where);
      for(const order of read.orderBy ?? []) required.add(order.column);
      if (read.limit !== 0) await candidates.until(() => !read.orderBy?.length && read.limit !== undefined && result.length >= read.limit).each(record => {
        if (record.deleted || excluded.has(record.id)) return;
        const row = JSON.parse(record.value) as JsonObject;
        if (primaryKeys) foundPrimary.add(record.id);
        if([...required].some(column=>!Object.hasOwn(row,column))) fieldsComplete=false;
        if (read.where && !matchesDataPredicate(row, read.where)) return;
        if (read.orderBy?.length && read.limit !== undefined) {
          // Retain a bounded ordered candidate set, never sort a full collection.
          let low = 0, high = result.length;
          while (low < high) { const mid = (low + high) >>> 1; if (compare(read, result[mid]!, row) <= 0) low = mid + 1; else high = mid; }
          if (low < read.limit) { result.splice(low, 0, row); if (result.length > read.limit) result.pop(); }
        } else result.push(row);
      });
      if (read.orderBy?.length && read.limit === undefined) result.sort((a, b) => compare(read, a, b));
      const complete = read.limit === 0 || fieldsComplete && (readIsCovered(known, read) || (primaryKeys !== undefined && primaryKeys.every(id => excluded.has(id) || foundPrimary.has(id))));
      return { rows: read.columns ? result.map(row => Object.fromEntries(read.columns!.map(column => [column, row[column] ?? null]))) : result, complete };
    },
  };
}
