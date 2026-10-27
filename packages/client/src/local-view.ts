import type { JsonValue } from '@gonvex/protocol';
import type { ReadCoverage } from '@gonvex/local-runtime/read-view';
import { readIsCovered } from '@gonvex/local-runtime/read-view';
import { smallestRows, smallestRowSelection, type OfflineLiveQueryResult } from './query-expression.js';
import { compareReplicaMembershipValues } from './replica-membership-order.js';
import { CompactRelationSet } from './compact-relation-set.js';
import { LocalQueryBudgetError } from './local-query-budget.js';
import { mapResult, readValues, firstResult, visitRows, visitRowsWhile, immediate, isPending, type MaybePromise, type RowStream } from './local-view-stream.js';

/** A client view is separate from the server's SQL Live Query plan. Its values
 * are data, not executable callbacks, and its rows remain canonical entities. */
export type LocalViewValue =
  | { kind: 'field'; column: string }
  | { kind: 'literal'; value: JsonValue }
  | { kind: 'context'; key: 'account.id' | 'member.id' }
  | { kind: 'predicate'; predicate: LocalViewPredicate }
  | { kind: 'if'; condition: LocalViewPredicate; then: LocalViewValue; else: LocalViewValue }
  | { kind: 'coalesce'; values: readonly LocalViewValue[] }
  | { kind: 'array'; values: readonly LocalViewValue[] }
  | { kind: 'cast'; mode: 'string' | 'text' | 'lower' | 'number' | 'date' | 'boolean' | 'numericText'; value: LocalViewValue }
  | { kind: 'lookup'; table: string; id: LocalViewValue; value: LocalViewValue }
  | { kind: 'related'; table: string; foreignKey: string; match: LocalViewValue; where?: LocalViewPredicate;
      value: LocalViewValue; reduce: 'first' | 'last' | 'array' };
export type LocalViewPredicate =
  | { kind: 'constant'; value: boolean }
  | { kind: 'and' | 'or'; terms: readonly LocalViewPredicate[] }
  | { kind: 'not'; term: LocalViewPredicate }
  | { kind: 'compare'; op: 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte'; left: LocalViewValue; right: LocalViewValue; mode: 'strict' | 'number' | 'natural' }
  | { kind: 'text'; op: 'contains' | 'startsWith' | 'endsWith'; value: LocalViewValue; text: string }
  | { kind: 'test'; op: 'empty' | 'blank' | 'finite' | 'truthy' | 'array'; value: LocalViewValue }
  | { kind: 'arrayContains'; array: LocalViewValue; value: LocalViewValue; mode: 'strict' | 'string' }
  | { kind: 'in'; value: LocalViewValue; values: readonly JsonValue[]; mode: 'strict' | 'string'; array?: boolean }
  | { kind: 'exists'; table: string; foreignKey: string; match: LocalViewValue; where?: LocalViewPredicate };
export type LocalViewPlan = {
  where?: LocalViewPredicate;
  /** Count distinct non-null scalar column values in total. Requires an
   * ID-only zero window, no grouping and no ordering. */
  countDistinct?: string;
  /** Exact counts before pagination. Scalar values become String(value ?? '').
   * Groups retain first matching source order; the caller chooses display order. */
  groupBy?: LocalViewValue;
  /** Return selected IDs and these expression values instead of entity bodies.
   * An empty list selects IDs only, without selected-body point reads. */
  project?: readonly LocalViewValue[];
  orderBy: readonly { value: LocalViewValue; direction: 'asc' | 'desc'; mode: 'natural' | 'number' | 'replica'; blanks?: 'last' }[];
  window: { offset: number; limit: number;
    /** Union of data-relative windows, used when collapsed groups leave gaps.
     * Overrides the single window; result positions align with returned rows. */
    ranges?: readonly { offset: number; limit: number }[] };
  /** Pin locale explicitly when the UI's natural order depends on it. */
  locale?: string;
};
/** Validate and merge sparse windows without expanding their positions. */
export function localViewRanges(window: LocalViewPlan['window'], visit: () => void = () => {}): { offset: number; limit: number }[] | undefined {
  const valid = (value: { offset: number; limit: number }) => value && [value.offset, value.limit, value.offset + value.limit].every(n => Number.isSafeInteger(n) && n >= 0);
  if (!valid(window)) throw Error('Invalid local view window');
  if (window.ranges === undefined) return undefined;
  if (!Array.isArray(window.ranges)) throw Error('Invalid local view ranges');
  const ranges = window.ranges.map(range => { visit(); if (!valid(range)) throw Error('Invalid local view range'); return { offset: range.offset, limit: range.limit }; })
    .filter(range => range.limit > 0).sort((a, b) => { visit(); return a.offset - b.offset; });
  const merged: { offset: number; limit: number }[] = [];
  for (const range of ranges) {
    visit(); const previous = merged.at(-1);
    if (previous && range.offset <= previous.offset + previous.limit) previous.limit = Math.max(previous.offset + previous.limit, range.offset + range.limit) - previous.offset;
    else merged.push(range);
  }
  return merged;
}

export type LocalViewSource = {
  key(table: string): string | undefined;
  rows(table: string): Iterable<Record<string, JsonValue>>;
  rootRows?(): Iterable<Record<string, JsonValue>>;
  rootOrder?: AsyncLocalViewSource['rootOrder'];
  get(table: string, id: string): Record<string, JsonValue> | undefined;
};

export type AsyncLocalViewSource = {
  key(table: string): string | undefined;
  rows(table: string): RowStream<Record<string, JsonValue>>;
  rootRows?(): RowStream<Record<string, JsonValue>>;
  /** Logical membership order when disk rows arrive in physical key order.
   * Undefined marks appended predictions, after all persisted members. */
  rootOrder?(row: Record<string, JsonValue>): { order: string; id: string } | undefined;
  /** Exhaustive root stream in ascending UTF-16 string-column order. Nulls
   * may be omitted; missing fields must still make coverage partial. */
  rootDistinctOrder?: string;
  /** Exhaustive semantic pass when an ordered aggregate stream omits null
   * keys. Includes those rows so predicate errors/coverage cannot disappear. */
  rootValidationRows?(): RowStream<Record<string, JsonValue>>;
  get(table: string, id: string): MaybePromise<Record<string, JsonValue> | undefined>;
  /** Register a relation before its one streaming semantic validation pass.
   * With this hook, narrowing must not hide missing fields or errors elsewhere
   * in the admitted source. Validation retains no global key/value reduction. */
  prepareRelated?(table: string, foreignKey: string): void;
  /** Opt into streamed semi-joins instead of retaining all foreign keys. The
   * prepareRelated validation pass still protects global field/error coverage. */
  streamRelatedExistence?: boolean;
  /** Exact related candidates in canonical source order. A disk adapter uses
   * its foreign-key index instead of retaining the whole dependency table. */
  relatedRows?(table: string, foreignKey: string, match: unknown): {
    rows: RowStream<Record<string, JsonValue>>;
    /** Exhaustive membership and field coverage at this same read root.
     * An index alone cannot prove this for projected or legacy missing fields. */
    complete: boolean;
  };
};
type SourcePosition = { position: number; id: string; sourceOrder?: string };
type Candidate = SourcePosition & { values: unknown[] };
function compareSourcePosition(left: SourcePosition, right: SourcePosition): number {
  const a = left.sourceOrder, b = right.sourceOrder;
  if (a !== undefined && b !== undefined) return a < b ? -1 : a > b ? 1 : left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
  if (a !== undefined || b !== undefined) return a !== undefined ? -1 : 1;
  return left.position - right.position;
}

export function localViewDependencies(root: string, view: LocalViewPlan): string[] {
  const tables = new Set([root]);
  const seen = new WeakSet<object>();
  const visit = (input: unknown, depth = 0): void => {
    if (depth > 64) throw Error('Local view expression depth budget');
    if (!input || typeof input !== 'object') return;
    if (seen.has(input)) return; seen.add(input);
    if (Array.isArray(input)) { for (const item of input) visit(item, depth + 1); return; }
    const node = input as Record<string, unknown>;
    if (['lookup', 'related', 'exists'].includes(String(node.kind)) && typeof node.table === 'string') tables.add(node.table);
    // Literal JSON can contain arbitrary application data, including `kind`.
    if (node.kind === 'literal') return;
    for (const [key, value] of Object.entries(node)) if (key !== 'values' || node.kind === 'coalesce' || node.kind === 'array') visit(value, depth + 1);
  };
  visit(view.where); visit(view.groupBy); visit(view.project); for (const order of view.orderBy) visit(order.value);
  return [...tables];
}

/** Streaming exact selection. Logical retained-byte/work limits match the
 * existing disk query contract; they are not a whole-process heap estimate. */
function createLocalViewEvaluation(source: AsyncLocalViewSource, table: string, key: string, view: LocalViewPlan,
  coverage: ReadCoverage, context: Readonly<Record<string, JsonValue>> | undefined,
  budget: { candidateBytes: number; visit(): void }, columns: readonly string[] = [], streaming = false) {
  if (source.key(table) !== key) throw Error('Invalid local view table or key');
  const ranges = localViewRanges(view.window, budget.visit), { offset, limit } = view.window;
  if (!Number.isSafeInteger(budget.candidateBytes) || budget.candidateBytes < 0) throw Error('Invalid local view budget');
  if (!Array.isArray(view.orderBy)) throw Error('Invalid local view order');
  const collator = new Intl.Collator(view.locale, { numeric: true, sensitivity: 'base' });
  let retained = 0, partial = false, unsupported: string | undefined;
  type LookupRow = { row: Record<string, JsonValue> | undefined; rowBytes: number; bytes: number };
  // This read owns one immutable storage root. Reuse reference rows only for
  // its lifetime, within the same candidate budget. Optional entries yield to
  // query results, relation indexes and active-row temporaries.
  const lookupRows = new Map<string, LookupRow>();
  const evictLookup = () => {
    const identity = lookupRows.keys().next().value!;
    retained -= lookupRows.get(identity)!.bytes; lookupRows.delete(identity); budget.visit();
  };
  const reserve = (bytes: number) => {
    while (bytes > 0 && lookupRows.size && retained + bytes > budget.candidateBytes) evictLookup();
    retained += bytes; if (retained > budget.candidateBytes) throw new LocalQueryBudgetError('candidateByteBudget', 'Local view candidate byte budget');
  };
  const cost = (value: unknown) => (JSON.stringify(value) ?? '').length * 2 + 128;
  const required = new Map<string, Set<string>>();
  const require = (name: string, column: string) => {
    if (!source.key(name) || typeof column !== 'string' || !column) throw Error('Invalid local view table or field');
    let columns = required.get(name); if (!columns) required.set(name, columns = new Set()); columns.add(column);
  };
  type Memory = { bytes: number };
  type Env = { row: Record<string, JsonValue>; lookups: Map<string, MaybePromise<Record<string, JsonValue> | undefined>>; memory: Memory };
  type Value = (env: Env) => MaybePromise<unknown>;
  type Predicate = (env: Env) => MaybePromise<boolean>;
  const env = (row: Record<string, JsonValue>, memory: Memory = { bytes: 0 }): Env => ({ row, lookups: new Map(), memory });
  const temporary = (current: Env, bytes: number) => { reserve(bytes); current.memory.bytes += bytes; };
  const lookupRow = (name: string, id: string, identity: string, current: Env): MaybePromise<Record<string, JsonValue> | undefined> => {
    const known = streaming ? lookupRows.get(identity) : undefined;
    if (known) {
      lookupRows.delete(identity); lookupRows.set(identity, known);
      // Charge active use independently, so eviction cannot leave a live
      // reference outside the read's accounting while its predicate awaits.
      if (known.row) temporary(current, known.rowBytes);
      return known.row;
    }
    return mapResult(source.get(name, id), row => {
      if (streaming) {
        const rowBytes = row ? cost(row) : 0, bytes = cost(identity) + rowBytes;
        while (lookupRows.size && retained + bytes > budget.candidateBytes) evictLookup();
        if (retained + bytes <= budget.candidateBytes) { retained += bytes; lookupRows.set(identity, { row, rowBytes, bytes }); }
        if (row) temporary(current, rowBytes);
      }
      return row;
    });
  };
  const scoped = <T>(current: Env, read: () => MaybePromise<T>): MaybePromise<T> => {
    const release = () => { reserve(-current.memory.bytes); current.memory.bytes = 0; };
    try {
      const result = read();
      if (isPending(result)) return Promise.resolve(result).finally(release);
      release(); return result;
    } catch (error) { release(); throw error; }
  };
  const depthCheck = (depth: number) => { budget.visit(); if (depth > 64) throw Error('Local view expression depth budget'); };
  const validJSON = (input: unknown, depth = 0): void => {
    depthCheck(depth);
    if (input === null || typeof input === 'string' || typeof input === 'boolean' || typeof input === 'number' && Number.isFinite(input)) return;
    if (!input || typeof input !== 'object' || !Array.isArray(input) && ![Object.prototype, null].includes(Object.getPrototypeOf(input))) throw Error('Invalid local view JSON value');
    for (const key in input) if (Object.hasOwn(input, key)) validJSON((input as Record<string, unknown>)[key], depth + 1);
  };
  const empty = (value: unknown) => value == null || value === '' || Array.isArray(value) && !value.length;
  const blank = (value: unknown) => value == null || typeof value === 'string' && !value.trim();
  const natural = (left: unknown, right: unknown): number => {
    if (left === right) return 0;
    if (left == null || left === '') return 1;
    if (right == null || right === '') return -1;
    if (typeof left === 'string' && typeof right === 'string') return collator.compare(left, right);
    const a = Number(left), b = Number(right);
    if (Number.isFinite(a) && Number.isFinite(b)) return a < b ? -1 : a > b ? 1 : 0;
    return collator.compare(String(left), String(right));
  };
  const relation = (node: { table: string; foreignKey: string; where?: LocalViewPredicate }, depth: number,
    select?: Value, reduce?: 'first' | 'last' | 'array') => {
    require(node.table, node.foreignKey);
    const where: Predicate = node.where ? predicate(node.where, node.table, depth + 1) : () => true;
    type Index = Map<unknown, unknown> | CompactRelationSet;
    let index: MaybePromise<Index> | undefined;
    let validationStarted = false, validation: MaybePromise<void>;
    const validate = (): MaybePromise<void> => {
      if (!source.prepareRelated) return;
      if (!validationStarted) {
        validationStarted = true; source.prepareRelated(node.table, node.foreignKey);
        validation = visitRows(source.rows(node.table), row => {
          budget.visit(); const current = env(row);
          return scoped(current, () => mapResult(where(current), accepted => {
            if (!accepted) return;
            if (!Object.hasOwn(row, node.foreignKey)) partial = true;
            if (row[node.foreignKey] == null || !select) return;
            return mapResult(select(current), () => undefined);
          }));
        });
      }
      return validation;
    };
    return (match: unknown, owner: Env): MaybePromise<unknown> => {
      // Providers can stream semi-joins after the same global validation pass,
      // keeping both value reductions and existence independent of corpus RAM.
      if (source.relatedRows && (select || !source.prepareRelated || source.streamRelatedExistence)) return mapResult(validate(), () => {
        if (match == null) return select ? undefined : false;
        let found = false, result: unknown;
        const related = source.relatedRows!(node.table, node.foreignKey, match);
        if (typeof related.complete !== 'boolean') throw Error('Invalid local view related coverage');
        if (!related.complete) partial = true;
        const visit = (row: Record<string, JsonValue>) => {
          budget.visit(); const current = env(row);
          return scoped(current, () => mapResult(where(current), accepted => {
            if (!accepted) return;
            if (!Object.hasOwn(row, node.foreignKey)) partial = true;
            const id = row[node.foreignKey];
            if (id == null || !(id === match || Number.isNaN(id) && Number.isNaN(match))) return;
            if (!select) { found = true; return; }
            return mapResult(select(current), value => {
              if (reduce === 'array') {
                if (!found) { result = []; temporary(owner, 128); }
                temporary(owner, cost(value)); (result as unknown[]).push(value);
              } else if (reduce === 'last' || !found) {
                temporary(owner, cost(value) - (found ? cost(result) : 0)); result = value;
              }
              found = true;
            });
          }));
        };
        const done = !select && source.streamRelatedExistence && source.prepareRelated
          ? visitRowsWhile(related.rows, row => mapResult(visit(row), () => !found)) : visitRows(related.rows, visit);
        return mapResult(done, () => select ? result : found);
      });
      if (!index) {
        const next: Index = select ? new Map() : new CompactRelationSet(reserve, budget.visit);
        const done = visitRows(source.rows(node.table), row => {
          budget.visit(); const current = env(row);
          return scoped(current, () => mapResult(where(current), accepted => {
            if (!accepted) return;
            if (!Object.hasOwn(row, node.foreignKey)) partial = true;
            const id = row[node.foreignKey]; if (id == null) return;
            if (next instanceof CompactRelationSet) { next.add(id); return; }
            if (!next.has(id)) reserve(cost(id) + 64);
            return mapResult(select!(current), value => {
              if (reduce === 'array') {
                let values = next.get(id) as unknown[] | undefined;
                if (!values) next.set(id, values = []);
                reserve(cost(value)); values.push(value);
              } else if (reduce === 'last' || !next.has(id)) {
                reserve(cost(value) - (next.has(id) ? cost(next.get(id)) : 0)); next.set(id, value);
              }
            });
          }));
        });
        index = mapResult(done, () => next);
      }
      return mapResult(index, ready => select ? (ready as Map<unknown, unknown>).get(match) : ready.has(match));
    };
  };
  function value(node: LocalViewValue, name: string, depth = 0): Value {
    const read = compileValue(node, name, depth);
    return current => { budget.visit(); return read(current); };
  }
  function compileValue(node: LocalViewValue, name: string, depth: number): Value {
    depthCheck(depth);
    if (!node || typeof node !== 'object') throw Error('Invalid local view value');
    switch (node.kind) {
      case 'field':
        require(name, node.column);
        return current => { if (!Object.hasOwn(current.row, node.column)) { partial = true; return undefined; } return current.row[node.column]; };
      case 'literal': validJSON(node.value); reserve(cost(node.value)); return () => node.value;
      case 'context':
        if (context?.[node.key] === undefined) unsupported = 'missingIdentityContext';
        return () => context?.[node.key];
      case 'predicate': { const test = predicate(node.predicate, name, depth + 1); return current => test(current); }
      case 'if': {
        const test = predicate(node.condition, name, depth + 1), yes = value(node.then, name, depth + 1), no = value(node.else, name, depth + 1);
        return current => mapResult(test(current), accepted => accepted ? yes(current) : no(current));
      }
      case 'coalesce': {
        if (!Array.isArray(node.values)) throw Error('Invalid local view coalesce');
        const values = node.values.map(child => value(child, name, depth + 1));
        return current => firstResult(values, read => read(current), result => result != null, undefined);
      }
      case 'array': {
        if (!Array.isArray(node.values)) throw Error('Invalid local view array');
        const values = node.values.map(child => value(child, name, depth + 1));
        return current => {
          temporary(current, 64 + values.length * 8);
          return readValues(values, read => mapResult(read(current), item => {
            // Match scalar projection output. Missing fields still mark the
            // evaluation partial, but their array slots must remain valid JSON.
            const projected = item ?? null;
            temporary(current, cost(projected)); return projected;
          }));
        };
      }
      case 'cast': {
        const read = value(node.value, name, depth + 1);
        if (!['string', 'text', 'lower', 'number', 'date', 'boolean', 'numericText'].includes(node.mode)) throw Error('Invalid local view cast');
        return current => mapResult(read(current), input => {
          switch (node.mode) {
            case 'string': return String(input ?? '');
            case 'text': return String(input ?? '').trim().toLocaleLowerCase(view.locale);
            case 'lower': return String(input ?? '').toLowerCase();
            case 'number': return Number(input);
            case 'boolean': return Boolean(input);
            case 'numericText': {
              if (typeof input !== 'string') return input;
              const trimmed = input.trim();
              return !trimmed ? null : /^-?\d+(?:\.\d+)?$/.test(trimmed) ? Number(trimmed) : trimmed;
            }
            case 'date': return typeof input === 'number' || /^\d+$/.test(String(input ?? '')) ? Number(input) : Date.parse(String(input ?? ''));
          }
        });
      }
      case 'lookup': {
        if (!source.key(node.table)) throw Error('Invalid local view lookup');
        const id = value(node.id, name, depth + 1), select = value(node.value, node.table, depth + 1);
        return current => mapResult(id(current), key => {
          if (key == null || key === '') return undefined;
          const identity = node.table + '\0' + String(key);
          if (!current.lookups.has(identity)) {
            budget.visit(); current.lookups.set(identity, lookupRow(node.table, String(key), identity, current));
          }
          return mapResult(current.lookups.get(identity), row => row ? select(env(row, current.memory)) : undefined);
        });
      }
      case 'related': {
        if (!['first', 'last', 'array'].includes(node.reduce)) throw Error('Invalid local view reduction');
        const match = value(node.match, name, depth + 1), select = value(node.value, node.table, depth + 1), rows = relation(node, depth, select, node.reduce);
        return current => mapResult(match(current), match => rows(match, current));
      }
      default: throw Error('Unsupported local view value');
    }
  }
  function predicate(node: LocalViewPredicate, name: string, depth = 0): Predicate {
    const test = compilePredicate(node, name, depth);
    return current => { budget.visit(); return test(current); };
  }
  function compilePredicate(node: LocalViewPredicate, name: string, depth: number): Predicate {
    depthCheck(depth);
    if (!node || typeof node !== 'object') throw Error('Invalid local view predicate');
    switch (node.kind) {
      case 'constant': if (typeof node.value !== 'boolean') throw Error('Invalid local view constant'); return () => node.value;
      case 'and': case 'or': {
        if (!Array.isArray(node.terms)) throw Error('Invalid local view group');
        const terms = node.terms.map(child => predicate(child, name, depth + 1));
        return current => firstResult(terms, test => test(current), accepted => accepted === (node.kind === 'or'), node.kind === 'and');
      }
      case 'not': { const test = predicate(node.term, name, depth + 1); return current => mapResult(test(current), accepted => !accepted); }
      case 'test': {
        const read = value(node.value, name, depth + 1);
        if (!['empty', 'blank', 'finite', 'truthy', 'array'].includes(node.op)) throw Error('Invalid local view test');
        return current => mapResult(read(current), input => node.op === 'empty' ? empty(input) : node.op === 'blank' ? blank(input) : node.op === 'finite' ? typeof input === 'number' && Number.isFinite(input) : node.op === 'array' ? Array.isArray(input) : Boolean(input));
      }
      case 'arrayContains': {
        if (!['strict', 'string'].includes(node.mode)) throw Error('Invalid local view array membership');
        const array = value(node.array, name, depth + 1), read = value(node.value, name, depth + 1);
        return current => mapResult(array(current), items => mapResult(read(current), expected => Array.isArray(items) && items.some(item => node.mode === 'strict' ? item === expected : String(item) === String(expected))));
      }
      case 'text': {
        if (!['contains', 'startsWith', 'endsWith'].includes(node.op) || typeof node.text !== 'string') throw Error('Invalid local view text predicate');
        const read = value(node.value, name, depth + 1);
        return current => mapResult(read(current), input => String(input ?? '')[node.op === 'contains' ? 'includes' : node.op](node.text));
      }
      case 'in': {
        if (!Array.isArray(node.values) || !['strict', 'string'].includes(node.mode)) throw Error('Invalid local view membership');
        for (const item of node.values) {
          validJSON(item); if (item !== null && typeof item === 'object') throw Error('Local view membership requires scalar values');
        }
        const read = value(node.value, name, depth + 1), keys = new Set(node.values.map(item => node.mode === 'string' ? String(item) : item));
        reserve(cost(node.values));
        const has = (input: unknown) => keys.has((node.mode === 'string' ? String(input ?? '') : input) as JsonValue);
        return current => mapResult(read(current), input => node.array && Array.isArray(input) ? input.some(has) : has(input));
      }
      case 'compare': {
        if (!['eq', 'ne', 'lt', 'lte', 'gt', 'gte'].includes(node.op) || !['strict', 'number', 'natural'].includes(node.mode)) throw Error('Invalid local view comparison');
        if (node.mode === 'strict' && !['eq', 'ne'].includes(node.op)) throw Error('Strict local view comparison requires equality');
        const left = value(node.left, name, depth + 1), right = value(node.right, name, depth + 1);
        return current => mapResult(left(current), a => mapResult(right(current), b => {
          if (node.mode === 'strict') {
            return node.op === 'eq' ? a === b : a !== b;
          }
          if (node.mode === 'number' && (!Number.isFinite(a) || !Number.isFinite(b))) return false;
          const order = node.mode === 'natural' ? natural(a, b) : a! < b! ? -1 : a! > b! ? 1 : 0;
          return node.op === 'eq' ? order === 0 : node.op === 'ne' ? order !== 0 : node.op === 'lt' ? order < 0 : node.op === 'lte' ? order <= 0 : node.op === 'gt' ? order > 0 : order >= 0;
        }));
      }
      case 'exists': {
        const match = value(node.match, name, depth + 1), rows = relation(node, depth);
        return current => mapResult(match(current), match => mapResult(rows(match, current), found => Boolean(found)));
      }
      default: throw Error('Unsupported local view predicate');
    }
  }
  require(table, key); if (view.project === undefined) for (const column of columns) require(table, column);
  const matches = view.where ? predicate(view.where, table) : () => true;
  if (view.countDistinct !== undefined && (typeof view.countDistinct !== 'string' || !view.countDistinct
    || offset !== 0 || limit !== 0 || ranges !== undefined || view.groupBy !== undefined || view.orderBy.length
    || !Array.isArray(view.project) || view.project.length)) throw Error('Invalid local view distinct count');
  const distinctValue = view.countDistinct === undefined ? undefined : value({ kind: 'field', column: view.countDistinct }, table);
  const orderedDistinct = distinctValue !== undefined && source.rootDistinctOrder === view.countDistinct;
  const distinctKeys = distinctValue && !orderedDistinct ? new CompactRelationSet(reserve, budget.visit) : undefined;
  let lastDistinct: string | undefined;
  const groupValue = view.groupBy === undefined ? undefined : value(view.groupBy, table);
  const groups = groupValue ? new Map<string, SourcePosition & { count: number }>() : undefined;
  const groupOrderCost = (value: SourcePosition) => value.sourceOrder === undefined ? 0 : cost({ id: value.id, order: value.sourceOrder });
  const orders = view.orderBy.map(order => {
    if (!['asc', 'desc'].includes(order.direction) || !['natural', 'number', 'replica'].includes(order.mode) || order.blanks !== undefined && order.blanks !== 'last') throw Error('Invalid local view order');
    return { ...order, read: value(order.value, table) };
  });
  // Replica ordering already breaks every tie by unique row ID. Ungrouped
  // views with that order need no additional source label in their candidates.
  const retainSourceOrder = groupValue !== undefined || !orders.some(order => order.mode === 'replica');
  if (view.project !== undefined && !Array.isArray(view.project)) throw Error('Invalid local view projection');
  const projection = view.project?.map(item => value(item, table));
  for (const name of localViewDependencies(table, view)) {
    const known = coverage[name];
    if (!known || known.key !== source.key(name) || !readIsCovered(known, { table: name })
      || [...required.get(name) ?? []].some(column => known.columns && !known.columns.includes(column))) partial = true;
  }
  let total = 0;
  const readRow = (row: Record<string, JsonValue>, position: number, collect = true): MaybePromise<Candidate | undefined> => {
    budget.visit(); const current = env(row);
    const sourceOrder = source.rootOrder?.(row);
    if (sourceOrder && (typeof sourceOrder.order !== 'string' || sourceOrder.id !== String(row[key]))) throw Error('Invalid local view source order');
    return scoped(current, () => mapResult(matches(current), accepted => {
      if (!accepted) return;
      if (view.project === undefined && columns.some(column => !Object.hasOwn(row, column))) partial = true;
      if (distinctValue) return mapResult(distinctValue(current), (item): Candidate | undefined => {
        if (item == null) return;
        if (!['string', 'number', 'boolean'].includes(typeof item) || typeof item === 'number' && !Number.isFinite(item)) throw Error('Invalid local view distinct value');
        if (!collect) return;
        if (orderedDistinct) {
          if (typeof item !== 'string' || lastDistinct !== undefined && item < lastDistinct) throw Error('Invalid local view distinct order');
          if (item === lastDistinct) return;
          reserve(cost(item) - (lastDistinct === undefined ? 0 : cost(lastDistinct))); lastDistinct = item;
        } else {
          if (distinctKeys!.has(item)) return;
          distinctKeys!.add(item);
        }
        total++;
        return;
      });
      return mapResult(readValues(orders, order => mapResult(order.read(current), value => {
        if (order.mode === 'number' && value != null && (typeof value !== 'number' || !Number.isFinite(value))) throw Error('Invalid local view numeric sort value');
        return value;
      })), values => mapResult(collect && groupValue ? groupValue(current) : undefined, group => {
        // Retain the ID once and add only its ordering label. Build candidates
        // after predicate acceptance, rather than for every rejected root row.
        const candidate: Candidate = { id: String(row[key]), values, position,
          ...(sourceOrder && retainSourceOrder ? { sourceOrder: sourceOrder.order } : {}) };
        if (collect && groupValue && groups) {
          if (group != null && !['string', 'number', 'boolean'].includes(typeof group)
            || typeof group === 'number' && !Number.isFinite(group)) throw Error('Invalid local view group value');
          const name = String(group ?? '');
          const previous = groups.get(name);
          if (!previous) {
            reserve(cost(name) + 64 + groupOrderCost(candidate));
            groups.set(name, { position, sourceOrder: candidate.sourceOrder, id: sourceOrder ? candidate.id : '', count: 1 });
          } else {
            previous.count++;
            if (compareSourcePosition(candidate, previous) < 0) {
              reserve(groupOrderCost(candidate) - groupOrderCost(previous));
              previous.position = position; previous.id = sourceOrder ? candidate.id : ''; previous.sourceOrder = candidate.sourceOrder;
            }
          }
        }
        if (collect) total++;
        return candidate;
      }));
    }));
  };
  const compare = (left: Candidate, right: Candidate) => {
    for (let i = 0; i < orders.length; i++) {
      const order = orders[i]!, a = left.values[i], b = right.values[i];
      if (order.mode === 'replica') {
        const delta = compareReplicaMembershipValues(a, b, left.id, right.id, order.direction);
        if (delta) return delta; continue;
      }
      if (order.blanks === 'last' && (blank(a) || blank(b))) { if (blank(a) !== blank(b)) return blank(a) ? 1 : -1; continue; }
      const delta = order.mode === 'natural' ? natural(a, b) : a === b ? 0 : a == null ? 1 : b == null ? -1 : Number(a) < Number(b) ? -1 : Number(a) > Number(b) ? 1 : 0;
      if (delta) return order.direction === 'desc' ? -delta : delta;
    }
    return compareSourcePosition(left, right);
  };
  const readSelected = (item: Candidate) => {
    budget.visit();
    if (projection?.length === 0) return { id: item.id, values: [] as JsonValue[] };
    return mapResult(source.get(table, item.id), row => {
      if (!row || String(row[key]) !== item.id) throw Error('Local view selected row disappeared');
      if (!projection) return row;
      const current = env(row);
      return scoped(current, () => mapResult(readValues(projection, read => mapResult(read(current), value => {
        const item = value ?? null; validJSON(item); return item as JsonValue;
      })), values => ({ id: item.id, values })));
    });
  };
  return { ranges, offset, limit, projection, reserve, cost, readRow, compare, readSelected,
    total: () => total, unsupported,
    finish: (rows: Record<string, JsonValue>[], projected: { id: string; values: JsonValue[] }[] | undefined, positions: number[]): OfflineLiveQueryResult<Record<string, JsonValue>> =>
      unsupported ? { rows: [], supported: false, completeness: 'partial', unsupportedOperator: unsupported }
      : { rows, ...(projected ? { projected } : {}), total, offset, limit, supported: true, completeness: partial ? 'partial' : 'complete',
        ...(groups ? { groups: [...groups].sort(([, a], [, b]) => compareSourcePosition(a, b)).map(([key, { count }]) => ({ key, count })) } : {}), ...(ranges ? { positions } : {}) },
  };
}

function localViewPage(selection: ReturnType<typeof createLocalViewEvaluation>, page: Candidate[], selected: Candidate[], positions: number[], offset: number) {
  selection.reserve(page.reduce((sum, item) => sum + selection.cost(item), 0));
  for (let index = 0; index < page.length; index++) {
    selected.push(page[index]!);
    if (selection.ranges) { selection.reserve(16); positions.push(offset + index); }
  }
}
function localViewOutput(selection: ReturnType<typeof createLocalViewEvaluation>) {
  const rows: Record<string, JsonValue>[] = [], projected: { id: string; values: JsonValue[] }[] | undefined = selection.projection ? [] : undefined;
  return { rows, projected, add(value: Awaited<ReturnType<typeof selection.readSelected>>) {
    selection.reserve(selection.cost(value));
    if (projected) projected.push(value as { id: string; values: JsonValue[] }); else rows.push(value as Record<string, JsonValue>);
  } };
}

export function readLocalView(source: LocalViewSource, table: string, key: string, view: LocalViewPlan,
  coverage: ReadCoverage, context: Readonly<Record<string, JsonValue>> | undefined,
  budget: { candidateBytes: number; visit(): void }, columns: readonly string[] = []): OfflineLiveQueryResult<Record<string, JsonValue>> {
  const selection = createLocalViewEvaluation(source, table, key, view, coverage, context, budget, columns);
  if (selection.unsupported) return selection.finish([], undefined, []);
  const { ranges, offset, limit, compare, cost, reserve } = selection;
  function* candidates(collect = true) {
    let position = 0;
    for (const row of source.rootRows?.() ?? source.rows(table)) {
      const candidate = immediate(selection.readRow(row, position, collect));
      if (candidate) { position++; yield candidate; }
    }
  }
  const countFirst = ranges !== undefined || offset > 0 && limit > 0;
  if (countFirst) for (const _ of candidates()) { /* Count before selecting sparse/deep windows. */ }
  const selected: Candidate[] = [], positions: number[] = [];
  for (const window of ranges ?? [{ offset, limit }]) {
    let candidateBytes = 0;
    const reserveCandidate = (bytes: number) => { candidateBytes += bytes; reserve(bytes); };
    let page: Candidate[];
    if (countFirst) {
      const tail = Math.max(0, selection.total() - window.offset), fromEnd = tail < window.offset + window.limit;
      page = tail === 0 || window.limit === 0 ? [] : fromEnd
        ? smallestRows(candidates(false), tail, (a, b) => -compare(a, b), cost, reserveCandidate).reverse().slice(0, window.limit)
        : smallestRows(candidates(false), window.offset + window.limit, compare, cost, reserveCandidate).slice(window.offset, window.offset + window.limit);
    } else page = smallestRows(candidates(), window.limit, compare, cost, reserveCandidate);
    reserve(-candidateBytes); localViewPage(selection, page, selected, positions, window.offset);
  }
  const output = localViewOutput(selection);
  for (const item of selected) output.add(immediate(selection.readSelected(item)));
  return selection.finish(output.rows, output.projected, positions);
}

/** The caller holds one database read transaction across every page/pass and
 * point read. This API never materializes the complete root-row stream. */
export async function readLocalViewAsync(source: AsyncLocalViewSource, table: string, key: string, view: LocalViewPlan,
  coverage: ReadCoverage, context: Readonly<Record<string, JsonValue>> | undefined,
  budget: { candidateBytes: number; visit(): void }, columns: readonly string[] = []): Promise<OfflineLiveQueryResult<Record<string, JsonValue>>> {
  const selection = createLocalViewEvaluation(source, table, key, view, coverage, context, budget, columns, true);
  if (selection.unsupported) return selection.finish([], undefined, []);
  const { ranges, offset, limit, compare, cost, reserve } = selection;
  if (source.rootValidationRows) await visitRows(source.rootValidationRows(), row => mapResult(selection.readRow(row, 0, false), () => undefined));
  const candidates = (collect: boolean, accept: (candidate: Candidate) => void) => {
    let position = 0;
    return visitRows(source.rootRows?.() ?? source.rows(table), row => mapResult(selection.readRow(row, position, collect), candidate => {
      if (candidate) { position++; accept(candidate); }
    }));
  };
  const countFirst = ranges !== undefined || offset > 0 && limit > 0;
  if (countFirst) await candidates(true, () => {});
  const selected: Candidate[] = [], positions: number[] = [];
  for (const window of ranges ?? [{ offset, limit }]) {
    let candidateBytes = 0;
    const reserveCandidate = (bytes: number) => { candidateBytes += bytes; reserve(bytes); };
    const tail = Math.max(0, selection.total() - window.offset), fromEnd = countFirst && tail < window.offset + window.limit;
    let page: Candidate[] = [];
    if (!countFirst || tail > 0 && window.limit > 0) {
      const heap = smallestRowSelection<Candidate>(countFirst ? fromEnd ? tail : window.offset + window.limit : window.limit,
        fromEnd ? (a, b) => -compare(a, b) : compare, cost, reserveCandidate);
      await candidates(!countFirst, heap.add);
      page = heap.finish();
      if (countFirst) page = fromEnd ? page.reverse().slice(0, window.limit) : page.slice(window.offset, window.offset + window.limit);
    }
    reserve(-candidateBytes); localViewPage(selection, page, selected, positions, window.offset);
  }
  const output = localViewOutput(selection);
  for (const item of selected) output.add(await selection.readSelected(item));
  return selection.finish(output.rows, output.projected, positions);
}
