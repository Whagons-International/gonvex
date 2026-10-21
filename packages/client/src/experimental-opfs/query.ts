import { assert, decode, encode, hex, prefixEnd, unhex } from "./codec.js";
import {
  canonical,
  dirtyPrefix,
  gramPrefix,
  rowKey,
  rowPrefix,
  trigrams,
  indexPrefix,
  scalarKey,
  readPointer,
} from "./keys.js";
import { postingBits } from "./postings.js";
import { validJSON } from "./codec.js";
import type {
  OPFSReplicaEngine,
  ReplicaSchema,
  Row,
  Json,
  FieldKind,
} from "./replica.js";

export type Scalar = string | number | boolean | null;
export type Predicate =
  | {
      op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte";
      field: string;
      value: Scalar;
    }
  | { op: "and" | "or"; children: readonly Predicate[] }
  | { op: "not"; child: Predicate }
  | { op: "in"; field: string; values: readonly Scalar[] }
  | { op: "arrayContains"; field: string; value: Scalar }
  | { op: "jsonEq"; field: string; value: Json }
  | { op: "contains"; field: string; value: string }
  | {
      op: "relation";
      field: string;
      table: string;
      relatedField: string;
      where?: Predicate;
    };
export type Order = { field: string; direction: "asc" | "desc" };
export type PageCursor = {
  sequence: number;
  scope: string;
  plan: string;
  key: string;
  id: string;
};
export type PagePlan = {
  table: string;
  where?: Predicate;
  order: readonly Order[];
  search?: string;
  limit: number;
  select?: readonly string[];
  textOrder?: "binary";
  after?: PageCursor;
};
export type Page = {
  rows: Row[];
  next: PageCursor | null;
  sequence: number;
  examined: number;
  access: "trigram" | "index" | "scan";
};
export type PageBudget = {
  pageBytes?: number;
  candidateBytes?: number;
  examined?: number;
};
type Work = { examined: number; max: number };
const own = (value: object, key: string) => Object.hasOwn(value, key);
const spend = (work: Work) => {
  assert(++work.examined <= work.max, "Query work budget");
};
const value = (row: Row, field: string): Json =>
  own(row, field) ? row[field]! : null;
const cmp = (a: Scalar, b: Scalar): number | null =>
  a === null || b === null || typeof a !== typeof b
    ? null
    : a < b
      ? -1
      : a > b
        ? 1
        : 0;
const scalarType = (kind: FieldKind, input: unknown) =>
  input === null ||
  (kind !== "json" &&
    typeof input === (kind === "text" ? "string" : kind) &&
    (typeof input !== "number" || Number.isFinite(input)));

function validate(schema: ReplicaSchema, plan: PagePlan): string {
  assert(
    plan &&
      typeof plan === "object" &&
      Object.keys(plan).every((key) =>
        [
          "table",
          "where",
          "order",
          "search",
          "limit",
          "select",
          "textOrder",
          "after",
        ].includes(key),
      ),
    "Unsupported query plan",
  );
  assert(
    own(schema, plan.table) &&
      Number.isSafeInteger(plan.limit) &&
      plan.limit >= 1 &&
      plan.limit <= 256 &&
      Array.isArray(plan.order) &&
      plan.order.length >= 1 &&
      plan.order.length <= 8,
    "Query shape/limit",
  );
  const table = schema[plan.table]!;
  for (const order of plan.order) {
    assert(
      order &&
        own(table.fields, order.field) &&
        table.fields[order.field] !== "json" &&
        ["asc", "desc"].includes(order.direction),
      "Unsupported sort",
    );
    if (table.fields[order.field] === "text")
      assert(
        plan.textOrder === "binary",
        "Text sort requires explicit binary collation",
      );
  }
  if (plan.select !== undefined) {
    assert(
      Array.isArray(plan.select) && plan.select.length <= 64,
      "Projection budget",
    );
    for (const field of plan.select)
      assert(own(table.fields, field), "Unknown projection field");
  }
  if (plan.search !== undefined)
    assert(
      typeof plan.search === "string" &&
        plan.search.length <= 512 &&
        table.search?.length,
      "Search plan",
    );
  let nodes = 0;
  const visit = (name: string, predicate: Predicate, depth = 0) => {
    assert(
      ++nodes <= 128 &&
        depth <= 16 &&
        predicate &&
        typeof predicate === "object",
      "Predicate structure budget",
    );
    const table = schema[name]!;
    if (predicate.op === "and" || predicate.op === "or") {
      assert(Array.isArray(predicate.children), "Predicate children");
      for (const child of predicate.children) visit(name, child, depth + 1);
      return;
    }
    if (predicate.op === "not") {
      visit(name, predicate.child, depth + 1);
      return;
    }
    assert(
      "field" in predicate && own(table.fields, predicate.field),
      "Unknown predicate field",
    );
    const kind = table.fields[predicate.field]!;
    if (predicate.op === "relation") {
      assert(
        own(schema, predicate.table) &&
          own(schema[predicate.table]!.fields, predicate.relatedField) &&
          kind !== "json" &&
          kind === schema[predicate.table]!.fields[predicate.relatedField],
        "Relation field types",
      );
      if (predicate.where) visit(predicate.table, predicate.where, depth + 1);
    } else if (predicate.op === "in") {
      assert(
        Array.isArray(predicate.values) &&
          predicate.values.length <= 256 &&
          predicate.values.every((input) => scalarType(kind, input)),
        "IN predicate",
      );
    } else if (predicate.op === "jsonEq") {
      validJSON(predicate.value);
      assert(
        kind === "json" && encode(canonical(predicate.value)).length <= 16384,
        "JSON predicate",
      );
    } else if (predicate.op === "arrayContains") {
      validJSON(predicate.value);
      assert(
        kind === "json" &&
          (predicate.value === null ||
            ["string", "boolean", "number"].includes(typeof predicate.value)),
        "Array predicate",
      );
    } else if (predicate.op === "contains") {
      assert(
        kind === "text" &&
          typeof predicate.value === "string" &&
          predicate.value.length <= 512,
        "Substring predicate",
      );
    } else
      assert(
        ["eq", "ne", "lt", "lte", "gt", "gte"].includes(predicate.op) &&
          "value" in predicate &&
          scalarType(kind, predicate.value),
        "Scalar predicate",
      );
  };
  if (plan.where) visit(plan.table, plan.where);
  const identity = canonical({ ...plan, after: undefined });
  assert(encode(identity).length <= 16384, "Query plan byte budget");
  return identity;
}

/** SQL's UNKNOWN is retained through NOT/AND/OR, rather than accidentally
 * turning nullable inequalities into matches. eq/ne NULL use IS [NOT] NULL. */
function matches(
  engine: OPFSReplicaEngine,
  row: Row,
  predicate: Predicate,
  work: Work,
): boolean | null {
  spend(work);
  switch (predicate.op) {
    case "and": {
      let unknown = false;
      for (const child of predicate.children) {
        const result = matches(engine, row, child, work);
        if (result === false) return false;
        unknown ||= result === null;
      }
      return unknown ? null : true;
    }
    case "or": {
      let unknown = false;
      for (const child of predicate.children) {
        const result = matches(engine, row, child, work);
        if (result === true) return true;
        unknown ||= result === null;
      }
      return unknown ? null : false;
    }
    case "not": {
      const result = matches(engine, row, predicate.child, work);
      return result === null ? null : !result;
    }
    case "relation": {
      const target = value(row, predicate.field);
      if (target === null) return null;
      const prefix = rowPrefix(predicate.table);
      for (const item of engine.database.range(prefix, prefixEnd(prefix))) {
        spend(work);
        const related = JSON.parse(decode(item.value)) as Row;
        if (
          value(related, predicate.relatedField) === target &&
          (!predicate.where ||
            matches(engine, related, predicate.where, work) === true)
        )
          return true;
      }
      return false;
    }
    case "arrayContains": {
      const array = value(row, predicate.field);
      return (
        Array.isArray(array) && array.some((item) => item === predicate.value)
      );
    }
    case "jsonEq":
      return (
        own(row, predicate.field) &&
        canonical(row[predicate.field]) === canonical(predicate.value)
      );
    case "contains": {
      const actual = value(row, predicate.field);
      return typeof actual === "string"
        ? actual.toLowerCase().includes(predicate.value.toLowerCase())
        : null;
    }
    case "in": {
      const actual = value(row, predicate.field) as Scalar;
      if (!predicate.values.length) return false;
      if (actual === null) return null;
      if (
        predicate.values.some(
          (item) => item !== null && cmp(actual, item) === 0,
        )
      )
        return true;
      return predicate.values.includes(null) ? null : false;
    }
    default: {
      const actual = value(row, predicate.field) as Scalar;
      if (
        predicate.value === null &&
        (predicate.op === "eq" || predicate.op === "ne")
      )
        return (actual === null) === (predicate.op === "eq");
      const order = cmp(actual, predicate.value);
      if (order === null) return null;
      switch (predicate.op) {
        case "eq":
          return order === 0;
        case "ne":
          return order !== 0;
        case "lt":
          return order < 0;
        case "lte":
          return order <= 0;
        case "gt":
          return order > 0;
        case "gte":
          return order >= 0;
      }
    }
  }
}
function textKey(text: string): string {
  return hex(text) + "!";
}
function sortKey(engine: OPFSReplicaEngine, row: Row, plan: PagePlan): string {
  let key = "";
  for (const order of plan.order) {
    const item = value(row, order.field),
      desc = order.direction === "desc";
    if (item === null) {
      key += desc ? "0" : "2";
      continue;
    }
    key += "1";
    const part = scalarKey(item).slice(1);
    key += desc
      ? Array.from(part, (char) =>
          String.fromCharCode(127 - char.charCodeAt(0)),
        ).join("")
      : part;
  }
  return key + textKey(row[engine.schema[plan.table]!.key] as string);
}

export function runPage(
  engine: OPFSReplicaEngine,
  plan: PagePlan,
  budget: PageBudget = {},
): Page {
  const identity = validate(engine.schema, plan),
    sequence = engine.database.sequence;
  if (plan.after)
    assert(
      plan.after.sequence === sequence &&
        plan.after.scope === engine.scope &&
        plan.after.plan === identity &&
        typeof plan.after.key === "string" &&
        plan.after.key.length <= VALUE_KEY_BUDGET,
      "Stale or foreign query cursor",
    );
  let anchor: Row | undefined;
  if (plan.after) {
    assert(typeof plan.after.id === "string", "Invalid cursor identity");
    anchor = engine.entity(plan.table, plan.after.id);
    assert(
      anchor && sortKey(engine, anchor, plan) === plan.after.key,
      "Invalid cursor sort anchor",
    );
  }
  const pageBytes = budget.pageBytes ?? 262144,
    candidateBytes = budget.candidateBytes ?? 4 * 1024 * 1024;
  const work: Work = { examined: 0, max: budget.examined ?? 10000000 };
  for (const limit of [pageBytes, candidateBytes, work.max])
    assert(Number.isSafeInteger(limit) && limit >= 0, "Invalid query budget");
  type Candidate = { key: string; body: Uint8Array; cost: number };
  const heap: Candidate[] = [];
  let bytes = 0,
    more = false;
  const push = (candidate: Candidate) => {
    let i = heap.length;
    heap.push(candidate);
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      if (heap[parent]!.key >= candidate.key) break;
      heap[i] = heap[parent]!;
      i = parent;
    }
    heap[i] = candidate;
  };
  const pop = () => {
    const first = heap[0]!,
      last = heap.pop()!;
    if (heap.length) {
      let i = 0;
      while (i * 2 + 1 < heap.length) {
        let child = i * 2 + 1;
        if (child + 1 < heap.length && heap[child + 1]!.key > heap[child]!.key)
          child++;
        if (heap[child]!.key <= last.key) break;
        heap[i] = heap[child]!;
        i = child;
      }
      heap[i] = last;
    }
    return first;
  };
  const search = plan.search?.toLowerCase();
  const consider = (body: Uint8Array, decoded?: Row) => {
    spend(work);
    const row = decoded ?? (JSON.parse(decode(body)) as Row);
    if (plan.where && matches(engine, row, plan.where, work) !== true)
      return true;
    if (
      search &&
      !(engine.schema[plan.table]!.search ?? []).some(
        (field) =>
          typeof row[field] === "string" &&
          row[field].toLowerCase().includes(search),
      )
    )
      return true;
    const key = sortKey(engine, row, plan);
    assert(key.length <= VALUE_KEY_BUDGET, "Sort key byte budget");
    if (plan.after && key <= plan.after.key) return true;
    if (heap.length === plan.limit) {
      more = true;
      if (key >= heap[0]!.key) return false;
      bytes -= pop().cost;
    }
    const cost = body.length + key.length * 2 + 96;
    assert(bytes + cost <= candidateBytes, "Query candidate byte budget");
    bytes += cost;
    push({ key, body, cost });
    return true;
  };
  const grams = search ? new Set(trigrams(search)) : new Set<string>();
  let access: Page["access"] = "scan";
  let best: string | undefined,
    min = Infinity;
  if (grams.size) {
    for (const gram of grams) {
      const prefix = gramPrefix(plan.table, gram),
        raw = engine.database.get(prefixEnd(prefix)),
        count = raw ? Number(decode(raw)) : 0;
      assert(
        Number.isSafeInteger(count) && count >= 0,
        "Corrupt posting cardinality",
      );
      if (count < min) {
        best = prefix;
        min = count;
      }
    }
  }
  const indexed = chooseIndex(engine, plan);
  if (indexed?.ordered && anchor) {
    const columns =
      engine.schema[plan.table]!.indexes![indexed.number]!.columns;
    const seek =
      indexPrefix(plan.table, indexed.number) +
      columns.map((field) => scalarKey(anchor![field])).join("") +
      hex(plan.after!.id) +
      "!";
    if (seek > indexed.start) indexed.start = seek;
  }
  let useIndex = !!indexed && !best;
  if (indexed && best && min > 0) {
    let count = 0;
    for (const item of engine.database.range(indexed.start, indexed.end)) {
      spend(work);
      if (++count > min) break;
    }
    useIndex = count <= min;
  }
  const dirtyRows = () => {
    const prefix = dirtyPrefix(plan.table);
    for (const item of engine.database.range(prefix, prefixEnd(prefix))) {
      const row = engine.database.get(
        rowPrefix(plan.table) + item.key.slice(prefix.length),
      );
      if (row) consider(row);
    }
  };
  if (useIndex) {
    access = "index";
    dirtyRows();
    for (const item of engine.database.range(indexed!.start, indexed!.end)) {
      spend(work);
      const body = engine.database.frame(
          readPointer(item.value),
          engine.database.bytes,
          2,
        ),
        row = JSON.parse(decode(body)) as Row,
        id = row[engine.schema[plan.table]!.key];
      assert(typeof id === "string", "Corrupt indexed row identity");
      if (engine.database.get(dirtyPrefix(plan.table) + hex(id))) continue;
      if (!consider(body, row) && indexed!.ordered) break;
    }
  } else if (best) {
    access = "trigram";
    for (const item of engine.database.range(best, prefixEnd(best)))
      for (const bit of postingBits(item.value)) {
        spend(work);
        const bucket = Number.parseInt(item.key.slice(best!.length), 16),
          ordinal = bucket * 256 + bit;
        assert(Number.isSafeInteger(ordinal), "Corrupt posting ordinal");
        const reference = engine.database.get(
          "u/" + ordinal.toString(16).padStart(14, "0"),
        );
        assert(reference, "Missing posting row reference");
        const body = engine.database.frame(
            readPointer(reference),
            engine.database.bytes,
            2,
          ),
          row = JSON.parse(decode(body)) as Row,
          id = row[engine.schema[plan.table]!.key];
        assert(typeof id === "string", "Corrupt posting row identity");
        if (engine.database.get(dirtyPrefix(plan.table) + hex(id))) continue;
        consider(body, row);
      }
    dirtyRows();
  } else {
    const prefix = rowPrefix(plan.table);
    for (const item of engine.database.range(prefix, prefixEnd(prefix)))
      consider(item.value);
  }
  heap.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  let delivered = 64;
  const rows = heap.map((item) => {
    let row = JSON.parse(decode(item.body)) as Row;
    if (plan.select) {
      const selected = new Set([
        engine.schema[plan.table]!.key,
        ...plan.select,
      ]);
      row = Object.fromEntries(
        Object.entries(row).filter(([key]) => selected.has(key)),
      );
    }
    delivered += encode(JSON.stringify(row)).length + 1;
    assert(delivered <= pageBytes, "Query page byte budget");
    return row;
  });
  return {
    rows,
    next:
      more && heap.length
        ? {
            sequence,
            scope: engine.scope,
            plan: identity,
            key: heap.at(-1)!.key,
            id: (JSON.parse(decode(heap.at(-1)!.body)) as Row)[
              engine.schema[plan.table]!.key
            ] as string,
          }
        : null,
    sequence,
    examined: work.examined,
    access,
  };
}
const VALUE_KEY_BUDGET = 262144;

function chooseIndex(
  engine: OPFSReplicaEngine,
  plan: PagePlan,
):
  | { start: string; end: string; ordered: boolean; number: number }
  | undefined {
  const state = engine.metadata("m/indexes") as
    | Record<string, { valid: boolean[] }>
    | undefined;
  if (!state || !own(state, plan.table)) return;
  const equals = new Map<string, Scalar>();
  const visit = (predicate?: Predicate) => {
    if (predicate?.op === "and")
      for (const child of predicate.children) visit(child);
    else if (predicate?.op === "eq")
      equals.set(predicate.field, predicate.value);
  };
  visit(plan.where);
  let best:
    | {
        start: string;
        end: string;
        ordered: boolean;
        equalCount: number;
        number: number;
      }
    | undefined;
  for (const [number, index] of (
    engine.schema[plan.table]!.indexes ?? []
  ).entries()) {
    if (!state[plan.table]!.valid[number]) continue;
    let start = indexPrefix(plan.table, number),
      equalCount = 0;
    for (const field of index.columns) {
      if (!equals.has(field)) break;
      start += scalarKey(equals.get(field));
      equalCount++;
    }
    const remaining = index.columns.slice(equalCount),
      order = plan.order.filter((term) => !equals.has(term.field));
    const ordered =
      order.length === remaining.length &&
      order.every(
        (term, i) => term.field === remaining[i] && term.direction === "asc",
      );
    if (!equalCount && !ordered) continue;
    if (
      !best ||
      equalCount > best.equalCount ||
      (equalCount === best.equalCount && ordered)
    )
      best = { start, end: prefixEnd(start), ordered, equalCount, number };
  }
  return best;
}
