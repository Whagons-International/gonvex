// Tenant-local declared reference integrity: rust/crates/store/src/reference_integrity.rs.
//
// Applications own the catalog and deletion policy. Every declared table gets
// `AFTER INSERT/UPDATE/DELETE` triggers that stage row images in
// `_gonvex_reference_pending`. `flushReferences` (run by the writer before its
// transaction ends, and by the migration runner) evaluates the declarations,
// maintains the edge and target-token ledger, and lets SQLite's deferred
// foreign key from edges to targets reject a dangling reference before
// commit (K01's deferred-FK check, 0016 G0-5). Staged rows reference an
// always-empty guard table through another deferred FK, so a writer that
// never flushes cannot commit at all.
//
// Migrations declare references with
// `INSERT INTO _gonvex_reference_requests(declaration) VALUES('{…}')`;
// `afterMigration` applies requests in order inside the migration
// transaction. `SELECT * FROM gonvex_reference_violations` lists inventoried
// legacy violations.
//
// Semantics, ordering and error texts are Rust's. The SQL differs where the
// platform or the design rules ask for it:
//   - per-row statements over a list become one statement over a `json_each`
//     list (edge deletes and inserts of one row, configuration deletes and
//     the legacy check of one row, the inventory's configurations, target
//     counts and edges of a page);
//   - staged rows and inventoried table rows are read in pages;
//   - configuration rows and a page's existing edges are read once and kept
//     current as the ledger writes them;
//   - integers are written as literals or cast from TEXT (Durable Object SQL
//     binds every JS number as REAL).

import { integerLiteral, quoteIdentifier, scalar, type CaptureSql, type RawResult } from "../../capture/sql.ts";
import {
  charCount,
  compareTuples,
  equal,
  fromJs,
  get,
  isNumber,
  isObject,
  JsonError,
  parseJson,
  stringify,
  textOf,
  trimRust,
  type Json,
  type JsonObject,
} from "./json.ts";
import { JsonPath, PathError } from "./jsonpath.ts";
import {
  applicationTables,
  asciiUpper,
  columnExists,
  dropStagingTriggers,
  installStagingTriggers,
  jsonObjectExpr,
  kindOf,
  primaryKey,
  tableColumns,
  tableExists,
  tablesColumns,
  uniqueColumn,
} from "./schema.ts";

const STATES = ["exists", "live"] as const;
const CONFIGURATION_PREFIX = "$configuration:";
const EXCLUSION_KINDS = ["external", "enum", "local", "content", "snapshot", "derived", "lifecycle", "identity"];

/** Staged rows read per statement. */
const PENDING_PAGE = 256;
/** Application rows read per statement while inventorying a declaration. */
const INVENTORY_PAGE = 256;
/** Items per `json_each` list statement. */
const LIST_CHUNK = 500;

/** `DatabaseError::ReferenceIntegrity`; `message` is its Display text. */
export class ReferenceIntegrityError extends Error {
  override readonly name = "ReferenceIntegrityError";
  /** The inner text, without the `reference integrity: ` prefix. */
  readonly detail: string;
  constructor(detail: string) {
    super(`reference integrity: ${detail}`);
    this.detail = detail;
  }
}

const violation = (message: string) => new ReferenceIntegrityError(message);

/** A synchronous savepoint: the caller passes `fn => ctx.storage.transactionSync(fn)`. */
export type Atomic = <T>(fn: () => T) => T;
const inline: Atomic = (fn) => fn();

export interface DeclareOptions {
  readonly atomic?: Atomic;
}

export interface FlushStats {
  /** Staged row changes processed (and deleted). */
  readonly rows: number;
}

/** `serde_json::from_str`, a parse error mapped by `wrap`. */
function parseWith(text: string, wrap: (message: string) => Error): Json {
  try {
    return parseJson(text);
  } catch (error) {
    if (error instanceof JsonError) throw wrap(error.message);
    throw error;
  }
}

const tryParse = (text: string): Json | undefined => {
  try {
    return parseJson(text);
  } catch {
    return undefined;
  }
};

const strField = (object: Json | undefined, key: string): string | null => {
  const value = get(object, key);
  return typeof value === "string" ? value : null;
};

/** `->>` text of a field, including non-string scalars. */
const fieldText = (object: Json | undefined, key: string): string | null => textOf(get(object, key));

const reasonOk = (object: Json, key: string): boolean => {
  const reason = fieldText(object, key);
  return reason !== null && charCount(trimRust(reason)) >= 12;
};

function parsePath(source: string): JsonPath {
  try {
    return JsonPath.parse(source);
  } catch (error) {
    if (error instanceof PathError) throw violation(`invalid JSON path: ${error.message}`);
    throw error;
  }
}

/** `["row", version]`, the identity of one configuration version. */
export function bindingId(rowId: string, version: bigint | null): string {
  return stringify([rowId, version]);
}

/** The bound columns of a parent row. */
export function configuration(document: Json, columns: readonly string[]): JsonObject {
  return new Map(columns.map((column) => [column, get(document, column) ?? null]));
}

const isFrameworkTable = (name: string) => name.startsWith("_gonvex") || name.startsWith("gonvex_");

/** A JSON list parameter for `json_each`. */
const list = (items: readonly unknown[]) => JSON.stringify(items);

function chunks<T>(items: readonly T[], size = LIST_CHUNK): T[][] {
  const out: T[][] = [];
  for (let index = 0; index < items.length; index += size) out.push(items.slice(index, index + size));
  return out;
}

interface TableDecl {
  readonly key: string;
  readonly live: JsonPath;
}

interface LookupStep {
  readonly binding: string;
  readonly from: string;
  readonly key: JsonPath;
  readonly when: JsonPath | null;
  readonly required: boolean;
}

class Rule {
  private constructor(
    readonly name: string,
    readonly source: string,
    readonly state: string,
    readonly declaration: Json,
    readonly at: JsonPath,
    readonly paths: JsonPath[],
    readonly sourceWhen: JsonPath | null,
    readonly lookup: LookupStep[] | null,
    readonly discriminator: JsonPath | null,
  ) {}

  static compile(declaration: Json): Rule {
    const name = fieldText(declaration, "name") ?? "";
    const pathsValue = get(declaration, "paths");
    let paths: string[];
    if (Array.isArray(pathsValue)) {
      paths = pathsValue.map((path) => {
        const text = textOf(path);
        if (text === null) throw violation(`reference ${name} has a null path`);
        return text;
      });
    } else {
      const path = fieldText(declaration, "path");
      if (path === null) throw violation(`reference ${name} has a null path`);
      paths = [path];
    }
    const lookupValue = get(declaration, "lookup");
    let lookup: LookupStep[] | null = null;
    if (lookupValue !== undefined && lookupValue !== null) {
      const steps = Array.isArray(lookupValue) ? lookupValue : [lookupValue];
      lookup = steps.map((step) => {
        const when = fieldText(step, "when");
        return {
          binding: fieldText(step, "binding") ?? "",
          from: fieldText(step, "from") ?? "container",
          key: parsePath(fieldText(step, "key") ?? ""),
          when: when === null ? null : parsePath(when),
          required: get(step, "required") === true,
        };
      });
    }
    const source = fieldText(declaration, "source") ?? "";
    const state = fieldText(declaration, "state") ?? "";
    const at = parsePath(fieldText(declaration, "at") ?? "$");
    const compiled = paths.map(parsePath);
    const sourceWhen = fieldText(declaration, "sourceWhen");
    const sourceWhenPath = sourceWhen === null ? null : parsePath(sourceWhen);
    const discriminator = fieldText(declaration, "discriminator");
    const discriminatorPath = discriminator === null ? null : parsePath(discriminator);
    return new Rule(name, source, state, declaration, at, compiled, sourceWhenPath, lookup, discriminatorPath);
  }

  allowsNumbers(): boolean {
    return get(this.declaration, "allowNumbers") === true;
  }

  accepts(identifier: Json): boolean {
    return typeof identifier === "string" || (isNumber(identifier) && this.allowsNumbers());
  }
}

interface Binding {
  readonly name: string;
  readonly source: string;
  readonly columns: string[];
}

/** One stored configuration version. */
interface Configuration {
  readonly document: string;
  readonly version: bigint;
}

/** (rule, target table, target id, target state). */
type Edge = readonly [string, string, string, string];
/** (token, row id, state). */
type Target = readonly [string, string, string];

const tupleKey = (tuple: readonly string[]) => JSON.stringify(tuple);

function sortedUnique<T extends readonly string[]>(tuples: T[]): T[] {
  const seen = new Map<string, T>();
  for (const tuple of tuples) seen.set(tupleKey(tuple), tuple);
  return [...seen.values()].sort(compareTuples);
}

function nextVersions(sql: CaptureSql, count: number): bigint {
  const last = scalar(
    sql.run(
      `UPDATE _gonvex_reference_sequence SET last_version=last_version+${integerLiteral(count)} WHERE singleton=1 RETURNING CAST(last_version AS TEXT)`,
    ),
  );
  if (last === undefined || last === null) throw new Error("no rows returned by a query that expected to return at least one row");
  return BigInt(String(last));
}

/** reference_integrity.rs `next_version`. */
const nextVersion = (sql: CaptureSql) => nextVersions(sql, 1);

/**
 * reference_integrity.rs `apply_target_delta` for every nonzero delta of one
 * row (distinct targets). A positive delta adds owners and enforces
 * inventoried edges whose target now exists; a negative one needs the
 * target's ledger row and deletes it at zero. Rust applies them one target
 * at a time in key order; here decrements, then increments, each in one
 * statement per chunk. Only which error a corrupt ledger reports first can
 * differ.
 */
function applyTargetDeltas(sql: CaptureSql, deltas: readonly (readonly [Target, number])[]): void {
  const decrements = deltas.filter(([, delta]) => delta < 0);
  for (const chunk of chunks(decrements)) {
    const items = list(chunk.map(([[token, rowId, state], delta]) => [token, rowId, state, delta]));
    const remaining = sql.run(
      `UPDATE _gonvex_reference_targets SET owner_count=owner_count+j.value->>3
         FROM json_each(?1) j
         WHERE table_name=j.value->>0 AND row_id=j.value->>1 AND state=j.value->>2
         RETURNING owner_count`,
      [items],
    ).rows;
    if (remaining.length !== chunk.length) throw violation("reference target ledger is inconsistent");
    if (remaining.some((row) => Number(row[0]) === 0)) {
      sql.run(
        `DELETE FROM _gonvex_reference_targets WHERE owner_count=0
           AND (table_name,row_id,state) IN (SELECT value->>0, value->>1, value->>2 FROM json_each(?1))`,
        [items],
      );
    }
  }
  addTargets(sql, deltas.filter(([, delta]) => delta > 0));
}

/** Positive target counts: `apply_target_delta` for each, in two statements per chunk. */
function addTargets(sql: CaptureSql, counts: Iterable<readonly [Target, number]>): void {
  const entries = [...counts].sort(([a], [b]) => compareTuples(a, b));
  for (const chunk of chunks(entries)) {
    const items = list(chunk.map(([[token, rowId, state], count]) => [token, rowId, state, count]));
    sql.run(
      `INSERT INTO _gonvex_reference_targets(table_name,row_id,state,owner_count)
         SELECT value->>0, value->>1, value->>2, value->>3 FROM json_each(?1) WHERE true
         ON CONFLICT(table_name,row_id,state)
         DO UPDATE SET owner_count=_gonvex_reference_targets.owner_count+excluded.owner_count`,
      [items],
    );
    sql.run(
      `UPDATE _gonvex_reference_edges
         SET enforced_table=target_table,enforced_id=target_id,enforced_state=target_state
         WHERE enforced_table IS NULL
           AND (target_table,target_id,target_state) IN (SELECT value->>0, value->>1, value->>2 FROM json_each(?1))`,
      [items],
    );
  }
}

/**
 * Explicit adapters for existing selector encodings (the former
 * `_gonvex_reference_decode`). Plain content never opts in.
 */
export function decode(value: Json, normalization: string | null): Json[] {
  if (normalization === null) return [value];
  if (value === null) return [];
  switch (normalization) {
    case "task-relation":
      if (isObject(value)) return value.has("taskId") ? [value.get("taskId")!] : [];
      return [value];
    case "csv-or-json-list":
      break;
    default:
      throw violation("unsupported selector normalization");
  }
  let decoded: Json = value;
  if (typeof value === "string") {
    const raw = trimRust(value);
    if (raw.startsWith("[")) {
      const parsed = tryParse(raw);
      if (Array.isArray(parsed)) decoded = parsed;
    }
  }
  const scalarOf = (item: Json, container: string): Json[] => {
    if (typeof item === "string" || isNumber(item) || typeof item === "boolean") {
      const raw = trimRust(textOf(item) ?? "");
      return raw === "" ? [] : [raw];
    }
    throw violation(`selector ${container} needs scalar values`);
  };
  const output: Json[] = [];
  if (Array.isArray(decoded)) {
    for (const item of decoded) output.push(...scalarOf(item, "list"));
  } else if (isObject(decoded)) {
    for (const item of decoded.values()) output.push(...scalarOf(item, "map"));
  } else {
    for (const part of (textOf(decoded) ?? "").split(",")) {
      const trimmed = trimRust(part);
      if (trimmed !== "") output.push(trimmed);
    }
  }
  return output;
}

/** The installed declaration, loaded once per flush or declaration. */
class Ledger {
  /** Configuration rows read or written through this ledger, by (binding, row). */
  private readonly configurations = new Map<string, Configuration | null>();
  /** Edges of source rows, by (table, row): read once per page and kept current. */
  private readonly edges = new Map<string, Edge[]>();

  private constructor(
    private readonly sql: CaptureSql,
    readonly tables: Map<string, TableDecl>,
    readonly rules: Rule[],
    readonly keys: [string, string, string][],
    readonly bindings: Binding[],
  ) {}

  static load(sql: CaptureSql): Ledger {
    const tables = new Map<string, TableDecl>();
    for (const row of sql.run("SELECT table_name, key_column, live_when FROM _gonvex_reference_tables").rows) {
      tables.set(String(row[0]), { key: String(row[1]), live: parsePath(String(row[2])) });
    }
    const rules: Rule[] = [];
    for (const row of sql.run("SELECT declaration FROM _gonvex_reference_rules ORDER BY name").rows) {
      rules.push(Rule.compile(parseWith(String(row[0]), violation)));
    }
    const keys = sql
      .run("SELECT table_name, token_name, key_column FROM _gonvex_reference_keys ORDER BY table_name, token_name")
      .rows.map((row) => [String(row[0]), String(row[1]), String(row[2])] as [string, string, string]);
    const bindings: Binding[] = [];
    for (const row of sql.run("SELECT name, source_table, columns_ FROM _gonvex_reference_bindings ORDER BY name").rows) {
      const columns = parseWith(String(row[2]), violation);
      if (!Array.isArray(columns) || !columns.every((column) => typeof column === "string")) {
        throw violation("invalid type: expected a sequence of strings");
      }
      bindings.push({ name: String(row[0]), source: String(row[1]), columns: columns as string[] });
    }
    return new Ledger(sql, tables, rules, keys, bindings);
  }

  bindingsOf(source: string): Binding[] {
    return this.bindings.filter((binding) => binding.source === source);
  }

  /** `SELECT document, version FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2`. */
  configuration(binding: string, rowId: string): Configuration | null {
    const key = tupleKey([binding, rowId]);
    const cached = this.configurations.get(key);
    if (cached !== undefined) return cached;
    const row = this.sql.run(
      "SELECT document, CAST(version AS TEXT) FROM _gonvex_reference_configurations WHERE binding_name=?1 AND row_id=?2",
      [binding, rowId],
    ).rows[0];
    const found = row === undefined ? null : { document: String(row[0]), version: BigInt(String(row[1])) };
    this.configurations.set(key, found);
    return found;
  }

  /** Records a configuration row this ledger wrote (null: deleted). */
  remember(binding: string, rowId: string, value: Configuration | null): void {
    this.configurations.set(tupleKey([binding, rowId]), value);
  }

  /** Reads the edges of these source rows in one statement per chunk. */
  prefetchEdges(sources: readonly (readonly [string, string])[]): void {
    const wanted = sortedUnique(sources.filter(([table, id]) => !this.edges.has(tupleKey([table, id]))) as (readonly [string, string])[]);
    for (const chunk of chunks(wanted)) {
      for (const source of chunk) this.edges.set(tupleKey(source), []);
      const rows = this.sql.run(
        `SELECT e.source_table, e.source_id, e.rule_name, e.target_table, e.target_id, e.target_state
         FROM json_each(?1) j JOIN _gonvex_reference_edges e ON e.source_table=j.value->>0 AND e.source_id=j.value->>1`,
        [list(chunk)],
      ).rows;
      for (const row of rows) {
        this.edges.get(tupleKey([String(row[0]), String(row[1])]))?.push([String(row[2]), String(row[3]), String(row[4]), String(row[5])]);
      }
    }
  }

  private existingEdges(table: string, rowId: string): Edge[] {
    const key = tupleKey([table, rowId]);
    const cached = this.edges.get(key);
    if (cached !== undefined) return cached;
    const edges = this.sql
      .run(
        `SELECT rule_name, target_table, target_id, target_state FROM _gonvex_reference_edges
         WHERE source_table=?1 AND source_id=?2`,
        [table, rowId],
      )
      .rows.map((row) => [String(row[0]), String(row[1]), String(row[2]), String(row[3])] as const);
    this.edges.set(key, edges);
    return edges;
  }

  /** One staged row change (the former `_gonvex_reference_changed` trigger). `undefined` is SQL NULL. */
  changed(table: string, old: Json | undefined, next: Json | undefined): void {
    const declared = this.tables.get(table);
    if (declared === undefined) return;
    const oldId = old === undefined ? null : fieldText(old, declared.key);
    const newId = next === undefined ? null : fieldText(next, declared.key);
    const update = old !== undefined && next !== undefined;
    if (next === undefined || (update && oldId !== newId)) this.syncRow(table, oldId, undefined, old);
    if (next !== undefined) this.syncRow(table, newId, next, update && oldId === newId ? old : undefined);
  }

  /** The source rows `changed` will read edges for. */
  changedSources(table: string, old: Json | undefined, next: Json | undefined): [string, string][] {
    const declared = this.tables.get(table);
    if (declared === undefined) return [];
    const ids = [old === undefined ? null : fieldText(old, declared.key), next === undefined ? null : fieldText(next, declared.key)];
    return ids.filter((id): id is string => id !== null).map((id) => [table, id]);
  }

  private syncRow(table: string, rowId: string | null, document: Json | undefined, previous: Json | undefined): void {
    const desired = this.desired(table, document);
    if (rowId !== null) {
      // Preserve unchanged edges: an inventoried legacy violation must
      // not be silently repaired or re-enforced by an unrelated update.
      const wanted = new Set(desired.map(tupleKey));
      const stale = this.existingEdges(table, rowId).filter((edge) => !wanted.has(tupleKey(edge)));
      for (const chunk of chunks(stale)) {
        this.sql.run(
          `DELETE FROM _gonvex_reference_edges WHERE source_id=?1
             AND (rule_name,target_table,target_id,target_state) IN (SELECT value->>0, value->>1, value->>2, value->>3 FROM json_each(?2))`,
          [rowId, list(chunk)],
        );
      }
      for (const chunk of chunks(desired)) {
        this.sql.run(
          `INSERT INTO _gonvex_reference_edges
             (rule_name,source_table,source_id,target_table,target_id,target_state,
              enforced_table,enforced_id,enforced_state)
             SELECT value->>0, ?1, ?2, value->>1, value->>2, value->>3, value->>1, value->>2, value->>3
             FROM json_each(?3) WHERE true ON CONFLICT DO NOTHING`,
          [table, rowId, list(chunk)],
        );
      }
      this.edges.set(tupleKey([table, rowId]), desired);
    }
    this.syncTargets(table, previous, document);
  }

  /** Desired edges of one row, sorted and unique. */
  desired(source: string, document: Json | undefined): Edge[] {
    const table = this.tables.get(source);
    if (document === undefined || table === undefined) return [];
    const desired: Edge[] = [];
    for (const rule of this.rules) {
      if (rule.source !== source) continue;
      if ((rule.sourceWhen ?? table.live).matches(document) !== true) continue;
      for (const [targetTable, targetId] of this.values(document, rule)) desired.push([rule.name, targetTable, targetId, rule.state]);
    }
    return sortedUnique(desired);
  }

  /** Selected target identities of one rule (the former `_gonvex_reference_values`). */
  private values(original: Json, rule: Rule): [string, string][] {
    const name = rule.name;
    let document = original;
    // Some consumers accept both JSON values and JSON serialized in a
    // text column. Decoding is declared per reference, never guessed.
    if (strField(rule.declaration, "encoding") === "json-or-value") {
      const column = strField(rule.declaration, "column");
      if (column !== null) {
        const serialized = get(document, column);
        if (typeof serialized === "string" && isObject(document)) {
          const parsed = parseWith(serialized, () => violation(`reference ${name} has invalid serialized JSON`));
          document = new Map(document).set(column, parsed);
        }
      }
    }
    const selected: [string, string][] = [];
    containers: for (const container of rule.at.query(document)) {
      const vars: JsonObject = new Map([["entry", container], ["document", document]]);
      let configuration: Json | undefined;
      for (const step of rule.lookup ?? []) {
        const lookupDocument = step.from === "document" ? document : step.from === "configuration" ? configuration : container;
        let identifier = lookupDocument === undefined ? undefined : step.key.queryFirst(lookupDocument, vars);
        if (identifier === null) identifier = undefined;
        if (identifier === undefined) {
          if (step.required) throw violation(`reference ${name} needs a configuration key`);
          continue containers;
        }
        if (!rule.accepts(identifier)) throw violation(`reference ${name} needs a string configuration key`);
        const parentId = textOf(identifier) ?? "";
        const row = this.configuration(step.binding, parentId);
        selected.push([`${CONFIGURATION_PREFIX}${step.binding}`, bindingId(parentId, row?.version ?? null)]);
        configuration = row === null ? undefined : tryParse(row.document);
        // Missing legacy parents remain auditable violations. New
        // writes fail the native FK, including stale configuration.
        const applies = configuration !== undefined && (step.when === null ? true : step.when.matches(configuration, vars)) === true;
        if (!applies) continue containers;
      }
      for (const path of rule.paths) {
        for (const item of path.query(container)) {
          for (const identifier of decode(item, strField(rule.declaration, "normalize"))) {
            if (identifier === null) continue;
            if (!rule.accepts(identifier)) throw violation(`reference ${name} must contain string IDs`);
            const targetId = textOf(identifier) ?? "";
            if (targetId === "") {
              if (fieldText(rule.declaration, "allowEmpty") === "true") continue;
              throw violation(`reference ${name} contains an empty ID`);
            }
            const ignoreValues = get(rule.declaration, "ignoreValues");
            if (Array.isArray(ignoreValues) && ignoreValues.some((value) => value === targetId)) continue;
            let destination = fieldText(rule.declaration, "target");
            const targets = get(rule.declaration, "targets");
            if (targets !== undefined) {
              const scope = rule.lookup !== null ? configuration : container;
              const discriminator = scope !== undefined && rule.discriminator !== null ? rule.discriminator.queryFirst(scope, vars) : undefined;
              if (typeof discriminator !== "string") throw violation(`reference ${name} requires a target discriminator`);
              destination = fieldText(targets, discriminator);
            }
            if (destination === null) throw violation(`reference ${name} has an undeclared target type`);
            selected.push([destination, targetId]);
          }
        }
      }
    }
    return selected;
  }

  /** Target tokens a row owns (the former `_gonvex_reference_row_targets`). */
  rowTargets(source: string, document: Json | undefined): Target[] {
    const targets: Target[] = [];
    const table = this.tables.get(source);
    if (document === undefined || table === undefined) return targets;
    const live = table.live.matches(document) === true;
    const states = STATES.filter((state) => state === "exists" || live);
    for (const [owner, token, column] of this.keys) {
      if (owner !== source) continue;
      const value = fieldText(document, column);
      if (value === null) continue;
      for (const state of states) targets.push([token, value, state]);
    }
    const rowId = fieldText(document, table.key);
    if (rowId === null) return targets;
    for (const binding of this.bindingsOf(source)) {
      const row = this.configuration(binding.name, rowId);
      if (row === null) continue;
      const stored = tryParse(row.document);
      if (stored === undefined || !equal(stored, configuration(document, binding.columns))) continue;
      const id = bindingId(rowId, row.version);
      for (const state of states) targets.push([`${CONFIGURATION_PREFIX}${binding.name}`, id, state]);
    }
    return targets;
  }

  private syncTargets(source: string, previous: Json | undefined, document: Json | undefined): void {
    const table = this.tables.get(source);
    if (table === undefined) return;
    // Snapshot old tokens before updating the parent projection.
    // Configuration versions never repeat; comparing JSON retains the
    // version across unrelated parent edits.
    const previousTargets = this.rowTargets(source, previous);
    const previousId = previous === undefined ? null : fieldText(previous, table.key);
    const documentId = document === undefined ? null : fieldText(document, table.key);
    const bindings = this.bindingsOf(source);
    if (previousId !== null && bindings.length > 0 && (document === undefined || documentId !== previousId)) {
      this.sql.run(
        "DELETE FROM _gonvex_reference_configurations WHERE row_id=?1 AND binding_name IN (SELECT value FROM json_each(?2))",
        [previousId, list(bindings.map((binding) => binding.name))],
      );
      for (const binding of bindings) this.remember(binding.name, previousId, null);
    }
    if (document !== undefined && documentId !== null) {
      if (previous === undefined && bindings.length > 0) {
        // Installing a formerly missing parent must not reinterpret
        // legacy values whose domain was unknown at inventory.
        const legacy = scalar(
          this.sql.run(
            "SELECT EXISTS(SELECT 1 FROM _gonvex_reference_edges WHERE target_id=?1 AND target_table IN (SELECT value FROM json_each(?2)))",
            [bindingId(documentId, null), list(bindings.map((binding) => `${CONFIGURATION_PREFIX}${binding.name}`))],
          ),
        );
        if (Number(legacy) !== 0) throw violation("configuration has unresolved legacy dependents");
      }
      for (const binding of bindings) {
        const projected = configuration(document, binding.columns);
        const stored = this.configuration(binding.name, documentId);
        const parsed = stored === null ? undefined : tryParse(stored.document);
        if (parsed !== undefined && equal(parsed, projected)) continue;
        const version = nextVersion(this.sql);
        const text = stringify(projected);
        this.sql.run(
          `INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document,version)
             VALUES(?1,?2,?3,CAST(?4 AS INTEGER)) ON CONFLICT(binding_name,row_id)
             DO UPDATE SET document=excluded.document,version=excluded.version`,
          [binding.name, documentId, text, version.toString()],
        );
        this.remember(binding.name, documentId, { document: text, version });
      }
    }
    const deltas = new Map<string, [Target, number]>();
    const add = (target: Target, delta: number) => {
      const key = tupleKey(target);
      const entry = deltas.get(key);
      if (entry === undefined) deltas.set(key, [target, delta]);
      else entry[1] += delta;
    };
    for (const target of this.rowTargets(source, document)) add(target, 1);
    for (const target of previousTargets) add(target, -1);
    applyTargetDeltas(this.sql, [...deltas.values()].filter(([, delta]) => delta !== 0));
  }
}

/**
 * Processes staged row changes. Call inside the writing transaction before
 * it ends. A no-op on databases without a reference ledger or with nothing
 * staged.
 */
export function flushReferences(sql: CaptureSql): FlushStats {
  if (!tableExists(sql, "_gonvex_reference_pending")) return { rows: 0 };
  let ledger: Ledger | null = null;
  let last: string | null = null;
  let processed = 0;
  for (;;) {
    const page: RawResult["rows"] = sql.run(
      last === null
        ? `SELECT CAST(seq AS TEXT), table_name, old_doc, new_doc FROM _gonvex_reference_pending ORDER BY seq LIMIT ${PENDING_PAGE}`
        : `SELECT CAST(seq AS TEXT), table_name, old_doc, new_doc FROM _gonvex_reference_pending WHERE seq > CAST(?1 AS INTEGER) ORDER BY seq LIMIT ${PENDING_PAGE}`,
      last === null ? [] : [last],
    ).rows;
    if (page.length === 0) break;
    ledger ??= Ledger.load(sql);
    // Parse errors surface in row order, after the rows before them are applied.
    const parse = (value: unknown): Json | undefined | JsonError => {
      if (value === null || value === undefined) return undefined;
      try {
        return parseJson(String(value));
      } catch (error) {
        if (error instanceof JsonError) return error;
        throw error;
      }
    };
    const staged = page.map((row) => ({ table: String(row[1]), old: parse(row[2]), next: parse(row[3]) }));
    const sources: [string, string][] = [];
    for (const { table, old, next } of staged) {
      if (old instanceof JsonError || next instanceof JsonError) continue;
      sources.push(...ledger.changedSources(table, old, next));
    }
    ledger.prefetchEdges(sources);
    for (const { table, old, next } of staged) {
      if (old instanceof JsonError) throw violation(`invalid staged row image: ${old.message}`);
      if (next instanceof JsonError) throw violation(`invalid staged row image: ${next.message}`);
      ledger.changed(table, old, next);
    }
    processed += page.length;
    last = String(page[page.length - 1]![0]);
    if (page.length < PENDING_PAGE) break;
  }
  if (last !== null) sql.run("DELETE FROM _gonvex_reference_pending WHERE seq <= CAST(?1 AS INTEGER)", [last]);
  return { rows: processed };
}

/** Whether `flushReferences` has staged rows to process (reference_integrity.rs `staged_direct`). */
export function hasStagedReferences(sql: CaptureSql): boolean {
  if (sql.run("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_reference_pending'").rows.length === 0) return false;
  return sql.run("SELECT 1 FROM _gonvex_reference_pending LIMIT 1").rows.length > 0;
}

/** A caller's catalog: a parsed value (plain JavaScript or this module's `Json`) or JSON text. */
function catalogValue(catalog: unknown): Json {
  if (typeof catalog === "string") return parseWith(catalog, (message) => violation(`invalid reference catalog JSON: ${message}`));
  return fromJs(catalog);
}

/**
 * Installs (or replaces) the reference catalog inside `options.atomic` (a
 * savepoint), so it may run inside an open transaction and the whole
 * declaration applies or none of it does.
 */
export function declareReferences(sql: CaptureSql, catalog: unknown, options: DeclareOptions = {}): void {
  const value = catalogValue(catalog);
  (options.atomic ?? inline)(() => declareIn(sql, value));
}

/**
 * Runs after each tenant migration script inside its transaction: applies
 * declaration requests, regenerates staging triggers for changed columns,
 * processes staged rows and enforces opted-in storage coverage.
 */
export function afterMigration(sql: CaptureSql, options: DeclareOptions = {}): void {
  const requests = sql.run("SELECT seq, declaration FROM _gonvex_reference_requests ORDER BY seq").rows;
  for (const request of requests) {
    const catalog = parseWith(String(request[1]), (message) => violation(`invalid reference catalog JSON: ${message}`));
    (options.atomic ?? inline)(() => declareIn(sql, catalog));
  }
  if (requests.length > 0) sql.run("DELETE FROM _gonvex_reference_requests");
  refreshTriggers(sql);
  flushReferences(sql);
  const installed = scalar(sql.run("SELECT declaration FROM _gonvex_reference_catalog WHERE json_type(declaration,'$.exclusions') IS NOT NULL"));
  if (installed !== undefined && installed !== null) checkCoverageIn(sql, parseWith(String(installed), violation));
}

function refreshTriggers(sql: CaptureSql): void {
  const tables = sql.run("SELECT table_name FROM _gonvex_reference_tables").rows.map((row) => String(row[0]));
  for (const table of tables) {
    if (tableExists(sql, table)) installStagingTriggers(sql, table);
  }
}

/** The SQL that names a table's rowid, unless a column shadows every alias. */
function rowidAlias(sql: CaptureSql, table: string): string | null {
  const create = scalar(sql.run("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1", [table]));
  if (typeof create === "string" && /\bWITHOUT\s+ROWID\b/i.test(create)) return null;
  const columns = new Set(sql.run("SELECT lower(name) FROM pragma_table_info(?1)", [table]).rows.map((row) => String(row[0])));
  return ["rowid", "_rowid_", "oid"].find((alias) => !columns.has(alias)) ?? null;
}

/**
 * reference_integrity.rs `read_documents`, a page at a time in rowid order
 * (the order of Rust's full scan). A WITHOUT ROWID table is read whole, as in Rust.
 */
function forEachDocumentPage(sql: CaptureSql, table: string, visit: (rows: Json[]) => void): void {
  const kinds = tableColumns(sql, table).map((column) => [column.name, kindOf(column.declared)] as const);
  const image = jsonObjectExpr("", kinds);
  const quoted = quoteIdentifier(table);
  const decodeRow = (text: unknown) => parseWith(String(text), violation);
  const rowid = rowidAlias(sql, table);
  if (rowid === null) {
    visit(sql.run(`SELECT ${image} FROM ${quoted}`).rows.map((row) => decodeRow(row[0])));
    return;
  }
  let last: string | null = null;
  for (;;) {
    const page: RawResult["rows"] = sql.run(
      `SELECT CAST(${rowid} AS TEXT), ${image} FROM ${quoted}${last === null ? "" : ` WHERE ${rowid} > CAST(?1 AS INTEGER)`} ORDER BY ${rowid} LIMIT ${INVENTORY_PAGE}`,
      last === null ? [] : [last],
    ).rows;
    if (page.length === 0) return;
    visit(page.map((row) => decodeRow(row[1])));
    if (page.length < INVENTORY_PAGE) return;
    last = String(page[page.length - 1]![0]);
  }
}

/** Inventory: one binding's configurations for a page of parent rows, versions in row order. */
function inventoryConfigurations(sql: CaptureSql, ledger: Ledger, binding: Binding, key: string, rows: Json[]): void {
  const entries: [string, string][] = [];
  for (const row of rows) {
    const rowId = fieldText(row, key);
    if (rowId !== null) entries.push([rowId, stringify(configuration(row, binding.columns))]);
  }
  if (entries.length === 0) return;
  // `next_version` once per row: reserve the same consecutive versions in one statement.
  const lastVersion = nextVersions(sql, entries.length);
  const first = lastVersion - BigInt(entries.length) + 1n;
  const versioned = entries.map(([rowId, document], index) => [rowId, document, (first + BigInt(index)).toString()] as const);
  for (const chunk of chunks(versioned)) {
    sql.run(
      `INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document,version)
         SELECT ?1, value->>0, value->>1, CAST(value->>2 AS INTEGER) FROM json_each(?2)`,
      [binding.name, list(chunk)],
    );
  }
  for (const [rowId, document, version] of versioned) ledger.remember(binding.name, rowId, { document, version: BigInt(version) });
}

function declareIn(sql: CaptureSql, catalog: Json): void {
  const tables = get(catalog, "tables");
  const references = get(catalog, "references");
  if (!Array.isArray(tables) || !Array.isArray(references)) throw violation("invalid reference catalog");
  if (fieldText(catalog, "version") !== "1") throw violation("invalid reference catalog");
  const previousTables = sql.run("SELECT table_name FROM _gonvex_reference_tables").rows.map((row) => String(row[0]));
  const names = [...previousTables, ...tables.map((entry) => fieldText(entry, "name"))];
  for (const name of names) {
    if (name === null || isFrameworkTable(name)) throw violation("invalid application reference table");
  }
  const tableNames = new Set(tables.map((entry) => fieldText(entry, "name")).filter((name): name is string => name !== null));
  for (const entry of tables) {
    const name = fieldText(entry, "name") ?? "";
    const key = fieldText(entry, "key");
    const actual = primaryKey(sql, name);
    if (key === null || actual.length !== 1 || key !== actual[0]) throw violation(`reference table ${name} must declare its single primary key`);
    const aliasesValue = get(entry, "targetKeys");
    let aliases: Json[] = [];
    if (Array.isArray(aliasesValue)) aliases = aliasesValue;
    else if (aliasesValue !== undefined) throw violation(`invalid target key aliases for ${name}`);
    for (const alias of aliases) {
      const aliasName = fieldText(alias, "name") ?? "";
      const column = fieldText(alias, "column") ?? "";
      if (
        aliasName === "" ||
        aliasName.startsWith(CONFIGURATION_PREFIX) ||
        tableNames.has(aliasName) ||
        column === "" ||
        !columnExists(sql, name, column)
      ) {
        throw violation(`target alias for ${name} needs an existing key column and a separate namespace`);
      }
      if (!uniqueColumn(sql, name, column) && (get(alias, "multipleOwners") !== true || !reasonOk(alias, "reason"))) {
        throw violation(`nonunique target alias for ${name} needs an explicit multiple-owner policy and reason`);
      }
    }
    const live = fieldText(entry, "live");
    if (live === null) throw violation(`reference table ${name} needs a lifecycle`);
    parsePath(live);
  }

  // A prior statement may have staged changes. Apply them against the old
  // declaration and refuse to rebuild over an enforced violation.
  flushReferences(sql);
  const dangling = Number(scalar(sql.run("SELECT count(*) FROM pragma_foreign_key_check('_gonvex_reference_edges')")) ?? 0);
  if (dangling > 0) throw violation("FOREIGN KEY constraint failed: a declared reference would dangle");
  for (const table of previousTables) dropStagingTriggers(sql, table);
  for (const ledgerTable of ["edges", "targets", "rules", "keys", "configurations", "bindings", "tables"]) {
    sql.run(`DELETE FROM _gonvex_reference_${ledgerTable}`);
  }
  for (const entry of tables) {
    const name = fieldText(entry, "name") ?? "";
    const key = fieldText(entry, "key") ?? "";
    sql.run("INSERT INTO _gonvex_reference_tables(table_name,key_column,live_when) VALUES(?1,?2,?3)", [name, key, fieldText(entry, "live") ?? ""]);
    const keys: [string, string][] = [[name, key]];
    const aliases = get(entry, "targetKeys");
    for (const alias of Array.isArray(aliases) ? aliases : []) keys.push([fieldText(alias, "name") ?? "", fieldText(alias, "column") ?? ""]);
    for (const [token, column] of keys) {
      sql.run("INSERT INTO _gonvex_reference_keys(table_name,token_name,key_column) VALUES(?1,?2,?3)", [name, token, column]);
    }
  }

  const bindingsValue = get(catalog, "bindings");
  let bindings: Json[] = [];
  if (Array.isArray(bindingsValue)) bindings = bindingsValue;
  else if (bindingsValue !== undefined) throw violation("invalid configuration bindings");
  for (const entry of bindings) {
    const name = fieldText(entry, "name") ?? "";
    const source = fieldText(entry, "source") ?? "";
    const columns = get(entry, "columns");
    if (!Array.isArray(columns) || columns.length === 0) throw violation("invalid configuration binding");
    if (name === "" || !tableNames.has(source)) throw violation("invalid configuration binding");
    const columnNames: string[] = [];
    for (const column of columns) {
      const text = textOf(column) ?? "";
      if (!columnExists(sql, source, text)) throw violation(`configuration binding ${name} needs existing columns`);
      columnNames.push(text);
    }
    sql.run("INSERT INTO _gonvex_reference_bindings(name,source_table,columns_) VALUES(?1,?2,?3)", [name, source, stringify(columnNames)]);
  }

  for (const rule of references) {
    validateRule(sql, rule, tableNames);
    sql.run("INSERT INTO _gonvex_reference_rules(name,source_table,declaration) VALUES(?1,?2,?3)", [
      fieldText(rule, "name"),
      fieldText(rule, "source"),
      stringify(rule),
    ]);
  }

  // Inventory: configurations and target tokens for every table first, then
  // edges, so a backfilled edge sees every existing target.
  const ledger = Ledger.load(sql);
  const inventoried = new Set<string>();
  for (const entry of tables) {
    const name = fieldText(entry, "name") ?? "";
    const key = ledger.tables.get(name)!.key;
    const bindingsOf = ledger.bindingsOf(name);
    // Rust inserts every configuration of one binding before the next
    // binding's: with several bindings, one pass per binding keeps the versions.
    if (bindingsOf.length > 1) {
      for (const binding of bindingsOf) forEachDocumentPage(sql, name, (rows) => inventoryConfigurations(sql, ledger, binding, key, rows));
    }
    forEachDocumentPage(sql, name, (rows) => {
      if (bindingsOf.length === 1) inventoryConfigurations(sql, ledger, bindingsOf[0]!, key, rows);
      const counts = new Map<string, [Target, number]>();
      for (const row of rows) {
        for (const target of ledger.rowTargets(name, row)) {
          const id = tupleKey(target);
          const entry = counts.get(id);
          if (entry === undefined) counts.set(id, [target, 1]);
          else entry[1] += 1;
        }
      }
      addTargets(sql, counts.values());
    });
    installStagingTriggers(sql, name);
    inventoried.add(name);
  }
  for (const name of [...inventoried].sort((a, b) => compareTuples([a], [b]))) {
    const key = ledger.tables.get(name)!.key;
    forEachDocumentPage(sql, name, (rows) => {
      const edges: [string, string, string, string, string][] = [];
      for (const row of rows) {
        const rowId = fieldText(row, key);
        if (rowId === null) continue;
        for (const [rule, targetTable, targetId, state] of ledger.desired(name, row)) edges.push([rule, rowId, targetTable, targetId, state]);
      }
      // Inventory keeps legacy violations visible without enforcing
      // them (PostgreSQL NOT VALID); new edges are enforced.
      for (const chunk of chunks(edges)) {
        sql.run(
          `INSERT INTO _gonvex_reference_edges
             (rule_name,source_table,source_id,target_table,target_id,target_state,
              enforced_table,enforced_id,enforced_state)
             SELECT e.value->>0, ?1, e.value->>1, e.value->>2, e.value->>3, e.value->>4, target.table_name, target.row_id, target.state
             FROM json_each(?2) e LEFT JOIN _gonvex_reference_targets target
               ON target.table_name=e.value->>2 AND target.row_id=e.value->>3 AND target.state=e.value->>4`,
          [name, list(chunk)],
        );
      }
    });
  }
  if (get(catalog, "exclusions") !== undefined) checkCoverageIn(sql, catalog);
  sql.run(
    `INSERT INTO _gonvex_reference_catalog(singleton,declaration) VALUES(1,?1)
       ON CONFLICT(singleton) DO UPDATE SET declaration=excluded.declaration`,
    [stringify(catalog)],
  );
}

function validateRule(sql: CaptureSql, rule: Json, tableNames: ReadonlySet<string>): void {
  const name = fieldText(rule, "name");
  const label = name ?? "";
  const has = (key: string) => get(rule, key) !== undefined;
  const state = fieldText(rule, "state");
  const source = fieldText(rule, "source");
  if (name === null || (state !== "exists" && state !== "live") || source === null || !tableNames.has(source) || has("target") === has("targets")) {
    throw violation(`invalid reference declaration ${label}`);
  }
  const paths = get(rule, "paths");
  const pathsInvalid = has("paths") && !(Array.isArray(paths) && paths.length > 0);
  if (has("path") === has("paths") || pathsInvalid) throw violation(`reference ${label} needs exactly one path or nonempty paths array`);
  const ignoreValues = get(rule, "ignoreValues");
  if (has("ignoreValues") && !(Array.isArray(ignoreValues) && ignoreValues.every((value) => typeof value === "string") && reasonOk(rule, "ignoreReason"))) {
    throw violation(`reference ${label} needs declared string sentinels and their meaning`);
  }
  if (has("allowNumbers") && (get(rule, "allowNumbers") !== true || !reasonOk(rule, "numberReason"))) {
    throw violation(`reference ${label} needs an explicit numeric selector policy and reason`);
  }
  const normalize = strField(rule, "normalize");
  if (has("normalize") && ((normalize !== "csv-or-json-list" && normalize !== "task-relation") || !reasonOk(rule, "normalizeReason"))) {
    throw violation(`reference ${label} needs a supported selector decoder and reason`);
  }
  if (has("encoding") && (strField(rule, "encoding") !== "json-or-value" || fieldText(rule, "column") === null)) {
    throw violation(`reference ${label} has unsupported storage encoding`);
  }
  const lookup = get(rule, "lookup");
  if (lookup !== undefined) {
    let steps: Json[];
    if (Array.isArray(lookup) && lookup.length > 0) steps = lookup;
    else if (isObject(lookup)) steps = [lookup];
    else throw violation(`reference ${label} needs configuration lookup steps`);
    for (const step of steps) {
      const binding = fieldText(step, "binding") ?? "";
      const declared = Number(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM _gonvex_reference_bindings WHERE name=?1)", [binding]))) !== 0;
      const from = fieldText(step, "from") ?? "container";
      if (!declared || fieldText(step, "key") === null || !["container", "document", "configuration"].includes(from)) {
        throw violation(`reference ${label} needs a declared configuration binding and key`);
      }
    }
  }
  const targetsValue = get(rule, "targets");
  if (has("targets") && (fieldText(rule, "discriminator") === null || !(isObject(targetsValue) && targetsValue.size > 0))) {
    throw violation(`reference ${label} needs discriminator variants`);
  }
  // Parses every JSON path (`at`, paths, sourceWhen, lookup keys, discriminator).
  const compiled = Rule.compile(rule);
  const targets: (string | null)[] = [];
  if (has("target")) targets.push(fieldText(rule, "target"));
  if (isObject(targetsValue)) targets.push(...[...targetsValue.values()].map((target) => textOf(target)));
  for (const target of targets) {
    const known = Number(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM _gonvex_reference_keys WHERE token_name=?1)", [target]))) !== 0;
    if (!known) throw violation(`reference ${compiled.name} has unknown target ${target ?? ""}`);
  }
}

/**
 * Opt-in storage coverage: every text or JSON column of an application table
 * needs a declared reference, a target key or a reasoned exclusion.
 */
export function checkCoverage(sql: CaptureSql, catalog: unknown): void {
  checkCoverageIn(sql, catalogValue(catalog));
}

function checkCoverageIn(sql: CaptureSql, catalog: Json): void {
  const exclusions = get(catalog, "exclusions");
  if (!Array.isArray(exclusions)) throw violation("reference coverage needs explicit exclusions");
  const referencesValue = get(catalog, "references");
  const references = Array.isArray(referencesValue) ? referencesValue : [];
  for (const rule of references) {
    const label = fieldText(rule, "name") ?? "";
    const source = fieldText(rule, "source") ?? "";
    const column = fieldText(rule, "column");
    if (column === null || !columnExists(sql, source, column)) throw violation(`reference ${label} needs an existing source column`);
    // Every selection must be anchored to the declared storage column; a
    // sibling path cannot count as coverage for another column.
    const atText = fieldText(rule, "at");
    const at = atText === null ? null : parsePath(atText);
    let selections: JsonPath[];
    if (at !== null && !at.isRoot()) {
      selections = [at];
    } else {
      const paths = get(rule, "paths");
      selections = Array.isArray(paths) ? paths.map((path) => parsePath(textOf(path) ?? "")) : [parsePath(fieldText(rule, "path") ?? "")];
    }
    if (selections.some((selection) => selection.rootMember() !== column)) {
      throw violation(`reference ${label} selection must start at its declared column`);
    }
  }
  for (const entry of exclusions) {
    const source = fieldText(entry, "source") ?? "";
    const column = fieldText(entry, "column") ?? "";
    const kind = fieldText(entry, "kind");
    const valid = reasonOk(entry, "reason") && kind !== null && EXCLUSION_KINDS.includes(kind) && columnExists(sql, source, column);
    if (!valid) throw violation(`invalid reference exclusion for ${source}.${column}`);
  }
  const keys = sql.run("SELECT table_name, key_column FROM _gonvex_reference_keys").rows.map((row) => [String(row[0]), String(row[1])] as const);
  const classified = (items: Json[], table: string, column: string) =>
    items.some((item) => strField(item, "source") === table && strField(item, "column") === column);
  const tables = applicationTables(sql);
  const columnsByTable = tablesColumns(sql, tables);
  for (const table of tables) {
    for (const column of columnsByTable.get(table) ?? []) {
      const declared = asciiUpper(column.declared);
      const candidate = !column.primaryKey && ["TEXT", "CHAR", "CLOB", "JSON"].some((kind) => declared.includes(kind));
      if (
        candidate &&
        !classified(references, table, column.name) &&
        !keys.some(([owner, key]) => owner === table && key === column.name) &&
        !classified(exclusions, table, column.name)
      ) {
        throw violation(`unclassified reference candidate ${table}.${column.name}`);
      }
    }
  }
}
