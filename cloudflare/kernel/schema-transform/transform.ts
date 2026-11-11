// The schema transform (0014): the app's unchanged migrations and the
// runtime's provisioning DDL in, DDL a Durable Object's SQLite accepts out.
//
// Deterministic and pure: the output is a function of the input statements,
// the options and the catalog passed in, never of time or randomness. One
// SchemaTransform instance is fed migrations in order, because later
// statements depend on what earlier ones created (a trigger that reads a view
// T6 split, a DROP INDEX that must drop T3's generated columns).

import { chunkStatements, CHUNK_DEFAULTS, type ChunkOptions } from "./chunk.ts";
import { rewriteCompounds } from "./compound.ts";
import { callsCaptureFunctions, CaptureError, recordView, rewriteCaptureCalls, type RecordView, type TableShape } from "./capture.ts";
import { TransformError, type TransformFailure } from "./errors.ts";
import { foldChainsInIndex, foldChainsInQuery } from "./fold.ts";
import {
  buildTrigger,
  classify,
  isNameToken,
  mentionedNames,
  parseTrigger,
  quoteIdent,
  renameRelations,
  sameName,
  significant,
  splitStatements,
  stripLeadingComments,
  text,
  unquote,
  utf8Bytes,
  type StatementKind,
} from "./lexer.ts";
import { checkStatement, effectiveDdlBytes, LIMITS, unionBranchesRead, type Limits } from "./limits.ts";
import { finalOver, parseUnionView, pieceSql, resultColumns, type UnionView } from "./union-view.ts";

/** Recorded in the tenant object's migration ledger metadata with every applied migration. */
export const TRANSFORM_VERSION = "a01.1";

/** Every object the transform adds starts with this prefix (never `_cf_`, which the platform reserves). */
export const ADDED_PREFIX = "_gonvex_st_";

export type Rule = "T1" | "T2" | "T3" | "T4" | "T5" | "T6" | "T7";

export interface TransformOptions {
  limits?: Partial<Limits>;
  chunk?: Partial<ChunkOptions>;
  /** T6 splits a reference union view with at least this many branches. */
  referenceMinBranches?: number;
  /** At most this many branch views per fill trigger (each read by its own INSERT). */
  fillGroupBranches?: number;
  /** At most this many bytes of branch-view SQL behind one fill trigger (keeps each trigger program small). */
  fillGroupBytes?: number;
}

export interface CatalogInput {
  /** `_gonvex_record_views` rows or parsed record views, for gonvex_record_value. */
  records?: ReadonlyArray<RecordView | { name: string; storage_table: string; key_column: string; schema: string }>;
  /** Column shapes of captured tables, for gonvex_capture. */
  tables?: readonly TableShape[];
}

export interface AddedObject {
  type: "table" | "view" | "trigger" | "column";
  name: string;
  /** For a column: its table. For the rest: the table or view it serves. */
  of: string;
  rule: Rule;
}

export interface Rewrite {
  rule: Rule;
  object: string;
}

export interface TransformedMigration {
  name: string;
  version: string;
  /** Output statements in apply order, without trailing `;`. */
  statements: string[];
  /** The same statements cut into storage transactions (T7). */
  chunks: string[][];
  /** `-- gonvex:` directive lines from the source, verbatim. */
  directives: string[];
  /** `-- gonvex:foreign-key-rebuild`: apply with foreign keys off, then check. */
  foreignKeyRebuild: boolean;
  sourceStatements: number;
  rewrites: Rewrite[];
}

/** A `sqlite_schema` row, for transforming an existing schema image. */
export interface SchemaObject {
  type: "table" | "index" | "view" | "trigger";
  name: string;
  tbl_name: string;
  sql: string | null;
}

interface SplitView {
  view: string;
  kind: "T6" | "T2";
  /** Views and tables created for it, in creation order. */
  pieces: AddedObject[];
  rows?: string;
  queue?: string;
  expected?: string;
}

interface Folded {
  table: string;
  columns: string[];
}

interface Context {
  migration: string;
  rewrites: Rewrite[];
  temp: Map<string, number>; // converted TEMP table -> output index of its CREATE
  tempGroups: Array<[number, number]>;
  /** Output index ranges a chunk boundary must not cut (T6 fill..clear wrappers). */
  keep: Array<[number, number]>;
  out: string[];
}

const fail = (ctx: { migration: string }, object: string, limit: TransformFailure, detail: string): never => {
  throw new TransformError(ctx.migration, object, limit, detail);
};

export class SchemaTransform {
  readonly version = TRANSFORM_VERSION;
  private readonly limits: Readonly<Limits>;
  private readonly chunkOptions: ChunkOptions;
  private readonly referenceMinBranches: number;
  private readonly fillGroupBranches: number;
  private readonly fillGroupBytes: number;
  /** Schema objects by lower-case name: type and current (transformed) SQL. */
  private readonly objects = new Map<string, { type: string; name: string; sql: string; table: string }>();
  private readonly split = new Map<string, SplitView>();
  private readonly folded = new Map<string, Folded>();
  private readonly added = new Map<string, AddedObject>();
  private readonly records = new Map<string, RecordView>();
  private readonly tables = new Map<string, TableShape>();
  /** Table -> split views its triggers' WHEN clauses read (the reference guards). */
  private readonly guarded = new Map<string, Set<string>>();
  /** Table -> trigger -> split views that trigger's statements writing the table read. */
  private readonly writers = new Map<string, Map<string, Set<string>>>();
  private branchMemo = new Map<string, number>();
  /** Tables with a DEFERRABLE INITIALLY DEFERRED foreign key (lower-case). */
  private readonly deferred = new Set<string>();

  constructor(options: TransformOptions = {}) {
    this.limits = { ...LIMITS, ...options.limits };
    this.chunkOptions = { ...CHUNK_DEFAULTS, ...options.chunk };
    this.referenceMinBranches = options.referenceMinBranches ?? 2;
    this.fillGroupBranches = options.fillGroupBranches ?? 4;
    this.fillGroupBytes = options.fillGroupBytes ?? 48_000;
  }

  /** Adds record views and table shapes for T5. Later entries replace earlier ones by name. */
  setCatalog(catalog: CatalogInput): void {
    for (const r of catalog.records ?? []) {
      const record = "storageTable" in r ? r : recordView(r);
      this.records.set(record.name, record);
    }
    for (const t of catalog.tables ?? []) this.tables.set(t.name, t);
  }

  /** Objects the transform added that exist now (M02 and checksum tools exclude exactly these). */
  addedObjects(): AddedObject[] {
    return [...this.added.values()].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  }

  /** Transforms one migration file. Call in migration order. */
  migration(name: string, sql: string): TransformedMigration {
    return this.run(name, sql, splitStatements(sql));
  }

  /** Transforms DDL the runtime provisions (capture and record triggers, runtime tables). */
  runtime(name: string, sql: string, catalog?: CatalogInput): TransformedMigration {
    if (catalog) this.setCatalog(catalog);
    return this.run(name, sql, splitStatements(sql));
  }

  /**
   * Transforms an existing schema (`sqlite_schema` rows of a tenant file):
   * tables, then indexes, views and triggers, each in the given order.
   * Internal (`sqlite_*`) objects and FTS5 shadow tables are skipped.
   */
  image(name: string, objects: readonly SchemaObject[], catalog?: CatalogInput): TransformedMigration {
    if (catalog) this.setCatalog(catalog);
    const virtual = objects.filter((o) => o.type === "table" && o.sql && /^\s*CREATE\s+VIRTUAL\s+TABLE/i.test(o.sql)).map((o) => o.name);
    const shadow = new Set(virtual.flatMap((v) => ["_data", "_idx", "_content", "_docsize", "_config"].map((s) => (v + s).toLowerCase())));
    const keep = objects.filter((o) => o.sql && !o.name.startsWith("sqlite_") && !shadow.has(o.name.toLowerCase()));
    const order = ["table", "index", "view", "trigger"];
    const sorted = order.flatMap((type) => keep.filter((o) => o.type === type));
    return this.run(name, sorted.map((o) => o.sql!).join(";\n"), sorted.map((o) => o.sql!));
  }

  private run(name: string, source: string, statements: string[]): TransformedMigration {
    const ctx: Context = { migration: name, rewrites: [], temp: new Map(), tempGroups: [], keep: [], out: [] };
    for (const statement of statements) {
      const produced = this.statement(statement, ctx);
      for (const s of produced) this.validate(s, ctx);
      if (produced.length > 1 && classify(stripLeadingComments(statement)).kind === "other") ctx.keep.push([ctx.out.length, ctx.out.length + produced.length - 1]);
      ctx.out.push(...produced);
    }
    // A TEMP table dies with the connection; its ordinary replacement must not outlive the migration.
    for (const [lower, index] of ctx.temp) {
      const created = this.objects.get(lower);
      ctx.out.push(`DROP TABLE IF EXISTS ${quoteIdent(created ? created.name : lower)}`);
      ctx.tempGroups.push([index, ctx.out.length - 1]);
      this.forget(lower);
      ctx.rewrites.push({ rule: "T4", object: created ? created.name : lower });
    }
    const directives = source.split("\n").filter((l) => /^\s*--\s*gonvex:/.test(l)).map((l) => l.trim());
    return {
      name,
      version: TRANSFORM_VERSION,
      statements: ctx.out,
      chunks: chunkStatements(ctx.out, this.chunkOptions, mergeRanges([...ctx.tempGroups, ...ctx.keep, ...this.deferredSpans(ctx.out)])),
      directives,
      foreignKeyRebuild: directives.some((d) => /^--\s*gonvex:foreign-key-rebuild$/.test(d)),
      sourceStatements: statements.length,
      rewrites: ctx.rewrites,
    };
  }

  /**
   * T7: rows written to a table with a deferred foreign key may violate it
   * until a later statement of the same migration deletes them (the
   * reference scope rows and their always-empty guard table). A chunk COMMIT
   * inside such a span would hit the violation, and on the platform a
   * deferred violation at commit resets the object. Each span runs from the
   * INSERT to the next DELETE from the same table.
   */
  private deferredSpans(out: readonly string[]): Array<[number, number]> {
    const spans: Array<[number, number]> = [];
    const open = new Map<string, number>();
    for (const [i, s] of out.entries()) {
      const target = writeTarget(s);
      if (!target || !this.deferred.has(target)) continue;
      const verb = significant(s)[0]?.upper;
      if ((verb === "INSERT" || verb === "REPLACE") && !open.has(target)) open.set(target, i);
      else if (verb === "DELETE" && open.has(target)) {
        spans.push([open.get(target)!, i]);
        open.delete(target);
      }
    }
    return spans;
  }

  private validate(statement: string, ctx: Context): void {
    const violations = checkStatement(statement, { limits: this.limits, branchesRead: (r) => this.branchesRead(r) });
    if (!violations.length) return;
    const v = violations[0]!;
    fail(ctx, objectName(statement), v.limit, `${v.detail}${violations.length > 1 ? ` (and ${violations.slice(1).map((x) => x.limit).join(", ")})` : ""}`);
  }

  /** Union branches a statement reads when it selects from `relation`, through nested views. */
  private branchesRead(relation: string): number {
    const known = this.branchMemo.get(relation);
    if (known !== undefined) return known;
    const object = this.objects.get(relation);
    if (!object || object.type !== "view") return 0;
    this.branchMemo.set(relation, 0); // cycle guard
    const value = unionBranchesRead(object.sql, (r) => (r === relation ? 0 : this.branchesRead(r)));
    this.branchMemo.set(relation, value);
    return value;
  }

  private remember(type: string, name: string, sql: string, table = name): void {
    this.objects.set(name.toLowerCase(), { type, name, sql, table });
    this.branchMemo = new Map();
  }

  private forget(lower: string): void {
    this.objects.delete(lower);
    this.added.delete(lower);
    this.branchMemo = new Map();
  }

  private add(object: AddedObject): void {
    this.added.set(object.name.toLowerCase(), object);
  }

  private statement(raw: string, ctx: Context): string[] {
    const statement = stripLeadingComments(raw);
    if (!statement) return [];
    let kind: StatementKind;
    try {
      kind = classify(statement);
    } catch (error) {
      return fail(ctx, objectName(statement), "syntax", (error as Error).message);
    }
    const carrying = [...this.folded.values()].map((f) => f.table.toLowerCase());
    if (carrying.length) {
      const star = starReads(statement, new Set(carrying));
      if (star) fail(ctx, objectName(statement), "unhandled-construct", `selects * from ${star}, which carries T3's generated columns (they would appear in the result)`);
    }
    switch (kind.kind) {
      case "drop":
        return this.drop(statement, kind, ctx);
      case "alter-rename":
        return this.rename(statement, kind.table, kind.to);
      case "alter-rename-column":
      case "alter-add-column":
      case "alter-drop-column":
        return [statement];
      case "create-table":
        return this.createTable(statement, kind, ctx);
      case "create-view":
        return this.createView(statement, kind, ctx);
      case "create-index":
        return this.createIndex(statement, kind, ctx);
      case "create-trigger":
        return this.createTrigger(statement, kind, ctx);
      case "other":
        return this.other(statement, ctx);
    }
  }

  private drop(statement: string, kind: Extract<StatementKind, { kind: "drop" }>, ctx: Context): string[] {
    const lower = kind.name.toLowerCase();
    const out = [statement];
    const split = this.split.get(lower);
    if (kind.object === "view" && split) {
      for (const piece of split.pieces.slice().reverse()) {
        if (piece.type === "trigger") continue; // dropped with their table
        out.push(`DROP ${piece.type === "view" ? "VIEW" : "TABLE"} IF EXISTS ${quoteIdent(piece.name)}`);
        this.forget(piece.name.toLowerCase());
      }
      for (const piece of split.pieces) if (piece.type === "trigger") this.forget(piece.name.toLowerCase());
      this.split.delete(lower);
      ctx.rewrites.push({ rule: split.kind, object: kind.name });
    }
    const folded = this.folded.get(lower);
    if (kind.object === "index" && folded) {
      for (const column of folded.columns.slice().reverse()) {
        out.push(`ALTER TABLE ${quoteIdent(folded.table)} DROP COLUMN ${quoteIdent(column)}`);
        this.added.delete(`${folded.table}.${column}`.toLowerCase());
      }
      this.folded.delete(lower);
      ctx.rewrites.push({ rule: "T3", object: kind.name });
    }
    if (kind.object === "table") {
      for (const [index, f] of this.folded) {
        if (sameName(f.table, kind.name)) {
          for (const c of f.columns) this.added.delete(`${f.table}.${c}`.toLowerCase());
          this.folded.delete(index);
        }
      }
      if (ctx.temp.has(lower)) {
        ctx.tempGroups.push([ctx.temp.get(lower)!, ctx.out.length]);
        ctx.temp.delete(lower);
      }
      for (const [triggerLower, o] of this.objects) if (o.type === "trigger" && sameName(o.table, kind.name)) this.dropTriggerState(triggerLower);
      this.guarded.delete(lower);
    }
    if (kind.object === "trigger") this.dropTriggerState(lower);
    this.forget(lower);
    return out;
  }

  private dropTriggerState(lower: string): void {
    for (const byTrigger of this.writers.values()) byTrigger.delete(lower);
    this.objects.delete(lower);
  }

  private rename(statement: string, from: string, to: string): string[] {
    const lower = from.toLowerCase();
    const object = this.objects.get(lower);
    if (object) {
      this.objects.delete(lower);
      this.objects.set(to.toLowerCase(), { ...object, name: to, table: to });
    }
    for (const f of this.folded.values()) if (sameName(f.table, from)) f.table = to;
    for (const a of this.added.values()) if (a.type === "column" && sameName(a.of, from)) a.of = to;
    for (const o of this.objects.values()) if (o.type !== "table" && sameName(o.table, from)) o.table = to;
    const guard = this.guarded.get(lower);
    if (guard) {
      this.guarded.delete(lower);
      this.guarded.set(to.toLowerCase(), guard);
    }
    const writers = this.writers.get(lower);
    if (writers) {
      this.writers.delete(lower);
      this.writers.set(to.toLowerCase(), writers);
    }
    this.branchMemo = new Map();
    return [statement];
  }

  private createTable(statement: string, kind: Extract<StatementKind, { kind: "create-table" }>, ctx: Context): string[] {
    let sql = statement;
    if (kind.temp) {
      const lower = kind.name.toLowerCase();
      if (this.objects.has(lower)) fail(ctx, kind.name, "temp-schema", "a TEMP table with the name of an existing object cannot become an ordinary table");
      sql = statement.replace(/^(\s*CREATE\s+)(TEMP|TEMPORARY)\s+/i, "$1").replace(/(\bTABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)(?:temp|"temp")\s*\.\s*/i, "$1");
      ctx.temp.set(lower, ctx.out.length);
      ctx.rewrites.push({ rule: "T4", object: kind.name });
    }
    if (!(kind.ifNotExists && this.objects.has(kind.name.toLowerCase()))) this.remember("table", kind.name, sql);
    if (/\bDEFERRABLE\s+INITIALLY\s+DEFERRED\b/i.test(sql)) this.deferred.add(kind.name.toLowerCase());
    return [sql];
  }

  private createIndex(statement: string, kind: Extract<StatementKind, { kind: "create-index" }>, ctx: Context): string[] {
    if (kind.ifNotExists && this.objects.has(kind.name.toLowerCase())) return [];
    let fold;
    try {
      fold = foldChainsInIndex(statement, kind.name, kind.table);
    } catch (error) {
      return fail(ctx, kind.name, "expression-depth", (error as Error).message);
    }
    if (!fold) {
      this.remember("index", kind.name, statement, kind.table);
      return [statement];
    }
    ctx.rewrites.push({ rule: "T3", object: kind.name });
    const table = this.objects.get(kind.table.toLowerCase())?.name ?? kind.table;
    this.folded.set(kind.name.toLowerCase(), { table, columns: fold.columns.map((c) => c.name) });
    for (const c of fold.columns) this.added.set(`${table}.${c.name}`.toLowerCase(), { type: "column", name: c.name, of: table, rule: "T3" });
    this.remember("index", kind.name, fold.index, kind.table);
    return [...fold.columns.map((c) => c.sql), fold.index];
  }

  private createView(statement: string, kind: Extract<StatementKind, { kind: "create-view" }>, ctx: Context): string[] {
    const lower = kind.name.toLowerCase();
    if (kind.ifNotExists && this.objects.has(lower)) return [];
    if (kind.temp) fail(ctx, kind.name, "temp-schema", "TEMP views are not supported");
    let sql = statement;
    try {
      const folded = foldChainsInQuery(sql);
      if (folded !== sql) ctx.rewrites.push({ rule: "T3", object: kind.name });
      sql = folded;
    } catch (error) {
      return fail(ctx, kind.name, "expression-depth", (error as Error).message);
    }
    const view = parseUnionView(sql);
    if (view && view.terms.length >= this.referenceMinBranches) return this.splitReferenceView(view, ctx);
    const chained = rewriteCompounds(sql, this.limits.compoundTerms);
    if (chained !== sql) ctx.rewrites.push({ rule: "T1", object: kind.name });
    if (effectiveDdlBytes(chained, kind.name) <= this.limits.ddlBytes) {
      this.remember("view", kind.name, chained);
      return [chained];
    }
    if (!view) return fail(ctx, kind.name, "ddl-bytes", `${effectiveDdlBytes(chained, kind.name)} bytes and not a CTE union view that can be partitioned`);
    return this.partitionView(view, ctx);
  }

  /** T2: parts of at most ddlBytes each; the view becomes its final SELECT over their union. */
  private partitionView(view: UnionView, ctx: Context): string[] {
    const max = this.limits.ddlBytes - 500;
    const out: string[] = [];
    const pieces: AddedObject[] = [];
    let group: string[] = [];
    const flush = () => {
      if (!group.length) return;
      const name = `${ADDED_PREFIX}${view.name}__part${pieces.length}`;
      const sql = rewriteCompounds(pieceSql(view, name, group), this.limits.compoundTerms);
      if (effectiveDdlBytes(sql, name) > this.limits.ddlBytes) fail(ctx, view.name, "ddl-bytes", `one union branch alone needs ${utf8Bytes(sql)} bytes`);
      out.push(sql);
      pieces.push({ type: "view", name, of: view.name, rule: "T2" });
      this.remember("view", name, sql);
      group = [];
    };
    for (const term of view.terms) {
      if (group.length && effectiveDdlBytes(pieceSql(view, "x", [...group, term])) > max) flush();
      group.push(term);
    }
    flush();
    const union = rewriteCompounds(pieces.map((p) => `SELECT * FROM ${quoteIdent(p.name)}`).join(" UNION ALL "), this.limits.compoundTerms);
    const sql = `CREATE VIEW ${quoteIdent(view.name)} AS ${finalOver(view, `(${union})`)}`;
    out.push(sql);
    this.remember("view", view.name, sql);
    for (const p of pieces) this.add(p);
    this.split.set(view.name.toLowerCase(), { view: view.name, kind: "T2", pieces });
    ctx.rewrites.push({ rule: "T2", object: view.name });
    return out;
  }

  /**
   * T6: one view per union branch, a scratch table for the branch rows, a
   * queue table whose AFTER INSERT triggers fill the scratch table one branch
   * per INSERT, and an `expected` view (the original aggregate over the
   * scratch rows). Statements that read the original view read `expected`
   * after a fill; the original view stays, as the aggregate over the union of
   * its branch views, for anything else that reads it.
   */
  private splitReferenceView(view: UnionView, ctx: Context): string[] {
    const base = `${ADDED_PREFIX}${view.name}`;
    let columns: string[];
    try {
      columns = resultColumns(view.terms[0]!);
    } catch (error) {
      return fail(ctx, view.name, "unhandled-construct", `cannot read the union's result columns: ${(error as Error).message}`);
    }
    if (columns.length > this.limits.columns) fail(ctx, view.name, "columns", `${columns.length} columns`);
    const out: string[] = [];
    const pieces: AddedObject[] = [];
    const branches = view.terms.map((term, k) => {
      const name = `${base}__b${k}`;
      const sql = rewriteCompounds(pieceSql(view, name, [term]), this.limits.compoundTerms);
      out.push(sql);
      pieces.push({ type: "view", name, of: view.name, rule: "T6" });
      this.remember("view", name, sql);
      return { name, bytes: utf8Bytes(sql) };
    });
    const union = rewriteCompounds(branches.map((b) => `SELECT * FROM ${quoteIdent(b.name)}`).join(" UNION ALL "), this.limits.compoundTerms);
    const original = `CREATE VIEW ${quoteIdent(view.name)} AS ${finalOver(view, `(${union})`)}`;
    out.push(original);
    this.remember("view", view.name, original);
    const rows = `${base}__rows`;
    const queue = `${base}__queue`;
    const expected = `${base}__expected`;
    out.push(`CREATE TABLE ${quoteIdent(rows)}(${columns.map(quoteIdent).join(", ")})`);
    this.remember("table", rows, out[out.length - 1]!);
    pieces.push({ type: "table", name: rows, of: view.name, rule: "T6" });
    out.push(`CREATE TABLE ${quoteIdent(queue)}(n)`);
    this.remember("table", queue, out[out.length - 1]!);
    pieces.push({ type: "table", name: queue, of: view.name, rule: "T6" });
    let group: typeof branches = [];
    let groupBytes = 0;
    const flush = () => {
      if (!group.length) return;
      const name = `${base}__fill${pieces.filter((p) => p.type === "trigger").length}`;
      const body = group.map((b) => `INSERT INTO ${quoteIdent(rows)} SELECT * FROM ${quoteIdent(b.name)}`);
      const sql = buildTrigger({ head: `CREATE TRIGGER ${quoteIdent(name)} AFTER INSERT ON ${quoteIdent(queue)}`, when: null, body });
      out.push(sql);
      this.remember("trigger", name, sql, queue);
      pieces.push({ type: "trigger", name, of: view.name, rule: "T6" });
      group = [];
      groupBytes = 0;
    };
    for (const b of branches) {
      if (group.length && (group.length >= this.fillGroupBranches || groupBytes + b.bytes > this.fillGroupBytes)) flush();
      group.push(b);
      groupBytes += b.bytes;
    }
    flush();
    const expectedSql = `CREATE VIEW ${quoteIdent(expected)} AS ${finalOver(view, quoteIdent(rows))}`;
    out.push(expectedSql);
    this.remember("view", expected, expectedSql);
    pieces.push({ type: "view", name: expected, of: view.name, rule: "T6" });
    for (const p of pieces) this.add(p);
    this.split.set(view.name.toLowerCase(), { view: view.name, kind: "T6", pieces, rows, queue, expected });
    ctx.rewrites.push({ rule: "T6", object: view.name });
    return out;
  }

  /** Split views (T6) a statement reads. */
  private readsSplit(sql: string): SplitView[] {
    const names = mentionedNames(sql);
    const out: SplitView[] = [];
    for (const name of names) {
      const s = this.split.get(name);
      if (s && s.kind === "T6") out.push(s);
    }
    return out.sort((a, b) => (a.view < b.view ? -1 : 1));
  }

  /** Fill, the statement reading `expected` instead of the view, then clear. */
  private wrap(sql: string, reads: SplitView[]): string[] {
    if (!reads.length) return [sql];
    const map = new Map(reads.map((s) => [s.view.toLowerCase(), s.expected!]));
    const fill = reads.flatMap((s) => [`DELETE FROM ${quoteIdent(s.rows!)}`, `INSERT INTO ${quoteIdent(s.queue!)}(n) VALUES (NULL)`, `DELETE FROM ${quoteIdent(s.queue!)}`]);
    const clear = reads.map((s) => `DELETE FROM ${quoteIdent(s.rows!)}`);
    return [...fill, renameRelations(sql, map), ...clear];
  }

  private queryStatement(sql: string, ctx: Context, object: string, inTrigger: boolean): string[] {
    let s = sql;
    if (callsCaptureFunctions(s)) {
      try {
        s = rewriteCaptureCalls(s, { records: this.records, tables: this.tables }, inTrigger);
      } catch (error) {
        if (error instanceof CaptureError) return fail(ctx, object, "catalog", error.message);
        throw error;
      }
      ctx.rewrites.push({ rule: "T5", object });
    }
    try {
      const folded = foldChainsInQuery(s);
      if (folded !== s) ctx.rewrites.push({ rule: "T3", object });
      s = folded;
    } catch (error) {
      return fail(ctx, object, "expression-depth", (error as Error).message);
    }
    const reads = this.readsSplit(s);
    if (reads.length) ctx.rewrites.push({ rule: "T6", object });
    return this.wrap(s, reads).map((x) => {
      const chained = rewriteCompounds(x, this.limits.compoundTerms);
      if (chained !== x) ctx.rewrites.push({ rule: "T1", object });
      return chained;
    });
  }

  private createTrigger(statement: string, kind: Extract<StatementKind, { kind: "create-trigger" }>, ctx: Context): string[] {
    const lower = kind.name.toLowerCase();
    if (kind.ifNotExists && this.objects.has(lower)) return [];
    if (kind.temp) fail(ctx, kind.name, "temp-schema", "TEMP triggers are not supported");
    let parts;
    try {
      parts = parseTrigger(statement);
    } catch (error) {
      return fail(ctx, kind.name, "syntax", (error as Error).message);
    }
    const table = kind.table.toLowerCase();
    let when = parts.when;
    if (when !== null) {
      if (callsCaptureFunctions(when)) fail(ctx, kind.name, "custom-function", "a WHEN clause calls a capture function");
      const folded = foldChainsInQuery(when);
      if (folded !== when) ctx.rewrites.push({ rule: "T3", object: kind.name });
      when = folded;
      const reads = this.readsSplit(when);
      if (reads.length) {
        // A WHEN clause cannot fill: it reads the scratch rows of the statement
        // that writes this table. Every persisting writer must be a filled
        // statement for the same views (checked here and for later writers).
        const set = this.guarded.get(table) ?? new Set<string>();
        for (const r of reads) set.add(r.view.toLowerCase());
        this.guarded.set(table, set);
        for (const [writer, views] of this.writers.get(table) ?? []) {
          for (const r of reads) if (!views.has(r.view.toLowerCase())) fail(ctx, kind.name, "unhandled-construct", `its WHEN reads ${r.view}, but trigger ${writer} writes ${kind.table} without reading it`);
        }
        when = renameRelations(when, new Map(reads.map((r) => [r.view.toLowerCase(), r.expected!])));
        ctx.rewrites.push({ rule: "T6", object: kind.name });
      }
      when = rewriteCompounds(when, this.limits.compoundTerms);
    }
    const body: string[] = [];
    for (const s of parts.body) {
      this.checkWriter(s, ctx, kind.name, lower);
      body.push(...this.queryStatement(s, ctx, kind.name, true));
    }
    const sql = buildTrigger({ head: parts.head, when, body });
    this.remember("trigger", kind.name, sql, kind.table);
    return [sql];
  }

  /** Records which split views a statement writing a guarded table reads; fails when it reads none of them. */
  private checkWriter(sql: string, ctx: Context, object: string, trigger: string | null): void {
    const target = writeTarget(sql);
    if (!target) return;
    const reads = new Set(this.readsSplit(sql).map((r) => r.view.toLowerCase()));
    if (trigger) {
      const byTrigger = this.writers.get(target) ?? new Map<string, Set<string>>();
      const views = byTrigger.get(trigger) ?? new Set<string>();
      for (const r of reads) views.add(r);
      byTrigger.set(trigger, views);
      this.writers.set(target, byTrigger);
    }
    for (const view of this.guarded.get(target) ?? []) {
      if (!reads.has(view)) fail(ctx, object, "unhandled-construct", `writes ${target}, whose guard reads ${view}, without reading ${view}`);
    }
  }

  private other(statement: string, ctx: Context): string[] {
    this.learnRecords(statement);
    this.checkWriter(statement, ctx, objectName(statement), null);
    return this.queryStatement(statement, ctx, objectName(statement), false);
  }

  /** Keeps the record catalog current from `INSERT INTO _gonvex_record_views VALUES(...)` in migrations. */
  private learnRecords(statement: string): void {
    const toks = significant(statement);
    let i = 0;
    if (toks[i]?.upper !== "INSERT") return;
    while (toks[i] && toks[i]!.upper !== "INTO") i += 1;
    const target = toks[i + 1];
    if (!isNameToken(target) || unquote(text(statement, target)).toLowerCase() !== "_gonvex_record_views") return;
    if (toks[i + 2]?.upper !== "VALUES") return;
    // Rows of four string literals.
    const values: string[] = [];
    for (let k = i + 3; k < toks.length; k += 1) {
      const t = toks[k]!;
      if (t.type === "string") values.push(text(statement, t).slice(1, -1).replace(/''/g, "'"));
      else if (t.type === ")" && values.length === 4) {
        const [name, storage_table, key_column, schema] = values as [string, string, string, string];
        this.records.set(name, recordView({ name, storage_table, key_column, schema }));
        values.length = 0;
      }
    }
  }
}

const CLAUSE_END = new Set(["WHERE", "GROUP", "ORDER", "LIMIT", "UNION", "EXCEPT", "INTERSECT", "WINDOW", "HAVING", "RETURNING", "ON"]);

/**
 * The first table in `tables` (lower-case) that a `*` or `alias.*` result
 * column reads, through the FROM clause of the same SELECT, or null.
 */
export function starReads(sql: string, tables: ReadonlySet<string>): string | null {
  const toks = significant(sql);
  for (let k = 0; k < toks.length; k += 1) {
    const t = toks[k]!;
    if (t.type !== "op") continue;
    const op = text(sql, t);
    const prev = toks[k - 1];
    // `*` after SELECT / DISTINCT / ALL / ",", or `alias.*` (lexed as one `.*` token, or `.` then `*`).
    const qualified = (op === ".*" || op.startsWith(".*")) && isNameToken(prev);
    const bare = op.startsWith("*") && !!prev && (prev.upper === "SELECT" || prev.upper === "DISTINCT" || prev.upper === "ALL" || prev.type === "," || (prev.type === "op" && text(sql, prev) === "."));
    const inList = qualified || bare;
    if (!inList) continue;
    // Walk to this SELECT's FROM at the same depth, then read its relation names.
    let depth = 0;
    let j = k + 1;
    for (; j < toks.length; j += 1) {
      const u = toks[j]!;
      if (u.type === "(") depth += 1;
      else if (u.type === ")") {
        if (depth === 0) break;
        depth -= 1;
      } else if (depth === 0 && u.upper === "FROM") break;
    }
    if (toks[j]?.upper !== "FROM") continue;
    depth = 0;
    for (j += 1; j < toks.length; j += 1) {
      const u = toks[j]!;
      if (u.type === "(") depth += 1;
      else if (u.type === ")") {
        if (depth === 0) break;
        depth -= 1;
      } else if (depth === 0 && (u.type === ";" || CLAUSE_END.has(u.upper ?? ""))) break;
      else if (depth === 0 && isNameToken(u)) {
        const name = unquote(text(sql, u)).toLowerCase();
        if (tables.has(name)) return name;
      }
    }
  }
  return null;
}

/** Merges overlapping or nested [first, last] ranges. */
export function mergeRanges(ranges: ReadonlyArray<readonly [number, number]>): Array<[number, number]> {
  const sorted = ranges.map(([a, b]): [number, number] => [a, b]).sort((x, y) => x[0] - y[0] || x[1] - y[1]);
  const out: Array<[number, number]> = [];
  for (const r of sorted) {
    const last = out[out.length - 1];
    if (last && r[0] <= last[1]) last[1] = Math.max(last[1], r[1]);
    else out.push(r);
  }
  return out;
}

/** Table an INSERT / UPDATE / DELETE / REPLACE writes, lower-case, or null. */
export function writeTarget(sql: string): string | null {
  const toks = significant(sql);
  let i = 0;
  if (toks[0]?.upper === "WITH") return null; // CTE-prefixed DML is not used by the app schema; treated as not writing a guarded table
  const verb = toks[0]?.upper;
  if (verb === "INSERT" || verb === "REPLACE") {
    while (toks[i] && toks[i]!.upper !== "INTO") i += 1;
    i += 1;
  } else if (verb === "UPDATE") {
    i = 1;
    if (toks[i]?.upper === "OR") i += 2;
  } else if (verb === "DELETE") {
    i = 2;
  } else return null;
  const t = toks[i];
  if (!isNameToken(t)) return null;
  const dot = toks[i + 1];
  if (dot && dot.type === "op" && text(sql, dot) === "." && isNameToken(toks[i + 2])) return unquote(text(sql, toks[i + 2]!)).toLowerCase();
  return unquote(text(sql, t)).toLowerCase();
}

/** The object a statement creates, drops or writes, for error messages. */
export function objectName(statement: string): string {
  try {
    const kind = classify(statement);
    if ("name" in kind) return kind.name;
    if ("table" in kind) return kind.table;
  } catch {
    // fall through
  }
  return writeTarget(statement) ?? statement.replace(/\s+/g, " ").slice(0, 60);
}

/** Transforms a migration list in order with a fresh transform. */
export function transformMigrations(
  migrations: ReadonlyArray<{ name: string; sql: string }>,
  options: TransformOptions = {},
): { version: string; migrations: TransformedMigration[]; added: AddedObject[]; transform: SchemaTransform } {
  const transform = new SchemaTransform(options);
  const out = migrations.map((m) => transform.migration(m.name, m.sql));
  return { version: TRANSFORM_VERSION, migrations: out, added: transform.addedObjects(), transform };
}
