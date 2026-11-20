// One SQL resolution of a member's principal per (plan sets, account), shared
// by every collection and open that needs it (principal_cache.rs).
//
// An entry is the SQL part of a version 2 resolution (member row, principal,
// grant and derived sets) for one account, keyed by the plan's set
// definitions, valid over a revision range. A plan reuses it only when its
// active sets for the member's role are the entry's. An open at another
// revision reuses it only when the change log proves that no table the
// resolution read changed in between: one indexed probe of
// `_gonvex_sync_changes` in the open's own snapshot. Tables without entity
// change capture are never cached, nor are logical record storage and owned
// tables (their changes are logged under the record's name); the record view
// itself is cacheable.
//
// X17p: when an input table changed between the entry and a later open, the
// entry is rolled forward through those commits instead of dropped. A change to
// `members` (role, member row) still drops it. Otherwise each active set whose
// joined tables changed, or whose base table changed other than by inserts, is
// re-run with the same SQL; a set whose base table only gained rows adds what
// those rows contribute (a set is inner joins and equality predicates, so it is
// monotone in its rows); derived sets are re-run. An insert without a row
// image, a REPLACE marker (`../capture/replace-marker.ts`: rows may have been
// deleted unlogged), or inserted keys that do not match their rows exactly
// (`insertedKeysExact`) count as other changes, so the set re-runs. The rolled
// sets are the sets a fresh resolution at the open's revision returns. Rust
// drops the entry here; both give the same resolution, fingerprint and frames.
//
// Synchronous: Durable Object SQL is, and the object runs one event at a
// time, so Rust's single-flight lock per key is not needed.

import { sha256Hex } from "../capture/digest.ts";
import { markedTable, replacedMarker } from "../capture/replace-marker.ts";
import { activeSets, setOf, setsSignatureText, type VisibilityPlan } from "./plan.ts";
import { addInsertedRowsInto, resolveDerived, resolveSetInto, visibilityFingerprint, type VisibilitySession, type VisibilitySql } from "./principal.ts";
import type { DirectContext, ResolvedVisibility, TokenSets } from "./resolved.ts";
import type { JsonValue } from "./json.ts";

/** The read snapshot's clock (`_gonvex_sync_clock`). */
export interface PrincipalClock {
  readonly epoch: string;
  readonly revision: number;
  readonly retained: number;
}

interface Core {
  readonly direct: DirectContext;
  readonly role: string;
  readonly permissions: JsonValue;
  readonly sets: TokenSets;
}

interface Entry {
  epoch: string;
  /** Valid at every revision in `low..=revision`. */
  low: number;
  revision: number;
  readonly core: Core;
  /** The sets resolved (the plan's active sets for `core.role`), joined. */
  readonly active: string;
  readonly fingerprints: WeakMap<VisibilityPlan, string>;
  /** Tables the resolution read. */
  readonly tables: readonly string[];
  used: number;
}

export interface PrincipalCacheStats {
  entries: number;
  /** Reused within the entry's own revision range (no SQL). */
  hits: number;
  /** Reused after a change-log probe found no change. */
  probedHits: number;
  /** Resolved with SQL: no entry. */
  misses: number;
  /** Resolved with SQL: an input table changed since the entry and the entry could not be rolled forward. */
  invalidated: number;
  /** Rolled forward through changed input tables (X17p): only the affected sets ran SQL. */
  rolledForward: number;
  /** Sets re-run by roll-forwards. */
  setsRerun: number;
  /** Sets patched with their base table's inserted rows by roll-forwards. */
  setsPatched: number;
  /** Resolved with SQL and not stored: an input table has no change capture. */
  uncacheable: number;
  evictions: number;
}

export interface PrincipalCacheConfig {
  readonly enabled: boolean;
  /** Bound on entries (Rust default 8192). */
  readonly maxEntries: number;
  /** Entries unused for this long are dropped (Rust default 120 s). */
  readonly ttlMs: number;
  readonly now: () => number;
}

const DEFAULT_CONFIG: PrincipalCacheConfig = { enabled: true, maxEntries: 8192, ttlMs: 120_000, now: () => Date.now() };

export class PrincipalCache {
  private readonly config: PrincipalCacheConfig;
  private enabled: boolean;
  private readonly entries = new Map<string, Entry>();
  private captured: ReadonlySet<string> | undefined;
  private inserts = 0;
  private readonly counters = { hits: 0, probedHits: 0, misses: 0, invalidated: 0, rolledForward: 0, setsRerun: 0, setsPatched: 0, uncacheable: 0, evictions: 0 };
  private keys: ReadonlyMap<string, string> | undefined;
  private readonly signatures = new WeakMap<VisibilityPlan, string>();

  constructor(config: Partial<PrincipalCacheConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.enabled = this.config.enabled;
  }

  /** Switches sharing on or off (oracles compare both). */
  setEnabled(enabled: boolean): void {
    this.enabled = enabled;
    if (!enabled) this.entries.clear();
  }

  stats(): PrincipalCacheStats {
    return { entries: this.entries.size, ...this.counters };
  }

  /** Call when the schema changes (migrations, a new generation): capture classes may have changed. */
  invalidateSchema(): void {
    this.captured = undefined;
    this.keys = undefined;
    this.entries.clear();
  }

  /** The key of `plan` for `session`, or undefined when never shared (cache off, developer sessions). */
  key(session: VisibilitySession, plan: VisibilityPlan): string | undefined {
    if (!this.enabled || (session.developerSession !== null && session.developerSession !== undefined)) return undefined;
    let signature = this.signatures.get(plan);
    if (signature === undefined) this.signatures.set(plan, (signature = sha256Hex(setsSignatureText(plan))));
    return `${session.route.tenantId}\u0000${session.identity.account.id}\u0000${signature}`;
  }

  /**
   * A shared resolution valid at `clock`, proving validity with the change
   * log when the entry is at another revision. Returns a function building the
   * resolution at a revision (no parents or indexes attached yet).
   */
  lookup(key: string, plan: VisibilityPlan, sql: VisibilitySql, clock: PrincipalClock): ((revision: number) => ResolvedVisibility) | undefined {
    const entry = this.entries.get(key);
    if (!entry) {
      this.counters.misses += 1;
      return undefined;
    }
    // Same set definitions; the plan must also select the same sets for this role.
    if (entry.epoch !== clock.epoch || activeKey(plan, entry.core.role) !== entry.active) {
      this.entries.delete(key);
      this.counters.misses += 1;
      return undefined;
    }
    entry.used = this.config.now();
    if (clock.revision >= entry.low && clock.revision <= entry.revision) {
      this.counters.hits += 1;
      return this.shared(entry, plan);
    }
    const low = Math.min(entry.revision, clock.revision);
    const high = Math.max(entry.low, clock.revision);
    if (low < clock.retained) {
      this.entries.delete(key);
      this.counters.misses += 1;
      return undefined;
    }
    if (clock.revision > entry.revision) {
      const changes = changeSummary(sql, entry.tables, entry.revision, clock.revision);
      if (changes.size) {
        const rolled = this.rollForward(entry, plan, sql, clock, changes);
        if (!rolled) {
          this.counters.invalidated += 1;
          this.entries.delete(key);
          return undefined;
        }
        this.entries.set(key, rolled);
        this.counters.rolledForward += 1;
        return this.shared(rolled, plan);
      }
    } else if (changed(sql, entry.tables, low, high)) {
      this.counters.invalidated += 1;
      this.entries.delete(key);
      return undefined;
    }
    // No input changed in (low, high]: the entry holds over the union.
    entry.low = Math.min(entry.low, clock.revision);
    entry.revision = Math.max(entry.revision, clock.revision);
    this.counters.probedHits += 1;
    return this.shared(entry, plan);
  }

  /**
   * `entry` at `clock.revision`, from the input tables' changes in `(entry.revision, clock.revision]`, or undefined
   * when the member must be resolved again (a `members` change).
   */
  private rollForward(entry: Entry, plan: VisibilityPlan, sql: VisibilitySql, clock: PrincipalClock, changes: ReadonlyMap<string, TableChanges>): Entry | undefined {
    if (changes.has("members")) return undefined;
    const core = entry.core;
    const keys = this.entityKeys(sql);
    const sets = new Map<string, ReadonlySet<string>>(core.sets);
    const exact = new Map<string, boolean>();
    const patchable = (table: string, key: string) => {
      let found = exact.get(table);
      if (found === undefined) exact.set(table, (found = insertedKeysExact(sql, table, key, entry.revision, clock.revision)));
      return found;
    };
    for (const name of activeSets(plan, core.role)) {
      const set = setOf(plan, name);
      if (set.kind === "derived") continue;
      // A changed joined table (including the base table joined to itself) can change any row's contribution.
      if (set.joins.some((join) => changes.has(join.table))) {
        resolveSetInto(sql, plan, name, set, core.direct, sets);
        this.counters.setsRerun += 1;
        continue;
      }
      const base = changes.get(set.table);
      if (!base) continue;
      const key = keys.get(set.table);
      if (base.insertsOnly && base.rows <= MAX_PATCHED_ROWS && key !== undefined && patchable(set.table, key)) {
        addInsertedRowsInto(sql, name, set, core.direct, sets, key, entry.revision, clock.revision);
        this.counters.setsPatched += 1;
      } else {
        resolveSetInto(sql, plan, name, set, core.direct, sets);
        this.counters.setsRerun += 1;
      }
    }
    const resolved: ResolvedVisibility = {
      ephemeralMember: false,
      revision: clock.revision,
      direct: core.direct,
      role: core.role,
      permissions: core.permissions,
      sets,
      fingerprint: "",
      inherited: new Map(),
      indexes: new Map(),
    };
    // Re-runs every derived set from the rolled sets (and sets the fingerprint).
    resolveDerived(sql, plan, resolved);
    const fingerprints = new WeakMap<VisibilityPlan, string>();
    fingerprints.set(plan, resolved.fingerprint);
    return {
      epoch: entry.epoch,
      low: clock.revision,
      revision: clock.revision,
      core: { direct: core.direct, role: core.role, permissions: core.permissions, sets: resolved.sets },
      active: entry.active,
      fingerprints,
      tables: entry.tables,
      used: entry.used,
    };
  }

  /** Each entity-captured table's key column: the column its insert capture trigger logs as `row_id`. */
  private entityKeys(sql: VisibilitySql): ReadonlyMap<string, string> {
    if (this.keys) return this.keys;
    const captured = this.capturedTables(sql);
    const keys = new Map<string, string>();
    for (const row of sql.run("SELECT name, coalesce(sql, '') FROM sqlite_schema WHERE type = 'trigger' AND name LIKE '\\_gonvex\\_sync\\_%' ESCAPE '\\'").rows) {
      const match = /^_gonvex_sync_(.*)_insert$/.exec(String(row[0]));
      if (!match || !captured.has(match[1]!)) continue;
      const key = entityKeyColumn(String(row[1]));
      if (key !== undefined) keys.set(match[1]!, key);
    }
    this.keys = keys;
    return keys;
  }

  private shared(entry: Entry, plan: VisibilityPlan): (revision: number) => ResolvedVisibility {
    const core = entry.core;
    let fingerprint = entry.fingerprints.get(plan);
    if (fingerprint === undefined) entry.fingerprints.set(plan, (fingerprint = visibilityFingerprint(plan, core.direct, core.role, core.permissions, core.sets)));
    const value = fingerprint;
    // The core is shared and never mutated: resolutions replace `sets` instead of editing it.
    return (revision) => ({ ephemeralMember: false, revision, direct: core.direct, role: core.role, permissions: core.permissions, sets: core.sets, fingerprint: value, inherited: new Map(), indexes: new Map() });
  }

  /** Records a fresh SQL resolution of `plan` at `clock`. */
  store(key: string, plan: VisibilityPlan, sql: VisibilitySql, clock: PrincipalClock, resolved: ResolvedVisibility): void {
    if (resolved.ephemeralMember || !this.enabled) return;
    const tables = inputTables(plan, resolved.role);
    const captured = this.capturedTables(sql);
    if (!tables.every((table) => captured.has(table))) {
      this.counters.uncacheable += 1;
      return;
    }
    const fingerprints = new WeakMap<VisibilityPlan, string>();
    fingerprints.set(plan, resolved.fingerprint);
    this.entries.delete(key);
    this.entries.set(key, {
      epoch: clock.epoch,
      low: clock.revision,
      revision: clock.revision,
      core: { direct: resolved.direct, role: resolved.role, permissions: resolved.permissions, sets: resolved.sets },
      active: activeKey(plan, resolved.role),
      fingerprints,
      tables,
      used: this.config.now(),
    });
    this.inserts += 1;
    if (this.inserts % 256 === 1 || this.entries.size > this.config.maxEntries) this.sweep();
  }

  /** Drops entries unused for the TTL; then, over the bound, the least recently used eighth. */
  sweep(): void {
    const now = this.config.now();
    const before = this.entries.size;
    for (const [key, entry] of this.entries) if (now - entry.used >= this.config.ttlMs) this.entries.delete(key);
    if (this.entries.size > this.config.maxEntries) {
      const used = [...this.entries.values()].map((entry) => entry.used).sort((a, b) => a - b);
      const cutoff = used[Math.max(Math.floor(this.entries.size / 8), this.entries.size - this.config.maxEntries)]!;
      for (const [key, entry] of this.entries) if (entry.used <= cutoff) this.entries.delete(key);
    }
    this.counters.evictions += before - this.entries.size;
  }

  /** Relations whose changes the log records under their own name (cached until `invalidateSchema`). */
  capturedTables(sql: VisibilitySql): ReadonlySet<string> {
    if (this.captured) return this.captured;
    const triggers = sql.run("SELECT name, coalesce(sql, '') FROM sqlite_schema WHERE type = 'trigger' AND name LIKE '\\_gonvex\\_sync\\_%' ESCAPE '\\'").rows.map(
      (row) => [String(row[0]), String(row[1])] as const,
    );
    const hasRecords = sql.run("SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_gonvex_record_views')").rows[0]?.[0];
    const records = hasRecords ? sql.run("SELECT name, storage_table FROM _gonvex_record_views").rows.map((row) => [String(row[0]), String(row[1])] as const) : [];
    this.captured = capturedTables(triggers, records);
    return this.captured;
  }
}

function activeKey(plan: VisibilityPlan, role: string): string {
  return [...activeSets(plan, role)].join("\u0000");
}

/** Every table a version 2 resolution of `plan` for `role` reads: `members` and each active set's base and join tables. */
export function inputTables(plan: VisibilityPlan, role: string): string[] {
  const tables = new Set(["members"]);
  for (const name of activeSets(plan, role)) {
    const set = setOf(plan, name);
    tables.add(set.table);
    if (set.kind !== "derived") for (const join of set.joins) tables.add(join.table);
  }
  return [...tables].sort();
}

/** Bound on a base table's changes patched into a set; above it the set is re-run. */
const MAX_PATCHED_ROWS = 256;

interface TableChanges {
  readonly rows: number;
  readonly insertsOnly: boolean;
}

/**
 * Per input table with changes in `(low, high]`: how many, and whether all are inserts that carry a row image.
 * A REPLACE marker of a table (`replace-marker.ts`) makes it a non-insert change: rows may have gone unlogged.
 * Index seeks on `(table_name, revision)`.
 */
export function changeSummary(sql: VisibilitySql, tables: readonly string[], low: number, high: number): Map<string, TableChanges> {
  const found = new Map<string, TableChanges>();
  if (!tables.length || low >= high) return found;
  const names = [...tables, ...tables.map(replacedMarker)];
  const list = names.map((_, index) => `$${index + 3}`).join(", ");
  const rows = sql.run(
    `SELECT table_name, count(*), sum(operation <> 'insert' OR new_value IS NULL) FROM _gonvex_sync_changes WHERE table_name IN (${list}) AND revision > $1 AND revision <= $2 GROUP BY table_name`,
    [low, high, ...names],
  ).rows;
  for (const row of rows) {
    const name = String(row[0]);
    const marked = markedTable(name);
    const table = marked ?? name;
    const before = found.get(table);
    found.set(table, { rows: Number(row[1]) + (before?.rows ?? 0), insertsOnly: marked === undefined && Number(row[2]) === 0 && (before?.insertsOnly ?? true) });
  }
  return found;
}

/**
 * Whether the inserts of `table` in `(low, high]` can be matched exactly by their typed key images (#555 R2):
 * every image's key is an integer, real or text whose text form is the logged `row_id` (a BLOB key's image is
 * hex text and fails this), and the distinct keys match as many stored rows as there are keys (under inserts
 * only, every inserted row still exists). Otherwise the set re-runs.
 */
export function insertedKeysExact(sql: VisibilitySql, table: string, key: string, low: number, high: number): boolean {
  const path = `$."${key}"`;
  const inserted = `FROM _gonvex_sync_changes WHERE table_name = $1 AND revision > $2 AND revision <= $3 AND operation = 'insert'`;
  const row = sql.run(
    `SELECT (SELECT count(*) ${inserted} AND (json_type(new_value, $4) NOT IN ('integer', 'real', 'text') OR CAST(json_extract(new_value, $4) AS TEXT) IS NOT row_id)),
            (SELECT count(DISTINCT json_extract(new_value, $4)) ${inserted}),
            (SELECT count(*) FROM "${table.replace(/"/g, '""')}" WHERE "${key}" IN (SELECT json_extract(new_value, $4) ${inserted}))`,
    [table, low, high, path],
  ).rows[0];
  return row !== undefined && Number(row[0]) === 0 && Number(row[1]) === Number(row[2]);
}

/**
 * The key column of an entity insert capture trigger (`entityCaptureSql`: `row_id` is `CAST(NEW."key" AS TEXT)`),
 * or undefined when the trigger has another form or the key cannot be a plain JSON path member.
 */
export function entityKeyColumn(trigger: string): string | undefined {
  const match = /VALUES\s*\(\s*'(?:[^']|'')*'\s*,\s*CAST\(NEW\."((?:[^"]|"")*)" AS TEXT\)\s*,\s*'insert'/.exec(trigger);
  if (!match) return undefined;
  const key = match[1]!.replace(/""/g, '"');
  return /^[^"\\]+$/.test(key) ? key : undefined;
}

/** `EXISTS` per table, each an index seek on `(table_name, revision)`. */
export function probeStatement(tables: number): string {
  const terms = Array.from({ length: tables }, (_, index) => `EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE table_name = $${index + 3} AND revision > $1 AND revision <= $2)`);
  return `SELECT (${terms.join(" OR ")})`;
}

/** Whether any of `tables` has a change in `(low, high]`. */
export function changed(sql: VisibilitySql, tables: readonly string[], low: number, high: number): boolean {
  if (!tables.length || low >= high) return false;
  const value = sql.run(probeStatement(tables.length), [low, high, ...tables]).rows[0]?.[0];
  return Number(value) !== 0;
}

const ENTITY_LOG = "_gonvex_sync_changes";
const RECORD_STAGE = "_gonvex_record_pending";

function captureTriggerNames(table: string): [string, string, string] {
  return [`_gonvex_sync_${table}_insert`, `_gonvex_sync_${table}_update`, `_gonvex_sync_${table}_delete`];
}

/**
 * The relations `_gonvex_sync_changes` logs under their own name, from the
 * `_gonvex_sync_*` triggers and the logical records. An ordinary table counts
 * when all three of its triggers write the log directly (entity capture: in
 * the kernel the row image is plain SQL, A01 T5, so the test is the log table
 * rather than Rust's `gonvex_capture(` call). Record storage and owned tables
 * stage the owning record in `_gonvex_record_pending` and never count; the
 * record view counts once its storage has all three staging triggers.
 */
export function capturedTables(triggers: ReadonlyArray<readonly [string, string]>, records: ReadonlyArray<readonly [string, string]>): Set<string> {
  const sql = new Map(triggers);
  const triplet = (table: string, body: (sql: string) => boolean) => captureTriggerNames(table).every((name) => {
    const found = sql.get(name);
    return found !== undefined && body(found);
  });
  const tables = new Set<string>();
  for (const name of sql.keys()) {
    const match = /^_gonvex_sync_(.*)_insert$/.exec(name);
    if (!match) continue;
    const table = match[1]!;
    if (triplet(table, (body) => body.includes(ENTITY_LOG) && !body.includes(RECORD_STAGE))) tables.add(table);
  }
  for (const [name, storage] of records) {
    if (triplet(storage, (body) => body.includes(RECORD_STAGE) && !body.includes(ENTITY_LOG))) tables.add(name);
  }
  return tables;
}
