// Change-feed installation: provision.rs `install_change_feed_locked`, as a
// plan of units K02 can apply across storage transactions.
//
// A unit is the statements for one receipt in `_gonvex_sync_bootstrap`
// (`infrastructure`, `table:<t>`, `dependency:<digest>`) plus that receipt's
// digest. Digests are SHA-256 of the Rust-form script (triggers.ts), so the
// bootstrap table equals Rust's; statements are the plain-SQL form
// (images.ts). A unit is planned only when its receipt or its installed
// trigger SQL does not match, as in Rust, so a warm wake plans nothing.
//
// Capture classes (`_gonvex_table_options.capture`):
//   entity   AFTER triggers with row images (the default for every table);
//   owned    BEFORE triggers staging the owning logical record;
//   derived  no capture: the derived-table-capture-v1 rule. Rust installs no
//            triggers on a derived table, drops its old capture and receipt,
//            and requires the owner foreign key it declares;
//   metadata no capture.
// Logical record storage tables get record triggers; record dependencies get
// owned-style triggers under their own names.

import type { SchemaTransform } from "../schema-transform/index.ts";
import { splitStatements } from "../schema-transform/index.ts";
import { sha256Hex } from "./digest.ts";
import { keepRowidReference, plainCaptureStatements } from "./images.ts";
import { SCHEMA_OPTIONS_SQL, SYNC_INFRASTRUCTURE_SQL } from "./infrastructure.ts";
import { objects, quoteIdentifier, scalar, type CaptureSql } from "./sql.ts";
import {
  captureTriggerNames,
  dependencyTriggerNames,
  entityCaptureSql,
  ownedCaptureSql,
  recordCaptureSql,
  triggerStatements,
  type RecordOwner,
} from "./triggers.ts";

export const INFRASTRUCTURE_SQL = `${SYNC_INFRASTRUCTURE_SQL}\n${SCHEMA_OPTIONS_SQL}`;
const BOOTSTRAP_TABLE_SQL = "CREATE TABLE IF NOT EXISTS _gonvex_sync_bootstrap (name TEXT PRIMARY KEY, digest TEXT NOT NULL)";

export interface InstallUnit {
  readonly kind: "infrastructure" | "table" | "dependency" | "drop";
  /** `_gonvex_sync_bootstrap.name`; empty for a unit that only drops retired dependency triggers. */
  readonly receipt: string;
  /** SHA-256 of the Rust-form script; null deletes the receipt. */
  readonly digest: string | null;
  /** Plain-SQL statements, in order, without trailing `;`. */
  readonly statements: readonly string[];
}

export class CaptureInstallError extends Error {
  override readonly name = "CaptureInstallError";
}

/** provision.rs `bootstrap_digest`. */
export const bootstrapDigest = sha256Hex;

interface Column {
  name: string;
  declared: string;
  notNull: boolean;
  pk: number;
}

interface RecordRow {
  name: string;
  storage_table: string;
  key_column: string;
  schema: string;
}

const REQUIRED: ReadonlyArray<[string, string]> = [
  ["table", "_gonvex_sync_clock"], ["table", "_gonvex_sync_changes"], ["table", "_gonvex_sync_transactions"], ["table", "_gonvex_action_outbox"],
  ["table", "_gonvex_schema_capabilities"], ["table", "_gonvex_table_options"], ["table", "_gonvex_id_domains"], ["table", "_gonvex_identity_keys"],
  ["table", "_gonvex_interned_text"], ["table", "_gonvex_record_views"], ["table", "_gonvex_record_dependencies"], ["table", "_gonvex_record_pending"],
  ["table", "_gonvex_record_guard"], ["index", "gonvex_sync_changes_revision"], ["index", "gonvex_sync_changes_table"], ["index", "gonvex_sync_changes_pending"],
  ["index", "gonvex_sync_changes_created_at"], ["index", "gonvex_sync_changes_command"], ["index", "_gonvex_action_outbox_pending"],
  ["index", "_gonvex_action_outbox_pending_path"], ["index", "_gonvex_action_outbox_processing"],
];

const tableExists = (sql: CaptureSql, name: string) =>
  Number(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)", [name])) ?? 0) === 1;

function infrastructurePresent(sql: CaptureSql): boolean {
  const predicates = REQUIRED.map(([kind, name]) => `(type='${kind}' AND name='${name}')`).join(" OR ");
  if (Number(scalar(sql.run(`SELECT count(*) FROM sqlite_schema WHERE ${predicates}`))) !== REQUIRED.length) return false;
  return Number(scalar(sql.run("SELECT EXISTS(SELECT 1 FROM _gonvex_sync_clock WHERE singleton=1)"))) === 1;
}

/** Rust's `{:?}` of a string, for error texts. */
const debug = (value: string) => JSON.stringify(value);

/** `JsonKind::from_declared(kind) == Text` for the record schema's public key. */
function publicKeyIsText(schemaJson: string): boolean {
  const schema = JSON.parse(schemaJson) as { key?: string; columns?: Record<string, { type?: string }> };
  const type = schema.columns?.[schema.key ?? ""]?.type;
  if (typeof type !== "string") return false;
  const d = type.toUpperCase();
  return !(d.includes("JSON") || d.includes("BOOL") || d.includes("INT") || d.includes("REAL") || d.includes("DOUB") || d.includes("FLOA") || d.includes("NUMERIC") || d.includes("DECIMAL"));
}

/**
 * provision.rs `application_tables`, over `sqlite_schema` (0016 G0-12:
 * `pragma_table_list` costs seconds of CPU on the Whagons schema). The
 * platform's own `_cf_*` tables are excluded too, and so is local workerd's
 * `__miniflare_*` bookkeeping, which the platform does not have.
 */
function applicationTables(sql: CaptureSql, hasOptions: boolean): string[] {
  // Rust excludes a 7-term UNION ALL; Durable Object SQLite refuses more than 5 compound terms (0014 T1).
  const options = hasOptions
    ? "AND m.name NOT IN (SELECT name FROM _gonvex_table_options WHERE name IS NOT NULL AND capture NOT IN ('entity','owned'))"
    : "";
  return sql
    .run(
      `WITH virtual(name) AS MATERIALIZED (
         SELECT name FROM sqlite_schema WHERE type='table' AND sql LIKE 'CREATE VIRTUAL TABLE%')
       SELECT m.name FROM sqlite_schema m
       WHERE m.type='table'
         AND m.name NOT LIKE '\\_gonvex%' ESCAPE '\\'
         AND m.name NOT LIKE 'gonvex\\_%' ESCAPE '\\'
         AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
         AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\'
         AND m.name NOT LIKE '\\_\\_miniflare\\_%' ESCAPE '\\'
         AND NOT EXISTS (SELECT 1 FROM virtual v WHERE m.name IN (v.name, v.name||'_data', v.name||'_idx', v.name||'_content', v.name||'_docsize', v.name||'_config'))
         ${options}
       ORDER BY m.name`,
    )
    .rows.map((r) => String(r[0]));
}

/** provision.rs `all_table_columns` (declared types, pk position), for ordinary tables. */
function allTableColumns(sql: CaptureSql): Map<string, Column[]> {
  const rows = objects(
    sql.run(
      `SELECT m.name AS tbl, p.name AS name, p.type AS declared, p."notnull" AS nn, p.pk AS pk
       FROM sqlite_schema AS m JOIN pragma_table_info(m.name) AS p
       WHERE m.type = 'table' AND m.sql NOT LIKE 'CREATE VIRTUAL TABLE%' AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
       ORDER BY m.name, p.cid`,
    ),
  );
  const tables = new Map<string, Column[]>();
  for (const r of rows) {
    const list = tables.get(String(r.tbl)) ?? [];
    list.push({ name: String(r.name), declared: String(r.declared ?? ""), notNull: Number(r.nn) !== 0, pk: Number(r.pk) });
    tables.set(String(r.tbl), list);
  }
  return tables;
}

export interface PlanOptions {
  /** K02's transform instance (knows split views); a fresh one by default. */
  readonly transform?: SchemaTransform;
}

/**
 * The units that bring the change feed up to date, in Rust's order:
 * infrastructure, stale table receipts, tables (by name), record dependencies,
 * retired dependency triggers and receipts. Reads the live catalog once.
 */
export function planChangeFeedInstall(sql: CaptureSql, options: PlanOptions = {}): InstallUnit[] {
  const units: InstallUnit[] = [];
  const hasBootstrap = tableExists(sql, "_gonvex_sync_bootstrap");
  const receipts = new Map<string, string>(
    hasBootstrap ? sql.run("SELECT name, digest FROM _gonvex_sync_bootstrap").rows.map((r) => [String(r[0]), String(r[1])]) : [],
  );
  const infraDigest = bootstrapDigest(INFRASTRUCTURE_SQL);
  if (receipts.get("infrastructure") !== infraDigest || !infrastructurePresent(sql)) {
    units.push({ kind: "infrastructure", receipt: "infrastructure", digest: infraDigest, statements: [BOOTSTRAP_TABLE_SQL, ...splitStatements(INFRASTRUCTURE_SQL)] });
  } else if (!hasBootstrap) {
    units.push({ kind: "infrastructure", receipt: "infrastructure", digest: infraDigest, statements: [BOOTSTRAP_TABLE_SQL] });
  }
  // Until the infrastructure unit is applied, its tables read as empty.
  const hasOptions = tableExists(sql, "_gonvex_table_options");
  const hasRecords = tableExists(sql, "_gonvex_record_views");
  const hasDependencies = tableExists(sql, "_gonvex_record_dependencies");

  if (hasOptions) {
    const invalid = scalar(
      sql.run(
        `SELECT o.name FROM _gonvex_table_options o WHERE o.capture='derived' AND NOT EXISTS(SELECT 1 FROM pragma_foreign_key_list(o.name) f WHERE f."from"=o.owner_column AND f."table"=o.owner_table) LIMIT 1`,
      ),
    );
    if (invalid !== undefined && invalid !== null) throw new CaptureInstallError(`derived table ${debug(String(invalid))} must declare its owner foreign key`);
  }
  const recordRows = hasRecords ? (objects(sql.run("SELECT name,storage_table,key_column,schema FROM _gonvex_record_views ORDER BY name")) as unknown as RecordRow[]) : [];
  /** Keyed by storage table, as record_views.rs `descriptors`. */
  const records = new Map(recordRows.map((r) => [r.storage_table, r]));
  const owned = new Map<string, { table: string; column: string; direction: string }>(
    hasOptions
      ? objects(sql.run("SELECT name,owner_table,owner_column,owner_direction FROM _gonvex_table_options WHERE capture='owned'")).map((r) => [
          String(r.name),
          { table: String(r.owner_table), column: String(r.owner_column), direction: String(r.owner_direction) },
        ])
      : [],
  );
  const tables = applicationTables(sql, hasOptions);
  const columns = allTableColumns(sql);
  const withoutRowid = new Set(
    sql.run("SELECT name FROM sqlite_schema WHERE type='table' AND sql LIKE '%WITHOUT%ROWID%'").rows.map((r) => String(r[0])),
  );
  const installed = new Map<string, string>(
    objects(sql.run("SELECT tbl_name, name, sql FROM sqlite_schema WHERE type='trigger' AND sql IS NOT NULL")).map((r) => [`${String(r.tbl_name)}\u0000${String(r.name)}`, String(r.sql)]),
  );
  const fkExists = (from: string, to: string, column: string, target: string) =>
    Number(scalar(sql.run(`SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_list(?1) WHERE "table"=?2 AND "from"=?3 AND "to"=?4)`, [from, to, column, target]))) === 1;
  const integerOwnerKey = (owner: RecordRow) =>
    (columns.get(owner.storage_table) ?? []).some((c) => c.name === owner.key_column && c.declared.toUpperCase() === "INTEGER") && publicKeyIsText(owner.schema);
  const asOwner = (r: RecordRow): RecordOwner => ({ name: r.name, storageTable: r.storage_table, keyColumn: r.key_column });
  const plain = (name: string, script: string, shapes: Array<{ name: string; columns: Column[] }>) =>
    plainCaptureStatements(name, script, { records: recordRows, tables: shapes.map((s) => ({ name: s.name, columns: s.columns.map((c) => ({ name: c.name, declared: c.declared, pk: c.pk })) })) }, options.transform);
  const matches = (table: string, statements: readonly string[], names: readonly string[]) => {
    const created = statements.filter((s) => /^\s*CREATE\s+TRIGGER\b/i.test(s));
    return created.length === names.length && names.every((name, i) => installed.get(`${table}\u0000${name}`)?.trim() === created[i]!.trim());
  };

  const active = new Set(tables);
  for (const name of [...receipts.keys()].sort()) {
    const table = name.startsWith("table:") ? name.slice("table:".length) : undefined;
    if (table === undefined || active.has(table)) continue;
    units.push({ kind: "drop", receipt: name, digest: null, statements: captureTriggerNames(table).map((t) => `DROP TRIGGER IF EXISTS ${quoteIdentifier(t)}`) });
  }

  for (const table of tables) {
    const cols = columns.get(table) ?? [];
    const key = cols.find((c) => c.pk !== 0)?.name;
    if (key === undefined) throw new CaptureInstallError(`change-feed table ${debug(table)} has no primary key`);
    let script: string;
    let applied: string | undefined;
    let shapes: Array<{ name: string; columns: Column[] }> = [];
    const own = owned.get(table);
    if (own) {
      const owner = records.get(own.table);
      if (!owner) throw new CaptureInstallError(`owned storage ${debug(table)} needs a logical record owner`);
      const inbound = own.direction === "inbound";
      const [from, to, column, target] = inbound ? [own.table, table, own.column, key] : [table, own.table, own.column, owner.key_column];
      if (!fkExists(from, to, column, target)) throw new CaptureInstallError(`owned storage ${debug(table)} must declare its owner relationship in SQL`);
      script = ownedCaptureSql(table, key, asOwner(owner), own.column, inbound, integerOwnerKey(owner));
    } else {
      const record = records.get(table);
      if (record) {
        if (record.key_column !== key) {
          throw new CaptureInstallError(`logical record ${debug(record.name)} declares storage key ${debug(record.key_column)}, but ${debug(table)} has primary key ${debug(key)}`);
        }
        const integer = cols.some((c) => c.name === record.key_column && c.declared.toUpperCase() === "INTEGER") && publicKeyIsText(record.schema);
        script = recordCaptureSql(table, record.key_column, record.name, integer);
      } else {
        script = entityCaptureSql(table, key);
        shapes = [{ name: table, columns: cols }];
        if (withoutRowid.has(table)) applied = keepRowidReference(script);
      }
    }
    const receipt = `table:${table}`;
    const digest = bootstrapDigest(script);
    const statements = plain(receipt, applied ?? script, shapes);
    if (receipts.get(receipt) === digest && matches(table, statements, captureTriggerNames(table))) continue;
    units.push({ kind: "table", receipt, digest, statements });
  }

  const expectedTriggers = new Set<string>();
  const expectedReceipts = new Set<string>();
  const dependencies = hasDependencies
    ? objects(sql.run("SELECT source_table,source_column,owner_storage,owner_column,owner_direction,watch_columns FROM _gonvex_record_dependencies ORDER BY 1,2,3,4"))
    : [];
  for (const d of dependencies) {
    const source = String(d.source_table), sourceKey = String(d.source_column), ownerTable = String(d.owner_storage), column = String(d.owner_column);
    const inbound = String(d.owner_direction) === "inbound";
    const owner = records.get(ownerTable);
    if (!owner) throw new CaptureInstallError(`record dependency needs a logical owner: ${debug(ownerTable)}`);
    const [from, to, target] = inbound ? [ownerTable, source, sourceKey] : [source, ownerTable, owner.key_column];
    if (!fkExists(from, to, column, target)) throw new CaptureInstallError(`record dependency ${debug(source)} -> ${debug(ownerTable)} needs a native SQL relationship`);
    let watch: string[];
    try {
      watch = JSON.parse(String(d.watch_columns)) as string[];
    } catch (error) {
      throw new CaptureInstallError(`invalid dependency columns: ${String(error)}`);
    }
    const valid = Number(scalar(sql.run("SELECT count(*) FROM pragma_table_info(?1) WHERE name IN (SELECT value FROM json_each(?2))", [source, String(d.watch_columns)])));
    if (valid !== watch.length) throw new CaptureInstallError(`record dependency references unknown or repeated columns on ${debug(source)}`);
    const namespace = bootstrapDigest(`${source}:${sourceKey}:${ownerTable}:${column}`);
    const script = ownedCaptureSql(source, sourceKey, asOwner(owner), column, inbound, integerOwnerKey(owner), { namespace, watch });
    const names = dependencyTriggerNames(namespace);
    for (const n of names) expectedTriggers.add(n);
    const receipt = `dependency:${namespace}`;
    expectedReceipts.add(receipt);
    const digest = bootstrapDigest(script);
    const statements = plain(receipt, script, []);
    if (receipts.get(receipt) === digest && matches(source, statements, names)) continue;
    units.push({ kind: "dependency", receipt, digest, statements });
  }
  const retired: string[] = [];
  for (const k of installed.keys()) {
    const name = k.slice(k.indexOf("\u0000") + 1);
    if (name.startsWith("gonvex_record_dependency_") && !expectedTriggers.has(name)) retired.push(`DROP TRIGGER ${quoteIdentifier(name)}`);
  }
  const staleDependencies = [...receipts.keys()].filter((n) => n.startsWith("dependency:") && !expectedReceipts.has(n));
  if (retired.length) units.push({ kind: "drop", receipt: "", digest: null, statements: retired });
  for (const receipt of staleDependencies) units.push({ kind: "drop", receipt, digest: null, statements: [] });
  return units;
}

/** Applies one unit and writes (or deletes) its receipt, in the caller's transaction. */
export function applyInstallUnit(sql: CaptureSql, unit: InstallUnit): void {
  for (const statement of unit.statements) sql.run(statement);
  if (!unit.receipt) return;
  if (unit.digest === null) sql.run("DELETE FROM _gonvex_sync_bootstrap WHERE name=?1", [unit.receipt]);
  else sql.run("INSERT INTO _gonvex_sync_bootstrap(name,digest) VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET digest=excluded.digest", [unit.receipt, unit.digest]);
}

export interface InstallResult {
  readonly units: readonly InstallUnit[];
  readonly statements: number;
}

/**
 * Plans and applies the whole install in the caller's transaction (tests,
 * small schemas). The full Whagons install does not fit one transaction under
 * the object's CPU limit; K02 applies `planChangeFeedInstall` in chunks. The
 * plan is read again after the infrastructure unit, because the table units
 * depend on the tables it creates.
 */
export function installChangeFeed(sql: CaptureSql, options: PlanOptions = {}): InstallResult {
  const applied: InstallUnit[] = [];
  let plan = planChangeFeedInstall(sql, options);
  if (plan[0]?.kind === "infrastructure") {
    applyInstallUnit(sql, plan[0]);
    applied.push(plan[0]);
    plan = planChangeFeedInstall(sql, options);
  }
  for (const unit of plan) {
    applyInstallUnit(sql, unit);
    applied.push(unit);
  }
  return { units: applied, statements: applied.reduce((n, u) => n + u.statements.length, 0) };
}
