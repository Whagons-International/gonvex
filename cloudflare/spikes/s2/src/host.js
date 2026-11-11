// Database host calls for one invocation, with the semantics of
// rust/crates/runtime/src/host_calls.rs (SqlCalls + DatabaseHostCalls) at the
// reference commit: single-statement admission, `$n` JSON parameters,
// pragma_table_info / _gonvex_record_views catalogs, typed RETURNING rows,
// the "aborted until ROLLBACK TO" rule, deterministic outbox and scheduler ids.
//
// The engine is synchronous SQL plus asynchronous savepoints:
//   engine.exec(sql, binds) -> { columnNames, rows: any[][], rowsRead, rowsWritten }
//   engine.savepoint(name) / engine.release(name) / engine.rollbackTo(name) -> Promise
//   engine.decltypes?(sql) -> (string|null)[]   (node:sqlite only; DO SQL has none)
import { admit, numberedParameters, writeTarget, selectProjection } from "./sql.js";
import { installed } from "./runtime-globals.js";

const ABORTED = "current transaction is aborted, commands ignored until end of transaction block";
const SCHEDULE_OUTBOX_PATH = "_gonvex.scheduler.enqueue";
const DEFAULT_KEY = "id";

export function kindOf(declared) {
  const type = String(declared ?? "").toUpperCase();
  if (type.includes("JSON")) return "json";
  if (type.includes("BOOL")) return "boolean";
  if (type.includes("INT")) return "integer";
  if (/REAL|DOUB|FLOA|NUMERIC|DECIMAL/.test(type)) return "real";
  return "text";
}
/** sqlx's declared-type mapping (host_calls.rs sqlx_declared_kind). */
function sqlxKind(declared) {
  if (declared == null) return "text";
  const d = declared.toLowerCase();
  if (d === "int4" || d === "int8") return "integer";
  if (d === "boolean" || d === "bool") return "boolean";
  if (["date", "time", "datetime", "timestamp"].includes(d)) return "text";
  if (d.includes("int")) return "integer";
  if (d.includes("char") || d.includes("clob") || d.includes("text")) return "text";
  if (d.includes("blob")) return "text";
  if (d.includes("real") || d.includes("floa") || d.includes("doub")) return "real";
  return "text";
}
const hex = (buffer) => Array.from(new Uint8Array(buffer), (b) => b.toString(16).padStart(2, "0")).join("");
const jsonShaped = (text) => { const t = text.trim(); return t[0] === "{" || t[0] === "[" || t[0] === '"' || t === "true" || t === "false"; };

/** direct.rs write_json: decode one cell by its declared kind. */
export function decodeCell(kind, value) {
  if (value === null || value === undefined) return null;
  if (value instanceof ArrayBuffer) return hex(value);
  if (kind === "boolean" && typeof value === "number") return value !== 0;
  if (kind === "json" && typeof value === "string") { try { return JSON.parse(value); } catch { return value; } }
  if (kind === undefined && typeof value === "string" && jsonShaped(value)) { try { return JSON.parse(value); } catch { return value; } }
  if (typeof value === "bigint") return Number(value);
  return value;
}

/** sql_parameters.rs Parameter::scalar / Parameter::column. */
function scalar(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "string" || typeof value === "number") return value;
  return JSON.stringify(value);
}
function columnParameter(kind, value) {
  if (value === null || value === undefined) return null;
  if (kind === "json") return JSON.stringify(value);
  if (kind === "boolean" && typeof value === "boolean") return value ? 1 : 0;
  return scalar(value);
}
const quote = (name) => {
  if (typeof name !== "string" || !name || name.includes("\0")) throw new Error(`invalid identifier ${JSON.stringify(name)}`);
  return `"${name.replaceAll('"', '""')}"`;
};

/** Same seed and UUID layout as module-sdk reducerRowId / host_calls.rs intent_deferred_id. */
async function intentDeferredId(subtle, tenant, account, command, kind, ordinal) {
  const seed = JSON.stringify(["gonvex.reducer.ids.v1", tenant, account, command, `deferred:${kind}`, ordinal]);
  const bytes = new Uint8Array(await subtle.digest("SHA-256", new TextEncoder().encode(seed))).slice(0, 16);
  bytes[6] = (bytes[6] & 15) | 128;
  bytes[8] = (bytes[8] & 63) | 128;
  const h = hex(bytes);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

export class SqlHost {
  /**
   * options: { capability: "reducer"|"query", subtle, identity, invocation,
   *   permissions(permission, scope) -> boolean, schedulable(path) -> boolean,
   *   decltypeFallback: boolean (node:sqlite reference only) }
   */
  constructor(engine, options) {
    this.engine = engine;
    this.options = options;
    this.tables = new Map();
    this.aborted = undefined;
    this.ordinals = new Map();
    this.stats = { calls: 0, statements: 0, rowsRead: 0, rowsWritten: 0, catalogStatements: 0, catalogRowsRead: 0, savepoints: 0, byKind: {}, decltypeDependentColumns: 0, statementsLog: [] };
  }

  exec(sql, binds, catalog = false) {
    this.stats.statements++;
    try {
      const result = this.engine.exec(sql, binds);
      this.stats.rowsRead += result.rowsRead ?? 0;
      if (catalog) { this.stats.catalogStatements++; this.stats.catalogRowsRead += result.rowsRead ?? 0; }
      this.stats.rowsWritten += result.rowsWritten ?? 0;
      return result;
    } catch (error) {
      const message = String(error?.message ?? error);
      this.aborted ??= message;
      throw new Error(message);
    }
  }

  requireOpen() { if (this.aborted !== undefined) throw new Error(ABORTED); }
  requireWrite() { if (this.options.capability !== "reducer") throw new Error("a Query cannot modify application tables"); }

  /** Host-call entry point: returns the bootstrap envelope. */
  async handle(payload) {
    this.stats.calls++;
    this.stats.byKind[payload.kind] = (this.stats.byKind[payload.kind] ?? 0) + 1;
    try {
      return { status: "ok", value: await this.dispatch(payload) };
    } catch (error) {
      return { status: "failed", message: String(error?.message ?? error) };
    }
  }

  async dispatch(payload) {
    switch (payload.kind) {
      case "dbQuery": return this.query(payload.statement, payload.parameters ?? []);
      case "dbInsert": this.requireWrite(); return this.insert(payload.table, payload.row, payload.generatedId);
      case "dbUpdate": this.requireWrite(); return this.update(payload.table, payload.id, payload.patch);
      case "dbDelete": this.requireWrite(); return this.delete(payload.table, payload.id);
      case "dbDeleteMany": this.requireWrite(); return this.deleteMany(payload.table, payload.ids);
      case "permissionsHas": return Boolean(this.options.permissions?.(payload.permission, payload.scope));
      case "actionEnqueue": return this.enqueueAction(payload.function, payload.args);
      case "scheduleAfter": this.requireWrite(); return this.enqueueScheduled(payload.function, payload.args, this.options.wallNow() + payload.delayMs, false);
      case "scheduleAt": this.requireWrite(); return this.enqueueScheduled(payload.function, payload.args, payload.atUnixMs, payload.coalesce === true);
      case "storage": throw new Error("storage metadata is unavailable");
      default: throw new Error(this.options.capability === "query" ? "a Query may only use its read-only database capability" : "this Reducer host capability is not implemented in the Rust runtime yet");
    }
  }

  async query(statement, parameters) {
    const admitted = admit(statement);
    if (this.options.capability === "query" && !["select", "with", "values", "explain"].includes(admitted.firstWord)) throw new Error("a Query cannot modify application tables");
    const head = admitted.firstWord;
    const savepoint = this.savepointCommand(admitted.tokens);
    const recovers = head === "rollback";
    if (!recovers) this.requireOpen();
    if (!Array.isArray(parameters)) throw new Error("query parameters must be an array");
    if (savepoint) {
      // DO SQL refuses SAVEPOINT/RELEASE/ROLLBACK TO; the engine maps them
      // onto nested storage transactions.
      this.stats.savepoints++;
      this.stats.statementsLog.push(statement.trim());
      if (savepoint.command === "savepoint") await this.engine.savepoint(savepoint.name);
      else if (savepoint.command === "release") await this.engine.release(savepoint.name);
      else { await this.engine.rollbackTo(savepoint.name); this.aborted = undefined; }
      return [];
    }
    const binds = parameters.map((value, index) => { try { return scalar(value); } catch (error) { throw new Error(`parameter $${index + 1}: ${error.message}`); } });
    const target = writeTarget(admitted.tokens);
    const catalog = target ? this.catalog(target) : undefined;
    const projection = catalog ? undefined : this.projectionKinds(admitted.tokens);
    const { sql, count } = numberedParameters(statement.trim(), admitted.tokens);
    if (count > binds.length) throw new Error(`statement uses $${count} but ${binds.length} parameters were bound`);
    this.stats.statementsLog.push(statement.trim());
    const result = this.exec(sql, binds.slice(0, Math.max(count, 0)));
    if (["create", "alter", "drop"].includes(head)) this.tables.clear();
    let decltypes;
    const kinds = result.columnNames.map((name, index) => {
      const fromProjection = projection && projection.length === result.columnNames.length ? projection[index] : undefined;
      if (fromProjection) return fromProjection;
      const fromTarget = catalog?.columns.get(name);
      if (fromTarget) return fromTarget;
      // Rust falls back to sqlite3_column_decltype here. DO SQL has no
      // decltype; count the columns whose typing depended on it.
      if (this.options.decltypeFallback && this.engine.decltypes) {
        decltypes ??= this.engine.decltypes(sql);
        const declared = decltypes[index];
        if (declared) { this.stats.decltypeDependentColumns++; return kindOf(declared); }
      }
      return undefined;
    });
    if (recovers) this.aborted = undefined;
    return result.rows.map((row) => Object.fromEntries(result.columnNames.map((name, index) => [name, decodeCell(kinds[index], row[index])])));
  }

  savepointCommand(tokens) {
    const w = (i) => (tokens[i]?.type === "word" ? tokens[i].lower : "");
    const name = (i) => tokens[i]?.value;
    if (w(0) === "savepoint") return { command: "savepoint", name: name(1) };
    if (w(0) === "release") return { command: "release", name: w(1) === "savepoint" ? name(2) : name(1) };
    if (w(0) === "rollback") {
      let i = 1;
      if (w(i) === "transaction" || w(i) === "work") i++;
      if (w(i) === "to") { i++; if (w(i) === "savepoint") i++; return { command: "rollback", name: name(i) }; }
    }
    return undefined;
  }

  projectionKinds(tokens) {
    const columns = selectProjection(tokens);
    if (!columns) return undefined;
    const kinds = [];
    for (const column of columns) {
      if (column.star) {
        for (const table of column.tables) {
          const catalog = this.tryCatalog(table);
          if (!catalog) return undefined;
          for (const name of catalog.order) kinds.push(catalog.columns.get(name));
        }
        continue;
      }
      if (!column.source) { kinds.push(undefined); continue; }
      const [table, name, candidates] = column.source;
      if (table) { kinds.push(this.tryCatalog(table)?.columns.get(name)); continue; }
      const owners = (candidates ?? []).map((t) => this.tryCatalog(t)).filter((c) => c?.columns.has(name));
      kinds.push(owners.length === 1 ? owners[0].columns.get(name) : undefined);
    }
    return kinds;
  }

  tryCatalog(table) { try { return this.catalog(table); } catch { return undefined; } }

  /** host_calls.rs catalog_table: pragma_table_info plus logical record schemas. */
  catalog(table) {
    const cached = this.tables.get(table);
    if (cached) return cached;
    this.requireOpen();
    const info = this.exec("SELECT name, type, pk, dflt_value IS NOT NULL AS has_default FROM pragma_table_info(?1, 'main') ORDER BY cid", [table], true);
    if (!info.rows.length) throw new Error(`no such table: ${table}`);
    const order = info.rows.map((r) => r[0]);
    let record;
    const present = this.exec("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_record_views')", [], true).rows[0][0] === 1;
    if (present) {
      const found = this.exec("SELECT schema FROM _gonvex_record_views WHERE name=?1 COLLATE NOCASE", [table], true).rows[0];
      if (found) record = JSON.parse(found[0]);
    }
    let catalog;
    if (record) {
      const columns = new Map(), defaults = new Map();
      for (const [name, definition] of Object.entries(record.columns)) {
        columns.set(name, kindOf(definition.type));
        if (typeof definition.default === "string") defaults.set(name, definition.default);
      }
      const keyDefinition = record.columns[record.key];
      catalog = { key: { column: record.key, dataType: keyDefinition?.type, databaseGenerated: defaults.has(record.key) }, columns, order, defaults, recordView: true };
    } else {
      const columns = new Map();
      const keys = [];
      for (const [name, declared, pk, hasDefault] of info.rows) {
        columns.set(name, kindOf(declared));
        if (pk > 0) keys.push([pk, name, declared, hasDefault !== 0]);
      }
      keys.sort((a, b) => a[0] - b[0]);
      if (keys.length > 1) throw new Error(`table ${JSON.stringify(table)} has a composite primary key, which db.update/delete do not support`);
      const key = keys.length === 0
        ? { column: DEFAULT_KEY, dataType: undefined, databaseGenerated: true }
        : { column: keys[0][1], dataType: keys[0][2], databaseGenerated: keys[0][3] || keys[0][2].trim().toUpperCase() === "INTEGER" };
      catalog = { key, columns, order, defaults: new Map(), recordView: false };
    }
    this.tables.set(table, catalog);
    return catalog;
  }

  textKey(key) { return key.dataType !== undefined && key.dataType.trim() !== "" && kindOf(key.dataType) === "text"; }

  returningRow(sql, binds, catalog) {
    const result = this.exec(sql, binds);
    if (!result.rows.length) return null;
    const row = result.rows[0];
    return Object.fromEntries(result.columnNames.map((name, index) => [name, decodeCell(catalog.columns.get(name) ?? "text", row[index])]));
  }

  returningRecordRow(table, sql, binds, catalog) {
    const row = this.returningRow(sql, binds, catalog);
    if (!catalog.recordView || row === null) return row;
    const key = row[catalog.key.column];
    if (key === undefined) throw new Error("record write did not return its key");
    return this.returningRow(`SELECT * FROM ${quote(table)} WHERE ${quote(catalog.key.column)}=?1`, [scalar(key)], catalog);
  }

  insert(table, row, generatedId) {
    this.requireOpen();
    if (row !== null && (typeof row !== "object" || Array.isArray(row))) throw new Error("row must be an object");
    const values = new Map(Object.entries(row ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
    const catalog = this.catalog(table);
    const key = catalog.key;
    if (!values.has(key.column) && generatedId && this.textKey(key)) values.set(key.column, generatedId);
    if (!values.has(key.column) && !key.databaseGenerated && key.dataType !== undefined) {
      if (this.textKey(key)) values.set(key.column, installed.crypto.randomUUID());
      else throw new Error(`insert into ${JSON.stringify(table)} requires primary key ${JSON.stringify(key.column)}; Gonvex can only allocate omitted text keys, but this column is ${key.dataType}`);
    }
    // BTreeMap order, as in Rust.
    const ordered = new Map([...values].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
    const columns = [...ordered.keys()].map(quote);
    const placeholders = [...ordered.keys()].map((_, index) => `?${index + 1}`);
    if (catalog.recordView) for (const [column, expression] of catalog.defaults) if (!ordered.has(column)) { columns.push(quote(column)); placeholders.push(expression); }
    const binds = [...ordered].map(([column, value]) => columnParameter(catalog.columns.get(column), value));
    this.stats.statementsLog.push(`[insert ${table}]`);
    return this.returningRecordRow(table, `INSERT INTO ${quote(table)} (${columns.join(", ")}) VALUES (${placeholders.join(", ")}) RETURNING *`, binds, catalog);
  }

  update(table, id, patch) {
    this.requireOpen();
    const catalog = this.catalog(table);
    if (typeof id !== "string" && typeof id !== "number") throw new Error("a row id must be a string or number");
    const values = [...Object.entries(patch ?? {})].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    if (!values.length) throw new Error("an update requires at least one column");
    const assignments = values.map(([column], index) => `${quote(column)} = ?${index + 1}`);
    const binds = values.map(([column, value]) => columnParameter(catalog.columns.get(column), value));
    binds.push(scalar(id));
    this.stats.statementsLog.push(`[update ${table}]`);
    return this.returningRecordRow(table, `UPDATE ${quote(table)} SET ${assignments.join(", ")} WHERE ${quote(catalog.key.column)} = ?${values.length + 1} RETURNING *`, binds, catalog);
  }

  delete(table, id) {
    this.requireOpen();
    const catalog = this.catalog(table);
    if (typeof id !== "string" && typeof id !== "number") throw new Error("a row id must be a string or number");
    this.stats.statementsLog.push(`[delete ${table}]`);
    const result = this.exec(`DELETE FROM ${quote(table)} WHERE ${quote(catalog.key.column)} = ?1 RETURNING 1`, [scalar(id)]);
    return { deleted: result.rows.length };
  }

  deleteMany(table, ids) {
    this.requireOpen();
    if (!Array.isArray(ids)) throw new Error("deleteMany ids must be an array");
    if (ids.length > 10_000) throw new Error("deleteMany cannot delete more than 10000 rows per call");
    if (!ids.length) return { deleted: 0 };
    for (const id of ids) if (typeof id !== "string" && typeof id !== "number") throw new Error("a row id must be a string or number");
    const catalog = this.catalog(table);
    this.stats.statementsLog.push(`[deleteMany ${table}]`);
    const result = this.exec(`DELETE FROM ${quote(table)} WHERE ${quote(catalog.key.column)} IN (SELECT value FROM json_each(?1)) RETURNING 1`, [JSON.stringify(ids)]);
    return { deleted: result.rows.length };
  }

  async deferredId(kind) {
    const { identity, invocation } = this.options;
    if (!invocation?.commandId) return installed.crypto.randomUUID();
    const ordinal = this.ordinals.get(kind) ?? 0;
    this.ordinals.set(kind, ordinal + 1);
    return intentDeferredId(installed.crypto.subtle, identity?.tenant?.id ?? "", identity?.account?.id ?? "", invocation.commandId, kind, ordinal);
  }

  provenance() {
    const i = this.options.invocation ?? {};
    return { commandId: i.commandId ?? null, rootCommandId: i.rootCommandId ?? null, rootChannel: i.rootChannel ?? null, channel: i.channel ?? null, actorAccountId: i.actorAccountId ?? null, actorMemberId: i.actorMemberId ?? null, onBehalfOfMemberId: i.onBehalfOfMemberId ?? null, agentExecutionId: null };
  }

  async enqueueAction(path, args) {
    this.requireWrite();
    this.requireOpen();
    const name = String(path ?? "").trim();
    if (!name) throw new Error("actions.enqueue requires an Action path");
    if (name === SCHEDULE_OUTBOX_PATH) throw new Error("actions.enqueue cannot invoke a Gonvex system operation");
    const id = await this.deferredId("action");
    this.writeOutbox(id, name, args);
    return id;
  }

  async enqueueScheduled(path, args, atUnixMs, coalesce) {
    this.requireOpen();
    const name = String(path ?? "").trim();
    if (!this.options.schedulable?.(name)) throw new Error(`scheduled function ${JSON.stringify(name)} is not a registered Reducer or Action`);
    const jobId = `job_${await this.deferredId("schedule")}`;
    // store::enqueue_action: the outbox row id itself is a random UUID.
    this.writeOutbox(installed.crypto.randomUUID(), SCHEDULE_OUTBOX_PATH, { jobId, function: name, args: args ?? null, runAtUnixMs: atUnixMs, coalesce });
    return jobId;
  }

  writeOutbox(id, path, args) {
    const account = this.options.identity?.account ?? {};
    this.stats.statementsLog.push(`[outbox ${path}]`);
    this.exec("INSERT INTO _gonvex_action_outbox (id, action_path, args, actor_user_id, actor_email, provenance) VALUES (?1, ?2, ?3, NULLIF(?4, ''), NULLIF(?5, ''), ?6)",
      [id, path, JSON.stringify(args ?? null), account.id ?? "", account.email ?? "", JSON.stringify(this.provenance())]);
  }
}
