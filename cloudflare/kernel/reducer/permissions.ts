// Per-tenant permission bits (rust/crates/runtime/src/permission_bits.rs):
// one bit per (permission, scope), one bitset per member, a reference count
// per bit so duplicate grant rows stay correct. `ctx.permissions.has` is a
// bit test (CONTRACT B.2a `permissionsHas`, adapt: in-memory index in the
// tenant object).
//
// Commit order, as in Rust: the index holds **committed** state only. Rust
// loads it in a read transaction and then applies committed feed transactions
// in revision order; a Reducer waits until the bits cover the revision its own
// transaction sees. Here the object is single-threaded and the Reducer path
// is serialized, so `ensure()` runs before the Reducer's storage transaction
// opens: it reads the clock and applies the grant table's change rows since
// the index's revision (or reloads when they were pruned or the epoch
// changed). The Reducer then answers from exactly the state its transaction
// started from, never from its own uncommitted grant rows.

import type { CaptureSql } from "../capture/sql.ts";
import { integerLiteral, quoteIdentifier, quoteLiteral } from "../capture/sql.ts";

/** The manifest's `permissionGrants` declaration (`PermissionGrantSource`). */
export interface PermissionGrantSource {
  readonly table: string;
  readonly member: string;
  readonly permission: string;
  readonly scope: string;
  readonly whereNull?: readonly string[];
  /** Row id column of the table in the change feed (default `_id`). */
  readonly key?: string;
}

export const NO_PROJECTION = "the module declares no permissionGrants projection";
const LOAD_PAGE_ROWS = 2000;

/** visibility.rs `identifier_ok`: `^[A-Za-z_][A-Za-z0-9_]*$`, at most 63 bytes. */
function identifierOk(name: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) || name.length > 63) throw new Error(`SQL identifier ${JSON.stringify(name)} is invalid`);
}

export function validateGrantSource(source: PermissionGrantSource): void {
  for (const column of [source.table, source.member, source.permission, source.scope, ...(source.whereNull ?? []), ...(source.key ? [source.key] : [])]) identifierOk(column);
}

/** Reads `permissionGrants` from a manifest value; undefined when absent or malformed. */
export function grantSourceOf(value: unknown): PermissionGrantSource | undefined {
  if (!value || typeof value !== "object") return undefined;
  const v = value as Record<string, unknown>;
  const text = (key: string) => (typeof v[key] === "string" ? (v[key] as string) : undefined);
  const table = text("table"), member = text("member"), permission = text("permission"), scope = text("scope");
  if (!table || !member || !permission || !scope) return undefined;
  const whereNull = Array.isArray(v.whereNull) ? v.whereNull.filter((c): c is string => typeof c === "string") : [];
  const key = text("key");
  return { table, member, permission, scope, whereNull, ...(key ? { key } : {}) };
}

const keyColumn = (source: PermissionGrantSource) => (source.key ? source.key : "_id");
const positionKey = (permission: string, scope: string) => `${permission}\u0000${scope}`;

/** `(member, permission, scope)` a projected row grants (`PermissionGrantSource::grant`). */
function grantOf(source: PermissionGrantSource, row: unknown): [string, string, string] | undefined {
  if (!row || typeof row !== "object" || Array.isArray(row)) return undefined;
  const r = row as Record<string, unknown>;
  if ((source.whereNull ?? []).some((column) => r[column] !== undefined && r[column] !== null)) return undefined;
  const text = (column: string): string | undefined => {
    const value = r[column];
    if (value === undefined || value === null) return undefined;
    // serde_json Value::to_string for non-strings (numbers, booleans, objects).
    return typeof value === "string" ? value : JSON.stringify(value);
  };
  const member = text(source.member), permission = text(source.permission);
  if (member === undefined || permission === undefined) return undefined;
  return [member, permission, text(source.scope) ?? ""];
}

class MemberBits {
  words: number[] = [];
  readonly counts = new Map<number, number>();
  add(bit: number): void {
    const count = (this.counts.get(bit) ?? 0) + 1;
    this.counts.set(bit, count);
    if (count === 1) {
      const word = bit >>> 5;
      while (this.words.length <= word) this.words.push(0);
      this.words[word] = (this.words[word]! | (1 << (bit & 31))) >>> 0;
    }
  }
  remove(bit: number): void {
    const count = this.counts.get(bit);
    if (count === undefined) return;
    if (count > 1) { this.counts.set(bit, count - 1); return; }
    this.counts.delete(bit);
    const word = bit >>> 5;
    this.words[word] = (this.words[word]! & ~(1 << (bit & 31))) >>> 0;
  }
  test(bit: number): boolean {
    const word = this.words[bit >>> 5];
    return word !== undefined && (word & (1 << (bit & 31))) !== 0;
  }
}

export interface PermissionStats {
  loads: number;
  applied: number;
  rowsRead: number;
}

/**
 * The permission index of one tenant object. Implements K04's
 * `PermissionIndex` (`has` → boolean, or `"rebuilding"`).
 */
export class PermissionBits {
  private epoch = "";
  private revision = -1;
  private poisoned = true;
  private readonly positions = new Map<string, number>();
  private readonly members = new Map<string, MemberBits>();
  /** Row id → (member, bit) it grants, to undo updates and deletes. */
  private readonly rows = new Map<string, [string, number]>();
  readonly stats: PermissionStats = { loads: 0, applied: 0, rowsRead: 0 };

  constructor(private source: PermissionGrantSource | undefined) {}

  /** A new generation may declare a different projection: reload on the next `ensure`. */
  setSource(source: PermissionGrantSource | undefined): void {
    const same = JSON.stringify(source ?? null) === JSON.stringify(this.source ?? null);
    this.source = source;
    if (!same) this.poisoned = true;
  }

  get declared(): boolean { return this.source !== undefined; }
  get appliedRevision(): number { return this.revision; }

  /** Brings the index to the committed clock. Call outside any Reducer transaction. */
  ensure(sql: CaptureSql): void {
    const source = this.source;
    if (!source) return;
    const clock = sql.run("SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = 1").rows[0];
    if (!clock) { this.poisoned = true; return; }
    const epoch = String(clock[0]);
    const revision = Math.max(0, Number(clock[1]));
    const retained = Number(clock[2] ?? 0);
    if (this.poisoned || epoch !== this.epoch || retained > this.revision) {
      this.load(sql, source, epoch, revision);
      return;
    }
    if (revision <= this.revision) return;
    // Committed transactions since the index's revision, in order (permission_bits.rs `apply`).
    const changes = sql.run(
      `SELECT revision, row_id, new_value FROM _gonvex_sync_changes INDEXED BY gonvex_sync_changes_table
       WHERE table_name = ?1 AND revision > ${integerLiteral(this.revision)} AND revision <= ${integerLiteral(revision)}
       ORDER BY revision, ordinal`,
      [source.table],
    );
    this.stats.rowsRead += changes.rowsRead;
    for (const [, rowId, newValue] of changes.rows) {
      const id = String(rowId);
      this.removeRow(id);
      if (newValue !== null && newValue !== undefined) this.insertRow(source, id, JSON.parse(String(newValue)) as unknown);
    }
    this.revision = revision;
    this.stats.applied += 1;
  }

  /** `ctx.permissions.has` (exact scope). */
  has(member: string, permission: string, scope: string): boolean | "rebuilding" {
    if (!this.source) throw new Error(NO_PROJECTION);
    if (this.poisoned) return "rebuilding";
    const bit = this.positions.get(positionKey(permission, scope));
    if (bit === undefined) return false;
    return this.members.get(member)?.test(bit) ?? false;
  }

  /** Distinct (permission, scope) bits and members with any grant. */
  size(): [number, number] { return [this.positions.size, this.members.size]; }

  private load(sql: CaptureSql, source: PermissionGrantSource, epoch: string, revision: number): void {
    validateGrantSource(source);
    this.positions.clear();
    this.members.clear();
    this.rows.clear();
    const columns = [source.member, source.permission, source.scope, ...(source.whereNull ?? [])];
    const projection = columns.map((column) => `${quoteLiteral(column)}, r.${quoteIdentifier(column)}`).join(", ");
    // Paged by rowid (design rule: every loop over rows works in pages).
    let after = Number.MIN_SAFE_INTEGER;
    for (;;) {
      const page = sql.run(
        `SELECT r.rowid, CAST(r.${quoteIdentifier(keyColumn(source))} AS TEXT), json_object(${projection}) FROM ${quoteIdentifier(source.table)} AS r
         WHERE r.rowid > ${integerLiteral(after)} ORDER BY r.rowid LIMIT ${LOAD_PAGE_ROWS}`,
      );
      this.stats.rowsRead += page.rowsRead;
      for (const [rowid, id, value] of page.rows) {
        after = Number(rowid);
        if (id !== null && id !== undefined) this.insertRow(source, String(id), JSON.parse(String(value)) as unknown);
      }
      if (page.rows.length < LOAD_PAGE_ROWS) break;
    }
    this.epoch = epoch;
    this.revision = revision;
    this.poisoned = false;
    this.stats.loads += 1;
  }

  private insertRow(source: PermissionGrantSource, id: string, row: unknown): void {
    const grant = grantOf(source, row);
    if (!grant) return;
    const [member, permission, scope] = grant;
    const key = positionKey(permission, scope);
    let bit = this.positions.get(key);
    if (bit === undefined) { bit = this.positions.size; this.positions.set(key, bit); }
    let bits = this.members.get(member);
    if (!bits) { bits = new MemberBits(); this.members.set(member, bits); }
    bits.add(bit);
    this.rows.set(id, [member, bit]);
  }

  private removeRow(id: string): void {
    const entry = this.rows.get(id);
    if (!entry) return;
    this.rows.delete(id);
    this.members.get(entry[0])?.remove(entry[1]);
  }
}
