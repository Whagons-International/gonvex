// Per-tenant permission bitsets: one bit per (permission, scope)
// (permission_bits.rs). A module may declare a grant projection in its
// manifest:
//
//   "permissionGrants": {"table": "memberPermissionGrants", "member": "memberId",
//     "permission": "permission", "scope": "scope", "whereNull": ["deletedAt"]}
//
// Each row grants `member` the permission in `scope` (an empty or null scope
// is the tenant-wide scope ""). Rows with a non-null `whereNull` column grant
// nothing. Every distinct (permission, scope) gets a bit; a member's grants
// are one bitset with a reference count per bit, so duplicate rows stay
// correct. Reads are bit tests: `ctx.permissions.has` (K04's `PermissionIndex`).
//
// Commit order: the bits change only from committed transactions, in
// revision order, so a rolled-back Reducer never flips a bit. The tenant
// object brings the bits to its snapshot's revision (`sync`) before a Reducer
// reads them; a Reducer reads the committed state, not its own uncommitted
// grant rows, as in Rust.

import { readChanges, readClock, type FeedTransaction, type LogChange } from "./feed.ts";
import { get, isObject, serdeJson, type JsonValue } from "./json.ts";
import { identifier, quote } from "./plan.ts";
import type { VisibilitySql } from "./principal.ts";

/** The manifest declaration (`PermissionGrantSource`, serde `deny_unknown_fields`). */
export interface PermissionGrantSource {
  readonly table: string;
  readonly member: string;
  readonly permission: string;
  readonly scope: string;
  /** Columns that must be null for a row to grant (e.g. `deletedAt`). */
  readonly whereNull: readonly string[];
  /** Row id column of the table in the change feed (default `_id`). */
  readonly key: string;
}

const FIELDS = new Set(["table", "member", "permission", "scope", "whereNull", "key"]);

/** Reads the manifest's `permissionGrants` as serde does. */
export function parsePermissionGrants(value: unknown): PermissionGrantSource {
  if (!isObject(value as JsonValue)) throw new Error("invalid type: expected struct PermissionGrantSource");
  const object = value as Record<string, unknown>;
  for (const key of Object.keys(object)) {
    if (!FIELDS.has(key)) throw new Error(`unknown field \`${key}\`, expected one of \`table\`, \`member\`, \`permission\`, \`scope\`, \`whereNull\`, \`key\``);
  }
  const text = (key: string, required: boolean): string => {
    const found = object[key];
    if (found === undefined) {
      if (required) throw new Error(`missing field \`${key}\``);
      return "";
    }
    if (typeof found !== "string") throw new Error(`invalid type for \`${key}\`: expected a string`);
    return found;
  };
  const whereNull = object["whereNull"] ?? [];
  if (!Array.isArray(whereNull) || whereNull.some((item) => typeof item !== "string")) throw new Error("invalid type for `whereNull`: expected a sequence of strings");
  return { table: text("table", true), member: text("member", true), permission: text("permission", true), scope: text("scope", true), whereNull: whereNull as string[], key: text("key", false) };
}

/** `PermissionGrantSource::validate`: every name is a plain SQL identifier. */
export function validatePermissionGrants(source: PermissionGrantSource): void {
  for (const column of [source.table, source.member, source.permission, source.scope, ...source.whereNull, ...(source.key ? [source.key] : [])]) identifier(column);
}

function keyColumn(source: PermissionGrantSource): string {
  return source.key || "_id";
}

/** (member, permission, scope) a projected row grants, if any. */
function grant(source: PermissionGrantSource, row: JsonValue): [string, string, string] | undefined {
  if (!isObject(row)) return undefined;
  if (source.whereNull.some((column) => {
    const value = get(row, column);
    return value !== undefined && value !== null;
  })) return undefined;
  const text = (column: string): string | undefined => {
    const value = get(row, column);
    if (value === undefined || value === null) return undefined;
    return typeof value === "string" ? value : serdeJson(value);
  };
  const member = text(source.member);
  const permission = text(source.permission);
  if (member === undefined || permission === undefined) return undefined;
  return [member, permission, text(source.scope) ?? ""];
}

/** One member's bitset plus per-bit reference counts. */
class MemberBits {
  readonly words: number[] = [];
  readonly counts = new Map<number, number>();

  add(bit: number): void {
    const count = (this.counts.get(bit) ?? 0) + 1;
    this.counts.set(bit, count);
    if (count === 1) {
      const word = bit >>> 5;
      while (this.words.length <= word) this.words.push(0);
      this.words[word]! |= 1 << (bit & 31);
    }
  }

  remove(bit: number): void {
    const count = this.counts.get(bit);
    if (count === undefined) return;
    if (count > 1) {
      this.counts.set(bit, count - 1);
      return;
    }
    this.counts.delete(bit);
    this.words[bit >>> 5]! &= ~(1 << (bit & 31));
  }

  test(bit: number): boolean {
    const word = this.words[bit >>> 5];
    return word !== undefined && (word & (1 << (bit & 31))) !== 0;
  }
}

function positionKey(permission: string, scope: string): string {
  return `${permission}\u0000${scope}`;
}

/** The permission bitsets of one tenant. */
export class PermissionIndex {
  private poisonedValue = false;
  private readonly positions = new Map<string, number>();
  private readonly members = new Map<string, MemberBits>();
  /** Row id → (member, bit) it grants, to undo updates and deletes. */
  private readonly rows = new Map<string, readonly [string, number]>();

  constructor(
    readonly epoch: string,
    private revisionValue: number,
  ) {}

  /** Builds from loaded (row id, row) pairs. */
  static fromRows(source: PermissionGrantSource, epoch: string, revision: number, rows: Iterable<readonly [string, JsonValue]>): PermissionIndex {
    const index = new PermissionIndex(epoch, revision);
    for (const [id, row] of rows) index.insertRow(source, id, row);
    return index;
  }

  get revision(): number {
    return this.revisionValue;
  }

  get poisoned(): boolean {
    return this.poisonedValue;
  }

  poison(): void {
    this.poisonedValue = true;
  }

  private insertRow(source: PermissionGrantSource, id: string, row: JsonValue): void {
    const granted = grant(source, row);
    if (!granted) return;
    const [member, permission, scope] = granted;
    const key = positionKey(permission, scope);
    let bit = this.positions.get(key);
    if (bit === undefined) this.positions.set(key, (bit = this.positions.size));
    let bits = this.members.get(member);
    if (!bits) this.members.set(member, (bits = new MemberBits()));
    bits.add(bit);
    this.rows.set(id, [member, bit]);
  }

  private removeRow(id: string): void {
    const found = this.rows.get(id);
    if (!found) return;
    this.rows.delete(id);
    this.members.get(found[0])?.remove(found[1]);
  }

  /** Applies one committed transaction's rows of the grant table. Revisions at or below the loaded one are already in it. */
  apply(source: PermissionGrantSource, revision: number, changes: readonly LogChange[]): void {
    if (revision <= this.revisionValue) return;
    for (const change of changes) {
      if (change.table !== source.table) continue;
      this.removeRow(change.rowId);
      if (change.newValue !== null) this.insertRow(source, change.rowId, change.newValue);
    }
    this.revisionValue = revision;
  }

  /** Advances the revision past transactions that did not touch the grant table. */
  advance(revision: number): void {
    if (revision > this.revisionValue) this.revisionValue = revision;
  }

  /** The bit position of (permission, scope), if any member ever held it. */
  position(permission: string, scope: string): number | undefined {
    return this.positions.get(positionKey(permission, scope));
  }

  /** Does `member` hold `permission` in `scope` (exact scope)? */
  has(member: string, permission: string, scope: string): boolean {
    const bit = this.position(permission, scope);
    return bit !== undefined && (this.members.get(member)?.test(bit) ?? false);
  }

  hasPosition(member: string, position: number): boolean {
    return this.members.get(member)?.test(position) ?? false;
  }

  /** The member's permission state as 32-bit words. */
  bits(member: string): readonly number[] {
    return this.members.get(member)?.words ?? [];
  }

  /** Distinct (permission, scope) bits, and members with any grant. */
  size(): [number, number] {
    return [this.positions.size, this.members.size];
  }
}

/** Loads the projection in one read: the clock and every row. */
export function loadPermissionIndex(sql: VisibilitySql, source: PermissionGrantSource): PermissionIndex {
  validatePermissionGrants(source);
  const clock = readClock(sql);
  const columns = [source.member, source.permission, source.scope, ...source.whereNull];
  const projection = columns.map((column) => `'${column}', r.${quote(column)}`).join(", ");
  const rows: Array<[string, JsonValue]> = [];
  for (const row of sql.run(`SELECT CAST(r.${quote(keyColumn(source))} AS TEXT), json_object(${projection}) FROM ${quote(source.table)} AS r`).rows) {
    if (row[0] === null || row[0] === undefined) continue;
    rows.push([String(row[0]), JSON.parse(String(row[1])) as JsonValue]);
  }
  return PermissionIndex.fromRows(source, clock.epoch, clock.revision, rows);
}

/**
 * The tenant object's permission bits (K04 `PermissionIndex` port): loaded
 * on first `sync`, then kept at the snapshot's revision from pushed
 * transactions or the change log. `has` answers "rebuilding" until synced.
 */
export class PermissionBits {
  private index: PermissionIndex | undefined;
  readonly counters = { loads: 0, applied: 0 };

  constructor(readonly source: PermissionGrantSource) {
    validatePermissionGrants(source);
  }

  /** Brings the bits to the clock of the snapshot `sql` reads (a Reducer's transaction, before its first `has`). */
  sync(sql: VisibilitySql): PermissionIndex {
    const clock = readClock(sql);
    const index = this.index;
    if (index && !index.poisoned && index.epoch === clock.epoch && index.revision >= clock.retained) {
      for (const transaction of readChanges(sql, clock.epoch, [this.source.table], index.revision, clock.revision)) {
        index.apply(this.source, transaction.revision, transaction.changes);
        this.counters.applied += 1;
      }
      index.advance(clock.revision);
      return index;
    }
    index?.poison();
    this.index = loadPermissionIndex(sql, this.source);
    this.counters.loads += 1;
    return this.index;
  }

  /** The push path: one committed transaction, in revision order. */
  apply(transaction: FeedTransaction): void {
    const index = this.index;
    if (!index || index.poisoned || transaction.revision <= index.revision) return;
    if (index.epoch !== transaction.epoch) {
      index.poison();
      return;
    }
    index.apply(this.source, transaction.revision, transaction.changes);
    this.counters.applied += 1;
  }

  poison(): void {
    this.index?.poison();
  }

  current(): PermissionIndex | undefined {
    return this.index && !this.index.poisoned ? this.index : undefined;
  }

  has(memberId: string, permission: string, scope: string): boolean | "rebuilding" {
    const index = this.current();
    return index ? index.has(memberId, permission, scope) : "rebuilding";
  }
}

