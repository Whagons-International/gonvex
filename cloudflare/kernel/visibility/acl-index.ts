// Tenant-owned indexes for mask grants and inherited parent ACL values
// (visibility_index.rs). They apply feed rows without database access.

import { compareText, jsonEqual, type JsonValue } from "./json.ts";

/**
 * Grant contributions per grant row, with reference counts: several rows may
 * grant the same token, and revoking one keeps the others.
 */
export class GrantIndex {
  private readonly rows = new Map<string, Set<string>>();
  private readonly counts = new Map<string, Map<string, number>>();

  /**
   * Replaces a row's complete contributions, as `member\u0000token` pairs (see
   * `grantPair`). Returns the members whose effective token set changed.
   */
  replace(rowId: string, contributions: ReadonlySet<string>): Set<string> {
    const previous = this.rows.get(rowId) ?? new Set<string>();
    this.rows.delete(rowId);
    const affected = new Set<string>();
    for (const pair of previous) {
      if (contributions.has(pair)) continue;
      const [member, token] = splitPair(pair);
      const tokens = this.counts.get(member);
      if (!tokens) continue;
      const count = tokens.get(token);
      if (count !== undefined) {
        if (count - 1 === 0) {
          tokens.delete(token);
          affected.add(member);
        } else tokens.set(token, count - 1);
      }
      if (tokens.size === 0) this.counts.delete(member);
    }
    for (const pair of contributions) {
      if (previous.has(pair)) continue;
      const [member, token] = splitPair(pair);
      let tokens = this.counts.get(member);
      if (!tokens) this.counts.set(member, (tokens = new Map()));
      const count = tokens.get(token) ?? 0;
      if (count === 0) affected.add(member);
      tokens.set(token, count + 1);
    }
    if (contributions.size) this.rows.set(rowId, new Set(contributions));
    return affected;
  }

  contains(member: string, token: string): boolean {
    return this.counts.get(member)?.has(token) ?? false;
  }
}

export function grantPair(member: string, token: string): string {
  return `${member}\u0000${token}`;
}

function splitPair(pair: string): [string, string] {
  const at = pair.indexOf("\u0000");
  return [pair.slice(0, at), pair.slice(at + 1)];
}

/** The index's published progress. */
export interface IndexSignal {
  readonly revision: number;
  readonly poisoned: boolean;
}

let nextIndexId = 1;

/**
 * Owned once per tenant, table and key column. A key may name several rows
 * (association rows by task), so rows are grouped by key and remembered by
 * their own row id for removal.
 *
 * A maintained index is loaded at a known revision of the tenant's change
 * log and then changed only by its applier, in revision order: its revision
 * is a completeness certificate. A poisoned index certifies nothing; every
 * holder rebuilds through the registry.
 */
export class ParentAclIndex {
  /** Identity for class keys (Rust: the Arc's address). */
  readonly id = nextIndexId++;
  private revisionValue: number;
  private modifiedRevisionValue: number;
  private readonly rows = new Map<string, Map<string, JsonValue>>();
  private readonly keys = new Map<string, string>();
  private epochValue = "";
  private maintainedValue = false;
  private poisonedValue = false;
  private readonly waiters = new Set<(signal: IndexSignal) => void>();

  private constructor(revision: number) {
    this.revisionValue = revision;
    this.modifiedRevisionValue = revision;
  }

  /** A unique-key index whose key is the row id. */
  static fromSnapshot(revision: number, rows: Iterable<[string, JsonValue]>): ParentAclIndex {
    return ParentAclIndex.fromRows(revision, [...rows].map(([key, row]) => [key, key, row] as [string, string, JsonValue]));
  }

  /** Rows as (row id, key, projected row). */
  static fromRows(revision: number, rows: Iterable<readonly [string, string, JsonValue]>): ParentAclIndex {
    const index = new ParentAclIndex(revision);
    for (const [id, key, row] of rows) index.insert(id, key, row);
    return index;
  }

  /** An index owned by one feed applier, loaded at `revision` of `epoch`. */
  static maintained(epoch: string, revision: number, rows: Iterable<readonly [string, string, JsonValue]>): ParentAclIndex {
    const index = ParentAclIndex.fromRows(revision, rows);
    index.epochValue = epoch;
    index.maintainedValue = true;
    return index;
  }

  get revision(): number {
    return this.revisionValue;
  }

  /** Last change to projected ACL rows, independent of unrelated commits. */
  get modifiedRevision(): number {
    return this.modifiedRevisionValue;
  }

  get epoch(): string {
    return this.epochValue;
  }

  get maintained(): boolean {
    return this.maintainedValue;
  }

  get poisoned(): boolean {
    return this.poisonedValue;
  }

  /** Rows held, for metrics. */
  get size(): number {
    return this.keys.size;
  }

  /** Permanently invalidates the index and releases its rows. */
  poison(): void {
    this.poisonedValue = true;
    this.rows.clear();
    this.keys.clear();
    this.publish();
  }

  private publish(): void {
    const signal = { revision: this.revisionValue, poisoned: this.poisonedValue };
    for (const waiter of [...this.waiters]) waiter(signal);
  }

  /** Resolves when the index reached `revision` (true) or was poisoned (false). */
  waitFor(revision: number): Promise<boolean> {
    if (this.poisonedValue) return Promise.resolve(false);
    if (this.revisionValue >= revision) return Promise.resolve(true);
    return new Promise((resolve) => {
      const waiter = (signal: IndexSignal) => {
        if (!signal.poisoned && signal.revision < revision) return;
        this.waiters.delete(waiter);
        resolve(!signal.poisoned);
      };
      this.waiters.add(waiter);
    });
  }

  private insert(id: string, key: string, row: JsonValue): void {
    this.remove(id);
    let bucket = this.rows.get(key);
    if (!bucket) this.rows.set(key, (bucket = new Map()));
    bucket.set(id, row);
    this.keys.set(id, key);
  }

  private remove(id: string): void {
    const key = this.keys.get(id);
    if (key === undefined) return;
    this.keys.delete(id);
    const bucket = this.rows.get(key);
    if (bucket) {
      bucket.delete(id);
      if (bucket.size === 0) this.rows.delete(key);
    }
  }

  /** Applies one complete transaction to a unique-key index: (key, row or undefined for a delete). */
  apply(revision: number, changes: Iterable<readonly [string, JsonValue | undefined]>): boolean {
    return this.applyRows(
      revision,
      [...changes].map(([key, row]) => [key, row === undefined ? undefined : ([key, row] as const)] as const),
    );
  }

  /** Applies one complete transaction as (row id, new (key, row) or undefined). */
  applyRows(revision: number, changes: Iterable<readonly [string, (readonly [string, JsonValue]) | undefined]>): boolean {
    if (this.poisonedValue || revision <= this.revisionValue) return false;
    for (const [id, row] of changes) {
      if (row) {
        const [key, value] = row;
        const current = this.keys.get(id) === key ? this.rows.get(key)?.get(id) : undefined;
        if (current === undefined || !jsonEqual(current, value)) {
          this.insert(id, key, value);
          this.modifiedRevisionValue = revision;
        }
      } else if (this.keys.has(id)) {
        this.remove(id);
        this.modifiedRevisionValue = revision;
      }
    }
    this.revisionValue = revision;
    this.publish();
    return true;
  }

  /** One row for a unique key (a parent), or undefined when it does not exist. */
  get(key: string): JsonValue | undefined {
    const bucket = this.rows.get(key);
    if (!bucket) return undefined;
    if (bucket.size === 1) return bucket.values().next().value;
    // BTreeMap order: the smallest row id.
    let first: string | undefined;
    for (const id of bucket.keys()) if (first === undefined || compareText(id, first) < 0) first = id;
    return bucket.get(first!);
  }

  /** Every row with this key. */
  getAll(key: string): JsonValue[] {
    const bucket = this.rows.get(key);
    return bucket ? [...bucket.values()] : [];
  }
}

/**
 * `ready_at` without waiting: true ready, false never ready at this
 * revision, undefined when it would wait for the applier.
 */
export function readyNow(index: ParentAclIndex, revision: number, epoch: string): boolean | undefined {
  if (index.poisoned || (index.epoch !== "" && index.epoch !== epoch)) return false;
  if (index.revision >= revision) return true;
  if (!index.maintained) return false;
  return undefined;
}

/** Whether `index` reflects every committed transaction through `revision` of `epoch`, waiting for its applier. */
export async function readyAt(index: ParentAclIndex, revision: number, epoch: string): Promise<boolean> {
  const now = readyNow(index, revision, epoch);
  if (now !== undefined) return now;
  return index.waitFor(revision);
}

/** The revision `index` is complete through, or undefined when poisoned or at another epoch. */
export function usableRevision(index: ParentAclIndex, epoch: string): number | undefined {
  return !index.poisoned && (index.epoch === "" || index.epoch === epoch) ? index.revision : undefined;
}

/** Whether `index` is complete through exactly `revision` of `epoch`. */
export function exactlyAt(index: ParentAclIndex, revision: number, epoch: string): boolean {
  return !index.poisoned && (index.epoch === "" || index.epoch === epoch) && index.revision === revision;
}
