/**
 * X17d: the admitted rows of a cold replica snapshot, kept in isolate memory, so another cold open of the same
 * collection for the same member serves them without scanning again (on dev -12 a cold load's 67 opens spent ~545 ms
 * in the SQL scan and ~300 ms parsing, canonicalizing and hashing 15.5k rows).
 *
 * Exactness (subscription.ts decides; this module only stores):
 * - Only plans without `inherits`/`inheritsAny` are cached: a child's visibility then depends on parent rows, `via`
 *   rows and the parents' own principal tables, which the validity check below does not cover (review of #528).
 * - An entry holds the rows exactly as the scan admitted them (row text, key, canonical hash) at revision `revision`
 *   of epoch `epoch`, and the resolved visibility `fingerprint` (plan, direct context, role, permissions, principal
 *   sets) they were admitted under.
 * - It is served only when the open's own resolution has the same fingerprint, the epoch is the same, the history is
 *   retained past the entry, and the change log shows no transaction since then on the collection's table, the
 *   plan's dependency tables or `members`. A fresh scan then admits the same rows, so the pages, hashes and digest are
 *   the same bytes, at the current cursor. Frames are never cached (they carry the request id and cursor).
 *
 * Ownership and bounds: entries belong to one engine (its visibility registry: one tenant object's lifetime) and name
 * their module generation. The isolate keeps at most `SNAPSHOT_CACHE_MAX_BYTES` (estimated retained bytes: UTF-16 text
 * and key, the hash, and per-row object overhead), least recently used out first; a collected owner's entries leave
 * the budget when it is finalized.
 */
import type { ScannedRow } from "./scan.ts";
import type { ReplicaWindow } from "./window.ts";

export interface CachedSnapshot {
  readonly epoch: string;
  readonly revision: number;
  readonly fingerprint: string;
  /** #528 re-review 3: the tenant's schema generation when the entry was checked; any schema change since is a miss. */
  readonly schema: number;
  readonly rows: readonly ScannedRow[];
  readonly window: ReplicaWindow;
  readonly bytes: number;
}

export const SNAPSHOT_CACHE_MAX_BYTES = 24 * 1024 * 1024;
/** Estimated V8 overhead of one cached row (object, three string headers, array slot). */
export const ROW_OVERHEAD_BYTES = 160;

/** Estimated retained bytes of cached rows. */
export function retainedRowBytes(rows: readonly { key: string; text: string; hash: string }[]): number {
  let bytes = 0;
  for (const row of rows) bytes += 2 * (row.text.length + row.key.length + row.hash.length) + ROW_OVERHEAD_BYTES;
  return bytes;
}

export class SnapshotCache {
  private readonly byOwner = new WeakMap<object, { id: number; entries: Map<string, CachedSnapshot> }>();
  /** Entry maps by owner id (they never reference their owner, so an owner can still be collected). */
  private readonly maps = new Map<number, Map<string, CachedSnapshot>>();
  /** LRU order of `${ownerId}\u0001${key}` -> bytes. */
  private readonly order = new Map<string, number>();
  private readonly finalizer = new FinalizationRegistry<number>((id) => this.dropOwner(id));
  private total = 0;
  private next = 0;
  readonly stats = { hits: 0, misses: 0, stores: 0, evictions: 0 };

  constructor(private readonly maxBytes = SNAPSHOT_CACHE_MAX_BYTES) {}

  get(owner: object, key: string): CachedSnapshot | undefined {
    const record = this.byOwner.get(owner);
    const entry = record?.entries.get(key);
    if (entry) {
      const slot = `${record!.id}\u0001${key}`;
      const bytes = this.order.get(slot)!;
      this.order.delete(slot);
      this.order.set(slot, bytes);
    }
    return entry;
  }

  set(owner: object, key: string, entry: CachedSnapshot): void {
    if (entry.bytes > this.maxBytes / 4) return;
    let record = this.byOwner.get(owner);
    if (!record) {
      record = { id: ++this.next, entries: new Map() };
      this.byOwner.set(owner, record);
      this.maps.set(record.id, record.entries);
      this.finalizer.register(owner, record.id);
    }
    this.remove(record.id, record.entries, key);
    record.entries.set(key, entry);
    this.order.set(`${record.id}\u0001${key}`, entry.bytes);
    this.total += entry.bytes;
    this.stats.stores++;
    while (this.total > this.maxBytes && this.order.size) {
      const slot = this.order.keys().next().value!;
      const at = slot.indexOf("\u0001");
      const id = Number(slot.slice(0, at)), k = slot.slice(at + 1);
      this.remove(id, this.maps.get(id), k, slot);
      this.stats.evictions++;
    }
  }

  delete(owner: object, key: string): void {
    const record = this.byOwner.get(owner);
    if (record) this.remove(record.id, record.entries, key);
  }

  get bytes(): number { return this.total; }

  private remove(id: number, entries: Map<string, CachedSnapshot> | undefined, key: string, slot = `${id}\u0001${key}`): void {
    const bytes = this.order.get(slot);
    if (bytes === undefined) return;
    this.order.delete(slot);
    entries?.delete(key);
    this.total -= bytes;
  }

  /** A collected owner: its entries are unreachable, so their bytes leave the budget now. */
  private dropOwner(id: number): void {
    const prefix = `${id}\u0001`;
    for (const [slot, bytes] of [...this.order]) if (slot.startsWith(prefix)) { this.order.delete(slot); this.total -= bytes; }
    this.maps.delete(id);
  }
}

/** One per isolate: every tenant object of the isolate shares the byte budget. */
export const snapshotCache = new SnapshotCache();
