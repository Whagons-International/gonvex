/**
 * X17d: the admitted rows of a cold replica snapshot, kept in isolate memory, so another cold open of the same
 * collection for the same member serves them without scanning again (on dev -12 a cold load's 67 opens spent ~545 ms
 * in the SQL scan and ~300 ms parsing, canonicalizing and hashing 15.5k rows).
 *
 * Exactness: an entry holds the rows exactly as the scan admitted them (row text, key, canonical hash) at revision
 * `revision` of epoch `epoch`. It is used only when the change log proves that no transaction since then touched
 * the collection's table, any table its visibility plan depends on, or `members`. A fresh scan then admits the very
 * same rows, so the pages, hashes and digest are the same bytes; only the cursor is the current one, as for a fresh
 * scan. Frames are never cached (they carry the request id and cursor). Pruned history (`retained` past the entry's
 * revision) or another epoch is a miss. Entries belong to one engine (its visibility registry: one tenant object's
 * lifetime) and name their module generation, so nothing crosses objects, resets or publications.
 *
 * Bounded: only snapshots small enough to be kept in memory by `coldSnapshot` (RETAIN_BYTES) are stored, and the
 * isolate keeps at most `MAX_BYTES` of row text (least recently used first out). Keys are per module object, so a new
 * module generation never sees an old entry.
 */
import type { ScannedRow } from "./scan.ts";
import type { ReplicaWindow } from "./window.ts";

export interface CachedSnapshot {
  readonly epoch: string;
  readonly revision: number;
  readonly rows: readonly ScannedRow[];
  readonly window: ReplicaWindow;
  readonly bytes: number;
}

export const SNAPSHOT_CACHE_MAX_BYTES = 64 * 1024 * 1024;

export class SnapshotCache {
  private readonly byModule = new WeakMap<object, Map<string, CachedSnapshot>>();
  private readonly order = new Map<string, { module: WeakRef<object>; bytes: number }>();
  private total = 0;
  readonly stats = { hits: 0, misses: 0, stores: 0, evictions: 0 };

  constructor(private readonly maxBytes = SNAPSHOT_CACHE_MAX_BYTES) {}

  get(module: object, key: string): CachedSnapshot | undefined {
    const entry = this.byModule.get(module)?.get(key);
    if (entry) { const slot = this.order.get(this.slot(module, key)); if (slot) { this.order.delete(this.slot(module, key)); this.order.set(this.slot(module, key), slot); } }
    return entry;
  }

  set(module: object, key: string, entry: CachedSnapshot): void {
    if (entry.bytes > this.maxBytes / 4) return;
    let map = this.byModule.get(module);
    if (!map) { map = new Map(); this.byModule.set(module, map); }
    this.delete(module, key);
    map.set(key, entry);
    this.order.set(this.slot(module, key), { module: new WeakRef(module), bytes: entry.bytes });
    this.total += entry.bytes;
    this.stats.stores++;
    while (this.total > this.maxBytes && this.order.size) {
      const [slot, { module: ref }] = this.order.entries().next().value!;
      const owner = ref.deref();
      const k = slot.slice(slot.indexOf("\u0001") + 1);
      if (owner) this.delete(owner, k); else { this.total -= this.order.get(slot)!.bytes; this.order.delete(slot); }
      this.stats.evictions++;
    }
  }

  delete(module: object, key: string): void {
    const map = this.byModule.get(module);
    const old = map?.get(key);
    if (!old) return;
    map!.delete(key);
    this.order.delete(this.slot(module, key));
    this.total -= old.bytes;
  }

  get bytes(): number { return this.total; }

  private ids = new WeakMap<object, number>();
  private next = 0;
  private slot(module: object, key: string): string {
    let id = this.ids.get(module);
    if (id === undefined) { id = ++this.next; this.ids.set(module, id); }
    return `${id}\u0001${key}`;
  }
}

/** One per isolate: every tenant object of the isolate shares the byte budget. */
export const snapshotCache = new SnapshotCache();
