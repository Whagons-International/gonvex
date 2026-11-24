// Hoisted O(tables) batch-read fan-out into WLP1 PackedPages.
// Solves the per-member N+1 fan-out bottleneck:
//   1. Reads changed rows from SQLite ONCE per touched table (chunked <=100 binds), NEVER per member.
//   2. Groups connected members by visibility class key.
//   3. Filters and encodes each visibility class ONCE into shared WLP1 PackedPage bytes + delete rid array.
//   4. Shares the exact same Uint8Array bytes across every member in the same visibility class.

import { PAGE_ROWS_MAX, encodePage, type PageCol } from "../codec/index";
import { TX_OP_DELETE, TX_OP_UPSERT } from "./schema";
import type { TxChange } from "./tx";

const MAX_IN_BINDS = 100;

/** Specification for reading one replicated table in hoisted batch fan-out. */
export interface FanoutTableSpec {
  readonly tbl: number;
  readonly tableName: string;
  /** SQL column list starting with `rid, ver, ...` matching `cols`. */
  readonly selectColumnsSql: string;
  readonly cols: readonly PageCol[];
  /** Visibility predicate evaluated in memory against a raw row tuple `[rid, ver, ...]`. */
  readonly visibleToClass: (classKey: number, row: readonly SqlStorageValue[]) => boolean;
}

/** One subscribed member routed to a visibility class (dense integer class key). */
export interface FanoutMember {
  readonly memberId: string;
  readonly classKey: number;
}

/** Shared encoded wire payload for one `(tbl, classKey)` pair. */
export interface ClassTablePayload {
  readonly tbl: number;
  readonly classKey: number;
  readonly upsertPages: readonly Uint8Array[];
  readonly deletedRids: Int32Array;
}

export interface FanoutStats {
  readonly sqlStatements: number;
  readonly rowsRead: number;
  readonly classesEncoded: number;
  readonly pagesEncoded: number;
}

export interface BatchFanoutResult {
  /** Shared encoded payloads keyed by dense classKey. */
  readonly byClass: ReadonlyMap<number, readonly ClassTablePayload[]>;
  /** Per-member lookup pointing to the exact shared class payload array (zero copy). */
  readonly byMember: ReadonlyMap<string, readonly ClassTablePayload[]>;
  readonly stats: FanoutStats;
}

interface TableBucket {
  readonly upsertRids: number[];
  readonly deleteRids: number[];
  readonly seenUpsert: Set<number>;
  readonly seenDelete: Set<number>;
}

/**
 * Builds shared `WLP1` PackedPage deliveries for a committed batch of `TxChange`s across `members`.
 * SQL statement count and `rowsRead` are O(touched tables), completely invariant to `members.length`.
 */
export function buildBatchFanout(
  sql: SqlStorage,
  specs: ReadonlyMap<number, FanoutTableSpec>,
  changes: readonly TxChange[],
  members: readonly FanoutMember[],
): BatchFanoutResult {
  const buckets = new Map<number, TableBucket>();
  const touchedTbls: number[] = [];

  for (let i = 0; i < changes.length; i++) {
    const ch = changes[i]!;
    if (!specs.has(ch.tbl)) continue;
    let bucket = buckets.get(ch.tbl);
    if (bucket === undefined) {
      bucket = { upsertRids: [], deleteRids: [], seenUpsert: new Set(), seenDelete: new Set() };
      buckets.set(ch.tbl, bucket);
      touchedTbls.push(ch.tbl);
    }
    if (ch.op === TX_OP_UPSERT) {
      bucket.seenDelete.delete(ch.rid);
      if (!bucket.seenUpsert.has(ch.rid)) {
        bucket.seenUpsert.add(ch.rid);
        bucket.upsertRids.push(ch.rid);
      }
    } else if (ch.op === TX_OP_DELETE) {
      bucket.seenUpsert.delete(ch.rid);
      if (!bucket.seenDelete.has(ch.rid)) {
        bucket.seenDelete.add(ch.rid);
        bucket.deleteRids.push(ch.rid);
      }
    }
  }

  // Group unique visibility classes once.
  const distinctClasses: number[] = [];
  const seenClasses = new Set<number>();
  for (let i = 0; i < members.length; i++) {
    const ck = members[i]!.classKey;
    if (!seenClasses.has(ck)) {
      seenClasses.add(ck);
      distinctClasses.push(ck);
    }
  }

  let sqlStatements = 0;
  let rowsRead = 0;
  let pagesEncoded = 0;

  // Hoisted batch read: fetch current raw rows ONCE per touched table, outside any member/class loop.
  const fetchedRows = new Map<number, (readonly SqlStorageValue[])[]>();
  for (let t = 0; t < touchedTbls.length; t++) {
    const tbl = touchedTbls[t]!;
    const spec = specs.get(tbl)!;
    const bucket = buckets.get(tbl)!;
    const rids = bucket.upsertRids;
    const rows: (readonly SqlStorageValue[])[] = [];
    let offset = 0;
    while (offset < rids.length) {
      const chunkLen = rids.length - offset < MAX_IN_BINDS ? rids.length - offset : MAX_IN_BINDS;
      let marks = "";
      const binds = new Array<SqlStorageValue>(chunkLen);
      for (let i = 0; i < chunkLen; i++) {
        if (i > 0) marks += ",";
        marks += "?";
        binds[i] = rids[offset + i]!;
      }
      const cur = sql.exec(
        `SELECT ${spec.selectColumnsSql} FROM "${spec.tableName}" WHERE rid IN (${marks}) ORDER BY rid`,
        ...binds,
      );
      sqlStatements++;
      const iter = cur.raw();
      for (;;) {
        const next = iter.next();
        if (next.done) break;
        rows.push(next.value);
      }
      rowsRead += cur.rowsRead;
      offset += chunkLen;
    }
    fetchedRows.set(tbl, rows);
  }

  // Encode ONCE per visibility class per touched table.
  const byClass = new Map<number, ClassTablePayload[]>();
  for (let c = 0; c < distinctClasses.length; c++) {
    const classKey = distinctClasses[c]!;
    const list: ClassTablePayload[] = [];
    for (let t = 0; t < touchedTbls.length; t++) {
      const tbl = touchedTbls[t]!;
      const spec = specs.get(tbl)!;
      const bucket = buckets.get(tbl)!;
      const rows = fetchedRows.get(tbl) ?? [];

      const visibleRows: (readonly SqlStorageValue[])[] = [];
      for (let r = 0; r < rows.length; r++) {
        const row = rows[r]!;
        if (spec.visibleToClass(classKey, row)) visibleRows.push(row);
      }

      const upsertPages: Uint8Array[] = [];
      let pOff = 0;
      while (pOff < visibleRows.length) {
        const pEnd = pOff + PAGE_ROWS_MAX < visibleRows.length ? pOff + PAGE_ROWS_MAX : visibleRows.length;
        const slice = visibleRows.slice(pOff, pEnd);
        upsertPages.push(encodePage(tbl, spec.cols, slice));
        pagesEncoded++;
        pOff = pEnd;
      }

      const deletedRids = new Int32Array(bucket.deleteRids.length);
      for (let d = 0; d < bucket.deleteRids.length; d++) deletedRids[d] = bucket.deleteRids[d]!;

      if (upsertPages.length > 0 || deletedRids.length > 0) {
        list.push({ tbl, classKey, upsertPages, deletedRids });
      }
    }
    byClass.set(classKey, list);
  }

  // Point every member to its class's shared payload array (zero extra copies).
  const byMember = new Map<string, readonly ClassTablePayload[]>();
  for (let i = 0; i < members.length; i++) {
    const m = members[i]!;
    byMember.set(m.memberId, byClass.get(m.classKey) ?? []);
  }

  return {
    byClass,
    byMember,
    stats: {
      sqlStatements,
      rowsRead,
      classesEncoded: distinctClasses.length,
      pagesEncoded,
    },
  };
}
