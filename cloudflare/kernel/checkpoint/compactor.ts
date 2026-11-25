// Alarm-driven Tenant DO checkpoint compactor (Phase 3).
// Streams each replicated table directly from DO `SqlStorageCursor.raw()` into immutable `WLP1`
// PackedPages (optionally wrapped in `F_PAGE` binary frames) and publishes them to R2 so cold
// clients can boot straight from R2/CDN with 0 Tenant DO wakes and 0 SQLite reads.

import {
  F_PAGE,
  PAGE_ROWS_MAX,
  encodeFrame,
  encodeSqlCursor,
  type PageCol,
} from "../codec/index";
import { readTxSeq } from "../tx-log/index";
import {
  CHECKPOINT_CACHE_IMMUTABLE,
  CHECKPOINT_CACHE_LATEST,
  CHECKPOINT_CONTENT_TYPE_FRAMES,
  CHECKPOINT_CONTENT_TYPE_JSON,
  CHECKPOINT_SCHEMA_VERSION,
  checkpointLatestKey,
  checkpointManifestKey,
  checkpointPageKey,
  pruneOldCheckpoints,
  serializeLatestPointer,
  serializeManifest,
  type CheckpointBucket,
  type CheckpointLatestPointer,
  type CheckpointManifest,
  type CheckpointPageRef,
  type CheckpointTableManifest,
} from "./manifest";

export const CHECKPOINT_STATE_KEY = "dod/checkpoint-state";

export interface CheckpointState {
  readonly epoch: number;
  readonly lastSeq: number;
  readonly lastCompactedAtMs: number;
  readonly lastManifestKey: string | null;
  readonly totalRows: number;
  readonly totalBytes: number;
}

export interface CheckpointTableSpec {
  readonly tbl: number;
  readonly tableName: string;
  /** SQL column list starting with `rid, ver, ...` matching `cols`. */
  readonly selectColumnsSql: string;
  /** Optional SQL filter predicate without leading `WHERE` (e.g. `deleted = 0`). */
  readonly whereSql?: string;
  readonly cols: readonly PageCol[];
}

export interface CompactOptions {
  readonly tenantId: string;
  readonly bucket: CheckpointBucket;
  readonly tables: readonly CheckpointTableSpec[];
  readonly pageRows?: number;
  readonly nowMs?: number;
  /** Skip compaction if `currentSeq - state.lastSeq < minSeqDelta` (unless `force` is true). */
  readonly minSeqDelta?: number;
  readonly force?: boolean;
  /** Keep the newest N checkpoints in R2 and prune older runs (0 disables pruning; default 3). */
  readonly retainCheckpoints?: number;
  /** Wrap each WLP1 page in a `F_PAGE` binary frame so edge serving streams frames verbatim (default true). */
  readonly frameWrap?: boolean;
}

export interface CompactResult {
  readonly skipped: boolean;
  readonly epoch: number;
  readonly seq: number;
  readonly manifest: CheckpointManifest | null;
  readonly latest: CheckpointLatestPointer | null;
  readonly rowsRead: number;
  readonly pagesUploaded: number;
  readonly bytesUploaded: number;
  readonly prunedKeys: number;
}

export type CheckpointStorage = Pick<DurableObjectStorage, "sql" | "get" | "put">;

interface EncodedTableScan {
  readonly pages: Uint8Array[];
  readonly totalRows: number;
  readonly rowsRead: number;
}

/**
 * Scans one replicated table via `SqlStorageCursor.raw()` into WLP1 PackedPages
 * with zero per-row JS objects and zero JSON serialization.
 */
function scanTableToPages(
  sql: SqlStorage,
  spec: CheckpointTableSpec,
  pageRows: number,
): EncodedTableScan {
  const whereClause = spec.whereSql !== undefined && spec.whereSql.length > 0
    ? ` WHERE ${spec.whereSql}`
    : "";
  const cur = sql.exec(
    `SELECT ${spec.selectColumnsSql} FROM "${spec.tableName}"${whereClause} ORDER BY rid`,
  );
  return encodeSqlCursor(spec.tbl, spec.cols, cur, pageRows);
}

const IMMUTABLE_FRAME_PUT = {
  httpMetadata: {
    contentType: CHECKPOINT_CONTENT_TYPE_FRAMES,
    cacheControl: CHECKPOINT_CACHE_IMMUTABLE,
  },
} as const;

const IMMUTABLE_JSON_PUT = {
  httpMetadata: {
    contentType: CHECKPOINT_CONTENT_TYPE_JSON,
    cacheControl: CHECKPOINT_CACHE_IMMUTABLE,
  },
} as const;

const LATEST_JSON_PUT = {
  httpMetadata: {
    contentType: CHECKPOINT_CONTENT_TYPE_JSON,
    cacheControl: CHECKPOINT_CACHE_LATEST,
  },
} as const;

const DEFAULT_STATE: CheckpointState = {
  epoch: 1,
  lastSeq: 0,
  lastCompactedAtMs: 0,
  lastManifestKey: null,
  totalRows: 0,
  totalBytes: 0,
};

/**
 * Tenant DO checkpoint compactor.
 * Safe to invoke from a DO alarm or admin RPC; publishes immutable pages + manifest first
 * and swaps `latest.json` last so concurrent edge readers never observe a torn checkpoint.
 */
export class CheckpointCompactor {
  constructor(private readonly storage: CheckpointStorage) {}

  async loadState(): Promise<CheckpointState> {
    const stored = await this.storage.get<CheckpointState>(CHECKPOINT_STATE_KEY);
    if (stored === undefined || stored === null) return DEFAULT_STATE;
    return stored;
  }

  /** Increments checkpoint epoch (e.g., after a schema migration or full tenant import). */
  async bumpEpoch(): Promise<CheckpointState> {
    const cur = await this.loadState();
    const next: CheckpointState = {
      epoch: cur.epoch + 1,
      lastSeq: 0,
      lastCompactedAtMs: cur.lastCompactedAtMs,
      lastManifestKey: null,
      totalRows: 0,
      totalBytes: 0,
    };
    await this.storage.put(CHECKPOINT_STATE_KEY, next);
    return next;
  }

  async compact(options: CompactOptions): Promise<CompactResult> {
    const state = await this.loadState();
    const seq = readTxSeq(this.storage.sql);
    let rowsRead = 1; // _gonvex_tx_seq singleton read

    const minDelta = options.minSeqDelta ?? 1;
    const force = options.force ?? false;
    if (!force && state.lastManifestKey !== null && seq - state.lastSeq < minDelta) {
      return {
        skipped: true,
        epoch: state.epoch,
        seq,
        manifest: null,
        latest: null,
        rowsRead,
        pagesUploaded: 0,
        bytesUploaded: 0,
        prunedKeys: 0,
      };
    }

    const pageRows = options.pageRows ?? PAGE_ROWS_MAX;
    const frameWrap = options.frameWrap ?? true;
    const nowMs = options.nowMs ?? Date.now();
    const epoch = state.epoch;

    const tableManifests: CheckpointTableManifest[] = [];
    let totalRows = 0;
    let totalPages = 0;
    let totalBytes = 0;

    for (let t = 0; t < options.tables.length; t++) {
      const spec = options.tables[t]!;
      const scanned = scanTableToPages(this.storage.sql, spec, pageRows);
      rowsRead += scanned.rowsRead;

      const pageRefs: CheckpointPageRef[] = [];
      let tblBytes = 0;
      let remaining = scanned.totalRows;

      for (let p = 0; p < scanned.pages.length; p++) {
        const rawPage = scanned.pages[p]!;
        const payload = frameWrap
          ? encodeFrame({ k: F_PAGE, t: spec.tbl, flags: 0, bytes: rawPage })
          : rawPage;
        const pageRowCount = remaining > pageRows ? pageRows : remaining;
        remaining -= pageRowCount;

        const key = checkpointPageKey(options.tenantId, epoch, seq, spec.tbl, p);
        const putRes = await options.bucket.put(key, payload, IMMUTABLE_FRAME_PUT);
        const etag = putRes !== null ? putRes.etag : "";

        pageRefs.push({
          page: p,
          key,
          rows: pageRowCount,
          bytes: payload.length,
          etag,
        });
        tblBytes += payload.length;
        totalPages++;
      }

      tableManifests.push({
        tbl: spec.tbl,
        name: spec.tableName,
        totalRows: scanned.totalRows,
        totalBytes: tblBytes,
        pages: pageRefs,
      });
      totalRows += scanned.totalRows;
      totalBytes += tblBytes;
    }

    const manifest: CheckpointManifest = {
      v: CHECKPOINT_SCHEMA_VERSION,
      tenantId: options.tenantId,
      epoch,
      seq,
      createdAtMs: nowMs,
      frameWrapped: frameWrap,
      totalRows,
      totalPages,
      totalBytes,
      tables: tableManifests,
    };

    const manifestKey = checkpointManifestKey(options.tenantId, epoch, seq);
    await options.bucket.put(manifestKey, serializeManifest(manifest), IMMUTABLE_JSON_PUT);

    const latest: CheckpointLatestPointer = {
      v: CHECKPOINT_SCHEMA_VERSION,
      tenantId: options.tenantId,
      epoch,
      seq,
      manifestKey,
      createdAtMs: nowMs,
      frameWrapped: frameWrap,
      totalRows,
      totalPages,
      totalBytes,
    };

    const latestKey = checkpointLatestKey(options.tenantId);
    await options.bucket.put(latestKey, serializeLatestPointer(latest), LATEST_JSON_PUT);

    const nextState: CheckpointState = {
      epoch,
      lastSeq: seq,
      lastCompactedAtMs: nowMs,
      lastManifestKey: manifestKey,
      totalRows,
      totalBytes,
    };
    await this.storage.put(CHECKPOINT_STATE_KEY, nextState);

    const retain = options.retainCheckpoints ?? 3;
    let prunedKeys = 0;
    if (retain > 0) {
      const pruned = await pruneOldCheckpoints(options.bucket, options.tenantId, retain);
      prunedKeys = pruned.deletedKeys;
    }

    return {
      skipped: false,
      epoch,
      seq,
      manifest,
      latest,
      rowsRead,
      pagesUploaded: totalPages,
      bytesUploaded: totalBytes,
      prunedKeys,
    };
  }
}
