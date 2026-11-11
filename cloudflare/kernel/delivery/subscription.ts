// `replica.open`: a cold snapshot or a resume, and the subscription it
// installs (replica.rs `open_replica_with`, `ReplicaSubscription`,
// `SnapshotPages`; replica_cursor.rs `cursor_resumable`).
//
// Everything here runs while the caller holds the tenant's invocation queue,
// so every read of one open sees the same committed state. Rows are never
// collected beyond `RETAIN_BYTES`: a larger window is scanned twice, once for
// its digest and boundary and once to stream its pages.

import type { TenantSession } from "../session/ports.ts";
import type { StorageSession } from "../storage/index.ts";
import { HandleCache } from "../visibility/class.ts";
import type { JsonObject, JsonValue } from "../visibility/json.ts";
import type { VisibilityPlan } from "../visibility/plan.ts";
import { resolveMember, resolveWithParents, resolvedDependencyColumns } from "../visibility/principal.ts";
import type { ResolvedVisibility } from "../visibility/resolved.ts";
import type { VisibilityRegistry } from "../visibility/tenant.ts";
import { Aggregate, hashesDigest, utf8Length } from "./canonical.ts";
import { bounded, type ReplicaDefinition, type ReplicaModule } from "./definition.ts";
import { boundedSqlVisibility, tableChangeRelevance } from "./rules.ts";
import { deltaFrame, errorFrame, needHashesFrame, readyFrame, snapshotFrame, snapshotPage, type Cursor, type PageMetadata } from "./frames.ts";
import { CollapsedChanges, readChangeRows, readClock, type LogChange, type LogSql } from "./log.ts";
import { scanPlan, scanWindow, type ScannedRow, type ScanProgress } from "./scan.ts";
import { collectionFiltersAdmit, errors, projectRow, ReplicaError, visible, type ReplicaWindow } from "./window.ts";
import { canonical } from "./canonical.ts";

/** Legacy snapshot pages (replica.rs `SNAPSHOT_PAGE_BYTES`, `SNAPSHOT_PAGE_ROWS`). */
export const PAGE_BYTES = 256 * 1024;
export const PAGE_ROWS = 512;
/** Inline `hashes` in a non-paged snapshot below this many rows (`INLINE_SNAPSHOT_HASH_LIMIT`). */
export const INLINE_HASH_LIMIT = 2048;
/** Row text an open keeps before switching to two passes (128 MB isolates). */
export const RETAIN_BYTES = 4 << 20;
/** Row text read between drains of the socket channel. */
export const CHUNK_BYTES = 2 << 20;
/** A non-paged snapshot frame must stay under the platform's outbound limit (0018). */
export const MAX_SNAPSHOT_FRAME_BYTES = 32 * 1024 * 1024 - 1024;

/** One open Replica Collection of one connection. Holds no rows. */
export interface Subscription {
  readonly id: string;
  readonly path: string;
  readonly args: JsonValue;
  /** Arguments as serde writes them: the stream identity and the shared-decision key. */
  readonly argsText: string;
  readonly definition: ReplicaDefinition;
  readonly plan: VisibilityPlan;
  readonly visibilityScope: string;
  cursor: Cursor;
  resolved: ResolvedVisibility;
  digest: Aggregate;
  window: ReplicaWindow;
  /** The compiled visibility binding of `resolved`; released on removal and rebind. */
  readonly mask: HandleCache;
  concernTables?: Set<string>;
  parentAclColumns?: Map<string, Set<string>>;
}

/** The client's open request (decoded by the session layer). */
export interface OpenRequest {
  readonly id: string;
  readonly path: string;
  readonly args: JsonValue;
  readonly cursor?: { readonly epoch: string; readonly revision: number };
  readonly keys: readonly string[];
  readonly hashes: Readonly<Record<string, string>>;
  readonly digest?: string;
  readonly fullIntegrity: boolean;
}

export interface OpenContext {
  readonly session: TenantSession;
  readonly module: ReplicaModule | undefined;
  /** `replicaSetDigest` negotiated. */
  readonly sumNegotiated: boolean;
  /** `replicaSnapshotPages` negotiated without native rows: legacy pages. */
  readonly paged: boolean;
  readonly scope: string;
  readonly visibilityScope: string;
  readonly registry: VisibilityRegistry;
  /** One synchronous body in one Query invocation (the caller holds the invocation queue across calls). */
  readonly read: <T>(body: (db: StorageSession) => T) => Promise<T>;
  /** Frames for this socket, in order. */
  readonly emit: (frames: string[]) => void;
  /** Resolves when what was emitted has left this object (backpressure between chunks). */
  readonly drain: () => Promise<void>;
  readonly progress?: ScanProgress;
}

/** `cursor_resumable`: only within the retained history of the current epoch. */
export function cursorResumable(client: OpenRequest["cursor"], current: Cursor, retained: number): boolean {
  return client !== undefined && client.epoch === current.epoch && client.revision <= current.revision && client.revision >= retained;
}

/** Stable JSON text of arguments (serde with `preserve_order`: the client's key order). */
export const argsText = (args: JsonValue): string => JSON.stringify(args) ?? "null";

type DigestRequest = "aggregate" | "wholeSet" | "reconcile";

class PageWriter {
  private rows: string[] = [];
  private bytes: number;
  private sequence = 0;
  private readonly snapshotId = crypto.randomUUID();
  private readonly pending: string[] = [];

  constructor(private readonly base: { id: string; cursor: Cursor; meta: PageMetadata }, private readonly digestShape: string) {
    this.bytes = this.reserve();
  }

  /** `serde_json::to_vec(&self.page(Vec::new(), true)).len() + 1`. */
  private reserve(): number {
    return utf8Length(snapshotPage({ ...this.base, snapshotId: this.snapshotId, sequence: this.sequence, last: true, rows: [], digest: this.digestShape })) + 1;
  }

  push(text: string): void {
    let rowBytes = utf8Length(text) + (this.rows.length ? 1 : 0);
    if (this.rows.length && (this.bytes + rowBytes > PAGE_BYTES || this.rows.length === PAGE_ROWS)) {
      this.pending.push(snapshotPage({ ...this.base, snapshotId: this.snapshotId, sequence: this.sequence, last: false, rows: this.rows }));
      this.sequence += 1;
      this.rows = [];
      this.bytes = this.reserve();
      rowBytes -= 1;
    }
    this.rows.push(text);
    this.bytes += rowBytes;
  }

  /** Pages completed since the last call. */
  take(): string[] {
    return this.pending.splice(0);
  }

  finish(digest: string): string[] {
    this.pending.push(snapshotPage({ ...this.base, snapshotId: this.snapshotId, sequence: this.sequence, last: true, rows: this.rows, digest }));
    this.rows = [];
    return this.take();
  }
}

function requestedSumDigest(request: OpenRequest, negotiated: boolean): boolean {
  const sum = request.digest?.startsWith("sum128-v1:") ?? false;
  if (sum && !negotiated) throw errors.digestNotNegotiated();
  return sum;
}

function resolve(db: StorageSession, context: OpenContext, definition: ReplicaDefinition, plan: VisibilityPlan, revision: number): ResolvedVisibility {
  const module = context.module!;
  if (plan.version === 2) {
    const acl = context.registry.acl.ensure(db, module.visibility, [plan]);
    const clock = readClock(sqlOf(db));
    return resolveWithParents(db, context.session, plan, revision, module.visibility, acl, undefined, {
      cache: context.registry.principals, clock: { epoch: clock.epoch, revision: clock.revision, retained: clock.retained },
    });
  }
  // `resolve_for_query` keeps legacy sets in SQL for bounded collections; `resolve` materializes them.
  return resolveMember(db, context.session, plan, revision, !bounded(definition));
}

/** The K01 session as the log reader's `sql.exec` shape (kernel statements, no admission). */
export function sqlOf(db: StorageSession): LogSql {
  return {
    exec: (sql: string, ...bindings: unknown[]) => {
      let n = 0;
      const text = sql.replace(/\?/g, () => `$${++n}`);
      const result = db.run(text, bindings);
      return { toArray: () => result.rows.map((row) => Object.fromEntries(result.columnNames.map((name, index) => [name, row[index] ?? null]))) };
    },
  };
}

/**
 * Opens one collection. Emits its frames (pages or a snapshot, a resume
 * delta, `replica.ready`, or `replica.needHashes` / `replica.error`) through
 * `context.emit` and returns the installed subscription, if any.
 */
export async function openReplica(request: OpenRequest, context: OpenContext): Promise<Subscription | undefined> {
  const fail = (error: unknown) => {
    context.emit([errorFrame(request.id, request.path, error instanceof Error ? error.message : String(error))]);
    return undefined;
  };
  try {
    return await open(request, context);
  } catch (error) {
    if (!(error instanceof ReplicaError)) console.error("delivery: replica open failed", error);
    return fail(error);
  }
}

async function open(request: OpenRequest, context: OpenContext): Promise<Subscription | undefined> {
  const sum = requestedSumDigest(request, context.sumNegotiated);
  if (context.session.anonymous) throw new ReplicaError("authenticate with an active tenant before opening a Replica Collection");
  const module = context.module;
  if (!module) throw errors.moduleMissing();
  const fn = module.functions.get(request.path);
  if (!fn || fn.kind !== "query" || fn.delivery !== "replica" || !fn.replica) throw errors.functionMissing(request.path);
  const definition = fn.replica;
  const plan = module.visibility.get(definition.table);
  if (!plan) throw errors.visibilityMissing(definition.table);

  // The read: clock, principal, scan statement.
  const begun = await context.read((db) => {
    const clock = readClock(sqlOf(db));
    const resolved = resolve(db, context, definition, plan, clock.revision);
    const scan = scanPlan(definition, plan, resolved, request.args, db.catalogOf(definition.table).columns);
    return { clock, resolved, scan };
  });
  const { clock, resolved, scan } = begun;
  const cursor: Cursor = { epoch: clock.epoch, revision: clock.revision };
  const resumable = cursorResumable(request.cursor, cursor, clock.retained);
  const digest = new Aggregate(sum ? "sum128-v1" : "xor1");
  const requested: DigestRequest = request.digest && !request.fullIntegrity && Object.keys(request.hashes).length === 0
    ? (request.digest.startsWith("xor1:") || sum ? "aggregate" : "wholeSet")
    : "reconcile";
  const meta = (truncated: boolean): PageMetadata => ({ path: request.path, definition, scope: context.scope, visibilityScope: context.visibilityScope, truncated });
  const scanAll = (visit: (row: ScannedRow) => boolean | void, light = false) => scanWindow(context.read, definition, scan, visit, {
    chunkBytes: CHUNK_BYTES, between: context.drain, light, ...(context.progress ? { progress: context.progress } : {}),
  });
  let window: ReplicaWindow;

  if (!resumable) {
    window = await coldSnapshot(request, context, scanAll, digest, cursor, meta);
  } else if (requested === "aggregate") {
    const after = request.cursor?.revision ?? 0;
    const dependencies = resolvedDependencyColumns(plan, resolved);
    const tables = [...new Set([...dependencies.tables(), definition.table])].sort();
    const changes: LogChange[] = [];
    if (after < clock.revision) await context.read((db) => { readChangeRows(sqlOf(db), after, clock.revision, tables, (c) => changes.push(c)); });
    if (boundedSqlVisibility(definition, plan, resolved) || tableChangeRelevance(definition.table, plan, resolved, changes)[1]) {
      context.emit([needHashesFrame(request.id, request.path)]);
      return undefined;
    }
    window = await scanAll((row) => { digest.add(row.hash); });
    const prior = digest.clone();
    const upserts = new Map<string, JsonObject>();
    const deleted = new Set<string>();
    const image = (row: JsonValue): { row: JsonObject; hash: string } | undefined => {
      if (row === null || !collectionFiltersAdmit(row, request.args, definition) || !visible(plan, resolved, row)) return undefined;
      const projection = projectRow(row, definition);
      return { row: projection, hash: canonical(projection).hash };
    };
    for (const change of new CollapsedChanges(changes).table(definition.table)) {
      const old = image(change.old);
      const next = image(change.new);
      if (next) prior.remove(next.hash);
      if (old) prior.add(old.hash);
      if (old && next && old.hash !== next.hash) upserts.set(change.rowId, next.row);
      else if (!old && next) upserts.set(change.rowId, next.row);
      else if (old && !next) deleted.add(change.rowId);
    }
    if ((window.truncated && (upserts.size || deleted.size)) || request.digest !== prior.digest()) {
      context.emit([needHashesFrame(request.id, request.path)]);
      return undefined;
    }
    if (upserts.size || deleted.size || !sameCursor(request.cursor, cursor)) {
      const ids = [...upserts.keys()].sort();
      context.emit([deltaFrame(request.id, { path: request.path, cursor, upserts: ids.map((id) => upserts.get(id)!), deleted: [...deleted].sort(), digest: digest.digest(), truncated: window.truncated })]);
    }
  } else if (requested === "wholeSet") {
    const hashes = new Map<string, string>();
    window = await scanAll((row) => { digest.add(row.hash); hashes.set(row.key, row.hash); });
    if (request.digest !== hashesDigest(hashes)) {
      context.emit([needHashesFrame(request.id, request.path)]);
      return undefined;
    }
  } else {
    // The client sent its keys and hashes: only rows whose hash differs, and keys SQL no longer returns.
    const upserts: JsonObject[] = [];
    const seen = new Set<string>();
    let retained = 0;
    window = await scanAll((row) => {
      digest.add(row.hash);
      if (request.hashes[row.key] !== row.hash) {
        upserts.push(row.value);
        retained += row.text.length;
      }
      seen.add(row.key);
      if (retained > MAX_SNAPSHOT_FRAME_BYTES) throw new ReplicaError("replica reconcile delta exceeds the outbound frame limit");
    });
    const deleted = request.keys.filter((key) => !seen.has(key));
    if (upserts.length || deleted.length || !sameCursor(request.cursor, cursor)) {
      context.emit([deltaFrame(request.id, { path: request.path, cursor, upserts, deleted, digest: digest.digest(), truncated: window.truncated })]);
    }
  }
  context.emit([readyFrame(request.id, cursor, digest.digest(), definition, request.path, window.truncated)]);
  return {
    id: request.id, path: request.path, args: request.args, argsText: argsText(request.args), definition, plan,
    visibilityScope: context.visibilityScope, cursor, resolved, digest, window, mask: new HandleCache(),
  };
}

const sameCursor = (a: OpenRequest["cursor"], b: Cursor) => a !== undefined && a.epoch === b.epoch && a.revision === b.revision;

/**
 * A cold open: pages (or one snapshot frame) then nothing; the caller's ready
 * follows. Small windows are scanned once and kept; a window over
 * `RETAIN_BYTES` is scanned for its digest and boundary, then again to page it.
 */
async function coldSnapshot(
  request: OpenRequest, context: OpenContext, scanAll: (visit: (row: ScannedRow) => boolean | void, light?: boolean) => Promise<ReplicaWindow>,
  digest: Aggregate, cursor: Cursor, meta: (truncated: boolean) => PageMetadata,
): Promise<ReplicaWindow> {
  let kept: ScannedRow[] | undefined = [];
  let keptBytes = 0;
  const window = await scanAll((row) => {
    digest.add(row.hash);
    if (kept) {
      kept.push(row);
      keptBytes += row.text.length;
      if (keptBytes > RETAIN_BYTES) kept = undefined;
    }
  });
  if (!context.paged) {
    if (!kept) throw new ReplicaError("replica snapshot exceeds the outbound frame limit; negotiate replicaSnapshotPages");
    const inline = kept.length <= INLINE_HASH_LIMIT ? new Map(kept.map((row) => [row.key, row.hash])) : undefined;
    const frame = snapshotFrame({
      id: request.id, rows: kept.map((row) => row.text), cursor, meta: meta(window.truncated),
      ...(inline ? { digest: hashesDigest(inline), hashes: inline } : {}),
    });
    if (utf8Length(frame) >= MAX_SNAPSHOT_FRAME_BYTES) throw new ReplicaError("replica snapshot exceeds the outbound frame limit; negotiate replicaSnapshotPages");
    context.emit([frame]);
    return window;
  }
  const final = digest.digest();
  const pages = new PageWriter({ id: request.id, cursor, meta: meta(window.truncated) }, final);
  if (kept) {
    for (const row of kept) pages.push(row.text);
    context.emit(pages.finish(final));
    return window;
  }
  // Second pass: the same rows (the invocation queue is held), paged as they stream; nothing is hashed again.
  let remaining = window.rows;
  await scanAll((row) => {
    if (remaining-- <= 0) return false;
    pages.push(row.text);
    const done = pages.take();
    if (done.length) context.emit(done);
    return remaining > 0;
  }, true);
  context.emit(pages.finish(final));
  return window;
}
