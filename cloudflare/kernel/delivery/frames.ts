// Replica frames in serde's byte order (protocol `ServerMessage`; flattened
// `BTreeMap` payloads in key order). Rows of snapshot pages are spliced as the
// JSON text SQLite produced; other values go through `JSON.stringify`.

import type { JsonObject, JsonValue } from "../visibility/json.ts";
import type { ReplicaDefinition } from "./definition.ts";

export interface Cursor { readonly epoch: string; readonly revision: number }

const s = JSON.stringify;
const cursorText = (cursor: Cursor) => `{"epoch":${s(cursor.epoch)},"revision":${cursor.revision}}`;

/** Snapshot metadata shared by every page (`replica_snapshot_message` minus `pageDigest`/`digest`/`hashes`). */
export interface PageMetadata {
  readonly path: string;
  readonly definition: ReplicaDefinition;
  readonly scope: string;
  readonly visibilityScope: string;
  readonly truncated: boolean;
}

/**
 * One `replica.snapshotPage` (A.4 row 10). `digest` only on the last page
 * (the aggregate, `pageDigest`).
 */
export function snapshotPage(input: {
  snapshotId: string; sequence: number; last: boolean; id: string; rows: readonly string[]; cursor: Cursor; meta: PageMetadata; digest?: string;
}): string {
  const { meta } = input;
  const d = meta.definition;
  return `{"type":"replica.snapshotPage","snapshotId":${s(input.snapshotId)},"sequence":${input.sequence},"last":${input.last},"id":${s(input.id)},`
    + `"result":[${input.rows.join(",")}],"cursor":${cursorText(input.cursor)},"key":${s(d.key)},`
    + (input.digest !== undefined ? `"digest":${s(input.digest)},` : "")
    + `"maxBytes":${d.maxBytes},"maxRows":${d.maxRows},"mode":${s(d.mode)},"orderBy":${s(d.orderBy)},"orderDirection":${s(d.orderDirection)},`
    + `"path":${s(meta.path)},"scope":${s(meta.scope)},"truncated":${meta.truncated},"visibilityScope":${s(meta.visibilityScope)}}`;
}

/**
 * A non-paged `replica.snapshot` (`legacy_snapshot_frame`: `pageDigest`, `scope`
 * and `visibilityScope` stripped; `digest` and `hashes` inline only below 2,048 rows).
 */
export function snapshotFrame(input: {
  id: string; rows: readonly string[]; cursor: Cursor; meta: PageMetadata; digest?: string; hashes?: ReadonlyMap<string, string>;
}): string {
  const d = input.meta.definition;
  let hashes = "";
  if (input.hashes) {
    const keys = [...input.hashes.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    hashes = `"hashes":{${keys.map((k) => `${s(k)}:${s(input.hashes!.get(k))}`).join(",")}},`;
  }
  return `{"type":"replica.snapshot","id":${s(input.id)},"result":[${input.rows.join(",")}],"cursor":${cursorText(input.cursor)},"key":${s(d.key)},`
    + (input.digest !== undefined ? `"digest":${s(input.digest)},` : "") + hashes
    + `"maxBytes":${d.maxBytes},"maxRows":${d.maxRows},"mode":${s(d.mode)},"orderBy":${s(d.orderBy)},"orderDirection":${s(d.orderDirection)},`
    + `"path":${s(input.meta.path)},"truncated":${input.meta.truncated}}`;
}

/** The parts of a `replica.delta` (`replica_delta`). */
export interface Delta {
  readonly path: string;
  readonly cursor: Cursor;
  readonly upserts: readonly JsonObject[];
  readonly deleted: readonly string[];
  readonly digest: string;
  readonly truncated: boolean;
}

/** `replica.delta` as a plain frame: `id, cursor`, then the payload keys in order. */
export function deltaFrame(id: string, delta: Delta): string {
  return `{"type":"replica.delta","id":${s(id)},"cursor":${cursorText(delta.cursor)},"deleted":${s(delta.deleted)},"digest":${s(delta.digest)},`
    + `"originCommandIds":[],"path":${s(delta.path)},"truncated":${delta.truncated},"upserts":${s(delta.upserts)}}`;
}

/** A delta item's shared body (A.4 row 3): `digest`/`truncated` removed, closing brace stripped. */
export function deltaBody(token: string, delta: Omit<Delta, "digest" | "truncated">): string {
  return `{"type":"replica.delta","id":${s(token)},"cursor":${cursorText(delta.cursor)},"deleted":${s(delta.deleted)},"originCommandIds":[],"path":${s(delta.path)},"upserts":${s(delta.upserts)}`;
}

/** The per-connection tail of a delta item. */
export const deltaTail = (digest: string, truncated: boolean): string => `,"digest":${s(digest)},"truncated":${truncated}}`;

/** `replica.ready` (`replica_ready`): `id, cursor, digest`, then `mode, path, truncated`. */
export function readyFrame(id: string, cursor: Cursor, digest: string, definition: ReplicaDefinition, path: string, truncated: boolean): string {
  return `{"type":"replica.ready","id":${s(id)},"cursor":${cursorText(cursor)},"digest":${s(digest)},"mode":${s(definition.mode)},"path":${s(path)},"truncated":${truncated}}`;
}

export const needHashesFrame = (id: string, path: string): string => `{"type":"replica.needHashes","id":${s(id)},"path":${s(path)}}`;
export const resetFrame = (id: string, path: string, reason: string): string => `{"type":"replica.reset","id":${s(id)},"path":${s(path)},"reason":${s(reason)}}`;
export const errorFrame = (id: string, path: string | undefined, error: string): string =>
  path === undefined ? `{"type":"replica.error","id":${s(id)},"error":${s(error)}}` : `{"type":"replica.error","id":${s(id)},"path":${s(path)},"error":${s(error)}}`;
export const watermarkFrame = (revision: number): string => `{"type":"replica.watermark","revision":${revision}}`;

/** `ReplicaChange`. */
export interface Change {
  readonly entity: string;
  readonly id: string;
  readonly operation: string;
  readonly oldValue?: JsonObject;
  readonly newValue?: JsonObject;
  readonly changedColumns: readonly string[];
}

/** `PublicInvocationProvenance`. */
export interface Provenance {
  readonly rootCommandId: string;
  readonly rootChannel?: string;
  readonly channel: string;
  readonly actorAccountId?: string | null;
  readonly actorMemberId?: string | null;
  readonly onBehalfOfMemberId?: string | null;
  readonly agentExecutionId?: string | null;
}

function provenanceText(p: Provenance): string {
  let out = `{"rootCommandId":${s(p.rootCommandId)}`;
  if (p.rootChannel !== undefined) out += `,"rootChannel":${s(p.rootChannel)}`;
  out += `,"channel":${s(p.channel)}`;
  for (const key of ["actorAccountId", "actorMemberId", "onBehalfOfMemberId", "agentExecutionId"] as const) {
    const value = p[key];
    if (value !== undefined && value !== null) out += `,${s(key)}:${s(value)}`;
  }
  return `${out}}`;
}

function changeText(change: Change, withOld: boolean): string {
  let out = `{"entity":${s(change.entity)},"id":${s(change.id)},"operation":${s(change.operation)}`;
  if (withOld && change.oldValue !== undefined) out += `,"oldValue":${s(change.oldValue)}`;
  if (change.newValue !== undefined) out += `,"newValue":${s(change.newValue)}`;
  if (change.changedColumns.length) out += `,"changedColumns":${s(change.changedColumns)}`;
  return `${out}}`;
}

/** `replica.transaction`. `withOld` false strips every `oldValue` (batch items, compact frames). */
export function transactionFrame(input: { cursor: Cursor; originCommandId?: string; provenance?: Provenance; changes: readonly Change[] }, withOld: boolean): string {
  let out = `{"type":"replica.transaction","cursor":${cursorText(input.cursor)}`;
  if (input.originCommandId !== undefined) out += `,"originCommandId":${s(input.originCommandId)}`;
  if (input.provenance !== undefined) out += `,"provenance":${provenanceText(input.provenance)}`;
  return `${out},"changes":[${input.changes.map((c) => changeText(c, withOld)).join(",")}]}`;
}

/** Compression threshold of a socket (`GONVEX_WS_COMPRESSION_THRESHOLD`): what the compact choice compares with. */
export const COMPRESSION_THRESHOLD = 1024;

const utf8 = (text: string) => new TextEncoder().encode(text).length;

/**
 * `encode_compact_replica_transaction` (protocol/src/replica_wire.rs): the
 * compact frame, unless keeping the old images makes a frame under the
 * compression threshold cross it.
 */
export function compactTransaction(input: Parameters<typeof transactionFrame>[0], threshold: number | undefined = COMPRESSION_THRESHOLD): string {
  const compact = transactionFrame(input, false);
  if (threshold === undefined) return compact;
  let count = utf8(compact);
  if (count >= threshold) return compact;
  for (const change of input.changes) {
    if (change.oldValue === undefined) continue;
    count += ',"oldValue":'.length + utf8(s(change.oldValue));
    if (count >= threshold) return transactionFrame(input, true);
  }
  return compact;
}

/** Bytes of a JSON value as serde writes it (page budgets). */
export const jsonBytes = (value: JsonValue): number => utf8(s(value));
