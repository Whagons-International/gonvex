import type { ServerMessage } from "@gonvex/protocol";
import { replicaAggregateDigest, replicaHashesDigest, replicaRowsHashes } from "./replica-integrity.js";
import { isReplicaRevision } from "./revision-policy.js";

export type SnapshotPage = Extract<ServerMessage, { type: "replica.snapshotPage" }>;
type Snapshot = Extract<ServerMessage, { type: "replica.snapshot" }>;

/** Inactive rows never enter LocalReplica until the final digest is valid. */
export class SnapshotPageStager {
  private readonly first: Omit<SnapshotPage, "result" | "digest">;
  private readonly rows: Snapshot["result"] = [];
  private readonly hashes: Record<string, string> = Object.create(null);
  private sequence = 0;
  private finished = false;
  private bytes = 0;

  constructor(page: SnapshotPage) {
    const { result, digest, ...metadata } = page;
    this.first = metadata;
  }

  async append(page: SnapshotPage): Promise<Snapshot | undefined> {
    const { result, digest, ...metadata } = page;
    if (this.finished || "hashes" in page || !Number.isSafeInteger(page.sequence) || page.sequence !== this.sequence
      || typeof page.last !== "boolean" || typeof page.snapshotId !== "string" || !page.snapshotId
      || typeof page.key !== "string" || !page.key || typeof page.scope !== "string" || !page.scope
      || typeof page.visibilityScope !== "string" || !page.visibilityScope
      || !isReplicaRevision(page.cursor.revision) || !page.cursor.epoch
      || !Array.isArray(result)
      || !sameMetadata(metadata, this.first)
      || (!page.last && (result.length === 0 || digest !== undefined))
      || result.length > 512) throw new Error("invalid-snapshot-page");
    const pageBytes = new TextEncoder().encode(JSON.stringify(page)).byteLength;
    if (result.length > 1 && pageBytes > 256 * 1024) throw new Error("snapshot-page-too-large");
    for (const row of result) this.bytes += new TextEncoder().encode(JSON.stringify(row)).byteLength;
    if ((page.maxRows !== undefined && page.maxRows > 0 && this.rows.length + result.length > page.maxRows)
      || (page.maxBytes !== undefined && page.maxBytes > 0 && this.bytes > page.maxBytes)) {
      throw new Error("snapshot-page-budget-exceeded");
    }
    if (result.some(row => !row || Array.isArray(row) || typeof row !== "object"
      || (typeof row[page.key] !== "string" && typeof row[page.key] !== "number"))) {
      throw new Error("invalid-snapshot-key");
    }
    const hashes = await replicaRowsHashes(result, page.key);
    for (const [id, hash] of Object.entries(hashes)) {
      if (Object.prototype.hasOwnProperty.call(this.hashes, id)) throw new Error("duplicate-snapshot-key");
      this.hashes[id] = hash;
    }
    for (const row of result) this.rows.push(row);
    this.sequence += 1;
    if (!page.last) return undefined;
    this.finished = true;
    const actual = digest?.startsWith("xor1:") ? replicaAggregateDigest(this.hashes) : await replicaHashesDigest(this.hashes);
    if (!digest || actual !== digest) throw new Error("snapshot-page-integrity-mismatch");
    const { snapshotId, sequence, last, ...snapshot } = page;
    return { ...snapshot, type: "replica.snapshot", result: this.rows, hashes: this.hashes, digest };
  }
}

function sameMetadata(left: Omit<SnapshotPage, "result" | "digest">, right: Omit<SnapshotPage, "result" | "digest">) {
  return left.id === right.id && left.snapshotId === right.snapshotId && left.path === right.path
    && left.key === right.key && left.mode === right.mode && left.orderBy === right.orderBy
    && left.orderDirection === right.orderDirection && left.maxRows === right.maxRows
    && left.maxBytes === right.maxBytes && left.truncated === right.truncated
    && left.cursor.epoch === right.cursor.epoch && left.cursor.revision === right.cursor.revision
    && left.scope === right.scope && left.visibilityScope === right.visibilityScope;
}
