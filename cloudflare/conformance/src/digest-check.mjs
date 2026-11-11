/**
 * Per-backend digest self-check.
 *
 * The normalizer masks every `digest` (they cover values that differ between
 * runs), so the differ alone cannot see a backend that computes them wrongly.
 * This check runs on each backend's raw frames, before normalization: for
 * every collection subscription it keeps the row hashes the client holds and
 * verifies each digest the server sends in `replica.ready` and `replica.delta`
 * against them, with the SDK's algorithm (packages/client/src/replica-integrity.ts):
 *
 *   row hash  sha256(stableStringify(row))           rows as sent (already
 *                                                     projected to the collection's columns)
 *   xor1:…    XOR of the row hashes                   (replicaAggregateDigest)
 *   legacy    sha256(stableStringify(sorted [key, hash] pairs))  (replicaHashesDigest)
 *
 * The client's state comes from snapshots (which replace it), deltas (upserts
 * and deletes), and the `keys`/`hashes` a client sends when it resumes, which
 * is exactly what the client claims to hold.
 */
import { createHash } from 'node:crypto';
import { expand } from './normalize.mjs';

const utf8Compare = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
export function stableStringify(value) {
  if (typeof value === 'string') return JSON.stringify(value).replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  return `{${Object.keys(value).sort(utf8Compare).map((k) => `${stableStringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}
const sha256 = (text) => createHash('sha256').update(text).digest('hex');
export const rowHash = (row) => sha256(stableStringify(row));

/** The digest a client holding `hashes` (key -> row hash) expects, in the server's format. */
export function expectedDigest(hashes, format) {
  if (format === 'xor1') {
    const bytes = Buffer.alloc(32);
    for (const hash of hashes.values()) { const h = Buffer.from(hash, 'hex'); for (let i = 0; i < 32; i++) bytes[i] ^= h[i]; }
    return 'xor1:' + bytes.toString('hex');
  }
  const pairs = [...hashes.keys()].sort(utf8Compare).map(k => [k, hashes.get(k)]);
  return sha256(stableStringify(pairs));
}

/**
 * @param {Array<{dir:'in'|'out', frame:any, socket:number}>} records one session's raw frames, in order
 * @returns {{ checked: number, problems: string[] }}
 */
export function checkDigests(records) {
  const subs = new Map(); // socket \0 id -> { path, key, hashes: Map, staging?: Map }
  const sockets = new Map();
  const problems = [];
  let checked = 0;
  const sub = (socket, id) => subs.get(socket + '\u0000' + id);
  const verify = (s, frame) => {
    if (!s || typeof frame.digest !== 'string' || frame.truncated) return;
    const format = frame.digest.startsWith('xor1:') ? 'xor1' : 'legacy';
    const expected = expectedDigest(s.hashes, format);
    checked++;
    if (expected !== frame.digest) {
      problems.push(`digest mismatch on ${frame.type} for ${s.path}: server sent ${frame.digest}, the ${s.hashes.size} rows the client holds give ${expected}`);
    }
  };
  for (const r of records) {
    const state = sockets.get(r.socket) ?? sockets.set(r.socket, { bindings: new Map() }).get(r.socket);
    for (const frame of expand(r.frame, state)) {
      if (!frame || typeof frame !== 'object') continue;
      if (r.dir === 'out') {
        if (frame.type !== 'replica.openMany' && frame.type !== 'replica.open') continue;
        for (const open of frame.type === 'replica.open' ? [frame] : frame.opens ?? []) {
          const claimed = new Map(open.hashes && open.keys ? open.keys.map(k => [String(k), open.hashes[k]]) : []);
          subs.set(r.socket + '\u0000' + open.id, { path: open.path, key: 'id', hashes: claimed });
        }
        continue;
      }
      // Subscriptions are scoped to one auth: a re-auth without
      // `subscriptionsPreserved` drops them all (CONTRACT.md A.1.4). Frames a
      // stale stream binding still expands to a dropped id are not checked.
      if (frame.type === 'auth.result' && frame.result?.subscriptionsPreserved !== true) {
        for (const k of [...subs.keys()]) if (k.startsWith(r.socket + '\u0000')) subs.delete(k);
        continue;
      }
      const s = frame.id !== undefined ? sub(r.socket, frame.id) : undefined;
      if (!s) continue;
      switch (frame.type) {
        case 'replica.snapshot':
        case 'replica.snapshotPage':
        case 'replica.snapshotEncodedPage':
          if (frame.type === 'replica.snapshot' || frame.sequence === 0 || !s.staging) s.staging = new Map();
          s.key = frame.key ?? s.key;
          for (const row of frame.result ?? frame.canonicalRows?.map(c => JSON.parse(c)) ?? []) s.staging.set(String(row[s.key]), rowHash(row));
          if (frame.type === 'replica.snapshot' || frame.last) { s.hashes = s.staging; s.staging = undefined; }
          break;
        case 'replica.delta':
          for (const row of frame.upserts ?? []) s.hashes.set(String(row[s.key]), rowHash(row));
          for (const k of frame.deleted ?? []) s.hashes.delete(String(k));
          verify(s, frame);
          break;
        case 'replica.ready':
          verify(s, frame);
          break;
        case 'replica.reset':
        case 'replica.closed':
          subs.delete(r.socket + '\u0000' + frame.id);
          break;
        default:
      }
    }
  }
  return { checked, problems };
}
