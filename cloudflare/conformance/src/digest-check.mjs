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
  const cursor = c => c && typeof c.epoch === 'string' && Number.isSafeInteger(c.revision) && c.revision >= 0;
  const descriptor = open => JSON.stringify(Object.fromEntries(Object.keys(open).filter(k => !['type','id','cursor','digest','keys','hashes','fullIntegrity'].includes(k)).sort().map(k => [k,open[k]])));
  const authScope = result => typeof result?.projectId === 'string' && typeof result?.tenantId === 'string' && typeof result?.accountId === 'string' && typeof result?.replica?.scope === 'string' && typeof result?.replica?.visibilityScope === 'string'
    ? JSON.stringify([result.projectId,result.tenantId,result.accountId,result.replica?.scope ?? null,result.replica?.visibilityScope ?? null]) : undefined;
  const clear = socket => { for(const k of [...subs.keys()]) if(k.startsWith(socket+'\u0000')) subs.delete(k); };
  const sub = (socket, id) => subs.get(socket + '\u0000' + id);
  const verify = (s, frame) => {
    if (!s || typeof frame.digest !== 'string' || frame.truncated) return;
    if (s.evidenceMissing) problems.push(`unproven resume baseline for ${s.path}: ${s.evidenceMissing}`);
    const format = frame.digest.startsWith('xor1:') ? 'xor1' : 'legacy';
    const expected = expectedDigest(s.hashes, format);
    checked++;
    if (expected !== frame.digest) {
      problems.push(`digest mismatch on ${frame.type} for ${s.path}: server sent ${frame.digest}, the ${s.hashes.size} rows the client holds give ${expected}`);
    }
    if (frame.truncated) s.committed = false;
    else if (cursor(frame.cursor) && !s.staging && s.hasState && !s.evidenceMissing && expected === frame.digest) {
      s.cursor = {...frame.cursor}; s.committed = true;
    }
  };
  for (const r of records) {
    const state = sockets.get(r.socket) ?? sockets.set(r.socket, { bindings: new Map(), watermark: 0, authorizing: false, scope: undefined }).get(r.socket);
    for (const frame of expand(r.frame, state)) {
      if (!frame || typeof frame !== 'object') continue;
      if (r.dir === 'out') {
        if (frame.type === 'auth') { state.authorizing = true; continue; }
        if (frame.type === 'replica.close') { subs.delete(r.socket + '\u0000' + frame.id); continue; }
        if (frame.type !== 'replica.openMany' && frame.type !== 'replica.open') continue;
        for (const open of frame.type === 'replica.open' ? [frame] : frame.opens ?? []) {
          const claimed = new Map(open.hashes && open.keys ? open.keys.map(k => [String(k), open.hashes[k]]) : []);
          const old = sub(r.socket, open.id), shape = descriptor(open);
          const explicit = Object.hasOwn(open,'keys') || Object.hasOwn(open,'hashes');
          let reuse = false, evidenceMissing;
          if (!explicit && open.cursor) {
            reuse = !!old?.committed && !old.staging && old.descriptor === shape && state.scope !== undefined
              && !state.authorizing && old.scope === state.scope && cursor(open.cursor) && cursor(old.cursor)
              && old.cursor.epoch === open.cursor.epoch && open.cursor.revision >= old.cursor.revision
              && open.cursor.revision <= Math.max(old.cursor.revision, state.watermark)
              && typeof open.digest === 'string' && expectedDigest(old.hashes,open.digest.startsWith('xor1:')?'xor1':'legacy') === open.digest;
            if (!reuse) evidenceMissing = 'same socket/id/descriptor/scope, complete rows and observed cursor/digest could not be established';
          }
          subs.set(r.socket + '\u0000' + open.id, {
            path:open.path, descriptor:shape, scope:state.scope, key:reuse?old.key:'id', hashes:reuse?new Map(old.hashes):claimed,
            hasState:reuse || explicit || !open.cursor, committed:reuse, ...(reuse?{cursor:{...open.cursor}}:{}), evidenceMissing,
          });
        }
        continue;
      }
      // Subscriptions are scoped to one auth: a re-auth without
      // `subscriptionsPreserved` drops them all (CONTRACT.md A.1.4). Frames a
      // stale stream binding still expands to a dropped id are not checked.
      if (frame.type === 'auth.result') {
        const scope = authScope(frame.result);
        if (frame.result?.subscriptionsPreserved !== true || scope !== state.scope) { clear(r.socket); state.watermark = 0; }
        state.scope = scope; state.authorizing = false;
        continue;
      }
      if (frame.type === 'auth.error' || frame.type === 'client.updateRequired') {
        clear(r.socket); state.scope = undefined; state.watermark = 0; state.authorizing = false;
        continue;
      }
      if (frame.type === 'replica.watermark' && Number.isSafeInteger(frame.revision) && frame.revision >= 0) state.watermark = Math.max(state.watermark,frame.revision);
      const s = frame.id !== undefined ? sub(r.socket, frame.id) : undefined;
      if (!s) continue;
      switch (frame.type) {
        case 'replica.snapshot':
        case 'replica.snapshotPage':
        case 'replica.snapshotEncodedPage':
          if (frame.type === 'replica.snapshot' || frame.sequence === 0 || !s.staging) { s.staging = new Map(); s.committed = false; }
          s.key = frame.key ?? s.key;
          for (const row of frame.result ?? frame.canonicalRows?.map(c => JSON.parse(c)) ?? []) s.staging.set(String(row[s.key]), rowHash(row));
          if (frame.type === 'replica.snapshot' || frame.last) { s.hashes = s.staging; s.staging = undefined; s.hasState = true; s.evidenceMissing = undefined; if(frame.truncated) s.committed = false; }
          break;
        case 'replica.delta':
          if (frame.truncated) s.committed = false;
          for (const row of frame.upserts ?? []) s.hashes.set(String(row[s.key]), rowHash(row));
          for (const k of frame.deleted ?? []) s.hashes.delete(String(k));
          verify(s, frame);
          break;
        case 'replica.ready':
          if (frame.truncated) s.committed = false;
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
