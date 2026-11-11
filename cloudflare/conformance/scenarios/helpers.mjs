/** Small helpers shared by scenarios. */
import { createHash, randomUUID } from 'node:crypto';

/** Mounts a Replica Collection or Live Query on an SDK or thin client. */
export function watch(client, ref, args = {}) {
  const w = client.watchReplica(ref, args);
  const stop = w.onUpdate(() => { try { w.localReplicaResult(); } catch {} });
  return {
    w,
    stop,
    ready: () => { try { return w.status().isUpToDate && w.localReplicaState()?.completeness === 'complete'; } catch { return false; } },
    rows: () => { try { return sortBy(w.localReplicaResult() ?? [], ref.replica?.key ?? 'id'); } catch (error) { return { error: String(error.message) }; } },
  };
}

/** Rows projected to the collection's declared columns, sorted by key. */
export function project(rows, columns) {
  return rows.map(r => Object.fromEntries(columns.map(c => [c, r[c] ?? null])));
}

export const sortBy = (rows, key) => [...rows].sort((a, b) => String(a[key]).localeCompare(String(b[key])));

/** Calls a Reducer online and settles to { result } or { error, code, errorClass }. */
export async function call(client, ref, args) {
  try { return { result: await client.reducer({ ...ref, localExecution: undefined }, args, { offline: 'reject' }) }; }
  catch (error) { return { error: String(error?.message ?? error), ...(error?.code ? { code: error.code } : {}), ...(error?.errorClass ? { errorClass: error.errorClass } : {}) }; }
}

/** Waits for the thin/SDK collection to be up to date and to hold `predicate`. */
export async function rowsWhen(ctx, view, predicate, label) {
  await ctx.until(() => view.ready() && predicate(view.rows()), label);
  return view.rows();
}

// The SDK's replica-integrity stableStringify (sorted keys by UTF-8 bytes, U+2028/9 escaped).
const utf8Compare = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
export function stableStringify(value) {
  if (typeof value === 'string') return JSON.stringify(value).replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  return `{${Object.keys(value).sort(utf8Compare).map((k) => `${stableStringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}

/** keys, per-row hashes and xor1 digest over rows projected to `columns`, as the SDK resumes. */
export function integrity(rows, key, columns) {
  const keys = [], hashes = {}, aggregate = Buffer.alloc(32);
  for (const row of rows) {
    const projected = Object.fromEntries(columns.filter(c => Object.prototype.hasOwnProperty.call(row, c)).map(c => [c, row[c]]));
    const hash = createHash('sha256').update(stableStringify(projected)).digest();
    for (let i = 0; i < 32; i++) aggregate[i] ^= hash[i];
    keys.push(String(row[key])); hashes[String(row[key])] = hash.toString('hex');
  }
  return { keys, hashes, digest: 'xor1:' + aggregate.toString('hex') };
}

export const ZERO_DIGEST = 'xor1:' + '0'.repeat(64);

/** Sends one frame-level reducer.call and waits for its reducer.result / reducer.error. */
export async function rawReducer(socket, path, args, extra = {}) {
  const id = randomUUID();
  socket.send({ type: 'reducer.call', id, path, args, ...extra });
  const reply = await socket.next(m => (m.type === 'reducer.result' || m.type === 'reducer.error') && m.id === id, { label: `${path} reply` });
  return reply.type === 'reducer.result' ? { result: reply.result } : { error: reply.error, class: reply.class ?? null, retryable: reply.retryable ?? null };
}

/** Frame-level one-shot query. */
export async function rawQuery(socket, path, args, extra = {}) {
  const id = randomUUID();
  socket.send({ type: 'query.call', id, path, args, ...extra });
  const reply = await socket.next(m => (m.type === 'query.result' || m.type === 'query.error') && m.id === id, { label: `${path} reply` });
  return reply.type === 'query.result' ? { result: reply.result } : { error: reply.error };
}

/**
 * Opens a collection on a raw socket and applies what comes back onto `base`
 * (the rows the client already holds when it resumes). Resolves with
 * { rows, ready, types, changed } once replica.ready arrives; `changed` lists
 * the row keys the backend sent or deleted.
 */
export async function rawOpen(socket, open, base = []) {
  const id = open.id ?? randomUUID();
  socket.send({ type: 'replica.openMany', opens: [{ digest: ZERO_DIGEST, args: {}, ...open, id }] });
  const rows = new Map(base.map(r => [String(r.id), r]));
  const types = [], changed = new Set();
  let snapshotStarted = false;
  for (;;) {
    const m = await socket.next(x => x.id === id || (x.type === 'replica.readyMany' && x.ready.some(r => r.id === id)), { label: `${open.path} ready` });
    types.push(m.type);
    if (m.type === 'replica.snapshot' || m.type === 'replica.snapshotPage') {
      if (!snapshotStarted) { rows.clear(); snapshotStarted = true; }
      for (const r of m.result ?? []) { rows.set(String(r[m.key ?? 'id']), r); changed.add(String(r[m.key ?? 'id'])); }
    }
    if (m.type === 'replica.delta') {
      for (const r of m.upserts ?? []) { rows.set(String(r.id), r); changed.add(String(r.id)); }
      for (const k of m.deleted ?? []) { rows.delete(String(k)); changed.add(String(k)); }
    }
    if (m.type === 'replica.error') return { id, error: m.error, types };
    if (m.type === 'replica.reset') return { id, reset: m.reason, types };
    const ready = m.type === 'replica.ready' ? m : m.type === 'replica.readyMany' ? m.ready.find(r => r.id === id) : undefined;
    if (ready) return { id, rows: [...rows.values()].sort((a, b) => String(a.id).localeCompare(String(b.id))), ready, types, changed: [...changed].sort() };
  }
}

/** Auth capabilities for raw sockets: JSON rows, no batching (frames stay one per message). */
export const RAW_CAPABILITIES = { replicaNoOldValues: 1, replicaSnapshotPages: 1, replicaSetDigest: 1, replicaWatermark: 1, replicaReadyMany: 1 };
