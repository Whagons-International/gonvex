/**
 * Frame normalizer.
 *
 * Turns the raw frames one backend exchanged with one client session into a
 * form that can be compared with another backend's frames. It removes what the
 * contract leaves free and keeps everything else:
 *
 *  - Containers are expanded: `replica.batch` (delivery v2 stream bindings,
 *    exactly as `expandReplicaBatch` in @gonvex/protocol), `replica.readyMany`,
 *    `query.batch`, `query.fanout` and outbound `reducer.callMany`, and outbound
 *    `replica.openMany` becomes one frame per open. Whether a backend batches is
 *    a transport choice, not behaviour.
 *  - Snapshot pages are merged into one logical `replica.snapshot` per
 *    (subscription, snapshotId), rows sorted by key. Page size is tuning.
 *  - Client-minted identifiers become `<id#n>` / `<uuid#n>`, numbered by first
 *    appearance in the session and keyed by value, so one id stays linked
 *    across frames. Server-minted race tokens become unnumbered `<key>`.
 *  - Opaque scopes and epochs become `<scope#n>` / `<epoch#n>`; tokens `<token>`.
 *  - Clocks become `<time>`; `trace` and `device` are dropped.
 *  - Revisions become ranks `r1, r2, ...` (Live Query sequences `s1, ...`),
 *    so order is kept but absolute numbers (which depend on provisioning) are not.
 *    Ranks come from exactly-compared frames only, not from watermark barriers.
 *  - Digests and row hashes become `<digest>`.
 *  - A `replica.needHashes` episode that moves no row is dropped.
 *  - The resume cursor revision of an integrity reopen that answers a
 *    `replica.needHashes` becomes `<persisted-watermark>` (contract-changes 0022 §6).
 *  - Object keys are sorted; batched opens are sorted by path and args.
 *  - Presence Members are sorted by their (aliased) Member id.
 *  - `session.ready` loses the three capabilities of 0012 R20.
 *  - Error text loses JavaScript stack frame lines (ruling 0015).
 *
 * Every rule is listed in RULES below; README.md explains how to change them.
 */
let expandReplicaBatch;
try { ({ expandReplicaBatch } = await import('../../../packages/protocol/dist/index.js')); } catch { /* not built: same algorithm below */ }
expandReplicaBatch ??= (batch, bindings) => {
  const out = [];
  for (const item of batch.items) {
    if (item.type === 'replica.stream') {
      for (const [stream, ids] of bindings) { const i = ids.indexOf(item.id); if (i >= 0 && stream !== item.stream) { ids.splice(i, 1); if (!ids.length) bindings.delete(stream); } }
      const ids = bindings.get(item.stream); if (!ids) bindings.set(item.stream, [item.id]); else if (!ids.includes(item.id)) ids.push(item.id);
    } else if (item.type === 'replica.delta' || item.type === 'replica.ready') { for (const id of bindings.get(item.id) ?? []) out.push({ ...item, id }); }
    else out.push(item);
  }
  if (typeof batch.watermark === 'number') out.push({ type: 'replica.watermark', revision: batch.watermark });
  return out;
};

export const RULES = {
  /** Keys whose string values are identifiers. */
  idKeys: new Set(['id', 'snapshotId', 'originCommandId', 'idempotencyKey', 'stream', 'intentEntropy', 'rootCommandId', 'agentExecutionId', 'subscriptionId', 'windowRevision', 'artifactHash', 'visibilityScope', 'scope', 'replicaScope', 'epoch', 'sessionId', 'connectionId']),
  /** Nested objects whose keys are protocol fields rather than row data. */
  envelopeKeys: new Set(['cursor', 'provenance', 'result_meta', 'subscriptionRevision', 'baseRevision', 'throughRevision']),
  /** Opaque protocol values at any depth (also inside auth.result). */
  opaqueKeys: new Set(['epoch', 'scope', 'visibilityScope', 'replicaScope']),
  /** Keys whose values are lists of identifiers. */
  idListKeys: new Set(['originCommandIds', 'ids']),
  /** Keys holding revisions. */
  revisionKeys: new Set(['revision', 'committedRevision', 'watermark', 'from', 'sequence']),
  /**
   * Tokens the server mints and may send in either order when two updates
   * race (two Live Query windows changing in one commit): unnumbered, so
   * arrival order cannot renumber them.
   */
  serverTokens: new Set(['snapshotId', 'windowRevision', 'stream']),
  /** Keys dropped entirely. */
  dropKeys: new Set(['trace', 'device', 'serverTrace', 'perf']),
  /** Numeric clock keys. */
  timeKey: /(?:At|AtMs|Time|Ms)$|^since$|^serverTime$|^_creationTime$|^expires|^issued/,
  /** Opaque hashes. */
  hashKeys: new Set(['digest', 'hashes', 'contentHash', 'bundleHash']),
  uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
  token: /^gvx_[a-z]+_/,
  /** RFC 3339 instants (Control Plane timestamps are strings, e.g. `expiresAt`). */
  /** JWTs (`header.payload.signature`, base64url JSON): ID tokens an identity provider mints per run. */
  jwt: /^eyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*$/,
  rfc3339: /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/,
};

/**
 * Stable placeholders for one session's identifiers. Keyed by value, so the
 * same id seen in different places (an open's id inside `opens`, then the
 * frame-level id of its snapshot) gets the same placeholder.
 */
class Names {
  constructor() { this.map = new Map(); this.counts = new Map(); }
  name(kind, value) {
    let v = this.map.get(value);
    if (!v) { const n = (this.counts.get(kind) || 0) + 1; this.counts.set(kind, n); v = `<${kind}#${n}>`; this.map.set(value, v); }
    return v;
  }
}

/**
 * Free text (error messages). The message is contract (ruling 0015) and is
 * compared exactly, except for: JavaScript stack frame lines (`\n    at …`),
 * which are not contract; measured durations; embedded UUIDs and tokens.
 */
const text = (v, names) => v
  .replace(/\n[ \t]*at [^\n]*/g, '')
  .replace(/\b\d+(?:\.\d+)? ?ms\b/g, '<n> ms')
  .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, (u) => names.name('uuid', u))
  .replace(/gvx_[a-z]+_[A-Za-z0-9._-]+/g, '<token>');

/** Identifier-like: UUIDs, long hex, or long strings with digits. Words such as "control" or "membership-changed" stay. */
const opaque = (v) => RULES.uuid.test(v) || /^[0-9a-f]{16,}$/i.test(v) || (v.length >= 12 && /[0-9]/.test(v) && !/\s/.test(v));

export const canonical = (v) => Array.isArray(v) ? v.map(canonical)
  : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])])) : v;

/**
 * Clients may list a batch's opens in any order (the SDK's follows random
 * subscription ids). Sort them by path and args so placeholder numbering and
 * comparison do not depend on it.
 */
function canonicalOrder(frame) {
  const by = (x) => `${x.path}\u0000${JSON.stringify(canonical(x.args ?? {}))}`;
  if (frame?.type === 'replica.openMany' && Array.isArray(frame.opens)) return { ...frame, opens: [...frame.opens].sort((a, b) => by(a).localeCompare(by(b))) };
  if (frame?.type === 'query.subscribeMany' && Array.isArray(frame.subscribes)) return { ...frame, subscribes: [...frame.subscribes].sort((a, b) => by(a).localeCompare(by(b))) };
  // Presence lists Members sorted by Member id, which each backend assigns at random: sort by the alias instead.
  const byMember = (a, b) => String(a?.memberId).localeCompare(String(b?.memberId));
  if (frame?.type === 'presence.snapshot' && Array.isArray(frame.members)) return { ...frame, members: [...frame.members].sort(byMember) };
  if (frame?.type === 'presence.patch' && Array.isArray(frame.changes)) return { ...frame, changes: [...frame.changes].sort(byMember) };
  return frame;
}

/**
 * Capabilities a backend may advertise in `session.ready` without them being
 * compared: Rust advertises them to every socket though the shipped SDK never
 * uses them, and the kernel does not (contract-changes 0012 R20).
 */
export const UNCOMPARED_SERVER_CAPABILITIES = ['replicaNativeRows', 'replicaSetDigest', 'replicaFieldPatches'];
function dropUncomparedCapabilities(frame) {
  if (frame?.type !== 'session.ready' || !frame.capabilities) return frame;
  const capabilities = { ...frame.capabilities };
  for (const key of UNCOMPARED_SERVER_CAPABILITIES) delete capabilities[key];
  return { ...frame, capabilities };
}

/** Expands container frames into the frames they stand for. `state.bindings` is per socket. */
export function expand(frame, state) {
  // Malformed frames a scenario sends on purpose (decoder vectors) stay as they are.
  const list = { 'replica.batch': 'items', 'replica.readyMany': 'ready', 'query.batch': 'messages', 'query.fanout': 'ids', 'reducer.callMany': 'calls', 'replica.openMany': 'opens' }[frame?.type];
  if (list && !Array.isArray(frame[list])) return [frame];
  switch (frame?.type) {
    case 'replica.batch': return expandReplicaBatch(frame, state.bindings).flatMap(f => expand(f, state));
    case 'replica.readyMany': return frame.ready.map(r => ({ type: 'replica.ready', ...r }));
    case 'query.batch': return frame.messages.flatMap(f => expand(f, state));
    case 'query.fanout': { const { queryType, ids, ...rest } = frame; return ids.map(id => ({ ...rest, type: queryType, id })); }
    case 'reducer.callMany': return frame.calls.map(c => ({ type: 'reducer.call', ...c }));
    // One frame per open, in path and args order: how many opens the SDK groups depends on when the frames that
    // prompted them arrived (two `needHashes` in one tick or two), not on the backend.
    case 'replica.openMany': return canonicalOrder(frame).opens.map(o => ({ type: 'replica.openMany', opens: [o] }));
    default: return [frame];
  }
}

/**
 * Normalizes the frames of one session.
 * @param {Array<{dir:'in'|'out', frame:any, socket:number, step?:string}>} records
 * @returns {Array<{dir:string, step?:string, frame:any}>}
 */
export function normalizeSession(records) {
  const names = new Names();
  const expanded = [];
  const sockets = new Map();
  const snapshots = new Map();
  for (const r of records) {
    const state = sockets.get(r.socket) ?? sockets.set(r.socket, { bindings: new Map() }).get(r.socket);
    for (const frame of expand(canonicalOrder(dropUncomparedCapabilities(r.frame)), state)) {
      // Merge snapshot pages into one logical snapshot, emitted at the last page.
      if (frame.type === 'replica.snapshotPage' || frame.type === 'replica.snapshotEncodedPage' || frame.type === 'replica.snapshotStart') {
        const key = r.socket + '\u0000' + frame.id + '\u0000' + frame.snapshotId;
        const acc = snapshots.get(key) ?? snapshots.set(key, { first: frame, rows: [] }).get(key);
        if (frame.result) acc.rows.push(...frame.result);
        if (frame.canonicalRows) acc.rows.push(...frame.canonicalRows.map(c => JSON.parse(c)));
        if (frame.truncated) acc.truncated = true;
        if (frame.last) {
          snapshots.delete(key);
          const { sequence, last, result, canonicalRows, scope, ...rest } = frame;
          expanded.push({ dir: r.dir, step: r.step, socket: r.socket, frame: { ...rest, type: 'replica.snapshot', result: sortRows(acc.rows, frame.key), ...(acc.truncated ? { truncated: true } : {}) } });
        }
        continue;
      }
      if (frame.type === 'replica.snapshot') { expanded.push({ dir: r.dir, step: r.step, socket: r.socket, frame: { ...frame, result: sortRows(frame.result ?? [], frame.key) } }); continue; }
      if (frame.type === 'replica.snapshotAck') continue; // credit flow: transport tuning
      expanded.push({ dir: r.dir, step: r.step, socket: r.socket, frame });
    }
  }
  const kept = maskAnsweredReopenCursors(dropNoopReconciles(expanded));
  // Revisions (and, separately, Live Query sequences) become ranks over the
  // distinct values the session saw.
  const family = (key) => (key === 'sequence' ? 's' : 'r');
  const seen = { r: new Set(), s: new Set() };
  const collect = (v, key) => {
    if (Array.isArray(v)) return v.forEach(x => collect(x));
    if (v && typeof v === 'object') return Object.entries(v).forEach(([k, x]) => collect(x, k));
    if (typeof v === 'number' && RULES.revisionKeys.has(key)) seen[family(key)].add(v);
  };
  // Only exactly-compared frames set the ranks. A barrier (`present` policy:
  // watermarks) is compared as sent or not, never by value, and Rust sends or
  // coalesces quiet-tenant watermarks on a timer; letting their values in would
  // renumber every later revision of the session (0022 §6). Barrier revisions
  // keep the rank of an equal compared revision, else become `<barrier>`.
  kept.forEach(e => { if (policyOf(e.frame) !== 'present') collect(e.frame); });
  const ranks = Object.fromEntries(Object.entries(seen).map(([f, values]) => [f,
    new Map([...values].sort((a, b) => a - b).map((v, i) => [v, v === 0 ? `${f}0` : `${f}${i + (values.has(0) ? 0 : 1)}`]))]));
  const rank = (key, v) => ranks[family(key)].get(v) ?? '<barrier>';
  // `depth` 0 is the frame itself: only there (and in `cursor`/`provenance`)
  // are `id`-like keys protocol identifiers. Below that they are row data,
  // compared as-is except for clocks, UUID-shaped values and tokens.
  const walk = (v, key, depth = 0, envelope = true) => {
    if (Array.isArray(v)) return envelope && RULES.idListKeys.has(key) ? v.map(x => typeof x === 'string' ? names.name(key === 'ids' ? 'id' : 'command', x) : walk(x, undefined, depth + 1, false)).sort() : v.map(x => walk(x, undefined, depth + 1, false));
    if (v && typeof v === 'object') {
      const out = {};
      for (const k of Object.keys(v).sort()) {
        if (RULES.dropKeys.has(k)) continue;
        if (RULES.hashKeys.has(k)) { out[k] = k === 'hashes' && v[k] && typeof v[k] === 'object' ? Object.keys(v[k]).map(x => walk(x, undefined, depth + 1, false)).sort() : '<digest>'; continue; }
        out[k] = walk(v[k], k, depth + 1, depth === 0 || (envelope && RULES.envelopeKeys.has(key)) || (depth === 0 && RULES.envelopeKeys.has(k)));
      }
      return out;
    }
    if (typeof v === 'number') {
      if (RULES.revisionKeys.has(key) && (envelope || key === 'revision')) return rank(key, v);
      if (key && RULES.timeKey.test(key)) return '<time>';
      return v;
    }
    if (typeof v === 'string') {
      // 0015 also applies when an Action catches an actual host error and
      // returns its string. Only JS stack lines are free here; preserve the
      // complete first line, non-stack detail, UUIDs, tokens and durations.
      if (key !== 'error' && key !== 'reason' && key !== 'message' && v.startsWith('GonvexHostError: host call failed:')) return v.replace(/\n[ \t]*at [^\n]*/g, '');
      if (key && RULES.timeKey.test(key) && RULES.rfc3339.test(v)) return '<time>';
      if (key && envelope && RULES.serverTokens.has(key)) return `<${key}>`;
      if (key && envelope && RULES.idKeys.has(key) && opaque(v)) return names.name(key === 'epoch' ? 'epoch' : key.endsWith('Scope') || key === 'scope' ? 'scope' : key === 'artifactHash' ? 'artifact' : 'id', v);
      if (key && RULES.opaqueKeys.has(key) && opaque(v)) return names.name(key === 'epoch' ? 'epoch' : 'scope', v);
      if (RULES.token.test(v)) return '<token>';
      if (RULES.jwt.test(v)) return '<jwt>';
      if (RULES.uuid.test(v)) return names.name('uuid', v);
      return key === 'error' || key === 'reason' || key === 'message' ? text(v, names) : v;
    }
    return v;
  };
  return kept.map(e => ({ dir: e.dir, step: e.step, frame: walk(e.frame) }));
}

/**
 * A backend may ask a client to prove a collection (`replica.needHashes`)
 * whenever it cannot cheaply tell what changed; the Rust runtime does so
 * depending on timing. When the client's integrity reopen comes back with no
 * row change, the episode changed nothing the client can observe, and is
 * dropped. Episodes that move rows are kept and compared.
 *
 * The SDK reopens every collection it was asked about in the same tick in one
 * `replica.openMany`, so whether two `needHashes` give one frame of two opens
 * or two frames of one depends on when they arrive. A reopen frame is dropped
 * with its episodes when every open in it answers a `needHashes` and none of
 * them moves a row (K10: Rust and the kernel batch the same episode either way).
 */
function dropNoopReconciles(list) {
  const drop = new Set();
  const asked = new Map(); // socket -> id -> index of its unanswered needHashes
  for (let i = 0; i < list.length; i++) {
    const e = list[i];
    if (e.dir === 'in' && e.frame?.type === 'replica.needHashes') {
      (asked.get(e.socket) ?? asked.set(e.socket, new Map()).get(e.socket)).set(e.frame.id, i);
      continue;
    }
    const f = e.frame;
    const opens = e.dir === 'out' && (f?.type === 'replica.openMany' ? f.opens : f?.type === 'replica.open' ? [f] : null);
    if (!Array.isArray(opens) || !opens.length) continue;
    const pending = asked.get(e.socket);
    if (!opens.every(o => o?.fullIntegrity && pending?.has(o.id))) {
      for (const o of opens) pending?.delete(o?.id);
      continue;
    }
    // Each reopened id: an optional row-less delta, then its ready.
    const episode = [i];
    let noop = true;
    for (const open of opens) {
      episode.push(pending.get(open.id));
      pending.delete(open.id);
      let answered = false;
      for (let j = i + 1; j < list.length; j++) {
        const g = list[j];
        if (g.socket !== e.socket || g.dir !== 'in' || g.frame?.id !== open.id) continue;
        if (g.frame.type === 'replica.delta' && !g.frame.upserts?.length && !g.frame.deleted?.length) { episode.push(j); continue; }
        if (g.frame.type === 'replica.ready') { episode.push(j); answered = true; }
        break;
      }
      if (!answered) noop = false;
    }
    if (noop) for (const k of episode) drop.add(k);
  }
  return list.filter((_, k) => !drop.has(k));
}

/**
 * When a backend asks for hashes (`replica.needHashes`), the client reopens the
 * collection with `fullIntegrity` and the cursor it has persisted. That cursor
 * includes the last watermark the client applied, and watermarks are coalescible
 * barriers sent on a timer (compared only as sent or not, FRAME_POLICY). So the
 * revision depends on whether a quiet-tenant watermark landed before the
 * `needHashes` (thin-sdk-parity, 0022 §6). Only that value is masked, before
 * ranking so no other rank moves. The epoch, the other fields and every
 * integrity open a client sends on its own (replica-resume) stay compared.
 */
function maskAnsweredReopenCursors(list) {
  const asked = new Map(); // socket -> ids with an unanswered needHashes
  return list.map(e => {
    const f = e.frame;
    if (e.dir === 'in' && f?.type === 'replica.needHashes') {
      (asked.get(e.socket) ?? asked.set(e.socket, new Set()).get(e.socket)).add(f.id);
      return e;
    }
    const opens = e.dir === 'out' && (f?.type === 'replica.openMany' ? f.opens : f?.type === 'replica.open' ? [f] : null);
    const ids = asked.get(e.socket);
    if (!Array.isArray(opens) || !ids?.size) return e;
    let changed = false;
    const masked = opens.map(o => {
      if (!o?.fullIntegrity || !ids.has(o.id)) return o;
      ids.delete(o.id);
      if (typeof o.cursor?.revision !== 'number') return o;
      changed = true;
      return { ...o, cursor: { ...o.cursor, revision: '<persisted-watermark>' } };
    });
    if (!changed) return e;
    return { ...e, frame: f.type === 'replica.open' ? masked[0] : { ...f, opens: masked } };
  });
}

function sortRows(rows, key = '_id') {
  return [...rows].sort((a, b) => String(a?.[key]).localeCompare(String(b?.[key])));
}

/**
 * Comparison class of a normalized frame. `exact`: the multiset of such frames
 * in a step must match. `present`: both backends sent at least one, or neither
 * (coalescible barriers). `ignore`: not compared.
 */
export const FRAME_POLICY = {
  'replica.watermark': 'present',
  'control.watermark': 'present',
  'query.progress': 'present',
  'replica.syncing': 'present',
  'telemetry.batch': 'ignore',
  'telemetry.event': 'ignore',
  // error.* are contract (CONTRACT.md A.2, keep) but timer-driven in the SDK:
  // K15 owns a dedicated scenario for them (README, "Normalization").
  'error.envelope': 'ignore',
  'error.ack': 'ignore',
  'error.heartbeat': 'ignore',
  'error.register': 'ignore',
};
export const policyOf = (frame) => FRAME_POLICY[frame?.type] ?? 'exact';

/** One-line, stable text for a normalized frame (canonical JSON). */
export const frameText = (frame) => JSON.stringify(canonical(frame));

/**
 * Normalizes a scenario observation or error text: clocks, tokens and
 * UUID-shaped values as in frames, nothing else. Observations are what the
 * scenario chose to record, so ids in them are data unless UUID-shaped.
 */
export function normalizeValue(value, names = new Names()) {
  const walk = (v, key) => {
    if (Array.isArray(v)) return v.map(x => walk(x));
    if (v && typeof v === 'object') return Object.fromEntries(Object.keys(v).sort().filter(k => !RULES.dropKeys.has(k)).map(k => [k, RULES.hashKeys.has(k) ? '<digest>' : walk(v[k], k)]));
    if (typeof v === 'number' && key && RULES.timeKey.test(key)) return '<time>';
    // Absolute revisions depend on how a backend provisions a tenant; frames keep their order as ranks.
    if (typeof v === 'number' && key && RULES.revisionKeys.has(key)) return '<revision>';
    if (typeof v === 'string') {
      if (key && RULES.timeKey.test(key) && RULES.rfc3339.test(v)) return '<time>';
      if (key && RULES.opaqueKeys.has(key) && opaque(v)) return names.name(key === 'epoch' ? 'epoch' : 'scope', v);
      if (RULES.token.test(v)) return '<token>';
      if (RULES.jwt.test(v)) return '<jwt>';
      if (RULES.uuid.test(v)) return names.name('uuid', v);
      return text(v, names);
    }
    return v;
  };
  return walk(value);
}
export { Names };
