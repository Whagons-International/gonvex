/**
 * Per-backend watermark self-check (contract-changes 0024 §2, K10).
 *
 * The normalizer compares watermarks only as sent or not (they are
 * coalescible barriers, and Rust sends some on a timer), so their values are
 * checked here, on each backend's raw frames, before anything is masked:
 *
 *  - watermarks never go down on one socket (between authentications that
 *    drop the socket's subscriptions);
 *  - a watermark never exceeds the highest committed revision the backend had
 *    shown any session by the end of that step (a call result's
 *    `committedRevision`, or a replica frame's cursor);
 *  - a watermark never falls below the client's applied feed cursor: the
 *    highest `replica.transaction` it applied on that socket, or its previous
 *    watermark.
 *
 * As in the diff (README, "Normalization"), only sockets that subscribed to
 * something are checked: on a socket with no subscription a watermark only
 * completes calls. Rust sends such a socket a call's immediate watermark R and
 * then the feed's watermark of the previous commit R-1 (contract-changes 0046).
 */
import { expand } from './normalize.mjs';

const CURSOR_FRAMES = new Set(['replica.transaction', 'replica.snapshot', 'replica.snapshotPage', 'replica.delta', 'replica.ready']);

/** Committed revisions a frame shows. */
function revisionsOf(frame) {
  const out = [];
  if ((frame.type === 'reducer.result' || frame.type === 'action.result') && typeof frame.committedRevision === 'number') out.push(frame.committedRevision);
  if (CURSOR_FRAMES.has(frame.type) && typeof frame.cursor?.revision === 'number') out.push(frame.cursor.revision);
  if (frame.type === 'query.result' && frame.result?.found === true && typeof frame.result?.cursor?.revision === 'number') out.push(frame.result.cursor.revision);
  return out;
}

/**
 * @param {Array<{tag:string, socket:number, dir:'in'|'out', frame:any, step?:string}>} records every session's raw frames, in arrival order
 * @returns {{ checked: number, problems: string[] }}
 */
export function checkWatermarks(records) {
  // Highest revision shown to anyone by the end of each step (cross-socket arrival order is not contract).
  const steps = [];
  const stepIndex = new Map();
  for (const r of records) if (!stepIndex.has(r.step)) { stepIndex.set(r.step, steps.length); steps.push(0); }
  const expanded = records.map((r) => {
    const state = { bindings: new Map() };
    return { r, frames: r.dir === 'in' ? expandSafe(r.frame, state) : [] };
  });
  // Bindings are per socket: expand again in order with one state per socket for replica.batch.
  const bindings = new Map();
  for (const e of expanded) {
    if (e.r.dir !== 'in') continue;
    const key = `${e.r.tag}\u0000${e.r.socket}`;
    const state = bindings.get(key) ?? bindings.set(key, { bindings: new Map() }).get(key);
    e.frames = expandSafe(e.r.frame, state);
    const i = stepIndex.get(e.r.step);
    for (const f of e.frames) for (const v of revisionsOf(f)) steps[i] = Math.max(steps[i], v);
  }
  for (let i = 1; i < steps.length; i++) steps[i] = Math.max(steps[i], steps[i - 1]);
  const sockets = new Map();
  const problems = [];
  let checked = 0;
  for (const { r, frames } of expanded) {
    const key = `${r.tag}\u0000${r.socket}`;
    const s = sockets.get(key) ?? sockets.set(key, { last: -1, applied: 0, subscribed: false }).get(key);
    if (r.dir === 'out' && ['replica.open', 'replica.openMany', 'query.subscribe', 'query.subscribeMany'].includes(r.frame?.type)) s.subscribed = true;
    for (const f of frames) {
      if (!f || typeof f !== 'object') continue;
      if (f.type === 'auth.result' && f.result?.subscriptionsPreserved !== true) { s.last = -1; s.applied = 0; }
      if (f.type === 'auth.error' && (f.id === 'membership-changed' || f.id === 'session-expired')) { s.last = -1; s.applied = 0; }
      if (f.type === 'replica.transaction' && typeof f.cursor?.revision === 'number') s.applied = Math.max(s.applied, f.cursor.revision);
      if (f.type !== 'replica.watermark' || typeof f.revision !== 'number' || !s.subscribed) continue;
      checked++;
      const w = f.revision;
      const committed = steps[stepIndex.get(r.step)] ?? 0;
      if (w < s.last) problems.push(`session ${r.tag}: watermark ${w} after ${s.last} (went down)`);
      if (w > committed) problems.push(`session ${r.tag}: watermark ${w} beyond the highest committed revision shown by then (${committed})`);
      if (w < s.applied) problems.push(`session ${r.tag}: watermark ${w} below the applied feed cursor ${s.applied}`);
      s.last = Math.max(s.last, w);
      s.applied = Math.max(s.applied, w);
    }
  }
  return { checked, problems };
}

function expandSafe(frame, state) {
  try { return expand(frame, state); } catch { return [frame]; }
}
