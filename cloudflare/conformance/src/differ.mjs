/**
 * Differ: compares the traces two backends produced for one scenario.
 *
 * A trace (see runner.mjs) has, per step: the step error (if any), the
 * scenario's recorded observations, and per session the normalized frames.
 * At the end it has per-table checksums of every tenant the scenario seeded.
 *
 * Output is a list of human-readable problem lines; empty means equal.
 */
import { canonical, frameText, policyOf } from './normalize.mjs';

const MAX_LINES_PER_SECTION = 12;
const clip = (s, n = 240) => (s.length > n ? s.slice(0, n) + '…' : s);

/** Deep structural diff of two JSON values; returns `path: a != b` lines. */
export function diffValues(a, b, path = '', out = []) {
  if (out.length >= MAX_LINES_PER_SECTION) return out;
  if (Object.is(a, b)) return out;
  const ta = Array.isArray(a) ? 'array' : a === null ? 'null' : typeof a;
  const tb = Array.isArray(b) ? 'array' : b === null ? 'null' : typeof b;
  if (ta !== tb || (ta !== 'object' && ta !== 'array')) {
    out.push(`${path || '<root>'}: ${clip(JSON.stringify(a) ?? 'undefined', 120)} != ${clip(JSON.stringify(b) ?? 'undefined', 120)}`);
    return out;
  }
  if (ta === 'array') {
    if (a.length !== b.length) out.push(`${path || '<root>'}: length ${a.length} != ${b.length}`);
    for (let i = 0; i < Math.min(a.length, b.length); i++) diffValues(a[i], b[i], `${path}[${i}]`, out);
    return out;
  }
  for (const k of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) diffValues(a[k], b[k], path ? `${path}.${k}` : k, out);
  return out;
}

/**
 * Compares the frames of one session in one step. `present`-class frames
 * (coalescible barriers such as watermarks, some sent on timers) are compared
 * here only when `withPresent`; diffTraces compares them over the whole trace.
 */
export function diffFrames(framesA, framesB, labels = ['A', 'B'], { withPresent = true } = {}) {
  const out = [];
  const bucket = (frames) => {
    const exact = new Map(), present = new Map();
    for (const { dir, frame } of frames) {
      const policy = policyOf(frame);
      if (policy === 'ignore') continue;
      if (policy === 'present') { present.set(`${dir} ${frame.type}`, true); continue; }
      const text = `${dir} ${frameText(frame)}`;
      exact.set(text, (exact.get(text) || 0) + 1);
    }
    return { exact, present };
  };
  const a = bucket(framesA), b = bucket(framesB);
  if (withPresent) for (const k of new Set([...a.present.keys(), ...b.present.keys()])) {
    if (!!a.present.get(k) !== !!b.present.get(k)) out.push(`  ${k}: ${labels[0]} ${a.present.get(k) ? 'sent' : 'none'}, ${labels[1]} ${b.present.get(k) ? 'sent' : 'none'}`);
  }
  const onlyA = [], onlyB = [];
  for (const k of new Set([...a.exact.keys(), ...b.exact.keys()])) {
    const ca = a.exact.get(k) || 0, cb = b.exact.get(k) || 0;
    if (ca > cb) onlyA.push([k, ca - cb]); else if (cb > ca) onlyB.push([k, cb - ca]);
  }
  // Pair frames of the same direction and type to show a field-level diff.
  const typeOf = (k) => k.split(' ')[0] + ' ' + (JSON.parse(k.slice(k.indexOf(' ') + 1)).type ?? '?');
  const used = new Set();
  for (const [ka, na] of onlyA) {
    const match = onlyB.find(([kb]) => !used.has(kb) && typeOf(kb) === typeOf(ka));
    if (match) {
      used.add(match[0]);
      const fa = JSON.parse(ka.slice(ka.indexOf(' ') + 1)), fb = JSON.parse(match[0].slice(match[0].indexOf(' ') + 1));
      out.push(`  ${typeOf(ka)} differs (${labels[0]} x${na}, ${labels[1]} x${match[1]}):`);
      for (const line of diffValues(fa, fb)) out.push(`    ${line}`);
    } else out.push(`  only ${labels[0]} x${na}: ${clip(ka)}`);
  }
  for (const [kb, nb] of onlyB) if (!used.has(kb)) out.push(`  only ${labels[1]} x${nb}: ${clip(kb)}`);
  return out.slice(0, MAX_LINES_PER_SECTION * 3);
}

/** Compares per-table checksums; rows (if present) give a row-level diff. */
export function diffChecksums(ca = {}, cb = {}, labels = ['A', 'B']) {
  const out = [];
  for (const tenant of [...new Set([...Object.keys(ca), ...Object.keys(cb)])].sort()) {
    const ta = ca[tenant] ?? {}, tb = cb[tenant] ?? {};
    for (const table of [...new Set([...Object.keys(ta), ...Object.keys(tb)])].sort()) {
      const x = ta[table], y = tb[table];
      if (!x || !y) { out.push(`  ${tenant}.${table}: only on ${x ? labels[0] : labels[1]}`); continue; }
      if (x.sha256 === y.sha256 && x.count === y.count) continue;
      out.push(`  ${tenant}.${table}: ${labels[0]} ${x.count} rows ${x.sha256.slice(0, 12)}, ${labels[1]} ${y.count} rows ${y.sha256.slice(0, 12)}`);
      if (x.rows && y.rows) {
        const key = (r) => JSON.stringify(r.__key ?? r.id ?? r._id ?? r);
        const mx = new Map(x.rows.map(r => [key(r), r])), my = new Map(y.rows.map(r => [key(r), r]));
        let shown = 0;
        for (const k of new Set([...mx.keys(), ...my.keys()])) {
          if (shown >= 6) { out.push('    …'); break; }
          const rx = mx.get(k), ry = my.get(k);
          if (!rx) { out.push(`    only ${labels[1]}: ${clip(JSON.stringify(ry))}`); shown++; }
          else if (!ry) { out.push(`    only ${labels[0]}: ${clip(JSON.stringify(rx))}`); shown++; }
          else if (JSON.stringify(canonical(rx)) !== JSON.stringify(canonical(ry))) { out.push(`    row ${k}: ${diffValues(rx, ry).join('; ')}`); shown++; }
        }
      }
    }
  }
  return out;
}

/**
 * Compares two scenario traces.
 * @returns {string[]} problem lines; empty when the traces agree.
 */
export function diffTraces(a, b) {
  const labels = [a.backend, b.backend];
  const out = [];
  const steps = Math.max(a.steps.length, b.steps.length);
  for (let i = 0; i < steps; i++) {
    const sa = a.steps[i], sb = b.steps[i];
    if (!sa || !sb) { out.push(`step ${i + 1} "${(sa ?? sb).name}" only ran on ${sa ? labels[0] : labels[1]}`); continue; }
    const section = [];
    if ((sa.error ?? '') !== (sb.error ?? '')) section.push(`  error: ${labels[0]} ${JSON.stringify(sa.error ?? null)}, ${labels[1]} ${JSON.stringify(sb.error ?? null)}`);
    for (const line of diffValues(sa.observations ?? {}, sb.observations ?? {})) section.push(`  observation ${line}`);
    for (const session of [...new Set([...Object.keys(sa.frames ?? {}), ...Object.keys(sb.frames ?? {})])].sort()) {
      const lines = diffFrames(sa.frames?.[session] ?? [], sb.frames?.[session] ?? [], labels, { withPresent: false });
      if (lines.length) section.push(`  frames of ${session}:`, ...lines.map(l => '  ' + l));
    }
    if (section.length) out.push(`step ${i + 1} "${sa.name}":`, ...section);
  }
  // Coalescible barriers: each session must receive (or send) them on both backends or on neither.
  const all = (trace) => { const m = {}; for (const st of trace.steps) for (const [tag, fr] of Object.entries(st.frames ?? {})) (m[tag] ??= []).push(...fr); return m; };
  const fa = all(a), fb = all(b), presence = [];
  const SUBSCRIBES = new Set(['replica.open', 'replica.openMany', 'query.subscribe', 'query.subscribeMany']);
  for (const tag of [...new Set([...Object.keys(fa), ...Object.keys(fb)])].sort()) {
    // A watermark certifies subscribed state. Sessions that never subscribed
    // may still get timer-driven ones (quiet-tenant ticks), so skip them there.
    const subscribed = [...(fa[tag] ?? []), ...(fb[tag] ?? [])].some(f => f.dir === 'out' && SUBSCRIBES.has(f.frame.type));
    const barriers = (frames) => (frames ?? []).filter(f => policyOf(f.frame) === 'present' && (subscribed || f.frame.type !== 'replica.watermark'));
    const lines = diffFrames(barriers(fa[tag]), barriers(fb[tag]), labels);
    if (lines.length) presence.push(`  ${tag}:`, ...lines.map(l => '  ' + l));
  }
  if (presence.length) out.push('barrier frames over the whole scenario:', ...presence);
  const sums = diffChecksums(a.checksums, b.checksums, labels);
  if (sums.length) out.push('table checksums:', ...sums);
  return out;
}
