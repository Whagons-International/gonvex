// Client decode: JSON.parse of every snapshot page (+ restoreReplicaFrameNulls for omit pages).
import { readFileSync } from 'node:fs';
const [dir, protocolPath] = process.argv.slice(2);
const { restoreReplicaFrameNulls } = await import(protocolPath);
const legacy = JSON.parse(readFileSync(`${dir}/legacy.json`, 'utf8'));
const omit = JSON.parse(readFileSync(`${dir}/omit.json`, 'utf8'));
function run(pages, restore) {
  const columns = new Map();
  const started = performance.now();
  let rows = 0;
  for (const text of pages) {
    const message = JSON.parse(text);
    if (restore) restoreReplicaFrameNulls(message, columns);
    rows += message.result.length;
  }
  return [performance.now() - started, rows];
}
const median = (xs) => xs.sort((a, b) => a - b)[xs.length >> 1];
const out = {};
for (const [name, pages, restore] of [['legacy', legacy, false], ['omit_parse_only', omit, false], ['omit', omit, true], ['legacy', legacy, false], ['omit', omit, true]]) {
  const times = [];
  let rows = 0;
  for (let i = 0; i < 15; i++) { const [t, r] = run(pages, restore); times.push(t); rows = r; }
  out[name] = { ms: +median(times).toFixed(2), rows, bytes: pages.reduce((n, p) => n + p.length, 0) };
}
// Equality: restored rows equal legacy rows (text, key order included).
const flat = (pages, restore) => { const c = new Map(); return pages.flatMap(t => { const m = JSON.parse(t); if (restore) restoreReplicaFrameNulls(m, c); return m.result.map(r => JSON.stringify(r)); }); };
const a = flat(legacy, false), b = flat(omit, true);
out.identical = a.length === b.length && a.every((row, i) => row === b[i]);
// Heap of the parsed rows (retained objects), approximate.
console.log(JSON.stringify(out));
