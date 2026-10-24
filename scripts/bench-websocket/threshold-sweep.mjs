import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const directory = resolve(process.argv[2] ?? 'scripts/bench-websocket/results');
const source = JSON.parse(await readFile(`${directory}/fixtures.json`, 'utf8'));
const small = source.fixtures.find(row => row.name === 'small-update');
const description = source.fixtures.find(row => row.name === 'large-update').before[0].description;
const fixtures = [];
// Cover both sides of the current 1 KiB per-message threshold.
for (const length of [160, 192, 256, 320, 384, 512, 768, 1024, 1536, 2048, 4096]) {
  const before = { ...small.before[0], description: description.slice(0, length) };
  const after = { ...before, status: 'done' };
  const baseline = structuredClone(small.variants.baseline);
  baseline[0].changes[0].oldValue = before;
  baseline[0].changes[0].newValue = after;
  baseline[1].upserts = [after];
  const compact = structuredClone(baseline);
  delete compact[0].changes[0].oldValue;
  fixtures.push({ name: String(length), before: [before], after: [after], variants: { baseline, 'no-old': compact } });
}
await writeFile(`${directory}/sweep-fixtures.json`, JSON.stringify({ training: source.training, fixtures }));
