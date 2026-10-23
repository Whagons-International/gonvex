import { readFile, writeFile } from 'node:fs/promises';
const result = JSON.parse(await readFile(new URL(process.argv[2] ?? 'results.json', import.meta.url)));
const quantile = (values, p) => { const sorted = values.filter(value => typeof value === "number" && Number.isFinite(value)).sort((a, b) => a - b); return sorted.length ? sorted[Math.ceil(p * sorted.length) - 1] : null; };
let seed = 0x109112;
const random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 2 ** 32; };
const bootstrap = (before, after) => {
  if (!before.length || !after.length) return null;
  const ratios = [];
  for (let iteration = 0; iteration < 10_000; iteration++) {
    const left = before.map(() => before[Math.floor(random() * before.length)]);
    const right = after.map(() => after[Math.floor(random() * after.length)]);
    ratios.push(quantile(right, .5) / quantile(left, .5));
  }
  return { ratio: quantile(after, .5) / quantile(before, .5), ci95: [quantile(ratios, .025), quantile(ratios, .975)] };
};
const pairedBootstrap = (workload, metric) => {
  const pairs = new Map();
  for (const sample of result.samples) {
    if (typeof sample[workload]?.[metric] !== 'number') continue;
    const pair = pairs.get(sample.index) ?? {}; pair[sample.mode] = sample[workload][metric]; pairs.set(sample.index, pair);
  }
  const ratios = [...pairs.values()].filter(pair => pair.legacy > 0 && pair.worker >= 0).map(pair => pair.worker / pair.legacy);
  if (!ratios.length) return null;
  const medians = Array.from({ length: 10_000 }, () => quantile(ratios.map(() => ratios[Math.floor(random() * ratios.length)]), .5));
  return { ratio: quantile(ratios, .5), ci95: [quantile(medians, .025), quantile(medians, .975)], pairs: ratios.length };
};
const summary = { metadata: result.metadata, samplesPerMode: result.samples.filter(sample => sample.mode === 'worker').length, metrics: {} };
for (const workload of ['cold', 'burst', 'snapshot', 'drain']) {
  const rows = {};
  const scriptTimingValid = workload !== 'cold' || result.metadata.coldSamplingAligned === true;
  for (const mode of ['legacy', 'worker']) {
    const samples = result.samples.filter(sample => sample.mode === mode && sample[workload]).map(sample => sample[workload]);
    rows[mode] = { wallMedianMs: quantile(samples.map(sample => sample.wallMs), .5), wallP95Ms: quantile(samples.map(sample => sample.wallMs), .95), scriptMedianMs: scriptTimingValid ? quantile(samples.map(sample => sample.scriptMs), .5) : null, longTasks: samples.flatMap(sample => sample.longTasks).length, maxLongTaskMs: Math.max(0, ...samples.flatMap(sample => sample.longTasks)), clickP95Ms: quantile(samples.flatMap(sample => sample.clicks), .95), frameP95Ms: quantile(samples.flatMap(sample => sample.frames), .95) };
    if (workload === 'drain') Object.assign(rows[mode], {
      intentLatencyMedianMs: quantile(samples.flatMap(sample => sample.admissionLatenciesMs ?? []), .5),
      intentLatencyP95Ms: quantile(samples.flatMap(sample => sample.admissionLatenciesMs ?? []), .95),
      admissionMedianMs: quantile(samples.map(sample => sample.admissionMs), .5),
      admissionP95Ms: quantile(samples.map(sample => sample.admissionMs), .95),
      postAdmissionMedianMs: quantile(samples.map(sample => sample.wallMs - sample.admissionMs), .5),
      // Aggregate average per round, not the p95 of individual command latency.
      averageAdmissionPerIntentMedianMs: quantile(samples.map(sample => sample.admissionMs / 500), .5),
    });
  }
  const before = result.samples.filter(sample => sample.mode === 'legacy' && sample[workload]).map(sample => sample[workload].scriptMs);
  const after = result.samples.filter(sample => sample.mode === 'worker' && sample[workload]).map(sample => sample[workload].scriptMs);
  summary.metrics[workload] = { ...rows, scriptTimingValid, scriptRatio: scriptTimingValid && before.every(value => typeof value === 'number') && after.every(value => typeof value === 'number') && before.length && after.length ? bootstrap(before, after) : null, pairedWallRatio: pairedBootstrap(workload, "wallMs"), pairedScriptRatio: scriptTimingValid ? pairedBootstrap(workload, "scriptMs") : null, wallRatio: bootstrap(result.samples.filter(sample => sample.mode === 'legacy' && sample[workload]).map(sample => sample[workload].wallMs), result.samples.filter(sample => sample.mode === 'worker' && sample[workload]).map(sample => sample[workload].wallMs)) };
}
summary.gate = { drain: summary.metrics.drain.pairedWallRatio?.ci95[1] <= 1 ? 'PASS' : 'FAIL', coldScript: summary.metrics.cold.pairedScriptRatio ? summary.metrics.cold.pairedScriptRatio.ci95[1] <= .6 ? 'PASS' : 'FAIL' : 'UNAVAILABLE', burst: result.samples.some(sample => sample.burst.longTaskSupported === false) ? 'UNAVAILABLE' : summary.metrics.burst.worker.maxLongTaskMs <= 50 ? 'PASS' : 'FAIL', correctness: result.metadata.correctness, multiTab: result.metadata.multiTab, maxLoadFraction: Math.max(...result.samples.flatMap(sample => (sample.loadChecks ?? [sample]).map(check => check.load[0]))) / result.metadata.cores };
await writeFile(new URL(process.argv[3] ?? 'summary.json', import.meta.url), JSON.stringify(summary, null, 2));
process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
