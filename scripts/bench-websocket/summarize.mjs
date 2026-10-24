import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const directory = resolve(process.argv[2] ?? 'scripts/bench-websocket/results');
const browser = JSON.parse(await readFile(`${directory}/browser.json`, 'utf8'));
const rust = JSON.parse(await readFile(`${directory}/rust.json`, 'utf8'));
if (!browser.complete) throw new Error('Browser run is incomplete');
const lines = ['# Browser transport and storage measurements', '',
  'Synthetic single-client loopback trials on a shared workstation. SQL, authentication, SDK dispatch, digest verification and TLS/WAN are excluded. Native PMD decompression is included in latency; explicit decoder time cannot separate it from browser internals. The TCP proxy adds measurement overhead. Samples are sequential, not a saturation throughput test.', '',
  'Small/100-row updates have 100 samples. The expensive 1,000-row durable cases have 30 samples; memory cases retained their original 100. Memory snapshots have eight samples; the expensive IndexedDB snapshots have three. They span runs, so only their medians are shown. p99 is omitted below 100 samples. Host load and GC/storage variability prevent treating these numbers as production latency estimates. The network byte totals are verified against actual TCP frames.', ''];
for (const storage of ['memory', 'indexeddb']) {
  lines.push(`Storage: ${storage}`, '', '| Workload | Variant | Codec/compression | WS bytes | p50 ms | p95 ms | p99 ms | Serial rows/s | Sampled peak MiB | Post-GC MiB |',
    '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const row of browser.pipeline.filter(row => row.storage === storage)) {
    const matrix = rust.matrix.find(r => ['workload', 'variant', 'codec', 'compression'].every(key => r[key] === row[key]));
    const snapshot = row.workload.startsWith('snapshot');
    lines.push(`| ${row.workload} | ${row.variant} | ${row.codec}/${row.compression} | ${matrix.serverWebSocketBytes.toLocaleString('en-US')} | ${row.latencyMs.p50.toFixed(2)} | ${snapshot ? 'n/a' : row.latencyMs.p95.toFixed(2)} | ${snapshot || row.samples < 100 ? 'n/a' : row.latencyMs.p99.toFixed(2)} | ${row.updatesPerSecond.toFixed(0)} | ${(row.sampledPeakHeapBytes / 1048576).toFixed(1)} | ${(row.retainedHeapAfterGcBytes / 1048576).toFixed(1)} |`);
  }
  lines.push('');
}
lines.push('Peak heap is sampled at message boundaries and is a lower bound. Post-GC heap includes the retained fixture and benchmark code. Task/GC totals in browser.json include untimed seeding and database cleanup. Codec/compression costs interact with buffering, allocation and storage; isolated decoder speed is not an end-to-end ranking.', '');
await writeFile(`${directory}/summary.md`, lines.join('\n'));
