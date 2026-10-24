import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const directory = resolve(process.argv[2] ?? 'scripts/bench-websocket/results');
const rust = JSON.parse(await readFile(`${directory}/rust.json`, 'utf8'));
const browser = JSON.parse(await readFile(`${directory}/browser.json`, 'utf8'));
assert.equal(browser.complete, true);
for (const result of browser.pipeline) {
  if (result.storage === 'indexeddb') assert.equal(result.independentBrowserContext, true,
    'Durable trials must use isolated browser contexts and the fresh database checks');
  const matrix = rust.matrix.find(row => ['workload', 'variant', 'codec', 'compression'].every(key => row[key] === result[key]));
  assert.ok(matrix, 'matching Rust compression case');
  assert.equal(result.actualWirePerTrial.frames, matrix.frames);
  assert.equal(result.actualWirePerTrial.wireBytes, matrix.serverWebSocketBytes,
    `${result.workload}/${result.variant}/${result.codec}/${result.compression}: actual TCP frame bytes differ from matrix`);
  assert.equal(result.actualWirePerTrial.payloadBytes, matrix.compressedPayloadBytes);
  for (const key of ['latencyMs', 'decodeUs', 'applyUs']) {
    assert.ok(result[key].p50 >= 0 && result[key].p50 <= result[key].p95 && result[key].p95 <= result[key].p99);
  }
}
console.log(`Verified actual TCP bytes against the Rust matrix for ${browser.pipeline.length} browser/storage cases`);
