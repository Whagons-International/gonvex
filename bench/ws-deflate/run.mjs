import { fixture } from './fixture.mjs';
import { stats, ratioCI } from './stats.mjs';
import { chromium } from '@playwright/test';
import WebSocket from 'ws';
import { spawn } from 'node:child_process';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir, cpus } from 'node:os';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { once } from 'node:events';

const samples = 30;
const cpuOnly = process.env.BENCH_CPU_ONLY === '1';
const dir = await mkdtemp(join(tmpdir(), 'ws-deflate-'));
const fixturePath = join(dir, 'fixture.json');
const cases = Object.fromEntries(Object.entries(fixture()).map(([name, frames]) => [name, frames.map(JSON.stringify)]));
await writeFile(fixturePath, JSON.stringify(cases));
const binary = process.env.BENCH_BINARY ?? 'rust/target-websocket/release/examples/ws_deflate';
const active = new Set();
async function server() {
  const proc = spawn(binary, [fixturePath], { stdio: ['ignore', 'pipe', 'inherit'] });
  active.add(proc);
  const address = await new Promise((resolve, reject) => {
    proc.once('error', reject);
    proc.stdout.once('data', data => resolve(String(data).trim()));
    proc.once('exit', code => reject(new Error(`server exited ${code}`)));
  });
  return { proc, url: `ws://${address}/ws` };
}
async function stop(proc) { proc.kill(); await once(proc, 'exit'); active.delete(proc); }
async function rss(pid) { return Number((await readFile(`/proc/${pid}/status`, 'utf8')).match(/VmRSS:\s+(\d+)/)[1]) * 1024; }
async function cpu(pid) { return Number((await readFile(`/proc/${pid}/schedstat`, 'utf8')).split(' ')[0]) / 1e6; }
async function socket(url, compression) {
  const ws = new WebSocket(`${url}?compression=${Number(compression)}`, { perMessageDeflate: compression });
  await once(ws, 'open');
  return ws;
}
async function transfer(ws, kind) {
  const before = ws._socket.bytesRead;
  let count = 0;
  await new Promise((resolve, reject) => {
    function receive(data) {
      if (String(data) === 'done') { ws.off('message', receive); ws.off('error', reject); resolve(); }
      else { JSON.parse(String(data)); count++; }
    }
    ws.on('message', receive);
    ws.once('error', reject);
    ws.send(kind);
  });
  if (count !== cases[kind].length) throw new Error('frame count mismatch');
  return ws._socket.bytesRead - before;
}

const results = { samples, timestamp: new Date().toISOString(), node: process.version, cpu: cpus()[0].model, binarySha256: createHash('sha256').update(await readFile(binary)).digest('hex'), configuration: { thresholdBytes: 1024, serverWindowBits: 12, level: Number(process.env.BENCH_LEVEL ?? 1), serverNoContextTakeover: true, clientNoContextTakeover: true }, cases: {}, memory: {} };
try {
  // Separate fresh processes avoid allocator high-water marks from an earlier mode.
  for (const compression of cpuOnly ? [] : [false, true]) {
    const { proc, url } = await server();
    const before = await rss(proc.pid);
    const sockets = [];
    for (let i = 0; i < 1000; i++) sockets.push(await socket(url, compression));
    const after = await rss(proc.pid);
    console.error(`memory compression=${compression}: ${after - before} bytes / 1000 sockets`);
    results.memory[compression ? 'compressed' : 'plain'] = { n: 1000, baselineRss: before, idleRss: after, deltaRss: after - before, bytesPerSocket: (after - before) / 1000 };
    for (const ws of sockets) ws.terminate();
    await stop(proc);
  }
  const { proc, url } = await server();
  for (const [kind, frames] of Object.entries(cases)) {
    const rawBytes = frames.reduce((n, f) => n + Buffer.byteLength(f), 0);
    const readings = { plain: [], compressed: [] };
    const wires = {};
    const sockets = [await socket(url, false), await socket(url, true)];
    for (let i = -3; i < samples; i++) {
      for (const index of i % 2 === 0 ? [0, 1] : [1, 0]) {
        const before = await cpu(proc.pid);
        wires[index] = await transfer(sockets[index], kind);
        const elapsed = await cpu(proc.pid) - before;
        if (i >= 0) readings[index ? 'compressed' : 'plain'].push(elapsed / (rawBytes / 1e6));
      }
    }
    console.error(`wire/cpu ${kind} complete`);
    results.cases[kind] = { jsonPayloadBytes: rawBytes, wireBytes: { plain: wires[0], compressed: wires[1] }, savingsPercent: 100 * (1 - wires[1] / wires[0]), rawServerCpuSamplesMsPerMB: readings, serverCpuMsPerMB: Object.fromEntries(Object.entries(readings).map(([k, v]) => [k, stats(v)])) };
    for (const ws of sockets) ws.terminate();
  }
  if (!cpuOnly) {
    const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
    results.chromium = browser.version();
    const page = await browser.newPage();
    const timings = await page.evaluate(async ({ url, samples }) => {
      const sockets = await Promise.all([0, 1].map(mode => new Promise((resolve, reject) => {
        const ws = new WebSocket(`${url}?compression=${mode}`);
        ws.onopen = () => resolve(ws); ws.onerror = reject;
      })));
      const all = {};
      for (const kind of ['snapshot', 'transaction', 'delta']) {
        const values = [[], []];
        for (let i = -3; i < samples; i++) {
          for (const index of i % 2 === 0 ? [0, 1] : [1, 0]) {
            const ws = sockets[index];
            const elapsed = await new Promise((resolve, reject) => {
              let rows = 0;
              const start = performance.now();
              ws.onmessage = event => {
                if (event.data === 'done') {
                  const expected = kind === 'snapshot' ? 20000 : kind === 'transaction' ? 1000 : 1;
                  if (rows !== expected) reject(new Error(`rows ${rows} != ${expected}`));
                  else resolve(performance.now() - start);
                } else {
                  const frame = JSON.parse(event.data);
                  rows += kind === 'snapshot' ? frame.result.length : kind === 'transaction' ? frame.changes.length : frame.upserts.length;
                }
              };
              ws.onerror = reject; ws.send(kind);
            });
            if (i >= 0) values[index].push(elapsed);
          }
        }
        all[kind] = values;
      }
      for (const ws of sockets) ws.close();
      return all;
    }, { url, samples });
    for (const [kind, values] of Object.entries(timings)) {
      results.cases[kind].chromiumReceiveDecodeMs = { plain: stats(values[0]), compressed: stats(values[1]), pairedDifferenceMs: stats(values[1].map((v, i) => v - values[0][i])), increasePercent: ratioCI(values[0], values[1]) };
      results.cases[kind].rawBrowserSamplesMs = values;
    }
    const snapshot = results.cases.snapshot;
    results.gatePassed = snapshot.savingsPercent >= 50 && snapshot.chromiumReceiveDecodeMs.increasePercent.ci95[1] <= 10;
    await browser.close();
  }
  await stop(proc);
  await writeFile(process.env.BENCH_OUTPUT ?? 'bench/ws-deflate/results.json', JSON.stringify(results, null, 2) + '\n');
  if (!cpuOnly && !results.gatePassed) process.exitCode = 1;
  console.log(JSON.stringify(results, (key, value) => key.startsWith('raw') ? undefined : value, 2));
} finally {
  for (const proc of active) proc.kill();
  await rm(dir, { recursive: true, force: true });
}
