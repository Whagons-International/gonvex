// Sample RSS while 32 connections receive the same snapshot concurrently.
import { fixture } from './fixture.mjs';
import { stats } from './stats.mjs';
import WebSocket from 'ws';
import { spawn } from 'node:child_process';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
const binary = process.env.BENCH_BINARY ?? 'rust/target-websocket/release/examples/ws_deflate';
const dir = await mkdtemp(join(tmpdir(), 'ws-active-'));
const path = join(dir, 'fixture.json');
await writeFile(path, JSON.stringify(Object.fromEntries(Object.entries(fixture()).map(([k, frames]) => [k, frames.map(JSON.stringify)]))));
const count = 32;
const output = { timestamp: new Date().toISOString(), connections: count, trials: 5, sampleIntervalMs: 1, binarySha256: createHash('sha256').update(await readFile(binary)).digest('hex'), plain: [], compressed: [] };
const rss = async pid => Number((await readFile(`/proc/${pid}/status`, 'utf8')).match(/VmRSS:\s+(\d+)/)[1]) * 1024;
try {
  for (let trial = 0; trial < output.trials; trial++) {
    for (const compression of trial % 2 ? [true, false] : [false, true]) {
      const proc = spawn(binary, [path], { stdio: ['ignore', 'pipe', 'inherit'] });
      const sockets = [];
      try {
        const [address] = await once(proc.stdout, 'data');
        for (let i = 0; i < count; i++) {
          const ws = new WebSocket(`ws://${String(address).trim()}/ws?compression=${Number(compression)}`, { perMessageDeflate: compression });
          await once(ws, 'open'); sockets.push(ws);
        }
        const idleRss = await rss(proc.pid);
        let peakRss = idleRss;
        let samples = 0;
        let sampling = true;
        const sampler = (async () => {
          while (sampling) {
            peakRss = Math.max(peakRss, await rss(proc.pid)); samples++;
            await new Promise(resolve => setTimeout(resolve, 1));
          }
        })();
        await Promise.all(sockets.map(ws => new Promise((resolve, reject) => {
          let rows = 0;
          ws.on('message', data => {
            if (String(data) === 'done') {
              if (rows !== 20000) reject(new Error(`row count ${rows}`)); else resolve();
            } else rows += JSON.parse(String(data)).result.length;
          });
          ws.once('error', reject); ws.send('snapshot');
        })));
        sampling = false; await sampler;
        await new Promise(resolve => setTimeout(resolve, 100));
        const settledRss = await rss(proc.pid);
        const measurement = { idleRss, peakRss, settledRss, peakAboveIdle: peakRss - idleRss, retainedAboveIdle: settledRss - idleRss, samples };
        output[compression ? 'compressed' : 'plain'].push(measurement);
        console.log(JSON.stringify({ trial, compression, ...measurement }));
      } finally {
        for (const ws of sockets) ws.terminate();
        proc.kill(); await once(proc, 'exit');
      }
    }
  }
  output.summary = Object.fromEntries(['plain', 'compressed'].map(mode => [mode, { peakAboveIdle: stats(output[mode].map(x => x.peakAboveIdle)), retainedAboveIdle: stats(output[mode].map(x => x.retainedAboveIdle)) }]));
  await writeFile('bench/ws-deflate/active-memory.json', JSON.stringify(output, null, 2) + '\n');
} finally { await rm(dir, { recursive: true, force: true }); }
