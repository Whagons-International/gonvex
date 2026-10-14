import WebSocket from 'ws';
import { spawn } from 'node:child_process';
import { readFile, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import { createServer } from 'node:net';
import { createHash } from 'node:crypto';
const binary = process.env.RUNTIME_BINARY ?? 'rust/target/debug/gonvex-runtime';
const trials = 5;
const count = 1000;
const rss = async pid => Number((await readFile(`/proc/${pid}/status`, 'utf8')).match(/VmRSS:\s+(\d+)/)[1]) * 1024;
const output = { timestamp: new Date().toISOString(), trials, socketsPerTrial: count, binarySha256: createHash('sha256').update(await readFile(binary)).digest('hex'), plain: [], compressed: [] };
for (let trial = 0; trial < trials; trial++) {
  for (const compression of trial % 2 === 0 ? [false, true] : [true, false]) {
    const portServer = createServer();
    portServer.listen(0, '127.0.0.1'); await once(portServer, 'listening');
    const port = portServer.address().port;
    await new Promise(resolve => portServer.close(resolve));
    const proc = spawn(binary, [], { stdio: ['ignore', 'ignore', 'pipe'], env: { ...process.env,
      DATABASE_URL: '', GONVEX_CONTROL_PLANE_DATABASE_URL: '', GONVEX_TENANT_DATABASE_URLS: '{}', GONVEX_PROJECT_DATABASE_URLS: '{}',
      GONVEX_ADDR: `127.0.0.1:${port}`, GONVEX_REQUIRE_AUTH: 'false', GONVEX_MODULE_HOST_ENABLED: 'false',
      GONVEX_WS_COMPRESSION: String(compression), GONVEX_PENDING_CONNECTIONS_PER_IP: '2000', GONVEX_AUTH_TIMEOUT_SECONDS: '3600',
    }});
    let stderr = ''; proc.stderr.on('data', data => { stderr += data; });
    const sockets = [];
    try {
      for (let attempt = 0; ; attempt++) {
        if (proc.exitCode !== null) throw new Error(`runtime exit ${proc.exitCode}: ${stderr}`);
        try { const response = await fetch(`http://127.0.0.1:${port}/healthz`); if (response.ok) break; } catch {}
        if (attempt >= 100) throw new Error('runtime not ready');
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      const baselineRss = await rss(proc.pid);
      for (let i = 0; i < count; i++) {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, { perMessageDeflate: true });
        const ready = once(ws, 'message');
        await once(ws, 'open');
        const [frame] = await ready;
        if (JSON.parse(String(frame)).type !== 'session.ready') throw new Error('missing session.ready');
        if (Boolean(ws.extensions) !== compression) throw new Error('negotiation mismatch');
        sockets.push(ws);
      }
      const idleRss = await rss(proc.pid);
      const measurement = { baselineRss, idleRss, deltaRss: idleRss - baselineRss, bytesPerSocket: (idleRss - baselineRss) / count };
      output[compression ? 'compressed' : 'plain'].push(measurement);
      console.log(JSON.stringify({ trial, compression, ...measurement }));
    } finally {
      for (const ws of sockets) ws.terminate();
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill(); await once(proc, 'exit');
      }
    }
  }
}
function stats(values) {
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const sd = Math.sqrt(values.reduce((n, x) => n + (x - mean) ** 2, 0) / (values.length - 1));
  const margin = 2.77645 * sd / Math.sqrt(values.length);
  return { mean, ci95: [mean - margin, mean + margin] };
}
output.summary = { plainBytesPerSocket: stats(output.plain.map(x => x.bytesPerSocket)), compressedBytesPerSocket: stats(output.compressed.map(x => x.bytesPerSocket)), additionalBytesPerSocket: stats(output.compressed.map((x, i) => x.bytesPerSocket - output.plain[i].bytesPerSocket)) };
await writeFile('bench/ws-deflate/runtime-memory.json', JSON.stringify(output, null, 2) + '\n');
console.log(JSON.stringify(output.summary, null, 2));
