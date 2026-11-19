// X17 step repro client: median round trip per mode over one WebSocket to a Durable Object.
const base = process.argv[2] || 'wss://gonvex-cf-dev-x17-repro.whagons-international.workers.dev/ws'; const N = +(process.argv[3] || 25);
const gw = 'step-gw-' + (process.argv[4] || 'a');
const ws = new WebSocket(`${base}?o=${gw}`); const waiters = []; ws.onmessage = () => waiters.shift()?.();
await new Promise((r, j) => { ws.onopen = r; ws.onerror = j; });
const rt = async (mode) => { const t = performance.now(); const p = new Promise(r => waiters.push(r)); ws.send(JSON.stringify({ mode, self: gw, peer: 'step-tenant-' + (process.argv[4] || 'a') })); await p; return performance.now() - t; };
for (const m of ['direct', 'hop', 'deliver']) await rt(m); // warm
const out = { at: new Date().toISOString() };
for (const m of ['direct', 'hop', 'deliver']) { const a = []; for (let i = 0; i < N; i++) { a.push(await rt(m)); await new Promise(r => setTimeout(r, 30)); } a.sort((x, y) => x - y); out[m] = +a[a.length >> 1].toFixed(1); }
console.log(JSON.stringify(out)); ws.close();
