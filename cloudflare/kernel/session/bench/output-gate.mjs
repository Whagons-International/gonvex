#!/usr/bin/env node
/**
 * X13e: does a Durable Object's storage write hold its outgoing WebSocket sends (the output gate)?
 * A local miniflare DO answers each client frame with one send, after: nothing; a 1 MB put; the same put
 * with allowUnconfirmed; setAlarm; setAlarm with allowUnconfirmed; setAlarm to an unchanged time; the send
 * first and setAlarm after it; setAlarm deferred with setTimeout(0). Prints the client round trip p50/p90.
 * Run: node cloudflare/kernel/session/bench/output-gate.mjs (from the repository root, after install).
 * Recorded 2026-11-18 (arch3, miniflare from wrangler 4.155), p50 over 4 runs: none 0.3 ms; put 23-46;
 * put allowUnconfirmed 8-10; setAlarm 16-43; setAlarm allowUnconfirmed 16-43 (no effect on alarms here);
 * unchanged setAlarm 0.4-0.7; send then setAlarm 16-39; setTimeout(0)-deferred setAlarm 16-44. Any changing
 * alarm write in the event holds the send; an unchanged one does not.
 */
import { createRequire } from 'node:module';
const require = createRequire(new URL('../../../package.json', import.meta.url));
const { Miniflare } = require(createRequire(require.resolve('wrangler/package.json')).resolve('miniflare'));
const script = `
export class Gate { constructor(ctx){ this.ctx=ctx; }
  async fetch(req){ const p=new WebSocketPair(); this.ctx.acceptWebSocket(p[1]); return new Response(null,{status:101,webSocket:p[0]}); }
  async webSocketMessage(ws, msg){ const [mode,i]=String(msg).split(':'); const big='x'.repeat(1<<20);
    if(mode==='put') this.ctx.storage.put('k'+i,big);
    if(mode==='putU') this.ctx.storage.put('k'+i,big,{allowUnconfirmed:true});
    if(mode==='alarm') this.ctx.storage.setAlarm(Date.now()+600000+Number(i));
    if(mode==='alarmU') this.ctx.storage.setAlarm(Date.now()+600000+Number(i),{allowUnconfirmed:true});
    if(mode==='sql') this.ctx.storage.sql.exec("INSERT OR REPLACE INTO t VALUES (?,?)", Number(i), big);
    if(mode==='sendThenLater'){ ws.send(mode+':'+i); setTimeout(()=>this.ctx.storage.setAlarm(Date.now()+600000+Number(i)),0); return; }
    if(mode==='alarmSame') this.ctx.storage.setAlarm(1e15);
    if(mode==='sendThenAlarm'){ ws.send(mode+':'+i); this.ctx.storage.setAlarm(Date.now()+600000+Number(i)); return; }
    ws.send(mode+':'+i); }
  async alarm(){} }
export default { fetch(req,env){ return env.GATE.get(env.GATE.idFromName('g')).fetch(req); } };`;
const config = { name: 'g', compatibilityDate: '2026-10-23', manifest: { mainModule: 'worker.mjs', modules: { 'worker.mjs': { type: 'esm', contents: script } } },
  env: { GATE: { type: 'durable-object', worker: 'g', exportName: 'Gate' } }, exports: { Gate: { type: 'durable-object', storage: 'sqlite' } } };
const mf = new Miniflare({ workers: [{ config }] });
const res = await mf.dispatchFetch('http://x/', { headers: { upgrade: 'websocket' } });
const ws = res.webSocket; ws.accept();
const waiting = new Map(); ws.addEventListener('message', (e) => { const r = waiting.get(e.data); if (r) { waiting.delete(e.data); r(); } });
const once = (frame) => new Promise((r) => { waiting.set(frame, r); ws.send(frame); });
const out = {};
for (const mode of ['none', 'put', 'putU', 'alarm', 'alarmU', 'alarmSame', 'sendThenAlarm', 'sendThenLater', 'none']) {
  const t = []; for (let i = 0; i < 40; i++) { const s = performance.now(); await once(mode + ':' + i); t.push(performance.now() - s); }
  t.sort((a, b) => a - b); out[mode] = { p50: +t[20].toFixed(2), p90: +t[36].toFixed(2) };
}
console.log(JSON.stringify(out)); ws.close(); await mf.dispose();
