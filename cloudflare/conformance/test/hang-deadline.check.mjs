// Needs built packages (like raw-close-frame.check.mjs): node --test test/hang-deadline.check.mjs
// X03 run10 (2026-11-18): a raw socket that closed before opening left an await nothing could settle, and the
// suite hung with the lane lock held. Opening now fails on close or after a deadline; each scenario has one too.
process.env.GONVEX_CONFORMANCE_OPEN_MS = '300';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
const { RawSocket, Recorder } = await import('../src/sessions.mjs');
const { withDeadline, runScenario } = await import('../src/runner.mjs');

async function server(onUpgrade) {
  const s = createServer(); s.on('upgrade', onUpgrade);
  await new Promise(r => s.listen(0, '127.0.0.1', r));
  return { url: `ws://127.0.0.1:${s.address().port}/`, close: () => { s.closeAllConnections?.(); s.close(); } };
}

test('a raw socket whose upgrade never answers fails within the open deadline', async () => {
  const held = []; const s = await server((_req, socket) => { held.push(socket); });
  try {
    const started = Date.now();
    await assert.rejects(new RawSocket(s.url, 'hung', new Recorder()).open(), /did not open within 300 ms|closed before opening|failed to open/);
    assert.ok(Date.now() - started < 5_000);
  } finally { held.forEach(x => x.destroy()); s.close(); }
});

test('a raw socket whose connection drops during the upgrade rejects instead of hanging', async () => {
  const s = await server((_req, socket) => socket.destroy());
  try { await assert.rejects(new RawSocket(s.url, 'dropped', new Recorder()).open(), /closed before opening|failed to open|did not open/); }
  finally { s.close(); }
});

test('withDeadline passes results and errors through and rejects a hang with the message', async () => {
  assert.equal(await withDeadline(Promise.resolve(7), 1_000, () => 'x'), 7);
  await assert.rejects(withDeadline(Promise.reject(new Error('own')), 1_000, () => 'x'), /own/);
  await assert.rejects(withDeadline(new Promise(() => {}), 50, () => 'exceeded the 50 ms scenario deadline in step "demo accounts"'), /scenario deadline in step "demo accounts"/);
});

// Review on #498: after the deadline the scenario must stop acting on the shared backend before the next one starts.
function stubBackend(wsUrl) {
  return {
    name: 'stub', wsUrl, capabilities: {},
    createProject: async () => ({ projectId: 'p-' + Math.random().toString(36).slice(2) }),
    deploy: async () => ({ ok: true }), createTenant: async () => {}, createMember: async () => ({ accountId: 'a', memberId: 'm' }),
    readTable: async () => [],
  };
}
const fixtures = { 1: { manifest: {}, api: {} } };

test('a scenario past its deadline is aborted: its socket wait rejects, no later step runs, and it settles', async () => {
  // Opens the WebSocket (101) and then never sends a frame, so the step waits on a reply forever.
  const { createHash } = await import('node:crypto');
  const held = [];
  const s = await server((req, socket) => {
    held.push(socket);
    const accept = createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  });
  const ran = [];
  let settled = false;
  const scenario = { name: 'hang-socket', tenants: {}, tables: [], async run(ctx) {
    try {
      await ctx.step('waits on a reply', async () => { const r = await ctx.raw('r'); ran.push('opened'); await r.next({ type: 'never' }, { timeoutMs: 60_000 }); });
      await ctx.step('after the hang', async () => { ran.push('after'); });
    } finally { settled = true; }
  } };
  try {
    const started = Date.now();
    const trace = await runScenario({ scenario, backend: stubBackend(s.url), fixtures, scenarioMs: 400, graceMs: 5_000 });
    assert.ok(Date.now() - started < 5_000, 'the run returned promptly');
    assert.equal(settled, true, 'scenario.run had settled before runScenario returned');
    // A scenario left running would reach its next step against the backend once its socket wait failed.
    await new Promise(r => setTimeout(r, 1_000));
    assert.deepEqual(ran, ['opened'], 'no step ran after the abort');
    assert.equal(trace.runInvalid, undefined);
    assert.ok(trace.failures.some(f => /scenario aborted: .*exceeded the (400|<n>) ms scenario deadline in step "waits on a reply" on stub/.test(f)), trace.failures.join('\n'));
  } finally { held.forEach(x => x.destroy()); s.close(); }
});

test('a fetch a scenario started is aborted at its deadline', async () => {
  const pending = [];
  const http = createServer((_req, res) => pending.push(res)); // never answers
  await new Promise(r => http.listen(0, '127.0.0.1', r));
  let outcome;
  const scenario = { name: 'hang-fetch', tenants: {}, tables: [], async run(ctx) {
    await ctx.step('fetch', async () => {
      try { await fetch(`http://127.0.0.1:${http.address().port}/`); outcome = 'answered'; } catch (e) { outcome = e.message; throw e; }
    });
  } };
  try {
    const trace = await runScenario({ scenario, backend: stubBackend('ws://127.0.0.1:1/'), fixtures, scenarioMs: 300, graceMs: 5_000 });
    // Aborted with the scenario's deadline as the reason, not answered.
    assert.match(outcome, /scenario deadline in step "fetch"/);
    assert.equal(trace.runInvalid, undefined);
    // Outside a scenario fetch is unchanged.
    assert.equal(globalThis.fetch.name, 'scopedFetch');
  } finally { pending.forEach(r => r.destroy()); http.closeAllConnections?.(); http.close(); }
});

test('a scenario that ignores the abort marks the run invalid', async () => {
  const scenario = { name: 'stubborn', tenants: {}, tables: [], async run() { await new Promise(r => setTimeout(r, 3_000)); } };
  const trace = await runScenario({ scenario, backend: stubBackend('ws://127.0.0.1:1/'), fixtures, scenarioMs: 200, graceMs: 300 });
  assert.match(trace.runInvalid, /stubborn on stub still ran 300 ms after it was aborted/);
  assert.ok(trace.failures.some(f => /scenario deadline/.test(f)));
});
