// Needs built packages (like raw-close-frame.check.mjs): node --test test/hang-deadline.check.mjs
// X03 run10 (2026-11-18): a raw socket that closed before opening left an await nothing could settle, and the
// suite hung with the lane lock held. Opening now fails on close or after a deadline; each scenario has one too.
process.env.GONVEX_CONFORMANCE_OPEN_MS = '300';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
const { RawSocket, Recorder } = await import('../src/sessions.mjs');
const { withDeadline } = await import('../src/runner.mjs');

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
