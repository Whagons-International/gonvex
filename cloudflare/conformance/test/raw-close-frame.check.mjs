// Needs built packages and the ws dependency (like mutation.check.mjs): node --test test/raw-close-frame.check.mjs
// 0092: RawSocket `closeFrame` reports the server's close FRAME (code, reason, arrival) even while the
// connection teardown, and so the WHATWG/close event, is still pending, as on Cloudflare.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { createHash } from 'node:crypto';
import { RawSocket, Recorder } from '../src/sessions.mjs';

const frame = (opcode, payload) => {
  const body = Buffer.from(payload);
  return Buffer.concat([Buffer.from([0x80 | opcode, body.length]), body]); // server frames are unmasked; < 126 bytes
};
const closePayload = (code, reason) => Buffer.concat([Buffer.from([code >> 8, code & 0xff]), Buffer.from(reason)]);

/** Upgrades, sends one text frame, then a close frame after `delayMs`, and never closes the TCP connection. */
function stallingServer(delayMs) {
  const sockets = [];
  const server = createServer((socket) => {
    sockets.push(socket);
    socket.once('data', (head) => {
      const key = /sec-websocket-key: *(\S+)/i.exec(String(head))[1];
      const accept = createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
      socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
      socket.write(frame(0x1, '{"type":"session.ready"}'));
      server.sentCloseAt = undefined;
      setTimeout(() => { server.sentCloseAt = Date.now(); socket.write(frame(0x8, closePayload(1008, 'authentication deadline exceeded'))); }, delayMs);
    });
    socket.on('error', () => {});
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({ server, url: `ws://127.0.0.1:${server.address().port}/ws`, stop: () => { sockets.forEach((s) => s.destroy()); server.close(); } })));
}

test('the close frame is reported on arrival, with its code and reason, while the close event still waits', async () => {
  const { server, url, stop } = await stallingServer(300);
  const recorder = new Recorder();
  const socket = await new RawSocket(url, 'deadline', recorder, { closeFrame: true }).open();
  try {
    assert.deepEqual(await socket.next({ type: 'session.ready' }), { type: 'session.ready' });
    const close = await socket.whenCloseFrame(5_000);
    assert.equal(close.code, 1008);
    assert.equal(close.reason, 'authentication deadline exceeded');
    assert.ok(close.at - server.sentCloseAt < 250, `frame reported ${close.at - server.sentCloseAt} ms after it was sent`);
    assert.equal(socket.closeEvent, undefined, 'the connection is not torn down, so no close event yet');
    // Frames are recorded like every other socket's.
    assert.deepEqual(recorder.frames('deadline', { dir: 'in' }), [{ type: 'session.ready' }]);
  } finally { socket.close(); stop(); }
});

test('without a close frame, whenCloseFrame times out with undefined; plain sockets refuse it', async () => {
  const { url, stop } = await stallingServer(60_000);
  const socket = await new RawSocket(url, 'quiet', new Recorder(), { closeFrame: true }).open();
  try { assert.equal(await socket.whenCloseFrame(200), undefined); } finally { socket.close(); stop(); }
  await assert.rejects(new RawSocket('ws://127.0.0.1:1/ws', 'plain', new Recorder()).whenCloseFrame(1), /closeFrame: true/);
});
