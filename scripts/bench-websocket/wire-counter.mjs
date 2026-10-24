import { createServer, connect } from 'node:net';

// Count actual server WebSocket frames on the TCP stream, after PMD encoding.
// Benchmark trailers, handshake headers and control frames are excluded.
export async function wireCounter(upstream) {
  const target = new URL(upstream);
  const totals = { frames: 0, payloadBytes: 0, wireBytes: 0 };
  const connections = new Set();
  const proxy = createServer(client => {
    const server = connect(Number(target.port), target.hostname);
    client.setNoDelay(true); server.setNoDelay(true);
    connections.add(client); connections.add(server);
    client.on('close', () => { connections.delete(client); server.destroy(); });
    server.on('close', () => { connections.delete(server); client.destroy(); });
    client.on('error', () => server.destroy()); server.on('error', () => client.destroy());
    client.pipe(server); server.pipe(client);
    let buffer = Buffer.alloc(0), headers = true;
    server.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      if (headers) {
        const end = buffer.indexOf('\r\n\r\n');
        if (end < 0) return;
        buffer = buffer.subarray(end + 4); headers = false;
      }
      while (buffer.length >= 2) {
        const opcode = buffer[0] & 15;
        let size = buffer[1] & 127, header = 2;
        if (size === 126) { if (buffer.length < 4) return; size = buffer.readUInt16BE(2); header = 4; }
        else if (size === 127) { if (buffer.length < 10) return; size = Number(buffer.readBigUInt64BE(2)); header = 10; }
        if (buffer.length < header + size) return;
        const payload = buffer.subarray(header, header + size);
        const trailer = opcode === 1 && !(buffer[0] & 64) && payload.includes(Buffer.from('"bench.done"'));
        if ((opcode === 1 || opcode === 2) && !trailer) {
          totals.frames++; totals.payloadBytes += size; totals.wireBytes += header + size;
        }
        buffer = buffer.subarray(header + size);
      }
    });
  });
  await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve));
  return { url: `ws://127.0.0.1:${proxy.address().port}`, totals, close: () => { for (const socket of connections) socket.destroy(); proxy.close(); } };
}
