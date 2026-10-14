import WebSocket from 'ws';
for (const host of ['gonvex-stg.whagons.com', 'gonvex.whagons.com']) {
  await new Promise((resolve, reject) => {
    const ws = new WebSocket(`wss://${host}/ws`, { perMessageDeflate: true, handshakeTimeout: 15000 });
    let upgraded = false;
    ws.on('upgrade', response => {
      upgraded = true;
      console.log(JSON.stringify({ timestamp: new Date().toISOString(), host, status: response.statusCode, extensions: response.headers['sec-websocket-extensions'] ?? null }));
    });
    ws.on('open', () => { ws.terminate(); resolve(); });
    ws.on('error', error => { if (upgraded) resolve(); else reject(error); });
  });
}
