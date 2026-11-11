/**
 * Child process for the crash scenario: authenticates, sends one keyed
 * reducer.call, prints "sent" and waits to be SIGKILLed by the parent, so the
 * client dies without a close handshake and never reads the reply.
 * argv: wsUrl project tenant token path argsJson idempotencyKey
 */
const [wsUrl, project, tenant, token, path, argsJson, key] = process.argv.slice(2);
const ws = new WebSocket(wsUrl);
ws.addEventListener('open', () => ws.send(JSON.stringify({ type: 'auth', id: 'auth-1', project, tenant, token, clientContract: 1, capabilities: {} })));
ws.addEventListener('message', (event) => {
  const m = JSON.parse(String(event.data));
  if (m.type === 'auth.error') { console.error('auth.error ' + m.error); process.exit(3); }
  if (m.type === 'auth.result' && m.id === 'auth-1') {
    ws.send(JSON.stringify({ type: 'reducer.call', id: 'child-call-1', path, args: JSON.parse(argsJson), idempotencyKey: key, receiptPath: path }));
    console.log('sent');
  }
});
setInterval(() => {}, 1000);
