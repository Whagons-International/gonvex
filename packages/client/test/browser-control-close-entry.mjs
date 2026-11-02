import { GonvexClient } from '../src/index.ts';

const reference = { kind: 'query', scope: 'control', delivery: 'live', path: 'control.private' };
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const tick = () => new Promise(resolve => setTimeout(resolve, 0));
async function until(check) {
  const deadline = Date.now() + 5000;
  while (!check()) { if (Date.now() > deadline) throw new Error('native control-close fixture timed out'); await tick(); }
}
window.controlCloseOwnership = async ({ url, id, order }) => {
  const NativeSocket = window.WebSocket;
  const nativeConstructor = NativeSocket.toString().includes('[native code]');
  const trace = [], sdkClosed = deferred();
  let client, closeCode, rendered, armed = false, forced = false;
  class ObservedSocket extends NativeSocket {
    constructor(address) {
      super(address);
      const externalClose = event => {
        trace.push('external-close'); closeCode = event.code;
        if (order !== 'unchanged') client.setAuth({ token: 'bob-session', identity: { sub: 'bob', iss: 'issuer' } });
      };
      if (order.startsWith('after-sdk')) queueMicrotask(() => super.addEventListener('close', externalClose));
      else super.addEventListener('close', externalClose);
    }
    addEventListener(type, listener, options) {
      if (type !== 'close') return super.addEventListener(type, listener, options);
      return super.addEventListener(type, event => {
        trace.push('sdk-close');
        try { listener.call(this, event); } finally { sdkClosed.resolve(); }
      }, options);
    }
  }
  window.WebSocket = ObservedSocket;
  client = new GonvexClient(url, { role: 'control', project: 'project', tenant: 'tenant', token: 'alice-session',
    identity: { sub: 'alice', iss: 'issuer' }, errorReporting: false });
  const watch = client.watchControlQuery(reference, {}), observed = [];
  const stopReconnect = watch.onUpdate(() => {
    if (armed && !forced && order === 'after-sdk-reconnect' && watch.getSnapshot().result === undefined) {
      forced = true; trace.push('public-connect'); client.connect();
    }
  });
  const stop = watch.onUpdate(() => { rendered = watch.getSnapshot().result; if (armed && rendered === undefined) trace.push('clear-notification'); observed.push(rendered ?? null); });
  try {
    await until(() => rendered?.private === 'Alice control data'); await tick(); observed.length = 0;
    const snapshot = watch.getSnapshot(); armed = true;
    await window.controlCloseFixture({ id, action: 'disconnect' });
    await sdkClosed.promise; await tick();
    const afterClose = { rendered: rendered ?? null, snapshot: watch.getSnapshot().result ?? null,
      sameSnapshot: watch.getSnapshot() === snapshot, observed: [...observed], trace: [...trace], closeCode, forced };
    if (order !== 'unchanged') {
      await window.controlCloseFixture({ id, action: 'release-auth' });
      await until(() => rendered?.private === 'Bob control data');
    }
    return { order, nativeConstructor, afterClose, final: rendered ?? null };
  } finally {
    stopReconnect(); stop(); client.close(); window.WebSocket = NativeSocket;
    await window.controlCloseFixture({ id, action: 'cleanup' });
  }
};
