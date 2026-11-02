import { GonvexClient } from '../src/index.ts';
import { DexieReducerOutbox } from '../src/outbox.ts';

window.intentRefreshOwnership = async (name, change) => {
  const url = 'ws://intent-browser.test/ws';
  const scope = ['identity', url, 'project', 'tenant', 'issuer', 'alice'].join('\0');
  const seed = new DexieReducerOutbox({ databaseName: name });
  const append = async path => {
    const entry = await seed.enqueue({ scope, path, args: {}, idempotencyKey: path });
    await seed.reject(entry.id, 'fixture rejection');
    return entry;
  };
  const originalEntry = await append('tasks.old');
  const client = new GonvexClient(url, { project: 'project', tenant: 'tenant',
    identity: { sub: 'alice', iss: 'issuer' }, errorReporting: false,
    outbox: { databaseName: name } });
  let entered, release;
  const held = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  let hold = false, stop = () => {};
  const observed = [];
  try {
    await client.listIntents();
    // Retain the completed native adapter read, rather than replacing storage
    // with a fixture result. The public observer is the publication boundary.
    const list = client.reducerOutbox.list.bind(client.reducerOutbox);
    client.reducerOutbox.list = async owner => {
      const entries = await list(owner);
      if (hold && owner === scope) { hold = false; entered(); await gate; }
      return entries;
    };
    hold = true;
    stop = client.subscribeIntents(() => observed.push(client.intentsSnapshot().map(row => row.reducer)));
    await held;
    if (change === 'account' || change === 'return') {
      client.setAuth({ identity: { sub: 'bob', iss: 'issuer' } });
      await client.localSessionReady;
      if (change === 'return') {
        await seed.discard(originalEntry.id, ['rejected']);
        await append('tasks.latest');
        client.setAuth({ identity: { sub: 'alice', iss: 'issuer' } });
        await client.localSessionReady;
      }
    } else if (change === 'tenant') {
      client.setAuth({ tenant: 'other' });
      await client.localSessionReady;
    } else if (change === 'close') client.close();
    observed.length = 0;
    release();
    while (client.intentsRefreshRunning) await new Promise(resolve => setTimeout(resolve, 0));
    const durable = await seed.list(scope);
    return { change, observed, final: client.intentsSnapshot().map(row => row.reducer),
      nativeReadCompletedBeforeRotation: true,
      durable: durable.map(row => ({ id: row.id, key: row.idempotencyKey, path: row.path, state: row.state })) };
  } finally { release(); stop(); client.close(); }
};
