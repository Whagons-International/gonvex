/**
 * Harness self-test backend: the Rust runtime behind a WebSocket proxy that
 * rewrites one kind of server frame. A run of `rust` against `mutant` must
 * fail with a readable diff; if it passes, the harness has gone blind.
 * Also a worked example of plugging in a backend from a module path:
 *
 *   GONVEX_CONFORMANCE_MUTATION=reducer-error-class \
 *     node src/cli.mjs --backend rust=rust --backend mutant=src/backends/mutant.mjs
 *
 * Mutations:
 *   reducer-error-class  reducer.error class "rejected" becomes "transient"
 *   row-value            row titles in replica.transaction/delta get a "!" suffix
 *   drop-watermarks      replica.watermark frames (and batch watermarks) vanish
 */
import { WebSocketServer, WebSocket as WsClient } from 'ws';
import { RustBackend } from './rust.mjs';

const MUTATIONS = {
  'reducer-error-class': (m) => (m.type === 'reducer.error' && m.class === 'rejected' ? { ...m, class: 'transient', retryable: true } : m),
  'row-value': (m) => {
    const bang = (row) => (row && typeof row.title === 'string' ? { ...row, title: row.title + '!' } : row);
    if (m.type === 'replica.transaction') return { ...m, changes: m.changes.map(c => ({ ...c, newValue: bang(c.newValue) })) };
    if (m.type === 'replica.delta') return { ...m, upserts: m.upserts?.map(bang) };
    if (m.type === 'replica.batch') return { ...m, items: m.items.map(i => MUTATIONS['row-value'](i)) };
    return m;
  },
  'drop-watermarks': (m) => {
    if (m.type === 'replica.watermark') return undefined;
    if (m.type === 'replica.batch' && 'watermark' in m) { const { watermark, ...rest } = m; return rest; }
    return m;
  },
};

export default class MutantBackend extends RustBackend {
  constructor(options) {
    super(options);
    this.mutation = process.env.GONVEX_CONFORMANCE_MUTATION ?? 'reducer-error-class';
    if (!MUTATIONS[this.mutation]) throw new Error(`unknown mutation ${this.mutation} (known: ${Object.keys(MUTATIONS).join(', ')})`);
  }

  async start() {
    await super.start();
    const upstream = this.wsUrl;
    const mutate = MUTATIONS[this.mutation];
    this.proxy = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await new Promise(resolve => this.proxy.once('listening', resolve));
    this.proxy.on('connection', (client, request) => {
      const server = new WsClient(upstream + (request.url?.includes('?') ? request.url.slice(request.url.indexOf('?')) : ''));
      const queue = [];
      server.on('open', () => { for (const m of queue.splice(0)) server.send(m); });
      client.on('message', (data, binary) => { const m = binary ? data : data.toString(); if (server.readyState === 1) server.send(m); else queue.push(m); });
      server.on('message', (data, binary) => {
        if (binary) return client.send(data);
        const out = mutate(JSON.parse(data.toString()));
        if (out !== undefined) client.send(JSON.stringify(out));
      });
      client.on('close', () => server.close());
      server.on('close', () => client.close());
      client.on('error', () => {}); server.on('error', () => client.terminate());
    });
    this.wsUrl = `ws://127.0.0.1:${this.proxy.address().port}/ws`;
  }

  async stop() {
    for (const c of this.proxy?.clients ?? []) c.terminate();
    await new Promise(resolve => (this.proxy ? this.proxy.close(resolve) : resolve()));
    await super.stop();
  }
}
