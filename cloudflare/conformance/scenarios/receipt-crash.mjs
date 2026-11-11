/**
 * Receipt case "crash" (sdk_process_crash and check_restart):
 *  - the client process is SIGKILLed right after sending a keyed Reducer call;
 *    a new process replays the key and the call applies exactly once;
 *  - the backend is killed without shutdown after a commit; after restart the
 *    commit, its receipt and the Replica are intact, and a connected SDK
 *    client recovers on its own;
 *  - the backend is killed right after a keyed call is sent; the replay after
 *    restart applies exactly once whether or not the first attempt committed.
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { watch, rowsWhen, project, rawReducer, rawQuery, RAW_CAPABILITIES } from './helpers.mjs';

const CHILD = fileURLToPath(new URL('./children/send-and-die.mjs', import.meta.url));
const COLUMNS = ['id', 'title', 'position', 'bumps'];

export default {
  name: 'receipt-crash',
  description: 'Client process crash and backend crash (SIGKILL) around keyed Reducer calls',
  tenants: { a: ['alice', 'bob'] },
  requires: ['crash'],
  async run(ctx) {
    const { api } = ctx;
    let socket, token;
    const connect = async () => {
      socket = await ctx.raw('client');
      if (!token) token = (await ctx.rawLogin(socket, 'alice')).accessToken;
      await socket.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
    };
    await ctx.step('seed a counter', async () => {
      await connect();
      ctx.observe('add', await rawReducer(socket, 'board.add', { id: 'counter', title: 'counter', position: 1 }));
    });
    await ctx.step('client process dies right after sending a keyed call', async () => {
      const child = spawn(process.execPath, [CHILD, ctx.wsUrl, ctx.project, ctx.tenants.a, token, 'board.bump', JSON.stringify({ id: 'counter' }), 'conformance-crash-key-0001'], { stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '', err = '';
      child.stdout.on('data', d => { out += d; }); child.stderr.on('data', d => { err += d; });
      const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
      await ctx.until(() => out.includes('sent') || child.exitCode !== null, 'child sent its call');
      ctx.expect(out.includes('sent'), `child did not send its call: ${err}`);
      child.kill('SIGKILL');
      ctx.observe('childExit', (await exited).signal);
    });
    await ctx.step('a new client replays the key', async () => {
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0001', receiptPath: 'board.bump' }));
    });
    let sdk, view;
    await ctx.step('an SDK client watches the board', async () => {
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      view = watch(sdk, api.board.all);
      ctx.observe('rows', project(await rowsWhen(ctx, view, rows => rows.length === 1, 'sdk snapshot'), COLUMNS));
    });
    // The SDK reconnects on its own backoff; wait for that reconnect, not only
    // for its rows (they may have arrived before the kill).
    let connections;
    const reconnected = () => sdk.connectionState().connectionCount > connections && sdk.connectionState().isWebSocketConnected && view.ready();
    await ctx.step('commit, then the backend is killed', async () => {
      ctx.observe('bump', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0002', receiptPath: 'board.bump' }));
      await rowsWhen(ctx, view, rows => rows[0]?.bumps === 2, 'sdk sees the commit before the kill');
      connections = sdk.connectionState().connectionCount;
      await ctx.restart({ crash: true });
      // Inside this step, so the reconnect's frames always land here.
      await ctx.until(reconnected, 'sdk reconnected after restart', 30_000);
    });
    await ctx.step('after restart: replay, receipt and replica', async () => {
      await connect();
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0002', receiptPath: 'board.bump' }));
      ctx.observe('receipt', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: 'conformance-crash-key-0002', receiptPath: 'board.bump' }));
      ctx.observe('sdkRows', project(await rowsWhen(ctx, view, rows => rows[0]?.bumps === 2, 'sdk recovered after restart'), COLUMNS));
    });
    await ctx.step('backend killed right after a keyed call is sent', async () => {
      socket.send({ type: 'reducer.call', id: 'conformance-in-flight-0001', path: 'board.bump', args: { id: 'counter' }, idempotencyKey: 'conformance-crash-key-0003', receiptPath: 'board.bump' });
      socket.drop(); // the reply, if any, is never read
      connections = sdk.connectionState().connectionCount;
      await ctx.restart({ crash: true });
      await ctx.until(reconnected, 'sdk reconnected after second restart', 30_000);
    });
    await ctx.step('replay after the second restart applies once', async () => {
      await connect();
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0003', receiptPath: 'board.bump' }));
      ctx.observe('sdkRows', project(await rowsWhen(ctx, view, rows => rows[0]?.bumps === 3, 'sdk recovered after second restart'), COLUMNS));
    });
  },
};
