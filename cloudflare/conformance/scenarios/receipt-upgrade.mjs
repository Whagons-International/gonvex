/**
 * Receipt case "upgrade" (sdk_module_upgrade, check_module_upgrade): a new
 * bundle generation is deployed through /dev/sync while clients are
 * connected. Generation 2 adds a column by migration, changes a Reducer and
 * changes the board collection's definition.
 *
 *  - connected sockets see the new generation on their next call;
 *  - existing rows take the migration default, new rows the v2 value;
 *  - a call pinned to the old artifact hash is refused as `update_required`;
 *  - receipts written under generation 1 still answer after the upgrade;
 *  - an SDK client on the v1 collection definition and one on v2 both
 *    converge on the rows the backend serves.
 */
import { watch, rowsWhen, project, rawReducer, rawQuery, RAW_CAPABILITIES } from './helpers.mjs';

export default {
  name: 'receipt-upgrade',
  description: 'Module upgrade via /dev/sync with connected clients: migration, new generation, stale artifact, receipts',
  tenants: { a: ['alice', 'bob'] },
  async run(ctx) {
    const { api } = ctx;
    const v2 = ctx.fixtures[2].api;
    let socket, token, v1Hash, sdk, view;
    await ctx.step('generation 1 serving', async () => {
      socket = await ctx.raw('client');
      token = (await ctx.rawLogin(socket, 'alice')).accessToken;
      const auth = await socket.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
      v1Hash = auth.artifactHash;
      ctx.expect(typeof v1Hash === 'string' && v1Hash.length > 0, 'auth.result carries the artifact hash');
      ctx.observe('generation', await rawReducer(socket, 'board.generation', {}));
      ctx.observe('add', await rawReducer(socket, 'board.add', { id: 'old', title: 'before upgrade', position: 1 }, { idempotencyKey: 'conformance-upgrade-key-0001', receiptPath: 'board.add' }));
      sdk = await ctx.sdk('sdk-v1', { member: 'bob' });
      view = watch(sdk, api.board.all);
      ctx.observe('sdkRows', project(await rowsWhen(ctx, view, rows => rows.length === 1, 'v1 snapshot'), ['id', 'title', 'position', 'bumps']));
    });
    await ctx.step('deploy generation 2', async () => {
      const r = await ctx.deploy(2);
      ctx.observe('deploy', { ok: r.ok, functionCount: r.functionCount });
    });
    await ctx.step('the connected socket runs generation 2', async () => {
      await ctx.until(async () => (await rawReducer(socket, 'board.generation', {})).result?.generation === 2, 'generation 2 active');
      ctx.observe('generation', await rawReducer(socket, 'board.generation', {}));
      ctx.observe('add', await rawReducer(socket, 'board.add', { id: 'new', title: 'after upgrade', position: 2 }));
    });
    await ctx.step('a call pinned to the generation 1 artifact', async () => {
      ctx.observe('stale', await rawReducer(socket, 'board.rename', { id: 'old', title: 'stale client' }, { artifactHash: v1Hash }));
    });
    await ctx.step('generation 1 receipts survive the upgrade', async () => {
      ctx.observe('receipt', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: 'conformance-upgrade-key-0001', receiptPath: 'board.add' }));
      ctx.observe('replay', await rawReducer(socket, 'board.add', { id: 'old', title: 'before upgrade', position: 1 }, { idempotencyKey: 'conformance-upgrade-key-0001', receiptPath: 'board.add' }));
    });
    await ctx.step('SDK clients on v1 and v2 definitions converge', async () => {
      ctx.observe('v1Rows', project(await rowsWhen(ctx, view, rows => rows.length === 2, 'v1 SDK sees the new row'), ['id', 'title', 'position', 'bumps']));
      const fresh = await ctx.sdk('sdk-v2', { member: 'bob' });
      const v2view = watch(fresh, v2.board.all);
      ctx.observe('v2Rows', project(await rowsWhen(ctx, v2view, rows => rows.length === 2, 'v2 snapshot'), ['id', 'title', 'position', 'bumps', 'label']));
    });
  },
};
