/**
 * Receipt case "owner switch" (sdk_owner_switch, account and tenant
 * dimensions): one SDK client and one socket change the authenticated owner.
 *
 *  - account switch alice -> bob -> alice in tenant A: the owner-scoped notes
 *    collection follows the authenticated Member; alice's receipt is not
 *    visible to bob under the same key; bob cannot overwrite alice's note;
 *  - tenant switch A -> B -> A for alice: tenant B's rows replace A's and A's
 *    never leak into B.
 */
import { watch, call, rowsWhen, project, rawQuery, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const NOTE = ['id', 'owner_id', 'body', 'version'];
const BOARD = ['id', 'title', 'position', 'bumps'];

export default {
  name: 'receipt-owner-switch',
  description: 'Owner switch on one SDK socket: account A->B->A and tenant A->B->A, receipts per owner',
  tenants: { a: ['alice', 'bob'], b: ['alice'] },
  async run(ctx) {
    const { api } = ctx;
    let sdk, notes, board, aliceSession, bobSession;
    const key = 'conformance-owner-key-0001';
    await ctx.step('alice in tenant A', async () => {
      sdk = await ctx.sdk('sdk', { member: 'alice' });
      aliceSession = sdk.session;
      notes = watch(sdk, api.notes.mine);
      board = watch(sdk, api.board.all);
      ctx.observe('note', await call(sdk, api.notes.write, { id: 'n-alice', body: 'alice private' }));
      ctx.observe('board', await call(sdk, api.board.add, { id: 'a-row', title: 'tenant A row', position: 1 }));
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 1, 'alice note'), NOTE));
      ctx.observe('boardRows', project(await rowsWhen(ctx, board, rows => rows.length === 1, 'tenant A board'), BOARD));
    });
    await ctx.step('alice writes with an idempotency key (raw)', async () => {
      const raw = await ctx.raw('raw-alice');
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token: aliceSession.accessToken, capabilities: RAW_CAPABILITIES });
      ctx.observe('keyed', await rawReducer(raw, 'notes.write', { id: 'n-keyed', body: 'alice keyed' }, { idempotencyKey: key, receiptPath: 'notes.write' }));
      ctx.observe('receipt', await rawQuery(raw, '__gonvex.reducerReceipt', { idempotencyKey: key, receiptPath: 'notes.write' }));
      raw.close();
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 2, 'alice keyed note'), NOTE));
    });
    await ctx.step('switch the socket to bob', async () => {
      const login = await ctx.sdk('login-bob', { member: 'bob', authenticate: false, fresh: true });
      bobSession = login.session;
      login.close();
      sdk.setAuth({ project: ctx.project, token: bobSession.accessToken, tenant: ctx.tenants.a });
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 0, 'bob sees no notes'), NOTE));
      ctx.observe('boardRows', project(await rowsWhen(ctx, board, rows => rows.length === 1, 'bob sees the shared board'), BOARD));
    });
    await ctx.step('bob: same key, separate owner', async () => {
      const raw = await ctx.raw('raw-bob');
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token: bobSession.accessToken, capabilities: RAW_CAPABILITIES });
      ctx.observe('receiptForBob', await rawQuery(raw, '__gonvex.reducerReceipt', { idempotencyKey: key, receiptPath: 'notes.write' }));
      ctx.observe('sameKeyForBob', await rawReducer(raw, 'notes.write', { id: 'n-bob-keyed', body: 'bob keyed' }, { idempotencyKey: key, receiptPath: 'notes.write' }));
      raw.close();
      ctx.observe('overwriteAlice', await call(sdk, api.notes.write, { id: 'n-alice', body: 'bob overwrite attempt' }));
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 1, 'bob keyed note'), NOTE));
    });
    await ctx.step('switch back to alice', async () => {
      sdk.setAuth({ project: ctx.project, token: aliceSession.accessToken, tenant: ctx.tenants.a });
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 2 && rows.every(r => r.id === 'n-alice' || r.id === 'n-keyed'), 'alice notes again'), NOTE));
    });
    await ctx.step('tenant switch A -> B', async () => {
      sdk.setAuth({ project: ctx.project, token: aliceSession.accessToken, tenant: ctx.tenants.b });
      ctx.observe('boardRows', project(await rowsWhen(ctx, board, rows => rows.length === 0, 'tenant B board is empty'), BOARD));
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 0, 'tenant B notes are empty'), NOTE));
      ctx.observe('add', await call(sdk, api.board.add, { id: 'b-row', title: 'tenant B row', position: 1 }));
      ctx.observe('boardAfter', project(await rowsWhen(ctx, board, rows => rows.length === 1, 'tenant B row'), BOARD));
    });
    await ctx.step('tenant switch B -> A', async () => {
      sdk.setAuth({ project: ctx.project, token: aliceSession.accessToken, tenant: ctx.tenants.a });
      ctx.observe('boardRows', project(await rowsWhen(ctx, board, rows => rows.length === 1 && rows[0].id === 'a-row', 'tenant A board again'), BOARD));
      ctx.observe('notes', project(await rowsWhen(ctx, notes, rows => rows.length === 2, 'alice notes in A again'), NOTE));
    });
  },
};
