/**
 * Receipt case "expired receipt" (intended behavior IB3, contract-changes
 * 0001 and 0009): a receipt's result expires after the client's offline
 * window plus 24 hours and leaves a tombstone. A late duplicate of the
 * committed intent is then refused instead of applied again, and the
 * receipt query answers the same; the tombstone goes 365 days later.
 *
 * Rust never prunes receipts, so only backends with `receiptExpiry` run it
 * (the kernel); it asserts the kernel's rule, not a comparison with Rust.
 * The adapter runs the tenant alarm's expiry at a chosen time, with a 7-day
 * window standing in for the fixture's (it declares none).
 */
import { rawReducer, rawQuery, RAW_CAPABILITIES } from './helpers.mjs';

const DAY = 24 * 60 * 60 * 1000;
const WINDOW = 7 * DAY;

export default {
  name: 'receipt-expiry',
  description: 'Receipts expire after the offline window + 24 h: a late duplicate is refused, then forgotten after 365 days (0009)',
  tenants: { a: ['alice'] },
  requires: ['receiptExpiry'],
  async run(ctx) {
    const key = 'conformance-expiry-key-0001';
    const start = Date.now();
    let socket;
    await ctx.step('a keyed call commits and replays', async () => {
      socket = await ctx.raw('client');
      const token = (await ctx.rawLogin(socket, 'alice')).accessToken;
      await socket.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
      ctx.observe('add', await rawReducer(socket, 'board.add', { id: 'counter', title: 'counter', position: 1 }));
      ctx.observe('bump', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
    });
    await ctx.step('inside the window plus 24 h the receipt is kept', async () => {
      const pruned = await ctx.backend.expireReceipts(ctx.project, ctx.tenants.a, { now: start + WINDOW + DAY - 60_000, offlineMaxAgeMs: WINDOW });
      ctx.expect(pruned.expired === 0, `nothing expires inside the window: ${JSON.stringify(pruned)}`);
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
    });
    await ctx.step('after it, a late duplicate is refused (0009 rule 3)', async () => {
      const pruned = await ctx.backend.expireReceipts(ctx.project, ctx.tenants.a, { now: start + WINDOW + DAY + 60_000, offlineMaxAgeMs: WINDOW });
      ctx.expect(pruned.expired === 1, `one result expires: ${JSON.stringify(pruned)}`);
      const late = await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' });
      ctx.observe('late', late);
      ctx.expect(late.error === 'Reducer receipt expired' && late.class === 'rejected' && late.retryable === false, `expired receipt refuses the key: ${JSON.stringify(late)}`);
      const receipt = await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: key, receiptPath: 'board.bump' });
      ctx.observe('receipt', receipt);
      ctx.expect(receipt.error === 'Reducer receipt expired', `the receipt query answers the same: ${JSON.stringify(receipt)}`);
      ctx.observe('fresh', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-expiry-key-0002', receiptPath: 'board.bump' }));
    });
    await ctx.step('365 days later the tombstone is gone (0009 rule 2)', async () => {
      const pruned = await ctx.backend.expireReceipts(ctx.project, ctx.tenants.a, { now: start + WINDOW + DAY + 366 * DAY, offlineMaxAgeMs: WINDOW });
      ctx.expect(pruned.deleted === 1, `one tombstone is deleted: ${JSON.stringify(pruned)}`);
      ctx.observe('again', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
    });
  },
};
