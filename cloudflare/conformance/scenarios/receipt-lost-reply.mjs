/**
 * Receipt case "lost reply" (verification/backend-receipts sdk_composition
 * mode lost_reply): a Reducer commits but its reply never reaches the client.
 * The client replays the call with the same idempotency key and must get the
 * stored result without a second application. The receipt query answers for
 * the key; unknown keys and other paths are not found.
 */
import { randomUUID } from 'node:crypto';
import { rawReducer, rawQuery, RAW_CAPABILITIES } from './helpers.mjs';

export default {
  name: 'receipt-lost-reply',
  description: 'Lost reply: replay with the same idempotency key applies once; receipt query',
  tenants: { a: ['alice'] },
  async run(ctx) {
    const key = 'conformance-lost-reply-key-0001';
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
    await ctx.step('call with a key, connection drops before the reply', async () => {
      socket.send({ type: 'reducer.call', id: randomUUID(), path: 'board.bump', args: { id: 'counter' }, idempotencyKey: key, receiptPath: 'board.bump' });
      socket.drop();
    });
    await ctx.step('replay the same key on a new connection', async () => {
      await connect();
      // The dropped call may still be committing; the replay must wait for it, not run again.
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
      ctx.observe('replayAgain', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
    });
    await ctx.step('receipt query', async () => {
      ctx.observe('known', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: key, receiptPath: 'board.bump' }));
      ctx.observe('unknownKey', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: 'conformance-never-sent-0001', receiptPath: 'board.bump' }));
      ctx.observe('otherPath', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: key, receiptPath: 'board.rename' }));
      ctx.observe('missingArgs', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: key }));
    });
    await ctx.step('same key with different arguments', async () => {
      ctx.observe('differentArgs', await rawReducer(socket, 'board.bump', { id: 'other' }, { idempotencyKey: key, receiptPath: 'board.bump' }));
    });
    await ctx.step('a fresh key applies again', async () => {
      ctx.observe('fresh', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-lost-reply-key-0002', receiptPath: 'board.bump' }));
    });
    await ctx.step('a rejected call with a key replays its rejection', async () => {
      const k = 'conformance-lost-reply-key-0003';
      ctx.observe('first', await rawReducer(socket, 'errors.reject', { id: 'never' }, { idempotencyKey: k, receiptPath: 'errors.reject' }));
      ctx.observe('replay', await rawReducer(socket, 'errors.reject', { id: 'never' }, { idempotencyKey: k, receiptPath: 'errors.reject' }));
      ctx.observe('receipt', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: k, receiptPath: 'errors.reject' }));
    });
  },
};
