/**
 * Reducer error classes (packages/protocol ReducerErrorClass), at frame level
 * (`class`, `retryable`, message) and as the SDK surfaces them
 * (GonvexClientError code / errorClass):
 *
 *  - rejected: the module throws (its earlier write rolls back), invalid
 *    arguments, a result that violates the declared schema, an unknown path;
 *  - transient: the invocation deadline is exceeded;
 *  - update_required: a stale client contract;
 *  - unauthenticated: a tenant Reducer on a control-only session.
 */
import { call, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

export default {
  name: 'reducer-errors',
  description: 'Reducer error classes: rejected (throw/rollback, args, result, unknown), transient, update_required, unauthenticated',
  tenants: { a: ['alice'] },
  async run(ctx) {
    const { api } = ctx;
    let raw, token, sdk;
    await ctx.step('connect', async () => {
      raw = await ctx.raw('raw');
      token = (await ctx.rawLogin(raw, 'alice')).accessToken;
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
      sdk = await ctx.sdk('sdk', { member: 'alice' });
    });
    await ctx.step('rejected: the module throws after writing', async () => {
      ctx.observe('raw', await rawReducer(raw, 'errors.reject', { id: 'rolled-back' }));
      ctx.observe('sdk', await call(sdk, api.errors.reject, { id: 'rolled-back-sdk' }));
    });
    await ctx.step('rejected: invalid arguments', async () => {
      ctx.observe('wrongType', await rawReducer(raw, 'board.add', { id: 'x', title: 7, position: 'one' }));
      ctx.observe('missing', await rawReducer(raw, 'board.add', { id: 'x' }));
      ctx.observe('extra', await rawReducer(raw, 'board.add', { id: 'x', title: 't', position: 1, unexpected: true }));
    });
    await ctx.step('rejected: result violates the declared schema', async () => {
      ctx.observe('raw', await rawReducer(raw, 'errors.badResult', {}));
    });
    await ctx.step('rejected: unknown function and wrong kind', async () => {
      ctx.observe('unknown', await rawReducer(raw, 'board.doesNotExist', {}));
      ctx.observe('query as reducer', await rawReducer(raw, 'board.all', {}));
    });
    await ctx.step('transient: invocation deadline', async () => {
      ctx.observe('raw', await rawReducer(raw, 'errors.spin', {}));
      // The backend keeps serving after a timed-out invocation.
      ctx.observe('after', await rawReducer(raw, 'board.add', { id: 'after-spin', title: 'still serving', position: 1 }));
    });
    await ctx.step('update_required: stale client contract', async () => {
      ctx.observe('raw', await rawReducer(raw, 'board.add', { id: 'old-client', title: 't', position: 2 }, { clientContract: 0 }));
      ctx.observe('artifact', await rawReducer(raw, 'board.add', { id: 'old-artifact', title: 't', position: 2 }, { artifactHash: 'f'.repeat(64) }));
    });
    await ctx.step('unauthenticated: tenant Reducer on a control-only session', async () => {
      const anon = await ctx.raw('control-only');
      await anon.auth({ project: ctx.project });
      ctx.observe('raw', await rawReducer(anon, 'board.add', { id: 'anon', title: 't', position: 3 }));
      anon.close();
    });
  },
};
