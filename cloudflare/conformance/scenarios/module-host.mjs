/**
 * Module host (CONTRACT.md B, package K04), through raw frames:
 *
 *  - the ctx surface per kind (Query vs Reducer), and globals module code
 *    must not see (`fetch`, `performance`);
 *  - the pinned clock, the seeded Math.random sequence (vectors ab…ab) and its
 *    absence without intentEntropy, and secure-entropy denial;
 *  - Rust's host-call error texts: a write in a Query, ctx.permissions in a
 *    Query, an unregistered scheduler target, a bad identifier;
 *  - the aborted-transaction rule: a caught SQL error refuses the commit;
 *  - module savepoints: ROLLBACK TO recovers, RELEASE keeps the rest.
 */
import { rawQuery, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

export default {
  name: 'module-host',
  description: 'Module host: ctx surface per kind, pinned clock, seeded random, host-call errors, aborted transactions, savepoints',
  tenants: { a: ['alice'] },
  async run(ctx) {
    let raw;
    await ctx.step('connect', async () => {
      raw = await ctx.raw('raw');
      const token = (await ctx.rawLogin(raw, 'alice')).accessToken;
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
    });
    await ctx.step('a Query with a handler', async () => {
      ctx.observe('query', await rawQuery(raw, 'host.querySurface', {}));
    });
    await ctx.step('a Reducer with intentEntropy', async () => {
      ctx.observe('seeded', await rawReducer(raw, 'host.reducerSurface', {}, { intentEntropy: 'ab'.repeat(32) }));
    });
    await ctx.step('a Reducer without intentEntropy', async () => {
      ctx.observe('unseeded', await rawReducer(raw, 'host.reducerSurface', {}));
    });
    await ctx.step('a caught SQL error refuses the commit', async () => {
      ctx.observe('caught', await rawReducer(raw, 'host.caughtSqlError', { id: 'caught' }));
    });
    await ctx.step('savepoints recover', async () => {
      ctx.observe('savepoints', await rawReducer(raw, 'host.savepoints', { id: 'sp' }));
    });
  },
};
