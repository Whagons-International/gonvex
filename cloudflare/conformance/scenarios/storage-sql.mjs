/**
 * Module SQL against the tenant database (package K01, storage layer), seen
 * through Reducer results: `$N` placeholders bound by number, integer and
 * JSON parameters, decoding by declared type (JSON, BOOLEAN, BLOB,
 * expression columns, RETURNING rows), refused statements (several
 * statements, ATTACH, PRAGMA, VACUUM, BEGIN), module savepoints, a caught SQL
 * error that must not commit, a deferred foreign key that fails at commit
 * (the backend keeps serving), existsDataRows over 100 bound parameters, and
 * the `matches` predicate (REGEXP).
 */
import { rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

export default {
  name: 'storage-sql',
  description: 'Module SQL: placeholders, types, decoding, refusals, savepoints, aborted commit, deferred FK, >100 parameters, REGEXP',
  tenants: { a: ['alice'] },
  async run(ctx) {
    let raw;
    await ctx.step('connect', async () => {
      raw = await ctx.raw('raw');
      const token = (await ctx.rawLogin(raw, 'alice')).accessToken;
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
    });
    await ctx.step('placeholders, parameter types, decoding and refused statements', async () => {
      ctx.observe('statements', await rawReducer(raw, 'storage.statements', {}));
    });
    await ctx.step('module savepoints', async () => {
      ctx.observe('savepoints', await rawReducer(raw, 'storage.savepoints', {}));
    });
    await ctx.step('a caught SQL error without ROLLBACK TO does not commit', async () => {
      ctx.observe('caughtFailure', await rawReducer(raw, 'storage.caughtFailure', {}));
    });
    await ctx.step('a deferred foreign key fails at commit, and the backend keeps serving', async () => {
      ctx.observe('orphan', await rawReducer(raw, 'storage.orphan', { id: 'orphan-1' }));
      ctx.observe('adopt', await rawReducer(raw, 'storage.adopt', { id: 'child-1' }));
      ctx.observe('orphan again', await rawReducer(raw, 'storage.orphan', { id: 'orphan-2' }));
    });
    await ctx.step('existsDataRows over 100 bound parameters', async () => {
      ctx.observe('exists', await rawReducer(raw, 'storage.exists', {}));
    });
    await ctx.step('the matches predicate (REGEXP)', async () => {
      ctx.observe('matching', await rawReducer(raw, 'storage.matching', {}));
    });
  },
};
