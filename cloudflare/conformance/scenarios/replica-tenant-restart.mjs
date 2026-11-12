/**
 * The tenant object alone restarts (an eviction, a crash or a redeploy of the
 * object; kernel only). Its subscriptions live in memory, so they are gone,
 * while the clients' sockets stay open on their gateways. On the next commit
 * the gateway sees a new tenant instance and sends `replica.reset` with reason
 * `change-feed-reset` for every collection the old instance served; clients
 * resume with their cursor and digest and converge. Rust keeps subscriptions
 * and sockets in one task and has no such restart, so it skips this scenario.
 */
import { watch, call, rowsWhen, project, integrity, rawOpen, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const COLUMNS = ['id', 'title', 'position', 'bumps'];

export default {
  name: 'replica-tenant-restart',
  description: 'The tenant object restarts under open sockets: change-feed-reset, then every client resumes (kernel only)',
  tenants: { a: ['alice', 'bob'] },
  requires: ['tenantRestart'],
  async run(ctx) {
    const { api } = ctx;
    let writer, reader, opened, sdk, view;
    await ctx.step('a raw reader and the SDK watch the board', async () => {
      writer = await ctx.raw('writer');
      const s = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 't1', title: 'before', position: 1 }));
      reader = await ctx.raw('reader');
      const r = await ctx.rawLogin(reader, 'bob');
      await reader.auth({ project: ctx.project, tenant: ctx.tenants.a, token: r.accessToken, capabilities: RAW_CAPABILITIES });
      opened = await rawOpen(reader, { path: 'board.all' });
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      view = watch(sdk, api.board.all);
      await rowsWhen(ctx, view, (rows) => rows.length === 1, 'sdk snapshot');
    });
    await ctx.step('the tenant object restarts; the next commit resets its collections', async () => {
      await ctx.backend.restartTenant(ctx.project, ctx.tenants.a);
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 't2', title: 'after the restart', position: 2 }));
      const reset = await reader.next((m) => m.type === 'replica.reset' && m.id === opened.id, { label: 'replica.reset' });
      ctx.observe('reset', { reason: reset.reason, path: reset.path });
      // The client resumes with the cursor, keys and row hashes it holds (as the SDK does after a reset).
      const resumed = await rawOpen(reader, { id: opened.id, path: 'board.all', cursor: opened.ready.cursor, ...integrity(opened.rows, 'id', COLUMNS) }, opened.rows);
      ctx.observe('resume', { types: resumed.types, rows: project(resumed.rows ?? [], COLUMNS) });
      ctx.expect(JSON.stringify((resumed.rows ?? []).map((r) => r.id)) === '["t1","t2"]', 'the raw reader resumes onto both rows');
      ctx.observe('sdk', project(await rowsWhen(ctx, view, (rows) => rows.length === 2, 'the SDK resumes'), COLUMNS));
    });
    await ctx.step('the restarted object serves the resumed collections', async () => {
      ctx.observe('call', await call(sdk, api.board.bump, { id: 't1' }));
      ctx.observe('sdk', project(await rowsWhen(ctx, view, (rows) => rows.some((r) => r.id === 't1' && r.bumps === 1), 'sdk sees its call'), COLUMNS));
      const delta = await reader.next((m) => m.type === 'replica.delta' && m.id === opened.id, { label: 'delta after the resume' });
      ctx.observe('delta', project(delta.upserts, COLUMNS));
    });
  },
};
