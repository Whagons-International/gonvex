/**
 * Replica open, snapshot and resume, at frame level and through the SDK.
 *
 *  - open with no cursor: full snapshot then ready;
 *  - close, commit while closed, reopen on a new socket with the old cursor,
 *    keys, row hashes and digest: the backend answers with what changed;
 *  - reopen with a matching cursor and nothing changed;
 *  - reopen with a cursor from a foreign epoch;
 *  - reopen with a wrong digest (integrity);
 *  - the SDK across a dropped connection, with commits during the outage.
 */
import { watch, call, rowsWhen, project, integrity, rawOpen, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const COLUMNS = ['id', 'title', 'position', 'bumps'];
const converged = (ctx, r, label) => ctx.expect(JSON.stringify((r.rows ?? []).map(x => x.id)) === '["r1","r3","r4"]', `${label}: client state converges on r1, r3, r4 (got ${JSON.stringify((r.rows ?? []).map(x => x.id))})`);
const shape = (r) => ({ types: r.types, rows: r.rows && project(r.rows, COLUMNS), changed: r.changed, reset: r.reset, error: r.error, truncated: r.ready?.truncated });

export default {
  name: 'replica-resume',
  description: 'Replica open, snapshot, resume by cursor and hashes, foreign epoch, digest mismatch, SDK reconnect',
  tenants: { a: ['alice', 'bob'] },
  async run(ctx) {
    const { api } = ctx;
    let writer, reader, first, session;
    await ctx.step('seed rows', async () => {
      writer = await ctx.raw('writer');
      session = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: session.accessToken, capabilities: RAW_CAPABILITIES });
      for (const [id, position] of [['r1', 1], ['r2', 2], ['r3', 3]]) ctx.observe(id, await rawReducer(writer, 'board.add', { id, title: `row ${id}`, position }));
    });
    await ctx.step('open without cursor: snapshot', async () => {
      reader = await ctx.raw('reader');
      const s = await ctx.rawLogin(reader, 'bob');
      await reader.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      first = await rawOpen(reader, { path: 'board.all' });
      ctx.observe('open', shape(first));
      ctx.expect(first.rows?.length === 3, 'snapshot has the three seeded rows');
    });
    await ctx.step('commits while the reader is away', async () => {
      reader.close();
      ctx.observe('rename', await rawReducer(writer, 'board.rename', { id: 'r1', title: 'renamed while away' }));
      ctx.observe('remove', await rawReducer(writer, 'board.remove', { id: 'r2' }));
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 'r4', title: 'added while away', position: 4 }));
    });
    let resumed;
    await ctx.step('resume with cursor, keys and hashes', async () => {
      reader = await ctx.raw('reader');
      const s = await ctx.rawLogin(reader, 'bob');
      await reader.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      const proof = integrity(first.rows, 'id', COLUMNS);
      resumed = await rawOpen(reader, { path: 'board.all', cursor: first.ready.cursor, ...proof, fullIntegrity: true }, first.rows);
      ctx.observe('resume', shape(resumed));
      converged(ctx, resumed, 'resume');
    });
    await ctx.step('resume again with nothing changed', async () => {
      const rows = resumed.rows ?? [];
      const again = await rawOpen(reader, { path: 'board.all', cursor: resumed.ready?.cursor ?? first.ready.cursor, ...integrity(rows, 'id', COLUMNS), fullIntegrity: true }, rows);
      ctx.observe('unchanged', shape(again));
      converged(ctx, again, 'unchanged resume');
    });
    await ctx.step('resume with a foreign epoch', async () => {
      const foreign = await rawOpen(reader, { path: 'board.all', cursor: { epoch: 'conformance-foreign-epoch', revision: first.ready.cursor.revision }, ...integrity(first.rows, 'id', COLUMNS), fullIntegrity: true }, first.rows);
      ctx.observe('foreignEpoch', shape(foreign));
      converged(ctx, foreign, 'foreign epoch');
    });
    await ctx.step('resume with a digest that does not match the rows', async () => {
      const proof = integrity(first.rows, 'id', COLUMNS);
      const wrong = await rawOpen(reader, { path: 'board.all', cursor: first.ready.cursor, ...proof, digest: 'xor1:' + 'f'.repeat(64), fullIntegrity: true }, first.rows);
      ctx.observe('wrongDigest', shape(wrong));
      converged(ctx, wrong, 'wrong digest');
    });
    let sdk, view;
    await ctx.step('SDK mounts the collection', async () => {
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      view = watch(sdk, api.board.all);
      ctx.observe('rows', project(await rowsWhen(ctx, view, rows => rows.length === 3, 'sdk snapshot'), COLUMNS));
    });
    await ctx.step('SDK connection drops; commits during the outage', async () => {
      sdk.socket.close(4001, 'conformance network drop');
      ctx.observe('bump', await rawReducer(writer, 'board.bump', { id: 'r3' }));
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 'r5', title: 'during outage', position: 5 }));
      const rows = await rowsWhen(ctx, view, rows => rows.length === 4 && rows.some(r => r.id === 'r3' && r.bumps === 1), 'sdk resumed');
      ctx.observe('rows', project(rows, COLUMNS));
    });
    await ctx.step('SDK sees a commit after resuming', async () => {
      ctx.observe('rename', await call(sdk, api.board.rename, { id: 'r5', title: 'after resume' }));
      ctx.observe('rows', project(await rowsWhen(ctx, view, rows => rows.some(r => r.title === 'after resume'), 'post-resume commit'), COLUMNS));
    });
  },
};
