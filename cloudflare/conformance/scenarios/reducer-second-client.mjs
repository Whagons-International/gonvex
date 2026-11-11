/**
 * A Reducer commit made by one client reaches a second client's Replica
 * Collection: insert, update, delete, each seen by the real SDK and the thin
 * client, with the writer's own collection converging too.
 */
import { watch, call, rowsWhen, project } from './helpers.mjs';

const COLUMNS = ['id', 'title', 'position', 'bumps'];

export default {
  name: 'reducer-second-client',
  description: 'Reducer commit seen by a second client (SDK and thin) through a replica',
  tenants: { a: ['alice', 'bob'] },
  async run(ctx) {
    const { api } = ctx;
    let writer, sdkReader, thinReader, views = {};
    await ctx.step('connect writer and two readers', async () => {
      writer = await ctx.sdk('writer', { member: 'alice' });
      sdkReader = await ctx.sdk('reader-sdk', { member: 'bob' });
      thinReader = await ctx.thin('reader-thin', { member: 'bob' });
      for (const [tag, client] of [['writer', writer], ['reader-sdk', sdkReader], ['reader-thin', thinReader]]) views[tag] = watch(client, api.board.all);
      for (const view of Object.values(views)) await rowsWhen(ctx, view, rows => rows.length === 0, 'empty board');
      ctx.observe('sessionTenants', writer.session.tenants?.map(t => t.id));
    });
    const expectEverywhere = async (label, expected) => {
      for (const [tag, view] of Object.entries(views)) {
        const rows = await rowsWhen(ctx, view, rows => JSON.stringify(project(rows, COLUMNS)) === JSON.stringify(expected), `${tag}: ${label}`);
        ctx.observe(tag, project(rows, COLUMNS));
      }
    };
    await ctx.step('insert two rows', async () => {
      ctx.observe('add1', await call(writer, api.board.add, { id: 'b1', title: 'first', position: 1 }));
      ctx.observe('add2', await call(writer, api.board.add, { id: 'b2', title: 'second', position: 2 }));
      await expectEverywhere('inserts', [{ id: 'b1', title: 'first', position: 1, bumps: 0 }, { id: 'b2', title: 'second', position: 2, bumps: 0 }]);
    });
    await ctx.step('update one row twice', async () => {
      ctx.observe('rename', await call(writer, api.board.rename, { id: 'b1', title: 'renamed' }));
      ctx.observe('bump', await call(writer, api.board.bump, { id: 'b1' }));
      await expectEverywhere('updates', [{ id: 'b1', title: 'renamed', position: 1, bumps: 1 }, { id: 'b2', title: 'second', position: 2, bumps: 0 }]);
    });
    await ctx.step('delete a row', async () => {
      ctx.observe('remove', await call(writer, api.board.remove, { id: 'b2' }));
      await expectEverywhere('delete', [{ id: 'b1', title: 'renamed', position: 1, bumps: 1 }]);
    });
    await ctx.step('a reader writes back', async () => {
      ctx.observe('readerAdd', await call(sdkReader, api.board.add, { id: 'b3', title: 'from bob', position: 3 }));
      await expectEverywhere('reader write', [{ id: 'b1', title: 'renamed', position: 1, bumps: 1 }, { id: 'b3', title: 'from bob', position: 3, bumps: 0 }]);
    });
  },
};
