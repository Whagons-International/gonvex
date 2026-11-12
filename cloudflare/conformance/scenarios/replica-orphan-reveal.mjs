/**
 * Intended behavior IB5 (contract-changes 0031, ruled in 0032), kernel only.
 * A note whose task does not exist yet is visible only through `onMissing`
 * (its author). When the task is inserted, its children switch to `inherits`:
 * the task's creator must receive the note and its author, who cannot see the
 * task, must lose it. Rust never delivers either change (it skips the children
 * of a parent its transaction created), although a fresh snapshot shows them;
 * the kernel delivers them, so every client converges on a fresh snapshot.
 * Backends without `orphanReveal` (Rust) skip it.
 */
import { watch, call, rowsWhen, project } from './helpers.mjs';

const NOTE_COLUMNS = ['id', 'task_id'];

export default {
  name: 'replica-orphan-reveal',
  description: 'IB5: an orphan note is delivered to whoever can see its task once the task appears (kernel only)',
  tenants: { a: ['alice', 'bob'] },
  requires: ['orphanReveal'],
  tables: ['mask_tasks', 'mask_notes'],
  async run(ctx) {
    const { api } = ctx;
    const clients = {}, views = {};
    const ids = (rows) => rows.map((r) => r.id);
    const sees = async (name, what, expected, label) => {
      const rows = await rowsWhen(ctx, views[name][what], (r) => JSON.stringify(ids(r)) === JSON.stringify(expected), `${name} ${what}: ${label}`);
      ctx.observe(`${name}.${what}`, project(rows, what === 'notes' ? NOTE_COLUMNS : ['id', 'workspace_id']));
    };
    await ctx.step('two members watch tasks and notes', async () => {
      for (const name of ['alice', 'bob']) {
        clients[name] = await ctx.sdk(name, { member: name });
        views[name] = { tasks: watch(clients[name], api.masks.maskTasks), notes: watch(clients[name], api.masks.maskNotes) };
        await sees(name, 'tasks', [], 'empty');
        await sees(name, 'notes', [], 'empty');
      }
    });
    await ctx.step('bob writes a note under a task that does not exist yet', async () => {
      ctx.observe('note', await call(clients.bob, api.masks.maskAddNote, { id: 'n-orphan', taskId: 't-late' }));
      await sees('bob', 'notes', ['n-orphan'], 'the author sees the orphan');
      await sees('alice', 'notes', [], 'nobody else does');
    });
    await ctx.step('alice creates the task: its creator gains the note, its author loses it', async () => {
      ctx.observe('task', await call(clients.alice, api.masks.maskPutTask, { id: 't-late', workspaceId: 'w-late' }));
      await sees('alice', 'tasks', ['t-late'], 'the creator sees the task');
      await sees('alice', 'notes', ['n-orphan'], 'and the note under it');
      await sees('bob', 'tasks', [], 'bob cannot see the task');
      await sees('bob', 'notes', [], 'so the note now follows it');
    });
    await ctx.step('a fresh snapshot shows the same rows', async () => {
      for (const name of ['alice', 'bob']) {
        const fresh = await ctx.sdk(`${name}-fresh`, { member: name, fresh: true });
        const view = watch(fresh, api.masks.maskNotes);
        const rows = await rowsWhen(ctx, view, () => true, `${name}: fresh snapshot`);
        ctx.expect(JSON.stringify(ids(rows)) === JSON.stringify(ids(views[name].notes.rows())), `${name}: delivered rows equal a fresh snapshot`);
      }
    });
  },
};
