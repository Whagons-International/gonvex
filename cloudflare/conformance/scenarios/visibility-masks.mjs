/**
 * Visibility masks v2 (K09): what three Members see of two Replica
 * Collections as permissions change. A principal set through two permission
 * tables (team -> workspace), a grant set (assignment), a creator context,
 * and notes that inherit their task's visibility, with an onMissing fallback
 * for orphans (the author). Covers a team join, a workspace link, an
 * assignment and its revocation, a parent moving out of reach, and a parent
 * deleted under its notes. SDK and thin clients.
 */
import { watch, call, rowsWhen, project } from './helpers.mjs';

const TASK_COLUMNS = ['id', 'workspace_id'];
const NOTE_COLUMNS = ['id', 'task_id'];

export default {
  name: 'visibility-masks',
  description: 'Masks v2: team, grant, creator and inherited visibility through permission changes',
  tenants: { a: ['alice', 'bob', 'carol'] },
  tables: ['mask_tasks', 'mask_notes', 'mask_teams', 'mask_workspace_teams', 'mask_task_users'],
  async run(ctx) {
    const { api } = ctx;
    const clients = {};
    const views = {};
    await ctx.step('connect three members', async () => {
      clients.alice = await ctx.sdk('alice', { member: 'alice' });
      clients.bob = await ctx.sdk('bob', { member: 'bob' });
      clients.carol = await ctx.thin('carol', { member: 'carol' });
      for (const [name, client] of Object.entries(clients)) {
        views[name] = { tasks: watch(client, api.masks.maskTasks), notes: watch(client, api.masks.maskNotes) };
        await rowsWhen(ctx, views[name].tasks, rows => rows.length === 0, `${name}: no tasks`);
        await rowsWhen(ctx, views[name].notes, rows => rows.length === 0, `${name}: no notes`);
      }
    });

    /** Waits until each member sees exactly `expected[name] = { tasks: [ids], notes: [ids] }`, then records it. */
    const expectSeen = async (label, expected) => {
      for (const [name, sees] of Object.entries(expected)) {
        const seen = async (view, ids, what) => {
          try {
            return await rowsWhen(ctx, view, rows => JSON.stringify(rows.map(r => r.id)) === JSON.stringify(ids), `${name} ${what}: ${label}`);
          } catch (error) {
            ctx.observe(`${name}.${what} (timed out)`, view.rows());
            throw error;
          }
        };
        const tasks = await seen(views[name].tasks, sees.tasks, 'tasks');
        const notes = await seen(views[name].notes, sees.notes, 'notes');
        ctx.observe(`${name}.tasks`, project(tasks, TASK_COLUMNS));
        ctx.observe(`${name}.notes`, project(notes, NOTE_COLUMNS));
      }
    };

    await ctx.step('creator sees her tasks and notes; an orphan note is its author\'s', async () => {
      ctx.observe('t1', await call(clients.alice, api.masks.maskPutTask, { id: 't1', workspaceId: 'w1' }));
      ctx.observe('t2', await call(clients.alice, api.masks.maskPutTask, { id: 't2', workspaceId: 'w2' }));
      ctx.observe('n1', await call(clients.alice, api.masks.maskAddNote, { id: 'n1', taskId: 't1' }));
      ctx.observe('n2', await call(clients.alice, api.masks.maskAddNote, { id: 'n2', taskId: 't2' }));
      ctx.observe('n3', await call(clients.bob, api.masks.maskAddNote, { id: 'n3', taskId: 'later' }));
      await expectSeen('created', {
        alice: { tasks: ['t1', 't2'], notes: ['n1', 'n2'] },
        bob: { tasks: [], notes: ['n3'] },
        carol: { tasks: [], notes: [] },
      });
    });

    await ctx.step('a team join and a workspace link reveal w1 to bob', async () => {
      ctx.observe('join', await call(clients.bob, api.masks.maskJoinTeam, { id: 'm-bob-x', teamId: 'team-x' }));
      ctx.observe('link', await call(clients.alice, api.masks.maskLinkWorkspace, { id: 'l-x-w1', teamId: 'team-x', workspaceId: 'w1' }));
      await expectSeen('team', {
        alice: { tasks: ['t1', 't2'], notes: ['n1', 'n2'] },
        bob: { tasks: ['t1'], notes: ['n1', 'n3'] },
        carol: { tasks: [], notes: [] },
      });
    });

    await ctx.step('an assignment grants carol one task and its notes', async () => {
      ctx.observe('assign', await call(clients.alice, api.masks.maskAssign, { id: 'a-carol-t2', taskId: 't2', memberId: ctx.members.carol.memberIds.a }));
      await expectSeen('assigned', {
        alice: { tasks: ['t1', 't2'], notes: ['n1', 'n2'] },
        bob: { tasks: ['t1'], notes: ['n1', 'n3'] },
        carol: { tasks: ['t2'], notes: ['n2'] },
      });
    });

    await ctx.step('a task moving out of bob\'s workspaces takes its notes', async () => {
      ctx.observe('move', await call(clients.alice, api.masks.maskPutTask, { id: 't1', workspaceId: 'w2' }));
      await expectSeen('moved', {
        alice: { tasks: ['t1', 't2'], notes: ['n1', 'n2'] },
        bob: { tasks: [], notes: ['n3'] },
        carol: { tasks: ['t2'], notes: ['n2'] },
      });
    });

    await ctx.step('deleting a parent orphans its notes: the author keeps them, the grantee loses them', async () => {
      ctx.observe('delete', await call(clients.alice, api.masks.maskRemove, { table: 'mask_tasks', id: 't2' }));
      await expectSeen('orphaned', {
        alice: { tasks: ['t1'], notes: ['n1', 'n2'] },
        bob: { tasks: [], notes: ['n3'] },
        carol: { tasks: [], notes: [] },
      });
    });

    await ctx.step('a second workspace link reveals, leaving the team hides', async () => {
      ctx.observe('relink', await call(clients.alice, api.masks.maskLinkWorkspace, { id: 'l-x-w2', teamId: 'team-x', workspaceId: 'w2' }));
      await expectSeen('relinked', {
        alice: { tasks: ['t1'], notes: ['n1', 'n2'] },
        bob: { tasks: ['t1'], notes: ['n1', 'n3'] },
        carol: { tasks: [], notes: [] },
      });
      ctx.observe('unassign', await call(clients.alice, api.masks.maskRemove, { table: 'mask_task_users', id: 'a-carol-t2' }));
      ctx.observe('leave', await call(clients.bob, api.masks.maskRemove, { table: 'mask_teams', id: 'm-bob-x' }));
      await expectSeen('left', {
        alice: { tasks: ['t1'], notes: ['n1', 'n2'] },
        bob: { tasks: [], notes: ['n3'] },
        carol: { tasks: [], notes: [] },
      });
    });
  },
};
