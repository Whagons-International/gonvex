/**
 * Whagons' task grid (`tasks.grid`, whagons5-client `2457bb65f`) as a Live
 * Query (K08). Bob and Carol watch windows of workspace ws-1 over raw
 * sockets while Alice writes through `tasks.putRow` / `tasks.removeRow`:
 *
 * - the default window (id desc, exact count), the index sort by status, the
 *   search over the grid projection and the related search documents (plain
 *   and `$or`), the filter operators, a second Member's visibility class;
 * - rows entering and leaving through visibility sets (assignment, team
 *   membership), through the `inRelation` workspace contexts (shares,
 *   findings, revocation), a write Bob cannot see (no frame), dependency
 *   writes (status names and order), a related search document, a delete;
 * - resume with the current `windowRevision` (`query.progress`) and a stale
 *   one, `query.subscribeMany`, `query.unsubscribe`;
 * - the one-shot structured plan `tasks.byIds` over 150 ids (more than 100
 *   bound parameters), and the error texts of bad plans and arguments.
 *
 * The scenario is data (`SCRIPT`), so the kernel's parity test
 * (`kernel/query/test/task-grid-parity.test.ts`) replays exactly these
 * actions against the change log Rust wrote (`src/live-query-golden.mjs`).
 */
import { randomUUID } from 'node:crypto';
import { RAW_CAPABILITIES } from './helpers.mjs';

const WS = 'ws-1';
const grid = (args) => ({ workspaceId: WS, offset: 0, limit: 3, ...args });

/** One task: its row, its grid projection row and its related search document. */
function task(t, m) {
  const statuses = { 's-open': ['Open', 1], 's-prog': ['In progress', 2], 's-done': ['Done', 3] };
  const priorities = { 'p-low': ['Low', 1], 'p-high': ['High', 2] };
  const [statusName, statusOrder] = statuses[t.statusId ?? 's-open'];
  const [priorityName, priorityOrder] = priorities[t.priorityId ?? 'p-low'];
  const row = {
    _id: t._id, _creationTime: 1700000000000.5 + t.id, id: t.id, name: t.name, description: t.description ?? null,
    workspaceId: t.workspaceId ?? WS, statusId: t.statusId ?? 's-open', priorityId: t.priorityId ?? 'p-low', categoryId: 'cat-1',
    spotId: null, teamId: null, templateId: null, createdBy: m(t.createdBy), attributedCreatorId: null, createdVia: 'conformance',
    requiresSignature: t.id % 2 === 0, assetId: null, requiresAcknowledgment: false,
    audienceRules: t.id % 3 === 0 ? { teams: ['team-1'], weight: 0.25 } : null, reportedByGuest: false, guestVip: false,
    reportedByTeamId: t.reportedByTeamId ?? null, reportedByUserId: t.reportedByUserId ? m(t.reportedByUserId) : null,
    dueDate: 'dueDate' in t ? t.dueDate : 1700100000000 + t.id * 1000, startDate: null, latitude: t.id % 2 ? 40.5 : 12, longitude: -3.125,
    completedAt: null, createdAt: 1700000000000 + t.id, updatedAt: 1700000000000 + t.id * 10, flagColor: null, rowHash: `h${t.id}`,
    deletedAt: null, deletedBy: null,
  };
  const gridRow = {
    taskId: t._id, assetId: null, reportedByGuest: false, guestVip: false, id: t.id, workspaceId: row.workspaceId, name: t.name,
    description: row.description, statusId: row.statusId, priorityId: row.priorityId, categoryId: 'cat-1', spotId: null, teamId: null,
    templateId: null, createdBy: row.createdBy, dueDate: row.dueDate, startDate: null, completedAt: null, createdAt: row.createdAt,
    updatedAt: row.updatedAt, _creationTime: row._creationTime, flagColor: null, deletedAt: null,
    statusOrder, statusName, priorityOrder, priorityName, searchText: `${t.id} ${t.name} ${statusName} ${priorityName}`.toLowerCase(),
  };
  return [
    { write: 'tasks.putRow', args: { table: 'tasks', row } },
    { write: 'tasks.putRow', args: { table: 'taskGridRows', row: gridRow } },
    { write: 'tasks.putRow', args: { table: 'taskRelatedSearchDocuments', row: { taskId: t._id, searchText: (t.related ?? '').toLowerCase() } } },
  ];
}
const put = (table, row) => ({ write: 'tasks.putRow', args: { table, row } });
const remove = (table, id) => ({ write: 'tasks.removeRow', args: { table, id } });
const subscribe = (session, id, args, extra = {}) => ({ send: session, frame: { type: 'query.subscribe', id, path: 'tasks.grid', args, ...extra } });

/**
 * The script. `m(name)` is a Member id in tenant a. A `resumeOf` subscribe
 * sends the `windowRevision` of the named subscription's latest result.
 */
export function script(m) {
  return [
    { step: 'seed statuses, teams and tasks', actions: [
      put('statuses', { _id: 's-open', name: 'Open', order: 1 }),
      put('statuses', { _id: 's-prog', name: 'In progress', order: 2 }),
      put('statuses', { _id: 's-done', name: 'Done', order: 3 }),
      put('priorities', { _id: 'p-low', name: 'Low', order: 1 }),
      put('priorities', { _id: 'p-high', name: 'High', order: 2 }),
      put('memberTeams', { _id: 'mt-bob-1', memberId: m('bob'), teamId: 'team-1', deletedAt: null }),
      ...task({ _id: 't1', id: 101, name: 'Fix pump', createdBy: 'bob', priorityId: 'p-high' }, m),
      ...task({ _id: 't2', id: 102, name: 'Paint lobby', createdBy: 'alice', reportedByTeamId: 'team-1', statusId: 's-prog' }, m),
      ...task({ _id: 't3', id: 103, name: 'Replace filter', createdBy: 'alice', related: 'pump filter note' }, m),
      put('taskUsers', { _id: 'tu-1', taskId: 't3', userId: m('bob'), deletedAt: null }),
      ...task({ _id: 't4', id: 104, name: 'Inspect roof', createdBy: 'alice', reportedByTeamId: 'team-2' }, m),
      ...task({ _id: 't5', id: 105, name: 'Order chairs', createdBy: 'carol', reportedByUserId: 'bob', statusId: 's-done' }, m),
      ...task({ _id: 't6', id: 106, name: 'Clean "pool" 100%', createdBy: 'bob', description: 'line one\nline two\ttab', dueDate: null }, m),
      ...task({ _id: 't7', id: 107, name: 'Pump maintenance', createdBy: 'bob', statusId: 's-done', priorityId: 'p-high' }, m),
      ...task({ _id: 't8', id: 201, name: 'Remote audit', createdBy: 'alice', workspaceId: 'ws-2' }, m),
      put('taskUsers', { _id: 'tu-2', taskId: 't8', userId: m('bob'), deletedAt: null }),
      put('taskWorkspaceContexts', { _id: 'ctx-1', taskId: 't8', workspaceId: WS, kind: 'share', recipientUserIds: null, targetUserId: m('bob'), targetTeamId: null, revokedAt: null }),
      ...task({ _id: 't9', id: 202, name: 'Finding: leak', createdBy: 'bob', workspaceId: 'ws-2' }, m),
      put('taskWorkspaceContexts', { _id: 'ctx-2', taskId: 't9', workspaceId: WS, kind: 'finding', recipientUserIds: [], targetUserId: null, targetTeamId: null, revokedAt: null }),
      put('taskFindings', { _id: 'f-1', linkedTaskId: 't9', deletedAt: null }),
    ] },
    { step: 'bob and carol open grid windows', actions: [
      subscribe('bob', 'g-default', grid({})),
      subscribe('bob', 'g-status', grid({ sortColumn: 'statusId', sortDirection: 'ASC', limit: 4 })),
      subscribe('bob', 'g-priority', grid({ sortColumn: 'priorityId', sortDirection: 'desc', limit: 10 })),
      subscribe('bob', 'g-name', grid({ sortColumn: 'name', limit: 10, offset: 2 })),
      subscribe('bob', 'g-due-desc', grid({ sortColumn: 'dueDate', sortDirection: 'desc', limit: 2 })),
      subscribe('bob', 'g-due-asc', grid({ sortColumn: 'dueDate', limit: 2, offset: 6 })),
      subscribe('bob', 'g-search', grid({ search: 'Pump', limit: 10 })),
      subscribe('bob', 'g-search-or', grid({ search: 'pump $or lobby', limit: 10 })),
      subscribe('bob', 'g-filter', grid({ limit: 10, filters: [
        { column: 'statusId', operator: 'oneOf', value: '["s-open","s-done"]' },
        { column: 'id', operator: 'inRange', value: '100', valueTo: '199' },
        { column: 'name', operator: 'contains', value: 'p' },
      ] })),
      subscribe('bob', 'g-filter-2', grid({ limit: 10, filters: [
        { column: 'description', operator: 'empty', value: '' },
        { column: 'createdAt', operator: 'greaterThan', value: '1700000000103' },
        { column: 'name', operator: 'notContains', value: '100%' },
      ] })),
      subscribe('carol', 'g-default', grid({})),
    ] },
    { step: 'resume windows', actions: [
      subscribe('bob', 'g-resume', grid({}), { resumeOf: 'g-default' }),
      subscribe('bob', 'g-stale', grid({}), { windowRevision: 'stale' }),
      { send: 'bob', frame: { type: 'query.unsubscribe', id: 'g-stale' } },
    ] },
    { step: 'a task bob cannot see changes', actions: [
      put('tasks', { _id: 't4', name: 'Inspect roof again' }),
    ] },
    { step: 'a task assigned to bob enters the windows', actions: [
      ...task({ _id: 't10', id: 203, name: 'Pump room check', createdBy: 'alice' }, m),
      put('taskUsers', { _id: 'tu-3', taskId: 't10', userId: m('bob'), deletedAt: null }),
    ] },
    { step: 'status order and names change', actions: [
      put('statuses', { _id: 's-done', name: 'Closed', order: 0 }),
      put('taskGridRows', { taskId: 't5', statusOrder: 0, statusName: 'Closed', searchText: '105 order chairs closed low' }),
      put('taskGridRows', { taskId: 't7', statusOrder: 0, statusName: 'Closed', searchText: '107 pump maintenance closed high' }),
      put('tasks', { _id: 't5', name: 'Order more chairs', updatedAt: 1700000009999 }),
    ] },
    { step: 'bob joins team-2', actions: [
      put('memberTeams', { _id: 'mt-bob-2', memberId: m('bob'), teamId: 'team-2', deletedAt: null }),
    ] },
    { step: 'a workspace context is revoked', actions: [
      put('taskWorkspaceContexts', { _id: 'ctx-1', revokedAt: 1700000005000 }),
    ] },
    { step: 'a related search document changes', actions: [
      put('taskRelatedSearchDocuments', { taskId: 't2', searchText: 'lobby pump room' }),
    ] },
    { step: 'a task is deleted', actions: [
      remove('taskGridRows', 't7'),
      remove('tasks', 't7'),
    ] },
    { step: 'subscribeMany, unsubscribe and a later write', actions: [
      { send: 'bob', frame: { type: 'query.subscribeMany', subscribes: [
        { id: 'g-many-1', path: 'tasks.grid', args: grid({ offset: '1', limit: 0 }) },
        { id: 'g-many-2', path: 'tasks.grid', args: grid({ search: '$and', limit: 2 }) },
        { id: 'g-many-3', path: 'tasks.nope', args: {} },
      ] } },
      { send: 'bob', frame: { type: 'query.unsubscribe', id: 'g-search' } },
      put('tasks', { _id: 't1', name: 'Fix pump now' }),
      put('taskGridRows', { taskId: 't1', name: 'Fix pump now', searchText: '101 fix pump now open high' }),
    ] },
    { step: 'one-shot plans and errors', actions: [
      { send: 'bob', frame: { type: 'query.call', id: 'c-ids', path: 'tasks.byIds', args: { ids: [...Array.from({ length: 146 }, (_, i) => `missing-${i}`), 't1', 't3', 't4', 't9'] } } },
      { send: 'bob', frame: { type: 'query.call', id: 'c-window', path: 'tasks.byIds', args: { ids: ['t1', 't2', 't3', 't4', 't5', 't6'], offset: 1, limit: 2 } } },
      { send: 'carol', frame: { type: 'query.call', id: 'c-carol', path: 'tasks.byIds', args: { ids: ['t1', 't5'] } } },
      { send: 'bob', frame: { type: 'query.call', id: 'c-bad-args', path: 'tasks.byIds', args: { ids: 5 } } },
      { send: 'bob', frame: { type: 'query.call', id: 'c-live', path: 'tasks.grid', args: grid({}) } },
      { send: 'bob', frame: { type: 'query.call', id: 'c-reducer', path: 'tasks.putRow', args: {} } },
      { send: 'bob', frame: { type: 'query.call', id: 'c-missing', path: 'tasks.nope', args: {} } },
      subscribe('bob', 'e-not-live', {}, { path: 'tasks.byIds' }),
      subscribe('bob', 'e-args', []),
      subscribe('bob', 'e-filters', grid({ filters: 'x' })),
      subscribe('bob', 'e-column', grid({ filters: [{ column: 'secret', operator: 'equals', value: '1' }] })),
      subscribe('bob', 'e-operator', grid({ filters: [{ column: 'name', operator: 'regex', value: '1' }] })),
      subscribe('bob', 'e-value', grid({ filters: [{ column: 'name', operator: 'equals', value: 1 }] })),
      subscribe('bob', 'e-number', grid({ filters: [{ column: 'id', operator: 'lessThan', value: 'ten' }] })),
      subscribe('bob', 'e-range', grid({ filters: [{ column: 'id', operator: 'inRange', value: '1' }] })),
      subscribe('bob', 'e-oneof', grid({ filters: [{ column: 'id', operator: 'oneOf', value: 'nope' }] })),
    ] },
  ];
}

/**
 * What each backend did, for the golden generator: the actions in order with
 * the revision each one saw (a write's committed revision, a subscription's
 * snapshot revision), and the raw frames of the watching sessions per step.
 */
export const RECORDINGS = new Map();

const SESSIONS = ['bob', 'carol'];
const isQuery = (frame) => typeof frame?.type === 'string' && frame.type.startsWith('query.');

export default {
  name: 'live-query-task-grid',
  description: 'Whagons task grid Live Query: windows, index sort, search, filters, visibility, inRelation, resume, one-shot plans, errors',
  tenants: { a: ['alice', 'bob', 'carol'] },
  tables: [],
  async run(ctx) {
    const m = (name) => ctx.members[name].memberIds.a;
    const record = { members: Object.fromEntries(Object.entries(ctx.members).map(([name, v]) => [name, { accountId: v.accountId, memberId: v.memberIds.a }])), actions: [], frames: {} };
    RECORDINGS.set(ctx.backend.name, record);
    const sockets = {};
    const latest = new Map(); // subscription id -> windowRevision of its latest result (bob)
    const seen = {};
    const capture = (stepIndex) => {
      for (const session of SESSIONS) {
        for (const dir of ['in', 'out']) {
          const frames = ctx.frames(session, { dir });
          const fresh = frames.slice(seen[`${session}:${dir}`] ?? 0);
          seen[`${session}:${dir}`] = frames.length;
          for (const frame of fresh) if (dir === 'in' && frame.type === 'query.result' && frame.windowRevision) latest.set(`${session}:${frame.id}`, frame.windowRevision);
          (record.frames[session] ??= []).push(...fresh.filter(isQuery).map(frame => ({ step: stepIndex, dir, frame })));
        }
      }
    };
    let writer;
    await ctx.step('connect alice, bob and carol', async () => {
      writer = await ctx.raw('alice');
      const s = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      for (const name of SESSIONS) {
        const socket = await ctx.raw(name);
        const session = await ctx.rawLogin(socket, name);
        await socket.auth({ project: ctx.project, tenant: ctx.tenants.a, token: session.accessToken, capabilities: { ...RAW_CAPABILITIES, queryPagePatch: 1, queryObjectPatch: 1 } });
        sockets[name] = socket;
      }
      capture(0);
    });
    const steps = script(m);
    for (const [index, { step, actions }] of steps.entries()) {
      await ctx.step(step, async () => {
        for (const action of actions) {
          if (action.write) {
            const id = randomUUID();
            writer.send({ type: 'reducer.call', id, path: action.write, args: action.args });
            const reply = await writer.next(f => (f.type === 'reducer.result' || f.type === 'reducer.error') && f.id === id, { label: `${action.write} reply` });
            ctx.expect(reply.type === 'reducer.result', `${action.write} ${JSON.stringify(action.args).slice(0, 80)}: ${reply.error}`);
            record.actions.push({ step: index + 1, write: action.write, args: action.args, revision: reply.committedRevision ?? null });
            // Once windows are open, let every rerun finish before the next write: Rust reruns
            // asynchronously at the then-current revision, so writes racing a rerun are coalesced
            // into one result depending on timing.
            if (index > 0) await ctx.quiesce();
            continue;
          }
          const socket = sockets[action.send];
          const frame = { ...action.frame };
          const resumeOf = frame.resumeOf;
          if (resumeOf) {
            capture(index + 1);
            frame.windowRevision = latest.get(`${action.send}:${resumeOf}`);
            delete frame.resumeOf;
          }
          socket.send(frame);
          const ids = frame.type === 'query.subscribeMany' ? frame.subscribes.map(s => s.id) : frame.type === 'query.unsubscribe' ? [] : [frame.id];
          const replies = [];
          for (const id of ids) {
            replies.push(await socket.next(f => f.id === id && ['query.result', 'query.progress', 'query.error'].includes(f.type), { label: `${frame.type} ${id}` }));
          }
          const revisions = replies.map(r => r.subscriptionRevision?.sequence ?? r.throughRevision?.sequence ?? null);
          record.actions.push({ step: index + 1, send: action.send, frame, revisions, ...(resumeOf ? { resumeOf } : {}) });
          ctx.observe(`${action.send}:${frame.type}:${ids.join(',') || frame.id}`, replies.map(r => r.type === 'query.error' ? { error: r.error } : { type: r.type }));
        }
        await ctx.quiesce();
        capture(index + 1);
      });
    }
  },
};
