/**
 * Replica windows (K10): the parts of a collection definition beyond a plain
 * table, at frame level and through the SDK.
 *
 *  - A window of 1,205 rows arrives in several snapshot pages (256 KiB or
 *    512 rows each), merged by the normalizer into one snapshot.
 *  - A row budget (`maxRows: 3`, progressive) and a byte budget (`maxBytes:
 *    300`, descending) truncate their windows. A change inside a truncated
 *    window is a delta; a change outside it sends nothing to that window; a row
 *    entering or leaving moves the other edge, so the backend asks for row
 *    hashes (`replica.needHashes`) and the client reconciles.
 *  - An argument filter (`equalFilters`) and an exclusion (`excludeWhenSet`):
 *    rows enter and leave the group as they change group or are archived.
 *  - A socket without `replicaSnapshotPages` gets one `replica.snapshot` frame
 *    with the row hashes inline (fewer than 2,048 rows).
 *  - A resume of a truncated window with its cursor and aggregate digest.
 */
import { randomUUID } from 'node:crypto';
import { watch, call, rowsWhen, project, integrity, rawOpen, RAW_CAPABILITIES } from './helpers.mjs';

const ALL = ['id', 'grp', 'rank', 'label'];
const SHORT = ['id', 'rank', 'label'];
const PATHS = { all: 'windows.winAll', top: 'windows.winTop', bytes: 'windows.winBytes' };

/** A frame-level call that also returns the committed revision. */
async function commit(socket, path, args) {
  const id = randomUUID();
  socket.send({ type: 'reducer.call', id, path, args });
  const reply = await socket.next(m => (m.type === 'reducer.result' || m.type === 'reducer.error') && m.id === id, { label: `${path} reply` });
  if (reply.type === 'reducer.error') return { error: reply.error };
  return { result: reply.result, revision: reply.committedRevision };
}

/** Waits for the reader's watermark at `revision`, then takes every frame queued for `ids`, in order. */
async function settle(socket, revision, ids) {
  for (;;) {
    const w = await socket.next(m => m.type === 'replica.watermark', { label: `watermark ${revision}` });
    if (w.revision >= revision) break;
  }
  const frames = [];
  for (let i = 0; i < socket.inbox.length;) {
    if (ids.includes(socket.inbox[i].id)) frames.push(socket.inbox.splice(i, 1)[0]);
    else i++;
  }
  return frames;
}

/** What a window client holds after `frames` (deltas applied; needHashes noted). */
function apply(rows, frames) {
  const map = new Map(rows.map(r => [r.id, r]));
  let needHashes = false;
  for (const f of frames) {
    if (f.type === 'replica.delta') {
      for (const r of f.upserts ?? []) map.set(r.id, r);
      for (const k of f.deleted ?? []) map.delete(k);
    }
    if (f.type === 'replica.needHashes') needHashes = true;
  }
  return { rows: [...map.values()].sort((a, b) => a.id.localeCompare(b.id)), needHashes, types: frames.map(f => f.type) };
}

export default {
  name: 'replica-windows',
  description: 'Snapshot pages, row and byte budgets, truncated windows, argument filters and exclusions, a non-paged snapshot',
  tenants: { a: ['alice', 'bob'] },
  tables: ['win_items'],
  async run(ctx) {
    const { api } = ctx;
    let writer, reader;
    const subs = {};
    await ctx.step('fill 1,205 items in two groups', async () => {
      writer = await ctx.raw('writer');
      const s = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      ctx.observe('g1', (await commit(writer, 'windows.winFill', { prefix: 'a', grp: 'g1', from: 100, count: 1200 })).result);
      ctx.observe('g2', (await commit(writer, 'windows.winFill', { prefix: 'b', grp: 'g2', from: 10, count: 5 })).result);
    });
    await ctx.step('the whole table arrives in several pages', async () => {
      reader = await ctx.raw('reader');
      const s = await ctx.rawLogin(reader, 'bob');
      await reader.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      const all = await rawOpen(reader, { path: 'windows.winAll' });
      const pages = all.types.filter(t => t === 'replica.snapshotPage').length;
      ctx.expect(pages >= 3, `1,205 rows of ~100 bytes take at least 3 pages of 512 rows (got ${pages})`);
      ctx.observe('all', { rows: all.rows?.length, first: all.rows?.[0]?.id, last: all.rows?.at(-1)?.id, truncated: all.ready?.truncated, mode: all.ready?.mode });
      subs.all = all;
    });
    await ctx.step('row and byte budgets truncate their windows', async () => {
      subs.top = await rawOpen(reader, { path: 'windows.winTop' });
      subs.bytes = await rawOpen(reader, { path: 'windows.winBytes' });
      ctx.observe('top', { rows: project(subs.top.rows ?? [], SHORT), truncated: subs.top.ready?.truncated, mode: subs.top.ready?.mode });
      ctx.observe('bytes', { rows: project(subs.bytes.rows ?? [], SHORT), truncated: subs.bytes.ready?.truncated, mode: subs.bytes.ready?.mode });
      ctx.expect(subs.top.rows?.length === 3 && subs.top.ready?.truncated === true, 'maxRows 3 keeps three rows, truncated');
      ctx.expect(subs.bytes.ready?.truncated === true && (subs.bytes.rows?.length ?? 0) >= 1, 'maxBytes truncates the descending window');
    });
    const ids = () => [subs.all.id, subs.top.id, subs.bytes.id];
    const step = async (label, path, args) => {
      const done = await commit(writer, path, args);
      const frames = await settle(reader, done.revision, ids());
      const per = {};
      for (const key of ['all', 'top', 'bytes']) {
        const next = apply(subs[key].rows ?? [], frames.filter(f => f.id === subs[key].id));
        per[key] = { types: next.types, needHashes: next.needHashes };
        if (next.needHashes) {
          const columns = key === 'all' ? ALL : SHORT;
          const reopened = await rawOpen(reader, { id: subs[key].id, path: PATHS[key], cursor: subs[key].ready.cursor, ...integrity(subs[key].rows, 'id', columns), fullIntegrity: true }, subs[key].rows);
          subs[key] = { ...reopened, id: subs[key].id };
          per[key].reconciled = project(reopened.rows ?? [], columns).slice(0, 6);
        } else {
          subs[key] = { ...subs[key], rows: next.rows, ready: frames.filter(f => f.id === subs[key].id && f.type === 'replica.ready').at(-1) ?? subs[key].ready };
        }
      }
      ctx.observe(label, per);
    };
    await ctx.step('an edit inside the row-budget window', async () => {
      await step('inside', 'windows.winPut', { id: 'b-0001', grp: 'g2', rank: 11, label: 'edited inside the top window' });
      ctx.expect(JSON.stringify(project(subs.top.rows, SHORT).map(r => r.label)).includes('edited inside'), 'the top window holds the edit');
    });
    await ctx.step('an edit outside both truncated windows', async () => {
      await step('outside', 'windows.winPut', { id: 'a-0500', grp: 'g1', rank: 600, label: 'edited in the middle' });
    });
    await ctx.step('a row entering the top of the row-budget window', async () => {
      await step('enter', 'windows.winPut', { id: 'c-first', grp: 'g3', rank: 1, label: 'new first' });
      ctx.expect(JSON.stringify(subs.top.rows.map(r => r.id)) === JSON.stringify(['b-0000', 'b-0001', 'c-first']), `top reconciles to the three lowest ranks (got ${JSON.stringify(subs.top.rows.map(r => r.id))})`);
    });
    await ctx.step('a row entering the byte-budget window', async () => {
      await step('enterBytes', 'windows.winPut', { id: 'c-last', grp: 'g3', rank: 9000, label: 'new last' });
    });
    await ctx.step('a row leaving the row-budget window', async () => {
      await step('leave', 'windows.winDrop', { id: 'c-first' });
    });
    await ctx.step('resume the truncated window by cursor and digest', async () => {
      const top = subs.top;
      const resumed = await rawOpen(reader, { path: 'windows.winTop', cursor: top.ready.cursor, digest: integrity(top.rows, 'id', SHORT).digest }, top.rows);
      ctx.observe('resume', { types: resumed.types, rows: resumed.rows && project(resumed.rows, SHORT), truncated: resumed.ready?.truncated });
    });
    let sdk, group;
    await ctx.step('an argument filter and an exclusion through the SDK', async () => {
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      group = watch(sdk, api.windows.winGroup, { grp: 'g2' });
      ctx.observe('g2', project(await rowsWhen(ctx, group, rows => rows.length === 5, 'g2 snapshot'), ALL));
      ctx.observe('archive', await call(sdk, api.windows.winArchive, { id: 'b-0003' }));
      ctx.observe('move', await call(sdk, api.windows.winPut, { id: 'a-0000', grp: 'g2', rank: 50, label: 'moved to g2' }));
      ctx.observe('drop', await call(sdk, api.windows.winDrop, { id: 'b-0004' }));
      ctx.observe('g2 after', project(await rowsWhen(ctx, group, rows => JSON.stringify(rows.map(r => r.id)) === JSON.stringify(['a-0000', 'b-0000', 'b-0001', 'b-0002']), 'g2 after'), ALL));
    });
    await ctx.step('a socket without snapshot pages gets one snapshot frame', async () => {
      const plain = await ctx.raw('plain');
      const s = await ctx.rawLogin(plain, 'bob');
      const { replicaSnapshotPages: _, ...capabilities } = RAW_CAPABILITIES;
      await plain.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities });
      const opened = await rawOpen(plain, { path: 'windows.winGroup', args: { grp: 'g2' } });
      ctx.observe('plain', { types: opened.types, rows: opened.rows && project(opened.rows, ALL) });
      ctx.expect(opened.types[0] === 'replica.snapshot', 'a single replica.snapshot frame');
    });
  },
};
