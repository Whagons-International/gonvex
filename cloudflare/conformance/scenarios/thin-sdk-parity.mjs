/**
 * Port of bench/whagons/thin-conformance.ts to the fixture bundle.
 *
 * The same Member is signed in twice, once through the real SDK and once
 * through the thin client. An actor applies a scripted sequence: creates,
 * edits, a share and revoke (row gain and loss through visibility), a
 * collection opened and released (replica.close after retention), the
 * observed sessions' own Reducers, a dropped connection and edits during
 * recovery. After every step both sessions must hold the same rows, and must
 * have sent the same frames (types, paths, arguments; ids, clocks and
 * snapshot ACKs excluded). Across backends, the differ compares the rest.
 */
import { watch, call, project } from './helpers.mjs';

const COLLECTIONS = { board: ['id', 'title', 'position', 'bumps'], notes: ['id', 'owner_id', 'body', 'version'] };

const signature = (m) => {
  switch (m.type) {
    case 'auth': return `auth controlOnly=${m.controlOnly} tenant=${!!m.tenant} contract=${m.clientContract}`;
    case 'action.call': return `action ${m.path}`;
    case 'reducer.call': return `reducer ${m.path} args=${JSON.stringify(m.args)}`;
    // An integrity reopen answers the server's replica.needHashes; it is not client-initiated.
    case 'replica.openMany': return m.opens.every(o => o.fullIntegrity) ? 'integrity-reopen' : 'open ' + m.opens.map(o => `${o.path}${JSON.stringify(o.args)}`).sort().join(' | ');
    case 'replica.open': return `open ${m.path}${JSON.stringify(m.args)}`;
    case 'replica.snapshotAck': return 'ack';
    default: return m.type;
  }
};

export default {
  name: 'thin-sdk-parity',
  description: 'thin-conformance port: SDK and thin client for one Member stay equal in rows and frames',
  tenants: { a: ['alice', 'bob'] },
  async run(ctx) {
    const { api } = ctx;
    const pair = { sdk: undefined, thin: undefined };
    const views = { sdk: {}, thin: {} };
    let actor;
    const marks = {};
    const mark = () => { for (const tag of ['alice-sdk', 'alice-thin']) marks[tag] = ctx.frames(tag, { dir: 'out' }).length; };
    const sent = (tag) => ctx.frames(tag, { dir: 'out' }).slice(marks[tag] ?? 0).map(signature).filter(s => s !== 'ack' && s !== 'integrity-reopen' && !s.startsWith('telemetry') && !s.startsWith('presence')).sort();
    const rows = (side) => Object.fromEntries(Object.entries(views[side]).map(([name, v]) => [name, v.ready() ? project(v.rows(), COLLECTIONS[name]) : '<not ready>']));
    /** Waits until both sides are ready and equal (and stay equal for 500 ms), then records them. */
    const converge = async (check = () => true, label = 'sdk and thin agree') => {
      const same = () => { const a = JSON.stringify(rows('sdk')), b = JSON.stringify(rows('thin')); return !a.includes('<not ready>') && a === b && check(rows('sdk')); };
      await ctx.until(same, label);
      await ctx.delay(500);
      ctx.expect(same(), `${label}: diverged after converging\n  sdk  ${JSON.stringify(rows('sdk'))}\n  thin ${JSON.stringify(rows('thin'))}`);
      ctx.observe('rows', rows('sdk'));
      const a = sent('alice-sdk'), b = sent('alice-thin');
      ctx.expect(JSON.stringify(a) === JSON.stringify(b), `frames sent differ\n  sdk  ${a.join('; ')}\n  thin ${b.join('; ')}`);
      ctx.observe('sent', a);
    };
    const step = (name, fn, check, label) => ctx.step(name, async () => { mark(); await fn(); await converge(check, label); });

    await step('sign in and mount collections', async () => {
      // Both log in themselves, so both send the same frames.
      pair.sdk = await ctx.sdk('alice-sdk', { member: 'alice', fresh: true });
      pair.thin = await ctx.thin('alice-thin', { member: 'alice', fresh: true });
      for (const side of ['sdk', 'thin']) views[side].board = watch(pair[side], api.board.all);
      actor = await ctx.sdk('actor', { member: 'bob' });
    });
    await step('actor creates rows', async () => {
      await call(actor, api.board.add, { id: 'c1', title: 'Conformance 1', position: 1 });
      await call(actor, api.board.add, { id: 'c2', title: 'Conformance 2', position: 2 });
    }, r => r.board.length === 2);
    await step('actor renames and bumps', async () => {
      await call(actor, api.board.rename, { id: 'c1', title: 'Conformance renamed' });
      await call(actor, api.board.bump, { id: 'c1' });
    }, r => r.board[0]?.bumps === 1);
    await step('mount the notes collection', async () => {
      for (const side of ['sdk', 'thin']) views[side].notes = watch(pair[side], api.notes.mine);
    });
    await step('actor shares a note with the member (row gain)', async () => {
      await call(actor, api.notes.write, { id: 'bob-note', body: 'from bob' });
      await call(actor, api.notes.share, { id: 'share-1', noteId: 'bob-note', memberId: ctx.members.alice.memberIds.a });
    }, r => r.notes.length === 1);
    await step('actor edits the shared note', async () => {
      await call(actor, api.notes.write, { id: 'bob-note', body: 'from bob, edited' });
    }, r => r.notes[0]?.version === 2);
    await step('actor revokes the share (row loss)', async () => {
      await call(actor, api.notes.unshare, { id: 'share-1' });
    }, r => r.notes.length === 0);
    await step('observed sessions call their own reducers', async () => {
      // Same call from both sessions: the second rewrites the first (version 2).
      await call(pair.sdk, api.notes.write, { id: 'alice-note', body: 'from alice' });
      await call(pair.thin, api.notes.write, { id: 'alice-note', body: 'from alice' });
    }, r => r.notes.length === 1 && r.notes[0].version === 2);
    await step('notes collection released (replica.close after retention)', async () => {
      for (const side of ['sdk', 'thin']) { views[side].notes.stop(); delete views[side].notes; }
      await ctx.delay(600);
    });
    await step('network drop and reconnect on both sessions', async () => {
      const closed = (c) => new Promise(resolve => c.socket.addEventListener('close', resolve, { once: true }));
      const waits = [closed(pair.sdk), closed(pair.thin)];
      const auths = (tag) => ctx.frames(tag, { dir: 'out', type: 'auth' }).length;
      const before = { sdk: auths('alice-sdk'), thin: auths('alice-thin') };
      pair.sdk.socket.close(4001, 'conformance network drop');
      pair.thin.socket.close(4001, 'conformance network drop');
      await Promise.all(waits);
      // Both reconnect on their own backoff; wait for it inside this step.
      await ctx.until(() => auths('alice-sdk') > before.sdk && auths('alice-thin') > before.thin && views.sdk.board.ready() && views.thin.board.ready(), 'both sessions reconnected');
    });
    await step('actor edits during recovery are delivered', async () => {
      await call(actor, api.board.rename, { id: 'c2', title: 'After reconnect' });
    }, r => r.board.some(x => x.title === 'After reconnect'));
  },
};
