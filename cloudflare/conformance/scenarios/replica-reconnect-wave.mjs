/**
 * Reconnect waves: six SDK clients of three Members watch the board and their
 * own notes. First every socket drops at once while a writer commits; then the
 * backend itself restarts (when the backend can be restarted from here), so
 * every client reconnects at the same moment.
 * Each wave ends when every client has resumed (by cursor and digest, or a
 * snapshot) onto exactly the rows a fresh read shows, and a commit made after
 * the wave reaches all of them.
 */
import { watch, call, rowsWhen, project, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const BOARD = ['id', 'title', 'position', 'bumps'];
const NOTES = ['id', 'owner_id', 'body', 'version'];
const MEMBERS = ['alice', 'bob', 'carol'];

export default {
  name: 'replica-reconnect-wave',
  description: 'Six SDK clients drop at once with commits during the outage, then the backend restarts under them',
  tenants: { a: MEMBERS },
  async run(ctx) {
    const { api } = ctx;
    const clients = [];
    let writer;
    const board = (titles) => (rows) => JSON.stringify(rows.map((r) => [r.id, r.title, r.bumps])) === JSON.stringify(titles);
    const converge = async (label, boardRows, notesOf) => {
      for (const c of clients) {
        ctx.observe(`${c.tag}.board`, project(await rowsWhen(ctx, c.board, board(boardRows), `${c.tag} board: ${label}`), BOARD));
        const notes = notesOf(c.member);
        ctx.observe(`${c.tag}.notes`, project(await rowsWhen(ctx, c.notes, (rows) => JSON.stringify(rows.map((r) => [r.id, r.body])) === JSON.stringify(notes), `${c.tag} notes: ${label}`), NOTES));
      }
    };
    const connected = () => ctx.until(() => clients.every((c) => c.client.socket?.readyState === 1), 'every client reconnected');
    await ctx.step('six clients watch the board and their notes', async () => {
      writer = await ctx.raw('writer');
      const s = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      for (const [id, position] of [['w1', 1], ['w2', 2], ['w3', 3]]) await rawReducer(writer, 'board.add', { id, title: `row ${id}`, position });
      for (const member of MEMBERS) {
        for (const n of [1, 2]) {
          const tag = `${member}-${n}`;
          const client = await ctx.sdk(tag, { member });
          clients.push({ tag, member, client, board: watch(client, api.board.all), notes: watch(client, api.notes.mine) });
        }
        await call(clients.at(-1).client, api.notes.write, { id: `note-${member}`, body: `${member} first` });
      }
      await converge('seeded', [['w1', 'row w1', 0], ['w2', 'row w2', 0], ['w3', 'row w3', 0]], (m) => [[`note-${m}`, `${m} first`]]);
    });
    await ctx.step('every socket drops at once; commits during the outage', async () => {
      for (const c of clients) c.client.socket.close(4001, 'conformance network drop');
      ctx.observe('rename', await rawReducer(writer, 'board.rename', { id: 'w1', title: 'renamed in the outage' }));
      ctx.observe('remove', await rawReducer(writer, 'board.remove', { id: 'w2' }));
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 'w4', title: 'added in the outage', position: 4 }));
      ctx.observe('note', await rawReducer(writer, 'notes.write', { id: 'note-alice', body: 'alice in the outage' }));
      await converge('after the drop', [['w1', 'renamed in the outage', 0], ['w3', 'row w3', 0], ['w4', 'added in the outage', 0]],
        (m) => [[`note-${m}`, m === 'alice' ? 'alice in the outage' : `${m} first`]]);
      await connected();
    });
    await ctx.step('a commit after the first wave reaches everyone', async () => {
      ctx.observe('bump', await call(clients[2].client, api.board.bump, { id: 'w3' }));
      await converge('after the first wave', [['w1', 'renamed in the outage', 0], ['w3', 'row w3', 1], ['w4', 'added in the outage', 0]],
        (m) => [[`note-${m}`, m === 'alice' ? 'alice in the outage' : `${m} first`]]);
    });
    // A deployed Worker cannot be restarted from the harness; it runs the first wave only.
    if (!ctx.backend.capabilities.restart) return;
    await ctx.step('the backend restarts under every client', async () => {
      await ctx.restart();
      await ctx.until(() => clients.every((c) => c.client.socket?.readyState === 1), 'every client reconnected after the restart', 60_000);
      // Every client has resumed before the next commit (its snapshot must not race the commit).
      await converge('restarted', [['w1', 'renamed in the outage', 0], ['w3', 'row w3', 1], ['w4', 'added in the outage', 0]],
        (m) => [[`note-${m}`, m === 'alice' ? 'alice in the outage' : `${m} first`]]);
      await ctx.quiesce();
    });
    await ctx.step('a commit after the restart reaches everyone', async () => {
      ctx.observe('bump', await call(clients[4].client, api.board.bump, { id: 'w4' }));
      await converge('after the restart', [['w1', 'renamed in the outage', 0], ['w3', 'row w3', 1], ['w4', 'added in the outage', 1]],
        (m) => [[`note-${m}`, m === 'alice' ? 'alice in the outage' : `${m} first`]]);
    });
  },
};
