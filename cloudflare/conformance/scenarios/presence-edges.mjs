/**
 * Presence v2 edges the `presence` scenario does not reach (K14), on raw
 * sockets so every frame is compared: state parsing (invalid ignored, trimmed
 * and case-insensitive, `gone` = offline without grace), a re-auth that
 * re-announces inside the grace (no transition), a re-auth dropping the
 * subscription, unsubscribe and resubscribe, a socket without the `presence`
 * capability, tenant isolation, and a dropped socket going offline after the
 * 10 s grace (`CONTRACT.md` E.e).
 */
import { RAW_CAPABILITIES } from './helpers.mjs';

const CAPS = { ...RAW_CAPABILITIES, presence: 1 };
/** Rust's offline grace (`GONVEX_PRESENCE_OFFLINE_GRACE_MS`) and the window after it (presence-v2.md 3.4). */
const GRACE_MS = 10_000;

export default {
  name: 'presence-edges',
  description: 'Presence: state parsing, gone without grace, re-auth inside the grace, unsubscribe, no capability, tenant isolation, offline after grace',
  tenants: { a: ['alice', 'bob', 'carol', 'dave'], b: ['erin'] },
  async run(ctx) {
    const sockets = {};
    const ids = (name, tenant = 'a') => ctx.members[name].memberIds[tenant];
    /** A raw socket logged in and authenticated as `member` in `tenant`. */
    const open = async (tag, member, tenant = 'a', capabilities = CAPS) => {
      const raw = await ctx.raw(tag);
      const session = await ctx.rawLogin(raw, member);
      await raw.auth({ project: ctx.project, tenant: ctx.tenants[tenant], token: session.accessToken, capabilities });
      raw.member = member; raw.tenant = tenant; raw.token = session.accessToken; raw.capabilities = capabilities;
      sockets[tag] = raw;
      return raw;
    };
    const names = () => new Map(Object.entries(ctx.members).flatMap(([name, m]) => Object.entries(m.memberIds).map(([t, id]) => [id, `${name}@${t}`])));
    const byAlias = (id) => names().get(id) ?? id;
    const codePoints = (v) => [...v].map(c => c.codePointAt(0).toString(16).padStart(6, '0')).join('');
    const reauth = (raw) => raw.auth({ project: ctx.project, tenant: ctx.tenants[raw.tenant], token: raw.token, capabilities: raw.capabilities });
    const snapshot = async (raw, id) => {
      raw.send({ type: 'presence.subscribe', id });
      const m = await raw.next(x => x.type === 'presence.snapshot' && x.id === id, { label: `snapshot ${id}` });
      // Sorted by Member id on the wire (Rust String order); ids differ per backend, so observe by name.
      const raw_ = m.members.map(x => x.memberId);
      ctx.expect(raw_.every((v, i) => i === 0 || codePoints(raw_[i - 1]) < codePoints(v)), `snapshot ${id} is not sorted by Member id: ${raw_.join(', ')}`);
      return m.members.map(x => [x.memberId, x.state]).sort(([a], [b]) => byAlias(a).localeCompare(byAlias(b)));
    };
    /** The next patch on `raw` that mentions `member`. */
    const patch = async (raw, member, state) => {
      const m = await raw.next(x => x.type === 'presence.patch' && x.changes.some(c => c.memberId === member && c.state === state), { label: `${raw.member} sees ${member} ${state}` });
      return m.changes.map(c => [c.memberId, c.state]);
    };
    /** Patches still unread on `raw` (after a quiet period). */
    const unread = (raw) => raw.inbox.filter(m => m.type === 'presence.patch' || m.type === 'presence.snapshot').map(m => m.type);

    await ctx.step('watchers subscribe in two tenants', async () => {
      const carol = await open('carol-watch', 'carol');
      const erin = await open('erin-watch', 'erin', 'b');
      ctx.observe('carol', await snapshot(carol, 'carol-1'));
      ctx.observe('erin', await snapshot(erin, 'erin-1'));
    });

    await ctx.step('invalid states are ignored; states are trimmed and case-insensitive', async () => {
      const alice = await open('alice', 'alice');
      alice.send({ type: 'presence.set', state: 'busy' });
      alice.send({ type: 'presence.set', state: '  ACTIVE\t' });
      ctx.observe('patch', await patch(sockets['carol-watch'], ids('alice'), 'active'));
      await ctx.quiesce();
      ctx.observe('carol unread', unread(sockets['carol-watch']));
      ctx.observe('erin unread (other tenant)', unread(sockets['erin-watch']));
    });

    await ctx.step('"gone" is offline without grace', async () => {
      const at = Date.now();
      sockets.alice.send({ type: 'presence.set', state: 'Gone' });
      ctx.observe('patch', await patch(sockets['carol-watch'], ids('alice'), 'offline'));
      ctx.expect(Date.now() - at < GRACE_MS / 2, `explicit offline took ${Date.now() - at} ms; it must skip the grace`);
    });

    await ctx.step('re-auth that re-announces inside the grace shows no transition', async () => {
      sockets.alice.send({ type: 'presence.set', state: 'active' });
      ctx.observe('patch', await patch(sockets['carol-watch'], ids('alice'), 'active'));
      await reauth(sockets.alice);
      sockets.alice.send({ type: 'presence.set', state: 'active' });
      // Past the grace: the reset published nothing because the Member never left.
      await new Promise(r => setTimeout(r, GRACE_MS + 1_500));
      ctx.observe('carol unread', unread(sockets['carol-watch']));
    });

    await ctx.step('a socket without the presence capability can report and subscribe', async () => {
      const dave = await open('dave-nocap', 'dave', 'a', RAW_CAPABILITIES);
      dave.send({ type: 'presence.set', state: 'idle' });
      ctx.observe('patch', await patch(sockets['carol-watch'], ids('dave'), 'idle'));
      ctx.observe('dave', await snapshot(dave, 'dave-1'));
    });

    await ctx.step('unsubscribe stops patches; resubscribe gets a snapshot', async () => {
      const bob = await open('bob-watch', 'bob');
      ctx.observe('bob', await snapshot(bob, 'bob-1'));
      sockets['carol-watch'].send({ type: 'presence.unsubscribe' });
      await ctx.quiesce();
      sockets.alice.send({ type: 'presence.set', state: 'idle' });
      ctx.observe('bob patch', await patch(bob, ids('alice'), 'idle'));
      await ctx.quiesce();
      ctx.observe('carol unread', unread(sockets['carol-watch']));
      ctx.observe('carol', await snapshot(sockets['carol-watch'], 'carol-2'));
    });

    await ctx.step('a re-auth drops the subscription until the client subscribes again', async () => {
      const carol = sockets['carol-watch'];
      await reauth(carol);
      sockets.alice.send({ type: 'presence.set', state: 'active' });
      ctx.observe('bob patch', await patch(sockets['bob-watch'], ids('alice'), 'active'));
      await ctx.quiesce();
      ctx.observe('carol unread', unread(carol));
      ctx.observe('carol', await snapshot(carol, 'carol-3'));
    });

    await ctx.step('tenants never share presence', async () => {
      const erin = await open('erin', 'erin', 'b');
      erin.send({ type: 'presence.set', state: 'active' });
      ctx.observe('erin patch', await patch(sockets['erin-watch'], ids('erin', 'b'), 'active'));
      await ctx.quiesce();
      ctx.observe('carol unread', unread(sockets['carol-watch']));
      ctx.observe('bob unread', unread(sockets['bob-watch']));
    });

    await ctx.step('a dropped socket goes offline after the grace', async () => {
      const at = Date.now();
      sockets.alice.drop();
      ctx.observe('patch', await patch(sockets['carol-watch'], ids('alice'), 'offline'));
      const elapsed = Date.now() - at;
      ctx.expect(elapsed >= GRACE_MS - 500 && elapsed <= GRACE_MS + 2_500, `offline after ${elapsed} ms; the contract's grace is ${GRACE_MS} ms`);
      ctx.observe('bob patch', await patch(sockets['bob-watch'], ids('alice'), 'offline'));
    });
  },
};
