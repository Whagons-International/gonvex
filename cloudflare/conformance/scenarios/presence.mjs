/**
 * Presence v2 transitions (capability `presence`): snapshot on subscribe,
 * patches for active / idle / offline, offline on a dropped socket, the same
 * Member on two sockets, and refusal on a control-only session. Watched with
 * the real SDK (PresenceFeed) and the thin client (presenceV2).
 */
import { RAW_CAPABILITIES } from './helpers.mjs';

const view = (client) => {
  const s = client.presenceSnapshot();
  return { supported: s.supported, ready: s.ready, members: Object.fromEntries([...s.members].sort(([a], [b]) => a.localeCompare(b)).map(([id, m]) => [id, m.state])) };
};

export default {
  name: 'presence',
  description: 'Presence transitions: snapshot, active/idle/offline patches, socket drop, two sockets, control-only refusal',
  tenants: { a: ['alice', 'bob', 'carol'] },
  async run(ctx) {
    let watcher, thinWatcher, bob, bob2;
    const has = (client, expected) => {
      const s = client.presenceSnapshot();
      if (!s.ready) return false;
      const got = Object.fromEntries([...s.members].map(([id, m]) => [id, m.state]));
      return Object.entries(expected).every(([id, state]) => (state === undefined ? !(id in got) : got[id] === state));
    };
    const ids = {};
    const both = async (expected, label) => {
      const e = Object.fromEntries(Object.entries(expected).map(([n, s]) => [ids[n], s]));
      await ctx.until(() => has(watcher, e) && has(thinWatcher, e), label);
      ctx.observe('sdk', view(watcher));
      ctx.observe('thin', view(thinWatcher));
    };
    await ctx.step('watchers subscribe', async () => {
      watcher = await ctx.sdk('alice-sdk', { member: 'alice' });
      thinWatcher = await ctx.thin('carol-thin', { member: 'carol', options: { presenceV2: true } });
      for (const n of ['alice', 'bob', 'carol']) ids[n] = ctx.members[n].memberIds.a;
      ctx.observe('supported', { sdk: watcher.presenceSupported(), thin: thinWatcher.presenceSupported() });
      // Subscribe first, then report: the snapshot is taken before the
      // transition, which then arrives as a patch. One watcher after the
      // other, so carol's snapshot already contains alice.
      watcher.subscribePresence(() => {});
      await ctx.until(() => watcher.presenceSnapshot().ready, 'alice snapshot');
      watcher.setPresence('active');
      await ctx.until(() => has(watcher, { [ids.alice]: 'active' }), 'alice sees herself');
      thinWatcher.subscribePresence(() => {});
      await ctx.until(() => thinWatcher.presenceSnapshot().ready, 'carol snapshot');
      thinWatcher.setPresence('active');
      await both({ alice: 'active', carol: 'active' }, 'watchers see each other');
    });
    await ctx.step('bob becomes active', async () => {
      bob = await ctx.sdk('bob-sdk', { member: 'bob' });
      bob.setPresence('active');
      await both({ bob: 'active' }, 'bob active');
    });
    await ctx.step('bob goes idle', async () => {
      bob.setPresence('idle');
      await both({ bob: 'idle' }, 'bob idle');
    });
    await ctx.step('bob reports offline', async () => {
      bob.setPresence('offline');
      await both({ bob: undefined }, 'bob offline');
    });
    await ctx.step('bob on two sockets; one closes', async () => {
      bob.setPresence('active');
      bob2 = await ctx.sdk('bob-sdk-2', { member: 'bob' });
      bob2.setPresence('idle');
      await both({ bob: 'active' }, 'bob active on two sockets');
      bob.close();
      await both({ bob: 'idle' }, 'bob idle on the remaining socket');
    });
    await ctx.step('the last socket drops', async () => {
      bob2.socket.close(4001, 'conformance network drop');
      bob2.close();
      await both({ bob: undefined }, 'bob gone after socket drop');
    });
    await ctx.step('control-only session cannot subscribe', async () => {
      const raw = await ctx.raw('control-only');
      await raw.auth({ project: ctx.project, capabilities: { ...RAW_CAPABILITIES, presence: 1 } });
      raw.send({ type: 'presence.subscribe', id: 'presence-control-only' });
      const reply = await raw.next(m => m.type === 'presence.error' || m.type === 'presence.snapshot', { label: 'presence refusal', timeoutMs: 5000 }).catch(e => ({ type: 'none', error: e.message }));
      ctx.observe('reply', { type: reply.type, error: reply.error });
      raw.close();
    });
  },
};
