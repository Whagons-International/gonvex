/**
 * contract-changes 0062, separate multi-holder regression (not a parity comparison with Rust).
 *
 * Telemetry windows belong to the gateway holder that owns the socket (0012 R11). Under automatic placement an
 * account's sockets can sit on different holders, so this checks the per-holder domain itself, on the actual
 * holders the deployment reports (`/__assembly/holders`, fixture key):
 *  - same-account sockets on ONE holder share the 120-event window;
 *  - sockets on DIFFERENT holders have independent 120-event windows;
 *  - anonymous connections keep their own 20-event window each;
 *  - performance events are charged to the same account window as error frames.
 * Runs only where telemetry spans several holders (`multiHolderTelemetry`); the one-holder parity comparison
 * is `error-telemetry` (`singleTelemetryHolder`).
 */
const ACCOUNT_LIMIT = 120, CONNECTION_LIMIT = 20, MAX_SOCKETS = 24;

export default {
  name: 'error-telemetry-holders',
  description: '0062 multi-holder regression: same-holder sockets share the 120-event window, distinct holders do not; anonymous 20 per connection; performance events share the domain',
  requires: ['multiHolderTelemetry'],
  tenants: { a: ['alice'] },
  tables: [],
  async run(ctx) {
    const backend = ctx.backend, project = ctx.project;
    const ack = (s, id) => s.next((m) => m.type === 'error.ack' && m.id === id, { label: `error.ack ${id}` });
    let n = 0;
    const heartbeat = async (s) => { const id = `hb-${n++}`; s.send({ type: 'error.heartbeat', id }); return ack(s, id); };
    /** Alice's window count on every holder that has one of this project's sockets. */
    const windows = async () => {
      const { holders } = await backend.inspectHolders(project);
      return new Map(holders.map((h) => [h.holder, h.sockets.find((s) => s.accountId === ctx.members.alice.accountId)?.window?.count ?? 0]));
    };
    const flood = async (s, max) => { let accepted = 0; for (let i = 0; i < max; i++) { if ((await heartbeat(s)).error) return { accepted, refused: true }; accepted++; } return { accepted, refused: false }; };

    const alice = []; // { socket, holder }
    await ctx.step('map alice sockets to their actual holders', async () => {
      const session = await (async () => { const s = await ctx.raw('alice-0'); const login = await ctx.rawLogin(s, 'alice'); await s.auth({ project, tenant: ctx.tenants.a, token: login.accessToken }); alice.push({ socket: s }); return login; })();
      const seen = () => { const by = new Map(); for (const a of alice) by.set(a.holder, (by.get(a.holder) ?? 0) + 1); return by; };
      for (let i = 0; ; i++) {
        if (i > 0) { const s = await ctx.raw(`alice-${i}`); await s.auth({ project, tenant: ctx.tenants.a, token: session.accessToken }); alice.push({ socket: s }); }
        const before = await windows();
        ctx.expect(!(await heartbeat(alice[i].socket)).error, `mapping heartbeat ${i} accepted`);
        const after = await windows();
        const moved = [...after].filter(([h, c]) => c === (before.get(h) ?? 0) + 1).map(([h]) => h);
        ctx.expect(moved.length === 1, `heartbeat ${i} charged exactly one holder (${moved.length})`);
        alice[i].holder = moved[0];
        const by = seen();
        if ((by.size >= 2 && [...by.values()].some((c) => c >= 2)) || alice.length >= MAX_SOCKETS) break;
      }
      const by = seen();
      ctx.observe('mapped', { sockets: alice.length, holders: by.size, colocated: [...by.values()].some((c) => c >= 2) });
      ctx.expect(by.size >= 2, 'alice sockets span at least two holders');
      ctx.expect([...by.values()].some((c) => c >= 2), 'two alice sockets share a holder');
    });

    await ctx.step('same-holder sockets share one 120-event window', async () => {
      const groups = new Map(); for (const a of alice) groups.set(a.holder, [...(groups.get(a.holder) ?? []), a]);
      const [holder, [a, b]] = [...groups].find(([, g]) => g.length >= 2) ?? [undefined, []];
      if (!a) return;
      const r = await flood(a.socket, ACCOUNT_LIMIT + 5);
      ctx.expect(r.refused, 'the first socket is eventually refused');
      ctx.expect((await windows()).get(holder) === ACCOUNT_LIMIT, `the shared window holds exactly ${ACCOUNT_LIMIT}`);
      ctx.expect(!!(await heartbeat(b.socket)).error, 'the other socket on the same holder is refused at once');
      ctx.observe('sameHolderRefusedOnSecondSocket', true);
    });

    await ctx.step('a socket on another holder keeps its own window; performance events share it', async () => {
      const exhausted = new Set([...(await windows())].filter(([, c]) => c >= ACCOUNT_LIMIT).map(([h]) => h));
      const other = alice.find((a) => !exhausted.has(a.holder));
      ctx.expect(!!other, 'a socket on a holder whose window is not exhausted');
      if (!other) return;
      const before = (await windows()).get(other.holder);
      for (let i = 0; i < 5; i++) other.socket.send({ type: 'telemetry.event', id: `perf-${i}`, kind: 'query', path: 'k15.query', outcome: 'ok', clientReceivedAtMs: 1 });
      await heartbeat(other.socket); // a reply after the performance frames: they were handled in order
      ctx.expect((await windows()).get(other.holder) === before + 6, 'five performance events and one heartbeat charge the same window');
      const r = await flood(other.socket, ACCOUNT_LIMIT + 5);
      ctx.expect(r.refused, 'the other holder refuses only at its own limit');
      ctx.expect((await windows()).get(other.holder) === ACCOUNT_LIMIT, `its window reached exactly ${ACCOUNT_LIMIT} independently`);
      ctx.observe('independentHolderReachedLimit', true);
    });

    await ctx.step('anonymous connections each keep a 20-event window', async () => {
      const anon = [];
      for (let i = 0; i < 2; i++) { const s = await ctx.raw(`anon-${i}`); await s.auth({ project }); anon.push(s); }
      for (const [i, s] of anon.entries()) {
        const r = await flood(s, CONNECTION_LIMIT + 5);
        ctx.expect(r.refused && r.accepted === CONNECTION_LIMIT, `anonymous connection ${i} accepted exactly ${CONNECTION_LIMIT} (${r.accepted})`);
      }
      ctx.observe('anonymousPerConnection', CONNECTION_LIMIT);
    });
  },
};
