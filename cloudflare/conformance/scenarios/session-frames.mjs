/**
 * Frame acceptance, close codes and size limits on the socket (CONTRACT.md
 * A.1.7, A.1.9, A.6 item 7; contract-changes 0018; package K05):
 *
 *  - the decoder's accept/close boundary: each vector goes on a fresh socket,
 *    followed by a probe `auth`; the socket either answers the probe or
 *    closes 1003 "invalid Gonvex protocol frame" (serde strictness: integer
 *    widths, number lexemes, null for required values, unknown `type`,
 *    sequence-form structs, nesting depth);
 *  - the public transport (`?public=1`): transport and `auth.public`
 *    mismatch, a non-public tenant, the 64 KiB transport limit (1009);
 *  - 64 KiB on unscoped sockets (1009 "public payload too large"), and no
 *    such limit once an identity is established;
 *  - a 16 MiB frame on a socket with an identity. Rust's transport refuses
 *    single frames over 16 MiB (no close frame); the kernel accepts up to the
 *    platform's 32 MiB (0018, contract-changes 0020), which the kernel's own
 *    workerd tests check;
 *  - the 10 s pending-auth deadline (1008), which a token-less control-only
 *    auth does not clear and a session auth does.
 *
 * Close outcomes: Rust ends some sockets without a close frame (the client
 * sees 1006), where the kernel sends a close frame (A.1.9, row "none", adapt).
 * `closeOutcome` reports those as "closed without a protocol code"; every
 * code the runtime sends (1002, 1003, 1008, 1009) is compared exactly. 1002 is
 * what Rust's compressed transport sends for an oversize public frame; the
 * 1009 "public transport limit exceeded" of A.1.9 never reaches the client.
 */
const PROTOCOL_CODES = new Set([1002, 1003, 1007, 1008, 1009, 4000]);
const closeOutcome = (event) => !event ? 'open'
  : PROTOCOL_CODES.has(event.code) ? `closed ${event.code} ${event.reason}`
  : 'closed without a protocol code';

/** [label, text]. Expected Rust outcome in the comment; the trace records it. */
const VECTORS = [
  ['not json', 'hello'],                                                                           // 1003
  ['json scalar', '5'],                                                                            // 1003
  ['unknown type', '{"type":"nope","id":"x"}'],                                                    // 1003
  ['missing type', '{"id":"x"}'],                                                                  // 1003
  ['type not a string', '{"type":5}'],                                                              // 1003
  ['ping frame', '{"type":"pong"}'],                                                               // 1003 (unknown type)
  ['unsubscribe ok', '{"type":"query.unsubscribe","id":"x"}'],
  ['unsubscribe extra field', '{"type":"query.unsubscribe","id":"x","extra":[1,{"a":null}]}'],
  ['unsubscribe missing id', '{"type":"query.unsubscribe"}'],                                      // 1003
  ['unsubscribe id number', '{"type":"query.unsubscribe","id":5}'],                                // 1003
  ['unsubscribe id null', '{"type":"query.unsubscribe","id":null}'],                               // 1003
  ['duplicate id', '{"type":"query.unsubscribe","id":"a","id":"b"}'],
  ['duplicate type', '{"type":"query.unsubscribe","type":"query.unsubscribe","id":"a"}'],
  ['sequence form', '["query.unsubscribe","x"]'],
  ['sequence form too long', '["query.unsubscribe","x","y"]'],
  ['sequence form missing', '["query.unsubscribe"]'],
  ['presence.unsubscribe empty', '{"type":"presence.unsubscribe"}'],
  ['presence.set free string', '{"type":"presence.set","state":"gone"}'],
  ['presence.set missing state', '{"type":"presence.set"}'],
  ['auth contract -1', '{"type":"auth","id":"v","clientContract":-1}'],
  ['auth contract 1.5', '{"type":"auth","id":"v","clientContract":1.5}'],
  ['auth contract 1.0', '{"type":"auth","id":"v","clientContract":1.0}'],
  ['auth contract 1e0', '{"type":"auth","id":"v","clientContract":1e0}'],
  ['auth contract u64 max', '{"type":"auth","id":"v","clientContract":18446744073709551615}'],
  ['auth contract u64 max + 1', '{"type":"auth","id":"v","clientContract":18446744073709551616}'],
  ['auth contract string', '{"type":"auth","id":"v","clientContract":"1"}'],
  ['auth contract null', '{"type":"auth","id":"v","clientContract":null}'],
  ['auth capability 2', '{"type":"auth","id":"v","capabilities":{"presence":2}}'],
  ['auth capability 255', '{"type":"auth","id":"v","capabilities":{"presence":255}}'],
  ['auth capability 256', '{"type":"auth","id":"v","capabilities":{"presence":256}}'],
  ['auth capability -0', '{"type":"auth","id":"v","capabilities":{"presence":-0}}'],
  ['auth capability string', '{"type":"auth","id":"v","capabilities":{"presence":"1"}}'],
  ['auth capability null', '{"type":"auth","id":"v","capabilities":{"presence":null}}'],
  ['auth capability unknown', '{"type":"auth","id":"v","capabilities":{"somethingNew":"x"}}'],
  ['auth capabilities array', '{"type":"auth","id":"v","capabilities":[]}'],
  ['auth capabilities array of 1', '{"type":"auth","id":"v","capabilities":[1]}'],
  ['auth capabilities number', '{"type":"auth","id":"v","capabilities":5}'],
  ['auth capabilities null', '{"type":"auth","id":"v","capabilities":null}'],
  ['auth controlOnly null', '{"type":"auth","id":"v","controlOnly":null}'],
  ['auth controlOnly 1', '{"type":"auth","id":"v","controlOnly":1}'],
  ['auth public string', '{"type":"auth","id":"v","public":"false"}'],
  ['auth token number', '{"type":"auth","id":"v","token":5}'],
  ['auth token null', '{"type":"auth","id":"v","token":null}'],
  ['auth device viewport 1.5', '{"type":"auth","id":"v","device":{"viewportWidth":1.5}}'],
  ['auth device viewport u32 max', '{"type":"auth","id":"v","device":{"viewportWidth":4294967295}}'],
  ['auth device viewport u32 max + 1', '{"type":"auth","id":"v","device":{"viewportWidth":4294967296}}'],
  ['auth device memory int', '{"type":"auth","id":"v","device":{"deviceMemory":8}}'],
  ['auth device memory string', '{"type":"auth","id":"v","device":{"deviceMemory":"8"}}'],
  ['auth device userAgent number', '{"type":"auth","id":"v","device":{"userAgent":5}}'],
  ['auth device null', '{"type":"auth","id":"v","device":null}'],
  ['auth id missing', '{"type":"auth","controlOnly":true}'],
  ['query.call no args', '{"type":"query.call","id":"q","path":"p"}'],
  ['query.call scope control', '{"type":"query.call","id":"q","path":"p","scope":"control"}'],
  ['query.call scope other', '{"type":"query.call","id":"q","path":"p","scope":"other"}'],
  ['query.call scope null', '{"type":"query.call","id":"q","path":"p","scope":null}'],
  ['query.call path missing', '{"type":"query.call","id":"q"}'],
  ['query.subscribe windowRevision number', '{"type":"query.subscribe","id":"q","path":"p","windowRevision":5}'],
  ['subscribeMany null', '{"type":"query.subscribeMany","subscribes":null}'],
  ['subscribeMany item missing path', '{"type":"query.subscribeMany","subscribes":[{"id":"q"}]}'],
  ['reducer.call contract float', '{"type":"reducer.call","id":"r","path":"p","clientContract":1e3}'],
  ['reducer.call trace string', '{"type":"reducer.call","id":"r","path":"p","trace":{"clientSentAtMs":"1"}}'],
  ['reducer.call trace int', '{"type":"reducer.call","id":"r","path":"p","trace":{"clientSentAtMs":1}}'],
  ['reducer.callMany not array', '{"type":"reducer.callMany","calls":{}}'],
  ['reducer.callMany empty', '{"type":"reducer.callMany","calls":[]}'],
  ['replica.open cursor revision string', '{"type":"replica.open","id":"o","path":"p","cursor":{"epoch":"e","revision":"5"}}'],
  ['replica.open cursor sequence form', '{"type":"replica.open","id":"o","path":"p","cursor":["e",5]}'],
  ['replica.open fullIntegrity null', '{"type":"replica.open","id":"o","path":"p","fullIntegrity":null}'],
  ['replica.open hashes number value', '{"type":"replica.open","id":"o","path":"p","hashes":{"a":5}}'],
  ['replica.open keys number', '{"type":"replica.open","id":"o","path":"p","keys":[1]}'],
  ['replica.openMany missing', '{"type":"replica.openMany"}'],
  ['replica.snapshotAck sequence -1', '{"type":"replica.snapshotAck","id":"o","snapshotId":"s","sequence":-1}'],
  ['telemetry.event missing clientReceivedAtMs', '{"type":"telemetry.event","id":"t","kind":"query","path":"p","outcome":"ok"}'],
  ['telemetry.event ok', '{"type":"telemetry.event","id":"t","kind":"anything","path":"p","outcome":"ok","clientReceivedAtMs":1.5}'],
  ['telemetry.batch null events', '{"type":"telemetry.batch","events":null}'],
  ['error.heartbeat id number', '{"type":"error.heartbeat","id":1}'],
  ['lone surrogate in id', '{"type":"query.unsubscribe","id":"\\ud800"}'],
  ['escaped pair in id', '{"type":"query.unsubscribe","id":"\\ud83d\\ude00"}'],
  ['nesting 126', `{"type":"query.call","id":"q","path":"p","args":${'['.repeat(125)}${']'.repeat(125)}}`],
  ['nesting 127', `{"type":"query.call","id":"q","path":"p","args":${'['.repeat(126)}${']'.repeat(126)}}`],
  ['nesting 128', `{"type":"query.call","id":"q","path":"p","args":${'['.repeat(127)}${']'.repeat(127)}}`],
  ['trailing garbage', '{"type":"query.unsubscribe","id":"x"} x'],
  ['trailing whitespace', '{"type":"query.unsubscribe","id":"x"} \n'],
];

/** `frame` as JSON text of exactly `bytes` UTF-8 bytes, padded in a `pad` field. */
const sized = (frame, bytes) => {
  const base = JSON.stringify({ ...frame, pad: '' });
  if (bytes < base.length) throw new Error(`frame needs at least ${base.length} bytes`);
  return JSON.stringify({ ...frame, pad: 'x'.repeat(bytes - base.length) });
};

const PROBE = { type: 'auth', id: 'probe', controlOnly: true, clientContract: 1 };

export default {
  name: 'session-frames',
  description: 'Decoder accept/close boundary (1003), public transport, 64 KiB and 32 MiB limits (1009), pending-auth deadline (1008)',
  tenants: { a: ['alice'] },
  async run(ctx) {
    const project = ctx.project;
    const opened = async (tag, options) => { const s = await ctx.raw(tag, options); await s.next({ type: 'session.ready' }); return s; };
    /** Sends `data`, then the probe; resolves to the probe's reply type or the close. */
    const outcome = async (socket, data) => {
      socket.sendRaw(data);
      socket.send(PROBE);
      try {
        const m = await socket.next(x => x.id === 'probe', { label: 'probe reply', timeoutMs: 10_000 });
        return `answered ${m.type}`;
      } catch (error) {
        if (!socket.closed) throw error;
        return closeOutcome(await socket.whenClosed(1_000));
      }
    };

    // Opened first, checked last: the deadline runs while the other steps do.
    let deadline, deadlineControl, deadlineSession, session;
    await ctx.step('three sockets start the 10 s auth deadline', async () => {
      deadline = await opened('deadline');
      deadlineControl = await opened('deadline-control');
      deadlineControl.send({ type: 'auth', id: 'dc', controlOnly: true, project, clientContract: 1 });
      ctx.observe('controlOnly', (await deadlineControl.next({ id: 'dc' })).type);
      deadlineSession = await opened('deadline-session');
      session = await ctx.rawLogin(deadlineSession, 'alice');
      deadlineSession.send({ type: 'auth', id: 'ds', project, token: session.accessToken, tenant: ctx.tenants.a, clientContract: 1 });
      ctx.observe('session', (await deadlineSession.next({ id: 'ds' })).type);
    });

    await ctx.step('decoder accept/close boundary', async () => {
      const results = {};
      for (const [label, text] of VECTORS) {
        const socket = await opened('decoder');
        results[label] = await outcome(socket, text);
        socket.close();
      }
      ctx.observe('vectors', results);
    });

    await ctx.step('binary frames', async () => {
      const small = await opened('binary');
      ctx.observe('smallUnscoped', await outcome(small, new Uint8Array([1, 2, 3])));
      small.close();
      const big = await opened('binary');
      ctx.observe('oversizeUnscoped', await outcome(big, new Uint8Array(64 * 1024 + 1)));
      big.close();
    });

    await ctx.step('public transport', async () => {
      const mismatch = await opened('public', { public: true });
      mismatch.send({ type: 'auth', id: 'pm', project, controlOnly: true, clientContract: 1 });
      const m = await mismatch.next({ id: 'pm' });
      ctx.observe('notPublicOnPublicTransport', { type: m.type, code: m.code, error: m.error, close: closeOutcome(await mismatch.whenClosed(5_000)) });

      const inverse = await opened('public');
      inverse.send({ type: 'auth', id: 'pi', project, tenant: ctx.tenants.a, public: true, clientContract: 1 });
      const i = await inverse.next({ id: 'pi' });
      ctx.observe('publicOnMemberTransport', { type: i.type, code: i.code, error: i.error, close: closeOutcome(await inverse.whenClosed(5_000)) });

      const notPublic = await opened('public', { public: true });
      notPublic.send({ type: 'auth', id: 'pn', project, tenant: ctx.tenants.a, public: true, clientContract: 1 });
      const n = await notPublic.next({ id: 'pn' });
      ctx.observe('nonPublicTenant', { type: n.type, code: n.code, error: n.error });
      notPublic.send({ type: 'auth', id: 'pn2', project, public: true, clientContract: 1 });
      const n2 = await notPublic.next({ id: 'pn2' });
      ctx.observe('publicWithoutTenant', { type: n2.type, code: n2.code, error: n2.error });
      ctx.observe('stillOpen', notPublic.closed === false);
      notPublic.close();

      const limit = await opened('public', { public: true });
      ctx.observe('exactly64KiB', await outcome(limit, sized({ type: 'query.unsubscribe', id: 'x' }, 64 * 1024)));
      limit.close();
      const over = await opened('public', { public: true });
      ctx.observe('over64KiB', await outcome(over, sized({ type: 'query.unsubscribe', id: 'x' }, 64 * 1024 + 1)));
      over.close();
    });

    await ctx.step('64 KiB on unscoped sockets, 16 MiB after auth', async () => {
      const pad = (bytes) => sized({ type: 'telemetry.batch', events: [] }, bytes);
      const unscoped = await opened('limits');
      ctx.observe('unscoped64KiB', await outcome(unscoped, pad(64 * 1024)));
      unscoped.close();
      const over = await opened('limits');
      ctx.observe('unscopedOver64KiB', await outcome(over, pad(64 * 1024 + 1)));
      over.close();
      const multibyte = await opened('limits');
      // 22,000 characters, 66,000 UTF-8 bytes: over the limit in bytes, not in UTF-16 units.
      ctx.observe('unscopedOver64KiBInBytes', await outcome(multibyte, JSON.stringify({ type: 'telemetry.batch', events: [], pad: '€'.repeat(22_000) })));
      multibyte.close();
      const controlOnly = await opened('limits');
      controlOnly.send({ type: 'auth', id: 'l-control', project, controlOnly: true, clientContract: 1 });
      await controlOnly.next({ id: 'l-control' });
      ctx.observe('controlOnlyWithoutTokenOver64KiB', await outcome(controlOnly, pad(100 * 1024)));
      controlOnly.close();
      const scoped = await opened('limits');
      scoped.send({ type: 'auth', id: 'l-token', project, controlOnly: true, token: session.accessToken, clientContract: 1 });
      await scoped.next({ id: 'l-token' });
      ctx.observe('identityOver64KiB', await outcome(scoped, pad(100 * 1024)));
      ctx.observe('identity16MiB', await outcome(scoped, pad(16 * 1024 * 1024)));
      scoped.close();
    });

    await ctx.step('the pending-auth deadline', async () => {
      const result = {};
      for (const [name, socket] of [['none', deadline], ['controlOnly', deadlineControl], ['session', deadlineSession]]) {
        const event = await socket.whenClosed(Math.max(0, socket.openedAt + 13_000 - Date.now()));
        result[name] = closeOutcome(event);
        if (event) ctx.expect(event.at - socket.openedAt >= 9_500 && event.at - socket.openedAt <= 12_500, `${name} closed after ${event.at - socket.openedAt} ms, expected about 10 s`);
      }
      ctx.observe('outcomes', result);
      deadlineSession.close();
    });
  },
};
