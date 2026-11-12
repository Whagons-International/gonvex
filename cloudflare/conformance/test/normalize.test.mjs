import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeSession, normalizeValue } from '../src/normalize.mjs';
import { diffFrames, diffTraces, diffValues, diffChecksums } from '../src/differ.mjs';

const rec = (dir, frame, socket = 1, step = 's') => ({ dir, frame, socket, step });

test('ids become first-appearance placeholders, kept consistent within a session', () => {
  const a = normalizeSession([
    rec('out', { type: 'reducer.call', id: '11111111-1111-4111-8111-111111111111', path: 'p', args: {}, trace: { clientSentAtMs: 5 } }),
    rec('in', { type: 'reducer.result', id: '11111111-1111-4111-8111-111111111111', originCommandId: 'command-000000001', committedRevision: 41, result: null }),
  ]);
  const b = normalizeSession([
    rec('out', { type: 'reducer.call', id: '99999999-9999-4999-8999-999999999999', path: 'p', args: {} }),
    rec('in', { type: 'reducer.result', id: '99999999-9999-4999-8999-999999999999', originCommandId: 'command-000000009', committedRevision: 7, result: null }),
  ]);
  assert.deepEqual(a, b);
  assert.equal(a[0].frame.id, '<id#1>');
  assert.equal(a[1].frame.id, '<id#1>');
  assert.equal(a[1].frame.originCommandId, '<id#2>');
  assert.equal(a[1].frame.committedRevision, 'r1');
});

test('meaningful short values under id-like keys are kept', () => {
  const [{ frame }] = normalizeSession([rec('in', { type: 'auth.error', id: 'membership-changed', error: 'x', code: 'revoked' }), rec('out', { type: 'action.call', id: 'x', scope: 'control' })]);
  assert.equal(frame.id, 'membership-changed');
  const [, { frame: call }] = normalizeSession([rec('in', { type: 'auth.error', id: 'membership-changed' }), rec('out', { type: 'action.call', id: 'x', scope: 'control' })]);
  assert.equal(call.scope, 'control');
});

test('the same id links across envelope and nested positions', () => {
  const sub = '22222222-2222-4222-8222-222222222222';
  const n = normalizeSession([rec('out', { type: 'replica.openMany', opens: [{ id: sub, path: 'p', args: {} }] }), rec('in', { type: 'replica.ready', id: sub, cursor: { epoch: 'e', revision: 1 } })]);
  assert.equal(n[0].frame.opens[0].id, n[1].frame.id);
});

test('revisions become ranks that keep order', () => {
  const n = normalizeSession([rec('in', { type: 'reducer.result', id: 'a', committedRevision: 90 }), rec('in', { type: 'reducer.result', id: 'b', committedRevision: 12 })]);
  assert.deepEqual(n.map(x => x.frame.committedRevision), ['r2', 'r1']);
});

test('times, digests, tokens and uuids are masked', () => {
  const [{ frame }] = normalizeSession([rec('in', { type: 'presence.patch', serverTime: 123, changes: [{ memberId: '0b9c1f7e-1111-4222-8333-944455556666', state: 'active', since: 99 }], digest: 'xor1:ab', token: 'gvx_session_secret' })]);
  assert.deepEqual(frame, { type: 'presence.patch', serverTime: '<time>', changes: [{ memberId: '<uuid#1>', state: 'active', since: '<time>' }], digest: '<digest>', token: '<token>' });
});

test('snapshot pages merge into one sorted snapshot regardless of page size', () => {
  const page = (sequence, last, result) => rec('in', { type: 'replica.snapshotPage', id: 's1', snapshotId: 'x', scope: 'q', visibilityScope: 'v', sequence, last, key: 'id', result, cursor: { epoch: 'e', revision: 3 } });
  const one = normalizeSession([page(0, true, [{ id: 'b' }, { id: 'a' }])]);
  const two = normalizeSession([page(0, false, [{ id: 'b' }]), page(1, true, [{ id: 'a' }])]);
  assert.deepEqual(one, two);
  assert.equal(one.length, 1);
  assert.equal(one[0].frame.type, 'replica.snapshot');
  assert.deepEqual(one[0].frame.result, [{ id: 'a' }, { id: 'b' }]);
});

test('replica.batch expands like separate frames', () => {
  const batched = normalizeSession([rec('in', { type: 'replica.batch', tenant: 't', class: 'c', from: 1, items: [
    { type: 'replica.stream', id: 'sub', stream: 'S' }, { type: 'replica.delta', id: 'S', cursor: { epoch: 'e', revision: 2 }, upserts: [{ id: 'a' }] },
  ], watermark: 2 })]);
  const plain = normalizeSession([rec('in', { type: 'replica.delta', id: 'sub', cursor: { epoch: 'e', revision: 2 }, upserts: [{ id: 'a' }] }), rec('in', { type: 'replica.watermark', revision: 2 })]);
  assert.deepEqual(batched, plain);
});

test('the resume cursor of an integrity reopen answering needHashes is masked, and nothing else', () => {
  const episode = (persisted) => [
    rec('in', { type: 'replica.watermark', revision: 5 }),
    rec('in', { type: 'replica.watermark', revision: 6 }),
    rec('in', { type: 'replica.needHashes', id: 'sub', path: 'p' }),
    rec('out', { type: 'replica.openMany', opens: [{ id: 'sub', path: 'p', args: {}, cursor: { epoch: 'e', revision: persisted }, fullIntegrity: true, keys: [], hashes: {} }] }),
    rec('in', { type: 'replica.delta', id: 'sub', cursor: { epoch: 'e', revision: 7 }, upserts: [{ id: 'row' }], deleted: [] }),
    rec('in', { type: 'replica.ready', id: 'sub', cursor: { epoch: 'e', revision: 7 }, digest: 'd' }),
  ];
  const a = normalizeSession(episode(5)), b = normalizeSession(episode(6));
  assert.deepEqual(a, b);
  assert.equal(a[3].frame.opens[0].cursor.revision, '<persisted-watermark>');
  assert.equal(a[5].frame.cursor.revision, 'r1'); // the only compared revision; watermark-only values do not rank
  // An integrity open the client sends on its own (no needHashes) keeps its cursor.
  const own = normalizeSession([rec('out', { type: 'replica.openMany', opens: [{ id: 'x', path: 'p', cursor: { epoch: 'e', revision: 4 }, fullIntegrity: true }] })]);
  assert.equal(own[0].frame.opens[0].cursor.revision, 'r1');
  // A plain reopen of an asked id (not fullIntegrity) keeps its cursor too.
  const plain = normalizeSession([rec('in', { type: 'replica.needHashes', id: 'y' }), rec('out', { type: 'replica.open', id: 'y', path: 'p', cursor: { epoch: 'e', revision: 4 } })]);
  assert.equal(plain[1].frame.cursor.revision, 'r1');
});

test('a watermark the backend coalesced away does not renumber later revisions', () => {
  const session = (quietWatermark) => normalizeSession([
    rec('in', { type: 'replica.ready', id: 'sub', cursor: { epoch: 'e', revision: 5 }, digest: 'd' }),
    ...(quietWatermark ? [rec('in', { type: 'replica.watermark', revision: 6 })] : []),
    rec('in', { type: 'replica.delta', id: 'sub', cursor: { epoch: 'e', revision: 7 }, upserts: [{ id: 'row' }], deleted: [] }),
    rec('in', { type: 'replica.watermark', revision: 7 }),
  ]).filter(e => e.frame.type !== 'replica.watermark');
  assert.deepEqual(session(true), session(false));
  assert.deepEqual(session(true).map(e => e.frame.cursor.revision), ['r1', 'r2']);
  const marks = normalizeSession([rec('in', { type: 'replica.delta', id: 's', cursor: { epoch: 'e', revision: 7 }, upserts: [], deleted: [] }), rec('in', { type: 'replica.watermark', revision: 6 }), rec('in', { type: 'replica.watermark', revision: 7 })]);
  assert.deepEqual(marks.slice(1).map(e => e.frame.revision), ['<barrier>', 'r1']);
});

test('malformed containers a scenario sends on purpose stay unexpanded', () => {
  const records = [
    { dir: 'out', socket: 1, step: 0, frame: { type: 'reducer.callMany', calls: {} } },
    { dir: 'out', socket: 1, step: 0, frame: { type: 'replica.openMany' } },
    { dir: 'in', socket: 1, step: 0, frame: { type: 'replica.batch', items: null } },
  ];
  const out = normalizeSession(records).map(r => r.frame.type);
  assert.deepEqual(out, ['reducer.callMany', 'replica.openMany', 'replica.batch']);
});

test('diffFrames reports field-level differences and policies', () => {
  const a = normalizeSession([rec('in', { type: 'reducer.error', id: '1', error: 'x', class: 'rejected' }), rec('in', { type: 'replica.watermark', revision: 1 })]);
  const b = normalizeSession([rec('in', { type: 'reducer.error', id: '1', error: 'x', class: 'transient' })]);
  const lines = diffFrames(a, b, ['rust', 'cf']).join('\n');
  assert.match(lines, /class: "rejected" != "transient"/);
  assert.match(lines, /in replica.watermark: rust sent, cf none/);
  assert.deepEqual(diffFrames(a, a), []);
});

test('watermark counts are not compared, only presence', () => {
  const a = normalizeSession([rec('in', { type: 'replica.watermark', revision: 1 }), rec('in', { type: 'replica.watermark', revision: 2 })]);
  const b = normalizeSession([rec('in', { type: 'replica.watermark', revision: 2 })]);
  assert.deepEqual(diffFrames(a, b), []);
});

test('diffValues and diffChecksums print readable paths', () => {
  assert.deepEqual(diffValues({ a: [1, { b: 2 }] }, { a: [1, { b: 3 }] }), ['a[1].b: 2 != 3']);
  const lines = diffChecksums({ t: { notes: { count: 1, sha256: 'aa', rows: [{ __key: 'n1', v: 1 }] } } }, { t: { notes: { count: 1, sha256: 'bb', rows: [{ __key: 'n1', v: 2 }] } } }, ['x', 'y']);
  assert.match(lines.join('\n'), /t\.notes: x 1 rows aa, y 1 rows bb/);
  assert.match(lines.join('\n'), /v: 1 != 2/);
});

test('diffTraces covers errors, observations and missing steps', () => {
  const t = (backend, steps) => ({ backend, steps, checksums: {} });
  const out = diffTraces(t('a', [{ name: 'one', observations: { n: 1 }, frames: {} }]), t('b', [{ name: 'one', error: 'boom', observations: { n: 2 }, frames: {} }, { name: 'two', frames: {} }]));
  const text = out.join('\n');
  assert.match(text, /error: a null, b "boom"/);
  assert.match(text, /observation n: 1 != 2/);
  assert.match(text, /step 2 "two" only ran on b/);
});

test('barrier frames are compared over the whole trace, watermarks only for subscribed sessions', () => {
  const wm = { dir: 'in', frame: { type: 'replica.watermark', revision: 'r1' } };
  const open = { dir: 'out', frame: { type: 'replica.openMany', opens: [] } };
  const t = (backend, steps) => ({ backend, steps: steps.map((frames, i) => ({ name: `s${i}`, observations: {}, frames })), checksums: {} });
  // Same watermark, different step: equal.
  assert.deepEqual(diffTraces(t('a', [{ x: [open, wm] }, {}]), t('b', [{ x: [open] }, { x: [wm] }])), []);
  // Subscribed session that never gets one: reported.
  assert.match(diffTraces(t('a', [{ x: [open, wm] }]), t('b', [{ x: [open] }])).join('\n'), /in replica.watermark: a sent, b none/);
  // Unsubscribed session: timer-driven watermarks are not compared.
  assert.deepEqual(diffTraces(t('a', [{ x: [wm] }]), t('b', [{}])), []);
});

test('server-minted tokens are unnumbered so arrival order cannot renumber them', () => {
  const f = (id, w) => rec('in', { type: 'query.result', id, windowRevision: w, result: [] });
  const a = normalizeSession([f('33333333-3333-4333-8333-333333333333', 'w-1111111111111'), f('44444444-4444-4444-8444-444444444444', 'w-2222222222222')]);
  assert.equal(a[0].frame.windowRevision, '<windowRevision>');
});

test('a no-op integrity reconcile is dropped; one that moves rows is kept', () => {
  const id = '55555555-5555-4555-8555-555555555555';
  const cursor = { epoch: 'e', revision: 6 };
  const episode = (upserts) => [
    rec('in', { type: 'replica.needHashes', id, path: 'notes.mine' }),
    rec('out', { type: 'replica.openMany', opens: [{ id, path: 'notes.mine', args: {}, cursor, keys: [], hashes: {}, digest: 'x', fullIntegrity: true }] }),
    rec('in', { type: 'replica.delta', id, cursor, upserts, deleted: [] }),
    rec('in', { type: 'replica.ready', id, cursor }),
  ];
  assert.deepEqual(normalizeSession(episode([])), []);
  assert.equal(normalizeSession(episode([{ id: 'n1' }])).length, 4);
});

test('no-op integrity reconciles batched into one reopen frame are dropped together; a moving one keeps them all', () => {
  const a = '66666666-6666-4666-8666-666666666666', b = '77777777-7777-4777-8777-777777777777';
  const cursor = { epoch: 'e', revision: 6 };
  const open = (id) => ({ id, path: `p.${id}`, args: {}, cursor, keys: [], hashes: {}, digest: 'x', fullIntegrity: true });
  const batched = (upserts) => [
    rec('in', { type: 'replica.needHashes', id: a, path: `p.${a}` }),
    rec('in', { type: 'replica.needHashes', id: b, path: `p.${b}` }),
    rec('out', { type: 'replica.openMany', opens: [open(a), open(b)] }),
    rec('in', { type: 'replica.delta', id: a, cursor, upserts: [], deleted: [] }),
    rec('in', { type: 'replica.ready', id: a, cursor }),
    rec('in', { type: 'replica.delta', id: b, cursor, upserts, deleted: [] }),
    rec('in', { type: 'replica.ready', id: b, cursor }),
  ];
  const split = [
    rec('in', { type: 'replica.needHashes', id: a, path: `p.${a}` }),
    rec('out', { type: 'replica.openMany', opens: [open(a)] }),
    rec('in', { type: 'replica.ready', id: a, cursor }),
    rec('in', { type: 'replica.needHashes', id: b, path: `p.${b}` }),
    rec('out', { type: 'replica.openMany', opens: [open(b)] }),
    rec('in', { type: 'replica.ready', id: b, cursor }),
  ];
  assert.deepEqual(normalizeSession(batched([])), []);
  assert.deepEqual(normalizeSession(split), []);
  // The reopen is split into one frame per open: the no-op episode goes, the moving one stays whole.
  const moving = normalizeSession(batched([{ id: 'n1' }]));
  assert.deepEqual(moving.map((r) => r.frame.type), ['replica.needHashes', 'replica.openMany', 'replica.delta', 'replica.ready']);
  assert.equal(moving[1].frame.opens.length, 1);
  // A reopen the client sends on its own (never asked about) is kept with its answer.
  assert.equal(normalizeSession(batched([]).filter((r) => !(r.frame.type === 'replica.needHashes' && r.frame.id === b))).length, 3);
});

test('0012 R20: session.ready drops replicaNativeRows, replicaSetDigest and replicaFieldPatches', () => {
  const ready = (capabilities) => normalizeSession([rec('in', { type: 'session.ready', capabilities })])[0].frame;
  const rust = ready({ presence: 1, replicaBatchFrames: 1, replicaNativeRows: 1, replicaSetDigest: 1, replicaFieldPatches: 1 });
  const kernel = ready({ presence: 1, replicaBatchFrames: 1 });
  assert.deepEqual(rust, kernel);
  assert.deepEqual(rust.capabilities, { presence: 1, replicaBatchFrames: 1 });
  // Other capabilities are still compared.
  assert.notDeepEqual(ready({ presence: 1 }), kernel);
});

test('ruling 0015: error messages stay exact, JavaScript stack lines are stripped', () => {
  const error = (e) => normalizeSession([rec('in', { type: 'reducer.error', id: 'x', class: 'rejected', error: e })])[0].frame.error;
  const rust = 'TypeScript module host returned execution_failed: module p function errors.reject: Error: fixture rejected the intent\n    at run (file:///gonvex/p/module.js:1:28805)\n    at eventLoopTick (ext:core/01_core.js:175:7)\n    at async gonvex:bootstrap.js:788:15';
  const other = 'TypeScript module host returned execution_failed: module p function errors.reject: Error: fixture rejected the intent\n    at run (worker.js:9:1)';
  assert.equal(error(rust), 'TypeScript module host returned execution_failed: module p function errors.reject: Error: fixture rejected the intent');
  assert.equal(error(rust), error(other));
  // The message itself, host wording included, is still compared exactly.
  assert.notEqual(error(rust), error('host call failed: fixture rejected the intent'));
  // Observations get the same rule.
  assert.equal(normalizeValue({ error: rust }).error, error(rust));
});

test('RFC 3339 clock strings under clock keys are masked; other strings and keys are kept', () => {
  const [{ frame }] = normalizeSession([{ dir: 'in', socket: 1, frame: { type: 'query.result', id: 'q1', result: [{ expiresAt: '2026-11-18T00:00:00.123+00:00', createdAt: '2026-11-11T09:30:00Z', name: '2026-11-11T09:30:00Z', state: 'pending' }] } }]);
  assert.deepEqual(frame.result, [{ createdAt: '<time>', expiresAt: '<time>', name: '2026-11-11T09:30:00Z', state: 'pending' }]);
  assert.deepEqual(normalizeValue({ lastSeenAt: '2026-11-11T09:30:00+00:00', label: 'x' }), { label: 'x', lastSeenAt: '<time>' });
  assert.deepEqual(normalizeValue({ expiresAt: 'not a date' }), { expiresAt: 'not a date' });
});

test('JWTs (identity-provider ID tokens) are masked; other dotted strings are kept', () => {
  const jwt = 'eyJhbGciOiJub25lIn0.eyJzdWIiOiJ4In0.';
  const [{ frame }] = normalizeSession([{ dir: 'out', socket: 1, frame: { type: 'action.call', id: 'a1', path: 'control.auth.exchangeExternalToken', scope: 'control', args: { provider: 'firebase', token: jwt, note: 'a.b.c' } } }]);
  assert.deepEqual(frame.args, { note: 'a.b.c', provider: 'firebase', token: '<jwt>' });
  assert.deepEqual(normalizeValue({ token: jwt }), { token: '<jwt>' });
});

test('presence Members are sorted by aliased Member id (backends assign the ids)', () => {
  const m = (memberId, state) => ({ memberId, state, since: 1 });
  const [snapshot, patch] = normalizeSession([
    rec('in', { type: 'presence.snapshot', id: 'p-1', serverTime: 1, members: [m('<member:dave@a>', 'idle'), m('<member:alice@a>', 'active')] }),
    rec('in', { type: 'presence.patch', serverTime: 2, changes: [m('<member:bob@a>', 'offline'), m('<member:alice@a>', 'idle')] }),
  ]).map(e => e.frame);
  assert.deepEqual(snapshot.members.map(x => x.memberId), ['<member:alice@a>', '<member:dave@a>']);
  assert.deepEqual(patch.changes.map(x => [x.memberId, x.state]), [['<member:alice@a>', 'idle'], ['<member:bob@a>', 'offline']]);
});
