// contract-changes 0093: receipt-crash accepts exactly the two documented outcomes of the call sent right
// before the second crash, and rejects anything else.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { canonicalizeInFlightOutcome } from '../scenarios/receipt-crash.mjs';

const KILLED = 'backend killed right after a keyed call is sent', REPLAY = 'replay after the second restart applies once';
const row = (bumps) => ({ bumps, id: 'counter', position: 1, title: 'counter' });
const at = (r) => ({ epoch: '<epoch#2>', revision: r });
const tx = (r, bumps) => ({ dir: 'in', frame: { type: 'replica.transaction', cursor: at(r), originCommandId: '<id#12>', changes: [{ entity: 'board', id: 'counter', newValue: row(bumps), operation: 'update', changedColumns: ['bumps'] }] } });
const delta = (r, bumps) => ({ dir: 'in', frame: { type: 'replica.delta', cursor: at(r), id: '<uuid#3>', path: 'board.all', upserts: [row(bumps)], deleted: [], digest: '<digest>', originCommandIds: [], truncated: false } });
const ready = (r) => ({ dir: 'in', frame: { type: 'replica.ready', cursor: at(r), id: '<uuid#3>', path: 'board.all', mode: 'eager', digest: '<digest>', truncated: false } });
const mark = (r) => ({ dir: 'in', frame: { type: 'replica.watermark', revision: r } });
const snap = (r, bumps) => ({ dir: 'in', frame: { type: 'replica.snapshot', cursor: at(r), id: '<uuid#3>', path: 'board.all', key: 'id', result: [row(bumps)] } });
const reconnect = [{ dir: 'out', frame: { type: 'auth', id: '<id#6>' } }, { dir: 'in', frame: { type: 'session.ready' } }, { dir: 'in', frame: { type: 'auth.result', id: '<id#6>' } }, { dir: 'out', frame: { type: 'replica.openMany', opens: [{ id: '<uuid#3>', path: 'board.all' }] } }];
const effect = (r, bumps) => [tx(r, bumps), delta(r, bumps), ready(r), mark(r)];

const onOldSubscription = (frames) => frames.map((f) => (f.frame.id === '<uuid#3>' ? { ...f, frame: { ...f.frame, id: '<uuid#2>' } } : f));
function trace(killedSdk, replaySdk) {
  return { failures: [], steps: [
    // Before the second crash the SDK watches on its pre-crash subscription (<uuid#2>); the reconnect opens <uuid#3>.
    { name: 'after restart: replay, receipt and replica', observations: { sdkRows: [row(2)] }, frames: { sdk: onOldSubscription([snap('r2', 2), ready('r2'), mark('r2')]) } },
    { name: KILLED, observations: {}, frames: { client: [{ dir: 'out', frame: { type: 'reducer.call', id: '<id#12>' } }], sdk: killedSdk } },
    { name: REPLAY, observations: { replay: { result: { bumps: 3 } }, sdkRows: [row(3)] }, frames: { sdk: replaySdk } },
  ] };
}
const afterRestart = () => trace([...reconnect, snap('r2', 2), ready('r2'), mark('r2')], effect('r3', 3));
// As normalized from a real run: the early effect arrives on the pre-crash subscription (<uuid#2>), and the
// call's command id is aliased before the reconnect's auth id (first-appearance order).
const swapIds = (frames) => JSON.parse(JSON.stringify(frames).replaceAll('"<id#6>"', '"T"').replaceAll('"<id#12>"', '"<id#6>"').replaceAll('"T"', '"<id#12>"'));
const beforeCrash = () => trace(swapIds([...onOldSubscription(effect('r3', 3)), ...reconnect, snap('r3', 3), ready('r3'), mark('r3')]), []);
const sorted = (frames) => frames.map((f) => JSON.stringify(f)).sort();
const sdkOf = (t, name) => sorted(t.steps.find((s) => s.name === name).frames.sdk);

test('committed after the restart (the Rust reference shape) is accepted unchanged', () => {
  const t = afterRestart(), copy = structuredClone(t);
  assert.equal(canonicalizeInFlightOutcome(t), 'committed-after-restart');
  assert.deepEqual(t, copy);
});

test('committed before the crash is accepted and compares equal to the reference shape', () => {
  const t = beforeCrash();
  assert.equal(canonicalizeInFlightOutcome(t), 'committed-before-crash');
  assert.deepEqual(t.failures, []);
  const ref = afterRestart();
  assert.deepEqual(sdkOf(t, KILLED), sdkOf(ref, KILLED));
  assert.deepEqual(sdkOf(t, REPLAY), sdkOf(ref, REPLAY));
});

for (const [label, make] of [
  ['a doubled effect (before the crash and again after the restart)', () => trace([...effect('r3', 3), ...reconnect, snap('r3', 3), ready('r3'), mark('r3')], effect('r4', 4))],
  ['two effects after the restart', () => trace([...reconnect, snap('r2', 2), ready('r2'), mark('r2')], [...effect('r3', 3), ...effect('r4', 4)])],
  ['a bump other than +1', () => trace([...reconnect, snap('r2', 2), ready('r2'), mark('r2')], effect('r3', 4))],
  ['a lost call (no effect at all)', () => trace([...reconnect, snap('r2', 2), ready('r2'), mark('r2')], [])],
  // Review r4611557477: an early effect on another subscription is misrouted, not the documented outcome.
  ['an early delta on another subscription', () => trace(swapIds([...onOldSubscription(effect('r3', 3)).map((f) => (f.frame.type === 'replica.delta' ? { ...f, frame: { ...f.frame, id: '<uuid#9>' } } : f)), ...reconnect, snap('r3', 3), ready('r3'), mark('r3')]), [])],
  ['an early effect on the reconnect subscription before the reconnect', () => trace(swapIds([...effect('r3', 3), ...reconnect, snap('r3', 3), ready('r3'), mark('r3')]), [])],
  ['an early ready on another path', () => trace(swapIds([...onOldSubscription(effect('r3', 3)).map((f) => (f.frame.type === 'replica.ready' ? { ...f, frame: { ...f.frame, path: 'notes.mine' } } : f)), ...reconnect, snap('r3', 3), ready('r3'), mark('r3')]), [])],
  ['a snapshot that disagrees with the early effect', () => trace([...effect('r3', 3), ...reconnect, snap('r3', 2), ready('r3'), mark('r3')], [])],
]) {
  test(`rejected: ${label}`, () => {
    const t = make(), frames = structuredClone(t.steps.map((s) => s.frames));
    assert.equal(canonicalizeInFlightOutcome(t), undefined);
    assert.equal(t.failures.length, 1);
    assert.match(t.failures[0], /receipt-crash in-flight outcome/);
    assert.deepEqual(t.steps.map((s) => s.frames), frames, 'frames are left untouched');
  });
}

test('other scenarios and traces without those steps are left alone', () => {
  const t = { failures: [], steps: [{ name: 'something else', frames: {} }] };
  assert.equal(canonicalizeInFlightOutcome(t), undefined);
  assert.deepEqual(t.failures, []);
});
