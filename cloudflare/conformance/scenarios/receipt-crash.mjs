/**
 * Receipt case "crash" (sdk_process_crash and check_restart):
 *  - the client process is SIGKILLed right after sending a keyed Reducer call;
 *    a new process replays the key and the call applies exactly once;
 *  - the backend is killed without shutdown after a commit; after restart the
 *    commit, its receipt and the Replica are intact, and a connected SDK
 *    client recovers on its own;
 *  - the backend is killed right after a keyed call is sent; the replay after
 *    restart applies exactly once whether or not the first attempt committed.
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { watch, rowsWhen, project, rawReducer, rawQuery, RAW_CAPABILITIES } from './helpers.mjs';

const CHILD = fileURLToPath(new URL('./children/send-and-die.mjs', import.meta.url));
const COLUMNS = ['id', 'title', 'position', 'bumps'];

const KILLED_STEP = 'backend killed right after a keyed call is sent';
const REPLAY_STEP = 'replay after the second restart applies once';
const BOARD = new Set(['replica.transaction', 'replica.delta', 'replica.ready', 'replica.watermark', 'replica.snapshot']);
const counterIn = (frame) => (frame.upserts ?? frame.result ?? []).find((r) => r?.id === 'counter')?.bumps;
const revisionOf = (frame) => frame.cursor?.revision ?? frame.revision;

/**
 * contract-changes 0093: the call sent right before the second crash may commit before the crash or only through
 * the replay after the restart; the scenario accepts both (exactly once either way). Rust's SIGKILL lands before
 * the call commits; a deployed backend's restart is a separate request, so the call may commit first. This
 * rewrites ONLY the documented "committed before the crash" shape of the SDK's board frames in the last two steps
 * into the "committed after the restart" shape, so both compare equal. Watermarks take no part (they are
 * timing-placed barriers); an accepted run's SDK watermarks in those steps are left out of the comparison. Anything else (two effects, a bump other
 * than +1, no effect, effects in both steps) fails the run and is left untouched. Runs on normalized frames.
 * @returns {'committed-after-restart' | 'committed-before-crash'}
 */
export function canonicalizeInFlightOutcome(trace) {
  const killed = trace.steps.find((s) => s.name === KILLED_STEP), replay = trace.steps.find((s) => s.name === REPLAY_STEP);
  if (!killed || !replay) return undefined;
  const fail = (why) => { trace.failures.push(`receipt-crash in-flight outcome: ${why}`); return undefined; };
  const before = trace.steps.slice(0, trace.steps.indexOf(killed));
  const n = before.map((s) => s.observations?.sdkRows?.[0]?.bumps).filter((b) => typeof b === 'number').at(-1);
  const earlier = before.flatMap((s) => s.frames?.sdk ?? []).filter((f) => f.dir === 'in' && BOARD.has(f.frame.type));
  const p = revisionOf(earlier.at(-1)?.frame ?? {});
  if (typeof n !== 'number' || !p) return fail('no counter or revision before the crash');
  const k = killed.frames?.sdk ?? [], r = replay.frames?.sdk ?? [];
  const effect = (frames) => {
    const tx = frames.filter((f) => f.dir === 'in' && f.frame.type === 'replica.transaction');
    const deltas = frames.filter((f) => f.dir === 'in' && f.frame.type === 'replica.delta');
    return { tx, deltas, ok: tx.length === 1 && deltas.length === 1 && counterIn(deltas[0].frame) === n + 1 };
  };
  const reconnect = k.findIndex((f) => f.dir === 'out' && f.frame.type === 'auth');
  const snapshot = k.find((f) => f.dir === 'in' && f.frame.type === 'replica.snapshot');
  if (reconnect < 0 || !snapshot) return fail('the SDK did not reconnect with a snapshot');
  // Review r4611631438: watermarks are barriers (compared as sent or not, their values checked per backend by
  // the raw-frame watermark self-check). How many arrive, and where, in these two steps is timing: they take no
  // part in the shape checks, and an accepted run's SDK watermarks of steps 7-8 are dropped from the comparison.
  const isMark = (f) => f.frame.type === 'replica.watermark';
  const dropMarks = (frames) => frames.filter((f) => !(f.dir === 'in' && isMark(f)));
  const rBoard = r.filter((f) => f.dir === 'in' && BOARD.has(f.frame.type) && !isMark(f));
  const kBefore = k.slice(0, reconnect).filter((f) => f.dir === 'in' && BOARD.has(f.frame.type) && !isMark(f));
  // A: committed after the restart (the reference shape).
  if (!kBefore.length && counterIn(snapshot.frame) === n && revisionOf(snapshot.frame) === p && effect(r).ok) {
    killed.frames.sdk = dropMarks(k); replay.frames.sdk = dropMarks(r);
    return 'committed-after-restart';
  }
  // B: committed before the crash.
  const e = effect(kBefore), c = revisionOf(e.deltas[0]?.frame ?? {});
  if (e.ok && !rBoard.length && kBefore.length === 3 && kBefore.every((f) => revisionOf(f.frame) === c)
    && counterIn(snapshot.frame) === n + 1 && revisionOf(snapshot.frame) === c) {
    const after = k.slice(reconnect).map((f) => {
      if (f.dir !== 'in' || !BOARD.has(f.frame.type) || revisionOf(f.frame) !== c) return f;
      const frame = structuredClone(f.frame);
      if (frame.cursor) frame.cursor.revision = p; else frame.revision = p;
      for (const row of frame.result ?? []) if (row?.id === 'counter') row.bumps = n;
      return { ...f, frame };
    });
    // Where the effect arrives also shifts its aliases: in the reference shape it comes on the reconnect's
    // subscription, and the reconnect's auth id is aliased before the call's command id (first-appearance order).
    // Only the pre-crash board subscription's own frames move to the reconnect's subscription (review
    // r4611557477): a delta or ready under any other id or path is a misrouted frame and fails the run.
    const prior = earlier.findLast((f) => f.frame.type === 'replica.snapshot' || f.frame.type === 'replica.ready')?.frame;
    const ownFrame = (frame) => frame.type === 'replica.transaction'
      || (frame.id === prior?.id && (frame.path === undefined || frame.path === prior?.path));
    if (!prior?.id || prior.path !== snapshot.frame.path || !kBefore.every((f) => ownFrame(f.frame))) {
      return fail('pre-crash replication frames use an unexpected subscription');
    }
    const subscription = snapshot.frame.id;
    const moved = kBefore.map((f) => (f.frame.type === 'replica.delta' || f.frame.type === 'replica.ready' ? { ...f, frame: { ...f.frame, id: subscription } } : f));
    const authId = k[reconnect].frame.id, commandId = e.tx[0].frame.originCommandId;
    const swap = (frames) => (typeof authId === 'string' && typeof commandId === 'string' && authId !== commandId
      ? JSON.parse(JSON.stringify(frames).replaceAll(`"${authId}"`, '"\u0000a"').replaceAll(`"${commandId}"`, `"${authId}"`).replaceAll('"\u0000a"', `"${commandId}"`))
      : frames);
    killed.frames.sdk = swap(dropMarks([...k.slice(0, reconnect).filter((f) => !(f.dir === 'in' && BOARD.has(f.frame.type))), ...after]));
    replay.frames.sdk = swap(dropMarks([...moved, ...r]));
    return 'committed-before-crash';
  }
  return fail('neither committed before the crash nor after the restart (a lost or doubled effect)');
}

export default {
  name: 'receipt-crash',
  /** 0093: both documented outcomes of the call sent right before the second crash (see above). */
  canonicalize: (trace) => { const outcome = canonicalizeInFlightOutcome(trace); if (outcome) trace.inFlightOutcome = outcome; },
  description: 'Client process crash and backend crash (SIGKILL) around keyed Reducer calls',
  tenants: { a: ['alice', 'bob'] },
  requires: ['crash'],
  async run(ctx) {
    const { api } = ctx;
    let socket, token;
    const connect = async () => {
      socket = await ctx.raw('client');
      if (!token) token = (await ctx.rawLogin(socket, 'alice')).accessToken;
      await socket.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
    };
    await ctx.step('seed a counter', async () => {
      await connect();
      ctx.observe('add', await rawReducer(socket, 'board.add', { id: 'counter', title: 'counter', position: 1 }));
    });
    await ctx.step('client process dies right after sending a keyed call', async () => {
      const child = spawn(process.execPath, [CHILD, ctx.wsUrl, ctx.project, ctx.tenants.a, token, 'board.bump', JSON.stringify({ id: 'counter' }), 'conformance-crash-key-0001'], { stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '', err = '';
      child.stdout.on('data', d => { out += d; }); child.stderr.on('data', d => { err += d; });
      const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
      await ctx.until(() => out.includes('sent') || child.exitCode !== null, 'child sent its call');
      ctx.expect(out.includes('sent'), `child did not send its call: ${err}`);
      child.kill('SIGKILL');
      ctx.observe('childExit', (await exited).signal);
    });
    await ctx.step('a new client replays the key', async () => {
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0001', receiptPath: 'board.bump' }));
    });
    let sdk, view;
    await ctx.step('an SDK client watches the board', async () => {
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      view = watch(sdk, api.board.all);
      ctx.observe('rows', project(await rowsWhen(ctx, view, rows => rows.length === 1, 'sdk snapshot'), COLUMNS));
    });
    // The SDK reconnects on its own backoff; wait for that reconnect, not only
    // for its rows (they may have arrived before the kill).
    let connections;
    const reconnected = () => sdk.connectionState().connectionCount > connections && sdk.connectionState().isWebSocketConnected && view.ready();
    await ctx.step('commit, then the backend is killed', async () => {
      ctx.observe('bump', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0002', receiptPath: 'board.bump' }));
      await rowsWhen(ctx, view, rows => rows[0]?.bumps === 2, 'sdk sees the commit before the kill');
      connections = sdk.connectionState().connectionCount;
      await ctx.restart({ crash: true });
      // Inside this step, so the reconnect's frames always land here.
      await ctx.until(reconnected, 'sdk reconnected after restart', 30_000);
    });
    await ctx.step('after restart: replay, receipt and replica', async () => {
      await connect();
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0002', receiptPath: 'board.bump' }));
      ctx.observe('receipt', await rawQuery(socket, '__gonvex.reducerReceipt', { idempotencyKey: 'conformance-crash-key-0002', receiptPath: 'board.bump' }));
      ctx.observe('sdkRows', project(await rowsWhen(ctx, view, rows => rows[0]?.bumps === 2, 'sdk recovered after restart'), COLUMNS));
    });
    await ctx.step('backend killed right after a keyed call is sent', async () => {
      socket.send({ type: 'reducer.call', id: 'conformance-in-flight-0001', path: 'board.bump', args: { id: 'counter' }, idempotencyKey: 'conformance-crash-key-0003', receiptPath: 'board.bump' });
      socket.drop(); // the reply, if any, is never read
      connections = sdk.connectionState().connectionCount;
      await ctx.restart({ crash: true });
      await ctx.until(reconnected, 'sdk reconnected after second restart', 30_000);
    });
    await ctx.step('replay after the second restart applies once', async () => {
      await connect();
      ctx.observe('replay', await rawReducer(socket, 'board.bump', { id: 'counter' }, { idempotencyKey: 'conformance-crash-key-0003', receiptPath: 'board.bump' }));
      ctx.observe('sdkRows', project(await rowsWhen(ctx, view, rows => rows[0]?.bumps === 3, 'sdk recovered after second restart'), COLUMNS));
    });
  },
};
