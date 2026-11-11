/**
 * Action runtime (CONTRACT.md B.1, B.2a, B.6; package K12), through raw
 * `action.call` frames. One step per Action capability:
 *
 *  - the ctx surface of an Action with no capabilities, its clock and entropy,
 *    and the invocation it sees (channel, command id shape);
 *  - `fetch`: status, canonical status text, headers, request bodies, the
 *    redirect loop inside and outside the declared origins, the 8 MiB cap by
 *    content-length and by body, lossy UTF-8, an unknown method;
 *  - event streams read as they arrive (`fetchRead`), a cancelled stream, the
 *    cumulative 8 MiB cap on a stream;
 *  - the origin allowlist as a sticky denial (an undeclared origin, a
 *    relative URL);
 *  - `secrets`: an unconfigured secret fails the call before it runs;
 *  - `tools.*`: Query, Reducer and internal Reducer bindings, a duplicate
 *    insert, bad arguments and a binding of the wrong kind;
 *  - `functions.invoke`: Query, Reducer and Action targets, a stale artifact
 *    hash, internal, system, missing and recursive targets, bad arguments and a
 *    replica collection;
 *  - `scheduler`: runAfter, runAt, wakeAt coalescing and the refusals;
 *  - `storage`: the surface and an unconfigured object store;
 *  - a throwing Action, an unknown path, the wrong kind, bad arguments and an
 *    internal Action path called by a client. (Anonymous callers need a public
 *    tenant, which the harness does not seed: the 60 s public deadline is
 *    covered by the kernel's own tests.)
 *
 * The Actions fetch `scenarios/actions-upstream.mjs`, served on
 * 127.0.0.1:47812 for the whole run (the fixture's declared origin).
 */
import { randomUUID } from 'node:crypto';
import { RAW_CAPABILITIES } from './helpers.mjs';
import { startUpstream } from './actions-upstream.mjs';

/** Frame-level Action call. */
async function rawAction(socket, path, args, extra = {}) {
  const id = randomUUID();
  socket.send({ type: 'action.call', id, path, args, ...extra });
  const reply = await socket.next(m => (m.type === 'action.result' || m.type === 'action.error') && m.id === id, { label: `${path} reply`, timeoutMs: 60_000 });
  return reply.type === 'action.result' ? { result: reply.result } : { error: reply.error };
}

// Every backend runs the scenario at the same time; they share one upstream.
let shared = null;
let users = 0;
async function acquireUpstream() {
  users += 1;
  shared ??= startUpstream();
  return shared;
}
async function releaseUpstream() {
  users -= 1;
  if (users === 0 && shared) { const server = await shared; shared = null; await server.close(); }
}

export default {
  name: 'actions',
  description: 'Action runtime: ctx surface, fetch (allowlist, redirects, 8 MiB cap, event streams), secrets, tools, functions.invoke, scheduler, storage, and errors',
  tenants: { a: ['alice'] },
  async run(ctx) {
    await acquireUpstream();
    try {
      let raw;
      await ctx.step('connect', async () => {
        raw = await ctx.raw('raw');
        const token = (await ctx.rawLogin(raw, 'alice')).accessToken;
        await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
      });
      await ctx.step('an Action with no capabilities', async () => {
        ctx.observe('surface', await rawAction(raw, 'actions.surface', {}));
      });
      await ctx.step('fetch within the declared origin', async () => {
        ctx.observe('fetch', await rawAction(raw, 'actions.fetchProbe', {}));
      });
      await ctx.step('an event stream read as it arrives', async () => {
        ctx.observe('stream', await rawAction(raw, 'actions.streamProbe', {}));
      });
      await ctx.step('an undeclared origin is a sticky denial', async () => {
        ctx.observe('undeclared', await rawAction(raw, 'actions.fetchUndeclared', {}));
        ctx.observe('relative', await rawAction(raw, 'actions.fetchRelative', {}));
      });
      await ctx.step('an unconfigured secret', async () => {
        ctx.observe('secret', await rawAction(raw, 'actions.secretMissing', {}));
      });
      await ctx.step('tools', async () => {
        ctx.observe('tools', await rawAction(raw, 'actions.toolsProbe', { id: 'tool-row' }));
      });
      await ctx.step('functions.invoke', async () => {
        ctx.observe('functions', await rawAction(raw, 'actions.functionsProbe', { id: 'function-row' }));
      });
      await ctx.step('scheduler', async () => {
        ctx.observe('scheduler', await rawAction(raw, 'actions.schedulerProbe', {}));
      });
      await ctx.step('storage', async () => {
        ctx.observe('storage', await rawAction(raw, 'actions.storageProbe', {}));
      });
      await ctx.step('errors', async () => {
        ctx.observe('throws', await rawAction(raw, 'actions.fails', {}));
        ctx.observe('unknown', await rawAction(raw, 'actions.nope', {}));
        ctx.observe('wrongKind', await rawAction(raw, 'actions.boardCount', {}));
        ctx.observe('badArgs', await rawAction(raw, 'actions.publicEcho', { text: 1 }));
        ctx.observe('internal', await rawAction(raw, 'actions.internalAdd', { id: 'x' }));
      });
    } finally {
      await releaseUpstream();
    }
  },
};
