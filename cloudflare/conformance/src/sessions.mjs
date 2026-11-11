/**
 * Client sessions with frame capture.
 *
 * Three kinds of client talk to a backend, all through the global WebSocket,
 * which this module replaces with a recording subclass:
 *
 *  - `sdk`: the real `@gonvex/client` GonvexClient (packages/client/dist);
 *  - `thin`: the thin protocol client (bench/whagons/thin-client.mjs);
 *  - `raw`: a frame-level socket for exact sequences the SDKs hide (replaying
 *    an idempotency key, dropping a socket between commit and reply).
 *
 * Every socket belongs to one session tag. Both SDKs open sockets only from
 * their own `connect()` (first connect and every reconnect), so wrapping the
 * instance's `connect` and setting the current tag around the call tags
 * reconnect sockets too.
 */
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { REPO } from './paths.mjs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const clientModule = await import(pathToFileURL(join(REPO, 'packages/client/dist/index.js')).href).catch((error) => {
  throw new Error(`cannot load packages/client/dist (run \`pnpm build:packages\` at the repo root): ${error.message}`);
});
export const { GonvexClient, control } = clientModule;
export const { ThinClient } = await import(pathToFileURL(join(REPO, 'bench/whagons/thin-client.mjs')).href);

const NativeWebSocket = globalThis.WebSocket;
if (!NativeWebSocket) throw new Error('Node with a global WebSocket (>= 22) is required');

/** Records every frame of every tagged socket. One recorder per scenario run. */
export class Recorder {
  constructor() {
    this.records = []; // { tag, socket, dir, frame, step, at }
    this.step = undefined;
    this.sockets = new Map(); // tag -> count
    this.lastFrameAt = 0;
  }
  next(tag) { const n = (this.sockets.get(tag) || 0) + 1; this.sockets.set(tag, n); return n; }
  push(tag, socket, dir, data) {
    let frame;
    if (typeof data === 'string') { try { frame = JSON.parse(data); } catch { frame = { type: '<unparseable>' }; } }
    else frame = { type: '<binary>', bytes: data?.byteLength ?? data?.length ?? 0 };
    this.records.push({ tag, socket, dir, frame, step: this.step, at: Date.now() });
    this.lastFrameAt = Date.now();
  }
  /** Raw frames of one session (for scenario assertions). */
  frames(tag, { dir, type, step } = {}) {
    return this.records.filter(r => r.tag === tag && (!dir || r.dir === dir) && (!type || r.frame.type === type) && (!step || r.step === step)).map(r => r.frame);
  }
}

/**
 * Tag and recorder of the socket being constructed. Both SDKs construct
 * sockets only inside their own `connect()` (first connect and every
 * reconnect), which tagConnect wraps, so these are set synchronously around
 * that call. Runs on several backends go concurrently; each client carries its
 * own run's recorder, so frames never cross runs.
 */
let current = { tag: undefined, recorder: undefined };
const constructing = (tag, recorder, fn) => { const prev = current; current = { tag, recorder }; try { return fn(); } finally { current = prev; } };

class RecordingWebSocket extends NativeWebSocket {
  constructor(url, protocols) {
    super(url, protocols);
    const { tag = 'untagged', recorder: rec } = current;
    const socket = rec?.next(tag) ?? 0;
    const send = this.send.bind(this);
    this.send = (data) => { rec?.push(tag, socket, 'out', data); return send(data); };
    this.addEventListener('message', (event) => rec?.push(tag, socket, 'in', event.data));
    let closed = false;
    this.addEventListener('close', () => { closed = true; });
    // Node 22's WebSocket fires `error` but no `close` when the connection is
    // refused, and leaves readyState at CONNECTING (Node 26 and browsers close
    // as the WHATWG spec requires). Both SDKs reconnect from `close` and refuse
    // to connect while a socket is CONNECTING, so a client retrying while a
    // backend restarts would stall. Complete the spec's failure steps here.
    this.addEventListener('error', () => setTimeout(() => {
      if (closed || super.readyState !== 0) return;
      this.failed = true;
      const event = new Event('close');
      Object.assign(event, { code: 1006, reason: '', wasClean: false });
      this.dispatchEvent(event);
    }, 50));
  }
  get readyState() { return this.failed ? 3 : super.readyState; }
}
globalThis.WebSocket = RecordingWebSocket;

function tagConnect(client, tag, recorder) {
  const connect = client.connect.bind(client);
  client.connect = (...args) => constructing(tag, recorder, () => connect(...args));
}

/** Real SDK session. `auth` is { project, tenant?, token? }. */
export function sdkClient(wsUrl, tag, recorder, auth, options = {}) {
  const client = new GonvexClient(wsUrl, { ...auth, outbox: { enabled: false }, errorReporting: false, telemetry: false, ...options });
  tagConnect(client, tag, recorder);
  return client;
}

/** Thin protocol client session (connects immediately; `authenticate` for tenant auth). */
export function thinClient(wsUrl, tag, recorder, project, options = {}) {
  const client = new ThinClient(wsUrl, { project, ...options });
  tagConnect(client, tag, recorder);
  return client;
}

/**
 * Frame-level client. `send` a frame, `next(match)` waits for the next
 * unconsumed inbound frame that matches. Frames are recorded like any other.
 */
export class RawSocket {
  constructor(wsUrl, tag, recorder) {
    this.url = wsUrl; this.tag = tag; this.recorder = recorder; this.inbox = []; this.waiters = []; this.closed = false;
  }
  async open() {
    this.ws = constructing(this.tag, this.recorder, () => new globalThis.WebSocket(this.url));
    this.inbox = []; this.closed = false;
    this.ws.addEventListener('message', (e) => {
      let m; try { m = JSON.parse(String(e.data)); } catch { return; }
      this.inbox.push(m); this.wake();
    });
    this.ws.addEventListener('close', () => { this.closed = true; this.wake(); });
    await new Promise((resolve, reject) => { this.ws.addEventListener('open', resolve, { once: true }); this.ws.addEventListener('error', () => reject(new Error('raw socket failed to open')), { once: true }); });
    return this;
  }
  wake() { const w = this.waiters.splice(0); w.forEach(f => f()); }
  send(frame) { this.ws.send(JSON.stringify(frame)); }
  async next(match, { timeoutMs = 15_000, label } = {}) {
    const test = typeof match === 'function' ? match : (m) => Object.entries(match).every(([k, v]) => m[k] === v);
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const i = this.inbox.findIndex(test);
      if (i >= 0) return this.inbox.splice(i, 1)[0];
      if (this.closed) throw new Error(`raw socket closed while waiting for ${label ?? JSON.stringify(match)}`);
      if (Date.now() > deadline) throw new Error(`timed out waiting for ${label ?? JSON.stringify(match)}`);
      await Promise.race([new Promise(r => this.waiters.push(r)), delay(Math.max(1, deadline - Date.now()))]);
    }
  }
  /** Auth as a tenant member (or control-only without token). Returns auth.result. */
  async auth({ project, tenant, token, capabilities = {} }) {
    const id = randomUUID();
    this.send({ type: 'auth', id, project, ...(token ? { token, tenant } : { controlOnly: true }), clientContract: 1, capabilities });
    const m = await this.next((x) => (x.type === 'auth.result' || x.type === 'auth.error') && x.id === id, { label: 'auth.result' });
    if (m.type === 'auth.error') throw new Error('auth.error: ' + m.error);
    return m.result;
  }
  close() { try { this.ws?.close(); } catch {} }
  /** Closes without waiting, as a dropped network would. */
  drop() { try { this.ws?.close(); } catch {} this.closed = true; this.wake(); }
}
