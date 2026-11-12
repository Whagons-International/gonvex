/**
 * Error and performance telemetry (CONTRACT.md A.2 `error.*`, `telemetry.*`,
 * A.1.7 envelope limits, D.1 "Metrics, logs, errors"; K15; ruling 0015 S0-3).
 *
 * The frames go over raw sockets in four roles: an unauthenticated socket
 * (no project), an anonymous control-only socket (project, no identity: 20
 * per connection per minute), and two sockets of one Member (120 per account
 * per minute). Every `error.ack` is observed. The error-triage routes are then
 * read and compared: groups (with filters), one group, its bug report, a
 * status change, a regression, the metrics `logs` and `/dev/logs`.
 *
 * The inputs are data (`SCRIPT`), so `src/error-telemetry-golden.mjs` can
 * record Rust's answers as the golden the kernel replays in workerd
 * (`cloudflare/kernel/telemetry/test/parity.spec.ts`), where sockets and the
 * operator routes are not yet one backend.
 *
 * Values that depend on the run are scrubbed with the named rules of `scrub`:
 * times derived from the clock (events without a valid `timestamp`), and
 * connection ids (`sessionId`), numbered by first appearance.
 */
import { createHash } from 'node:crypto';

/**
 * Compact markers, so the script (and the golden that embeds it) stays small:
 * `{ $pad: [eventId, bytes] }` is an event whose serialized JSON is exactly
 * `bytes` long; `{ $repeat: [text, n] }` is `text` repeated `n` times.
 * `expand` turns them into the frames that are sent.
 */
export function expand(value) {
  if (Array.isArray(value)) return value.map(expand);
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value.$repeat)) return String(value.$repeat[0]).repeat(value.$repeat[1]);
  if (Array.isArray(value.$pad)) {
    const [eventId, bytes] = value.$pad;
    const base = { eventId, message: 'boundary', culprit: 'pad.ts', pad: '' };
    return { ...base, pad: 'p'.repeat(bytes - JSON.stringify(base).length) };
  }
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, expand(item)]));
}
const PAD = (eventId, bytes) => ({ $pad: [eventId, bytes] });
const REPEAT = (text, n) => ({ $repeat: [text, n] });

const WIDE = Object.fromEntries(Array.from({ length: 120 }, (_, i) => [`k${i}`, i]));
const DEEP = { l1: { l2: { l3: { l4: { l5: { l6: { l7: { l8: 'deep' } } } } } } } };

/** The richest event: spoofed attribution, secrets at every depth, long, deep and wide values, an offset timestamp. */
const EVENT_A = {
  eventId: 'evt-a-1',
  name: 'TypeError',
  message: "Cannot read properties of undefined (reading 'id')",
  culprit: 'src/tasks/grid.ts:42',
  level: 'error',
  timestamp: '2025-03-04T05:06:07.123456789+02:00',
  release: '1.0.0',
  environment: 'production',
  deviceId: 'device-1',
  stack: "TypeError: Cannot read properties of undefined (reading 'id')\n    at grid (src/tasks/grid.ts:42:7)",
  project: 'spoofed-project',
  tenant: 'spoofed-tenant',
  sessionId: 'spoofed-session',
  account: { id: 'spoofed-account', email: 'spoof@example.invalid' },
  context: {
    route: '/tasks',
    password: 'hunter2',
    'api-key': 'k',
    AUTHORIZATION: 'Bearer x',
    'Set_Cookie': 'c=1',
    // KELVIN SIGN: Rust's to_ascii_lowercase leaves it, so this key is not "token".
    'TOKEN': 'kept',
    nested: { refresh_token: 't', safe: 'ok', deep: DEEP },
    list: Array.from({ length: 60 }, (_, i) => i),
    wide: WIDE,
  },
  breadcrumbs: [{ message: 'clicked', data: { cookie: 'c', ok: true } }],
  long: REPEAT('\u{1F600}', 4_100),
  passwd: 'top-level secret',
};

/** Same fingerprint as A (name, message, culprit), another release and device. */
const EVENT_A2 = { ...EVENT_A, eventId: 'evt-a-2', timestamp: '2025-03-05T00:00:00Z', release: '1.1.0', deviceId: 'device-2', environment: 'staging' };

export const SCRIPT = [
  {
    step: 'no project: telemetry is unavailable',
    role: 'none',
    frames: [
      { type: 'error.register', id: 'none-1', release: '1', environment: 'x' },
      { type: 'error.heartbeat', id: 'none-2' },
      { type: 'error.envelope', id: 'none-3', events: [{ message: 'x' }] },
      { type: 'telemetry.event', id: 'perf-none', kind: 'query', path: 'k15.none', outcome: 'ok', clientReceivedAtMs: 1 },
    ],
  },
  {
    step: 'an anonymous connection: no identity, no context',
    role: 'anonymous',
    frames: [
      { type: 'error.register', id: 'anon-1', release: '9.9.9', environment: 'anon' },
      {
        type: 'error.envelope', id: 'anon-2', events: [{
          eventId: 'evt-anon', name: 'AnonError', message: 'anonymous failure', account: { id: 'x' }, a: 1, context: { safe: 1 }, b: 2,
          breadcrumbs: [1], c: 3, timestamp: '2025-02-01T00:00:00Z', release: '9.9.9', deviceId: 'device-anon',
        }],
      },
    ],
  },
  {
    step: 'a Member registers an error session',
    role: 'alice',
    frames: [
      { type: 'error.register', id: 'reg-1', release: ' 1.0.0 ', environment: 'production' },
      { type: 'error.register', id: 'reg-2', release: ' 1.0.0 ', environment: 'production' },
      { type: 'error.heartbeat', id: 'reg-3' },
      { type: 'error.register', id: 'reg-4', release: REPEAT('r', 250), environment: REPEAT('e', 150) },
    ],
  },
  {
    step: 'envelope limits',
    role: 'alice',
    frames: [
      { type: 'error.envelope', id: 'lim-empty', events: [] },
      { type: 'error.envelope', id: 'lim-21', events: Array.from({ length: 21 }, (_, i) => ({ message: `m${i}` })) },
      { type: 'error.envelope', id: 'lim-event-ok', events: [PAD('evt-pad-exact', 32 * 1024)] },
      { type: 'error.envelope', id: 'lim-event-big', events: [PAD('evt-pad-over', 32 * 1024 + 1)] },
      { type: 'error.envelope', id: 'lim-total-ok', events: Array.from({ length: 8 }, (_, i) => PAD(`evt-total-${i}`, 32 * 1024)) },
      { type: 'error.envelope', id: 'lim-total-big', events: [...Array.from({ length: 8 }, (_, i) => PAD(`evt-total-x${i}`, 32 * 1024)), {}] },
    ],
  },
  {
    step: 'grouping, scrubbing and duplicates',
    role: 'alice',
    frames: [
      {
        type: 'error.envelope', id: 'grp-1', events: [
          42, {}, { message: 7 }, { message: ' \t\n' }, EVENT_A,
          { name: 'Warn', message: 'slow render', level: 'warning', timestamp: 'not a time', deviceId: 'device-2', release: '2.0.0', stack: 'at render' },
          { ...EVENT_A, message: 'a duplicate id is not stored' },
          EVENT_A2,
          // U+FEFF is not White_Space for Rust's trim: the event is kept.
          { name: 'Bom', message: '﻿', eventId: 'evt-bom', timestamp: '2025-01-02T03:04:05Z' },
          { message: 'leap second', eventId: 'evt-leap', timestamp: '2016-12-31T23:59:60.5Z' },
          { message: 'lowercase t, minus sign, ten fraction digits', eventId: 'evt-minus', timestamp: '2025-01-01t00:00:00.1234567891−05:30' },
          { message: 'space separator, offset out of range', eventId: 'evt-offset', timestamp: '2025-01-01 10:00:00+24:00' },
          { message: 'space separator', eventId: 'evt-space', timestamp: '2025-01-01 10:00:00-01:00' },
        ],
      },
      { type: 'error.envelope', id: 'grp-2', events: [{ name: 'Warn', message: 'slow render', level: 'warning', timestamp: 'not a time', deviceId: 'device-2', release: '2.0.0', stack: 'at render' }] },
    ],
  },
  {
    step: 'a second socket of the same Member',
    role: 'alice2',
    frames: [
      { type: 'error.register', id: 'two-1', release: '1.1.0', environment: 'staging' },
      { type: 'error.envelope', id: 'two-2', events: [{ ...EVENT_A2, eventId: 'evt-a-3', deviceId: 'device-3', timestamp: '2025-03-06T00:00:00Z' }] },
    ],
  },
  {
    step: 'performance events',
    role: 'alice',
    frames: [
      { type: 'telemetry.event', id: 'perf-1', kind: 'query', path: 'k15.query', outcome: 'ok', clientSentAtMs: 900, clientReceivedAtMs: 1000, clientDurationMs: 100 },
      { type: 'telemetry.event', id: 'perf-2', kind: 'reducer', path: 'k15.reducer', outcome: 'error', error: REPEAT('boom ', 300), reason: 'change', clientReceivedAtMs: 2000, clientDurationMs: 0 },
      { type: 'telemetry.event', id: 'perf-1', kind: 'query', path: 'k15.query', outcome: 'ok', clientReceivedAtMs: 3000, clientDurationMs: 5 },
      { type: 'telemetry.event', id: 'perf-bad-kind', kind: 'mutation', path: 'k15.query', outcome: 'ok', clientReceivedAtMs: 1 },
      { type: 'telemetry.event', id: 'perf-bad-outcome', kind: 'query', path: 'k15.query', outcome: 'maybe', clientReceivedAtMs: 1 },
      { type: 'telemetry.event', id: 'perf-negative', kind: 'query', path: 'k15.query', outcome: 'ok', clientReceivedAtMs: 1, clientDurationMs: -1 },
      { type: 'telemetry.event', id: REPEAT('i', 201), kind: 'query', path: 'k15.query', outcome: 'ok', clientReceivedAtMs: 1 },
      { type: 'telemetry.event', id: 'perf-long-path', kind: 'query', path: REPEAT('p', 501), outcome: 'ok', clientReceivedAtMs: 1 },
      { type: 'telemetry.batch', events: [
        { id: 'batch-1', kind: 'action', path: 'k15.action', outcome: 'ok', clientReceivedAtMs: 10, clientDurationMs: 12.5 },
        { id: 'batch-2', kind: 'query', path: 'k15.query', outcome: 'error', error: 'denied', reason: 'initial', clientReceivedAtMs: 11, clientDurationMs: 7 },
      ] },
      { type: 'telemetry.batch', events: Array.from({ length: 51 }, (_, i) => ({ id: `batch-over-${i}`, kind: 'query', path: 'k15.over', outcome: 'ok', clientReceivedAtMs: 1 })) },
      { type: 'error.heartbeat', id: 'perf-barrier' },
    ],
  },
  {
    step: 'without telemetryBatch a batch is dropped',
    role: 'alice2',
    frames: [
      { type: 'telemetry.batch', events: [{ id: 'batch-unnegotiated', kind: 'query', path: 'k15.query', outcome: 'ok', clientReceivedAtMs: 1 }] },
      { type: 'telemetry.event', id: 'perf-alice2', kind: 'action', path: 'k15.action', outcome: 'error', clientReceivedAtMs: 20, clientDurationMs: 30 },
      { type: 'error.heartbeat', id: 'two-barrier' },
    ],
  },
  { step: 'triage reads', http: 'reads' },
  { step: 'triage writes', http: 'writes' },
  {
    step: 'a resolved group regresses',
    role: 'alice',
    frames: [{ type: 'error.envelope', id: 'reg-again', events: [{ ...EVENT_A, eventId: 'evt-a-4', timestamp: '2025-03-07T00:00:00Z' }] }],
  },
  { step: 'regression read', http: 'regression' },
  { step: 'anonymous connection rate limit', role: 'anonymous', flood: 'anon-flood' },
  { step: 'account rate limit (both sockets)', role: 'alice2', flood: 'alice-flood' },
  {
    step: 'after the limit',
    role: 'alice',
    frames: [
      { type: 'error.envelope', id: 'after-1', events: [{ message: 'limited' }] },
      { type: 'error.register', id: 'after-2', release: 'x' },
    ],
  },
];

/** HTTP reads and writes of each `http` step: [label, method, path (relative to the project), body?]. */
export const HTTP = {
  reads: [
    ['status', 'GET', '/dev/errors/status?project=$P'],
    ['groups', 'GET', '/dev/errors/groups?project=$P'],
    ['unresolved', 'GET', '/dev/errors/groups?project=$P&status=unresolved'],
    ['warnings', 'GET', '/dev/errors/groups?project=$P&level=WARN'],
    ['release', 'GET', '/dev/errors/groups?project=$P&release=1.1.0'],
    ['groupA', 'GET', '/dev/errors/groups/$A?project=$P'],
    ['bugReportA', 'GET', '/dev/errors/groups/$A/bug-report?project=$P'],
    ['missing', 'GET', '/dev/errors/groups/nope?project=$P'],
    ['metrics', 'GET', '/dev/metrics?project=$P'],
  ],
  writes: [
    ['resolveA', 'PATCH', '/dev/errors/groups/$A?project=$P', { status: 'resolved', priority: 'high', assignee: ' ops@example.invalid ' }],
    ['badPriority', 'PATCH', '/dev/errors/groups/$A?project=$P', { priority: 'urgent' }],
    ['clearLogs', 'DELETE', '/dev/logs?project=$P'],
    ['metricsAfterClear', 'GET', '/dev/metrics?project=$P'],
  ],
  regression: [
    ['groupA', 'GET', '/dev/errors/groups/$A?project=$P'],
    ['resolved', 'GET', '/dev/errors/groups?project=$P&status=resolved'],
  ],
};

/** The parts of a `/dev/metrics` body that client telemetry decides (the rest is process state, D.1 replace). */
export function metricsView(body) {
  if (!body || typeof body !== 'object' || !body.functions) return body;
  const functions = Object.fromEntries(Object.entries(body.functions).filter(([path]) => path.startsWith('k15.')).sort(([a], [b]) => a.localeCompare(b)));
  const logs = [...(body.logs ?? [])].filter((log) => String(log.path).startsWith('k15.')).sort((a, b) => String(a.operationId).localeCompare(String(b.operationId)));
  return { functions, logs };
}

/**
 * Named scrub rules, applied to the JSON text of every observation:
 * - `clock`: an RFC 3339 time or a Unix-ms number inside [from, to] (the run) becomes `<now>`;
 * - `connection`: each distinct `sessionId` value becomes `<connection#n>` by first appearance;
 * - `long`: a string longer than 256 UTF-16 units becomes `<long:length:sha256/16>` (exact, but small).
 *   `hash(text)` is SHA-256 hex of the UTF-8 text.
 */
export function scrub(value, { from, to, hash }) {
  const shorten = (item) => {
    if (typeof item === 'string') return item.length > 256 ? `<long:${item.length}:${hash(item).slice(0, 16)}>` : item;
    if (Array.isArray(item)) return item.map(shorten);
    if (item && typeof item === 'object') return Object.fromEntries(Object.entries(item).map(([key, inner]) => [key, shorten(inner)]));
    return item;
  };
  let text = JSON.stringify(shorten(value));
  const connections = new Map();
  text = text.replace(/"sessionId":"([^"]*)"/g, (_, id) => {
    if (!connections.has(id)) connections.set(id, `<connection#${connections.size + 1}>`);
    return `"sessionId":"${connections.get(id)}"`;
  });
  text = text.replace(/"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))"/g, (m, iso) => {
    const ms = Date.parse(iso);
    return ms >= from && ms <= to ? '"<now>"' : m;
  });
  text = text.replace(/(?<=[:,[])(\d{13})(?=[,}\]])/g, (m) => (Number(m) >= from && Number(m) <= to ? '"<now>"' : m));
  return JSON.parse(text);
}

const hash = (text) => createHash('sha256').update(text, 'utf8').digest('hex');

/** Per-backend record of the run, read by the golden recorder. */
export const RECORDINGS = new Map();

async function http(ctx, method, path, body) {
  const init = { method, headers: { authorization: `Bearer ${ctx.backend.adminKey}` } };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers['content-type'] = 'application/json';
  }
  const response = await fetch(ctx.backend.httpUrl + path, init);
  const text = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = text;
  }
  return { status: response.status, body: parsed };
}

export default {
  name: 'error-telemetry',
  description: 'error.register/envelope/heartbeat acks, envelope limits, scrubbing, grouping, rate limits, performance events, error-triage routes',
  tenants: { a: ['alice'] },
  tables: [],
  async run(ctx) {
    const from = Date.now() - 60_000;
    const record = { project: ctx.project, tenant: ctx.tenants.a, account: ctx.members.alice.accountId, steps: [] };
    RECORDINGS.set(ctx.backend.name, record);
    const sockets = {};
    const socket = async (role) => {
      if (sockets[role]) return sockets[role];
      const s = await ctx.raw(`k15-${role}`);
      if (role === 'anonymous') await s.auth({ project: ctx.project });
      if (role === 'alice' || role === 'alice2') {
        const session = await ctx.rawLogin(s, 'alice');
        // The socket that sends batches negotiates them; the second one does not.
        await s.auth({ project: ctx.project, tenant: ctx.tenants.a, token: session.accessToken, capabilities: role === 'alice' ? { telemetryBatch: 1 } : {} });
      }
      sockets[role] = s;
      return s;
    };
    const ack = (s, id) => s.next((m) => m.type === 'error.ack' && m.id === id, { label: `error.ack ${id}` });
    let fingerprintA;
    const resolve = (path) => path.replace('$P', encodeURIComponent(ctx.project)).replace('$A', fingerprintA ?? 'unknown');

    for (const entry of SCRIPT) {
      await ctx.step(entry.step, async () => {
        const out = { step: entry.step };
        record.steps.push(out);
        if (entry.frames) {
          const s = await socket(entry.role);
          out.role = entry.role;
          out.acks = {};
          for (const frame of expand(entry.frames)) {
            s.send(frame);
            if (!frame.type.startsWith('error.')) continue;
            const reply = await ack(s, frame.id);
            out.acks[frame.id] = reply;
            ctx.observe(frame.id, reply);
            if (frame.id === 'grp-1') fingerprintA = reply.fingerprints?.[0];
          }
        }
        if (entry.flood) {
          // Heartbeats until the first refusal: how much of the window the earlier steps left.
          const s = await socket(entry.role);
          out.role = entry.role;
          let accepted = 0;
          let refusal;
          for (let i = 0; i < 200; i++) {
            const id = `${entry.flood}-${i}`;
            s.send({ type: 'error.heartbeat', id });
            const reply = await ack(s, id);
            if (reply.error) { refusal = reply; break; }
            accepted++;
          }
          out.flood = { accepted, refusal };
          ctx.observe('acceptedBeforeLimit', accepted);
          ctx.observe('refusal', refusal);
        }
        if (entry.http) {
          out.http = {};
          for (const [label, method, path, body] of HTTP[entry.http]) {
            const reply = await http(ctx, method, resolve(path), body);
            const view = label.startsWith('metrics') ? { status: reply.status, body: metricsView(reply.body) } : reply;
            const scrubbed = scrub(view, { from, to: Date.now() + 60_000, hash });
            out.http[label] = scrubbed;
            ctx.observe(label, scrubbed);
          }
        }
      });
    }
    record.window = { from, to: Date.now() + 60_000 };
    record.fingerprintA = fingerprintA;
  },
};
