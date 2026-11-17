/**
 * Scenario runner.
 *
 * Runs one scenario against one backend and returns its trace:
 *
 *   { backend, scenario, skipped?, failures: string[], digestsChecked,
 *     steps: [{ name, error?, observations, frames: { <session tag>: normalized frames } }],
 *     checksums: { <tenant>: { <table>: { count, sha256, rows } } } }
 *
 * Each scenario gets a fresh project on the backend: generation 1 of the
 * fixture deployed, then its tenants and Members seeded. Members log in with
 * `control.auth.passwordLogin` from the client that will use the session, as
 * an app does.
 *
 * A trace fails on its own (independently of any diff) when a step throws,
 * an `expect` is false, or a digest the backend sent does not match the rows
 * it delivered (digest-check.mjs). diffTraces (differ.mjs) then compares traces between
 * backends.
 */
import { setTimeout as delay } from 'node:timers/promises';
import { Recorder, sdkClient, thinClient, RawSocket, control } from './sessions.mjs';
import { normalizeSession, normalizeValue, Names } from './normalize.mjs';
import { checksumRows } from './checksum.mjs';
import { checkDigests } from './digest-check.mjs';
import { checkWatermarks } from './watermark-check.mjs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export const PASSWORD = 'conformance-password-1';
/** Client contract both SDKs announce (the fixture generates no local executor). */
export const CLIENT_CONTRACT = { version: 1, offlineMaxAgeMs: null };
const QUIET_MS = 300;
/** Default `ctx.until` limit. The mutation self-test lowers it to fail fast. */
const UNTIL_MS = Number(process.env.GONVEX_CONFORMANCE_UNTIL_MS) || 15_000;
const QUIET_MAX_MS = 5_000;

/** Replaces backend-assigned identifiers with stable aliases, deep. */
class Aliases {
  constructor() { this.map = new Map(); }
  add(value, alias) { if (typeof value === 'string' && value.length >= 6) this.map.set(value, alias); }
  mask = (v) => {
    if (Array.isArray(v)) return v.map(this.mask);
    if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [this.mask(k), this.mask(x)]));
    if (typeof v !== 'string') return v;
    const exact = this.map.get(v);
    if (exact) return exact;
    let out = v;
    for (const [value, alias] of this.map) if (out.includes(value)) out = out.split(value).join(alias);
    return out;
  };
}

class ExpectationError extends Error {}

/**
 * @param {{ scenario: any, backend: import('./backend.mjs').Backend, fixtures: Record<number, any>, verbose?: boolean }} options
 */
export async function runScenario({ scenario, backend, fixtures, verbose = false }) {
  const trace = { backend: backend.name, scenario: scenario.name, failures: [], steps: [], checksums: {} };
  const missing = (scenario.requires ?? []).filter(c => !backend.capabilities[c]);
  if (missing.length) { trace.skipped = `backend lacks ${missing.join(', ')}`; return trace; }

  const recorder = new Recorder();
  const aliases = new Aliases();
  const names = new Names();
  const clients = [];
  let current;

  // ---- seed ---------------------------------------------------------------
  const { projectId } = await backend.createProject(`conformance-${scenario.name}`);
  aliases.add(projectId, '<project>');
  const deployed = await backend.deploy(projectId, fixtures[1].manifest);
  if (!deployed.ok) throw new Error(`${backend.name}: deploy of fixture generation 1 failed`);
  const tenants = {}, members = {};
  for (const [tenantName, memberNames] of Object.entries(scenario.tenants ?? { a: ['alice'] })) {
    // Tenant ids are unique per backend (not per project) on the Rust runtime.
    const tenantId = `${scenario.name}-${tenantName}`;
    aliases.add(tenantId, `<tenant:${tenantName}>`);
    await backend.createTenant(projectId, tenantId, `Tenant ${tenantName.toUpperCase()}`);
    tenants[tenantName] = tenantId;
    for (const m of memberNames) {
      const email = `${m}@conformance.invalid`;
      const r = await backend.createMember(projectId, tenantId, { email, password: PASSWORD, name: m });
      members[m] ??= { name: m, email, password: PASSWORD, accountId: r.accountId, memberIds: {} };
      members[m].memberIds[tenantName] = r.memberId;
      aliases.add(r.accountId, `<account:${m}>`);
      aliases.add(r.memberId, `<member:${m}@${tenantName}>`);
    }
  }

  // ---- context --------------------------------------------------------------
  const quiesce = async () => {
    const end = Date.now() + QUIET_MAX_MS;
    while (Date.now() < end && Date.now() - recorder.lastFrameAt < QUIET_MS) await delay(50);
  };
  // The first client of a Member logs in with control.auth.passwordLogin on
  // its own socket; later clients reuse that session, as a second tab would.
  // (Password hashing makes each login cost ~1.5 s on a debug build.)
  const sessions = new Map();
  const login = async (client, member, fresh = false) => {
    const m = members[member];
    if (!m) throw new Error(`unknown member ${member}`);
    if (sessions.has(member) && !fresh) return sessions.get(member);
    const session = await client.action(control.auth.passwordLogin, { email: m.email, password: m.password });
    aliases.add(session.accessToken, `<access-token>`);
    aliases.add(session.refreshToken, `<refresh-token>`);
    sessions.set(member, session);
    return session;
  };
  const ctx = {
    backend, fixtures, api: fixtures[1].api, project: projectId, tenants, members, wsUrl: backend.wsUrl, PASSWORD, control,
    /**
     * Real SDK client, logged in and authenticated as `member` in `tenant`.
     * The Member's first client logs in over its own socket; later ones reuse
     * that session unless `fresh`. `authenticate: false` stops after login.
     */
    async sdk(tag, { member, tenant = 'a', options = {}, authenticate = true, fresh = false } = {}) {
      const c = sdkClient(backend.wsUrl, tag, recorder, { project: projectId }, { clientContract: CLIENT_CONTRACT, ...options });
      clients.push(c);
      c.connect();
      if (!member) return c;
      const session = await login(c, member, fresh);
      c.session = session;
      if (authenticate) await c.authenticate({ project: projectId, token: session.accessToken, tenant: tenants[tenant] });
      return c;
    },
    /** Thin protocol client, logged in and authenticated. */
    async thin(tag, { member, tenant = 'a', options = {}, fresh = false } = {}) {
      const c = thinClient(backend.wsUrl, tag, recorder, projectId, { clientContract: CLIENT_CONTRACT, ...options });
      clients.push(c);
      c.connect();
      const session = await login(c, member, fresh);
      c.session = session;
      await c.authenticate({ project: projectId, token: session.accessToken, tenant: tenants[tenant] });
      return c;
    },
    /** Raw frame socket (opened, not authenticated). `public: true` uses the `?public=1` transport. */
    async raw(tag, { public: publicTransport = false, query } = {}) {
      // `query` (K21): extra URL parameters, e.g. the Go bridge's `?project=&tenant=`.
      const params = new URLSearchParams(query ?? {});
      if (publicTransport) params.set('public', '1');
      const s = await new RawSocket(params.size ? `${backend.wsUrl}?${params}` : backend.wsUrl, tag, recorder).open();
      clients.push(s);
      return s;
    },
    /** Logs in over a raw socket; returns the control session. */
    async rawLogin(socket, member) {
      if (sessions.has(member)) return sessions.get(member);
      await socket.auth({ project: projectId });
      const id = crypto.randomUUID();
      socket.send({ type: 'action.call', id, path: 'control.auth.passwordLogin', scope: 'control', args: { email: members[member].email, password: PASSWORD }, idempotencyKey: id });
      const reply = await socket.next(m => (m.type === 'action.result' || m.type === 'action.error') && m.id === id, { label: 'passwordLogin' });
      if (reply.type === 'action.error') throw new Error('passwordLogin: ' + reply.error);
      aliases.add(reply.result.accessToken, '<access-token>'); aliases.add(reply.result.refreshToken, '<refresh-token>');
      sessions.set(member, reply.result);
      return reply.result;
    },
    async step(name, fn) {
      if (verbose) console.log(`    [${backend.name}] ${name}`);
      current = { name, observations: {} };
      const at = Date.now();
      trace.steps.push(current);
      recorder.step = trace.steps.length - 1;
      try { await fn(); } catch (error) {
        current.error = (error instanceof ExpectationError ? 'expectation failed: ' : '') + String(error?.message ?? error);
        trace.failures.push(`step "${name}": ${current.error}`);
      }
      await quiesce();
      current.ms = Date.now() - at; // reported, never compared
      if (verbose) console.log(`      ${current.ms} ms${current.error ? ' ERROR ' + current.error.slice(0, 200) : ''}`);
    },
    observe(label, value) { current.observations[label] = value === undefined ? '<undefined>' : value; },
    expect(condition, message) { if (!condition) throw new ExpectationError(message); },
    /** Polls until `check()` is truthy (default 15 s, or $GONVEX_CONFORMANCE_UNTIL_MS). */
    async until(check, label, timeoutMs = UNTIL_MS) {
      const end = Date.now() + timeoutMs;
      for (;;) {
        try { if (await check()) return; } catch {}
        if (Date.now() > end) throw new Error(`timed out waiting for ${label}`);
        await delay(25);
      }
    },
    /** Raw frames a session sent or received (unnormalized). */
    frames: (tag, filter) => recorder.frames(tag, filter),
    /**
     * Registers a server-minted identifier (a created tenant, invitation,
     * account or grant) under a stable alias, as seeded ids are, so frames and
     * observations compare across backends. Returns the value.
     */
    alias(value, name) { aliases.add(value, `<${name}>`); return value; },
    async deploy(generation) { const r = await backend.deploy(projectId, fixtures[generation].manifest); if (!r.ok) throw new Error(`deploy of generation ${generation} failed`); return r; },
    restart: (options) => backend.restart(options),
    quiesce,
    delay,
  };

  // ---- run ------------------------------------------------------------------
  try {
    await scenario.run(ctx);
  } catch (error) {
    trace.failures.push(`scenario aborted: ${error?.stack ?? error}`);
  } finally {
    // Optional read-only deployment diagnostics. They never replace a serving
    // handler or normalize the compared transcript; a failed check is a failure.
    try { await backend.captureEvidence?.({ scenario: scenario.name, project: projectId }); }
    catch (error) { trace.failures.push(`runtime evidence failed: ${error?.message ?? error}`); }
    for (const c of clients) { try { c.close(); } catch {} }
    await quiesce();
  }

  // ---- checksums --------------------------------------------------------------
  for (const [tenantName, tenantId] of Object.entries(tenants)) {
    trace.checksums[tenantName] = {};
    for (const table of scenario.tables ?? ['board', 'notes', 'note_shares']) {
      try { trace.checksums[tenantName][table] = checksumRows(await backend.readTable(projectId, tenantId, table), { mask: aliases.mask }); }
      catch (error) { trace.failures.push(`reading ${tenantName}.${table}: ${error.message}`); }
    }
  }

  // ---- digest self-check (raw frames, before digests are masked) ----------------
  trace.digestsChecked = 0;
  for (const tag of new Set(recorder.records.map(r => r.tag))) {
    const { checked, problems } = checkDigests(recorder.records.filter(r => r.tag === tag));
    trace.digestsChecked += checked;
    for (const p of [...new Set(problems)].slice(0, 5)) trace.failures.push(`digest self-check, session ${tag}: ${p}`);
    if (new Set(problems).size > 5) trace.failures.push(`digest self-check, session ${tag}: ${new Set(problems).size - 5} more mismatches`);
  }

  // ---- watermark self-check (0024 §2: values, which the diff compares only as sent or not) ----
  {
    const { checked, problems } = checkWatermarks(recorder.records);
    trace.watermarksChecked = checked;
    for (const p of [...new Set(problems)].slice(0, 5)) trace.failures.push(`watermark self-check: ${p}`);
    if (new Set(problems).size > 5) trace.failures.push(`watermark self-check: ${new Set(problems).size - 5} more problems`);
  }

  // ---- normalize ----------------------------------------------------------------
  if (process.env.GONVEX_CONFORMANCE_RAW_DIR) {
    const directory = process.env.GONVEX_CONFORMANCE_RAW_DIR;
    await mkdir(directory, { recursive: true, mode: 0o700 });
    // Private synthetic-fixture evidence: may contain session credentials.
    // Never publish this file or substitute it for the unchanged comparison.
    await writeFile(join(directory, `${backend.name}-${scenario.name}.json`), JSON.stringify({ records: recorder.records, steps: trace.steps, checksums: trace.checksums }), { mode: 0o600 });
  }
  const byTag = new Map();
  for (const r of recorder.records) (byTag.get(r.tag) ?? byTag.set(r.tag, []).get(r.tag)).push({ ...r, frame: aliases.mask(r.frame) });
  const normalized = new Map([...byTag].map(([tag, records]) => [tag, normalizeSession(records)]));
  if (recorder.records.some(r => r.step === undefined)) trace.steps.unshift({ name: '<frames outside steps>', observations: {}, index: undefined });
  trace.steps.forEach((step, i) => { if (!('index' in step)) step.index = trace.steps[0].name === '<frames outside steps>' ? i - 1 : i; });
  for (const step of trace.steps) {
    step.frames = {};
    for (const [tag, frames] of normalized) {
      const inStep = frames.filter(f => f.step === step.index).map(({ dir, frame }) => ({ dir, frame }));
      if (inStep.length) step.frames[tag] = inStep;
    }
    step.observations = normalizeValue(aliases.mask(step.observations), names);
    if (step.error) step.error = normalizeValue(aliases.mask(step.error), names);
    delete step.index;
  }
  trace.failures = trace.failures.map(f => normalizeValue(aliases.mask(f), names));
  return trace;
}
