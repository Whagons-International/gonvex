/**
 * Conformance check: the thin load client against the real SDK.
 *
 * The same user is signed in twice, once through the real SDK and once through
 * thin-client.mjs, for an admin and for an ordinary member. Both sessions mount
 * the generator's collections. A separate SDK "actor" session then applies a
 * scripted sequence of changes: task edits, a share and revoke, team changes,
 * a detail window, a dropped connection and the observed users' own reducers.
 *
 * After every step, once both sessions are up to date, it compares for every
 * collection: the row key set, and each row's projected column values. It also
 * compares the frames each pair sent during the step: types, paths, arguments,
 * opened collections and ACK counts (ids and timestamps excluded).
 *
 * Rows: legacy JSON rows by default (the SDK without a native staging adapter,
 * as the Whagons app ships it); LOAD_NATIVE_ROWS=true runs both clients with
 * native rows (SDK with file staging).
 *
 * Exits non-zero on any difference. Run it in the private lab:
 *   node thin-conformance.ts
 */
import { createRequire } from 'node:module';
import { readFile, realpath } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { ThinClient } from './thin-client.mjs';
import { installTransportFaults } from './transport-faults.mjs';
import { fileSnapshotStaging } from './native-stage.mjs';

const app = process.env.WHAGONS_CHECKOUT!;
const require = createRequire(app + '/package.json');
const sdkPath = await realpath(app + '/node_modules/@gonvex/client');
const { GonvexClient, control } = await import(pathToFileURL(require.resolve('@gonvex/client')).href);
const { api } = await import(pathToFileURL(app + '/gonvex/_generated/api.ts').href);
const executor = await readFile(app + '/gonvex/_generated/local-executor.ts', 'utf8');
const clientContract = JSON.parse(executor.match(/clientContract: (\{[^}]+\})/)![1]);
const url = process.env.LOAD_URL || 'http://127.0.0.1:8080';
const auth = process.env.LOAD_AUTH_URL || 'http://127.0.0.1:9099';
const { projectId, fixtures } = JSON.parse(await readFile(process.env.LOAD_STATE!, 'utf8'));
const f = fixtures[0];
const nativeRows = process.env.LOAD_NATIVE_ROWS === 'true';
const staging = nativeRows ? fileSnapshotStaging((process.env.LOAD_CONFORMANCE_STAGING || '/tmp/thin-conformance-staging') + '-' + Date.now()) : undefined;
// Object key order is not significant; compare canonical (sorted-key) JSON.
const canonical = (v: any): any => Array.isArray(v) ? v.map(canonical) : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])])) : v;
const pause = (ms: number) => new Promise(r => setTimeout(r, ms));

// ---- frame capture, tagged by the client that opened the socket ------------
let owner: string | undefined;
const sent = new Map<string, any[]>();
const Native = globalThis.WebSocket;
class Captured extends Native {
  constructor(u: any, p?: any) {
    super(u, p);
    const tag = owner!;
    const send = this.send.bind(this);
    (this as any).send = (data: any) => { try { (sent.get(tag) ?? sent.set(tag, []).get(tag)!).push(JSON.parse(String(data))); } catch {} return send(data); };
  }
}
(globalThis as any).WebSocket = Captured;
const faults = installTransportFaults();

const identity = async (email: string) => {
  const body = { email, password: 'local-synthetic-password', returnSecureToken: true };
  let r = await fetch(auth + '/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=local', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  const b: any = await r.json(); if (!r.ok) throw Error('identity failed for ' + email); return b.idToken;
};
const make = (kind: 'sdk' | 'thin', tag: string) => {
  const ws = url.replace('http:', 'ws:') + '/ws';
  const c: any = kind === 'thin' ? new ThinClient(ws, { project: projectId, clientContract, nativeRows })
    : new GonvexClient(ws, { project: projectId, clientContract, outbox: { enabled: false }, ...(staging ? { nativeSnapshotStaging: staging.client().staging } : {}) });
  faults.attach(c);
  const connect = c.connect.bind(c);
  c.connect = (...a: any[]) => { const prev = owner; owner = tag; try { return connect(...a); } finally { owner = prev; } };
  c.connect();
  return c;
};
const login = async (kind: 'sdk' | 'thin', tag: string, email: string) => {
  const c = make(kind, tag);
  const grant = await c.action(control.auth.exchangeExternalToken, { provider: 'firebase', token: await identity(email), tenantId: f.tenant });
  await c.authenticate({ project: projectId, token: grant.accessToken, tenant: f.tenant });
  return { c, member: grant.tenants.find((t: any) => t.id === f.tenant).memberId as string };
};
const call = (c: any, ref: any, args: any) => c.reducer({ ...ref, localExecution: undefined }, args, { offline: 'reject' });

// Same mounts as run.ts connect(): presence, metadata, tasks.
const baseRefs: [any, any][] = [api.presence.list, api.workspaces.list, api.statuses.statuses, api.tags.tags, api.members.listMembers,
  api.members.listMemberTeams, api.taskResources.taskUsers, api.taskResources.taskTags, api.workspacePreferences.workspacePreferences,
  api.tasks.recentWorkspaceTasks].map(ref => [ref, {}] as [any, any]);
type Session = { tag: string; c: any; watches: Map<string, { ref: any; w: any; stop: () => void }> };
const mount = (s: Session, refs: [any, any][]) => {
  for (const [ref, args] of refs) {
    const key = ref.path + JSON.stringify(args);
    if (s.watches.has(key)) continue;
    const w = s.c.watchReplica(ref, args);
    const stop = w.onUpdate(() => { try { w.localReplicaResult(); } catch {} });
    s.watches.set(key, { ref, w, stop });
  }
};
const unmount = (s: Session, refs: [any, any][]) => { for (const [ref, args] of refs) { const key = ref.path + JSON.stringify(args); s.watches.get(key)?.stop(); s.watches.delete(key); } };

const ready = (s: Session) => [...s.watches.values()].every(({ w }) => w.status().isUpToDate && w.localReplicaState()?.completeness === 'complete');
const columnsOf = (ref: any) => ref.replica?.columns as string[] | undefined;
const snapshotOf = (s: Session) => {
  const out: Record<string, Map<string, string>> = {};
  for (const [key, { ref, w }] of s.watches) {
    const cols = columnsOf(ref), k = ref.replica?.key || '_id';
    const rows = (w.localReplicaResult() || []) as any[];
    out[key] = new Map(rows.map(r => [String(r[k]), JSON.stringify(canonical(cols ? cols.map(c => r[c] ?? null) : r))]));
  }
  return out;
};
const diff = (a: Session, b: Session) => {
  const sa = snapshotOf(a), sb = snapshotOf(b), problems: string[] = [];
  for (const key of new Set([...Object.keys(sa), ...Object.keys(sb)])) {
    const x = sa[key] ?? new Map(), y = sb[key] ?? new Map();
    const onlyA = [...x.keys()].filter(id => !y.has(id)), onlyB = [...y.keys()].filter(id => !x.has(id));
    const changed = [...x.keys()].filter(id => y.has(id) && x.get(id) !== y.get(id));
    if (onlyA.length || onlyB.length || changed.length) problems.push(`${key}: sdk-only ${onlyA.length}, thin-only ${onlyB.length}, values differ ${changed.length}` + (changed[0] ? ` e.g. ${changed[0]}\n  sdk  ${x.get(changed[0])!.slice(0, 300)}\n  thin ${y.get(changed[0])!.slice(0, 300)}` : ''));
  }
  return { problems, rows: Object.values(sa).reduce((n, m) => n + m.size, 0) };
};
const settle = async (pairs: [Session, Session][], ms = 30000) => {
  const end = Date.now() + ms;
  let last = '';
  while (Date.now() < end) {
    if (pairs.every(([a, b]) => ready(a) && ready(b) && diff(a, b).problems.length === 0)) {
      // Must stay equal for a moment, so a late frame on either side is caught.
      await pause(750);
      if (pairs.every(([a, b]) => ready(a) && ready(b) && diff(a, b).problems.length === 0)) return '';
    }
    last = pairs.map(([a, b]) => `${a.tag} ready=${ready(a)} / ${b.tag} ready=${ready(b)}: ${diff(a, b).problems.join('; ')}`).join('\n');
    await pause(100);
  }
  return last || 'timed out';
};

// Frame signature: what the server sees, without ids and clocks.
const signature = (m: any) => {
  switch (m.type) {
    case 'auth': return `auth controlOnly=${m.controlOnly} tenant=${!!m.tenant} caps=${Object.keys(m.capabilities || {}).filter(k => m.capabilities[k] !== undefined).sort().join(',')} contract=${m.clientContract}`;
    case 'action.call': return `action ${m.path} scope=${m.scope ?? ''} keys=${Object.keys(m).sort().join(',')}`;
    case 'reducer.call': return `reducer ${m.path} scope=${m.scope ?? ''} args=${JSON.stringify(m.args, (k, v) => /Id$|^id$|^_id$|^entityId$|^taskTagId$/.test(k) && typeof v === 'string' ? '<id>' : /Time$|At$/.test(k) && typeof v === 'number' ? '<time>' : v)} keys=${Object.keys(m).sort().join(',')}`;
    case 'replica.openMany': return 'open ' + m.opens.map((o: any) => `${o.path}${JSON.stringify(o.args)}[${Object.keys(o).sort().join(',')}]`).sort().join(' | ');
    case 'replica.open': return `open ${m.path}${JSON.stringify(m.args)}`;
    case 'replica.snapshotAck': return 'ack';
    default: return m.type;
  }
};
const framesSince = (tag: string, from: number) => (sent.get(tag) || []).slice(from).map(signature);
const counts = (xs: string[]) => { const c = new Map<string, number>(); for (const x of xs) c.set(x, (c.get(x) || 0) + 1); return c; };
const compareFrames = (a: string[], b: string[]) => {
  const ca = counts(a.filter(x => x !== 'ack')), cb = counts(b.filter(x => x !== 'ack')), out: string[] = [];
  for (const k of new Set([...ca.keys(), ...cb.keys()])) if ((ca.get(k) || 0) !== (cb.get(k) || 0)) out.push(`  ${k.slice(0, 260)}: sdk ${ca.get(k) || 0}, thin ${cb.get(k) || 0}`);
  const acksA = a.filter(x => x === 'ack').length, acksB = b.filter(x => x === 'ack').length;
  if (acksA !== acksB) out.push(`  snapshot ACKs: sdk ${acksA}, thin ${acksB}`);
  return out;
};

// ---- scenario ----------------------------------------------------------------
const failures: string[] = [];
const results: any[] = [];
// Users of the largest tenant in load-user order, as staff.py assigns roles:
// ranks 0-1 admins (every team), then members with teams [T0],[T1],[T2],[T0,T3]
// by rank % 4; rank % 7 == 0 sees all spots.
const weights = fixtures.map((_: any, i: number) => [39, 39, 11, 6, 2, 2, 1][i] || 0);
const total = weights.reduce((a: number, b: number) => a + b, 0);
const chooseTenant = (i: number) => { let x = ((i * 0.61803398875) % 1) * total; return fixtures[weights.findIndex((w: number) => (x -= w) < 0)] || fixtures[0]; };
const prepared = Number(process.env.LOAD_PREPARED_USERS || 200);
const ranked = Array.from({ length: prepared }, (_, i) => i).filter(i => chooseTenant(i) === f);
const rank = ranked.findIndex((_, k) => k >= 5 && k % 4 < 3 && k % 7 !== 0);
if (rank < 0 || !f.adminMemberIds?.length) throw Error('fixture has no staffed single-team ordinary member (run staff.py)');
const adminEmail = `load-user-${ranked[0]}@example.invalid`, actorEmail = `load-user-${ranked[1]}@example.invalid`;
const member = { email: `load-user-${ranked[rank]}@example.invalid`, teamIds: [f.teamIds[rank % 4]] };
const extraTeam = f.teamIds.find((t: string) => !member.teamIds.includes(t));

const marks = new Map<string, number>();
const mark = () => { for (const [tag, xs] of sent) marks.set(tag, xs.length); };
const pairs: [Session, Session][] = [];
const step = async (name: string, fn: () => Promise<void>) => {
  mark();
  const at = Date.now();
  let error = '';
  try { await fn(); } catch (e: any) { error = String(e?.message || e); }
  const settleError = await settle(pairs);
  const frameProblems = pairs.flatMap(([a, b]) => { const p = compareFrames(framesSince(a.tag, marks.get(a.tag) || 0), framesSince(b.tag, marks.get(b.tag) || 0)); return p.length ? [`${a.tag}/${b.tag} frames:`, ...p] : []; });
  const rows = pairs.map(([a]) => diff(a, a).rows);
  const ok = !error && !settleError && frameProblems.length === 0;
  results.push({ step: name, ok, ms: Date.now() - at, rows, error: error || undefined, rowDifferences: settleError || undefined, frameDifferences: frameProblems.length ? frameProblems : undefined });
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name} (${Date.now() - at} ms, rows ${rows.join('/')})`);
  if (!ok) { failures.push(name); if (error) console.log('  error: ' + error); if (settleError) console.log('  rows: ' + settleError); for (const p of frameProblems) console.log(p); }
};

const sessions = { adminSdk: { tag: 'admin-sdk' } as Session, adminThin: { tag: 'admin-thin' } as Session, memberSdk: { tag: 'member-sdk' } as Session, memberThin: { tag: 'member-thin' } as Session };
let actor: any, memberId = '', createdId = randomUUID(), shareId: any, addedMemberTeam: any;
await step('sign in and mount collections', async () => {
  for (const [s, kind, email] of [[sessions.adminSdk, 'sdk', adminEmail], [sessions.adminThin, 'thin', adminEmail], [sessions.memberSdk, 'sdk', member.email], [sessions.memberThin, 'thin', member.email]] as const) {
    const { c, member: m } = await login(kind, s.tag, email); s.c = c; s.watches = new Map(); if (email === member.email) memberId = m;
    mount(s, baseRefs);
  }
  pairs.push([sessions.adminSdk, sessions.adminThin], [sessions.memberSdk, sessions.memberThin]);
  actor = (await login('sdk', 'actor', actorEmail)).c;
});
await step('actor creates a task', async () => { await call(actor, api.tasks.create, { entityId: createdId, name: 'Conformance ' + createdId.slice(0, 8), workspaceId: f.workspaceId, categoryId: f.categoryId, priorityId: f.priorityId, statusId: f.statusId }); });
await step('actor renames and reprioritizes it', async () => {
  await call(actor, api.tasks.rename, { taskId: createdId, name: 'Conformance renamed ' + randomUUID().slice(0, 8) });
  await call(actor, api.tasks.setPriority, { taskId: createdId, priorityId: f.priorityIds.find((p: string) => p !== f.priorityId) || f.priorityId });
});
await step('actor tags it and edits its description', async () => {
  await call(actor, api.taskResources.addTagByTaskId, { taskTagId: randomUUID(), taskId: createdId, tagId: f.tag._id });
  await call(actor, api.tasks.updateDescription, { taskId: createdId, description: 'Conformance description ' + randomUUID() });
});
const hidden = () => { const seen = new Set(((sessions.memberSdk.watches.get('tasks.recentWorkspaceTasks{}')!.w.localReplicaResult()) || []).map((r: any) => r._id)); return ((sessions.adminSdk.watches.get('tasks.recentWorkspaceTasks{}')!.w.localReplicaResult()) || []).find((r: any) => !seen.has(r._id) && r.statusId !== f.completedStatusId); };
let sharedTask: any;
await step('actor shares a hidden task with the member (row gain)', async () => { sharedTask = hidden(); if (!sharedTask) throw Error('no hidden task'); shareId = await call(actor, api.taskResources.shareTask, { taskId: sharedTask._id, sharedToUserId: memberId }); });
await step('actor revokes the share (row loss)', async () => { await call(actor, api.taskResources.revokeShare, { id: shareId }); });
await step('actor adds the member to another team (bulk gain, re-auth)', async () => { addedMemberTeam = await call(actor, api.members.addMemberTeam, { memberId, teamId: extraTeam, roleKey: 'member' }); });
await step('actor removes that team membership (bulk loss, re-auth)', async () => { await call(actor, api.members.removeMemberTeam, { id: addedMemberTeam }); });
const detail = (id: string): [any, any][] => [[api.tasks.get, { id }], [api.taskResources.listTaskNotes, { taskId: id }], [api.taskResources.listTaskLogs, { taskId: id }], [api.taskResources.listTaskViewsByTaskId, { taskId: id }], [api.approvals.listTaskApprovals, { taskId: id }]];
await step('both admin sessions open a task detail window', async () => { mount(sessions.adminSdk, detail(createdId)); mount(sessions.adminThin, detail(createdId)); });
await step('observed users call their own reducers', async () => {
  for (const s of [sessions.adminSdk, sessions.adminThin]) {
    await call(s.c, api.taskResources.recordTaskViewByTaskId, { taskId: createdId });
    await call(s.c, api.presence.beat, {});
    await call(s.c, control.support.heartbeat, { release: 'synthetic-local', environment: 'test' });
  }
  for (const s of [sessions.adminSdk, sessions.adminThin]) await call(s.c, api.taskResources.createNote, { entity: { _id: randomUUID(), _creationTime: Date.now(), taskId: createdId, userId: 'x', source: 'web', note: 'Conformance note' } }).catch(() => {});
});
await step('detail windows close (replica.close after retention)', async () => { unmount(sessions.adminSdk, detail(createdId)); unmount(sessions.adminThin, detail(createdId)); await pause(600); });
await step('network drop and reconnect on both admin sessions', async () => { await Promise.all([faults.drop(sessions.adminSdk.c), faults.drop(sessions.adminThin.c)]); await pause(500); });
await step('actor edits during recovery are delivered', async () => { await call(actor, api.tasks.rename, { taskId: createdId, name: 'After reconnect ' + randomUUID().slice(0, 8) }); });

for (const s of Object.values(sessions)) { for (const { stop } of s.watches?.values() ?? []) stop(); s.c?.close(); }
actor?.close();
console.log(JSON.stringify({ conformance: failures.length ? 'failed' : 'passed', rows: nativeRows ? 'native' : 'legacy', sdk: sdkPath.split('/node_modules/').pop(), failures, thinStats: ThinClient.stats, steps: results }, null, 1).slice(0, 4000));
process.exit(failures.length ? 1 : 0);
