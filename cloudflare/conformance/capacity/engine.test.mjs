import { it, expect } from 'vitest';
import { execute } from './engine.mjs';
import { binding } from './test-data.mjs';

// DATA-only orchestration seam; production worker imports only the hash-bound genuine SDK.
function setup() {
  const b = binding(), instances = [], events = [], credentials = { sessions: [] };
  b.tenants = ['A', 'B'].map(alias => ({ ...b.tenants[0], alias, tenantId: alias })); b.budget.opensPerSecond = 1000;
  const phaseTenant = alias => ({ alias, connections: alias === 'A' ? 3 : 2, active: 1, reconnect: 0, rates: [{ operation: 'q', perSecond: 1000 }] });
  b.phases = [{ name: 'grow', stage: 'warmup', durationMs: 1, tenants: ['A', 'B'].map(phaseTenant) }, { name: 'shrink-reconnect', stage: 'measure', durationMs: 1, tenants: [{ ...phaseTenant('A'), connections: 2, reconnect: 1 }] }];
  const f = { ownedIds: ['DATA'], replica: { ref: ['rows'], args: {}, keyPath: ['id'] }, liveQueries: [],
    operations: [{ alias: 'q', kind: 'query', ref: ['rows'], identityPaths: [], cases: [{ args: {}, expect: null }] }], cleanup: [], finalChecks: [{ key: 'DATA', valuePath: ['name'], value: 'DATA' }] };
  for (const t of b.tenants) credentials.sessions.push({ principal: 'DATA', tenantId: t.tenantId, accountId: 'a', memberId: 'm', token: 'DATA' });
  class Client {
    constructor(_url, options) { this.options = options; this.closed = false; instances.push(this); }
    close() { this.closed = true; } connect() { this.closed = false; }
    connectionState() { return { isWebSocketConnected: !this.closed, connectionCount: 1, connectionRetries: 0 }; }
    localSessionState() { return 'authenticated'; } activeArtifactHash() { return b.deployment.moduleSha256; }
    tenantAdmission() { return { project: 'DATA', tenantId: this.options.tenant, accountId: 'a', memberId: 'm' }; }
    subscribeToConnectionState() { return () => {}; }
    watchReplica() { return { localReplicaResult: () => [{ id: 'DATA', name: 'DATA' }], localReplicaState: () => ({ source: 'server', completeness: 'complete', isUpToDate: true }), onUpdate: () => () => {} }; }
    async query() { return []; }
    reducerWithTransactionEvidence() { throw Error('no write in this DATA test'); }
  }
  return { b, sdk: { GonvexClient: Client }, api: { rows: { kind: 'query', path: 'DATA.rows' } }, localRuntime: { artifactHash: b.deployment.moduleSha256 }, credentials,
    fixtures: new Map(b.tenants.map(t => [t.alias, f])), check() {}, signal: new AbortController().signal, emit: e => events.push(e), beginCleanup() {}, instances, events };
}
it('runs growing/shrinking tenant rates, active rotation, reconnect and owned cleanup without fabricated durability', async () => {
  const c = setup(); const result = await execute(c);
  expect(result.execution).toBe('EXECUTED_NOT_VERIFIED'); expect(c.instances).toHaveLength(7); expect(c.instances.every(s => s.closed)).toBe(true);
  expect(c.events.filter(e => e.type === 'offered')).toHaveLength(3); expect(c.events.filter(e => e.type === 'query-response')).toHaveLength(3);
  expect(c.events.filter(e => e.completed)).toHaveLength(0); expect(c.events.at(-1).quiescence).toBe('UNKNOWN');
});
it('stops at offered budget and still closes its actual owned DATA clients; remote cleanup stays UNKNOWN', async () => {
  const c = setup(); c.b.budget.maxOffered = 1;
  await expect(execute(c)).rejects.toThrow(); expect(c.instances.every(s => s.closed)).toBe(true);
  expect(c.events.at(-1).cleanup).toBe('UNKNOWN'); expect(c.events.at(-1).finalOwner).toBe('UNKNOWN');
});
