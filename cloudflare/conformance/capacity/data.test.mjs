import { describe, it, expect, vi } from 'vitest';
import { validateBinding } from './binding.mjs';
import { installEffects } from './effects.mjs';
import { schedule } from './schedule.mjs';
import { matches, projectionDigest, reference } from './fixture.mjs';
import { invoke, durable } from './engine.mjs';
import { validateTelemetry, validateCost, METRICS, SCOPES } from './telemetry.mjs';
import { distribution } from './statistics.mjs';

import { binding } from './test-data.mjs';
describe('strict finite local binding DATA (no serving target)', () => {
  it('represents 1000 sessions separately from principals', () => { expect(validateBinding(binding()).tenants[0].principals).toHaveLength(1); });
  it('represents 26 independent tenant-rate lanes', () => { const b = binding(); b.budget.maxConnections = 26000; b.budget.maxOffered = 26000;
    b.tenants = Array.from({ length: 26 }, (_, i) => ({ ...b.tenants[0], alias: `DATA${i}`, tenantId: `DATA${i}` }));
    b.phases[0].tenants = b.tenants.map(t => ({ ...b.phases[0].tenants[0], alias: t.alias })); expect(validateBinding(b).tenants).toHaveLength(26); });
  for (const change of [b => b.assembled = true, b => b.budget.maxWallMs = Infinity, b => b.deployment.endpoint = 'wss://production.invalid',
    b => b.deployment.resources[0].name = 'production', b => b.auth.token = 'DATA', b => b.phases[0].tenants[0].active = 1000,
    b => b.phases[0].tenants[0].rates[0].perSecond = NaN, b => b.budget.maxOffered = 1, b => b.telemetry.extra = true])
    it('refuses malformed/unknown/unbounded input', () => { const b = binding(); change(b); expect(() => validateBinding(b)).toThrow(); });
});
export class DataSocket extends EventTarget {
  static OPEN = 1; static CONNECTING = 0; static CLOSING = 2; static CLOSED = 3; static instances = [];
  readyState = 0; sent = [];
  constructor(url) { super(); this.url = url; DataSocket.instances.push(this); }
  send(s) { this.sent.push(s); }
  close() { this.readyState = 3; this.dispatchEvent(new Event('close')); }
  open() { this.readyState = 1; this.dispatchEvent(new Event('open')); }
}
describe('effect boundary DATA', () => {
  it('guards are installed before an imported module can construct a socket or start fetch', async () => {
    const b = binding(), c = new AbortController(); c.abort(); let calls = 0;
    const f = installEffects({ WebSocket: DataSocket, fetch: async () => { calls++; }, socketUrl: b.deployment.endpoint, httpUrls: ['DATA'], deadline: Infinity, signal: c.signal, budget: b.budget });
    vi.stubGlobal('WebSocket', f.WebSocket); vi.stubGlobal('fetch', f.fetch);
    try {
      const url = `data:text/javascript,${encodeURIComponent(`await fetch('DATA'); new WebSocket('${b.deployment.endpoint}');`)}`;
      await expect(import(/* @vite-ignore */ url)).rejects.toThrow(); expect(calls).toBe(0);
      const socketImport = `data:text/javascript,${encodeURIComponent(`new WebSocket('${b.deployment.endpoint}');`)}`;
      await expect(import(/* @vite-ignore */ socketImport)).rejects.toThrow();
    } finally { f.stop(); vi.unstubAllGlobals(); }
  });
  it('fences constructors, direct sends and fetch even if a deadline timer has not run', async () => {
    let now = 0, calls = 0; const b = binding();
    const f = installEffects({ WebSocket: DataSocket, fetch: async () => { calls++; }, socketUrl: b.deployment.endpoint, httpUrls: ['https://DATA'], deadline: 10, budget: b.budget, now: () => now });
    const s = new f.WebSocket(b.deployment.endpoint); s.send('DATA'); now = 10;
    expect(() => s.send('late receipt/auth')).toThrow(); expect(() => new f.WebSocket(b.deployment.endpoint)).toThrow();
    await expect(f.fetch('https://DATA')).rejects.toThrow(); expect(calls).toBe(0); expect(s.sent).toHaveLength(1); f.stop();
  });
  it('aborts owned fetches and closes sockets on stop', async () => {
    let captured; const b = binding(); const f = installEffects({ WebSocket: DataSocket, fetch: async (_u, o) => { captured = o.signal; await new Promise(r => o.signal.addEventListener('abort', r)); }, socketUrl: b.deployment.endpoint, httpUrls: ['DATA'], deadline: Infinity, budget: b.budget });
    const s = new f.WebSocket(b.deployment.endpoint), p = f.fetch('DATA'); f.stop(); await p;
    expect(captured.aborted).toBe(true); expect(s.readyState).toBe(3);
  });
  it('an already aborted lifetime fences import-time effects; expiry cannot be undone by rollback', async () => {
    const b = binding(), c = new AbortController(); c.abort(); let effects = 0;
    const f = installEffects({ WebSocket: DataSocket, fetch: async () => { effects++; }, socketUrl: b.deployment.endpoint, httpUrls: ['DATA'], deadline: Infinity, signal: c.signal, budget: b.budget });
    expect(() => new f.WebSocket(b.deployment.endpoint)).toThrow(); await expect(f.fetch('DATA')).rejects.toThrow(); expect(effects).toBe(0);
    let clock = 10; const g = installEffects({ WebSocket: DataSocket, fetch: async () => {}, socketUrl: b.deployment.endpoint, httpUrls: [], deadline: 10, now: () => clock, budget: b.budget });
    expect(() => g.check()).toThrow(); clock = 0; expect(() => g.check()).toThrow();
  });
  it('bounds wire bytes and allocation rate independently of sends', () => {
    const b = binding(); b.budget.maxSentBytes = 2;
    const f = installEffects({ WebSocket: DataSocket, fetch: async () => {}, socketUrl: b.deployment.endpoint, httpUrls: [], deadline: Infinity, budget: b.budget });
    const s = new f.WebSocket(b.deployment.endpoint); expect(() => new f.WebSocket(b.deployment.endpoint)).toThrow(); expect(() => s.send('oversize')).toThrow(); expect(s.sent).toHaveLength(0); f.stop();
  });
  it('bounds aggregate actual native queues before a further send', () => {
    const b = binding(); b.budget.maxBufferedBytes = 10; let now = 0;
    const f = installEffects({ WebSocket: DataSocket, fetch: async () => {}, socketUrl: b.deployment.endpoint, httpUrls: [], deadline: Infinity, budget: b.budget, now: () => now });
    const first = new f.WebSocket(b.deployment.endpoint); first.bufferedAmount = 5; now = 1000;
    const second = new f.WebSocket(b.deployment.endpoint); second.bufferedAmount = 5;
    expect(() => second.send('x')).toThrow(); expect(second.sent).toHaveLength(0); expect(f.snapshot().sentBytes).toBe(0); f.stop();
  });
});
describe('open loop DATA', () => {
  it('keeps intended timestamps with independent rates and no retry/double count', async () => {
    let clock = 0; const offered = [], dispatched = [];
    await schedule({ durationMs: 1000, tenants: [{ alias: 'a', rates: [{ operation: 'q', perSecond: 10 }] }, { alias: 'b', rates: [{ operation: 'q', perSecond: 20 }] }] },
      { signal: new AbortController().signal, check() {}, emit: e => offered.push(e), dispatch: async (t, _o, at) => { dispatched.push([t.alias, at]); }, maxPending: 100, maxLagMs: 100, now: () => clock, wait: async ms => { clock += ms; } });
    expect(offered).toHaveLength(30); expect(dispatched.filter(x => x[0] === 'a')).toHaveLength(10); expect(dispatched.filter(x => x[0] === 'b')).toHaveLength(20);
    expect(dispatched.filter(x => x[0] === 'a').map(x => x[1])).toEqual(Array.from({ length: 10 }, (_, i) => i * 100));
  });
  it('retains lag and stops instead of coordinated omission', async () => {
    let clock = 0; const events = [];
    await expect(schedule({ durationMs: 100, tenants: [{ alias: 'a', rates: [{ operation: 'q', perSecond: 1 }] }] },
      { signal: new AbortController().signal, check() {}, emit: e => events.push(e), dispatch() { throw Error('must not send'); }, maxPending: 1, maxLagMs: 10, now: () => clock, wait: async () => { clock += 11; } })).rejects.toThrow();
    expect(events[0].offered).toBe(1); expect(events[0].generatorLagMs).toBe(11);
  });
  it('cancels without dispatch', async () => {
    const c = new AbortController(); c.abort(); let n = 0;
    await expect(schedule({ durationMs: 1000, tenants: [{ rates: [{ operation: 'q', perSecond: 1 }] }] }, { signal: c.signal, check() {}, dispatch() { n++; } })).rejects.toThrow(); expect(n).toBe(0);
  });
});
describe('command and receiver evidence DATA', () => {
  it('never calls ordinary reducer or retries an ambiguous write', async () => {
    let calls = 0; const rows = [], events = [], b = binding();
    const session = { alias: 'DATA', client: { reducerWithTransactionEvidence: async () => { calls++; throw { possibleWriteApplied: true, reason: 'deadline' }; } } };
    const observer = { fixture: { replica: { keyPath: ['id'] } }, replica: { localReplicaResult: () => rows } };
    await expect(invoke(session, observer, { kind: 'reducer', alias: 'write', ref: ['rename'] }, { args: {}, expect: { key: 'DATA', valuePath: ['name'], value: 'new' } },
      { b, api: { rename: { kind: 'reducer', path: 'tasks.rename' } }, check() {}, signal: new AbortController().signal, emit: e => events.push(e), intended: performance.now(), id: 'DATA' })).rejects.toThrow();
    expect(calls).toBe(1); expect(events.map(e => e.type)).toEqual(['attempt', 'UNKNOWN']); expect(events.filter(e => e.completed)).toHaveLength(0);
  });
  it('requires exact matching original transaction evidence; ordinary response/later fence is not enough', () => {
    expect(durable({ result: true })).toBe(false); expect(durable({ confirmation: { source: 'server-durable', basis: 'receipt-fence', cursor: { epoch: 'x', revision: 50 } } })).toBe(false);
    expect(durable({ confirmation: { source: 'server-durable', basis: 'matching-replica-transaction', cursor: { epoch: 'x', revision: 4 } } })).toBe(true);
  });
  it('uses receiver state, refuses duplicate rows, and preserves genuine refs', () => {
    const e = { key: 'DATA', valuePath: ['name'], value: 'new' }, rows = [{ id: 'DATA', name: 'new' }];
    expect(matches(rows, ['id'], e)).toBe(true); expect(matches([...rows, ...rows], ['id'], e)).toBe(false);
    expect(projectionDigest(rows, ['id'], [e])).not.toBe(projectionDigest([], ['id'], [e]));
    const ref = { kind: 'reducer', path: 'tasks.rename', localExecution: 1 }; expect(reference({ task: ref }, ['task'], 'reducer')).toBe(ref);
  });
  it('counts durable and receiver completion independently; send completion is not observation', async () => {
    let rows = [], calls = 0; const events = [], b = binding();
    const result = { confirmation: { source: 'server-durable', basis: 'matching-replica-transaction', cursor: { epoch: 'DATA', revision: 1 } } };
    const client = { reducerWithTransactionEvidence: async () => { calls++; setTimeout(() => { rows = [{ id: 'DATA', name: 'new' }]; }, 5); return result; } };
    const observer = { fixture: { replica: { keyPath: ['id'] } }, client: { localSessionState: () => 'authenticated', connectionState: () => ({ isWebSocketConnected: true }), activeArtifactHash: () => b.deployment.moduleSha256 },
      replica: { localReplicaResult: () => rows, localReplicaState: () => ({ source: 'server', isUpToDate: true, completeness: 'complete' }) } };
    await invoke({ alias: 'DATA', client }, observer, { kind: 'reducer', alias: 'write', ref: ['rename'] }, { args: {}, expect: { key: 'DATA', valuePath: ['name'], value: 'new' } },
      { b, api: { rename: { kind: 'reducer', path: 'tasks.rename' } }, check() {}, signal: new AbortController().signal, emit: e => events.push(e), intended: performance.now(), id: 'DATA' });
    expect(calls).toBe(1); expect(events.map(e => e.type)).toEqual(['attempt', 'durable', 'receiver-applied']);
    expect(events.reduce((sum, e) => sum + (e.completed ?? 0), 0)).toBe(1);
  });
  it('reports empty latency distribution UNKNOWN and never counts censored timeouts as zeros', () => { expect(distribution([])).toEqual({ samples: 0, p50: null, p95: null, p99: null }); expect(distribution([10, 30]).p95).toBe(30); });
});
function sample(b) { return {v:1,runId:b.runId,sourceSha:null,moduleSha256:b.deployment.moduleSha256,configSha256:null,resources:b.deployment.resources,
 sequence:1,sampledAtUnixMs:1000,sampler:{requests:1,responses:1,requestLoss:0,errors:0},sourceHistory:'UNKNOWN',observation:'cached',
 induced:{rpcAttempts:2,catalogAttempts:1,controllerAttempts:1,capacityAttempts:0,drainStatusAttempts:0,actualControllerWakeups:null,actualHolderWakeups:null,snapshotMayAdvance:false,socketSampleMayRecord:false,requestsWithUnknownRpcEffects:0,controllerReadMayWake:true,holdersMayWake:false},
 services:SCOPES.map(scope=>({scope,id:'DATA',...Object.fromEntries(METRICS.map(k=>[k,null]))})),holders:[],events:[],correctness:{duplicateWrites:null,lostWrites:null},port:{pages:[]},
 eligibility:{boundedExecution:true,fullCapacity:'NOT_ELIGIBLE',reasons:['SOURCE_EVENT_HISTORY_UNKNOWN']}}; }
export function provider(b,asOf=1000,total=900){return {v:1,resourceVersions:b.deployment.resources,identityAsOfUnixMs:asOf,sourceSha:null,configurationSha256:null,
 window:{id:b.telemetry.providerWindowId,startUnixMs:1000,asOfUnixMs:asOf,baselineCostUsd:900,totalCostUsd:total,coverage:'COMPLETE_TO_ASOF'},facts:{path:'/DATA',sha256:'0'.repeat(64)},attribution:{path:'/DATA',sha256:'0'.repeat(64)}};}
describe('measurement judgments DATA',()=>{
 it('retains UNKNOWN source history while allowing explicitly bounded execution',()=>{const b=binding();expect(validateTelemetry(sample(b),b,undefined,1000).sourceHistory).toBe('UNKNOWN');b.telemetry.judgment='full-capacity';expect(()=>validateTelemetry(sample(b),b,undefined,1000)).toThrow();});
 it('rejects a rate relabelled as a cumulative counter',()=>{const b=binding(),s=sample(b);s.services.find(v=>v.scope==='gateway').frames=4;expect(()=>validateTelemetry(s,b,undefined,1000)).toThrow();});
 it('keeps actual wakeups UNKNOWN even though cached requests attempted two RPCs',()=>{const b=binding(),s=sample(b);expect(validateTelemetry(s,b,undefined,1000).induced.rpcAttempts).toBe(2);s.induced.actualControllerWakeups=1;expect(()=>validateTelemetry(s,b,undefined,1000)).toThrow();});
 it('does not charge lifetime spend; accepts scoped initial baseline and budgets updates by run delta',()=>{const b=binding(),base=provider(b);expect(validateCost(base,b,undefined,1000)).toBe(0);const update=provider(b,1001,900.5);expect(validateCost(update,b,base,1001)).toBe(.5);expect(()=>validateCost(provider(b,1001,901),b,base,1001)).toThrow();});
 it('rejects changing baselines, incomplete attribution, metering lag or missing measured spend',()=>{const b=binding(),base=provider(b);for(const mutate of [c=>c.window.baselineCostUsd=899,c=>c.window.coverage='UNKNOWN',c=>c.window.totalCostUsd=null,c=>c.window.id='other']){const c=provider(b,1001);mutate(c);expect(()=>validateCost(c,b,base,1001)).toThrow();}expect(()=>validateCost(base,b,undefined,2001)).toThrow();});
 it('rejects wrong observed five-component identity and replayed telemetry',()=>{const b=binding(),s=sample(b);expect(()=>validateTelemetry(s,b,s,1000)).toThrow();const c=provider(b);c.resourceVersions=c.resourceVersions.slice(0,4);expect(()=>validateCost(c,b,undefined,1000)).toThrow();});
});
