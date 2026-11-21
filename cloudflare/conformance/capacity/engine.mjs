import { Refusal, refuse, hash } from './binding.mjs';
import { reference, matches, projectionDigest } from './fixture.mjs';
import { schedule, sleep } from './schedule.mjs';
import { cohort } from './cohort.mjs';

export async function until(predicate, { check, signal, timeoutMs }) {
  const end = performance.now() + timeoutMs;
  for (;;) { check(); if (signal.aborted) throw new Refusal('STOPPED', 'cancelled');
    if (predicate()) return;
    if (performance.now() >= end) throw new Refusal('UNKNOWN', 'receiver/readiness-timeout');
    await sleep(Math.min(20, end - performance.now()), signal);
  }
}
export function ready(session, b) {
  const s = session.replica.localReplicaState(), c = session.client;
  return c.localSessionState() === 'authenticated' && c.connectionState().isWebSocketConnected
    && c.activeArtifactHash() === b.deployment.moduleSha256 && s?.source === 'server' && s?.isUpToDate === true
    && s?.completeness === 'complete';
}
export function durable(receipt) {
  const c = receipt?.confirmation;
  return c?.source === 'server-durable' && c?.basis === 'matching-replica-transaction'
    && typeof c.cursor?.epoch === 'string' && Number.isSafeInteger(c.cursor?.revision) && c.cursor.revision >= 0;
}
/** Actual public SDK methods only. There is intentionally no ordinary-reducer evidence fallback. */
export async function invoke(session, observer, operation, c, context) {
  const { b, api, check, signal, emit, intended, id } = context;
  const started = performance.now(); check();
  const ref = reference(api, operation.ref, operation.kind);
  if (operation.kind !== 'query' && matches(observer.replica.localReplicaResult(), observer.fixture.replica.keyPath, c.expect)) refuse('receiver.preexisting-expectation');
  const row = { tenantAlias: session.alias, operation: operation.alias, receipt: id, intendedMs: intended, startedMs: started,
    phase: context.phase ?? 'cleanup', stage: context.stage ?? 'cleanup' };
  emit({ ...row, type: 'attempt' }); // Persist identity BEFORE effect; no args/result/customer content.
  let receipt;
  try {
    if (operation.kind === 'reducer') {
      if (typeof session.client.reducerWithTransactionEvidence !== 'function') throw new Refusal('UNSUPPORTED', 'SDK.reducerWithTransactionEvidence');
      receipt = await session.client.reducerWithTransactionEvidence(ref, c.args, { offline: 'reject', optimistic: [], timeoutMs: b.budget.operationTimeoutMs, execution: 'server' });
      if (!durable(receipt)) throw new Refusal('UNKNOWN', 'command-bound-durable-receipt');
      emit({ ...row, type: 'durable', dispatched: 1, completed: 1, durableConfirmationMs: performance.now() - intended,
        serviceConfirmationMs: performance.now() - started, epochHash: hash(receipt.confirmation.cursor.epoch), revision: receipt.confirmation.cursor.revision });
    } else {
      await session.client[operation.kind](ref, c.args, { timeoutMs: b.budget.operationTimeoutMs });
      emit({ ...row, type: operation.kind === 'query' ? 'query-response' : 'action-response-NOT-durable', dispatched: 1 });
    }
  } catch (e) {
    emit({ ...row, type: e.possibleWriteApplied === false ? 'not-eligible' : 'UNKNOWN', dispatched: e.possibleWriteApplied === false ? 0 : 1,
      errors: 1, timeouts: e.reason === 'deadline' ? 1 : 0, possibleWriteApplied: e.possibleWriteApplied !== false });
    throw new Refusal(e.code === 'transaction-evidence-unavailable' && e.possibleWriteApplied === false ? 'NOT_ELIGIBLE' : 'UNKNOWN', 'SDK-attempt');
  }
  if (c.expect) {
    try {
      await until(() => ready(observer, b) && matches(observer.replica.localReplicaResult(), observer.fixture.replica.keyPath, c.expect),
        { check, signal, timeoutMs: b.budget.operationTimeoutMs });
      emit({ ...row, type: 'receiver-applied', receiverDelayMs: performance.now() - intended, receiverSampleCount: 1 });
    } catch (e) { emit({ ...row, type: 'receiver-censored', timeouts: 1 }); throw e; }
  }
}

export async function execute({ b, sdk, api, localRuntime, credentials, fixtures, check, signal, emit, beginCleanup, cancel, awaitClosed }) {
  if (!sdk.GonvexClient || !sdk.GonvexClient.prototype.reducerWithTransactionEvidence) throw new Refusal('UNSUPPORTED', 'SDK.reducerWithTransactionEvidence');
  if (!localRuntime || localRuntime.artifactHash !== b.deployment.moduleSha256) refuse('generated-localRuntime.artifact');
  const sessions = new Map(), cursors = new Map(), visited = new Set(); let sequence = 0, offered = 0, created = 0, closeCalls = 0;
  let admission = Promise.resolve(), nextOpen = 0;
  const pacedStart = () => {
    const next = admission.then(async () => { check(); await sleep(Math.max(0, nextOpen - performance.now()), signal); check(); nextOpen = performance.now() + 1000 / b.budget.opensPerSecond; });
    admission = next.catch(() => {}); return next;
  };
  const all = () => [...sessions.values()].flat();
  const close = s => { for (const off of s.off) { try { off(); } catch {} } closeCalls++; s.client.close(); };
  const create = async (tenant, index) => {
    await pacedStart(); check(); const principal = tenant.principals[index % tenant.principals.length];
    const credential = credentials.sessions.find(c => c.principal === principal && c.tenantId === tenant.tenantId);
    if (!credential) refuse('principal-pool');
    const fixture = fixtures.get(tenant.alias);
    const client = new sdk.GonvexClient(b.deployment.endpoint, { tenant: tenant.tenantId, project: tenant.project,
      fetchToken: async () => { check(); return credential.token; }, localRuntime, localDependencyResidency: 'off',
      outbox: { enabled: false }, telemetry: false, errorReporting: false,
      localReplica: { maxResidentRows: b.budget.maxResidentRows, maxResidentBytes: b.budget.maxResidentBytes },
      timeouts: { queryTimeoutMs: b.budget.operationTimeoutMs, reducerTimeoutMs: b.budget.operationTimeoutMs, actionTimeoutMs: b.budget.operationTimeoutMs } });
    const session = { client, fixture, alias: tenant.alias, busy: false, off: [], replica: null };
    created++;
    // Register ownership BEFORE any effectful subscription can fail.
    sessions.get(tenant.alias)[index] = session;
    session.off.push(client.subscribeToConnectionState(state => emit({ type: 'connection', tenantAlias: tenant.alias, sessionAlias: `${tenant.alias}-${index}-${created}`,
      connected: state.isWebSocketConnected ? 1 : 0, connectionAttempts: state.connectionCount, reconnectAttempts: state.connectionRetries })));
    session.replica = client.watchReplica(reference(api, fixture.replica.ref, 'query'), fixture.replica.args);
    session.off.push(session.replica.onUpdate(() => {}));
    for (const q of fixture.liveQueries) session.off.push(client.watchLiveQuery(reference(api, q.ref, 'query'), q.args).onUpdate(() => {}));
    await until(() => ready(session, b), { check, signal, timeoutMs: b.budget.setupTimeoutMs });
    const admission = client.tenantAdmission();
    if (!admission || admission.project !== tenant.projectId || admission.tenantId !== tenant.tenantId || admission.accountId !== credential.accountId || admission.memberId !== credential.memberId) refuse('authoritative-principal-identity');
    return session;
  };
  const emitOffered = e => { if (e.offered) { offered += e.offered; if (offered > b.budget.maxOffered) throw new Refusal('STOPPED', 'offered-budget'); } emit(e); };
  let cleanup = 'UNKNOWN', execution = 'STOPPED_OR_FAILED';
  try {
    for (const phase of b.phases) {
      check(); emit({ type: 'phase', phase: phase.name, stage: phase.stage });
      for (const [alias, pool] of sessions) if (!phase.tenants.some(t => t.alias === alias)) { pool.forEach(close); sessions.delete(alias); }
      for (const t of phase.tenants) {
        visited.add(t.alias);
        const tenant = b.tenants.find(v => v.alias === t.alias); let pool = sessions.get(t.alias);
        if (!pool) { pool = []; sessions.set(t.alias, pool); }
        while (pool.length > t.connections) close(pool.pop());
        const limits = { limit: b.budget.maxPending, intervalMs: 1000 / b.budget.opensPerSecond, check, signal, cancel };
        await cohort(Array.from({ length: t.connections - pool.length }, (_, i) => pool.length + i), index => create(tenant, index), limits);
        // Public close() disposes the SDK's replica/outbox/watch ownership. A planned fresh-client reconnect
        // must construct a new client and re-establish its real observations; never close()+connect() reuse.
        await cohort(Array.from({ length: t.reconnect }, (_, i) => i), async index => {
          check(); close(pool[index]); await awaitClosed(); check();
          await create(tenant, index); emit({ type: 'planned-fresh-client-reconnect', tenantAlias: t.alias });
        }, limits);
      }
      emit({ type: 'population', connected: all().filter(s => s.client.connectionState().isWebSocketConnected).length,
        active: phase.tenants.reduce((n, t) => n + t.active, 0),
        uniqueAccounts: new Set(all().map(s => s.client.tenantAdmission()?.accountId)).size,
        uniqueTenantMembers: new Set(all().map(s => `${s.alias}:${s.client.tenantAdmission()?.memberId}`)).size });
      await schedule(phase, { signal, check, emit: e => emitOffered({ ...e, phase: phase.name, stage: phase.stage }), maxPending: b.budget.maxPending, maxLagMs: b.budget.maxLagMs,
        async dispatch(t, alias, intended) {
          const pool = sessions.get(t.alias), f = fixtures.get(t.alias), operation = f.operations.find(o => o.alias === alias);
          if (!operation) refuse('fixture.operation');
          const rotationKey = `${t.alias}:rotation`, rotation = cursors.get(rotationKey) ?? 0;
          const s = pool[rotation % t.active]; cursors.set(rotationKey, rotation + 1);
          if (s.busy) { emit({ type: 'censored-client-busy', tenantAlias: t.alias, operation: alias, timeouts: 1 }); return; }
          const key = `${t.alias}:${alias}`, n = cursors.get(key) ?? 0;
          const c = operation.cases[operation.kind === 'query' ? n % operation.cases.length : n];
          if (!c) refuse('fixture.cases-exhausted'); cursors.set(key, n + 1); s.busy = true;
          try { await invoke(s, pool.at(-1), operation, c, { b, api, check, signal, emit, intended, phase: phase.name, stage: phase.stage, id: `${b.runId}-${++sequence}` }); }
          finally { s.busy = false; }
        } });
    }
    beginCleanup();
    // Reopen only the two owned clients necessary to inspect/clean a tenant removed in an earlier ramp.
    for (const alias of visited) {
      for (const [other, pool] of sessions) if (other !== alias) { pool.forEach(close); sessions.delete(other); }
      if (!sessions.has(alias)) {
        sessions.set(alias, []); const tenant = b.tenants.find(t => t.alias === alias);
        for (let i = 0; i < 2; i++) { await sleep(1000 / b.budget.opensPerSecond, signal); await create(tenant, i); }
      }
      const pool = sessions.get(alias);
      const observer = pool.at(-1), f = fixtures.get(alias);
      for (const op of f.cleanup) for (const c of op.cases) {
        if (!matches(observer.replica.localReplicaResult(), f.replica.keyPath, c.expect)) {
          emitOffered({ type: 'cleanup-offered', tenantAlias: alias, operation: op.alias, offered: 1 });
          await invoke(pool[0], observer, op, c, { b, api, check, signal, emit, intended: performance.now(), id: `${b.runId}-cleanup-${++sequence}` });
        }
      }
      const rows = observer.replica.localReplicaResult();
      const match = ready(observer, b) && f.finalChecks.every(e => matches(rows, f.replica.keyPath, e));
      emit({ type: 'final-field-projection', tenantAlias: alias, digestMatch: match,
        observedProjectionHash: projectionDigest(rows ?? [], f.replica.keyPath, f.finalChecks) });
      if (!match) throw new Refusal('UNKNOWN', 'final-field-projection');
      pool.forEach(close); sessions.delete(alias);
    }
    cleanup = 'OBSERVED_FIELD_PROJECTION_ONLY'; execution = 'EXECUTED_NOT_VERIFIED';
    return { execution, cleanup };
  } finally {
    for (const s of all()) { try { close(s); } catch {} }
    emit({ type: 'cleanup', cleanup, execution, ownClientsCreated: created, ownClientCloseCalls: closeCalls, quiescence: 'UNKNOWN', finalOwner: 'UNKNOWN' });
  }
}
