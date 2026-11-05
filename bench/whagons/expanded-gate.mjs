import { sha, canonical, validCursor } from './thin-checkpoint.mjs';
import { quantiles } from './safety.mjs';

export function visibilityDelta(before, after) {
  return { additions: [...after].filter(id => !before.has(id)).length,
    removals: [...before].filter(id => !after.has(id)).length };
}

// A deploy refresh is a separate offered burst, never a slower sign-in ramp.
// A missing/unsupported case remains a mandatory qualification failure.
export function qualifyExpanded({ reloads = [], targetUsers, permissions = [], capabilities = {}, samples = {}, errors = [], attempts = [], reloadWindow }) {
  const completed = reloads.filter(r => Number.isFinite(r.readyAt));
  const latencies = completed.map(r => r.readyAt - r.plannedAt);
  const latencyMs = { samples: latencies.length, ...quantiles(latencies) };
  const inWindow = x => reloadWindow && x.at >= reloadWindow[0] && x.at < reloadWindow[1];
  const operationLatencyMs = Object.fromEntries(Object.entries(samples).map(([kind,xs]) => {
    const values=xs.filter(inWindow).map(x=>x.ms);
    return [kind,{samples:values.length,...quantiles(values)}];
  }));
  const failed=errors.filter(inWindow).length;
  const operations=['reducer','query','login','reconnect'].reduce((n,k)=>n+(operationLatencyMs[k]?.samples||0),0);
  const errorRate=operations+failed?failed/(operations+failed):1;
  const slow=Object.entries(operationLatencyMs).filter(([,v])=>v.samples && (v.p95>=500 || v.p99>=1000)).map(([k])=>k);
  const pending=attempts.filter(a=>inWindow(a)&&!a.done).length;
  const reloadPassed = !!reloadWindow && !pending && errorRate<0.01 && !slow.length && reloads.length === targetUsers && new Set(reloads.map(r => r.user)).size === targetUsers
    && completed.length === targetUsers && reloads.every(r => r.cacheSupported && r.cachedWindows > 0 && r.cachedFullSetVerified === true && !r.error)
    && latencyMs.p95 < 500 && latencyMs.p99 < 1000;
  const effective = permissions.filter(p => p.granted && p.revoked && p.oldRowRemoved && p.additions > 0 && p.removals > 0);
  const permissionPassed = effective.length > 0 && permissions.every(p => !p.error)
    && effective.every(p => p.grantMs < 1000 && p.revokeMs < 1000);
  const required = ['currentSQLitePair', 'currentAppOpens', 'fullPermissionCoverage', 'starterNegotiatedPipeline'];
  const blockers = required.filter(k => capabilities[k] !== true);
  return { passed: reloadPassed && permissionPassed && !blockers.length,
    cachedReload: { passed: reloadPassed, attempted: reloads.length, completed: completed.length, latencyMs, operationLatencyMs, errorRate, pendingOperations:pending, failingKinds:slow },
    permissions: { passed: permissionPassed, effectiveCycles: effective.length }, blockers };
}

// Observes the actual SDK transport. It never retries or edits a protocol frame.
// Errors remain failures even if a later open recovers; needHashes is not exempted.
export function replicaLifecycle() {
  const states = new Map(), events = [];
  return { events,
    frame(socket, direction, m, at = Date.now()) {
      const key = id => `${socket}:${id}`;
      if (direction === 'send') {
        const opens = m.type === 'replica.openMany' ? m.opens : m.type === 'replica.open' ? [m] : [];
        for (const open of opens) {
          const state = states.get(key(open.id)) || { path: open.path };
          if (state.failedAt !== undefined) events.push({ type: 'retry', at, failedAt: state.failedAt, path: state.path });
          states.set(key(open.id), state);
        }
        if (m.type === 'replica.close') states.delete(key(m.id));
      } else {
        const state = states.get(key(m.id));
        if (!state) return;
        if (m.type === 'replica.error') {
          state.failedAt ??= at;
          events.push({ type: 'refused', at, failedAt: state.failedAt, path: state.path });
        }
        if (m.type === 'replica.ready' && state.failedAt !== undefined) {
          events.push({ type: 'recovered', at, failedAt: state.failedAt, path: state.path });
          delete state.failedAt;
        }
      }
    },
  };
}

// Check the persisted snapshot supplied to the new SDK, before any network catchup.
export function cachedFullSet(snapshot, signatures) {
  return signatures.length > 0 && signatures.every(key => {
    const w=snapshot?.liveQueries?.[key];
    return w?.completeness==='complete' && validCursor(w.cursor) && w.ids.every(id=>snapshot.entities?.[w.entity]?.[id] !== undefined && String(snapshot.entities[w.entity][id][w.key])===id);
  });
}

export function sdkCacheEvidence(snapshot, signatures) {
 const windows=signatures.map(signature=>({signatureHash:sha(signature),window:snapshot?.liveQueries?.[signature]}));
 return {cachedWindows:Object.keys(snapshot?.liveQueries||{}).length,expectedWindows:signatures.length,
  cachedFullSetVerified:cachedFullSet(snapshot,signatures)&&windows.some(x=>x.window.ids.length>0),
  checkpointInventory:windows.map(({signatureHash,window:w})=>({signatureHash,rows:w?.ids.length,cursor:w?.cursor,digest:w?.aggregateDigest,membershipHash:w?sha(canonical([...w.ids].sort())):undefined}))};
}
