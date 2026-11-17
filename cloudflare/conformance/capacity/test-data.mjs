// Local DATA-only factory. Never an execution binding.
export function binding() {
  const file = { path: '/DATA-only', sha256: '0'.repeat(64) };
  return { v: 1, runId: 'DATA-only', cell: 'DATA', deployment: { accountId: '30962192f9607a89eda812daa15e55fa', sourceSha: '0'.repeat(40), moduleSha256: '0'.repeat(64), configSha256: '0'.repeat(64),
    endpoint: 'wss://gonvex-cf-dev-data.whagons-international.workers.dev/ws', resources: ['kernel', 'runtime', 'actions', 'builder', 'sandbox'].map(role => ({ role, name: role === 'kernel' ? 'gonvex-cf-dev-data' : `gonvex-cf-dev-data-${role}`, version: 'DATA-only' })), catalogVersion: 'DATA', catalogReleases: [{ projectId: 'DATA', moduleSha256: '0'.repeat(64) }] },
    sources: { sdk: file, refs: file, localRuntime: null, sdkSourceSha: '0'.repeat(40), webSourceSha: '0'.repeat(40), closure: [file] },
    auth: { fileEnv: 'WHAGONS_CREDENTIAL_FILE' }, resourceAdmission: null, tenants: [{ alias: 'DATA', tenantId: 'DATA', project: 'DATA', projectId: 'DATA', principals: ['DATA'], fixture: file, dataProvenance: null }],
    mix: { kind: 'synthetic-stress', evidence: null }, telemetry: { url: null, adapter: null, contractSha256: null, providerFile: null },
    phases: [{ name: 'DATA', stage: 'measure', durationMs: 1000, tenants: [{ alias: 'DATA', connections: 1000, active: 999, reconnect: 1000, rates: [{ operation: 'rename', perSecond: 10 }] }] }],
    budget: { maxWallMs: 3000, maxConnections: 1000, maxPending: 4, maxOffered: 1000, maxSentBytes: 10000, maxReceivedBytes: 10000, maxBufferedBytes: 10000, maxArtifactBytes: 10000,
      maxResidentRows: 1000, maxResidentBytes: 102400, maxGeneratorRssBytes: 2 ** 30, maxGeneratorFds: 4096,
      maxUsd: 1, maxLagMs: 100, operationTimeoutMs: 100, setupTimeoutMs: 1000, cleanupTimeoutMs: 1000, opensPerSecond: 10, telemetryIntervalMs: 100, maxTelemetryAgeMs: 1000, maxCostAgeMs: 1000 } };
}
