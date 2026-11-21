import { refuse, hash } from './binding.mjs';
export function versionUrl(b) { const u = new URL(b.deployment.endpoint); u.protocol = 'https:'; u.pathname = '/__assembly/version'; return u.href; }
/** Actual 471 CatalogPort/fixtureControl response, not a proposed capacity endpoint schema. */
export function verifyIdentity(body, b) {
  if (!body || body.ready !== true || body.version !== b.deployment.catalogVersion
    || body.worker?.id !== b.deployment.resources.find(r => r.role === 'kernel').version
    || body.runtimeWorker?.id !== b.deployment.resources.find(r => r.role === 'runtime').version
    || !body.releases || Object.keys(body.releases).length!==b.deployment.catalogReleases.length || b.deployment.catalogReleases.some(r => body.releases[r.projectId] !== r.moduleSha256)) refuse('RUNTIME_CATALOG.identity');
  // This endpoint proves stable/runtime catalog facts only. All five physical versions still require telemetry/provider evidence.
  const releases=Object.entries(body.releases).sort((a,b)=>a[0]<b[0]?-1:a[0]>b[0]?1:0);
  return { stableVersion: body.worker.id, runtimeVersion: body.runtimeWorker.id, catalogVersion: body.version,
    projects:releases.length,releasesSha256:hash(JSON.stringify(releases)),moduleSha256:body.releases[b.tenants[0].projectId] };
}
export async function readJSON(response, maximum = 512 * 1024) {
  if (!response.ok || !response.body) refuse('measurement.response');
  const reader = response.body.getReader(); let length = 0; const parts = [];
  try { for (;;) { const { done, value } = await reader.read(); if (done) break; length += value.byteLength;
    if (length > maximum) refuse('measurement.byte-bound'); parts.push(value); }
  } finally { await reader.cancel(); }
  return JSON.parse(Buffer.concat(parts).toString('utf8'));
}
