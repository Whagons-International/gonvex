import { it, expect } from 'vitest';
import { binding } from './test-data.mjs';
import { validateBinding } from './binding.mjs';
import { verifyIdentity, readJSON, versionUrl } from './identity.mjs';
it('binds actual stable/runtime catalog facts and rejects assembled booleans or stale Runtime identity', () => {
  const b = binding(), body = { ready: true, worker: { id: 'DATA-only' }, runtimeWorker: { id: 'DATA-only' }, version: 'DATA', releases: { DATA: b.deployment.moduleSha256 } };
  expect(verifyIdentity(body, b).runtimeVersion).toBe('DATA-only');
  for (const bad of [{ assembled: true }, { ...body, runtimeWorker: { id: 'old' } }, { ...body, version: 'old' }, { ...body, releases: {} }]) expect(() => verifyIdentity(bad, b)).toThrow();
  expect(versionUrl(b)).toBe('https://gonvex-cf-dev-data.whagons-international.workers.dev/__assembly/version');
});
it('four-component monolith bindings and mismatched project releases refuse before execution', () => {
  const b = binding(); b.deployment.resources = b.deployment.resources.filter(r => r.role !== 'runtime'); expect(() => validateBinding(b)).toThrow();
  const c = binding(); c.tenants[0].projectId = 'unbound'; expect(() => validateBinding(c)).toThrow();
});
it('bounds a real Response body locally without issuing a request', async () => {
  await expect(readJSON(new Response('x'.repeat(33)), 32)).rejects.toThrow();
  expect(await readJSON(Response.json({ DATA: true }))).toEqual({ DATA: true });
});
