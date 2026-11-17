import { shape, text, list, unique, refuse, jsonFile, hash } from './binding.mjs';
export function at(value, path) { return path.reduce((v, k) => v?.[k], value); }
function path(v) { list(v, 16, 'selector'); for (const k of v) if (typeof k !== 'string' || !k.length || ['__proto__', 'constructor', 'prototype'].includes(k)) refuse('selector'); }
export function reference(api, selector, kind) {
  path(selector); const ref = at(api, selector);
  if (!ref || ref.kind !== kind || typeof ref.path !== 'string') refuse('generated-reference');
  return ref; // Never synthesize, clone or strip localExecution/optimistic metadata.
}
export function matches(rows, keyPath, e) {
  if (!Array.isArray(rows)) return false;
  const found = rows.filter(row => at(row, keyPath) === e.key);
  return found.length === 1 && JSON.stringify(at(found[0], e.valuePath)) === JSON.stringify(e.value);
}
export function projectionDigest(rows, keyPath, checks) {
  return hash(JSON.stringify(checks.map(e => ({ key: e.key, values: rows.filter(r => at(r, keyPath) === e.key).map(r => at(r, e.valuePath)) }))));
}
export async function loadFixture(file) {
  const { value: f } = await jsonFile(file.path);
  shape(f, ['ownedIds', 'replica', 'liveQueries', 'operations', 'cleanup', 'finalChecks'], 'fixture');
  list(f.ownedIds, 100000, 'ownedIds'); unique(f.ownedIds, 'ownedIds'); f.ownedIds.forEach(id => text(id, 'ownedId'));
  shape(f.replica, ['ref', 'args', 'keyPath'], 'replica'); path(f.replica.ref); path(f.replica.keyPath);
  list(f.liveQueries, 8, 'liveQueries', 0); for (const q of f.liveQueries) { shape(q, ['ref', 'args'], 'liveQuery'); path(q.ref); }
  list(f.operations, 16, 'operations'); unique(f.operations.map(o => o.alias), 'operations');
  const operation = o => {
    shape(o, ['alias', 'kind', 'ref', 'identityPaths', 'cases'], 'operation'); text(o.alias, 'operation.alias'); path(o.ref);
    if (!['reducer', 'action', 'query'].includes(o.kind)) refuse('operation.kind');
    list(o.identityPaths, 8, 'identityPaths', o.kind === 'query' ? 0 : 1); o.identityPaths.forEach(path);
    list(o.cases, 100000, 'cases');
    for (const c of o.cases) {
      shape(c, ['args', 'expect'], 'case');
      if (!c.args || typeof c.args !== 'object' || Array.isArray(c.args)) refuse('case.args');
      if (o.identityPaths.some(p => !f.ownedIds.includes(at(c.args, p)))) refuse('write-isolation');
      if (c.expect !== null) { shape(c.expect, ['key', 'valuePath', 'value'], 'expect'); path(c.expect.valuePath);
        if (!f.ownedIds.includes(c.expect.key)) refuse('expect.owned-id'); }
      if (o.kind !== 'query' && !c.expect) refuse('write.receiver-expectation');
    }
  };
  f.operations.forEach(operation); list(f.cleanup, 16, 'cleanup', 0); f.cleanup.forEach(operation);
  if (f.operations.some(o => o.kind !== 'query') && !f.cleanup.length) refuse('fixture.cleanup-required');
  const expectations = f.operations.filter(o => o.kind !== 'query').flatMap(o => o.cases.map(c => JSON.stringify(c.expect)));
  unique(expectations, 'write-expectations.must-be-distinct');
  list(f.finalChecks, 100000, 'finalChecks');
  for (const e of f.finalChecks) { shape(e, ['key', 'valuePath', 'value'], 'finalCheck'); path(e.valuePath); if (!f.ownedIds.includes(e.key)) refuse('finalCheck.ownership'); }
  return f;
}
