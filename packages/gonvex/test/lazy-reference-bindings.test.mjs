import assert from 'node:assert/strict';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import { lazyReferenceRuntime, renderLazyReferences } from '../dist/lazy-reference-bindings.js';

test('generated references allocate on access, preserve identity, and serialize the same contract', () => {
  const metadata = { tasks: { update: { kind: 'reducer', path: 'tasks.update', args: { id: 'string' } } }, 'other-module': { list: { kind: 'query', path: 'other-module.list' } } };
  const expression = renderLazyReferences(metadata, 0, value => Boolean(value?.kind), value => `construct(${JSON.stringify(value)})`);
  const source = ts.transpile(`${lazyReferenceRuntime}\n(globalThis as any).api = ${expression};`, { target: ts.ScriptTarget.ES2022 });
  const constructed = [];
  const context = { construct: value => { constructed.push(value.path); return value; } };
  vm.runInNewContext(source, context);
  assert.deepEqual(constructed, []);
  assert.deepEqual(Object.keys(context.api).sort(), Object.keys(metadata).sort());
  const tasks = context.api.tasks;
  assert.deepEqual(constructed, []);
  Object.freeze(tasks);
  const update = tasks.update;
  assert.equal(context.api.tasks.update, update);
  assert.deepEqual(constructed, ['tasks.update']);
  assert.deepEqual(JSON.parse(JSON.stringify(context.api)), metadata);
  assert.deepEqual(constructed, ['tasks.update', 'other-module.list']);
});


test('object-literal reference factories remain valid TypeScript expressions', () => {
  const metadata = { tasks: { update: { kind: 'reducer', path: 'tasks.update' } } };
  const expression = renderLazyReferences(metadata, 0, value => Boolean(value?.kind), value => `${JSON.stringify(value)} as const`);
  const source = ts.transpile(`${lazyReferenceRuntime}\n(globalThis as any).api = ${expression};`, { target: ts.ScriptTarget.ES2022 });
  const context = {};
  vm.runInNewContext(source, context);
  assert.equal(context.api.tasks.update.path, 'tasks.update');
});

test('unused internal references are removed from a browser bundle', async () => {
  const { build } = await import('rolldown');
  const metadata = { tasks: { secret: { kind: 'reducer', path: 'INTERNAL_METADATA_MUST_NOT_SHIP' } } };
  const expression = renderLazyReferences(metadata, 0, value => Boolean(value?.kind), value => JSON.stringify(value));
  const source = ts.transpile(`${lazyReferenceRuntime}\nconst internal = ${expression}; export const publicValue = 'public';`, { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext });
  const result = await build({ input: 'test-entry', write: false, output: { format: 'es' }, plugins: [{ name: 'generated-reference-test', resolveId: id => id === 'test-entry' ? '\0test-entry' : null, load: id => id === '\0test-entry' ? source : null }] });
  assert.doesNotMatch(result.output.find(item => item.type === 'chunk').code, /INTERNAL_METADATA_MUST_NOT_SHIP/);
});
