import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {extract} from './verify-revisions.mjs';

const source = readFileSync(new URL('../../packages/client/src/revision-policy.ts', import.meta.url), 'utf8');
test('extracts all five actual TypeScript policies', () => {
  assert.equal((extract(source).match(/define-fun/g) ?? []).length, 5);
});

for (const [name, before, after] of [
  ['ignored side effect', 'return !(incoming < current);', 'console.log(incoming); return !(incoming < current);'],
  ['unmodeled call', 'return !(incoming < current);', 'return Boolean(incoming);'],
  ['arithmetic', 'incoming < current', 'incoming + 1 < current'],
  ['coercion', 'incoming < current', '"1" < current'],
  ['default parameter', 'incoming: number, current:', 'incoming: number = 1, current:'],
  ['shadowed builtin', 'revision: number', 'Number: number'],
  ['unknown constant', 'revision >= 0', 'revision >= 1'],
  ['wrong operand sorts', 'incoming < current', '(incoming > current) < current'],
  ['numeric negation', '!(incoming < current)', '-(incoming < current)'],
  ['different return type', '): boolean', '): number'],
  ['extra declaration', 'export function', 'const hidden = 1; export function'],
]) {
  test(`fails closed on ${name}`, () => {
    assert.notEqual(source.replace(before, after), source);
    assert.throws(() => extract(source.replace(before, after)));
  });
}
