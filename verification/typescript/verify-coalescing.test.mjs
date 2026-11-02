import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { extract } from './verify-coalescing.mjs';
const source = readFileSync(new URL('../../packages/client/src/coalescing-policy.ts', import.meta.url), 'utf8');
test('extracts actual coalescing admission', () => assert.match(extract(source), /^\(define-fun admits/));
for (const [name, before, after] of [
  ['coercion', 'knownUnsent === true', 'knownUnsent == true'],
  ['truthiness', 'knownUnsent === true', 'knownUnsent'],
  ['unknown call', 'knownUnsent === true', 'Boolean(knownUnsent)'],
  ['other unknown comparison', 'knownUnsent === true', 'knownUnsent === false'],
  ['type assertion', 'knownUnsent === true', '(knownUnsent as boolean) === true'],
  ['default parameter', 'knownUnsent: unknown', 'knownUnsent: unknown = true'],
  ['wrong input type', 'knownUnsent: unknown', 'knownUnsent: boolean'],
  ['wrong result type', '): boolean', '): unknown'],
  ['side effect', 'return state', 'console.log(state); return state'],
  ['extra declaration', 'export function', 'let hidden = true; export function'],
  ['unknown identifier', 'state === "pending"', 'other === "pending"'],
  ['computed literal', '"pending"', '"pen" + "ding"'],
]) test(`rejects ${name}`, () => {
  const changed = source.replace(before, after); assert.notEqual(changed, source); assert.throws(() => extract(changed));
});
