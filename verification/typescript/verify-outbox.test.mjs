import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {extract} from './verify-outbox.mjs';
const source = readFileSync(new URL('../../packages/client/src/outbox-policy.ts', import.meta.url), 'utf8');
test('extracts the actual production transition', () => assert.match(extract(source), /^\(define-fun next/));
for (const [name, before, after] of [
  ['extra state', '"failed" | "rejected"', '"failed" | "rejected" | "unknown"'],
  ['extra statement', 'return action', 'console.log(state); return action'],
  ['call', 'return action', 'return String(action)'],
  ['coercion', 'action ===', 'action =='],
  ['wrong comparison sorts', 'action === "start"', 'action === "pending"'],
  ['wrong result sort', '? "inflight" : state', '? "start" : state'],
  ['unknown identifier', '? "inflight" : state', '? "inflight" : hidden'],
  ['type assertion', '? "inflight" : state', '? "inflight" : state as ReducerOutboxState'],
  ['default parameter', 'action: DeliveryAction', 'action: DeliveryAction = "start"'],
  ['extra declaration', 'export function', 'const hidden = 1; export function'],
]) test(`rejects ${name}`, () => { const changed = source.replace(before, after); assert.notEqual(source, changed); assert.throws(() => extract(changed)); });
