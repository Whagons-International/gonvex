import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {extract} from './verify-outbox.mjs';
import {extractSettlement} from './receipt-settlement.mjs';
const source = readFileSync(new URL('../../packages/client/src/outbox-policy.ts', import.meta.url), 'utf8');
const records = readFileSync(new URL('../../packages/client/src/outbox.ts', import.meta.url), 'utf8');
test('extracts both production receipt record transitions', () => {
  const result = extractSettlement(records);
  assert.deepEqual(Object.keys(result.functions), ['markAcceptedChange', 'markCommittedChange']);
  assert.match(result.definitions, /markAcceptedChange_waiting/);
  assert.match(result.definitions, /markCommittedChange_state/);
});
for (const [name, before, after] of [
  ['extra effect', 'if (entry.state === "committed")', 'hidden(); if (entry.state === "committed")'],
  ['unmodeled identity read', 'if (entry.state === "committed")', 'if (entry.scope === "committed")'],
  ['identity overwrite', 'knownUnsent: false, state: deliveryState', 'scope: "other", knownUnsent: false, state: deliveryState'],
  ['coercion', 'if (entry.state === "committed")', 'if (entry.state == "committed")'],
  ['unknown spread', 'return { ...entry, knownUnsent: false, state: deliveryState', 'return { ...hidden, knownUnsent: false, state: deliveryState'],
  ['asserted field type', 'awaitingReplica: true };', 'awaitingReplica: hidden as true };'],
  ['unsupported call', 'if (entry.state === "committed")', 'if (hidden())'],
]) test(`rejects unsupported receipt transition ${name}`, () => {
  const method = extractSettlement(records).functions.markAcceptedChange;
  assert.equal(method.split(before).length, 2);
  assert.throws(() => extractSettlement(records.replace(method, method.replace(before, after))));
});
test('extracts the actual production transition', () => assert.match(extract(source), /^\(define-fun next/));
for (const [name, before, after] of [
  ['extra state', '"failed" | "rejected"', '"failed" | "rejected" | "unknown"'],
  ['extra action', '"fail-park";', '"fail-park" | "unknown";'],
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
