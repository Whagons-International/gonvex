import { test } from 'node:test';
import assert from 'node:assert/strict';
import { addressHeaders, registerAddressHeaders } from '../src/client-address.mjs';

test('CF-Connecting-IP by default (the Rust reference trusts it from 127.0.0.1)', () => {
  assert.deepEqual(addressHeaders('ws://127.0.0.1:1/ws', '203.0.113.5'), { 'cf-connecting-ip': '203.0.113.5' });
});

test('a registered backend names the address its own way, keyed by its wsUrl only', () => {
  registerAddressHeaders('wss://dev.example/ws', (a) => ({ 'x-gonvex-fixture-key': 'k', 'x-gonvex-fixture-client-ip': a }));
  assert.deepEqual(addressHeaders('wss://dev.example/ws', '203.0.113.6'), { 'x-gonvex-fixture-key': 'k', 'x-gonvex-fixture-client-ip': '203.0.113.6' });
  assert.deepEqual(addressHeaders('ws://127.0.0.1:2/ws', '203.0.113.6'), { 'cf-connecting-ip': '203.0.113.6' });
});
