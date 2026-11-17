import { test } from 'node:test';
import assert from 'node:assert/strict';
import { numbersByValue } from '../src/number-text.mjs';
test('0028 REAL row-image spelling rule preserves strings, order and large integer precision', () => {
  const rust = '{"b":"1.0e+300","i":9007199254740993,"n":5,"r":1e+300,"s":1e-7,"z":0}';
  const kernel = '{"b":"1.0e+300","i":9007199254740993,"n":5.0,"r":1.0e+300,"s":1.0e-07,"z":0.0}';
  assert.equal(numbersByValue(rust), numbersByValue(kernel));
  assert.notEqual(numbersByValue(rust), numbersByValue(rust.replace('9007199254740993', '9007199254740992')));
  assert.notEqual(numbersByValue(rust), numbersByValue(rust.replace('"1.0e+300"', '"1e+300"')));
  assert.notEqual(numbersByValue('{"a":1,"b":2}'), numbersByValue('{"b":2,"a":1}'));
  assert.equal(numbersByValue(null), null);
});
