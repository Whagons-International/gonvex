import { describe, expect, it } from 'vitest';
import { schema } from '@gonvex/module-sdk';
import { validateValue } from './validation.js';
describe('local portable schema validation', () => {
  it('matches optional fields and rejects unknown fields and explicit invalid nulls', () => {
    const definition = schema.object({ title: schema.string(), count: schema.optional(schema.number({ integer: true })) });
    expect(() => validateValue(definition, { title: 'task' })).not.toThrow();
    expect(() => validateValue(definition, { title: 'task', count: null })).toThrow(/number/);
    expect(() => validateValue(definition, { title: 'task', count: 1.5 })).toThrow(/integer/);
    expect(() => validateValue(definition, { title: 'task', hidden: true })).toThrow(/unknown/);
  });
  it('uses Unicode character lengths and nested array validation', () => {
    expect(() => validateValue(schema.string({ minLength: 1, maxLength: 1 }), '😀')).not.toThrow();
    expect(() => validateValue(schema.array(schema.number({ maximum: 2 })), [1, 3])).toThrow(/at most 2/);
  });
});
