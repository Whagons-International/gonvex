import {expect, it} from 'vitest';
import {mergeReplicaRecord} from './replica-record.js';
const at = (revision: number) => ({epoch: 'e', revision});

it('does not restore pre-deletion fields after a narrower row recreation', () => {
  const first = mergeReplicaRecord(undefined, {id: 'a', obsolete: 'old lifetime'}, at(1));
  const deleted = mergeReplicaRecord(first, null, at(2));
  const recreated = mergeReplicaRecord(deleted, {id: 'a', status: 'new lifetime'}, at(3));
  expect(mergeReplicaRecord(recreated, {id: 'a', obsolete: 'old lifetime'}, at(1)).row)
    .toEqual({id: 'a', status: 'new lifetime'});
  expect(mergeReplicaRecord(recreated, {id: 'a', obsolete: 'deletion tie'}, at(2)).row)
    .toEqual({id: 'a', status: 'new lifetime'});
  // A later projection within the new lifetime can still fill missing fields.
  expect(mergeReplicaRecord(recreated, {description: 'current'}, at(3)).row)
    .toEqual({id: 'a', status: 'new lifetime', description: 'current'});
});

it('keeps valid deletion history but clears legacy NaN history on recovery', () => {
  const deleted = mergeReplicaRecord(undefined, null, at(2));
  const recreated = mergeReplicaRecord(deleted, {id: 'a'}, at(3));
  expect(recreated.authority.deleted).toBe(2);
  const recovered = mergeReplicaRecord({row: null, authority: {epoch: 'e', fields: {}, deleted: NaN}}, {id: 'a'}, at(3));
  expect(recovered.authority.deleted).toBeUndefined();
  expect(mergeReplicaRecord(recovered, null, at(2))).toBe(recovered);
  expect(mergeReplicaRecord(recreated, {id: 'a', other: 'new epoch'}, {epoch: 'new', revision: 1}).row)
    .toEqual({id: 'a', other: 'new epoch'});
});

it('preserves a JSON __proto__ column as data and records its authority', () => {
  const incoming = JSON.parse('{"id":"a","__proto__":{"label":"data"}}');
  const record = mergeReplicaRecord(undefined, incoming, at(1));
  expect(Object.prototype.hasOwnProperty.call(record.row, '__proto__')).toBe(true);
  expect(Object.getPrototypeOf(record.row)).toBe(Object.prototype);
  expect(JSON.parse(JSON.stringify(record.row))).toEqual(incoming);
  expect(Object.prototype.hasOwnProperty.call(record.authority.fields, '__proto__')).toBe(true);
  expect(record.authority.fields['__proto__']).toBe(1);
  const next = mergeReplicaRecord(record, JSON.parse('{"__proto__":{"label":"new"}}'), at(3));
  const stale = mergeReplicaRecord(next, incoming, at(2));
  expect(stale.row?.['__proto__']).toEqual({label: 'new'});
  expect(record.row?.['__proto__']).toEqual({label: 'data'});
  expect(mergeReplicaRecord(next, null, at(2))).toBe(next);
});
