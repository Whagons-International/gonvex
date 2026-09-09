import {expect,it} from 'vitest';
import {mergeReplicaRecord} from './replica-record.js';
const at = (revision:number) => ({epoch:'e',revision});

it('retains newer fields while filling an older missing projection', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',status:'working'},at(5));
  const merged=mergeReplicaRecord(current,{id:'a',status:'new',description:'Loaded later'},at(3));
  expect(merged.row).toEqual({id:'a',status:'working',description:'Loaded later'});
  expect(merged.authority.fields).toEqual({id:5,status:5,description:3});
  expect(mergeReplicaRecord(merged,{description:null},at(6)).row).toEqual({id:'a',status:'working',description:null});
});

it('does not resurrect deleted rows from a delayed tab snapshot', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',status:'working'},at(5));
  const deleted=mergeReplicaRecord(current,null,at(6));
  expect(mergeReplicaRecord(deleted,{id:'a',status:'new'},at(5))).toBe(deleted);
  expect(mergeReplicaRecord(deleted,{id:'a',status:'new'},at(6))).toBe(deleted);
  expect(mergeReplicaRecord(deleted,{id:'a',status:'created again'},at(7)).row).toEqual({id:'a',status:'created again'});
});

it('does not let a delayed deletion erase a newer row', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',name:'Recreated'},at(8));
  expect(mergeReplicaRecord(current,null,at(7))).toBe(current);
});

it('does not carry fields across a server epoch replacement', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',name:'Old'},at(8));
  expect(mergeReplicaRecord(current,{id:'a',status:'New'},{epoch:'replacement',revision:1}).row).toEqual({id:'a',status:'New'});
});

it('preserves a repeated snapshot and its decoded row without rebuilding indexes', () => {
  const current = mergeReplicaRecord(undefined, {id:'a', config:{color:'gray'}, status:'working'}, at(5));
  expect(mergeReplicaRecord(current, {id:'a', config:{color:'gray'}}, at(5))).toBe(current);
  const newer = mergeReplicaRecord(current, {status:'working'}, at(6));
  expect(newer.row).toBe(current.row);
  expect(newer.authority.fields.status).toBe(6);
  expect(current.authority.fields.status).toBe(5);
  expect(mergeReplicaRecord(newer, {status:'done'}, at(6)).row?.status).toBe('done');
  const deleted = mergeReplicaRecord(newer, null, at(7));
  expect(mergeReplicaRecord(deleted, null, at(7))).toBe(deleted);
});
