import {validInitialRef} from '../delivery/protocol';
import type {SessionRecord} from './session';
/** Counter/book consistency belongs to the same recovery publication. */
export function assertInitialState(record:SessionRecord):void {
 const sequence=record.initialSequence??0,epoch=record.subscriptionEpoch??0;
 if(!Number.isSafeInteger(sequence)||sequence<0)throw new Error('invalid initial request sequence');
 for(const [key,ref] of Object.entries(record.initialRequests??{})){
  if(!validInitialRef(ref)||key!==String(ref.generation)||ref.generation>sequence||ref.epoch!==epoch)throw new Error('invalid initial recovery authority');
  const request=record.pendingLiveQueries?.[ref.id]?.find(r=>r.initialGeneration===ref.generation);
  if(ref.kind==='query'&&(!request||request.path!==ref.path))throw new Error('initial semantic request is missing');
 }
 for(const [id,pending] of Object.entries(record.pendingLiveQueries??{}))for(const request of pending){
  const ref=record.initialRequests?.[String(request.initialGeneration)];
  if(!ref||ref.kind!=='query'||ref.id!==id||ref.path!==request.path)throw new Error('initial semantic request authority unsupported');
 }
 for(const [id,generation] of Object.entries(record.actionGenerations??{}))if(!Number.isSafeInteger(generation)||generation<=0||generation>sequence||!Object.hasOwn(record.pendingActions??{},id))throw new Error('invalid Action recovery authority');
}
