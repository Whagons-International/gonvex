// 0070: bounded recovery offers and processing-only subset validation. Nothing
// here installs a subscription or certifies data, a revision, or a scope.
import { initialRefBytes, sameInitialRef, validInitialRef, type InitialCompletionStatus, type InitialRequestRef } from './protocol';

/** Selection envelope within the existing reset/control credits; never a request/argument limit. */
export const RECOVERY_INITIAL_METADATA_BYTES = 64 * 1024;
const MAX_EXAMINED = 256;
export const recoveryInitialRefBytes = (ref: InitialRequestRef) => 512 + initialRefBytes(ref);
export interface RecoveryInitialOwner<Owner> {
 readonly id: string;
 readonly owner: Owner;
 readonly epoch: number;
 readonly refs: ReadonlyMap<number, InitialRequestRef>;
}
export interface RecoveryInitialCapture<Owner> {
 readonly requests: Readonly<Record<string, readonly InitialRequestRef[]>>;
 readonly owners: ReadonlyMap<string, {readonly owner: Owner; readonly epoch: number; readonly refs: ReadonlyMap<number, InitialRequestRef>}>;
 readonly omitted: number;
 readonly offered: number;
 readonly bytes: number;
}

export function captureRecoveryInitials<Owner>(entries: Iterable<RecoveryInitialOwner<Owner>>): RecoveryInitialCapture<Owner> {
 const owners = new Map<string, {owner:Owner;epoch:number;refs:ReadonlyMap<number,InitialRequestRef>}>();
 const requests: Array<[string,readonly InitialRequestRef[]]> = [];
 let bytes=256,total=0,offered=0,examined=0;
 for(const entry of entries){
  total+=entry.refs.size;
  const refs:InitialRequestRef[]=[],book=new Map<number,InitialRequestRef>(),header=128+entry.id.length*8;
  if(bytes+header<=RECOVERY_INITIAL_METADATA_BYTES&&examined<MAX_EXAMINED){
   for(const ref of entry.refs.values()){
    if(examined++>=MAX_EXAMINED)break;
    if(!validInitialRef(ref)||ref.epoch!==entry.epoch)continue;
    const charge=recoveryInitialRefBytes(ref)+(refs.length?0:header);
    if(bytes+charge>RECOVERY_INITIAL_METADATA_BYTES)continue;
    const copy=Object.freeze({...ref});refs.push(copy);book.set(copy.generation,copy);bytes+=charge;offered++;
   }
  }
  if(refs.length){requests.push([entry.id,Object.freeze(refs)]);owners.set(entry.id,{owner:entry.owner,epoch:entry.epoch,refs:book});}
 }
 return {requests:Object.freeze(Object.fromEntries(requests)),owners,omitted:total-offered,offered,bytes};
}

/** Validate ALL facts before returning ANY cleanup. Omitted facts are UNKNOWN.
 * A caller must apply this plan synchronously, reusing these exact owner books. */
export function recoveryInitialCleanup<Owner>(capture:RecoveryInitialCapture<Owner>,facts:unknown,
 current:(id:string)=>RecoveryInitialOwner<Owner>|undefined):readonly {id:string;request:InitialRequestRef}[]|undefined {
 if(facts===undefined)return [];
 if(!facts||typeof facts!=='object'||Array.isArray(facts))return undefined;
 const prototype=Object.getPrototypeOf(facts);
 if(prototype!==null&&prototype!==Object.prototype)return undefined;
 const plan:Array<{id:string;request:InitialRequestRef}>=[];
 for(const [id,value] of Object.entries(facts)){
  const offered=capture.owners.get(id),now=current(id);
  if(!offered||!now||now.owner!==offered.owner||now.epoch!==offered.epoch||!Array.isArray(value))return undefined;
  const seen=new Set<number>();
  for(const status of value as InitialCompletionStatus[]){
   if(!status||!Number.isSafeInteger(status.generation)||status.generation<=0||seen.has(status.generation)
    ||(status.status!=='enqueued'&&status.status!=='obsolete'))return undefined;
   seen.add(status.generation);
   const original=offered.refs.get(status.generation),pending=now.refs.get(status.generation);
   if(!original||!pending||original.epoch!==now.epoch||!sameInitialRef(original,pending))return undefined;
   plan.push({id,request:original});
  }
 }
 return plan;
}
