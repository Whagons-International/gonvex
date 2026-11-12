// 0065 provisional admission for active/UNKNOWN tenant scope authority.
// No TTL, eviction or caller-side timeout is quiescence. Until a proven
// terminal-release protocol lands, charges remain for this tenant instance.
export const TENANT_AUTHORITY_COUNT = 4096;
const TENANT_BYTES = 16 * 1024 * 1024;
const ISOLATE_COUNT = 16_384;
const ISOLATE_BYTES = 32 * 1024 * 1024;
const owners = new Set<WeakRef<AuthorityAdmission>>();
let count = 0, bytes = 0;
export const authorityPressure = () => ({count,bytes});
export class DeliveryAdmissionError extends Error {
 readonly code = 'GONVEX_DELIVERY_ADMISSION';
 constructor(){super('tenant subscription authority admission exhausted; retry');}
}
export class AuthorityAdmission {
 private readonly entries = new Map<string,number>();
 private readonly owner = new WeakRef(this);
 private retained = 0;
 private sweep():void {
  count=0;bytes=0;
  for(const ref of owners){const live=ref.deref();if(!live)owners.delete(ref);else{count+=live.entries.size;bytes+=live.retained;}}
 }
 set(id:string,estimate:number):void {
  const previous=this.entries.get(id)??0;
  // Do not release estimated in-flight/deferred copies just because their
  // connection was dropped. UNKNOWN entries retain their peak charge.
  if(previous>=estimate)return;
  const additional=estimate-previous, fresh=previous===0;
  if(count+Number(fresh)>ISOLATE_COUNT||bytes+additional>ISOLATE_BYTES)this.sweep();
  if((fresh&&this.entries.size>=TENANT_AUTHORITY_COUNT)||this.retained+additional>TENANT_BYTES
   ||count+Number(fresh)>ISOLATE_COUNT||bytes+additional>ISOLATE_BYTES)throw new DeliveryAdmissionError();
  this.entries.set(id,estimate);this.retained+=additional;
  count+=Number(fresh);bytes+=additional;owners.add(this.owner);
 }
}
