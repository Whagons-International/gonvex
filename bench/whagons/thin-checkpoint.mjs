/** Harness-owned, process-local checkpoint storage. Immutable pooled rows are
 * shared as values only: every principal owns its scope and window memberships.
 * Publication replaces one complete map synchronously, never an async torn set.
 * No claim of disk durability is made. */
import { createHash } from 'node:crypto';
export const utf8Compare=(a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b));
export function canonical(value) {
 if(typeof value==='string')return JSON.stringify(value).replace(/\u2028/g,'\\u2028').replace(/\u2029/g,'\\u2029');
 if(value===null||typeof value!=='object')return JSON.stringify(value);
 if(Array.isArray(value))return `[${value.map(canonical).join(',')}]`;
 return `{${Object.keys(value).sort(utf8Compare).map(k=>`${canonical(k)}:${canonical(value[k])}`).join(',')}}`;
}
export const sha=value=>createHash('sha256').update(value).digest('hex');
const rowHashes=new WeakMap();
export function integrity(rows,columns,mode='xor1') {
 const hashes=Object.create(null), bytes=Buffer.alloc(32),columnKey=canonical(columns||[]);
 for(const [id,entry] of rows){
  let cached=rowHashes.get(entry);if(!cached)rowHashes.set(entry,cached=new Map());
  // Values exposed to callers are checked against their canonical pool record.
  if(JSON.stringify(entry.value)!==entry.serializedValue)throw Error('mutated-checkpoint-row');
  let h=cached.get(columnKey);
  if(!h){let row=entry.value;if(columns?.length)row=Object.fromEntries(columns.filter(c=>Object.hasOwn(row,c)).map(c=>[c,row[c]]));h=sha(canonical(row));cached.set(columnKey,h);}
  hashes[id]=h;const b=Buffer.from(h,'hex');for(let i=0;i<32;i++)bytes[i]^=b[i];
 }
 return {hashes,digest:mode==='xor1'?'xor1:'+bytes.toString('hex'):sha(canonical(Object.keys(hashes).sort(utf8Compare).map(k=>[k,hashes[k]])))};
}
export function windowKey(ref,args={}) {
 return canonical(JSON.parse(JSON.stringify([ref.path,args,{replica:ref.replica||{},scope:ref.scope||'tenant',delivery:ref.delivery||'replica'}])));
}
export function validCursor(c){return !!c&&typeof c.epoch==='string'&&c.epoch.length>0&&Number.isSafeInteger(c.revision)&&c.revision>=0;}
export class ThinCheckpointStore {
 constructor(){this.binding=undefined;this.windows=new Map();this.version=0;this.bytes=0;this.peakBytes=0;this.commits=0;this.loads=0;this.loadedWindows=0;}
 bind(binding){const key=canonical(binding);if(key!==this.binding){this.clear();this.binding=key;}return key;}
 clear(){for(const w of this.windows.values())for(const e of w.rows.values())w.pool.release(e);this.windows=new Map();this.bytes=0;this.version++;}
 invalidate(key){const w=this.windows.get(key);if(!w)return;const next=new Map(this.windows);next.delete(key);this.windows=next;for(const e of w.rows.values())w.pool.release(e);this.bytes-=w.bytes;this.version++;}
 publish(windows){
  const next=new Map(this.windows);let bytes=this.bytes;const retired=[];
  for(const w of windows){
   const old=next.get(w.key);if(old){retired.push(old);bytes-=old.bytes;}
   const rows=new Map();for(const [id,e] of w.rows)rows.set(id,w.pool.acquire(e.canonical,e.value));
   const record={...w,rows,cursor:{...w.cursor},hashes:{...w.hashes}};
   record.bytes=Buffer.byteLength(w.key)+rows.size*80+[...rows.values()].reduce((n,e)=>n+Buffer.byteLength(e.canonical),0);
   next.set(w.key,record);bytes+=record.bytes;
  }
  this.windows=next;this.bytes=bytes;this.peakBytes=Math.max(this.peakBytes,bytes);this.version++;this.commits++;
  for(const w of retired)for(const e of w.rows.values())w.pool.release(e);
 }
 load(key,binding,pool){
  this.loads++;const w=this.windows.get(key);if(binding!==this.binding||!w||!w.complete||!validCursor(w.cursor))return;
  if([...w.rows].some(([id,e])=>String(e.value[w.keyField])!==id)){this.invalidate(key);throw Error('checkpoint-membership-key');}
  const actual=integrity(w.rows,w.columns,w.mode);if(actual.digest!==w.digest){this.invalidate(key);throw Error('checkpoint-integrity-mismatch');}
  const rows=new Map();for(const [id,e] of w.rows)rows.set(id,pool.acquire(e.canonical,e.value));
  this.loadedWindows++;return {...w,rows,cursor:{...w.cursor}};
 }
 stats(){return {bytes:this.bytes,peakBytes:this.peakBytes,windows:this.windows.size,commits:this.commits,loads:this.loads,loadedWindows:this.loadedWindows,version:this.version};}
}
