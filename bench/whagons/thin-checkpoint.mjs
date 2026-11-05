/** Harness-owned, process-local checkpoint storage. Immutable pooled rows are
 * shared as values only: every principal owns its scope and window memberships.
 * Publication replaces one complete map synchronously, never an async torn set.
 * No claim of disk durability is made. */
import { createHash } from 'node:crypto';
export const utf8Compare=(a,b)=>(/[^\x00-\x7f]/.test(a)||/[^\x00-\x7f]/.test(b))?Buffer.compare(Buffer.from(a),Buffer.from(b)):(a<b?-1:a>b?1:0);
export function canonical(value) {
 if(typeof value==='string')return JSON.stringify(value).replace(/\u2028/g,'\\u2028').replace(/\u2029/g,'\\u2029');
 if(value===null||typeof value!=='object')return JSON.stringify(value);
 if(Array.isArray(value))return `[${value.map(canonical).join(',')}]`;
 return `{${Object.keys(value).sort(utf8Compare).map(k=>`${canonical(k)}:${canonical(value[k])}`).join(',')}}`;
}
export const sha=value=>createHash('sha256').update(value).digest('hex');
const rowHashes=new WeakMap(),trusted=new WeakSet();
export const integrityWork={rowVisits:0,hashComputes:0,fastDigestReads:0,checkpointCopiedRows:0,checkpointDeltaRows:0,cursorAdvances:0};
// RowPool alone creates these entries after deep freezing the value. Keep the
// refcount mutable while making the actual value and its serialization fixed.
export function trustImmutableEntry(entry){
 if(!Object.isFrozen(entry.value))throw Error('mutable-row-entry');
 if(entry.canonical!==entry.serializedValue&&JSON.stringify(JSON.parse(entry.canonical))!==entry.serializedValue)throw Error('row-canonical-mismatch');
 for(const key of ['value','canonical','serializedValue','canonicalBytes','pool','identity'])Object.defineProperty(entry,key,{writable:false,configurable:false});
 trusted.add(entry);return entry;
}
function hashEntry(entry,columns,columnKey){
 if(!trusted.has(entry)&&JSON.stringify(entry.value)!==entry.serializedValue)throw Error('mutated-checkpoint-row');
 let cached=rowHashes.get(entry);if(!cached)rowHashes.set(entry,cached=new Map());
 let h=cached.get(columnKey);
 if(!h){let row=entry.value;if(columns?.length)row=Object.fromEntries(columns.filter(c=>Object.hasOwn(row,c)).map(c=>[c,row[c]]));const bytes=createHash('sha256').update(canonical(row)).digest();h={hash:bytes.toString('hex'),words:new Uint32Array(new Uint8Array(bytes).buffer)};cached.set(columnKey,h);integrityWork.hashComputes++;}
 return h;
}
const xor=(words,hash)=>{for(let i=0;i<8;i++)words[i]^=hash.words[i];};
export class WindowIntegrity {
 constructor(columns){this.columns=columns;this.columnKey=canonical(columns||[]);this.hashes=new Map();this.words=new Uint32Array(8);this.bytes=Buffer.from(this.words.buffer);}
 set(id,entry){const h=hashEntry(entry,this.columns,this.columnKey),old=this.hashes.get(id);if(old===h)return;if(old)xor(this.words,old);xor(this.words,h);this.hashes.set(id,h);}
 delete(id){const old=this.hashes.get(id);if(old){xor(this.words,old);this.hashes.delete(id);}}
 digest(mode='xor1'){integrityWork.fastDigestReads++;return mode==='xor1'?'xor1:'+this.bytes.toString('hex'):sha(canonical([...this.hashes].map(([id,h])=>[id,h.hash]).sort(([a],[b])=>utf8Compare(a,b))));}
 result(mode='xor1',full=true){return {digest:this.digest(mode),...(full?{hashes:Object.fromEntries([...this.hashes].map(([id,h])=>[id,h.hash]))}:{})};}
 static from(rows,columns){const state=new WindowIntegrity(columns);for(const [id,e]of rows){integrityWork.rowVisits++;state.set(id,e);}return state;}
}
// Independent full scan remains available for checkpoint corruption checks and
// differential oracles; normal ready/watermark frames use the maintained state.
export function integrity(rows,columns,mode='xor1'){return WindowIntegrity.from(rows,columns).result(mode);}
export function windowKey(ref,args={}) {
 return canonical(JSON.parse(JSON.stringify([ref.path,args,{replica:ref.replica||{},scope:ref.scope||'tenant',delivery:ref.delivery||'replica'}])));
}
export function validCursor(c){return !!c&&typeof c.epoch==='string'&&c.epoch.length>0&&Number.isSafeInteger(c.revision)&&c.revision>=0;}
export class ThinCheckpointStore {
 constructor(){this.binding=undefined;this.windows=new Map();this.cursors=new Map();this.invalid=new Set();this.version=0;this.bytes=0;this.peakBytes=0;this.commits=0;this.loads=0;this.loadedWindows=0;}
 bind(binding){const key=canonical(binding);if(key!==this.binding){this.clear();this.binding=key;}return key;}
 clear(){for(const w of this.windows.values())for(const e of w.rows.values())w.pool.release(e);this.windows=new Map();this.cursors.clear();this.invalid.clear();this.bytes=0;this.version++;}
 suspend(key){if(this.windows.has(key))this.invalid.add(key);}
 invalidate(key){const w=this.windows.get(key);this.invalid.delete(key);this.cursors.delete(key);if(!w)return;this.windows.delete(key);for(const e of w.rows.values())w.pool.release(e);this.bytes-=w.bytes;this.version++;}
 advance(windows,cursor){
  if(!validCursor(cursor))throw Error('invalid-checkpoint-watermark');
  const shared=Object.freeze({...cursor});
  for(const {key,version}of windows){const w=this.windows.get(key);if(!w||this.invalid.has(key)||w.version!==version||w.authority!==this.binding)continue;const before=this.cursors.get(key)||w.cursor;if(before.epoch!==cursor.epoch||before.revision>cursor.revision)continue;this.cursors.set(key,shared);integrityWork.cursorAdvances++;}
 }
 publish(windows){
  // Suspended records cannot be loaded during a multi-window transaction, even
  // by a reentrant test pool. No await/callback publishes a partial row set.
  for(const w of windows)this.invalid.add(w.key);
  if(windows.every(w=>{const old=this.windows.get(w.key);return old&&!w.reset&&old.version===w.baseVersion&&!w.changes.size&&old.digest===w.digest;})){
   const cursors=new Map();for(const w of windows){const key=w.cursor.epoch+':'+w.cursor.revision;let cursor=cursors.get(key);if(!cursor){cursor=Object.freeze({...w.cursor});cursors.set(key,cursor);}this.cursors.set(w.key,cursor);}
   this.version++;this.commits++;for(const w of windows)this.invalid.delete(w.key);return;
  }
  const next=new Map(this.windows),retired=[];let bytes=this.bytes;
  for(const w of windows){
   const old=next.get(w.key),delta=old&&!w.reset&&old.version===w.baseVersion;
   if(delta&&!w.changes.size&&old.digest===w.digest){this.cursors.set(w.key,Object.freeze({...w.cursor}));continue;}
   let rows,rowBytes;
   if(delta){
    rows=old.rows;rowBytes=old.bytes-Buffer.byteLength(w.key);
    for(const [id,entry]of w.changes){
     const previous=rows.get(id);if(previous===entry)continue;
     if(previous){rowBytes-=80+previous.canonicalBytes;old.pool.release(previous);}
     if(entry){rows.set(id,w.pool.retain(entry));rowBytes+=80+entry.canonicalBytes;}else rows.delete(id);
     integrityWork.checkpointDeltaRows++;
    }
   }else{
    rows=new Map();rowBytes=0;
    for(const [id,e]of w.rows){rows.set(id,w.pool.retain(e));rowBytes+=80+e.canonicalBytes;integrityWork.checkpointCopiedRows++;}
    if(old)retired.push(old);
   }
   if(old)bytes-=old.bytes;
   const record=Object.freeze({key:w.key,keyField:w.keyField,rows,pool:w.pool,columns:w.columns,mode:w.mode,digest:w.digest,cursor:Object.freeze({...w.cursor}),complete:true,authority:this.binding,version:this.version+1,bytes:Buffer.byteLength(w.key)+rowBytes});
   next.set(w.key,record);this.cursors.delete(w.key);bytes+=record.bytes;
  }
  this.windows=next;this.bytes=bytes;this.peakBytes=Math.max(this.peakBytes,bytes);this.version++;this.commits++;
  for(const w of retired)for(const e of w.rows.values())w.pool.release(e);
  for(const w of windows)this.invalid.delete(w.key);
 }
 versionFor(key){return this.windows.get(key)?.version;}
 load(key,binding,pool){
  this.loads++;const w=this.windows.get(key);if(binding!==this.binding||this.invalid.has(key)||!w||w.authority!==binding||!w.complete||!validCursor(w.cursor))return;
  const cursor=this.cursors.get(key)||w.cursor;
  if(!validCursor(cursor)||cursor.epoch!==w.cursor.epoch||cursor.revision<w.cursor.revision)throw Error('checkpoint-cursor-mismatch');
  for(const [id,e]of w.rows)if(String(e.value[w.keyField])!==id){this.invalidate(key);throw Error('checkpoint-membership-key');}
  const state=WindowIntegrity.from(w.rows,w.columns);if(state.digest(w.mode)!==w.digest){this.invalidate(key);throw Error('checkpoint-integrity-mismatch');}
  const rows=new Map();for(const [id,e]of w.rows)rows.set(id,pool.retain(e));
  this.loadedWindows++;return {...w,rows,state,cursor:{...cursor}};
 }
 stats(){return {bytes:this.bytes,peakBytes:this.peakBytes,windows:this.windows.size-this.invalid.size,retainedWindows:this.windows.size,usableWindows:this.windows.size-this.invalid.size,commits:this.commits,loads:this.loads,loadedWindows:this.loadedWindows,version:this.version};}
}
