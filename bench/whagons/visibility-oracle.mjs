/** Compare only complete, certified windows at one authority and revision.
 * Unmatched/partial observations fail qualification; they never count as equality
 * and their provisional row differences are not labelled authoritative leaks. */
import { canonical, windowKey, validCursor, sha, integrity } from './thin-checkpoint.mjs';
export function captureVisibility({client,watch,ref,args={},principal,authority}){
 const descriptor={principal,window:windowKey(ref,args),authority:authority&&{protocolVersion:authority.protocolVersion,scope:authority.scope,visibilityScope:authority.visibilityScope,definitionEpoch:authority.epoch}};
 const base={descriptor,at:Date.now(),qualified:false};
 try{
  const status=watch.status(),state=watch.localReplicaState();
  if(!authority||authority.protocolVersion!==1||!authority.scope||!authority.visibilityScope||!authority.epoch||!principal?.member||!principal?.tenant||!principal?.identity)return {...base,reason:'missing-authority'};
  if(!status.isUpToDate||state?.completeness!=='complete')return {...base,reason:'window-not-complete-current',status,state};
  const rows=watch.localReplicaResult();if(!Array.isArray(rows))return {...base,reason:'missing-rows'};
  const window=client.replicaWindowState?client.replicaWindowState(ref,args):client.localReplica?.getWindow(client.replicaSignature(ref,args));
  const cursor=window?.cursor,globalCursor=client.localReplica?.cursor();
  if(!validCursor(cursor)||window?.consistent===false||window?.truncated||window?.completeness==='partial')return {...base,reason:'uncertified-window',cursor};
  if(state.computedRevision!==undefined&&state.computedRevision!==cursor.revision)return {...base,reason:'window-revision-disagrees',cursor,state};
  if(globalCursor&&(globalCursor.epoch!==cursor.epoch||globalCursor.revision!==cursor.revision))return {...base,reason:'uncovered-entity-revision',cursor,globalCursor};
  const ids=window.ids;if(!Array.isArray(ids)||ids.length!==rows.length)return {...base,reason:'incomplete-membership',cursor};
  const key=ref.replica?.key||'_id',byId=new Map(),hashRows=new Map();
  for(const row of rows){const id=String(row[key]);if(byId.has(id))return {...base,reason:'duplicate-row',cursor};const projected=Object.fromEntries((ref.replica?.columns||Object.keys(row)).filter(k=>Object.hasOwn(row,k)).map(k=>[k,row[k]]));byId.set(id,canonical(projected));hashRows.set(id,{value:projected,serializedValue:JSON.stringify(projected)});}
  if(ids.some(id=>!byId.has(String(id))))return {...base,reason:'missing-membership-row',cursor};
  const digest=window.digest||window.aggregateDigest;
  if(typeof digest!=='string'||!(/^(xor1:)?[a-f0-9]{64}$/).test(digest))return {...base,reason:'missing-validated-digest',cursor};
  if(integrity(hashRows,ref.replica?.columns,digest.startsWith('xor1:')?'xor1':'sha256').digest!==digest)return {...base,reason:'checkpoint-row-digest-mismatch',cursor};
  // Immutable strings pin values across later deltas/scope changes.
  const result={...base,qualified:true,cursor:{...cursor},digest:window.digest||window.aggregateDigest,rowCount:byId.size,rows:byId,rowsHash:sha(canonical([...byId].sort(([a],[b])=>a.localeCompare(b))))};return result;
 }catch(e){return {...base,reason:'window-error',error:String(e)};}
}
export function compareVisibility(left,right){
 const metadata=v=>{const{rows,...rest}=v;return rest;};
 const result={left:metadata(left),right:metadata(right),qualified:false,passed:false};
 if(!left.qualified||!right.qualified)return {...result,kind:'unqualified',reason:'incomplete-or-uncertified-window'};
 if(canonical(left.descriptor)!==canonical(right.descriptor))return {...result,kind:'unqualified',reason:'authority-or-window-mismatch'};
 if(canonical(left.cursor)!==canonical(right.cursor))return {...result,kind:'unqualified',reason:'revision-mismatch'};
 const extra=[...left.rows.keys()].filter(id=>!right.rows.has(id)),missing=[...right.rows.keys()].filter(id=>!left.rows.has(id));
 const changed=[...left.rows].filter(([id,value])=>right.rows.has(id)&&right.rows.get(id)!==value).map(([id])=>id);
 return {...result,qualified:true,kind:extra.length||missing.length||changed.length?'mismatch':'equal',passed:!extra.length&&!missing.length&&!changed.length,extra,missing,changed};
}
