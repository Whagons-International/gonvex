import type { JsonValue, ServerMessage, StarterOffer } from "@gonvex/protocol";
import { replicaRowsHashes, replicaAggregateDigest } from "./replica-integrity.js";

export type StarterInflater = (encoded: Uint8Array, limits: { maxOutputBytes: number; windowBits: 12; flush: "sync" }) => Promise<Uint8Array>;
type Snapshot = Extract<ServerMessage,{type:"replica.snapshot"}>;
type Ready = Extract<ServerMessage,{type:"replica.ready"}>;
type Delta = Extract<ServerMessage,{type:"replica.delta"}>;
const PIECE=256*1024, TOTAL=32*1024*1024, ROWS=100_000;
function integer(value:number,max:number) { if(!Number.isSafeInteger(value)||value<0||value>max)throw new Error("starter-integer-limit"); }
/** Conservative UTF-8/JSON upper bound without allocating a serialized copy. */
function jsonCharge(value:unknown,limit:number,depth=0):number {
  if(depth>64)throw new Error("starter-json-depth");
  let size=0;const add=(n:number)=>{size+=n;integer(size,limit);};
  if(value===null||typeof value==="boolean")return 5;
  if(typeof value==="number"){if(!Number.isFinite(value))throw new Error("starter-number");return 32;}
  if(typeof value==="string"){const n=value.length*6+2;integer(n,limit);return n;}
  if(Array.isArray(value)){add(2);for(const v of value){add(1);add(jsonCharge(v,limit-size,depth+1));}return size;}
  if(value&&typeof value==="object"){add(2);for(const [k,v] of Object.entries(value)){add(k.length*6+4);add(jsonCharge(v,limit-size,depth+1));}return size;}
  throw new Error("starter-json-value");
}
async function sha(bytes:Uint8Array) { const digest=await globalThis.crypto.subtle.digest("SHA-256",bytes as Uint8Array<ArrayBuffer>);return Array.from(new Uint8Array(digest),b=>b.toString(16).padStart(2,"0")).join(""); }
function checkType(value:JsonValue,kind:string) {
  if(value===null)return;
  if(kind==="text"&&typeof value==="string")return;
  if(kind==="integer"&&typeof value==="number"&&Number.isSafeInteger(value))return;
  if(kind==="real"&&typeof value==="number"&&Number.isFinite(value))return;
  if(kind==="boolean"&&typeof value==="boolean")return;
  if(kind==="json")return;
  throw new Error("starter-column-type");
}
function strictJson(line:string):JsonValue {
  // Check duplicate decoded keys and depth BEFORE allocating the parsed tree.
  const stack:Array<{object:boolean;key:boolean;seen:Set<string>}>=[];
  const token=/"(?:\\.|[^"\\])*"|[{}\[\]:,]/g;
  for(let m:RegExpExecArray|null;(m=token.exec(line));) {
    const t=m[0],top=stack.at(-1);
    if(t==="{"||t==="["){if(stack.length>=64)throw new Error("starter-json-depth");stack.push({object:t==="{",key:true,seen:new Set()});}
    else if(t==="}"||t==="]")stack.pop();
    else if(t===","){if(top?.object)top.key=true;}
    else if(t.startsWith('"')&&top?.object&&top.key){const k=JSON.parse(t) as string;if(top.seen.has(k))throw new Error("starter-duplicate-column");top.seen.add(k);top.key=false;}
  }
  return JSON.parse(line) as JsonValue;
}
/** SDK-instance aggregate accounting; estimates retained JS objects, hashes,
 * key maps and worst-case pending base64/codec buffers. Not an RSS hard cap. */
export class StarterBudget {
  private bytes=0; private count=0; private peak=0;
  readonly maxBytes=64*1024*1024; readonly maxStages=4;
  acquire(bytes:number) {integer(bytes,this.maxBytes);if(this.count>=this.maxStages||this.bytes+bytes>this.maxBytes)throw new Error("starter-sdk-budget");this.bytes+=bytes;this.peak=Math.max(this.peak,this.bytes);this.count++;let held=bytes,closed=false;
    return {resize:(next:number)=>{integer(next,this.maxBytes);if(closed||this.bytes-held+next>this.maxBytes)throw new Error("starter-sdk-budget");this.bytes+=next-held;this.peak=Math.max(this.peak,this.bytes);held=next;},release:()=>{if(!closed){closed=true;this.bytes-=held;this.count--;}}};
  }
  usage(){return {chargedBytes:this.bytes,peakChargedBytes:this.peak,stages:this.count,maxBytes:this.maxBytes,maxStages:this.maxStages};}
}
/** UNCOMMITTED isolated memory namespace. No normal cache API can see this map.
 * Crash/reload drops it; active storage remains untouched until final ready.
 */
export class StarterStage {
  private sequence=0;
  private phase:"pieces"|"catchup"|"promoted"="pieces";
  private readonly rows=new Map<string,JsonValue>();
  private bytes=0;
  private cursor: StarterOffer["cursor"];
  private loan?: ReturnType<StarterBudget["acquire"]>;
  private overhead=0;
  private reservedRaw=0;
  private pending=0;private disposed=false;
  constructor(readonly offer:StarterOffer,private readonly inflate:StarterInflater, private readonly budget:StarterBudget) {
    if(offer.version!==1||offer.format!==1||offer.encoding!=="json-lines-pmd-level1-window12-no-context-v1"||offer.truncated!==false||!offer.offerId||!offer.cursor.epoch||offer.expiresAt<=Date.now())throw new Error("starter-offer-invalid");
    if(typeof offer.serializedDefinition!=="string"||offer.serializedDefinition.length>256*1024||offer.args===null||typeof offer.args!=="object"||Array.isArray(offer.args)||Object.keys(offer.args).length)throw new Error("starter-definition");
    integer(offer.expiresAt,Number.MAX_SAFE_INTEGER);integer(offer.cursor.revision,Number.MAX_SAFE_INTEGER);integer(offer.rows,ROWS);integer(offer.pieces.length,1024);integer(offer.maxRows,ROWS);integer(offer.maxBytes,TOTAL);
    if(!["asc","desc"].includes(offer.orderDirection)||offer.columnTypes[offer.key]!=="text"||!offer.columnTypes[offer.orderBy]||offer.columnTypes[offer.orderBy]==="json"||Object.keys(offer.columnTypes).length>1024||!/^xor1:[0-9a-f]{64}$/.test(offer.digest))throw new Error("starter-identity");
    if(Object.values(offer.columnTypes).some(t=>!["text","integer","real","boolean","json"].includes(t)))throw new Error("starter-column-kind");
    let total=0;
    for(const [i,p] of offer.pieces.entries()) {integer(p.index,1023);integer(p.bytes,PIECE);integer(p.rawBytes,PIECE);if(p.index!==i||typeof p.compressed!=="boolean"||! /^[0-9a-f]{64}$/.test(p.hash)||! /^[0-9a-f]{64}$/.test(p.rawHash))throw new Error("starter-piece-descriptor");total+=p.rawBytes;integer(total,TOTAL);}
    this.overhead=new TextEncoder().encode(JSON.stringify(offer)).byteLength*2+Math.max(0,...offer.pieces.map(p=>p.bytes))*8;
    this.reservedRaw=total;
    this.loan=budget.acquire(total*12+offer.rows*512+this.overhead);
    this.cursor={...offer.cursor};
  }
  dispose(){this.disposed=true;this.rows.clear();if(this.pending===0){this.loan?.release();this.loan=undefined;}}
  private begin(){if(this.disposed)throw new Error("starter-retired");this.pending++;}
  private end(){this.pending--;if(this.disposed && this.pending===0){this.loan?.release();this.loan=undefined;}}
  nextIndex() {return this.sequence<this.offer.pieces.length?this.sequence:undefined;}
  private validate(row:JsonValue) {
    jsonCharge(row,PIECE);
    integer(new TextEncoder().encode(JSON.stringify(row)).byteLength,PIECE);
    if(!row||typeof row!=="object"||Array.isArray(row)||Object.keys(row).length!==Object.keys(this.offer.columnTypes).length)throw new Error("starter-projection");
    for(const k of Object.keys(row)) {if(!Object.hasOwn(this.offer.columnTypes,k))throw new Error("starter-private-column");checkType(row[k]!,this.offer.columnTypes[k]!);}
    const key=row[this.offer.key];if(typeof key!=="string"||!key||key.includes("\0"))throw new Error("starter-key");return key;
  }
  async piece(message:Extract<ServerMessage,{type:"replica.starterPiece"}>) {
    this.begin();try {
    if(this.phase!=="pieces"||message.id!==this.offer.id||message.offerId!==this.offer.offerId||message.index!==this.sequence||Date.now()>=this.offer.expiresAt)throw new Error("starter-piece-owner");
    const p=this.offer.pieces[this.sequence];if(!p)throw new Error("starter-piece-overflow");
    const maxBase64=4*Math.ceil(p.bytes/3);
    if(message.data.length!==maxBase64||message.data.length>4*Math.ceil(PIECE/3)||! /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(message.data))throw new Error("starter-base64-limit");
    const encoded=Uint8Array.from(atob(message.data),c=>c.charCodeAt(0));
    if(encoded.length!==p.bytes||await sha(encoded)!==p.hash)throw new Error("starter-encoded-integrity");
    // The injected codec MUST enforce maxOutputBytes DURING inflation. The
    // Node subpath uses zlib maxOutputLength, not a post-allocation check.
    const raw=p.compressed?await this.inflate(encoded,{maxOutputBytes:p.rawBytes,windowBits:12,flush:"sync"}):encoded;
    if(raw.byteLength!==p.rawBytes||raw.byteLength>PIECE||await sha(raw)!==p.rawHash)throw new Error("starter-expanded-integrity");
    if(this.disposed)throw new Error("starter-retired");
    const text=new TextDecoder("utf-8",{fatal:true}).decode(raw);
    if(!text.endsWith("\n"))throw new Error("starter-ndjson-end");
    for(const line of text.slice(0,-1).split("\n")) {
      if(!line)throw new Error("starter-empty-row");
      this.loan?.resize(this.reservedRaw*12+Math.max(this.offer.rows,this.rows.size+1)*512+this.overhead);
      const row=strictJson(line),key=this.validate(row);
      if(this.rows.has(key))throw new Error("starter-duplicate-key");this.rows.set(key,row);this.bytes+=new TextEncoder().encode(line).byteLength;this.bound();
    }
    if(this.disposed)throw new Error("starter-retired");
    this.sequence++;
    } finally {this.end();}
  }
  private bound() {integer(this.rows.size,ROWS);integer(this.bytes,TOTAL);if((this.offer.maxRows>0&&this.rows.size>this.offer.maxRows)||(this.offer.maxBytes>0&&this.bytes>this.offer.maxBytes))throw new Error("starter-window-limit");}
  async seal() {
    this.begin();try {
    if(this.phase!=="pieces"||this.sequence!==this.offer.pieces.length||this.rows.size!==this.offer.rows)throw new Error("starter-incomplete");
    const hashes=await replicaRowsHashes([...this.rows.values()],this.offer.key);
    if(this.disposed)throw new Error("starter-retired");
    if(replicaAggregateDigest(hashes)!==this.offer.digest)throw new Error("starter-digest");this.phase="catchup";
    return {type:"replica.starterCommit" as const,id:this.offer.id,offerId:this.offer.offerId,cursor:this.offer.cursor,digest:this.offer.digest};
    } finally {this.end();}
  }
  delta(message:Delta) {
    if(this.phase!=="catchup"||message.cursor.epoch!==this.cursor.epoch||message.cursor.revision<this.cursor.revision)throw new Error("starter-catchup-cursor");
    const incoming=jsonCharge(message,TOTAL);
    this.loan?.resize((this.bytes+incoming)*12+(this.rows.size+(message.upserts?.length??0))*512+this.overhead);
    for(const key of message.deleted??[])this.rows.delete(key);
    for(const row of message.upserts??[])this.rows.set(this.validate(row),row);
    this.bytes=0;for(const row of this.rows.values())this.bytes+=new TextEncoder().encode(JSON.stringify(row)).byteLength;
    this.bound();this.loan?.resize(this.bytes*12+this.rows.size*512+this.overhead);this.cursor={...message.cursor};
  }
  async promote(ready:Ready):Promise<Snapshot> {
    this.begin();try {
    if(this.phase!=="catchup"||ready.cursor.epoch!==this.cursor.epoch||ready.cursor.revision<this.cursor.revision||ready.truncated===true||Date.now()>=this.offer.expiresAt)throw new Error("starter-ready");
    const direction=this.offer.orderDirection==="asc"?1:-1;
    const textCompare=(a:string,b:string)=>{if(a===b)return 0;const x=new TextEncoder().encode(a),y=new TextEncoder().encode(b);for(let i=0;i<Math.min(x.length,y.length);i++){if(x[i]!==y[i])return x[i]!-y[i]!;}return x.length-y.length;};
    const scalar=(a:JsonValue,b:JsonValue)=>a===b?0:a===null?-1:b===null?1:typeof a==="string"&&typeof b==="string"?textCompare(a,b):Number(a)-Number(b);
    const result=[...this.rows.values()].sort((a,b)=>{const x=a as Record<string,JsonValue>,y=b as Record<string,JsonValue>;return direction*(scalar(x[this.offer.orderBy]!,y[this.offer.orderBy]!)||textCompare(x[this.offer.key] as string,y[this.offer.key] as string));});const hashes=await replicaRowsHashes(result,this.offer.key);
    if(this.disposed)throw new Error("starter-retired");
    if(ready.digest!==replicaAggregateDigest(hashes))throw new Error("starter-ready-integrity");
    this.phase="promoted";
    // Only FINAL ready cursor enters the existing atomic replaceWindow storage
    // transaction. No starter/intermediate cursor was installed or persisted.
    return {type:"replica.snapshot",id:this.offer.id,path:this.offer.path,result,key:this.offer.key,cursor:ready.cursor,orderBy:this.offer.orderBy,orderDirection:this.offer.orderDirection,mode:this.offer.mode,maxRows:this.offer.maxRows,maxBytes:this.offer.maxBytes,truncated:false,hashes,digest:ready.digest};
    } finally {this.end();}
  }
}
