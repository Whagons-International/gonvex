import type {JsonValue} from '@gonvex/protocol';
import type {FunctionReference} from './index.js';
import {GonvexClientError} from './client-error.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
import type {ByteLedger} from './byte-budget.js';
type Snapshot={result:JsonValue|undefined;version:number;status?:'current'|'refreshing'|'error';error?:Error};
type Watch={retry?:ReturnType<typeof setTimeout>;retries?:number;key:string;path:string;args:JsonValue;refs:number;listeners:Set<()=>void>;state:Snapshot;error?:Error;serverVersion?:number;signature?:string;bound:number;bytes:number;resultBytes:number;opening?:Promise<void>;retiring?:Promise<void>;queued:boolean;again?:boolean};
const stable=(value:JsonValue):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable(value[key]!)).join(',')+'}';
/** Bounded shared Control Plane watches. Creating a React watch is inert;
 * listener leases own subscriptions. Server notifications drive refresh, and
 * one revision fences every asynchronous open/read across identity changes. */
export class SQLiteBrowserControls {
 private readonly watches=new Map<string,Watch>();private readonly queue:Watch[]=[];private active=0;private revision=0;private closed=false;private bytes=0;
 constructor(private readonly rpc:<T>(input:unknown)=>Promise<T>,ledger:ByteLedger){ledger.register('browser-control-watches',()=>({name:'browser-control-watches',bytes:this.bytes,entries:this.watches.size,maxBytes:655360,maxEntries:32,evictions:0,refusals:0}));}
 private notify(watch:Watch){for(const listener of watch.listeners)try{listener();}catch{/* Observer only. */}}
 private check(revision:number,watch:Watch){if(this.closed||revision!==this.revision||!watch.refs)fail('FENCED','Control watch admission changed');}
 private async open(watch:Watch){if(watch.retiring)await watch.retiring;if(watch.bound===this.revision&&watch.signature)return;if(watch.opening)return watch.opening;const revision=this.revision;
  const opening=this.rpc<{signature:string}>({method:'watchControl',path:watch.path,args:watch.args}).then(source=>{this.check(revision,watch);watch.signature=source.signature;watch.bound=revision;watch.serverVersion=undefined;});watch.opening=opening;try{await opening;}finally{if(watch.opening===opening)watch.opening=undefined;}
 }
 private schedule(watch:Watch){if(this.closed||!watch.refs)return;if(watch.queued){watch.again=true;return;}watch.queued=true;this.queue.push(watch);this.pump();}
 private pump(){while(!this.closed&&this.active<4&&this.queue.length){const watch=this.queue.shift()!;this.active++;void this.read(watch).finally(()=>{watch.queued=false;this.active--;if(watch.again){watch.again=false;this.schedule(watch);}this.pump();});}}
 private async read(watch:Watch){const revision=this.revision;try{this.check(revision,watch);await this.open(watch);this.check(revision,watch);const value=await this.rpc<{version:number;result?:JsonValue;error?:string}>({method:'controlState',signature:watch.signature,afterVersion:watch.serverVersion});this.check(revision,watch);if(value.version===watch.serverVersion){if(watch.state.status){watch.retries=0;watch.state={result:watch.state.result,version:watch.state.version+1};this.notify(watch);}return;}
  const bytes=sqlitePayloadBytes(value,131072);if(this.bytes-watch.resultBytes+bytes>655360)fail('BUDGET','Control watch results exceed their byte budget');this.bytes+=bytes-watch.resultBytes;watch.resultBytes=bytes;watch.serverVersion=value.version;watch.error=value.error?new GonvexClientError(value.error,{code:'server',path:watch.path,operation:'query'}):undefined;watch.retries=0;watch.state={result:value.result,version:watch.state.version+1};this.notify(watch);
 }catch(error){if(!this.closed&&revision===this.revision&&watch.refs){const failure=error instanceof Error?error:Error(String(error));
 if(['TIMEOUT','timeout','FENCED','PAGE_CHANGED'].includes(String((error as {code?:string})?.code))){
  if(['FENCED','PAGE_CHANGED'].includes(String((error as {code?:string})?.code))){watch.bound=-1;watch.signature=undefined;watch.serverVersion=undefined;}
  watch.error=undefined;watch.retries=(watch.retries??0)+1;watch.state={...watch.state,status:watch.retries<=3?'refreshing':'error',error:failure,version:watch.state.version+1};
  clearTimeout(watch.retry);if(watch.retries<=3)watch.retry=setTimeout(()=>{watch.retry=undefined;this.schedule(watch);},Math.min(1000,100*2**(watch.retries-1)));
 }else{watch.error=failure;watch.state={...watch.state,version:watch.state.version+1};}this.notify(watch);}}}
 count(){return this.watches.size;}
 refresh(){for(const watch of this.watches.values())this.schedule(watch);}
 invalidate(clear:boolean){this.revision++;for(const watch of this.watches.values()){clearTimeout(watch.retry);watch.retry=undefined;watch.retries=0;watch.signature=undefined;watch.bound=-1;watch.serverVersion=undefined;watch.error=undefined;if(clear){this.bytes-=watch.resultBytes;watch.resultBytes=0;watch.state={result:undefined,version:watch.state.version+1};this.notify(watch);}}this.refresh();}
 watch<T extends JsonValue>(ref:Pick<FunctionReference,'path'>,args:JsonValue){
  sqlitePayloadBytes({path:ref.path,args},16384);const key=stable({path:ref.path,args}),empty:Snapshot={result:undefined,version:0};let selected:Watch|undefined;const listeners=new Map<()=>void,()=>void>();
  return {getSnapshot:():Readonly<Omit<Snapshot,'result'>&{result:T|undefined}>=>{if(selected?.error)throw selected.error;return (selected?.state??empty) as {result:T|undefined;version:number};},onUpdate:(listener:()=>void)=>{
   if(this.closed)fail('FENCED','Control watches are closed');if(listeners.size>=32)fail('BUDGET','Control listener allowance exceeded');let watch=this.watches.get(key);
   if(!watch){const bytes=sqlitePayloadBytes({key,path:ref.path,args},32768)+1024;if(this.watches.size>=32||this.bytes+bytes>655360)fail('BUDGET','Control watch allowance exceeded');watch={key,path:ref.path,args:JSON.parse(stable(args)),refs:0,listeners:new Set(),state:empty,bound:-1,bytes,resultBytes:0,queued:false};this.watches.set(key,watch);this.bytes+=bytes;}
   if(watch.listeners.size>=32)fail('BUDGET','Shared Control listener allowance exceeded');selected=watch;const record=watch,wrapped=()=>listener();listeners.set(wrapped,wrapped);record.listeners.add(wrapped);record.refs++;this.schedule(record);queueMicrotask(()=>{if(listeners.has(wrapped))wrapped();});let released=false;
   return ()=>{if(released)return;released=true;if(this.closed)return;listeners.delete(wrapped);record.listeners.delete(wrapped);if(--record.refs)return;clearTimeout(record.retry);record.retry=undefined;this.bytes-=record.resultBytes;record.resultBytes=0;record.state={result:undefined,version:record.state.version+1};record.serverVersion=undefined;record.error=undefined;
    const retiring=(async()=>{try{await record.opening?.catch(()=>{});if(record.refs)return;if(record.signature)await this.rpc({method:'unwatchControl',signature:record.signature});}finally{record.signature=undefined;record.bound=-1;if(!record.refs&&this.watches.get(key)===record){this.watches.delete(key);this.bytes-=record.bytes+record.resultBytes;}}})();record.retiring=retiring;void retiring.then(()=>{if(record.retiring===retiring)record.retiring=undefined;},()=>{if(record.retiring===retiring)record.retiring=undefined;});
   };
  }};
 }
 close(){if(this.closed)return;this.closed=true;this.revision++;this.queue.length=0;for(const watch of this.watches.values()){clearTimeout(watch.retry);watch.retry=undefined;watch.refs=0;watch.listeners.clear();watch.state={result:undefined,version:watch.state.version+1};watch.error=undefined;}this.watches.clear();this.bytes=0;}
}
