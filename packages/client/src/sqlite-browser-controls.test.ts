import {expect,it,vi} from 'vitest';
import {ByteLedger} from './byte-budget.js';
import {SQLiteBrowserControls} from './sqlite-browser-controls.js';
function fixture(){
 let version=1,result:unknown=[{id:'tenant'}],reads=0,opens=0,closes=0,handler:((input:any)=>Promise<any>)|undefined;const ledger=new ByteLedger(1048576);
 const controls=new SQLiteBrowserControls(async<T>(input:any):Promise<T>=>{if(handler){const override=await handler(input);if(override!==undefined)return override;}if(input.method==='watchControl'){opens++;return {signature:input.path} as T;}if(input.method==='controlState'){reads++;return (input.afterVersion===version?{version}:{version,result}) as T;}if(input.method==='unwatchControl'){closes++;return null as T;}throw Error('Unknown RPC');},ledger);
 return {controls,ledger,get reads(){return reads;},get opens(){return opens;},get closes(){return closes;},set handler(value:typeof handler){handler=value;},update(value:unknown){version++;result=value;controls.refresh();}};
}
it('creates inert React watches, shares the owner source and releases retained rows with the last listener',async()=>{
 const f=fixture(),one=f.controls.watch({path:'tenants'},{}),two=f.controls.watch({path:'tenants'},{});expect(f.opens).toBe(0);expect(f.ledger.bytes).toBe(0);expect(one.getSnapshot()).toBe(one.getSnapshot());const release=one.onUpdate(()=>{}),releaseTwo=two.onUpdate(()=>{});
 await vi.waitFor(()=>expect(one.getSnapshot().result).toEqual([{id:'tenant'}]));expect(f.opens).toBe(1);expect(two.getSnapshot()).toBe(one.getSnapshot());const stable=one.getSnapshot();f.controls.refresh();await vi.waitFor(()=>expect(f.reads).toBeGreaterThan(1));expect(one.getSnapshot()).toBe(stable);
 f.update([{id:'next'}]);await vi.waitFor(()=>expect(two.getSnapshot().result).toEqual([{id:'next'}]));release();expect(f.closes).toBe(0);releaseTwo();await vi.waitFor(()=>expect(f.closes).toBe(1));expect(f.ledger.bytes).toBe(0);expect(one.getSnapshot().result).toBeUndefined();f.controls.close();
});
it('does not lose a notification which arrives while an earlier state read is pending',async()=>{
 const f=fixture(),watch=f.controls.watch({path:'tenants'},{});let finish!:(value:any)=>void;const barrier=new Promise(resolve=>{finish=resolve;});let held=false;f.handler=async input=>{if(input.method==='controlState'&&!held){held=true;return barrier;}};const release=watch.onUpdate(()=>{});await vi.waitFor(()=>expect(held).toBe(true));f.update([{id:'latest'}]);finish({version:1,result:[{id:'old'}]});await vi.waitFor(()=>expect(watch.getSnapshot().result).toEqual([{id:'latest'}]));release();f.controls.close();
});
it('redacts a prior identity immediately and refuses a late result before reopening the watch',async()=>{
 const f=fixture(),watch=f.controls.watch({path:'tenants'},{}),release=watch.onUpdate(()=>{});await vi.waitFor(()=>expect(watch.getSnapshot().result).toBeDefined());let finish!:(value:any)=>void;const barrier=new Promise(resolve=>{finish=resolve;});let held=false;f.handler=async input=>{if(input.method==='controlState'&&!held){held=true;return barrier;}};f.controls.refresh();await vi.waitFor(()=>expect(held).toBe(true));f.controls.invalidate(true);expect(watch.getSnapshot().result).toBeUndefined();f.update([{id:'new-identity'}]);finish({version:10,result:[{id:'secret-old'}]});await vi.waitFor(()=>expect(watch.getSnapshot().result).toEqual([{id:'new-identity'}]));release();f.controls.close();
});
it('serializes a new listener against an in-flight unsubscribe and bounds shared observers',async()=>{
 const f=fixture(),watch=f.controls.watch({path:'tenants'},{}),release=watch.onUpdate(()=>{});await vi.waitFor(()=>expect(watch.getSnapshot().result).toBeDefined());let finish!:(value:any)=>void;const barrier=new Promise(resolve=>{finish=resolve;});let retiring=false;f.handler=async input=>{if(input.method==='unwatchControl'){retiring=true;return barrier;}};release();await vi.waitFor(()=>expect(retiring).toBe(true));const again=watch.onUpdate(()=>{});await new Promise(resolve=>setTimeout(resolve,0));expect(f.opens).toBe(1);finish(null);await vi.waitFor(()=>expect(f.opens).toBe(2));await vi.waitFor(()=>expect(watch.getSnapshot().result).toBeDefined());const releases=Array.from({length:31},()=>f.controls.watch({path:'tenants'},{}).onUpdate(()=>{}));expect(()=>f.controls.watch({path:'tenants'},{}).onUpdate(()=>{})).toThrow('listener allowance');for(const release of releases)release();again();f.controls.close();
});

it('keeps known control rows on read timeout and retries without throwing during render',async()=>{
 const f=fixture(),watch=f.controls.watch({path:'tenants'},{}),release=watch.onUpdate(()=>{});await vi.waitFor(()=>expect(watch.getSnapshot().result).toBeDefined());const previous=watch.getSnapshot().result;
 let fail=true;f.handler=async input=>{if(input.method==='controlState'&&fail){fail=false;throw Object.assign(Error('Storage read timed out'),{code:'TIMEOUT',outcome:'rejected'});}};
 f.controls.refresh();await vi.waitFor(()=>expect(watch.getSnapshot().status).toBe('refreshing'));expect(watch.getSnapshot().result).toBe(previous);
 await vi.waitFor(()=>expect(watch.getSnapshot().status).toBeUndefined());expect(watch.getSnapshot().result).toBe(previous);release();f.controls.close();
});

it('treats a superseded cold-session read as refreshing and resubscribes without throwing into render',async()=>{
 const f=fixture(),watch=f.controls.watch({path:'tenants'},{}),release=watch.onUpdate(()=>{});await vi.waitFor(()=>expect(watch.getSnapshot().result).toBeDefined());const previous=watch.getSnapshot().result;let superseded=true;
 f.handler=async input=>{if(input.method==='controlState'&&superseded){superseded=false;throw Object.assign(Error('Native browser session changed'),{code:'FENCED'});}};
 f.controls.refresh();await vi.waitFor(()=>expect(watch.getSnapshot().status).toBe('refreshing'));expect(watch.getSnapshot().result).toBe(previous);expect(()=>watch.getSnapshot()).not.toThrow();
 await vi.waitFor(()=>expect(watch.getSnapshot().status).toBeUndefined());expect(watch.getSnapshot().result).toBe(previous);release();f.controls.close();
});
