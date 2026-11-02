import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage } from "./index.js";
class Socket extends EventTarget {
  static OPEN=1; static latest: Socket; readyState=0; sent:any[]=[];
  constructor(){super();Socket.latest=this;}
  send(text:string){this.sent.push(JSON.parse(text));}
  open(){this.readyState=1;this.dispatchEvent(new Event("open"));}
  receive(frame:unknown){this.dispatchEvent(new MessageEvent("message",{data:JSON.stringify(frame)}));}
  close(){this.readyState=3;}
  disconnect(){this.readyState=3;this.dispatchEvent(new Event("close"));}
}
const clients:GonvexClient[]=[];
afterEach(()=>{clients.splice(0).forEach(client=>client.close());vi.unstubAllGlobals();});
const reference={kind:"query",scope:"control",delivery:"live",path:"control.private"} as const;
const privateResult={private:"Alice control data"};
function rotate(client:GonvexClient,change:string){
 if(change==="account"||change==="return"){
  client.setAuth({token:"bob-session",identity:{sub:"bob",iss:"issuer"}});
  if(change==="return")client.setAuth({token:"alice-new-session",identity:{sub:"alice",iss:"issuer"}});
 }else if(change==="tenant")client.setAuth({tenant:"other"});
 else if(change==="close")client.close();
}
async function setup(role: "control" | "app" = "control"){
 vi.stubGlobal("WebSocket",Socket);
 const client=new GonvexClient("ws://control-watch.test/ws",{role:role==="control"?"control":undefined,project:"project",tenant:"tenant",token:"alice-session",identity:{sub:"alice",iss:"issuer"},errorReporting:false,localReplica:{storage:new MemoryLocalReplicaStorage()}});
 clients.push(client);const watch=client.watchControlQuery(reference,{});
 const socket=Socket.latest;socket.open();await vi.waitFor(()=>expect(socket.sent.some(frame=>frame.type==="auth")).toBe(true));
 const auth=socket.sent.filter(frame=>frame.type==="auth").at(-1);
 const previousIDs=new Set(socket.sent.filter(frame=>frame.type==="query.subscribe").map(frame=>frame.id));
 socket.receive({type:"auth.result",id:auth.id,result:{accountId:"alice",tenantId:"tenant",replica:{protocolVersion:1,scope:"tenant",visibilityScope:"visible-alice-tenant",epoch:"db"}}});
 await vi.waitFor(()=>expect(socket.sent.some(frame=>frame.type==="query.subscribe"&&(role==="control"||!previousIDs.has(frame.id)))).toBe(true));
 const query=socket.sent.filter(frame=>frame.type==="query.subscribe"&&(role==="control"||!previousIDs.has(frame.id))).at(-1);
 return {client,watch,socket,query};
}
it.each(["unchanged","account","tenant","return","close"])("queued Control Query watch notifications retain ownership across %s",async change=>{
 const {client,watch,socket,query}=await setup();const observed:unknown[]=[];
 const stop=watch.onUpdate(()=>observed.push(watch.getSnapshot().result));
 await new Promise(done=>setTimeout(done,0));observed.length=0;
 socket.receive({type:"query.result",id:query.id,result:privateResult});
 rotate(client,change);await new Promise(done=>setTimeout(done,0));
 if(change==="close")expect(observed,"closed Control Query watch emitted a queued notification").toEqual([]);
 expect(observed.filter(value=>JSON.stringify(value)===JSON.stringify(privateResult)),"old Control Query watch notification crossed its owner").toHaveLength(change==="unchanged"?1:0);
 stop();
});
it.each(["account","tenant","return"])("Control Query watch snapshot clears immediately across %s",async change=>{
 const {client,watch,socket,query}=await setup();const observed:unknown[]=[];const stop=watch.onUpdate(()=>observed.push(watch.getSnapshot().result));
 socket.receive({type:"query.result",id:query.id,result:privateResult});
 await vi.waitFor(()=>expect(watch.getSnapshot().result).toEqual(privateResult));
 await new Promise(done=>setTimeout(done,0));observed.length=0;
 rotate(client,change);
 expect(watch.getSnapshot().result,"old Control Query snapshot remained readable under a new owner").toBeUndefined();
 await vi.waitFor(()=>expect(observed,"new owner was not notified that the snapshot cleared").toContain(undefined));stop();
});
it.each(["unchanged","account","tenant","return","close"])("Control Query watch listener fanout retains ownership across reentrant %s",async change=>{
 const {client,watch,socket,query}=await setup();let acted=false;const later:unknown[]=[];
 const first=watch.onUpdate(()=>{if(acted||watch.getSnapshot().result===undefined)return;acted=true;rotate(client,change);});
 const second=watch.onUpdate(()=>later.push(watch.getSnapshot().result));
 await new Promise(done=>setTimeout(done,0));later.length=0;
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));
 if(change==="close")expect(later,"closed Control Query watch called a later listener").toEqual([]);
 expect(later.filter(value=>JSON.stringify(value)===JSON.stringify(privateResult)),"old Control Query watch reached a later listener after owner invalidation").toHaveLength(change==="unchanged"?1:0);
 first();second();
});

it.each(["unchanged","account","tenant","return"])("Control Query watch error state belongs to its owner across %s",async change=>{
 const {client,watch,socket,query}=await setup();const stop=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.error",id:query.id,error:"Alice private error"});
 expect(()=>watch.getSnapshot()).toThrow("Alice private error");rotate(client,change);
 if(change==="unchanged")expect(()=>watch.getSnapshot()).toThrow("Alice private error");
 else expect(()=>watch.getSnapshot(),"old Control Query error remained readable under a new owner").not.toThrow();
 stop();
});
it.each(["unchanged","account","tenant","return","close"])("late Control Query watch listener retains its initial scheduling owner across %s",async change=>{
 const {client,watch,socket,query}=await setup();const first=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.result",id:query.id,result:privateResult});
 await new Promise(done=>setTimeout(done,0));const observed:unknown[]=[];
 const later=watch.onUpdate(()=>observed.push(watch.getSnapshot().result));rotate(client,change);
 await new Promise(done=>setTimeout(done,0));
 if(change==="close")expect(observed,"closed Control Query watch emitted an initial notification").toEqual([]);
 expect(observed.filter(value=>JSON.stringify(value)===JSON.stringify(privateResult)),"old Control Query initial notification crossed its owner").toHaveLength(change==="unchanged"?1:0);
 first();later();
});
it.each(["account","tenant","return"])("Control Query watch continues with the fresh owner across %s",async change=>{
 const {client,watch,socket,query}=await setup();const observed:unknown[]=[];const stop=watch.onUpdate(()=>observed.push(watch.getSnapshot().result));
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));
 rotate(client,change);
 let auth=socket.sent.filter(frame=>frame.type==="auth").at(-1);
 const accept=()=>socket.receive({type:"auth.result",id:auth.id,result:{accountId:change==="account"?"bob":"alice",tenantId:change==="tenant"?"other":"tenant"}});
 const before=socket.sent.filter(frame=>frame.type==="query.subscribe").length;
 accept();const latest=socket.sent.filter(frame=>frame.type==="auth").at(-1);
 if(latest.id!==auth.id){auth=latest;accept();}
 await vi.waitFor(()=>expect(socket.sent.filter(frame=>frame.type==="query.subscribe").length).toBeGreaterThan(before));
 const fresh=socket.sent.filter(frame=>frame.type==="query.subscribe").at(-1);expect(fresh.id).not.toBe(query.id);
 socket.receive({type:"query.result",id:fresh.id,result:{private:"Fresh owner control data"}});
 await vi.waitFor(()=>expect(observed,"fresh Control Query owner made no progress").toContainEqual({private:"Fresh owner control data"}));
 stop();
});

it.each(["account","tenant","return"])("inactive Control Query watch snapshot clears for a later owner across %s",async change=>{
 const {client,watch,socket,query}=await setup();const stop=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));
 expect(watch.getSnapshot().result).toEqual(privateResult);stop();rotate(client,change);
 expect(watch.getSnapshot().result,"inactive Control Query snapshot crossed its last owner").toBeUndefined();
});
it.each(["account","tenant","return"])("inactive Control Query watch resumes query delivery for the later owner across %s",async change=>{
 const {client,watch,socket,query}=await setup();const first=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));first();rotate(client,change);
 const observed:unknown[]=[];const stop=watch.onUpdate(()=>observed.push(watch.getSnapshot().result));
 let auth=socket.sent.filter(frame=>frame.type==="auth").at(-1);
 const accept=()=>socket.receive({type:"auth.result",id:auth.id,result:{accountId:change==="account"?"bob":"alice",tenantId:change==="tenant"?"other":"tenant"}});
 const before=socket.sent.filter(frame=>frame.type==="query.subscribe").length;accept();
 const latest=socket.sent.filter(frame=>frame.type==="auth").at(-1);if(latest.id!==auth.id){auth=latest;accept();}
 await vi.waitFor(()=>expect(socket.sent.filter(frame=>frame.type==="query.subscribe").length,"resumed Control Query sent no subscription").toBeGreaterThan(before));
 const fresh=socket.sent.filter(frame=>frame.type==="query.subscribe").at(-1);
 socket.receive({type:"query.result",id:fresh.id,result:{private:"Resumed owner control data"}});
 await vi.waitFor(()=>expect(observed,"resumed Control Query owner made no progress").toContainEqual({private:"Resumed owner control data"}));stop();
});

it("Control Query watch retains its valid owner snapshot during a transport reconnect",async()=>{
 const {client,watch,socket,query}=await setup();const stop=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));
 const before=watch.getSnapshot();expect(before.result).toEqual(privateResult);
 socket.disconnect();client.connect();expect(Socket.latest).not.toBe(socket);
 expect(watch.getSnapshot(),"same-owner reconnect discarded an authorized Control Query cache").toBe(before);stop();
});

it("inactive Control Query watch rejects a previous visibility activation after a scope return",async()=>{
 const {client,watch,socket,query}=await setup("app");const stop=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));
 expect(watch.getSnapshot().result).toEqual(privateResult);stop();
 for(const visibilityScope of ["visible-restricted", "visible-alice-tenant"]){
  client.setAuth({token:"same-alice-session-"+visibilityScope});
  const auth=socket.sent.filter(frame=>frame.type==="auth").at(-1);
  socket.receive({type:"auth.result",id:auth.id,result:{accountId:"alice",tenantId:"tenant",replica:{protocolVersion:1,scope:"tenant",visibilityScope,epoch:"db"}}});
 }
 expect(watch.getSnapshot().result,"inactive Control Query cache survived a different visibility activation").toBeUndefined();
});

it("inactive Control Query cache invalidates before scope observers can read it",async()=>{
 const {client,watch,socket,query}=await setup("app");const stop=watch.onUpdate(()=>undefined);
 socket.receive({type:"query.result",id:query.id,result:privateResult});await new Promise(done=>setTimeout(done,0));stop();
 const observed:unknown[]=[];const stopScope=client.onSessionScopeChange(()=>observed.push(watch.getSnapshot().result));
 client.setAuth({token:"same-alice-visibility-transition"});
 const auth=socket.sent.filter(frame=>frame.type==="auth").at(-1);
 socket.receive({type:"auth.result",id:auth.id,result:{accountId:"alice",tenantId:"tenant",replica:{protocolVersion:1,scope:"tenant",visibilityScope:"visible-restricted",epoch:"db"}}});
 expect(observed,"scope observer read an inactive Control Query cache before owner invalidation").toEqual([undefined]);stopScope();
});
