// 0062 actual-holder regression. Uses normal router admission, sockets,
// telemetry ports and operator reads. No placement override or fake limiter.
import { rawQuery, RAW_CAPABILITIES } from '../../../conformance/scenarios/helpers.mjs';
export default {
 name:'telemetry-holder-domains',tenants:{a:['alice']},tables:[],
 async run(ctx) {
  if(!ctx.backend.inspectHolders) throw new Error('actual holder inspection required');
  let token, pair, separate, anonPair;
  const sockets=[], known=new Set();
  const inspect=()=>ctx.backend.inspectHolders(ctx.project);
  const add=async(anonymous,index)=>{
   const socket=await ctx.raw(`${anonymous?'anon':'member'}-${index}`);
   if(!anonymous && !token) token=(await ctx.rawLogin(socket,'alice')).accessToken;
   await socket.auth(anonymous?{project:ctx.project}:{project:ctx.project,tenant:ctx.tenants.a,token,capabilities:RAW_CAPABILITIES});
   const state=await inspect(); ctx.expect(state.placement==='automatic','this regression requires actual automatic admission');
   const fresh=state.holders.flatMap(h=>h.sockets.map(s=>({...s,holder:h.holder}))).filter(s=>!known.has(s.connectionId));
   ctx.expect(fresh.length===1,'inspection must identify exactly this accepted socket');
   for(const h of state.holders) for(const s of h.sockets) known.add(s.connectionId);
   const item={...fresh[0],socket,anonymous}; sockets.push(item); return item;
  };
  const findPair=(items)=>{for(const a of items) for(const b of items) if(a!==b&&a.holder===b.holder)return[a,b];};
  const envelope=async(item,id,message)=>{
   item.socket.send({type:'error.envelope',id,events:[{eventId:id,message,account:{id:'spoofed'},tenant:'spoofed',project:'spoofed',sessionId:'spoofed',context:{secret:'must disappear for anonymous'}}]});
   return item.socket.next(f=>f.type==='error.ack'&&f.id===id);
  };
  const success=(reply)=>ctx.expect(reply.accepted===1&&!reply.error,`expected accepted event: ${JSON.stringify(reply)}`);
  const refused=(reply)=>ctx.expect(reply.error==='error telemetry rate limit exceeded'&&reply.accepted===undefined,`expected exact budget refusal: ${JSON.stringify(reply)}`);
  const window=async(item)=>{
   const state=await inspect();return state.holders.find(h=>h.holder===item.holder)?.sockets.find(s=>s.connectionId===item.connectionId)?.window;
  };
  let memberFingerprint,anonymousFingerprint;
  await ctx.step('identify actual same-holder and independent-holder sockets',async()=>{
   for(let i=0;i<40;i++){
    await add(false,i);pair=findPair(sockets);
    separate=pair&&sockets.find(s=>s.holder!==pair[0].holder);
    if(pair&&separate)break;
   }
   ctx.expect(pair&&separate,'bounded admission sample must supply measured holder domains');
   for(let i=0;i<40;i++){
    await add(true,i);anonPair=findPair(sockets.filter(s=>s.anonymous));if(anonPair)break;
   }
   ctx.expect(anonPair,'anonymous independence must be tested on the SAME actual holder');
   ctx.observe('placement',sockets.map(({socket,...s})=>s));
  });
  await ctx.step('same account shares 120 units including performance work on its holder',async()=>{
   const at=Date.now();
   for(let i=0;i<20;i++)pair[0].socket.send({type:'telemetry.event',id:`perf-${i}`,kind:'query',path:'k22.domain',outcome:'ok',clientReceivedAtMs:Date.now(),clientDurationMs:1});
   await rawQuery(pair[0].socket,'host.querySurface',{}); // serialized, noncharging barrier
   ctx.expect((await window(pair[0]))?.count===20,'20 actual performance events charged the account window');
   for(let i=0;i<100;i++){const reply=await envelope(pair[i%2],`shared-${i}`,'shared-holder-member');success(reply);memberFingerprint=reply.fingerprints[0];}
   refused(await envelope(pair[0],'shared-over-a','limited'));refused(await envelope(pair[1],'shared-over-b','limited'));
   const a=await window(pair[0]),b=await window(pair[1]);
   ctx.expect(a?.count===120&&JSON.stringify(a)===JSON.stringify(b),'same holder/account has one actual 120-unit window');
   ctx.expect(Date.now()-at<60_000&&Date.now()-a.started<60_000,'offered work must remain in one measured minute');
   ctx.observe('offeredAcceptedRefused',{performance:20,errors:100,refused:2,window:a,holder:pair[0].holder});
  });
  await ctx.step('another actual holder has its own account window',async()=>{
   ctx.expect((await window(separate))===null,'independent holder starts uncharged');
   for(let i=0;i<120;i++)success(await envelope(separate,`other-${i}`,'independent-holder-member'));
   refused(await envelope(separate,'other-over','limited'));
   const w=await window(separate);ctx.expect(w?.count===120&&Date.now()-w.started<60_000,'independent 120-unit window');
   ctx.observe('offeredAcceptedRefused',{offered:121,accepted:120,refused:1,window:w,holder:separate.holder});
  });
  await ctx.step('anonymous sockets on one holder have independent 20-unit windows',async()=>{
   for(let j=0;j<2;j++){
    for(let i=0;i<20;i++){const reply=await envelope(anonPair[j],`anon-${j}-${i}`,'anonymous-domain');success(reply);anonymousFingerprint=reply.fingerprints[0];}
    refused(await envelope(anonPair[j],`anon-over-${j}`,'limited'));
    const w=await window(anonPair[j]);ctx.expect(w?.count===20&&Date.now()-w.started<60_000,'per-connection anonymous window');
    ctx.observe(`anonymous-${j}`,{offered:21,accepted:20,refused:1,holder:anonPair[j].holder,connectionId:anonPair[j].connectionId,window:w});
   }
  });
  await ctx.step('real operator records preserve authenticated and anonymous attribution',async()=>{
   const group=fp=>ctx.backend.request('GET',`/dev/errors/groups/${fp}?${new URLSearchParams({project:ctx.project})}`);
   const member=await group(memberFingerprint),anonymous=await group(anonymousFingerprint);
   ctx.expect(member.count===100&&member.latest.project===ctx.project&&member.latest.tenant===ctx.tenants.a&&member.latest.account?.id===ctx.members.alice.accountId,'Member attribution comes from actual auth');
   ctx.expect(pair.some(s=>s.connectionId===member.latest.sessionId),'actual same-holder socket attribution');
   ctx.expect(anonymous.count===40&&!anonymous.latest.account&&!anonymous.latest.context&&anonymous.latest.tenant===''&&anonymous.latest.project===ctx.project,'anonymous privacy and per-connection attribution');
   ctx.expect(anonPair.some(s=>s.connectionId===anonymous.latest.sessionId),'actual anonymous session attribution');
   ctx.observe('groups',{member,anonymous});
  });
 },
};
