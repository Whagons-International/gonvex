// Controlled registration-RPC fault on the real assembled path. This is a
// synthetic application integration, not platform outage or Rust parity proof.
import { watch, rowsWhen, rawReducer, RAW_CAPABILITIES } from '../../../conformance/scenarios/helpers.mjs';

export default {
  name:'sdk-quiet-initial-failure',tenants:{a:['alice','bob']},
  async run(ctx){
    const {api}=ctx,holder='gw-0';
    let sdk,writer,view,failed,stopFailed;
    const post=(path,body={})=>ctx.backend.request('POST','/__assembly/'+path,{body:{tenant:ctx.tenants.a,...body}});
    const status=()=>ctx.backend.request('GET','/__assembly/bootstrap-gate?holder='+holder);
    await ctx.step('real SDK replica and authoritative one-holder baseline',async()=>{
      const placement=await ctx.backend.inspectHolders(ctx.project);
      ctx.expect(placement.placement==='static','probe requires the declared controller-removed static campaign');
      sdk=await ctx.sdk('quiet-reader',{member:'bob'});view=watch(sdk,api.board.all);
      writer=await ctx.raw('writer');const token=(await ctx.rawLogin(writer,'alice')).accessToken;
      await writer.auth({project:ctx.project,tenant:ctx.tenants.a,token,capabilities:RAW_CAPABILITIES});
      ctx.expect((await rawReducer(writer,'board.add',{id:'seed',title:'before',position:1})).result?.id==='seed','seed commit');
      await rowsWhen(ctx,view,rows=>rows.length===1,'SDK initial replica');
      const actual=await ctx.backend.inspectHolders(ctx.project);
      ctx.expect(actual.holders.length===1&&actual.holders[0].holder===holder,'actual holder must be gw-0');
      ctx.observe('placement',actual);
    });
    await ctx.step('quiet initial gets genuine correlated error; unchanged SDK write and durable receipt remain live',async()=>{
      ctx.expect(sdk&&writer&&view,'setup must succeed');
      await ctx.backend.restartTenant(ctx.project,ctx.tenants.a);
      await post('recovery-fail-once');
      await post('bootstrap-gate',{holder,hold:true});
      try{
        ctx.expect((await rawReducer(writer,'board.bump',{id:'seed'})).result?.bumps===1,'trigger commit is real');
        await ctx.until(async()=> (await status()).entered>0,'actual empty bootstrap RPC held');
        failed=sdk.watchLiveQuery(api.board.page,{offset:0,limit:3});
        stopFailed=failed.onUpdate(()=>{});
        await ctx.until(async()=> (await status()).pendingInitials>0,'originating initial request retained');
        ctx.observe('held',await status());
      }finally{await post('bootstrap-gate',{holder,hold:false});}
      let actualError;
      await ctx.until(()=>{
        try{failed.localLiveQueryResult();return false;}catch(error){actualError=error;return true;}
      },'unchanged SDK observes terminal subscription error');
      ctx.expect(actualError?.code==='server'&&String(actualError.message).includes('delivery recovery failed'),'real SDK server error, not a raw-frame-only check');
      const errors=ctx.frames('quiet-reader',{dir:'in',type:'query.error'});
      ctx.expect(errors.length===1,'one genuine correlated query.error');
      ctx.observe('terminalError',{message:actualError.message,code:actualError.code,frames:errors});
      // The unchanged SDK writes a durable intent; assert one actual invocation
      // and inspect its receipt so retries cannot mask a lost revision barrier.
      const before=ctx.frames('quiet-reader',{dir:'out',type:'reducer.call'}).length;
      const at=Date.now();
      const result=await sdk.reducer({...api.board.bump,localExecution:undefined},{id:'seed'},
        {offline:'reject',timeoutMs:15000,optimistic:[{entity:'board',rowId:'seed',op:'update',fields:{bumps:2}}]});
      ctx.expect(result.bumps===2,'awaited SDK write completes after its real commit watermark');
      const calls=ctx.frames('quiet-reader',{dir:'out',type:'reducer.call'}).slice(before);
      ctx.expect(calls.length===1&&calls[0].idempotencyKey,'exactly one durable SDK invocation');
      const receipt={result:await sdk.query({kind:'query',path:'__gonvex.reducerReceipt',scope:'tenant'},
        {idempotencyKey:calls[0].idempotencyKey,receiptPath:'board.bump'})};
      ctx.expect(receipt.result?.found===true&&Number.isSafeInteger(receipt.result?.cursor?.revision),'actual durable receipt exists');
      ctx.expect((await sdk.listIntents()).length===0,'durable SDK intent retires after revision confirmation');
      await rowsWhen(ctx,view,rows=>rows[0]?.bumps===2,'same SDK replica catches up');
      stopFailed();
      const fresh=sdk.watchLiveQuery(api.board.page,{offset:0,limit:2}),stop=fresh.onUpdate(()=>{});
      try{await ctx.until(()=>fresh.localLiveQueryResult()?.[0]?.bumps===2,'fresh authorized initial succeeds');}
      finally{stop();}
      ctx.observe('write',{result,elapsedMs:Date.now()-at,receipt,calls});
    });
    await ctx.step('repeated fresh-event recovery clears only real terminal facts without pending-reference growth',async()=>{
      const state=()=>ctx.backend.request('GET','/__assembly/delivery-state?'+new URLSearchParams({tenant:ctx.tenants.a}));
      for(let cycle=0;cycle<2;cycle++){
        await ctx.backend.restartTenant(ctx.project,ctx.tenants.a);
        await post('recovery-fail-once');await post('bootstrap-gate',{holder,hold:true});
        let refused,stopRefused,current,stopCurrent;
        try{
          const trigger=await rawReducer(writer,'board.bump',{id:'seed'});
          ctx.expect(trigger.result?.bumps===3+cycle*2,'real restart-triggering commit');
          await ctx.until(async()=> (await status()).entered>0,'actual bootstrap held again');
          refused=sdk.watchLiveQuery(api.board.page,{offset:10+cycle,limit:3});stopRefused=refused.onUpdate(()=>{});
          await ctx.until(async()=> (await status()).pendingInitials>0,'new exact pending request');
          // The pending subscription already earned one owed external event.
          // Arm its next challenge atomically before releasing the failing
          // bootstrap, rather than assuming there will be no coalesced retry.
          await post('bootstrap-gate',{holder,hold:true,phase:'challenge'});
          await ctx.until(()=>{try{refused.localLiveQueryResult();return false;}catch(error){return error.code==='server';}},'real SDK terminal error before further traffic');
          await ctx.until(async()=> (await status()).entered>0,'real owed-event challenge held');
          current=sdk.watchLiveQuery(api.board.page,{offset:0,limit:4+cycle});stopCurrent=current.onUpdate(()=>{});
          await ctx.until(async()=> (await status()).pendingInitials>0,'exact current request before offer capture');
          await ctx.until(async()=> (await state()).pendingInitials>0,'engine current request before offer capture');
          const before=await state();ctx.expect(before.pendingInitials>0,'engine really retains the originating ref');
          ctx.observe('pending-'+cycle,before);
        }finally{await post('bootstrap-gate',{holder,hold:false});}
        try{
          await ctx.until(()=>current.localLiveQueryResult()?.[0]?.bumps===3+cycle*2,'real rebuilt initial accepted by SDK');
          await ctx.until(async()=>{const after=await state();return after.pendingInitials===0&&after.recoveryInitialsCleared>0;},'exact processing facts clear engine correlations');
          const after=await state();ctx.expect(after.recoveryInitialFactsRejected===0,'no malformed subset accepted');
          ctx.observe('reconciled-'+cycle,after);
          const value=await sdk.reducer({...api.board.bump,localExecution:undefined},{id:'seed'},{offline:'reject',timeoutMs:15000});
          ctx.expect(value.bumps===4+cycle*2,'same SDK revision barrier still progresses after reconciliation');
          await rowsWhen(ctx,view,rows=>rows[0]?.bumps===4+cycle*2,'replica after repeated recovery');
        }finally{stopRefused?.();stopCurrent?.();}
      }
    });
    stopFailed?.();view?.stop();
  },
};
