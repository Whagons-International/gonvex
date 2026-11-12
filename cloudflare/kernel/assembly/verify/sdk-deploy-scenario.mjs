// 0060 supplemental SDK deployment semantics. Original raw continuity scenarios
// stay unchanged/failed. Transport traces are retained, never normalized into
// same-socket continuity or claimed as an exact transcript comparison.
import { watch, rowsWhen, project, rawReducer, rawQuery, RAW_CAPABILITIES } from '../../../conformance/scenarios/helpers.mjs';

export default {
  name: 'sdk-deploy-recovery', tenants: { a: ['alice', 'bob'], b: ['alice'] },
  async run(ctx) {
    const { api } = ctx, api2 = ctx.fixtures[2].api;
    let tenant, owner, control, board, notes, live, oldHash, raw, token;
    const key = 'k22-sdk-deploy-bump-0001', closes = [], updates = [];
    const observeSocket = (client, tag) => {
      const seen = new WeakSet();
      const attach = () => {
        const ws = client.socket;
        if (!ws || seen.has(ws)) return; seen.add(ws);
        ws.addEventListener('close', e => closes.push({tag,code:e.code,reason:e.reason,at:Date.now()}));
      };
      const connect = client.connect.bind(client);
      client.connect = (...args) => { const value = connect(...args); attach(); return value; };
      attach();
    };
    const first = async (label, operation) => {
      const at = Date.now();
      try { const result = await operation(); ctx.observe(label, {result,elapsedMs:Date.now()-at}); return result; }
      catch(error) { ctx.observe(label, {error:error.message,code:error.code,elapsedMs:Date.now()-at}); throw error; }
    };
    const eq = (actual, expected, label) => ctx.expect(JSON.stringify(actual) === JSON.stringify(expected), `${label}: ${JSON.stringify(actual)}`);
    await ctx.step('SDK tenant/control views and a durable v1 receipt', async () => {
      owner = await ctx.sdk('owner', {member:'alice'});
      tenant = await ctx.sdk('tenant', {member:'bob', options:{onUpdateRequired:reason=>updates.push(reason)}});
      control = await ctx.sdk('control', {member:'bob', authenticate:false, options:{role:'control'}});
      await control.authenticate({project:ctx.project,token:control.session.accessToken});
      for (const [tag, client] of [['owner',owner],['tenant',tenant],['control',control]]) observeSocket(client,tag);
      board = watch(tenant, api.board.all); notes = watch(tenant, api.notes.mine);
      live = tenant.watchLiveQuery(api.board.page, {offset:0,limit:3}); live.onUpdate(()=>{});
      eq(await first('SDK add',()=>owner.reducer(api.board.add,{id:'old',title:'before',position:1},{offline:'reject'})),{id:'old',title:'before',position:1,bumps:0},'v1 row');
      await owner.reducer(api.notes.write,{id:'private',body:'alice only'},{offline:'reject'});
      await rowsWhen(ctx,notes,r=>r.length===0,'private note hidden');
      raw = await ctx.raw('receipt-v1'); token = (await ctx.rawLogin(raw,'alice')).accessToken;
      oldHash = (await raw.auth({project:ctx.project,tenant:ctx.tenants.a,token,capabilities:RAW_CAPABILITIES})).artifactHash;
      eq((await rawReducer(raw,'board.bump',{id:'old'},{idempotencyKey:key,receiptPath:'board.bump'})).result,{bumps:1},'keyed v1 result');
      await rowsWhen(ctx,board,r=>r[0]?.bumps===1,'v1 replica catches up');
      await ctx.until(()=>live.localLiveQueryResult()?.[0]?.bumps===1,'v1 live result');
    });
    await ctx.step('actual generation 2 deploy and unchanged SDK recovery', async () => {
      const at = Date.now(); ctx.observe('deploy', await ctx.deploy(2));
      await ctx.until(()=>['owner','tenant'].every(tag=>ctx.frames(tag,{dir:'in'}).some(f=>
        f.type==='system.reload' || f.type==='auth.result' && (f.artifactHash===ctx.fixtures[2].hash || f.result?.artifactHash===ctx.fixtures[2].hash))), 'SDK sees real generation 2');
      // No mutation retry: a failed first post-deployment call fails this probe.
      eq(await first('generation',()=>tenant.reducer(api2.board.generation,{}, {offline:'reject'})),{generation:2},'generation 2');
      await first('new row',()=>owner.reducer(api2.board.add,{id:'new',title:'after',position:2},{offline:'reject'}));
      const fresh = await ctx.sdk('v2',{member:'bob'}), view2 = watch(fresh,api2.board.all);
      const rows = await rowsWhen(ctx,view2,r=>r.length===2,'v2 replica');
      eq(project(rows,['id','title','position','bumps','label']),[
        {id:'new',title:'after',position:2,bumps:0,label:'v2'}, {id:'old',title:'before',position:1,bumps:1,label:'v1'},
      ],'migration default and new schema');
      await rowsWhen(ctx,board,r=>r.length===2,'v1 definition survives');
      await ctx.until(()=>live.localLiveQueryResult()?.length===2,'live query rebuilt');
      ctx.expect(notes.rows().length===0,'deployment must not reveal private note');
      ctx.observe('recovery', {elapsedMs:Date.now()-at,closes:[...closes],connections:tenant.connectionState().connectionCount});
    });
    await ctx.step('stale artifact and original durable receipt remain exact', async () => {
      const current = await ctx.raw('receipt-v2');
      await current.auth({project:ctx.project,tenant:ctx.tenants.a,token,capabilities:RAW_CAPABILITIES});
      const stale = await rawReducer(current,'board.rename',{id:'old',title:'forbidden'},{artifactHash:oldHash});
      ctx.expect(stale.class==='update_required' && stale.retryable===false,'stale artifact is refused');
      ctx.observe('stale',stale);
      const receipt = await rawQuery(current,'__gonvex.reducerReceipt',{idempotencyKey:key,receiptPath:'board.bump'});
      ctx.observe('receipt',receipt);
      ctx.expect(receipt.result?.found===true && Number.isSafeInteger(receipt.result?.cursor?.revision),'v1 receipt cursor remains durable');
      eq((await rawReducer(current,'board.bump',{id:'old'},{idempotencyKey:key,receiptPath:'board.bump'})).result,{bumps:1},'receipt replay');
      await rowsWhen(ctx,board,r=>r.find(x=>x.id==='old')?.bumps===1,'no duplicate mutation');
      await tenant.authenticate({project:ctx.project,tenant:ctx.tenants.a,token:owner.session.accessToken});
      await rowsWhen(ctx,notes,r=>r.length===1 && r[0].body==='alice only','explicit new auth sees owner note');
      await tenant.authenticate({project:ctx.project,tenant:ctx.tenants.a,token:tenant.session.accessToken});
      await rowsWhen(ctx,notes,r=>r.length===0,'reauth clears owner private rows');
      const listed = await first('control query',()=>control.query(ctx.control.tenants.mine,{}));
      ctx.expect(Array.isArray(listed),'control-only SDK recovers actual control query');
    });
    await ctx.step('actual client-contract rollout gates tenant SDK and retains control role', async () => {
      ctx.observe('deploy',await ctx.deploy(3));
      await ctx.until(()=>updates.length>0,'tenant SDK update-required callback');
      ctx.expect(ctx.frames('tenant',{dir:'in',type:'client.updateRequired'}).some(f=>f.contract===2),'actual contract v2 frame');
      await first('control after contract change',()=>control.query(ctx.control.tenants.mine,{}));
      ctx.expect(ctx.frames('control',{dir:'in',type:'client.updateRequired'}).length===0,'control role is not contract gated');
      ctx.observe('updates',updates); ctx.observe('closes',closes);
    });
  },
};
