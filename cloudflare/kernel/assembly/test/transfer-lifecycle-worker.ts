// Node/Miniflare maintenance regression only. Production Tenant and Control
// lifecycle, synthetic catalog/rows, and no provider operation or WebSocket.
import {AssemblyTenant as ServingTenant} from '../tenant';
import {TransitionControl} from '../transition-control';
import {MODULE_KEY} from '../../reducer/tenant';
import {buildReleasePlan,migrationsFromFiles,R2PlanStore} from '../../migrate';

export class AssemblyTenant extends ServingTenant {
 override async fetch(request:Request){
  const action=new URL(request.url).pathname;
  if(action==='/seed'){
   const {project,manifest}=await request.json() as {project:string;manifest:Record<string,unknown>};
   const plan=await buildReleasePlan(await migrationsFromFiles({}));
   await new R2PlanStore(this.env.RELEASES).write(plan);
   await this.activate(project,manifest,plan.id);this.ctx.storage.kv.put('assembly/project',project);
   this.ctx.storage.sql.exec('CREATE TABLE fixture_retained (id TEXT PRIMARY KEY, value TEXT NOT NULL)');
   this.ctx.storage.sql.exec("INSERT INTO fixture_retained VALUES ('original','preserved')");
  }
  if(action==='/call'){
   const module=this.kernel.module!;
   return Response.json(await this.serviceCall({principalId:'fixture',route:{projectId:module.projectId,tenantId:this.ctx.id.name!,databaseUrl:'do://fixture'},functions:['probe.generation'],maxDelegationSeconds:0,channel:'system'},
    {kind:'query',id:'fixture-query',path:'probe.generation',args:{}}));
  }
  if(action==='/activate'||action==='/plan'||action==='/provision'||action==='/ack'){
   const {project,manifest,publicationGeneration}=await request.json() as {project:string;manifest:Record<string,unknown>;publicationGeneration?:number};
   const module=manifest.module as {hash:string;generation:number};
   try{
    if(action==='/activate')await this.activate(project,manifest,null);
    if(action==='/plan')await this.operatorApplyRelease('not-a-plan',project);
    if(action==='/provision')await this.provision(project,this.ctx.id.name!);
    if(action==='/ack')return Response.json({ack:await this.operatorActivateRelease({projectId:project,hash:module.hash,generation:publicationGeneration!})});
   }catch{return new Response('maintenance refused release mutation',{status:409});}
  }
  return Response.json({stored:this.ctx.storage.kv.get(MODULE_KEY),hash:this.kernel.module?.artifactHash,generation:this.kernel.module?.generation,
   rows:this.ctx.storage.sql.exec('SELECT * FROM fixture_retained ORDER BY id').toArray()});
 }
}
export class Control extends TransitionControl {
 override async fetch(request:Request){
  const action=new URL(request.url).searchParams.get('action');
  if(action==='seed'){
   const {project,hash,generation}=await request.json() as {project:string;hash:string;generation:number};
   await this.devCreateProject({id:project,name:'Maintenance fixture'});
   this.sql.run("INSERT INTO gonvex_cf_active_releases(project_id,hash,generation,plan_id) VALUES($1,$2,$3,'')",project,hash,generation);
   return Response.json({ok:true});
  }
  return Response.json(await this.runtimeTopology(action==='freeze'?'freeze':'status'));
 }
}
export default {async fetch(request:Request,env:any){
 const url=new URL(request.url),control=url.pathname==='/control';
 const target=control?env.CONTROL.getByName('control'):env.TENANT.getByName('original');
 try{return await target.fetch(request);}catch(error){return new Response(String((error as Error).message).slice(0,512),{status:503});}
}};
