// Private local fixture entry. Production Control/Gateway implementation, with
// setup methods for the synthetic release rows normally written by K19's builder.
import {SessionControlObject} from '../control';
import type {ReleaseSummary} from '../../../control/operator/release';
import {AssemblyGateway} from '../gateway';
export {ActionWorker,ActionWorkflow} from '../../../actions/src/worker';
export {ActionHost} from '../../../actions/src/host';
import {configureActions} from '../../../actions/src/worker';
import {X13_PROJECT,x13Registry} from './x13-fixture';
// X13: the Action Worker serves the X13 fixture app (its other fixtures run no Actions here).
configureActions({registry:(_env,projectId)=>{if(projectId!==X13_PROJECT)throw new Error(`project ${JSON.stringify(projectId)} has no active TypeScript module`);return x13Registry();}});
export class TestControl extends SessionControlObject {
 /** Prewarm fixture: a small per-tenant throttle bound, so a test can reach it. */
 protected override prewarmMax = 3;
 /** X15 test seam: the stored GATEWAY watchers of a project. */
 /** X17 seam: the watchers a support-scoped change (presence, error sessions) notifies. */
 async fixtureSupportWatchers(project:string){ return (this as unknown as {supportWatchers(p:string):Array<{binding:string;name:string}>}).supportWatchers(project); }
 async fixtureWatchers(project:string){ return this.ctx.storage.kv.get<Array<{binding:string;name:string}>>(`control/watchers/${project}`)??[]; }
 async fixtureTenant(project:string,tenant:string){
  this.sql.run("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_name,database_url,status,description,provisioned,runtime_created) VALUES ($1,$2,$1,'Fixture',$1,$3,'active','synthetic',1,1)",tenant,project,'do://tenant/'+tenant);
 }
 /** Prewarm fixture: a tenant's domain and status. */
 /** Prewarm fixture: how many tenants the per-tenant throttle remembers (its bound). */
 async fixturePrewarmedSize(){ return (this as unknown as {prewarmed:Map<string,number>}).prewarmed.size; }
 async fixtureTenantRow(tenant:string,domain:string,status='active',databaseUrl?:string){ this.sql.run("UPDATE gonvex_runtime_tenants SET domain=$2,status=$3,database_url=COALESCE($4,database_url) WHERE tenant_id=$1",tenant,domain,status,databaseUrl??null); }
 /** Co-tenant prewarm fixture: what an exchange reply records (`noteCoTenants`). */
 async fixtureNoteCoTenants(result:unknown){ this.noteCoTenants(result); }
 async fixturePublication(id:string,summary:ReleaseSummary,version:string,tenants:string[],publicationGeneration:number){
  const owner=this.syncs.deployments.acquire(summary.project,id);
  if(!owner.acquired)throw new Error('publication fixture could not acquire deployment');
  const catalog=this.syncs.activeReleases().filter(r=>r.project!==summary.project);
  catalog.push({project:summary.project,hash:summary.hash,generation:publicationGeneration});
  this.syncs.deployments.catalog(id,owner.fence,catalog);
  this.syncs.deployments.staged(id,owner.fence,version);
  this.sql.run("INSERT INTO gonvex_cf_releases(project_id,hash,summary) VALUES($1,$2,$3)",summary.project,summary.hash,JSON.stringify(summary));
  this.sql.run("INSERT INTO gonvex_cf_publications(sync_id,project_id,hash,generation,version,tenants) VALUES($1,$2,$3,$4,$5,$6)",id,summary.project,summary.hash,publicationGeneration,version,JSON.stringify(tenants));
  return owner.fence;
 }
 async fixtureRestart():Promise<void>{this.ctx.abort('local fixture Control reconstruction');}
}
export class TestGateway extends AssemblyGateway {
 private held:Promise<void>|undefined;
 private release:(()=>void)|undefined;
 private pending=0;
 private failOnce=false;
 async fixtureReloadFailure(){this.failOnce=true;}
 async fixtureReload(hold?:boolean){
  if(hold===true&&!this.held)this.held=new Promise(resolve=>{this.release=resolve;});
  if(hold===false){this.release?.();this.held=undefined;this.release=undefined;}
  return {pending:this.pending};
 }
 override async moduleReloaded(...args:Parameters<AssemblyGateway['moduleReloaded']>){
  this.pending++;try{await this.held;if(this.failOnce){this.failOnce=false;throw new Error('synthetic notification failure');}return await super.moduleReloaded(...args);}finally{this.pending--;}
 }
}
export default {fetch:()=>new Response('private local stable-worker fixture',{status:404})};
