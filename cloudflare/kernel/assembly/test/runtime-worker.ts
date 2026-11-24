import {RuntimeCatalog} from '../runtime';
import {install,clear} from './runtime-catalog';
import {AssemblyTenant as ServingTenant} from '../tenant';
/** The serving tenant class, plus a counter of `ping()` calls (the tenant prewarm's wake, #562 review). */
export class AssemblyTenant extends ServingTenant {
 private pings=0;
 override async ping(){ this.pings++; return super.ping(); }
 async fixturePings(){ return this.pings; }
 /** C2: `prewarm()` calls (the page wake), counted apart from pings. */
 private prewarms=0;
 override prewarm(){ this.prewarms++; super.prewarm(); }
 async fixturePrewarms(){ return this.prewarms; }
}
export class WrongTenant extends AssemblyTenant {}
let wrongNamespace=false;
export class TestRuntimeCatalog extends RuntimeCatalog {
 async install(project:string,manifest:Record<string,unknown>,generation:number,version:string){install(project,manifest,generation,version);}
 async clear(project:string){clear(project);}
 async fixtureNamespace(wrong:boolean){wrongNamespace=wrong;}
 override async tenantId(name:string){return wrongNamespace?(this.env as unknown as {OTHER_TENANT:DurableObjectNamespace}).OTHER_TENANT.idFromName(name).toString():super.tenantId(name);}
}
export default {fetch:()=>new Response('private local runtime fixture',{status:404})};
