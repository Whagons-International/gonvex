import {RuntimeCatalog} from '../runtime';
import {install,clear} from './runtime-catalog';
import {AssemblyTenant} from '../tenant';
export {AssemblyTenant};
export class WrongTenant extends AssemblyTenant {}
let wrongNamespace=false;
export class TestRuntimeCatalog extends RuntimeCatalog {
 async install(project:string,manifest:Record<string,unknown>,generation:number,version:string){install(project,manifest,generation,version);}
 async clear(project:string){clear(project);}
 async fixtureNamespace(wrong:boolean){wrongNamespace=wrong;}
 override async tenantId(name:string){return wrongNamespace?(this.env as unknown as {OTHER_TENANT:DurableObjectNamespace}).OTHER_TENANT.idFromName(name).toString():super.tenantId(name);}
}
export default {fetch:()=>new Response('private local runtime fixture',{status:404})};
