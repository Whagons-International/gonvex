import {WorkerEntrypoint} from "cloudflare:workers";
import type {BakedRelease} from "./releases";
// @ts-expect-error Normal builder alias, with the whole fenced catalog.
import * as generated from "gonvex:releases";
import {AssemblyTenant as Tenant} from "./tenant";
import {resident,residentHandler} from "./residency";
/** Data residency: restricted to GONVEX_JURISDICTION when the stack has one (residency.ts). */
export const AssemblyTenant = resident(Tenant);
export type AssemblyTenant = Tenant;
interface Env {TENANT:DurableObjectNamespace;WORKER_VERSION?:WorkerVersionMetadata;ACTIONS_CATALOG:{ready(version:string):Promise<boolean>}}
/** This Worker exports no client socket owner. Only it adopts app code immediately. */
class Catalog extends WorkerEntrypoint<Env> {
  async ready(version:string):Promise<boolean>{return (generated as {version?:string}).version===version&&await this.env.ACTIONS_CATALOG.ready(version);}
  async identity(){return {version:(generated as {version?:string}).version??null,worker:this.env.WORKER_VERSION??null,
    releases:Object.fromEntries(Object.entries((generated as {releases:Record<string,BakedRelease>}).releases).map(([id,r])=>[id,r.hash]))};}
  /** Per component, so Control can tell that no target of a publication is serving. */
  async serving(version:string):Promise<{runtime:boolean;actions:boolean}>{return {runtime:(generated as {version?:string}).version===version,actions:await this.env.ACTIONS_CATALOG.ready(version)};}
  async tenantId(name:string):Promise<string>{return this.env.TENANT.idFromName(name).toString();}
}
export const RuntimeCatalog = resident(Catalog);
export type RuntimeCatalog = Catalog;
export default residentHandler({fetch:()=>new Response("Private runtime service",{status:404})});
