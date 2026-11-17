import {WorkerEntrypoint} from "cloudflare:workers";
import type {BakedRelease} from "./releases";
// @ts-expect-error Normal builder alias, with the whole fenced catalog.
import * as generated from "gonvex:releases";
export {AssemblyTenant} from "./tenant";
interface Env {TENANT:DurableObjectNamespace;WORKER_VERSION?:WorkerVersionMetadata;ACTIONS_CATALOG:{ready(version:string):Promise<boolean>}}
/** This Worker exports no client socket owner. Only it adopts app code immediately. */
export class RuntimeCatalog extends WorkerEntrypoint<Env> {
  async ready(version:string):Promise<boolean>{return (generated as {version?:string}).version===version&&await this.env.ACTIONS_CATALOG.ready(version);}
  async identity(){return {version:(generated as {version?:string}).version??null,worker:this.env.WORKER_VERSION??null,
    releases:Object.fromEntries(Object.entries((generated as {releases:Record<string,BakedRelease>}).releases).map(([id,r])=>[id,r.hash]))};}
  async tenantId(name:string):Promise<string>{return this.env.TENANT.idFromName(name).toString();}
}
export default {fetch:()=>new Response("Private runtime service",{status:404})};
