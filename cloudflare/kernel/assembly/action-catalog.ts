import {WorkerEntrypoint} from 'cloudflare:workers';
// @ts-expect-error The builder supplies the Action-only catalog of this version.
import * as generated from 'gonvex:action-releases';
/** Private readiness only; never a source of client/tenant authority. */
export class ActionCatalog extends WorkerEntrypoint {
 async ready(version:string):Promise<boolean>{return (generated as {version?:string}).version===version;}
}
