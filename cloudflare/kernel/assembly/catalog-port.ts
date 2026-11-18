/** Private service binding to the stateless, currently serving runtime catalog.
 * Control and operator sockets stay in the stable Worker. No caller-supplied
 * hash/version is accepted as evidence that tenant code is deployed. */
export interface CatalogPort {
  ready(version:string):Promise<boolean>;
  identity():Promise<{version:string|null;worker:WorkerVersionMetadata|null;releases:Record<string,string>}>;
  tenantId(name:string):Promise<string>;
  serving(version:string):Promise<{runtime:boolean;actions:boolean}>;
}
