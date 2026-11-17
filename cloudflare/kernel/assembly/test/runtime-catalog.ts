// Controlled baked-catalog changes for local composition tests only. The
// production runtime has immutable generated imports and no mutation API.
import type {BakedRelease} from '../releases';
export const releases:Record<string,BakedRelease>={};
export let version='bootstrap';
export function install(project:string,manifest:Record<string,unknown>,generation:number,next:string){
 const module=manifest.module as {hash:string};
 releases[project]={project,hash:module.hash,generation,manifest,exports:{generation:async()=>({generation})}};
 version=next;
}
export function clear(project:string){delete releases[project];version='bootstrap';}
