// Controlled synthetic native fixture only; not a Whagons application module.
import {reducer,replicaCollection,schema,visibility} from '@gonvex/module-sdk';
export const boardVisibility=visibility({table:'board',key:'id',sets:{},where:{operator:'public'}});
export const all=replicaCollection({args:schema.object({}),result:schema.array(schema.any()),replica:{table:'board',key:'id',columns:['id','title','archived'],mode:'eager',maxRows:10,maxBytes:16384}});
export const add=reducer({args:schema.object({id:schema.string(),title:schema.string()}),result:schema.any(),run:({db},args)=>db.insert('board',{...args,archived:0})});
export const rename=reducer({args:schema.object({id:schema.string(),title:schema.string()}),result:schema.any(),run:({db},args)=>db.update('board',args.id,{title:args.title})});
export const archive=reducer({args:schema.object({id:schema.string()}),result:schema.any(),run:({db},args)=>db.update('board',args.id,{archived:1})});
export const noop=reducer({args:schema.object({}),result:schema.string(),run:()=> 'unchanged'});
