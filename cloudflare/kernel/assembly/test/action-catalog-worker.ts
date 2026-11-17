import {ActionCatalog} from '../action-catalog';
import {install} from './runtime-catalog';
export class TestActionCatalog extends ActionCatalog {
 async select(version:string){install('action-fixture',{module:{hash:'fixture'}},1,version);}
}
export default {fetch:()=>new Response('private local Actions catalog fixture',{status:404})};
