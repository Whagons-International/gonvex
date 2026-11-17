import { ControlObject } from '../../../control/src/control-object';
import { AssemblyGateway } from '../gateway';
import { AssemblyTenant } from '../tenant';
import { SessionControlObject } from '../control';
import { MemoryPlanStore } from '../../migrate';
import { configureActions } from '../../../actions/src/worker';
import { exports, registry } from './catalog';
export { ActionWorker, ActionWorkflow } from '../../../actions/src/worker';
configureActions({registry});
export const plans = new MemoryPlanStore();
export class CompositionTenant extends AssemblyTenant {
 protected override bundle() { return exports; }
 protected override plans() { return plans; }
}
export class CompositionControl extends ControlObject {}
function publicationEnv(env:any){return {...env,GONVEX_DASHBOARD_SESSION_SECRET:'synthetic-publication-secret',GONVEX_ASSEMBLY_FIXTURE_KEY:'synthetic-inspection',CONTROL:env.PUB_CONTROL,TENANT:env.PUB_TENANT,GATEWAY:env.PUB_GATEWAY};}
export class PublicationControl extends SessionControlObject { constructor(ctx:DurableObjectState,env:any){super(ctx,publicationEnv(env));} }
export class PublicationTenant extends AssemblyTenant {
 constructor(ctx:DurableObjectState,env:any){super(ctx,publicationEnv(env));}
 protected override plans(){return plans;}
}
export class PublicationGateway extends AssemblyGateway { constructor(ctx:DurableObjectState,env:any){super(ctx,publicationEnv(env));} }
export class CompositionGateway extends AssemblyGateway {
 async operatorDataChanged() {}
}
export default {fetch:()=>new Response('K22 synthetic composition fixture; actual tenant/Action handlers')};
