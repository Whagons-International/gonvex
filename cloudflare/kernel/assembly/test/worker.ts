import { AssemblyGateway } from '../gateway';
import { AssemblyTenant } from '../tenant';
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
export class CompositionGateway extends AssemblyGateway {
 async operatorDataChanged() {}
}
export default {fetch:()=>new Response('K22 synthetic composition fixture; actual tenant/Action handlers')};
