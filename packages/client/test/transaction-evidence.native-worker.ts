// Test assembly only: actual K10 object (K12 + capture + delivery), original fixture bundle.
import type {ModuleExports} from '../../../cloudflare/kernel/host/src/index.js';
import {ReplicaHarnessTenant,deliveryFetch,type HarnessEnv} from '../../../cloudflare/kernel/delivery/harness/index.ts';
import {archive} from './transaction-evidence.fixture.module.js';
import {bundleFor} from '../../../cloudflare/kernel/reducer/test/fixture.ts';
export {GatewayObject,KernelControlStub,RefTestObject,PublicLimiterObject} from '../../../cloudflare/kernel/reducer/test/worker.ts';
export class TestTenant extends ReplicaHarnessTenant {
  protected bundle(hash:string):ModuleExports|undefined{return hash==='m05-transaction-fixture-v1'?{...bundleFor('k07-fixture-v1'),'m05Synthetic.archive':archive}:bundleFor(hash);}
}
export default {fetch:(request:Request,env:HarnessEnv)=>deliveryFetch(request,env)};
