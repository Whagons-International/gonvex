// Private native test assembly. Both generated bundles are materialized by the offline proof builder.
import type {ModuleExports} from '../../../cloudflare/kernel/host/src/index.js';
import {ReplicaHarnessTenant,deliveryFetch,type HarnessEnv} from '../../../cloudflare/kernel/delivery/harness/index.ts';
import {bundleFor} from '../../../cloudflare/kernel/reducer/test/fixture.ts';
import * as v1 from './explicit-server-fixture/gonvex/_build/native.module.js';
import * as v2 from './explicit-server-fixture/gonvex/_build/native-v2.module.js';
import manifests from './explicit-server-fixture/gonvex/_build/native-manifests.json';
export {GatewayObject,KernelControlStub,RefTestObject,PublicLimiterObject} from '../../../cloudflare/kernel/reducer/test/worker.ts';
export class TestTenant extends ReplicaHarnessTenant {
 protected bundle(hash:string):ModuleExports|undefined {return hash===manifests.v1.module.hash?v1:hash===manifests.v2.module.hash?v2:bundleFor(hash);}
}
export default {fetch:(request:Request,env:HarnessEnv)=>deliveryFetch(request,env)};
