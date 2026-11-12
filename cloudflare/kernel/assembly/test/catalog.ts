import { ModuleRegistry } from '../../host/src/index';
import { projectModuleFromManifest, moduleArtifact } from '../../reducer/module';
// @ts-expect-error fixture module gets the real app prelude at build time.
import * as probe from './probe.module.js';
// @ts-expect-error Vite raw fixture data.
import sql from '../../../conformance/fixtures/app/migrations/0001_fixture.sql?raw';
const any = { kind: 'any' };
const entry = (kind: string, handler: string, metadata: Record<string, unknown> = {}) => ({kind,handler,args:any,result:any,...metadata});
export const files = {'migrations/0001_fixture.sql':sql};
export const manifest = { functions: {
 'probe.publicTool':entry('action','publicTool',{public:true,actionProfile:'standard',actionCapabilities:{tools:{write:{kind:'internalReducer',function:'probe.capabilityWrite'}}}}),
 'probe.nestedWrite':entry('action','publicTool',{actionProfile:'standard',actionCapabilities:{tools:{write:{kind:'internalReducer',function:'probe.write'}}}}),
 'probe.capabilityWrite':entry('reducer','capabilityWrite',{internal:true,interactive:false}),
 'probe.write':entry('reducer','write',{internal:true,interactive:false}),
 'probe.service':entry('action','service',{internal:true,actionProfile:'standard'}),
 'probe.serviceAgent':entry('action','service',{internal:true,actionProfile:'agent'}),
 'probe.resultSize':entry('action','resultSize',{internal:true,actionProfile:'standard'}),
 'probe.invite':entry('reducer','invite',{internal:true,interactive:false}),
 'probe.noop':entry('action','noop',{actionProfile:'standard'}),
 },schema:{tables:{},controlPlaneTables:{},tenantTables:{}}, module:{language:'typescript',hash:'assembly-boundary-fixture',generation:1,files:Object.fromEntries(Object.entries(files).map(([path,text])=>[path,btoa(text)]))}};
export const exports = probe as Record<string, unknown>;
export function registry() { const r = new ModuleRegistry('p'); const module=projectModuleFromManifest('p',manifest);r.install(moduleArtifact(module,1,exports));return r; }
