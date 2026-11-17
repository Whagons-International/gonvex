// Supplemental real-handler probes, separate from the unchanged formal matrix.
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { parseArgs } from 'node:util';
import { buildFixture } from '../../../conformance/src/fixture.mjs';
import { backendClass } from '../../../conformance/src/backends/index.mjs';
import { runScenario } from '../../../conformance/src/runner.mjs';
import { diffChecksums } from '../../../conformance/src/differ.mjs';
import sdkDeploy from './sdk-deploy-scenario.mjs';
import telemetryDomains from './telemetry-domains-scenario.mjs';
import quietInitial from './quiet-initial-scenario.mjs';
const {values} = parseArgs({options:{work:{type:'string'},backend:{type:'string',multiple:true},probe:{type:'string'}}});
if(!values.work) throw new Error('owned --work directory required');
const work = resolve(values.work); await mkdir(work,{recursive:true,mode:0o700});
const fixtures = {1:await buildFixture(work,1),2:await buildFixture(work,2),3:await buildFixture(work,3)};
const probes={'telemetry-domains':telemetryDomains,'sdk-deploy':sdkDeploy,'quiet-initial':quietInitial};
const scenario=probes[values.probe??'sdk-deploy'];
if(!scenario) throw new Error('unknown supplemental probe');
const backends=[];
for(const kind of values.backend??(values.probe&&values.probe!=='sdk-deploy'?['cloudflare-assembly']:['rust','cloudflare-assembly'])) {
 const Class = await backendClass(kind), directory=join(work,kind); await mkdir(directory,{recursive:true,mode:0o700});
 backends.push(new Class({name:kind,workDir:directory}));
}
const report={classification:'Supplemental actual-handler semantic regression; transport transcripts retained and NOT compared or normalized to formal parity',probe:scenario.name,fixtures:Object.fromEntries(Object.entries(fixtures).map(([k,v])=>[k,v.hash])),traces:[],checksumDiffs:[]};
try {
 await Promise.all(backends.map(b=>b.start()));
 report.traces=await Promise.all(backends.map(backend=>runScenario({scenario,backend,fixtures,verbose:true})));
 if(report.traces.length===2) report.checksumDiffs=diffChecksums(report.traces[0].checksums,report.traces[1].checksums);
} finally {
 await Promise.allSettled(backends.map(b=>b.stop()));
 await writeFile(join(work,'report.json'),JSON.stringify(report,null,2),{mode:0o600});
}
for(const trace of report.traces) console.log(`${trace.backend}: ${trace.failures.length} failures, ${trace.digestsChecked} digests checked\n${trace.failures.join('\n')}`);
console.log(`checksum differences: ${report.checksumDiffs.length}`);
process.exitCode=report.traces.some(t=>t.failures.length)||report.checksumDiffs.length?1:0;
