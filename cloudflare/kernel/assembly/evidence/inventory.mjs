// Trace references, without treating documentation mentions as passing receipts.
import {execFileSync} from 'node:child_process';
import {readFile,writeFile} from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
const directory=dirname(fileURLToPath(import.meta.url)),root=resolve(directory,'../../../..');
const files=execFileSync('git',['ls-files','cloudflare','docs/cloudflare'],{cwd:root,encoding:'utf8'}).trim().split('\n');
const matrix=JSON.parse(await readFile(join(directory,'scenarios.json'),'utf8'));
const documents=[];
for(const path of files.filter(p=>p.endsWith('.md')&&!p.includes('K22-evidence')))documents.push({path,text:await readFile(join(root,path),'utf8')});
for(const scenario of matrix.scenarios){
 scenario.sourceSha256=execFileSync('sha256sum',[join(root,scenario.source)],{encoding:'utf8'}).split(' ')[0];
 scenario.packageReferences=[];
 for(const doc of documents){
  const lines=doc.text.split('\n');
  for(let i=0;i<lines.length;i++)if(lines[i].includes(scenario.scenario))scenario.packageReferences.push({path:doc.path,line:i+1,excerpt:lines[i].trim()});
 }
 scenario.evidenceClassification='Formal real fixture-module/SDK/raw-socket scenario through backend HTTP+WS; documentation references are attribution, not proof of a passing prior run';
 scenario.remote='NOT RUN: actual single assembled development deployment required';
}
await writeFile(join(directory,'scenarios.json'),JSON.stringify(matrix,null,2)+'\n');
const comparisons=[];
for(const path of files.filter(p=>p.endsWith('.json')&&/golden\//.test(p))){
 const value=JSON.parse(await readFile(join(root,path),'utf8'));
 comparisons.push({source:path,sha256:execFileSync('sha256sum',[join(root,path)],{encoding:'utf8'}).split(' ')[0],sections:Object.fromEntries(Object.entries(value).filter(([,v])=>Array.isArray(v)||v&&typeof v==='object').map(([k,v])=>[k,Array.isArray(v)?v.length:Object.keys(v).length])),classification:path.endsWith('fixture-functions.json')?'Fixture function definitions; not a Rust comparison result':'Package captured Rust golden/replay or vectors; not assembled deployment proof',assembledStatus:'Mapping individual golden cases to assembled transcript/assertions remains OPEN'});
}
const gaps=[
 {package:'K02',sources:['cloudflare/kernel/migrate/test/'],gap:'Real assembled migration interruption/fault cases beyond the deploy scenario require explicit case mapping and private-platform proof'},
 {package:'K11',sources:['cloudflare/kernel/scheduler/test/golden/rust-vectors.json'],gap:'Formal scheduler scenario covers runAfter/runAt/wakeAt/32 hops; exact cron/retry/stagger vector cases require assembled execution, not a package-golden substitution'},
 {package:'K12',sources:['cloudflare/actions/test/scenarios.test.ts','cloudflare/actions/test/golden/actions-rust.json'],gap:'Formal actions scenario covers its 10 golden steps. Package 12 crash/timeout/cancel/zombie/lease cases require actual assembled Workflow scenarios, distinguishing platform-kill stand-ins'},
 {package:'K13',sources:['cloudflare/kernel/files/test/','cloudflare/kernel/files/README.md'],gap:'26 package cases use real local R2 but no formal Rust/assembled HTTP+Action file matrix exists; implement owner/refusal/upload/download/store/finalize/limits through actual serving handlers'},
 {package:'K17',sources:['cloudflare/kernel/principals/verify/gateway-contract.mjs'],gap:'Real Whagons committed-release gateway consumer probe must target the final assembled handler, with source provenance and unchanged assertions'},
 {package:'K18',sources:['cloudflare/router/ingress/test/'],gap:'Actual assembled ingress service-grant/routing/refusal probes need mapping; unit-route success does not certify deployed composition'},
 {package:'K20',sources:['cloudflare/sandbox/test/rust.test.ts','cloudflare/sandbox/test/whagons.ts','cloudflare/sandbox/README.md'],gap:'Seven Whagons/Rust+DuckDB scenarios and legal limits must run via real assembled Actions ctx.sandbox, Container and K13 owners. Current package protocol-stub skips are not proof'},
 {package:'K21',sources:['cloudflare/bridge-support/README.md','cloudflare/conformance/scenarios/bridge-upstream.mjs'],gap:'Part1 upstream formal scenario only; real app/device bridge remains V02 and is not waived'},
];
await writeFile(join(directory,'package-comparisons.json'),JSON.stringify({status:'INCOMPLETE: explicit missing assembled cases must be implemented; none waived',base:matrix.base,comparisons,gaps},null,2)+'\n');
console.log(`indexed ${matrix.scenarios.length} formal scenarios, ${comparisons.length} package golden/vector sources, ${gaps.length} explicit additional package coverage gaps`);
