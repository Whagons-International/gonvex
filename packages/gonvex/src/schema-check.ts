import { execFileSync } from 'node:child_process';
import { projectSchemaInspection } from './local-bindings.js';
import { schemaFindings, type SchemaInspection } from './schema-inspection.js';

export const inspectionFile='gonvex/_generated/schema-inspection.json';
export async function schemaReport(root:string, base=process.env.BASE_SHA??'HEAD') {
  const {inspection}=await projectSchemaInspection(root);
  let previous:SchemaInspection|undefined, revision:string|undefined, coverage:string;
  try{
    revision=execFileSync('git',['rev-parse','--verify','--end-of-options',`${base}^{commit}`],{cwd:root,encoding:'utf8',stdio:['ignore','pipe','ignore']}).trim();
    const data=execFileSync('git',['show',`${revision}:${inspectionFile}`],{cwd:root,encoding:'utf8',maxBuffer:16*1024*1024,stdio:['ignore','pipe','ignore']});
    const parsed=JSON.parse(data) as SchemaInspection;
    if(parsed.version!==1)throw new Error('unsupported inspection version');
    previous=parsed;
    coverage=`SQLite inspection compared with ${revision}`;
  }catch{
    coverage=`No compatible inspection at ${base}; no backlog emitted. Run codegen on the base and commit ${inspectionFile} to enable PR deltas. Migration validity was checked; runtime enforcement and cost were not measured.`;
  }
  return {base:revision??base,coverage,findings:previous?schemaFindings(previous,inspection):[],inspection};
}
const lastReports=new Map<string,string>();
export async function reportSchemaAdvisories(root:string) {
  const report=await schemaReport(root);
  const text=JSON.stringify({base:report.base,coverage:report.coverage,findings:report.findings});
  if(lastReports.get(root)!==text){
    console.warn(`[gonvex schema] ${report.coverage}`);
    for(const finding of report.findings)console.warn(`[gonvex schema] advisory ${finding.code} (${finding.evidence}) ${finding.source?`${finding.source.file}:${finding.source.line}`:'SQLite object'}: ${finding.concern} Change: ${finding.change}. ${finding.suggestion}`);
    lastReports.set(root,text);
  }
  return report.inspection;
}
