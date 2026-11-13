import source from './whagons-task.json' with { type: 'json' };
import { ordinaryCommitEvidenceSupport } from './commit-evidence.mjs';

const freeze = value => {
  if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); }
  return value;
};
export const whagonsTaskPreparation = freeze(source);
const roles = Object.keys(source.selection);
const methods = freeze({ create:'reducer', update:'reducer', remove:'reducer', read:'query', collection:'watchReplica' });
const policy = freeze({ offline:'reject', localExecution:'disabled', optimistic:'disabled', freshClients:true, telemetry:'disabled' });
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  && Object.getPrototypeOf(value) === Object.prototype;
const keys = (value, allowed) => record(value) && Object.keys(value).every(key => allowed.includes(key));
const sameFields = (value, expected) => Array.isArray(value) && value.length === expected.length
  && value.every(v => typeof v === 'string') && new Set(value).size === value.length
  && expected.every(v => value.includes(v));

/** Declarations ONLY. Contains no principals, URLs, arguments, callbacks or executable refs. */
export function proposedTaskBinding() {
  return {
    version:1, enabled:false, mode:source.mode,
    applicationCommit:source.application.commit, frameworkCommit:source.framework.commit,
    refs:Object.fromEntries(roles.map(role => {
      const path=source.selection[role], fact=source.functions[path];
      return [role,{path,kind:fact.kind,delivery:fact.delivery}];
    })),
    methods:{...methods}, argumentFields:structuredClone(source.argumentFields), sdkPolicy:{...policy},
    prerequisites:Object.fromEntries(source.missingPrerequisites.map(key => [key,'missing'])),
  };
}

/** Offline plan audit, NOT a binding factory or attestation validator.
 * All outputs are allowlisted constants: never return candidate values, rows, paths or errors.
 * Even perfectly declared prerequisites cannot turn cached source into execution evidence.
 */
export function auditTaskBinding(candidate = proposedTaskBinding()) {
  const issues = new Set();
  const result = integrity => ({
    version:1, status:'BLOCKED', domain:'offline-source-audit', executionAllowed:false,
    applicationCommit:source.application.commit, frameworkCommit:source.framework.commit,
    integrity, sourceAudit:integrity==='valid' && !issues.size ? 'consistent' : 'blocked',
    commitEvidenceSupport:ordinaryCommitEvidenceSupport(),
    issues:[...issues].sort(),
    blockers:['offline-only','sdk-read-delivery',ordinaryCommitEvidenceSupport().reason, ...source.missingPrerequisites.map(key => `unverified:${key}`)],
  });
  try {
    if (!keys(candidate,['version','enabled','mode','applicationCommit','frameworkCommit','refs','methods','argumentFields','sdkPolicy','prerequisites'])
      || !keys(candidate.refs,roles) || !keys(candidate.methods,roles) || !keys(candidate.argumentFields,roles)
      || !keys(candidate.sdkPolicy,Object.keys(policy)) || !keys(candidate.prerequisites,source.missingPrerequisites)) {
      issues.add('input-integrity'); return result('invalid');
    }
    if (candidate.version!==1 || candidate.enabled!==false || candidate.mode!==source.mode) issues.add('disabled-preparation-required');
    if (candidate.applicationCommit!==source.application.commit || candidate.frameworkCommit!==source.framework.commit) issues.add('source-pin-mismatch');
    for (const role of roles) {
      const ref=candidate.refs[role], expected=source.functions[source.selection[role]];
      if (!keys(ref,['path','kind','delivery'])) { issues.add(`ref-shape:${role}`); continue; }
      if (ref.path!==source.selection[role]) issues.add(`function-mismatch:${role}`);
      if (ref.kind!==expected.kind) issues.add(`function-kind:${role}`);
      if (ref.delivery!==expected.delivery) issues.add(`delivery-mismatch:${role}`);
      if (candidate.methods[role]!==methods[role]) issues.add(`sdk-method:${role}`);
      // Argument NAMES only; actual values/credentials/row content are prohibited here.
      if (!sameFields(candidate.argumentFields[role],source.argumentFields[role])) issues.add(`argument-scope:${role}`);
    }
    for (const key of Object.keys(policy)) if (candidate.sdkPolicy[key]!==policy[key]) issues.add(`sdk-policy:${key}`);
    for (const key of source.missingPrerequisites) {
      const state=candidate.prerequisites[key];
      if (!['missing','proposed','configured-unverified'].includes(state)) issues.add(`prerequisite-declaration:${key}`);
      // An external assertion is never accepted as actual app/deployment/durability proof.
    }
    return result('valid');
  } catch {
    issues.clear(); issues.add('input-integrity'); return result('invalid');
  }
}
