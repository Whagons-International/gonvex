// Source assessment only: ordinary reducer.result + existing receipt lookup.
// No serving imports, SDK implementation, evidence parser, callbacks or execution.
const support = Object.freeze({
  status:'unsupported',
  reason:'command-bound-commit-cursor-unavailable',
  executionAllowed:false,
  frameworkCommit:'5c5fa3795d821d5995179e19b712c89bfa0eb8d8',
  scope:'general-reducerWithCommitEvidence-from-ordinary-result-and-receipt-lookup',
});

/** Declarations, capabilities and caller-supplied observations cannot upgrade this
 * source-pinned assessment. A separately guaranteed matching transaction/full cursor
 * design is outside this scope; existing atomic receipt recovery stays unchanged.
 */
export function ordinaryCommitEvidenceSupport() { return support; }
