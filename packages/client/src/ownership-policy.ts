/** Primitive scope/activation identities are captured before an operation yields.
 * Callers allocate a fresh Symbol for each owner activation and check
 * again after every await preceding live publication. */
export function ownsCurrentScope(
  capturedScope: string,
  currentScope: string,
  capturedGeneration: symbol,
  currentGeneration: symbol,
  closed: boolean,
): boolean {
  return !closed && capturedScope === currentScope && capturedGeneration === currentGeneration;
}
