/** Unknown or previously delivered work cannot be replaced by a later edit. */
export function coalescingAdmission(state: string, knownUnsent: unknown): boolean {
  return state === "pending" && knownUnsent === true;
}
