/** A valid query exceeded its bounded execution resources. Callers can choose
 * their existing compatibility path without hiding storage or protocol errors. */
export class LocalQueryBudgetError extends Error {
  constructor(readonly reason: 'candidateByteBudget' | 'workBudget', message: string) {
    super(message);
    this.name = 'LocalQueryBudgetError';
  }
}
