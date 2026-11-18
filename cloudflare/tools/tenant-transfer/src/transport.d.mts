export const REQUEST_BUDGET_MS: number;
export class TenantTransfer {
  constructor(ctx: DurableObjectState, options?: { budgetMs?: number });
  fetch(request: Request): Promise<Response>;
}
