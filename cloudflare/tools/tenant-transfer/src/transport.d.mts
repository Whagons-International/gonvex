export class TenantTransfer {
  constructor(ctx: DurableObjectState);
  fetch(request: Request): Promise<Response>;
}
