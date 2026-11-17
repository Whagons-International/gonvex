/** Placement names are addresses; an opaque DO ID must never become a new name. */
export const ROUTING_IDENTITY_KEY = "gateway-routing-identity";
interface RoutingIdentity { v: 1; name: string }

export class GatewayRoutingIdentity {
  private logicalName: string | undefined;
  private persisted = false;
  private invalid = false;

  constructor(private readonly ctx: DurableObjectState, private readonly namespace: DurableObjectNamespace | undefined) {
    const stored = ctx.storage.kv.get<RoutingIdentity>(ROUTING_IDENTITY_KEY);
    if (stored !== undefined) {
      // Fail closed on misplaced/corrupt durable identity, rather than replacing it
      // with an incoming header. Validate even when ctx.id.name is available.
      if (stored?.v !== 1 || !this.owns(stored.name)) this.invalid = true;
      else { this.logicalName = stored.name; this.persisted = true; }
    } else if (ctx.id.name && this.owns(ctx.id.name)) this.logicalName = ctx.id.name;
  }

  get known(): boolean { return this.logicalName !== undefined; }
  get name(): string { return this.logicalName ?? this.ctx.id.toString(); }

  matches(name: string): boolean {
    return !this.invalid && this.owns(name) && (this.logicalName === undefined || name === this.logicalName);
  }

  /** Call only after the controller has durably claimed this exact assignment. */
  claimed(name: string): void {
    if (!this.matches(name)) throw new Error("gateway routing identity mismatch");
    if (!this.persisted) {
      this.ctx.storage.kv.put(ROUTING_IDENTITY_KEY, { v: 1, name } satisfies RoutingIdentity);
      this.persisted = true;
    }
    this.logicalName = name;
  }

  private owns(name: unknown): name is string {
    if (!this.namespace || typeof name !== "string" || !name.length || name.length > 100) return false;
    try { return this.namespace.idFromName(name).equals(this.ctx.id); }
    catch { return false; }
  }
}
