/**
 * How the Ingress Worker runs an Action (K18).
 *
 * A webhook ends as an Action invocation through a service principal
 * (0001 IB4, 0002). The Rust reference for the call is
 * `service_principal.rs` `handle_service_call` with kind `action`: the
 * principal's allowlist is checked, the call runs as the synthetic member
 * `_gonvex_service:<id>` (role `system`), and `ctx.invocation` comes from
 * `direct_provenance`. The ingress differs in one field: the channel is
 * `system`, not `api` (0040 §3), because `relay.recordWebhookEvent` refuses a
 * root channel of `api` and the ingress is the host, not an API client.
 *
 * The tenant-side service call is K17's and the Action runtime K12's; neither
 * is merged. This file is the port they implement (`serviceAction`), plain
 * data across Workers RPC, plus the binding adapter.
 */

export type IngressChannel = "system";

export interface ServiceActionRequest {
  v: 1;
  principal: {
    id: string;
    projectId: string;
    tenantId: string;
    /** The grant's allowlist; the implementation checks the path against it again. */
    functions: readonly string[];
  };
  path: string;
  args: Record<string, unknown>;
  /** `ctx.invocation.channel` and `rootChannel`. */
  channel: IngressChannel;
  /** `ctx.invocation.commandId` and `rootCommandId` (`ingress_<uuid>`). */
  commandId: string;
}

/** The Action's result JSON value, or its error text as Rust would put it on a service socket. */
export type ServiceActionReport =
  | { ok: true; value: unknown }
  | { ok: false; error: string; transient?: boolean };

export interface ServiceActionPort {
  serviceAction(request: ServiceActionRequest): Promise<ServiceActionReport>;
}

/** The Rust service session's member id (`service_principal.rs:71`). */
export const serviceMemberId = (principalId: string): string => `_gonvex_service:${principalId}`;

export const commandId = (): string => `ingress_${crypto.randomUUID().replaceAll("-", "")}`;

/**
 * The deployed adapter: the Action Worker (K12) or the tenant object (K17)
 * bound as `INGRESS_ACTIONS`, exposing `serviceAction` over RPC.
 */
export function bindingPort(binding: unknown): ServiceActionPort | null {
  if (!binding || typeof (binding as Partial<ServiceActionPort>).serviceAction !== "function") return null;
  const target = binding as ServiceActionPort;
  return { serviceAction: (request) => target.serviceAction(request) };
}
