/**
 * Tenant resolution for the Ingress Worker (K18).
 *
 * The ingress is a service principal, exactly as the external adapter it
 * replaces (whagons-api-gateway `runtime/pool.rs` `service`): it presents its
 * `gvx_svc_` credential with the request's project and tenant selector to the
 * control object's `authenticate` (K06, `service_principal.rs:241`). That
 * gives the canonical tenant id (the selector may be the id or the tenant's
 * domain, `resolve_tenant`), refuses disabled and deleted tenants with
 * "tenant is not available", and returns the principal's function allowlist.
 * Nothing in K06 changes for this.
 */
import type { AuthOutcome, AuthRequest } from "../../control/src/auth.ts";

/** What the ingress may do in one tenant. */
export interface IngressGrant {
  principalId: string;
  projectId: string;
  tenantId: string;
  /** The principal's allowlist (`GONVEX_SERVICE_PRINCIPALS[].functions`). */
  functions: readonly string[];
}

export interface IngressDirectory {
  /** The grant, or `null` when the project or tenant is unknown, disabled or deleted. */
  resolve(projectId: string, tenant: string): Promise<IngressGrant | null>;
}

/** The control object's method this needs (K06 `ControlObject.authenticate`). */
export interface ControlAuthenticator {
  authenticate(request: AuthRequest): Promise<AuthOutcome>;
}

/** K06's text for a credential that is wrong or does not list the project (auth.ts `SERVICE_FAILURE`). */
export const SERVICE_FAILURE = "service credential is invalid or not permitted for this project";
const UNKNOWN_TENANT = "tenant is not available";

/** The ingress credential itself was refused: a deploy error, never the caller's. */
export class IngressMisconfigured extends Error {}

interface Cached {
  grant: IngressGrant | null;
  until: number;
}

/**
 * The gateway remembers an unknown tenant for 30 s (`UNKNOWN_TENANT_TTL`) and
 * keeps a resolved service socket open with no revalidation (CONTRACT C.3.8
 * item 11). The ingress remembers both answers for 30 s per isolate, so a
 * disabled tenant stops receiving deliveries within 30 s.
 */
export const DIRECTORY_TTL_MS = 30_000;
const MAX_CACHED = 10_000;

export class ControlDirectory implements IngressDirectory {
  private readonly cache = new Map<string, Cached>();

  constructor(
    private readonly control: () => ControlAuthenticator,
    private readonly token: string,
    private readonly clock: () => number = () => Date.now(),
  ) {}

  async resolve(projectId: string, tenant: string): Promise<IngressGrant | null> {
    const key = `${projectId}\u0000${tenant.toLowerCase()}`;
    const now = this.clock();
    const cached = this.cache.get(key);
    if (cached && cached.until > now) return cached.grant;
    const outcome = await this.control().authenticate({
      id: "ingress",
      token: this.token,
      project: projectId,
      tenant,
      controlOnly: false,
      public: false,
      connectionId: `ingress:${crypto.randomUUID()}`,
    });
    let grant: IngressGrant | null;
    if (outcome.message.type === "auth.result" && outcome.service) {
      const route = outcome.service.route;
      grant = { principalId: outcome.service.principalId, projectId: route.projectId, tenantId: route.tenantId, functions: [...outcome.service.functions] };
    } else if (outcome.message.type === "auth.error" && outcome.message.error === UNKNOWN_TENANT) {
      grant = null;
    } else if (outcome.message.type === "auth.error" && outcome.message.error === SERVICE_FAILURE) {
      // The project is one this ingress serves (the router checked), so the
      // credential or GONVEX_SERVICE_PRINCIPALS is wrong. Never cached.
      throw new IngressMisconfigured(`the control object refused the ingress service credential for project ${projectId}`);
    } else {
      throw new Error(outcome.message.type === "auth.error" ? outcome.message.error : "the control object answered no service grant");
    }
    if (this.cache.size >= MAX_CACHED) {
      for (const [entry, value] of this.cache) if (value.until <= now) this.cache.delete(entry);
      if (this.cache.size >= MAX_CACHED) this.cache.delete(this.cache.keys().next().value!);
    }
    this.cache.set(key, { grant, until: now + DIRECTORY_TTL_MS });
    return grant;
  }
}
