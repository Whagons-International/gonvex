/**
 * Tenant resolution for the Ingress Worker (K18).
 *
 * The ingress is a service principal, exactly as the external adapter it
 * replaces (whagons-api-gateway `runtime/pool.rs` `service`): it presents its
 * `gvx_svc_` credential with the request's project and tenant selector to the
 * control object's `authenticate` (K06, `service_principal.rs:241`). That
 * gives the grant: the canonical tenant (the selector may be the id or the
 * tenant's domain, `resolve_tenant`), the function allowlist and the
 * principal's configured channel. Disabled and deleted tenants answer
 * "tenant is not available". Nothing in K06 changes for this.
 *
 * The cache is keyed by the **exact** selector, as the gateway's is. K06
 * matches `tenant_id` case-sensitively and the domain case-insensitively, so
 * folding case here would let `?tenant=TENANT_ALPHA` (a miss) poison the
 * entry of `tenant_alpha` (0043 §5, review of #368).
 */
import type { AuthOutcome, AuthRequest } from "../../control/src/auth.ts";
import type { ServiceGrant } from "../../control/src/types.ts";
import { LOOKUP_RATE, Limiter, type RateLimit } from "./limits.ts";

export interface IngressDirectory {
  /** The grant, or `null` when the tenant is unknown, disabled or deleted. */
  resolve(projectId: string, tenant: string): Promise<ServiceGrant | null>;
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

/** The project's budget of control lookups is spent: answered 429, the provider retries. */
export class LookupBudgetExceeded extends Error {}

/**
 * The gateway remembers an unknown tenant for 30 s (`UNKNOWN_TENANT_TTL`) and
 * keeps a resolved service socket open with no revalidation (CONTRACT C.3.8
 * item 11). The ingress remembers both answers for 30 s per isolate, so a
 * disabled tenant stops receiving deliveries within 30 s.
 */
export const DIRECTORY_TTL_MS = 30_000;
/** The gateway's `MAX_UNKNOWN_TENANTS`; the same cap bounds known tenants. */
export const MAX_CACHED = 10_000;

interface Cached<T> {
  value: T;
  until: number;
}

/** A TTL cache with a size cap: expired entries go first, then the oldest. */
class Bounded<T> {
  readonly entries = new Map<string, Cached<T>>();

  constructor(private readonly cap: number) {}

  get(key: string, now: number): Cached<T> | undefined {
    const entry = this.entries.get(key);
    if (entry && entry.until <= now) {
      this.entries.delete(key);
      return undefined;
    }
    return entry;
  }

  set(key: string, value: T, until: number, now: number): void {
    this.entries.delete(key);
    if (this.entries.size >= this.cap) for (const [entry, cached] of this.entries) if (cached.until <= now) this.entries.delete(entry);
    while (this.entries.size >= this.cap) this.entries.delete(this.entries.keys().next().value!);
    this.entries.set(key, { value, until });
  }
}

export class ControlDirectory implements IngressDirectory {
  readonly known: Bounded<ServiceGrant>;
  readonly unknown: Bounded<null>;
  private readonly budget: Limiter;
  /** Lookups that reached the control object (tests and numbers). */
  lookups = 0;

  constructor(
    private readonly control: () => ControlAuthenticator,
    private readonly token: string,
    options: { clock?: () => number; lookupRate?: RateLimit; cap?: number } = {},
  ) {
    this.clock = options.clock ?? (() => Date.now());
    this.known = new Bounded(options.cap ?? MAX_CACHED);
    this.unknown = new Bounded(options.cap ?? MAX_CACHED);
    this.budget = new Limiter({ lookup: options.lookupRate ?? LOOKUP_RATE });
  }

  private readonly clock: () => number;

  async resolve(projectId: string, tenant: string): Promise<ServiceGrant | null> {
    const key = `${projectId}\u0000${tenant}`;
    const now = this.clock();
    const known = this.known.get(key, now);
    if (known) return known.value;
    if (this.unknown.get(key, now)) return null;
    if (!this.budget.take("lookup", now, projectId)) throw new LookupBudgetExceeded(`tenant lookups for project ${projectId} are over budget`);
    this.lookups++;
    const outcome = await this.control().authenticate({
      id: "ingress",
      token: this.token,
      project: projectId,
      tenant,
      controlOnly: false,
      public: false,
      connectionId: `ingress:${crypto.randomUUID()}`,
    });
    if (outcome.message.type === "auth.result" && outcome.service) {
      this.known.set(key, outcome.service, now + DIRECTORY_TTL_MS, now);
      return outcome.service;
    }
    if (outcome.message.type === "auth.error" && outcome.message.error === UNKNOWN_TENANT) {
      this.unknown.set(key, null, now + DIRECTORY_TTL_MS, now);
      return null;
    }
    if (outcome.message.type === "auth.error" && outcome.message.error === SERVICE_FAILURE) {
      // The project is one this ingress serves (the router checked), so the
      // credential or GONVEX_SERVICE_PRINCIPALS is wrong. Never cached.
      throw new IngressMisconfigured(`the control object refused the ingress service credential for project ${projectId}`);
    }
    throw new Error(outcome.message.type === "auth.error" ? outcome.message.error : "the control object answered no service grant");
  }
}
