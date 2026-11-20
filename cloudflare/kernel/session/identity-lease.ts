/**
 * X17j: short identity leases on the gateway (lead decision, 2026-11-20).
 *
 * Every tenant auth was gateway -> Control (token) -> tenant (admit) -> Control -> gateway: four one-way Durable Object
 * legs where Rust checks the token in-process. A successful auth leaves a lease here for the exact token (and, for a
 * tenant auth, the exact requested tenant), valid for `IDENTITY_LEASE_MS`:
 *   - a control-only auth with that token is answered from the lease;
 *   - a tenant auth with that token and tenant reuses the leased identity and route and admits the member LIVE, with
 *     one direct tenant RPC (two legs). A member the tenant no longer admits falls back to Control (its error).
 * Revocation is pushed: every Control change that can revoke or disable a session, account or membership notifies the
 * project's watching gateways (`controlChanged`), which drops that project's leases here. The TTL is the backstop if a
 * notification fails: a revoked token can authenticate on a gateway for at most `IDENTITY_LEASE_MS` (Rust: none).
 * Leases never cross tokens; impersonation, delegated, developer and anonymous sessions are never leased.
 */
import type { Member, SessionIdentity, TenantSession } from "./ports";

export const IDENTITY_LEASE_MS = 10_000;
const MAX_LEASES = 4096;

interface Lease { readonly project: string; readonly at: number; readonly identity?: SessionIdentity; readonly session?: TenantSession }

export class IdentityLeases {
  private readonly entries = new Map<string, Lease>();
  constructor(private readonly now: () => number = () => Date.now(), private readonly ttlMs = IDENTITY_LEASE_MS) {}

  private static key(kind: "identity" | "tenant", token: string, project: string | undefined, tenant = ""): string {
    return `${kind}\u0000${project ?? ""}\u0000${tenant}\u0000${token}`;
  }
  private get(key: string): Lease | undefined {
    const lease = this.entries.get(key);
    if (!lease) return undefined;
    if (this.now() - lease.at >= this.ttlMs) { this.entries.delete(key); return undefined; }
    return lease;
  }
  private put(key: string, lease: Lease): void {
    this.entries.delete(key);
    this.entries.set(key, lease);
    while (this.entries.size > MAX_LEASES) this.entries.delete(this.entries.keys().next().value!);
  }

  identity(token: string, project: string | undefined): SessionIdentity | undefined {
    return this.get(IdentityLeases.key("identity", token, project))?.identity;
  }
  putIdentity(token: string, project: string | undefined, identity: SessionIdentity): void {
    if (!token.trim() || token.startsWith("gvx_")) return;
    this.put(IdentityLeases.key("identity", token, project), { project: identity.projectId, at: this.now(), identity });
  }
  session(token: string, project: string | undefined, tenant: string): TenantSession | undefined {
    return tenant ? this.get(IdentityLeases.key("tenant", token, project, tenant))?.session : undefined;
  }
  putSession(token: string, project: string | undefined, tenant: string, session: TenantSession): void {
    if (!tenant || !token.trim() || token.startsWith("gvx_") || session.anonymous || session.delegation !== null || session.developerSession !== null) return;
    this.put(IdentityLeases.key("tenant", token, project, tenant), { project: session.identity.projectId, at: this.now(), session });
  }
  drop(token: string, project: string | undefined, tenant: string): void {
    this.entries.delete(IdentityLeases.key("tenant", token, project, tenant));
  }
  /** A Control change for `project` (`controlChanged`): every lease of that project goes. */
  invalidate(project: string): void {
    for (const [key, lease] of this.entries) if (lease.project === project) this.entries.delete(key);
  }
  get size(): number { return this.entries.size; }
}

/** One lease store per isolate (a lease is only ever used for its own token). */
export const identityLeases = new IdentityLeases();

/** A tenant's `admit` answer as the session needs it (the assembled tenant also returns `member`/`revision`). */
export function admittedMember(admitted: (Member & { admissionRevision: number; member?: Member }) | null): { member: Member; admissionRevision: number } | null {
  if (!admitted) return null;
  const { admissionRevision, member, ...rest } = admitted as Member & { admissionRevision: number; member?: Member; revision?: number };
  delete (rest as { revision?: number }).revision;
  return { member: member ?? (rest as Member), admissionRevision };
}
