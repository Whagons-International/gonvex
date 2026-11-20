/**
 * X17j: short identity leases on the gateway (lead decision, 2026-11-20).
 *
 * Every tenant auth was gateway -> Control (token) -> tenant (admit) -> Control -> gateway: four one-way Durable Object
 * legs where Rust checks the token in-process. A successful auth leaves a lease here for the exact token (and, for a
 * tenant auth, the exact requested tenant), valid for `IDENTITY_LEASE_MS`:
 *   - a control-only auth with that token is answered from the lease;
 *   - a tenant auth with that token and tenant reuses the leased identity and route and admits the member LIVE, with
 *     one direct tenant RPC (two legs). A member the tenant no longer admits falls back to Control (its error).
 * Only Control's own session tokens (`gvx_session_*`) are leased, keyed by their SHA-256 (as Control keys them), and a
 * lease never outlives the session's `expires_at` (Control returns it with the auth): it lasts until
 * min(lease time + `IDENTITY_LEASE_MS`, expiresAt); an answer without `expiresAt` is not leased.
 * Revocation is pushed: every Control change that can revoke or disable a session, account, membership, tenant or
 * project notifies the project's watching gateways (`controlChanged`), which drops that project's leases; the gateway
 * that forwarded a Control write drops them itself, synchronously. Another gateway sees a revocation one Control alarm
 * later (the notice is sent from Control's alarm, X17e). The TTL is the backstop if a notification is lost: a revoked
 * token can authenticate on a gateway for at most `IDENTITY_LEASE_MS` (Rust: none).
 * Leases never cross tokens; impersonation, delegated, developer and anonymous sessions are never leased. The store
 * belongs to one gateway object (`leasesFor`), so it goes with that object and is the one its notices reach.
 */
import { createHash } from "node:crypto";
import type { Member, SessionIdentity, TenantSession } from "./ports";

export const IDENTITY_LEASE_MS = 10_000;
const MAX_LEASES = 4096;

interface Lease { readonly project: string; readonly until: number; readonly identity?: SessionIdentity; readonly session?: TenantSession }

const SESSION_PREFIX = "gvx_session_";
function digest(token: string): string { return createHash("sha256").update(token.trim()).digest("hex"); }

export class IdentityLeases {
  private readonly entries = new Map<string, Lease>();
  constructor(private readonly now: () => number = () => Date.now(), private readonly ttlMs = IDENTITY_LEASE_MS) {}

  private static leasable(token: string): boolean { return token.trim().startsWith(SESSION_PREFIX); }
  private static key(kind: "identity" | "tenant", token: string, project: string | undefined, tenant = ""): string {
    return `${kind}\u0000${project ?? ""}\u0000${tenant}\u0000${digest(token)}`;
  }
  private get(key: string): Lease | undefined {
    const lease = this.entries.get(key);
    if (!lease) return undefined;
    if (this.now() >= lease.until) { this.entries.delete(key); return undefined; }
    return lease;
  }
  private put(key: string, project: string, expiresAt: number | undefined, value: Omit<Lease, "project" | "until">): void {
    if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)) return;
    const until = Math.min(this.now() + this.ttlMs, expiresAt);
    if (until <= this.now()) return;
    this.entries.delete(key);
    this.entries.set(key, { project, until, ...value });
    while (this.entries.size > MAX_LEASES) this.entries.delete(this.entries.keys().next().value!);
  }

  identity(token: string, project: string | undefined): SessionIdentity | undefined {
    return IdentityLeases.leasable(token) ? this.get(IdentityLeases.key("identity", token, project))?.identity : undefined;
  }
  putIdentity(token: string, project: string | undefined, identity: SessionIdentity, expiresAt: number | undefined): void {
    if (!IdentityLeases.leasable(token)) return;
    this.put(IdentityLeases.key("identity", token, project), identity.projectId, expiresAt, { identity });
  }
  session(token: string, project: string | undefined, tenant: string): TenantSession | undefined {
    return tenant && IdentityLeases.leasable(token) ? this.get(IdentityLeases.key("tenant", token, project, tenant))?.session : undefined;
  }
  putSession(token: string, project: string | undefined, tenant: string, session: TenantSession, expiresAt: number | undefined): void {
    if (!tenant || !IdentityLeases.leasable(token) || session.anonymous || session.delegation !== null || session.developerSession !== null) return;
    this.put(IdentityLeases.key("tenant", token, project, tenant), session.identity.projectId, expiresAt, { session });
  }
  drop(token: string, project: string | undefined, tenant: string): void {
    if (IdentityLeases.leasable(token)) this.entries.delete(IdentityLeases.key("tenant", token, project, tenant));
  }
  /** A Control change for `project` (`controlChanged`, or a write this gateway forwarded): its leases go. */
  invalidate(project: string): void {
    for (const [key, lease] of this.entries) if (lease.project === project) this.entries.delete(key);
  }
  get size(): number { return this.entries.size; }
}

/**
 * One store per gateway object. `gatedEnv` gives each gateway instance its own env object, which both its ports and
 * the object itself see, so the env identifies the instance (and its store is collected with it).
 */
const stores = new WeakMap<object, IdentityLeases>();
export function leasesFor(owner: object): IdentityLeases {
  let store = stores.get(owner);
  if (!store) stores.set(owner, store = new IdentityLeases());
  return store;
}

/** A tenant's `admit` answer as the session needs it (the assembled tenant also returns `member`/`revision`). */
export function admittedMember(admitted: (Member & { admissionRevision: number; member?: Member }) | null): { member: Member; admissionRevision: number } | null {
  if (!admitted) return null;
  const { admissionRevision, member, ...rest } = admitted as Member & { admissionRevision: number; member?: Member; revision?: number };
  delete (rest as { revision?: number }).revision;
  return { member: member ?? (rest as Member), admissionRevision };
}
