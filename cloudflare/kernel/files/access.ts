// Who may do what with a file: Rust's visibility and owner rules, with the
// intended owner checks of IB1 (0001), 0012 R12 and R13. See README.md and
// contract-changes for the member-id reading of "owner".

/** The caller of a storage operation, as the tenant session names it. */
export interface FilesSession {
  projectId: string;
  tenantId: string;
  /** Account id; empty for anonymous and system callers (Rust: `is_anonymous` is an empty account). */
  accountId: string;
  /** The caller's tenant Member id; empty when there is none. */
  memberId: string;
  /** A trusted principal with no account (scheduler, migrations): may name the owner of a new file. */
  system?: boolean | undefined;
}

export const FILE_NOT_FOUND = "file not found";
export const OWNER_REFUSED = "ownerId must be the calling account or its tenant member";

export function isAnonymous(session: FilesSession): boolean {
  return session.accountId === "" && session.system !== true;
}

/**
 * IB1: a private file is readable, linkable and deletable only by its owner.
 * Owners are written as an account id (Rust's default) or, as Whagons does, a
 * tenant Member id; both name the caller. An empty owner is nobody's but a
 * system principal's. No session kind is exempt on reads (E.d.4).
 */
export function mayAccess(row: { visibility: string; owner_id: string }, session: FilesSession): boolean {
  if (row.visibility !== "private") return true;
  if (row.owner_id === "") return session.system === true;
  return row.owner_id === session.accountId || (session.memberId !== "" && row.owner_id === session.memberId);
}

/**
 * The owner of a new file (storage.rs `owner`, replaced by IB1). Anonymous:
 * empty, as Rust. A system principal: as requested. Anyone else: their
 * account by default, or their own account or Member id when named; naming
 * someone else is refused.
 */
export function ownerFor(session: FilesSession, requested: string): string {
  if (isAnonymous(session)) return "";
  const wanted = requested.trim();
  if (session.system === true) return wanted;
  if (!wanted) return session.accountId;
  if (wanted === session.accountId || (session.memberId !== "" && wanted === session.memberId)) return wanted;
  throw new Error(OWNER_REFUSED);
}

/** storage.rs: an empty account (anonymous, or a principal without one) always stores `tenant` files. */
export function visibilityFor(session: FilesSession, requested: "private" | "tenant" | "public"): "private" | "tenant" | "public" {
  return session.accountId === "" ? "tenant" : requested;
}
