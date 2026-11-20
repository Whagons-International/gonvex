import type { TenantSession } from "./ports";

/** A tenant `members` row, as read for an access re-check. */
export interface MemberRow {
  account_id: string;
  status: string;
  role: string;
  permissions: string;
  membership_revision: number;
}

/** lib.rs `member_access_changed`: whether `current` (undefined: no row) differs from the session's admitted Member. */
export function memberAccessChanged(session: TenantSession, current: MemberRow | undefined): boolean {
  if (!current) return true;
  const admitted = session.member;
  return admitted.accountId !== current.account_id || admitted.status !== current.status || admitted.role !== current.role
    // #543 review: empty permissions read as `{}`, as the admission reads them (assembly/memberships.ts).
    || JSON.stringify(admitted.permissions) !== JSON.stringify(JSON.parse(current.permissions || "{}"))
    || admitted.membershipRevision !== Number(current.membership_revision);
}

/** Sessions whose Member is re-checked against the tenant (not anonymous, not a service principal). */
export function memberChecked(session: TenantSession): boolean {
  return !session.anonymous && !!session.member.id && !session.member.id.startsWith("_gonvex_service:");
}

/** The members row for an access re-check (one indexed read). */
export const MEMBER_ACCESS_SQL = "SELECT account_id, status, role, permissions, membership_revision FROM members WHERE id = ?";
