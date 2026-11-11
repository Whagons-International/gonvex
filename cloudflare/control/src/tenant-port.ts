/**
 * What the control object asks of a tenant object.
 *
 * Tenant `members` is authoritative (contract C.4). Every Rust code path in
 * control.rs that opens a tenant transaction maps to exactly one call here,
 * so each one stays one atomic tenant transaction. The tenant object (K01/K02,
 * module host K04) implements this interface as Durable Object RPC methods;
 * `test-support/stub-tenant.ts` implements it with the same SQL for the
 * control object's own tests and conformance runs.
 *
 * Membership projection (C.4 item 6, membership_projector.rs) runs the other
 * way: after a commit that wrote `_gonvex_control_plane_membership_outbox`,
 * the tenant object pushes the rows to `ControlObject.projectMemberships`,
 * deletes what was acknowledged, and retries from its alarm if the push failed.
 */
import type { Member } from "./types";

export interface Admission {
  member: Member;
  /** `_gonvex_sync_clock.revision` in the same read snapshot. */
  revision: number;
}

/** One write to `members`, each the body of one reference tenant transaction. */
export type MembershipWrite =
  /** control.accounts.provisionMemberLogin (control.rs:780-810), command `member-provision:<acct>`. */
  | { kind: "provision"; commandId: string; accountId: string; displayName: string; role: string; permissions: unknown }
  /** control.developer.provisionSelf (control.rs:889-905). */
  | { kind: "developerProvision"; commandId: string; accountId: string; displayName: string; avatarUrl: string }
  /** control.developer.removeSelf (control.rs:906-923). */
  | { kind: "developerRemove"; commandId: string; accountId: string }
  /** control.tenants.create owner row (control.rs:1224-1248), command `tenant-create:<tenant>`. */
  | { kind: "tenantOwner"; commandId: string; accountId: string; displayName: string; avatarUrl: string }
  /** control.demos.create (control.rs:1412-1436), command `demo-create:<acct>`. */
  | { kind: "demoCreate"; commandId: string; accountId: string; displayName: string }
  /** control.demos.delete (control.rs:1504-1517), command `demo-delete:<acct>`. */
  | { kind: "demoDelete"; commandId: string; accountId: string }
  /** Test seeding, `/dev/internal/e2e/members` (lib.rs:1301-1326), command `e2e-member:<acct>`. */
  | { kind: "e2eMember"; commandId: string; accountId: string; displayName: string; avatarUrl: string };

export interface MembershipWriteResult {
  memberId: string;
  /** `membership_revision` the write left (0 when nothing changed and no row exists). */
  revision: number;
  /** developerRemove only: whether an active row was revoked. */
  updated: boolean;
}

/** control.invitations.accept, the tenant transaction (control.rs:1684-1901). */
export interface InvitationAcceptance {
  commandId: string; // `invitation:<id>`
  invitationId: string;
  accountId: string;
  accountEmail: string;
  accountName: string;
  accountAvatarUrl: string;
  idempotencyKey: string;
  role: string;
  permissions: unknown;
  teamIds: unknown;
  payload: unknown;
  /** The inviter's admitted Member id, or null when `tenant_session_for_account` failed. */
  inviterAccountId: string;
  inviterMemberId: string | null;
  /** Whether the project has a deployed module ("project module is unavailable" otherwise). */
  moduleAvailable: boolean;
  /** The deployed module's declared invitation Reducer (`module.invitationAcceptanceReducer`). */
  reducerPath: string;
  reducerDefinition: { kind: string; internal: boolean } | null;
  artifactHash: string;
}

export type InvitationAcceptanceResult =
  /** Committed now, or already committed by an earlier attempt with the same key. */
  | { ok: true; memberId: string }
  /** Refused and rolled back. `release` says the Control claim must be released (control.rs:1756-1765, 1872-1880). */
  | { ok: false; error: string; release: boolean };

export interface MembershipOutboxRow {
  accountId: string;
  memberId: string;
  status: string;
  membershipRevision: number;
}

export interface TenantPort {
  /** `admit_member`: the active Member of an account and the change-feed revision, one snapshot. */
  admit(accountId: string): Promise<Admission | null>;
  /** `active_member_accounts`: the subset of accounts with an active Member. */
  activeMemberAccounts(accountIds: string[]): Promise<string[]>;
  /** `member_by_id`: the current row, whatever its status. */
  memberById(memberId: string): Promise<Member | null>;
  /** control.auth.memberProviders: `SELECT id,account_id FROM members WHERE id IN (...)`. */
  memberAccounts(memberIds: string[]): Promise<Array<{ id: string; accountId: string }>>;
  /** control.accounts.resetMemberPassword: the active Member's account and role. */
  activeMember(memberId: string): Promise<{ accountId: string; role: string } | null>;
  /** control.support.listTenants: `SELECT COUNT(*) FROM members WHERE status='active'`. */
  activeMemberCount(): Promise<number>;
  /** One membership write, one tenant transaction. */
  writeMembership(write: MembershipWrite): Promise<MembershipWriteResult>;
  /** The invitation acceptance transaction. */
  acceptInvitation(acceptance: InvitationAcceptance): Promise<InvitationAcceptanceResult>;
  /** control.tenants.create: provision the tenant schema with the project's migrations (K02). */
  provision(projectId: string, tenantId: string): Promise<void>;
}
