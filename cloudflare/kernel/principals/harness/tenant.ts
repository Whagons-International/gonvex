/// <reference path="../../host/src/shims.d.ts" />
/**
 * The tenant object of the K17 harness: K07's tenant object (K01-K04 and the
 * Reducer path, with K17's `serviceCall`), plus the part of K06's
 * `TenantPort` the control object needs here: admission, the e2e seeding
 * write and member reads. The other `TenantPort` writes (provisioning
 * Members, invitations, demos) belong to the tenant-object composition and
 * fail closed in this harness.
 *
 * `admit` answers both callers' shapes: K06 reads `{member, revision}`, K05's
 * and K07's code reads the Member fields and `admissionRevision`.
 */
import type { ModuleExports } from "../../host/src/index.js";
import type { Member } from "../../session/ports";
import { HarnessTenant } from "../../reducer/harness/index.ts";

type ControlMember = Member;
interface Admission { member: ControlMember; revision: number }
type DualAdmission = Member & { admissionRevision: number } & Admission;

const unsupported = (what: string) => new Error(`${what} is not available in the K17 harness tenant object`);

function memberOf(row: Record<string, SqlStorageValue>): Member {
  return {
    id: String(row.id), accountId: String(row.account_id), status: String(row.status), displayName: String(row.display_name),
    avatarUrl: String(row.avatar_url ?? ""), role: String(row.role), permissions: JSON.parse(String(row.permissions ?? "null")),
    membershipRevision: Number(row.membership_revision),
  };
}

export abstract class PrincipalsTenant extends HarnessTenant {
  protected abstract override bundle(artifactHash: string): ModuleExports | undefined;

  override async admit(accountId: string): Promise<DualAdmission | null> {
    const admitted = await super.admit(accountId);
    if (!admitted) return null;
    const { admissionRevision, ...member } = admitted;
    return { ...admitted, member, revision: admissionRevision };
  }

  // ---- K06 TenantPort ----------------------------------------------------------------------

  async activeMemberAccounts(accountIds: string[]): Promise<string[]> {
    if (!accountIds.length) return [];
    return this.ctx.storage.sql.exec<{ account_id: string }>(
      "SELECT DISTINCT account_id FROM members WHERE status='active' AND account_id IN (SELECT value FROM json_each(?))", JSON.stringify(accountIds),
    ).toArray().map((row) => row.account_id);
  }

  async memberById(memberId: string): Promise<Member | null> {
    const row = this.ctx.storage.sql.exec<Record<string, SqlStorageValue>>(
      "SELECT id,account_id,status,display_name,avatar_url,role,permissions,membership_revision FROM members WHERE id=?", memberId,
    ).toArray()[0];
    return row ? memberOf(row) : null;
  }

  /** Only the e2e seeding write (`/dev/internal/e2e/members`), which K07's `addMember` already performs. */
  async writeMembership(write: { kind: string; accountId: string; displayName?: string }): Promise<{ memberId: string; revision: number; updated: boolean }> {
    if (write.kind !== "e2eMember") throw unsupported(`membership write ${JSON.stringify(write.kind)}`);
    const member = await this.addMember({ id: crypto.randomUUID(), accountId: write.accountId, displayName: write.displayName ?? "" });
    return { memberId: member.id, revision: member.membershipRevision, updated: true };
  }

  /** The control object activates the release itself after creating the tenant (`devCreateTenant`). */
  async provision(_projectId: string, _tenantId: string): Promise<void> {}

  async memberAccounts(): Promise<never> { throw unsupported("memberAccounts"); }
  async activeMember(): Promise<never> { throw unsupported("activeMember"); }
  async activeMemberCount(): Promise<number> {
    return Number(this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM members WHERE status='active'").one().n);
  }
  async acceptInvitation(): Promise<never> { throw unsupported("acceptInvitation"); }
}
