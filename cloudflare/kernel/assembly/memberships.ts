// K22: authoritative control-plane membership operations on K07's captured storage.
// SQL follows runtime control.rs; no test-support implementation is imported.
import type { KernelTenant } from "../reducer/tenant";
import { ensureReceipts, readReceipt, storeReceipt } from "../reducer/receipts";
import { identityOf } from "../../actions/src/session";
import type { InvitationAcceptance, InvitationAcceptanceResult } from "../../control/src/tenant-port";
import { invokeCaptured, directSql } from "../capture/index";
import type { StorageSession } from "../storage";
import { Sql, NOW_MS, num, str, type Row } from "../../control/src/sql";
import { uuidSimple } from "../../control/src/util";
import type { MembershipWrite, MembershipWriteResult } from "../../control/src/tenant-port";
import type { Member } from "../../control/src/types";

function memberFrom(row: Row): Member {
  return { id: str(row.id), accountId: str(row.account_id), status: str(row.status), displayName: str(row.display_name), avatarUrl: str(row.avatar_url), role: str(row.role), permissions: JSON.parse(str(row.permissions) || "{}"), membershipRevision: num(row.membership_revision) };
}

/** The same small query surface for direct reads and reads inside K01's transaction. */
export function sessionSql(db: StorageSession) {
  const all = (text: string, ...parameters: unknown[]): Row[] => {
    const result = db.run(text, parameters);
    return result.rows.map((row) => Object.fromEntries(result.columnNames.map((column, i) => [column, row[i] ?? null])));
  };
  return {
    all, run: (text: string, ...parameters: unknown[]) => { db.run(text, parameters); },
    get: (text: string, ...parameters: unknown[]) => all(text, ...parameters)[0],
    value: <T extends SqlStorageValue = SqlStorageValue>(text: string, ...parameters: unknown[]) => db.run(text, parameters).rows[0]?.[0] as T | undefined,
  };
}

export class TenantMemberships {
  private current: ReturnType<typeof sessionSql> | undefined;
  private readonly direct: Sql;
  constructor(private readonly kernel: KernelTenant, private readonly storage: DurableObjectStorage) { this.direct = new Sql(storage); }
  private get sql() { return this.current ?? this.direct; }
  private commit<T>(commandId: string, body: () => T): Promise<T> {
    return this.kernel.reducers.serial(async () => {
      const result = await invokeCaptured(this.kernel.storage, "reducer", { commandId, channel: "system" }, async (db) => {
        this.current = sessionSql(db);
        try { return body(); } finally { this.current = undefined; }
      }, this.kernel.capture);
      if (!result.ok) throw result.error;
      return result.value;
    });
  }
  async activeMemberAccounts(accountIds: string[]): Promise<string[]> {
    return this.sql
      .all("SELECT DISTINCT account_id FROM members WHERE status = 'active' AND account_id IN (SELECT value FROM json_each($1))", JSON.stringify(accountIds))
      .map((row) => str(row.account_id));
  }

  async memberById(memberId: string): Promise<Member | null> {
    const row = this.sql.get("SELECT id, account_id, status, display_name, avatar_url, role, permissions, membership_revision FROM members WHERE id = $1", memberId);
    return row ? memberFrom(row) : null;
  }

  async memberAccounts(memberIds: string[]): Promise<Array<{ id: string; accountId: string }>> {
    return this.sql
      .all("SELECT id,account_id FROM members WHERE id IN (SELECT value FROM json_each($1))", JSON.stringify(memberIds))
      .map((row) => ({ id: str(row.id), accountId: str(row.account_id) }));
  }

  async activeMember(memberId: string): Promise<{ accountId: string; role: string } | null> {
    const row = this.sql.get("SELECT account_id,role FROM members WHERE id=$1 AND status='active'", memberId);
    return row ? { accountId: str(row.account_id), role: str(row.role) } : null;
  }

  async activeMemberCount(): Promise<number> {
    return num(this.sql.value("SELECT COUNT(*) FROM members WHERE status='active'"));
  }

  async writeMembership(write: MembershipWrite): Promise<MembershipWriteResult> {
    const result = await this.commit(write.commandId, (): MembershipWriteResult => {
      const latest = () => this.sql.value<string>("SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1", write.accountId) as string | undefined;
      switch (write.kind) {
        case "provision": {
          const memberId = latest() ?? `member_${uuidSimple()}`;
          const revision = num(
            this.sql.value(
              `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,'',$4,$5,${NOW_MS})
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 role=EXCLUDED.role,permissions=EXCLUDED.permissions,
                 membership_revision=members.membership_revision+1,updated_at=${NOW_MS}
               RETURNING membership_revision`,
              memberId,
              write.accountId,
              write.displayName,
              write.role,
              JSON.stringify(write.permissions),
            ),
          );
          return { memberId, revision, updated: true };
        }
        case "developerProvision": {
          const memberId = latest() ?? `member_${uuidSimple()}`;
          const revision = num(
            this.sql.value(
              `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,$4,'admin','{"developer":true}',${NOW_MS})
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 avatar_url=EXCLUDED.avatar_url,role='admin',permissions='{"developer":true}',
                 membership_revision=members.membership_revision+1,updated_at=${NOW_MS}
               RETURNING membership_revision`,
              memberId,
              write.accountId,
              write.displayName,
              write.avatarUrl,
            ),
          );
          return { memberId, revision, updated: true };
        }
        case "developerRemove": {
          const row = this.sql.get("SELECT id,membership_revision,status FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1", write.accountId);
          if (!row) return { memberId: `member_${uuidSimple()}`, revision: 0, updated: false };
          if (str(row.status) !== "active") return { memberId: str(row.id), revision: num(row.membership_revision), updated: false };
          const revision = num(
            this.sql.value(
              `UPDATE members SET status='revoked',membership_revision=membership_revision+1,updated_at=${NOW_MS} WHERE id=$1 RETURNING membership_revision`,
              str(row.id),
            ),
          );
          return { memberId: str(row.id), revision, updated: true };
        }
        case "tenantOwner": {
          const memberId = (this.sql.value<string>("SELECT id FROM members WHERE account_id=$1 LIMIT 1", write.accountId) as string | undefined) ?? `member_${uuidSimple()}`;
          const revision = num(
            this.sql.value(
              `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,$4,'owner','{}',${NOW_MS})
               ON CONFLICT(id) DO UPDATE SET status='active',role='owner',
                 membership_revision=members.membership_revision+1,updated_at=${NOW_MS}
               RETURNING membership_revision`,
              memberId,
              write.accountId,
              write.displayName,
              write.avatarUrl,
            ),
          );
          return { memberId, revision, updated: true };
        }
        case "demoCreate": {
          const memberId = latest() ?? `member_${uuidSimple()}`;
          const revision = num(
            this.sql.value(
              `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,'','member','{}',${NOW_MS})
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 membership_revision=members.membership_revision+1,updated_at=${NOW_MS}
               RETURNING membership_revision`,
              memberId,
              write.accountId,
              write.displayName,
            ),
          );
          return { memberId, revision, updated: true };
        }
        case "demoDelete": {
          const revision = this.sql.value(
            `UPDATE members SET status='revoked',membership_revision=membership_revision+1,updated_at=${NOW_MS}
             WHERE account_id=$1 AND status='active' RETURNING membership_revision`,
            write.accountId,
          );
          return { memberId: "", revision: revision === undefined ? 0 : num(revision), updated: revision !== undefined };
        }
        case "e2eMember": {
          const memberId = latest() ?? `member_${uuidSimple()}`;
          const revision = num(
            this.sql.value(
              `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
               VALUES($1,$2,'active',$3,$4,'admin','{"e2e":true}',${NOW_MS})
               ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
                 avatar_url=EXCLUDED.avatar_url,role='admin',permissions='{"e2e":true}',
                 membership_revision=members.membership_revision+1,updated_at=${NOW_MS}
               RETURNING membership_revision`,
              memberId,
              write.accountId,
              write.displayName,
              write.avatarUrl,
            ),
          );
          return { memberId, revision, updated: true };
        }
      }
    });

    return result;
  }

  async acceptInvitation(a: InvitationAcceptance, projectId: string, tenantId: string): Promise<InvitationAcceptanceResult> {
    class Refusal extends Error { constructor(text: string, readonly release: boolean) { super(`invalid arguments: ${text}`); } }
    return this.kernel.reducers.serial(async () => {
      try {
        // Ensure the normal K07 receipt schema; never create a parallel invitation receipt store.
        ensureReceipts(directSql(this.storage.sql));
        const receipt = this.direct.get("SELECT path,result FROM _gonvex_reducer_idempotency WHERE subject=$1 AND idempotency_key=$2", a.accountId, a.idempotencyKey);
        const memberId = this.direct.value<string>("SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1", a.accountId) ?? `member_${a.invitationId}`;
        if (receipt?.result != null) return { ok: true as const, memberId };
        const inviter: Record<string, unknown> = { id: a.inviterMemberId, accountId: a.inviterAccountId };
        const prepare = (db: StorageSession) => {
          const sql = sessionSql(db);
          const deny = () => new Refusal("invitation inviter can no longer issue these membership grants", true);
          if (!a.inviterMemberId) throw deny();
          const row = sql.get("SELECT status,role,permissions FROM members WHERE id=$1 AND account_id=$2", a.inviterMemberId, a.inviterAccountId);
          if (!row || row.status !== "active" || !["owner", "admin"].includes(str(row.role)) || (row.role === "admin" && ["owner", "admin"].includes(a.role))) throw deny();
          if (!["owner", "admin", "member", "viewer"].includes(a.role)) throw new Refusal("role is invalid", true);
          Object.assign(inviter, { status: str(row.status), role: str(row.role), permissions: JSON.parse(str(row.permissions)) });
          sql.run(`INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
            VALUES($1,$2,'active',$3,$4,$5,$6,${NOW_MS}) ON CONFLICT(id) DO UPDATE SET account_id=EXCLUDED.account_id,status='active',display_name=EXCLUDED.display_name,avatar_url=EXCLUDED.avatar_url,role=EXCLUDED.role,permissions=EXCLUDED.permissions,membership_revision=members.membership_revision+1,updated_at=${NOW_MS}`,
            memberId, a.accountId, a.accountName, a.accountAvatarUrl, a.role, JSON.stringify(a.permissions));
          if (!a.moduleAvailable) throw new Refusal("project module is unavailable", false);
          const hasPayload = (Array.isArray(a.teamIds) && a.teamIds.length > 0) || (!!a.payload && typeof a.payload === "object" && !Array.isArray(a.payload) && Object.keys(a.payload).length > 0);
          if (!a.reducerPath && hasPayload) throw new Refusal("module must declare invitationAcceptance for application invitation payloads", false);
          if (a.reducerPath && (!a.reducerDefinition || a.reducerDefinition.kind !== "reducer" || !a.reducerDefinition.internal)) throw new Refusal("invitation acceptance Reducer is invalid", false);
        };
        const store = (db: StorageSession, value: string) => { storeReceipt(db, a.accountId, a.idempotencyKey, "$system.invitation.accept", value); };
        const provenance = { channel: "ui", rootChannel: "ui", commandId: a.commandId, rootCommandId: a.commandId, actorAccountId: a.accountId, actorMemberId: memberId };
        if (a.reducerPath) {
          this.kernel.storage.arm({ provenance, before: prepare, afterSuccess: store });
          try {
            const result = await this.kernel.invoke({ function: a.reducerPath, kind: "reducer",
              args: { accountId: a.accountId, memberId, invitationId: a.invitationId, teamIds: a.teamIds, payload: a.payload },
              identity: identityOf({ projectId, tenantId, account: { id: a.accountId, email: a.accountEmail, name: a.accountName, avatarUrl: a.accountAvatarUrl }, member: { id: memberId, accountId: a.accountId, status: "active", role: a.role, permissions: a.permissions } }),
              invocation: { ...provenance, artifactHash: a.artifactHash }, invitationInviter: inviter,
            });
            if (!result.ok) return { ok: false as const, error: `invalid arguments: ${result.error?.message ?? "invitation acceptance failed"}`, release: true };
          } finally { this.kernel.storage.disarm(); }
        } else {
          const result = await invokeCaptured(this.kernel.storage, "reducer", provenance, async (db) => { prepare(db); store(db, JSON.stringify({ memberId })); }, this.kernel.capture);
          if (!result.ok) throw result.error;
        }
        return { ok: true as const, memberId };
      } catch (error) {
        if (error instanceof Refusal) return { ok: false as const, error: error.message, release: error.release };
        throw error;
      }
    });
  }

}
