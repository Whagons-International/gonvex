/**
 * A tenant object with only what the control object needs: the reference
 * tenant identity schema (store/src/provision.rs TENANT_IDENTITY_SQL, with the
 * membership outbox triggers), the change-feed clock and a minimal receipt
 * table. It implements `TenantPort` with the reference's SQL so the control
 * object can be tested end to end before the real tenant object (K01/K02/K04)
 * exists, and it pushes membership outbox rows to the control object after
 * each commit, retrying from its alarm (contract C.4 item 6).
 *
 * It is test support, not the tenant object: it runs no module code, so an
 * invitation that names an acceptance Reducer fails closed here.
 */
import { DurableObject } from "cloudflare:workers";
import type { ControlObject } from "../src/control-object";
import { NOW_MS, Sql, json, num, str, type Row } from "../src/sql";
import type {
  Admission,
  InvitationAcceptance,
  InvitationAcceptanceResult,
  MembershipOutboxRow,
  MembershipWrite,
  MembershipWriteResult,
  TenantPort,
} from "../src/tenant-port";
import type { Member } from "../src/types";
import { uuidSimple } from "../src/util";

const TENANT_IDENTITY_SQL = `
CREATE TABLE IF NOT EXISTS members (
  id TEXT PRIMARY KEY, account_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'active',
  display_name TEXT NOT NULL DEFAULT '', avatar_url TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'member', permissions JSON NOT NULL DEFAULT '{}',
  membership_revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS members_by_role ON members(role);
CREATE INDEX IF NOT EXISTS members_by_status ON members(status,id);
CREATE TABLE IF NOT EXISTS _gonvex_control_plane_membership_outbox (
  account_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, status TEXT NOT NULL,
  membership_revision INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_insert;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_update;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_delete;
CREATE TRIGGER gonvex_queue_control_plane_membership_insert AFTER INSERT ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(NEW.account_id,NEW.id,NEW.status,NEW.membership_revision,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TRIGGER gonvex_queue_control_plane_membership_update AFTER UPDATE ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(NEW.account_id,NEW.id,NEW.status,NEW.membership_revision,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TRIGGER gonvex_queue_control_plane_membership_delete AFTER DELETE ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(OLD.account_id,OLD.id,'revoked',OLD.membership_revision+1,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TABLE IF NOT EXISTS _gonvex_sync_clock (singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL);
INSERT OR IGNORE INTO _gonvex_sync_clock(singleton,revision) VALUES(1,0);
CREATE TABLE IF NOT EXISTS _gonvex_reducer_idempotency (
  account_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, path TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending', result JSON,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(account_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS _stub_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _stub_commands (revision INTEGER PRIMARY KEY, command_id TEXT NOT NULL);
`;

interface StubEnv {
  CONTROL: DurableObjectNamespace<ControlObject>;
}

function memberFrom(row: Row): Member {
  return {
    id: str(row.id),
    accountId: str(row.account_id),
    status: str(row.status),
    displayName: str(row.display_name),
    avatarUrl: str(row.avatar_url),
    role: str(row.role),
    permissions: json(row.permissions),
    membershipRevision: num(row.membership_revision),
  };
}

const PUSH_RETRY_MS = 2000;

export class StubTenantObject extends DurableObject<StubEnv> implements TenantPort {
  private readonly sql: Sql;

  constructor(state: DurableObjectState, env: StubEnv) {
    super(state, env);
    this.sql = new Sql(state.storage);
    state.blockConcurrencyWhile(async () => this.sql.script(TENANT_IDENTITY_SQL));
  }

  private meta(key: string): string | undefined {
    return this.sql.value<string>("SELECT value FROM _stub_meta WHERE key=$1", key) as string | undefined;
  }

  /** One committed tenant transaction: bumps the change-feed clock and records its command id. */
  private commit<T>(commandId: string, body: () => T): T {
    return this.sql.tx(() => {
      const result = body();
      const revision = num(this.sql.value("UPDATE _gonvex_sync_clock SET revision=revision+1 WHERE singleton=1 RETURNING revision"));
      this.sql.run("INSERT INTO _stub_commands(revision,command_id) VALUES($1,$2)", revision, commandId);
      return result;
    });
  }

  /** Pushes outbox rows to the control object and deletes what it acknowledged. */
  private async pushMemberships(): Promise<void> {
    const projectId = this.meta("project");
    const tenantId = this.meta("tenant");
    if (!projectId || !tenantId) return;
    for (;;) {
      const rows: MembershipOutboxRow[] = this.sql
        .all("SELECT account_id,member_id,status,membership_revision FROM _gonvex_control_plane_membership_outbox ORDER BY membership_revision,account_id LIMIT 1000")
        .map((row) => ({ accountId: str(row.account_id), memberId: str(row.member_id), status: str(row.status), membershipRevision: num(row.membership_revision) }));
      if (rows.length === 0) return;
      try {
        await this.env.CONTROL.getByName("control").projectMemberships(projectId, tenantId, rows);
      } catch {
        await this.ctx.storage.setAlarm(Date.now() + PUSH_RETRY_MS);
        return;
      }
      this.sql.tx(() => {
        for (const row of rows) {
          this.sql.run("DELETE FROM _gonvex_control_plane_membership_outbox WHERE account_id=$1 AND membership_revision<=$2", row.accountId, row.membershipRevision);
        }
      });
    }
  }

  override async alarm(): Promise<void> {
    await this.pushMemberships();
  }

  private afterCommit(): void {
    this.ctx.waitUntil(this.pushMemberships());
  }

  // ---------------------------------------------------------------- TenantPort

  async provision(projectId: string, tenantId: string): Promise<void> {
    this.sql.tx(() => {
      this.sql.run("INSERT OR REPLACE INTO _stub_meta(key,value) VALUES('project',$1)", projectId);
      this.sql.run("INSERT OR REPLACE INTO _stub_meta(key,value) VALUES('tenant',$1)", tenantId);
    });
  }

  async admit(accountId: string): Promise<Admission | null> {
    const row = this.sql.get(
      `SELECT id, account_id, status, display_name, avatar_url, role, permissions, membership_revision
       FROM members WHERE account_id = $1 AND status = 'active'`,
      accountId,
    );
    if (!row) return null;
    return { member: memberFrom(row), revision: num(this.sql.value("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1")) };
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
    const result = this.commit(write.commandId, (): MembershipWriteResult => {
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
    this.afterCommit();
    return result;
  }

  async acceptInvitation(acceptance: InvitationAcceptance): Promise<InvitationAcceptanceResult> {
    class Refusal extends Error {
      constructor(message: string, readonly release: boolean) {
        super(message);
      }
    }
    let committed = false;
    try {
      const memberId = this.sql.tx(() => {
        // `claim_reducer`: a first claim runs; an existing receipt means already committed.
        const claimed =
          this.sql.affected(
            "INSERT INTO _gonvex_reducer_idempotency(account_id,idempotency_key,path) VALUES($1,$2,'$system.invitation.accept') ON CONFLICT DO NOTHING",
            acceptance.accountId,
            acceptance.idempotencyKey,
          ) === 1;
        const memberId =
          (this.sql.value<string>("SELECT id FROM members WHERE account_id=$1 ORDER BY updated_at DESC LIMIT 1", acceptance.accountId) as string | undefined) ??
          `member_${acceptance.invitationId}`;
        if (!claimed) return memberId;
        // Re-admit the inviter inside this transaction (control.rs:1728-1767).
        const denial = () => new Refusal("invalid arguments: invitation inviter can no longer issue these membership grants", true);
        if (acceptance.inviterMemberId === null) throw denial();
        const inviter = this.sql.get("SELECT status,role,permissions FROM members WHERE id=$1 AND account_id=$2", acceptance.inviterMemberId, acceptance.inviterAccountId);
        if (!inviter) throw denial();
        if (!["owner", "admin", "member", "viewer"].includes(acceptance.role)) throw new Refusal("invalid arguments: role is invalid", true);
        const status = str(inviter.status);
        const role = str(inviter.role);
        if (status !== "active" || !["owner", "admin"].includes(role) || (role === "admin" && ["owner", "admin"].includes(acceptance.role))) throw denial();
        this.sql.run(
          `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
           VALUES($1,$2,'active',$3,$4,$5,$6,${NOW_MS})
           ON CONFLICT(id) DO UPDATE SET account_id=EXCLUDED.account_id,status='active',
             display_name=EXCLUDED.display_name,avatar_url=EXCLUDED.avatar_url,
             role=EXCLUDED.role,permissions=EXCLUDED.permissions,
             membership_revision=members.membership_revision+1,updated_at=${NOW_MS}`,
          memberId,
          acceptance.accountId,
          acceptance.accountName,
          acceptance.accountAvatarUrl,
          acceptance.role,
          JSON.stringify(acceptance.permissions),
        );
        if (!acceptance.moduleAvailable) throw new Refusal("invalid arguments: project module is unavailable", false);
        const hasPayload =
          (Array.isArray(acceptance.teamIds) && acceptance.teamIds.length > 0) ||
          (!!acceptance.payload && typeof acceptance.payload === "object" && !Array.isArray(acceptance.payload) && Object.keys(acceptance.payload).length > 0);
        if (!acceptance.reducerPath && hasPayload) {
          throw new Refusal("invalid arguments: module must declare invitationAcceptance for application invitation payloads", false);
        }
        if (acceptance.reducerPath) {
          const definition = acceptance.reducerDefinition;
          if (!definition || definition.kind !== "reducer" || !definition.internal) throw new Refusal("invalid arguments: invitation acceptance Reducer is invalid", false);
          throw new Refusal("invalid arguments: the control test stub cannot run module Reducers", true);
        }
        this.sql.run(
          "UPDATE _gonvex_reducer_idempotency SET state='completed',result=$3 WHERE account_id=$1 AND idempotency_key=$2",
          acceptance.accountId,
          acceptance.idempotencyKey,
          JSON.stringify({ memberId }),
        );
        const revision = num(this.sql.value("UPDATE _gonvex_sync_clock SET revision=revision+1 WHERE singleton=1 RETURNING revision"));
        this.sql.run("INSERT INTO _stub_commands(revision,command_id) VALUES($1,$2)", revision, acceptance.commandId);
        committed = true;
        return memberId;
      });
      if (committed) this.afterCommit();
      return { ok: true, memberId };
    } catch (error) {
      if (error instanceof Refusal) return { ok: false, error: error.message, release: error.release };
      throw error;
    }
  }

  // ------------------------------------------------------------ test helpers

  /** Every row of `members` (conformance `readTable`). */
  async readTable(table: string): Promise<Array<Record<string, unknown>>> {
    if (table !== "members") return [];
    return this.sql.all("SELECT id,account_id,status,display_name,avatar_url,role,permissions,membership_revision FROM members ORDER BY id");
  }

  async outboxSize(): Promise<number> {
    return num(this.sql.value("SELECT COUNT(*) FROM _gonvex_control_plane_membership_outbox"));
  }

  async setMemberStatus(memberId: string, status: string): Promise<void> {
    this.commit(`test-member-status:${memberId}`, () =>
      this.sql.run(`UPDATE members SET status=$2,membership_revision=membership_revision+1,updated_at=${NOW_MS} WHERE id=$1`, memberId, status),
    );
    this.afterCommit();
  }

  async flushMemberships(): Promise<void> {
    await this.pushMemberships();
  }
}
