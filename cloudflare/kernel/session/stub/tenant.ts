/**
 * Stand-in for the tenant object until K01-K04/K07-K10 build it. It holds what
 * the session layer needs from a tenant: the `members` rows admission reads
 * (C.4 item 4), the tenant's revision, the gateway registry
 * (`TenantGateways`), and a `commit` that fans a batch out once per gateway.
 * Tenant-scoped frames get a fixed "not implemented" error so a client never
 * hangs on them.
 */
import { DurableObject } from "cloudflare:workers";
import type { ClientFrame, Json } from "../decode";
import { frames } from "../protocol";
import type { Member, SocketView } from "../ports";
import { type DeliveryBatch, TenantGateways, type TenantLink } from "../placement/gateway";
import { type GatewayRpc, rpc } from "../rpc";
import { type ConnectionReport, type PresenceEnv, type PresenceMember, presenceConfig } from "../../presence/hub";
import { TenantPresence } from "../../presence/tenant";

export const STUB_NOT_IMPLEMENTED = "not implemented by the K05 tenant stub";

export interface TenantStubEnv extends PresenceEnv {
  GATEWAY: DurableObjectNamespace<any>;
}

export class TenantStubObject extends DurableObject<TenantStubEnv> implements TenantLink {
  private readonly sql: SqlStorage;
  private readonly gateways: TenantGateways;
  /** K14: presence v2 of this tenant (kernel/presence), embedded as the real tenant object will. */
  private readonly presence: TenantPresence;

  constructor(ctx: DurableObjectState, env: TenantStubEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS members (
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, status TEXT NOT NULL, display_name TEXT NOT NULL,
      avatar_url TEXT NOT NULL DEFAULT '', role TEXT NOT NULL, permissions TEXT NOT NULL, membership_revision INTEGER NOT NULL DEFAULT 1)`);
    this.sql.exec("CREATE TABLE IF NOT EXISTS clock (id INTEGER PRIMARY KEY CHECK (id = 1), revision INTEGER NOT NULL)");
    this.sql.exec("INSERT OR IGNORE INTO clock (id, revision) VALUES (1, 0)");
    this.gateways = new TenantGateways(ctx.storage, (name) => rpc<GatewayRpc>(env.GATEWAY, name));
    this.presence = new TenantPresence({
      tenantId: ctx.id.name ?? "",
      config: presenceConfig(env),
      gateway: (name) => rpc<GatewayRpc>(env.GATEWAY, name),
      registeredGateways: () => this.gateways.list(),
      armAlarm: (at) => { void ctx.storage.setAlarm(at); },
      // Presence is the stub's only alarm user, so a set alarm means a grace end was pending.
      lostPending: async () => (await ctx.storage.getAlarm()) !== null,
      waitUntil: (promise) => ctx.waitUntil(promise),
    });
  }

  /** The single alarm: today only presence grace ends use it. */
  override async alarm(): Promise<void> {
    await this.presence.alarm();
    const next = this.presence.nextAlarm();
    if (next !== null) await this.ctx.storage.setAlarm(next);
  }

  /** K14: a socket closed or changed session; its presence leaves with grace. */
  async presenceLeave(connectionId: string, host: string, previous?: ConnectionReport | null): Promise<void> { await this.presence.leave(connectionId, host, previous ?? null); }

  /** K10's subscription reset: this stand-in holds no subscriptions. */
  async connectionReset(_connectionId: string, _epoch: number): Promise<void> {}

  /** Test seam: the tenant's published Members. */
  async presencePublished(projectId: string): Promise<PresenceMember[]> { return this.presence.published(projectId); }

  /** Seeding: an active Member (the e2e route's defaults: role admin, `{"e2e":true}`). */
  async addMember(input: { id: string; accountId: string; displayName: string }): Promise<Member> {
    this.sql.exec(
      "INSERT INTO members (id, account_id, status, display_name, role, permissions) VALUES (?, ?, 'active', ?, 'admin', ?) ON CONFLICT(id) DO NOTHING",
      input.id, input.accountId, input.displayName, JSON.stringify({ e2e: true }),
    );
    this.bump();
    return (await this.admit(input.accountId))!;
  }

  /** `admit_member`: the active Member of an account, read with the revision it covers. */
  async admit(accountId: string): Promise<(Member & { admissionRevision: number }) | null> {
    const row = this.sql.exec<Record<string, string | number>>(
      "SELECT m.*, c.revision FROM members m, clock c WHERE m.account_id = ? AND m.status = 'active' LIMIT 1", accountId,
    ).toArray()[0];
    if (!row) return null;
    return {
      id: String(row.id), accountId: String(row.account_id), status: String(row.status), displayName: String(row.display_name),
      avatarUrl: String(row.avatar_url), role: String(row.role), permissions: JSON.parse(String(row.permissions)) as Json,
      membershipRevision: Number(row.membership_revision), admissionRevision: Number(row.revision),
    };
  }

  /** Test seam: changes a Member's role and reports the commit as a membership change. */
  async setRole(memberId: string, role: string): Promise<{ gateways: number; sockets: number }> {
    this.sql.exec("UPDATE members SET role = ?, membership_revision = membership_revision + 1 WHERE id = ?", role, memberId);
    const revision = this.bump();
    return this.gateways.deliver({ tenantId: this.ctx.id.name ?? "", revision, membershipChanged: [memberId] });
  }

  /** Test and benchmark seam: one commit, fanned out once per gateway. */
  async commit(batch: Omit<DeliveryBatch, "v" | "revision" | "tenantId">): Promise<{ revision: number; gateways: number; sockets: number }> {
    const revision = this.bump();
    return { revision, ...await this.gateways.deliver({ ...batch, tenantId: this.ctx.id.name ?? "", revision }) };
  }

  async attachGateway(gateway: string): Promise<void> { await this.gateways.attach(gateway); }
  async detachGateway(gateway: string): Promise<void> { await this.gateways.detach(gateway); }
  async registeredGateways(): Promise<string[]> { return this.gateways.list(); }

  /** Tenant-scoped frames: owned by K07, K08, K10, K14, K15. */
  async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    switch (frame.type) {
      case "query.call":
      case "query.subscribe": return [frames.queryError(frame.id, frame.path, STUB_NOT_IMPLEMENTED)];
      case "reducer.call": return [frames.reducerError(frame.id, frame.path, STUB_NOT_IMPLEMENTED, "transient", frame.trace)];
      case "action.call": return [frames.actionError(frame.id, frame.path, STUB_NOT_IMPLEMENTED, frame.trace)];
      case "replica.open": return [frames.replicaError(frame.id, frame.path, STUB_NOT_IMPLEMENTED)];
      case "replica.openMany": return frame.opens.map(o => frames.replicaError(o.id, o.path, STUB_NOT_IMPLEMENTED));
      case "presence.set":
      case "presence.subscribe":
      case "presence.unsubscribe": return this.presence.frame(socket, frame);
      default: return [];
    }
  }

  // ---- benchmark seams ----------------------------------------------------------------

  async ping(): Promise<number> { return Date.now(); }

  /** Times `n` sequential tenant -> gateway RPCs (one hop, empty delivery). */
  async hopToGateway(gateway: string, n: number): Promise<number[]> {
    const stub = rpc<GatewayRpc>(this.env.GATEWAY, gateway);
    await stub.ping();
    const out: number[] = [];
    for (let i = 0; i < n; i++) {
      const t = performance.now();
      await stub.ping();
      out.push(performance.now() - t);
    }
    return out;
  }

  private bump(): number {
    return Number(this.sql.exec<{ revision: number }>("UPDATE clock SET revision = revision + 1 WHERE id = 1 RETURNING revision").one().revision);
  }
}
