/// <reference path="../host/src/shims.d.ts" />
// The tenant Durable Object around `KernelTenant`. It speaks K05's
// `TenantRpc` (session layer and its control stand-in), so gateways and the
// control port reach it unchanged, plus the release activation the deploy
// path calls.
//
// A Worker version subclasses it to provide its bundles (0016 G0-2: the code
// is part of the Worker version) and its plan store (K02/K19).

import { DurableObject } from "cloudflare:workers";
import type { ModuleExports } from "../host/src/index.js";
import type { PlanStore } from "../migrate/index.ts";
import type { ClientFrame, Json } from "../session/decode";
import { type DeliveryBatch, TenantGateways } from "../session/placement/gateway";
import type { Member, SocketView } from "../session/ports";
import { type GatewayRpc, rpc } from "../session/rpc";
import type { CommittedCall } from "./path.ts";
import { RECEIPT_PRUNE_INTERVAL_MS } from "./receipts.ts";
import { KernelTenant } from "./tenant.ts";

export interface ReducerTenantEnv {
  GATEWAY: DurableObjectNamespace<any>;
}

/** `members` row as the session layer reads it. */
function memberOf(row: Record<string, SqlStorageValue>): Member {
  return {
    id: String(row.id), accountId: String(row.account_id), status: String(row.status), displayName: String(row.display_name),
    avatarUrl: String(row.avatar_url ?? ""), role: String(row.role), permissions: JSON.parse(String(row.permissions ?? "null")) as Json,
    membershipRevision: Number(row.membership_revision),
  };
}

const NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

export abstract class ReducerTenantObject<E extends ReducerTenantEnv = ReducerTenantEnv> extends DurableObject<E> {
  readonly kernel: KernelTenant;
  protected readonly gateways: TenantGateways;
  /** Committed calls that wrote the outbox. K11 owns the drain and wires its wake here (`committed`). */
  outboxWakes = 0;

  /** The bundle of an artifact hash in this Worker version. */
  protected abstract bundle(artifactHash: string): ModuleExports | undefined;
  /** Where this Worker version reads release plans. */
  protected abstract plans(): PlanStore;

  constructor(ctx: DurableObjectState, env: E) {
    super(ctx, env);
    this.gateways = new TenantGateways(ctx.storage, (name) => rpc<GatewayRpc>(env.GATEWAY, name));
    this.kernel = new KernelTenant({
      ctx,
      bundle: (hash) => this.bundle(hash),
      plans: this.plans(),
      onCommit: (call) => this.committed(call),
      ownMembershipChanged: (socket, revision) => this.ownMembershipChanged(socket, revision),
    });
    ctx.blockConcurrencyWhile(async () => this.kernel.restore());
  }

  protected committed(call: CommittedCall): void {
    if (call.outboxWritten) this.outboxWakes += 1;
  }

  /**
   * lib.rs `membership_changed_frames`: the caller's own Member changed in its
   * commit. The gateways discard the Member's tenant sessions and send the
   * watermark, after the call's reply (the session queues delivery behind the
   * frame it is answering).
   */
  protected ownMembershipChanged(socket: SocketView, revision: number): void {
    const memberId = socket.tenant?.member.id;
    if (!memberId) return;
    const batch = { tenantId: this.ctx.id.name ?? socket.tenant!.route.tenantId, revision, membershipChanged: [memberId] } as Omit<DeliveryBatch, "v">;
    this.ctx.waitUntil(this.gateways.deliver(batch as DeliveryBatch).then(() => undefined, (error) => console.error("reducer: membership delivery failed", error)));
  }

  // ---- deploy (K19 calls this per tenant; the conformance control stand-in does today) ----

  /** Migrate to the release's plan and activate its generation. */
  async activate(projectId: string, manifest: Record<string, unknown>, planId: string | null): Promise<{ ok: true }> {
    await this.kernel.activate(projectId, manifest, planId);
    await this.armReceiptExpiry();
    return { ok: true };
  }

  /**
   * 0009 rule 5: receipt expiry runs on the tenant alarm when the module
   * declares an offline window. The object has one alarm (CONTRACT E.timers):
   * only move it earlier, never later, so other duties (K02's watchdog, K11)
   * keep theirs; each handler checks what is due.
   */
  protected async armReceiptExpiry(now = Date.now()): Promise<void> {
    if (this.kernel.module?.offlineMaxAgeMs == null) return;
    const due = now + RECEIPT_PRUNE_INTERVAL_MS;
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > due) await this.ctx.storage.setAlarm(due);
  }

  // ---- TenantRpc (K05) -------------------------------------------------------------------

  /** Seeding (`/dev/internal/e2e/members`, lib.rs:1301-1326): an active admin Member, one captured commit. */
  async addMember(input: { id: string; accountId: string; displayName: string }): Promise<Member> {
    await this.kernel.reducers.serial(async () => {
      this.kernel.storage.arm({ provenance: { commandId: `e2e-member:${input.accountId}`, channel: "system" } });
      const result = await this.kernel.storage.invoke("reducer", async (session) => {
        const latest = session.run("SELECT id FROM members WHERE account_id=?1 ORDER BY updated_at DESC LIMIT 1", [input.accountId]).rows[0];
        session.run(
          `INSERT INTO members(id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
           VALUES(?1,?2,'active',?3,'','admin','{"e2e":true}',${NOW_MS})
           ON CONFLICT(id) DO UPDATE SET status='active',display_name=EXCLUDED.display_name,
             avatar_url=EXCLUDED.avatar_url,role='admin',permissions='{"e2e":true}',
             membership_revision=members.membership_revision+1,updated_at=${NOW_MS}`,
          [latest ? String(latest[0]) : input.id, input.accountId, input.displayName],
        );
      });
      if (!result.ok) throw result.error;
    });
    return (await this.admit(input.accountId))!;
  }

  /** `admit_member`: the active Member of an account and the revision its read covers. */
  async admit(accountId: string): Promise<(Member & { admissionRevision: number }) | null> {
    const row = this.ctx.storage.sql.exec<Record<string, SqlStorageValue>>(
      `SELECT m.id, m.account_id, m.status, m.display_name, m.avatar_url, m.role, m.permissions, m.membership_revision,
              (SELECT revision FROM _gonvex_sync_clock WHERE singleton = 1) AS revision
       FROM members m WHERE m.account_id = ? AND m.status = 'active' LIMIT 1`, accountId,
    ).toArray()[0];
    if (!row) return null;
    return { ...memberOf(row), admissionRevision: Number(row.revision ?? 0) };
  }

  async attachGateway(gateway: string): Promise<void> { await this.gateways.attach(gateway); }
  async detachGateway(gateway: string): Promise<void> { await this.gateways.detach(gateway); }
  async registeredGateways(): Promise<string[]> { return this.gateways.list(); }

  async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    return this.kernel.dispatch(socket, frame);
  }

  async ping(): Promise<number> { return Date.now(); }

  // ---- test and conformance seams ------------------------------------------------------------

  /** Every row of a table (the conformance adapter's checksums). */
  async readTable(table: string): Promise<Record<string, SqlStorageValue>[]> {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)) throw new Error("invalid table name");
    return this.ctx.storage.sql.exec<Record<string, SqlStorageValue>>(`SELECT * FROM "${table}"`).toArray();
  }

  /** Rows read and written by the last call that ran the module. */
  async lastNumbers(): Promise<unknown> { return this.kernel.reducers.lastNumbers ?? null; }

  /** The tenant alarm's receipt expiry (0009). */
  async pruneReceipts(now = Date.now(), force = true, offlineMaxAgeMs?: number | null): Promise<unknown> {
    return this.kernel.pruneReceipts(now, { force, ...(offlineMaxAgeMs !== undefined ? { offlineMaxAgeMs } : {}) });
  }

  override async alarm(): Promise<void> {
    if (this.kernel.migrator.wantsAlarm()) await this.kernel.migrator.alarm();
    const result = await this.kernel.pruneReceipts(Date.now(), { maxPages: 20 });
    // A run stopped by its page budget continues soon; otherwise the next hourly run.
    if (result && !result.done) await this.ctx.storage.setAlarm(Date.now() + 1_000);
    else await this.armReceiptExpiry();
  }
}
