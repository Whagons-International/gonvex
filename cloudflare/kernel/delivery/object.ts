/// <reference path="../host/src/shims.d.ts" />
// The tenant object with replica delivery: K07's `ReducerTenantObject` plus
// the engine of this package. Commits reach the engine through K03's
// `ChangeWake`, replica frames go to the gateways holding the sockets
// (`GatewayOutlet`), and K07's call watermark takes its floor from here.
//
// A Worker version subclasses it to provide its bundles and plan store, as
// for `ReducerTenantObject`.

import type { ClientFrame } from "../session/decode";
import type { SocketView } from "../session/ports";
import { rpc } from "../session/rpc";
import { snapshotRunner } from "../query/live.ts";
import { MODULE_KEY, type StoredModule } from "../reducer/tenant.ts";
import { ReducerTenantObject, type ReducerTenantEnv } from "../reducer/object.ts";
import { replicaModule, type ReplicaModule } from "./definition.ts";
import { ReplicaDeliveryEngine } from "./engine.ts";
import { GatewayOutlet, type ReplicaHolderRpc } from "./outlet.ts";

export abstract class ReplicaTenantObject<E extends ReducerTenantEnv = ReducerTenantEnv> extends ReducerTenantObject<E> {
  readonly delivery: ReplicaDeliveryEngine;
  private cachedModule: { key: string; module: ReplicaModule } | undefined;
  private dataChangedScheduled = false;

  /**
   * Where the operator dashboard learns of commits (K19 0036 §8, 0037): the operator control object's
   * `operatorDataChanged`. The Worker version that binds it overrides this; without it nothing is sent.
   */
  protected operatorSink(): { operatorDataChanged(projectId: string, tenantId: string): Promise<void> } | undefined {
    return undefined;
  }

  constructor(ctx: DurableObjectState, env: E) {
    super(ctx, env);
    const tenantId = ctx.id.name ?? ctx.id.toString();
    this.delivery = new ReplicaDeliveryEngine({
      tenantId,
      serial: (fn) => this.kernel.reducers.serial(fn),
      read: snapshotRunner(this.kernel.storage),
      module: () => this.replicaModule(),
      outlet: new GatewayOutlet(tenantId, (name) => rpc<ReplicaHolderRpc>(env.GATEWAY, name), () => this.gateways.list()),
      delivered: () => this.dataChanged(tenantId),
    });
    this.kernel.capture.wake.subscribe((wake) => this.delivery.wake(wake.revision));
    // Commits after this point are owed to the sockets; a tenant not provisioned yet has no clock.
    try {
      const row = ctx.storage.sql.exec<{ epoch: string; revision: number }>("SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton = 1").toArray()[0];
      this.delivery.start(row ? { epoch: String(row.epoch), revision: Number(row.revision) } : undefined);
    } catch {
      this.delivery.start(undefined);
    }
  }

  /** One `operatorDataChanged` per 100 ms of delivered commits (as K19's local tenant object coalesces them). */
  private dataChanged(tenantId: string): void {
    const sink = this.operatorSink();
    const projectId = this.kernel.module?.projectId;
    if (!sink || !projectId || this.dataChangedScheduled) return;
    this.dataChangedScheduled = true;
    this.ctx.waitUntil((async () => {
      await new Promise((resolve) => setTimeout(resolve, 100));
      this.dataChangedScheduled = false;
      await sink.operatorDataChanged(projectId, tenantId).catch((error) => console.error("delivery: operatorDataChanged failed", error));
    })());
  }

  /** The replica view of the active generation, rebuilt when the generation changes. */
  private replicaModule(): ReplicaModule | undefined {
    const active = this.kernel.module;
    if (!active) return undefined;
    const key = `${active.projectId}\u0000${active.artifactHash}`;
    if (this.cachedModule?.key !== key) {
      const stored = this.ctx.storage.kv.get<StoredModule>(MODULE_KEY);
      if (!stored) return undefined;
      this.cachedModule = { key, module: replicaModule(stored.projectId, stored.manifest) };
    }
    return this.cachedModule.module;
  }

  override async activate(projectId: string, manifest: Record<string, unknown>, planId: string | null): Promise<{ ok: true }> {
    const result = await super.activate(projectId, manifest, planId);
    // A migration may change tables visibility reads (capture classes, projections).
    this.delivery.registry.invalidateSchema();
    this.cachedModule = undefined;
    return result;
  }

  protected override subscriptionFloor(socket: SocketView): number | undefined {
    return this.delivery.subscriptionFloor(socket);
  }

  override async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    const replica = this.delivery.dispatch(socket, frame);
    if (replica) return replica;
    return super.dispatch(socket, frame);
  }

  /** K05 `TenantPort.connectionReset`: the socket's subscriptions were dropped. */
  async connectionReset(connectionId: string, epoch: number): Promise<void> {
    this.delivery.reset(connectionId, epoch);
  }

  /** Delivery counters (tests and the PR's numbers). */
  async deliveryStats(): Promise<unknown> {
    await this.delivery.settled();
    return { ...this.delivery.stats };
  }
}
