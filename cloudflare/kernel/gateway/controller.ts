import { DurableObject } from "cloudflare:workers";
import {
  COOLDOWN_MS, LEASE_MS, MAX_GATEWAYS_PER_CELL, MAX_PENDING, MAX_SOCKETS, SCALE_IN_MS,
  controllerCell, pressured, quiet, withDeadline,
  type CapacitySample, type GatewayAssignment, type GatewayCapacityRpc,
} from "./placement";

interface Slot {
  name: string;
  epoch: number;
  mode: "active" | "draining" | "retired";
  changedAt: number;
  quietSince: number | null;
  pressuredSince: number | null;
  replacement: string | null;
  nextCheck?: number;
  sample: CapacitySample | null;
}
interface Lease { assignment: GatewayAssignment; claimed: boolean }
interface ScaleEvent { at: number; action: "allocate" | "drain" | "retire"; gateway: string; epoch: number }
interface State { v: 1; epoch: number; slots: Slot[]; leases: Lease[]; events?: ScaleEvent[] }
interface ControllerEnv { GATEWAY: DurableObjectNamespace }

/**
 * One durable owner per pre-auth hash cell, never one fleet-wide allocation DO.
 * KV stores only bounded routing/epoch/admission leases. No tenant/member SQL.
 * Mutations are serialized, including probe -> decision -> durable publication.
 */
export class GatewayControllerObject extends DurableObject<ControllerEnv> {
  private state!: State;
  private tail: Promise<unknown> = Promise.resolve();
  private waiting = 0;
  constructor(ctx: DurableObjectState, env: ControllerEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.state = await ctx.storage.get<State>("placement") ?? { v: 1, epoch: 0, slots: [], leases: [] };
      if (this.state.v !== 1) throw new Error("unsupported placement state");
    });
  }

  private serial<T>(fn: () => Promise<T>): Promise<T> {
    if (this.waiting >= 64) return Promise.reject(new Error("gateway allocation busy"));
    this.waiting++;
    const next = this.tail.then(async () => {
      // Reload after any previous storage/alarm error before publishing another decision.
      this.state = await this.ctx.storage.get<State>("placement") ?? { v: 1, epoch: 0, slots: [], leases: [] };
      return fn();
    }).finally(() => { this.waiting--; });
    this.tail = next.catch(() => undefined);
    return next;
  }
  private event(action: ScaleEvent["action"], slot: Slot): void {
    this.state.events = [...(this.state.events ?? []).slice(-63), { at: Date.now(), action, gateway: slot.name, epoch: slot.epoch }];
  }
  private prune(now: number): void { this.state.leases = this.state.leases.filter(l => l.assignment.expiresAt > now); }
  private cell(): number {
    const match = /^gateway-cell-(\d+)$/.exec(this.ctx.id.name ?? "");
    if (!match || Number(match[1]) >= 32) throw new Error("invalid controller name");
    return Number(match[1]);
  }
  private pending(s: Slot): number { return this.state.leases.filter(l => l.assignment.gateway === s.name && l.assignment.epoch === s.epoch).length; }
  private async save(): Promise<void> {
    if (new TextEncoder().encode(JSON.stringify(this.state)).byteLength > 120 * 1024) throw new Error("gateway routing state byte ceiling reached");
    await this.ctx.storage.put("placement", this.state);
    // Only finite lease cleanup / scale-in deadlines. A stable idle cell has no alarm.
    const deadlines = this.state.leases.map(l => l.assignment.expiresAt);
    for (const s of this.state.slots) {
      if (s.mode === "draining") deadlines.push(s.nextCheck ?? Date.now() + 30_000);
      if (s.mode === "active" && s.quietSince !== null && !this.pending(s)
        && !this.state.slots.some(s => s.mode === "draining")
        && this.state.slots.filter(s => s.mode === "active").length > 1) deadlines.push(Math.max(s.quietSince + SCALE_IN_MS, s.changedAt + COOLDOWN_MS));
    }
    if (deadlines.length) await this.ctx.storage.setAlarm(Math.max(Date.now() + 100, Math.min(...deadlines)));
    else await this.ctx.storage.deleteAlarm();
  }
  private async probe(s: Slot): Promise<CapacitySample> {
    const rpc = this.env.GATEWAY.getByName(s.name) as unknown as GatewayCapacityRpc;
    const sample = await withDeadline(rpc.capacity(s.epoch));
    if (sample.v !== 1 || sample.epoch !== s.epoch) throw new Error("gateway epoch mismatch");
    this.record(s, sample);
    return sample;
  }
  private record(s: Slot, sample: CapacitySample): void {
    if (s.sample && sample.at < s.sample.at) return;
    s.sample = sample;
    if (pressured(sample)) s.pressuredSince ??= Date.now();
    else s.pressuredSince = null;
    if (quiet(sample)) s.quietSince ??= Date.now();
    else s.quietSince = null;
  }

  acquire(connectionId: string): Promise<GatewayAssignment> {
    if (typeof connectionId !== "string" || connectionId.length > 100) return Promise.reject(new Error("invalid placement key"));
    return this.serial(async () => {
      const now = Date.now(), cell = this.cell();
      if (typeof connectionId !== "string" || connectionId.length > 100 || controllerCell(connectionId) !== cell) throw new Error("invalid placement key");
      this.prune(now);
      // Retrying the same acquire does not leak another reservation.
      const prior = this.state.leases.find(l => l.assignment.connectionId === connectionId);
      if (prior) return prior.assignment;
      if (this.state.leases.length >= MAX_PENDING) throw new Error("gateway admissions exhausted");
      const active = this.state.slots.filter(s => s.mode === "active");
      active.sort((a, b) => ((a.sample?.sockets ?? 0) + this.pending(a)) - ((b.sample?.sockets ?? 0) + this.pending(b)));
      let chosen: Slot | undefined;
      // At most two probes per admission, even with a failed/pressured fleet.
      for (const s of active.slice(0, 2)) {
        try {
          const sample = await this.probe(s);
          if (!pressured(sample) && sample.sockets + this.pending(s) < MAX_SOCKETS) { chosen = s; break; }
        } catch { /* Faulted holder retains ownership, but receives no new admission. */ }
      }
      if (!chosen) {
        const retired = this.state.slots.find(s => s.mode === "retired");
        if (!retired && this.state.slots.length >= MAX_GATEWAYS_PER_CELL) throw new Error("gateway resource ceiling reached");
        const epoch = ++this.state.epoch;
        // Reuse a finite set of holder names; the durable epoch fences delayed assignments.
        const slot = retired ? this.state.slots.indexOf(retired) : this.state.slots.length;
        chosen = { name: `auto-gw-${cell}-${slot}`, epoch, mode: "active", changedAt: now, quietSince: now, pressuredSince: null, replacement: null, sample: null };
        if (retired) this.state.slots[slot] = chosen;
        else this.state.slots.push(chosen);
        this.event("allocate", chosen);
      }
      const assignment: GatewayAssignment = { v: 1, cell, gateway: chosen.name, epoch: chosen.epoch, token: crypto.randomUUID(), connectionId, expiresAt: now + LEASE_MS };
      this.state.leases.push({ assignment, claimed: false });
      await this.save(); // Durable before the router can use it.
      this.dispatchDrains();
      return assignment;
    });
  }
  claim(a: GatewayAssignment): Promise<boolean> {
    return this.serial(async () => {
      this.prune(Date.now());
      const lease = this.state.leases.find(l => l.assignment.token === a.token && l.assignment.connectionId === a.connectionId
        && l.assignment.gateway === a.gateway && l.assignment.epoch === a.epoch && l.assignment.cell === a.cell);
      const slot = this.state.slots.find(s => s.name === a.gateway && s.epoch === a.epoch);
      if (!lease || lease.claimed || slot?.mode !== "active") return false;
      lease.claimed = true;
      await this.save();
      return true;
    });
  }
  release(a: GatewayAssignment): Promise<void> {
    return this.serial(async () => {
      // Claimed leases remain until expiry: a lost upgrade response must never free a live socket's budget.
      this.state.leases = this.state.leases.filter(l => l.assignment.token !== a.token || l.assignment.connectionId !== a.connectionId
        || l.assignment.gateway !== a.gateway || l.assignment.epoch !== a.epoch || l.assignment.cell !== a.cell || l.claimed);
      await this.save();
    });
  }
  drainReady(name: string, epoch: number, replacement: string): Promise<boolean> {
    return this.serial(async () => this.state.slots.some(s => s.name === name && s.epoch === epoch && s.mode === "draining" && s.replacement === replacement)
      && this.state.slots.some(s => s.name === replacement && s.mode === "active"));
  }
  observe(name: string, sample: CapacitySample): Promise<void> {
    return this.serial(async () => {
      const s = this.state.slots.find(s => s.name === name && s.epoch === sample.epoch && s.mode !== "retired");
      if (!s || sample.v !== 1 || !Object.values(sample).every(n => typeof n === "number" && Number.isFinite(n) && n >= 0)
        || sample.at > Date.now() + 1_000 || sample.at < Date.now() - 30_000) return;
      this.prune(Date.now());
      this.record(s, sample);
      await this.reconcile();
      await this.save();
      this.dispatchDrains();
    });
  }
  private async reconcile(): Promise<void> {
    const now = Date.now();
    let active = this.state.slots.filter(s => s.mode === "active").length;
    // At most one reconnecting holder per cell; pressure must persist for two sample windows.
    if (!this.state.slots.some(s => s.mode === "draining")) {
      const hot = this.state.slots.find(s => s.mode === "active" && s.pressuredSince !== null
        && now - s.pressuredSince >= 10_000 && now - s.changedAt >= COOLDOWN_MS && !this.pending(s));
      if (hot) {
        let replacement = this.state.slots.find(s => s !== hot && s.mode === "active" && s.sample && !pressured(s.sample));
        if (!replacement) {
          const retired = this.state.slots.find(s => s.mode === "retired");
          if (retired || this.state.slots.length < MAX_GATEWAYS_PER_CELL) {
            const index = retired ? this.state.slots.indexOf(retired) : this.state.slots.length;
            replacement = { name: `auto-gw-${this.cell()}-${index}`, epoch: ++this.state.epoch, mode: "active", changedAt: now,
              quietSince: now, pressuredSince: null, replacement: null, sample: null };
            if (retired) this.state.slots[index] = replacement; else this.state.slots.push(replacement);
            this.event("allocate", replacement);
            active++;
          }
        }
        if (replacement) {
          try {
            // Fresh successful probe, never merely an allocation record, before any close.
            const healthy = await this.probe(replacement);
            if (!pressured(healthy)) {
              hot.mode = "draining"; hot.nextCheck = now + 30_000; hot.changedAt = now; hot.quietSince = null; hot.replacement = replacement.name; active--; this.event("drain", hot);
            }
          } catch { /* Replacement failure pauses the rebalance; no sockets are closed. */ }
        }
      }
    }
    for (const s of this.state.slots) {
      let consideredScaleIn = false;
      if (s.mode === "active" && active > 1 && !this.state.slots.some(s => s.mode === "draining") && s.quietSince !== null
        && now - s.quietSince >= SCALE_IN_MS && now - s.changedAt >= COOLDOWN_MS && !this.pending(s)) {
        const replacement = this.state.slots.find(other => other !== s && other.mode === "active");
        if (replacement) {
          consideredScaleIn = true;
          try {
            if (!pressured(await this.probe(replacement))) {
              s.mode = "draining"; s.nextCheck = now + 30_000; s.changedAt = now; s.quietSince = null; s.replacement = replacement.name; active--; this.event("drain", s);
            } else s.quietSince = now;
          } catch { s.quietSince = now; }
        }
      }
      if (s.mode === "draining" && !this.pending(s)) {
        s.nextCheck = now + 30_000;
        try {
          if ((await this.probe(s)).sockets === 0) { s.mode = "retired"; s.sample = null; s.quietSince = null; s.replacement = null; this.event("retire", s); }
        } catch { /* Never infer empty from failure; retry on the next active report/admission. */ }
      }
      if (s.mode === "active" && active === 1) s.quietSince = null;
      if (consideredScaleIn) break; // At most one scale-in candidate probe per control event.
    }
  }
  private dispatchDrains(): void {
    for (const s of this.state.slots) {
      if (s.mode !== "draining" || !s.replacement) continue;
      const rpc = this.env.GATEWAY.getByName(s.name) as unknown as GatewayCapacityRpc;
      // Publication is durable before dispatch. A lost command is retried by a later active report.
      this.ctx.waitUntil(withDeadline(rpc.drain({ v: 1, epoch: s.epoch, replacement: s.replacement,
        replacementEpoch: this.state.slots.find(slot => slot.name === s.replacement)!.epoch, notBefore: s.changedAt + 30_000 })).catch(() => undefined));
    }
  }

  override alarm(): Promise<void> {
    return this.serial(async () => {
      this.prune(Date.now());
      await this.reconcile();
      await this.save();
      this.dispatchDrains();
    });
  }
  /** Bounded diagnostics, no per-tenant/member/token labels. */
  status(): Promise<{ v: 1; epoch: number; pending: number; events: ScaleEvent[]; slots: Omit<Slot, "quietSince">[] }> {
    return this.serial(async () => ({ v: 1, epoch: this.state.epoch, pending: this.state.leases.length, events: this.state.events ?? [],
      slots: this.state.slots.map(({ quietSince: _, ...s }) => s) }));
  }
}
