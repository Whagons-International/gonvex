import { DurableObject } from "cloudflare:workers";
import {
  COOLDOWN_MS, LEASE_MS, MAX_GATEWAYS_PER_CELL, MAX_PENDING, MAX_SOCKETS, SCALE_IN_MS, SCALE_IN_GRACE_MS, SCALE_IN_CHECK_MS, PRESSURE_DRAIN_START_MS,
  controllerCell, pressured, quiet, withDeadline, checkedCapacitySample, completedWindow, windowPressured, freshWindow, WINDOW_FIELDS,
  type CapacitySample, type GatewayAssignment, type GatewayCapacityRpc,
} from "./placement";

interface Slot {
  name: string;
  epoch: number;
  mode: "active" | "draining" | "retired";
  changedAt: number;
  quietSince: number | null;
  pressuredSince: number | null;
  pressureEvidence?: { epoch: number; window: readonly number[]; since: number };
  replacement: string | null;
  nextCheck?: number;
  sample: CapacitySample | null;
  /** BUG-9: why the slot drains. Absent (older state): a pressure drain, the previous behavior. */
  drainKind?: "pressure" | "scale-in";
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
    const sample = checkedCapacitySample(await withDeadline(rpc.capacity(s.epoch)), Date.now());
    if (!sample || sample.epoch !== s.epoch) throw new Error("gateway epoch mismatch");
    this.record(s, sample);
    return sample;
  }
  private record(s: Slot, sample: CapacitySample): void {
    if (s.sample && sample.at < s.sample.at) return;
    const previous = completedWindow(s.sample), window = completedWindow(sample);
    if (previous && window && ((window.windowInstance === previous.windowInstance
      && (window.windowSequence < previous.windowSequence
        || (window.windowSequence === previous.windowSequence && WINDOW_FIELDS.some(key => window[key] !== previous[key]))
        || (window.windowSequence > previous.windowSequence && window.windowStart < previous.windowEnd)))
      || (window.windowInstance !== previous.windowInstance && window.windowStart < previous.windowEnd))) {
      s.pressuredSince = null; delete s.pressureEvidence; // Keep the high-water observation; a restamped/revised old bucket is not new evidence.
      return;
    }
    const wasHot = s.sample?.currentPressure === 1 && previous && windowPressured(previous)
      && this.ownsEvidence(s, previous);
    s.sample = sample;
    if (window && freshWindow(window, Date.now()) && sample.currentPressure === 1 && windowPressured(window)) {
      const repeated = previous && window.windowInstance === previous.windowInstance && window.windowSequence === previous.windowSequence;
      const adjacent = previous && window.windowInstance === previous.windowInstance
        && window.windowSequence === previous.windowSequence + 1 && window.windowStart === previous.windowEnd;
      if (!wasHot || s.pressuredSince === null || (!repeated && !adjacent)) s.pressuredSince = window.windowStart;
      s.pressureEvidence = { epoch: s.epoch, window: Object.freeze(WINDOW_FIELDS.map(key => window[key])), since: s.pressuredSince };
    } else { s.pressuredSince = null; delete s.pressureEvidence; }
    if (quiet(sample)) s.quietSince ??= Date.now();
    else s.quietSince = null;
  }
  private ownsEvidence(s: Slot, w: NonNullable<ReturnType<typeof completedWindow>>): boolean {
    const e = s.pressureEvidence;
    return Boolean(e && e.epoch === s.epoch && e.since === s.pressuredSince && Number.isSafeInteger(e.since)
      && e.since >= 0 && e.since <= w.windowStart && Array.isArray(e.window) && e.window.length === WINDOW_FIELDS.length
      && WINDOW_FIELDS.every((key, index) => e.window[index] === w[key]));
  }
  private trustedPressure(s: Slot, now: number): boolean {
    const w = completedWindow(s.sample);
    return Boolean(w && this.ownsEvidence(s, w) && freshWindow(w, now)
      && s.sample?.currentPressure === 1 && windowPressured(w));
  }
  private mature(s: Slot, now: number): boolean {
    return this.trustedPressure(s, now) && completedWindow(s.sample)!.windowEnd - s.pressureEvidence!.since >= 10_000;
  }
  private allocate(now: number): Slot {
    const retired = this.state.slots.find(s => s.mode === "retired");
    if (!retired && this.state.slots.length >= MAX_GATEWAYS_PER_CELL) throw new Error("gateway resource ceiling reached");
    const index = retired ? this.state.slots.indexOf(retired) : this.state.slots.length;
    const slot: Slot = { name: `auto-gw-${this.cell()}-${index}`, epoch: ++this.state.epoch, mode: "active",
      changedAt: now, quietSince: now, pressuredSince: null, replacement: null, sample: null };
    if (retired) this.state.slots[index] = slot; else this.state.slots.push(slot);
    this.event("allocate", slot);
    return slot;
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
      if (!chosen) chosen = this.allocate(now);
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
  observe(name: string, input: CapacitySample): Promise<void> {
    return this.serial(async () => {
      const sample = checkedCapacitySample(input, Date.now());
      if (!sample) return;
      const s = this.state.slots.find(s => s.name === name && s.epoch === sample.epoch && s.mode !== "retired");
      if (!s) return;
      this.prune(Date.now());
      this.record(s, sample);
      await this.reconcile();
      await this.save();
      this.dispatchDrains();
    });
  }
  private async reconcile(): Promise<void> {
    const now = Date.now();
    for (const s of this.state.slots) {
      if (!this.trustedPressure(s, now)) { s.pressuredSince = null; delete s.pressureEvidence; }
    }
    // One reconnecting drain per cell, and at most TWO holder probes per control event. A scale-in holder
    // still inside its natural-disconnect grace reconnects nobody, so it does not block pressure decisions.
    const draining = this.state.slots.filter(s => s.mode === "draining");
    // Review r4609054691: a drain's replacement may itself drain (pressure B -> C) or retire. Re-point each
    // draining holder at a live active target, so drainReady accepts it and every waiting drain can finish.
    for (const slot of draining) this.repoint(slot, now);
    // Review r4609054682: every overdue schedule advances this round, whichever slot is probed, so save()
    // never re-arms an alarm at an expired deadline.
    const overdue = new Set(draining.filter(s => (s.nextCheck ?? 0) <= now));
    for (const slot of overdue) slot.nextCheck = now + (this.waitingScaleIn(slot, now) ? SCALE_IN_CHECK_MS : 30_000);
    const reconnecting = draining.find(s => !this.waitingScaleIn(s, now));
    if (reconnecting) {
      if (!this.pending(reconnecting)) {
        reconnecting.nextCheck = now + 30_000;
        try { if ((await this.probe(reconnecting)).sockets === 0) this.retire(reconnecting); }
        catch { /* Failure is not proof of empty ownership. */ }
      }
      return;
    }
    const due = draining.find(s => !this.pending(s) && (overdue.has(s) || s.sample?.sockets === 0));
    if (due) {
      due.nextCheck = now + SCALE_IN_CHECK_MS;
      try { if ((await this.probe(due)).sockets === 0) this.retire(due); }
      catch { /* Failure is not proof of empty ownership. */ }
      return;
    }
    const hot = this.state.slots.find(s => s.mode === "active" && this.mature(s, now)
      && now - s.changedAt >= COOLDOWN_MS && !this.pending(s));
    if (hot) {
      let current: CapacitySample;
      try { current = await this.probe(hot); } catch { hot.pressuredSince = null; delete hot.pressureEvidence; return; }
      if (hot.sample !== current || current.currentPressure !== 1 || !this.mature(hot, Date.now())) {
        hot.pressuredSince = null; delete hot.pressureEvidence; return;
      } // Revalidate BEFORE allocation/publication, without counting a bucket twice.
      let replacement = this.state.slots.find(s => s !== hot && s.mode === "active" && s.sample && !pressured(s.sample));
      if (!replacement) {
        if (!this.state.slots.some(s => s.mode === "retired") && this.state.slots.length >= MAX_GATEWAYS_PER_CELL) return;
        replacement = this.allocate(now);
      }
      try { if (!pressured(await this.probe(replacement))) this.beginDrain(hot, replacement, now, "pressure"); }
      catch { /* Replacement failure never authorizes a reconnect. */ }
      return;
    }
    // One scale-in at a time: a waiting holder already keeps its quiet sockets.
    if (draining.length) return;
    const active = this.state.slots.filter(s => s.mode === "active");
    if (active.length === 1) { active[0]!.quietSince = null; return; }
    const low = active.find(s => s.quietSince !== null && now - s.quietSince >= SCALE_IN_MS
      && now - s.changedAt >= COOLDOWN_MS && !this.pending(s));
    if (!low) return;
    try {
      const current = await this.probe(low);
      if (!quiet(current)) return;
      const replacement = active.find(s => s !== low)!;
      if (!pressured(await this.probe(replacement))) {
        this.beginDrain(low, replacement, now, "scale-in");
        // Fresh empty probe + no lease: no third probe or pointless reconnect is needed.
        if (current.sockets === 0) this.retire(low);
      } else low.quietSince = now;
    } catch { low.quietSince = now; }
  }
  private beginDrain(slot: Slot, replacement: Slot, now: number, kind: "pressure" | "scale-in"): void {
    slot.mode = "draining"; slot.drainKind = kind; slot.changedAt = now;
    slot.nextCheck = now + (kind === "scale-in" ? SCALE_IN_CHECK_MS : 30_000);
    slot.quietSince = null; slot.replacement = replacement.name; this.event("drain", slot);
  }
  /** The live active target of a draining slot: its replacement, else the end of that slot's own drain chain,
   * else any other active slot, else a new allocation (bounded by the cell's ceiling). */
  private repoint(slot: Slot, now: number): void {
    const byName = (name: string | null) => (name === null ? undefined : this.state.slots.find(s => s.name === name));
    let target = byName(slot.replacement);
    for (let hops = 0; target && target.mode === "draining" && target !== slot && hops < this.state.slots.length; hops++) target = byName(target.replacement);
    if (!target || target === slot || target.mode !== "active") {
      target = this.state.slots.find(s => s !== slot && s.mode === "active");
      if (!target && (this.state.slots.some(s => s.mode === "retired") || this.state.slots.length < MAX_GATEWAYS_PER_CELL)) target = this.allocate(now);
    }
    if (target && target.name !== slot.replacement) slot.replacement = target.name;
  }
  /** A scale-in drain inside its grace: no admissions, no reconnects; it retires when its sockets close. */
  private waitingScaleIn(slot: Slot, now: number): boolean {
    return slot.mode === "draining" && slot.drainKind === "scale-in" && now < slot.changedAt + SCALE_IN_GRACE_MS;
  }
  /** When the holder may start its bounded 1012 cohort (0054): 30 s for pressure, after the grace for scale-in. */
  private drainStart(slot: Slot): number {
    return slot.changedAt + (slot.drainKind === "scale-in" ? SCALE_IN_GRACE_MS : PRESSURE_DRAIN_START_MS);
  }
  private retire(slot: Slot): void {
    slot.mode = "retired"; slot.sample = null; slot.quietSince = null; delete slot.drainKind;
    slot.pressuredSince = null; delete slot.pressureEvidence; slot.replacement = null; this.event("retire", slot);
  }
  private dispatchDrains(): void {
    for (const s of this.state.slots) {
      if (s.mode !== "draining" || !s.replacement) continue;
      const rpc = this.env.GATEWAY.getByName(s.name) as unknown as GatewayCapacityRpc;
      // Publication is durable before dispatch. A lost command is retried by a later active report.
      this.ctx.waitUntil(withDeadline(rpc.drain({ v: 1, epoch: s.epoch, replacement: s.replacement,
        replacementEpoch: this.state.slots.find(slot => slot.name === s.replacement)!.epoch, notBefore: this.drainStart(s) })).catch(() => undefined));
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
