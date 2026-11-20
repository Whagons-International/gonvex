/** Internal routing only. No SDK/auth/tenant identity is needed before upgrade. */
export const PLACEMENT_VERSION = 1;
export const CONTROLLER_CELLS = 32;
export const ASSIGNMENT_HEADER = "x-gonvex-gateway-assignment";
/**
 * X17g: set by the router only (a client's copy is removed). The upgrade goes straight to the gateway the router's
 * isolate admitted its last upgrade to; that gateway admits it with one controller RPC (`admitDirect`) and the address
 * limiter in parallel, instead of the router's controller `acquire`, limiter `admit` and the gateway's `claim` in turn.
 */
export const DIRECT_HEADER = "x-gonvex-gateway-direct";
export const LEASE_MS = 30_000;
export const MAX_PENDING = 256;
export const MAX_GATEWAYS_PER_CELL = 64;
export const MAX_SOCKETS = 125; // S3 starting point, not measured full-app capacity.
export const SAMPLE_MS = 5_000;
export const SCALE_IN_MS = 120_000;
export const COOLDOWN_MS = 60_000;
/**
 * BUG-9 / 0054: normal scale-in waits for natural disconnect before any reconnect cohort. A quiet holder stops
 * taking admissions at once and retires when empty; only after this grace may its remaining sockets get 1012.
 * Pressure (hot-holder) drains keep their 30 s start.
 */
export const SCALE_IN_GRACE_MS = 6 * 60 * 60_000;
export const PRESSURE_DRAIN_START_MS = 30_000;
/** How often a waiting scale-in holder is probed for emptiness (an empty report probes it at once). */
export const SCALE_IN_CHECK_MS = 300_000;

export interface GatewayAssignment {
  v: 1;
  cell: number;
  gateway: string;
  epoch: number;
  token: string;
  connectionId: string;
  expiresAt: number;
}
/** X17g: what the router asks a gateway to admit directly (no token: the gateway's `admitDirect` creates the lease). */
export interface DirectAdmission {
  v: 1; cell: number; gateway: string; epoch: number; connectionId: string;
  /** #538 lead check: only callers that send this get `{ refused }`; older holders keep getting null on refusal. */
  wantReason?: true;
}
/** #538 re-review: why `admitDirect` did not grant a lease (an older controller answers null). */
export interface DirectRefusal { refused: "invalid" | "prior" | "pending-cap" | "slot" | "pressured" | "full" }
export function isDirectRefusal(value: unknown): value is DirectRefusal { return !!value && typeof (value as DirectRefusal).refused === "string"; }
export interface GatewayPlacement {
  acquire(connectionId: string): Promise<GatewayAssignment>;
  release(assignment: GatewayAssignment): Promise<void>;
}
/** Numeric-only additive wire fields: bc247 controllers accept every value as numeric telemetry. */
export interface CapacityWindow {
  windowInstance: number;
  windowSequence: number;
  windowStart: number;
  windowEnd: number;
  /** Minimum explicitly sampled gauge in the bucket; zero samples means unknown, not empty. */
  windowSockets: number;
  windowSocketSamples: number;
  windowFramesPerSecond: number;
  windowBytesPerSecond: number;
  windowDeliveryMs: number;
  windowQueueAgeMs: number;
  windowRpcMs: number;
}
export const WINDOW_FIELDS = ["windowInstance", "windowSequence", "windowStart", "windowEnd", "windowSockets", "windowSocketSamples",
  "windowFramesPerSecond", "windowBytesPerSecond", "windowDeliveryMs", "windowQueueAgeMs", "windowRpcMs"] as const;
export interface CapacitySample extends Partial<CapacityWindow> {
  /** Current gauge/latency and one reporting interval of throughput, not admission maxima. */
  currentPressure?: 0 | 1;
  v: 1;
  epoch: number;
  at: number;
  sockets: number;
  framesPerSecond: number;
  bytesPerSecond: number;
  deliveryMs: number;
  queueAgeMs: number;
  rpcMs: number;
}
export interface DrainCommand { v: 1; epoch: number; replacement: string; replacementEpoch: number; notBefore: number }
export interface DrainResult { closed: number; blocked: Record<string, number>; remaining: number }
export interface GatewayCapacityRpc {
  capacity(epoch: number): Promise<CapacitySample>;
  drain(command: DrainCommand): Promise<DrainResult>;
}
export interface ControllerRpc {
  acquire(connectionId: string): Promise<GatewayAssignment>;
  release(assignment: GatewayAssignment): Promise<void>;
  claim(assignment: GatewayAssignment): Promise<boolean>;
  /** X17g: a claimed lease on `gateway` at `epoch` for `connectionId`, or null when that holder must not take it. */
  admitDirect(request: DirectAdmission, sockets: number): Promise<GatewayAssignment | DirectRefusal | null>;
  /** X17g follow-up: removes a lease `admitDirect` granted for `request` after the holder had given up on it. */
  cancelDirect(request: DirectAdmission): Promise<boolean>;
  drainReady(gateway: string, epoch: number, replacement: string): Promise<boolean>;
  observe(gateway: string, sample: CapacitySample): Promise<void>;
}
export interface AutoscaleEnv { GATEWAY_CONTROLLER?: DurableObjectNamespace }

export function hashConnection(id: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) h = Math.imul(h ^ id.charCodeAt(i), 0x01000193) >>> 0;
  return h;
}
export function controllerCell(id: string): number { return hashConnection(id) % CONTROLLER_CELLS; }
export function controller(env: AutoscaleEnv, cell: number): ControllerRpc {
  if (!env.GATEWAY_CONTROLLER || !Number.isInteger(cell) || cell < 0 || cell >= CONTROLLER_CELLS) throw new Error("gateway controller unavailable");
  return env.GATEWAY_CONTROLLER.getByName(`gateway-cell-${cell}`) as unknown as ControllerRpc;
}
export function automaticPlacement(env: AutoscaleEnv): GatewayPlacement | undefined {
  if (!env.GATEWAY_CONTROLLER) return undefined; // explicit static adapter for old deployments/tests only
  return {
    acquire: id => controller(env, controllerCell(id)).acquire(id),
    release: a => controller(env, a.cell).release(a),
  };
}
export function parseAssignment(text: string | null): GatewayAssignment | undefined {
  if (!text || text.length > 1024) return undefined;
  try {
    const a = JSON.parse(text) as GatewayAssignment;
    if (a.v !== 1 || !Number.isInteger(a.cell) || a.cell < 0 || a.cell >= CONTROLLER_CELLS
      || !Number.isSafeInteger(a.epoch) || a.epoch < 1 || !Number.isFinite(a.expiresAt)
      || typeof a.gateway !== "string" || a.gateway.length > 100
      || typeof a.connectionId !== "string" || a.connectionId.length > 100
      || typeof a.token !== "string" || a.token.length > 100) return undefined;
    return a;
  } catch { return undefined; }
}

export function parseDirect(text: string | null): DirectAdmission | undefined {
  if (!text || text.length > 512) return undefined;
  try {
    const d = JSON.parse(text) as DirectAdmission;
    if (d.v !== 1 || !Number.isInteger(d.cell) || d.cell < 0 || d.cell >= CONTROLLER_CELLS
      || !Number.isSafeInteger(d.epoch) || d.epoch < 1
      || typeof d.gateway !== "string" || d.gateway.length > 100
      || typeof d.connectionId !== "string" || d.connectionId.length > 100
      || controllerCell(d.connectionId) !== d.cell) return undefined;
    return { v: 1, cell: d.cell, gateway: d.gateway, epoch: d.epoch, connectionId: d.connectionId };
  } catch { return undefined; }
}

/** Validate/copy bounded internal telemetry. Unknown numeric additions remain skew-compatible but are not stored. */
export function checkedCapacitySample(input: CapacitySample, now: number): CapacitySample | undefined {
  const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n) && n >= 0 && n <= Number.MAX_SAFE_INTEGER;
  if (!input || Object.keys(input).length > 32 || !Object.values(input).every(finite)) return undefined;
  const base = [input.v, input.epoch, input.at, input.sockets, input.framesPerSecond, input.bytesPerSecond, input.deliveryMs, input.queueAgeMs, input.rpcMs];
  if (!base.every(finite) || input.v !== 1 || !Number.isSafeInteger(input.epoch) || input.epoch < 1
    || !Number.isSafeInteger(input.at) || !Number.isSafeInteger(input.sockets)
    || input.at > now + 1_000 || input.at < now - 30_000) return undefined;
  if (input.currentPressure !== undefined && input.currentPressure !== 0 && input.currentPressure !== 1) return undefined;
  const count = WINDOW_FIELDS.filter(key => input[key] !== undefined).length;
  if (count !== 0 && count !== WINDOW_FIELDS.length) return undefined;
  const sample: CapacitySample = { v: 1, epoch: input.epoch, at: input.at, sockets: input.sockets,
    framesPerSecond: input.framesPerSecond, bytesPerSecond: input.bytesPerSecond,
    deliveryMs: input.deliveryMs, queueAgeMs: input.queueAgeMs, rpcMs: input.rpcMs };
  if (input.currentPressure === 1 && !pressured(sample)) return undefined;
  if (input.currentPressure !== undefined) sample.currentPressure = input.currentPressure;
  if (count) {
    const w = input as CapacitySample & CapacityWindow;
    if (w.currentPressure === undefined || ![w.windowInstance, w.windowSequence, w.windowStart, w.windowEnd, w.windowSockets, w.windowSocketSamples].every(Number.isSafeInteger)
      || w.windowInstance < 1 || w.windowEnd - w.windowStart !== SAMPLE_MS || w.windowEnd > w.at) return undefined;
    for (const key of WINDOW_FIELDS) sample[key] = w[key];
  }
  return Object.freeze(sample);
}
export function completedWindow(sample: CapacitySample | null): CapacityWindow | undefined {
  return sample?.windowInstance === undefined ? undefined : sample as CapacitySample & CapacityWindow;
}
export function windowPressured(w: CapacityWindow): boolean {
  return pressured({ v: 1, epoch: 1, at: w.windowEnd, sockets: w.windowSocketSamples ? w.windowSockets : 0,
    framesPerSecond: w.windowFramesPerSecond, bytesPerSecond: w.windowBytesPerSecond,
    deliveryMs: w.windowDeliveryMs, queueAgeMs: w.windowQueueAgeMs, rpcMs: w.windowRpcMs });
}
export function freshWindow(w: CapacityWindow, now: number): boolean { return w.windowEnd <= now && now - w.windowEnd <= 2 * SAMPLE_MS; }

/** Provisional protective thresholds; V03 must calibrate them with receiver-applied delay. */
export function pressured(s: CapacitySample): boolean {
  return s.sockets >= MAX_SOCKETS || s.framesPerSecond >= 4_000 || s.bytesPerSecond >= 4 * 1024 * 1024
    || s.deliveryMs >= 100 || s.queueAgeMs >= 100 || s.rpcMs >= 100;
}
export function quiet(s: CapacitySample): boolean {
  return s.sockets < MAX_SOCKETS / 4 && s.framesPerSecond < 1_000 && s.bytesPerSecond < 1024 * 1024
    && s.deliveryMs < 25 && s.queueAgeMs < 25 && s.rpcMs < 25;
}

/** Per-active-event deadline, cleared on completion; never an idle polling timer. RPC cancellation is not implied. */
export async function withDeadline<T>(work: Promise<T>, ms = 2_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("gateway RPC deadline")), ms); })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}
