/** Internal routing only. No SDK/auth/tenant identity is needed before upgrade. */
export const PLACEMENT_VERSION = 1;
export const CONTROLLER_CELLS = 32;
export const ASSIGNMENT_HEADER = "x-gonvex-gateway-assignment";
export const LEASE_MS = 30_000;
export const MAX_PENDING = 256;
export const MAX_GATEWAYS_PER_CELL = 64;
export const MAX_SOCKETS = 125; // S3 starting point, not measured full-app capacity.
export const SAMPLE_MS = 5_000;
export const SCALE_IN_MS = 120_000;
export const COOLDOWN_MS = 60_000;

export interface GatewayAssignment {
  v: 1;
  cell: number;
  gateway: string;
  epoch: number;
  token: string;
  connectionId: string;
  expiresAt: number;
}
export interface GatewayPlacement {
  acquire(connectionId: string): Promise<GatewayAssignment>;
  release(assignment: GatewayAssignment): Promise<void>;
}
export interface CapacitySample {
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
