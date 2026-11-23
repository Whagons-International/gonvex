/**
 * A0 SPIKE (option A, M0; docs/cloudflare/design/option-a-tenant-socket.md). NOT production code.
 *
 * The tenant object holds the client's application socket itself: an `AssemblyGateway` instance is embedded in
 * the tenant object and shares its `ctx` (sockets, storage). The two bindings that connect them are short-circuited
 * to in-process calls:
 *  - the holder's `TENANT.getByName(<own tenant>)` returns the tenant object (dispatch, admit, attachGateway, ...);
 *  - the tenant's `GATEWAY.getByName(<holder name>)` returns the embedded holder (replica deliveries, presence,
 *    action replies).
 * Everything else (Control, other tenants, other gateways) goes through the real bindings.
 *
 * Shortcuts that do not change the measured path: the own tenant only, no shedding, no cap, no K16 slot
 * reservation (the router reserves none for these upgrades), X02 call gating off (no gateway <-> tenant RPC
 * ping-pong exists for a local socket), Control notices still address the gateway namespace (no revocation).
 * The holder's alarm is virtualized onto the tenant's alarm (earliest wins, checked on every socket event).
 *
 * Enabled only when the Worker var GONVEX_A0_TENANT_SOCKETS is "1" and the router sees `?a0=1` on `/ws`.
 */
import { AssemblyGateway, type AssemblyGatewayEnv } from "./gateway";

export const A0_HOLDER_PREFIX = "a0~";
export const a0Enabled = (env: unknown): boolean => (env as { GONVEX_A0_TENANT_SOCKETS?: string }).GONVEX_A0_TENANT_SOCKETS === "1";
export const a0HolderName = (tenantId: string): string => `${A0_HOLDER_PREFIX}${tenantId}`;

/** The per-tenant-object link between the tenant and its embedded holder. */
export interface A0Cell {
  tenantId: string;
  tenant?: object;
  holder?: A0LocalHolder;
}

/**
 * A namespace whose `getByName(name)` answers `local()` for one name and the real namespace for every other.
 * Every other member (idFromName, get, ...) is the real namespace's.
 */
function shortCircuit(real: DurableObjectNamespace<any> | undefined, name: string, local: () => object | undefined): DurableObjectNamespace<any> | undefined {
  if (!real) return real;
  return new Proxy(real, {
    get(target, prop) {
      if (prop === "getByName") return (n: string) => (n === name && local()) || (target as unknown as { getByName(n: string): unknown }).getByName(n);
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

/** RPC copies arguments; an in-process call must not let the callee keep the caller's objects (small args only). */
function copying(object: object): object {
  return new Proxy(object, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => (value as (...a: unknown[]) => unknown).apply(target, args.map((a) => a === undefined ? a : structuredClone(a)));
    },
  });
}

/** The tenant's env with GATEWAY short-circuited to its embedded holder (when A0 is on). */
export function a0TenantEnv<E extends object>(ctx: DurableObjectState, env: E, cell: A0Cell): E {
  if (!a0Enabled(env)) return env;
  const e = env as unknown as AssemblyGatewayEnv;
  return { ...env, GATEWAY: shortCircuit(e.GATEWAY, a0HolderName(cell.tenantId), () => cell.holder) } as E;
}

/** The holder's env: TENANT short-circuited to the tenant, no controller (plain `accept`). */
function holderEnv(env: AssemblyGatewayEnv, cell: A0Cell): AssemblyGatewayEnv {
  const tenantProxy = () => cell.tenant && copying(cell.tenant);
  return { ...env, TENANT: shortCircuit(env.TENANT, cell.tenantId, tenantProxy)!, GATEWAY_CONTROLLER: undefined } as unknown as AssemblyGatewayEnv;
}

/** The holder's view of `ctx`: the same object except a virtual alarm (the tenant owns the platform alarm). */
function holderCtx(ctx: DurableObjectState, alarm: { at: number | null }): DurableObjectState {
  const storage = new Proxy(ctx.storage, {
    get(target, prop) {
      if (prop === "setAlarm") return async (at: number | Date) => {
        const t = typeof at === "number" ? at : at.getTime();
        alarm.at = t;
        const current = await target.getAlarm();
        if (current === null || current > t) await target.setAlarm(t);
      };
      if (prop === "getAlarm") return async () => alarm.at;
      if (prop === "deleteAlarm") return async () => { alarm.at = null; };
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return new Proxy(ctx, {
    get(target, prop) {
      if (prop === "storage") return storage;
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export class A0LocalHolder extends AssemblyGateway {
  /** Read by GatewayObjectBase: no X02 gating, no entry tracking. */
  static ungated = true;
  /** Read by GatewayObjectBase: the SocketHost's ctx (virtual alarm). */
  static hostCtx(ctx: DurableObjectState): DurableObjectState { return holderCtx(ctx, pendingAlarm!); }
  readonly virtualAlarm: { at: number | null };
  private readonly holderName: string;
  private alarmRunning = false;
  protected override holderId(): string { return (this.env.GATEWAY as DurableObjectNamespace).idFromName(this.gatewayName).toString(); }
  override get gatewayName(): string { return this.holderName ?? pendingName!; }

  static create(ctx: DurableObjectState, env: AssemblyGatewayEnv, cell: A0Cell): A0LocalHolder {
    const alarm = { at: null as number | null };
    pendingName = a0HolderName(cell.tenantId);
    pendingAlarm = alarm;
    try { return new A0LocalHolder(ctx, holderEnv(env, cell)); }
    finally { pendingName = undefined; pendingAlarm = undefined; }
  }

  private constructor(ctx: DurableObjectState, env: AssemblyGatewayEnv) {
    super(ctx, env);
    this.holderName = pendingName!;
    this.virtualAlarm = pendingAlarm!;
  }

  /** Runs the holder's alarm when its virtual alarm is due; re-arms the platform alarm for the next one. */
  async alarmIfDue(platformCtx: DurableObjectState): Promise<void> {
    const at = this.virtualAlarm.at;
    if (at !== null && at <= Date.now() + 5 && !this.alarmRunning) {
      this.virtualAlarm.at = null;
      this.alarmRunning = true;
      try { await this.alarm(); } finally { this.alarmRunning = false; }
    }
    const next = this.virtualAlarm.at;
    if (next !== null) {
      const current = await platformCtx.storage.getAlarm();
      if (current === null || current > next) await platformCtx.storage.setAlarm(next);
    }
  }
}
// The base constructor reads `gatewayName` (SocketHost options) only lazily, but the name must exist before any
// session is created; these carry it into the private constructor.
let pendingName: string | undefined;
let pendingAlarm: { at: number | null } | undefined;
