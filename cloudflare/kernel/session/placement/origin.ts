/**
 * X02: the gateway's outgoing calls never inherit a peer object's subrequest depth.
 *
 * The platform counts a Durable Object's outgoing call from its NEWEST in-flight
 * incoming request (workerd `IoContext::getCurrentIncomingRequest()` returns
 * `incomingRequests.front()`; each delivered request is added at the front and a
 * finished one stays there, draining, until a newer one arrives). Which code
 * "caused" the call does not matter. So while a tenant -> gateway RPC is the newest
 * request, a client frame's gateway -> tenant call is counted one hop below that
 * RPC; the tenant then answers with another delivery RPC one hop lower again.
 * During a busy session the two objects ping-pong until the platform refuses
 * ("Subrequest depth limit exceeded"), which the session reports as a 1011 close
 * (dev logs 2026-11-17 18:50:21 and 18:50:44, invocation `deliverReplicaInitial`).
 *
 * The rule here: the gateway LAUNCHES an outgoing call only while every request
 * that can be the platform's front is a fresh event (a client WebSocket event, the
 * router's upgrade, or this object's alarm). Those start a new chain, so a gateway
 * call is always one hop below a fresh event, however long the session runs and
 * however much tenant traffic arrives. Otherwise the call waits, in order, for the
 * next fresh event; `wake` arms an immediate alarm so one always comes.
 *
 * The model is conservative. The front is the newest ALIVE request. A running
 * handler is alive. A finished request is gone once a newer request was delivered
 * (its drain is cancelled or it was never the front); only the newest one can still
 * be alive while finished. So the candidates for the front are the newest running
 * entry and anything newer than it.
 */

export type InboundKind = "fresh" | "peer";

interface Entry { readonly fresh: boolean; running: boolean }

interface Deferred { launch(): void }

export interface OriginStats {
  /** Calls launched in the turn they were made. */
  immediate: number;
  /** Calls that waited for a fresh event. */
  deferred: number;
  /** Calls currently waiting. */
  waiting: number;
}

export class CallOrigin {
  /** Inbound requests that may still be alive, oldest first. */
  private entries: Entry[] = [];
  private readonly queue: Deferred[] = [];
  private woken = false;
  private stallListeners: Array<() => void> = [];
  private readonly counts = { immediate: 0, deferred: 0 };

  /** `wake` must lead to a fresh inbound event (the gateway arms an alarm now). */
  constructor(private readonly wake: () => void) {}

  /** A platform-delivered request starts. A fresh one releases waiting calls in this turn. */
  enter(kind: InboundKind): Entry {
    // Every finished entry now has a newer request: the platform has let it go.
    this.entries = this.entries.filter((entry) => entry.running);
    const entry: Entry = { fresh: kind === "fresh", running: true };
    this.entries.push(entry);
    if (entry.fresh) {
      this.woken = false;
      this.flush();
    }
    return entry;
  }

  /** The request's handler settled. Only the newest finished request can stay the front. */
  exit(entry: Entry): void {
    entry.running = false;
    if (this.entries[this.entries.length - 1] !== entry) this.entries = this.entries.filter((e) => e !== entry);
  }

  /** True when every request that can be the platform's front is a fresh event. */
  safe(): boolean {
    if (!this.entries.length) return false;
    for (let i = this.entries.length - 1; i >= 0; i--) {
      const entry = this.entries[i]!;
      if (!entry.fresh) return false;
      if (entry.running) return true;
    }
    return true;
  }

  /**
   * Runs `call` now if it is safe and nothing is waiting (calls keep their order),
   * else when the next fresh event arrives. `call` must start its RPC synchronously.
   */
  run<T>(call: () => T | PromiseLike<T>): Promise<T> {
    if (!this.queue.length && this.safe()) {
      this.counts.immediate++;
      try { return Promise.resolve(call()); } catch (error) { return Promise.reject(error); }
    }
    this.counts.deferred++;
    return new Promise<T>((resolve, reject) => {
      this.queue.push({ launch: () => {
        try { Promise.resolve(call()).then(resolve, reject); } catch (error) { reject(error); }
      } });
      if (!this.woken) { this.woken = true; this.wake(); }
      const listeners = this.stallListeners;
      this.stallListeners = [];
      for (const listener of listeners) listener();
    });
  }

  /** Launches waiting calls while it is safe, in their original order, in this turn. */
  flush(): void {
    while (this.queue.length && this.safe()) this.queue.shift()!.launch();
  }

  /** Resolves when the next call has to wait (the alarm yields so a wake alarm can run). */
  stalled(): { promise: Promise<void>; cancel(): void } {
    let listener!: () => void;
    const promise = new Promise<void>((resolve) => { listener = resolve; });
    this.stallListeners.push(listener);
    return { promise, cancel: () => { this.stallListeners = this.stallListeners.filter((l) => l !== listener); } };
  }

  stats(): OriginStats { return { ...this.counts, waiting: this.queue.length }; }
}

/** Stub members that are not calls. Symbols (dispose) pass through too. RPC members are
 * themselves proxies (`.apply` would be a remote method), so calls use `Reflect.apply`. */
const NOT_CALLS = new Set(["id", "name", "then"]);

/** Every method call on the stub goes through `origin`. */
export function gatedStub<T extends object>(stub: T, origin: CallOrigin): T {
  return new Proxy(stub, {
    get(target, prop) {
      if (prop === "then") return undefined;
      const value = Reflect.get(target, prop, target) as unknown;
      if (typeof prop === "symbol" || NOT_CALLS.has(prop) || typeof value !== "function") return value;
      return (...args: unknown[]) => origin.run(() => Reflect.apply(value as (...a: unknown[]) => unknown, target, args));
    },
  });
}

function isNamespace(value: unknown): value is DurableObjectNamespace {
  return typeof value === "object" && value !== null
    && typeof (value as DurableObjectNamespace).idFromName === "function" && typeof (value as DurableObjectNamespace).get === "function";
}

function isFetcher(value: unknown): value is Fetcher {
  return typeof value === "object" && value !== null && !isNamespace(value) && typeof (value as Fetcher).fetch === "function"
    && typeof (value as Fetcher).connect === "function";
}

/** Namespace whose stubs are gated. Id helpers stay synchronous and local. */
export function gatedNamespace<N extends DurableObjectNamespace<any>>(namespace: N, origin: CallOrigin): N {
  return new Proxy(namespace, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target) as unknown;
      if (typeof value !== "function") return value;
      if (prop === "get" || prop === "getByName") {
        return (...args: unknown[]) => gatedStub(Reflect.apply(value as (...a: unknown[]) => object, target, args) as object, origin);
      }
      if (prop === "jurisdiction") {
        return (...args: unknown[]) => gatedNamespace(Reflect.apply(value as (...a: unknown[]) => N, target, args) as N, origin);
      }
      return (...args: unknown[]) => Reflect.apply(value as (...a: unknown[]) => unknown, target, args);
    },
  });
}

/** A copy of `env` whose Durable Object namespaces and Worker bindings are gated. */
export function gatedEnv<E extends object>(env: E, origin: CallOrigin): E {
  const out: Record<string, unknown> = { ...(env as Record<string, unknown>) };
  for (const [key, value] of Object.entries(out)) {
    if (isNamespace(value)) out[key] = gatedNamespace(value, origin);
    else if (isFetcher(value)) out[key] = gatedStub(value, origin);
  }
  return out as E;
}

// ---- entry tracking on the object's own prototype -------------------------------------

/** Platform events that start a new call chain. */
export const FRESH_ENTRIES: ReadonlySet<string> = new Set(["fetch", "alarm", "webSocketMessage", "webSocketClose", "webSocketError"]);

const WRAPPED = Symbol("gonvex.origin.wrapped");
type Wrapped = ((this: any, ...args: unknown[]) => unknown) & { [WRAPPED]?: (...args: unknown[]) => unknown };

/**
 * Installs entry tracking on `proto` (the constructed class's own prototype): each
 * method the platform can call records an inbound request for its whole run. Names
 * in `internal` are helpers only this object calls (so their calls are not inbound
 * requests). Every other method counts as a peer RPC, the conservative default.
 * `fresh` lists the platform's own events; `override` replaces the default wrapper.
 */
export function trackEntries(
  proto: object, stopAt: object, internal: ReadonlySet<string>,
  originOf: (self: object) => CallOrigin | undefined,
  override?: (name: string, original: (...args: unknown[]) => unknown) => ((this: object, ...args: unknown[]) => unknown) | undefined,
): void {
  const names = new Set<string>();
  for (let p: object | null = proto; p && p !== stopAt && p !== Object.prototype; p = Object.getPrototypeOf(p)) {
    for (const name of Object.getOwnPropertyNames(p)) {
      if (name === "constructor" || internal.has(name)) continue;
      const descriptor = Object.getOwnPropertyDescriptor(p, name);
      if (descriptor && typeof descriptor.value === "function") names.add(name);
    }
  }
  for (const name of names) {
    const found = (proto as Record<string, Wrapped>)[name]!;
    const original = found[WRAPPED] ?? found;
    const custom = override?.(name, original);
    const kind: InboundKind = FRESH_ENTRIES.has(name) ? "fresh" : "peer";
    const wrapper: Wrapped = custom ?? function (this: unknown, ...args: unknown[]) {
      const origin = originOf(this as object);
      if (!origin) return original.apply(this, args);
      const entry = origin.enter(kind);
      let result: unknown;
      try { result = original.apply(this, args); }
      catch (error) { origin.exit(entry); throw error; }
      if (result && typeof (result as PromiseLike<unknown>).then === "function") {
        return Promise.resolve(result).finally(() => origin.exit(entry));
      }
      origin.exit(entry);
      return result;
    };
    wrapper[WRAPPED] = original;
    Object.defineProperty(proto, name, { value: wrapper, writable: true, configurable: true, enumerable: false });
  }
}
