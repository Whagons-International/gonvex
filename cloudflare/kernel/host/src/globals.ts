// Runtime globals of the module isolate (CONTRACT.md B.3, B.4).
//
// The Rust isolate keeps one `activeRuntimeRequest` per isolate and replaces
// `globalThis.Date` / `Math` / `crypto`. A workerd isolate is shared by every
// Durable Object of the class and by concurrent events, so:
//
// - the active invocation lives in an AsyncLocalStorage (nodejs_als), which
//   follows the invocation across every await, savepoint turn and host call;
// - the app bundle sees the shadowed globals through *lexical* bindings
//   (`moduleGlobals`, imported by the bundle prelude in `bundle.ts`), exactly
//   like generated local bundles do (packages/gonvex/src/local-bindings.ts).
//   The kernel's own `Date`, `Math` and `crypto` stay native, so a kernel
//   timer or a vitest runner is never pinned or denied.
//
// The determinism algorithms come from the published runtime-contract factory
// (packages/module-sdk/runtime-contract/globals.js) so they cannot drift.
import { AsyncLocalStorage } from "node:async_hooks";
import { createRuntimeGlobals, type RuntimeInvocation } from "../../../../packages/module-sdk/runtime-contract/globals.js";

export type FunctionKind = "query" | "reducer" | "action";

/** What the globals need to know about the running invocation. */
export interface ActiveInvocation {
  readonly kind: FunctionKind;
  readonly now: number;
  readonly intentEntropy?: string | undefined;
}

const invocations = new AsyncLocalStorage<ActiveInvocation>();

/** Runs `run` as the active invocation. Every await inside it keeps the same store. */
export function withInvocation<T>(invocation: ActiveInvocation, run: () => T): T {
  return invocations.run(Object.freeze({ ...invocation }), run);
}

export const activeInvocation = (): ActiveInvocation | undefined => invocations.getStore();

// The factory's type has no `| undefined` on intentEntropy; it only tests the string.
const runtime = createRuntimeGlobals(() => invocations.getStore() as RuntimeInvocation | undefined);
const nativeCrypto = crypto;
const nativeSubtle = nativeCrypto.subtle;

function deniedEntropy(): never {
  // V8/isolate.rs:767 and BS:352-359: a DOMException named InvalidAccessError, Rust's text.
  throw new DOMException("secure entropy is available only in Actions; use intentEntropy in Reducers", "InvalidAccessError");
}

const algorithmName = (algorithm: unknown): string =>
  String(typeof algorithm === "string" ? algorithm : (algorithm as { name?: unknown } | null)?.name ?? "").toUpperCase();

/**
 * workerd's Ed25519 `verify` throws OperationError for a signature that is
 * not 64 bytes; WebCrypto and the Rust host return `false` (vectors
 * `ed25519-*`). Every other method is the native SubtleCrypto.
 */
const subtle: SubtleCrypto = Object.freeze({
  encrypt: nativeSubtle.encrypt.bind(nativeSubtle),
  decrypt: nativeSubtle.decrypt.bind(nativeSubtle),
  sign: nativeSubtle.sign.bind(nativeSubtle),
  digest: nativeSubtle.digest.bind(nativeSubtle),
  generateKey: nativeSubtle.generateKey.bind(nativeSubtle),
  deriveKey: nativeSubtle.deriveKey.bind(nativeSubtle),
  deriveBits: nativeSubtle.deriveBits.bind(nativeSubtle),
  importKey: nativeSubtle.importKey.bind(nativeSubtle),
  exportKey: nativeSubtle.exportKey.bind(nativeSubtle),
  wrapKey: nativeSubtle.wrapKey.bind(nativeSubtle),
  unwrapKey: nativeSubtle.unwrapKey.bind(nativeSubtle),
  timingSafeEqual: nativeSubtle.timingSafeEqual.bind(nativeSubtle),
  async verify(algorithm: Parameters<SubtleCrypto["verify"]>[0], key: CryptoKey, signature: ArrayBuffer | ArrayBufferView, data: ArrayBuffer | ArrayBufferView) {
    if (algorithmName(algorithm) === "ED25519" && signature.byteLength !== 64) return false;
    return nativeSubtle.verify(algorithm, key, signature, data);
  },
} as SubtleCrypto);

/**
 * The module's `crypto`: `subtle` for every kind; `getRandomValues` and
 * `randomUUID` only in Actions (and at module initialization, which the Rust
 * isolate also leaves to the native source).
 */
const moduleCrypto = Object.freeze({
  get subtle() { return subtle; },
  getRandomValues(target: Parameters<Crypto["getRandomValues"]>[0]) {
    const invocation = invocations.getStore();
    if (invocation && invocation.kind !== "action") deniedEntropy();
    return nativeCrypto.getRandomValues(target);
  },
  randomUUID(): string {
    const invocation = invocations.getStore();
    if (invocation && invocation.kind !== "action") deniedEntropy();
    return nativeCrypto.randomUUID();
  },
}) as unknown as Crypto;

/**
 * Globals the Rust bootstrap does not install (B.3, last row). workerd has
 * them; module code must not reach real I/O (`fetch`, `WebSocket`, `caches`)
 * from a Query or Reducer, and `typeof fetch` must read "undefined" as in Rust.
 */
const absent = ["fetch", "WebSocket", "Request", "caches", "structuredClone", "queueMicrotask", "performance", "Blob", "FormData", "EventTarget", "navigator", "scheduler"] as const;

/** BS:44-54: module output carries the `[gonvex:module]` prefix. */
const nativeConsole = console;
const moduleConsole = Object.freeze(Object.fromEntries(
  (["log", "info", "debug", "warn", "error", "trace"] as const).map((level) => [level, (...args: unknown[]) => nativeConsole[level]("[gonvex:module]", ...args)]),
));

const overrides: Record<string, unknown> = {
  Date: runtime.Date,
  Math: runtime.Math,
  crypto: moduleCrypto,
  console: moduleConsole,
  ...Object.fromEntries(absent.map((name) => [name, undefined])),
};

/**
 * The module's view of the global object: the real global with the
 * overridden names shadowed. Bundles that write `globalThis.fetch` or
 * `self.crypto` see the same thing as bare `fetch` / `crypto`.
 */
const moduleGlobalThis: typeof globalThis = new Proxy(globalThis, {
  get(target, property, receiver) {
    if (typeof property === "string" && Object.hasOwn(overrides, property)) return overrides[property];
    if (property === "globalThis" || property === "self") return moduleGlobalThis;
    return Reflect.get(target, property, receiver === moduleGlobalThis ? target : receiver);
  },
  has(target, property) {
    if (typeof property === "string" && Object.hasOwn(overrides, property)) return overrides[property] !== undefined;
    return Reflect.has(target, property);
  },
});

/** Lexical bindings the bundle prelude imports (`bundle.ts`). */
export const moduleGlobals = Object.freeze({
  ...overrides,
  globalThis: moduleGlobalThis,
  self: moduleGlobalThis,
});

/** Names the bundle prelude binds, in a stable order. */
export const moduleGlobalNames = Object.freeze(Object.keys(moduleGlobals));

export const runtimeDate = runtime.Date;
export const runtimeMath = runtime.Math;
export { moduleCrypto as runtimeCrypto };
