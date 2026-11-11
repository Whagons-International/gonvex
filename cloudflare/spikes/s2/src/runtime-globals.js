// Installs the Gonvex runtime globals before the app bundle evaluates.
//
// The Rust isolate (gonvex_bootstrap.js) keeps one `activeRuntimeRequest` per
// isolate. A workerd isolate is shared by every Durable Object of the class
// and by concurrent requests, so the active invocation lives in an
// AsyncLocalStorage instead: Date, Math.random and the entropy gate read the
// invocation that owns the current async context, including across awaits.
//
// Import this module first: ES module imports evaluate in order, so the app
// bundle's top level already sees the replaced globals.
import { AsyncLocalStorage } from "node:async_hooks";
import { createRuntimeGlobals } from "./contract/globals.js";

const invocations = new AsyncLocalStorage();
const runtimeGlobals = createRuntimeGlobals(() => invocations.getStore());
const nativeCrypto = globalThis.crypto;

const deniedEntropy = () => {
  const error = new Error("Secure entropy is available only in Actions; use intentEntropy in Reducers");
  error.name = "InvalidAccessError";
  throw error;
};
// workerd's Ed25519 verify throws OperationError for a signature that is not
// 64 bytes; WebCrypto (and the Rust host, published vectors ed25519-*) return
// false. Everything else is the native SubtleCrypto.
const nativeSubtle = nativeCrypto.subtle;
const subtle = Object.freeze(Object.fromEntries(
  ["encrypt", "decrypt", "sign", "digest", "generateKey", "deriveKey", "deriveBits", "importKey", "exportKey", "wrapKey", "unwrapKey", "timingSafeEqual"]
    .filter((name) => typeof nativeSubtle[name] === "function")
    .map((name) => [name, nativeSubtle[name].bind(nativeSubtle)])
    .concat([["verify", async (algorithm, key, signature, data) => {
      const name = typeof algorithm === "string" ? algorithm : algorithm?.name;
      if (String(name).toUpperCase() === "ED25519" && (signature?.byteLength ?? 0) !== 64) return false;
      return nativeSubtle.verify(algorithm, key, signature, data);
    }]]),
));
// Queries and Reducers get WebCrypto's subtle API but no secure entropy;
// Actions (and module initialization, which has no invocation) keep both.
const gatedCrypto = Object.freeze({
  get subtle() { return subtle; },
  getRandomValues(target) {
    const invocation = invocations.getStore();
    if (invocation && invocation.kind !== "action") deniedEntropy();
    return nativeCrypto.getRandomValues(target);
  },
  randomUUID() {
    const invocation = invocations.getStore();
    if (invocation && invocation.kind !== "action") deniedEntropy();
    return nativeCrypto.randomUUID();
  },
});

export const installed = { Date: globalThis.Date, Math: globalThis.Math, crypto: nativeCrypto };
globalThis.Date = runtimeGlobals.Date;
globalThis.Math = runtimeGlobals.Math;
Object.defineProperty(globalThis, "crypto", { value: gatedCrypto, configurable: true, writable: false, enumerable: true });

/** Runs `run` as the active invocation `{ kind, now, intentEntropy }`. */
export function withInvocation(invocation, run) {
  return invocations.run(Object.freeze({ ...invocation }), run);
}
export const activeInvocation = () => invocations.getStore();
