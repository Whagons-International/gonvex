import { createRuntimeGlobals, type RuntimeInvocation } from "../runtime-contract/globals.js";

let active: RuntimeInvocation | undefined;
let completion = Promise.resolve();
const globals = createRuntimeGlobals(() => active);
export const runtimeDate = globals.Date;
export const runtimeMath = globals.Math;
const deniedEntropy = (): never => {
  const error = new Error("Secure entropy is available only in Actions; use intentEntropy in Reducers");
  error.name = "InvalidAccessError";
  throw error;
};
export const runtimeCrypto: Crypto = {
  get subtle() {
    // Native hosts may install their crypto adapter after module evaluation.
    const nativeCrypto = Reflect.get(globalThis, "crypto") as Crypto | undefined;
    if (!nativeCrypto) throw new Error("WebCrypto is unavailable");
    return nativeCrypto.subtle;
  },
  getRandomValues: deniedEntropy,
  randomUUID: deniedEntropy,
};

/** Generated reducer bundles inject these lexical globals. Browser/UI globals
 * stay untouched, including while a reducer awaits database or hashing work.
 * A bundle shares its lexical bindings, so its invocations run serially.
 */
export async function withReducerRuntimeGlobals<T>(context: { now: number; intentEntropy?: string }, run: () => T | Promise<T>): Promise<T> {
  const previous = completion;
  let finish!: () => void;
  completion = new Promise<void>(resolve => { finish = resolve; });
  await previous;
  try {
    active = { kind: "reducer", now: context.now, intentEntropy: context.intentEntropy };
    return await run();
  } finally { active = undefined; finish(); }
}
