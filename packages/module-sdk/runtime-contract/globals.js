export function createRuntimeGlobals(current) {
  const NativeDate = Reflect.get(globalThis, "Date");
  const nativeMath = Reflect.get(globalThis, "Math");
  const generators = new WeakMap();
  const state = () => {
    const value = current();
    if (!value) throw new Error("Time and randomness require an active runtime invocation");
    return value;
  };
  const now = () => {
    const invocation = state();
    return invocation.kind === "action" ? NativeDate.now() : invocation.now;
  };
  function RuntimeDate(...args) {
    if (!new.target) return new NativeDate(now()).toString();
    return Reflect.construct(NativeDate, args.length ? args : [now()], new.target);
  }
  // Copy Date's methods without exposing NativeDate through a prototype's
  // constructor, and without patching a browser's process-wide Date prototype.
  const descriptors = Object.getOwnPropertyDescriptors(NativeDate.prototype);
  delete descriptors.constructor;
  RuntimeDate.prototype = Object.setPrototypeOf(new NativeDate(NaN), Object.prototype);
  Object.defineProperties(RuntimeDate.prototype, descriptors);
  Object.defineProperty(RuntimeDate.prototype, "constructor", { value: RuntimeDate });
  Object.defineProperties(RuntimeDate, {
    now: { value: now }, parse: { value: NativeDate.parse }, UTC: { value: NativeDate.UTC },
  });
  const random = () => {
    const invocation = state();
    if (invocation.kind === "action") return nativeMath.random();
    const entropy = invocation.intentEntropy;
    if (typeof entropy !== "string" || !/^[0-9a-f]{64}$/.test(entropy)) {
      throw new Error("Math.random requires intentEntropy in Queries and Reducers");
    }
    let next = generators.get(invocation);
    if (!next) {
      // sfc32, a non-cryptographic PRNG for Math.random only. All 256 intent
      // bits seed its four words; secure entropy remains an Action capability.
      const words = Array.from({ length: 8 }, (_, index) => parseInt(entropy.slice(index * 8, index * 8 + 8), 16));
      let a = words[0] + words[4] | 0, b = words[1] + words[5] | 0, c = words[2] + words[6] | 0, d = words[3] + words[7] | 0;
      next = () => {
        const value = ((a + b | 0) + d | 0) >>> 0;
        d = d + 1 | 0;
        a = b ^ b >>> 9;
        b = c + (c << 3) | 0;
        c = (c << 21 | c >>> 11) + value | 0;
        return value / 4294967296;
      };
      generators.set(invocation, next);
    }
    return next();
  };
  const math = Object.create(Object.getPrototypeOf(nativeMath), Object.getOwnPropertyDescriptors(nativeMath));
  Object.defineProperty(math, "random", { value: random });
  return Object.freeze({ Date: RuntimeDate, Math: Object.freeze(math) });
}
