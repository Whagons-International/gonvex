// Number-token normalization for comparing change rows with Rust's (note 0026).

/**
 * JSON text with every number token rewritten as its value (note 0026). Rust's
 * hook reports an integral REAL stored as an integer (|v| <= 2^47) as `5`,
 * and other REALs in Rust's float format then SQLite's (`1e+300`); the kernel's
 * plain SQL reports `5.0` and `1.0e+300`. Strings, keys, key order, and integer
 * tokens (kept as written, so precision beyond 2^53 still counts) are compared exactly.
 */
export function numbersByValue(text: string | null): string | null {
  if (text === null) return null;
  return text.replace(/"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g, (token) => {
    if (token.startsWith('"')) return token;
    if (/^-?\d+$/.test(token)) return token === "-0" ? "0" : token;
    const value = Number(token);
    return Object.is(value, -0) ? "0" : String(value);
  });
}
