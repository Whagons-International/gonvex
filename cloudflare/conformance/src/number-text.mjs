// EXACT named rule accepted in 0028 §2a, shared with K03's parity comparison.
// Keep integer tokens, strings, key order and every non-number byte unchanged.
export function numbersByValue(text) {
  if (text === null) return null;
  return text.replace(/"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g, (token) => {
    if (token.startsWith('"')) return token;
    if (/^-?\d+$/.test(token)) return token === '-0' ? '0' : token;
    const value = Number(token);
    return Object.is(value, -0) ? '0' : String(value);
  });
}
