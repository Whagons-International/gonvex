/**
 * Join compiler-owned predicates without a left-deep SQLite expression tree.
 * Parentheses alone do not balance `a OR b OR c`: SQLite parses it as
 * `(a OR b) OR c`. Reassociate only the same AND/OR operator, retaining
 * operand order, every leaf and SQL's three-valued boolean semantics.
 */
export function booleanSql(operator: "AND" | "OR", terms: readonly string[]): string {
  const emit = (from: number, to: number): string => {
    if (to - from === 1) return `(${terms[from]})`;
    const middle = from + Math.floor((to - from) / 2);
    return `(${emit(from, middle)} ${operator} ${emit(middle, to)})`;
  };
  return terms.length ? emit(0, terms.length) : "";
}
