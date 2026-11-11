// SQL text helpers for the database host calls: admission
// (RT/sql_admission.rs), `$N` numbering, write targets (host_calls.rs
// write_target) and SELECT projection typing (a simplified sql_projection.rs).
// Ported from the S2 spike (cloudflare/spikes/s2/src/sql.js).

export interface Token {
  type: "word" | "id" | "string" | "param" | "symbol" | "number";
  text: string;
  value: string;
  lower: string;
  start: number;
  end: number;
}

const UNTERMINATED = "a database statement contains an unterminated literal or comment";

export function tokenize(sql: string): Token[] {
  const out: Token[] = [];
  const push = (type: Token["type"], start: number, end: number, value = sql.slice(start, end)) =>
    out.push({ type, text: sql.slice(start, end), value, lower: value.toLowerCase(), start, end });
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i] as string;
    if (c === "\0") throw new Error("a database statement may not contain NUL");
    if (/\s/.test(c)) { i++; continue; }
    if (c === "-" && sql[i + 1] === "-") { const nl = sql.indexOf("\n", i); i = nl < 0 ? n : nl + 1; continue; }
    if (c === "/" && sql[i + 1] === "*") { const end = sql.indexOf("*/", i + 2); if (end < 0) throw new Error(UNTERMINATED); i = end + 2; continue; }
    if (c === "'" || c === '"' || c === "`" || c === "[") {
      const close = c === "[" ? "]" : c;
      let j = i + 1, value = "";
      for (;;) {
        if (j >= n) throw new Error(UNTERMINATED);
        if (sql[j] === close) { if (close !== "]" && sql[j + 1] === close) { value += close; j += 2; continue; } j++; break; }
        value += sql[j++];
      }
      push(c === "'" ? "string" : "id", i, j, value);
      i = j; continue;
    }
    if ((c === "x" || c === "X") && sql[i + 1] === "'") {
      const end = sql.indexOf("'", i + 2);
      if (end < 0) throw new Error(UNTERMINATED);
      push("string", i, end + 1); i = end + 1; continue;
    }
    if (c === "$" || c === "?" || c === ":" || c === "@") {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_]/.test(sql[j] as string)) j++;
      if (c === ":" && j === i + 1) { push("symbol", i, i + 1); i++; continue; }
      push("param", i, j); i = j; continue;
    }
    if (/[A-Za-z_\u0080-￿]/.test(c)) {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_$\u0080-￿]/.test(sql[j] as string)) j++;
      push("word", i, j); i = j; continue;
    }
    if (/[0-9.]/.test(c)) {
      let j = i + 1;
      while (j < n && /[0-9A-Za-z_.]/.test(sql[j] as string)) j++;
      push("number", i, j); i = j; continue;
    }
    push("symbol", i, i + 1); i++;
  }
  return out;
}

const word = (tokens: readonly Token[], index: number): string => {
  const token = tokens[index];
  return token?.type === "word" ? token.lower : token?.text === "(" ? "(" : "";
};

export interface Admitted {
  tokens: Token[];
  firstWord: string;
}

/** sql_admission.rs admit_statement: exactly one statement, no transaction control, no ATTACH/DETACH/PRAGMA/VACUUM. */
export function admit(statement: string): Admitted {
  const sql = statement.trim();
  if (!sql) throw new Error("a database statement is required");
  const tokens = tokenize(sql);
  let boundary = true;
  let statements = 0;
  for (const token of tokens) {
    if (token.text === ";") { boundary = true; continue; }
    if (boundary) { statements++; boundary = false; }
  }
  if (statements > 1) throw new Error("a database statement may not contain more than one statement");
  const body = tokens.filter((token) => token.text !== ";");
  const [first, second, third] = [word(body, 0), word(body, 1), word(body, 2)];
  const control = ["commit", "end", "abort", "begin", "start"].includes(first)
    || (first === "rollback" && !(second === "to" || ((second === "work" || second === "transaction") && third === "to")))
    || (first === "prepare" && second === "transaction" && third !== "as" && third !== "(");
  if (control) throw new Error("transaction boundaries belong to the invocation; raw transaction control is not allowed");
  if (["attach", "detach", "pragma", "vacuum"].includes(first)) throw new Error(`${first.toUpperCase()} statements are not allowed in module SQL`);
  return { tokens: body, firstWord: first };
}

/** host_calls.rs require_read_head. */
export function requireReadHead(head: string): void {
  if (!["select", "with", "values", "explain"].includes(head)) throw new Error(`a Query may only read; ${JSON.stringify(head)} is not a read statement`);
}

/** SAVEPOINT / RELEASE / ROLLBACK TO, which DO SQL refuses in `exec` (0006). */
export function savepointCommand(tokens: readonly Token[]): { command: "savepoint" | "release" | "rollback"; name: string } | undefined {
  const name = (i: number) => tokens[i]?.value ?? "";
  if (word(tokens, 0) === "savepoint") return { command: "savepoint", name: name(1) };
  if (word(tokens, 0) === "release") return { command: "release", name: word(tokens, 1) === "savepoint" ? name(2) : name(1) };
  if (word(tokens, 0) === "rollback") {
    let i = 1;
    if (word(tokens, i) === "transaction" || word(tokens, i) === "work") i++;
    if (word(tokens, i) === "to") { i++; if (word(tokens, i) === "savepoint") i++; return { command: "rollback", name: name(i) }; }
  }
  return undefined;
}

/**
 * Durable Object SQL binds `$name` parameters by order of first appearance,
 * so `$2 … $1` would swap values (0011.1). Rewrite `$N` to `?N` outside
 * literals and comments. Returns the rewritten SQL and the highest index.
 */
export function numberedParameters(sql: string, tokens: readonly Token[] = tokenize(sql)): { sql: string; count: number } {
  let out = "", last = 0, count = 0;
  for (const token of tokens) {
    if (token.type !== "param") continue;
    const match = /^[$?](\d+)$/.exec(token.text);
    if (!match) throw new Error(`unsupported SQL parameter ${token.text}; use $1..$n`);
    count = Math.max(count, Number(match[1]));
    out += sql.slice(last, token.start) + `?${match[1]}`;
    last = token.end;
  }
  return { sql: out + sql.slice(last), count };
}

/** host_calls.rs write_target: the table of a simple INSERT/REPLACE/UPDATE/DELETE head. */
export function writeTarget(tokens: readonly Token[]): string | undefined {
  const first = word(tokens, 0);
  let index: number;
  if (first === "insert" || first === "replace") {
    index = 1;
    while (["or", "rollback", "abort", "replace", "fail", "ignore"].includes(word(tokens, index))) index++;
    if (word(tokens, index) !== "into") return undefined;
    index++;
  } else if (first === "delete") {
    if (word(tokens, 1) !== "from") return undefined;
    index = 2;
  } else if (first === "update") {
    index = 1;
    if (word(tokens, 1) === "or") index = 3;
  } else return undefined;
  const target = tokens[index];
  if (!target || (target.type !== "word" && target.type !== "id")) return undefined;
  if (tokens[index + 1]?.text === ".") return tokens[index + 2]?.value;
  return target.value;
}

const keywords = new Set(["where", "group", "order", "limit", "having", "window", "union", "except", "intersect", "on", "using", "join", "inner", "left", "right", "full", "cross", "natural", "outer", "as", "returning"]);
const clauseEnd = new Set(["where", "group", "order", "limit", "having", "window", "union", "except", "intersect", "returning"]);

export type ProjectionColumn =
  | { star: true; tables: string[] }
  | { star?: false; name: string | undefined; source: [string | undefined, string, string[]?] | undefined };

/** Tables a SELECT's top-level FROM clause names (for typing and catalog warm-up). */
export function selectProjection(tokens: readonly Token[]): ProjectionColumn[] | undefined {
  if (word(tokens, 0) !== "select") return undefined;
  let depth = 0, fromIndex = -1;
  for (let index = 1; index < tokens.length; index++) {
    const token = tokens[index] as Token;
    if (token.text === "(") depth++;
    else if (token.text === ")") depth--;
    else if (depth === 0 && token.type === "word" && token.lower === "from") { fromIndex = index; break; }
  }
  const start = word(tokens, 1) === "distinct" || word(tokens, 1) === "all" ? 2 : 1;
  const listEnd = fromIndex < 0 ? tokens.length : fromIndex;
  const items: Token[][] = [];
  let current: Token[] = [];
  depth = 0;
  for (let index = start; index < listEnd; index++) {
    const token = tokens[index] as Token;
    if (token.text === "(") depth++;
    if (token.text === ")") depth--;
    if (depth === 0 && token.text === ",") { items.push(current); current = []; continue; }
    current.push(token);
  }
  if (current.length) items.push(current);
  const tables = new Map<string, string>();
  const order: string[] = [];
  if (fromIndex >= 0) {
    depth = 0;
    let expectTable = true;
    for (let index = fromIndex + 1; index < tokens.length; index++) {
      const token = tokens[index] as Token;
      if (token.text === "(") { depth++; expectTable = false; continue; }
      if (token.text === ")") { depth--; continue; }
      if (depth > 0) continue;
      if (token.type === "word" && clauseEnd.has(token.lower)) break;
      if (token.text === "," || (token.type === "word" && token.lower === "join")) { expectTable = true; continue; }
      if (token.type === "word" && token.lower === "on") { expectTable = false; continue; }
      if (expectTable && (token.type === "word" || token.type === "id") && !(token.type === "word" && keywords.has(token.lower))) {
        const name = token.value;
        let alias = name;
        let next = index + 1;
        if (word(tokens, next) === "as") next++;
        const candidate = tokens[next];
        if (candidate && (candidate.type === "id" || (candidate.type === "word" && !keywords.has(candidate.lower)))) { alias = candidate.value; index = next; }
        tables.set(alias, name);
        order.push(alias);
        expectTable = false;
      }
    }
  }
  const columns: ProjectionColumn[] = [];
  for (const item of items) {
    let alias: string | undefined;
    let expression = item;
    if (item.length >= 3 && word(item, item.length - 2) === "as") { alias = (item[item.length - 1] as Token).value; expression = item.slice(0, -2); }
    const [e0, e1, e2] = expression;
    if (expression.length === 1 && e0?.text === "*") { columns.push({ star: true, tables: order.map((a) => tables.get(a) as string) }); continue; }
    if (expression.length === 3 && e1?.text === "." && e2?.text === "*") {
      const table = tables.get(e0?.value ?? "");
      columns.push({ star: true, tables: table === undefined ? [] : [table] });
      continue;
    }
    let source: [string | undefined, string, string[]?] | undefined;
    if (expression.length === 1 && e0 && (e0.type === "id" || (e0.type === "word" && !keywords.has(e0.lower)))) {
      source = order.length === 1 ? [tables.get(order[0] as string), e0.value] : [undefined, e0.value, order.map((a) => tables.get(a) as string)];
    } else if (expression.length === 3 && e1?.text === "." && e2 && (e2.type === "id" || e2.type === "word")) {
      const table = tables.get(e0?.value ?? "");
      if (table) source = [table, e2.value];
    }
    columns.push({ name: alias ?? source?.[1], source });
  }
  return columns;
}
