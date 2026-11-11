// SQL text helpers for the Durable Object host. Behavior follows
// rust/crates/runtime/src/sql_admission.rs (one statement, no transaction
// control, no ATTACH/DETACH/PRAGMA/VACUUM) and the write-target and
// projection typing of host_calls.rs / sql_projection.rs, simplified.

/** Tokens with byte ranges: { type: "word"|"id"|"string"|"param"|"symbol"|"number", text, value, start, end }. */
export function tokenize(sql) {
  const out = [];
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i];
    if (c === "\0") throw new Error("a database statement may not contain NUL");
    if (/\s/.test(c)) { i++; continue; }
    if (c === "-" && sql[i + 1] === "-") { const nl = sql.indexOf("\n", i); i = nl < 0 ? n : nl + 1; continue; }
    if (c === "/" && sql[i + 1] === "*") { const end = sql.indexOf("*/", i + 2); if (end < 0) throw new Error("a database statement contains an unterminated literal or comment"); i = end + 2; continue; }
    if (c === "'" || c === '"' || c === "`" || c === "[") {
      const close = c === "[" ? "]" : c;
      let j = i + 1, value = "";
      for (;;) {
        if (j >= n) throw new Error("a database statement contains an unterminated literal or comment");
        if (sql[j] === close) { if (close !== "]" && sql[j + 1] === close) { value += close; j += 2; continue; } j++; break; }
        value += sql[j++];
      }
      out.push({ type: c === "'" ? "string" : "id", text: sql.slice(i, j), value, start: i, end: j });
      i = j; continue;
    }
    if ((c === "x" || c === "X") && sql[i + 1] === "'") {
      const end = sql.indexOf("'", i + 2);
      if (end < 0) throw new Error("a database statement contains an unterminated literal or comment");
      out.push({ type: "string", text: sql.slice(i, end + 1), start: i, end: end + 1 }); i = end + 1; continue;
    }
    if (c === "$" || c === "?" || c === ":" || c === "@") {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_]/.test(sql[j])) j++;
      if (c === ":" && j === i + 1) { out.push({ type: "symbol", text: c, start: i, end: i + 1 }); i++; continue; }
      out.push({ type: "param", text: sql.slice(i, j), start: i, end: j }); i = j; continue;
    }
    if (/[A-Za-z_\u0080-￿]/.test(c)) {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_$\u0080-￿]/.test(sql[j])) j++;
      const text = sql.slice(i, j);
      out.push({ type: "word", text, value: text, lower: text.toLowerCase(), start: i, end: j }); i = j; continue;
    }
    if (/[0-9.]/.test(c)) {
      let j = i + 1;
      while (j < n && /[0-9A-Za-z_.]/.test(sql[j])) j++;
      out.push({ type: "number", text: sql.slice(i, j), start: i, end: j }); i = j; continue;
    }
    out.push({ type: "symbol", text: c, start: i, end: i + 1 }); i++;
  }
  return out;
}

const word = (tokens, index) => (tokens[index]?.type === "word" ? tokens[index].lower : tokens[index]?.text === "(" ? "(" : "");

/** Admission (sql_admission.rs): returns { tokens, firstWord }. */
export function admit(statement) {
  const sql = statement.trim();
  if (!sql) throw new Error("a database statement is required");
  const tokens = tokenize(sql);
  let seenSemicolonBeforeContent = true;
  let statements = 0;
  for (const token of tokens) {
    if (token.text === ";") { seenSemicolonBeforeContent = true; continue; }
    if (seenSemicolonBeforeContent) { statements++; seenSemicolonBeforeContent = false; }
  }
  if (statements > 1) throw new Error("a database statement may not contain more than one statement");
  const body = tokens.filter((token, index) => !(token.text === ";" && tokens.slice(0, index).every((t) => t.text === ";")));
  const [first, second, third] = [word(body, 0), word(body, 1), word(body, 2)];
  const control = ["commit", "end", "abort", "begin", "start"].includes(first)
    || (first === "rollback" && !(second === "to" || ((second === "work" || second === "transaction") && third === "to")))
    || (first === "prepare" && second === "transaction" && third !== "as" && third !== "(");
  if (control) throw new Error("transaction boundaries belong to the invocation; raw transaction control is not allowed");
  if (["attach", "detach", "pragma", "vacuum"].includes(first)) throw new Error(`${first.toUpperCase()} statements are not allowed in module SQL`);
  return { tokens: body, firstWord: first };
}

/**
 * Durable Object SQL binds `$name` parameters by order of first appearance,
 * so `$2 … $1` would swap values. Rewrite `$N` to SQLite's explicit `?N`.
 * Returns { sql, count } where count is the highest index used.
 */
export function numberedParameters(sql, tokens = tokenize(sql)) {
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

/** host_calls.rs write_target: the table of a simple INSERT/UPDATE/DELETE head. */
export function writeTarget(tokens) {
  const first = word(tokens, 0);
  let index;
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

/**
 * Result typing for SELECT: maps each output column to a [table, column]
 * source when the FROM clause names tables (with aliases and joins) and the
 * column is a bare or qualified reference. Anything else is an expression.
 * Returns an array of { name, source?: [table, column] } entries plus a flag
 * for `*` expansion, or undefined when the shape is not understood.
 */
export function selectProjection(tokens) {
  if (word(tokens, 0) !== "select") return undefined;
  // Split the top-level select list and the FROM clause.
  let depth = 0, fromIndex = -1;
  for (let index = 1; index < tokens.length; index++) {
    const token = tokens[index];
    if (token.text === "(") depth++;
    else if (token.text === ")") depth--;
    else if (depth === 0 && token.type === "word" && token.lower === "from") { fromIndex = index; break; }
  }
  let start = 1;
  if (word(tokens, 1) === "distinct" || word(tokens, 1) === "all") start = 2;
  const listEnd = fromIndex < 0 ? tokens.length : fromIndex;
  const items = [];
  let current = [];
  depth = 0;
  for (let index = start; index < listEnd; index++) {
    const token = tokens[index];
    if (token.text === "(") depth++;
    if (token.text === ")") depth--;
    if (depth === 0 && token.text === ",") { items.push(current); current = []; continue; }
    current.push(token);
  }
  if (current.length) items.push(current);
  // FROM tables: name [AS] alias, separated by joins/commas, stopping at clauses.
  const tables = new Map();
  const tableOrder = [];
  if (fromIndex >= 0) {
    depth = 0;
    let expectTable = true;
    for (let index = fromIndex + 1; index < tokens.length; index++) {
      const token = tokens[index];
      if (token.text === "(") { depth++; expectTable = false; continue; }
      if (token.text === ")") { depth--; continue; }
      if (depth > 0) continue;
      if (token.type === "word" && ["where", "group", "order", "limit", "having", "window", "union", "except", "intersect", "returning"].includes(token.lower)) break;
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
        tableOrder.push(alias);
        expectTable = false;
      }
    }
  }
  const columns = [];
  for (const item of items) {
    // Alias: trailing `AS name` or trailing bare identifier after an expression.
    let alias;
    let expression = item;
    if (item.length >= 3 && word(item, item.length - 2) === "as") { alias = item[item.length - 1].value; expression = item.slice(0, -2); }
    if (expression.length === 1 && expression[0].text === "*") { columns.push({ star: true, tables: tableOrder.map((a) => tables.get(a)) }); continue; }
    if (expression.length === 3 && expression[1].text === "." && expression[2].text === "*") { columns.push({ star: true, tables: [tables.get(expression[0].value)].filter(Boolean) }); continue; }
    let source;
    if (expression.length === 1 && (expression[0].type === "id" || (expression[0].type === "word" && !keywords.has(expression[0].lower)))) {
      if (tableOrder.length === 1) source = [tables.get(tableOrder[0]), expression[0].value];
      else source = [undefined, expression[0].value, tableOrder.map((a) => tables.get(a))];
    } else if (expression.length === 3 && expression[1].text === "." && (expression[2].type === "id" || expression[2].type === "word")) {
      const table = tables.get(expression[0].value);
      if (table) source = [table, expression[2].value];
    }
    columns.push({ name: alias ?? (source ? source[1] : undefined), source });
  }
  return columns;
}
