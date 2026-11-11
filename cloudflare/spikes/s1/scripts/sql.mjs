// A small SQLite tokenizer, statement splitter and the rewrites the S1 spike
// tests. It understands quotes, comments, parentheses and CREATE TRIGGER
// bodies (a trigger ends at `; END ;`, as in sqlite3_complete).

const WORD = /[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*/y;

export function tokenize(sql) {
  const tokens = [];
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i];
    const start = i;
    if (c === " " || c === "\t" || c === "\n" || c === "\r" || c === "\f") {
      while (i < n && /\s/.test(sql[i])) i += 1;
      tokens.push({ type: "space", start, end: i });
    } else if (c === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i += 1;
      tokens.push({ type: "space", start, end: i });
    } else if (c === "/" && sql[i + 1] === "*") {
      const close = sql.indexOf("*/", i + 2);
      i = close < 0 ? n : close + 2;
      tokens.push({ type: "space", start, end: i });
    } else if (c === "'" || c === '"' || c === "`") {
      i += 1;
      while (i < n) {
        if (sql[i] === c) {
          if (sql[i + 1] === c) i += 2;
          else { i += 1; break; }
        } else i += 1;
      }
      tokens.push({ type: c === "'" ? "string" : "ident", start, end: i });
    } else if (c === "[") {
      const close = sql.indexOf("]", i);
      i = close < 0 ? n : close + 1;
      tokens.push({ type: "ident", start, end: i });
    } else if (c === "(" || c === ")" || c === "," || c === ";") {
      i += 1;
      tokens.push({ type: c, start, end: i });
    } else {
      WORD.lastIndex = i;
      const m = WORD.exec(sql);
      if (m) {
        i += m[0].length;
        tokens.push({ type: "word", start, end: i, upper: m[0].toUpperCase() });
      } else {
        i += 1;
        while (i < n && !/[\s'"`\[(),;A-Za-z_\-\/]/.test(sql[i])) i += 1;
        tokens.push({ type: "op", start, end: i });
      }
    }
  }
  return tokens;
}

// Splits a script into statements (without the trailing `;`), skipping blanks.
export function splitStatements(sql) {
  const tokens = tokenize(sql);
  const out = [];
  let start = 0;
  let words = [];
  let lastWord = null;
  let beforeLast = null;
  const isTrigger = () => {
    const w = words.slice(0, 6);
    return w[0] === "CREATE" && (w[1] === "TRIGGER" || (w[2] === "TRIGGER" && (w[1] === "TEMP" || w[1] === "TEMPORARY")));
  };
  for (const token of tokens) {
    if (token.type === "space") continue;
    if (token.type === ";") {
      if (!isTrigger() || (lastWord === "END" && beforeLast === ";")) {
        const text = sql.slice(start, token.start).trim();
        if (text) out.push(text);
        start = token.end;
        words = [];
        lastWord = null;
        beforeLast = null;
        continue;
      }
      beforeLast = lastWord;
      lastWord = ";";
      continue;
    }
    const word = token.type === "word" ? token.upper : token.type;
    if (words.length < 6) words.push(word);
    beforeLast = lastWord;
    lastWord = word;
  }
  const tail = sql.slice(start).trim();
  if (tail && tokenize(tail).some((t) => t.type !== "space")) out.push(tail);
  return out;
}

const COMPOUND = new Set(["UNION", "INTERSECT", "EXCEPT"]);

// Every compound-select chain: {depth, terms}. Terms = operators + 1.
export function compoundChains(sql) {
  const tokens = tokenize(sql).filter((t) => t.type !== "space");
  const stack = [0];
  const chains = [];
  const close = () => {
    const operators = stack.pop();
    if (operators > 0) chains.push(operators + 1);
  };
  for (let i = 0; i < tokens.length; i += 1) {
    const t = tokens[i];
    if (t.type === "(") stack.push(0);
    else if (t.type === ")") close();
    else if (t.type === ";") { close(); stack.push(0); }
    else if (t.type === "word" && COMPOUND.has(t.upper)) stack[stack.length - 1] += 1;
  }
  while (stack.length) close();
  return chains;
}

// Words followed by "(" that are not function calls.
const NOT_FUNCTIONS = new Set(["AS", "MATERIALIZED", "IN", "VALUES", "EXISTS", "KEY", "CHECK", "UNIQUE", "CONFLICT", "USING",
  "ON", "OVER", "FILTER", "WHERE", "AND", "OR", "NOT", "SELECT", "FROM", "JOIN", "WHEN", "THEN", "ELSE", "SET", "DEFAULT",
  "BY", "INTO", "TABLE", "INDEX", "REFERENCES", "IF", "WINDOW", "PARTITION", "RETURNING", "CASE", "END", "IS", "LIKE",
  "GLOB", "BETWEEN", "ALL", "DISTINCT", "UNION", "EXCEPT", "INTERSECT", "WITH", "RECURSIVE", "VIEW", "TRIGGER", "BEGIN",
  "ROW", "LIMIT", "OFFSET", "HAVING", "GROUP", "ORDER", "UPDATE", "INSERT", "DELETE", "REPLACE_INTO", "FTS5"]);

// Every function call with its argument count: {name, args}. A word before
// "(" is a call unless it is a keyword or a table/CTE name in a column list.
export function functionCalls(sql) {
  const tokens = tokenize(sql).filter((t) => t.type !== "space");
  const calls = [];
  const stack = [];
  for (let i = 0; i < tokens.length; i += 1) {
    const t = tokens[i];
    if (t.type === "(") {
      const prev = tokens[i - 1];
      const before = tokens[i - 2];
      const listHead = before && ((before.type === "word" && ["INTO", "TABLE", "ON", "REFERENCES", "VIEW", "EXISTS", "WITH", "RECURSIVE", "USING"].includes(before.upper)));
      const name = prev && prev.type === "word" && !NOT_FUNCTIONS.has(prev.upper) && !listHead ? prev.upper : null;
      stack.push({ name, commas: 0, empty: tokens[i + 1] && tokens[i + 1].type === ")" });
    } else if (t.type === ")") {
      const frame = stack.pop();
      const next = tokens[i + 1];
      const after = tokens[i + 2];
      const cteColumns = next && next.type === "word" && next.upper === "AS" && after && (after.type === "(" || (after.type === "word" && (after.upper === "MATERIALIZED" || after.upper === "NOT")));
      if (frame && frame.name && !cteColumns) calls.push({ name: frame.name.toLowerCase(), args: frame.empty ? 0 : frame.commas + 1 });
    } else if (t.type === "," && stack.length) stack[stack.length - 1].commas += 1;
  }
  return calls;
}

// String literals used as LIKE/GLOB patterns: [{bytes, text}].
export function likeLiterals(sql) {
  const tokens = tokenize(sql).filter((t) => t.type !== "space");
  const out = [];
  for (let i = 0; i + 1 < tokens.length; i += 1) {
    const t = tokens[i];
    if (t.type === "word" && (t.upper === "LIKE" || t.upper === "GLOB") && tokens[i + 1].type === "string") {
      const text = sql.slice(tokens[i + 1].start + 1, tokens[i + 1].end - 1).replace(/''/g, "'");
      out.push({ bytes: Buffer.byteLength(text), text });
    }
  }
  return out;
}

// --- workaround: rewrite long compound chains ---------------------------------
//
// SQLITE_LIMIT_COMPOUND_SELECT is 5 on Durable Objects. A chain
// t1 op1 t2 op2 ... tn is evaluated left to right, so
//   SELECT * FROM (t1 op1 ... t5) op5 t6 ... t9
// is the same query with chains of at most `limit` terms; repeat until it
// fits. A trailing ORDER BY / LIMIT stays on the outermost chain.
export function rewriteCompounds(sql, limit = 5) {
  for (let pass = 0; pass < 10_000; pass += 1) {
    const next = rewriteOne(sql, limit);
    if (next === null) return sql;
    sql = next;
  }
  throw new Error("compound rewrite did not converge");
}

function rewriteOne(sql, limit) {
  const tokens = tokenize(sql).filter((t) => t.type !== "space");
  // Find the first scope (paren level) whose chain has more than `limit` terms.
  const stack = [{ operators: [], open: -1 }];
  for (let i = 0; i < tokens.length; i += 1) {
    const t = tokens[i];
    if (t.type === "(") stack.push({ operators: [], open: i });
    else if (t.type === ")" || t.type === ";") {
      const frame = stack.pop();
      if (frame.operators.length >= limit) return rewriteScope(sql, tokens, frame, limit);
      if (t.type === ";") stack.push({ operators: [], open: i });
    } else if (t.type === "word" && COMPOUND.has(t.upper)) stack[stack.length - 1].operators.push(i);
  }
  const frame = stack.pop();
  if (frame && frame.operators.length >= limit) return rewriteScope(sql, tokens, frame, limit);
  return null;
}

function rewriteScope(sql, tokens, frame, limit) {
  // The chain starts at the first SELECT/VALUES of this scope (after any WITH
  // clause, a CREATE ... AS prefix or a trigger statement head).
  const operators = frame.operators;
  const firstOperator = operators[0];
  let depth = 0;
  let chainStart = -1;
  for (let i = frame.open + 1; i < firstOperator; i += 1) {
    const t = tokens[i];
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    else if (depth === 0 && t.type === "word" && (t.upper === "SELECT" || t.upper === "VALUES")) {
      // The last SELECT at this depth before the first operator begins the
      // chain (earlier ones belong to a CTE or an INSERT head in parentheses).
      chainStart = i;
    }
  }
  if (chainStart < 0) throw new Error("cannot find compound chain start");
  // Group the first `limit` terms: from chainStart up to (not including) operators[limit-1].
  const cut = operators[limit - 1];
  const head = sql.slice(tokens[chainStart].start, tokens[cut].start).trimEnd();
  const before = sql.slice(0, tokens[chainStart].start);
  const after = sql.slice(tokens[cut].start);
  return `${before}SELECT * FROM (${head}) ${after}`;
}

// Top-level UNION ALL terms of a select, or null if it is not a pure chain.
export function unionAllTerms(sql) {
  const toks = tokenize(sql).filter((t) => t.type !== "space");
  const cuts = [];
  let depth = 0;
  for (let i = 0; i < toks.length; i += 1) {
    const t = toks[i];
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    else if (depth === 0 && t.type === "word" && COMPOUND.has(t.upper)) {
      if (t.upper !== "UNION" || !toks[i + 1] || toks[i + 1].upper !== "ALL") return null;
      cuts.push([t.start, toks[i + 1].end]);
    } else if (depth === 0 && t.type === "word" && (t.upper === "ORDER" || t.upper === "LIMIT") && cuts.length) return null;
  }
  if (!cuts.length) return null;
  const terms = [];
  let from = 0;
  for (const [start, end] of cuts) { terms.push(sql.slice(from, start).trim()); from = end; }
  terms.push(sql.slice(from).trim());
  return terms;
}

// Replaces relation names (bare or quoted words not after ".") using `map`.
function renameRelations(sql, map) {
  const toks = tokenize(sql);
  let out = "";
  let last = 0;
  let prev = null;
  for (const t of toks) {
    if ((t.type === "word" || t.type === "ident") && !(prev && prev.type === "op" && sql.slice(prev.start, prev.end).endsWith("."))) {
      const name = unquote(sql.slice(t.start, t.end));
      if (map.has(name)) {
        out += sql.slice(last, t.start) + quoteIdent(map.get(name));
        last = t.end;
      }
    }
    if (t.type !== "space") prev = t;
  }
  return out + sql.slice(last);
}

const bytesOf = (s) => Buffer.byteLength(s);
// CREATE statements are parsed again inside `INSERT INTO sqlite_schema ...
// '%q'`, which doubles every single quote, so a DDL statement must fit the
// 100,000-byte SQL limit after quote doubling (plus ~100 bytes of wrapper).
export const ddlBytes = (s) => Buffer.byteLength(s) + (s.match(/'/g) || []).length + 120;
export const quoteIdent = (name) => `"${name.replace(/"/g, '""')}"`;
export function unquote(text) {
  if (text.startsWith('"') || text.startsWith("`")) return text.slice(1, -1).replace(/""/g, '"');
  if (text.startsWith("[")) return text.slice(1, -1);
  return text;
}

// Applies every workaround to one statement. Returns the statements to run.
// `hoisted` tracks views that became pieces, so DROP VIEW drops them too.
export function applyWorkarounds(statement, hoisted, { maxBytes = 100_000, limit = 5 } = {}) {
  const drop = /^\s*DROP\s+VIEW\s+(IF\s+EXISTS\s+)?("(?:[^"]|"")*"|[\w$]+)\s*$/i.exec(statement);
  if (drop) {
    const name = unquote(drop[2]);
    const pieces = hoisted.get(name);
    if (!pieces) return [statement];
    hoisted.delete(name);
    return [statement, ...pieces.slice().reverse().map((p) => `DROP VIEW IF EXISTS ${quoteIdent(p)}`)];
  }
  if (ddlBytes(statement) > maxBytes && /^\s*CREATE\s+VIEW/i.test(statement)) {
    const { statements, pieces } = partitionView(statement, { maxBytes, limit });
    const name = unquote(/^\s*CREATE\s+VIEW\s+(IF\s+NOT\s+EXISTS\s+)?("(?:[^"]|"")*"|[\w$]+)/i.exec(statement)[2]);
    hoisted.set(name, pieces);
    return statements;
  }
  // DO SQLite refuses the temp schema; migration guard tables are created and
  // dropped inside one migration, so an ordinary table behaves the same.
  const temp = statement.replace(/^(\s*(?:--[^\n]*\n\s*)*)CREATE\s+TEMP(?:ORARY)?\s+TABLE\b/i, "$1CREATE TABLE");
  return rewriteDeepReplace(temp).map((s) => rewriteCompounds(s, limit));
}

// --- workaround: partition an oversized CTE view by union branch ---------------
//
// For `CREATE VIEW v AS WITH r0 AS (...), ..., u AS (t1 UNION ALL ... tn)
// SELECT ... FROM u GROUP BY ...`: each part view keeps, as CTEs, only what its
// branches need (so MATERIALIZED still holds inside the part), and
// `v` becomes the final SELECT over a UNION ALL of the parts. Returns
// {statements, pieces}; throws when the view does not have that shape.
export function partitionView(sql, { maxBytes = 100_000, limit = 5, maxTerms = Infinity } = {}) {
  const toks = tokenize(sql).filter((t) => t.type !== "space");
  const asIndex = toks.findIndex((t, i) => t.upper === "AS" && i > 1);
  const viewName = unquote(sql.slice(toks[asIndex - 1].start, toks[asIndex - 1].end));
  let i = asIndex + 1;
  if (toks[i].upper !== "WITH") throw new Error(`${viewName}: no WITH clause`);
  i += 1;
  const recursive = toks[i].upper === "RECURSIVE";
  if (recursive) i += 1;
  const ctes = [];
  for (;;) {
    const start = toks[i].start;
    const name = unquote(sql.slice(toks[i].start, toks[i].end));
    i += 1;
    while (toks[i].upper !== "AS") i += 1;
    i += 1;
    while (toks[i].type === "word" && (toks[i].upper === "MATERIALIZED" || toks[i].upper === "NOT")) i += 1;
    const open = i;
    let depth = 0;
    for (; ; i += 1) { if (toks[i].type === "(") depth += 1; else if (toks[i].type === ")" && --depth === 0) break; }
    ctes.push({ name, text: sql.slice(start, toks[i].end), body: sql.slice(toks[open].end, toks[i].start) });
    i += 1;
    if (toks[i] && toks[i].type === ",") { i += 1; continue; }
    break;
  }
  const finalSelect = sql.slice(toks[i].start);
  const index = new Map(ctes.map((c, k) => [c.name, k]));
  const refsOf = (text) => {
    const out = new Set();
    for (const t of tokenize(text)) if ((t.type === "word" || t.type === "ident") && index.has(unquote(text.slice(t.start, t.end)))) out.add(index.get(unquote(text.slice(t.start, t.end))));
    return out;
  };
  const deps = ctes.map((c) => refsOf(c.body));
  const finalRefs = [...refsOf(finalSelect)];
  if (finalRefs.length !== 1) throw new Error(`${viewName}: final SELECT reads ${finalRefs.length} CTEs`);
  const union = finalRefs[0];
  const terms = unionAllTerms(ctes[union].body);
  if (!terms) throw new Error(`${viewName}: last CTE is not a UNION ALL chain`);
  const closure = (roots) => {
    const seen = new Set();
    const stack = [...roots];
    while (stack.length) { const k = stack.pop(); if (seen.has(k)) continue; seen.add(k); for (const d of deps[k]) stack.push(d); }
    return seen;
  };
  const prefix = recursive ? "WITH RECURSIVE " : "WITH ";
  const partSql = (name, group) => {
    const needed = [...closure(group.flatMap((t) => [...refsOf(t)]))].sort((a, b) => a - b);
    return `CREATE VIEW ${quoteIdent(name)} AS ${prefix}${needed.map((k) => ctes[k].text).join(",\n")}\n${rewriteCompounds(group.join("\nUNION ALL\n"), limit)}`;
  };
  const statements = [];
  const pieces = [];
  let group = [];
  const flush = () => {
    if (!group.length) return;
    const name = `${viewName}__part${pieces.length}`;
    const text = partSql(name, group);
    if (ddlBytes(text) > maxBytes) throw new Error(`${name}: one branch needs ${bytesOf(text)} bytes`);
    statements.push(text);
    pieces.push(name);
    group = [];
  };
  for (const term of terms) {
    if (group.length && (group.length >= maxTerms || ddlBytes(partSql("x", group.concat(term))) > maxBytes - 500)) flush();
    group.push(term);
  }
  flush();
  const unionOfParts = rewriteCompounds(pieces.map((p) => `SELECT * FROM ${quoteIdent(p)}`).join(" UNION ALL "), limit);
  const replaced = renameRelations(finalSelect, new Map([[ctes[union].name, "\u0000"]])).replace(`"\u0000"`, `(${unionOfParts})`);
  statements.push(`${sql.slice(0, toks[asIndex].end)} ${rewriteCompounds(replaced, limit)}`);
  return { statements, pieces };
}

// --- workaround: expression depth (SQLITE_LIMIT_EXPR_DEPTH is 100) ------------
//
// The Whagons schema folds Unicode case with ~200 nested replace() calls.
// Each replace(...) chain deeper than `maxDepth` is found by its run of
// `replace (` tokens. In a view it becomes a recursive CTE that applies the
// same (search, replacement) pairs in the same order; in an index (no
// subqueries allowed) the chain is cut into virtual generated columns of at
// most `maxDepth` levels each, the outer part reading the last column.
function replaceChains(sql, minLength) {
  const toks = tokenize(sql).filter((t) => t.type !== "space");
  const chains = [];
  for (let i = 0; i < toks.length; i += 1) {
    let n = 0;
    while (toks[i + 2 * n] && toks[i + 2 * n].upper === "REPLACE" && toks[i + 2 * n + 1] && toks[i + 2 * n + 1].type === "(") n += 1;
    if (n < minLength) { if (n) i += 2 * n - 1; continue; }
    // calls[k] = {open, close, args:[[startTok,endTok],...]} for level k (0 = outermost)
    const calls = [];
    for (let k = 0; k < n; k += 1) {
      const open = i + 2 * k + 1;
      let depth = 0;
      const commas = [];
      let close = open;
      for (let j = open; j < toks.length; j += 1) {
        if (toks[j].type === "(") depth += 1;
        else if (toks[j].type === ")") { depth -= 1; if (depth === 0) { close = j; break; } }
        else if (toks[j].type === "," && depth === 1) commas.push(j);
      }
      if (commas.length !== 2) throw new Error("replace() without three arguments");
      calls.push({ start: i + 2 * k, open, close, commas });
    }
    chains.push({ toks, calls });
    i = calls[0].close;
  }
  return chains;
}

export function rewriteDeepReplace(statement, { maxDepth = 90, minLength = 95 } = {}) {
  const chains = replaceChains(statement, minLength);
  if (!chains.length) return [statement];
  const isIndex = /^\s*CREATE\s+(UNIQUE\s+)?INDEX/i.test(statement);
  let out = statement;
  const extra = [];
  // Rewrite from the last chain backwards so offsets stay valid.
  for (const [c, { toks, calls }] of chains.entries().toArray().reverse()) {
    const text = (a, b) => statement.slice(toks[a].start, toks[b].end);
    const n = calls.length;
    const inner = text(calls[n - 1].open + 1, calls[n - 1].commas[0] - 1);
    const pairs = calls.slice().reverse().map((call) => [text(call.commas[0] + 1, call.commas[1] - 1), text(call.commas[1] + 1, call.close - 1)]);
    const chainStart = toks[calls[0].start].start;
    const chainEnd = toks[calls[0].close].end;
    let replacement;
    if (!isIndex) {
      const literal = /^'(?:[^']|'')*'$/;
      if (!pairs.every(([a, b]) => literal.test(a.trim()) && literal.test(b.trim()))) throw new Error("non-literal replace pairs in a view");
      const json = JSON.stringify(pairs.map(([a, b]) => [a.trim().slice(1, -1).replace(/''/g, "'"), b.trim().slice(1, -1).replace(/''/g, "'")])).replace(/'/g, "''");
      replacement = `(WITH RECURSIVE "__fold"(i, s) AS (SELECT 0, ${inner} UNION ALL SELECT i + 1, replace(s, json_extract('${json}', '$[' || i || '][0]'), json_extract('${json}', '$[' || i || '][1]')) FROM "__fold" WHERE i < ${pairs.length}) SELECT s FROM "__fold" WHERE i = ${pairs.length})`;
    } else {
      const table = unquote(/\bON\s+("(?:[^"]|"")*"|[\w$]+)/i.exec(statement)[1]);
      const name = unquote(/INDEX\s+(IF\s+NOT\s+EXISTS\s+)?("(?:[^"]|"")*"|[\w$]+)/i.exec(statement)[2]);
      let expression = inner;
      let column = 0;
      for (let k = 0; k < pairs.length; k += maxDepth) {
        const chunk = pairs.slice(k, k + maxDepth);
        let e = expression;
        for (const [a, b] of chunk) e = `replace(${e}, ${a.trim()}, ${b.trim()})`;
        if (k + maxDepth >= pairs.length) { expression = e; break; }
        const col = `__fold_${name}_${c}_${column++}`;
        extra.push(`ALTER TABLE ${quoteIdent(table)} ADD COLUMN ${quoteIdent(col)} TEXT GENERATED ALWAYS AS (${e}) VIRTUAL`);
        expression = quoteIdent(col);
      }
      replacement = expression;
    }
    out = out.slice(0, chainStart) + replacement + out.slice(chainEnd);
  }
  return [...extra, out];
}
