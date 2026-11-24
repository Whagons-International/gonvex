#!/usr/bin/env node
// Static Data-Oriented Design (DOD) gate for cloudflare/ (inspired by whagons-lite/scripts/dod.ts).
// Zero external dependencies (pure Node 24 ESM tokenizer + structural scanner).
//
// Repo-wide rules (every production .ts file in cloudflare/kernel, src, control, router, actions):
//   sql-in-loop      a DB call (db/tx/sql/storage.sql .exec/.run/.runTyped/.insert/.update/.del/.get/.all)
//                    inside a for/while/do loop or iteration callback (N+1)
//   concat-key       a '+' string concatenation or template literal with interpolation passed directly
//                    as the key to .get/.set/.has/.delete/.add
//
// Hot-zone rules (files matching scripts/dod.zones.json):
//   iter-method      .map/.filter/.forEach/.reduce/.some/.every/.find/.findIndex/.flatMap (closure per call)
//   for-of           for (... of ...) iterator protocol instead of an indexed for loop
//   for-in           for (... in ...) key enumeration
//   alloc-in-loop    Object.keys/values/entries, Array.from, or new ... inside a loop
//   json-in-zone     JSON.parse / JSON.stringify inside a hot zone (decode once at the edge)
//
// Ratchet workflow (scripts/dod.baseline.json):
//   node scripts/check-dod.mjs                     verify current counts <= baseline
//   node scripts/check-dod.mjs --report            print per-zone / per-rule / top-10 summary, then verify
//   node scripts/check-dod.mjs --update            ratchet baseline down (refuses any raise)
//   node scripts/check-dod.mjs --update --allow-raise   explicitly write new baseline

import { existsSync, globSync, readFileSync, writeFileSync } from "node:fs";
import { matchesGlob } from "node:path";
import { fileURLToPath } from "node:url";

export const RULES = [
  "sql-in-loop",
  "concat-key",
  "iter-method",
  "for-of",
  "for-in",
  "alloc-in-loop",
  "json-in-zone",
];

const ITER_METHODS = new Set([
  "map", "filter", "forEach", "reduce", "reduceRight",
  "some", "every", "find", "findIndex", "findLast", "findLastIndex", "flatMap",
]);
const KEYED_METHODS = new Set(["get", "set", "has", "delete", "add"]);
const DB_METHODS = new Set([
  "exec", "run", "runTyped", "insert", "update", "del", "upsert", "one", "all",
]);
const DB_RECEIVERS = new Set(["db", "tx", "rtx", "wtx", "sql"]);

/**
 * Tokenizes TypeScript source into `{ type, value, line }` tokens while skipping comments
 * and accurately tracking strings and template literals (marking interpolated templates).
 */
export function tokenizeTs(src) {
  const tokens = [];
  let i = 0;
  const len = src.length;
  let line = 1;

  while (i < len) {
    const ch = src.charCodeAt(i);
    // Whitespace
    if (ch === 32 || ch === 9 || ch === 13) { i++; continue; }
    if (ch === 10) { line++; i++; continue; }

    // Line comment
    if (ch === 47 && src.charCodeAt(i + 1) === 47) {
      i += 2;
      while (i < len && src.charCodeAt(i) !== 10) i++;
      continue;
    }
    // Block comment
    if (ch === 47 && src.charCodeAt(i + 1) === 42) {
      i += 2;
      while (i < len && !(src.charCodeAt(i) === 42 && src.charCodeAt(i + 1) === 47)) {
        if (src.charCodeAt(i) === 10) line++;
        i++;
      }
      i += 2;
      continue;
    }

    // Regex literal heuristic: `/` after operator/keyword/open-punct (not identifier/number/`)`/`]`)
    if (ch === 47) {
      const prev = tokens[tokens.length - 1];
      const canRegex = !prev
        || (prev.type === "punct" && prev.value !== ")" && prev.value !== "]" && prev.value !== "}")
        || (prev.type === "ident" && (prev.value === "return" || prev.value === "case" || prev.value === "throw" || prev.value === "typeof" || prev.value === "in" || prev.value === "of"));
      if (canRegex) {
        const startLine = line;
        let j = i + 1;
        let inClass = false;
        let closed = false;
        while (j < len && src.charCodeAt(j) !== 10) {
          const c = src.charCodeAt(j);
          if (c === 92) { j += 2; continue; }
          if (c === 91) inClass = true;
          else if (c === 93) inClass = false;
          else if (c === 47 && !inClass) { closed = true; j++; break; }
          j++;
        }
        if (closed) {
          while (j < len && /[a-z]/i.test(src[j])) j++;
          tokens.push({ type: "regex", value: src.slice(i, j), line: startLine });
          i = j;
          continue;
        }
      }
    }

    // String literals (' or ")
    if (ch === 39 || ch === 34) {
      const quote = ch;
      const startLine = line;
      i++;
      while (i < len) {
        const c = src.charCodeAt(i);
        if (c === 92) { i += 2; continue; }
        if (c === 10) line++;
        if (c === quote) { i++; break; }
        i++;
      }
      tokens.push({ type: "string", value: "\"\"", line: startLine });
      continue;
    }

    // Template literals (`...`)
    if (ch === 96) {
      const startLine = line;
      let hasExpr = false;
      i++;
      while (i < len) {
        const c = src.charCodeAt(i);
        if (c === 92) { i += 2; continue; }
        if (c === 10) line++;
        if (c === 36 && src.charCodeAt(i + 1) === 123) {
          hasExpr = true;
          // Skip nested `${...}` expression balance
          i += 2;
          let depth = 1;
          while (i < len && depth > 0) {
            const cc = src.charCodeAt(i);
            if (cc === 92) { i += 2; continue; }
            if (cc === 10) line++;
            if (cc === 123) depth++;
            else if (cc === 125) depth--;
            i++;
          }
          continue;
        }
        if (c === 96) { i++; break; }
        i++;
      }
      tokens.push({ type: hasExpr ? "template-expr" : "string", value: "`", line: startLine });
      continue;
    }

    // Numbers
    if (ch >= 48 && ch <= 57) {
      const start = i;
      while (i < len && /[0-9a-fA-FxXoObB_.eE]/.test(src[i])) i++;
      tokens.push({ type: "number", value: src.slice(start, i), line });
      continue;
    }

    // Identifiers / keywords
    if ((ch >= 65 && ch <= 90) || (ch >= 97 && ch <= 122) || ch === 95 || ch === 36) {
      const start = i;
      while (i < len) {
        const c = src.charCodeAt(i);
        if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122) || (c >= 48 && c <= 57) || c === 95 || c === 36) i++;
        else break;
      }
      tokens.push({ type: "ident", value: src.slice(start, i), line });
      continue;
    }

    // Multi-char punctuators we care about
    if (ch === 61 && src.charCodeAt(i + 1) === 62) {
      tokens.push({ type: "punct", value: "=>", line });
      i += 2;
      continue;
    }
    if (ch === 63 && src.charCodeAt(i + 1) === 46) {
      tokens.push({ type: "punct", value: "?.", line });
      i += 2;
      continue;
    }

    tokens.push({ type: "punct", value: src[i], line });
    i++;
  }
  return tokens;
}

function findMatching(tokens, startIdx, openVal, closeVal) {
  let depth = 0;
  for (let i = startIdx; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.type === "punct" && t.value === openVal) depth++;
    else if (t.type === "punct" && t.value === closeVal) {
      depth--;
      if (depth === 0) return i;
    }
  }
  return tokens.length - 1;
}

function isSimpleStringConcat(tokens, start, end) {
  // Detects template-expr directly, or a top-level `+` involving a string/template token.
  if (start === end && tokens[start]?.type === "template-expr") return true;
  let depth = 0;
  let sawPlus = false;
  let sawStr = false;
  for (let i = start; i <= end; i++) {
    const t = tokens[i];
    if (t.type === "punct" && (t.value === "(" || t.value === "[" || t.value === "{")) depth++;
    else if (t.type === "punct" && (t.value === ")" || t.value === "]" || t.value === "}")) depth--;
    else if (depth === 0) {
      if (t.type === "punct" && t.value === "+") sawPlus = true;
      if (t.type === "string" || t.type === "template-expr") sawStr = true;
    }
  }
  return sawPlus && sawStr;
}

export function zoneOf(file, zones) {
  for (const z of Object.keys(zones)) {
    for (const p of zones[z]) {
      if (p.endsWith("/") ? file.startsWith(p) : p.includes("*") ? matchesGlob(file, p) : file === p) return z;
    }
  }
  return null;
}

/**
 * Scans one TypeScript source file for DOD violations.
 * `inZone` enables the stricter hot-zone allocation/iterator rules.
 */
export function scan(src, file, inZone) {
  const tokens = tokenizeTs(src);
  const violations = [];
  const add = (line, rule, msg) => violations.push({ file, line, rule, msg });

  // Track active loop brace scopes and single-statement loop end tokens.
  const braceLoopStack = []; // boolean per `{`
  let loopDepth = 0;
  let pendingLoopStmtEnd = -1;

  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i];
    const inLoop = loopDepth > 0 || i <= pendingLoopStmtEnd;

    // Detect `for (...)`, `while (...)`, `do { ... }`
    if (tok.type === "ident" && (tok.value === "for" || tok.value === "while")) {
      // Optional `await` for `for await (...)`
      let lp = i + 1;
      if (tok.value === "for" && tokens[lp]?.type === "ident" && tokens[lp].value === "await") lp++;
      if (tokens[lp]?.type === "punct" && tokens[lp].value === "(") {
        const rp = findMatching(tokens, lp, "(", ")");
        // Check for `for-of` / `for-in` in hot zones
        if (tok.value === "for" && inZone) {
          let pDepth = 0;
          for (let j = lp + 1; j < rp; j++) {
            const jt = tokens[j];
            if (jt.type === "punct" && (jt.value === "(" || jt.value === "[" || jt.value === "{")) pDepth++;
            else if (jt.type === "punct" && (jt.value === ")" || jt.value === "]" || jt.value === "}")) pDepth--;
            else if (pDepth === 0 && jt.type === "ident") {
              if (jt.value === "of") { add(tok.line, "for-of", "iterator protocol in hot zone: use an indexed for loop"); break; }
              if (jt.value === "in") { add(tok.line, "for-in", "key enumeration in hot zone: use an indexed loop"); break; }
            }
          }
        }
        // Mark the loop body
        const bodyStart = rp + 1;
        if (tokens[bodyStart]?.type === "punct" && tokens[bodyStart].value === "{") {
          // Will be marked when we step onto `{` — track via index check below
          tokens[bodyStart]._isLoopBrace = true;
        } else {
          // Single-statement loop body: scan to next top-level `;`
          let sDepth = 0;
          let k = bodyStart;
          while (k < tokens.length) {
            const kt = tokens[k];
            if (kt.type === "punct" && (kt.value === "(" || kt.value === "[" || kt.value === "{")) sDepth++;
            else if (kt.type === "punct" && (kt.value === ")" || kt.value === "]" || kt.value === "}")) sDepth--;
            else if (sDepth === 0 && kt.type === "punct" && kt.value === ";") break;
            k++;
          }
          if (k > pendingLoopStmtEnd) pendingLoopStmtEnd = k;
        }
      }
      continue;
    }

    if (tok.type === "ident" && tok.value === "do" && tokens[i + 1]?.type === "punct" && tokens[i + 1].value === "{") {
      tokens[i + 1]._isLoopBrace = true;
      continue;
    }

    if (tok.type === "punct" && tok.value === "{") {
      const isLoop = Boolean(tok._isLoopBrace);
      braceLoopStack.push(isLoop);
      if (isLoop) loopDepth++;
      continue;
    }
    if (tok.type === "punct" && tok.value === "}") {
      const wasLoop = braceLoopStack.pop();
      if (wasLoop) loopDepth--;
      continue;
    }

    // Method calls: `<obj>.<prop>(` or `<obj>?.<prop>(`
    if (tok.type === "punct" && (tok.value === "." || tok.value === "?.")) {
      const prop = tokens[i + 1];
      const lp = tokens[i + 2];
      const recv = tokens[i - 1];
      if (prop?.type === "ident" && lp?.type === "punct" && lp.value === "(") {
        const rp = findMatching(tokens, i + 2, "(", ")");

        // 1. concat-key (repo-wide): `.get/.set/.has/.delete/.add(<str concat or template>)`
        if (KEYED_METHODS.has(prop.value) && rp > i + 3) {
          // Find end of first argument
          let firstArgEnd = rp - 1;
          let aDepth = 0;
          for (let j = i + 3; j < rp; j++) {
            const jt = tokens[j];
            if (jt.type === "punct" && (jt.value === "(" || jt.value === "[" || jt.value === "{")) aDepth++;
            else if (jt.type === "punct" && (jt.value === ")" || jt.value === "]" || jt.value === "}")) aDepth--;
            else if (aDepth === 0 && jt.type === "punct" && jt.value === ",") { firstArgEnd = j - 1; break; }
          }
          if (isSimpleStringConcat(tokens, i + 3, firstArgEnd)) {
            add(prop.line, "concat-key", `string-built key in .${prop.value}(): prefer dense int or structured key`);
          }
        }

        // 2. sql-in-loop (repo-wide): DB receiver method inside a loop
        if (inLoop && DB_METHODS.has(prop.value) && recv?.type === "ident" && DB_RECEIVERS.has(recv.value)) {
          add(prop.line, "sql-in-loop", `${recv.value}.${prop.value}() inside a loop (N+1): hoist and batch`);
        }

        // 3. Hot-zone rules
        if (inZone) {
          if (ITER_METHODS.has(prop.value)) {
            add(prop.line, "iter-method", `.${prop.value}() allocates a closure per call in hot zone: use an indexed loop`);
            // Also mark callback body `{` inside iter-method as a loop scope for sql-in-loop detection
            for (let j = i + 3; j < rp; j++) {
              if (tokens[j].type === "punct" && tokens[j].value === "=>" && tokens[j + 1]?.value === "{") {
                tokens[j + 1]._isLoopBrace = true;
              }
            }
          }
          if (recv?.type === "ident" && recv.value === "JSON" && (prop.value === "parse" || prop.value === "stringify")) {
            add(prop.line, "json-in-zone", `JSON.${prop.value}() in hot zone: decode/encode once at the edge`);
          }
          if (inLoop && recv?.type === "ident" && (
            (recv.value === "Object" && (prop.value === "keys" || prop.value === "values" || prop.value === "entries" || prop.value === "fromEntries"))
            || (recv.value === "Array" && prop.value === "from")
          )) {
            add(prop.line, "alloc-in-loop", `${recv.value}.${prop.value}() inside loop in hot zone`);
          }
        }
      }
      continue;
    }

    // `new <Ctor>` inside a loop in a hot zone (except typed array / DataView / TextEncoder views or Error)
    if (inZone && inLoop && tok.type === "ident" && tok.value === "new") {
      const ctor = tokens[i + 1];
      if (ctor?.type === "ident" && !ctor.value.endsWith("Error") && ctor.value !== "Uint8Array" && ctor.value !== "Int32Array" && ctor.value !== "Uint32Array" && ctor.value !== "Uint16Array" && ctor.value !== "Float64Array" && ctor.value !== "DataView") {
        add(tok.line, "alloc-in-loop", `new ${ctor.value}() inside loop in hot zone`);
      }
    }
  }

  return violations;
}

const ROOT = fileURLToPath(new URL("../", import.meta.url));
const EXCLUDE = /(\.d\.ts|\.test\.ts|\.spec\.ts)$|(^|\/)(tests?|test|__tests__|fixtures)\//;

export function files(root = ROOT) {
  return globSync(
    ["src/**/*.ts", "kernel/**/*.ts", "control/**/*.ts", "router/**/*.ts", "actions/**/*.ts"],
    { cwd: root },
  )
    .map((f) => f.replaceAll("\\", "/"))
    .filter((f) => !EXCLUDE.test(f))
    .sort();
}

export function loadZones(root = ROOT) {
  return JSON.parse(readFileSync(root + "scripts/dod.zones.json", "utf8"));
}

export function loadBaseline(root = ROOT) {
  const p = root + "scripts/dod.baseline.json";
  return existsSync(p) ? JSON.parse(readFileSync(p, "utf8")) : {};
}

export function scanRepo(root = ROOT, zones = loadZones(root)) {
  const out = [];
  for (const f of files(root)) {
    const src = readFileSync(root + f, "utf8");
    out.push(...scan(src, f, zoneOf(f, zones) !== null));
  }
  return out;
}

export function countsOf(vs) {
  const c = {};
  for (const v of vs) {
    const r = (c[v.file] ??= {});
    r[v.rule] = (r[v.rule] ?? 0) + 1;
  }
  return c;
}

export function compare(cur, base) {
  const over = [];
  const under = [];
  for (const file of Object.keys(cur)) {
    for (const rule of Object.keys(cur[file])) {
      const n = cur[file][rule];
      const b = base[file]?.[rule] ?? 0;
      if (n > b) over.push({ file, rule, n, base: b });
    }
  }
  for (const file of Object.keys(base)) {
    for (const rule of Object.keys(base[file])) {
      const b = base[file][rule];
      const n = cur[file]?.[rule] ?? 0;
      if (n < b) under.push({ file, rule, n, base: b });
    }
  }
  return { over, under };
}

export function updateBaseline(cur, base, allowRaise = false) {
  const { over } = compare(cur, base);
  if (over.length && !allowRaise) return { ok: false, raises: over };
  const baseline = {};
  for (const file of Object.keys(cur).sort()) {
    const r = {};
    for (const rule of RULES) if (cur[file][rule]) r[rule] = cur[file][rule];
    if (Object.keys(r).length) baseline[file] = r;
  }
  return { ok: true, baseline };
}

export const formatBaseline = (b) =>
  "{\n" + Object.keys(b).map((f) => `  ${JSON.stringify(f)}: ${JSON.stringify(b[f])}`).join(",\n") + "\n}\n";

export function report(vs, zones) {
  const byZone = new Map();
  const byRule = new Map();
  const byFile = new Map();
  for (const v of vs) {
    const z = zoneOf(v.file, zones) ?? "(repo-wide)";
    let zm = byZone.get(z);
    if (!zm) byZone.set(z, (zm = new Map()));
    zm.set(v.rule, (zm.get(v.rule) ?? 0) + 1);
    byRule.set(v.rule, (byRule.get(v.rule) ?? 0) + 1);
    byFile.set(v.file, (byFile.get(v.file) ?? 0) + 1);
  }
  const lines = ["dod report: per zone"];
  for (const z of [...Object.keys(zones), "(repo-wide)"]) {
    const m = byZone.get(z);
    if (!m) {
      lines.push(`  ${z.padEnd(16)}     0  (clean)`);
      continue;
    }
    let total = 0;
    for (const n of m.values()) total += n;
    lines.push(
      `  ${z.padEnd(16)} ${String(total).padStart(5)}  ` +
      RULES.filter((r) => m.has(r)).map((r) => `${r} ${m.get(r)}`).join(", "),
    );
  }
  lines.push("per rule:");
  for (const r of RULES) lines.push(`  ${r.padEnd(16)} ${String(byRule.get(r) ?? 0).padStart(5)}`);
  lines.push(`  ${"total".padEnd(16)} ${String(vs.length).padStart(5)}`, "top 10 files:");
  const top = [...byFile.entries()].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, 10);
  for (const [f, n] of top) lines.push(`  ${String(n).padStart(5)}  ${f}`);
  return lines.join("\n");
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const argv = process.argv.slice(2);
  const t0 = performance.now();
  const zones = loadZones();
  const base = loadBaseline();
  const vs = scanRepo(ROOT, zones);
  const cur = countsOf(vs);
  if (argv.includes("--report")) console.log(report(vs, zones));
  if (argv.includes("--update")) {
    const res = updateBaseline(cur, base, argv.includes("--allow-raise"));
    if (!res.ok) {
      for (const d of res.raises) console.error(`dod: refusing to raise ${d.file} ${d.rule} ${d.base} -> ${d.n}`);
      console.error("dod: baseline only ratchets down; fix the code or pass --allow-raise in a dedicated PR");
      process.exit(1);
    }
    writeFileSync(ROOT + "scripts/dod.baseline.json", formatBaseline(res.baseline));
    console.log(`dod: baseline written (${vs.length} violations in ${Object.keys(res.baseline).length} files)`);
  } else {
    const { over, under } = compare(cur, base);
    for (const d of over) {
      for (const v of vs) {
        if (v.file === d.file && v.rule === d.rule) console.error(`${v.file}:${v.line} ${v.rule} ${v.msg}`);
      }
      console.error(
        d.base
          ? `dod: ${d.file} ${d.rule} ${d.n} > baseline ${d.base}`
          : `dod: ${d.file} ${d.rule} ${d.n}, baseline 0${base[d.file] ? "" : " (new file: not in baseline)"}`,
      );
    }
    const elapsed = (performance.now() - t0).toFixed(0);
    if (over.length) {
      console.error(`dod: FAILED (${over.length} rule(s) over baseline in ${elapsed} ms)`);
      process.exit(1);
    }
    const ratchetNote = under.length ? `; ratchet: ${under.length} count(s) can be lowered via --update` : "";
    console.log(`dod: OK (${vs.length} baselined violations in ${elapsed} ms${ratchetNote})`);
  }
}
