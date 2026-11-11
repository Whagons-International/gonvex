// App-source checks that ride on the transform drift job (0022 §2a, §5).
//
//   node --experimental-transform-types verify/app-source-checks.mjs <whagons5-client checkout> [--json out.json]
//   (K01's sources use TypeScript parameter properties, which strip-only mode rejects.)
//
// 1. REGEXP: every pattern the app's module sources hand to the SDK's
//    `matches` predicate (`op: 'matches', pattern: ...`) or write after
//    `REGEXP` in SQL text, compiled with K01's translator
//    (kernel/storage/regexp.ts `regexpPredicate`). A template literal's
//    `${...}` parts are taken as plain letters (the app interpolates fixed
//    document prefixes). A pattern K01 cannot translate, or a pattern that is
//    not a literal, fails the job.
// 2. T3's generated columns: storage tables that carry them (from the
//    transform over the checkout's migrations) are visible to `SELECT *`, so
//    no module source may name them. Modules address records through their
//    public views, which list columns explicitly.
//
// Exit 1 on a failure. Prints file:line, patterns and table names only.
import { registerHooks } from "node:module";
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

// K01's sources use extension-less relative imports; resolve them to `.ts`.
registerHooks({
  resolve(specifier, context, next) {
    try {
      return next(specifier, context);
    } catch (error) {
      if (specifier.startsWith(".") && !/\.[cm]?[jt]s$/.test(specifier)) return next(`${specifier}.ts`, context);
      throw error;
    }
  },
});

const args = process.argv.slice(2);
const client = args[0];
const jsonOut = args.includes("--json") ? args[args.indexOf("--json") + 1] : null;
if (!client) {
  console.error("usage: app-source-checks.mjs <whagons5-client checkout> [--json out.json]");
  process.exit(2);
}
const here = fileURLToPath(new URL(".", import.meta.url));
const storageRegexp = join(here, "../../storage/regexp.ts");
const { SchemaTransform } = await import("../index.ts");

function walk(dir, out = []) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name === "_generated" || entry.name.startsWith(".")) continue;
    const path = join(dir, entry.name);
    if (entry.isDirectory()) walk(path, out);
    else if (/\.(ts|tsx|js|mjs|cjs)$/.test(entry.name)) out.push(path);
  }
  return out;
}
const files = walk(join(client, "gonvex")).sort();
const isTest = (f) => /\.(test|spec)\.[^.]+$/.test(f) || /\/(test|tests|__tests__)\//.test(f);
const lineOf = (text, index) => text.slice(0, index).split("\n").length;

// --- 1. REGEXP patterns ---------------------------------------------------------------
const patterns = [];
const LITERAL = /^\s*(?:'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)"|`((?:[^`\\]|\\.)*)`)/;
const unescape = (s) => s.replace(/\\(.)/g, "$1");
for (const file of files) {
  const text = readFileSync(file, "utf8");
  const where = (i) => `${relative(client, file)}:${lineOf(text, i)}`;
  for (const m of text.matchAll(/\bop\s*:\s*(['"`])matches\1/g)) {
    // The pattern property of the same object literal: search a bounded window around `op`.
    const start = Math.max(0, text.lastIndexOf("{", m.index), m.index - 400);
    const window = text.slice(start, m.index + 400);
    const p = /\bpattern\s*:\s*/.exec(window);
    if (!p) {
      patterns.push({ at: where(m.index), source: null, test: isTest(file), problem: "no `pattern:` next to `op: 'matches'`" });
      continue;
    }
    const lit = LITERAL.exec(window.slice(p.index + p[0].length));
    if (!lit) {
      patterns.push({ at: where(m.index), source: window.slice(p.index + p[0].length, p.index + p[0].length + 60).trim(), test: isTest(file), problem: "pattern is not a literal" });
      continue;
    }
    const raw = lit[1] ?? lit[2] ?? lit[3];
    const templated = lit[3] !== undefined && /\$\{[^}]*\}/.test(raw);
    patterns.push({ at: where(m.index), source: raw, pattern: unescape(raw.replace(/\$\{[^}]*\}/g, "A")), templated, test: isTest(file) });
  }
  for (const m of text.matchAll(/\bREGEXP\s+'((?:[^']|'')*)'/g)) {
    patterns.push({ at: where(m.index), source: m[1], pattern: m[1].replace(/''/g, "'"), templated: false, test: isTest(file) });
  }
}
let compiler = null;
if (existsSync(storageRegexp)) compiler = (await import(storageRegexp)).regexpPredicate;
for (const p of patterns) {
  if (p.problem || !compiler) continue;
  try {
    compiler(p.pattern, '"value"');
    p.compiles = true;
  } catch (error) {
    p.compiles = false;
    p.problem = String(error.message).slice(0, 200);
  }
}

// --- 2. storage tables with T3 generated columns ----------------------------------------
const migrations = join(client, "migrations");
const transform = new SchemaTransform();
for (const f of readdirSync(migrations).filter((f) => /^\d+_.*\.sql$/.test(f)).sort()) transform.migration(f, readFileSync(join(migrations, f), "utf8"));
const carrying = [...new Set(transform.addedObjects().filter((o) => o.type === "column").map((o) => o.of))].sort();
const mentions = [];
for (const file of files) {
  const text = readFileSync(file, "utf8");
  for (const table of carrying) {
    for (const m of text.matchAll(new RegExp(`\\b${table.replace(/[$]/g, "\\$&")}\\b`, "g"))) mentions.push({ at: `${relative(client, file)}:${lineOf(text, m.index)}`, table, test: isTest(file) });
  }
}

const failures = [
  ...patterns.filter((p) => !p.test && (p.problem || p.compiles === false)).map((p) => `REGEXP at ${p.at}: ${p.problem}`),
  ...mentions.filter((m) => !m.test).map((m) => `${m.at} names ${m.table}, which carries T3 generated columns (visible to SELECT *); use its public view`),
];
const report = {
  files: files.length,
  regexp: { compiler: compiler ? "kernel/storage/regexp.ts" : null, patterns },
  generatedColumnTables: carrying,
  storageTableMentions: mentions,
  failures,
};
console.log(JSON.stringify({ files: report.files, regexpCompiler: report.regexp.compiler, patterns: patterns.map((p) => ({ at: p.at, source: p.source, compiles: p.compiles ?? null, test: p.test })), generatedColumnTables: carrying, storageTableMentions: mentions.length, failures }, null, 1));
if (!compiler) console.log("::warning::kernel/storage/regexp.ts (K01) is not in the tree: REGEXP patterns were listed but not compiled.");
if (jsonOut) writeFileSync(jsonOut, JSON.stringify(report, null, 2) + "\n");
process.exit(failures.length ? 1 : 0);
