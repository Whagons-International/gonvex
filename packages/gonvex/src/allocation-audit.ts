import ts from "typescript-api";
import { createHash } from "node:crypto";
import { readFile, readdir, stat, writeFile } from "node:fs/promises";
import { relative, resolve, extname } from "node:path";

export type AllocationFinding = {
  file: string; line: number; column: number; rule: string; scope: string;
  message: string; fingerprint: string;
};
const messages = {
  "copying-accumulator": "Copying a reduce accumulator can allocate quadratically. Mutate one locally owned result instead.",
  "bulk-row-copy": "This map materializes a new object per row. Prefer canonical references or a bounded display projection.",
  "array-pipeline": "This array pipeline creates an intermediate collection. Consider one pass after measuring the hot path.",
  "repeated-encoder": "Reuse a TextEncoder outside frequently called functions; encodeInto can reuse a caller-owned output buffer.",
  "buffer-slice": "Typed-array slice copies bytes. A subarray shares storage but can retain the entire backing buffer; choose deliberately.",
  "deep-clone": "This creates a deep copy. Check canonical references or a narrow projection, while preserving required caller ownership.",
  "loop-allocation": "This loop or collection callback allocates a new container. Check its lifetime and whether bounded or reusable storage helps.",
  "hot-allocation": "Explicitly hot function creates a container, closure, string, view or serialized value. Reuse caller-owned storage or document the bounded/cold allocation. This is a syntactic check, not a zero-allocation proof.",
} as const;
type Rule = keyof typeof messages;
const collectionMethods = new Set(["map", "flatMap", "filter", "reduce", "forEach", "some", "every", "find"]);
const typedArrays = /^(?:Uint8Clamped|(?:U?Int|Uint|Int|Float|BigInt|BigUint)\d+)Array$/;

/** Syntactic hints only. Explicit hot paths avoid imposing allocation bans on ordinary UI code. */
export function auditAllocations(text: string, file: string, hotPath = true, strictHot = false): AllocationFinding[] {
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, /\.[jt]sx$/.test(file) ? ts.ScriptKind.TSX : ts.ScriptKind.TS);
  const findings: AllocationFinding[] = [];
  const lines = text.split(/\r?\n/);
  const occurrences = new Map<string, number>();
  const typedNames = new Set<string>();
  const method = (node: ts.Node): string | undefined => ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression) ? node.expression.name.text : undefined;
  const isFunction = (node: ts.Node): node is ts.FunctionLikeDeclaration => ts.isArrowFunction(node) || ts.isFunctionExpression(node) || ts.isFunctionDeclaration(node) || ts.isMethodDeclaration(node) || ts.isGetAccessor(node) || ts.isSetAccessor(node);
  function typeScan(node: ts.Node) {
    if ((ts.isVariableDeclaration(node) || ts.isParameter(node)) && ts.isIdentifier(node.name)) {
      const init = ts.isVariableDeclaration(node) ? node.initializer : undefined;
      const type = node.type?.getText(source).replace(/<.*>/, "");
      if ((type && typedArrays.test(type)) || (init && ts.isNewExpression(init) && typedArrays.test(init.expression.getText(source)))) typedNames.add(node.name.text);
    }
    ts.forEachChild(node, typeScan);
  }
  typeScan(source);
  function markedFunction(node: ts.FunctionLikeDeclaration): boolean {
    const declarations: ts.Node[] = [node];
    if ((ts.isArrowFunction(node) || ts.isFunctionExpression(node)) && ts.isVariableDeclaration(node.parent)) {
      let statement: ts.Node = node.parent;
      while (!ts.isStatement(statement) && statement.parent && !ts.isSourceFile(statement.parent)) statement = statement.parent;
      declarations.push(statement);
    }
    return declarations.some(declaration => (ts.getLeadingCommentRanges(text, declaration.getFullStart()) ?? [])
      .some(comment => /@allocation-hot\b/.test(text.slice(comment.pos, comment.end))));
  }
  function strictAllocation(node: ts.Node): boolean {
    if (ts.isObjectLiteralExpression(node) || ts.isArrayLiteralExpression(node) || ts.isNewExpression(node)
      || ts.isTemplateExpression(node) || node.kind === ts.SyntaxKind.RegularExpressionLiteral
      || ts.isBindingElement(node) && !!node.dotDotDotToken) return true;
    if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken
      && (ts.isStringLiteralLike(node.left) || ts.isStringLiteralLike(node.right))) return true;
    if (!ts.isCallExpression(node)) return false;
    const callee = node.expression.getText(source);
    if (/^(?:JSON\.(?:parse|stringify)|Object\.(?:keys|values|entries|fromEntries|create|groupBy)|Map\.groupBy|Reflect\.ownKeys|Array(?:\.from|\.of)?|structuredClone|Buffer\.(?:from|alloc|allocUnsafe))$/.test(callee)) return true;
    if (ts.isPropertyAccessExpression(node.expression) && ["from", "of"].includes(node.expression.name.text)
      && typedArrays.test(node.expression.expression.getText(source))) return true;
    return ["map", "filter", "flatMap", "flat", "slice", "subarray", "concat", "split", "match", "matchAll", "encode", "encodeInto", "decode", "toSorted", "toReversed", "toSpliced", "entries", "keys", "values"].includes(method(node) ?? "");
  }
  function emit(node: ts.Node, rule: Rule, scope: string) {
    const position = source.getLineAndCharacterOfPosition(node.getStart(source));
    // An exemption must name the rule and explain why; never apply across an entire file.
    const comment = [lines[position.line - 1] ?? "", (lines[position.line] ?? "").slice(0, position.character)].join("\n");
    if (new RegExp(`allocation-allow[ \\t]+${rule}:[ \\t]*[^\\s][^\\r\\n]+`).test(comment)) return;
    const signature = `${file}:${scope}:${rule}:${node.getText(source).replace(/\s+/g, " ")}`;
    const occurrence = (occurrences.get(signature) ?? 0) + 1;
    occurrences.set(signature, occurrence);
    findings.push({ file, line: position.line + 1, column: position.character + 1, rule, scope, message: messages[rule], fingerprint: createHash("sha256").update(`${signature}:${occurrence}`).digest("hex").slice(0, 20) });
  }
  function walk(node: ts.Node, hot: boolean, loop: boolean, scope: string, accumulator?: string, strict = false) {
    if (isFunction(node)) {
      if (strict && (ts.isArrowFunction(node) || ts.isFunctionExpression(node) || ts.isFunctionDeclaration(node))) emit(node, "hot-allocation", scope);
      const marked = markedFunction(node);
      hot ||= marked;
      strict = strictHot && marked;
      const parent = node.parent;
      if (ts.isVariableDeclaration(parent) && ts.isIdentifier(parent.name)) scope = parent.name.text;
      else if (node.name) scope = node.name.getText(source);
      // An allocation inside a nested declaration is not automatically executed by the enclosing loop.
      loop = ts.isCallExpression(parent) && collectionMethods.has(method(parent) ?? "");
      accumulator = method(parent) === "reduce" && node.parameters[0] && ts.isIdentifier(node.parameters[0].name) ? node.parameters[0].name.text : undefined;
      if (strict && node.modifiers?.some(modifier => modifier.kind === ts.SyntaxKind.AsyncKeyword)) emit(node, "hot-allocation", scope);
    }
    const enclosingLoop = loop;
    if (ts.isForStatement(node) || ts.isForOfStatement(node) || ts.isForInStatement(node) || ts.isWhileStatement(node) || ts.isDoStatement(node)) loop = true;
    if (strictHot) {
      if (strict && strictAllocation(node)) emit(node, "hot-allocation", scope);
    } else if (hot) {
      if (ts.isCallExpression(node) && (ts.isIdentifier(node.expression) && node.expression.text === 'structuredClone'
        || ts.isPropertyAccessExpression(node.expression) && node.expression.getText(source) === 'JSON.parse'
          && node.arguments[0] && ts.isCallExpression(node.arguments[0]) && node.arguments[0].expression.getText(source) === 'JSON.stringify')) emit(node, 'deep-clone', scope);
      const spreads = ts.isObjectLiteralExpression(node) ? node.properties.filter(ts.isSpreadAssignment) : ts.isArrayLiteralExpression(node) ? node.elements.filter(ts.isSpreadElement) : [];
      const copyingAccumulator = accumulator && spreads.some(spread => ts.isIdentifier(spread.expression) && spread.expression.text === accumulator);
      if (copyingAccumulator) emit(node, "copying-accumulator", scope);
      else if (ts.isObjectLiteralExpression(node) && spreads.length && insideMap(node)) emit(node, "bulk-row-copy", scope);
      else if (loop && (ts.isObjectLiteralExpression(node) || ts.isArrayLiteralExpression(node) || ts.isNewExpression(node))) emit(node, "loop-allocation", scope);
      if (ts.isNewExpression(node) && node.expression.getText(source) === "TextEncoder" && scope !== "<module>") emit(node, "repeated-encoder", scope);
      if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)) {
        const receiver = node.expression.expression, name = node.expression.name.text;
        if (["map", "filter", "flatMap"].includes(name) && ["map", "filter", "flatMap"].includes(method(receiver) ?? "")) emit(node, "array-pipeline", scope);
        if (name === "slice" && ((ts.isIdentifier(receiver) && typedNames.has(receiver.text)) || (ts.isNewExpression(receiver) && typedArrays.test(receiver.expression.getText(source))))) emit(node, "buffer-slice", scope);
      }
    }
    ts.forEachChild(node, child => {
      // A for-of collection and a for initializer execute once, outside the
      // repeated body. Do not flag a one-time output buffer as per-item churn.
      const once = (ts.isForOfStatement(node) || ts.isForInStatement(node)) && child !== node.statement
        || ts.isForStatement(node) && child === node.initializer;
      walk(child, hot, once ? enclosingLoop : loop, scope, accumulator, strict);
    });
  }
  function insideMap(node: ts.Node): boolean {
    for (let parent = node.parent; parent && !ts.isSourceFile(parent); parent = parent.parent) {
      if (isFunction(parent)) return method(parent.parent) === "map";
    }
    return false;
  }
  walk(source, hotPath, false, "<module>");
  return findings;
}

export async function runAllocationAudit(argv: string[]): Promise<void> {
  const paths: string[] = [];
  let json = false, check = false, markedOnly = false, strictHot = false, baseline: string | undefined, save: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === "--json") json = true;
    else if (arg === "--check") check = true;
    else if (arg === "--marked-only") markedOnly = true;
    else if (arg === "--strict-hot") { strictHot = true; markedOnly = true; }
    else if (arg === "--baseline" || arg === "--write-baseline") {
      const value = argv[++i]; if (!value) throw new Error(`Missing value for ${arg}`);
      if (arg === "--baseline") baseline = value; else save = value;
    } else if (arg.startsWith("--")) throw new Error(`Unknown allocation audit option ${arg}`);
    else paths.push(arg);
  }
  if (!paths.length) throw new Error("Usage: gonvex perf allocations <hot-path>... [--json] [--baseline FILE --check] [--write-baseline FILE]");
  if (strictHot && (baseline || save)) throw new Error("--strict-hot does not accept a baseline. Document individual justified allocations with a rule and reason.");
  const files = new Set<string>();
  async function collect(path: string) {
    const info = await stat(path);
    if (info.isDirectory()) {
      for (const entry of await readdir(path, { withFileTypes: true })) {
        if (entry.isSymbolicLink() || entry.name.startsWith(".") || ["node_modules", "dist", "test-results"].includes(entry.name)) continue;
        await collect(resolve(path, entry.name));
      }
    } else if ([".ts", ".tsx", ".js", ".jsx", ".mjs"].includes(extname(path)) && !/\.(?:test|spec)\.[^.]+$|\.d\.ts$/.test(path)) files.add(path);
  }
  for (const path of paths) await collect(resolve(path));
  const findings: AllocationFinding[] = [];
  for (const file of [...files].sort()) findings.push(...auditAllocations(await readFile(file, "utf8"), relative(process.cwd(), file).replaceAll("\\", "/"), !markedOnly, strictHot));
  const existing = new Set<string>(baseline ? JSON.parse(await readFile(baseline, "utf8")) : []);
  const added = findings.filter(finding => !existing.has(finding.fingerprint));
  if (save) await writeFile(save, JSON.stringify([...new Set(findings.map(f => f.fingerprint))].sort(), null, 2) + "\n");
  if (json) console.log(JSON.stringify({ scannedFiles: files.size, findings, newFindings: added.length, limitations: "Syntactic allocation hints; does not measure retained memory or prove zero allocations." }, null, 2));
  else {
    for (const f of check && baseline ? added : findings) console.log(`${f.file}:${f.line}:${f.column} warning ${f.rule} (${f.scope}): ${f.message}`);
    console.log(`${files.size} files; ${findings.length} hints; ${added.length} outside baseline.`);
  }
  if (check && added.length) process.exitCode = 1;
}
